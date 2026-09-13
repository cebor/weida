//! Transfer handles.
//!
//! These own their `quinn` streams outright: payload bytes go straight to the
//! socket without passing through the connection actor, so writing and reading
//! a transfer costs no task hop and takes no lock (master doc §49). The actor
//! is involved only for the two control frames a destructor may still need to
//! emit — and `finish()` costs one more thing, paid once at the end of a
//! transfer rather than per write: an unawaited `Delivery` hands its receipt
//! to the connection's parked set on drop, which is one uncontended lock and
//! a push, so that a drain can wait for it
//! (`docs/decisions/0009-drain.md` §4.2).
//!
//! Nothing here materializes a payload. `AsyncRead`/`AsyncWrite` are the
//! primitive API; `collect(max_bytes)` is an opt-in convenience with an
//! explicit cap (master doc §8, §81 rule 3).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::transport::{RecvHalf, SendHalf};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use weida_core::{Error, ErrorCode, PeerIdentity, TraceContext};
use weida_protocol::header::Acknowledgement;
use weida_protocol::{DataHeader, ErrorHeader, FrameKind, codes, encode_preamble};

use crate::conn::{ConnHandle, Ctl, read_frame, write_error_frame};
use crate::drain::Receipt;
use crate::ordering::Gap;

/// Per-transfer metadata supplied by the application.
#[derive(Clone, Debug, Default)]
pub struct TransferMeta {
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Trace context to propagate. `None` generates a fresh root context.
    pub trace: Option<TraceContext>,
    /// Topic this transfer is labelled with (DATA key `5`).
    ///
    /// Opaque bytes, selected by the filter grammar of `docs/PROTOCOL.md`
    /// §6.4. A publisher's fan-out sets it per copy and does not read this
    /// field; what it is here for is the **other** sender that needs a label:
    /// a producer sending to an L2 queue, whose consumers each filter on it
    /// (0018 §4.5). A topic is never a pattern — `*` in one is an ordinary
    /// byte.
    pub topic: Option<String>,
    /// The completion level this side **achieved** for the message it is
    /// answering (DATA key `8`).
    ///
    /// Only an L2 hop sets it, and only on a reply: it is the publisher
    /// confirm of
    /// [decisions/0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
    /// §4.6. A v0 application leaves it `None`, which claims nothing beyond
    /// the transport receipt.
    pub achieved: Option<Acknowledgement>,
}

impl TransferMeta {
    /// Sets the content type label.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> TransferMeta {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets the advisory content length.
    pub fn with_content_len(mut self, len: u64) -> TransferMeta {
        self.content_len = Some(len);
        self
    }

    /// Propagates an existing trace context.
    pub fn with_trace(mut self, trace: TraceContext) -> TransferMeta {
        self.trace = Some(trace);
        self
    }

    /// Labels the transfer with a topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> TransferMeta {
        self.topic = Some(topic.into());
        self
    }

    /// States the completion level this side achieved.
    ///
    /// A claim about this hop only, and a claim a peer will act on: `weida`
    /// itself never sets it, so anything here comes from an L2 layer that
    /// took responsibility for the message.
    pub fn with_achieved(mut self, achieved: Acknowledgement) -> TransferMeta {
        self.achieved = Some(achieved);
        self
    }
}

/// Metadata of an inbound transfer.
#[derive(Clone, Debug)]
pub struct IncomingMeta {
    /// Endpoint path. Present on an initiating stream, absent on a reply.
    pub endpoint: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// Parsed trace context.
    ///
    /// A malformed `traceparent` yields `None`: W3C Trace Context requires an
    /// unparsable value to be ignored, and it is within its wire cap, so it is
    /// not a protocol violation.
    pub trace: Option<TraceContext>,
    /// Opaque `tracestate`, forwarded unmodified.
    pub tracestate: Option<String>,
    /// Pub/Sub topic, when the transfer came from a publisher fan-out.
    pub topic: Option<String>,
    /// The public-key fingerprint the sending peer proved in the TLS
    /// handshake; `None` when it dialled anonymously.
    ///
    /// This is the identity to authorize on. It comes from the handshake, not
    /// from anything the peer wrote into a header, so it cannot be claimed —
    /// only proved (master doc §47).
    pub peer: Option<PeerIdentity>,
    /// The sender's per-producer sequence number, when it numbered this
    /// transfer (DATA key `6`).
    pub sequence: Option<u64>,
    /// What went missing before this transfer, when the connection
    /// negotiated `PerProducer(detect)` and a number was skipped.
    ///
    /// Detect mode reports the gap and delivers the message that arrived;
    /// nothing is held back and nothing is refetched
    /// ([decision 0001](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0001-sequence-field.md)
    /// §7.5). `None` means either that ordering is off or that nothing is
    /// missing.
    pub gap: Option<Gap>,
    /// The completion level the sender achieved for the message this transfer
    /// answers (DATA key `8`), when it claimed one.
    ///
    /// On the reply half of an exchange with an L2 broker this is the
    /// publisher confirm: `Some(Acknowledgement::Accepted)` means the broker
    /// has taken responsibility for the message in memory. `None` is the v0
    /// case and claims nothing beyond the transport receipt.
    pub achieved: Option<Acknowledgement>,
}

impl IncomingMeta {
    pub(crate) fn from_header(header: &DataHeader, peer: Option<PeerIdentity>) -> IncomingMeta {
        IncomingMeta {
            endpoint: header.endpoint.clone(),
            content_len: header.content_len,
            content_type: header.content_type.clone(),
            trace: header
                .traceparent
                .as_deref()
                .and_then(|v| TraceContext::parse_traceparent(v).ok()),
            tracestate: header.tracestate.clone(),
            topic: header.topic.clone(),
            peer,
            sequence: header.sequence,
            gap: None,
            achieved: header.achieved,
        }
    }

    /// Attaches what the connection's gap detector observed.
    pub(crate) fn with_gap(mut self, gap: Option<Gap>) -> IncomingMeta {
        self.gap = gap;
        self
    }
}

/// Generates a fresh root trace context.
pub(crate) fn new_trace_context() -> TraceContext {
    use rand::Rng;
    let mut rng = rand::rng();
    loop {
        let trace_id: [u8; 16] = rng.random();
        let span_id: [u8; 8] = rng.random();
        // All-zero identifiers are forbidden; retrying is free and keeps the
        // invariant in one place.
        if let Some(ctx) = TraceContext::new(trace_id, span_id, 0x01) {
            return ctx;
        }
    }
}

/// Builds the DATA header for an outgoing transfer.
///
/// `endpoint` is `Some` on an initiating stream and `None` on the reply half of
/// an exchange, which is the only distinction the header still makes: the
/// stream itself carries the role and the correlation.
pub(crate) fn data_header(
    endpoint: Option<&str>,
    meta: &TransferMeta,
    tracestate: Option<String>,
) -> (DataHeader, TraceContext) {
    let trace = meta.trace.unwrap_or_else(new_trace_context);
    let header = DataHeader {
        endpoint: endpoint.map(str::to_owned),
        content_len: meta.content_len,
        content_type: meta.content_type.clone(),
        traceparent: Some(trace.to_traceparent()),
        tracestate,
        topic: meta.topic.clone(),
        // Keys 6 and 7 are specified ahead of code: the codec carries them,
        // the v0 runtime writes neither (`docs/PROTOCOL.md` §6.2).
        sequence: None,
        producer: None,
        achieved: meta.achieved,
    };
    (header, trace)
}

/// An outgoing transfer: one QUIC send stream, owned outright.
///
/// Also implements [`AsyncWrite`], so a transfer can be a `tokio::io::copy`
/// destination. [`OutgoingTransfer::finish`] marks the FIN and hands back the
/// [`Delivery`] receipt.
pub struct OutgoingTransfer {
    stream: SendHalf,
    trace: TraceContext,
    settled: bool,
    /// The connection this stream belongs to: where an unawaited receipt is
    /// parked so a drain can wait on it
    /// (`docs/decisions/0009-drain.md` §4.2).
    conn: ConnHandle,
}

impl OutgoingTransfer {
    pub(crate) fn new(stream: SendHalf, trace: TraceContext, conn: ConnHandle) -> OutgoingTransfer {
        OutgoingTransfer {
            stream,
            trace,
            settled: false,
            conn,
        }
    }

    /// The trace context propagated with this transfer.
    pub fn trace(&self) -> TraceContext {
        self.trace
    }

    /// Writes the whole buffer.
    ///
    /// A peer that refuses the transfer mid-write surfaces here as
    /// [`Error::Rejected`], [`Error::UnknownEndpoint`], [`Error::Unsupported`]
    /// or [`Error::Canceled`].
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.stream.write_all(buf).await
    }

    /// Marks the end of the payload and returns the delivery receipt.
    ///
    /// Returns immediately: the FIN is queued and the peer's transport
    /// acknowledgement is awaited through [`Delivery::delivered`], or ignored
    /// entirely by dropping the receipt.
    ///
    /// Fails only if the stream is already closed — the peer reset it, or the
    /// connection went away — in which case no FIN was ever sent.
    pub fn finish(mut self) -> Result<Delivery, Error> {
        self.settled = true;
        self.stream.finish()?;
        // `stopped()` yields a `'static` future, so the receipt outlives the
        // handle it came from.
        Ok(Delivery {
            stopped: Some(self.stream.stopped()),
            conn: Arc::clone(&self.conn),
        })
    }

    /// Abandons the transfer, resetting the stream with `CANCELED`.
    pub fn cancel(mut self) {
        self.settled = true;
        self.stream.reset(codes::CANCELED);
    }
}

impl Drop for OutgoingTransfer {
    fn drop(&mut self) {
        if !self.settled {
            // Dropping without `finish` is an abandoned transfer: reset the
            // stream so the peer discards the partial payload instead of
            // waiting for a FIN that will never come.
            self.stream.reset(codes::CANCELED);
        }
    }
}

impl AsyncWrite for OutgoingTransfer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.stream), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.stream), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.stream), cx)
    }
}

/// Beside the Tokio traits, because a caller's executor need not be Tokio:
/// the two families differ only in shape, not in behaviour.
impl futures_io::AsyncWrite for OutgoingTransfer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        <Self as AsyncWrite>::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <Self as AsyncWrite>::poll_flush(self, cx)
    }

    /// `futures-io`'s close is Tokio's shutdown: both end the write half.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <Self as AsyncWrite>::poll_shutdown(self, cx)
    }
}

impl std::fmt::Debug for OutgoingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingTransfer")
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

/// The transport receipt for a finished transfer.
///
/// This is QUIC's own fin-acknowledgement, not an application acknowledgement:
/// [`Delivery::delivered`] resolving means *the peer's transport holds every
/// byte*, and says nothing about the peer's application having read, stored or
/// processed them. Guarantees of that shape belong to a broker hop and are
/// deliberately absent from the v0 core (`docs/GUARANTEES.md`).
///
/// Dropping a `Delivery` observes no outcome and waits for nothing, which is
/// the fire-and-forget path. It is not quite free: the receipt is handed to
/// the connection's parked set — one uncontended lock and a push — so that
/// [`crate::Runtime::drain`] has something to wait on
/// (`docs/decisions/0009-drain.md` §4.2). A receipt the caller *does* await
/// is never parked; whoever holds it is doing the waiting.
pub struct Delivery {
    stopped: Option<Receipt>,
    conn: ConnHandle,
}

impl Delivery {
    /// Waits for the peer's transport to acknowledge the whole payload.
    ///
    /// `Ok(())` means every byte and the FIN were acknowledged. A refusal
    /// (`STOP_SENDING`) surfaces as the matching error; a connection lost after
    /// the FIN yields [`Error::Indeterminate`], because the payload may or may
    /// not have arrived (`docs/FAILURE_MODEL.md`).
    pub async fn delivered(mut self) -> Result<(), Error> {
        let stopped = self.stopped.take().expect("receipt taken only here");
        match stopped.await {
            Ok(None) => Ok(()),
            Ok(Some(code)) => Err(codes::stop_reason(code).into()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(receipt) = self.stopped.take()
            && self.conn.parked.park(receipt)
        {
            // The connection's parked set was full of receipts that had not
            // settled, so the oldest was dropped unobserved: the next drain
            // reports it as outstanding rather than assuming it landed.
            self.conn.shared.drain.evict();
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery").finish_non_exhaustive()
    }
}

/// An inbound transfer: one receive stream plus the metadata that described
/// it.
///
/// Implements [`AsyncRead`]. Reaching EOF is just EOF: the v0 core emits no
/// acknowledgement, because a brokerless one would only restate what QUIC's
/// own transport receipt already says.
pub struct IncomingTransfer {
    stream: RecvHalf,
    meta: Arc<IncomingMeta>,
    /// Set once the payload ended, was reset, or was refused: `Drop` then has
    /// nothing left to stop.
    done: bool,
}

impl IncomingTransfer {
    pub(crate) fn new(stream: RecvHalf, meta: Arc<IncomingMeta>) -> IncomingTransfer {
        IncomingTransfer {
            stream,
            meta,
            done: false,
        }
    }

    /// Metadata from the DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// Refuses the payload with `STOP_SENDING(code)` and forgets the transfer.
    pub(crate) fn refuse(mut self, code: u64) {
        self.done = true;
        self.stream.stop(code);
    }

    /// Reads the whole remaining payload, refusing to exceed `max_bytes`.
    ///
    /// Over the cap the rest of the payload is refused with
    /// `STOP_SENDING(REJECTED)` and [`Error::LimitExceeded`] is returned: the
    /// bytes are never buffered first.
    ///
    /// This is a convenience for small payloads; streaming users should use
    /// [`AsyncRead`] instead. Use it on a borrowed body
    /// ([`IncomingRequest::body`]); [`IncomingTransfer::collect`] is the
    /// by-value form.
    pub async fn read_capped(&mut self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match self.stream.read(&mut chunk).await {
                Ok(Some(0)) => continue,
                Ok(Some(n)) => {
                    if out.len() + n > max_bytes {
                        self.done = true;
                        self.stream.stop(codes::REJECTED);
                        return Err(Error::LimitExceeded);
                    }
                    out.extend_from_slice(&chunk[..n]);
                }
                Ok(None) => {
                    self.done = true;
                    return Ok(out);
                }
                Err(e) => {
                    self.done = true;
                    return Err(e);
                }
            }
        }
    }

    /// Consuming form of [`IncomingTransfer::read_capped`].
    pub async fn collect(mut self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        self.read_capped(max_bytes).await
    }
}

impl Drop for IncomingTransfer {
    fn drop(&mut self) {
        if !self.done {
            // The application walked away mid-payload: refuse the rest rather
            // than draining bytes nobody wants.
            self.stream.stop(codes::REJECTED);
        }
    }
}

impl std::fmt::Debug for IncomingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingTransfer")
            .field("meta", &self.meta)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for IncomingTransfer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match AsyncRead::poll_read(Pin::new(&mut self.stream), cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() == before {
                    self.done = true;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self.done = true;
                Poll::Ready(Err(e))
            }
            pending => pending,
        }
    }
}

/// The `futures-io` counterpart. It goes through the Tokio implementation
/// above rather than the stream directly, so the end-of-payload bookkeeping
/// happens exactly once and in one place.
impl futures_io::AsyncRead for IncomingTransfer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        match <Self as AsyncRead>::poll_read(self, cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The reply half of an exchange, before the application claims it.
///
/// Dropping it without a reply is what tells the requester that none is
/// coming: the ERROR frame goes through the connection actor with a
/// non-blocking `notify`, because a destructor may run on a thread with no
/// reactor.
struct ReplyHalf {
    send: Option<SendHalf>,
    conn: ConnHandle,
}

impl Drop for ReplyHalf {
    fn drop(&mut self) {
        if let Some(send) = self.send.take() {
            self.conn.notify(Ctl::ReplyError {
                send,
                code: ErrorCode::NoReply,
            });
        }
    }
}

/// A request accepted by a [`crate::Replier`] or a raw [`crate::Acceptor`].
///
/// One bidirectional QUIC stream: the request payload arrives on the receive
/// half, the reply leaves on the send half. Dropping it without calling
/// [`IncomingRequest::reply`] tells the requester that no reply is coming,
/// instead of leaving it waiting.
pub struct IncomingRequest {
    body: Option<IncomingTransfer>,
    meta: Arc<IncomingMeta>,
    reply: ReplyHalf,
}

impl IncomingRequest {
    pub(crate) fn new(body: IncomingTransfer, send: SendHalf, conn: ConnHandle) -> IncomingRequest {
        IncomingRequest {
            meta: Arc::clone(&body.meta),
            body: Some(body),
            reply: ReplyHalf {
                send: Some(send),
                conn,
            },
        }
    }

    /// Metadata from the request's DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// The request payload, borrowed.
    ///
    /// Panics after [`IncomingRequest::take_body`] has detached it.
    pub fn body(&mut self) -> &mut IncomingTransfer {
        self.body
            .as_mut()
            .expect("the request body was detached by take_body")
    }

    /// Detaches the request payload so it outlives the request handle.
    ///
    /// Needed exactly when the handler streams in both directions at once:
    /// [`IncomingRequest::reply`] consumes the request, and a body left inside
    /// it is refused with `REJECTED` at that point. Detaching first keeps both
    /// halves of the exchange live simultaneously, which is what makes
    /// simultaneous request and reply streaming possible (master doc §10).
    ///
    /// Panics if called twice.
    pub fn take_body(&mut self) -> IncomingTransfer {
        self.body
            .take()
            .expect("the request body is detached at most once")
    }

    /// Resolves when the requester stops wanting a reply.
    ///
    /// This is the reply half's `STOP_SENDING`, which a requester emits by
    /// dropping its [`ReplyStream`]. The future is independent of the stream
    /// handle, so it can sit in a `select!` beside the reply writes — but it
    /// must be taken *before* [`IncomingRequest::reply`] consumes the request.
    pub fn canceled(&self) -> impl Future<Output = ()> + Send + use<> {
        let stopped = self.reply.send.as_ref().map(|s| s.stopped());
        async move {
            match stopped {
                Some(fut) => {
                    let _ = fut.await;
                }
                None => std::future::pending().await,
            }
        }
    }

    /// Opens the reply half.
    ///
    /// Consuming: an exchange has exactly one reply, so a second one is not
    /// representable. A request body still attached here is dropped, which
    /// refuses whatever is *left* of it with `REJECTED`; a body already read
    /// to EOF costs nothing. Call [`IncomingRequest::take_body`] first if the
    /// handler still needs to read while it replies.
    ///
    /// The reply inherits the request's trace context unless `meta` overrides
    /// it.
    pub async fn reply(mut self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        // `IncomingTransfer::drop` stops the half only when the payload has
        // not already ended, so a completed request is left alone.
        drop(self.body.take());
        let mut send = self
            .reply
            .send
            .take()
            .expect("the send half is taken exactly once, by this method");

        let meta = match (meta.trace, self.meta.trace) {
            (None, Some(inherited)) => meta.with_trace(inherited),
            _ => meta,
        };
        // No endpoint: the stream is the correlation, so the reply half
        // addresses nothing.
        let (header, trace) = data_header(None, &meta, self.meta.tracestate.clone());
        write_data_preamble(&mut send, &header).await?;
        Ok(OutgoingTransfer::new(
            send,
            trace,
            Arc::clone(&self.reply.conn),
        ))
    }

    /// Refuses the exchange: a typed ERROR instead of a reply.
    ///
    /// This is the one place where an **application** decides an outcome the
    /// requester will see, and it exists because Req/Rep is the only pattern
    /// where a refusal has somewhere to go: the reply half
    /// (`docs/decisions/0005-refusal-race.md` §4.3). The requester's
    /// `request()` fails with the matching [`Error`] — `Rejected` for
    /// [`ErrorCode::Rejected`], `NoReply` for [`ErrorCode::NoReply`] — rather
    /// than waiting for a reply that is not coming.
    ///
    /// The request half is stopped at the same time, so a peer still writing a
    /// payload stops rather than filling a window nobody will read.
    ///
    /// Use [`ErrorCode::NoReply`] where the request was taken and no reply
    /// will exist — an adapter whose far side dropped it silently, for
    /// instance — and [`ErrorCode::Rejected`] where this side declined it.
    pub async fn refuse(self, code: ErrorCode) {
        self.refuse_coded(code, codes::REJECTED).await;
    }

    /// Refuses with an explicit `STOP_SENDING` code, for the runtime's own
    /// routing refusals, which have their own codes (`docs/PROTOCOL.md` §9.3).
    pub(crate) async fn refuse_coded(mut self, code: ErrorCode, stop: u64) {
        if let Some(body) = self.body.take() {
            body.refuse(stop);
        }
        let Some(mut send) = self.reply.send.take() else {
            return;
        };
        if let Err(e) = write_error_frame(&mut send, code).await {
            tracing::debug!(error = %e, "failed to refuse an exchange");
        }
    }
}

impl std::fmt::Debug for IncomingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingRequest")
            .field("meta", self.meta())
            .finish_non_exhaustive()
    }
}

/// Writes the preamble and DATA header of an outgoing transfer.
pub(crate) async fn write_data_preamble(
    stream: &mut SendHalf,
    header: &DataHeader,
) -> Result<(), Error> {
    let encoded = header.encode();
    let mut buf = Vec::with_capacity(weida_protocol::MAX_PREAMBLE_LEN + encoded.len());
    encode_preamble(FrameKind::Data, encoded.len() as u64, &mut buf);
    buf.extend_from_slice(&encoded);
    stream.write_all(&buf).await
}

/// The reply half of an exchange the requester is waiting on.
///
/// Dropping it before [`ReplyStream::recv`] stops the half with `CANCELED`, so
/// a responder streaming a long reply learns that nobody is listening. That
/// stop replaces the CANCEL frame of earlier drafts: the stream carries the
/// correlation, so cancellation needs no identifier and no control frame.
pub struct ReplyStream {
    recv: Option<RecvHalf>,
    conn: ConnHandle,
}

impl ReplyStream {
    pub(crate) fn new(recv: RecvHalf, conn: ConnHandle) -> ReplyStream {
        ReplyStream {
            recv: Some(recv),
            conn,
        }
    }

    /// Waits for the reply header.
    ///
    /// A DATA header yields the reply payload; an ERROR header yields the
    /// corresponding [`Error`], which is how `UNKNOWN_ENDPOINT`, `UNSUPPORTED`
    /// and `NO_REPLY` reach the requester.
    ///
    /// A connection lost while waiting is [`Error::Indeterminate`], never
    /// `ConnectionLost`: the replier may already have acted on the request and
    /// produced an answer we never saw, so claiming a definite failure here
    /// would be a lie (master doc §22).
    pub async fn recv(mut self) -> Result<IncomingTransfer, Error> {
        let mut recv = self.recv.take().expect("the receiver is taken once");
        let (preamble, header) = read_frame(&mut recv, self.conn.limits.max_header_bytes)
            .await
            .map_err(indeterminate_on_loss)?;
        match preamble.kind {
            FrameKind::Data => {
                let header = DataHeader::decode(&header)?;
                let meta = Arc::new(IncomingMeta::from_header(&header, self.conn.peer.clone()));
                Ok(IncomingTransfer::new(recv, meta))
            }
            FrameKind::Error => {
                let header = ErrorHeader::decode(&header)?;
                Err(header.error_code().map_or_else(
                    || Error::Transport(format!("peer reported error code {}", header.code)),
                    Error::from,
                ))
            }
            other => Err(Error::Protocol(format!(
                "{other} is not legal on the reply half of an exchange"
            ))),
        }
    }
}

/// Re-labels a lost connection while awaiting a reply.
fn indeterminate_on_loss(e: Error) -> Error {
    match e {
        Error::ConnectionLost(_) => Error::Indeterminate,
        other => other,
    }
}

impl Drop for ReplyStream {
    fn drop(&mut self) {
        if let Some(mut recv) = self.recv.take() {
            recv.stop(codes::CANCELED);
        }
    }
}

impl std::fmt::Debug for ReplyStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyStream")
            .field("awaiting", &self.recv.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_meta_builders_compose() {
        let trace = new_trace_context();
        let meta = TransferMeta::default()
            .with_content_type("text/plain")
            .with_content_len(7)
            .with_trace(trace);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.content_len, Some(7));
        assert_eq!(meta.trace, Some(trace));
    }

    #[test]
    fn generated_trace_contexts_are_valid_and_distinct() {
        let a = new_trace_context();
        let b = new_trace_context();
        assert_ne!(a.trace_id, b.trace_id);
        assert!(a.is_sampled());
        assert_eq!(
            TraceContext::parse_traceparent(&a.to_traceparent()).unwrap(),
            a
        );
    }

    #[test]
    fn initiating_headers_carry_the_endpoint_and_a_trace_context() {
        let meta = TransferMeta::default().with_content_len(3);
        let (header, trace) = data_header(Some("/transform"), &meta, None);
        assert_eq!(header.endpoint.as_deref(), Some("/transform"));
        assert_eq!(header.content_len, Some(3));
        assert_eq!(
            header.traceparent.as_deref(),
            Some(&*trace.to_traceparent())
        );
        // Round-trips through the wire codec unchanged.
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn reply_headers_carry_no_endpoint_but_keep_tracestate() {
        let (header, _) = data_header(None, &TransferMeta::default(), Some("vendor=x".into()));
        assert_eq!(header.endpoint, None);
        assert_eq!(header.tracestate.as_deref(), Some("vendor=x"));
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn incoming_meta_ignores_a_malformed_traceparent() {
        let mut header = DataHeader::addressed("/x");
        header.traceparent = Some("not-a-traceparent".into());
        let meta = IncomingMeta::from_header(&header, None);
        assert!(meta.trace.is_none());

        let good = new_trace_context();
        header.traceparent = Some(good.to_traceparent());
        assert_eq!(IncomingMeta::from_header(&header, None).trace, Some(good));
    }
}
