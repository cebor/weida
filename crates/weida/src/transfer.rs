//! Transfer handles.
//!
//! These own their `quinn` streams outright: payload bytes go straight to the
//! socket without passing through the connection actor, so a transfer costs no
//! task hop and takes no lock (master doc §49). The actor is involved only for
//! registration and for the short control frames.
//!
//! Nothing here materializes a payload. `AsyncRead`/`AsyncWrite` are the
//! primitive API; `collect(max_bytes)` is an opt-in convenience with an
//! explicit cap (master doc §8, §81 rule 3).

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use quinn::VarInt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{oneshot, watch};
use weida_core::state::{RecvAction, RecvEvent, RecvMachine, RecvState};
use weida_core::{AckMode, Error, Outcome, Role, TraceContext, TransferId};
use weida_protocol::{DataHeader, FrameKind, codes, encode_preamble};

use crate::conn::{ConnHandle, Ctl, read_error, write_error};

/// Per-transfer metadata supplied by the application.
#[derive(Clone, Debug, Default)]
pub struct TransferMeta {
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Acknowledgement level requested from the peer.
    pub ack_mode: AckMode,
    /// Trace context to propagate. `None` generates a fresh root context.
    pub trace: Option<TraceContext>,
}

impl TransferMeta {
    /// Requests an `Accepted` acknowledgement.
    pub fn with_ack(mut self, ack_mode: AckMode) -> TransferMeta {
        self.ack_mode = ack_mode;
        self
    }

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
}

/// Metadata of an inbound transfer.
#[derive(Clone, Debug)]
pub struct IncomingMeta {
    /// Endpoint path, for requests.
    pub endpoint: Option<String>,
    /// The sender's transfer id.
    pub transfer_id: TransferId,
    /// The request this reply answers, for replies.
    pub correlation_id: Option<TransferId>,
    /// Acknowledgement level the sender requested.
    pub ack_mode: AckMode,
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
}

impl IncomingMeta {
    pub(crate) fn from_header(header: &DataHeader, ack_mode: AckMode) -> IncomingMeta {
        IncomingMeta {
            endpoint: header.endpoint.clone(),
            transfer_id: header.transfer_id,
            correlation_id: header.correlation_id,
            ack_mode,
            content_len: header.content_len,
            content_type: header.content_type.clone(),
            trace: header
                .traceparent
                .as_deref()
                .and_then(|v| TraceContext::parse_traceparent(v).ok()),
            tracestate: header.tracestate.clone(),
            topic: header.topic.clone(),
        }
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
/// `role` is explicit rather than inferred from `correlation_id`: oneshot and
/// request are both uncorrelated, so the presence of a correlation id no
/// longer determines the role.
pub(crate) fn data_header(
    role: Role,
    endpoint: Option<&str>,
    id: TransferId,
    correlation_id: Option<TransferId>,
    meta: &TransferMeta,
    tracestate: Option<String>,
) -> (DataHeader, TraceContext) {
    let trace = meta.trace.unwrap_or_else(new_trace_context);
    let header = DataHeader {
        endpoint: endpoint.map(str::to_owned),
        transfer_id: id,
        role: role.to_wire(),
        correlation_id,
        ack_mode: meta.ack_mode.to_wire(),
        content_len: meta.content_len,
        content_type: meta.content_type.clone(),
        traceparent: Some(trace.to_traceparent()),
        tracestate,
        topic: None,
    };
    (header, trace)
}

/// An outgoing transfer: a QUIC stream plus the outcome the peer owes us.
///
/// Also implements [`AsyncWrite`], so a transfer can be a `tokio::io::copy`
/// destination. `finish` must still be called to learn the outcome.
pub struct OutgoingTransfer {
    conn: ConnHandle,
    stream: quinn::SendStream,
    id: TransferId,
    trace: TraceContext,
    outcome: Option<oneshot::Receiver<Result<Outcome, Error>>>,
    settled: bool,
}

impl OutgoingTransfer {
    pub(crate) fn new(
        conn: ConnHandle,
        stream: quinn::SendStream,
        id: TransferId,
        trace: TraceContext,
        outcome: oneshot::Receiver<Result<Outcome, Error>>,
    ) -> OutgoingTransfer {
        OutgoingTransfer {
            conn,
            stream,
            id,
            trace,
            outcome: Some(outcome),
            settled: false,
        }
    }

    /// This transfer's id, unique per connection and sender.
    pub fn id(&self) -> TransferId {
        self.id
    }

    /// The trace context propagated with this transfer.
    pub fn trace(&self) -> TraceContext {
        self.trace
    }

    /// Writes the whole buffer.
    ///
    /// A peer that refuses the transfer mid-write surfaces here as
    /// [`Error::Rejected`], [`Error::UnknownEndpoint`] or [`Error::Canceled`].
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        match self.stream.write_all(buf).await {
            Ok(()) => Ok(()),
            Err(quinn::WriteError::Stopped(code)) => {
                let code = code.into_inner();
                self.conn.notify(Ctl::Stopped {
                    id: self.id.get(),
                    code,
                });
                Err(codes::stop_reason(code).into())
            }
            Err(e) => Err(write_error(e)),
        }
    }

    /// Finishes the transfer and waits for its outcome.
    ///
    /// `Ok` carries [`Outcome::SentBestEffort`] or [`Outcome::Acked`]. `Err`
    /// distinguishes [`Error::ConnectionLost`] (definitely not delivered) from
    /// [`Error::Indeterminate`] (unknown), per `docs/FAILURE_MODEL.md`.
    pub async fn finish(mut self) -> Result<Outcome, Error> {
        self.settled = true;
        // `finish` only marks the FIN, so the peer's answer can reach the actor
        // before this notification does. The send machine holds it.
        let closed = self.stream.finish().is_err();
        self.conn.notify(Ctl::Fin { id: self.id.get() });
        let outcome = self.outcome.take().expect("outcome taken once");
        match outcome.await {
            Ok(result) => result,
            Err(_) if closed => Err(Error::ConnectionLost),
            Err(_) => Err(Error::ConnectionLost),
        }
    }

    /// Abandons the transfer, resetting the stream with `CANCELED`.
    pub fn cancel(mut self) {
        self.settled = true;
        let _ = self.stream.reset(canceled());
        self.conn.notify(Ctl::Cancel { id: self.id.get() });
    }
}

impl Drop for OutgoingTransfer {
    fn drop(&mut self) {
        if !self.settled {
            // Dropping without `finish` is an abandoned transfer: reset the
            // stream so the peer discards the partial payload instead of
            // waiting for a FIN that will never come.
            let _ = self.stream.reset(canceled());
            self.conn.notify(Ctl::Cancel { id: self.id.get() });
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

impl std::fmt::Debug for OutgoingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingTransfer")
            .field("id", &self.id)
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

fn canceled() -> VarInt {
    VarInt::from_u32(codes::CANCELED as u32)
}

/// An inbound transfer: a QUIC stream plus the metadata that described it.
///
/// Implements [`AsyncRead`]. Reaching EOF is what triggers an ACK when the
/// sender asked for one: the acknowledgement means "the application consumed
/// the payload", not "the bytes reached the kernel".
pub struct IncomingTransfer {
    conn: ConnHandle,
    stream: quinn::RecvStream,
    meta: IncomingMeta,
    machine: RecvMachine,
}

impl IncomingTransfer {
    pub(crate) fn new(
        conn: ConnHandle,
        stream: quinn::RecvStream,
        meta: IncomingMeta,
    ) -> IncomingTransfer {
        IncomingTransfer {
            machine: RecvMachine::new(meta.ack_mode),
            conn,
            stream,
            meta,
        }
    }

    /// Metadata from the DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// Refuses the payload with `STOP_SENDING(code)` and forgets the transfer.
    pub(crate) fn refuse(mut self, code: u64) {
        self.machine.on(RecvEvent::AppAbandonedBody);
        let _ = self
            .stream
            .stop(VarInt::from_u64(code).expect("application codes are small"));
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
                        self.machine.on(RecvEvent::AppAbandonedBody);
                        let _ = self.stream.stop(rejected());
                        return Err(Error::LimitExceeded);
                    }
                    out.extend_from_slice(&chunk[..n]);
                }
                Ok(None) => {
                    self.on_eof();
                    return Ok(out);
                }
                Err(e) => {
                    self.machine.on(RecvEvent::PeerReset);
                    return Err(read_error(e));
                }
            }
        }
    }

    /// Consuming form of [`IncomingTransfer::read_capped`].
    pub async fn collect(mut self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        self.read_capped(max_bytes).await
    }

    /// Applies the end-of-payload transition, emitting an ACK if one is owed.
    fn on_eof(&mut self) {
        if let RecvAction::SendAck(_) = self.machine.on(RecvEvent::PayloadEnd) {
            self.conn.notify(Ctl::SendAck {
                re: self.meta.transfer_id,
            });
        }
    }
}

impl Drop for IncomingTransfer {
    fn drop(&mut self) {
        if self.machine.state() == RecvState::Reading {
            // The application walked away mid-payload: refuse the rest rather
            // than draining bytes nobody wants.
            self.machine.on(RecvEvent::AppAbandonedBody);
            let _ = self.stream.stop(rejected());
        }
    }
}

fn rejected() -> VarInt {
    VarInt::from_u32(codes::REJECTED as u32)
}

impl std::fmt::Debug for IncomingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingTransfer")
            .field("meta", &self.meta)
            .field("state", &self.machine.state())
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
                    self.on_eof();
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// A request accepted by a [`crate::Replier`].
///
/// Dropping it without calling [`IncomingRequest::reply`] tells the requester
/// that no reply is coming, instead of leaving it waiting.
pub struct IncomingRequest {
    body: IncomingTransfer,
    cancel: watch::Receiver<bool>,
    replied: AtomicBool,
}

impl IncomingRequest {
    pub(crate) fn new(body: IncomingTransfer, cancel: watch::Receiver<bool>) -> IncomingRequest {
        IncomingRequest {
            body,
            cancel,
            replied: AtomicBool::new(false),
        }
    }

    /// Metadata from the request's DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        self.body.meta()
    }

    /// The request payload.
    pub fn body(&mut self) -> &mut IncomingTransfer {
        &mut self.body
    }

    /// Watches for a CANCEL frame from the requester.
    ///
    /// The value flips to `true` once the requester stops wanting replies. A
    /// handler streaming a long reply should poll this and stop early.
    pub fn canceled(&self) -> watch::Receiver<bool> {
        self.cancel.clone()
    }

    /// True if the requester has canceled.
    pub fn is_canceled(&self) -> bool {
        *self.cancel.borrow()
    }

    /// Opens the correlated reply stream.
    ///
    /// This may be called before the request body has been fully read: the
    /// reply is an independent stream, which is what makes simultaneous
    /// request and reply streaming possible (master doc §10).
    ///
    /// The reply inherits the request's trace context unless `meta` overrides
    /// it.
    pub async fn reply(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        let conn = self.body.conn.clone();
        let correlation_id = self.body.meta.transfer_id;
        let meta = match (meta.trace, self.body.meta.trace) {
            (None, Some(inherited)) => meta.with_trace(inherited),
            _ => meta,
        };

        let reserved = conn.register(meta.ack_mode, false).await?;
        let (header, trace) = data_header(
            Role::Reply,
            None,
            reserved.id,
            Some(correlation_id),
            &meta,
            self.body.meta.tracestate.clone(),
        );

        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;
        self.replied.store(true, Ordering::Relaxed);
        Ok(OutgoingTransfer::new(
            conn,
            stream,
            reserved.id,
            trace,
            reserved.outcome,
        ))
    }
}

impl std::fmt::Debug for IncomingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingRequest")
            .field("meta", self.meta())
            .field("canceled", &self.is_canceled())
            .finish_non_exhaustive()
    }
}

impl Drop for IncomingRequest {
    fn drop(&mut self) {
        let id = self.body.meta.transfer_id;
        self.body.conn.notify(Ctl::UntrackInbound { id: id.get() });
        if !self.replied.load(Ordering::Relaxed) {
            // Without this the requester would wait for a reply forever.
            self.body.conn.notify(Ctl::SendError {
                re: id,
                code: weida_core::ErrorCode::NoReply,
            });
        }
    }
}

/// Writes the preamble and DATA header of an outgoing transfer.
pub(crate) async fn write_data_preamble(
    stream: &mut quinn::SendStream,
    header: &DataHeader,
) -> Result<(), Error> {
    let encoded = header.encode();
    let mut buf = Vec::with_capacity(weida_protocol::MAX_PREAMBLE_LEN + encoded.len());
    encode_preamble(FrameKind::Data, encoded.len() as u64, &mut buf);
    buf.extend_from_slice(&encoded);
    stream.write_all(&buf).await.map_err(write_error)
}

/// A reply the requester is waiting for.
///
/// Dropping it before [`PendingReply::recv`] sends a CANCEL frame, so a
/// responder streaming a long reply learns that nobody is listening.
pub struct PendingReply {
    conn: ConnHandle,
    id: TransferId,
    rx: Option<oneshot::Receiver<Result<IncomingTransfer, Error>>>,
}

impl PendingReply {
    pub(crate) fn new(
        conn: ConnHandle,
        id: TransferId,
        rx: oneshot::Receiver<Result<IncomingTransfer, Error>>,
    ) -> PendingReply {
        PendingReply {
            conn,
            id,
            rx: Some(rx),
        }
    }

    /// The request id this reply is correlated to.
    pub fn correlation_id(&self) -> TransferId {
        self.id
    }

    /// Waits for the reply stream.
    pub async fn recv(mut self) -> Result<IncomingTransfer, Error> {
        let rx = self.rx.take().expect("receiver taken once");
        match rx.await {
            Ok(result) => result,
            // The actor dropped the sender without a verdict: the connection
            // is gone and the request had already been finished.
            Err(_) => Err(Error::Indeterminate),
        }
    }
}

impl std::fmt::Debug for PendingReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingReply")
            .field("correlation_id", &self.id)
            .field("awaiting", &self.rx.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for PendingReply {
    fn drop(&mut self) {
        if self.rx.is_some() {
            self.conn.notify(Ctl::SendCancel { id: self.id });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_meta_builders_compose() {
        let trace = new_trace_context();
        let meta = TransferMeta::default()
            .with_ack(AckMode::Accepted)
            .with_content_type("text/plain")
            .with_content_len(7)
            .with_trace(trace);
        assert_eq!(meta.ack_mode, AckMode::Accepted);
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
    fn request_headers_carry_the_endpoint_and_a_trace_context() {
        let meta = TransferMeta::default().with_ack(AckMode::Accepted);
        let (header, trace) = data_header(
            Role::Request,
            Some("/transform"),
            TransferId::FIRST,
            None,
            &meta,
            None,
        );
        assert_eq!(header.endpoint.as_deref(), Some("/transform"));
        assert_eq!(header.role, weida_core::policy::ROLE_REQUEST);
        assert_eq!(header.correlation_id, None);
        assert_eq!(header.ack_mode, AckMode::Accepted.to_wire());
        assert_eq!(
            header.traceparent.as_deref(),
            Some(&*trace.to_traceparent())
        );
        // Round-trips through the wire codec unchanged.
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn reply_headers_carry_the_correlation_id_and_tracestate() {
        let correlation = TransferId::new(9).unwrap();
        let (header, _) = data_header(
            Role::Reply,
            None,
            TransferId::FIRST,
            Some(correlation),
            &TransferMeta::default(),
            Some("vendor=x".into()),
        );
        assert_eq!(header.endpoint, None);
        assert_eq!(header.role, weida_core::policy::ROLE_REPLY);
        assert_eq!(header.correlation_id, Some(correlation));
        assert_eq!(header.tracestate.as_deref(), Some("vendor=x"));
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn incoming_meta_ignores_a_malformed_traceparent() {
        let mut header = DataHeader::request("/x", TransferId::FIRST, AckMode::None);
        header.traceparent = Some("not-a-traceparent".into());
        let meta = IncomingMeta::from_header(&header, AckMode::None);
        assert!(meta.trace.is_none());

        let good = new_trace_context();
        header.traceparent = Some(good.to_traceparent());
        assert_eq!(
            IncomingMeta::from_header(&header, AckMode::None).trace,
            Some(good)
        );
    }
}
