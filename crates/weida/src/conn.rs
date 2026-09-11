//! Per-connection stream dispatch and the small control actor.
//!
//! There are no correlation tables left. Req/Rep rides one bidirectional QUIC
//! stream, so the stream *is* the correlation and there is nothing per
//! connection to index. What remains of the actor is a serialization point for
//! the two frames a destructor may need to emit without a reactor of its own.
//!
//! **Payload bytes never traverse the actor.** API handles own their `quinn`
//! streams directly, so writing and reading a transfer costs no task hop and
//! takes no lock (master doc §49).

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use weida_core::{Error, ErrorCode, Fingerprint, Limits, LossCause};
use weida_protocol::header::GuaranteeSet;
use weida_protocol::{
    Agreed, DataHeader, ErrorHeader, FrameKind, Hello, MAX_PREAMBLE_LEN, Preamble, PreambleError,
    SubscriptionHeader, codes, encode_frame, negotiate, parse_preamble,
};

use crate::dedup::DedupWindow;
use crate::listener::{Namespace, Route};
use crate::ordering::{GapDetector, Reassembler, Sequencer};
use crate::pubsub::SubRegistry;
use crate::runtime::{Exec, Shared};
use crate::stream::Incoming;
use crate::transfer::{IncomingMeta, IncomingRequest, IncomingTransfer};
use crate::transport::{Link, RecvHalf, SendHalf};

/// Depth of the control channel between API handles and the actor.
const CTL_QUEUE: usize = 1024;

/// Messages the connection actor accepts.
///
/// Both exist for the same reason: a `Drop` may run on a thread with no Tokio
/// reactor, and a destructor must never be the thing that needs one.
pub(crate) enum Ctl {
    /// Emit an UNSUBSCRIBE frame.
    SendUnsubscribe { path: Arc<str>, filter: String },
    /// Report a failure on the reply half of an exchange and close it.
    ReplyError { send: SendHalf, code: ErrorCode },
}

/// Shared, cheaply clonable handle to one connection.
pub(crate) struct ConnCtx {
    pub conn: Link,
    pub ctl: mpsc::Sender<Ctl>,
    pub limits: Limits,
    /// Endpoint routing table. Client connections get a fresh empty one rather
    /// than sharing the listener's: a `Sub` registers its path on the
    /// connection it dialled, so both directions need a namespace.
    pub namespace: Arc<Namespace>,
    /// Publisher-side subscriptions. `None` on a client connection: a peer
    /// that subscribes to us when we serve no publishers is useless, not
    /// hostile, so the frame is ignored rather than treated as a violation.
    pub subs: Option<Arc<SubRegistry>>,
    /// The identity the peer proved in the handshake, `None` for an anonymous
    /// client. Fixed for the life of the connection.
    pub peer: Option<Fingerprint>,
    /// The runtime this connection's tasks and timers run on.
    pub exec: Exec,
    /// The guarantee set this side offers and requires. Negotiation refuses a
    /// peer that cannot match it, so it is also the effective set.
    pub guarantees: GuaranteeSet,
    /// Numbers outgoing transfers per scope; inert unless ordering is on.
    pub sequencer: Sequencer,
    /// Reports gaps in inbound sequences; inert unless detect mode is on.
    pub gaps: GapDetector,
    /// Holds out-of-order arrivals back; inert unless reassemble mode is on.
    pub reorder: Reassembler<Held>,
    /// Suppresses repeated identities; inert unless deduplication is on.
    pub dedup: DedupWindow,
    /// Receipts of finished transfers on this connection that nobody is
    /// waiting on, for [`crate::Runtime::drain`].
    pub parked: crate::drain::ConnDrain,
    /// Counters and flags shared with every other connection of this
    /// runtime: the duplicate count and the drain's admission flag.
    pub shared: Arc<Shared>,
    agreed: watch::Receiver<Option<Agreed>>,
}

pub(crate) type ConnHandle = Arc<ConnCtx>;

impl ConnCtx {
    /// Spawns the actor, both accept loops and the HELLO exchange for `conn`.
    pub(crate) fn spawn(
        conn: Link,
        limits: Limits,
        namespace: Arc<Namespace>,
        subs: Option<Arc<SubRegistry>>,
        exec: Exec,
        guarantees: GuaranteeSet,
        shared: Arc<Shared>,
    ) -> ConnHandle {
        let (ctl_tx, ctl_rx) = mpsc::channel(CTL_QUEUE);
        let (agreed_tx, agreed_rx) = watch::channel(None);
        let agreed_tx = Arc::new(agreed_tx);

        let ctx = Arc::new(ConnCtx {
            peer: conn.peer(),
            conn,
            ctl: ctl_tx,
            limits,
            namespace,
            subs,
            exec: exec.clone(),
            guarantees,
            sequencer: Sequencer::new(guarantees.ordering),
            gaps: GapDetector::new(guarantees.ordering, limits.max_sequence_scopes),
            reorder: Reassembler::new(
                guarantees.ordering,
                limits.max_reorder_hold,
                limits.max_sequence_scopes,
            ),
            dedup: DedupWindow::new(
                guarantees.deduplication,
                guarantees.dedup_window_ms,
                limits.max_dedup_entries,
            ),
            parked: crate::drain::ConnDrain::new(&limits),
            shared,
            agreed: agreed_rx,
        });

        // Once per connection, never per message: a drain collects the
        // parked receipts from here.
        ctx.shared.drain.register(&ctx);

        exec.spawn(driver(Arc::downgrade(&ctx), ctl_rx, exec.clone()));
        exec.spawn(hello_deadline(Arc::clone(&ctx), Arc::clone(&agreed_tx)));
        exec.spawn(accept_uni_loop(Arc::clone(&ctx), agreed_tx));
        exec.spawn(accept_bi_loop(Arc::clone(&ctx)));
        exec.spawn(send_hello(Arc::clone(&ctx), limits, guarantees));
        ctx
    }

    /// Waits until the peer HELLO has been negotiated.
    ///
    /// QUIC streams are unordered relative to each other, so a DATA stream can
    /// be accepted before the peer's HELLO. Parking here is correct behaviour,
    /// not a protocol violation.
    ///
    /// A connection that dies while we wait fails this immediately with the
    /// reason it died. That matters most for a client whose handshake looked
    /// complete but whose identity the server then refused: the refusal is a
    /// connection close that arrives *after* `connect` resolved, and the
    /// HELLO that would end this wait is never coming.
    pub(crate) async fn negotiated(&self) -> Result<Agreed, Error> {
        let mut rx = self.agreed.clone();
        loop {
            if let Some(agreed) = *rx.borrow_and_update() {
                return Ok(agreed);
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        // The sender is gone because the connection is: the
                        // accept loops and the deadline hold it and end with
                        // it. Report why, not merely that.
                        return Err(self
                            .conn
                            .close_reason()
                            .unwrap_or(Error::ConnectionLost(LossCause::LocallyClosed)));
                    }
                }
                reason = self.conn.closed() => return Err(reason),
            }
        }
    }

    /// Sends a control message without waiting.
    ///
    /// Used from `Drop`, where blocking is not an option. A full queue means
    /// 1024 outstanding control messages on one connection; dropping the
    /// notification there is preferable to blocking a destructor.
    pub(crate) fn notify(&self, ctl: Ctl) {
        if self.ctl.try_send(ctl).is_err() {
            tracing::debug!("connection control queue full or closed; notification dropped");
        }
    }

    /// Opens a unidirectional stream.
    pub(crate) async fn open_uni(&self) -> Result<SendHalf, Error> {
        self.conn.open_uni().await
    }

    /// Opens a bidirectional stream.
    ///
    /// The peer learns of the stream only once the first bytes are written, and
    /// the DATA header is always written first, so an exchange never announces
    /// itself before it says what it is.
    pub(crate) async fn open_bi(&self) -> Result<(SendHalf, RecvHalf), Error> {
        self.conn.open_bi().await
    }
}

/// Maps a `quinn` connection error onto the outcome vocabulary.
pub(crate) fn conn_error(e: quinn::ConnectionError) -> Error {
    match e {
        quinn::ConnectionError::ApplicationClosed(frame) => match frame.error_code.into_inner() {
            codes::NEGOTIATION_FAILED => {
                Error::Negotiation("peer closed the connection: negotiation failed".into())
            }
            codes::PROTOCOL_VIOLATION => {
                Error::Protocol("peer closed the connection: protocol violation".into())
            }
            codes::LIMIT_EXCEEDED => Error::LimitExceeded,
            // Any other application close code is the peer saying goodbye in
            // its own words: a shutdown, or a code this version does not map.
            _ => Error::ConnectionLost(LossCause::PeerClosed),
        },
        quinn::ConnectionError::LocallyClosed => Error::ConnectionLost(LossCause::LocallyClosed),
        // An idle timeout or a stateless reset is a lost connection: whatever
        // was in flight will never complete, which is the definite outcome
        // `ConnectionLost` promises (`docs/FAILURE_MODEL.md`). The cause is
        // kept because the next action differs — an idle timeout invites a
        // redial, a reset says the peer forgot us, and a deliberate close may
        // mean it does not want one yet.
        quinn::ConnectionError::TimedOut => Error::ConnectionLost(LossCause::IdleTimeout),
        quinn::ConnectionError::Reset => Error::ConnectionLost(LossCause::Reset),
        // Error codes 0x100..0x200 carry a TLS alert: the handshake itself
        // failed, whether we detected it or the peer told us so. That is a
        // TLS outcome, not a transport one.
        quinn::ConnectionError::TransportError(t) if is_tls_alert(t.code) => {
            Error::Tls(t.to_string())
        }
        quinn::ConnectionError::ConnectionClosed(c) if is_tls_alert(c.error_code) => {
            Error::Tls(format!("peer aborted the handshake: {c}"))
        }
        other => Error::Transport(other.to_string()),
    }
}

fn is_tls_alert(code: quinn::TransportErrorCode) -> bool {
    (0x100..0x200).contains(&u64::from(code))
}

/// Maps a stream write failure onto the outcome vocabulary.
pub(crate) fn write_error(e: quinn::WriteError) -> Error {
    match e {
        quinn::WriteError::Stopped(code) => codes::stop_reason(code.into_inner()).into(),
        quinn::WriteError::ConnectionLost(e) => conn_error(e),
        quinn::WriteError::ClosedStream => Error::Transport("stream already closed".into()),
        quinn::WriteError::ZeroRttRejected => {
            Error::Transport("0-RTT data rejected by the peer".into())
        }
    }
}

/// Maps a stream read failure onto the outcome vocabulary.
pub(crate) fn read_error(e: quinn::ReadError) -> Error {
    match e {
        quinn::ReadError::Reset(code) => match code.into_inner() {
            codes::CANCELED => Error::Canceled,
            codes::REJECTED => Error::Rejected,
            codes::UNKNOWN_ENDPOINT => Error::UnknownEndpoint,
            codes::UNSUPPORTED => Error::Unsupported,
            other => Error::Transport(format!("peer reset the stream with code {other}")),
        },
        quinn::ReadError::ConnectionLost(e) => conn_error(e),
        quinn::ReadError::ClosedStream => Error::Transport("stream already closed".into()),
        quinn::ReadError::IllegalOrderedRead => {
            Error::Transport("ordered read after unordered read".into())
        }
        quinn::ReadError::ZeroRttRejected => {
            Error::Transport("0-RTT data rejected by the peer".into())
        }
    }
}

/// The connection actor: serializes the frames destructors ask for.
///
/// Holds the connection weakly. A transport handle is not clonable — a local
/// connection owns its channels — so the actor borrows the one the context
/// owns, and ends when the last handle to that context goes away and the
/// control queue closes with it.
async fn driver(ctx: Weak<ConnCtx>, mut rx: mpsc::Receiver<Ctl>, exec: Exec) {
    while let Some(ctl) = rx.recv().await {
        let Some(ctx) = ctx.upgrade() else { break };
        handle_ctl(ctx, ctl, &exec);
    }
}

fn handle_ctl(ctx: ConnHandle, ctl: Ctl, exec: &Exec) {
    match ctl {
        Ctl::SendUnsubscribe { path, filter } => {
            let header = SubscriptionHeader::new(&*path, filter).encode();
            exec.spawn(async move {
                if let Err(e) = write_control(&ctx.conn, FrameKind::Unsubscribe, &header).await {
                    tracing::debug!(error = %e, "failed to send an UNSUBSCRIBE frame");
                }
            });
        }
        Ctl::ReplyError { mut send, code } => {
            exec.spawn(async move {
                if let Err(e) = write_error_frame(&mut send, code).await {
                    tracing::debug!(error = %e, "failed to report a reply failure");
                }
            });
        }
    }
}

/// Writes an ERROR frame and the FIN on the reply half of an exchange.
///
/// ERROR is legal nowhere else: it is the alternative to a reply, so it needs
/// neither a transfer id nor a stream of its own.
pub(crate) async fn write_error_frame(send: &mut SendHalf, code: ErrorCode) -> Result<(), Error> {
    let frame = encode_frame(FrameKind::Error, &ErrorHeader::new(code).encode());
    send.write_all(&frame).await?;
    send.finish()
}

/// Writes one header-only control frame on its own unidirectional stream.
pub(crate) async fn write_control(
    conn: &Link,
    kind: FrameKind,
    header: &[u8],
) -> Result<(), Error> {
    let mut stream = conn.open_uni().await?;
    stream.write_all(&encode_frame(kind, header)).await?;
    stream.finish()?;
    Ok(())
}

/// The HELLO this side sends: v0 plus the configured guarantee declarations.
///
/// Offered and required are the same set: a peer that offers less fails the
/// handshake, which is what lets the rest of the code treat the local set as
/// the effective one (`docs/PROTOCOL.md` §2.3 step 6).
fn hello_for(limits: Limits, guarantees: GuaranteeSet) -> Hello {
    let declaration = (!guarantees.is_core()).then_some(guarantees);
    Hello {
        guarantees_offered: declaration,
        guarantees_required: declaration,
        ..Hello::v0(
            limits.max_header_bytes,
            u64::from(limits.max_concurrent_uni_streams),
        )
    }
}

/// Sends our HELLO, declaring the configured guarantee set.
async fn send_hello(ctx: ConnHandle, limits: Limits, guarantees: GuaranteeSet) {
    let hello = hello_for(limits, guarantees);
    if let Err(e) = write_control(&ctx.conn, FrameKind::Hello, &hello.encode()).await {
        tracing::debug!(error = %e, "failed to send HELLO");
    }
}

/// Closes the connection if the peer's HELLO never arrives.
async fn hello_deadline(ctx: ConnHandle, agreed_tx: Arc<watch::Sender<Option<Agreed>>>) {
    tokio::select! {
        () = ctx.exec.sleep(Duration::from_millis(ctx.limits.hello_timeout_ms)) => {}
        // A connection that is already gone needs no deadline, and holding
        // one would keep its state alive for the whole timeout.
        _ = ctx.conn.closed() => return,
    }
    if agreed_tx.borrow().is_none() {
        tracing::debug!("peer HELLO did not arrive in time");
        ctx.conn.close(codes::NEGOTIATION_FAILED, "hello timeout");
    }
}

/// Accepts inbound unidirectional streams and spawns one task per stream.
///
/// Concurrency is bounded by the QUIC `max_concurrent_uni_streams` limit, so no
/// extra semaphore is needed: the peer cannot create more parse tasks than the
/// transport lets it open streams.
async fn accept_uni_loop(ctx: ConnHandle, agreed_tx: Arc<watch::Sender<Option<Agreed>>>) {
    loop {
        match ctx.conn.accept_uni().await {
            Ok(stream) => {
                // Admission stopped: the drain refuses new work rather than
                // taking on more of it (`docs/decisions/0009-drain.md` §4.5).
                if ctx.shared.drain.is_draining() {
                    refuse_uni(stream);
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                let agreed_tx = Arc::clone(&agreed_tx);
                ctx.exec.clone().spawn(async move {
                    if let Err(e) = handle_stream(&ctx, &agreed_tx, stream).await {
                        tracing::debug!(error = %e, "inbound stream failed");
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed; uni accept loop ending");
                break;
            }
        }
    }
}

/// Accepts inbound bidirectional streams: one exchange each.
///
/// Bounded the same way, by `max_concurrent_bidi_streams`.
async fn accept_bi_loop(ctx: ConnHandle) {
    loop {
        match ctx.conn.accept_bi().await {
            Ok((mut send, recv)) => {
                if ctx.shared.drain.is_draining() {
                    refuse_uni(recv);
                    send.reset(codes::SHUTDOWN);
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                ctx.exec.clone().spawn(async move {
                    if let Err(e) = handle_bi(&ctx, send, recv).await {
                        tracing::debug!(error = %e, "inbound exchange failed");
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed; bidi accept loop ending");
                break;
            }
        }
    }
}

/// Refuses an inbound stream without reading it: the drain's answer to work
/// that arrived too late.
fn refuse_uni(mut stream: RecvHalf) {
    stream.stop(codes::SHUTDOWN);
}

/// Reads one preamble byte by byte, enforcing the header cap before allocating.
async fn read_preamble(stream: &mut RecvHalf, max_header_bytes: u64) -> Result<Preamble, Error> {
    // The preamble is at most ten bytes, and the header length cap is checked
    // inside `parse_preamble` before anything is allocated.
    let mut scratch = [0u8; MAX_PREAMBLE_LEN];
    let mut have = 0usize;
    loop {
        match parse_preamble(&scratch[..have], max_header_bytes) {
            Ok((preamble, used)) => {
                debug_assert_eq!(used, have, "the preamble is read byte by byte");
                return Ok(preamble);
            }
            Err(PreambleError::Incomplete) => {
                if have == scratch.len() {
                    return Err(Error::Protocol(
                        "preamble exceeds its maximum length".into(),
                    ));
                }
                match stream.read(&mut scratch[have..have + 1]).await? {
                    Some(0) => continue,
                    Some(n) => have += n,
                    None => return Err(Error::Protocol("stream ended inside the preamble".into())),
                }
            }
            Err(e) => return Err(Error::Protocol(e.to_string())),
        }
    }
}

async fn read_header(stream: &mut RecvHalf, preamble: &Preamble) -> Result<Vec<u8>, Error> {
    let mut header = vec![0u8; preamble.header_len as usize];
    stream.read_exact(&mut header).await?;
    Ok(header)
}

/// Reads a whole frame head: preamble plus the header bytes it announces.
pub(crate) async fn read_frame(
    stream: &mut RecvHalf,
    max_header_bytes: u64,
) -> Result<(Preamble, Vec<u8>), Error> {
    let preamble = read_preamble(stream, max_header_bytes).await?;
    let header = read_header(stream, &preamble).await?;
    Ok((preamble, header))
}

/// Reads the preamble and header of one inbound unidirectional stream, then
/// dispatches it.
async fn handle_stream(
    ctx: &ConnHandle,
    agreed_tx: &watch::Sender<Option<Agreed>>,
    mut stream: RecvHalf,
) -> Result<(), Error> {
    let preamble = match read_preamble(&mut stream, ctx.limits.max_header_bytes).await {
        Ok(preamble) => preamble,
        Err(Error::Protocol(reason)) => return violation(ctx, &reason),
        Err(e) => return Err(e),
    };

    // Nothing but HELLO may be interpreted before negotiation completes.
    if preamble.kind != FrameKind::Hello {
        ctx.negotiated().await?;
    }

    let header = read_header(&mut stream, &preamble).await?;

    match preamble.kind {
        FrameKind::Hello => handle_hello(ctx, agreed_tx, &header),
        FrameKind::Data => handle_data(ctx, stream, &header).await,
        // ERROR answers a request, and a request always arrives on a
        // bidirectional stream, so its reply half is the only place an ERROR
        // can legitimately appear.
        FrameKind::Error => violation(
            ctx,
            "ERROR is legal only on the reply half of a bidirectional stream",
        ),
        FrameKind::Subscribe => handle_subscription(ctx, &header, true),
        FrameKind::Unsubscribe => handle_subscription(ctx, &header, false),
    }
}

fn violation(ctx: &ConnHandle, reason: &str) -> Result<(), Error> {
    tracing::debug!(reason, "closing connection: protocol violation");
    ctx.conn.close(codes::PROTOCOL_VIOLATION, reason);
    Err(Error::Protocol(reason.to_owned()))
}

/// Applies a SUBSCRIBE or UNSUBSCRIBE frame.
///
/// Both carry the same header, and both are already parked behind negotiation
/// by `handle_stream`, so a subscription that overtakes the peer's HELLO is
/// delayed rather than refused.
fn handle_subscription(ctx: &ConnHandle, header: &[u8], subscribe: bool) -> Result<(), Error> {
    let header = match SubscriptionHeader::decode(header) {
        Ok(h) => h,
        Err(e) => return violation(ctx, &e.to_string()),
    };
    let Some(subs) = ctx.subs.as_ref() else {
        // A client connection serves no publishers. Nothing to do, and nothing
        // wrong with the peer's frame.
        tracing::debug!(
            endpoint = %header.endpoint,
            "ignoring a subscription frame: this side publishes nothing"
        );
        return Ok(());
    };

    if subscribe {
        if subs
            .subscribe(&header.endpoint, ctx, header.filter)
            .is_err()
        {
            // SUBSCRIBE arrives on a unidirectional stream, so there is no
            // reply half to answer with an ERROR frame; the connection is the
            // only granularity available.
            tracing::debug!(
                max = ctx.limits.max_subscriptions,
                "subscription limit reached; closing the connection"
            );
            ctx.conn.close(
                codes::LIMIT_EXCEEDED,
                "too many subscriptions on one connection",
            );
            return Err(Error::LimitExceeded);
        }
    } else {
        subs.unsubscribe(&header.endpoint, ctx.conn.stable_id(), &header.filter);
    }
    Ok(())
}

fn handle_hello(
    ctx: &ConnHandle,
    agreed_tx: &watch::Sender<Option<Agreed>>,
    header: &[u8],
) -> Result<(), Error> {
    let theirs = match Hello::decode(header) {
        Ok(h) => h,
        Err(e) => return violation(ctx, &e.to_string()),
    };
    let ours = hello_for(ctx.limits, ctx.guarantees);
    match negotiate(&ours, &theirs) {
        Ok(agreed) => {
            tracing::debug!(
                version = agreed.version,
                send_max_header_bytes = agreed.send_max_header_bytes,
                "negotiated"
            );
            let _ = agreed_tx.send(Some(agreed));
            Ok(())
        }
        Err(e) => {
            tracing::debug!(error = %e, "negotiation failed");
            ctx.conn.close(codes::NEGOTIATION_FAILED, &e.to_string());
            Err(e.into())
        }
    }
}

/// Dispatches a one-way transfer arriving on a unidirectional stream.
///
/// A misroute is answered with `STOP_SENDING`, not with a frame: there is no
/// reply half here to carry one, and the stop code says everything an ERROR
/// header would have.
async fn handle_data(ctx: &ConnHandle, stream: RecvHalf, header: &[u8]) -> Result<(), Error> {
    let header = match DataHeader::decode(header) {
        Ok(h) => h,
        Err(e) => return violation(ctx, &e.to_string()),
    };
    let Some(path) = header.endpoint.clone() else {
        return violation(ctx, "DATA on a unidirectional stream must name an endpoint");
    };

    // The scope is the topic for a published copy and the path otherwise,
    // which is the `(producer, endpoint or topic)` scope of decision 0001
    // §7.1. Both the dedup window and the gap detector key on it.
    let scope = header.topic.as_deref().unwrap_or(path.as_str());

    // Deduplication sits between the wire and everything else: a repeat is
    // read to EOF and thrown away, so the sender sees an ordinary receipt and
    // neither the application nor the gap detector ever sees the message
    // twice. A dedup window saves the application, not the bandwidth — the
    // bytes crossed the wire before anything could know they were a repeat.
    if ctx
        .dedup
        .is_duplicate(header.producer, scope, header.sequence)
    {
        ctx.shared.duplicates.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(path, sequence = ?header.sequence, "duplicate suppressed");
        return drain(stream).await;
    }

    let meta = IncomingMeta::from_header(&header, ctx.peer);

    // Reassemble mode: hold this arrival if the numbers before it have not
    // come yet, and dispatch whatever run that completes. Held transfers are
    // unread streams — nothing is materialized — so what waits is quinn's
    // receive buffer for them, which is what makes `max_reorder_hold` a
    // memory bound.
    if ctx.reorder.enabled() {
        let held = Held {
            stream,
            meta,
            path: path.clone(),
        };
        for (held, gap) in ctx.reorder.admit(scope, header.sequence, held) {
            let transfer = IncomingTransfer::new(held.stream, Arc::new(held.meta.with_gap(gap)));
            dispatch(ctx, &held.path, transfer).await;
        }
        return Ok(());
    }

    // Detect mode: report what is missing and deliver what arrived.
    let gap = header.sequence.and_then(|seq| ctx.gaps.observe(scope, seq));
    dispatch(
        ctx,
        &path,
        IncomingTransfer::new(stream, Arc::new(meta.with_gap(gap))),
    )
    .await;
    Ok(())
}

/// One transfer, held with everything needed to deliver it later.
///
/// The payload is **not** here: a held transfer is an unread `RecvStream`,
/// which is what keeps reassembly inside the "core transport does not require
/// payload materialization" invariant (`docs/INVARIANTS.md`).
pub(crate) struct Held {
    stream: RecvHalf,
    meta: IncomingMeta,
    path: String,
}

/// Hands one arrived transfer to whatever is bound at `path`.
async fn dispatch(ctx: &ConnHandle, path: &str, transfer: IncomingTransfer) {
    match ctx.namespace.lookup(path) {
        // Awaiting a queue slot is the backpressure path: it stalls this
        // stream's task, which stalls the peer through QUIC flow control.
        Some(Route::Transfer(queue)) => {
            if let Err(e) = queue.send(transfer).await {
                tracing::debug!(path, "endpoint went away while dispatching");
                e.0.refuse(codes::UNKNOWN_ENDPOINT);
            }
        }
        Some(Route::Raw(queue)) => {
            if let Err(e) = queue.send(Incoming::Stream(transfer)).await {
                tracing::debug!(path, "acceptor went away while dispatching");
                if let Incoming::Stream(t) = e.0 {
                    t.refuse(codes::UNKNOWN_ENDPOINT);
                }
            }
        }
        // The path exists but serves a different shape: a one-way transfer
        // aimed at a replier, or anything aimed at a publisher. Refusing is
        // the honest answer — the alternative is to reinterpret the sender.
        Some(Route::Request(_) | Route::Pub) => {
            tracing::debug!(path, "endpoint does not serve one-way transfers");
            transfer.refuse(codes::UNSUPPORTED);
        }
        None => {
            tracing::debug!(path, "no endpoint registered");
            transfer.refuse(codes::UNKNOWN_ENDPOINT);
        }
    }
}

/// Reads a stream to EOF and discards it.
///
/// Used for a suppressed duplicate: dropping the stream instead would reset
/// it, and the sender would read that as a refusal
/// ([FAILURE_MODEL.md](../../../docs/FAILURE_MODEL.md) §4) — which a
/// successfully deduplicated message is not.
async fn drain(mut stream: RecvHalf) -> Result<(), Error> {
    let mut scratch = [0u8; 8 * 1024];
    while stream.read(&mut scratch).await?.is_some() {}
    Ok(())
}

/// Dispatches one exchange arriving on a bidirectional stream.
///
/// Only DATA may open one. A misroute is answered on the reply half with a
/// real ERROR frame, which is the whole reason the reply half exists.
async fn handle_bi(ctx: &ConnHandle, send: SendHalf, mut recv: RecvHalf) -> Result<(), Error> {
    let preamble = match read_preamble(&mut recv, ctx.limits.max_header_bytes).await {
        Ok(preamble) => preamble,
        Err(Error::Protocol(reason)) => return violation(ctx, &reason),
        Err(e) => return Err(e),
    };
    if preamble.kind != FrameKind::Data {
        return violation(
            ctx,
            &format!("{} may not open a bidirectional stream", preamble.kind),
        );
    }
    ctx.negotiated().await?;

    let header = read_header(&mut recv, &preamble).await?;
    let header = match DataHeader::decode(&header) {
        Ok(h) => h,
        Err(e) => return violation(ctx, &e.to_string()),
    };
    let Some(path) = header.endpoint.clone() else {
        return violation(
            ctx,
            "the initiating half of an exchange must name an endpoint",
        );
    };

    let route = ctx.namespace.lookup(&path);
    let request = IncomingRequest::new(
        IncomingTransfer::new(recv, Arc::new(IncomingMeta::from_header(&header, ctx.peer))),
        send,
        Arc::clone(ctx),
    );
    match route {
        // Backpressure again: a full accept queue stalls this task, and the
        // peer feels it through flow control. If the endpoint went away, the
        // request's own destructor reports NO_REPLY.
        Some(Route::Request(queue)) => {
            if queue.send(request).await.is_err() {
                tracing::debug!(path, "endpoint went away while dispatching");
            }
        }
        Some(Route::Raw(queue)) => {
            if queue.send(Incoming::Exchange(request)).await.is_err() {
                tracing::debug!(path, "acceptor went away while dispatching");
            }
        }
        Some(Route::Transfer(_) | Route::Pub) => {
            tracing::debug!(path, "endpoint does not serve exchanges");
            request
                .refuse_coded(ErrorCode::Unsupported, codes::UNSUPPORTED)
                .await;
        }
        None => {
            tracing::debug!(path, "no endpoint registered");
            request
                .refuse_coded(ErrorCode::UnknownEndpoint, codes::UNKNOWN_ENDPOINT)
                .await;
        }
    }
    Ok(())
}
