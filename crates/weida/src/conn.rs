//! Per-connection actor and stream dispatch.
//!
//! One task owns the correlation tables of a connection; the same code runs on
//! both sides (a client connection simply has no endpoint namespace). The
//! tables are plain `HashMap`s inside that task rather than
//! `Arc<Mutex<HashMap>>` shared with the API handles (master doc §48).
//!
//! **Payload bytes never traverse the actor.** API handles own their `quinn`
//! streams directly, so writing and reading a transfer costs no task hop and
//! takes no lock (master doc §49). The actor sees only registrations and short
//! control frames.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use quinn::VarInt;
use tokio::sync::{mpsc, oneshot, watch};
use weida_core::state::{RecvAction, RecvEvent, RecvMachine};
use weida_core::{
    AckMode, AckState, Correlator, Error, ErrorCode, Limits, Outcome, ReplyDisposition, Role,
    SendAction, SendEvent, SendMachine, SendState, TransferId,
};
use weida_protocol::{
    AckHeader, Agreed, CancelHeader, DataHeader, ErrorHeader, FrameKind, Hello, MAX_PREAMBLE_LEN,
    PreambleError, codes, encode_frame, negotiate, parse_preamble,
};

use crate::listener::Namespace;
use crate::transfer::{IncomingMeta, IncomingRequest, IncomingTransfer};

/// Depth of the control channel between API handles and the actor.
const CTL_QUEUE: usize = 1024;

/// Messages the connection actor accepts.
pub(crate) enum Ctl {
    /// Reserve state for an outgoing transfer. Sent *before* the stream is
    /// opened, so an ACK can never arrive before its table entry exists.
    Register(Box<Registration>),
    /// The local side finished writing a transfer.
    Fin { id: u64 },
    /// The local side canceled a transfer.
    Cancel { id: u64 },
    /// The peer refused an outgoing transfer with `STOP_SENDING`.
    Stopped { id: u64, code: u64 },
    /// An ACK frame arrived.
    Ack(AckHeader),
    /// An ERROR frame arrived.
    PeerError(ErrorHeader),
    /// A CANCEL frame arrived.
    PeerCancel(CancelHeader),
    /// A reply DATA stream arrived and is still streaming.
    ReplyArrived {
        correlation_id: u64,
        transfer: Box<IncomingTransfer>,
    },
    /// Emit an ACK frame for one of the peer's transfers.
    SendAck { re: TransferId },
    /// Emit an ERROR frame for one of the peer's transfers.
    SendError { re: TransferId, code: ErrorCode },
    /// Emit a CANCEL frame for one of our own requests.
    SendCancel { id: TransferId },
    /// Track an accepted inbound request so CANCEL frames can reach it.
    TrackInbound {
        id: TransferId,
        cancel: watch::Sender<bool>,
    },
    /// Forget an inbound request.
    UntrackInbound { id: u64 },
}

/// Registration of one outgoing transfer.
pub(crate) struct Registration {
    pub id: TransferId,
    pub ack_mode: AckMode,
    pub outcome: oneshot::Sender<Result<Outcome, Error>>,
    pub reply: Option<oneshot::Sender<Result<IncomingTransfer, Error>>>,
    pub confirm: oneshot::Sender<Result<(), Error>>,
}

/// Actor-owned state of one outgoing transfer.
struct Outgoing {
    machine: SendMachine,
    outcome: Option<oneshot::Sender<Result<Outcome, Error>>>,
    reply: Option<oneshot::Sender<Result<IncomingTransfer, Error>>>,
}

impl Outgoing {
    fn is_done(&self) -> bool {
        self.outcome.is_none() && self.reply.is_none()
    }
}

/// Shared, cheaply clonable handle to one connection.
pub(crate) struct ConnCtx {
    pub conn: quinn::Connection,
    pub ctl: mpsc::Sender<Ctl>,
    pub limits: Limits,
    /// `None` on a client connection: nothing is registered to serve.
    pub namespace: Option<Arc<Namespace>>,
    agreed: watch::Receiver<Option<Agreed>>,
    next_id: AtomicU64,
}

pub(crate) type ConnHandle = Arc<ConnCtx>;

impl ConnCtx {
    /// Spawns the actor, the accept loop and the HELLO exchange for `conn`.
    pub(crate) fn spawn(
        conn: quinn::Connection,
        limits: Limits,
        namespace: Option<Arc<Namespace>>,
    ) -> ConnHandle {
        let (ctl_tx, ctl_rx) = mpsc::channel(CTL_QUEUE);
        let (agreed_tx, agreed_rx) = watch::channel(None);

        let ctx = Arc::new(ConnCtx {
            conn: conn.clone(),
            ctl: ctl_tx,
            limits,
            namespace,
            agreed: agreed_rx,
            next_id: AtomicU64::new(1),
        });

        tokio::spawn(driver(conn.clone(), limits, ctl_rx));
        tokio::spawn(accept_loop(Arc::clone(&ctx), agreed_tx));
        tokio::spawn(send_hello(conn, limits));
        ctx
    }

    /// Reserves the next outgoing transfer id.
    fn next_transfer_id(&self) -> TransferId {
        let raw = self.next_id.fetch_add(1, Ordering::Relaxed);
        TransferId::new(raw).expect("the counter starts at 1 and cannot reach 0 in practice")
    }

    /// Waits until the peer HELLO has been negotiated.
    ///
    /// QUIC unidirectional streams are unordered, so a DATA stream can be
    /// accepted before the peer's HELLO. Parking here is correct behaviour, not
    /// a protocol violation.
    pub(crate) async fn negotiated(&self) -> Result<Agreed, Error> {
        let mut rx = self.agreed.clone();
        loop {
            if let Some(agreed) = *rx.borrow_and_update() {
                return Ok(agreed);
            }
            if rx.changed().await.is_err() {
                return Err(Error::ConnectionLost);
            }
        }
    }

    /// Registers an outgoing transfer and waits for the actor's confirmation.
    ///
    /// Returning before the confirmation would reintroduce the race the
    /// registration exists to remove.
    pub(crate) async fn register(
        &self,
        ack_mode: AckMode,
        want_reply: bool,
    ) -> Result<Reserved, Error> {
        let id = self.next_transfer_id();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = if want_reply {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let (confirm_tx, confirm_rx) = oneshot::channel();

        self.send(Ctl::Register(Box::new(Registration {
            id,
            ack_mode,
            outcome: outcome_tx,
            reply: reply_tx,
            confirm: confirm_tx,
        })))
        .await?;

        confirm_rx.await.map_err(|_| Error::ConnectionLost)??;
        Ok(Reserved {
            id,
            outcome: outcome_rx,
            reply: reply_rx,
        })
    }

    /// Sends a control message, waiting for queue space.
    pub(crate) async fn send(&self, ctl: Ctl) -> Result<(), Error> {
        self.ctl.send(ctl).await.map_err(|_| Error::ConnectionLost)
    }

    /// Sends a control message without waiting.
    ///
    /// Used from `Drop` and from read paths where blocking is not an option. A
    /// full queue means 1024 outstanding control messages on one connection;
    /// dropping the notification there is preferable to blocking a destructor.
    pub(crate) fn notify(&self, ctl: Ctl) {
        if self.ctl.try_send(ctl).is_err() {
            tracing::debug!("connection control queue full or closed; notification dropped");
        }
    }

    /// Opens a unidirectional stream.
    pub(crate) async fn open_uni(&self) -> Result<quinn::SendStream, Error> {
        self.conn.open_uni().await.map_err(conn_error)
    }
}

/// A confirmed registration.
pub(crate) struct Reserved {
    pub id: TransferId,
    pub outcome: oneshot::Receiver<Result<Outcome, Error>>,
    pub reply: Option<oneshot::Receiver<Result<IncomingTransfer, Error>>>,
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
            _ => Error::ConnectionLost,
        },
        quinn::ConnectionError::LocallyClosed => Error::ConnectionLost,
        other => Error::Transport(other.to_string()),
    }
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

/// The connection actor: owns the correlation tables.
async fn driver(conn: quinn::Connection, limits: Limits, mut rx: mpsc::Receiver<Ctl>) {
    let mut outgoing: Correlator<Outgoing> = Correlator::new(limits.max_pending);
    let mut inbound: HashMap<u64, watch::Sender<bool>> = HashMap::new();

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(ctl) => handle_ctl(&conn, &mut outgoing, &mut inbound, ctl),
                None => break,
            },
            _ = conn.closed() => break,
        }
    }

    // Connection teardown: resolve every waiter per the sender-outcome rules.
    for (_, mut entry) in outgoing.drain() {
        let streaming = entry.machine.state() == SendState::Streaming;
        if let SendAction::Resolve(result) = entry.machine.on(SendEvent::ConnectionLost)
            && let Some(tx) = entry.outcome.take()
        {
            let _ = tx.send(result);
        }
        if let Some(tx) = entry.reply.take() {
            // Before our FIN the request definitely did not arrive; after it,
            // the reply may have been produced and lost.
            let _ = tx.send(Err(if streaming {
                Error::ConnectionLost
            } else {
                Error::Indeterminate
            }));
        }
    }
    for (_, cancel) in inbound.drain() {
        let _ = cancel.send(true);
    }
}

fn handle_ctl(
    conn: &quinn::Connection,
    outgoing: &mut Correlator<Outgoing>,
    inbound: &mut HashMap<u64, watch::Sender<bool>>,
    ctl: Ctl,
) {
    match ctl {
        Ctl::Register(reg) => {
            let Registration {
                id,
                ack_mode,
                outcome,
                reply,
                confirm,
            } = *reg;
            let entry = Outgoing {
                machine: SendMachine::new(ack_mode),
                outcome: Some(outcome),
                reply,
            };
            let _ = confirm.send(outgoing.register(id, entry));
        }
        Ctl::Fin { id } => apply(outgoing, id, SendEvent::LocalFin),
        Ctl::Cancel { id } => apply(outgoing, id, SendEvent::LocalCancel),
        Ctl::Stopped { id, code } => apply(
            outgoing,
            id,
            SendEvent::StopSending(codes::stop_reason(code)),
        ),
        Ctl::Ack(header) => {
            let Some(state) = AckState::from_wire(header.state) else {
                tracing::debug!(state = header.state, "ignoring ACK with an unknown state");
                return;
            };
            apply(outgoing, header.re.get(), SendEvent::Ack(state));
        }
        Ctl::PeerError(header) => {
            let code = ErrorCode::from_wire(header.code).unwrap_or(ErrorCode::Internal);
            let id = header.re.get();
            // An ERROR resolves the transfer *and* any reply waiting on it: no
            // reply is coming.
            if let Some(entry) = outgoing.take(id) {
                let mut entry = entry;
                if let Some(tx) = entry.reply.take() {
                    let _ = tx.send(Err(Error::from(code)));
                }
                reinsert(outgoing, id, entry);
            }
            apply(outgoing, id, SendEvent::ErrorFrame(code));
        }
        Ctl::PeerCancel(header) => match inbound.get(&header.id.get()) {
            Some(cancel) => {
                let _ = cancel.send(true);
            }
            None => tracing::debug!(id = %header.id, "ignoring CANCEL for an unknown request"),
        },
        Ctl::ReplyArrived {
            correlation_id,
            transfer,
        } => {
            if outgoing.classify(correlation_id) == ReplyDisposition::Reset {
                // Unknown or duplicate correlation: refuse the stream. This
                // races legitimately with cancellation, so it is never a
                // connection error.
                tracing::debug!(correlation_id, "refusing an uncorrelated reply");
                transfer.refuse(codes::CANCELED);
                return;
            }
            let mut entry = outgoing
                .take(correlation_id)
                .expect("classified as pending");
            match entry.reply.take() {
                Some(tx) => {
                    if let Err(Ok(unclaimed)) = tx.send(Ok(*transfer)) {
                        // The requester stopped waiting between classification
                        // and delivery.
                        unclaimed.refuse(codes::CANCELED);
                    }
                }
                None => transfer.refuse(codes::CANCELED),
            }
            reinsert(outgoing, correlation_id, entry);
        }
        Ctl::SendAck { re } => {
            spawn_control(conn, FrameKind::Ack, AckHeader::accepted(re).encode())
        }
        Ctl::SendError { re, code } => {
            spawn_control(conn, FrameKind::Error, ErrorHeader::new(re, code).encode())
        }
        Ctl::SendCancel { id } => {
            spawn_control(conn, FrameKind::Cancel, CancelHeader { id }.encode())
        }
        Ctl::TrackInbound { id, cancel } => {
            inbound.insert(id.get(), cancel);
        }
        Ctl::UntrackInbound { id } => {
            inbound.remove(&id);
        }
    }
}

/// Applies an event to one outgoing transfer, resolving and pruning as needed.
fn apply(outgoing: &mut Correlator<Outgoing>, id: u64, event: SendEvent) {
    let Some(mut entry) = outgoing.take(id) else {
        tracing::debug!(id, "ignoring an event for an unknown transfer");
        return;
    };
    if let SendAction::Resolve(result) = entry.machine.on(event)
        && let Some(tx) = entry.outcome.take()
    {
        let _ = tx.send(result);
    }
    reinsert(outgoing, id, entry);
}

/// Puts an entry back unless it has nothing left to resolve.
fn reinsert(outgoing: &mut Correlator<Outgoing>, id: u64, entry: Outgoing) {
    if entry.is_done() {
        return;
    }
    let id = TransferId::new(id).expect("ids in the table are non-zero");
    if outgoing.register(id, entry).is_err() {
        // Unreachable: the slot was just freed by `take`.
        tracing::error!(%id, "failed to reinsert a pending transfer");
    }
}

fn spawn_control(conn: &quinn::Connection, kind: FrameKind, header: Vec<u8>) {
    let conn = conn.clone();
    tokio::spawn(async move {
        if let Err(e) = write_control(&conn, kind, &header).await {
            tracing::debug!(%kind, error = %e, "failed to send a control frame");
        }
    });
}

/// Writes one header-only control frame on its own stream (master doc §11).
async fn write_control(
    conn: &quinn::Connection,
    kind: FrameKind,
    header: &[u8],
) -> Result<(), Error> {
    let mut stream = conn.open_uni().await.map_err(conn_error)?;
    stream
        .write_all(&encode_frame(kind, header))
        .await
        .map_err(write_error)?;
    stream
        .finish()
        .map_err(|_| Error::Transport("control stream closed early".into()))?;
    Ok(())
}

/// Sends our HELLO and closes the connection if the peer's never arrives.
async fn send_hello(conn: quinn::Connection, limits: Limits) {
    let hello = Hello::v0(
        limits.max_header_bytes,
        u64::from(limits.max_concurrent_uni_streams),
    );
    if let Err(e) = write_control(&conn, FrameKind::Hello, &hello.encode()).await {
        tracing::debug!(error = %e, "failed to send HELLO");
    }
}

/// Accepts inbound streams and spawns one task per stream.
///
/// Concurrency is bounded by the QUIC `max_concurrent_uni_streams` limit, so no
/// extra semaphore is needed: the peer cannot create more parse tasks than the
/// transport lets it open streams.
async fn accept_loop(ctx: ConnHandle, agreed_tx: watch::Sender<Option<Agreed>>) {
    let agreed_tx = Arc::new(agreed_tx);
    let hello_deadline = Duration::from_millis(ctx.limits.hello_timeout_ms);
    {
        let ctx = Arc::clone(&ctx);
        let agreed_tx = Arc::clone(&agreed_tx);
        tokio::spawn(async move {
            tokio::time::sleep(hello_deadline).await;
            if agreed_tx.borrow().is_none() {
                tracing::debug!("peer HELLO did not arrive in time");
                close(&ctx.conn, codes::NEGOTIATION_FAILED, "hello timeout");
            }
        });
    }

    loop {
        match ctx.conn.accept_uni().await {
            Ok(stream) => {
                let ctx = Arc::clone(&ctx);
                let agreed_tx = Arc::clone(&agreed_tx);
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(&ctx, &agreed_tx, stream).await {
                        tracing::debug!(error = %e, "inbound stream failed");
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed; accept loop ending");
                break;
            }
        }
    }
}

fn close(conn: &quinn::Connection, code: u64, reason: &str) {
    conn.close(
        VarInt::from_u64(code).expect("application codes are small"),
        reason.as_bytes(),
    );
}

/// Reads the preamble and header of one inbound stream, then dispatches it.
async fn handle_stream(
    ctx: &ConnHandle,
    agreed_tx: &watch::Sender<Option<Agreed>>,
    mut stream: quinn::RecvStream,
) -> Result<(), Error> {
    // The preamble is at most ten bytes, and the header length cap is checked
    // inside `parse_preamble` before anything is allocated.
    let mut scratch = [0u8; MAX_PREAMBLE_LEN];
    let mut have = 0usize;
    let preamble = loop {
        match parse_preamble(&scratch[..have], ctx.limits.max_header_bytes) {
            Ok((preamble, used)) => {
                debug_assert_eq!(used, have, "the preamble is read byte by byte");
                break preamble;
            }
            Err(PreambleError::Incomplete) => {
                if have == scratch.len() {
                    return violation(ctx, "preamble exceeds its maximum length");
                }
                match stream
                    .read(&mut scratch[have..have + 1])
                    .await
                    .map_err(read_error)?
                {
                    Some(0) => continue,
                    Some(n) => have += n,
                    None => return violation(ctx, "stream ended inside the preamble"),
                }
            }
            Err(e) => return violation(ctx, &e.to_string()),
        }
    };

    // Nothing but HELLO may be interpreted before negotiation completes.
    if preamble.kind != FrameKind::Hello {
        ctx.negotiated().await?;
    }

    let mut header = vec![0u8; preamble.header_len as usize];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|e| Error::Protocol(format!("truncated {} header: {e}", preamble.kind)))?;

    match preamble.kind {
        FrameKind::Hello => handle_hello(ctx, agreed_tx, &header),
        FrameKind::Data => handle_data(ctx, stream, &header).await,
        FrameKind::Ack => match AckHeader::decode(&header) {
            Ok(h) => {
                ctx.send(Ctl::Ack(h)).await?;
                Ok(())
            }
            Err(e) => violation(ctx, &e.to_string()),
        },
        FrameKind::Error => match ErrorHeader::decode(&header) {
            Ok(h) => {
                ctx.send(Ctl::PeerError(h)).await?;
                Ok(())
            }
            Err(e) => violation(ctx, &e.to_string()),
        },
        FrameKind::Cancel => match CancelHeader::decode(&header) {
            Ok(h) => {
                ctx.send(Ctl::PeerCancel(h)).await?;
                Ok(())
            }
            Err(e) => violation(ctx, &e.to_string()),
        },
    }
}

fn violation(ctx: &ConnHandle, reason: &str) -> Result<(), Error> {
    tracing::debug!(reason, "closing connection: protocol violation");
    close(&ctx.conn, codes::PROTOCOL_VIOLATION, reason);
    Err(Error::Protocol(reason.to_owned()))
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
    let ours = Hello::v0(
        ctx.limits.max_header_bytes,
        u64::from(ctx.limits.max_concurrent_uni_streams),
    );
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
            close(&ctx.conn, codes::NEGOTIATION_FAILED, &e.to_string());
            Err(e.into())
        }
    }
}

async fn handle_data(
    ctx: &ConnHandle,
    stream: quinn::RecvStream,
    header: &[u8],
) -> Result<(), Error> {
    let header = match DataHeader::decode(header) {
        Ok(h) => h,
        Err(e) => return violation(ctx, &e.to_string()),
    };

    // A reserved role or ack mode is refused, never reinterpreted: silently
    // weakening a requested guarantee is forbidden (master doc §21).
    let (Some(role), Some(ack_mode)) = (header.role(), header.ack_mode()) else {
        refuse(
            ctx,
            stream,
            header.transfer_id,
            RecvEvent::UnsupportedPolicy,
            ack_mode_or_none(&header),
        );
        return Ok(());
    };

    let meta = IncomingMeta::from_header(&header, ack_mode);
    match role {
        Role::Reply => {
            let correlation_id = header
                .correlation_id
                .expect("the decoder requires it for replies")
                .get();
            let transfer = IncomingTransfer::new(Arc::clone(ctx), stream, meta);
            ctx.send(Ctl::ReplyArrived {
                correlation_id,
                transfer: Box::new(transfer),
            })
            .await
        }
        Role::Request => {
            let path = header
                .endpoint
                .as_deref()
                .expect("the decoder requires it for requests");
            let route = ctx.namespace.as_ref().and_then(|ns| ns.lookup(path));
            let Some(route) = route else {
                tracing::debug!(path, "no endpoint registered");
                refuse(
                    ctx,
                    stream,
                    header.transfer_id,
                    RecvEvent::UnknownEndpoint,
                    ack_mode,
                );
                return Ok(());
            };

            let id = header.transfer_id;
            let (cancel_tx, cancel_rx) = watch::channel(false);
            ctx.send(Ctl::TrackInbound {
                id,
                cancel: cancel_tx,
            })
            .await?;

            let request = IncomingRequest::new(
                IncomingTransfer::new(Arc::clone(ctx), stream, meta),
                cancel_rx,
            );
            // Awaiting a queue slot is the backpressure path: it stalls this
            // stream's task, which stalls the peer through QUIC flow control.
            if route.send(request).await.is_err() {
                tracing::debug!(path, "endpoint went away while dispatching");
                ctx.notify(Ctl::UntrackInbound { id: id.get() });
                ctx.notify(Ctl::SendError {
                    re: id,
                    code: ErrorCode::UnknownEndpoint,
                });
            }
            Ok(())
        }
    }
}

fn ack_mode_or_none(header: &DataHeader) -> AckMode {
    header.ack_mode().unwrap_or(AckMode::None)
}

/// Refuses an inbound transfer according to the receiver state machine.
fn refuse(
    ctx: &ConnHandle,
    stream: quinn::RecvStream,
    re: TransferId,
    event: RecvEvent,
    ack_mode: AckMode,
) {
    let mut machine = RecvMachine::new(ack_mode);
    let mut stream = stream;
    match machine.on(event) {
        RecvAction::Refuse { reason, error } => {
            let code = match reason {
                weida_core::StopReason::Rejected => codes::REJECTED,
                weida_core::StopReason::Canceled => codes::CANCELED,
                weida_core::StopReason::UnknownEndpoint => codes::UNKNOWN_ENDPOINT,
                weida_core::StopReason::Other(c) => c,
            };
            let _ = stream.stop(VarInt::from_u64(code).expect("application codes are small"));
            if let Some(code) = error {
                ctx.notify(Ctl::SendError { re, code });
            }
        }
        RecvAction::SendError(code) => ctx.notify(Ctl::SendError { re, code }),
        other => tracing::debug!(?other, "unexpected refusal action"),
    }
}
