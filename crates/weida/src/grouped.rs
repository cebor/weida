//! Grouped local connections: the shape shared by `weida+unix://` and
//! `weida+pipe://`.
//!
//! The two local transports with a kernel between the peers
//! ([decision 0010](../../../docs/decisions/0010-local-transport.md) §4.1):
//! a stream socket on a filesystem path, or a named pipe, one connection per
//! transfer, no TLS, and the peer proved by the kernel rather than by a key
//! [0010 §4.4, §4.5]. What differs between the two — how a connection is
//! dialled, how it splits into halves, how its end is signalled, and what
//! the kernel says about the peer — is the [`Stream`] trait; everything
//! about *grouping* is here and written once.
//!
//! **How connections become a peer** is
//! [decision 0012](../../../docs/decisions/0012-local-connection-grouping.md).
//! An accepted connection cannot be dialled back, so the connections of one
//! peer are grouped instead of multiplexed:
//!
//! ```text
//! byte 0x01            control connection: the peer itself
//!   <- 16 bytes        the group token, issued by the server
//!   then HELLO both ways on this connection
//!
//! byte 0x02 + token    transfer connection: one weida stream
//!   accepted only if the token names a live control connection *and* the
//!   kernel credentials match that connection's [0012 §4.2]
//!
//! byte 0x03 + token    reverse connection: parked, so the *accepting* side
//!   can open a stream toward this peer; same admission rule [0012 §4.4]
//! ```
//!
//! The token binds connections and resumes nothing: no subscriptions, no
//! sequence position, no dedup window, meaningless once the control
//! connection closes. That is why it is not the session state
//! [0008](../../../docs/decisions/0008-session-identity.md) §4.5 forbids
//! [0012 §4.5].
//!
//! **Dispatch is by path**, as `docs/PROTOCOL.md` §2.1 already specifies for
//! local transports: a transfer connection carries one frame, and the pattern
//! registered at the path it addresses says whether a reply is expected. A
//! parked connection is the same thing in the other direction: the client
//! reads a DATA header on it and routes it by path, exactly as an accepted
//! unidirectional stream is routed on QUIC.
//!
//! **Fan-out is bounded by the pool.** A publisher takes one parked
//! connection per copy and the subscriber parks a replacement; a publisher
//! that finds none drops that copy and counts it, which is the answer
//! fan-out already gives an exhausted subscriber budget [0012 §4.4].

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use weida_core::{Error, LossCause, PeerIdentity};
use weida_protocol::codes;

/// First byte of a control connection.
const KIND_CONTROL: u8 = 0x01;
/// First byte of a transfer connection, followed by the 16-byte group token.
const KIND_TRANSFER: u8 = 0x02;
/// First byte of a parked reverse connection, followed by the same token:
/// a stream the accepting side may take when it needs to write to this peer
/// [0012 §4.4].
const KIND_REVERSE: u8 = 0x03;
/// Length of a group token [0012 §4.2].
const TOKEN_LEN: usize = 16;

const NO_CODE: u64 = u64::MAX;

/// One kind of local connection: what the grouping needs from the OS.
///
/// Implemented by the `AF_UNIX` stream and by the named pipe. The
/// preamble bytes of [0012] are written and read on the raw stream, so the
/// type itself is a byte stream; the halves it splits into are what a weida
/// stream is made of afterwards.
pub(crate) trait Stream: AsyncRead + AsyncWrite + Unpin + Send + Sized + 'static {
    /// What a dial needs to reach the same endpoint again: the socket path,
    /// the pipe path.
    type Endpoint: Send + Sync + 'static;
    /// What the kernel says about the process on the other end.
    type Principal: Clone + Send + Sync + 'static;
    /// The writing half of a split connection.
    type Writer: AsyncWrite + Unpin + Send + 'static;
    /// The reading half of a split connection.
    type Reader: AsyncRead + Unpin + Send + 'static;

    /// Dials a new connection to `endpoint`.
    ///
    /// An endpoint nobody serves is `ConnectionLost(PeerClosed)`, so a
    /// caller can redial; everything else is an I/O error.
    fn connect(endpoint: &Self::Endpoint) -> impl Future<Output = Result<Self, Error>> + Send;

    /// The kernel's answer to who is on the other end.
    ///
    /// Called on the accepting side after the first byte has been read —
    /// a pipe cannot identify a client that has not written yet — and on
    /// the dialling side after the token has been read.
    fn principal(&self) -> Result<Self::Principal, Error>;

    /// Splits into the reading and writing halves.
    fn split(self) -> (Self::Reader, Self::Writer);

    /// Whether the kernel calls two connections the same peer: the account
    /// always, the pid where the platform reports one, and never the pid
    /// alone [0012 §4.2].
    fn same_peer(group: &Self::Principal, asking: &Self::Principal) -> bool;

    /// The principal as a proved identity.
    fn identity(principal: &Self::Principal) -> PeerIdentity;

    /// Ends the payload on `writer`: the FIN the reader sees.
    fn finish(writer: Self::Writer);

    /// Abandons the payload on `writer` with `code`.
    fn reset(writer: Self::Writer, code: u64);

    /// Refuses the rest of the payload on `reader` with `code`.
    fn stop(reader: Self::Reader, code: u64);

    /// A read error as the transport reports it.
    fn read_error(error: std::io::Error) -> Error;
}

/// Both halves of one transfer connection.
pub(crate) type Halves<S> = (LocalSend<S>, LocalRecv<S>);

/// The peers a binding has admitted, keyed by their group token.
pub(crate) struct Groups<S: Stream> {
    entries: StdMutex<HashMap<[u8; TOKEN_LEN], Group<S>>>,
}

impl<S: Stream> Default for Groups<S> {
    fn default() -> Groups<S> {
        Groups {
            entries: StdMutex::new(HashMap::new()),
        }
    }
}

struct Group<S: Stream> {
    /// Credentials of the control connection: what a transfer connection must
    /// match [0012 §4.2].
    principal: S::Principal,
    transfers: mpsc::UnboundedSender<Halves<S>>,
    /// Connections this peer parked for streams in the other direction
    /// [0012 §4.4]. Shared with the peer's link, which is what spends them.
    reverse: Arc<ReversePool<S>>,
}

/// The connections one peer parked, and the two bounds they live under.
///
/// `max` is `Limits::max_parked_reverse`, the pool's own ceiling; `slots` is
/// the connection's `max_local_streams` accounting, which a parked
/// connection counts against like any other live local connection
/// [0012 §4.4]. Both are refusals rather than waits: a pool that cannot grow
/// is the peer's fan-out budget, not backpressure on a transfer.
pub(crate) struct ReversePool<S: Stream> {
    parked: StdMutex<VecDeque<LocalSend<S>>>,
    max: usize,
    slots: Arc<Semaphore>,
}

impl<S: Stream> ReversePool<S> {
    fn new(slots: Arc<Semaphore>, max: usize) -> ReversePool<S> {
        ReversePool {
            parked: StdMutex::new(VecDeque::new()),
            max,
            slots,
        }
    }

    /// Takes a parked connection in, or refuses it because a bound says so.
    fn park(&self, send: S::Writer) -> bool {
        let mut parked = self.parked.lock().expect("reverse pool poisoned");
        if parked.len() >= self.max {
            return false;
        }
        let Some(slot) = StreamSlot::try_acquire(&self.slots) else {
            return false;
        };
        parked.push_back(LocalSend::new(send, Some(Arc::new(slot))));
        true
    }

    /// Spends one on a stream toward the peer. `None` is a drop, not a
    /// failure of the connection [0012 §4.4].
    fn take(&self) -> Option<LocalSend<S>> {
        self.parked
            .lock()
            .expect("reverse pool poisoned")
            .pop_front()
    }
}

impl<S: Stream> Groups<S> {
    fn insert(
        &self,
        token: [u8; TOKEN_LEN],
        principal: S::Principal,
        transfers: mpsc::UnboundedSender<Halves<S>>,
        reverse: Arc<ReversePool<S>>,
    ) {
        self.entries
            .lock()
            .expect("group registry poisoned")
            .insert(
                token,
                Group {
                    principal,
                    transfers,
                    reverse,
                },
            );
    }

    fn remove(&self, token: &[u8; TOKEN_LEN]) {
        self.entries
            .lock()
            .expect("group registry poisoned")
            .remove(token);
    }

    /// Runs `f` against the group `token` names, if the kernel agrees that
    /// the caller is that group's peer.
    ///
    /// The token names the group; the kernel says who is asking
    /// [0012 §4.2].
    fn with_group<T>(
        &self,
        token: &[u8; TOKEN_LEN],
        principal: &S::Principal,
        f: impl FnOnce(&Group<S>) -> T,
    ) -> Option<T> {
        let entries = self.entries.lock().expect("group registry poisoned");
        let group = entries.get(token)?;
        if !S::same_peer(&group.principal, principal) {
            return None;
        }
        Some(f(group))
    }

    /// Hands a transfer connection to its peer.
    fn admit(&self, token: &[u8; TOKEN_LEN], principal: &S::Principal, halves: Halves<S>) -> bool {
        self.with_group(token, principal, |group| {
            group.transfers.send(halves).is_ok()
        })
        .unwrap_or(false)
    }

    /// Parks a reverse connection for its peer, if a bound leaves room.
    fn park(&self, token: &[u8; TOKEN_LEN], principal: &S::Principal, send: S::Writer) -> bool {
        self.with_group(token, principal, |group| group.reverse.park(send))
            .unwrap_or(false)
    }
}

/// The local preamble of an accepted connection, read.
pub(crate) enum Accepted<S: Stream> {
    Control(S, S::Principal),
    Transfer([u8; TOKEN_LEN], S, S::Principal),
    Reverse([u8; TOKEN_LEN], S, S::Principal),
}

/// Reads the local preamble of an accepted connection and asks the kernel
/// who sent it.
pub(crate) async fn read_accepted<S: Stream>(mut stream: S) -> Result<Accepted<S>, Error> {
    let mut kind = [0u8; 1];
    stream.read_exact(&mut kind).await.map_err(Error::Io)?;
    // After the first byte, on purpose: a pipe's client is identified
    // through what it wrote.
    let principal = stream.principal()?;
    match kind[0] {
        KIND_CONTROL => Ok(Accepted::Control(stream, principal)),
        KIND_TRANSFER | KIND_REVERSE => {
            let mut token = [0u8; TOKEN_LEN];
            stream.read_exact(&mut token).await.map_err(Error::Io)?;
            Ok(if kind[0] == KIND_TRANSFER {
                Accepted::Transfer(token, stream, principal)
            } else {
                Accepted::Reverse(token, stream, principal)
            })
        }
        other => Err(Error::Protocol(format!(
            "unknown local connection kind {other:#04x}"
        ))),
    }
}

/// Serves one accepted control connection: issues the token and builds the
/// accepting side's link.
pub(crate) async fn accept_control<S: Stream>(
    mut stream: S,
    principal: S::Principal,
    groups: Arc<Groups<S>>,
    max_streams: usize,
    max_parked: usize,
) -> Result<Grouped<S>, Error> {
    let token = random_token();
    stream.write_all(&token).await.map_err(Error::Io)?;
    let (transfers_tx, transfers_rx) = mpsc::unbounded_channel();
    let mut link = Grouped::new(
        Side::Accept {
            groups: Arc::clone(&groups),
            token,
        },
        stream,
        Some(principal.clone()),
        max_streams,
        max_parked,
    );
    // The pool is the peer's and the link's at once: the peer fills it over
    // the socket, the link spends it on fan-out [0012 §4.4].
    let reverse = Arc::new(ReversePool::new(Arc::clone(&link.slots), max_parked));
    link.reverse = Some(Arc::clone(&reverse));
    link.transfers = tokio::sync::Mutex::new(Some(transfers_rx));
    groups.insert(token, principal, transfers_tx, reverse);
    Ok(link)
}

/// Parks an accepted reverse connection with the peer its token names
/// [0012 §4.4]. The client never writes on it, so only the write half is
/// kept; the read half goes away with this call.
pub(crate) fn admit_reverse<S: Stream>(
    groups: &Groups<S>,
    token: &[u8; TOKEN_LEN],
    principal: &S::Principal,
    stream: S,
) -> bool {
    let (_recv, send) = stream.split();
    groups.park(token, principal, send)
}

/// Hands an accepted transfer connection to the peer its token names, if the
/// kernel agrees that it is the same peer [0012 §4.2].
pub(crate) fn admit_transfer<S: Stream>(
    groups: &Groups<S>,
    token: &[u8; TOKEN_LEN],
    principal: &S::Principal,
    stream: S,
) -> bool {
    let (recv, send) = stream.split();
    groups.admit(
        token,
        principal,
        (LocalSend::new(send, None), LocalRecv::new(recv, None)),
    )
}

/// Dials `endpoint`, completing the control handshake of [0012 §4.1].
pub(crate) async fn dial<S: Stream>(
    endpoint: S::Endpoint,
    max_streams: usize,
    max_parked: usize,
) -> Result<Grouped<S>, Error> {
    let mut stream = S::connect(&endpoint).await?;
    stream.write_all(&[KIND_CONTROL]).await.map_err(Error::Io)?;
    let mut token = [0u8; TOKEN_LEN];
    stream.read_exact(&mut token).await.map_err(Error::Io)?;
    let principal = stream.principal()?;
    Ok(Grouped::new(
        Side::Dial { endpoint, token },
        stream,
        Some(principal),
        max_streams,
        max_parked,
    ))
}

fn random_token() -> [u8; TOKEN_LEN] {
    use rand::RngCore;
    let mut token = [0u8; TOKEN_LEN];
    rand::rng().fill_bytes(&mut token);
    token
}

/// Which end of a local connection this is, and what it can do with that.
enum Side<S: Stream> {
    /// The dialling side: it may open more connections.
    Dial {
        endpoint: S::Endpoint,
        token: [u8; TOKEN_LEN],
    },
    /// The accepting side: it receives connections, and opens streams toward
    /// the peer only out of the connections that peer parked [0012 §4.4].
    Accept {
        groups: Arc<Groups<S>>,
        token: [u8; TOKEN_LEN],
    },
}

/// One local peer: its control connection plus the transfer connections
/// grouped with it.
pub(crate) struct Grouped<S: Stream> {
    side: Side<S>,
    /// The control connection's halves, handed out once each: the first
    /// outbound stream is this side's HELLO, the first inbound one the peer's
    /// [0012 §4.1].
    control_send: StdMutex<Option<S::Writer>>,
    control_recv: StdMutex<Option<S::Reader>>,
    /// Transfer connections the peer opened, for the accepting side.
    transfers: tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<Halves<S>>>>,
    /// Connections this peer parked for us, for the accepting side: the only
    /// way a server opens a stream toward a peer that dialled it.
    reverse: Option<Arc<ReversePool<S>>>,
    /// Connections this side parked, on the dialling side: their read halves
    /// arrive here and are handed to the connection driver, which dispatches
    /// whatever the peer writes on them by path [0012 §4.4].
    parked_tx: mpsc::UnboundedSender<LocalRecv<S>>,
    parked_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<LocalRecv<S>>>,
    /// Parked connections that have been spent and need replacing.
    deficit: Arc<Deficit>,
    peer: Option<S::Principal>,
    /// One permit per live local connection: `max_local_streams` of them, and
    /// an `open` with none free waits for one rather than failing
    /// (`docs/GUARANTEES.md` §6). The parked reverse pool shares this budget.
    slots: Arc<Semaphore>,
    max_parked: usize,
    closed: AtomicU64,
    closed_notify: Notify,
    id: usize,
}

/// How many parked connections have been consumed since the last refill.
///
/// A parked connection is spent the moment the peer writes its first byte on
/// it, not when the copy ends: refilling then keeps the pool at its size
/// while a long copy is still streaming.
#[derive(Default)]
pub(crate) struct Deficit {
    count: AtomicUsize,
    notify: Notify,
}

impl Deficit {
    fn record(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    /// Waits until at least one connection needs replacing, and claims them.
    async fn take(&self) -> usize {
        loop {
            let owed = self.count.swap(0, Ordering::Relaxed);
            if owed > 0 {
                return owed;
            }
            self.notify.notified().await;
        }
    }
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

impl<S: Stream> Grouped<S> {
    fn new(
        side: Side<S>,
        control: S,
        peer: Option<S::Principal>,
        max_streams: usize,
        max_parked: usize,
    ) -> Grouped<S> {
        let (recv, send) = control.split();
        let (parked_tx, parked_rx) = mpsc::unbounded_channel();
        Grouped {
            side,
            control_send: StdMutex::new(Some(send)),
            control_recv: StdMutex::new(Some(recv)),
            transfers: tokio::sync::Mutex::new(None),
            reverse: None,
            parked_tx,
            parked_rx: tokio::sync::Mutex::new(parked_rx),
            deficit: Arc::new(Deficit::default()),
            peer,
            slots: Arc::new(Semaphore::new(max_streams)),
            max_parked,
            closed: AtomicU64::new(NO_CODE),
            closed_notify: Notify::new(),
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn stable_id(&self) -> usize {
        self.id
    }

    pub(crate) fn peer(&self) -> Option<PeerIdentity> {
        self.peer.as_ref().map(S::identity)
    }

    pub(crate) fn close_reason(&self) -> Option<Error> {
        let code = self.closed.load(Ordering::Acquire);
        (code != NO_CODE).then(|| match code {
            codes::NEGOTIATION_FAILED => {
                Error::Negotiation("peer closed the connection: negotiation failed".into())
            }
            _ => Error::ConnectionLost(LossCause::PeerClosed),
        })
    }

    pub(crate) fn close(&self, code: u64, _reason: &str) {
        if self
            .closed
            .compare_exchange(NO_CODE, code, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Dropping the control halves is the close on the wire: there is
            // no frame for it and no code to carry [0012 §4.7].
            self.control_send.lock().expect("poisoned").take();
            self.control_recv.lock().expect("poisoned").take();
            if let Side::Accept { groups, token, .. } = &self.side {
                groups.remove(token);
            }
            self.closed_notify.notify_waiters();
        }
    }

    pub(crate) async fn closed(&self) -> Error {
        loop {
            if let Some(reason) = self.close_reason() {
                return reason;
            }
            self.closed_notify.notified().await;
        }
    }

    /// Whether every slot is taken right now.
    ///
    /// One atomic load, for the caller that has something to free before it
    /// parks on a slot (`ConnCtx::open_uni`).
    pub(crate) fn slots_exhausted(&self) -> bool {
        self.slots.available_permits() == 0
    }

    /// A slot for one more local connection, waiting for one where
    /// `max_local_streams` are already live.
    ///
    /// This is the local shape of what `quinn` does with the peer's
    /// `max_concurrent_uni_streams`: an `open` with no budget left parks until
    /// a stream ends, and the deadline is the caller's, not the transport's.
    /// Dropping the future is the cancellation and it releases nothing,
    /// because a waiter holds no descriptor.
    async fn slot(&self) -> Result<StreamSlot, Error> {
        let waiting = Arc::clone(&self.slots).acquire_owned();
        tokio::select! {
            permit = waiting => match permit {
                Ok(permit) => Ok(StreamSlot { _permit: permit }),
                Err(_) => Err(Error::ConnectionLost(LossCause::LocallyClosed)),
            },
            reason = self.closed() => Err(reason),
        }
    }

    /// Opens one stream toward the peer.
    ///
    /// On the dialling side that is a new transfer connection. On the
    /// accepting side there is nothing to dial, so it is one of the
    /// connections the peer parked; an empty pool is
    /// [`Error::NoParkedConnection`], which fan-out counts as a drop rather
    /// than as a failure of the subscription [0012 §4.4].
    ///
    /// The control connection's own write half is never handed out here: it
    /// belongs to `open_control`, so that a fan-out copy can never take the
    /// stream the HELLO is owed.
    pub(crate) async fn open_uni(&self) -> Result<LocalSend<S>, Error> {
        if let Side::Accept { .. } = &self.side {
            if let Some(closed) = self.close_reason() {
                return Err(closed);
            }
            return self
                .reverse
                .as_ref()
                .and_then(|pool| pool.take())
                .ok_or(Error::NoParkedConnection);
        }
        let (send, _recv) = self.open_transfer().await?;
        Ok(send)
    }

    /// Hands out the control connection's write half, once.
    ///
    /// This is the one stream each side has toward the other without dialling
    /// anything, and both sides spend it on HELLO ([0012 §4.1]).
    pub(crate) fn open_control(&self) -> Result<LocalSend<S>, Error> {
        match self.take_control_send() {
            Some(send) => Ok(LocalSend::new(send, None)),
            None => Err(Error::Transport("control stream already used".into())),
        }
    }

    pub(crate) async fn open_bi(&self) -> Result<Halves<S>, Error> {
        self.open_transfer().await
    }

    fn take_control_send(&self) -> Option<S::Writer> {
        self.control_send.lock().expect("poisoned").take()
    }

    async fn open_transfer(&self) -> Result<Halves<S>, Error> {
        if let Some(closed) = self.close_reason() {
            return Err(closed);
        }
        let (endpoint, token) = match &self.side {
            Side::Dial { endpoint, token } => (endpoint, token),
            // An exchange opened *by* the server does not exist: a parked
            // connection carries one stream, and the pattern that needs two
            // is always initiated by the dialling side.
            Side::Accept { .. } => return Err(Error::Unsupported),
        };
        let slot = self.slot().await?;
        let mut stream = match S::connect(endpoint).await {
            Ok(stream) => stream,
            // Nobody answers on the address any more: the peer is gone, and
            // the group with it. Learned here, at the next open, because a
            // local transport has no idle timeout and carries nothing on the
            // control connection after the HELLOs to learn it from earlier
            // (`docs/PATTERNS.md` §1.10). Marking the link closed is what
            // lets a redial start [0031 §4.10].
            Err(e @ Error::ConnectionLost(_)) => {
                self.close(codes::SHUTDOWN, "nobody answers on the address");
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        let mut preamble = [0u8; 1 + TOKEN_LEN];
        preamble[0] = KIND_TRANSFER;
        preamble[1..].copy_from_slice(token);
        stream.write_all(&preamble).await.map_err(Error::Io)?;
        let (recv, send) = stream.split();
        let slot = Arc::new(slot);
        Ok((
            LocalSend::new(send, Some(Arc::clone(&slot))),
            LocalRecv::new(recv, Some(slot)),
        ))
    }

    /// Streams the peer opened toward this side.
    ///
    /// The control connection's read half is the peer's HELLO, and after it
    /// come the connections this side parked: whatever the peer writes on
    /// one is read here and dispatched by path, which is what an accepted
    /// unidirectional stream is on QUIC [0012 §4.4]. A transfer connection
    /// the peer *opened* is not one of these - that is `accept_bi`, and it
    /// is dispatched by path too [0012 §4.3].
    pub(crate) async fn accept_uni(&self) -> Result<LocalRecv<S>, Error> {
        if let Some(recv) = self.control_recv.lock().expect("poisoned").take() {
            return Ok(LocalRecv::new(recv, None));
        }
        let mut parked = self.parked_rx.lock().await;
        tokio::select! {
            arrived = parked.recv() => arrived.ok_or(Error::ConnectionLost(LossCause::PeerClosed)),
            reason = self.closed() => Err(reason),
        }
    }

    /// Fills the reverse pool up to `max_parked_reverse`.
    ///
    /// Returns how many connections are parked. Zero with an error means the
    /// peer cannot fan out to this side at all, which is what a subscriber
    /// reports at subscribe time rather than discovering as silence
    /// [0012 §4.4].
    pub(crate) async fn park_reverse(&self) -> Result<usize, Error> {
        let mut parked = 0;
        for _ in 0..self.max_parked {
            match self.park_one().await {
                Ok(()) => parked += 1,
                Err(e) if parked == 0 => return Err(e),
                // A bound reached part way is the pool being smaller than
                // asked for, not a failure: the publisher's answer to an
                // empty pool is a counted drop either way.
                Err(e) => {
                    tracing::debug!(error = %e, parked, "reverse pool filled short");
                    break;
                }
            }
        }
        Ok(parked)
    }

    /// Replaces parked connections as they are spent, until the connection
    /// closes. Spawned once per subscribing connection.
    pub(crate) async fn maintain_reverse(&self) {
        loop {
            let owed = tokio::select! {
                owed = self.deficit.take() => owed,
                _ = self.closed() => return,
            };
            for _ in 0..owed {
                if let Err(e) = self.park_one().await {
                    tracing::debug!(error = %e, "reverse pool not replenished");
                    return;
                }
            }
        }
    }

    /// Dials one connection, marks it `reverse` and hands its read half to
    /// the connection driver to wait on.
    async fn park_one(&self) -> Result<(), Error> {
        if let Some(closed) = self.close_reason() {
            return Err(closed);
        }
        let Side::Dial { endpoint, token } = &self.side else {
            return Err(Error::Unsupported);
        };
        // A pool that cannot grow is a refusal, not backpressure: filling it
        // must never park on a slot a transfer is waiting for.
        let slot = Arc::new(StreamSlot::try_acquire(&self.slots).ok_or(Error::LimitExceeded)?);
        let mut stream = S::connect(endpoint).await?;
        let mut preamble = [0u8; 1 + TOKEN_LEN];
        preamble[0] = KIND_REVERSE;
        preamble[1..].copy_from_slice(token);
        stream.write_all(&preamble).await.map_err(Error::Io)?;
        // This side never writes on a parked connection; letting the write
        // half go costs nothing, because the peer never reads on it either.
        let (recv, _send) = stream.split();
        let mut recv = LocalRecv::new(recv, Some(slot));
        recv.deficit = Some(Arc::clone(&self.deficit));
        self.parked_tx
            .send(recv)
            .map_err(|_| Error::ConnectionLost(LossCause::LocallyClosed))
    }

    /// Transfer connections, for the accepting side.
    pub(crate) async fn accept_bi(&self) -> Result<Halves<S>, Error> {
        let mut queue = self.transfers.lock().await;
        let Some(queue) = queue.as_mut() else {
            return Err(self.closed().await);
        };
        tokio::select! {
            accepted = queue.recv() => accepted.ok_or(Error::ConnectionLost(LossCause::PeerClosed)),
            reason = self.closed() => Err(reason),
        }
    }
}

/// Keeps one live transfer connection counted against `max_local_streams`:
/// the permit is returned when the last half of that connection is dropped.
struct StreamSlot {
    _permit: OwnedSemaphorePermit,
}

impl StreamSlot {
    /// Counts one more live local connection, or `None` at the cap, without
    /// waiting: on this transport a stream *is* a connection, so
    /// `max_local_streams` is a file-descriptor count [0010 §4.2].
    ///
    /// Only the callers whose answer to a full budget is a refusal use this;
    /// a transfer waits, in `Grouped::slot`.
    fn try_acquire(slots: &Arc<Semaphore>) -> Option<StreamSlot> {
        Arc::clone(slots)
            .try_acquire_owned()
            .ok()
            .map(|permit| StreamSlot { _permit: permit })
    }
}

/// The writing half of one local stream.
pub(crate) struct LocalSend<S: Stream> {
    io: Option<S::Writer>,
    _slot: Option<Arc<StreamSlot>>,
}

impl<S: Stream> LocalSend<S> {
    fn new(io: S::Writer, slot: Option<Arc<StreamSlot>>) -> LocalSend<S> {
        LocalSend {
            io: Some(io),
            _slot: slot,
        }
    }

    pub(crate) fn io_mut(&mut self) -> Option<&mut S::Writer> {
        self.io.as_mut()
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        let Some(io) = self.io.as_mut() else {
            return Err(Error::Transport("stream already closed".into()));
        };
        io.write_all(buf).await.map_err(|e| match e.kind() {
            // The peer closed its read side: the local equivalent of
            // `STOP_SENDING`, without a code to carry [0012 §4.7].
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset => Error::Canceled,
            _ => Error::Transport(format!("local stream write failed: {e}")),
        })
    }

    /// Ends the payload: the FIN the reader sees.
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        match self.io.take() {
            Some(io) => {
                S::finish(io);
                Ok(())
            }
            None => Err(Error::Transport("stream already closed".into())),
        }
    }

    /// Abandons the payload.
    pub(crate) fn reset(&mut self, code: u64) {
        if let Some(io) = self.io.take() {
            S::reset(io, code);
        }
    }

    /// The receipt: on a socket the bytes are the peer's transport's as soon
    /// as `write_all` returned, and the FIN follows when this half drops, so
    /// there is nothing left to wait for (`docs/GUARANTEES.md` §3).
    ///
    /// **Named loss:** a local stream carries no code, so a refusal cannot be
    /// reported here the way `STOP_SENDING` is on QUIC. A peer that refuses
    /// surfaces on the *write*, as `Error::Canceled`, not on the receipt
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.7).
    pub(crate) fn stopped(
        &self,
    ) -> impl Future<Output = Result<Option<u64>, Error>> + Send + Sync + use<S> {
        async move { Ok(None) }
    }
}

impl<S: Stream> Drop for LocalSend<S> {
    /// A half dropped without `finish` or `reset` is an abandoned payload.
    fn drop(&mut self) {
        if let Some(io) = self.io.take() {
            S::reset(io, codes::CANCELED);
        }
    }
}

/// The reading half of one local stream.
pub(crate) struct LocalRecv<S: Stream> {
    io: Option<S::Reader>,
    _slot: Option<Arc<StreamSlot>>,
    /// Set on a parked reverse connection: the pool this one was taken from
    /// wants a replacement once the peer has spent it [0012 §4.4].
    deficit: Option<Arc<Deficit>>,
    spent: bool,
}

impl<S: Stream> LocalRecv<S> {
    fn new(io: S::Reader, slot: Option<Arc<StreamSlot>>) -> LocalRecv<S> {
        LocalRecv {
            io: Some(io),
            _slot: slot,
            deficit: None,
            spent: false,
        }
    }

    pub(crate) fn io_mut(&mut self) -> Option<&mut S::Reader> {
        self.io.as_mut()
    }

    /// Reports a parked connection as spent, once.
    ///
    /// The first byte the peer writes is the moment this connection stopped
    /// being available, so that is when the replacement is asked for - not
    /// when the copy it carries has been read.
    fn spend(&mut self) {
        if self.spent {
            return;
        }
        self.spent = true;
        if let Some(deficit) = &self.deficit {
            deficit.record();
        }
    }

    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Error> {
        let Some(io) = self.io.as_mut() else {
            return Ok(None);
        };
        let read = io.read(buf).await.map_err(S::read_error)?;
        if read > 0 {
            self.spend();
        }
        Ok((read > 0).then_some(read))
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.read(&mut buf[filled..]).await? {
                Some(n) => filled += n,
                None => return Err(Error::Protocol("stream ended mid-header".into())),
            }
        }
        Ok(())
    }

    /// Refuses the rest of the payload.
    pub(crate) fn stop(&mut self, code: u64) {
        if let Some(io) = self.io.take() {
            S::stop(io, code);
        }
    }
}

impl<S: Stream> Drop for LocalRecv<S> {
    /// A parked connection that dies without ever carrying a copy still has
    /// to be replaced, or the pool shrinks silently. A reader that walks away
    /// mid-payload refuses the rest of it.
    fn drop(&mut self) {
        self.spend();
        if let Some(io) = self.io.take() {
            S::stop(io, codes::CANCELED);
        }
    }
}
