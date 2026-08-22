//! Typed endpoints: one `Endpoint<P>` per messaging pattern.
//!
//! The pattern is a type parameter rather than a runtime mode flag, so a
//! requester cannot be asked to accept and a replier cannot be asked to dial
//! (master doc §3). The trait is sealed: patterns are part of the protocol.
//!
//! The four patterns decompose into the same primitives (`docs/ARCHITECTURE.md`
//! §Pattern taxonomy): a one-way transfer (P1), a correlation table (P2), a
//! peer set with a selection policy (P3) and a bounded inbound queue behind an
//! opaque path (P4). Req adds P2 to P1; Push is P1 alone; Pub replaces
//! round-robin selection with fan-out; Pull and Sub are P4 verbatim.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use weida_core::{EndpointAddr, Error, Outcome, Role, TraceContext};
use weida_protocol::{FrameKind, SubscriptionHeader};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl, write_control};
use crate::listener::Route;
use crate::pubsub::SubRegistry;
use crate::runtime::RuntimeInner;
use crate::transfer::{
    IncomingRequest, IncomingTransfer, OutgoingTransfer, PendingReply, TransferMeta, data_header,
    new_trace_context, write_data_preamble,
};

mod sealed {
    pub trait Sealed {}
}

/// A messaging pattern.
pub trait Pattern: sealed::Sealed {
    /// Pattern-specific endpoint state.
    type State;
}

/// The requesting side of Req/Rep.
pub enum Req {}
/// The replying side of Req/Rep.
pub enum Rep {}
/// The sending side of Push/Pull.
pub enum Push {}
/// The receiving side of Push/Pull.
pub enum Pull {}
/// The publishing side of Pub/Sub.
pub enum Pub {}
/// The subscribing side of Pub/Sub.
pub enum Sub {}

impl sealed::Sealed for Req {}
impl Pattern for Req {
    type State = ReqState;
}

impl sealed::Sealed for Rep {}
impl Pattern for Rep {
    type State = RepState;
}

impl sealed::Sealed for Push {}
impl Pattern for Push {
    type State = PushState;
}

impl sealed::Sealed for Pull {}
impl Pattern for Pull {
    type State = PullState;
}

impl sealed::Sealed for Pub {}
impl Pattern for Pub {
    type State = PubState;
}

impl sealed::Sealed for Sub {}
impl Pattern for Sub {
    type State = SubState;
}

/// A typed messaging endpoint.
pub struct Endpoint<P: Pattern> {
    state: P::State,
}

impl<P: Pattern> Endpoint<P> {
    pub(crate) fn from_state(state: P::State) -> Endpoint<P> {
        Endpoint { state }
    }
}

/// The requesting half of Req/Rep.
pub type Requester = Endpoint<Req>;
/// The replying half of Req/Rep.
pub type Replier = Endpoint<Rep>;
/// The sending half of Push/Pull.
pub type Pusher = Endpoint<Push>;
/// The receiving half of Push/Pull.
pub type Puller = Endpoint<Pull>;
/// The publishing half of Pub/Sub.
pub type Publisher = Endpoint<Pub>;
/// The subscribing half of Pub/Sub.
pub type Subscriber = Endpoint<Sub>;

/// One connected peer: the connection and the path that was dialled on it.
struct Peer {
    conn: ConnHandle,
    path: Arc<str>,
}

/// A dialling endpoint's peers plus its selection policy (primitive P3).
///
/// Peers accumulate: connecting again adds a peer rather than replacing one
/// (master doc §5). Shared by Req, Push and Sub — the policy differs (Req and
/// Push pick one, Sub addresses all of them), the set does not.
pub(crate) struct PeerSet {
    peers: std::sync::Mutex<Vec<Peer>>,
    cursor: AtomicUsize,
}

impl PeerSet {
    fn new() -> PeerSet {
        PeerSet {
            peers: std::sync::Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }

    fn add(&self, conn: ConnHandle, path: &str) {
        self.peers.lock().expect("peer list poisoned").push(Peer {
            conn,
            path: Arc::from(path),
        });
    }

    fn len(&self) -> usize {
        self.peers.lock().expect("peer list poisoned").len()
    }

    /// Picks the next live peer, round-robin.
    fn pick(&self) -> Result<(ConnHandle, Arc<str>), Error> {
        let peers = self.peers.lock().expect("peer list poisoned");
        if peers.is_empty() {
            return Err(Error::NotConnected);
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..peers.len() {
            let peer = &peers[(start + offset) % peers.len()];
            if peer.conn.conn.close_reason().is_none() {
                return Ok((Arc::clone(&peer.conn), Arc::clone(&peer.path)));
            }
        }
        Err(Error::ConnectionLost)
    }

    /// Runs `f` for every peer whose connection is still open.
    fn for_each_live(&self, mut f: impl FnMut(&ConnHandle, &str)) {
        let peers = self.peers.lock().expect("peer list poisoned");
        for peer in peers.iter() {
            if peer.conn.conn.close_reason().is_none() {
                f(&peer.conn, &peer.path);
            }
        }
    }
}

/// State of a requester.
pub struct ReqState {
    runtime: Arc<RuntimeInner>,
    tls: Arc<ClientTls>,
    peers: PeerSet,
}

impl ReqState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> ReqState {
        ReqState {
            runtime,
            tls,
            peers: PeerSet::new(),
        }
    }
}

impl Requester {
    /// Connects to `weida://host:port/path`.
    ///
    /// Connections are pooled per `host:port`, so several endpoints addressing
    /// the same peer share one QUIC connection. Returns once the HELLO
    /// exchange has been negotiated, so the first request cannot race
    /// negotiation.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let addr = EndpointAddr::parse(url)?;
        let conn = self
            .state
            .runtime
            .connect(&addr.host, addr.port, &self.state.tls)
            .await?;
        self.state.peers.add(conn, &addr.path);
        Ok(())
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peers.len()
    }

    /// Opens a request stream and the correlated reply slot.
    ///
    /// The transfer is registered with the connection actor *before* the stream
    /// is opened, so an ACK or a reply can never arrive before there is
    /// somewhere to put it.
    pub async fn open(
        &self,
        meta: TransferMeta,
    ) -> Result<(OutgoingTransfer, PendingReply), Error> {
        let (conn, path) = self.state.peers.pick()?;
        let reserved = conn.register(meta.ack_mode, true).await?;
        let (header, trace) =
            data_header(Role::Request, Some(&path), reserved.id, None, &meta, None);

        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;

        let reply = PendingReply::new(
            Arc::clone(&conn),
            reserved.id,
            reserved.reply.expect("registered with a reply slot"),
        );
        let transfer = OutgoingTransfer::new(conn, stream, reserved.id, trace, reserved.outcome);
        Ok((transfer, reply))
    }

    /// Sends `body` as one request and returns the reply stream.
    ///
    /// Convenience over [`Requester::open`] for small payloads; it never
    /// materializes the reply.
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error> {
        self.request_with(TransferMeta::default(), body).await
    }

    /// Like [`Requester::request`], with explicit metadata.
    pub async fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<IncomingTransfer, Error> {
        let (mut transfer, pending) = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish().await?;
        pending.recv().await
    }
}

/// State of a replier.
pub struct RepState {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`; contention is
    /// between deliberate concurrent acceptors only.
    queue: Mutex<mpsc::Receiver<IncomingRequest>>,
}

impl RepState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingRequest>) -> RepState {
        RepState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Replier {
    /// The endpoint path this replier serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next request.
    ///
    /// Requests queue up to `Limits::endpoint_queue`; beyond that the sending
    /// side blocks, which propagates backpressure to the peer through QUIC flow
    /// control rather than growing an unbounded queue (master doc §50).
    pub async fn accept(&self) -> Result<IncomingRequest, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- Push/Pull ------------------------------------------------------------

/// State of a pusher.
pub struct PushState {
    runtime: Arc<RuntimeInner>,
    tls: Arc<ClientTls>,
    peers: PeerSet,
}

impl PushState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> PushState {
        PushState {
            runtime,
            tls,
            peers: PeerSet::new(),
        }
    }
}

impl Pusher {
    /// Connects to `weida://host:port/path`.
    ///
    /// Like a requester, a pusher accumulates peers and pools connections per
    /// `host:port`.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let addr = EndpointAddr::parse(url)?;
        let conn = self
            .state
            .runtime
            .connect(&addr.host, addr.port, &self.state.tls)
            .await?;
        self.state.peers.add(conn, &addr.path);
        Ok(())
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peers.len()
    }

    /// Opens a fire-and-forget transfer to the next peer, round-robin.
    ///
    /// There is no reply slot, but the transfer is still registered: with
    /// `AckMode::Accepted` the peer owes an acknowledgement, and
    /// [`OutgoingTransfer::finish`] resolves to `Acked(Accepted)` rather than
    /// `SentBestEffort`. Acknowledgement modes are orthogonal to the pattern
    /// (master doc §16), so this needs no reliability code of its own.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        let (conn, path) = self.state.peers.pick()?;
        let reserved = conn.register(meta.ack_mode, false).await?;
        let (header, trace) =
            data_header(Role::Oneshot, Some(&path), reserved.id, None, &meta, None);

        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;
        Ok(OutgoingTransfer::new(
            conn,
            stream,
            reserved.id,
            trace,
            reserved.outcome,
        ))
    }

    /// Sends `body` as one transfer and waits for its outcome.
    pub async fn send(&self, body: &[u8]) -> Result<Outcome, Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`Pusher::send`], with explicit metadata.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<Outcome, Error> {
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish().await
    }
}

/// State of a puller.
pub struct PullState {
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

impl PullState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingTransfer>) -> PullState {
        PullState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Puller {
    /// The endpoint path this puller serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next transfer.
    ///
    /// `recv`, not `accept`: a pulled transfer is a payload with metadata, not
    /// a request that owes a reply. Backpressure works exactly as it does for a
    /// replier — the queue is bounded by `Limits::endpoint_queue` and a full
    /// queue stalls the sender through QUIC flow control.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- Pub/Sub --------------------------------------------------------------

/// State of a publisher.
pub struct PubState {
    path: Arc<str>,
    registry: Arc<SubRegistry>,
    max_payload: usize,
}

impl PubState {
    pub(crate) fn new(path: &str, registry: Arc<SubRegistry>, max_payload: usize) -> PubState {
        PubState {
            path: Arc::from(path),
            registry,
            max_payload,
        }
    }
}

impl Publisher {
    /// The endpoint path this publisher serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Fans `payload` out to every subscriber whose filter is a prefix of
    /// `topic`, and returns how many it was enqueued to.
    ///
    /// Synchronous and non-blocking: publishing never waits on a subscriber.
    /// A subscriber whose byte budget is exhausted loses this message, which is
    /// counted in [`Publisher::dropped`]. Zero subscribers is not an error —
    /// a publisher with no audience is the normal startup state.
    ///
    /// Fails with [`Error::LimitExceeded`] when `payload` is larger than
    /// `Limits::subscriber_buffer_bytes`: such a message could never be
    /// enqueued for anyone, so reporting it beats silently dropping it for
    /// every subscriber.
    pub fn publish(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error> {
        let payload = payload.into();
        if payload.len() > self.state.max_payload {
            return Err(Error::LimitExceeded);
        }
        let want = u32::try_from(payload.len()).map_err(|_| Error::LimitExceeded)?;
        Ok(self
            .state
            .registry
            .publish(&self.state.path, topic, payload, new_trace_context(), want))
    }

    /// Like [`Publisher::publish`], propagating an existing trace context.
    pub fn publish_with_trace(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        trace: TraceContext,
    ) -> Result<usize, Error> {
        let payload = payload.into();
        if payload.len() > self.state.max_payload {
            return Err(Error::LimitExceeded);
        }
        let want = u32::try_from(payload.len()).map_err(|_| Error::LimitExceeded)?;
        Ok(self
            .state
            .registry
            .publish(&self.state.path, topic, payload, trace, want))
    }

    /// Connections currently subscribed to this publisher.
    pub fn subscriber_count(&self) -> usize {
        self.state.registry.subscriber_count(&self.state.path)
    }

    /// Filters registered on this publisher, summed over subscribers.
    pub fn filter_count(&self) -> usize {
        self.state.registry.filter_count(&self.state.path)
    }

    /// Messages dropped because a subscriber could not take them.
    pub fn dropped(&self) -> u64 {
        self.state.registry.dropped(&self.state.path)
    }
}

/// State of a subscriber.
pub struct SubState {
    runtime: Arc<RuntimeInner>,
    tls: Arc<ClientTls>,
    peers: PeerSet,
    /// Filters this subscriber wants, remembered so a peer connected later
    /// receives the same subscriptions.
    filters: std::sync::Mutex<HashSet<String>>,
    queue_tx: mpsc::Sender<IncomingTransfer>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

impl SubState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>, depth: usize) -> SubState {
        let (queue_tx, queue) = mpsc::channel(depth);
        SubState {
            runtime,
            tls,
            peers: PeerSet::new(),
            filters: std::sync::Mutex::new(HashSet::new()),
            queue_tx,
            queue: Mutex::new(queue),
        }
    }
}

impl Subscriber {
    /// Connects to `weida://host:port/path` and subscribes to every filter
    /// registered so far.
    ///
    /// The subscriber registers `path` in the *client* connection's namespace:
    /// fanned-out copies arrive as ordinary inbound oneshot transfers, so the
    /// dialling side needs a route for them. Two subscribers sharing a pooled
    /// connection and claiming the same path therefore collide with
    /// [`Error::AlreadyRegistered`]; fanning one subscription out to several
    /// in-process consumers is the application's business, not the transport's.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let addr = EndpointAddr::parse(url)?;
        let conn = self
            .state
            .runtime
            .connect(&addr.host, addr.port, &self.state.tls)
            .await?;
        conn.namespace
            .register(&addr.path, Route::Transfer(self.state.queue_tx.clone()))?;
        self.state.peers.add(ConnHandle::clone(&conn), &addr.path);

        let filters: Vec<String> = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .iter()
            .cloned()
            .collect();
        for filter in filters {
            send_subscription(&conn, FrameKind::Subscribe, &addr.path, &filter).await?;
        }
        Ok(())
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peers.len()
    }

    /// Registers interest in every topic starting with `filter`.
    ///
    /// The filter is a byte prefix, not a pattern: `""` receives everything and
    /// no character is special. Subscribing twice to the same filter is
    /// idempotent.
    pub async fn subscribe(&self, filter: &str) -> Result<(), Error> {
        let fresh = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .insert(filter.to_owned());
        if !fresh {
            return Ok(());
        }
        self.broadcast(FrameKind::Subscribe, filter).await
    }

    /// Withdraws a filter. Unknown filters are ignored.
    pub async fn unsubscribe(&self, filter: &str) -> Result<(), Error> {
        let known = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .remove(filter);
        if !known {
            return Ok(());
        }
        self.broadcast(FrameKind::Unsubscribe, filter).await
    }

    /// Filters currently registered.
    pub fn filter_count(&self) -> usize {
        self.state
            .filters
            .lock()
            .expect("filter set poisoned")
            .len()
    }

    /// Waits for the next published message.
    ///
    /// The topic is on [`crate::IncomingMeta::topic`].
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    async fn broadcast(&self, kind: FrameKind, filter: &str) -> Result<(), Error> {
        // Collect first: the peer lock is a std mutex and must not be held
        // across an await.
        let mut targets = Vec::new();
        self.state.peers.for_each_live(|conn, path| {
            targets.push((ConnHandle::clone(conn), path.to_owned()));
        });
        for (conn, path) in targets {
            send_subscription(&conn, kind, &path, filter).await?;
        }
        Ok(())
    }
}

async fn send_subscription(
    conn: &ConnHandle,
    kind: FrameKind,
    path: &str,
    filter: &str,
) -> Result<(), Error> {
    let header = SubscriptionHeader::new(path, filter).encode();
    write_control(&conn.conn, kind, &header).await
}

impl Drop for SubState {
    fn drop(&mut self) {
        // Best effort: tell each peer we are gone and release the path so the
        // next subscriber on this pooled connection can claim it.
        //
        // The frames go through the connection actor with a non-blocking
        // `notify`, not `tokio::spawn`: a destructor may run on a thread with
        // no reactor, and panicking there aborts the process instead of
        // unwinding. The publisher drops the subscription anyway once the
        // connection closes, so a lost notification costs nothing.
        let filters: Vec<String> = self
            .filters
            .lock()
            .expect("filter set poisoned")
            .iter()
            .cloned()
            .collect();
        let mut targets = Vec::new();
        self.peers.for_each_live(|conn, path| {
            targets.push((ConnHandle::clone(conn), Arc::<str>::from(path)));
        });
        for (conn, path) in targets {
            conn.namespace.unregister(&path);
            for filter in &filters {
                conn.notify(Ctl::SendUnsubscribe {
                    path: Arc::clone(&path),
                    filter: filter.clone(),
                });
            }
        }
    }
}
