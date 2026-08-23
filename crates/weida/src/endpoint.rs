//! L1: typed endpoints, one `Endpoint<P>` per messaging pattern.
//!
//! The pattern is a type parameter rather than a runtime mode flag, so a
//! requester cannot be asked to accept and a replier cannot be asked to dial
//! (master doc §3). The trait is sealed: patterns are part of the protocol.
//!
//! Everything here is a thin wrapper over the stream core ([`crate::stream`]):
//! Req/Rep is one bidirectional stream per exchange, Push/Pull is a
//! unidirectional stream per message with round-robin peer selection, Pub/Sub
//! is the same stream with fan-out selection and publisher-side filtering. No
//! pattern adds a guarantee QUIC does not already provide; the vocabulary of
//! stored/replicated/processed belongs to a broker layer that does not exist
//! yet (`docs/GUARANTEES.md`).

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use weida_core::{Error, TraceContext};
use weida_protocol::{FrameKind, SubscriptionHeader};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl, write_control};
use crate::listener::Route;
use crate::pubsub::SubRegistry;
use crate::runtime::RuntimeInner;
use crate::stream::Peer;
use crate::transfer::{
    IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta,
    new_trace_context,
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

/// State of a requester.
pub struct ReqState {
    peer: Peer,
}

impl ReqState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> ReqState {
        ReqState {
            peer: Peer::new(runtime, tls),
        }
    }
}

impl Requester {
    /// Connects to `weida://host:port/path`.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// Opens one exchange: a request stream and its reply half.
    ///
    /// Both halves belong to the same bidirectional QUIC stream, so no
    /// correlation identifier is needed and none is sent.
    pub async fn open(&self, meta: TransferMeta) -> Result<(OutgoingTransfer, ReplyStream), Error> {
        self.state.peer.open_bi(meta).await
    }

    /// Sends `body` as one request and returns the reply stream.
    ///
    /// Convenience over [`Requester::open`] for small payloads; it never
    /// materializes the reply. The delivery receipt is discarded: a reply is a
    /// stronger answer than a transport acknowledgement, and waiting for both
    /// would only add a round trip.
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error> {
        self.request_with(TransferMeta::default(), body).await
    }

    /// Like [`Requester::request`], with explicit metadata.
    pub async fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<IncomingTransfer, Error> {
        let (mut transfer, reply) = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        reply.recv().await
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
    peer: Peer,
}

impl PushState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> PushState {
        PushState {
            peer: Peer::new(runtime, tls),
        }
    }
}

impl Pusher {
    /// Connects to `weida://host:port/path`.
    ///
    /// Like a requester, a pusher accumulates peers and pools connections per
    /// `host:port`.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// Opens a one-way transfer to the next peer, round-robin.
    ///
    /// Use this rather than [`Pusher::send`] when the transport receipt
    /// matters: [`OutgoingTransfer::finish`] hands back a
    /// [`crate::Delivery`] that resolves once the peer's transport holds every
    /// byte.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        self.state.peer.open(meta).await
    }

    /// Sends `body` as one transfer and returns once the FIN is queued.
    ///
    /// Pipeline semantics: the delivery receipt is discarded, so a push costs
    /// no round trip and reports only failures the local side already knows
    /// about. Callers who want the receipt use [`Pusher::open`] and await
    /// [`crate::Delivery::delivered`] themselves.
    pub async fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`Pusher::send`], with explicit metadata.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        Ok(())
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
        self.publish_inner(topic, payload.into(), new_trace_context())
    }

    /// Like [`Publisher::publish`], propagating an existing trace context.
    pub fn publish_with_trace(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        trace: TraceContext,
    ) -> Result<usize, Error> {
        self.publish_inner(topic, payload.into(), trace)
    }

    fn publish_inner(
        &self,
        topic: &str,
        payload: Bytes,
        trace: TraceContext,
    ) -> Result<usize, Error> {
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
    peer: Peer,
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
            peer: Peer::new(runtime, tls),
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
    /// fanned-out copies arrive as ordinary inbound one-way transfers, so the
    /// dialling side needs a route for them. Two subscribers sharing a pooled
    /// connection and claiming the same path therefore collide with
    /// [`Error::AlreadyRegistered`]; fanning one subscription out to several
    /// in-process consumers is the application's business, not the transport's.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let (conn, path) = self.state.peer.dial(url).await?;
        conn.namespace
            .register(&path, Route::Transfer(self.state.queue_tx.clone()))?;

        let filters: Vec<String> = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .iter()
            .cloned()
            .collect();
        for filter in filters {
            send_subscription(&conn, FrameKind::Subscribe, &path, &filter).await?;
        }
        Ok(())
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
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
        self.state.peer.for_each_live(|conn, path| {
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
        self.peer.for_each_live(|conn, path| {
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
