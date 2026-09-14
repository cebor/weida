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
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use weida_core::{Error, TraceContext};
use weida_protocol::{CreditHeader, FrameKind, SubscriptionHeader};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl, write_control};
use crate::listener::{PairOwner, Route};
use crate::pubsub::{FanOut, SubRegistry};
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
/// Either side of PAIR: one connection, one peer, both directions.
pub enum Pair {}
/// The asking side of SURVEY.
pub enum Survey {}
/// The answering side of SURVEY.
pub enum Respond {}
/// A member of a BUS.
pub enum Bus {}

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

impl sealed::Sealed for Pair {}
impl Pattern for Pair {
    type State = PairState;
}

impl sealed::Sealed for Survey {}
impl Pattern for Survey {
    type State = SurveyState;
}

impl sealed::Sealed for Respond {}
impl Pattern for Respond {
    type State = RespondState;
}

impl sealed::Sealed for Bus {}
impl Pattern for Bus {
    type State = BusState;
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
/// Either half of PAIR — the pattern is symmetric, so there is one role.
pub type Paired = Endpoint<Pair>;
/// One member of a BUS.
pub type BusMember = Endpoint<Bus>;

/// State of a requester.
pub struct ReqState {
    peer: Peer,
}
/// The asking half of SURVEY.
pub type Surveyor = Endpoint<Survey>;
/// The answering half of SURVEY.
pub type Respondent = Endpoint<Respond>;

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

    /// Opens a streamed publish: one stream per matched subscriber, written
    /// chunk by chunk.
    ///
    /// This is the fan-out for a payload that is not in memory and need not
    /// be — a video frame read from a capture device, a file, a response body
    /// being forwarded (B-064,
    /// [requirements/zeughaus-video.md](../../../docs/requirements/zeughaus-video.md)
    /// request 1). [`Publisher::publish`] refuses a payload above
    /// `Limits::subscriber_buffer_bytes` because such a message could not be
    /// enqueued for anybody; here that limit applies to a **chunk**, so the
    /// payload has no ceiling at all.
    ///
    /// The subscriber set is fixed at this call: a subscriber that arrives
    /// while the payload is in flight would receive a fragment with no way to
    /// know it, so it gets the next message instead. Zero subscribers is not
    /// an error, exactly as for [`Publisher::publish`].
    ///
    /// Every guarantee of [`Publisher::publish`] holds per subscriber rather
    /// than per publish: see [`FanOut`].
    pub fn open(&self, topic: &str) -> FanOut {
        self.state
            .registry
            .open(&self.state.path, topic, new_trace_context())
    }

    /// Like [`Publisher::open`], propagating an existing trace context.
    pub fn open_with_trace(&self, topic: &str, trace: TraceContext) -> FanOut {
        self.state.registry.open(&self.state.path, topic, trace)
    }

    /// Connections currently subscribed to this publisher.
    pub fn subscriber_count(&self) -> usize {
        self.state.registry.subscriber_count(&self.state.path)
    }

    /// Filters registered on this publisher, summed over subscribers.
    pub fn filter_count(&self) -> usize {
        self.state.registry.filter_count(&self.state.path)
    }

    /// Messages dropped because a subscriber could not take them, summed
    /// over topics and causes. "Something is being discarded"; for "which
    /// signal is starving", see [`Publisher::drops`].
    pub fn dropped(&self) -> u64 {
        self.state.registry.dropped(&self.state.path)
    }

    /// What was dropped on `topic`, by cause, or `None` if nothing was.
    ///
    /// Maintained as the drops happen, so this is a lookup and not a scan.
    /// The table holds at most `Limits::max_sequence_scopes` topics — the
    /// same ceiling the sequencer's per-topic table has — and a topic beyond
    /// it counts in [`Publisher::dropped`] only.
    pub fn dropped_on(&self, topic: &str) -> Option<crate::TopicDrops> {
        self.state.registry.dropped_on(&self.state.path, topic)
    }

    /// Every topic that lost a copy, with its counts by cause.
    pub fn drops(&self) -> Vec<crate::TopicDrops> {
        self.state.registry.drops(&self.state.path)
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
    ///
    /// On a transport where the accepting side cannot open a stream back,
    /// this is also where the reverse pool is filled: the subscriber parks
    /// `Limits::max_parked_reverse` connections and keeps replacing them as
    /// the publisher spends them
    /// ([0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4). A subscriber that parks nothing - because the limit is zero,
    /// or because the peer refused every attempt - cannot receive fan-out at
    /// all, and learns that here rather than by waiting forever.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let (conn, path) = self.state.peer.dial(url).await?;
        if conn.conn.needs_reverse_pool() && conn.conn.park_reverse().await? == 0 {
            return Err(Error::Unsupported);
        }
        conn.namespace
            .register(&path, Route::Transfer(self.state.queue_tx.clone()))?;
        if conn.conn.needs_reverse_pool() {
            let maintaining = Arc::clone(&conn);
            conn.exec
                .spawn(async move { maintaining.conn.maintain_reverse().await });
        }

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

    /// Registers interest in every topic `filter` matches.
    ///
    /// The filter is a **segmented pattern** ([`PROTOCOL.md`] §6.4): segments
    /// are split on `.`, `*` matches exactly one whole segment, a trailing
    /// `#` matches zero or more segments and must be the last one, and every
    /// other byte is literal — no escape character, no normalization, no case
    /// folding. `""` receives everything, and so does `#`. A published topic
    /// is never a pattern: `*` in a topic is an ordinary byte.
    ///
    /// So `sensors.*.temp` selects one segment in the middle, `sensors.#`
    /// selects `sensors` and everything under it, and `sensors.temp` does
    /// **not** select `sensors.temperature`.
    ///
    /// Subscribing twice to the same filter is idempotent. A filter that
    /// violates the grammar fails here with [`Error::Protocol`] rather than
    /// travelling to the publisher, which would answer it by closing the
    /// connection.
    ///
    /// [`PROTOCOL.md`]: https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/PROTOCOL.md
    pub async fn subscribe(&self, filter: &str) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
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

    /// States how many messages this subscriber will accept in total on the
    /// subscription `filter`, and sends it to every connected peer.
    ///
    /// This is the L2 credit of
    /// [0003](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
    /// §4.2, and it matters only where a **queue** serves the path: a
    /// publisher fans a message out to every matching subscriber and consults
    /// no credit, while a queue delivers to one consumer and delivers nothing
    /// at all until that consumer has granted some.
    ///
    /// **The limit is absolute and cumulative**, counted from the
    /// subscription's creation — not a delta and not a window — so a lost
    /// grant costs nothing and a duplicated one changes nothing. A broker
    /// keeps the **highest** limit it has seen for a subscription, which is
    /// what makes a reordered grant harmless: every control frame rides its
    /// own unidirectional stream, and QUIC does not order those against each
    /// other.
    ///
    /// Two consequences worth stating, because they are the whole usage
    /// pattern:
    ///
    /// - **An unmentioned subscription has a limit of zero**: a consumer that
    ///   subscribes and grants nothing receives nothing. That is the only
    ///   default that cannot surprise a consumer with a flood.
    /// - **To pause, restate what you have already received.** `grant(f, n)`
    ///   where `n` is the number of deliveries taken so far stops further
    ///   delivery without resetting a stream or closing the connection; a
    ///   later, larger grant resumes it. Granting *less* than that is ignored
    ///   by the broker, which is the same statement as "monotone" above.
    pub async fn grant(&self, filter: &str, limit: u64) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
        let mut targets = Vec::new();
        self.state.peer.for_each_live(|conn, path| {
            targets.push((ConnHandle::clone(conn), path.to_owned()));
        });
        for (conn, path) in targets {
            let header = CreditHeader::new(&path, filter, limit).encode();
            write_control(&conn.conn, FrameKind::Credit, &header).await?;
        }
        Ok(())
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

// --- PAIR -----------------------------------------------------------------

/// State of a paired endpoint.
///
/// One type for both sides, because PAIR is symmetric: both send and receive
/// with the same calls. What differs is only *how each side reaches its
/// peer*, and that is the whole asymmetry of a bound endpoint in this
/// library — a dialling side holds a [`Peer`], a bound side learns its peer
/// when that peer first speaks, which is what [`PairOwner`] records.
pub struct PairState {
    /// The dialling side's peer. `None` on a bound pair.
    peer: Option<Peer>,
    /// The bound side's peer, once it has claimed the endpoint. `None` on a
    /// dialling pair.
    owner: Option<Arc<PairOwner>>,
    /// The bound path; empty on a dialling pair, whose path is the one it
    /// dialled and is already recorded in its [`Peer`].
    path: Arc<str>,
    /// Where inbound transfers are delivered. The dialling side registers
    /// this in the connection's namespace; the bound side's copy lives in the
    /// listener's route instead, so it is `None` there.
    queue_tx: Option<mpsc::Sender<IncomingTransfer>>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

impl PairState {
    pub(crate) fn bound(
        path: &str,
        owner: Arc<PairOwner>,
        queue: mpsc::Receiver<IncomingTransfer>,
    ) -> PairState {
        PairState {
            peer: None,
            owner: Some(owner),
            path: Arc::from(path),
            queue_tx: None,
            queue: Mutex::new(queue),
        }
    }

    pub(crate) fn dialling(
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        depth: usize,
    ) -> PairState {
        let (queue_tx, queue) = mpsc::channel(depth);
        PairState {
            peer: Some(Peer::new(runtime, tls)),
            owner: None,
            path: Arc::from(""),
            queue_tx: Some(queue_tx),
            queue: Mutex::new(queue),
        }
    }
}

impl Paired {
    /// The endpoint path this pair uses; empty on a dialling pair that has
    /// not connected yet.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Connects to `weida://host:port/path`, once.
    ///
    /// A pair has **one** peer, so a second call is refused with
    /// [`Error::LimitExceeded`] rather than quietly becoming a Push: two
    /// peers on one endpoint would need a selection policy, and PAIR is the
    /// pattern that has none. On a bound pair there is nothing to dial and
    /// this is [`Error::Unsupported`].
    ///
    /// Like a subscriber, a dialling pair registers the path in the *client*
    /// connection's namespace: the peer's transfers arrive as ordinary
    /// inbound one-way transfers, so the dialling side needs a route for
    /// them — and on a transport where the accepting side cannot open a
    /// stream back, the reverse pool is filled here
    /// ([0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4).
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let Some(peer) = self.state.peer.as_ref() else {
            return Err(Error::Unsupported);
        };
        if peer.peer_count() > 0 {
            return Err(Error::LimitExceeded);
        }
        let (conn, path) = peer.dial(url).await?;
        if conn.conn.needs_reverse_pool() && conn.conn.park_reverse().await? == 0 {
            return Err(Error::Unsupported);
        }
        let queue = self
            .state
            .queue_tx
            .clone()
            .expect("a dialling pair has a queue");
        conn.namespace.register(
            &path,
            Route::Pair {
                queue,
                owner: Arc::new(PairOwner::new()),
            },
        )?;
        if conn.conn.needs_reverse_pool() {
            let maintaining = ConnHandle::clone(&conn);
            conn.exec
                .spawn(async move { maintaining.conn.maintain_reverse().await });
        }
        Ok(())
    }

    /// Number of connected peers: `0` or `1`.
    pub fn peer_count(&self) -> usize {
        self.state.peer.as_ref().map_or(0, Peer::peer_count)
    }

    /// Opens one outgoing transfer to the peer.
    ///
    /// The two sides reach their peer differently and that is the pattern's
    /// only asymmetry. A dialling pair sends on the connection it opened. A
    /// **bound** pair sends on the connection its peer dialled — and it can
    /// only know which one that is once the peer has spoken, so the first
    /// send on a bound pair **waits for the peer to appear**. That is not a
    /// queue: nothing is buffered, and a bound pair with no peer has nobody
    /// to address rather than a backlog to flush.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        if let Some(peer) = self.state.peer.as_ref() {
            return peer.open(meta).await;
        }
        let owner = self
            .state
            .owner
            .as_ref()
            .expect("a pair is either dialling or bound");
        let conn = owner.peer().await?;
        let (header, trace, cursors) =
            crate::transfer::outgoing_header(&conn, Some(&self.state.path), &meta, None)?;
        let mut stream = conn.open_uni().await?;
        crate::transfer::write_data_preamble(&mut stream, &header).await?;
        Ok(OutgoingTransfer::new(stream, trace, conn, cursors))
    }

    /// Sends `body` as one transfer and returns once the FIN is queued.
    pub async fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`Paired::send`], with explicit metadata.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        Ok(())
    }

    /// Waits for the next transfer from the peer.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- SURVEY ---------------------------------------------------------------

/// State of a surveyor.
pub struct SurveyState {
    peer: Peer,
}

impl SurveyState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> SurveyState {
        SurveyState {
            peer: Peer::new(runtime, tls),
        }
    }
}

/// State of a respondent.
pub struct RespondState {
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingRequest>>,
}

impl RespondState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingRequest>) -> RespondState {
        RespondState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Surveyor {
    /// Connects to `weida://host:port/path`.
    ///
    /// A surveyor addresses **every** connected respondent, so unlike a
    /// requester it accumulates peers on purpose: each `connect` adds one to
    /// the set a survey fans out over.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Number of connected respondents.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// Asks every connected respondent, and collects answers until
    /// `deadline`.
    ///
    /// One exchange per respondent, opened on the connection that respondent
    /// is on — not through the round-robin selection Req/Rep uses, which
    /// would ask exactly one. The deadline is the **caller's** and is not
    /// negotiated: nothing on the wire carries it, and a respondent never
    /// learns it.
    ///
    /// A survey with no respondents is not an error: it returns a
    /// [`SurveyRun`] whose first [`SurveyRun::next`] is `None`, because
    /// "nobody answered" is an answer.
    pub async fn survey(&self, body: &[u8], deadline: Duration) -> Result<SurveyRun, Error> {
        self.survey_with(TransferMeta::default(), body, deadline)
            .await
    }

    /// Like [`Surveyor::survey`], with explicit metadata.
    pub async fn survey_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
        deadline: Duration,
    ) -> Result<SurveyRun, Error> {
        let mut targets = Vec::new();
        self.state.peer.for_each_live(|conn, path| {
            targets.push((ConnHandle::clone(conn), Arc::<str>::from(path)));
        });

        let exec = self.state.peer.exec().clone();
        let late = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let expires = Instant::now() + deadline;
        // One slot per respondent: every answer has somewhere to go, so a
        // reply is never late merely because the caller is slow to read.
        let (tx, rx) = mpsc::channel(targets.len().max(1));
        let mut respondents = 0usize;

        for (conn, path) in targets {
            let (mut request, reply) =
                match crate::stream::open_exchange_on(&conn, &path, meta.clone()).await {
                    Ok(halves) => halves,
                    // A respondent that cannot even be asked is not an answer
                    // and not a failure of the survey: the others are still
                    // being asked.
                    Err(e) => {
                        tracing::debug!(error = %e, %path, "a respondent could not be asked");
                        continue;
                    }
                };
            if let Err(e) = request.write_all(body).await {
                tracing::debug!(error = %e, %path, "a survey question was not delivered");
                continue;
            }
            if let Err(e) = request.finish() {
                tracing::debug!(error = %e, %path, "a survey question was not finished");
                continue;
            }
            respondents += 1;
            let tx = tx.clone();
            let late = Arc::clone(&late);
            exec.spawn(async move {
                let answer = reply.recv().await;
                // Counted rather than delivered, and counted **here** so the
                // number means "after the deadline" whatever the caller does
                // with its handle (`docs/GUARANTEES.md` §6).
                if Instant::now() >= expires || tx.send(answer).await.is_err() {
                    late.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
        drop(tx);

        Ok(SurveyRun {
            rx,
            exec,
            expires,
            late,
            respondents,
            done: false,
        })
    }
}

/// One survey in progress: the answers, and what the deadline cost.
pub struct SurveyRun {
    rx: mpsc::Receiver<Result<IncomingTransfer, Error>>,
    exec: crate::runtime::Exec,
    expires: Instant,
    late: Arc<std::sync::atomic::AtomicU64>,
    respondents: usize,
    done: bool,
}

impl SurveyRun {
    /// The next answer, or `None` once the deadline has passed or every
    /// respondent has answered.
    ///
    /// `max_bytes` caps the reply this call reads; nothing is buffered before
    /// the caller states a cap. A respondent that refused or died is one
    /// `Some(Err(_))` among the answers and ends nothing: the other
    /// respondents are independent exchanges.
    pub async fn next(&mut self, max_bytes: usize) -> Option<Result<Vec<u8>, Error>> {
        if self.done {
            return None;
        }
        let remaining = self.expires.saturating_duration_since(Instant::now());
        match self.exec.within(remaining, self.rx.recv()).await {
            // The deadline. Answers still in flight are counted, not
            // delivered.
            None => {
                self.done = true;
                None
            }
            // Every respondent has answered or failed.
            Some(None) => {
                self.done = true;
                None
            }
            Some(Some(Ok(transfer))) => Some(transfer.collect(max_bytes).await),
            Some(Some(Err(e))) => Some(Err(e)),
        }
    }

    /// Answers that arrived after the deadline: dropped, and counted.
    pub fn late(&self) -> u64 {
        self.late.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many respondents were asked.
    pub fn respondents(&self) -> usize {
        self.respondents
    }
}

impl std::fmt::Debug for SurveyRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurveyRun")
            .field("respondents", &self.respondents)
            .field("late", &self.late())
            .finish_non_exhaustive()
    }
}

impl Respondent {
    /// The endpoint path this respondent serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next survey question.
    ///
    /// A question is an exchange, so this is a [`Replier::accept`] in every
    /// respect — the respondent needs no route of its own and no new wire
    /// vocabulary. What differs is only that the asker fans out and holds a
    /// deadline.
    pub async fn accept(&self) -> Result<IncomingRequest, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- BUS ------------------------------------------------------------------

/// State of a bus member.
///
/// The one role in this library that is **bound and dialling at once**: a
/// member accepts on its own path and dials every other member. That is why
/// its factory takes both a path and dialling terms, and why nothing else
/// does.
pub struct BusState {
    peer: Peer,
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
    /// One writer per joined member, so a member that stops reading stalls
    /// **its own** queue rather than the sender's task. This is Pub/Sub's
    /// shape for the same reason: a fan-out that can be blocked by one slow
    /// peer is not a fan-out (`docs/GUARANTEES.md` §6).
    writers: std::sync::Mutex<Vec<BusWriter>>,
    /// Copies that never reached a member, by the same rule Pub/Sub uses:
    /// best effort per peer, counted rather than retried.
    dropped: Arc<std::sync::atomic::AtomicU64>,
    /// Messages one member's writer may hold.
    depth: usize,
}

/// One joined member's writer queue.
struct BusWriter {
    tx: mpsc::Sender<BusMsg>,
}

/// One message on its way to one member.
struct BusMsg {
    meta: TransferMeta,
    body: Vec<u8>,
}

impl BusState {
    pub(crate) fn new(
        path: &str,
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        queue: mpsc::Receiver<IncomingTransfer>,
        depth: usize,
    ) -> BusState {
        BusState {
            peer: Peer::new(runtime, tls),
            path: Arc::from(path),
            queue: Mutex::new(queue),
            writers: std::sync::Mutex::new(Vec::new()),
            dropped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            depth,
        }
    }
}

impl BusMember {
    /// The path this member accepts on.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Joins another member.
    ///
    /// Joining is an ordinary connect and leaving an ordinary disconnect:
    /// there is no membership protocol, no election and nothing to
    /// synchronize, which is what keeps a bus a fan-out rather than a group.
    ///
    /// One writer task per member is started here, which is what makes a
    /// slow member cost its own copies rather than the sender's time.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let (conn, path) = self.state.peer.dial(url).await?;
        let (tx, rx) = mpsc::channel(self.state.depth);
        conn.exec.spawn(bus_writer(
            conn.clone(),
            path,
            rx,
            Arc::clone(&self.state.dropped),
        ));
        self.state
            .writers
            .lock()
            .expect("bus writers poisoned")
            .push(BusWriter { tx });
        Ok(())
    }

    /// Number of members this one has joined.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// Sends `body` to every **other** member, returning how many it reached.
    ///
    /// A message never reaches the sender: structurally, because `send`
    /// writes to the peers this member dialled and a member does not dial
    /// itself. There is also **no relay** — a bus of *n* members is
    /// *n* × (*n* − 1) deliveries, and weida forwards on nobody's behalf,
    /// which is the trade nanomsg's BUS makes too.
    ///
    /// Fan-out is best effort per peer with the same counted drops as Pub/Sub
    /// ([`BusMember::dropped`]): one member that cannot take a copy costs
    /// that copy, not the send.
    pub async fn send(&self, body: &[u8]) -> Result<usize, Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`BusMember::send`], with explicit metadata.
    ///
    /// "Reached" means accepted for delivery to that member, which is what
    /// [`Publisher::publish`] counts too: the bytes are on their way, and a
    /// member whose writer queue is full loses **this** copy and is counted
    /// in [`BusMember::dropped`].
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<usize, Error> {
        let mut reached = 0usize;
        let mut dropped = 0u64;
        {
            let mut writers = self.state.writers.lock().expect("bus writers poisoned");
            writers.retain(|writer| !writer.tx.is_closed());
            for writer in writers.iter() {
                let msg = BusMsg {
                    meta: meta.clone(),
                    body: body.to_vec(),
                };
                match writer.tx.try_send(msg) {
                    Ok(()) => reached += 1,
                    Err(_) => dropped += 1,
                }
            }
        }
        if dropped > 0 {
            tracing::debug!(dropped, "a bus member could not take a copy");
            self.state
                .dropped
                .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(reached)
    }

    /// Waits for the next message from another member.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// Copies that never reached a member.
    pub fn dropped(&self) -> u64 {
        self.state
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Serializes one member's copies, so a member that stops reading stalls only
/// its own queue.
///
/// A copy that fails on the wire is **counted here**, not silently lost: the
/// writer ends — which is what makes `send_with` prune this member on its
/// next call — and the copy it was carrying shows up in
/// [`BusMember::dropped`], exactly as Pub/Sub's writer counts a copy no
/// parked connection could take.
async fn bus_writer(
    conn: ConnHandle,
    path: Arc<str>,
    mut rx: mpsc::Receiver<BusMsg>,
    dropped: Arc<std::sync::atomic::AtomicU64>,
) {
    while let Some(msg) = rx.recv().await {
        if let Err(e) = send_one(&conn, &path, msg.meta, &msg.body).await {
            tracing::debug!(error = %e, %path, "a bus write failed; copy dropped");
            // This copy plus whatever was still queued for the member: a
            // member that died is dropped from the set without affecting the
            // others, and what it owed is counted rather than forgotten.
            dropped.fetch_add(1 + rx.len() as u64, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    }
}

/// Writes one whole one-way transfer on `conn`.
async fn send_one(
    conn: &ConnHandle,
    path: &str,
    meta: TransferMeta,
    body: &[u8],
) -> Result<(), Error> {
    let (header, trace, cursors) = crate::transfer::outgoing_header(conn, Some(path), &meta, None)?;
    let mut stream = conn.open_uni().await?;
    crate::transfer::write_data_preamble(&mut stream, &header).await?;
    let mut transfer = OutgoingTransfer::new(stream, trace, ConnHandle::clone(conn), cursors);
    transfer.write_all(body).await?;
    transfer.finish()?;
    Ok(())
}
