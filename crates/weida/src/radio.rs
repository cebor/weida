//! RADIO/DISH: lossy fan-out of segments
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
//!
//! A radio numbers segments per topic (DATA key `13`) and sends each to
//! every dish joined when it opens, one uni stream per dish. The segment
//! machinery — supersession, expiry, the budget and the write that never
//! waits — is [`crate::segment`]'s, shared with `Peer::segment`; a radio adds
//! the dish table, a dish's `max_age` and the datagram segments.
//!
//! The dish discards a segment older than the newest it delivered on the
//! topic and never blocks its connection on a full queue.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};
use weida_core::{Error, Limits, PeerIdentity};
use weida_protocol::header::limits::MAX_TOPIC_BYTES;
use weida_protocol::{FrameKind, codes, encode_varint, filter, split_flow_datagram};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl};
use crate::endpoint::{Dish, Radio, send_subscription};
use crate::flow::{Flow, FlowMeta, IncomingFlow, open_flow_on};
use crate::identity::PeerChain;
use crate::listener::{Namespace, Route};
use crate::pubsub::{DropCause, DropTable, TopicDrops};
use crate::reconnect::PeerEvents;
use crate::runtime::RuntimeInner;
use crate::segment::{CopyTarget, Segment, SegmentTerms, SegmentTopics, open_segment};
use crate::stream::{Attach, Peer};
use crate::transfer::IncomingTransfer;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A dish's request to join a filter, as a radio's admission sees it
/// ([`Radio::with_admission`],
/// [decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.3).
#[derive(Debug)]
#[non_exhaustive]
pub struct Join<'a> {
    /// The key or principal the dish's connection proved; `None` when it is
    /// anonymous.
    pub peer: Option<&'a PeerIdentity>,
    /// The certificate chain behind `peer`
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.2).
    pub peer_chain: Option<&'a PeerChain>,
    /// The filter exactly as the dish sent it.
    pub filter: &'a str,
}

impl<'a> Join<'a> {
    fn of(conn: &'a ConnHandle, filter: &'a str) -> Join<'a> {
        Join {
            peer: conn.peer.as_ref(),
            peer_chain: conn.peer_chain.as_ref(),
            filter,
        }
    }
}

/// A radio's admission policy: `true` records the join.
type Admission = Arc<dyn Fn(&Join<'_>) -> bool + Send + Sync>;

// --- radio side ---------------------------------------------------------------

/// One joined dish: a connection and the filters it joined with.
struct DishEntry {
    conn: ConnHandle,
    conn_id: usize,
    /// Filter to `max_age`.
    filters: HashMap<String, Option<Duration>>,
    /// Chunk bytes this dish's copies may hold unwritten, over all topics.
    budget: Arc<Semaphore>,
    /// One datagram flow per topic, opened lazily on the connection the join
    /// arrived on; bounded by the topics the dish's filters match.
    flows: Arc<Mutex<HashMap<Arc<str>, FlowSlot>>>,
}

/// A dish's datagram flow for one topic.
enum FlowSlot {
    /// The FLOW header is on its way; holds the newest datagram meanwhile,
    /// never more than one.
    Opening(Option<Bytes>),
    Open(Flow),
}

impl DishEntry {
    /// The dish's latency budget for `topic`: the smallest `max_age` among
    /// the matching filters that carry one. `None` for no match at all.
    fn matching(&self, topic: &str) -> Option<Option<Duration>> {
        let mut matched = false;
        let mut max_age: Option<Duration> = None;
        for (f, age) in &self.filters {
            if filter::matches(topic, f) {
                matched = true;
                if let Some(age) = age {
                    max_age = Some(max_age.map_or(*age, |m| m.min(*age)));
                }
            }
        }
        matched.then_some(max_age)
    }

    /// Withdraws `filter` and releases its `max_subscriptions` slot; `false`
    /// when the dish did not hold it. A topic no remaining filter matches
    /// loses its flow: dropping it closes the FLOW stream with FIN.
    fn drop_filter(&mut self, withdrawn: &str) -> bool {
        if self.filters.remove(withdrawn).is_none() {
            return false;
        }
        if let Some(subs) = self.conn.subs.as_ref() {
            subs.release(self.conn_id);
        }
        let filters = &self.filters;
        lock(&self.flows).retain(|topic, _| filters.keys().any(|f| filter::matches(topic, f)));
        true
    }
}

/// Everything a radio path holds: its dishes, its topics and its drops.
pub(crate) struct RadioHub {
    path: Arc<str>,
    dishes: Mutex<Vec<DishEntry>>,
    /// Per-topic numbering and the copies the next segment supersedes.
    topics: SegmentTopics,
    drops: Arc<DropTable>,
    limits: Limits,
    /// The admission policy and its generation, which moves with every
    /// [`RadioHub::set_admission`] so a join screened under an older policy
    /// is screened again.
    admission: Mutex<(u64, Option<Admission>)>,
}

impl RadioHub {
    pub(crate) fn new(path: &str, limits: Limits) -> RadioHub {
        RadioHub {
            path: Arc::from(path),
            dishes: Mutex::new(Vec::new()),
            topics: SegmentTopics::new(limits.max_sequence_scopes),
            drops: Arc::new(DropTable::new(limits.max_sequence_scopes)),
            limits,
            admission: Mutex::new((0, None)),
        }
    }

    /// The dish table, with every entry whose connection closed removed.
    fn dishes(&self) -> MutexGuard<'_, Vec<DishEntry>> {
        let mut dishes = lock(&self.dishes);
        dishes.retain(|d| d.conn.conn.close_reason().is_none());
        dishes
    }

    /// Records a join. `reserve` counts a new filter against the
    /// connection's `max_subscriptions` and runs only for a filter the dish
    /// did not already hold; a repeated join updates its `max_age`.
    ///
    /// The admission, when there is one, decides first, every time: a
    /// refusal records nothing, reserves nothing and is silence
    /// ([decisions/0017](../../../docs/decisions/0017-subscription-verdict.md)
    /// §4.1). It runs outside the dish table's lock, so user code never runs
    /// under the hub's lock.
    pub(crate) fn join(
        &self,
        ctx: &ConnHandle,
        filter: String,
        max_age_ms: Option<u64>,
        reserve: impl FnOnce() -> Result<(), Error>,
    ) -> Result<(), Error> {
        let conn_id = ctx.conn.stable_id();
        let max_age = max_age_ms.map(Duration::from_millis);
        let mut dishes = loop {
            let (generation, admit) = {
                let admission = lock(&self.admission);
                (admission.0, admission.1.clone())
            };
            if let Some(admit) = admit
                && !admit(&Join::of(ctx, &filter))
            {
                tracing::debug!(path = %self.path, filter, "join refused by admission");
                return Ok(());
            }
            let dishes = self.dishes();
            // A policy installed meanwhile re-screens only what it finds
            // recorded; this join is not yet, so it is screened again.
            if lock(&self.admission).0 == generation {
                break dishes;
            }
        };
        let index = match dishes.iter().position(|d| d.conn_id == conn_id) {
            Some(index) => index,
            None => {
                dishes.push(DishEntry {
                    conn: ConnHandle::clone(ctx),
                    conn_id,
                    filters: HashMap::new(),
                    budget: Arc::new(Semaphore::new(self.limits.subscriber_buffer_bytes)),
                    flows: Arc::new(Mutex::new(HashMap::new())),
                });
                dishes.len() - 1
            }
        };
        let entry = &mut dishes[index];
        if let Some(age) = entry.filters.get_mut(&filter) {
            *age = max_age;
            return Ok(());
        }
        if let Err(e) = reserve() {
            if entry.filters.is_empty() {
                dishes.swap_remove(index);
            }
            return Err(e);
        }
        entry.filters.insert(filter, max_age);
        Ok(())
    }

    /// Withdraws a join the dish asked to leave, and releases its slot.
    pub(crate) fn leave(&self, conn_id: usize, filter: &str) {
        let mut dishes = self.dishes();
        let Some(index) = dishes.iter().position(|d| d.conn_id == conn_id) else {
            return;
        };
        dishes[index].drop_filter(filter);
        if dishes[index].filters.is_empty() {
            dishes.swap_remove(index);
        }
    }

    /// Withdraws `filter` from every connection `peer` joined it on; the
    /// number of joins withdrawn.
    pub(crate) fn evict(&self, peer: &PeerIdentity, filter: &str) -> usize {
        let mut dishes = self.dishes();
        let mut evicted = 0;
        for entry in dishes.iter_mut() {
            if entry.conn.peer.as_ref() == Some(peer) && entry.drop_filter(filter) {
                evicted += 1;
            }
        }
        dishes.retain(|d| !d.filters.is_empty());
        evicted
    }

    /// Installs `admit` and screens every join already recorded with it,
    /// withdrawing the ones it refuses. The policy runs outside the lock,
    /// on a snapshot; a join recorded meanwhile is screened in
    /// [`RadioHub::join`], because the generation moved.
    pub(crate) fn set_admission(&self, admit: Admission) {
        {
            let mut admission = lock(&self.admission);
            admission.0 += 1;
            admission.1 = Some(Arc::clone(&admit));
        }
        let recorded: Vec<(usize, ConnHandle, Vec<String>)> = self
            .dishes()
            .iter()
            .map(|d| {
                (
                    d.conn_id,
                    ConnHandle::clone(&d.conn),
                    d.filters.keys().cloned().collect(),
                )
            })
            .collect();
        let refused: Vec<(usize, String)> = recorded
            .iter()
            .flat_map(|(conn_id, conn, filters)| {
                filters
                    .iter()
                    .filter(|f| !admit(&Join::of(conn, f)))
                    .map(|f| (*conn_id, f.clone()))
            })
            .collect();
        if refused.is_empty() {
            return;
        }
        let mut dishes = self.dishes();
        for (conn_id, filter) in &refused {
            if let Some(entry) = dishes.iter_mut().find(|d| d.conn_id == *conn_id) {
                entry.drop_filter(filter);
            }
        }
        dishes.retain(|d| !d.filters.is_empty());
    }
}

/// State of a radio.
///
/// Clonable: a clone is another handle to the same radio, and the path is
/// released when the last one goes.
#[derive(Clone)]
pub struct RadioState {
    hub: Arc<RadioHub>,
    _claim: Arc<Claim>,
}

/// The radio's claim on its path, released with the last handle.
struct Claim {
    namespace: Arc<Namespace>,
    path: Arc<str>,
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.namespace.unregister(&self.path);
    }
}

impl RadioState {
    pub(crate) fn new(hub: Arc<RadioHub>, namespace: Arc<Namespace>) -> RadioState {
        let path = Arc::clone(&hub.path);
        RadioState {
            hub,
            _claim: Arc::new(Claim { namespace, path }),
        }
    }
}

impl Clone for Radio {
    fn clone(&self) -> Radio {
        Radio::from_state(self.state().clone())
    }
}

impl Radio {
    fn hub(&self) -> &RadioHub {
        &self.state().hub
    }

    /// Opens segment *n+1* on `topic` as one stream per joined dish, and
    /// supersedes segment *n* on that topic: a copy whose writer had not
    /// finished it, or whose queued chunks the transport does not take at
    /// once, is reset at once; a finished copy gets the time its path needs,
    /// `rtt + 50 ms + rtt * bytes / cwnd`, and is reset only if it is still
    /// unacknowledged then. On a local transport that grace is zero.
    ///
    /// `terms` set the sender's side
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.2): a copy expires at the smaller of `terms.max_age` and its
    /// dish's `max_age`, its stream carries `terms.priority` within its
    /// layer, and `terms.follows_upstream` keeps a copy against its
    /// successor unless a write has to wait.
    ///
    /// The dish set is fixed here: a dish that joins while the segment is in
    /// flight receives the next one. Zero dishes is not an error. Fails with
    /// [`Error::LimitExceeded`] for a topic above 256 bytes, or when
    /// `max_sequence_scopes` topics all have a copy in flight.
    pub fn segment(&self, topic: &str, terms: SegmentTerms) -> Result<Segment, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.topics.next(&topic)?;
        let targets = hub
            .dishes()
            .iter()
            .filter_map(|entry| {
                entry.matching(&topic).map(|max_age| CopyTarget {
                    conn: ConnHandle::clone(&entry.conn),
                    path: Arc::clone(&hub.path),
                    budget: Arc::clone(&entry.budget),
                    max_age,
                })
            })
            .collect();
        Ok(open_segment(
            &hub.topics,
            &hub.drops,
            topic,
            number,
            &terms,
            targets,
        ))
    }

    /// Sends a one-packet segment on `topic` to every joined dish, as a
    /// datagram on that dish's flow for the topic; returns how many dishes it
    /// was handed to.
    ///
    /// It takes the topic's next segment number and supersedes the stream
    /// copies still in flight there, exactly as [`Radio::segment`] does. The
    /// flow is opened lazily on the connection the join arrived on; while it
    /// opens, the newest datagram waits and an older one is dropped. A dish
    /// whose connection carries no datagrams, and a payload larger than a
    /// dish's connection carries, are counted drops with their causes —
    /// never turned into a stream segment, which would arrive late
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.6 rule 4).
    pub fn datagram(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.topics.next(&topic)?;
        let payload = payload.into();
        let mut body = Vec::with_capacity(8 + payload.len());
        encode_varint(number, &mut body).expect("segment numbers stay below 2^62");
        body.extend_from_slice(&payload);
        let body = Bytes::from(body);

        let mut handed = 0;
        for entry in hub.dishes().iter() {
            if entry.matching(&topic).is_none() {
                continue;
            }
            if !entry.conn.agreed_now().is_some_and(|a| a.datagrams) {
                hub.drops.record(&topic, DropCause::NoDatagrams);
                continue;
            }
            let mut flows = lock(&entry.flows);
            match flows.get_mut(&topic) {
                Some(FlowSlot::Open(flow)) => match flow.send(body.clone()) {
                    Ok(()) => handed += 1,
                    Err(Error::TooLarge { .. }) => hub.drops.record(&topic, DropCause::TooLarge),
                    Err(Error::DatagramsUnavailable) => {
                        hub.drops.record(&topic, DropCause::NoDatagrams);
                    }
                    // The flow ended — refused, released or its connection
                    // gone. This datagram is lost; the next one reopens.
                    Err(_) => {
                        flows.remove(&topic);
                        hub.drops.record(&topic, DropCause::SubscriberQueue);
                    }
                },
                Some(FlowSlot::Opening(pending)) => {
                    if pending.replace(body.clone()).is_some() {
                        hub.drops.record(&topic, DropCause::SubscriberQueue);
                    }
                    handed += 1;
                }
                None => {
                    flows.insert(Arc::clone(&topic), FlowSlot::Opening(Some(body.clone())));
                    entry.conn.exec.spawn(open_datagram_flow(
                        ConnHandle::clone(&entry.conn),
                        Arc::clone(&hub.path),
                        Arc::clone(&topic),
                        Arc::clone(&entry.flows),
                        Arc::clone(&hub.drops),
                    ));
                    handed += 1;
                }
            }
        }
        Ok(handed)
    }

    /// The path this radio serves.
    pub fn path(&self) -> &str {
        &self.hub().path
    }

    /// Dishes currently joined.
    pub fn dish_count(&self) -> usize {
        self.hub().dishes().len()
    }

    /// Copies dropped, over topics and causes.
    pub fn dropped(&self) -> u64 {
        self.hub().drops.total()
    }

    /// What was dropped on `topic`, by cause, or `None` if nothing was. The
    /// table holds at most `Limits::max_sequence_scopes` topics.
    pub fn dropped_on(&self, topic: &str) -> Option<TopicDrops> {
        self.hub().drops.on_topic(topic)
    }

    /// Every topic that lost a copy, with its counts by cause.
    pub fn drops(&self) -> Vec<TopicDrops> {
        self.hub().drops.by_topic()
    }

    /// Decides which dish may join which filter
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.3). `admit` sees each join — the proved peer, its chain and the
    /// filter — before it is recorded, on every repeat too, and a join it
    /// refuses is not recorded, reserves no `max_subscriptions` slot and
    /// closes nothing: the dish is told nothing, which is what
    /// [decisions/0017](../../../docs/decisions/0017-subscription-verdict.md)
    /// §4.1 defines silence to mean. A refused repeat leaves the recorded
    /// filter alone; withdrawing it is [`Radio::evict`]'s.
    ///
    /// Installing a policy screens the joins already recorded and withdraws
    /// the ones it refuses, so no join slips in between creating the radio
    /// and calling this. The policy is shared by every clone of this radio,
    /// and a later call replaces it. It runs on the dish connection's task
    /// and must not block.
    pub fn with_admission(
        self,
        admit: impl Fn(&Join<'_>) -> bool + Send + Sync + 'static,
    ) -> Radio {
        self.hub().set_admission(Arc::new(admit));
        self
    }

    /// Withdraws `filter` — the exact string the dish joined with — from
    /// every connection of `peer`, and frees its `max_subscriptions` slot.
    /// The dish is not told
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.3); it receives nothing more on that filter, and a dish that joins
    /// again meets the admission again. Returns how many joins went.
    pub fn evict(&self, peer: &PeerIdentity, filter: &str) -> usize {
        self.hub().evict(peer, filter)
    }
}

/// Opens a dish's datagram flow for one topic and sends the datagram that
/// waited for it; on failure the slot goes, so the next datagram tries again.
async fn open_datagram_flow(
    conn: ConnHandle,
    path: Arc<str>,
    topic: Arc<str>,
    flows: Arc<Mutex<HashMap<Arc<str>, FlowSlot>>>,
    drops: Arc<DropTable>,
) {
    let meta = FlowMeta::default().with_topic(topic.as_ref());
    let opened = open_flow_on(&conn, &path, &meta).await;
    let mut flows = lock(&flows);
    match opened {
        Ok(flow) => {
            // The dish may have left the topic meanwhile, which removed the
            // slot; the new flow then just closes.
            let Some(slot) = flows.get_mut(&topic) else {
                return;
            };
            if let FlowSlot::Opening(pending) = slot
                && let Some(body) = pending.take()
            {
                match flow.send(body) {
                    Ok(()) => {}
                    Err(Error::TooLarge { .. }) => drops.record(&topic, DropCause::TooLarge),
                    Err(_) => drops.record(&topic, DropCause::SubscriberQueue),
                }
            }
            *slot = FlowSlot::Open(flow);
        }
        Err(e) => {
            if let Some(FlowSlot::Opening(Some(_))) = flows.remove(&topic) {
                let cause = match e {
                    Error::DatagramsUnavailable => DropCause::NoDatagrams,
                    _ => DropCause::SubscriberQueue,
                };
                drops.record(&topic, cause);
            }
        }
    }
}

/// Moves a radio's datagram segments from one flow into its dish's queue:
/// stale ones against the newest delivered on the connection, path and topic
/// are discarded, and a full queue discards rather than waits.
pub(crate) async fn pump_flow(ctx: ConnHandle, route: DishRoute, flow: IncomingFlow) {
    let Some(topic) = flow.info().topic.clone() else {
        // A radio names its topic; a flow without one is not a segment.
        return;
    };
    while let Some(datagram) = flow.recv().await {
        let Some((segment, offset)) = split_flow_datagram(&datagram) else {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if !ctx
            .segments_in
            .fresh(&flow.info().endpoint, &topic, segment)
        {
            route.shared.stale.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let received = Received::Datagram {
            topic: topic.clone(),
            segment,
            payload: datagram.slice(offset..),
        };
        if route.queue.try_send(received).is_err() {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// --- dish side ----------------------------------------------------------------

/// What a dish receives.
#[derive(Debug)]
pub enum Received {
    /// A stream segment: read it as it arrives. Its topic and number are on
    /// [`crate::IncomingMeta::topic`] and [`crate::IncomingMeta::segment`].
    Segment(IncomingTransfer),
    /// A datagram segment: one packet, whole.
    Datagram {
        /// The topic it was sent on.
        topic: String,
        /// Its segment number on that topic.
        segment: u64,
        /// The payload.
        payload: Bytes,
    },
}

/// A dish's counters. Which segment is stale is the connection's to know
/// (`ConnCtx::segments_in`), so a redial starts over
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.11).
pub(crate) struct DishShared {
    stale: AtomicU64,
    overflow: AtomicU64,
}

/// A dish's route on its connection's namespace.
#[derive(Clone)]
pub(crate) struct DishRoute {
    queue: mpsc::Sender<Received>,
    shared: Arc<DishShared>,
}

/// Delivers one arrived stream segment to its dish, or discards it: stale
/// against the newest delivered on its connection, path and topic, or with
/// the dish's queue full. Never blocks the connection.
pub(crate) fn deliver_segment(
    ctx: &ConnHandle,
    path: &str,
    route: &DishRoute,
    transfer: IncomingTransfer,
) {
    let meta = transfer.meta();
    let (Some(topic), Some(segment)) = (meta.topic.as_deref(), meta.segment) else {
        transfer.refuse(codes::UNSUPPORTED);
        return;
    };
    if !ctx.segments_in.fresh(path, topic, segment) {
        route.shared.stale.fetch_add(1, Ordering::Relaxed);
        transfer.refuse(codes::CANCELED);
        return;
    }
    if let Err(e) = route.queue.try_send(Received::Segment(transfer)) {
        route.shared.overflow.fetch_add(1, Ordering::Relaxed);
        if let Received::Segment(transfer) = e.into_inner() {
            transfer.refuse(codes::CANCELED);
        }
    }
}

/// What a dish does on every connection it gets: fill the reverse pool
/// where the transport needs one, claim its path, and re-send its joins.
struct DishAttach {
    joins: Arc<Mutex<HashMap<String, Option<Duration>>>>,
    route: DishRoute,
}

impl Attach for DishAttach {
    fn attach<'a>(
        &'a self,
        conn: &'a ConnHandle,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if conn.conn.needs_reverse_pool() && conn.conn.park_reverse().await? == 0 {
                return Err(Error::Unsupported);
            }
            conn.namespace
                .register(path, Route::Dish(self.route.clone()))?;
            if conn.conn.needs_reverse_pool() {
                let maintaining = ConnHandle::clone(conn);
                conn.exec
                    .spawn(async move { maintaining.conn.maintain_reverse().await });
            }
            let joins: Vec<(String, Option<Duration>)> = lock(&self.joins)
                .iter()
                .map(|(f, age)| (f.clone(), *age))
                .collect();
            for (filter, max_age) in joins {
                send_subscription(
                    conn,
                    FrameKind::Subscribe,
                    path,
                    &filter,
                    max_age.map(millis),
                )
                .await?;
            }
            Ok(())
        })
    }
}

fn millis(age: Duration) -> u64 {
    u64::try_from(age.as_millis()).unwrap_or(u64::MAX)
}

/// State of a dish.
pub struct DishState {
    peer: Peer,
    joins: Arc<Mutex<HashMap<String, Option<Duration>>>>,
    queue: tokio::sync::Mutex<mpsc::Receiver<Received>>,
    shared: Arc<DishShared>,
}

impl DishState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>, depth: usize) -> DishState {
        let (queue_tx, queue) = mpsc::channel(depth);
        let joins = Arc::new(Mutex::new(HashMap::new()));
        let shared = Arc::new(DishShared {
            stale: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
        });
        let attach = DishAttach {
            joins: Arc::clone(&joins),
            route: DishRoute {
                queue: queue_tx,
                shared: Arc::clone(&shared),
            },
        };
        DishState {
            peer: Peer::with_attach(runtime, tls, Some(Arc::new(attach))),
            joins,
            queue: tokio::sync::Mutex::new(queue),
            shared,
        }
    }
}

impl Dish {
    /// Connects to a radio at `url` and joins every topic joined so far. A
    /// redial joins again: what was sent in between is gone, and the numbers
    /// start over on the new connection.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state().peer.connect(url).await
    }

    /// Number of connected radios.
    pub fn peer_count(&self) -> usize {
        self.state().peer.peer_count()
    }

    /// One record per live connection to a radio, labelled by the URL as
    /// dialled; see [`crate::Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state().peer.connection_stats()
    }

    /// The event stream of this dish's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state().peer.events()
    }

    /// Joins every topic `filter` matches, with the latency budget `max_age`:
    /// a copy the radio cannot get acknowledged within it, on the radio's
    /// clock from the segment's open, is reset and counted. The filter
    /// grammar is Pub/Sub's. Joining a filter again updates its `max_age`.
    pub async fn join(&self, filter: &str, max_age: Option<Duration>) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
        lock(&self.state().joins).insert(filter.to_owned(), max_age);
        for (conn, path) in self.state().peer.live_peers() {
            send_subscription(
                &conn,
                FrameKind::Subscribe,
                &path,
                filter,
                max_age.map(millis),
            )
            .await?;
        }
        Ok(())
    }

    /// Leaves a filter. An unknown filter is ignored.
    pub async fn leave(&self, filter: &str) -> Result<(), Error> {
        if lock(&self.state().joins).remove(filter).is_none() {
            return Ok(());
        }
        for (conn, path) in self.state().peer.live_peers() {
            send_subscription(&conn, FrameKind::Unsubscribe, &path, filter, None).await?;
        }
        Ok(())
    }

    /// Waits for the next segment.
    pub async fn recv(&self) -> Result<Received, Error> {
        let mut queue = self.state().queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// Segments discarded on arrival because a newer one on the same topic
    /// had already been delivered on the same connection.
    pub fn stale(&self) -> u64 {
        self.state().shared.stale.load(Ordering::Relaxed)
    }

    /// Segments discarded on arrival because the receive queue was full.
    pub fn overflow(&self) -> u64 {
        self.state().shared.overflow.load(Ordering::Relaxed)
    }
}

impl Drop for DishState {
    fn drop(&mut self) {
        // Best effort, through the connection actor for the same reason a
        // subscriber's drop is: a destructor may run with no reactor.
        let joins: Vec<String> = lock(&self.joins).keys().cloned().collect();
        for (conn, path) in self.peer.live_peers() {
            conn.namespace.unregister(&path);
            for filter in &joins {
                conn.notify(Ctl::SendUnsubscribe {
                    path: Arc::clone(&path),
                    filter: filter.clone(),
                });
            }
        }
    }
}
