//! Publisher-side subscription registry and fan-out.
//!
//! A publisher does not know its subscribers: they arrive as SUBSCRIBE frames
//! on whatever connections the listener has accepted. This module owns that
//! mapping and the one genuinely new selection policy in Phase 3 — fan-out
//! (`docs/ARCHITECTURE.md`, pattern primitive P3). Everything else a published
//! message needs is the ordinary one-way transfer path.
//!
//! **Delivery is per-subscriber best effort.** Each subscriber has a byte
//! budget; a message that does not fit is dropped for that subscriber, counted,
//! and the publisher moves on. A slow consumer therefore costs the publisher
//! nothing but its own messages (master doc §17: telemetry fan-out tolerates
//! drops and cannot tolerate a stalled producer).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};
use weida_core::{Error, Limits, TraceContext};
use weida_protocol::header::OrderingMode;
use weida_protocol::{DataHeader, filter};

use crate::conn::ConnHandle;
use crate::ordering::Sequencer;
use crate::transfer::write_data_preamble;

/// Messages one subscriber's writer task may hold. The real bound is the byte
/// budget; this only keeps the channel itself from growing without limit when
/// messages are tiny.
const WRITER_QUEUE: usize = 1024;

/// Does `topic` match `filter`?
///
/// The segmented grammar of `docs/PROTOCOL.md` §6.4: segments split on `.`,
/// `*` for exactly one whole segment, a trailing `#` for zero or more, every
/// other byte literal, and the empty filter matching everything.
///
/// The objection this function used to carry was that treating `*` as a
/// wildcard "would make topics with a literal `*` unaddressable and would put
/// a matching language in the hot path". Both halves were true and both are
/// accepted deliberately
/// ([decisions/0007](../../../docs/decisions/0007-topic-namespace.md) §4.6): a
/// filter can no longer select a segment containing `.`, `*` or `#`
/// literally — there is no escape character, and no sheet reports a use for
/// one — while a byte prefix could not express a boundary at all, so
/// `sensors.temp` also selected `sensors.temperature`. The hot-path half is
/// answered by the shape rather than by the choice: `#` is legal only as the
/// final segment, so this is one left-to-right walk with no backtracking, no
/// allocation and work bounded by the 256 B filter cap.
///
/// A `topic` is never a pattern: `*` and `#` in a published topic are literal
/// bytes here, exactly like any other.
pub(crate) fn matches_filter(topic: &str, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    let mut topic_segments = topic.split(filter::SEPARATOR);
    let mut filter_segments = filter.split(filter::SEPARATOR);
    loop {
        let Some(pattern) = filter_segments.next() else {
            // The filter is spent: it matches only if the topic is too.
            return topic_segments.next().is_none();
        };
        // Only ever the final segment — `filter::validate` rejects anything
        // else at the codec boundary — so everything left over matches.
        if pattern == filter::REST {
            return true;
        }
        let Some(segment) = topic_segments.next() else {
            return false;
        };
        if pattern != filter::ONE_SEGMENT && pattern != segment {
            return false;
        }
    }
}

/// One message queued for one subscriber.
///
/// The DATA header is *not* pre-encoded and shared: each copy carries the
/// subscriber's own path. The payload is a `Bytes`, so the fan-out shares one
/// allocation regardless of subscriber count.
struct PubMsg {
    topic: Arc<str>,
    payload: Bytes,
    trace: TraceContext,
    /// The producer's sequence number for this topic, assigned once per
    /// published message. `None` unless `PerProducer` ordering is negotiated.
    sequence: Option<u64>,
}

/// One subscribing connection's state for one publisher path.
struct SubEntry {
    /// `quinn::Connection::stable_id`, the identity a SUBSCRIBE arrives with.
    conn_id: usize,
    filters: HashSet<String>,
    tx: mpsc::Sender<PubMsg>,
    /// Payload bytes this subscriber may hold queued. Permits are taken by
    /// `publish` and returned by the writer once the bytes are on the wire.
    budget: Arc<Semaphore>,
}

/// Why a published copy was dropped.
///
/// Three causes, because they are three different failures: the first two
/// are the subscriber not keeping up, the third is the *local* transport
/// having no connection to carry the copy
/// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
/// §4.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DropCause {
    /// The subscriber's byte budget (`Limits::subscriber_buffer_bytes`) had
    /// no room for the payload.
    SubscriberBudget,
    /// The subscriber's writer queue was full of messages.
    SubscriberQueue,
    /// The subscriber had parked no connection for this copy: a socket
    /// transport's fan-out rides connections the subscriber parks, and the
    /// pool was empty.
    NoParkedConnection,
}

/// What a publisher dropped on one topic, by cause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicDrops {
    /// The topic the dropped copies were published on.
    pub topic: Arc<str>,
    /// Copies dropped for an exhausted subscriber byte budget.
    pub subscriber_budget: u64,
    /// Copies dropped for a full subscriber queue.
    pub subscriber_queue: u64,
    /// Copies dropped because the subscriber had parked no connection.
    pub no_parked_connection: u64,
}

impl TopicDrops {
    /// All three causes summed.
    pub fn total(&self) -> u64 {
        self.subscriber_budget + self.subscriber_queue + self.no_parked_connection
    }
}

/// Three counters, one per cause.
#[derive(Default)]
struct Causes {
    budget: AtomicU64,
    queue: AtomicU64,
    no_parked: AtomicU64,
}

impl Causes {
    fn record(&self, cause: DropCause) {
        let counter = match cause {
            DropCause::SubscriberBudget => &self.budget,
            DropCause::SubscriberQueue => &self.queue,
            DropCause::NoParkedConnection => &self.no_parked,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, topic: &Arc<str>) -> TopicDrops {
        TopicDrops {
            topic: Arc::clone(topic),
            subscriber_budget: self.budget.load(Ordering::Relaxed),
            subscriber_queue: self.queue.load(Ordering::Relaxed),
            no_parked_connection: self.no_parked.load(Ordering::Relaxed),
        }
    }
}

/// The drops on one publisher path: a total, and a count per topic and
/// cause.
///
/// The per-topic table is bounded by `Limits::max_sequence_scopes`, the same
/// ceiling the sequencer's per-topic table has and for the same reason: the
/// topics are the publishing application's, but a table nobody bounds is a
/// table that grows for the life of the process. At the cap a new topic's
/// drops count in the total and nowhere else, which is what the sequencer
/// does with a new scope.
struct DropTable {
    total: AtomicU64,
    per_topic: RwLock<HashMap<Arc<str>, Causes>>,
    max_topics: usize,
}

impl DropTable {
    fn new(max_topics: usize) -> DropTable {
        DropTable {
            total: AtomicU64::new(0),
            per_topic: RwLock::new(HashMap::new()),
            max_topics,
        }
    }

    /// Counts one dropped copy of a message on `topic`.
    ///
    /// A read lock on the common path — the topic is known — and a write
    /// lock only for a topic's first drop. Drops are the exceptional path of
    /// fan-out, so neither is on the path of a message that gets through.
    fn record(&self, topic: &Arc<str>, cause: DropCause) {
        self.total.fetch_add(1, Ordering::Relaxed);
        {
            let table = self.per_topic.read().expect("drop table poisoned");
            if let Some(causes) = table.get(topic) {
                causes.record(cause);
                return;
            }
        }
        let mut table = self.per_topic.write().expect("drop table poisoned");
        if let Some(causes) = table.get(topic) {
            causes.record(cause);
        } else if table.len() < self.max_topics {
            let causes = Causes::default();
            causes.record(cause);
            table.insert(Arc::clone(topic), causes);
        }
    }

    fn on_topic(&self, topic: &str) -> Option<TopicDrops> {
        let table = self.per_topic.read().expect("drop table poisoned");
        table
            .get_key_value(topic)
            .map(|(topic, causes)| causes.snapshot(topic))
    }

    fn by_topic(&self) -> Vec<TopicDrops> {
        let table = self.per_topic.read().expect("drop table poisoned");
        table
            .iter()
            .map(|(topic, causes)| causes.snapshot(topic))
            .collect()
    }
}

/// Everything registered for one publisher path.
struct PathState {
    subs: Vec<SubEntry>,
    /// Copies dropped for a subscriber that could not take them, by topic
    /// and cause.
    drops: Arc<DropTable>,
}

/// Subscriptions for every publisher path served by one listener.
pub(crate) struct SubRegistry {
    paths: RwLock<HashMap<Arc<str>, PathState>>,
    /// Filters held per connection, summed over paths, for `max_subscriptions`.
    per_conn: RwLock<HashMap<usize, usize>>,
    limits: Limits,
    /// Numbers published messages per topic. The number belongs to the
    /// *message*, not to a subscriber's copy, which is what makes a dropped
    /// copy visible as a gap: the survivors keep the numbers the lost ones
    /// would have had.
    sequencer: Sequencer,
}

impl SubRegistry {
    pub(crate) fn new(limits: Limits, ordering: OrderingMode) -> SubRegistry {
        SubRegistry {
            paths: RwLock::new(HashMap::new()),
            per_conn: RwLock::new(HashMap::new()),
            limits,
            sequencer: Sequencer::new(ordering),
        }
    }

    /// Records interest of `ctx` in `filter` on `path`.
    ///
    /// The path need not be registered by a publisher yet: a subscriber may
    /// arrive first, and the subscription is bounded by `max_subscriptions`
    /// either way. Duplicate filters are idempotent.
    ///
    /// Returns [`Error::LimitExceeded`] when the connection is already holding
    /// `max_subscriptions` filters; the caller closes the connection, because
    /// SUBSCRIBE carries no transfer id to answer with an ERROR frame.
    pub(crate) fn subscribe(
        &self,
        path: &str,
        ctx: &ConnHandle,
        filter: String,
    ) -> Result<(), Error> {
        let conn_id = ctx.conn.stable_id();
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");

        let state = paths.entry(Arc::from(path)).or_insert_with(|| PathState {
            subs: Vec::new(),
            drops: Arc::new(DropTable::new(self.limits.max_sequence_scopes)),
        });
        if let Some(entry) = state.subs.iter_mut().find(|e| e.conn_id == conn_id) {
            if entry.filters.contains(&filter) {
                return Ok(());
            }
            let count = per_conn.entry(conn_id).or_insert(0);
            if *count >= self.limits.max_subscriptions {
                return Err(Error::LimitExceeded);
            }
            *count += 1;
            entry.filters.insert(filter);
            return Ok(());
        }

        let count = per_conn.entry(conn_id).or_insert(0);
        if *count >= self.limits.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        *count += 1;

        let (tx, rx) = mpsc::channel(WRITER_QUEUE);
        let budget = Arc::new(Semaphore::new(self.limits.subscriber_buffer_bytes));
        let drops = Arc::clone(&state.drops);
        state.subs.push(SubEntry {
            conn_id,
            filters: HashSet::from([filter]),
            tx,
            budget: Arc::clone(&budget),
        });
        // One writer task per (connection, path): it serializes this
        // subscriber's messages, which is what makes delivery FIFO per
        // subscriber even though each message rides its own QUIC stream.
        ctx.exec
            .spawn(writer(Arc::clone(ctx), Arc::from(path), rx, budget, drops));
        Ok(())
    }

    /// Withdraws one filter. An unknown filter, path or connection is ignored:
    /// UNSUBSCRIBE is idempotent and races legitimately with teardown.
    pub(crate) fn unsubscribe(&self, path: &str, conn_id: usize, filter: &str) {
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        let Some(state) = paths.get_mut(path) else {
            return;
        };
        let Some(index) = state.subs.iter().position(|e| e.conn_id == conn_id) else {
            return;
        };
        if !state.subs[index].filters.remove(filter) {
            return;
        }
        decrement(&mut per_conn, conn_id, 1);
        if state.subs[index].filters.is_empty() {
            // The writer task ends when its channel closes.
            state.subs.swap_remove(index);
        }
    }

    /// Drops every subscription held by one connection. Called when the
    /// connection closes, so a churn of peers cannot grow the registry.
    pub(crate) fn remove_connection(&self, conn_id: usize) {
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        for state in paths.values_mut() {
            if let Some(index) = state.subs.iter().position(|e| e.conn_id == conn_id) {
                state.subs.swap_remove(index);
            }
        }
        per_conn.remove(&conn_id);
    }

    /// Fans `payload` out to every subscriber of `path` whose filter matches
    /// `topic`, and returns how many it was enqueued to.
    ///
    /// Synchronous and non-blocking by design: a subscriber that cannot take
    /// the bytes right now loses this message rather than stalling everyone
    /// else. `want` is the payload length in permits.
    pub(crate) fn publish(
        &self,
        path: &str,
        topic: &str,
        payload: Bytes,
        trace: TraceContext,
        want: u32,
    ) -> usize {
        let paths = self.paths.read().expect("subscription lock poisoned");
        let Some(state) = paths.get(path) else {
            return 0;
        };

        let topic: Arc<str> = Arc::from(topic);
        // Before fan-out, so every copy of one message carries one number and
        // a copy dropped below leaves a hole rather than renumbering.
        let sequence = self.sequencer.next(&topic);
        let mut sent = 0usize;
        for entry in &state.subs {
            if !entry.filters.iter().any(|f| matches_filter(&topic, f)) {
                continue;
            }
            let Ok(permit) = entry.budget.try_acquire_many(want) else {
                state.drops.record(&topic, DropCause::SubscriberBudget);
                tracing::debug!(path, %topic, "subscriber budget exhausted; message dropped");
                continue;
            };
            let msg = PubMsg {
                topic: Arc::clone(&topic),
                payload: payload.clone(),
                trace,
                sequence,
            };
            match entry.tx.try_send(msg) {
                Ok(()) => {
                    // The writer returns the permits once the bytes are gone.
                    permit.forget();
                    sent += 1;
                }
                Err(_) => {
                    // Dropping `permit` returns the budget immediately.
                    state.drops.record(&topic, DropCause::SubscriberQueue);
                    tracing::debug!(path, %topic, "subscriber queue full; message dropped");
                }
            }
        }
        sent
    }

    /// Connections currently subscribed to `path`.
    pub(crate) fn subscriber_count(&self, path: &str) -> usize {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.subs.len())
    }

    /// Filters registered on `path`, summed over subscribers.
    pub(crate) fn filter_count(&self, path: &str) -> usize {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.subs.iter().map(|e| e.filters.len()).sum())
    }

    /// Messages dropped on `path` because a subscriber could not take them,
    /// summed over topics and causes.
    pub(crate) fn dropped(&self, path: &str) -> u64 {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.drops.total.load(Ordering::Relaxed))
    }

    /// The drops on `path` for one topic, or `None` if none was counted.
    pub(crate) fn dropped_on(&self, path: &str, topic: &str) -> Option<TopicDrops> {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .and_then(|s| s.drops.on_topic(topic))
    }

    /// The drops on `path`, one entry per topic that lost a copy.
    pub(crate) fn drops(&self, path: &str) -> Vec<TopicDrops> {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or_else(Vec::new, |s| s.drops.by_topic())
    }
}

fn decrement(counts: &mut HashMap<usize, usize>, conn_id: usize, by: usize) {
    if let Some(count) = counts.get_mut(&conn_id) {
        *count = count.saturating_sub(by);
        if *count == 0 {
            counts.remove(&conn_id);
        }
    }
}

/// Writes one subscriber's messages, in order, each on its own uni stream.
///
/// Fan-out is one-way by construction: a published copy owes nothing back, so
/// it rides a unidirectional stream and the publisher never waits on it.
async fn writer(
    ctx: ConnHandle,
    path: Arc<str>,
    mut rx: mpsc::Receiver<PubMsg>,
    budget: Arc<Semaphore>,
    drops: Arc<DropTable>,
) {
    loop {
        let msg = tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => msg,
                None => break,
            },
            // Without this a subscriber on a dead connection would sit here
            // forever holding an `Arc<ConnCtx>`.
            _ = ctx.conn.closed() => break,
        };
        let len = msg.payload.len();
        let outcome = write_one(&ctx, &path, &msg).await;
        budget.add_permits(len);
        match outcome {
            Ok(()) => {}
            // The subscriber parked no connection for this copy. That is a
            // drop of the copy, not a failure of the subscription: the pool
            // refills and the next message may well go out
            // ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
            // §4.4). Counted exactly like an exhausted byte budget.
            Err(Error::NoParkedConnection) => {
                drops.record(&msg.topic, DropCause::NoParkedConnection);
                tracing::debug!(path = %path, "no parked connection; copy dropped");
            }
            Err(e) => {
                tracing::debug!(path = %path, error = %e, "fan-out write failed; subscriber writer ending");
                break;
            }
        }
    }
    tracing::debug!(path = %path, "subscriber writer ended");
}

async fn write_one(ctx: &ConnHandle, path: &str, msg: &PubMsg) -> Result<(), Error> {
    let mut header = DataHeader::addressed(path);
    header.topic = Some(msg.topic.to_string());
    header.content_len = Some(msg.payload.len() as u64);
    header.traceparent = Some(msg.trace.to_traceparent());
    // The number the publisher assigned to this *message* (`None` under
    // `core`): every subscriber's copy carries the same one, so a copy this
    // subscriber lost shows up as a hole in its own sequence.
    header.sequence = msg.sequence;

    let mut stream = ctx.open_uni().await?;
    write_data_preamble(&mut stream, &header).await?;
    stream.write_all(&msg.payload).await?;
    stream.finish()?;
    // A published copy is a finished transfer like any other, and nobody
    // holds a receipt for it: park it on this connection so a drain waits
    // for it (`docs/decisions/0009-drain.md` §4.2).
    if ctx.parked.park(stream.stopped()) {
        ctx.shared.drain.evict();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_literal_filter_matches_whole_segments_only() {
        assert!(matches_filter("px.eur", "px.eur"));
        assert!(!matches_filter("px.eur", "px.eur.spot"));
        assert!(!matches_filter("fx.usd", "px.eur"));
        // The boundary a byte prefix could not see: `px.` used to select
        // everything under `px`, and `sensors.temp` used to select
        // `sensors.temperature`.
        assert!(!matches_filter("px.eur", "px."));
        assert!(!matches_filter("sensors.temperature", "sensors.temp"));
    }

    #[test]
    fn the_empty_filter_and_the_rest_wildcard_take_everything() {
        assert!(matches_filter("", ""));
        assert!(matches_filter("anything.at.all", ""));
        assert!(matches_filter("anything.at.all", "#"));
        assert!(matches_filter("", "#"));
    }

    #[test]
    fn one_segment_wildcard_matches_exactly_one() {
        assert!(matches_filter("px.eur", "px.*"));
        assert!(matches_filter("sensors.a.temp", "sensors.*.temp"));
        assert!(matches_filter("px.eur", "*.eur"));
        // Exactly one: neither none nor two.
        assert!(!matches_filter("px", "px.*"));
        assert!(!matches_filter("px.eur.spot", "px.*"));
    }

    #[test]
    fn the_rest_wildcard_matches_zero_or_more_trailing_segments() {
        assert!(matches_filter("px", "px.#"));
        assert!(matches_filter("px.eur", "px.#"));
        assert!(matches_filter("px.eur.spot", "px.#"));
        assert!(!matches_filter("fx", "px.#"));
        assert!(!matches_filter("pxx", "px.#"));
    }

    #[test]
    fn a_topic_is_never_a_pattern() {
        // `*` and `#` in a published topic are ordinary bytes.
        assert!(matches_filter("px.*", "px.*"));
        assert!(!matches_filter("px.*", "px.eur"));
        assert!(matches_filter("px.#", "px.#"));
        assert!(matches_filter("px.*", "*.*"));
    }

    #[test]
    fn empty_segments_match_only_empty_segments() {
        assert!(matches_filter("px.", "px."));
        assert!(!matches_filter("px.eur", "px."));
        assert!(matches_filter("px.", "px.*"));
    }

    #[test]
    fn matching_is_byte_exact_not_case_folded() {
        assert!(!matches_filter("PX.EUR", "px.*"));
        assert!(!matches_filter("px.eur", "PX.*"));
    }
}
