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

/// Everything registered for one publisher path.
#[derive(Default)]
struct PathState {
    subs: Vec<SubEntry>,
    /// Messages dropped for a slow subscriber, summed over subscribers.
    dropped: Arc<AtomicU64>,
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

        let state = paths.entry(Arc::from(path)).or_default();
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
            .spawn(writer(Arc::clone(ctx), Arc::from(path), rx, budget));
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
                state.dropped.fetch_add(1, Ordering::Relaxed);
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
                    state.dropped.fetch_add(1, Ordering::Relaxed);
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

    /// Messages dropped on `path` because a subscriber could not take them.
    pub(crate) fn dropped(&self, path: &str) -> u64 {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.dropped.load(Ordering::Relaxed))
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
        if let Err(e) = write_one(&ctx, &path, &msg).await {
            tracing::debug!(path = %path, error = %e, "fan-out write failed; subscriber writer ending");
            budget.add_permits(len);
            break;
        }
        budget.add_permits(len);
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
