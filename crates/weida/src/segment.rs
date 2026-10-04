//! Segments: the L0 unit a radio fans out and a dialling `Peer` sends
//! ([decisions/0037](../../../docs/decisions/0037-layered-segments.md) §4.2).
//!
//! A **segment** is a unit whose parts may depend on each other and on
//! nothing earlier — a voice frame, a raw preview frame, a video GOP. It is
//! numbered per `(sender, path, topic)` in DATA key `13`, and every copy of
//! it is one uni stream toward one receiver. The machinery here is shared by
//! [`crate::Radio::segment`], one copy per joined dish, and
//! [`crate::Peer::segment`], one copy toward the bound path the peer dialled.
//! Three things drop a copy, and each is counted per topic and cause:
//!
//! * **supersession** — opening segment *n+1* resets at once every copy of
//!   segment *n* on that topic whose writer had not finished it, or whose
//!   queued chunks the transport does not take at once; a finished copy gets
//!   the time its path needs, `rtt + 50 ms + rtt * bytes / cwnd`, and is
//!   reset only if still unacknowledged then. A copy that follows upstream
//!   ([`SegmentTerms::follows_upstream`]) is treated as finished until a
//!   write has to wait. No other topic's copies are touched;
//! * **expiry** — the smaller of the sender's and the receiver's `max_age`,
//!   on the sender's clock from the segment's open;
//! * **the budget** — `subscriber_buffer_bytes` of chunks a copy may hold
//!   unwritten (per dish at a radio, per `Peer` for `Peer::segment`), and a
//!   queue of [`COPY_QUEUE`] chunks.
//!
//! Nothing here ever waits for a receiver: [`Segment::write`] is
//! synchronous.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, Semaphore, mpsc};
use weida_core::Error;
use weida_protocol::{DataHeader, codes};

use crate::conn::ConnHandle;
use crate::pubsub::{DropCause, DropTable};
use crate::transfer::write_data_preamble;
use crate::transport::SendHalf;

/// Chunks one copy may hold queued for its writer task. The real bound is
/// the receiver's byte budget; this keeps the channel from growing on tiny
/// chunks.
pub(crate) const COPY_QUEUE: usize = 64;

/// What a finished copy waits beyond one round trip before a successor
/// supersedes it: quinn's default `max_ack_delay` of 25 ms (the transport
/// parameter default, quinn-proto `transport_parameters.rs`; weida's
/// `transport_config` in `tls.rs` keeps it) plus scheduling margin.
const SUPERSEDE_SLACK: Duration = Duration::from_millis(50);

/// How long a finished copy of `sent` bytes may still take to be
/// acknowledged on a path with smoothed round trip `rtt` and congestion
/// window `cwnd`: the time the window needs to carry every byte, one round
/// trip for the receipt and [`SUPERSEDE_SLACK`]. `sent` bounds what is still
/// unsent from above, so a copy whose bytes are already out gets more than
/// it needs, never less.
fn finish_grace(rtt: Duration, cwnd: u64, sent: u64) -> Duration {
    let carry = Duration::try_from_secs_f64(rtt.as_secs_f64() * sent as f64 / cwnd.max(1) as f64)
        .unwrap_or(Duration::MAX);
    rtt.saturating_add(SUPERSEDE_SLACK).saturating_add(carry)
}

/// The `quinn` priority of a copy's stream for `layer`: layer-major, so
/// every base layer on a connection goes before any enhancement layer, and
/// the application's `priority` orders topics within a layer
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.4).
fn copy_priority(layer: u8, priority: i16) -> i32 {
    (15 - i32::from(layer)) * 65_536 + i32::from(priority)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How a segment is sent: what the sender decides for every copy of it
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.2).
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SegmentTerms {
    /// The sender's latency budget: a copy still unfinished or
    /// unacknowledged this long after the segment opened is reset. At a
    /// radio the dish's own `max_age` applies too, and the smaller wins.
    pub max_age: Option<Duration>,
    /// Orders this segment's copies against other topics' copies of the
    /// same layer on one connection; higher goes first. Layer 0 of every
    /// topic still goes before any higher layer. A no-op on the local
    /// transports.
    pub priority: i16,
    /// The writer relays a segment its upstream is still streaming: the
    /// successor takes a copy only where a write has to wait, and otherwise
    /// the copy keeps taking chunks and gets a finished copy's grace once it
    /// is finished
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.11). Dropping the segment unfinished still resets every copy.
    pub follows_upstream: bool,
}

impl SegmentTerms {
    /// Sets the sender's latency budget.
    #[must_use]
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Sets the priority among topics within a layer.
    #[must_use]
    pub fn with_priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    /// Marks the segment as relayed from an upstream that may still be
    /// streaming its predecessor.
    #[must_use]
    pub fn with_follows_upstream(mut self, follows_upstream: bool) -> Self {
        self.follows_upstream = follows_upstream;
        self
    }
}

/// One copy's control: how its segment's successor or its end reach it.
#[derive(Default)]
pub(crate) struct CopyCtl {
    superseded: AtomicBool,
    acked: AtomicBool,
    ended: AtomicBool,
    /// The writer finished the segment: every chunk of it is queued.
    finished: AtomicBool,
    /// Bytes queued for this copy, all chunks together.
    total: AtomicU64,
    /// Supersession treats the copy as finished until a write has to wait
    /// ([`SegmentTerms::follows_upstream`]).
    follows_upstream: bool,
    notify: Notify,
}

impl CopyCtl {
    fn supersede(&self) {
        if !self.acked.load(Ordering::Acquire) {
            self.superseded.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    fn live(&self) -> bool {
        !self.ended.load(Ordering::Acquire)
    }

    async fn superseded(&self) {
        loop {
            let notified = self.notify.notified();
            if self.superseded.load(Ordering::Acquire) {
                return;
            }
            // `notify_one` leaves a permit when nobody waits, so a
            // supersession between the check and this await is not lost.
            notified.await;
        }
    }
}

/// Per-topic segment numbering and the copies the next segment supersedes.
#[derive(Default)]
struct TopicState {
    next: u64,
    live: Vec<Arc<CopyCtl>>,
}

/// A sender's topics: the number each takes next, and the copies the next
/// segment on it supersedes. A radio holds one per path, a [`crate::Peer`]
/// one for its own copies.
pub(crate) struct SegmentTopics {
    /// Per-topic state, and the number a topic created after an eviction
    /// starts from.
    topics: Mutex<(HashMap<Arc<str>, TopicState>, u64)>,
    max_scopes: usize,
}

impl SegmentTopics {
    /// A table of at most `max_scopes` topics.
    pub(crate) fn new(max_scopes: usize) -> SegmentTopics {
        SegmentTopics {
            topics: Mutex::new((HashMap::new(), 0)),
            max_scopes,
        }
    }

    /// Takes the next number on `topic` and supersedes every copy of the
    /// previous segment there that is not yet acknowledged. Fails with
    /// [`Error::LimitExceeded`] when `max_scopes` topics all have a copy in
    /// flight.
    pub(crate) fn next(&self, topic: &Arc<str>) -> Result<u64, Error> {
        let mut guard = lock(&self.topics);
        let (topics, floor) = &mut *guard;
        if !topics.contains_key(topic) && topics.len() >= self.max_scopes {
            // The table is the application's, but still bounded: a topic
            // with no copy in flight has nothing to supersede and can go.
            let idle = topics.iter_mut().find_map(|(topic, state)| {
                state.live.retain(|c| c.live());
                state
                    .live
                    .is_empty()
                    .then(|| (Arc::clone(topic), state.next))
            });
            match idle {
                Some((idle, next)) => {
                    topics.remove(&idle);
                    // A topic that comes back must not restart below what a
                    // receiver already delivered on it, or the receiver would
                    // discard it as stale: numbering resumes above every
                    // number an evicted topic reached.
                    *floor = (*floor).max(next);
                }
                None => return Err(Error::LimitExceeded),
            }
        }
        let state = topics
            .entry(Arc::clone(topic))
            .or_insert_with(|| TopicState {
                next: *floor,
                live: Vec::new(),
            });
        let number = state.next;
        state.next += 1;
        for copy in state.live.drain(..) {
            copy.supersede();
        }
        Ok(number)
    }

    /// Supersedes the previous segment's copies on `topic` without using the
    /// number: the sender takes its numbers elsewhere
    /// ([`SegmentNumbers`]).
    pub(crate) fn supersede(&self, topic: &Arc<str>) -> Result<(), Error> {
        self.next(topic).map(drop)
    }

    fn track(&self, topic: &Arc<str>, copy: &Arc<CopyCtl>) {
        if let Some(state) = lock(&self.topics).0.get_mut(topic) {
            state.live.push(Arc::clone(copy));
        }
    }
}

/// The newest number per path, per topic.
type PerPath = HashMap<Arc<str>, HashMap<Arc<str>, u64>>;

/// The newest segment delivered per `(path, topic)` on one connection: what
/// every receiver — a dish, an acceptor, a transfer endpoint, a pair —
/// checks an arriving segment against
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.11). A redial is a new connection and starts over.
pub(crate) struct Newest {
    /// Per path, per topic, the newest number; and the entries in total.
    table: Mutex<(PerPath, usize)>,
    max: usize,
}

impl Newest {
    /// A table of at most `max` `(path, topic)` entries.
    pub(crate) fn new(max: usize) -> Newest {
        Newest {
            table: Mutex::new((HashMap::new(), 0)),
            max,
        }
    }

    /// `true` when `segment` is newer than every segment delivered on
    /// (`path`, `topic`), which it then becomes. At the table's cap an
    /// untracked key is always fresh: the check needs memory, and memory is
    /// bounded.
    pub(crate) fn fresh(&self, path: &str, topic: &str, segment: u64) -> bool {
        let mut guard = lock(&self.table);
        let (paths, count) = &mut *guard;
        if let Some(last) = paths.get_mut(path).and_then(|t| t.get_mut(topic)) {
            if segment <= *last {
                return false;
            }
            *last = segment;
            return true;
        }
        if *count < self.max {
            match paths.get_mut(path) {
                Some(topics) => {
                    topics.insert(Arc::from(topic), segment);
                }
                None => {
                    paths.insert(
                        Arc::from(path),
                        HashMap::from([(Arc::from(topic), segment)]),
                    );
                }
            }
            *count += 1;
        }
        true
    }
}

/// The next segment number per `(path, topic)`.
type NextNumbers = HashMap<(Arc<str>, Arc<str>), u64>;

/// The segment numbers per `(path, topic)` of one dialling connection: every
/// [`crate::Peer`] that sends segments over it shares one sequence, so the
/// receiver sees them only rise
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.11).
pub(crate) struct SegmentNumbers {
    /// The next number per `(path, topic)`, and the number a key created
    /// after an eviction starts from.
    table: Mutex<(NextNumbers, u64)>,
    max: usize,
}

impl SegmentNumbers {
    /// A table of at most `max` `(path, topic)` entries.
    pub(crate) fn new(max: usize) -> SegmentNumbers {
        SegmentNumbers {
            table: Mutex::new((HashMap::new(), 0)),
            max,
        }
    }

    /// Takes the next number on (`path`, `topic`). Never fails: at the cap
    /// it evicts an entry, and a key that comes back resumes above every
    /// number an evicted one reached, so a receiver never takes it as stale.
    pub(crate) fn next(&self, path: &Arc<str>, topic: &Arc<str>) -> u64 {
        let mut guard = lock(&self.table);
        let (table, floor) = &mut *guard;
        let key = (Arc::clone(path), Arc::clone(topic));
        if let Some(next) = table.get_mut(&key) {
            let number = *next;
            *next += 1;
            return number;
        }
        if table.len() >= self.max {
            let evicted = table.keys().next().cloned();
            if let Some(evicted) = evicted
                && let Some(next) = table.remove(&evicted)
            {
                *floor = (*floor).max(next);
            }
        }
        let number = *floor;
        if table.len() < self.max {
            table.insert(key, number + 1);
        } else {
            // A table of no entries at all: every segment takes the floor.
            *floor += 1;
        }
        number
    }
}

/// What a copy's writer task is told.
enum SegItem {
    Chunk(Bytes),
    Finish,
}

/// The sending side of one copy, kept by the [`Segment`].
struct CopyTx {
    tx: mpsc::Sender<SegItem>,
    budget: Arc<Semaphore>,
    ctl: Arc<CopyCtl>,
}

/// Where one copy of a segment goes.
pub(crate) struct CopyTarget {
    pub(crate) conn: ConnHandle,
    pub(crate) path: Arc<str>,
    /// Chunk bytes this receiver's copies may hold unwritten.
    pub(crate) budget: Arc<Semaphore>,
    /// The receiver's own latency budget; `None` for a `Peer::segment` copy.
    pub(crate) max_age: Option<Duration>,
}

/// Opens segment `number` on `topic` toward every target, under `terms`.
///
/// The caller took `number` and superseded the previous segment on `topics`
/// already; the copies are tracked there so the next segment supersedes
/// them.
pub(crate) fn open_segment(
    topics: &SegmentTopics,
    drops: &Arc<DropTable>,
    topic: Arc<str>,
    number: u64,
    terms: &SegmentTerms,
    targets: Vec<CopyTarget>,
) -> Segment {
    let mut copies = Vec::with_capacity(targets.len());
    for target in targets {
        let (tx, rx) = mpsc::channel(COPY_QUEUE);
        let ctl = Arc::new(CopyCtl {
            follows_upstream: terms.follows_upstream,
            ..CopyCtl::default()
        });
        topics.track(&topic, &ctl);
        // Expiry is the smaller of the sender's and the receiver's budget,
        // MOQT's "smaller non-zero value"
        // ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
        // §4.2).
        let max_age = match (terms.max_age, target.max_age) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let deadline = max_age.map(|age| Box::pin(target.conn.exec.sleep(age)));
        let exec = target.conn.exec.clone();
        exec.spawn(segment_copy(
            target.conn,
            Copy {
                path: target.path,
                topic: Arc::clone(&topic),
                number,
                priority: terms.priority,
                ctl: Arc::clone(&ctl),
                budget: Arc::clone(&target.budget),
                drops: Arc::clone(drops),
            },
            rx,
            deadline,
        ));
        copies.push(CopyTx {
            tx,
            budget: target.budget,
            ctl,
        });
    }
    Segment {
        number,
        topic,
        copies,
        drops: Arc::clone(drops),
    }
}

/// One segment, open on every receiver it was opened toward.
///
/// [`Segment::write`] never waits: a receiver without room for a chunk loses
/// the segment, counted. Dropping a segment without [`Segment::finish`]
/// resets every copy, so no receiver mistakes a partial segment for a whole
/// one.
pub struct Segment {
    number: u64,
    topic: Arc<str>,
    copies: Vec<CopyTx>,
    drops: Arc<DropTable>,
}

impl Segment {
    /// This segment's number on its topic.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The topic this segment belongs to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Hands `chunk` to every copy still open; returns how many that is.
    ///
    /// A copy whose receiver has no budget or queue room left for the chunk
    /// is dropped and counted, and its receiver loses this segment. Fails
    /// with [`Error::LimitExceeded`] for a chunk above 4 GiB, which no
    /// budget can account.
    pub fn write(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        let chunk = chunk.into();
        let len = u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)?;
        let (topic, drops) = (&self.topic, &self.drops);
        self.copies.retain(|copy| {
            let Ok(permit) = copy.budget.try_acquire_many(len) else {
                drops.record(topic, DropCause::SubscriberBudget);
                return false;
            };
            match copy.tx.try_send(SegItem::Chunk(chunk.clone())) {
                Ok(()) => {
                    // Given back by the writer once the bytes are written.
                    permit.forget();
                    copy.ctl.total.fetch_add(u64::from(len), Ordering::AcqRel);
                    true
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    drops.record(topic, DropCause::SubscriberQueue);
                    false
                }
                // The copy already ended — superseded or expired — and its
                // task counted why.
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
        Ok(self.copies.len())
    }

    /// Ends the segment on every copy still open; returns how many that is.
    pub fn finish(mut self) -> usize {
        let copies = std::mem::take(&mut self.copies);
        let mut finished = 0;
        for copy in copies {
            match copy.tx.try_send(SegItem::Finish) {
                Ok(()) => {
                    // Stored before the successor can open: the caller opens
                    // it only after `finish` returns. A FIN that found the
                    // queue full leaves the copy unfinished, so its successor
                    // resets it at once.
                    copy.ctl.finished.store(true, Ordering::Release);
                    finished += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.drops.record(&self.topic, DropCause::SubscriberQueue);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        finished
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("topic", &self.topic)
            .field("number", &self.number)
            .field("copies", &self.copies.len())
            .finish()
    }
}

/// What a copy's task needs besides its channel and deadline.
struct Copy {
    path: Arc<str>,
    topic: Arc<str>,
    number: u64,
    priority: i16,
    ctl: Arc<CopyCtl>,
    budget: Arc<Semaphore>,
    drops: Arc<DropTable>,
}

/// How a copy's race ended.
enum Stop {
    Superseded,
    Expired,
}

async fn expiry(deadline: &mut Option<Pin<Box<tokio::time::Sleep>>>) {
    match deadline {
        Some(sleep) => sleep.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Resolves when the copy is superseded while its writer is still writing
/// the segment: such a copy is reset at once. A copy whose writer finished
/// before the successor opened, and one that follows upstream, go on: see
/// [`run_copy`].
async fn superseded_unfinished(ctl: &CopyCtl) {
    ctl.superseded().await;
    if ctl.follows_upstream || ctl.finished.load(Ordering::Acquire) {
        std::future::pending::<()>().await;
    }
}

/// Resolves when a copy past its FIN has had its grace after supersession:
/// the time its path needs to carry its bytes at the congestion window, one
/// round trip for the receipt and [`SUPERSEDE_SLACK`].
async fn finish_grace_over(ctl: &CopyCtl, conn: &ConnHandle) {
    ctl.superseded().await;
    let wait = conn
        .conn
        .transport_stats()
        .map(|t| t.path)
        .map_or(Duration::ZERO, |p| {
            finish_grace(p.rtt, p.cwnd, ctl.total.load(Ordering::Acquire))
        });
    conn.exec.sleep(wait).await;
}

/// Writes one receiver's copy of one segment, racing every step against
/// supersession and the deadline.
async fn segment_copy(
    conn: ConnHandle,
    copy: Copy,
    mut rx: mpsc::Receiver<SegItem>,
    mut deadline: Option<Pin<Box<tokio::time::Sleep>>>,
) {
    let stop = run_copy(&conn, &copy, &mut rx, &mut deadline).await;
    match stop {
        Some(Stop::Superseded) => copy.drops.record(&copy.topic, DropCause::Superseded),
        Some(Stop::Expired) => copy.drops.record(&copy.topic, DropCause::Expired),
        None => {}
    }
    copy.ctl.ended.store(true, Ordering::Release);
    // Chunks still queued were charged to the budget and will never be
    // written: give their bytes back.
    rx.close();
    while let Ok(item) = rx.try_recv() {
        if let SegItem::Chunk(chunk) = item {
            copy.budget.add_permits(chunk.len());
        }
    }
}

/// Opens and writes one copy. Supersession takes a copy whose segment the
/// writer had not finished at once. A copy whose writer finished first, or
/// that follows upstream, is not stalled merely because its successor
/// opened: it hands its chunks to the transport if the transport takes them
/// at once — a write the receiver's flow control holds back means a stalled
/// receiver, and the copy is reset — and once its FIN is written it gets
/// [`finish_grace_over`] before the reset.
async fn run_copy(
    conn: &ConnHandle,
    copy: &Copy,
    rx: &mut mpsc::Receiver<SegItem>,
    deadline: &mut Option<Pin<Box<tokio::time::Sleep>>>,
) -> Option<Stop> {
    let ctl = &copy.ctl;
    let opened = tokio::select! {
        biased;
        () = superseded_unfinished(ctl) => return Some(Stop::Superseded),
        () = expiry(deadline) => return Some(Stop::Expired),
        opened = open_copy(conn, copy) => opened,
    };
    let mut stream = match opened {
        Ok(stream) => stream,
        Err(Error::NoParkedConnection) => {
            copy.drops
                .record(&copy.topic, DropCause::NoParkedConnection);
            return None;
        }
        Err(e) => {
            tracing::debug!(error = %e, "a segment copy could not be opened");
            return None;
        }
    };
    loop {
        let item = tokio::select! {
            biased;
            () = superseded_unfinished(ctl) => return reset(&mut stream, Stop::Superseded),
            () = expiry(deadline) => return reset(&mut stream, Stop::Expired),
            item = rx.recv() => item,
        };
        match item {
            Some(SegItem::Chunk(chunk)) => {
                // The write first: supersession only takes a write that
                // has to wait.
                let written = tokio::select! {
                    biased;
                    written = stream.write_all(&chunk) => Ok(written),
                    () = ctl.superseded() => Err(Stop::Superseded),
                    () = expiry(deadline) => Err(Stop::Expired),
                };
                copy.budget.add_permits(chunk.len());
                match written {
                    Ok(Ok(())) => {}
                    // The receiver refused the stream or went away.
                    Ok(Err(_)) => return None,
                    Err(stop) => return reset(&mut stream, stop),
                }
            }
            Some(SegItem::Finish) => {
                if stream.finish().is_err() {
                    return None;
                }
                // Until the receiver's transport holds every byte, the copy
                // can still be superseded or expire: a reset is accepted
                // after FIN until then.
                return tokio::select! {
                    biased;
                    () = expiry(deadline) => reset(&mut stream, Stop::Expired),
                    receipt = stream.stopped() => acked(ctl, receipt),
                    () = finish_grace_over(ctl, conn) => reset(&mut stream, Stop::Superseded),
                };
            }
            // The segment was dropped unfinished, or this copy was dropped
            // for its budget or queue, which the writer already counted.
            None => {
                stream.reset(codes::CANCELED);
                return None;
            }
        }
    }
}

fn reset(stream: &mut SendHalf, stop: Stop) -> Option<Stop> {
    stream.reset(codes::CANCELED);
    Some(stop)
}

/// Records a finished copy's receipt: delivered means acknowledged, which no
/// successor supersedes any more.
fn acked(ctl: &CopyCtl, receipt: Result<Option<u64>, Error>) -> Option<Stop> {
    if matches!(receipt, Ok(None)) {
        ctl.acked.store(true, Ordering::Release);
    }
    None
}

/// Opens one copy's stream: the DATA header names the path, the topic and
/// the segment number, and the stream gets its layer-major priority before
/// any byte is queued, the only time `quinn` guarantees it takes effect.
async fn open_copy(conn: &ConnHandle, copy: &Copy) -> Result<SendHalf, Error> {
    let header = DataHeader {
        topic: Some(copy.topic.to_string()),
        segment: Some(copy.number),
        ..DataHeader::addressed(copy.path.as_ref())
    };
    let mut stream = conn.open_uni().await?;
    stream.set_priority(copy_priority(0, copy.priority));
    write_data_preamble(&mut stream, &header).await?;
    Ok(stream)
}
