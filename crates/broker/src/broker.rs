//! The broker: queues at endpoint paths, and admission into them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinSet;
use weida::{
    Acceptor, Acknowledgement, ConsumerId, CursorLevel, Error, ErrorCode, Incoming, IncomingMeta,
    IncomingRequest, IncomingTransfer, Listener, Reporter, TransferMeta,
};

use crate::consumers::Consumers;

use crate::queue::{Queue, QueueStats, QueuedMessage, Refusal};

/// What this broker can certify about a message, and the list is the whole
/// list.
///
/// One variant, on purpose. `Stored(Written)` is defined as surviving "the
/// broker process dying: crash, restart or orderly stop", and this broker
/// holds every message in memory, so reporting it would be a prohibited claim
/// rather than an optimistic one —
/// "`Stored` MUST NOT be reported for an in-memory buffer"
/// (`docs/GUARANTEES.md` §1,
/// [0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
/// §4.2). The enum is the enforcement: there is no value of this type that
/// names a durable level, so no code path here can accidentally issue one, and
/// the variant appears when Phase 5 gives it a store to survive in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Achieved {
    /// The broker has taken responsibility for the message in memory,
    /// according to its configured policy.
    Accepted,
}

impl Achieved {
    /// The wire level this certificate is.
    pub fn level(self) -> Acknowledgement {
        match self {
            Achieved::Accepted => Acknowledgement::Accepted,
        }
    }
}

/// How a broker is configured.
///
/// Every queue this broker serves is named here: there is no declare frame and
/// no way for a peer to create, configure or delete a queue, following AMQP
/// 1.0, where node lifecycle is "entirely out of scope of the core standard"
/// (0018 §4.5). A producer addressing a path with no queue gets ERROR
/// `{UNKNOWN_ENDPOINT}` from the runtime's own dispatch, because the broker
/// never registered that path.
#[derive(Clone, Debug)]
pub struct BrokerConfig {
    /// Endpoint paths to serve as queues.
    ///
    /// Local configuration, so its length is the operator's choice — but
    /// bounded anyway by `max_queues`, because a configuration file is still
    /// input.
    pub queues: Vec<String>,
    /// Bytes one queue may hold, charged as payload plus the retained labels
    /// plus [`crate::PER_MESSAGE_OVERHEAD`] per message.
    ///
    /// Default 8 MiB, the same number `weida-zmq`'s per-peer queue budget
    /// uses: a bound big enough that a queue decouples producer from consumer,
    /// small enough that a thousand queues are a gigabyte rather than a
    /// swap storm. A queue at this bound refuses admission (§4.8); it never
    /// discards a message it has already confirmed.
    pub queue_bytes: usize,
    /// Queues this broker will register. Default 64.
    pub max_queues: usize,
    /// Deliveries one subscription may have outstanding. Default 256.
    ///
    /// **Nothing reads this field yet, and that is deliberate.** An
    /// outstanding delivery is one that has been sent and not yet
    /// acknowledged, and this broker has no acknowledgement: a delivered
    /// message leaves the queue immediately. The one number credit gives it is
    /// *cumulative* — the total a subscription will ever accept — so clamping
    /// that to 256 would cap a subscription's lifetime delivery count rather
    /// than its outstanding one, which is not what the name means and not what
    /// an operator setting it would get. B-203 is the slice that adds the
    /// acknowledgement this counts against, and it is configured here rather
    /// than invented there so the name does not move once it becomes live. The
    /// bound that holds a queue's memory down today is
    /// [`BrokerConfig::queue_bytes`], alone.
    pub max_unacked: usize,
}

impl Default for BrokerConfig {
    fn default() -> BrokerConfig {
        BrokerConfig {
            queues: Vec::new(),
            queue_bytes: 8 * 1024 * 1024,
            max_queues: 64,
            max_unacked: 256,
        }
    }
}

impl BrokerConfig {
    /// A configuration serving exactly the given queue paths.
    pub fn with_queues<I, S>(queues: I) -> BrokerConfig
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BrokerConfig {
            queues: queues.into_iter().map(Into::into).collect(),
            ..BrokerConfig::default()
        }
    }
}

/// One queue, its consumers, and the acceptor that feeds both.
struct Served {
    acceptor: Acceptor,
    queue: Mutex<Queue>,
    /// Two locks rather than one, and never held together across an `await`:
    /// a delivery takes a message under one and a turn under the other, then
    /// writes with neither held.
    consumers: Mutex<Consumers>,
}

struct Inner {
    config: BrokerConfig,
    /// Registered once, at construction, and never mutated: a queue's
    /// existence is configuration, so the map needs no lock.
    queues: HashMap<String, Arc<Served>>,
}

/// A broker: queues at endpoint paths, admitting messages into memory.
///
/// Construction registers every configured queue, so a queue exists — and a
/// producer is answered — from the moment the broker is built, before
/// [`Broker::serve`] runs. Serving is the caller's task to drive: this crate
/// starts no reactor and owns no runtime.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

impl Broker {
    /// Registers every configured queue on `listener`.
    ///
    /// Fails with [`Error::LimitExceeded`] when the configuration names more
    /// than `max_queues` queues — checked before anything is registered — and
    /// with whatever [`Listener::acceptor`] refuses for an invalid or
    /// already-claimed path.
    ///
    /// **A failure part-way through leaves the earlier paths claimed.** A
    /// listener's namespace has no public release, and inventing one to
    /// half-undo a bad configuration would be the wrong repair: a path this
    /// broker could not take is a configuration error to fix and start again
    /// with, not a transient condition to retry. The queues registered before
    /// the failure are unusable by anyone else on that listener, which is
    /// exactly as visible as it should be.
    pub fn new(listener: &Listener, config: BrokerConfig) -> Result<Broker, Error> {
        if config.queues.len() > config.max_queues {
            return Err(Error::LimitExceeded);
        }
        let mut queues = HashMap::with_capacity(config.queues.len());
        for path in &config.queues {
            let acceptor = listener.acceptor(path)?;
            queues.insert(
                path.clone(),
                Arc::new(Served {
                    acceptor,
                    queue: Mutex::new(Queue::new(config.queue_bytes)),
                    consumers: Mutex::new(Consumers::default()),
                }),
            );
        }
        Ok(Broker {
            inner: Arc::new(Inner { config, queues }),
        })
    }

    /// The configuration this broker was built with.
    pub fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    /// What a queue holds, or `None` when no queue is registered on `path`.
    pub fn stats(&self, path: &str) -> Option<QueueStats> {
        let served = self.inner.queues.get(path)?;
        Some(served.queue.lock().expect("queue mutex poisoned").stats())
    }

    /// How many subscriptions a queue serves, or `None` when no queue is
    /// registered on `path`.
    pub fn consumer_count(&self, path: &str) -> Option<usize> {
        let served = self.inner.queues.get(path)?;
        Some(
            served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .len(),
        )
    }

    /// Deliveries counted against one subscription's credit, or `None` when
    /// the queue or the subscription is unknown.
    pub fn delivered(&self, path: &str, id: ConsumerId, filter: &str) -> Option<u64> {
        let served = self.inner.queues.get(path)?;
        served
            .consumers
            .lock()
            .expect("consumer mutex poisoned")
            .delivered(id, filter)
    }

    /// Serves every queue until the listener goes away.
    ///
    /// One task per queue, because a queue is a serialization point in any
    /// case: it has one order and one budget, so admitting one message at a
    /// time is the queue's own shape rather than a limitation. Backpressure is
    /// the runtime's: a producer whose payload the broker has not started
    /// reading is stalled by QUIC flow control, not queued in a second buffer.
    pub async fn serve(&self) {
        let mut tasks = JoinSet::new();
        // The path behind each task id. A `JoinError` carries the id of the
        // task that panicked and nothing else, and "some queue died" is not an
        // answer an operator can act on.
        let mut paths = HashMap::new();
        for (path, served) in &self.inner.queues {
            let served = Arc::clone(served);
            let handle = tasks.spawn(async move { serve_queue(served).await });
            paths.insert(handle.id(), path.as_str());
        }
        while let Some(joined) = tasks.join_next_with_id().await {
            match joined {
                Ok((id, ())) => {
                    let path = paths.get(&id).copied().unwrap_or("?");
                    tracing::debug!(path, "a queue task returned");
                }
                // A panicking queue and an orderly return are the same event
                // to a `JoinSet`, and they are not the same thing here: this
                // path has stopped serving while the others carry on.
                Err(e) => {
                    let path = paths.get(&e.id()).copied().unwrap_or("?");
                    tracing::error!(path, error = %e, "a queue task died");
                }
            }
        }
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("queues", &self.inner.queues.len())
            .finish_non_exhaustive()
    }
}

/// One queue's event loop: admission, subscriptions, credit, delivery.
///
/// Everything the peer causes on this path arrives through one channel, so the
/// loop needs no lock of its own between events and a delivery scan runs after
/// whichever event could have made one possible.
///
/// **The channel does not order the peer's intent.** It orders the *tasks*
/// that reached it: every control frame rides its own unidirectional stream
/// and the accept loop spawns one task per inbound stream, so a CREDIT can and
/// does overtake the SUBSCRIBE it belongs to — the credit path only needs this
/// queue's route, while the subscription path also reserves against
/// `max_subscriptions` and builds a `Consumer`. That is why an unmatched grant
/// is held rather than dropped ([`Consumers::grant`]) and why a subscription
/// may arrive with credit already standing.
async fn serve_queue(served: Arc<Served>) {
    loop {
        match served.acceptor.accept().await {
            Ok(Incoming::Exchange(request)) => {
                admit_exchange(&served, request).await;
                pump(&served).await;
            }
            Ok(Incoming::Stream(transfer)) => {
                admit_transfer(&served, transfer).await;
                pump(&served).await;
            }
            Ok(Incoming::Subscribed(consumer)) => {
                tracing::debug!(
                    path = served.acceptor.path(),
                    filter = consumer.filter(),
                    "a consumer subscribed"
                );
                let with_credit = served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .subscribe(consumer);
                // Normally there is nothing to scan for: a fresh subscription
                // has zero credit. The exception is a grant that overtook this
                // SUBSCRIBE and was held for it — then this event is the one
                // that made a delivery possible, and no later one would.
                if with_credit {
                    tracing::debug!(
                        path = served.acceptor.path(),
                        "a grant had arrived before its subscribe; delivering now"
                    );
                    pump(&served).await;
                }
            }
            Ok(Incoming::Credit(grant)) => {
                let raised = served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .grant(grant.id, &grant.filter, grant.limit);
                if raised {
                    pump(&served).await;
                }
            }
            Ok(Incoming::Unsubscribed { id, filter }) => {
                served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .remove(id, filter.as_deref());
                // A scan, because this event can be the one that unblocks the
                // queue: the subscription that just left may be the consumer
                // whose failed write ended the last round, and the messages it
                // put back at the head are now some other consumer's to take.
                pump(&served).await;
            }
            Err(e) => {
                tracing::debug!(path = served.acceptor.path(), error = %e, "queue stopped serving");
                return;
            }
        }
    }
}

/// Delivers as far as credit and messages allow.
///
/// One message to one consumer, each as a one-way transfer, until either the
/// queue is empty or no matching subscription has credit left. That is the
/// whole scheduling policy: a queue stops at the limit its consumers stated
/// and resumes when one of them raises it
/// ([0003](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
/// §4.2).
///
/// **A failed write costs that consumer the rest of the round, and nobody
/// else anything.** The message goes back to the head of the queue and the
/// credit is given back, so retrying the same pairing immediately would spin
/// on it forever; abandoning the whole scan instead — which is what this used
/// to do — stranded every other message and every other consumer until some
/// producer happened to admit another message, which on an idle queue is
/// never. So the failing subscription is excluded for the remainder of this
/// scan and the scan continues. The exclusion list is the termination
/// argument: every iteration either delivers a message or removes one
/// subscription from consideration, and both are finite.
///
/// **Serialized with admission, deliberately and at a cost.** A queue is one
/// order and one budget, so its events are one loop; the cost is that a
/// consumer whose flow-control window is full holds this queue's producers up
/// while a write waits. Nothing here invents a timeout to paper over it: the
/// answer is the acknowledgement deadline and the requeue of B-203, which is
/// where an unresponsive consumer stops being this loop's problem.
async fn pump(served: &Served) {
    // Empty in the ordinary case, so a round that delivers costs no
    // allocation for the failures it does not have.
    let mut failed: Vec<(ConsumerId, String)> = Vec::new();
    loop {
        let Some(next) = next_delivery(served, &failed) else {
            return;
        };
        let (id, filter, consumer, message) = next;
        let meta = TransferMeta {
            content_type: message.content_type.clone(),
            content_len: Some(message.body.len() as u64),
            trace: message.trace,
            topic: message.topic.clone(),
            achieved: None,
            ..TransferMeta::default()
        };
        if let Err(e) = consumer.deliver(meta, &message.body).await {
            // The write never landed: give the credit back, put the message
            // where it was — at the head of the queue — and carry on without
            // this consumer. Its subscription is removed for good by its own
            // `Unsubscribed` event, which pumps again.
            tracing::debug!(
                path = served.acceptor.path(),
                error = %e,
                "a delivery failed; requeuing the message and skipping the consumer"
            );
            served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .undo(id, &filter);
            served
                .queue
                .lock()
                .expect("queue mutex poisoned")
                .push_front(message);
            failed.push((id, filter));
        }
    }
}

/// Takes the first message some consumer may have, and that consumer's turn.
///
/// Both locks are taken here and released before the caller writes, always in
/// this order — queue, then consumers — so two queues cannot deadlock against
/// each other. A message is taken **only** when a consumer for it exists: a
/// queue with no credit keeps its messages, which is the point of a queue.
///
/// The credit pre-check is what keeps a filling queue linear. Without it, this
/// scan asked every subscription about every held message after every
/// admission — Θ(N²) eligible-set constructions for a queue nobody consumes,
/// with N bounded only by `queue_bytes / PER_MESSAGE_OVERHEAD` and driven
/// entirely by a remote producer. One pass over the subscriptions, bounded by
/// `max_subscriptions`, answers the whole question in that case.
fn next_delivery(
    served: &Served,
    skip: &[(ConsumerId, String)],
) -> Option<(ConsumerId, String, weida::Consumer, QueuedMessage)> {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    let mut consumers = served.consumers.lock().expect("consumer mutex poisoned");
    if !consumers.any_credit() {
        return None;
    }
    for index in 0..queue.len() {
        // The topic is borrowed for exactly the length of the question, so a
        // scan that finds nothing allocates nothing.
        let turn = {
            let topic = queue.topic_at(index).expect("index is in range");
            consumers.take_turn(topic, skip)
        };
        let Some((id, filter, consumer)) = turn else {
            continue;
        };
        let message = queue.take(index).expect("index is in range");
        return Some((id, filter, consumer, message));
    }
    None
}

/// Admits a producer's exchange and confirms it, or refuses it on the reply
/// half.
///
/// The confirm is the reply: DATA carrying the achieved level and a FIN, no
/// payload. An exchange already has a reply half, so a publisher confirm needs
/// no frame kind of its own — and the stream is the correlation, so the confirm
/// names nothing (0018 §4.6).
async fn admit_exchange(served: &Served, mut request: IncomingRequest) {
    let Some(room) = room_for(served, request.meta()) else {
        // Refused before a byte is read, which is what `Reject` backpressure
        // means; `refuse` stops the request half too, so a producer still
        // writing stops instead of filling a window nobody will read. The
        // question is [`Queue::push`]'s own arithmetic asked in advance, so a
        // message that fits the queue exactly is admitted with an empty
        // payload rather than refused for having a read cap of zero.
        request.refuse(ErrorCode::Rejected).await;
        return;
    };
    let body = request.take_body();
    // Taken before the payload is consumed: `reporter()` reads the order the
    // producer put in its DATA header, and `read_message` consumes the
    // transfer.
    let reporter = body.reporter();
    let queued = match read_message(body, request.meta(), room).await {
        Ok(message) => message,
        Err(ReadFailed::TooLarge) => {
            request.refuse(ErrorCode::Rejected).await;
            return;
        }
        Err(ReadFailed::Broken) => return,
    };
    let admitted_bytes = queued.body.len() as u64;
    // The producer's trace, logged where it is still available: after the
    // exchange ends nothing can reconstruct which trace a queued message
    // belongs to, and a delivery continues it (B-202).
    if let Some(trace) = queued.trace {
        tracing::debug!(
            path = served.acceptor.path(),
            bytes = queued.body.len(),
            trace_id = %trace.trace_id_hex(),
            "admitted a message into a queue"
        );
    }
    // The guard ends with this statement, on purpose: holding a `std::sync`
    // lock across the confirm's `await` would make the whole loop `!Send` and,
    // worse, hold a queue shut for a network write.
    let admitted = served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .push(queued);
    match admitted {
        Ok(()) => {
            // Both answers, and they say different things. The reply half is
            // the application's channel and carries the confirm it always
            // carried (DATA key `8`); the cursor is for a producer that
            // ordered one, and a producer that ordered none pays nothing.
            report_accepted(reporter, admitted_bytes);
            confirm(request).await;
        }
        // A refusal reports **nothing**: the producer learns from the
        // refusal, not from a missing cursor.
        Err(Refusal::Full) => request.refuse(ErrorCode::Rejected).await,
    }
}

/// Admits a one-way transfer, and reports a cursor if the producer ordered
/// one.
///
/// This is the shape [0024](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0024-three-families-one-back-channel.md)
/// §4.4a exists for: a producer that orders `Accepted` gets a reliable verdict
/// **on one unidirectional payload stream**, with no exchange and no reply
/// half. A producer that orders nothing gets the transport receipt and nothing
/// more — the honest spelling of `acks=0` (0018 §4.6) — and its transfer is
/// byte-for-byte what it was before cursors existed. A refusal has nowhere to
/// go on a unidirectional stream, so a queue with no room is a
/// `STOP_SENDING(REJECTED)` — dropping the unread transfer — and no cursor.
async fn admit_transfer(served: &Served, transfer: IncomingTransfer) {
    let Some(room) = room_for(served, transfer.meta()) else {
        // Dropping an unread payload is the refusal: the handle stops the
        // stream with `REJECTED`, which is the only answer a unidirectional
        // stream can carry.
        drop(transfer);
        return;
    };
    let meta = transfer.meta().clone();
    let reporter = transfer.reporter();
    let queued = match read_message(transfer, &meta, room).await {
        Ok(message) => message,
        Err(_) => return,
    };
    let admitted_bytes = queued.body.len() as u64;
    if let Err(Refusal::Full) = served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .push(queued)
    {
        // Not a race: a queue's events are one loop, so no second admission
        // ran between the room question and this push. What is left is the
        // payload's own slack — [`QueuedMessage::charge`] counts the `Vec`'s
        // allocation, and a payload read up to the cap may have reserved past
        // it. The payload is already read, so there is no stream left to stop
        // and **nothing is sent**: the producer that ordered a cursor sees it
        // never arrive, which is the only answer a one-way stream has left
        // once its FIN is in.
        tracing::debug!(
            path = served.acceptor.path(),
            bytes = admitted_bytes,
            "dropping a one-way message whose allocation overran the queue's budget"
        );
        return;
    }
    report_accepted(reporter, admitted_bytes);
}

/// How long a spawned report may wait for the producer's stream credit.
///
/// Not a correctness parameter: a cursor is best effort and nothing waits on
/// it. It bounds how long a task may sit parked in `open_uni` for a peer that
/// advertised `initial_max_streams_uni = 0` and never raised it — without it,
/// one parked task per admitted message would be an allocation a remote
/// producer drives and only shutdown releases.
const REPORT_DEADLINE: Duration = Duration::from_secs(10);

/// Reports `Accepted` at the admitted body length, once, and FINs — in a task
/// of its own.
///
/// `Accepted` is a **verdict rather than a prefix** (0023 §4.2): it is true of
/// the whole message or of none of it, so there is exactly one record even in
/// `Progress` mode and there is no granularity to configure. A level this
/// broker cannot reach — `Stored`, with no store — is simply not reported, and
/// the producer sees it missing rather than failed.
///
/// **The one thing in this loop that may be abandoned, and the one thing that
/// must not be awaited in it.** `Reporter::report` opens the cursor stream
/// lazily, and opening a stream blocks until the peer raises
/// `max_concurrent_uni_streams`, with the deadline left to the caller. Awaited
/// from [`serve_queue`] — the only driver of this queue — a single producer
/// that orders `Accepted` and never raises its stream limit would park
/// admission for every other producer and delivery for every consumer on the
/// path, permanently. A cursor is best effort by construction and nothing
/// waits on it, so it is the one thing here that may be handed to a task with
/// a deadline and forgotten: losing it costs the producer a record it was
/// never promised, while parking the loop costs everyone the queue.
fn report_accepted(reporter: Option<Reporter>, bytes: u64) {
    let Some(mut reporter) = reporter else {
        return;
    };
    tokio::spawn(async move {
        let report = async move {
            let level = CursorLevel::Known(Achieved::Accepted.level());
            if let Err(e) = reporter.report(level, bytes).await {
                tracing::debug!(error = %e, "failed to report an accepted cursor");
            }
            if let Err(e) = reporter.finish().await {
                tracing::debug!(error = %e, "failed to finish a cursor report");
            }
        };
        let done = tokio::time::timeout(REPORT_DEADLINE, report).await;
        if done.is_err() {
            tracing::debug!("gave up on a cursor: the peer opened no stream for it");
        }
    });
}

/// Why a payload did not become a queued message.
enum ReadFailed {
    /// It exceeded the room the queue had; nothing was buffered past the cap.
    TooLarge,
    /// The peer or the connection failed mid-payload.
    Broken,
}

/// Reads a payload under the queue's remaining budget.
async fn read_message(
    body: IncomingTransfer,
    meta: &IncomingMeta,
    room: usize,
) -> Result<QueuedMessage, ReadFailed> {
    let topic = meta.topic.clone();
    let content_type = meta.content_type.clone();
    let trace = meta.trace;
    match body.collect(room).await {
        Ok(body) => Ok(QueuedMessage {
            body,
            topic,
            content_type,
            trace,
        }),
        Err(Error::LimitExceeded) => Err(ReadFailed::TooLarge),
        Err(e) => {
            tracing::debug!(error = %e, "a producer's payload failed mid-read");
            Err(ReadFailed::Broken)
        }
    }
}

/// The payload cap for the next message on this queue, or `None` when the
/// queue has no room for a message at all.
fn room_for(served: &Served, meta: &IncomingMeta) -> Option<usize> {
    served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .room_for(meta)
}

/// Writes the confirm: the achieved level in the reply header, then FIN.
async fn confirm(request: IncomingRequest) {
    let meta = TransferMeta::default().with_achieved(Achieved::Accepted.level());
    match request.reply(meta).await {
        Ok(reply) => {
            if let Err(e) = reply.finish() {
                tracing::debug!(error = %e, "failed to finish a confirm");
            }
        }
        Err(e) => tracing::debug!(error = %e, "failed to send a confirm"),
    }
}
