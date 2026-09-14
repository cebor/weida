//! The broker: queues at endpoint paths, and admission into them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinSet;
use weida::{
    Acceptor, Acknowledgement, ConsumerId, Error, ErrorCode, Incoming, IncomingMeta,
    IncomingRequest, IncomingTransfer, Listener, TransferMeta,
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
    /// The ceiling on any credit the broker will honour: a consumer granting
    /// more is held to this number. Admission does not consult it — nothing is
    /// delivered in this slice — and B-202 and B-203 are the slices that
    /// enforce it, which is why it is configured here rather than invented
    /// there.
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
        for served in self.inner.queues.values() {
            let served = Arc::clone(served);
            tasks.spawn(async move { serve_queue(served).await });
        }
        while tasks.join_next().await.is_some() {}
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
/// Everything the peer causes on this path arrives through one channel and in
/// the order it was caused, so the loop needs no lock of its own between
/// events: a grant cannot overtake the SUBSCRIBE it belongs to on the same
/// connection, and a delivery scan runs after whichever event could have made
/// one possible.
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
                    "a consumer subscribed; credit is zero until it grants some"
                );
                served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .subscribe(consumer);
                // No scan: a fresh subscription has no credit, so nothing it
                // could take.
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
/// **Serialized with admission, deliberately and at a cost.** A queue is one
/// order and one budget, so its events are one loop; the cost is that a
/// consumer whose flow-control window is full holds this queue's producers up
/// while a write waits. Nothing here invents a timeout to paper over it: the
/// answer is the acknowledgement deadline and the requeue of B-203, which is
/// where an unresponsive consumer stops being this loop's problem.
async fn pump(served: &Served) {
    loop {
        let Some(next) = next_delivery(served) else {
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
        match consumer.deliver(meta, &message.body).await {
            Ok(()) => {}
            Err(e) => {
                // The write never landed: give the credit back and put the
                // message where it was, at the head of the queue. A consumer
                // whose connection is gone is removed by its own
                // `Unsubscribed` event, so this does not spin on it.
                tracing::debug!(
                    path = served.acceptor.path(),
                    error = %e,
                    "a delivery failed; requeuing the message"
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
                return;
            }
        }
    }
}

/// Takes the first message some consumer may have, and that consumer's turn.
///
/// Both locks are taken here and released before the caller writes, always in
/// this order — queue, then consumers — so two queues cannot deadlock against
/// each other. A message is taken **only** when a consumer for it exists: a
/// queue with no credit keeps its messages, which is the point of a queue.
fn next_delivery(served: &Served) -> Option<(ConsumerId, String, weida::Consumer, QueuedMessage)> {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    let mut consumers = served.consumers.lock().expect("consumer mutex poisoned");
    for index in 0..queue.len() {
        let topic = queue.topic_at(index).expect("index is in range").to_owned();
        let Some((id, filter, consumer)) = consumers.take_turn(&topic) else {
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
    let room = room_for(served, request.meta());
    if room == 0 {
        // Refused before a byte is read, which is what `Reject` backpressure
        // means; `refuse` stops the request half too, so a producer still
        // writing stops instead of filling a window nobody will read.
        request.refuse(ErrorCode::Rejected).await;
        return;
    }
    let queued = match read_message(request.take_body(), request.meta(), room).await {
        Ok(message) => message,
        Err(ReadFailed::TooLarge) => {
            request.refuse(ErrorCode::Rejected).await;
            return;
        }
        Err(ReadFailed::Broken) => return,
    };
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
        Ok(()) => confirm(request).await,
        Err(Refusal::Full) => request.refuse(ErrorCode::Rejected).await,
    }
}

/// Admits a one-way transfer: the same admission, no confirm.
///
/// A producer that will not wait for a certificate sends a one-way transfer and
/// gets the transport receipt and nothing more — the honest spelling of
/// `acks=0` (0018 §4.6). A refusal has nowhere to go on a unidirectional
/// stream, so a full queue is a `STOP_SENDING(REJECTED)` and no more.
async fn admit_transfer(served: &Served, transfer: IncomingTransfer) {
    let room = room_for(served, transfer.meta());
    let meta = transfer.meta().clone();
    if room == 0 {
        // Dropping an unread payload is the refusal: the handle stops the
        // stream with `REJECTED`, which is the only answer a unidirectional
        // stream can carry.
        drop(transfer);
        return;
    }
    let queued = match read_message(transfer, &meta, room).await {
        Ok(message) => message,
        Err(_) => return,
    };
    if let Err(Refusal::Full) = served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .push(queued)
    {
        tracing::debug!("a one-way transfer lost the race for the last bytes of a queue");
    }
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

/// The payload cap for the next message on this queue.
fn room_for(served: &Served, meta: &IncomingMeta) -> usize {
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
