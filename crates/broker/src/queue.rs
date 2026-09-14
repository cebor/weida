//! One queue: messages held in memory, under one byte budget.
//!
//! Nothing here talks to a peer. The queue answers two questions — is there
//! room, and what is held — and the broker turns those answers into a confirm
//! or a refusal. It holds no state a restart could recover, which is why
//! `Stored` is not expressible anywhere in this crate
//! ([0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
//! §4.2).

use std::collections::VecDeque;

use weida::{IncomingMeta, TraceContext};

/// What one queued message costs beyond its payload and its retained labels.
///
/// A budget that counted payload bytes only would bound nothing for a producer
/// sending empty messages: a million zero-byte messages are zero bytes of
/// payload and a million live `QueuedMessage`s. Charging a fixed overhead per
/// message makes `queue_bytes` bound the message *count* as well —
/// `queue_bytes / PER_MESSAGE_OVERHEAD` is the ceiling — which is what
/// `docs/INVARIANTS.md` asks of any allocation a remote peer can drive.
///
/// The number is a deliberate over-estimate of `QueuedMessage`'s own
/// footprint (three `Option`s, a `VecDeque` slot and an allocation header), not
/// a measurement: it is a bound, and a bound that tracks `size_of` would move
/// with every field added here.
pub const PER_MESSAGE_OVERHEAD: usize = 256;

/// One message a queue holds.
///
/// The payload, plus what a delivery will need when B-202 adds one: the topic
/// a consumer's filter selects on, the content type the producer labelled it
/// with, and the trace context, so that a delivery continues the producer's
/// trace instead of starting a new one.
#[derive(Debug)]
pub struct QueuedMessage {
    /// The payload, as read.
    pub body: Vec<u8>,
    /// The topic the producer addressed inside the queue's path, if any.
    pub topic: Option<String>,
    /// The producer's content-type label, forwarded unmodified.
    pub content_type: Option<String>,
    /// The producer's trace context, so a delivery is the same trace.
    pub trace: Option<TraceContext>,
}

impl QueuedMessage {
    /// What this message charges against the queue's budget: its
    /// **allocation**, not its payload.
    ///
    /// `body.capacity()` rather than `body.len()`, because a payload arrives
    /// through `IncomingTransfer::collect`, which grows a `Vec` from empty in
    /// 64 KiB reads — so a 2 MiB + 64 KiB message comes to rest in a 4 MiB
    /// allocation. Charging the length would let a queue hold close to twice
    /// `queue_bytes` of resident payload, with the factor chosen by the
    /// producer's message size, and `docs/INVARIANTS.md` states that number as
    /// the bound on what a queue *holds*. The slack is real memory; it is
    /// charged to whoever caused it.
    ///
    /// [`Queue::push`], [`Queue::take`] and [`Queue::push_front`] all
    /// recompute this from the same `Vec`, so an add and its release are the
    /// same number and the running total cannot drift.
    pub fn charge(&self) -> usize {
        PER_MESSAGE_OVERHEAD
            + self.body.capacity()
            + self.topic.as_ref().map_or(0, |t| t.len())
            + self.content_type.as_ref().map_or(0, |t| t.len())
    }
}

/// Why a message was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The queue is at `queue_bytes`.
    ///
    /// The only refusal admission has: `Reject` backpressure, "a cap refused
    /// before buffering", answered with ERROR `{REJECTED}` on the reply half
    /// (0018 §4.8).
    Full,
}

/// What a queue holds right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueStats {
    /// Messages held.
    pub messages: usize,
    /// Bytes charged against `queue_bytes`, including the per-message
    /// overhead of [`PER_MESSAGE_OVERHEAD`].
    pub bytes: usize,
}

/// A queue: a FIFO of messages and the budget they are charged against.
#[derive(Debug)]
pub struct Queue {
    messages: VecDeque<QueuedMessage>,
    /// Sum of [`QueuedMessage::charge`] over `messages`, maintained on both
    /// sides rather than recomputed: admission asks for it per message.
    charged: usize,
    budget: usize,
}

impl Queue {
    /// An empty queue with `budget` bytes.
    pub fn new(budget: usize) -> Queue {
        Queue {
            messages: VecDeque::new(),
            charged: 0,
            budget,
        }
    }

    /// How many payload bytes would still fit, or `None` when no message fits
    /// at all.
    ///
    /// `Some(n)` is the cap the payload is *read* under, so a producer's
    /// advisory `content_len` never decides how much is buffered — the queue's
    /// own remaining budget does. `Some(0)` is a real answer: a message whose
    /// labels and overhead fill the budget exactly is admitted with an empty
    /// payload, and [`Queue::push`] accepts it. `None` is the refusal, and it
    /// is [`Queue::push`]'s own arithmetic read backwards — `charged + labels`
    /// already past the budget — so the two cannot disagree at the boundary.
    ///
    /// The one thing this cannot promise is that a payload of exactly `n`
    /// bytes is admitted: [`QueuedMessage::charge`] counts the allocation, and
    /// a `Vec` that ends at `n` bytes may have reserved more. That is the
    /// whole remaining gap between this answer and `push`'s, and it is stated
    /// where `push` refuses.
    pub fn room_for(&self, meta: &IncomingMeta) -> Option<usize> {
        let labels = PER_MESSAGE_OVERHEAD
            + meta.topic.as_ref().map_or(0, |t| t.len())
            + meta.content_type.as_ref().map_or(0, |t| t.len());
        self.budget.checked_sub(self.charged + labels)
    }

    /// Admits a message, or refuses it because the queue is full.
    ///
    /// The authority on the bound, and the only one: [`Queue::room_for`] is
    /// this same comparison asked before the payload is read, so a message
    /// that passed that check is refused here only when its `Vec` reserved
    /// more than it was read under ([`QueuedMessage::charge`]). Nothing else
    /// can intervene — a queue's events are one loop, so no second admission
    /// runs between the question and the answer.
    pub fn push(&mut self, message: QueuedMessage) -> Result<(), Refusal> {
        let charge = message.charge();
        if self.charged + charge > self.budget {
            return Err(Refusal::Full);
        }
        self.charged += charge;
        self.messages.push_back(message);
        Ok(())
    }

    /// How many messages are held.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// The topic of the message at `index`, or the empty topic when it has
    /// none.
    ///
    /// The empty string is not a special case: a message with no topic is
    /// matched by the empty filter — which matches every topic — and by no
    /// other, which is exactly what an unlabelled message should be selected
    /// by.
    pub fn topic_at(&self, index: usize) -> Option<&str> {
        let message = self.messages.get(index)?;
        Some(message.topic.as_deref().unwrap_or(""))
    }

    /// Takes the message at `index`, keeping the order of the rest.
    ///
    /// Delivery selects **the first message some consumer may take**, not
    /// strictly the head: with consumer-side filters a queue holds messages
    /// for several filters at once, so a head nobody subscribes for would
    /// otherwise block every message behind it forever. Scanning costs one
    /// pass over a queue whose length is bounded by
    /// `queue_bytes / PER_MESSAGE_OVERHEAD`, and the common case — one
    /// consumer with the empty filter — stops at the first message.
    pub fn take(&mut self, index: usize) -> Option<QueuedMessage> {
        let message = self.messages.remove(index)?;
        self.charged -= message.charge();
        Some(message)
    }

    /// Puts a message back where it was.
    ///
    /// For a delivery whose write never landed: it was charged against this
    /// budget a moment ago, so the charge is re-added rather than re-checked —
    /// refusing here would discard a message the broker has already confirmed,
    /// which is the one thing a queue may never do
    /// (`docs/GUARANTEES.md` §1).
    pub fn push_front(&mut self, message: QueuedMessage) {
        self.charged += message.charge();
        self.messages.push_front(message);
    }

    /// What the queue holds.
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            messages: self.messages.len(),
            bytes: self.charged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(body: usize) -> QueuedMessage {
        QueuedMessage {
            body: vec![0u8; body],
            topic: None,
            content_type: None,
            trace: None,
        }
    }

    fn empty_meta() -> IncomingMeta {
        IncomingMeta {
            endpoint: Some("/q".to_owned()),
            content_len: None,
            content_type: None,
            trace: None,
            tracestate: None,
            topic: None,
            peer: None,
            sequence: None,
            gap: None,
            achieved: None,
            report: Vec::new(),
            report_mode: weida::ReportMode::default(),
            report_id: None,
        }
    }

    #[test]
    fn an_empty_message_still_costs_the_queue_something() {
        // The whole point of the overhead: a producer of empty messages must
        // run out of budget too.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 3);
        for _ in 0..3 {
            queue.push(message(0)).expect("within budget");
        }
        assert_eq!(queue.push(message(0)), Err(Refusal::Full));
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 3,
                bytes: PER_MESSAGE_OVERHEAD * 3
            }
        );
    }

    #[test]
    fn the_read_cap_shrinks_as_the_queue_fills() {
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2 + 100);
        assert_eq!(
            queue.room_for(&empty_meta()),
            Some(PER_MESSAGE_OVERHEAD + 100)
        );
        queue.push(message(60)).expect("within budget");
        assert_eq!(queue.room_for(&empty_meta()), Some(40));
        queue.push(message(40)).expect("exactly the remaining room");
        assert_eq!(
            queue.room_for(&empty_meta()),
            None,
            "not even a message's own overhead fits now, and that is the refusal"
        );
    }

    #[test]
    fn a_message_that_fits_exactly_is_admitted_rather_than_refused() {
        // The boundary the two authorities used to disagree on: a payload cap
        // of zero says a message fits, not that the queue is full, and
        // `push` agrees.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2);
        let meta = empty_meta();
        queue.push(message(0)).expect("within budget");
        let room = queue.room_for(&meta).expect("one empty message fits");
        assert_eq!(room, 0);
        queue.push(message(room)).expect("what the cap admits");
        assert_eq!(queue.room_for(&meta), None);
    }

    #[test]
    fn a_bodys_allocation_is_charged_and_not_its_payload() {
        // A payload read in 64 KiB chunks comes to rest in a geometrically
        // grown `Vec`, so the slack is real memory the producer's message size
        // chose. Charging the length would let this queue hold both messages.
        fn slack(len: usize, capacity: usize) -> QueuedMessage {
            let mut body = Vec::with_capacity(capacity);
            body.resize(len, 0u8);
            QueuedMessage {
                body,
                topic: None,
                content_type: None,
                trace: None,
            }
        }

        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2 + 100);
        queue.push(slack(10, 100)).expect("the first fits");
        assert_eq!(
            queue.push(slack(10, 100)),
            Err(Refusal::Full),
            "two ten-byte payloads fit this budget; two hundred-byte \
             allocations do not"
        );
        // And the release is the same number the add was.
        let taken = queue.take(0).expect("the first is still held");
        assert_eq!(taken.body.len(), 10);
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 0,
                bytes: 0
            }
        );
    }

    #[test]
    fn a_label_is_charged_against_the_same_budget_as_the_payload() {
        // Otherwise a producer sending 512-byte topics and no payload would
        // hold unbounded label bytes under a payload-only budget.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD + 10);
        let labelled = QueuedMessage {
            body: Vec::new(),
            topic: Some("x".repeat(11)),
            content_type: None,
            trace: None,
        };
        assert_eq!(queue.push(labelled), Err(Refusal::Full));
    }
}
