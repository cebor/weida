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
    /// What this message charges against the queue's budget.
    pub fn charge(&self) -> usize {
        PER_MESSAGE_OVERHEAD
            + self.body.len()
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

    /// How many payload bytes would still fit, given what a message's labels
    /// already cost.
    ///
    /// This is the cap the payload is *read* under, so a producer's advisory
    /// `content_len` never decides how much is buffered — the queue's own
    /// remaining budget does. Zero means the next message cannot be admitted
    /// at all, however small its payload.
    pub fn room_for(&self, meta: &IncomingMeta) -> usize {
        let labels = PER_MESSAGE_OVERHEAD
            + meta.topic.as_ref().map_or(0, |t| t.len())
            + meta.content_type.as_ref().map_or(0, |t| t.len());
        self.budget.saturating_sub(self.charged + labels)
    }

    /// Admits a message, or refuses it because the queue is full.
    ///
    /// The authority on the bound: [`Queue::room_for`] is what the payload was
    /// read under, but two producers can pass that check concurrently, so the
    /// charge is re-checked here under the same lock that mutates the queue.
    pub fn push(&mut self, message: QueuedMessage) -> Result<(), Refusal> {
        let charge = message.charge();
        if self.charged + charge > self.budget {
            return Err(Refusal::Full);
        }
        self.charged += charge;
        self.messages.push_back(message);
        Ok(())
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
        assert_eq!(queue.room_for(&empty_meta()), PER_MESSAGE_OVERHEAD + 100);
        queue.push(message(60)).expect("within budget");
        assert_eq!(queue.room_for(&empty_meta()), 40);
        queue.push(message(40)).expect("exactly the remaining room");
        assert_eq!(queue.room_for(&empty_meta()), 0);
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
