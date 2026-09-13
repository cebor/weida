//! Consumers of one queue, and the credit each of them granted.
//!
//! A queue delivers to **one** consumer per message, which is the whole
//! difference from a publisher's fan-out, so it has to know its consumers
//! individually rather than as a set to copy to. What it keeps per
//! subscription is two numbers — the absolute limit the consumer stated and
//! the count already delivered — and the handle to write with.

use std::collections::HashMap;

use weida::{Consumer, ConsumerId};

/// One subscription: a consumer, its filter, and its credit.
struct Subscription {
    consumer: Consumer,
    /// Highest absolute limit this subscription has granted.
    ///
    /// Monotone on purpose: a credit frame rides its own unidirectional
    /// stream, and QUIC orders no stream against another, so a grant that
    /// arrives late would otherwise lower a limit the consumer has already
    /// raised. Keeping the maximum is what makes a duplicated or reordered
    /// grant change nothing
    /// ([0003](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
    /// §4.3).
    limit: u64,
    /// Messages delivered on this subscription since it was created.
    delivered: u64,
}

impl Subscription {
    /// Is there room under the limit for one more delivery?
    fn has_credit(&self) -> bool {
        self.delivered < self.limit
    }
}

/// Every subscription on one queue.
///
/// Bounded by what the runtime already bounds: `max_connections` connections,
/// each holding at most `max_subscriptions` filters
/// (`docs/PROTOCOL.md` §10), and a closed connection takes its entries with
/// it.
#[derive(Default)]
pub(crate) struct Consumers {
    subs: HashMap<(ConsumerId, String), Subscription>,
    /// Where the next round-robin scan starts, so two consumers with credit
    /// share a queue's messages instead of one of them taking all of them.
    ///
    /// A counter rather than an index into the map: the map's iteration order
    /// is arbitrary but stable between mutations, and all this has to do is
    /// not pick the same entry every time.
    cursor: usize,
}

impl Consumers {
    /// Records a new subscription with **zero** credit.
    ///
    /// Zero is the initial value the whole scheme rests on: a consumer that
    /// subscribes and grants nothing receives nothing, which is the only
    /// default that cannot surprise it with a flood (AMQP 1.0's initial link
    /// credit, [0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
    /// §4.4). A repeated SUBSCRIBE for the same filter keeps the credit and
    /// the delivered count: it is idempotent, and forgetting the count would
    /// hand a consumer free credit by re-subscribing.
    pub(crate) fn subscribe(&mut self, consumer: Consumer) {
        let key = (consumer.id(), consumer.filter().to_owned());
        self.subs.entry(key).or_insert(Subscription {
            consumer,
            limit: 0,
            delivered: 0,
        });
    }

    /// Applies a grant. An unknown subscription is ignored: a grant that
    /// overtakes its own SUBSCRIBE would otherwise create a subscription
    /// nobody asked for, and the consumer will restate it.
    ///
    /// Returns whether anything changed, so a caller can skip a delivery scan
    /// that cannot succeed.
    pub(crate) fn grant(&mut self, id: ConsumerId, filter: &str, limit: u64) -> bool {
        let Some(sub) = self.subs.get_mut(&(id, filter.to_owned())) else {
            return false;
        };
        if limit <= sub.limit {
            return false;
        }
        sub.limit = limit;
        true
    }

    /// Removes one subscription, or every subscription of one connection when
    /// `filter` is `None`.
    pub(crate) fn remove(&mut self, id: ConsumerId, filter: Option<&str>) {
        match filter {
            Some(filter) => {
                self.subs.remove(&(id, filter.to_owned()));
            }
            None => self.subs.retain(|(held, _), _| *held != id),
        }
    }

    /// Picks the next subscription that may take a message on `topic`, and
    /// counts the delivery against its credit.
    ///
    /// Round-robin over the subscriptions with credit whose filter matches, so
    /// two consumers of one queue share its messages. Returns the handle to
    /// deliver with; [`Consumers::undo`] gives the credit back if the write
    /// fails.
    pub(crate) fn take_turn(&mut self, topic: &str) -> Option<(ConsumerId, String, Consumer)> {
        let mut eligible: Vec<&(ConsumerId, String)> = self
            .subs
            .iter()
            .filter(|((_, filter), sub)| sub.has_credit() && weida::filter::matches(topic, filter))
            .map(|(key, _)| key)
            .collect();
        if eligible.is_empty() {
            return None;
        }
        // Sorted so the cursor means the same thing between calls: the map's
        // own order is unspecified and changes as entries are added.
        eligible.sort();
        let key = eligible[self.cursor % eligible.len()].clone();
        self.cursor = self.cursor.wrapping_add(1);
        let sub = self.subs.get_mut(&key).expect("key came from the map");
        sub.delivered += 1;
        Some((key.0, key.1, sub.consumer.clone()))
    }

    /// Gives one counted delivery back, for a write that never landed.
    pub(crate) fn undo(&mut self, id: ConsumerId, filter: &str) {
        if let Some(sub) = self.subs.get_mut(&(id, filter.to_owned())) {
            sub.delivered = sub.delivered.saturating_sub(1);
        }
    }

    /// How many subscriptions this queue serves.
    pub(crate) fn len(&self) -> usize {
        self.subs.len()
    }

    /// Deliveries counted on one subscription, for the broker's own stats.
    pub(crate) fn delivered(&self, id: ConsumerId, filter: &str) -> Option<u64> {
        self.subs
            .get(&(id, filter.to_owned()))
            .map(|sub| sub.delivered)
    }
}
