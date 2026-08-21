//! Bounded correlation table.
//!
//! Used by the connection actor for pending ACKs and pending replies. The
//! capacity is the hard part: a table keyed by remote-influenced identifiers
//! must not grow without bound (master doc §50), and exceeding the local
//! capacity is local backpressure ([`Error::LimitExceeded`]) rather than a
//! connection error.
//!
//! There is deliberately no memory of resolved identifiers. A reply naming an
//! identifier that is not currently pending is indistinguishable from a
//! duplicate reply, and both get the same treatment: reset the stream with
//! `CANCELED`. Remembering resolved ids would be exactly the unbounded state
//! this type exists to avoid.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::error::Error;
use crate::id::TransferId;

/// How an inbound reply relates to the local pending set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyDisposition {
    /// A local request is waiting: hand the reply over.
    Deliver,
    /// Nothing is waiting. The reply is late, duplicated, or answers a request
    /// we canceled. Reset the stream with `CANCELED`; never a connection error.
    Reset,
}

/// A bounded map from transfer id to a locally owned waiter.
#[derive(Debug)]
pub struct Correlator<T> {
    pending: HashMap<u64, T>,
    capacity: usize,
}

impl<T> Correlator<T> {
    /// Creates a table that holds at most `capacity` entries.
    pub fn new(capacity: usize) -> Correlator<T> {
        Correlator {
            pending: HashMap::new(),
            capacity,
        }
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// True if no entry is live.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Registers a waiter.
    ///
    /// Fails with [`Error::LimitExceeded`] when the table is full and with
    /// [`Error::Protocol`] if the identifier is already registered, which would
    /// mean the local id allocator handed out a duplicate.
    pub fn register(&mut self, id: TransferId, waiter: T) -> Result<(), Error> {
        if self.pending.len() >= self.capacity {
            return Err(Error::LimitExceeded);
        }
        match self.pending.entry(id.get()) {
            Entry::Occupied(_) => Err(Error::Protocol(format!(
                "local transfer id {id} is already registered"
            ))),
            Entry::Vacant(slot) => {
                slot.insert(waiter);
                Ok(())
            }
        }
    }

    /// Takes the waiter for `id`, if any.
    pub fn take(&mut self, id: u64) -> Option<T> {
        self.pending.remove(&id)
    }

    /// Classifies an inbound reply for `correlation_id` without removing it.
    pub fn classify(&self, correlation_id: u64) -> ReplyDisposition {
        if self.pending.contains_key(&correlation_id) {
            ReplyDisposition::Deliver
        } else {
            ReplyDisposition::Reset
        }
    }

    /// Removes and returns every waiter, for connection teardown.
    pub fn drain(&mut self) -> Vec<(u64, T)> {
        self.pending.drain().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(v: u64) -> TransferId {
        TransferId::new(v).expect("non-zero")
    }

    #[test]
    fn register_take_roundtrip() {
        let mut c: Correlator<&str> = Correlator::new(4);
        assert!(c.is_empty());
        c.register(id(1), "a").unwrap();
        c.register(id(2), "b").unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c.take(1), Some("a"));
        assert_eq!(c.take(1), None);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn capacity_is_enforced_as_local_backpressure() {
        let mut c: Correlator<u8> = Correlator::new(2);
        c.register(id(1), 1).unwrap();
        c.register(id(2), 2).unwrap();
        let err = c.register(id(3), 3).unwrap_err();
        assert!(matches!(err, Error::LimitExceeded));
        // Freeing a slot makes room again.
        c.take(1).unwrap();
        c.register(id(3), 3).unwrap();
    }

    #[test]
    fn zero_capacity_registers_nothing() {
        let mut c: Correlator<u8> = Correlator::new(0);
        assert!(matches!(c.register(id(1), 1), Err(Error::LimitExceeded)));
    }

    #[test]
    fn duplicate_registration_is_a_local_bug_not_a_silent_overwrite() {
        let mut c: Correlator<u8> = Correlator::new(4);
        c.register(id(7), 1).unwrap();
        assert!(matches!(c.register(id(7), 2), Err(Error::Protocol(_))));
        assert_eq!(c.take(7), Some(1));
    }

    #[test]
    fn unknown_and_duplicate_replies_are_reset_not_fatal() {
        let mut c: Correlator<u8> = Correlator::new(4);
        c.register(id(5), 1).unwrap();
        assert_eq!(c.classify(5), ReplyDisposition::Deliver);
        assert_eq!(c.classify(6), ReplyDisposition::Reset);
        c.take(5).unwrap();
        // The second reply for the same correlation id looks exactly like an
        // unknown one, by design.
        assert_eq!(c.classify(5), ReplyDisposition::Reset);
    }

    #[test]
    fn drain_empties_the_table_for_teardown() {
        let mut c: Correlator<u8> = Correlator::new(4);
        c.register(id(1), 10).unwrap();
        c.register(id(2), 20).unwrap();
        let mut drained = c.drain();
        drained.sort_unstable();
        assert_eq!(drained, vec![(1, 10), (2, 20)]);
        assert!(c.is_empty());
        assert_eq!(c.take(1), None);
    }
}
