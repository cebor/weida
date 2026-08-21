//! Transfer identifiers.

use std::fmt;

/// A per-connection, per-sender transfer identifier.
///
/// Counters start at 1; `0` is reserved and is a protocol violation on the
/// wire, which this newtype makes unrepresentable.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TransferId(u64);

impl TransferId {
    /// The first identifier a sender may use.
    pub const FIRST: TransferId = TransferId(1);

    /// Wraps a wire value, rejecting the reserved `0`.
    pub const fn new(raw: u64) -> Option<TransferId> {
        if raw == 0 {
            None
        } else {
            Some(TransferId(raw))
        }
    }

    /// The wire value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_rejected() {
        assert!(TransferId::new(0).is_none());
        assert_eq!(TransferId::new(1).unwrap().get(), 1);
        assert_eq!(TransferId::new(u64::MAX).unwrap().get(), u64::MAX);
    }

    #[test]
    fn first_is_one() {
        assert_eq!(TransferId::FIRST.get(), 1);
        assert_eq!(TransferId::FIRST.to_string(), "1");
    }

    #[test]
    fn ordering_follows_the_counter() {
        assert!(TransferId::new(1).unwrap() < TransferId::new(2).unwrap());
    }
}
