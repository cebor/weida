//! The caller's bounds, passed to every decode.
//!
//! AMQP grants no credit before `open` has been read, and its compound
//! headers declare their own element counts: a `list32` announces up to
//! 2^32-1 elements in nine octets, and a decoder that reserved that count
//! before looking at it would be killed by a nine-octet frame. So the bound is
//! an *argument*, not a constant, and it is checked from the declared count
//! alone — before the first element is looked at and before anything is
//! reserved.
//!
//! Two numbers, because a peer has two ways to make a decoder commit memory:
//! breadth (one list with four billion elements) and depth (a list of a list
//! of a list, which is the call stack rather than the heap). Both are remote
//! input, and neither has a bound in the specification's grammar.

/// Bounds a decoder applies to remote input.
///
/// Every `decode` entry point in this crate takes one, by value, because it
/// is two words. There is no way to switch a bound off and no "unlimited"
/// value: `max_elements = u32::MAX` still cannot make a decoder read past the
/// input slice, and the element count of a `list` or `map` is additionally
/// bounded by the octets its own header declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest element count a single `list`, `map` or `array` header may
    /// declare. A `map` count is keys *and* values, so a cap of 1000 admits
    /// 500 entries — the count is the wire's, not the application's.
    pub max_elements: u32,
    /// The deepest nesting of described types, lists, maps and arrays.
    ///
    /// A performative is depth 2 (the described type, then its field list),
    /// an `attach` carrying a `source` with a `filter` is depth 5, so
    /// anything below 6 refuses ordinary traffic.
    pub max_depth: u32,
}

impl Limits {
    /// Bounds wide enough for every performative and message section this
    /// crate encodes, and narrow enough that a hostile frame cannot reach
    /// past them.
    ///
    /// `max_elements` = 4096 is three orders of magnitude above the widest
    /// composite the specification defines (`attach`, fourteen fields) and
    /// still bounds an `attach.unsettled` map at 2048 delivery tags — which
    /// is the one structure Part 2 §2.6.3 names as having no protocol bound of
    /// its own. `max_depth` = 16 admits a described value inside a map inside
    /// a `source.filter` with room to spare; no composite the specification
    /// defines is deeper than 5.
    pub const DEFAULT: Self = Self {
        max_elements: 4096,
        max_depth: 16,
    };

    /// Bounds for a message body, whose shape an application chose rather
    /// than the specification.
    ///
    /// Wider in breadth because an `amqp-sequence` is a list of application
    /// values and 4096 is a plausible application size; still bounded,
    /// because "the application chose it" is not a reason to let a peer
    /// choose it.
    pub const BODY: Self = Self {
        max_elements: 65_536,
        max_depth: 32,
    };

    /// Whether a value nested `depth` levels below the outermost one is still
    /// inside the bound. The outermost value is at depth 0.
    #[must_use]
    pub const fn admits_depth(self, depth: u32) -> bool {
        depth < self.max_depth
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_is_counted_from_the_outermost_value() {
        let limits = Limits {
            max_elements: 8,
            max_depth: 2,
        };
        assert!(limits.admits_depth(0), "the outermost value always fits");
        assert!(limits.admits_depth(1));
        assert!(!limits.admits_depth(2));
    }

    #[test]
    fn zero_depth_admits_nothing() {
        let limits = Limits {
            max_elements: 8,
            max_depth: 0,
        };
        assert!(!limits.admits_depth(0));
    }
}
