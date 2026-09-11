//! Per-producer sequencing and gap detection: `PerProducer(detect)`.
//!
//! Detect mode is the default of [decision 0001](../../../docs/decisions/0001-sequence-field.md)
//! §7.5: the sender numbers its transfers, the receiver reports the gap and
//! **delivers messages as they arrive**. Nothing is held back, so there is no
//! reassembly buffer and no head-of-line blocking above QUIC.
//!
//! Both halves are inert when the negotiated ordering is `None`: no map is
//! ever touched, so an endpoint that negotiated nothing allocates nothing —
//! the hot-path rule of `docs/INVARIANTS.md`, checked by the unit tests at the
//! bottom of this file rather than asserted in prose.

use std::collections::HashMap;
use std::sync::Mutex;

use weida_protocol::header::OrderingMode;

/// What a receiver observed about one transfer's place in its producer's
/// sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The sequence number the receiver expected next.
    pub expected: u64,
    /// The sequence number that actually arrived.
    pub seen: u64,
}

impl Gap {
    /// How many transfers went missing. Always at least one: a `Gap` is only
    /// reported when `seen` is ahead of `expected`.
    pub fn missed(&self) -> u64 {
        self.seen.saturating_sub(self.expected)
    }
}

/// Assigns sequence numbers per scope: the dialled path, or the topic of a
/// published copy.
///
/// The producer itself is the connection — the peer's proved fingerprint is
/// what names it, so nothing goes on the wire for it
/// ([decision 0008](../../../docs/decisions/0008-session-identity.md) §4.4) —
/// which is why one sequencer lives per connection.
pub(crate) struct Sequencer {
    enabled: bool,
    counters: Mutex<HashMap<Box<str>, u64>>,
}

impl Sequencer {
    pub(crate) fn new(ordering: OrderingMode) -> Sequencer {
        Sequencer {
            enabled: ordering != OrderingMode::None,
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// The next number for `scope`, or `None` when ordering is off.
    ///
    /// Numbers start at zero and are dense per scope; a receiver reads a
    /// missing number as a gap.
    pub(crate) fn next(&self, scope: &str) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let mut counters = self.counters.lock().expect("sequencer poisoned");
        let next = counters.entry_ref_or_insert(scope);
        Some(next)
    }

    #[cfg(test)]
    fn tracked_scopes(&self) -> usize {
        self.counters.lock().expect("sequencer poisoned").len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.counters.lock().expect("sequencer poisoned").capacity() > 0
    }
}

/// Small extension so the hot path does one lookup, not two.
trait CounterMap {
    fn entry_ref_or_insert(&mut self, scope: &str) -> u64;
}

impl CounterMap for HashMap<Box<str>, u64> {
    fn entry_ref_or_insert(&mut self, scope: &str) -> u64 {
        match self.get_mut(scope) {
            Some(counter) => {
                let issued = *counter;
                *counter = counter.wrapping_add(1);
                issued
            }
            None => {
                self.insert(scope.into(), 1);
                0
            }
        }
    }
}

/// Tracks the next expected number per scope and reports what is missing.
pub(crate) struct GapDetector {
    enabled: bool,
    /// Bounded by `max_sequence_scopes`: the scopes are the peer's paths and
    /// topics, so the table is remote-influenced and needs a named cap
    /// (`docs/INVARIANTS.md`).
    max_scopes: usize,
    expected: Mutex<HashMap<Box<str>, u64>>,
}

impl GapDetector {
    pub(crate) fn new(ordering: OrderingMode, max_scopes: usize) -> GapDetector {
        GapDetector {
            enabled: ordering != OrderingMode::None,
            max_scopes,
            expected: Mutex::new(HashMap::new()),
        }
    }

    /// Records `sequence` for `scope` and reports a gap if one opened.
    ///
    /// A number below the expected one is a repeat or a reordering, not a
    /// gap: detect mode reports what is missing and never reorders, so an
    /// out-of-order arrival lowers nothing and is delivered as it came.
    pub(crate) fn observe(&self, scope: &str, sequence: u64) -> Option<Gap> {
        if !self.enabled {
            return None;
        }
        let mut expected = self.expected.lock().expect("gap detector poisoned");
        let next = expected.get_mut(scope);
        match next {
            Some(next) => {
                let gap = (sequence > *next).then_some(Gap {
                    expected: *next,
                    seen: sequence,
                });
                // A number below the expected one carries no information
                // about what is still to come: advancing on it would consume
                // a number that has not arrived and silence its gap.
                if sequence >= *next {
                    *next = sequence.wrapping_add(1);
                }
                gap
            }
            None => {
                if expected.len() >= self.max_scopes {
                    // At the cap the detector stops tracking new scopes
                    // rather than growing: a peer that invents paths must not
                    // be able to size this table.
                    return None;
                }
                expected.insert(scope.into(), sequence.wrapping_add(1));
                // The first number seen for a scope establishes the position;
                // a connection may legitimately start anywhere, because the
                // peer's counter is older than this connection's view of it.
                None
            }
        }
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.expected
            .lock()
            .expect("gap detector poisoned")
            .capacity()
            > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disabled_sequencer_numbers_nothing_and_allocates_nothing() {
        let sequencer = Sequencer::new(OrderingMode::None);
        for _ in 0..1000 {
            assert_eq!(sequencer.next("/md"), None);
        }
        assert_eq!(sequencer.tracked_scopes(), 0);
        assert!(
            !sequencer.allocated(),
            "an off sequencer must not allocate a table"
        );
    }

    #[test]
    fn a_disabled_detector_reports_nothing_and_allocates_nothing() {
        let detector = GapDetector::new(OrderingMode::None, 1024);
        for seq in [0, 5, 1, 900] {
            assert_eq!(detector.observe("/md", seq), None);
        }
        assert!(
            !detector.allocated(),
            "an off detector must not allocate a table"
        );
    }

    #[test]
    fn numbers_are_dense_and_per_scope() {
        let sequencer = Sequencer::new(OrderingMode::PerProducerDetect);
        assert_eq!(sequencer.next("a"), Some(0));
        assert_eq!(sequencer.next("a"), Some(1));
        assert_eq!(sequencer.next("b"), Some(0));
        assert_eq!(sequencer.next("a"), Some(2));
        assert_eq!(sequencer.tracked_scopes(), 2);
    }

    #[test]
    fn a_missing_number_is_a_gap_of_that_size() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("a", 1), None);
        // 2 and 3 never arrive.
        let gap = detector.observe("a", 4).expect("a gap");
        assert_eq!(
            gap,
            Gap {
                expected: 2,
                seen: 4
            }
        );
        assert_eq!(gap.missed(), 2);
        // The detector resynchronizes: the next number in order is clean.
        assert_eq!(detector.observe("a", 5), None);
    }

    #[test]
    fn scopes_do_not_interfere() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("b", 0), None);
        assert_eq!(detector.observe("a", 1), None);
        assert_eq!(detector.observe("b", 7).expect("gap").missed(), 6);
    }

    #[test]
    fn an_out_of_order_arrival_is_not_a_gap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("a", 2).expect("gap").missed(), 1);
        // 1 arrives late: detect mode delivers it and reports no new gap,
        // because nothing further is missing.
        assert_eq!(detector.observe("a", 1), None);
        // And it did not consume 3: a late arrival moves the position
        // nowhere, so a later hole is still reported.
        assert_eq!(detector.observe("a", 4).expect("gap").missed(), 1);
    }

    #[test]
    fn the_scope_table_stops_growing_at_its_cap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 4);
        for i in 0..100 {
            detector.observe(&format!("scope-{i}"), 0);
        }
        assert_eq!(
            detector.expected.lock().expect("poisoned").len(),
            4,
            "a peer must not be able to size this table"
        );
    }
}
