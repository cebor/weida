//! The greeting decoder against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted greeting carries a mechanism
//! name that is a real name rather than padding, and re-encoding it yields a
//! greeting that decodes to the same fields. The padding and the filler are
//! deliberately outside that claim: the specification forbids interpreting the
//! padding at all, so the field set - not the octets - is the fixed point.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::{Greeting, Mechanism, greeting};

fuzz_target!(|data: &[u8]| {
    if let Ok(g) = Greeting::decode(data) {
        assert!(data.len() >= greeting::GREETING_LEN);
        let name = g.mechanism.name();
        assert!(!name.is_empty() && name.len() <= 20);
        assert!(!g.as_server || g.mechanism != Mechanism::NULL);
        assert_eq!(
            Greeting::decode(&g.encode()).expect("a re-encoded greeting decodes"),
            g
        );
        let _ = g.accept(Mechanism::NULL);
    }
    if let Ok(major) = greeting::sniff_major(data) {
        assert!(data.len() >= greeting::PARTIAL_LEN);
        // A full greeting and its own partial form must agree about the major
        // version; nothing between them may shift the octet.
        if let Ok(g) = Greeting::decode(data) {
            assert_eq!(major, g.version.major);
        }
    }
});
