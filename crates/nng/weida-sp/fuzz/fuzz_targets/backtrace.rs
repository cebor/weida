//! The REQ/REP and survey tag stack against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted stack never holds more peer
//! IDs than the bound, its tags plus payload account for exactly the body,
//! and re-encoding reproduces the leading bytes exactly. The stack has no
//! length field — only the terminal bit ends it [rfc-reqrep §5] — so a peer
//! can withhold that bit forever; the bound is what makes the loop finite and
//! this target is where an off-by-one in it shows.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_sp::backtrace;

const MAX_HOPS: usize = 4;

fuzz_target!(|data: &[u8]| {
    if let Ok((stack, payload)) = backtrace::decode(data, MAX_HOPS) {
        assert!(stack.peers.len() <= MAX_HOPS);
        assert!(stack.id <= backtrace::MAX_ID);
        assert!(stack.peers.iter().all(|p| *p <= backtrace::MAX_ID));
        assert_eq!(stack.encoded_len() + payload.len(), data.len());

        let re = stack.encode_message(payload);
        assert_eq!(re, data);
        let (again, payload2) = backtrace::decode(&re, MAX_HOPS).expect("a re-encoded stack");
        assert_eq!(again, stack);
        assert_eq!(payload2, payload);
    }
});
