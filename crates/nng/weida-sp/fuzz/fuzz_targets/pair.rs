//! The PAIR v1 hop count against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted body splits into exactly four
//! header octets plus the payload, the count is within the bound, and
//! re-encoding reproduces the input. The hop count is PAIR v1's whole loop
//! protection [nanomsg-nng §4], so a decoder that let a count past the bound
//! through would remove it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_sp::pair;

const MAX_HOPS: u32 = 8;

fuzz_target!(|data: &[u8]| {
    if let Ok((hops, payload)) = pair::decode(data, MAX_HOPS) {
        assert!(hops <= MAX_HOPS);
        assert_eq!(pair::HEADER_LEN + payload.len(), data.len());

        let re = pair::encode(hops, payload);
        assert_eq!(re, data);
        assert_eq!(
            pair::decode(&re, MAX_HOPS).expect("a re-encoded body"),
            (hops, payload)
        );
        // Forwarding is monotone: the next hop is never smaller, and saturates
        // rather than wrapping back under the bound.
        assert!(pair::next_hop(hops) > hops);
    }
});
