//! Preamble parser against arbitrary bytes.
//!
//! Property: parsing never panics, and an accepted preamble never reports a
//! header length above the cap or a consumed length beyond the input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::parse_preamble;

const MAX_HEADER_BYTES: u64 = 16 * 1024;

fuzz_target!(|data: &[u8]| {
    if let Ok((preamble, used)) = parse_preamble(data, MAX_HEADER_BYTES) {
        assert!(preamble.header_len <= MAX_HEADER_BYTES);
        assert!(used <= data.len());
        assert!(used >= 3);
    }
});
