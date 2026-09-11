//! A stream of messages: decode until the buffer is consumed or refused.
//!
//! One message at a time is not the interesting case — a peer sends a byte
//! stream, and the framing has to stay aligned across messages. Property:
//! the loop always terminates, every accepted message advances the cursor by
//! at least the size field, and the total consumed never exceeds the input.
//! A decoder that returned `used == 0` for some input would spin here, which
//! is the failure this target exists to find.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_sp::message;

const CAP: u64 = 4096;

fuzz_target!(|data: &[u8]| {
    let mut rest = data;
    let mut total = 0usize;
    while let Ok((body, used)) = message::decode(rest, CAP) {
        assert!(used >= message::SIZE_LEN);
        assert_eq!(used - message::SIZE_LEN, body.len());
        total += used;
        assert!(total <= data.len());
        rest = &rest[used..];
    }
});
