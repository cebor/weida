//! The message decoder against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted message never reports a body
//! longer than the cap, never borrows more than the input holds, and
//! re-encodes to something that decodes to the same body.
//!
//! The cap is small on purpose. An SP message may declare 2^64-1 octets
//! [rfc-tcp §3], there is no credit on the wire [nanomsg-nng §12/P12], and
//! `RECVMAXSZ` is unlimited by default [nanomsg-nng §5], so the local limit is
//! the whole defence: a decoder that reserved the declared length before
//! checking it would be killed by the fuzzer's memory limit rather than caught
//! by an assertion, and either way this target is where it shows.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_sp::message;

const CAP: u64 = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if let Ok((body, used)) = message::decode(data, CAP) {
        assert!(body.len() as u64 <= CAP);
        assert!(used <= data.len());
        assert_eq!(used, message::SIZE_LEN + body.len());

        let re = message::encode(body);
        let (body2, used2) = message::decode(&re, CAP).expect("a re-encoded message decodes");
        assert_eq!(body2, body);
        assert_eq!(used2, re.len());
    }
});
