//! The frame decoder against arbitrary bytes.
//!
//! Property: decoding never panics, and an accepted frame never reports a body
//! length above the cap, never borrows more than the input holds, and
//! re-encodes to something that decodes to the same header and body.
//!
//! The cap is small on purpose. A ZMTP frame may declare 2^63-1 octets and
//! there is no credit on the wire, so the only defence is the local limit: a
//! decoder that reserved the declared length before checking it would be
//! killed by the fuzzer's memory limit rather than caught by an assertion, and
//! either way this target is where it shows.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::frame;

const CAP: u64 = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if let Ok((header, body, used)) = frame::decode(data, CAP) {
        assert!(header.len <= CAP);
        assert_eq!(body.len() as u64, header.len);
        assert!(used <= data.len());
        assert!(used >= 2);

        let re = frame::encode(header.kind, body);
        let (again, body2, used2) = frame::decode(&re, CAP).expect("a re-encoded frame decodes");
        assert_eq!(again, header);
        assert_eq!(body2, body);
        assert_eq!(used2, re.len());
    }
});
