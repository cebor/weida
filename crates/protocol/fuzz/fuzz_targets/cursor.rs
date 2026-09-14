//! CURSOR head frame and record codec against arbitrary bytes.
//!
//! Properties: a head frame decodes without panicking and re-encodes
//! identically; a record decode never panics, never claims more than
//! `MAX_CURSOR_RECORD_LEN` bytes consumed, never returns a level the reserved
//! range does not define, and re-encodes to exactly the bytes it consumed —
//! which is what makes a reader's "advance by `used`" loop safe on hostile
//! input.
//!
//! Truncation is deliberately *not* a failure here: a reader sees whatever
//! slice the transport handed it, so `Ok(None)` means read more bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::header::{CursorLevel, MAX_CURSOR_RECORD_LEN};
use weida_protocol::{CursorHeader, decode_cursor_record, encode_cursor_record};

fuzz_target!(|data: &[u8]| {
    if let Ok(head) = CursorHeader::decode(data) {
        let reencoded = head.encode();
        assert_eq!(CursorHeader::decode(&reencoded), Ok(head));
    }

    // Records are read in a loop, so every step of that loop is exercised:
    // each accepted record must consume at least one byte, or the loop would
    // spin.
    let mut rest = data;
    while let Ok(Some((level, offset, used))) = decode_cursor_record(rest) {
        assert!(used > 0, "a record must consume bytes");
        assert!(used <= MAX_CURSOR_RECORD_LEN, "{used} bytes consumed");
        assert!(used <= rest.len());
        match level {
            CursorLevel::Application(value) => {
                assert!(value >= CursorLevel::APPLICATION_FLOOR)
            }
            CursorLevel::Known(known) => {
                assert!(known.to_wire() < CursorLevel::APPLICATION_FLOOR)
            }
        }
        // The wire form is the shortest encoding, so a decoded record
        // re-encodes to no more bytes than it consumed; a non-minimal varint
        // the decoder accepted shortens rather than growing.
        let mut out = Vec::new();
        encode_cursor_record(level, offset, &mut out)
            .expect("a decoded level and offset are both in varint range");
        assert!(out.len() <= used);
        assert_eq!(
            decode_cursor_record(&out),
            Ok(Some((level, offset, out.len())))
        );
        rest = &rest[used..];
    }
});
