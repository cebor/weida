//! ERROR header decoder against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted header respects the message
//! cap; and decoding is idempotent (re-encoding an accepted header and
//! decoding it again yields the same value).
//!
//! The code is a raw `u64` rather than an enum on purpose: a peer may name an
//! error this version does not define, and refusing the frame would turn a
//! reportable failure into a connection violation. So an unknown code must
//! decode, re-encode unchanged, and leave `error_code()` answering `None`
//! rather than panicking.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::ErrorHeader;
use weida_protocol::header::limits;

fuzz_target!(|data: &[u8]| {
    let Ok(header) = ErrorHeader::decode(data) else {
        return;
    };

    if let Some(message) = &header.message {
        assert!(message.len() <= limits::MAX_MESSAGE_BYTES);
    }
    let _ = header.error_code();

    let reencoded = header.encode();
    assert_eq!(ErrorHeader::decode(&reencoded).as_ref(), Ok(&header));
});
