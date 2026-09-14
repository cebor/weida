//! CREDIT header decoder against arbitrary bytes.
//!
//! Properties: decoding never panics; an accepted header respects both text
//! caps and carries a filter that has passed the grammar of
//! `docs/PROTOCOL.md` §6.4; and decoding is idempotent (re-encoding an
//! accepted header and decoding it again yields the same value).
//!
//! CREDIT is the only header whose three keys are all required, so it is the
//! one place where the missing-key path meets hostile input, and the only one
//! that runs `filter::validate` on a required field. That validation is the
//! reason a matcher may take a decoded filter unchecked, so a filter reaching
//! this target's assertion illegally would be a hole in every subscription
//! lookup downstream.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::CreditHeader;
use weida_protocol::header::{filter, limits};

fuzz_target!(|data: &[u8]| {
    let Ok(header) = CreditHeader::decode(data) else {
        return;
    };

    assert!(header.endpoint.len() <= limits::MAX_ENDPOINT_BYTES);
    assert!(header.filter.len() <= limits::MAX_FILTER_BYTES);
    assert!(filter::validate(&header.filter).is_ok());

    let reencoded = header.encode();
    assert_eq!(CreditHeader::decode(&reencoded).as_ref(), Ok(&header));
});
