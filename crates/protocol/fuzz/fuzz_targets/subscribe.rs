//! SUBSCRIBE/UNSUBSCRIBE header decoder against arbitrary bytes.
//!
//! Both frame kinds share one header layout, so one target covers both.
//!
//! Properties: decoding never panics and never allocates unboundedly; an
//! accepted header respects both documented caps; and decoding is idempotent
//! (re-encoding an accepted header and decoding it again yields the same
//! value). The empty filter is legal — it is the "every topic" subscription —
//! so no non-emptiness property is asserted.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::SubscriptionHeader;
use weida_protocol::header::limits;

fuzz_target!(|data: &[u8]| {
    let Ok(header) = SubscriptionHeader::decode(data) else {
        return;
    };

    assert!(header.endpoint.len() <= limits::MAX_ENDPOINT_BYTES);
    assert!(header.filter.len() <= limits::MAX_FILTER_BYTES);
    assert!(header.max_layer.is_none_or(|l| l <= limits::MAX_LAYER));

    let reencoded = header.encode();
    assert_eq!(SubscriptionHeader::decode(&reencoded).as_ref(), Ok(&header));
});
