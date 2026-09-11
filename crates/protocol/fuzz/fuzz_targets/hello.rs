//! HELLO header decoder and negotiation against arbitrary bytes.
//!
//! Properties: decoding never panics, list fields stay within the item cap,
//! an accepted header's declarations are self-consistent, decoding is
//! idempotent, and negotiating against a v0 HELLO never panics whatever the
//! peer claims — and when it succeeds, the effective set really is reachable
//! by both sides' requirements.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::header::limits;
use weida_protocol::{Hello, negotiate};

fuzz_target!(|data: &[u8]| {
    let Ok(hello) = Hello::decode(data) else {
        return;
    };

    assert!(hello.versions.len() <= limits::MAX_LIST_ITEMS);
    assert!(hello.capabilities.len() <= limits::MAX_LIST_ITEMS);
    assert!(hello.required_capabilities.len() <= limits::MAX_LIST_ITEMS);
    // §6.1: an accepted HELLO never requires more than it offers.
    assert!(hello.offered().reaches(&hello.required()));

    let reencoded = hello.encode();
    assert_eq!(Hello::decode(&reencoded).as_ref(), Ok(&hello));

    let ours = Hello::v0(16 * 1024, 1024);
    if let Ok(agreed) = negotiate(&ours, &hello) {
        assert!(hello.versions.contains(&agreed.version));
        assert!(ours.versions.contains(&agreed.version));
        assert_eq!(agreed.send_max_header_bytes, hello.max_header_bytes);
        assert!(hello.required_capabilities.is_empty());
        // The effective set satisfies both requirements, and is no stronger
        // than either offer.
        assert!(agreed.guarantees.reaches(&hello.required()));
        assert!(agreed.guarantees.reaches(&ours.required()));
        assert!(hello.offered().reaches(&agreed.guarantees));
        assert!(ours.offered().reaches(&agreed.guarantees));
    }
});
