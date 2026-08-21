//! HELLO header decoder and negotiation against arbitrary bytes.
//!
//! Properties: decoding never panics, list fields stay within the item cap,
//! decoding is idempotent, and negotiating against a v0 HELLO never panics
//! whatever the peer claims.

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

    let reencoded = hello.encode();
    assert_eq!(Hello::decode(&reencoded).as_ref(), Ok(&hello));

    let ours = Hello::v0(16 * 1024, 1024);
    if let Ok(agreed) = negotiate(&ours, &hello) {
        assert!(hello.versions.contains(&agreed.version));
        assert!(ours.versions.contains(&agreed.version));
        assert_eq!(agreed.send_max_header_bytes, hello.max_header_bytes);
        assert!(hello.required_capabilities.is_empty());
    }
});
