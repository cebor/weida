//! The metadata dictionary against arbitrary bytes.
//!
//! Property: decoding never panics, a four-octet value length can never make
//! this side allocate what it does not hold, and an accepted dictionary
//! re-encodes **byte for byte** - unlike a weida CBOR header there are no
//! unknown keys to drop, so the fixed point here is the octets and not merely
//! the value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::Metadata;

fuzz_target!(|data: &[u8]| {
    let Ok(md) = Metadata::decode(data) else {
        return;
    };
    for (name, value) in md.properties() {
        assert!(!name.is_empty() && name.len() <= 255);
        assert!(value.len() <= i32::MAX as usize);
        assert!(md.get(name).is_some(), "a decoded name must be findable");
    }
    let mut re = Vec::new();
    md.encode(&mut re)
        .expect("an accepted dictionary re-encodes");
    assert_eq!(re, data);
    let _ = md.socket_type();
});
