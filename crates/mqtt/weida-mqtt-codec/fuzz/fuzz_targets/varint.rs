//! The Variable Byte Integer against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted value is at most 268,435,455,
//! it occupied the minimal number of bytes for that value, and re-encoding
//! reproduces exactly the bytes that were consumed.
//!
//! The last clause is the one worth fuzzing. [MQTT-1.5.5-1] makes the encoding
//! canonical, and canonicality is a property of the *pair* of functions: a
//! decoder that accepted `81 00` and an encoder that emitted `01` would each
//! look correct alone. Asserting `encode(decode(x)) == x[..used]` is what
//! makes a length unsmugglable by padding, which is what a size check on a
//! declared length depends on.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::varint;

fuzz_target!(|data: &[u8]| {
    if let Ok((value, used)) = varint::decode(data) {
        assert!(value <= varint::MAX);
        assert!((1..=varint::MAX_BYTES).contains(&used));
        assert!(used <= data.len());
        assert_eq!(used, varint::encoded_len(value));

        let mut out = Vec::new();
        varint::encode(value, &mut out).expect("a decoded value re-encodes");
        assert_eq!(out, &data[..used], "the encoding is canonical");
    }
});
