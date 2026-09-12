//! The property framework against arbitrary bytes, in each of its sets.
//!
//! The input's first byte selects the packet's property set and the rest is
//! framed as a block whose declared length is exactly what follows, so every
//! declared byte is present by construction.
//!
//! Two properties:
//!
//! 1. **`Incomplete` is unreachable.** The block's own length is authoritative
//!    and all of it has arrived, so "not enough bytes" would be a lie that
//!    parks a reader on bytes the peer already finished sending. Everything
//!    else — an unknown identifier, a property the set does not carry, a
//!    repetition, a value out of range, a field running off the end — is a
//!    verdict.
//! 2. **Whatever decodes survives a round trip through the canonical form.**
//!    Not byte-for-byte: order between different identifiers is insignificant
//!    on the wire and this codec emits ascending identifier order [mqtt5 §3],
//!    so re-decoding rather than re-encoding is the invariant. It is also what
//!    proves the repeatable properties are re-walked correctly, since a
//!    `User Property` or `Subscription Identifier` dropped by the walker would
//!    vanish from the second decode.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::property::{Properties, PropertySet};
use weida_mqtt_codec::{DecodeError, Reader, varint};

fuzz_target!(|data: &[u8]| {
    let Some((selector, body)) = data.split_first() else {
        return;
    };
    let allowed = match selector % 10 {
        0 => PropertySet::CONNECT,
        1 => PropertySet::CONNACK,
        2 => PropertySet::WILL,
        3 => PropertySet::PUBLISH,
        4 => PropertySet::PUBACK,
        5 => PropertySet::SUBSCRIBE,
        6 => PropertySet::SUBACK,
        7 => PropertySet::UNSUBSCRIBE,
        8 => PropertySet::DISCONNECT,
        _ => PropertySet::AUTH,
    };

    let mut framed = Vec::new();
    if varint::encode(body.len() as u32, &mut framed).is_err() {
        return;
    }
    framed.extend_from_slice(body);

    let mut reader = Reader::new(&framed);
    match Properties::decode(&mut reader, allowed, None) {
        Ok(properties) => {
            assert!(reader.is_empty(), "the whole block was consumed");

            let len = properties
                .encoded_len(allowed)
                .expect("a decoded set re-encodes");
            let mut out = Vec::new();
            properties.encode(allowed, &mut out).expect("re-encodes");
            assert_eq!(out.len() as u32, len, "encoded_len agrees");

            let mut again = Reader::new(&out);
            let reparsed =
                Properties::decode(&mut again, allowed, None).expect("the canonical form");
            assert_eq!(reparsed, properties);
            assert!(again.is_empty());
        }
        Err(error) => assert_ne!(
            error,
            DecodeError::Incomplete,
            "an exact block never asks for more bytes"
        ),
    }
});
