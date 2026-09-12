//! The whole packet decoder against arbitrary bytes, under a small cap.
//!
//! Property: decoding never panics, an accepted packet fits the cap, it
//! reports its own encoded length as the bytes it consumed, and re-encoding
//! yields something that decodes to the same value.
//!
//! Re-decoding rather than re-encoding is deliberate, and it is the difference
//! between fuzzing a codec and fuzzing a serializer. A peer may send properties
//! in any order and may spell an acknowledgement's all-zero tail long or short
//! [mqtt5 §3]; this codec emits one canonical form. Demanding byte equality
//! would therefore report legal inputs as failures, while demanding *value*
//! equality catches the faults that matter: a field silently dropped, a
//! repeatable property lost by the walker, or a length computed differently by
//! `body_len` than by the encoder.
//!
//! The cap stays small so that the allocation bound is exercised rather than
//! described: a declared quarter-gigabyte body over a fifty-byte input must be
//! refused from the fixed header, and a decoder that reserved first would be
//! killed by the memory limit instead of caught here.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::Packet;

const CAP: u32 = 4096;

fuzz_target!(|data: &[u8]| {
    if let Ok((packet, used)) = Packet::decode(data, CAP) {
        assert!(used <= data.len());
        assert!(used as u32 <= CAP);
        assert_eq!(packet.encoded_len(), Ok(used as u32));

        let mut out = Vec::new();
        packet.encode(&mut out).expect("a decoded packet re-encodes");
        assert_eq!(out.len(), used as usize);

        let (again, used2) = Packet::decode(&out, CAP).expect("the canonical form decodes");
        assert_eq!(again, packet);
        assert_eq!(used2, out.len());

        // The refusal on the way out is the mirror of the one on the way in.
        let mut small = Vec::new();
        assert!(packet.encode_within(used as u32 - 1, &mut small).is_err());
        assert!(small.is_empty(), "a refused encode writes nothing");
    }
});
