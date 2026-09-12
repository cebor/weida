//! The fixed header against arbitrary bytes, under a deliberately small cap.
//!
//! Property: the declared packet size is checked before anything else happens.
//! An accepted header never reports a packet larger than the cap, the flags it
//! accepted are the ones its packet type requires, and the bytes it consumed
//! are the type byte plus the Remaining Length's own 1 to 4.
//!
//! The cap is 4 KiB on purpose. A packet may declare 268,435,455 bytes from
//! the Remaining Length alone (2.1.4) and `Maximum Packet Size` is absent by
//! default, meaning no limit below that (3.1.2.11.4), so the local limit is
//! the whole defence: a decoder that reserved the declared length before
//! checking it would be killed by the fuzzer's memory limit rather than caught
//! by an assertion, and either way this target is where it shows.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::{FixedHeader, QoS, varint};

const CAP: u32 = 4096;

fuzz_target!(|data: &[u8]| {
    if let Ok((header, used)) = FixedHeader::decode(data, CAP) {
        assert!(header.packet_len() as u32 <= CAP);
        assert!(header.remaining_length <= varint::MAX);
        assert_eq!(used, header.header_len());
        assert!(used <= data.len());

        match header.packet_type.required_flags() {
            Some(required) => assert_eq!(header.flags, required),
            // PUBLISH owns its flags, and QoS 3 is the one combination that is
            // malformed rather than merely unusual (3.3.1.2).
            None => {
                QoS::from_bits((header.flags >> 1) & 0b11).expect("QoS 3 must have been refused");
            }
        }

        let mut out = Vec::new();
        header.encode(&mut out).expect("a decoded header re-encodes");
        assert_eq!(out, &data[..used], "the fixed header is canonical");
    }
});
