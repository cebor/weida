//! The four payload lists against arbitrary bytes.
//!
//! SUBSCRIBE's (filter, options) pairs, UNSUBSCRIBE's bare filters, and
//! SUBACK's and UNSUBACK's per-filter reason codes have no count field: the
//! Remaining Length is the count (3.8.3, 3.9.3, 3.10.3, 3.11.3) [mqtt5 §3].
//! That makes them the one place where a decoder can silently *lose* entries —
//! an off-by-one in the walk shortens the list rather than failing — so the
//! invariant worth fuzzing is that the entry count survives a round trip.
//!
//! The input's first byte selects the packet type and the rest becomes the
//! payload, framed with a packet identifier and an empty property block so
//! that every declared byte is present by construction. Hence:
//!
//! 1. `Incomplete` is unreachable — the length is settled and has arrived, so
//!    a short entry is the packet contradicting its own header;
//! 2. an accepted list re-encodes to a list of the same entries in the same
//!    order, which is load-bearing for SUBACK and UNSUBACK because their codes
//!    are matched to filters by position alone;
//! 3. an empty payload is refused, never accepted as a list of nothing
//!    ([MQTT-3.8.3-2], [MQTT-3.10.3-2]).

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::{DecodeError, Packet, PacketType};

const CAP: u32 = 4096;

fuzz_target!(|data: &[u8]| {
    let Some((selector, payload)) = data.split_first() else {
        return;
    };
    let packet_type = match selector % 4 {
        0 => PacketType::Subscribe,
        1 => PacketType::Suback,
        2 => PacketType::Unsubscribe,
        _ => PacketType::Unsuback,
    };

    // packet identifier 0x0001, then an empty property block, then the
    // payload. The Remaining Length is exactly what follows.
    let body_len = 3 + payload.len();
    if body_len > 127 {
        return;
    }
    let mut wire = vec![
        (packet_type.as_bits() << 4) | packet_type.required_flags().unwrap_or(0),
        body_len as u8,
        0x00,
        0x01,
        0x00,
    ];
    wire.extend_from_slice(payload);

    match Packet::decode(&wire, CAP) {
        Ok((packet, used)) => {
            assert_eq!(used, wire.len());
            assert_eq!(packet.packet_type(), packet_type);

            let entries = match &packet {
                Packet::Subscribe(body) => body.filters.iter().count(),
                Packet::Suback(body) => body.reason_codes.iter().count(),
                Packet::Unsubscribe(body) => body.filters.iter().count(),
                Packet::Unsuback(body) => body.reason_codes.iter().count(),
                other => unreachable!("{}", other.packet_type()),
            };
            assert!(entries > 0, "an accepted list is never empty");

            let mut out = Vec::new();
            packet.encode(&mut out).expect("re-encodes");
            let (again, _) = Packet::decode(&out, CAP).expect("the canonical form");
            assert_eq!(again, packet, "the entries and their order survive");

            let entries_again = match &again {
                Packet::Subscribe(body) => body.filters.iter().count(),
                Packet::Suback(body) => body.reason_codes.iter().count(),
                Packet::Unsubscribe(body) => body.filters.iter().count(),
                Packet::Unsuback(body) => body.reason_codes.iter().count(),
                other => unreachable!("{}", other.packet_type()),
            };
            assert_eq!(entries_again, entries, "no entry was lost or invented");
        }
        Err(error) => {
            assert_ne!(
                error,
                DecodeError::Incomplete,
                "a complete packet never asks for more bytes"
            );
            if payload.is_empty() {
                assert_eq!(error, DecodeError::EmptyPayload { packet_type });
            }
        }
    }
});
