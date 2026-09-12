//! The fixed header: one byte of type and flags, then the Remaining Length.
//!
//! ```text
//! bit    7   6   5   4   3   2   1   0
//!      +---+---+---+---+---+---+---+---+
//!      |  packet type  |     flags     |
//!      +---+---+---+---+---+---+---+---+
//!      |      Remaining Length (1-4)   |
//!      +-------------------------------+
//! ```
//!
//! Remaining Length "is the number of bytes remaining within the current
//! packet, including data in the variable header and the payload. The
//! Remaining Length does not include the bytes used to encode the Remaining
//! Length", and the packet size is the fixed header plus that value (2.1.4)
//! [mqtt5 §3]. That definition is what [`FixedHeader::packet_len`] computes and
//! what `Maximum Packet Size` is measured against.
//!
//! **The flags are not free bits.** Eleven of the fifteen packet types require
//! all four to be zero; PUBREL, SUBSCRIBE and UNSUBSCRIBE require `0b0010`;
//! only PUBLISH uses them, for DUP, QoS and RETAIN. "Where a flag bit is
//! marked as 'Reserved', it is reserved for future use and MUST be set to the
//! value listed" ([MQTT-2.1.3-1]) [mqtt5 §3], so a wrong value is a Malformed
//! Packet rather than something to ignore — and that is the check that makes a
//! stream desynchronisation visible at the first byte instead of thirty
//! packets later.
//!
//! **Where the allocation bound is enforced.** [`FixedHeader::decode`] takes
//! the maximum packet size as an argument and rejects an over-large
//! declaration from the Remaining Length alone, before the body is looked at,
//! let alone reserved. A packet may declare 268,435,455 bytes (2.1.4)
//! [mqtt5 §3] and either peer may declare a lower ceiling, where absence means
//! no limit below the encoding's (3.1.2.11.4, 3.2.2.3.6) [mqtt5 §5] — so the
//! ceiling is a per-connection negotiated value and therefore an argument, not
//! a constant.

use crate::error::{DecodeError, EncodeError};
use crate::varint;

/// The fifteen control packet types (2.1.2) [mqtt5 §3].
///
/// Type 0 is Reserved and Forbidden, and is [`DecodeError::ReservedPacketType`]
/// rather than a variant here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PacketType {
    /// Connection request; the client's first packet ([MQTT-3.1.0-1]).
    Connect = 1,
    /// Connect acknowledgement; exactly one per connection ([MQTT-3.2.0-1]).
    Connack = 2,
    /// Publish message.
    Publish = 3,
    /// Publish acknowledgement, QoS 1.
    Puback = 4,
    /// Publish received, QoS 2 part 1.
    Pubrec = 5,
    /// Publish release, QoS 2 part 2.
    Pubrel = 6,
    /// Publish complete, QoS 2 part 3.
    Pubcomp = 7,
    /// Subscribe request.
    Subscribe = 8,
    /// Subscribe acknowledgement.
    Suback = 9,
    /// Unsubscribe request.
    Unsubscribe = 10,
    /// Unsubscribe acknowledgement.
    Unsuback = 11,
    /// PING request; client to server only (3.12).
    Pingreq = 12,
    /// PING response; server to client only (3.13).
    Pingresp = 13,
    /// Disconnect notification; either direction in 5.0 (3.14).
    Disconnect = 14,
    /// Authentication exchange; new in 5.0, Reserved in 3.1.1 (4.12).
    Auth = 15,
}

impl PacketType {
    /// The type from the high four bits of a fixed header's first byte.
    ///
    /// # Errors
    ///
    /// [`DecodeError::ReservedPacketType`] for 0.
    pub const fn from_bits(bits: u8) -> Result<PacketType, DecodeError> {
        Ok(match bits {
            1 => PacketType::Connect,
            2 => PacketType::Connack,
            3 => PacketType::Publish,
            4 => PacketType::Puback,
            5 => PacketType::Pubrec,
            6 => PacketType::Pubrel,
            7 => PacketType::Pubcomp,
            8 => PacketType::Subscribe,
            9 => PacketType::Suback,
            10 => PacketType::Unsubscribe,
            11 => PacketType::Unsuback,
            12 => PacketType::Pingreq,
            13 => PacketType::Pingresp,
            14 => PacketType::Disconnect,
            15 => PacketType::Auth,
            _ => return Err(DecodeError::ReservedPacketType),
        })
    }

    /// The four bits this type occupies in a fixed header.
    #[must_use]
    pub const fn as_bits(self) -> u8 {
        self as u8
    }

    /// The fixed-header flags this type requires, or `None` for PUBLISH, which
    /// is the only type that uses them (2.1.3) [mqtt5 §3].
    #[must_use]
    pub const fn required_flags(self) -> Option<u8> {
        match self {
            PacketType::Publish => None,
            PacketType::Pubrel | PacketType::Subscribe | PacketType::Unsubscribe => Some(0b0010),
            _ => Some(0b0000),
        }
    }

    /// Whether a client may send this type (2.1.2) [mqtt5 §12/P11].
    #[must_use]
    pub const fn from_client(self) -> bool {
        !matches!(
            self,
            PacketType::Connack | PacketType::Suback | PacketType::Unsuback | PacketType::Pingresp
        )
    }

    /// Whether a server may send this type (2.1.2) [mqtt5 §12/P11].
    #[must_use]
    pub const fn from_server(self) -> bool {
        !matches!(
            self,
            PacketType::Connect
                | PacketType::Subscribe
                | PacketType::Unsubscribe
                | PacketType::Pingreq
        )
    }

    /// The specification's name, as it appears in the standard.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            PacketType::Connect => "CONNECT",
            PacketType::Connack => "CONNACK",
            PacketType::Publish => "PUBLISH",
            PacketType::Puback => "PUBACK",
            PacketType::Pubrec => "PUBREC",
            PacketType::Pubrel => "PUBREL",
            PacketType::Pubcomp => "PUBCOMP",
            PacketType::Subscribe => "SUBSCRIBE",
            PacketType::Suback => "SUBACK",
            PacketType::Unsubscribe => "UNSUBSCRIBE",
            PacketType::Unsuback => "UNSUBACK",
            PacketType::Pingreq => "PINGREQ",
            PacketType::Pingresp => "PINGRESP",
            PacketType::Disconnect => "DISCONNECT",
            PacketType::Auth => "AUTH",
        }
    }
}

impl core::fmt::Display for PacketType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.name())
    }
}

/// Quality of Service: the three delivery levels (4.3) [mqtt5 §6].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QoS {
    /// At most once: no response, no retry, no stored state (4.3.1).
    #[default]
    AtMostOnce = 0,
    /// At least once: PUBLISH/PUBACK (4.3.2).
    AtLeastOnce = 1,
    /// Exactly once, strictly per hop: PUBLISH/PUBREC/PUBREL/PUBCOMP (4.3.3).
    ExactlyOnce = 2,
}

impl QoS {
    /// The level from two bits.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidQos`] for 3, which "is a Malformed Packet"
    /// wherever it appears (3.3.1.2) [mqtt5 §6].
    pub const fn from_bits(bits: u8) -> Result<QoS, DecodeError> {
        Ok(match bits {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            qos => return Err(DecodeError::InvalidQos { qos }),
        })
    }

    /// The two bits this level occupies.
    #[must_use]
    pub const fn as_bits(self) -> u8 {
        self as u8
    }
}

/// A decoded fixed header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixedHeader {
    /// The packet type from the high four bits.
    pub packet_type: PacketType,
    /// The low four bits, already validated against
    /// [`PacketType::required_flags`].
    pub flags: u8,
    /// Bytes of variable header plus payload, excluding the fixed header
    /// itself (2.1.4) [mqtt5 §3].
    pub remaining_length: u32,
}

impl FixedHeader {
    /// A header for `packet_type` with its required flags.
    ///
    /// PUBLISH has no required flags, so it gets `0b0000` here and a PUBLISH
    /// encoder sets DUP, QoS and RETAIN on the `flags` field itself.
    #[must_use]
    pub const fn new(packet_type: PacketType, remaining_length: u32) -> FixedHeader {
        FixedHeader {
            packet_type,
            flags: match packet_type.required_flags() {
                Some(flags) => flags,
                None => 0,
            },
            remaining_length,
        }
    }

    /// Bytes the fixed header itself occupies: the type byte plus the
    /// Remaining Length's 1 to 4.
    #[must_use]
    pub const fn header_len(&self) -> usize {
        1 + varint::encoded_len(self.remaining_length)
    }

    /// The whole packet's size, which is what `Maximum Packet Size` bounds
    /// (2.1.4) [mqtt5 §3].
    #[must_use]
    pub const fn packet_len(&self) -> usize {
        self.header_len() + self.remaining_length as usize
    }

    /// Decodes a fixed header from the front of `input`.
    ///
    /// Returns the header and the bytes it occupied, so a caller knows both
    /// where the body starts and how long it is before it has been received.
    ///
    /// `max_packet_size` is the ceiling on the **whole packet** and a larger
    /// declaration is rejected here — the only place the check can happen
    /// before memory is committed.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] while the Remaining Length is still
    /// arriving, [`DecodeError::ReservedPacketType`] for type 0,
    /// [`DecodeError::InvalidFlags`] or [`DecodeError::InvalidQos`] for wrong
    /// flags, and [`DecodeError::PacketTooLarge`] above `max_packet_size`.
    pub fn decode(input: &[u8], max_packet_size: u32) -> Result<(FixedHeader, usize), DecodeError> {
        let first = *input.first().ok_or(DecodeError::Incomplete)?;
        let packet_type = PacketType::from_bits(first >> 4)?;
        let flags = first & 0x0F;

        match packet_type.required_flags() {
            Some(required) if flags != required => {
                return Err(DecodeError::InvalidFlags { packet_type, flags });
            }
            // PUBLISH: DUP is bit 3, QoS bits 2-1, RETAIN bit 0, and QoS 3 is
            // the one combination that is malformed rather than merely unusual
            // (3.3.1.2).
            None => {
                QoS::from_bits((flags >> 1) & 0b11)?;
            }
            Some(_) => {}
        }

        let (remaining_length, used) = varint::decode(&input[1..])?;
        let header = FixedHeader {
            packet_type,
            flags,
            remaining_length,
        };

        let size = header.packet_len();
        // `packet_len` is at most 4 + 268,435,455, so the cast cannot wrap.
        if size as u64 > u64::from(max_packet_size) {
            return Err(DecodeError::PacketTooLarge {
                size: size as u32,
                max: max_packet_size,
            });
        }

        Ok((header, 1 + used))
    }

    /// Appends the fixed header to `out`.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PacketTooLong`] when the Remaining Length exceeds
    /// [`varint::MAX`].
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.push((self.packet_type.as_bits() << 4) | (self.flags & 0x0F));
        varint::encode(self.remaining_length, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_type_zero_is_reserved() {
        assert_eq!(
            FixedHeader::decode(&[0x00, 0x00], varint::MAX),
            Err(DecodeError::ReservedPacketType)
        );
    }

    #[test]
    fn every_type_round_trips_through_its_bits() {
        for bits in 1u8..=15 {
            let packet_type = PacketType::from_bits(bits).expect("a type");
            assert_eq!(packet_type.as_bits(), bits, "{packet_type}");
        }
    }

    /// The three types that require `0b0010` and one that requires zero. A
    /// wrong value is malformed, not ignorable ([MQTT-2.1.3-1]).
    #[test]
    fn reserved_flags_are_validated() {
        for packet_type in [
            PacketType::Pubrel,
            PacketType::Subscribe,
            PacketType::Unsubscribe,
        ] {
            assert_eq!(packet_type.required_flags(), Some(0b0010), "{packet_type}");
            let wire = [packet_type.as_bits() << 4, 0x02, 0x00, 0x01];
            assert_eq!(
                FixedHeader::decode(&wire, varint::MAX),
                Err(DecodeError::InvalidFlags {
                    packet_type,
                    flags: 0b0000
                }),
                "{packet_type} with zero flags"
            );
        }

        assert_eq!(
            FixedHeader::decode(&[0x1F, 0x00], varint::MAX),
            Err(DecodeError::InvalidFlags {
                packet_type: PacketType::Connect,
                flags: 0b1111
            })
        );
    }

    #[test]
    fn publish_owns_its_flags_and_refuses_qos_three() {
        // DUP 1, QoS 2, RETAIN 1 = 0b1101.
        let (header, used) = FixedHeader::decode(&[0x3D, 0x00], varint::MAX).expect("a header");
        assert_eq!(header.flags, 0b1101);
        assert_eq!(used, 2);

        // QoS 3 is the malformed combination.
        assert_eq!(
            FixedHeader::decode(&[0x36, 0x00], varint::MAX),
            Err(DecodeError::InvalidQos { qos: 3 })
        );
    }

    /// The ceiling is on the whole packet, fixed header included (2.1.4), and
    /// it is checked from the Remaining Length alone.
    #[test]
    fn an_oversized_declaration_is_refused_before_the_body() {
        // Remaining Length 128 encodes as `80 01`, so the fixed header is 3
        // bytes and the whole packet 131, over a 129-byte ceiling.
        let wire = [0x30, 0x80, 0x01];
        assert_eq!(
            FixedHeader::decode(&wire, 129),
            Err(DecodeError::PacketTooLarge {
                size: 131,
                max: 129
            })
        );
        // The same declaration passes under a ceiling that admits it, and the
        // decoder has still only seen three bytes.
        let (header, used) = FixedHeader::decode(&wire, 131).expect("fits");
        assert_eq!(header.remaining_length, 128);
        assert_eq!(used, 3);
        assert_eq!(header.packet_len(), 131);
    }

    #[test]
    fn a_truncated_remaining_length_asks_for_bytes() {
        assert_eq!(
            FixedHeader::decode(&[], varint::MAX),
            Err(DecodeError::Incomplete)
        );
        assert_eq!(
            FixedHeader::decode(&[0x10], varint::MAX),
            Err(DecodeError::Incomplete)
        );
        assert_eq!(
            FixedHeader::decode(&[0x10, 0x80], varint::MAX),
            Err(DecodeError::Incomplete)
        );
    }

    #[test]
    fn the_remaining_length_must_be_minimal() {
        assert_eq!(
            FixedHeader::decode(&[0x10, 0x81, 0x00], varint::MAX),
            Err(DecodeError::VarintNotMinimal)
        );
    }

    #[test]
    fn a_header_round_trips() {
        let header = FixedHeader::new(PacketType::Subscribe, 9);
        let mut out = Vec::new();
        header.encode(&mut out).expect("encodes");
        assert_eq!(out, [0x82, 0x09]);
        assert_eq!(
            FixedHeader::decode(&out, varint::MAX),
            Ok((header, out.len()))
        );
        assert_eq!(header.header_len(), 2);
    }

    #[test]
    fn the_direction_table_matches_the_specification() {
        assert!(PacketType::Connect.from_client() && !PacketType::Connect.from_server());
        assert!(PacketType::Connack.from_server() && !PacketType::Connack.from_client());
        assert!(PacketType::Pingreq.from_client() && !PacketType::Pingreq.from_server());
        assert!(PacketType::Pingresp.from_server() && !PacketType::Pingresp.from_client());
        // 5.0's change: DISCONNECT flows both ways, and AUTH does too.
        assert!(PacketType::Disconnect.from_client() && PacketType::Disconnect.from_server());
        assert!(PacketType::Auth.from_client() && PacketType::Auth.from_server());
        assert!(PacketType::Publish.from_client() && PacketType::Publish.from_server());
    }

    #[test]
    fn qos_three_is_malformed_and_the_others_round_trip() {
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            assert_eq!(QoS::from_bits(qos.as_bits()), Ok(qos));
        }
        assert_eq!(QoS::from_bits(3), Err(DecodeError::InvalidQos { qos: 3 }));
    }
}
