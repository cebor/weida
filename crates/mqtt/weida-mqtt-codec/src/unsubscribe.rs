//! UNSUBSCRIBE and UNSUBACK.
//!
//! ```text
//! UNSUBSCRIBE (fixed-header flags 0010)
//!   packet identifier  Two Byte Integer
//!   properties         User Property only (3.10.2.1)
//!   payload            one or more Topic Filters, no options byte
//!
//! UNSUBACK
//!   packet identifier  Two Byte Integer
//!   properties         Reason String, User Property (3.11.2.1)
//!   payload            one Reason Code per filter, in request order (3.11.3)
//! ```
//!
//! **UNSUBSCRIBE's payload entries carry no options byte**, which is the one
//! asymmetry with SUBSCRIBE worth stating: a subscription is keyed by its
//! filter and nothing else — "a session cannot hold two with the same filter,
//! so the filter is the key" (4.8.1) [mqtt5 §2] — so unsubscribing needs the
//! key and no more.
//!
//! **UNSUBACK is the packet 3.1.1 had nothing in.** There, UNSUBACK carried no
//! status at all [mqtt5 §1.9], so a client could not tell a deleted
//! subscription from one that was never there from one it was not allowed to
//! touch. 5.0 answers per filter, and `0x11 No subscription existed` is a
//! **success**: the end state the client asked for holds either way.
//!
//! Both packets require at least one entry ([MQTT-3.10.3-2]) and neither has
//! a short form.

use crate::data::{self, Reader};
use crate::error::{DecodeError, EncodeError};
use crate::list::{PayloadItem, PayloadList};
use crate::property::{Properties, PropertySet};
use crate::publish::non_zero;
use crate::reason::UnsubackReasonCode;
use crate::types::PacketType;

impl<'a> PayloadItem<'a> for &'a str {
    fn read(reader: &mut Reader<'a>) -> Result<Self, DecodeError> {
        reader.string()
    }

    fn item_len(&self) -> Result<u32, EncodeError> {
        data::field_len(self.len())
    }

    fn put(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        data::put_string(self, out)
    }
}

impl<'a> PayloadItem<'a> for UnsubackReasonCode {
    fn read(reader: &mut Reader<'a>) -> Result<Self, DecodeError> {
        UnsubackReasonCode::from_byte(reader.u8()?)
    }

    fn item_len(&self) -> Result<u32, EncodeError> {
        Ok(1)
    }

    fn put(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        out.push(self.as_byte());
        Ok(())
    }
}

/// An UNSUBSCRIBE packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Unsubscribe<'a> {
    /// The Packet Identifier; non-zero ([MQTT-2.2.1-3]). Freed on UNSUBACK
    /// [mqtt5 §2].
    pub packet_id: u16,
    /// The UNSUBSCRIBE properties: `User Property` only (3.10.2.1).
    pub properties: Properties<'a>,
    /// The Topic Filters to remove, one or more ([MQTT-3.10.3-2]).
    pub filters: PayloadList<'a, &'a str>,
}

impl<'a> Unsubscribe<'a> {
    /// Decodes the variable header and payload.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidPacketIdentifier`], [`DecodeError::EmptyPayload`],
    /// and anything the property or string readers report.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Unsubscribe<'a>, DecodeError> {
        let packet_id = non_zero(reader.u16()?)?;
        let properties = Properties::decode(
            reader,
            PropertySet::UNSUBSCRIBE,
            Some(PacketType::Unsubscribe),
        )?;
        let filters = PayloadList::decode(reader, PacketType::Unsubscribe)?;
        Ok(Unsubscribe {
            packet_id,
            properties,
            filters,
        })
    }

    /// Bytes the variable header and payload occupy.
    ///
    /// # Errors
    ///
    /// [`EncodeError::InvalidPacketIdentifier`] or
    /// [`EncodeError::EmptyPayload`], plus whatever the properties and filters
    /// report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidPacketIdentifier);
        }
        if self.filters.is_empty() {
            return Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Unsubscribe,
            });
        }
        Ok(2 + self.properties.encoded_len(PropertySet::UNSUBSCRIBE)? + self.filters.body_len()?)
    }

    /// Appends the variable header and payload.
    ///
    /// # Errors
    ///
    /// As [`Unsubscribe::body_len`].
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.body_len()?;
        out.extend_from_slice(&self.packet_id.to_be_bytes());
        self.properties.encode(PropertySet::UNSUBSCRIBE, out)?;
        self.filters.encode(out)
    }
}

/// An UNSUBACK packet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Unsuback<'a> {
    /// The Packet Identifier of the UNSUBSCRIBE being answered.
    pub packet_id: u16,
    /// The UNSUBACK properties: `Reason String` and `User Property`
    /// (3.11.2.1).
    pub properties: Properties<'a>,
    /// One reason code per filter, in the order the UNSUBSCRIBE listed them
    /// (3.11.3) [mqtt5 §6].
    pub reason_codes: PayloadList<'a, UnsubackReasonCode>,
}

impl<'a> Unsuback<'a> {
    /// Decodes the variable header and payload.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidPacketIdentifier`], [`DecodeError::EmptyPayload`],
    /// [`DecodeError::InvalidReasonCode`], and anything the property reader
    /// reports.
    pub fn decode_body(reader: &mut Reader<'a>) -> Result<Unsuback<'a>, DecodeError> {
        let packet_id = non_zero(reader.u16()?)?;
        let properties =
            Properties::decode(reader, PropertySet::SUBACK, Some(PacketType::Unsuback))?;
        let reason_codes = PayloadList::decode(reader, PacketType::Unsuback)?;
        Ok(Unsuback {
            packet_id,
            properties,
            reason_codes,
        })
    }

    /// Bytes the variable header and payload occupy.
    ///
    /// # Errors
    ///
    /// [`EncodeError::InvalidPacketIdentifier`] or
    /// [`EncodeError::EmptyPayload`], plus whatever the properties report.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidPacketIdentifier);
        }
        if self.reason_codes.is_empty() {
            return Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Unsuback,
            });
        }
        Ok(2 + self.properties.encoded_len(PropertySet::SUBACK)? + self.reason_codes.body_len()?)
    }

    /// Appends the variable header and payload.
    ///
    /// # Errors
    ///
    /// As [`Unsuback::body_len`].
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.body_len()?;
        out.extend_from_slice(&self.packet_id.to_be_bytes());
        self.properties.encode(PropertySet::SUBACK, out)?;
        self.reason_codes.encode(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;
    use crate::{Packet, reason};

    #[track_caller]
    fn round_trip(packet: &Packet<'_>, expected: &[u8]) {
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        assert_eq!(out, expected, "{:?}", packet.packet_type());
        let (decoded, used) = Packet::decode(expected, varint::MAX).expect("decodes");
        assert_eq!(used, expected.len());
        assert_eq!(&decoded, packet);
    }

    /// Vector 14: no options byte, unlike SUBSCRIBE. The same filter therefore
    /// costs one byte less here, which is the check that the two payload
    /// grammars have not been confused.
    #[test]
    fn vector_14_an_unsubscribe_has_no_options_byte() {
        let filters = ["a/+"];
        round_trip(
            &Packet::Unsubscribe(Unsubscribe {
                packet_id: 2,
                properties: Properties::new(),
                filters: PayloadList::new(&filters),
            }),
            &[0xA2, 0x08, 0x00, 0x02, 0x00, 0x00, 0x03, b'a', b'/', b'+'],
        );
    }

    /// Vector 15: 5.0 gives UNSUBACK reason codes where 3.1.1 gave none.
    #[test]
    fn vector_15_an_unsuback_answers_per_filter() {
        let codes = [UnsubackReasonCode::Success];
        round_trip(
            &Packet::Unsuback(Unsuback {
                packet_id: 2,
                properties: Properties::new(),
                reason_codes: PayloadList::new(&codes),
            }),
            &[0xB0, 0x04, 0x00, 0x02, 0x00, 0x00],
        );
    }

    /// The verdict a 3.1.1 client could never see: nothing was there, and that
    /// is a success rather than a complaint.
    #[test]
    fn no_subscription_existed_is_a_success() {
        let codes = [
            UnsubackReasonCode::Success,
            UnsubackReasonCode::NoSubscriptionExisted,
            UnsubackReasonCode::NotAuthorized,
        ];
        let packet = Packet::Unsuback(Unsuback {
            packet_id: 9,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&codes),
        });
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, packet);
        assert!(!codes[1].is_error(), "0x11 is below the 0x80 line");
        assert!(codes[2].is_error());
    }

    #[test]
    fn many_filters_keep_their_order() {
        let filters = ["a", "b/#", "$share/g/c/+"];
        let packet = Packet::Unsubscribe(Unsubscribe {
            packet_id: 0x1234,
            properties: Properties::new(),
            filters: PayloadList::new(&filters),
        });
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, packet);
        let Packet::Unsubscribe(decoded) = decoded else {
            panic!("an unsubscribe")
        };
        assert_eq!(decoded.filters.iter().collect::<Vec<_>>(), filters);
    }

    #[test]
    fn an_empty_payload_is_refused_both_ways() {
        for (wire, packet_type) in [
            (
                &[0xA2u8, 0x03, 0x00, 0x02, 0x00][..],
                PacketType::Unsubscribe,
            ),
            (&[0xB0, 0x03, 0x00, 0x02, 0x00][..], PacketType::Unsuback),
        ] {
            let error = Packet::decode(wire, varint::MAX).expect_err("no entries");
            assert_eq!(error, DecodeError::EmptyPayload { packet_type });
            assert_eq!(error.reason_code(), Some(crate::error::PROTOCOL_ERROR));
        }

        let mut out = Vec::new();
        assert_eq!(
            Packet::Unsubscribe(Unsubscribe {
                packet_id: 1,
                ..Unsubscribe::default()
            })
            .encode(&mut out),
            Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Unsubscribe
            })
        );
        assert_eq!(
            Packet::Unsuback(Unsuback {
                packet_id: 1,
                ..Unsuback::default()
            })
            .encode(&mut out),
            Err(EncodeError::EmptyPayload {
                packet_type: PacketType::Unsuback
            })
        );
    }

    #[test]
    fn every_unsuback_reason_code_survives_a_round_trip() {
        let codes: Vec<UnsubackReasonCode> = reason::UNSUBACK_REASON_CODES.to_vec();
        let packet = Packet::Unsuback(Unsuback {
            packet_id: 1,
            properties: Properties::new(),
            reason_codes: PayloadList::new(&codes),
        });
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, packet);
    }

    /// UNSUBACK's subset is narrower than SUBACK's: the granted-QoS codes and
    /// the three availability refusals have no meaning here.
    #[test]
    fn suback_codes_are_not_unsuback_codes() {
        // 0x01 is SUBACK's "Granted QoS 1" and nothing on UNSUBACK.
        assert_eq!(
            Packet::decode(&[0xB0, 0x04, 0x00, 0x02, 0x00, 0x01], varint::MAX),
            Err(DecodeError::InvalidReasonCode {
                packet_type: PacketType::Unsuback,
                code: 0x01
            })
        );
        // 0x11 is UNSUBACK's and nothing on SUBACK.
        assert_eq!(
            Packet::decode(&[0x90, 0x04, 0x00, 0x01, 0x00, 0x11], varint::MAX),
            Err(DecodeError::InvalidReasonCode {
                packet_type: PacketType::Suback,
                code: 0x11
            })
        );
    }

    #[test]
    fn only_user_property_rides_on_an_unsubscribe() {
        let pairs = [("k", "v")];
        let filters = ["a"];
        let packet = Packet::Unsubscribe(Unsubscribe {
            packet_id: 1,
            properties: Properties::new().with_user_properties(&pairs),
            filters: PayloadList::new(&filters),
        });
        let mut out = Vec::new();
        packet.encode(&mut out).expect("encodes");
        let (decoded, _) = Packet::decode(&out, varint::MAX).expect("decodes");
        assert_eq!(decoded, packet);

        // A Reason String is a SUBACK/UNSUBACK property, not an UNSUBSCRIBE
        // one (3.10.2.1).
        let mut out = Vec::new();
        assert!(
            Packet::Unsubscribe(Unsubscribe {
                packet_id: 1,
                properties: Properties {
                    reason_string: Some("why"),
                    ..Properties::new()
                },
                filters: PayloadList::new(&filters),
            })
            .encode(&mut out)
            .is_err()
        );
    }
}
