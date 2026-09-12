//! One control packet, fixed header included.
//!
//! [`Packet::decode`] is the entry point a reader loop calls with whatever it
//! has: it returns [`crate::DecodeError::Incomplete`] until the whole packet
//! is present, then the packet and the bytes it consumed. [`Packet::encode`]
//! is the mirror.
//!
//! **The order of operations is the bound.** `decode` reads the fixed header
//! first and therefore learns the declared size before it looks at the body;
//! the caller's `max_packet_size` is applied there, so an over-large
//! declaration is refused having consumed nothing and reserved nothing. Only
//! then is the body required to be present, and the body is *borrowed* rather
//! than copied. Between those two facts, "no remote input can cause unbounded
//! memory allocation" holds for this codec by construction and not by
//! inspection: a decoded packet's size is the sum of a few `Option`s and some
//! slices of the caller's own buffer.
//!
//! **A body that does not fill its Remaining Length is a Malformed Packet**,
//! not a request for more bytes. The declared length is the peer's own
//! statement about where this packet ends, so bytes left over inside it mean
//! the packet is wrong rather than late — [`crate::DecodeError::TrailingBytes`]
//! — and a field that runs past it is the same fault seen from the other side.
//! Getting this backwards is how a decoder deadlocks a connection: it waits
//! for bytes the peer already finished sending.
//!
//! This slice carries CONNECT and CONNACK. The remaining thirteen types arrive
//! in B-140, each as a variant here, so a caller's `match` is the list of what
//! the codec speaks.

use crate::connack::Connack;
use crate::connect::Connect;
use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, PacketType};

/// A decoded control packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    /// CONNECT (3.1).
    Connect(Connect<'a>),
    /// CONNACK (3.2).
    Connack(Connack<'a>),
}

impl<'a> Packet<'a> {
    /// This packet's type.
    #[must_use]
    pub const fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::Connack(_) => PacketType::Connack,
        }
    }

    /// Decodes one packet from the front of `input`.
    ///
    /// Returns the packet and the bytes it occupied, so a reader loop can
    /// advance its buffer. `max_packet_size` bounds the **whole packet**
    /// (2.1.4) [mqtt5 §3] and is checked from the fixed header alone.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Incomplete`] while bytes are still missing — the one
    /// variant that is not a verdict about the connection. Once the whole
    /// declared packet is present, a field that runs off its end is
    /// [`DecodeError::PacketLengthMismatch`] and bytes left inside it are
    /// [`DecodeError::TrailingBytes`]; both are Malformed Packets. Plus
    /// anything the fixed header or the properties report, and
    /// [`DecodeError::UnimplementedPacketType`] for a type this slice of the
    /// codec does not carry.
    pub fn decode(
        input: &'a [u8],
        max_packet_size: u32,
    ) -> Result<(Packet<'a>, usize), DecodeError> {
        let (header, header_len) = FixedHeader::decode(input, max_packet_size)?;

        let end = header_len
            .checked_add(header.remaining_length as usize)
            .ok_or(DecodeError::Incomplete)?;
        let body = input.get(header_len..end).ok_or(DecodeError::Incomplete)?;

        // Inside `body` the length is settled: the peer said this packet ends
        // here and every one of those bytes has arrived. So running out is the
        // packet contradicting its own header, and reporting `Incomplete` for
        // it would park a reader loop waiting for bytes the peer has already
        // finished sending.
        let mut reader = Reader::new(body);
        let packet = match header.packet_type {
            PacketType::Connect => {
                Packet::Connect(Connect::decode_body(&mut reader).map_err(exhausted)?)
            }
            PacketType::Connack => {
                Packet::Connack(Connack::decode_body(&mut reader).map_err(exhausted)?)
            }
            packet_type => return Err(DecodeError::UnimplementedPacketType { packet_type }),
        };

        if !reader.is_empty() {
            return Err(DecodeError::TrailingBytes {
                len: reader.remaining(),
            });
        }

        Ok((packet, end))
    }

    /// Bytes the packet's variable header and payload occupy: its Remaining
    /// Length.
    ///
    /// # Errors
    ///
    /// Whatever the packet body's own length computation reports, before a
    /// byte is written.
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        match self {
            Packet::Connect(connect) => connect.body_len(),
            Packet::Connack(connack) => connack.body_len(),
        }
    }

    /// Bytes the whole packet occupies, fixed header included. This is the
    /// number `Maximum Packet Size` bounds.
    ///
    /// # Errors
    ///
    /// As [`Packet::body_len`].
    pub fn encoded_len(&self) -> Result<u32, EncodeError> {
        let body = self.body_len()?;
        Ok(FixedHeader::new(self.packet_type(), body).packet_len() as u32)
    }

    /// Appends the whole packet to `out`.
    ///
    /// # Errors
    ///
    /// [`EncodeError::FieldTooLong`], [`EncodeError::PacketTooLong`],
    /// [`EncodeError::PropertyNotAllowed`] or
    /// [`EncodeError::InvalidPropertyValue`], each reported before anything is
    /// written.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let body = self.body_len()?;
        FixedHeader::new(self.packet_type(), body).encode(out)?;
        let before = out.len();
        match self {
            Packet::Connect(connect) => connect.encode_body(out)?,
            Packet::Connack(connack) => connack.encode_body(out)?,
        }
        debug_assert_eq!(out.len() - before, body as usize, "body_len agrees");
        Ok(())
    }

    /// Appends the whole packet, refusing one above the peer's declared
    /// `Maximum Packet Size`.
    ///
    /// "The Client MUST NOT send packets exceeding Maximum Packet Size to the
    /// Server" ([MQTT-3.2.2-15]) (3.2.2.3.6) [mqtt5 §5], so a client that uses
    /// this method cannot earn a DISCONNECT 0x95 by accident. The limit is a
    /// parameter rather than state because it is negotiated per connection and
    /// this crate holds none.
    ///
    /// # Errors
    ///
    /// [`EncodeError::PacketTooLarge`] above `max_packet_size`, plus anything
    /// [`Packet::encode`] reports.
    pub fn encode_within(
        &self,
        max_packet_size: u32,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        let size = self.encoded_len()?;
        if size > max_packet_size {
            return Err(EncodeError::PacketTooLarge {
                size,
                max: max_packet_size,
            });
        }
        self.encode(out)
    }
}

/// Inside a packet body whose declared length has fully arrived, "not enough
/// bytes" is the packet contradicting its own Remaining Length: a Malformed
/// Packet, never a request for more.
fn exhausted(error: DecodeError) -> DecodeError {
    match error {
        DecodeError::Incomplete => DecodeError::PacketLengthMismatch,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;

    /// Vector 1, byte by byte: every prefix asks for more, and the whole thing
    /// decodes. This is the property a stream reader depends on.
    #[test]
    fn every_short_prefix_is_incomplete_and_never_a_verdict() {
        let wire = [
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        for len in 0..wire.len() {
            let error = Packet::decode(&wire[..len], varint::MAX).expect_err("short");
            assert_eq!(error, DecodeError::Incomplete, "prefix of {len} bytes");
            assert!(!error.is_violation());
        }
        let (_, used) = Packet::decode(&wire, varint::MAX).expect("whole");
        assert_eq!(used, wire.len());
    }

    /// Trailing bytes past the fixed header's own declared end stop the packet
    /// rather than being consumed as the next one's.
    #[test]
    fn bytes_after_a_packet_are_left_for_the_next_one() {
        let mut wire = vec![
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        let packet_len = wire.len();
        wire.extend_from_slice(&[0xC0, 0x00]);
        let (_, used) = Packet::decode(&wire, varint::MAX).expect("the first packet");
        assert_eq!(used, packet_len);
    }

    /// A Remaining Length longer than the body needs leaves bytes inside the
    /// packet, which is malformed and deliberately not `Incomplete`.
    #[test]
    fn a_body_shorter_than_its_declared_length_is_malformed() {
        let wire = [
            0x10, 0x0F, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a', 0x00,
        ];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::TrailingBytes { len: 1 });
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
    }

    /// A field that runs past the declared end is the same fault from the
    /// other side, and is reported without waiting for bytes the peer has
    /// already stopped sending.
    #[test]
    fn a_field_running_past_the_declared_end_does_not_wait_for_more() {
        // Remaining Length 13 cuts the client identifier's payload off, and
        // the buffer holds more bytes than the packet claims.
        let wire = [
            0x10, 0x0D, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        let error = Packet::decode(&wire, varint::MAX).expect_err("refused");
        assert_eq!(error, DecodeError::PacketLengthMismatch);
        assert_eq!(error.reason_code(), Some(crate::error::MALFORMED_PACKET));
        // The distinction that matters: a reader loop must not park on this.
        // The buffer already holds more bytes than the packet claims, so no
        // further read can help.
        assert!(error.is_violation());
    }

    #[test]
    fn the_declared_size_is_checked_against_the_callers_maximum() {
        let wire = [
            0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, b'a',
        ];
        assert_eq!(
            Packet::decode(&wire, 15),
            Err(DecodeError::PacketTooLarge { size: 16, max: 15 })
        );
        assert!(Packet::decode(&wire, 16).is_ok());
    }

    /// The mirror on the way out: a packet above the peer's declared maximum
    /// is refused here rather than earning a DISCONNECT 0x95 there.
    #[test]
    fn encoding_refuses_a_packet_above_the_peers_maximum() {
        let packet = Packet::Connect(Connect {
            client_id: "a",
            clean_start: true,
            keep_alive: 60,
            ..Connect::default()
        });
        assert_eq!(packet.encoded_len(), Ok(16));

        let mut out = Vec::new();
        assert_eq!(
            packet.encode_within(15, &mut out),
            Err(EncodeError::PacketTooLarge { size: 16, max: 15 })
        );
        assert!(out.is_empty(), "nothing was written");
        assert!(packet.encode_within(16, &mut out).is_ok());
        assert_eq!(out.len(), 16);
    }

    /// A large declaration over a tiny buffer allocates nothing: if it did,
    /// this test would abort the process rather than fail.
    ///
    /// The ceiling here is 268,435,460 and not [`varint::MAX`], which is the
    /// arithmetic worth pinning: `Maximum Packet Size` bounds the **whole
    /// packet**, so the largest legal packet is the 268,435,455-byte Remaining
    /// Length plus its own five bytes of fixed header (2.1.4).
    #[test]
    fn a_quarter_gigabyte_declaration_over_five_bytes_allocates_nothing() {
        let wire = [0x10, 0xFF, 0xFF, 0xFF, 0x7F];
        assert_eq!(
            Packet::decode(&wire, 268_435_460),
            Err(DecodeError::Incomplete)
        );
        // One byte below that, the same declaration is refused outright.
        assert_eq!(
            Packet::decode(&wire, varint::MAX),
            Err(DecodeError::PacketTooLarge {
                size: 268_435_460,
                max: varint::MAX
            })
        );
        // And at a realistic ceiling, likewise.
        assert_eq!(
            Packet::decode(&wire, 1024),
            Err(DecodeError::PacketTooLarge {
                size: 268_435_460,
                max: 1024
            })
        );
    }
}
