//! The Variable Byte Integer: 1 to 4 bytes, seven value bits each, and
//! exactly one encoding per value.
//!
//! ```text
//! +---------------+---------------+---------------+---------------+
//! | 1 vvvvvvv     | 1 vvvvvvv     | 1 vvvvvvv     | 0 vvvvvvv     |
//! +---------------+---------------+---------------+---------------+
//!   least significant seven bits first, continuation bit is bit 7
//! ```
//!
//! It carries the Remaining Length of every packet, the length of every
//! property block, property identifiers, and the `Subscription Identifier`
//! value (1.5.5, 2.1.4, 2.2.2) [mqtt5 §3].
//!
//! **Minimality is the whole of the interesting behaviour.** "The encoded
//! value MUST use the minimum number of bytes necessary to represent the
//! value" ([MQTT-1.5.5-1]) [mqtt5 §3], which makes the encoding canonical:
//! one value, one byte string, so a golden vector is binding and a length
//! cannot be smuggled past a size check by padding it. The test is exact and
//! cheap — **more than one byte, and the last byte is `0x00`** — because a
//! value whose most significant encoded byte is zero fits in one byte fewer.
//! `0x81 0x00` (the value 1 in two bytes) and `0x80 0x00` (the value 0 in two
//! bytes) are therefore refused; `0x80 0x01` is *not* an example of the fault,
//! because 128 has no shorter encoding.
//!
//! Nothing here allocates, and `decode` never reads past the byte that clears
//! the continuation bit, so a caller may hand it a whole receive buffer.

use crate::error::{DecodeError, EncodeError};

/// Bytes a Variable Byte Integer may occupy (1.5.5) [mqtt5 §3].
pub const MAX_BYTES: usize = 4;

/// The largest value the encoding can carry: `0xFF 0xFF 0xFF 0x7F`, just under
/// 256 MiB, and therefore the protocol's ceiling on a whole packet (2.1.4)
/// [mqtt5 §3].
pub const MAX: u32 = 268_435_455;

/// Bytes the minimal encoding of `value` occupies.
///
/// A pure function, so a packet's encoded size can be computed before a single
/// byte is written — which is what lets [`crate::Packet::encode_within`] refuse
/// an oversized packet without building it.
#[must_use]
pub const fn encoded_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

/// Reads a Variable Byte Integer from the front of `input`.
///
/// Returns the value and the bytes it occupied.
///
/// # Errors
///
/// [`DecodeError::Incomplete`] when the continuation bit is set on the last
/// available byte — a request for more bytes, not a violation.
/// [`DecodeError::VarintTooLong`] when a fifth byte would be needed, and
/// [`DecodeError::VarintNotMinimal`] when the value had a shorter encoding.
pub fn decode(input: &[u8]) -> Result<(u32, usize), DecodeError> {
    let mut value: u32 = 0;
    let mut shift = 0;

    for (index, &byte) in input.iter().take(MAX_BYTES).enumerate() {
        value |= u32::from(byte & 0x7F) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            if index > 0 && byte == 0 {
                return Err(DecodeError::VarintNotMinimal);
            }
            return Ok((value, index + 1));
        }
    }

    if input.len() > MAX_BYTES {
        Err(DecodeError::VarintTooLong)
    } else {
        Err(DecodeError::Incomplete)
    }
}

/// Appends the minimal encoding of `value` to `out`.
///
/// # Errors
///
/// [`EncodeError::PacketTooLong`] when `value` exceeds [`MAX`], which is the
/// only way a Variable Byte Integer can fail to exist.
pub fn encode(value: u32, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if value > MAX {
        return Err(EncodeError::PacketTooLong {
            len: u64::from(value),
        });
    }

    let mut rest = value;
    loop {
        let mut byte = (rest % 128) as u8;
        rest /= 128;
        if rest > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if rest == 0 {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The specification's own boundary table (1.5.5): where each byte width
    /// starts and ends.
    #[test]
    fn the_four_widths_have_the_documented_boundaries() {
        for (value, width) in [
            (0u32, 1usize),
            (127, 1),
            (128, 2),
            (16_383, 2),
            (16_384, 3),
            (2_097_151, 3),
            (2_097_152, 4),
            (MAX, 4),
        ] {
            let mut out = Vec::new();
            encode(value, &mut out).expect("within range");
            assert_eq!(out.len(), width, "{value} encodes in {width} bytes");
            assert_eq!(encoded_len(value), width, "{value}: encoded_len agrees");
            assert_eq!(decode(&out), Ok((value, width)), "{value} round-trips");
        }
    }

    #[test]
    fn the_boundary_encodings_are_the_specifications_bytes() {
        let mut out = Vec::new();
        encode(128, &mut out).unwrap();
        assert_eq!(out, [0x80, 0x01]);

        out.clear();
        encode(MAX, &mut out).unwrap();
        assert_eq!(out, [0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn a_non_minimal_encoding_is_refused() {
        // The value 1 in two bytes, and 0 in two, three and four.
        for bytes in [
            &[0x81u8, 0x00][..],
            &[0x80, 0x00][..],
            &[0x80, 0x80, 0x00][..],
            &[0x80, 0x80, 0x80, 0x00][..],
            &[0xFF, 0xFF, 0x80, 0x00][..],
        ] {
            assert_eq!(
                decode(bytes),
                Err(DecodeError::VarintNotMinimal),
                "{bytes:02X?} is not minimal"
            );
        }
    }

    /// 128 is the encoding a naive minimality check gets wrong: its most
    /// significant byte is 0x01, not 0x00, so two bytes is minimal.
    #[test]
    fn the_minimal_two_byte_encoding_is_accepted() {
        assert_eq!(decode(&[0x80, 0x01]), Ok((128, 2)));
    }

    #[test]
    fn a_fifth_byte_is_not_a_variable_byte_integer() {
        assert_eq!(
            decode(&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
            Err(DecodeError::VarintTooLong)
        );
    }

    /// A truncated integer asks for bytes rather than condemning the
    /// connection, because MQTT runs on a stream.
    #[test]
    fn a_truncated_integer_is_incomplete_and_not_a_violation() {
        for bytes in [
            &[][..],
            &[0x80][..],
            &[0xFF, 0xFF][..],
            &[0x80, 0x80, 0x80][..],
        ] {
            assert_eq!(decode(bytes), Err(DecodeError::Incomplete), "{bytes:02X?}");
        }
        assert!(!DecodeError::Incomplete.is_violation());
        assert!(DecodeError::VarintNotMinimal.is_violation());
    }

    #[test]
    fn decoding_stops_at_the_terminating_byte() {
        let (value, used) = decode(&[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).expect("one byte");
        assert_eq!((value, used), (1, 1));
    }

    #[test]
    fn a_value_above_the_ceiling_cannot_be_encoded() {
        let mut out = Vec::new();
        assert_eq!(
            encode(MAX + 1, &mut out),
            Err(EncodeError::PacketTooLong {
                len: u64::from(MAX) + 1
            })
        );
    }
}
