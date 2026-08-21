//! Stream preamble: `[0x57][kind][header_len varint][CBOR header]`.
//!
//! Every unidirectional stream in both directions starts with this preamble.
//! Parsing it is the first thing that touches hostile bytes, so it is the place
//! where the header size cap is enforced — before any allocation.

use std::fmt;

use crate::varint::{self, VarintError, decode_varint, encode_varint};

/// First byte of every weida stream: ASCII `W`.
pub const MAGIC: u8 = 0x57;

/// Longest possible preamble: magic, kind and an 8-byte varint.
pub const MAX_PREAMBLE_LEN: usize = 2 + varint::MAX_ENCODED_LEN;

/// What a stream carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// Connection negotiation; header-only.
    Hello,
    /// A transfer: header followed by opaque payload until FIN.
    Data,
    /// Acknowledgement of a transfer; header-only.
    Ack,
    /// Failure report for a transfer; header-only.
    Error,
    /// Withdrawal of interest in a request's replies; header-only.
    Cancel,
    /// Registration of interest in a publisher's topics; header-only.
    Subscribe,
    /// Withdrawal of a previous SUBSCRIBE; header-only.
    Unsubscribe,
}

impl FrameKind {
    /// The wire code.
    pub const fn to_u8(self) -> u8 {
        match self {
            FrameKind::Hello => 0,
            FrameKind::Data => 1,
            FrameKind::Ack => 2,
            FrameKind::Error => 3,
            FrameKind::Cancel => 4,
            FrameKind::Subscribe => 5,
            FrameKind::Unsubscribe => 6,
        }
    }

    /// Interprets a wire code. Unknown kinds are a protocol violation, not a
    /// forward-compatibility hook: a receiver cannot know whether an unknown
    /// stream kind carries payload it would have to drain.
    pub const fn from_u8(code: u8) -> Option<FrameKind> {
        match code {
            0 => Some(FrameKind::Hello),
            1 => Some(FrameKind::Data),
            2 => Some(FrameKind::Ack),
            3 => Some(FrameKind::Error),
            4 => Some(FrameKind::Cancel),
            5 => Some(FrameKind::Subscribe),
            6 => Some(FrameKind::Unsubscribe),
            _ => None,
        }
    }

    /// True if payload bytes follow the header until FIN.
    pub const fn has_payload(self) -> bool {
        matches!(self, FrameKind::Data)
    }
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FrameKind::Hello => "HELLO",
            FrameKind::Data => "DATA",
            FrameKind::Ack => "ACK",
            FrameKind::Error => "ERROR",
            FrameKind::Cancel => "CANCEL",
            FrameKind::Subscribe => "SUBSCRIBE",
            FrameKind::Unsubscribe => "UNSUBSCRIBE",
        };
        f.write_str(s)
    }
}

/// A parsed stream preamble.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Preamble {
    /// Stream kind.
    pub kind: FrameKind,
    /// Length of the CBOR header that follows, in bytes.
    pub header_len: u64,
}

/// Why a preamble was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreambleError {
    /// Not enough bytes yet. Read more and retry; **not** a protocol violation.
    Incomplete,
    /// The first byte was not [`MAGIC`].
    BadMagic(u8),
    /// The kind byte is not defined in this protocol version.
    UnknownKind(u8),
    /// The advertised header length exceeds the local cap. Reported before any
    /// allocation is attempted.
    HeaderTooLarge {
        /// Length the peer advertised.
        len: u64,
        /// Local cap that was exceeded.
        max: u64,
    },
}

impl PreambleError {
    /// True if the error is fatal for the connection. Only
    /// [`PreambleError::Incomplete`] is not.
    pub const fn is_violation(self) -> bool {
        !matches!(self, PreambleError::Incomplete)
    }
}

impl fmt::Display for PreambleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreambleError::Incomplete => f.write_str("incomplete preamble"),
            PreambleError::BadMagic(b) => write!(f, "bad magic byte {b:#04x}, expected 0x57"),
            PreambleError::UnknownKind(k) => write!(f, "unknown frame kind {k}"),
            PreambleError::HeaderTooLarge { len, max } => {
                write!(f, "header length {len} exceeds the limit of {max} bytes")
            }
        }
    }
}

impl std::error::Error for PreambleError {}

/// Parses a preamble from the front of `input`.
///
/// Returns the preamble and how many bytes it occupied. `max_header_bytes` is
/// the local limit; a larger advertised length is rejected here, which is the
/// only place where that check can happen before memory is committed.
pub fn parse_preamble(
    input: &[u8],
    max_header_bytes: u64,
) -> Result<(Preamble, usize), PreambleError> {
    if input.len() < 2 {
        return Err(PreambleError::Incomplete);
    }
    if input[0] != MAGIC {
        return Err(PreambleError::BadMagic(input[0]));
    }
    let kind = FrameKind::from_u8(input[1]).ok_or(PreambleError::UnknownKind(input[1]))?;
    let (header_len, used) = match decode_varint(&input[2..]) {
        Ok(v) => v,
        Err(VarintError::Truncated) => return Err(PreambleError::Incomplete),
        // A varint cannot be out of range while decoding: two bits select the
        // length, so every 8-byte form is within 2^62.
        Err(VarintError::OutOfRange) => unreachable!("decoding cannot overflow"),
    };
    if header_len > max_header_bytes {
        return Err(PreambleError::HeaderTooLarge {
            len: header_len,
            max: max_header_bytes,
        });
    }
    Ok((Preamble { kind, header_len }, 2 + used))
}

/// Appends a preamble for `header_len` bytes of header to `out`.
pub fn encode_preamble(kind: FrameKind, header_len: u64, out: &mut Vec<u8>) {
    out.push(MAGIC);
    out.push(kind.to_u8());
    encode_varint(header_len, out).expect("header lengths are bounded far below 2^62");
}

/// Builds a complete header-only frame: preamble followed by `header`.
pub fn encode_frame(kind: FrameKind, header: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAX_PREAMBLE_LEN + header.len());
    encode_preamble(kind, header.len() as u64, &mut out);
    out.extend_from_slice(header);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: u64 = 16 * 1024;

    #[test]
    fn kind_codes_match_the_protocol_document() {
        let all = [
            (FrameKind::Hello, 0u8),
            (FrameKind::Data, 1),
            (FrameKind::Ack, 2),
            (FrameKind::Error, 3),
            (FrameKind::Cancel, 4),
            (FrameKind::Subscribe, 5),
            (FrameKind::Unsubscribe, 6),
        ];
        for (kind, code) in all {
            assert_eq!(kind.to_u8(), code);
            assert_eq!(FrameKind::from_u8(code), Some(kind));
        }
        for code in 7u8..=255 {
            assert_eq!(FrameKind::from_u8(code), None, "kind {code}");
        }
    }

    #[test]
    fn only_data_carries_payload() {
        assert!(FrameKind::Data.has_payload());
        for k in [
            FrameKind::Hello,
            FrameKind::Ack,
            FrameKind::Error,
            FrameKind::Cancel,
            FrameKind::Subscribe,
            FrameKind::Unsubscribe,
        ] {
            assert!(!k.has_payload(), "{k}");
        }
    }

    #[test]
    fn roundtrip_through_encode_and_parse() {
        for len in [0u64, 1, 63, 64, 16_383, 16_384] {
            let mut buf = Vec::new();
            encode_preamble(FrameKind::Data, len, &mut buf);
            let (p, used) = parse_preamble(&buf, CAP).unwrap();
            assert_eq!(p.kind, FrameKind::Data);
            assert_eq!(p.header_len, len);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn every_kind_encodes_its_documented_kind_byte() {
        // Per-kind preamble encoding and round-trip. The golden *frames* of
        // docs/PROTOCOL.md §8 are asserted end-to-end, against real headers,
        // in `tests/golden_vectors.rs`; this covers the kind byte for every
        // kind, including those no golden vector exercises.
        for (kind, code) in [
            (FrameKind::Hello, 0x00u8),
            (FrameKind::Data, 0x01),
            (FrameKind::Ack, 0x02),
            (FrameKind::Error, 0x03),
            (FrameKind::Cancel, 0x04),
            (FrameKind::Subscribe, 0x05),
            (FrameKind::Unsubscribe, 0x06),
        ] {
            for header_len in [0usize, 3, 5, 9, 11, 16, 18] {
                let frame = encode_frame(kind, &vec![0; header_len]);
                assert_eq!(frame[0], MAGIC, "{kind}: magic");
                assert_eq!(frame[1], code, "{kind}: kind byte");
                let (preamble, used) = parse_preamble(&frame, CAP).unwrap();
                assert_eq!(preamble.kind, kind);
                assert_eq!(preamble.header_len as usize, header_len);
                assert_eq!(frame.len() - used, header_len, "{kind}: header follows");
            }
        }
    }

    #[test]
    fn incomplete_input_is_not_a_violation() {
        for prefix in [
            &[][..],
            &[MAGIC][..],
            &[MAGIC, 1, 0x80][..],
            &[MAGIC, 1, 0xc0, 0, 0][..],
        ] {
            let err = parse_preamble(prefix, CAP).unwrap_err();
            assert_eq!(err, PreambleError::Incomplete, "{prefix:?}");
            assert!(!err.is_violation());
        }
    }

    #[test]
    fn bad_magic_is_a_violation() {
        let err = parse_preamble(&[0x58, 0x01, 0x00], CAP).unwrap_err();
        assert_eq!(err, PreambleError::BadMagic(0x58));
        assert!(err.is_violation());
    }

    #[test]
    fn unknown_kind_is_a_violation() {
        let err = parse_preamble(&[MAGIC, 0x07, 0x00], CAP).unwrap_err();
        assert_eq!(err, PreambleError::UnknownKind(7));
        assert!(err.is_violation());
    }

    #[test]
    fn oversized_header_is_rejected_before_allocation() {
        // 1 MiB advertised against a 16 KiB cap.
        let mut buf = vec![MAGIC, FrameKind::Data.to_u8()];
        encode_varint(1024 * 1024, &mut buf).unwrap();
        let err = parse_preamble(&buf, CAP).unwrap_err();
        assert_eq!(
            err,
            PreambleError::HeaderTooLarge {
                len: 1024 * 1024,
                max: CAP
            }
        );
        assert!(err.is_violation());
    }

    #[test]
    fn a_header_exactly_at_the_cap_is_accepted() {
        let mut buf = vec![MAGIC, FrameKind::Hello.to_u8()];
        encode_varint(CAP, &mut buf).unwrap();
        assert_eq!(parse_preamble(&buf, CAP).unwrap().0.header_len, CAP);
    }

    #[test]
    fn non_minimal_length_encodings_are_accepted() {
        // header_len = 5 in the 8-byte form.
        let buf = [MAGIC, FrameKind::Ack.to_u8(), 0xc0, 0, 0, 0, 0, 0, 0, 5];
        let (p, used) = parse_preamble(&buf, CAP).unwrap();
        assert_eq!(p.header_len, 5);
        assert_eq!(used, 10);
    }

    #[test]
    fn payload_bytes_after_the_preamble_are_untouched() {
        let frame = encode_frame(FrameKind::Data, &[0xaa, 0xbb]);
        let (p, used) = parse_preamble(&frame, CAP).unwrap();
        assert_eq!(p.header_len, 2);
        assert_eq!(&frame[used..], &[0xaa, 0xbb]);
    }
}
