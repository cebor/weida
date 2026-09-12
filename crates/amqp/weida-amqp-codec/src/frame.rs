//! The 8-octet frame header, and the two frame types AMQP assigns.
//!
//! ```text
//! +0       +1       +2       +3       +4     +5     +6       +7
//! +--------+--------+--------+--------+------+------+--------+--------+
//! |              SIZE (4 octets)      | DOFF | TYPE | type-specific   |
//! +-----------------------------------+------+------+-----------------+
//! |  extended header, DOFF*4 - 8 octets, ignored for an AMQP frame    |
//! +-------------------------------------------------------------------+
//! |  body, SIZE - DOFF*4 octets                                       |
//! +-------------------------------------------------------------------+
//! ```
//!
//! `SIZE` counts the whole frame including these eight octets. `DOFF` is the
//! body's offset in four-octet words, so `DOFF = 2` means "no extended
//! header". `TYPE` is `0x00` for an AMQP frame (Part 2 §2.3.2) or `0x01` for a
//! SASL frame (Part 5 §5.3.1). The two type-specific octets are the channel
//! for an AMQP frame and are required to be ignored for a SASL frame.
//!
//! Part 2 §2.3 makes `SIZE` < 8 and `DOFF` < 2 malformed, and this module
//! reports both from the header alone.
//!
//! # The bound before `open`
//!
//! `open.max-frame-size` defaults to `4294967295` — four gigabytes in one
//! frame — and is not known until the partner's `open` has been read. Until
//! then the ceiling is [`MIN_MAX_FRAME_SIZE`] = 512, the value Part 2 §2.4.1
//! fixes for the pre-negotiation period and the floor both peers must accept
//! forever after. So [`decode_header`] takes the ceiling as an **argument**:
//! a peer that pipelines a four-gigabyte frame ahead of its `open` is refused
//! on eight octets, and a caller cannot forget to pass the limit because
//! there is no parameterless form.
//!
//! # Empty frames
//!
//! A frame with a body of zero octets — `SIZE = 8`, `DOFF = 2` — is the idle
//! timeout's keep-alive. It "has no meaning beyond liveness", SHOULD be
//! channel 0 and MUST be channel 0 before `open` has been received
//! (Part 2 §2.4.5). [`empty`] writes one; [`Frame::is_empty`] recognizes one.

use crate::error::{DecodeError, EncodeError};

/// The smallest frame the grammar allows: the fixed header and nothing else.
pub const MIN_FRAME_SIZE: u32 = 8;

/// `MIN-MAX-FRAME-SIZE`: the largest frame either peer may send before
/// `open` has been read, and the smallest `max-frame-size` either peer may
/// advertise afterwards (Part 2 §2.4.1, §2.7.1).
///
/// Both halves matter. A peer MUST NOT send more than 512 octets in a frame
/// until it has read the partner's `open`, and a peer MUST accept frames of
/// at least 512 octets no matter what it advertised — so 512 is both the
/// initial ceiling and the permanent floor.
pub const MIN_MAX_FRAME_SIZE: u32 = 512;

/// `open.max-frame-size`'s default: no limit at all (Part 2 §2.7.1).
///
/// Named rather than inlined because it is the number a caller has to
/// override deliberately: accepting it means agreeing to a single frame of
/// four gigabytes.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 4_294_967_295;

/// The fixed size of a SASL frame, which is not negotiable
/// (Part 5 §5.3.1).
pub const SASL_MAX_FRAME_SIZE: u32 = MIN_MAX_FRAME_SIZE;

/// What a frame carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// `0x00`: the body is one performative, optionally followed by an opaque
    /// payload (Part 2 §2.3.2).
    Amqp,
    /// `0x01`: the body is one SASL performative and nothing else
    /// (Part 5 §5.3.1).
    Sasl,
}

impl FrameKind {
    /// The `TYPE` octet.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Amqp => 0x00,
            Self::Sasl => 0x01,
        }
    }

    /// The frame type an octet names.
    pub const fn from_octet(octet: u8) -> Result<Self, DecodeError> {
        match octet {
            0x00 => Ok(Self::Amqp),
            0x01 => Ok(Self::Sasl),
            other => Err(DecodeError::UnknownFrameType(other)),
        }
    }
}

/// A decoded frame header: what the frame is, how long it is, and where its
/// body starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// `SIZE`: the whole frame, these eight octets included.
    pub size: u32,
    /// `DOFF`: the body's offset in four-octet words, at least 2.
    pub doff: u8,
    /// `TYPE`.
    pub kind: FrameKind,
    /// The two type-specific octets, read as a `ushort`.
    ///
    /// For an AMQP frame this is the channel. For a SASL frame the
    /// specification says the octets are ignored; this decoder reports what
    /// arrived rather than zeroing it, because a peer that sets them is worth
    /// being able to see.
    pub channel: u16,
}

impl FrameHeader {
    /// Where the body starts, in octets from the front of the frame.
    #[must_use]
    pub const fn body_offset(&self) -> usize {
        self.doff as usize * 4
    }

    /// How many octets of body the frame carries.
    #[must_use]
    pub const fn body_len(&self) -> usize {
        self.size as usize - self.body_offset()
    }

    /// Whether this is an empty frame: a header, no extended header, no body.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.size == MIN_FRAME_SIZE && self.doff == 2
    }
}

/// A decoded frame: its header and its body, borrowed.
///
/// The extended header is skipped rather than exposed. Part 2 §2.3.2 says an
/// AMQP frame's extended header "is ignored", and a SASL frame's likewise, so
/// there is nothing for a caller to do with it — but the octets are counted,
/// which is what [`Frame::used`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    /// The fixed header.
    pub header: FrameHeader,
    /// The body: one performative and, for an AMQP frame, an optional
    /// payload after it.
    pub body: &'a [u8],
}

impl Frame<'_> {
    /// How many octets of the input the whole frame occupied, which is
    /// exactly `header.size`.
    #[must_use]
    pub const fn used(&self) -> usize {
        self.header.size as usize
    }

    /// Whether this is an empty frame, the idle timeout's keep-alive.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.header.is_empty()
    }
}

/// Decodes a frame header from the front of `input`.
///
/// `max_frame_size` is the ceiling in force: [`MIN_MAX_FRAME_SIZE`] until the
/// partner's `open` has been read, and whatever that `open` advertised
/// afterwards. The effective ceiling is the larger of `max_frame_size` and
/// [`MIN_MAX_FRAME_SIZE`], because a peer MUST accept a 512-octet frame
/// whatever it advertised, so a caller cannot make the check stricter than
/// the specification allows by advertising less.
///
/// ```
/// use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
///
/// // An empty frame: eight octets, body at word 2, AMQP, channel 0.
/// let header = frame::decode_header(&[0, 0, 0, 8, 2, 0, 0, 0], MIN_MAX_FRAME_SIZE)
///     .expect("a header");
/// assert_eq!(header.kind, FrameKind::Amqp);
/// assert!(header.is_empty());
///
/// // A frame claiming four gigabytes before `open` has been read is refused
/// // on these eight octets.
/// assert!(frame::decode_header(&[0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0], MIN_MAX_FRAME_SIZE).is_err());
/// ```
pub fn decode_header(input: &[u8], max_frame_size: u32) -> Result<FrameHeader, DecodeError> {
    if input.len() < MIN_FRAME_SIZE as usize {
        return Err(DecodeError::Incomplete {
            needed: MIN_FRAME_SIZE as usize - input.len(),
        });
    }
    let size = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    if size < MIN_FRAME_SIZE {
        return Err(DecodeError::FrameTooSmall { size });
    }
    let max = max_frame_size.max(MIN_MAX_FRAME_SIZE);
    if size > max {
        return Err(DecodeError::FrameTooLarge { size, max });
    }
    let doff = input[4];
    if doff < 2 {
        return Err(DecodeError::DataOffsetTooSmall { doff });
    }
    // `DOFF * 4` cannot overflow a u32 (255 * 4 = 1020) but it can exceed the
    // frame, which means the extended header has no room.
    if u32::from(doff) * 4 > size {
        return Err(DecodeError::ExtendedHeaderOverrunsFrame { doff, size });
    }
    let kind = FrameKind::from_octet(input[5])?;
    let channel = u16::from_be_bytes([input[6], input[7]]);
    Ok(FrameHeader {
        size,
        doff,
        kind,
        channel,
    })
}

/// Decodes a whole frame, borrowing its body and skipping its extended
/// header.
///
/// Returns [`DecodeError::Incomplete`] with the exact shortfall until all
/// `SIZE` octets are present, so a caller reading from a stream can size its
/// next read from the answer.
pub fn decode(input: &[u8], max_frame_size: u32) -> Result<Frame<'_>, DecodeError> {
    let header = decode_header(input, max_frame_size)?;
    let size = header.size as usize;
    if input.len() < size {
        return Err(DecodeError::Incomplete {
            needed: size - input.len(),
        });
    }
    Ok(Frame {
        header,
        body: &input[header.body_offset()..size],
    })
}

/// The eight octets of an empty frame on `channel`.
///
/// This is the idle-timeout keep-alive of Part 2 §2.4.5. It carries no
/// performative, which is why it is a function rather than a variant of
/// anything: there is nothing in it but the header.
#[must_use]
pub const fn empty(channel: u16) -> [u8; 8] {
    let [hi, lo] = channel.to_be_bytes();
    [0, 0, 0, 8, 2, 0x00, hi, lo]
}

/// Writes a frame whose body is produced by `body`, backfilling `SIZE` once
/// the body's length is known.
///
/// No scratch buffer: the header goes down as eight zero octets, `body`
/// appends straight into `out`, and the first four octets are filled in
/// afterwards. `max_frame_size` is the partner's advertised ceiling, and a
/// frame above it is [`EncodeError::FrameTooLarge`] — refused before the
/// octets leave, because the partner's answer would be
/// `amqp:connection:framing-error` (Part 2 §2.7.1).
///
/// The frame is removed from `out` again when `body` fails or the result is
/// too large, so a failed write leaves nothing half-written on a connection.
pub fn write<F>(
    out: &mut Vec<u8>,
    kind: FrameKind,
    channel: u16,
    max_frame_size: u32,
    body: F,
) -> Result<(), EncodeError>
where
    F: FnOnce(&mut Vec<u8>) -> Result<(), EncodeError>,
{
    let at = out.len();
    let [hi, lo] = channel.to_be_bytes();
    out.extend_from_slice(&[0, 0, 0, 0, 2, kind.octet(), hi, lo]);
    if let Err(error) = body(out) {
        out.truncate(at);
        return Err(error);
    }
    let size = out.len() - at;
    let max = max_frame_size.max(MIN_MAX_FRAME_SIZE) as usize;
    if size > max {
        out.truncate(at);
        return Err(EncodeError::FrameTooLarge {
            size,
            max: max_frame_size,
        });
    }
    let declared = u32::try_from(size).map_err(|_| EncodeError::FrameTooLarge {
        size,
        max: max_frame_size,
    })?;
    out[at..at + 4].copy_from_slice(&declared.to_be_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_frame_is_eight_octets_at_word_two() {
        let bytes = empty(0);
        let frame = decode(&bytes, MIN_MAX_FRAME_SIZE).expect("a frame");
        assert!(frame.is_empty());
        assert_eq!(frame.body, b"");
        assert_eq!(frame.used(), 8);
        assert_eq!(frame.header.kind, FrameKind::Amqp);
        assert_eq!(frame.header.channel, 0);
    }

    #[test]
    fn size_below_eight_is_malformed() {
        for size in 0u32..8 {
            let mut bytes = size.to_be_bytes().to_vec();
            bytes.extend_from_slice(&[2, 0, 0, 0]);
            assert_eq!(
                decode_header(&bytes, MIN_MAX_FRAME_SIZE),
                Err(DecodeError::FrameTooSmall { size }),
                "SIZE = {size}"
            );
        }
    }

    #[test]
    fn a_data_offset_below_two_is_malformed() {
        for doff in 0u8..2 {
            let bytes = [0, 0, 0, 8, doff, 0, 0, 0];
            assert_eq!(
                decode_header(&bytes, MIN_MAX_FRAME_SIZE),
                Err(DecodeError::DataOffsetTooSmall { doff }),
                "DOFF = {doff}"
            );
        }
        // Two is the smallest legal value and means "no extended header".
        assert!(decode_header(&[0, 0, 0, 8, 2, 0, 0, 0], MIN_MAX_FRAME_SIZE).is_ok());
    }

    #[test]
    fn the_extended_header_is_skipped_and_the_body_starts_after_it() {
        // DOFF = 4: eight octets of extended header between the fixed header
        // and a two-octet body.
        let mut bytes = vec![0, 0, 0, 18, 4, 0x00, 0x00, 0x07];
        bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe]);
        bytes.extend_from_slice(&[0x40, 0x41]);
        let frame = decode(&bytes, MIN_MAX_FRAME_SIZE).expect("a frame");
        assert_eq!(frame.header.doff, 4);
        assert_eq!(frame.header.channel, 7);
        assert_eq!(frame.header.body_offset(), 16);
        assert_eq!(frame.body, &[0x40, 0x41]);
        assert_eq!(frame.used(), 18);
        assert!(
            !frame.is_empty(),
            "an extended header is not an empty frame"
        );
    }

    #[test]
    fn an_extended_header_that_does_not_fit_is_malformed() {
        // SIZE 8 but DOFF 3: the body would begin at octet 12, past the end.
        assert_eq!(
            decode_header(&[0, 0, 0, 8, 3, 0, 0, 0], MIN_MAX_FRAME_SIZE),
            Err(DecodeError::ExtendedHeaderOverrunsFrame { doff: 3, size: 8 })
        );
    }

    #[test]
    fn the_two_frame_types_are_distinguished_and_the_rest_refused() {
        let mut bytes = [0, 0, 0, 8, 2, 0x00, 0, 0];
        assert_eq!(
            decode_header(&bytes, MIN_MAX_FRAME_SIZE).unwrap().kind,
            FrameKind::Amqp
        );
        bytes[5] = 0x01;
        assert_eq!(
            decode_header(&bytes, MIN_MAX_FRAME_SIZE).unwrap().kind,
            FrameKind::Sasl
        );
        for other in [0x02u8, 0x40, 0xff] {
            bytes[5] = other;
            assert_eq!(
                decode_header(&bytes, MIN_MAX_FRAME_SIZE),
                Err(DecodeError::UnknownFrameType(other))
            );
        }
    }

    #[test]
    fn the_pre_negotiation_ceiling_is_five_hundred_and_twelve() {
        // 513 octets is one too many before `open` has been read...
        let mut bytes = 513u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[2, 0, 0, 0]);
        assert_eq!(
            decode_header(&bytes, MIN_MAX_FRAME_SIZE),
            Err(DecodeError::FrameTooLarge {
                size: 513,
                max: MIN_MAX_FRAME_SIZE
            })
        );
        // ...and fine once the partner has advertised room for it. The
        // refusal is from the header alone: no body is present at all.
        assert_eq!(bytes.len(), 8);
        assert!(decode_header(&bytes, 4096).is_ok());
    }

    #[test]
    fn a_ceiling_below_the_floor_is_raised_to_it() {
        // A peer that advertised 0 - or one that has not advertised anything
        // - still MUST accept 512 octets, so the argument cannot make the
        // check stricter than the specification.
        let mut bytes = 512u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[2, 0, 0, 0]);
        assert!(decode_header(&bytes, 0).is_ok());
        assert!(decode_header(&bytes, 64).is_ok());
    }

    #[test]
    fn an_incomplete_frame_says_how_much_is_missing() {
        assert_eq!(
            decode_header(&[0, 0, 0], MIN_MAX_FRAME_SIZE),
            Err(DecodeError::Incomplete { needed: 5 })
        );
        // The header is complete; the body is not.
        let mut bytes = vec![0, 0, 0, 12, 2, 0, 0, 0];
        bytes.push(0x40);
        assert_eq!(
            decode(&bytes, MIN_MAX_FRAME_SIZE),
            Err(DecodeError::Incomplete { needed: 3 })
        );
    }

    #[test]
    fn write_backfills_the_size_and_refuses_an_oversized_frame() {
        let mut out = Vec::new();
        write(&mut out, FrameKind::Amqp, 3, MIN_MAX_FRAME_SIZE, |body| {
            body.extend_from_slice(&[0xde, 0xad]);
            Ok(())
        })
        .expect("writes");
        assert_eq!(out, [0, 0, 0, 10, 2, 0x00, 0x00, 0x03, 0xde, 0xad]);
        let frame = decode(&out, MIN_MAX_FRAME_SIZE).expect("a frame");
        assert_eq!(frame.body, &[0xde, 0xad]);
        assert_eq!(frame.header.channel, 3);

        // Over the ceiling: nothing is left behind in `out`.
        let mut out = Vec::new();
        let error = write(&mut out, FrameKind::Sasl, 0, MIN_MAX_FRAME_SIZE, |body| {
            body.extend_from_slice(&[0u8; 600]);
            Ok(())
        })
        .expect_err("refused");
        assert_eq!(
            error,
            EncodeError::FrameTooLarge {
                size: 608,
                max: MIN_MAX_FRAME_SIZE
            }
        );
        assert!(out.is_empty(), "a refused frame writes nothing");
    }

    #[test]
    fn a_failing_body_leaves_nothing_half_written() {
        let mut out = vec![0xaa];
        let error = write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            body.push(0x40);
            Err(EncodeError::NonAsciiSymbol)
        })
        .expect_err("refused");
        assert_eq!(error, EncodeError::NonAsciiSymbol);
        assert_eq!(out, [0xaa], "the caller's earlier bytes survive");
    }
}
