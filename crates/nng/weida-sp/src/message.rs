//! Message delimitation: a 64-bit size, then exactly that many octets.
//!
//! ```text
//! +------------+-----------------+
//! | size (64b) |     payload     |
//! +------------+-----------------+
//! ```
//!
//! "Every message starts with 64-bit unsigned integer in network byte order
//! representing the size, in bytes, of the remaining part of the message.
//! Thus, the message payload can be from 0 to 2^64-1 bytes long"
//! [rfc-tcp §3].
//!
//! This is the module where hostile bytes are first believed, so it is where
//! the cap lives. SP grants no credit on the wire [nanomsg-nng §12/P12] and
//! the only inbound defence is `NNG_OPT_RECVMAXSZ`, which is *unlimited by
//! default* [nanomsg-nng §5]. Every decode therefore takes the cap as an
//! argument and rejects an over-large declaration from the size field alone,
//! before the body is looked at, let alone reserved - the same rule weida's
//! own `max_header_bytes` obeys (`docs/PROTOCOL.md` §3.1).

use crate::error::MessageError;

/// Octets in the size field [rfc-tcp §3].
pub const SIZE_LEN: usize = 8;

/// Largest body the grammar allows [rfc-tcp §3]. Unlike ZMTP, which stops at
/// 2^63-1, SP spends the whole 64 bits.
pub const MAX_BODY: u64 = u64::MAX;

/// Reads the size field from the front of `input`.
///
/// Returns the declared body length and how many octets the field occupied.
/// `max_body_bytes` is the local cap and a larger declaration is rejected
/// here - the only place the check can happen before memory is committed.
pub fn decode_size(input: &[u8], max_body_bytes: u64) -> Result<(u64, usize), MessageError> {
    let Some(field) = input.get(..SIZE_LEN) else {
        return Err(MessageError::Incomplete);
    };
    let mut octets = [0u8; SIZE_LEN];
    octets.copy_from_slice(field);
    let len = u64::from_be_bytes(octets);
    if len > max_body_bytes {
        return Err(MessageError::BodyTooLarge {
            len,
            max: max_body_bytes,
        });
    }
    Ok((len, SIZE_LEN))
}

/// Decodes a whole message, borrowing its body.
///
/// Returns the body and the total octets consumed. Nothing is copied and
/// nothing is allocated: the body is a slice of the caller's buffer, which is
/// what lets a bridge hand a payload straight to a weida transfer without a
/// second copy.
pub fn decode(input: &[u8], max_body_bytes: u64) -> Result<(&[u8], usize), MessageError> {
    let (len, used) = decode_size(input, max_body_bytes)?;
    // The cap has already bounded `len`; this narrowing can only fail on a
    // 32-bit target, where a body that large could never be held anyway.
    let len = usize::try_from(len).map_err(|_| MessageError::BodyTooLarge {
        len,
        max: usize::MAX as u64,
    })?;
    let end = used.checked_add(len).ok_or(MessageError::Incomplete)?;
    let body = input.get(used..end).ok_or(MessageError::Incomplete)?;
    Ok((body, end))
}

/// Appends a size field for a body of `len` octets.
pub fn encode_size(len: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&len.to_be_bytes());
}

/// Builds a complete message: size field followed by `body`.
pub fn encode(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(SIZE_LEN + body.len());
    encode_size(body.len() as u64, &mut out);
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: u64 = 64 * 1024;

    #[test]
    fn a_message_round_trips_with_its_length() {
        for body in [&b""[..], &b"hi"[..], &vec![0xAB; 1000][..]] {
            let wire = encode(body);
            assert_eq!(wire.len(), SIZE_LEN + body.len());
            let (decoded, used) = decode(&wire, CAP).expect("decode");
            assert_eq!(decoded, body);
            assert_eq!(used, wire.len());
        }
    }

    #[test]
    fn an_oversized_declaration_is_refused_from_the_size_field_alone() {
        // Eight octets of 0xFF declare 2^64-1 and nothing follows. A decoder
        // that reserved before checking would die here rather than answer.
        let hostile = [0xFFu8; SIZE_LEN];
        assert_eq!(
            decode(&hostile, CAP),
            Err(MessageError::BodyTooLarge {
                len: u64::MAX,
                max: CAP
            })
        );
        assert_eq!(
            decode_size(&hostile, CAP),
            Err(MessageError::BodyTooLarge {
                len: u64::MAX,
                max: CAP
            })
        );
        assert!(decode(&hostile, CAP).unwrap_err().is_violation());
    }

    #[test]
    fn a_declaration_exactly_at_the_cap_is_accepted() {
        let body = vec![7u8; 64];
        let wire = encode(&body);
        assert!(decode(&wire, 64).is_ok());
        assert_eq!(
            decode(&wire, 63),
            Err(MessageError::BodyTooLarge { len: 64, max: 63 })
        );
    }

    #[test]
    fn a_short_read_is_not_a_violation() {
        let wire = encode(b"hello");
        for cut in 0..wire.len() {
            let err = decode(&wire[..cut], CAP).expect_err("incomplete");
            assert_eq!(err, MessageError::Incomplete);
            assert!(!err.is_violation(), "a short read is retryable");
        }
        assert!(decode(&wire, CAP).is_ok());
    }

    #[test]
    fn several_messages_decode_from_one_buffer_in_order() {
        let mut stream = encode(b"one");
        stream.extend_from_slice(&encode(b""));
        stream.extend_from_slice(&encode(b"three"));

        let mut rest = &stream[..];
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while !rest.is_empty() {
            let (body, used) = decode(rest, CAP).expect("decode");
            seen.push(body.to_vec());
            rest = &rest[used..];
        }
        assert_eq!(seen, vec![b"one".to_vec(), Vec::new(), b"three".to_vec()]);
    }
}
