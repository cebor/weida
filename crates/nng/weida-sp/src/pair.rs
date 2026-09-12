//! The PAIR v1 hop count.
//!
//! PAIR v1 prefixes a body with "one 32-bit header whose low-order byte is a
//! hop count", bounded by the local `MAXTTL` of 1-255
//! [nanomsg-nng §3, §4, §11]. It is the protocol's whole loop protection:
//! a forwarder increments and a node past its own limit drops.
//!
//! **Where the sheet, the source and the running implementation
//! disagreed.** The sheet says the counter is "initialized to one and
//! incremented at each node" [nanomsg-nng §4], and a reading of NNG's
//! source suggested it appends `0` on a cooked send
//! [nng-src `pair1/pair.c`]. **Measured, the implementation sends one**:
//! a cooked `nng_pair1` socket of NNG 1.4.0-rc.0 puts `00 00 00 01` in
//! front of its body [nanomsg-nng §31]. The source reading was wrong — or
//! true of some other version — and the RFC was right. This codec:
//!
//! * **encodes** one, which is what the RFC says and what a real peer was
//!   observed to send ([`INITIAL_HOPS`] is `1`);
//! * **decodes** both readings without complaint, because the difference
//!   is a count and not a format, and a peer built from the other reading
//!   still interoperates: a receiver only compares the count to its own
//!   `MAXTTL`;
//! * records where the number came from rather than resolving it
//!   silently.
//!
//! PAIR v0 has no header at all and is therefore not this module's business
//! [nanomsg-nng §4].

use crate::error::TagError;

/// Octets in the PAIR v1 header [nanomsg-nng §3].
pub const HEADER_LEN: usize = 4;

/// What a cooked PAIR v1 socket puts in a message it originates
/// [rfc-pair §3], and what NNG 1.4.0-rc.0 was measured to send
/// [nanomsg-nng §31]. See the module note for the source reading that
/// suggested zero.
pub const INITIAL_HOPS: u32 = 1;

/// The default `MAXTTL` [nanomsg-nng §4, §11].
pub const DEFAULT_MAX_HOPS: u32 = 8;

/// The largest count NNG will *send*: a raw PAIR v1 send whose header is
/// already `0xff` is rejected as malformed [nng-src `pair1/pair.c`].
pub const MAX_HOPS: u32 = 0xff;

/// Splits a PAIR v1 body into its hop count and its payload.
///
/// `max_hops` is the local `MAXTTL`. A count above it is
/// [`TagError::TooManyHops`], which is **not** a connection violation: NNG
/// drops the message and keeps the pipe, "because we can legitimately receive
/// messages with too many hops from devices" [nng-src `rep.c`,
/// `pair1/pair.c`]. A body shorter than the header is
/// [`TagError::Truncated`].
pub fn decode(body: &[u8], max_hops: u32) -> Result<(u32, &[u8]), TagError> {
    let Some(head) = body.get(..HEADER_LEN) else {
        return Err(TagError::Truncated);
    };
    let hops = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
    if hops > max_hops {
        return Err(TagError::TooManyHops { hops, max_hops });
    }
    Ok((hops, &body[HEADER_LEN..]))
}

/// Builds a PAIR v1 body: the hop count followed by `payload`.
pub fn encode(hops: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&hops.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// The body a cooked PAIR v1 socket originates.
pub fn encode_initial(payload: &[u8]) -> Vec<u8> {
    encode(INITIAL_HOPS, payload)
}

/// The count a forwarder writes after receiving `hops`.
pub const fn next_hop(hops: u32) -> u32 {
    hops.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cooked_body_round_trips_with_the_initial_count() {
        let body = encode_initial(b"hi");
        assert_eq!(body, [0, 0, 0, 1, b'h', b'i']);
        let (hops, payload) = decode(&body, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(hops, INITIAL_HOPS);
        assert_eq!(payload, b"hi");
    }

    /// Claim: the other reading of the initial count decodes too. A peer
    /// built from NNG's source rather than from the RFC sends zero, and
    /// nothing here refuses it: the count is compared to `MAXTTL` and
    /// nothing else.
    #[test]
    fn the_other_reading_of_the_initial_count_decodes() {
        let from_the_source_reading = encode(0, b"hi");
        let (hops, payload) = decode(&from_the_source_reading, DEFAULT_MAX_HOPS).expect("decode");
        assert_eq!(hops, 0);
        assert_eq!(payload, b"hi");
    }

    #[test]
    fn a_forwarded_body_carries_one_more_hop() {
        let incoming = encode_initial(b"hi");
        let (hops, payload) = decode(&incoming, DEFAULT_MAX_HOPS).expect("decode");
        let forwarded = encode(next_hop(hops), payload);
        assert_eq!(forwarded, [0, 0, 0, 2, b'h', b'i']);
        assert_eq!(decode(&forwarded, DEFAULT_MAX_HOPS).expect("decode").0, 2);
    }

    #[test]
    fn a_count_past_the_limit_drops_the_message_and_keeps_the_connection() {
        let body = encode(DEFAULT_MAX_HOPS + 1, b"x");
        let err = decode(&body, DEFAULT_MAX_HOPS).expect_err("too many hops");
        assert_eq!(
            err,
            TagError::TooManyHops {
                hops: DEFAULT_MAX_HOPS + 1,
                max_hops: DEFAULT_MAX_HOPS
            }
        );
        assert!(
            !err.is_violation(),
            "NNG drops the message and keeps the pipe [nng-src rep.c]"
        );
    }

    #[test]
    fn a_body_shorter_than_the_header_is_malformed() {
        for short in [&b""[..], &b"\x00"[..], &b"\x00\x00\x00"[..]] {
            let err = decode(short, DEFAULT_MAX_HOPS).expect_err("truncated");
            assert_eq!(err, TagError::Truncated);
            assert!(err.is_violation());
        }
    }

    #[test]
    fn a_count_at_the_limit_is_accepted_and_the_payload_may_be_empty() {
        let body = encode(MAX_HOPS, b"");
        let (hops, payload) = decode(&body, MAX_HOPS).expect("decode");
        assert_eq!(hops, MAX_HOPS);
        assert!(payload.is_empty());
    }
}
