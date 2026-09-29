//! The DATAGRAM payload of a datagram flow: `varint flow || opaque bytes`
//! (`docs/PROTOCOL.md` §6.9).
//!
//! The prefix is a QUIC varint rather than CBOR for the same reason a cursor
//! record is: it is on the hot path, self-delimiting, and a map header per
//! datagram would buy nothing the FLOW header does not already say.

use crate::varint::{self, decode_varint};

/// Largest datagram a local transport carries in one FLOW record
/// (`docs/PROTOCOL.md` §2.1). A longer record is a protocol violation.
pub const LOCAL_MAX_DATAGRAM: usize = 1200;

/// The varint prefix for flow `flow`, in a fixed buffer, and its length.
///
/// # Panics
///
/// Above 2^62 − 1, which no flow id reaches: ids are allocated by a counter
/// per connection and direction.
pub fn flow_prefix(flow: u64) -> ([u8; varint::MAX_ENCODED_LEN], usize) {
    let mut out = [0u8; varint::MAX_ENCODED_LEN];
    let len = varint::write_varint(flow, &mut out).expect("flow ids stay below 2^62");
    (out, len)
}

/// Splits a received datagram into its flow id and the offset of its payload.
///
/// `None` when the input ends inside the varint. That is not an error: a
/// truncated datagram is dropped and counted like one for an unknown flow
/// (`docs/PROTOCOL.md` §6.9).
pub fn split_flow_datagram(input: &[u8]) -> Option<(u64, usize)> {
    decode_varint(input).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_splits_back_to_its_flow_and_offset() {
        for flow in [0u64, 7, 63, 64, 16_383, 16_384, 1 << 40] {
            let (prefix, len) = flow_prefix(flow);
            let mut datagram = prefix[..len].to_vec();
            datagram.extend_from_slice(b"payload");
            assert_eq!(split_flow_datagram(&datagram), Some((flow, len)));
            assert_eq!(&datagram[len..], b"payload");
        }
    }

    #[test]
    fn a_truncated_prefix_is_none() {
        assert_eq!(split_flow_datagram(&[]), None);
        assert_eq!(split_flow_datagram(&[0x40]), None);
        assert_eq!(split_flow_datagram(&[0x80, 0, 0]), None);
    }
}
