//! weida wire protocol v0: framing, CBOR headers, negotiation and QUIC
//! application error codes.
//!
//! The crate performs no I/O. It parses and encodes byte slices, which makes it
//! directly fuzzable and keeps hostile-input handling in one place. Every
//! decoder treats its input as hostile: lengths are checked before allocation,
//! duplicate map keys are rejected and nesting depth is bounded.
//!
//! `docs/PROTOCOL.md` is the normative specification; this crate is its
//! implementation, and the golden vectors in §8 are asserted byte-for-byte by
//! the unit tests.

pub mod codes;
pub mod frame;
pub mod header;
pub mod negotiate;
pub mod varint;

pub use frame::{
    FrameKind, MAGIC, MAX_PREAMBLE_LEN, Preamble, PreambleError, encode_frame, encode_preamble,
    parse_preamble,
};
pub use header::{
    AckHeader, CancelHeader, DataHeader, ErrorHeader, HeaderError, Hello, limits as header_limits,
};
pub use negotiate::{Agreed, NegotiateError, negotiate};
pub use varint::{VarintError, decode_varint, encode_varint, varint_len};

/// Wire protocol version implemented by this crate (experimental).
pub const VERSION: u64 = 0;

/// TLS ALPN token identifying the weida native QUIC protocol.
pub const ALPN: &[u8] = b"weida/0";

#[cfg(test)]
mod tests {
    #[test]
    fn alpn_matches_the_wire_version() {
        assert_eq!(super::ALPN, b"weida/0");
        assert_eq!(super::VERSION, 0);
    }
}
