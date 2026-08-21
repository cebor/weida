//! weida wire protocol v0: framing, CBOR headers, negotiation and QUIC
//! application error codes.

/// Wire protocol version implemented by this crate (experimental).
pub const VERSION: u64 = 0;

/// TLS ALPN token identifying the weida native QUIC protocol.
pub const ALPN: &[u8] = b"weida/0";
