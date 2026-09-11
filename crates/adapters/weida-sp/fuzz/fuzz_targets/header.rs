//! The SP protocol header against arbitrary bytes.
//!
//! Property: decoding never panics, an accepted header re-encodes to the same
//! eight octets, and the pairing rule is symmetric — if a peer of type `a` is
//! accepted by a local `b`, then a peer of type `b` is accepted by a local
//! `a`. The header is the one place where every rule is fatal [rfc-tcp §2],
//! so a decoder that accepted something it should not would let a connection
//! proceed that a real NNG peer has already dropped.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_sp::{ProtocolHeader, header::HEADER_LEN};

fuzz_target!(|data: &[u8]| {
    if let Ok(header) = ProtocolHeader::decode(data) {
        let re = header.encode();
        assert_eq!(&re[..], &data[..HEADER_LEN]);
        assert_eq!(ProtocolHeader::decode(&re), Ok(header));

        let peer = header.endpoint.peer();
        assert!(header.accepts(peer));
        assert!(ProtocolHeader::new(peer).accepts(header.endpoint));
    }
});
