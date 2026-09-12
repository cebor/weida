//! The eight octets each peer sends before any frame.
//!
//! ```text
//! +--------+--------+--------+--------+-------------+-------+-------+----------+
//! |  'A'   |  'M'   |  'Q'   |  'P'   | protocol-id | major | minor | revision |
//! +--------+--------+--------+--------+-------------+-------+-------+----------+
//! ```
//!
//! The version is `1.0.0` and there is no other: Part 2 §2.2 assigns
//! `major=1, minor=0, revision=0` and no 1.1 exists. The protocol-id selects
//! the *layer*, not the version — `%d0` bare AMQP, `%d2` TLS, `%d3` SASL
//! (Part 5 §5.1) — and the distinction is load-bearing, because "highest
//! supported version" does not apply to it: a server requiring SASL answers a
//! `%d0` request with `%d3` and closes, which is how a client learns that a
//! security layer is mandatory rather than optional (Part 2 §2.2).
//!
//! That is why [`decode`] reports the version it read instead of refusing a
//! mismatch. A peer's answering header is *data* — the whole negotiation is
//! reading what came back and deciding — so only two things are errors here:
//! octets that do not begin with `AMQP`, and a protocol-id naming a layer
//! this codec does not implement.

use crate::error::DecodeError;

/// The length of a protocol header, in octets.
pub const LEN: usize = 8;

/// The four octets every protocol header begins with.
pub const PREFIX: [u8; 4] = *b"AMQP";

/// The only version AMQP 1.0 assigns (Part 2 §2.2).
pub const VERSION: Version = Version {
    major: 1,
    minor: 0,
    revision: 0,
};

/// Which layer the header opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolId {
    /// `%d0`: AMQP frames follow directly.
    Amqp,
    /// `%d2`: a TLS handshake follows, and another protocol header inside it
    /// (Part 5 §5.2).
    Tls,
    /// `%d3`: a SASL dialog in type-`0x01` frames follows, and another
    /// protocol header after the outcome (Part 5 §5.3).
    Sasl,
}

impl ProtocolId {
    /// The protocol-id octet.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Amqp => 0,
            Self::Tls => 2,
            Self::Sasl => 3,
        }
    }

    /// The layer an octet names.
    pub const fn from_octet(octet: u8) -> Result<Self, DecodeError> {
        match octet {
            0 => Ok(Self::Amqp),
            2 => Ok(Self::Tls),
            3 => Ok(Self::Sasl),
            other => Err(DecodeError::UnknownProtocolId(other)),
        }
    }
}

/// The three version octets, reported rather than judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    /// Major.
    pub major: u8,
    /// Minor.
    pub minor: u8,
    /// Revision.
    pub revision: u8,
}

impl Version {
    /// Whether this is the one version AMQP 1.0 assigns.
    #[must_use]
    pub const fn is_1_0_0(self) -> bool {
        self.major == 1 && self.minor == 0 && self.revision == 0
    }
}

impl core::fmt::Display for Version {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.revision)
    }
}

/// A protocol header: the layer and the version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProtocolHeader {
    /// Which layer follows.
    pub id: ProtocolId,
    /// Which version the sender claims.
    pub version: Version,
}

impl ProtocolHeader {
    /// `AMQP %d0 1.0.0`: bare AMQP.
    pub const AMQP: Self = Self {
        id: ProtocolId::Amqp,
        version: VERSION,
    };
    /// `AMQP %d2 1.0.0`: TLS follows.
    pub const TLS: Self = Self {
        id: ProtocolId::Tls,
        version: VERSION,
    };
    /// `AMQP %d3 1.0.0`: SASL follows.
    pub const SASL: Self = Self {
        id: ProtocolId::Sasl,
        version: VERSION,
    };

    /// The eight octets.
    #[must_use]
    pub const fn encode(&self) -> [u8; LEN] {
        [
            PREFIX[0],
            PREFIX[1],
            PREFIX[2],
            PREFIX[3],
            self.id.octet(),
            self.version.major,
            self.version.minor,
            self.version.revision,
        ]
    }
}

/// Decodes a protocol header from the front of `input`.
///
/// ```
/// use weida_amqp_codec::protocol_header::{self, ProtocolHeader, ProtocolId};
///
/// // We ask for bare AMQP; a server that requires SASL answers %d3.
/// let ours = ProtocolHeader::AMQP.encode();
/// assert_eq!(ours, *b"AMQP\x00\x01\x00\x00");
///
/// let theirs = protocol_header::decode(b"AMQP\x03\x01\x00\x00").expect("a header");
/// assert_eq!(theirs.id, ProtocolId::Sasl);
/// assert!(theirs.version.is_1_0_0());
/// // The mismatch is a demand, not an error: a security layer is mandatory.
/// assert_ne!(theirs.id, ProtocolId::Amqp);
/// ```
pub fn decode(input: &[u8]) -> Result<ProtocolHeader, DecodeError> {
    if input.len() < LEN {
        return Err(DecodeError::Incomplete {
            needed: LEN - input.len(),
        });
    }
    if input[..4] != PREFIX {
        return Err(DecodeError::NotAProtocolHeader);
    }
    Ok(ProtocolHeader {
        id: ProtocolId::from_octet(input[4])?,
        version: Version {
            major: input[5],
            minor: input[6],
            revision: input[7],
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_layers_round_trip() {
        for (header, octets) in [
            (ProtocolHeader::AMQP, *b"AMQP\x00\x01\x00\x00"),
            (ProtocolHeader::TLS, *b"AMQP\x02\x01\x00\x00"),
            (ProtocolHeader::SASL, *b"AMQP\x03\x01\x00\x00"),
        ] {
            assert_eq!(header.encode(), octets);
            assert_eq!(decode(&octets).expect("a header"), header);
        }
    }

    #[test]
    fn protocol_id_one_is_not_assigned() {
        // %d1 sits between TLS and SASL and names nothing. A decoder that
        // guessed would be guessing about which layer follows.
        assert_eq!(
            decode(b"AMQP\x01\x01\x00\x00"),
            Err(DecodeError::UnknownProtocolId(1))
        );
    }

    #[test]
    fn a_foreign_version_is_reported_rather_than_refused() {
        // A server answering with a version it supports is how negotiation
        // works, so the version is data. 0-9-1 shares this port and this
        // prefix, and its header is `AMQP` + `0 0 9 1`.
        let header = decode(b"AMQP\x00\x00\x09\x01").expect("a header");
        assert_eq!(
            header.version,
            Version {
                major: 0,
                minor: 9,
                revision: 1
            }
        );
        assert!(!header.version.is_1_0_0());
        assert_eq!(header.version.to_string(), "0.9.1");
    }

    #[test]
    fn octets_that_are_not_amqp_are_not_a_header() {
        assert_eq!(decode(b"HTTP/1.1"), Err(DecodeError::NotAProtocolHeader));
        assert_eq!(decode(b"AMQ"), Err(DecodeError::Incomplete { needed: 5 }));
    }
}
