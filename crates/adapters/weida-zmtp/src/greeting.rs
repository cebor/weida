//! The 64-octet greeting and version negotiation.
//!
//! ```text
//! greeting = signature version mechanism as-server filler
//! signature = %xFF padding %x7F        ; padding = 8 octets, not significant
//! version = %x03 %x01                  ; this codec
//! mechanism = 20 null-padded octets
//! as-server = %x00 | %x01
//! filler = 31 %x00
//! ```
//!
//! Two rules from the specification shape this module more than the layout
//! does. First, "a peer SHALL NOT assign any significance to the padding field
//! and MUST NOT validate this nor interpret it in any way whatsoever" - so the
//! eight octets between `0xFF` and `0x7F` are skipped, and the documented
//! ZMTP 1.0 detection trick that abuses them as a frame length is a thing this
//! codec deliberately cannot see. Second, negotiation is asymmetric: a peer may
//! send only the first 11 octets to sniff a version before committing to a
//! mechanism, and "a peer that reads a full greeting, including mechanism, MUST
//! also send a full greeting including mechanism", which is a deadlock rule
//! rather than a parsing one and therefore belongs to whoever drives the I/O.

use std::fmt;

use crate::error::GreetingError;

/// Length of a full greeting.
pub const GREETING_LEN: usize = 64;

/// Length of the partial greeting used to sniff a peer's major version:
/// signature and major version number.
pub const PARTIAL_LEN: usize = 11;

/// The version this codec speaks.
pub const VERSION: Version = Version { major: 3, minor: 1 };

/// Length of the mechanism field, null-padded.
const MECHANISM_LEN: usize = 20;

/// A ZMTP protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    /// Major version, `3` for every version this codec can talk to.
    pub major: u8,
    /// Minor version.
    pub minor: u8,
}

impl Version {
    /// True if this codec can talk to a peer announcing this version.
    ///
    /// "A peer MUST accept protocol versions greater or equal to 3.1", and a
    /// peer that cannot downgrade to a lower one "MUST close the connection".
    /// This codec implements no downgrade, so 3.1 is the floor.
    pub const fn is_supported(self) -> bool {
        self.major > 3 || (self.major == 3 && self.minor >= 1)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// A security mechanism name: 20 octets, null-padded.
///
/// Held as the raw field rather than an enum of the three public mechanisms,
/// because the only operation the specification defines on it is equality -
/// "if the mechanism that the peer received does not exactly match the
/// mechanism it sent, it MUST close the connection" - and because private
/// mechanisms are explicitly allowed.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mechanism([u8; MECHANISM_LEN]);

impl Mechanism {
    /// No authentication and no confidentiality. The only mechanism this codec
    /// implements a handshake for.
    pub const NULL: Mechanism = Mechanism(*b"NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
    /// Username and password in clear text ([24/ZMTP-PLAIN]).
    ///
    /// [24/ZMTP-PLAIN]: https://rfc.zeromq.org/spec/24/
    pub const PLAIN: Mechanism = Mechanism(*b"PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
    /// CurveZMQ authentication and encryption ([25/ZMTP-CURVE]).
    ///
    /// [25/ZMTP-CURVE]: https://rfc.zeromq.org/spec/25/
    pub const CURVE: Mechanism = Mechanism(*b"CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");

    /// Builds a mechanism from a name.
    ///
    /// Rejects anything outside the grammar's `mechanism-char` set - uppercase
    /// letters, digits, `-`, `_`, `.`, `+` - and any name too long to fit the
    /// field. An empty name is rejected too: it is not in the grammar, which
    /// requires the field to be a name followed by null padding.
    pub fn new(name: &str) -> Result<Self, GreetingError> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > MECHANISM_LEN {
            return Err(GreetingError::BadMechanismName);
        }
        if !bytes.iter().all(|b| is_mechanism_char(*b)) {
            return Err(GreetingError::BadMechanismName);
        }
        let mut field = [0u8; MECHANISM_LEN];
        field[..bytes.len()].copy_from_slice(bytes);
        Ok(Mechanism(field))
    }

    /// The name without its null padding.
    pub fn name(&self) -> &str {
        let end = self.0.iter().position(|b| *b == 0).unwrap_or(MECHANISM_LEN);
        // Every construction path validates the field against
        // `mechanism-char`, which is a subset of ASCII.
        std::str::from_utf8(&self.0[..end]).expect("a validated mechanism name is ASCII")
    }

    /// Parses the raw 20-octet field.
    fn from_field(field: &[u8]) -> Result<Self, GreetingError> {
        let mut seen_padding = false;
        for b in field {
            match (*b, seen_padding) {
                (0, _) => seen_padding = true,
                // A name octet after the padding started: the field is not a
                // name followed by zeros.
                (_, true) => return Err(GreetingError::BadMechanismName),
                (b, false) if !is_mechanism_char(b) => {
                    return Err(GreetingError::BadMechanismName);
                }
                _ => {}
            }
        }
        if field[0] == 0 {
            return Err(GreetingError::BadMechanismName);
        }
        let mut owned = [0u8; MECHANISM_LEN];
        owned.copy_from_slice(field);
        Ok(Mechanism(owned))
    }
}

/// `mechanism-char = "A"-"Z" | DIGIT | "-" | "_" | "." | "+"`.
const fn is_mechanism_char(b: u8) -> bool {
    b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.' | b'+')
}

impl fmt::Display for Mechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl fmt::Debug for Mechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mechanism({})", self.name())
    }
}

/// A decoded greeting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Greeting {
    /// The version the peer announced.
    pub version: Version,
    /// The mechanism the peer announced.
    pub mechanism: Mechanism,
    /// Whether the peer acts as the server for the security handshake. Under
    /// NULL this is always false: the field "MUST be zero", and the roles come
    /// from the topology instead - "the peer that binds SHALL be the server,
    /// and connecting peer SHALL be the client".
    pub as_server: bool,
}

impl Greeting {
    /// A greeting for the NULL mechanism, which is the one this codec drives.
    ///
    /// `as_server` is not a parameter: NULL requires the octet to be zero, and
    /// a NULL peer's role is decided by which side bound the socket.
    pub const fn null() -> Self {
        Greeting {
            version: VERSION,
            mechanism: Mechanism::NULL,
            as_server: false,
        }
    }

    /// The 64 octets to send.
    pub fn encode(&self) -> [u8; GREETING_LEN] {
        let mut out = [0u8; GREETING_LEN];
        out[0] = 0xFF;
        // out[1..9] is the padding: zeros, because this codec sends its full
        // greeting at once and never plays the version-detection game.
        out[9] = 0x7F;
        out[10] = self.version.major;
        out[11] = self.version.minor;
        out[12..32].copy_from_slice(&self.mechanism.0);
        out[32] = u8::from(self.as_server);
        // out[33..64] is the filler: 31 zeros.
        out
    }

    /// Decodes 64 octets.
    ///
    /// Validates the signature, the mechanism field and `as-server`, and
    /// deliberately validates neither the padding (forbidden) nor the filler:
    /// nothing in the specification asks a reader to check the filler, and
    /// closing a connection over trailing octets that carry no meaning would
    /// buy nothing.
    pub fn decode(input: &[u8]) -> Result<Self, GreetingError> {
        if input.len() < GREETING_LEN {
            return Err(GreetingError::Incomplete);
        }
        check_signature(input)?;
        let version = Version {
            major: input[10],
            minor: input[11],
        };
        let mechanism = Mechanism::from_field(&input[12..32])?;
        let as_server = match input[32] {
            0 => false,
            1 => true,
            other => return Err(GreetingError::BadAsServer(other)),
        };
        if as_server && mechanism == Mechanism::NULL {
            return Err(GreetingError::AsServerUnderNull);
        }
        Ok(Greeting {
            version,
            mechanism,
            as_server,
        })
    }

    /// Checks a peer's greeting against our own mechanism and returns the
    /// version to speak.
    ///
    /// The version is always ours: "a peer SHALL always use its own protocol
    /// (including framing) when talking to an equal or higher protocol peer",
    /// and a lower one is refused rather than accommodated.
    pub fn accept(&self, ours: Mechanism) -> Result<Version, GreetingError> {
        if !self.version.is_supported() {
            return Err(GreetingError::UnsupportedVersion(self.version));
        }
        if self.mechanism != ours {
            return Err(GreetingError::MechanismMismatch {
                theirs: self.mechanism,
            });
        }
        Ok(VERSION)
    }
}

/// The first 11 octets: signature and major version.
pub fn encode_partial() -> [u8; PARTIAL_LEN] {
    let mut out = [0u8; PARTIAL_LEN];
    out[0] = 0xFF;
    out[9] = 0x7F;
    out[10] = VERSION.major;
    out
}

/// Reads a peer's major version from a partial greeting.
///
/// This is the sniffing half of ZMTP's asymmetric negotiation: a peer that
/// wants to detect ZMTP 1.0 and 2.0 peers sends 11 octets and reads 11 back
/// before revealing a mechanism. This codec offers the reader because the
/// decision it enables - refuse, since no downgrade exists here - is cheaper to
/// make after 11 octets than after 64.
pub fn sniff_major(input: &[u8]) -> Result<u8, GreetingError> {
    if input.len() < PARTIAL_LEN {
        return Err(GreetingError::Incomplete);
    }
    check_signature(input)?;
    Ok(input[10])
}

/// Signature check shared by the full and partial forms: `0xFF` at 0, `0x7F` at
/// 9, and never a word about the eight octets in between.
fn check_signature(input: &[u8]) -> Result<(), GreetingError> {
    if input[0] != 0xFF {
        return Err(GreetingError::BadSignature {
            at: 0,
            found: input[0],
        });
    }
    if input[9] != 0x7F {
        return Err(GreetingError::BadSignature {
            at: 9,
            found: input[9],
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_null_greeting_is_64_octets_with_a_zero_as_server() {
        let bytes = Greeting::null().encode();
        assert_eq!(bytes.len(), GREETING_LEN);
        assert_eq!(bytes[0], 0xFF);
        assert_eq!(bytes[9], 0x7F);
        assert_eq!(&bytes[10..12], &[3, 1]);
        assert_eq!(&bytes[12..16], b"NULL");
        assert!(bytes[16..32].iter().all(|b| *b == 0), "mechanism padding");
        assert_eq!(bytes[32], 0);
        assert!(bytes[33..].iter().all(|b| *b == 0), "filler");
        assert_eq!(Greeting::decode(&bytes).expect("decode"), Greeting::null());
    }

    #[test]
    fn padding_and_filler_are_never_validated() {
        // The padding rule is normative: a peer MUST NOT interpret these eight
        // octets. The documented ZMTP 1.0 detection trick puts a frame length
        // there, so a greeting with junk padding is a greeting we must accept.
        let mut bytes = Greeting::null().encode();
        bytes[1..9].copy_from_slice(&[0x01, 0x7F, 0xAA, 0xFF, 0, 0, 0, 0x55]);
        for f in &mut bytes[33..] {
            *f = 0x5A;
        }
        assert_eq!(Greeting::decode(&bytes).expect("decode"), Greeting::null());
    }

    #[test]
    fn the_signature_octets_are_validated() {
        let good = Greeting::null().encode();

        let mut bad = good;
        bad[0] = 0xFE;
        assert_eq!(
            Greeting::decode(&bad),
            Err(GreetingError::BadSignature { at: 0, found: 0xFE })
        );

        let mut bad = good;
        bad[9] = 0x00;
        assert_eq!(
            Greeting::decode(&bad),
            Err(GreetingError::BadSignature { at: 9, found: 0 })
        );
    }

    #[test]
    fn a_short_greeting_asks_for_more_rather_than_failing() {
        let bytes = Greeting::null().encode();
        for n in 0..GREETING_LEN {
            assert_eq!(
                Greeting::decode(&bytes[..n]),
                Err(GreetingError::Incomplete),
                "{n} octets"
            );
            assert!(!GreetingError::Incomplete.is_violation());
        }
    }

    #[test]
    fn versions_at_or_above_3_1_are_accepted_and_lower_ones_close() {
        for (major, minor, ok) in [
            (3, 1, true),
            (3, 2, true),
            (4, 0, true),
            (3, 0, false),
            (2, 0, false),
            (1, 0, false),
        ] {
            let v = Version { major, minor };
            assert_eq!(v.is_supported(), ok, "{v}");
        }

        let mut bytes = Greeting::null().encode();
        bytes[11] = 0; // ZMTP 3.0
        let peer = Greeting::decode(&bytes).expect("3.0 decodes; it is the accept that refuses");
        assert_eq!(
            peer.accept(Mechanism::NULL),
            Err(GreetingError::UnsupportedVersion(Version {
                major: 3,
                minor: 0
            }))
        );
    }

    #[test]
    fn a_higher_peer_version_is_answered_with_ours() {
        let mut bytes = Greeting::null().encode();
        bytes[10] = 9;
        bytes[11] = 9;
        let peer = Greeting::decode(&bytes).expect("decode");
        assert_eq!(peer.accept(Mechanism::NULL), Ok(VERSION));
    }

    #[test]
    fn a_different_mechanism_closes_the_connection() {
        let theirs = Greeting {
            mechanism: Mechanism::CURVE,
            as_server: true,
            ..Greeting::null()
        };
        let bytes = theirs.encode();
        let peer = Greeting::decode(&bytes).expect("decode");
        assert_eq!(peer.mechanism, Mechanism::CURVE);
        assert_eq!(
            peer.accept(Mechanism::NULL),
            Err(GreetingError::MechanismMismatch {
                theirs: Mechanism::CURVE
            })
        );
        // And the reverse: our own NULL against a CURVE peer is symmetric.
        assert_eq!(
            Greeting::null().accept(Mechanism::CURVE),
            Err(GreetingError::MechanismMismatch {
                theirs: Mechanism::NULL
            })
        );
    }

    #[test]
    fn as_server_is_refused_under_null_and_carried_otherwise() {
        let mut bytes = Greeting::null().encode();
        bytes[32] = 1;
        assert_eq!(
            Greeting::decode(&bytes),
            Err(GreetingError::AsServerUnderNull)
        );

        bytes[32] = 2;
        assert_eq!(Greeting::decode(&bytes), Err(GreetingError::BadAsServer(2)));

        let curve = Greeting {
            mechanism: Mechanism::CURVE,
            as_server: true,
            ..Greeting::null()
        };
        assert_eq!(Greeting::decode(&curve.encode()).expect("decode"), curve);
    }

    #[test]
    fn mechanism_names_obey_the_grammar() {
        assert_eq!(Mechanism::new("NULL"), Ok(Mechanism::NULL));
        assert_eq!(
            Mechanism::new("X-9_A.B+C").expect("legal").name(),
            "X-9_A.B+C"
        );
        assert_eq!(
            Mechanism::new("TWENTYONECHARACTERS!"),
            Err(GreetingError::BadMechanismName)
        );
        for bad in ["", "null", "NUL L", "NULL\0X"] {
            assert_eq!(
                Mechanism::new(bad),
                Err(GreetingError::BadMechanismName),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_mechanism_field_must_be_a_name_then_padding() {
        let mut bytes = Greeting::null().encode();
        // A name octet after the padding began.
        bytes[20] = b'X';
        assert_eq!(
            Greeting::decode(&bytes),
            Err(GreetingError::BadMechanismName)
        );

        let mut bytes = Greeting::null().encode();
        // An empty mechanism is not a name.
        bytes[12..32].fill(0);
        assert_eq!(
            Greeting::decode(&bytes),
            Err(GreetingError::BadMechanismName)
        );

        let mut bytes = Greeting::null().encode();
        bytes[12] = b'n';
        assert_eq!(
            Greeting::decode(&bytes),
            Err(GreetingError::BadMechanismName)
        );
    }

    #[test]
    fn the_partial_greeting_sniffs_a_major_version() {
        let partial = encode_partial();
        assert_eq!(partial.len(), PARTIAL_LEN);
        assert_eq!(sniff_major(&partial), Ok(3));
        // A full greeting starts with a valid partial one.
        assert_eq!(sniff_major(&Greeting::null().encode()), Ok(3));
        assert_eq!(
            sniff_major(&partial[..PARTIAL_LEN - 1]),
            Err(GreetingError::Incomplete)
        );

        let mut two_oh = partial;
        two_oh[10] = 1;
        assert_eq!(
            sniff_major(&two_oh),
            Ok(1),
            "sniffing reports the version; refusing it is the caller's decision"
        );
    }
}
