//! Peer identity: the fingerprint of a certificate's public key.
//!
//! weida names a peer by what it can prove it holds — a private key — rather
//! than by what a certificate authority says about it. The fingerprint is the
//! SHA-256 digest of the DER-encoded `SubjectPublicKeyInfo` of the leaf
//! certificate, the same value `openssl x509 -pubkey | openssl pkey -pubin
//! -outform der | sha256sum` prints and the one HPKP and `curl --pinnedpubkey`
//! pin. Hashing the key rather than the certificate keeps the fingerprint
//! stable across certificate renewals that reuse the key.
//!
//! This module holds only the value type and its text form. Computing a
//! fingerprint from certificate bytes needs a parser and a hash, both of which
//! live in the transport crate.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// Text prefix of the canonical form; names the digest so the form can evolve.
const PREFIX: &str = "sha256:";

/// SHA-256 digest of a peer's public key.
///
/// Canonical text form: `sha256:` followed by 64 lowercase hex digits. Parsing
/// accepts either case; nothing else. Equality is byte equality.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    /// Wraps a digest already computed.
    pub const fn from_bytes(bytes: [u8; 32]) -> Fingerprint {
        Fingerprint(bytes)
    }

    /// The raw digest.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A principal the **kernel** proved, on a local transport.
///
/// Captured at connect time and fixed for the life of the connection, which
/// is what `SO_PEERCRED` and `LOCAL_PEERCRED` give and all they give
/// (`docs/research/ipc.md` §1.5, §2.2). Two rules come with it
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.4):
/// the credential is the one taken at connect and never at send time, and a
/// **PID is an observation** — it MUST NOT be the thing an application
/// authorizes on, because it is reusable and racy where it exists at all.
/// macOS reports no PID, so `pid` is `None` there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocalPrincipal {
    /// Effective user id of the peer process at connect time.
    pub uid: u32,
    /// Primary group id, where the platform reports one.
    pub gid: u32,
    /// Process id, where the platform reports one. An observation only.
    pub pid: Option<u32>,
}

/// A principal the **kernel** proved, on a Windows named pipe.
///
/// The client's token SID, read through `ImpersonateNamedPipeClient` on the
/// serving side and captured once at connect time
/// (`docs/research/ipc.md` §3.3, [0010 §4.4]). The same two rules as
/// [`LocalPrincipal`]: the credential is the one taken at connect, and the
/// PID is an observation that MUST NOT be authorized on — `GetNamedPipe
/// ClientProcessId` reports it and nothing signs it.
///
/// A separate type rather than a third field set on [`LocalPrincipal`],
/// because a SID and a uid are not comparable values and a caller that
/// authorizes on one must be made to say which. The SID is an `Arc<str>`
/// rather than a `String` so that a [`PeerIdentity`] stays a refcount bump
/// to clone: it is copied into every incoming transfer's metadata, and a
/// string allocation there would be one per message.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WindowsPrincipal {
    /// The account SID in its string form (`S-1-5-21-...`).
    pub sid: std::sync::Arc<str>,
    /// The client process id, where the pipe reports one. An observation
    /// only.
    pub pid: Option<u32>,
}

/// Who the peer is, once it has been **proved**.
///
/// Two kinds of proof, never a claim
/// ([decisions/0008](../../../docs/decisions/0008-session-identity.md) §4.1 as
/// amended by [0010](../../../docs/decisions/0010-local-transport.md) §4.4): a
/// key the peer demonstrated it holds in the TLS handshake, or a principal the
/// kernel attributed to the process on the other end of a local connection.
/// An anonymous TLS client and an in-process peer have neither, and are
/// reported as `None` rather than as an empty identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PeerIdentity {
    /// The peer's public-key fingerprint, proved by the TLS handshake.
    Key(Fingerprint),
    /// The peer's local principal, proved by the kernel.
    Local(LocalPrincipal),
    /// The peer's Windows account, proved by the kernel through the pipe's
    /// client token.
    Windows(WindowsPrincipal),
}

impl PeerIdentity {
    /// The proved key, if this identity is one.
    pub fn key(&self) -> Option<Fingerprint> {
        match self {
            PeerIdentity::Key(fp) => Some(*fp),
            PeerIdentity::Local(_) | PeerIdentity::Windows(_) => None,
        }
    }

    /// The proved local principal, if this identity is one.
    pub fn local(&self) -> Option<LocalPrincipal> {
        match self {
            PeerIdentity::Local(principal) => Some(*principal),
            PeerIdentity::Key(_) | PeerIdentity::Windows(_) => None,
        }
    }

    /// The proved Windows account, if this identity is one.
    pub fn windows(&self) -> Option<&WindowsPrincipal> {
        match self {
            PeerIdentity::Windows(principal) => Some(principal),
            PeerIdentity::Key(_) | PeerIdentity::Local(_) => None,
        }
    }
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerIdentity::Key(fp) => write!(f, "{fp}"),
            PeerIdentity::Local(p) => match p.pid {
                Some(pid) => write!(f, "uid:{} gid:{} pid:{pid}", p.uid, p.gid),
                None => write!(f, "uid:{} gid:{}", p.uid, p.gid),
            },
            PeerIdentity::Windows(p) => match p.pid {
                Some(pid) => write!(f, "sid:{} pid:{pid}", p.sid),
                None => write!(f, "sid:{}", p.sid),
            },
        }
    }
}

impl From<Fingerprint> for PeerIdentity {
    fn from(fp: Fingerprint) -> PeerIdentity {
        PeerIdentity::Key(fp)
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PREFIX)?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fingerprint({self})")
    }
}

impl FromStr for Fingerprint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Fingerprint, Error> {
        let invalid = || Error::InvalidFingerprint(s.to_owned());
        let hex = s.strip_prefix(PREFIX).ok_or_else(invalid)?;
        if hex.len() != 64 {
            return Err(invalid());
        }
        let mut out = [0u8; 32];
        for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(pair[0]).ok_or_else(invalid)?;
            let lo = hex_nibble(pair[1]).ok_or_else(invalid)?;
            out[i] = (hi << 4) | lo;
        }
        Ok(Fingerprint(out))
    }
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0xff,
    ];
    const TEXT: &str = "sha256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eff";

    #[test]
    fn display_is_prefixed_lowercase_hex() {
        assert_eq!(Fingerprint::from_bytes(DIGEST).to_string(), TEXT);
    }

    #[test]
    fn parse_accepts_either_case_and_roundtrips() {
        let fp: Fingerprint = TEXT.parse().unwrap();
        assert_eq!(fp.as_bytes(), &DIGEST);
        let upper = TEXT.to_uppercase().replace("SHA256", "sha256");
        assert_eq!(upper.parse::<Fingerprint>().unwrap(), fp);
        assert_eq!(fp.to_string().parse::<Fingerprint>().unwrap(), fp);
    }

    #[test]
    fn parse_rejects_anything_else() {
        let cases = [
            "",
            "sha256:",
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eff",
            "sha1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eff",
            "sha256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1ef",
            "sha256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1efff",
            "sha256:0g0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eff",
            "SHA256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eff",
        ];
        for c in cases {
            assert!(
                matches!(c.parse::<Fingerprint>(), Err(Error::InvalidFingerprint(_))),
                "expected rejection of {c:?}"
            );
        }
    }
}
