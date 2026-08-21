//! Version and capability negotiation.
//!
//! Negotiation is an exchange during connection establishment, not a
//! long-lived control stream (master doc §13, §11). Both sides run this same
//! function against their own and the peer's HELLO, so both reach the same
//! verdict without a round trip.

use std::fmt;

use crate::header::Hello;

/// The negotiated parameters of a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Agreed {
    /// Effective wire protocol version: the highest version both sides support.
    pub version: u64,
    /// Largest header the **peer** accepts. Outgoing headers must not exceed
    /// it; it is not a limit on what we receive.
    pub send_max_header_bytes: u64,
}

/// Why negotiation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NegotiateError {
    /// The version sets do not intersect.
    NoCommonVersion,
    /// The peer requires a capability this implementation does not support.
    UnsupportedRequiredCapability(u64),
}

impl fmt::Display for NegotiateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NegotiateError::NoCommonVersion => {
                f.write_str("no wire protocol version is supported by both peers")
            }
            NegotiateError::UnsupportedRequiredCapability(c) => {
                write!(f, "peer requires unsupported capability {c}")
            }
        }
    }
}

impl std::error::Error for NegotiateError {}

impl From<NegotiateError> for weida_core::Error {
    fn from(e: NegotiateError) -> weida_core::Error {
        weida_core::Error::Negotiation(e.to_string())
    }
}

/// Negotiates a connection from the two HELLO headers.
///
/// The effective version is the maximum of the intersection of the two version
/// lists. Every capability the peer marks as required must appear in our own
/// supported set; v0 defines no capability codes, so any requirement fails.
pub fn negotiate(ours: &Hello, theirs: &Hello) -> Result<Agreed, NegotiateError> {
    let version = theirs
        .versions
        .iter()
        .copied()
        .filter(|v| ours.versions.contains(v))
        .max()
        .ok_or(NegotiateError::NoCommonVersion)?;

    if let Some(missing) = theirs
        .required_capabilities
        .iter()
        .copied()
        .find(|c| !ours.capabilities.contains(c))
    {
        return Err(NegotiateError::UnsupportedRequiredCapability(missing));
    }

    Ok(Agreed {
        version,
        send_max_header_bytes: theirs.max_header_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello(versions: &[u64], caps: &[u64], required: &[u64], max_header: u64) -> Hello {
        Hello {
            versions: versions.to_vec(),
            max_header_bytes: max_header,
            max_transfers: 1024,
            capabilities: caps.to_vec(),
            required_capabilities: required.to_vec(),
        }
    }

    #[test]
    fn v0_peers_agree_on_version_zero() {
        let ours = Hello::v0(16384, 1024);
        let theirs = Hello::v0(4096, 512);
        let agreed = negotiate(&ours, &theirs).unwrap();
        assert_eq!(agreed.version, 0);
        // The send cap comes from the peer's advertised receive cap.
        assert_eq!(agreed.send_max_header_bytes, 4096);
    }

    #[test]
    fn the_highest_common_version_wins() {
        let ours = hello(&[0, 1, 2, 5], &[], &[], 16384);
        let theirs = hello(&[1, 2, 3], &[], &[], 16384);
        assert_eq!(negotiate(&ours, &theirs).unwrap().version, 2);
    }

    #[test]
    fn version_order_in_the_list_does_not_matter() {
        let ours = hello(&[5, 0, 2], &[], &[], 16384);
        let theirs = hello(&[2, 5], &[], &[], 16384);
        assert_eq!(negotiate(&ours, &theirs).unwrap().version, 5);
    }

    #[test]
    fn an_empty_intersection_fails() {
        let ours = hello(&[0], &[], &[], 16384);
        for theirs in [hello(&[99], &[], &[], 16384), hello(&[], &[], &[], 16384)] {
            assert_eq!(
                negotiate(&ours, &theirs),
                Err(NegotiateError::NoCommonVersion)
            );
        }
    }

    #[test]
    fn a_required_capability_we_lack_fails() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[0], &[7], &[7], 16384);
        assert_eq!(
            negotiate(&ours, &theirs),
            Err(NegotiateError::UnsupportedRequiredCapability(7))
        );
    }

    #[test]
    fn a_required_capability_we_have_succeeds() {
        let ours = hello(&[0], &[7, 9], &[], 16384);
        let theirs = hello(&[0], &[7], &[7], 8192);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap(),
            Agreed {
                version: 0,
                send_max_header_bytes: 8192
            }
        );
    }

    #[test]
    fn optional_peer_capabilities_are_ignored() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[0], &[1, 2, 3], &[], 16384);
        assert!(negotiate(&ours, &theirs).is_ok());
    }

    #[test]
    fn version_mismatch_is_checked_before_capabilities() {
        let ours = Hello::v0(16384, 1024);
        let theirs = hello(&[99], &[], &[42], 16384);
        assert_eq!(
            negotiate(&ours, &theirs),
            Err(NegotiateError::NoCommonVersion)
        );
    }

    #[test]
    fn negotiation_is_symmetric_for_the_agreed_version() {
        let a = hello(&[0, 1], &[], &[], 1000);
        let b = hello(&[1, 2], &[], &[], 2000);
        let ab = negotiate(&a, &b).unwrap();
        let ba = negotiate(&b, &a).unwrap();
        assert_eq!(ab.version, ba.version);
        assert_eq!(ab.send_max_header_bytes, 2000);
        assert_eq!(ba.send_max_header_bytes, 1000);
    }

    #[test]
    fn errors_become_negotiation_errors() {
        let e: weida_core::Error = NegotiateError::NoCommonVersion.into();
        assert!(matches!(e, weida_core::Error::Negotiation(_)));
    }
}
