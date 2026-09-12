//! What can go wrong in a bridge, kept separate from both protocols'
//! vocabularies.
//!
//! A bridge is a hop, not a tunnel: it terminates SP and it terminates
//! weida, so it has its own failures — a configuration it refuses, a peer
//! whose endpoint type may not talk to it, a message larger than it will
//! hold. Mapping those onto `weida::Error` would claim they happened on the
//! weida side; mapping them onto an SP code would claim SP has a word for
//! them, which it does not: **SP has no error frame at all**, so every
//! refusal here is observable to the peer only as a closed connection
//! (`docs/adapters/nng.md` §8 L10, [rfc-tcp §2]). Most of these are named
//! losses of that document's §8 and refused configurations of §9, and this
//! enum is where they become values.
//!
//! **What is no longer here.** The variants that described SP's own
//! failures — a wrong protocol header, a malformed frame, a bad tag stack —
//! are gone, because the bridge no longer parses SP: the sockets of
//! `weida-nng` do, and they report those in NNG's own `NNG_E*` vocabulary
//! ([0013](https://github.com/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2). What arrives here is [`BridgeError::Sp`], one variant carrying
//! that vocabulary unchanged, because an NNG code is exactly what happened.

use std::fmt;

use weida_sp::header::EndpointType;

/// Why a bridged connection or configuration failed.
#[derive(Debug)]
pub enum BridgeError {
    /// The SP peer closed the connection, or its pipe was removed. SP has no
    /// goodbye: a pipe is removed when either side closes it and nothing is
    /// said on the wire (`docs/research/nanomsg-nng.md` §1).
    PeerClosed,
    /// The bridge refuses to run with this configuration. Every refusal of
    /// `docs/adapters/nng.md` §9 that can be decided before serving is one
    /// of these, which is the rule at an adapter edge: refuse at
    /// configuration time rather than degrade at run time
    /// ([0006](https://github.com/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.7).
    Configuration(String),
    /// The peer this bridge dialled may not talk to the endpoint type it
    /// presents.
    ///
    /// There is nothing to answer with — see the module note — so the peer
    /// observes a close, exactly as it would from a real NNG socket
    /// ("incompatible peers must disconnect", `docs/research/nanomsg-nng.md`
    /// §1). The library refuses the pairing before any traffic and says why;
    /// `reason` is what it said.
    EndpointType {
        /// What the bridge presents.
        ours: EndpointType,
        /// Why the pairing was refused, in the library's words.
        reason: String,
    },
    /// The peer did something its own protocol forbids.
    Protocol(String),
    /// The SP side failed, in NNG's own vocabulary.
    Sp(weida_nng::Error),
    /// The weida side failed. Carried as-is, because a weida error is
    /// exactly what happened.
    Weida(weida::Error),
    /// Socket I/O on the bridge's own listeners.
    Io(std::io::Error),
}

impl BridgeError {
    /// True if the bridged connection is finished.
    ///
    /// Everything is, now that the bridge no longer parses SP: the sockets
    /// underneath decide per message what survives a malformed one — a
    /// message dropped for its hop count keeps the pipe, a malformed frame
    /// does not — and what reaches this type is the end of a connection or
    /// of a run.
    pub fn is_fatal(&self) -> bool {
        true
    }
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeError::PeerClosed => f.write_str("the SP peer closed the connection"),
            BridgeError::Configuration(m) => write!(f, "bridge configuration refused: {m}"),
            BridgeError::EndpointType { ours, reason } => write!(
                f,
                "the peer may not talk to the {ours:?} this bridge presents: {reason}"
            ),
            BridgeError::Protocol(m) => write!(f, "SP peer violated its own protocol: {m}"),
            BridgeError::Sp(e) => write!(f, "SP side: {e}"),
            BridgeError::Weida(e) => write!(f, "weida side: {e}"),
            BridgeError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for BridgeError {}

impl From<std::io::Error> for BridgeError {
    fn from(e: std::io::Error) -> Self {
        BridgeError::Io(e)
    }
}

impl From<weida::Error> for BridgeError {
    fn from(e: weida::Error) -> Self {
        BridgeError::Weida(e)
    }
}

/// An NNG code arrives as itself, except the two that mean "the peer is
/// gone": those are the one thing a bridge loop treats as ordinary, so they
/// become [`BridgeError::PeerClosed`] and are logged at debug rather than as
/// a failure.
impl From<weida_nng::Error> for BridgeError {
    fn from(e: weida_nng::Error) -> Self {
        match e {
            weida_nng::Error::ECLOSED(_) | weida_nng::Error::ECONNRESET(_) => {
                BridgeError::PeerClosed
            }
            other => BridgeError::Sp(other),
        }
    }
}
