//! What can go wrong in a bridge, kept separate from both protocols'
//! vocabularies.
//!
//! A bridge is a hop, not a tunnel: it terminates SP and it terminates weida,
//! so it has its own failures - a configuration it refuses, a peer whose
//! endpoint type may not talk to it, a message larger than it will hold.
//! Mapping those onto `weida::Error` would claim they happened on the weida
//! side, and mapping them onto an SP code would claim SP has a word for them,
//! which it does not: **SP has no error frame at all**, so every refusal here
//! is observable to the peer only as a closed connection
//! (`docs/adapters/nng.md` §8 L10, [rfc-tcp §2]). Most of these are named
//! losses of that document's §8 and refused configurations of §9, and this
//! enum is where they become values.

use std::fmt;

use weida_sp::error::{HeaderError, MessageError, TagError};
use weida_sp::header::EndpointType;

/// Why a bridged connection or configuration failed.
#[derive(Debug)]
pub enum BridgeError {
    /// The SP peer closed the connection. SP has no goodbye: a pipe is
    /// removed when either side closes it and nothing is said on the wire
    /// (`docs/research/nanomsg-nng.md` §1).
    PeerClosed,
    /// The bridge refuses to run with this configuration. Every refusal of
    /// `docs/adapters/nng.md` §9 that can be decided before serving is one of
    /// these, which is the rule at an adapter edge: refuse at configuration
    /// time rather than degrade at run time
    /// ([0006](https://github.com/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.7).
    Configuration(String),
    /// The 8-octet protocol header was wrong: magic, version or reserved
    /// field. "If the protocol header received from the peer differs, the TCP
    /// connection MUST be closed immediately" [rfc-tcp §2].
    Handshake(HeaderError),
    /// The peer's endpoint type may not talk to the one this bridge presents.
    ///
    /// There is nothing to answer with - see the module note - so the peer
    /// observes a close, exactly as it would from a real NNG socket
    /// ("incompatible peers must disconnect", `docs/research/nanomsg-nng.md`
    /// §1).
    EndpointType {
        /// What the bridge presents.
        ours: EndpointType,
        /// What the peer announced.
        theirs: EndpointType,
    },
    /// The peer did something its own protocol forbids - a SUB socket
    /// sending, for instance, which `docs/research/nanomsg-nng.md` §4 says it
    /// cannot do.
    Protocol(String),
    /// The peer violated the framing, or declared a message larger than
    /// `max_message_bytes`.
    Message(MessageError),
    /// A per-protocol header inside a body was malformed: a truncated tag, or
    /// a tag stack deeper than the configured hop limit.
    Tags(TagError),
    /// The weida side failed. Carried as-is, because a weida error is exactly
    /// what happened.
    Weida(weida::Error),
    /// Socket I/O.
    Io(std::io::Error),
}

impl BridgeError {
    /// True if the connection is finished. Everything but a refused message
    /// is, because SP offers no way to skip a message the bridge would not
    /// read: the next one starts after a body this side declined.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, BridgeError::Tags(e) if !e.is_violation())
    }
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeError::PeerClosed => f.write_str("the SP peer closed the connection"),
            BridgeError::Configuration(m) => write!(f, "bridge configuration refused: {m}"),
            BridgeError::Handshake(e) => write!(f, "SP protocol header rejected: {e}"),
            BridgeError::EndpointType { ours, theirs } => write!(
                f,
                "a {theirs:?} peer may not talk to the {ours:?} this bridge presents"
            ),
            BridgeError::Message(e) => write!(f, "{e}"),
            BridgeError::Protocol(m) => write!(f, "SP peer violated its own protocol: {m}"),
            BridgeError::Tags(e) => write!(f, "{e}"),
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

impl From<HeaderError> for BridgeError {
    fn from(e: HeaderError) -> Self {
        BridgeError::Handshake(e)
    }
}

impl From<MessageError> for BridgeError {
    fn from(e: MessageError) -> Self {
        BridgeError::Message(e)
    }
}

impl From<TagError> for BridgeError {
    fn from(e: TagError) -> Self {
        BridgeError::Tags(e)
    }
}
