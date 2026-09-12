//! What can go wrong in a bridge, kept separate from both protocols'
//! vocabularies.
//!
//! A bridge is a hop, not a tunnel: it terminates ZMTP and it terminates
//! weida, so it has its own failures — a configuration it refuses, a message
//! shape neither protocol can carry across. Mapping those onto `weida::Error`
//! would claim they happened on the weida side, and mapping them onto a ZMTP
//! code would claim ZeroMQ has a word for them. Most of these are **named
//! losses** of `docs/adapters/zmtp.md` §8 and refused configurations of §9,
//! and this enum is where they become values.
//!
//! **Narrowed when the protocol moved out.** The ZMTP-shaped variants —
//! `Handshake`, `SocketType` and the `From` conversions for the codec's
//! `GreetingError`, `FrameError` and `CommandError` — are gone with
//! `wire.rs`: the greeting, the socket-type table and the framing are
//! `weida-zmq`'s, so what they produce is a [`weida_zmq::Error`] carrying
//! libzmq's own errno name ([0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2). A bridge that kept its own names for them would be reporting a
//! protocol it no longer speaks.

use std::fmt;

/// Why a bridged connection or configuration failed.
#[derive(Debug)]
pub enum BridgeError {
    /// The ZeroMQ peer closed the connection, orderly and without saying
    /// anything. ZMTP has no goodbye: "Either peer may at any moment close the
    /// connection."
    PeerClosed,
    /// The bridge refuses to run with this configuration. Every refusal of
    /// `docs/adapters/zmtp.md` §9 that can be decided before serving is one of
    /// these, which is the rule at an adapter edge: refuse at configuration
    /// time rather than degrade at run time
    /// ([0006](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.7).
    Configuration(String),
    /// A message shape this bridge refuses: a multipart message where the
    /// pattern defines none (loss L1), or a published message without its
    /// topic frame.
    Protocol(String),
    /// A subscription that cannot be translated into a weida filter without
    /// changing what it selects: a byte prefix ending mid-segment (loss L2) or
    /// one containing weida's separator or wildcards (loss L4).
    Subscription(String),
    /// The weida side failed. Carried as-is, because a weida error is exactly
    /// what happened.
    Weida(weida::Error),
    /// The ZeroMQ side failed, under libzmq's own errno name.
    Zmq(weida_zmq::Error),
    /// Socket I/O the bridge itself did — parsing an address, mostly.
    Io(std::io::Error),
}

impl BridgeError {
    /// True if the connection cannot continue. Everything here is fatal to the
    /// connection except a subscription refusal, which costs the peer its
    /// subscription and nothing else.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, BridgeError::Subscription(_))
    }
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeError::PeerClosed => f.write_str("the ZeroMQ peer closed the connection"),
            BridgeError::Configuration(reason) => {
                write!(f, "this bridge configuration is refused: {reason}")
            }
            BridgeError::Protocol(reason) => write!(f, "ZMTP protocol error: {reason}"),
            BridgeError::Subscription(reason) => write!(f, "subscription refused: {reason}"),
            BridgeError::Weida(e) => write!(f, "weida side: {e}"),
            BridgeError::Zmq(e) => write!(f, "ZeroMQ side: {e}"),
            BridgeError::Io(e) => write!(f, "socket error: {e}"),
        }
    }
}

impl std::error::Error for BridgeError {}

impl From<std::io::Error> for BridgeError {
    fn from(e: std::io::Error) -> BridgeError {
        BridgeError::Io(e)
    }
}

impl From<weida::Error> for BridgeError {
    fn from(e: weida::Error) -> BridgeError {
        BridgeError::Weida(e)
    }
}

impl From<weida_zmq::Error> for BridgeError {
    fn from(e: weida_zmq::Error) -> BridgeError {
        BridgeError::Zmq(e)
    }
}
