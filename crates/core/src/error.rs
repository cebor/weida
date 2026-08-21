//! The single error type of the framework.
//!
//! Hand-written `Display`/`Error` impls: the project's dependency discipline
//! (master doc §72) does not admit `thiserror` for one enum.

use std::fmt;

/// Convenience alias used throughout the framework.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong in a weida operation.
///
/// The variants are deliberately outcome-shaped rather than cause-shaped: an
/// application must distinguish `ConnectionLost` (definitely not delivered)
/// from `Indeterminate` (may or may not have been delivered) because the two
/// permit different retry decisions. See `docs/FAILURE_MODEL.md`.
#[derive(Debug)]
pub enum Error {
    /// The runtime could not be created or used (e.g. no ambient reactor).
    Runtime(String),
    /// A `weida://` URL could not be parsed.
    InvalidAddress(String),
    /// An endpoint path violated the addressing rules.
    InvalidEndpointPath,
    /// An endpoint path is already registered on this listener.
    AlreadyRegistered,
    /// The endpoint has no usable peer connection.
    NotConnected,
    /// The connection was lost before the local transfer reached FIN; the
    /// payload was definitely not delivered.
    ConnectionLost,
    /// Version/capability negotiation failed.
    Negotiation(String),
    /// The peer violated the wire protocol.
    Protocol(String),
    /// The peer refused the transfer (`STOP_SENDING(REJECTED)` or
    /// `ERROR{REJECTED}`).
    Rejected,
    /// The peer has no endpoint registered under the requested path.
    UnknownEndpoint,
    /// The peer does not support a requested protocol feature.
    Unsupported,
    /// The peer accepted the request but never opened a reply stream.
    NoReply,
    /// The transfer was canceled, locally or by the peer.
    Canceled,
    /// The connection was lost after the local FIN while awaiting an ACK or a
    /// reply: the outcome is genuinely unknown (master doc §22).
    Indeterminate,
    /// A local or negotiated resource limit was reached.
    LimitExceeded,
    /// TLS material could not be loaded or configured.
    Tls(String),
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// Transport-level failure that is not one of the modelled outcomes.
    Transport(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Runtime(m) => write!(f, "runtime error: {m}"),
            Error::InvalidAddress(m) => write!(f, "invalid address: {m}"),
            Error::InvalidEndpointPath => f.write_str(
                "invalid endpoint path: must start with '/', be 1..=512 bytes and contain no control bytes",
            ),
            Error::AlreadyRegistered => f.write_str("endpoint path already registered"),
            Error::NotConnected => f.write_str("endpoint is not connected to any peer"),
            Error::ConnectionLost => f.write_str("connection lost before the transfer completed"),
            Error::Negotiation(m) => write!(f, "negotiation failed: {m}"),
            Error::Protocol(m) => write!(f, "protocol violation: {m}"),
            Error::Rejected => f.write_str("peer rejected the transfer"),
            Error::UnknownEndpoint => f.write_str("peer has no such endpoint"),
            Error::Unsupported => f.write_str("peer does not support the requested feature"),
            Error::NoReply => f.write_str("peer accepted the request but sent no reply"),
            Error::Canceled => f.write_str("transfer canceled"),
            Error::Indeterminate => {
                f.write_str("outcome indeterminate: the transfer may or may not have been accepted")
            }
            Error::LimitExceeded => f.write_str("resource limit exceeded"),
            Error::Tls(m) => write!(f, "tls error: {m}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Transport(m) => write!(f, "transport error: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl Error {
    /// True if the error proves the transfer had no effect at the peer.
    ///
    /// `Indeterminate` is deliberately *not* in this set: modelling it apart
    /// from definite failure is the point of master doc §22.
    pub fn is_definite_failure(&self) -> bool {
        matches!(
            self,
            Error::ConnectionLost
                | Error::Rejected
                | Error::UnknownEndpoint
                | Error::Canceled
                | Error::NotConnected
                | Error::LimitExceeded
        )
    }
}

/// Wire codes carried in an ERROR frame (`docs/PROTOCOL.md` §6.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// No endpoint is registered for the requested path.
    UnknownEndpoint,
    /// The receiving side declined the transfer.
    Rejected,
    /// A reserved or unsupported header value was requested.
    Unsupported,
    /// The receiving side failed internally.
    Internal,
    /// The request was accepted but no reply will be produced.
    NoReply,
}

impl ErrorCode {
    /// The wire code.
    pub const fn to_wire(self) -> u64 {
        match self {
            ErrorCode::UnknownEndpoint => 1,
            ErrorCode::Rejected => 2,
            ErrorCode::Unsupported => 3,
            ErrorCode::Internal => 4,
            ErrorCode::NoReply => 5,
        }
    }

    /// Interprets a wire code, returning `None` for unknown values.
    pub const fn from_wire(code: u64) -> Option<ErrorCode> {
        match code {
            1 => Some(ErrorCode::UnknownEndpoint),
            2 => Some(ErrorCode::Rejected),
            3 => Some(ErrorCode::Unsupported),
            4 => Some(ErrorCode::Internal),
            5 => Some(ErrorCode::NoReply),
            _ => None,
        }
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Error {
        match code {
            ErrorCode::UnknownEndpoint => Error::UnknownEndpoint,
            ErrorCode::Rejected => Error::Rejected,
            ErrorCode::Unsupported => Error::Unsupported,
            ErrorCode::Internal => Error::Transport("peer reported an internal error".into()),
            ErrorCode::NoReply => Error::NoReply,
        }
    }
}

/// Why a peer refused to receive more payload, as carried by
/// `STOP_SENDING`'s QUIC application error code.
///
/// The transport maps the numeric code (`weida_protocol::codes`) onto this enum;
/// the core stays free of transport constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StopReason {
    /// `REJECTED`: the application declined the transfer.
    Rejected,
    /// `CANCELED`: the peer is no longer interested.
    Canceled,
    /// `UNKNOWN_ENDPOINT`: no endpoint is registered for the path.
    UnknownEndpoint,
    /// Any other code, kept for diagnostics.
    Other(u64),
}

impl From<StopReason> for Error {
    fn from(reason: StopReason) -> Error {
        match reason {
            StopReason::Rejected => Error::Rejected,
            StopReason::Canceled => Error::Canceled,
            StopReason::UnknownEndpoint => Error::UnknownEndpoint,
            StopReason::Other(code) => {
                Error::Transport(format!("peer stopped receiving with code {code}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_non_empty_for_every_variant() {
        let variants = [
            Error::Runtime("x".into()),
            Error::InvalidAddress("x".into()),
            Error::InvalidEndpointPath,
            Error::AlreadyRegistered,
            Error::NotConnected,
            Error::ConnectionLost,
            Error::Negotiation("x".into()),
            Error::Protocol("x".into()),
            Error::Rejected,
            Error::UnknownEndpoint,
            Error::Unsupported,
            Error::NoReply,
            Error::Canceled,
            Error::Indeterminate,
            Error::LimitExceeded,
            Error::Tls("x".into()),
            Error::Io(std::io::Error::other("x")),
            Error::Transport("x".into()),
        ];
        for v in &variants {
            assert!(!v.to_string().is_empty(), "{v:?}");
        }
    }

    #[test]
    fn indeterminate_is_not_a_definite_failure() {
        assert!(!Error::Indeterminate.is_definite_failure());
        assert!(Error::ConnectionLost.is_definite_failure());
    }

    #[test]
    fn io_error_is_the_source() {
        use std::error::Error as _;
        let e = Error::Io(std::io::Error::other("boom"));
        assert!(e.source().is_some());
        assert!(Error::Canceled.source().is_none());
    }
}
