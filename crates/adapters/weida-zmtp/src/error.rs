//! Why a greeting, a frame or a command was rejected.
//!
//! Three types, split by the layer that produces them, because the layers
//! disagree about what "not enough bytes" means:
//!
//! * a **greeting** is 64 octets read once per connection, and a short read is
//!   normal;
//! * a **frame header** is decoded from a byte stream, so incompleteness is
//!   expected and is not a protocol violation;
//! * a **command body** is decoded from an already-complete frame body, where a
//!   field that runs off the end is a violation and never a request for more
//!   bytes.
//!
//! Every type answers [`is_violation`](FrameError::is_violation): true means
//! close the connection, which is the only remedy ZMTP defines
//! ([37/ZMTP](https://rfc.zeromq.org/spec/37/), "Error Handling").

use std::fmt;

use crate::greeting::{Mechanism, Version};

/// Why a greeting was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GreetingError {
    /// Fewer octets than the form being parsed needs. Read more and retry;
    /// **not** a protocol violation.
    Incomplete,
    /// The signature is wrong: octet 0 must be `0xFF` and octet 9 `0x7F`. The
    /// eight padding octets between them are never checked.
    BadSignature {
        /// Offset of the octet that was wrong, 0 or 9.
        at: usize,
        /// What was there instead.
        found: u8,
    },
    /// The peer speaks a version this codec cannot talk to. ZMTP requires
    /// accepting anything at or above 3.1 and permits, but does not require,
    /// downgrading below it; this codec implements 3.1 only and so closes.
    UnsupportedVersion(Version),
    /// A mechanism name contains an octet the grammar does not allow, or a
    /// non-zero octet after the null padding began.
    BadMechanismName,
    /// The peer announced a different mechanism than we did. ZMTP security is
    /// "assertive": there is no negotiation, and a mismatch closes.
    MechanismMismatch {
        /// What the peer announced.
        theirs: Mechanism,
    },
    /// The `as-server` octet was neither 0 nor 1.
    BadAsServer(u8),
    /// `as-server` was set while the mechanism is NULL, where the
    /// specification requires zero.
    AsServerUnderNull,
}

impl GreetingError {
    /// True if the error is fatal for the connection. Only
    /// [`GreetingError::Incomplete`] is not.
    pub const fn is_violation(self) -> bool {
        !matches!(self, GreetingError::Incomplete)
    }
}

impl fmt::Display for GreetingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GreetingError::Incomplete => f.write_str("incomplete greeting"),
            GreetingError::BadSignature { at, found } => {
                let want = if *at == 0 { 0xFF } else { 0x7F };
                write!(
                    f,
                    "bad signature octet {at}: {found:#04x}, expected {want:#04x}"
                )
            }
            GreetingError::UnsupportedVersion(v) => {
                write!(
                    f,
                    "peer speaks ZMTP {v}, which this codec cannot downgrade to"
                )
            }
            GreetingError::BadMechanismName => f.write_str("malformed mechanism name"),
            GreetingError::MechanismMismatch { theirs } => {
                write!(f, "peer announced mechanism {theirs}, which is not ours")
            }
            GreetingError::BadAsServer(b) => write!(f, "as-server octet is {b}, expected 0 or 1"),
            GreetingError::AsServerUnderNull => {
                f.write_str("as-server is set, but the NULL mechanism requires zero")
            }
        }
    }
}

impl std::error::Error for GreetingError {}

/// Why a frame header was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer octets than the frame needs. Read more and retry; **not** a
    /// protocol violation.
    Incomplete,
    /// Bits 7-3 of the flags octet are reserved and MUST be zero.
    ReservedFlags(u8),
    /// The MORE flag was set on a command frame, where it "SHALL be zero".
    MoreOnCommand,
    /// The declared body length exceeds the local cap. Reported **before** any
    /// allocation: a long frame may declare up to 2^63-1 octets, and a cap is
    /// the only defence ZMTP offers, `ZMQ_MAXMSGSIZE` being unlimited by
    /// default.
    BodyTooLarge {
        /// Length the peer declared.
        len: u64,
        /// Local cap that was exceeded.
        max: u64,
    },
}

impl FrameError {
    /// True if the error is fatal for the connection. Only
    /// [`FrameError::Incomplete`] is not.
    ///
    /// `BodyTooLarge` is fatal on purpose and matches libzmq, where exceeding
    /// `ZMQ_MAXMSGSIZE` disconnects the peer rather than skipping the message:
    /// a frame whose body is not read cannot be skipped, because the next
    /// frame starts after a body this side refused to buffer.
    pub const fn is_violation(self) -> bool {
        !matches!(self, FrameError::Incomplete)
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Incomplete => f.write_str("incomplete frame"),
            FrameError::ReservedFlags(b) => {
                write!(f, "reserved flag bits set in flags octet {b:#04x}")
            }
            FrameError::MoreOnCommand => f.write_str("MORE set on a command frame"),
            FrameError::BodyTooLarge { len, max } => {
                write!(f, "frame body of {len} octets exceeds the limit of {max}")
            }
        }
    }
}

impl std::error::Error for FrameError {}

/// Why a command body was rejected, on the way in or on the way out.
///
/// One vocabulary for both directions: every length rule the decoder enforces
/// is a rule the encoder must not break either, and a single set of names keeps
/// the two halves from drifting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandError {
    /// A length-prefixed field runs past the end of the body. The body is
    /// already complete when a command is decoded, so this is a violation and
    /// never a request for more bytes.
    Truncated,
    /// The command name is empty, or contains an octet other than a letter.
    BadName,
    /// A command name this codec does not implement. `JOIN` and `LEAVE` are
    /// the known omission: they belong to RADIO/DISH, which the adapter's
    /// socket-type table does not map.
    UnknownName,
    /// A metadata property name is empty or contains an octet the grammar does
    /// not allow.
    BadPropertyName,
    /// A metadata value is longer than the four-octet size field can describe.
    ValueTooLong(usize),
    /// A `PING`/`PONG` context exceeds 16 octets.
    ContextTooLong(usize),
    /// An `ERROR` reason exceeds 255 octets.
    ReasonTooLong(usize),
    /// An `ERROR` reason contains a non-printable octet.
    ReasonNotPrintable,
}

impl CommandError {
    /// True if the error is fatal for the connection. Every command error is:
    /// a command body arrives whole, so there is nothing to wait for.
    pub const fn is_violation(self) -> bool {
        true
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Truncated => f.write_str("command body ends inside a field"),
            CommandError::BadName => f.write_str("malformed command name"),
            CommandError::UnknownName => f.write_str("unimplemented command name"),
            CommandError::BadPropertyName => f.write_str("malformed metadata property name"),
            CommandError::ValueTooLong(n) => {
                write!(f, "metadata value of {n} octets exceeds 2147483647")
            }
            CommandError::ContextTooLong(n) => {
                write!(f, "ping context of {n} octets exceeds 16")
            }
            CommandError::ReasonTooLong(n) => write!(f, "error reason of {n} octets exceeds 255"),
            CommandError::ReasonNotPrintable => {
                f.write_str("error reason contains a non-printable octet")
            }
        }
    }
}

impl std::error::Error for CommandError {}
