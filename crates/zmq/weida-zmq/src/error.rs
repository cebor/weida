//! The error vocabulary: libzmq's errno names, each with a cause.
//!
//! A libzmq application branches on `errno`, and every recipe in the zguide
//! names one: Lazy Pirate closes and reopens a REQ socket after `EFSM`,
//! `ZMQ_DONTWAIT` is recognised by `EAGAIN`, a terminating context is
//! `ETERM`, `ZMQ_ROUTER_MANDATORY` reports `EHOSTUNREACH`
//! (`docs/research/zeromq.md` §12). So the vocabulary here is libzmq's and
//! not weida's: weida's `Error` is outcome-shaped for a protocol with
//! acknowledgements and guarantee sets, and ZMTP has neither
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4.2). Where the two meet — the calls this crate makes into
//! `weida-runtime` — the conversion is explicit and lives in
//! [`From<weida_core::Error>`](Error#impl-From<Error>).
//!
//! What libzmq does not have is a reason. `errno` is a number, and a caller
//! that wants to know *which* option was refused has to guess. Every error
//! here therefore carries a [`Cause`], and [`Display`](std::fmt::Display)
//! prints both: `EINVAL: inproc name must be 1..=256 bytes: ""`.

use std::borrow::Cow;
use std::fmt;

/// The result of any fallible `weida-zmq` operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What an error says beyond its errno.
///
/// A `Cow` so that the common case — a fixed sentence — costs no allocation,
/// while a message naming a value (a path, a length, an option) can be
/// formatted.
pub type Cause = Cow<'static, str>;

/// Why an operation failed, in libzmq's errno vocabulary.
///
/// The variants are spelled exactly as the C names, because that is what a
/// libzmq user knows, greps for and branches on; `rust-zmq` spells them the
/// same way for the same reason (`docs/research/zeromq.md` §13). Three of
/// them — `EFSM`, `ENOCOMPATPROTO`, `ETERM` and `EMTHREAD` — are libzmq's own
/// inventions rather than POSIX errnos.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// `EAGAIN`: the operation would have blocked. What `ZMQ_DONTWAIT` (and
    /// `try_send`/`try_recv`) reports on a full or empty queue, and what an
    /// expired `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` reports.
    EAGAIN(Cause),
    /// `EFSM`: the operation is not permitted in the socket's current state.
    /// REQ's strict send-then-receive alternation is the state machine that
    /// produces it (`docs/research/zeromq.md` §4.2).
    EFSM(Cause),
    /// `ETERM`: the context this socket belongs to has been terminated.
    /// Every operation on a socket of a terminated context reports it, which
    /// is how libzmq unblocks threads parked in `zmq_recv` at shutdown.
    ETERM(Cause),
    /// `EHOSTUNREACH`: the message cannot be routed — a ROUTER with
    /// `ZMQ_ROUTER_MANDATORY` and an unknown routing id, or a peer that is
    /// not connected.
    EHOSTUNREACH(Cause),
    /// `EMFILE`: the context's socket ceiling (`ZMQ_MAX_SOCKETS`, default
    /// 1023) is reached. libzmq's `zmq_socket()` reports the same errno at
    /// the same point (`docs/research/zeromq.md` §11).
    EMFILE(Cause),
    /// `EINVAL`: an endpoint, an option name or an option value is invalid —
    /// including an option this library refuses on purpose rather than
    /// silently ignoring (0013 §4.4).
    EINVAL(Cause),
    /// `EPROTONOSUPPORT`: the transport an endpoint names is not supported.
    /// `pgm`, `epgm`, `udp`, `ws`, `wss`, `vmci`, `tipc` and `vsock` are
    /// ZeroMQ transports this library does not implement, and saying so is
    /// better than pretending the string was malformed.
    EPROTONOSUPPORT(Cause),
    /// `ENOCOMPATPROTO`: the peer speaks something this socket cannot pair
    /// with — an incompatible ZMTP version, or a socket type that is not a
    /// legal peer of ours (`docs/research/zeromq.md` §4.1).
    ENOCOMPATPROTO(Cause),
    /// `EADDRINUSE`: the endpoint is already bound.
    EADDRINUSE(Cause),
    /// `EADDRNOTAVAIL`: the address cannot be bound on this host.
    EADDRNOTAVAIL(Cause),
    /// `ENOTSOCK`: the handle is not a socket any more — it was closed.
    ENOTSOCK(Cause),
    /// `EINTR`: the operation was interrupted.
    EINTR(Cause),
    /// `EMTHREAD`: libzmq's "no I/O thread available". Here it is the
    /// reactor: no ambient one where one was required, or the OS refused the
    /// threads an owned one asked for.
    EMTHREAD(Cause),
    /// `EMSGSIZE`: a message is larger than `ZMQ_MAXMSGSIZE` allows. Judged
    /// from the declared length, before any body is read.
    EMSGSIZE(Cause),
    /// `ENOTSUP`: the operation does not exist for this socket type — sending
    /// on a SUB, receiving on a PUB.
    ENOTSUP(Cause),
    /// `EIO`: the operating system reported something that is none of the
    /// above. The cause carries its text rather than flattening it away.
    EIO(Cause),
}

impl Error {
    /// The errno name, exactly as libzmq spells it.
    pub const fn errno(&self) -> &'static str {
        match self {
            Error::EAGAIN(_) => "EAGAIN",
            Error::EFSM(_) => "EFSM",
            Error::ETERM(_) => "ETERM",
            Error::EHOSTUNREACH(_) => "EHOSTUNREACH",
            Error::EMFILE(_) => "EMFILE",
            Error::EINVAL(_) => "EINVAL",
            Error::EPROTONOSUPPORT(_) => "EPROTONOSUPPORT",
            Error::ENOCOMPATPROTO(_) => "ENOCOMPATPROTO",
            Error::EADDRINUSE(_) => "EADDRINUSE",
            Error::EADDRNOTAVAIL(_) => "EADDRNOTAVAIL",
            Error::ENOTSOCK(_) => "ENOTSOCK",
            Error::EINTR(_) => "EINTR",
            Error::EMTHREAD(_) => "EMTHREAD",
            Error::EMSGSIZE(_) => "EMSGSIZE",
            Error::ENOTSUP(_) => "ENOTSUP",
            Error::EIO(_) => "EIO",
        }
    }

    /// Why it failed, in words.
    pub fn cause(&self) -> &str {
        match self {
            Error::EAGAIN(cause)
            | Error::EFSM(cause)
            | Error::ETERM(cause)
            | Error::EHOSTUNREACH(cause)
            | Error::EMFILE(cause)
            | Error::EINVAL(cause)
            | Error::EPROTONOSUPPORT(cause)
            | Error::ENOCOMPATPROTO(cause)
            | Error::EADDRINUSE(cause)
            | Error::EADDRNOTAVAIL(cause)
            | Error::ENOTSOCK(cause)
            | Error::EINTR(cause)
            | Error::EMTHREAD(cause)
            | Error::EMSGSIZE(cause)
            | Error::ENOTSUP(cause)
            | Error::EIO(cause) => cause,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.errno(), self.cause())
    }
}

impl std::error::Error for Error {}

/// The boundary conversion of [0013](../../../docs/decisions/0013-competitor-libraries.md)
/// §4.2: one OS-error vocabulary is better than two, so `weida-runtime`
/// reports `weida_core::Error` and this crate maps it to its own errno names
/// here — which it "would have to do regardless, because libzmq's
/// `EFSM`/`EAGAIN`/`ETERM`/`EHOSTUNREACH` vocabulary is not weida's and never
/// will be".
///
/// Only four of weida's outcomes can reach this crate, because only four
/// calls do: the reactor constructors report [`weida_core::Error::Runtime`],
/// the name registry reports [`weida_core::Error::InvalidAddress`] and
/// [`weida_core::Error::AlreadyRegistered`], and the socket helpers report
/// [`weida_core::Error::Io`]. Anything else is mapped to `EIO` with weida's
/// own text as the cause: no outcome is dropped on the floor, and a variant
/// that starts arriving will say so in the message rather than hide.
impl From<weida_core::Error> for Error {
    fn from(error: weida_core::Error) -> Error {
        match error {
            weida_core::Error::Runtime(why) => Error::EMTHREAD(Cow::Owned(why)),
            weida_core::Error::InvalidAddress(why) => Error::EINVAL(Cow::Owned(why)),
            weida_core::Error::AlreadyRegistered => {
                Error::EADDRINUSE(Cow::Borrowed("the endpoint is already bound"))
            }
            weida_core::Error::Io(io) => Error::from(io),
            other => Error::EIO(Cow::Owned(other.to_string())),
        }
    }
}

/// Maps the OS's own vocabulary, which libzmq passes through unchanged.
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        use std::io::ErrorKind;
        let cause = Cow::Owned(error.to_string());
        match error.kind() {
            ErrorKind::AddrInUse => Error::EADDRINUSE(cause),
            ErrorKind::AddrNotAvailable => Error::EADDRNOTAVAIL(cause),
            ErrorKind::Interrupted => Error::EINTR(cause),
            ErrorKind::WouldBlock => Error::EAGAIN(cause),
            ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::NotConnected
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable => Error::EHOSTUNREACH(cause),
            ErrorKind::InvalidInput | ErrorKind::InvalidData => Error::EINVAL(cause),
            _ => Error::EIO(cause),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: every variant names its own errno, and no two share a name.
    #[test]
    fn every_variant_names_its_errno() {
        let all = [
            Error::EAGAIN("".into()),
            Error::EFSM("".into()),
            Error::ETERM("".into()),
            Error::EHOSTUNREACH("".into()),
            Error::EMFILE("".into()),
            Error::EINVAL("".into()),
            Error::EPROTONOSUPPORT("".into()),
            Error::ENOCOMPATPROTO("".into()),
            Error::EADDRINUSE("".into()),
            Error::EADDRNOTAVAIL("".into()),
            Error::ENOTSOCK("".into()),
            Error::EINTR("".into()),
            Error::EMTHREAD("".into()),
            Error::EMSGSIZE("".into()),
            Error::ENOTSUP("".into()),
            Error::EIO("".into()),
        ];
        let mut names: Vec<&str> = all.iter().map(Error::errno).collect();
        assert!(names.iter().all(|name| name.starts_with('E')));
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "two variants share an errno name");
    }

    /// Claim: `Display` names the errno *and* the cause, which is the whole
    /// difference between this and a bare `errno`.
    #[test]
    fn display_names_the_errno_and_the_cause() {
        let err = Error::EFSM("a REQ socket must receive before it sends again".into());
        assert_eq!(
            err.to_string(),
            "EFSM: a REQ socket must receive before it sends again"
        );
        assert_eq!(err.errno(), "EFSM");
        assert_eq!(
            err.cause(),
            "a REQ socket must receive before it sends again"
        );
    }

    /// Claim: weida's outcomes arrive as libzmq errnos, and the text survives
    /// the crossing.
    #[test]
    fn weida_errors_map_onto_the_errno_vocabulary() {
        let runtime = Error::from(weida_core::Error::Runtime(
            "no ambient tokio runtime".into(),
        ));
        assert_eq!(runtime.errno(), "EMTHREAD");
        assert_eq!(runtime.cause(), "no ambient tokio runtime");

        let address = Error::from(weida_core::Error::InvalidAddress(
            "bus name too long".into(),
        ));
        assert_eq!(address.errno(), "EINVAL");
        assert_eq!(address.cause(), "bus name too long");

        assert_eq!(
            Error::from(weida_core::Error::AlreadyRegistered).errno(),
            "EADDRINUSE"
        );

        let io = Error::from(weida_core::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "address in use",
        )));
        assert_eq!(io.errno(), "EADDRINUSE");

        // An outcome that cannot arise on these paths is not silently
        // reshaped into a plausible errno: it becomes EIO and says what it
        // was.
        let unmapped = Error::from(weida_core::Error::Rejected);
        assert_eq!(unmapped.errno(), "EIO");
        assert!(!unmapped.cause().is_empty());
    }

    /// Claim: the OS's own errnos pass through with their meaning, because
    /// that is what libzmq does with them.
    #[test]
    fn os_errors_keep_their_meaning() {
        use std::io::ErrorKind;
        for (kind, errno) in [
            (ErrorKind::AddrInUse, "EADDRINUSE"),
            (ErrorKind::AddrNotAvailable, "EADDRNOTAVAIL"),
            (ErrorKind::Interrupted, "EINTR"),
            (ErrorKind::WouldBlock, "EAGAIN"),
            (ErrorKind::ConnectionRefused, "EHOSTUNREACH"),
            (ErrorKind::PermissionDenied, "EIO"),
        ] {
            let err = Error::from(std::io::Error::new(kind, "boom"));
            assert_eq!(err.errno(), errno, "{kind:?}");
        }
    }
}
