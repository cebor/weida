//! The error vocabulary: NNG's own `NNG_E*` names, each with a cause.
//!
//! An NNG application branches on the value `nng_dial()` or `nng_sendmsg()`
//! returned, and the manual names those values rather than numbering them:
//! a protocol-state violation is `NNG_ESTATE`, an expired survey or send
//! deadline is `NNG_ETIMEDOUT`, a closed socket is `NNG_ECLOSED`, a message
//! over `NNG_OPT_RECVMAXSZ` is `NNG_EMSGSIZE`, a refused dial is
//! `NNG_ECONNREFUSED` (`docs/research/nanomsg-nng.md` §4, §8). So the
//! vocabulary here is NNG's and not weida's: weida's `Error` is
//! outcome-shaped for a protocol with acknowledgements and guarantee sets,
//! and SP has neither — "SP has no transport-independent application
//! acknowledgement" (§6) — and the conversion at the boundary is explicit
//! and lives in [`From<weida_core::Error>`](Error#impl-From<Error>)
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4.2).
//!
//! **Where the `NNG_` prefix lives.** The variants are spelled without it —
//! `Error::ESTATE`, the way `weida-zmq` spells libzmq's `EFSM` — and
//! [`Error::name`] puts it back, so a log line reads `NNG_ESTATE: ...` and a
//! `match` arm reads `Error::ESTATE(_)`. One name, printed the way NNG
//! prints it and matched the way Rust matches.
//!
//! **What NNG does not have is a reason.** `nng_strerror()` renders a fixed
//! sentence per code, so an application that wants to know *which* option
//! was refused has to guess. Every error here carries a [`Cause`] and
//! [`Display`](std::fmt::Display) prints both:
//! `NNG_EADDRINVAL: an endpoint must be transport://address: "orders"`.

use std::borrow::Cow;
use std::fmt;

/// The result of any fallible `weida-nng` operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What an error says beyond its NNG name.
///
/// A `Cow` so that the common case — a fixed sentence — costs no allocation,
/// while a message naming a value (a URL, a length, an option) can be
/// formatted.
pub type Cause = Cow<'static, str>;

/// Why an operation failed, in NNG's own error vocabulary.
///
/// The set is what this library can produce, not all of `nng_errno.h`:
/// `NNG_ECRYPTO`, `NNG_EAMBIGUOUS`, `NNG_EBADTYPE` and the rest describe
/// situations no path here reaches, and an error nothing returns is an error
/// nobody can handle. Where NNG has no name for something this library must
/// report, the variant's own documentation says so and says which name was
/// borrowed instead.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// `NNG_ESTATE`: the operation is not permitted in the current protocol
    /// state. A cooked REP that sends before receiving, a cooked REQ that
    /// receives without an outstanding request, a surveyor receiving with no
    /// active survey (`docs/research/nanomsg-nng.md` §4).
    ESTATE(Cause),
    /// `NNG_ETIMEDOUT`: a bounded wait ran out — a send with nowhere to go, a
    /// receive under `NNG_OPT_RECVTIMEO`, a survey past its `SURVEYTIME`
    /// (§4, §5).
    ETIMEDOUT(Cause),
    /// `NNG_ECLOSED`: the socket, pipe, context or dialer is closed. Every
    /// operation on a closed object reports it, which is how a thread parked
    /// in a receive is unblocked at shutdown.
    ECLOSED(Cause),
    /// `NNG_EMSGSIZE`: a message is larger than `NNG_OPT_RECVMAXSZ` allows.
    /// Judged from the declared 64-bit length, before any body is read (§3,
    /// §11).
    EMSGSIZE(Cause),
    /// `NNG_ECONNREFUSED`: nothing was listening. What a synchronous
    /// `nng_dial()` reports for a refused first connection, where a
    /// non-blocking one instead starts retrying (§1).
    ECONNREFUSED(Cause),
    /// `NNG_ECONNABORTED`: the connection was aborted before it was usable.
    ECONNABORTED(Cause),
    /// `NNG_ECONNRESET`: the peer reset the connection. In SP this is also
    /// how a peer says "no": the mapping has no refusal frame, so a close is
    /// the whole error report (§6).
    ECONNRESET(Cause),
    /// `NNG_EUNREACHABLE`: there is no route to the peer, or no peer at all
    /// to route to.
    EUNREACHABLE(Cause),
    /// `NNG_EADDRINUSE`: the address is already bound. A second listener on
    /// one `inproc://` name or one `ipc://` path.
    EADDRINUSE(Cause),
    /// `NNG_EADDRINVAL`: the URL is not one this library can use — a
    /// malformed `transport://address`, an over-long address against
    /// [`NNG_MAXADDRLEN`](crate::endpoint::NNG_MAXADDRLEN), an `ipc://` path
    /// past its budget (§11).
    EADDRINVAL(Cause),
    /// `NNG_ENOTSUP`: the operation does not exist here — a send on a SUB
    /// socket, a receive on a PUSH, a context on a raw socket whose state is
    /// deliberately absent (§2, §4), or a transport SP defines and this
    /// library does not implement.
    ENOTSUP(Cause),
    /// `NNG_EINVAL`: an argument or an option value is invalid, including an
    /// option this library refuses on purpose rather than silently ignoring
    /// (0013 §4.4 item 4).
    EINVAL(Cause),
    /// `NNG_EAGAIN`: the operation would have blocked, and the caller asked
    /// not to. What a non-blocking send or receive reports on a full or empty
    /// queue.
    EAGAIN(Cause),
    /// `NNG_EPROTO`: a protocol error. A peer whose SP protocol header has
    /// the wrong magic, version or reserved field, or whose endpoint type may
    /// not pair with ours — in which case the connection is closed and
    /// nothing is sent back, because SP defines no way to say more (§3, §6).
    EPROTO(Cause),
    /// `NNG_EPEERAUTH`: the peer could not be authenticated. TLS
    /// verification failed, or a pipe-add-pre hook refused the peer (§8,
    /// §10).
    EPEERAUTH(Cause),
    /// `NNG_EPERM`: the operating system refused permission — an `ipc://`
    /// path whose directory is not writable, a socket file owned by somebody
    /// else.
    EPERM(Cause),
    /// `NNG_ENOENT`: the object named does not exist. An endpoint this socket
    /// never dialled or bound, or a filesystem path that is not there.
    ENOENT(Cause),
    /// `NNG_EREADONLY`: the option cannot be set, only read.
    /// `NNG_OPT_LOCADDR` and `NNG_OPT_REMADDR` are facts about a pipe, not
    /// settings on it (§10, §11).
    EREADONLY(Cause),
    /// `NNG_EWRITEONLY`: the option cannot be read, only set. A private key
    /// is configuration going in and never comes back out.
    EWRITEONLY(Cause),
    /// `NNG_ECANCELED`: the operation was aborted because the object it ran
    /// on was closed under it.
    ECANCELED(Cause),
    /// `NNG_EINTR`: the operation was interrupted.
    EINTR(Cause),
    /// `NNG_ENOFILES`: a resource ceiling was reached. NNG uses it for "too
    /// many open files"; here it is also the answer at this library's own
    /// ceilings — the sockets one [`Context`](crate::Context) admits and the
    /// pipes one socket admits — because SP bounds nothing a stranger opens
    /// and the bound has to be ours (`docs/INVARIANTS.md`).
    ENOFILES(Cause),
    /// `NNG_ESYSERR`: the operating system reported something that is none of
    /// the above. NNG's own catch-all, and the cause carries the OS's text
    /// rather than flattening it away.
    ESYSERR(Cause),
}

impl Error {
    /// The error's name, exactly as NNG spells it, `NNG_` prefix included.
    pub const fn name(&self) -> &'static str {
        match self {
            Error::ESTATE(_) => "NNG_ESTATE",
            Error::ETIMEDOUT(_) => "NNG_ETIMEDOUT",
            Error::ECLOSED(_) => "NNG_ECLOSED",
            Error::EMSGSIZE(_) => "NNG_EMSGSIZE",
            Error::ECONNREFUSED(_) => "NNG_ECONNREFUSED",
            Error::ECONNABORTED(_) => "NNG_ECONNABORTED",
            Error::ECONNRESET(_) => "NNG_ECONNRESET",
            Error::EUNREACHABLE(_) => "NNG_EUNREACHABLE",
            Error::EADDRINUSE(_) => "NNG_EADDRINUSE",
            Error::EADDRINVAL(_) => "NNG_EADDRINVAL",
            Error::ENOTSUP(_) => "NNG_ENOTSUP",
            Error::EINVAL(_) => "NNG_EINVAL",
            Error::EAGAIN(_) => "NNG_EAGAIN",
            Error::EPROTO(_) => "NNG_EPROTO",
            Error::EPEERAUTH(_) => "NNG_EPEERAUTH",
            Error::EPERM(_) => "NNG_EPERM",
            Error::ENOENT(_) => "NNG_ENOENT",
            Error::EREADONLY(_) => "NNG_EREADONLY",
            Error::EWRITEONLY(_) => "NNG_EWRITEONLY",
            Error::ECANCELED(_) => "NNG_ECANCELED",
            Error::EINTR(_) => "NNG_EINTR",
            Error::ENOFILES(_) => "NNG_ENOFILES",
            Error::ESYSERR(_) => "NNG_ESYSERR",
        }
    }

    /// Why it failed, in words.
    pub fn cause(&self) -> &str {
        match self {
            Error::ESTATE(cause)
            | Error::ETIMEDOUT(cause)
            | Error::ECLOSED(cause)
            | Error::EMSGSIZE(cause)
            | Error::ECONNREFUSED(cause)
            | Error::ECONNABORTED(cause)
            | Error::ECONNRESET(cause)
            | Error::EUNREACHABLE(cause)
            | Error::EADDRINUSE(cause)
            | Error::EADDRINVAL(cause)
            | Error::ENOTSUP(cause)
            | Error::EINVAL(cause)
            | Error::EAGAIN(cause)
            | Error::EPROTO(cause)
            | Error::EPEERAUTH(cause)
            | Error::EPERM(cause)
            | Error::ENOENT(cause)
            | Error::EREADONLY(cause)
            | Error::EWRITEONLY(cause)
            | Error::ECANCELED(cause)
            | Error::EINTR(cause)
            | Error::ENOFILES(cause)
            | Error::ESYSERR(cause) => cause,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name(), self.cause())
    }
}

impl std::error::Error for Error {}

/// The boundary conversion of [0013](../../../docs/decisions/0013-competitor-libraries.md)
/// §4.2: one OS-error vocabulary is better than two, so `weida-runtime`
/// reports `weida_core::Error` and this crate maps it to NNG's names here —
/// which it would have to do regardless, because
/// `NNG_ESTATE`/`NNG_EMSGSIZE`/`NNG_ECLOSED` is not weida's vocabulary and
/// never will be.
///
/// Only four of weida's outcomes can reach this crate, because only four
/// calls do: the reactor constructors report [`weida_core::Error::Runtime`],
/// the name registry reports [`weida_core::Error::InvalidAddress`] and
/// [`weida_core::Error::AlreadyRegistered`], and the `AF_UNIX` helpers report
/// [`weida_core::Error::Io`]. Anything else becomes `NNG_ESYSERR` carrying
/// weida's own text: no outcome is dropped on the floor, and a variant that
/// starts arriving says so in the message rather than hiding.
impl From<weida_core::Error> for Error {
    fn from(error: weida_core::Error) -> Error {
        match error {
            // No reactor, or the OS refused the threads an owned one asked
            // for. NNG's nearest name is the resource ceiling: nothing can
            // run without a reactor.
            weida_core::Error::Runtime(why) => Error::ENOFILES(Cow::Owned(why)),
            weida_core::Error::InvalidAddress(why) => Error::EADDRINVAL(Cow::Owned(why)),
            weida_core::Error::AlreadyRegistered => {
                Error::EADDRINUSE(Cow::Borrowed("the address is already bound"))
            }
            weida_core::Error::Io(io) => Error::from(io),
            other => Error::ESYSERR(Cow::Owned(other.to_string())),
        }
    }
}

/// Maps the OS's own vocabulary onto NNG's, which is what NNG's transports do
/// internally before an application ever sees a code.
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        use std::io::ErrorKind;
        let cause = Cow::Owned(error.to_string());
        match error.kind() {
            ErrorKind::ConnectionRefused => Error::ECONNREFUSED(cause),
            ErrorKind::ConnectionAborted => Error::ECONNABORTED(cause),
            ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::NotConnected => {
                Error::ECONNRESET(cause)
            }
            ErrorKind::HostUnreachable | ErrorKind::NetworkUnreachable => {
                Error::EUNREACHABLE(cause)
            }
            ErrorKind::AddrInUse => Error::EADDRINUSE(cause),
            ErrorKind::AddrNotAvailable => Error::EADDRINVAL(cause),
            ErrorKind::PermissionDenied => Error::EPERM(cause),
            ErrorKind::NotFound => Error::ENOENT(cause),
            ErrorKind::TimedOut => Error::ETIMEDOUT(cause),
            ErrorKind::WouldBlock => Error::EAGAIN(cause),
            ErrorKind::Interrupted => Error::EINTR(cause),
            ErrorKind::InvalidInput | ErrorKind::InvalidData => Error::EINVAL(cause),
            _ => Error::ESYSERR(cause),
        }
    }
}

/// What a peer's malformed SP protocol header is: `NNG_EPROTO`, and the close
/// that follows it is the whole error report (§3, §6).
impl From<weida_sp::HeaderError> for Error {
    fn from(error: weida_sp::HeaderError) -> Error {
        Error::EPROTO(Cow::Owned(error.to_string()))
    }
}

/// A message the framing refused. An over-large declaration is
/// `NNG_EMSGSIZE`, which is what `NNG_OPT_RECVMAXSZ` produces; a short read
/// is not an error a caller ever sees, because the session loops on it, so it
/// arrives here only if one escapes and is reported as the connection error
/// it then is.
impl From<weida_sp::MessageError> for Error {
    fn from(error: weida_sp::MessageError) -> Error {
        match error {
            weida_sp::MessageError::BodyTooLarge { .. } => {
                Error::EMSGSIZE(Cow::Owned(error.to_string()))
            }
            weida_sp::MessageError::Incomplete => Error::ECONNRESET(Cow::Owned(format!(
                "{error}: the peer closed before the message was complete"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: every variant prints NNG's own name with the `NNG_` prefix and
    /// its cause, because a log line that says only `NNG_EINVAL` does not say
    /// which option was refused.
    #[test]
    fn every_error_prints_its_nng_name_and_its_cause() {
        let all = [
            Error::ESTATE("a".into()),
            Error::ETIMEDOUT("a".into()),
            Error::ECLOSED("a".into()),
            Error::EMSGSIZE("a".into()),
            Error::ECONNREFUSED("a".into()),
            Error::ECONNABORTED("a".into()),
            Error::ECONNRESET("a".into()),
            Error::EUNREACHABLE("a".into()),
            Error::EADDRINUSE("a".into()),
            Error::EADDRINVAL("a".into()),
            Error::ENOTSUP("a".into()),
            Error::EINVAL("a".into()),
            Error::EAGAIN("a".into()),
            Error::EPROTO("a".into()),
            Error::EPEERAUTH("a".into()),
            Error::EPERM("a".into()),
            Error::ENOENT("a".into()),
            Error::EREADONLY("a".into()),
            Error::EWRITEONLY("a".into()),
            Error::ECANCELED("a".into()),
            Error::EINTR("a".into()),
            Error::ENOFILES("a".into()),
            Error::ESYSERR("a".into()),
        ];
        for error in &all {
            assert!(
                error.name().starts_with("NNG_E"),
                "{} is not an NNG name",
                error.name()
            );
            assert_eq!(error.cause(), "a");
            assert_eq!(error.to_string(), format!("{}: a", error.name()));
        }
        let names: std::collections::BTreeSet<&str> = all.iter().map(Error::name).collect();
        assert_eq!(names.len(), all.len(), "two variants share one NNG name");
    }

    /// Claim: weida's four reachable outcomes each land on the NNG name that
    /// describes them, and an outcome this crate does not expect keeps its
    /// text rather than vanishing.
    #[test]
    fn weidas_outcomes_map_onto_nngs_names() {
        assert!(matches!(
            Error::from(weida_core::Error::Runtime("no reactor".into())),
            Error::ENOFILES(_)
        ));
        assert!(matches!(
            Error::from(weida_core::Error::InvalidAddress("bad".into())),
            Error::EADDRINVAL(_)
        ));
        assert!(matches!(
            Error::from(weida_core::Error::AlreadyRegistered),
            Error::EADDRINUSE(_)
        ));
        assert!(matches!(
            Error::from(weida_core::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "nobody home"
            ))),
            Error::ECONNREFUSED(_)
        ));
        let unexpected = Error::from(weida_core::Error::Protocol("something new".into()));
        assert!(matches!(unexpected, Error::ESYSERR(_)));
        assert!(unexpected.cause().contains("something new"));
    }

    /// Claim: a malformed protocol header is `NNG_EPROTO` and an over-large
    /// declared length is `NNG_EMSGSIZE`, which are the two codes an SP peer's
    /// bytes can produce before any application sees them.
    #[test]
    fn the_codecs_refusals_carry_nngs_names() {
        let bad_magic = weida_sp::ProtocolHeader::decode(b"XXXX0000").expect_err("bad magic");
        assert!(matches!(Error::from(bad_magic), Error::EPROTO(_)));

        let too_big = weida_sp::message::decode(&[0xFF; 8], 1024).expect_err("too large");
        let mapped = Error::from(too_big);
        assert!(matches!(mapped, Error::EMSGSIZE(_)));
        assert!(mapped.cause().contains("1024"));
    }
}
