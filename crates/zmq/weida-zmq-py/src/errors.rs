//! libzmq's errno vocabulary, as this module's exception classes.
//!
//! `weida_zmq.EAGAIN`, `weida_zmq.EFSM`, `weida_zmq.ETERM` and sixteen more,
//! each its own class under `weida_zmq.ZmqError`, each instance carrying
//! `errno` and `cause`. A caller writes the branch the zguide's recipes are
//! made of — `except EAGAIN` for a timeout, `except EFSM` for a REQ socket out
//! of step — instead of matching on the text of a `RuntimeError`.
//!
//! **The table cannot drift.** [`name_of`] is a `match` over
//! [`weida_zmq::Error`] written by the same macro that writes the name list,
//! so a variant added to the library that is not added here is a compile
//! error in this file rather than an errno that silently arrives as the base
//! class.

use pyo3::prelude::*;
use weida_py_core::{Errno, ErrorClasses};
use weida_zmq::Error;

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every errno of this module derives from.
const BASE: &str = "ZmqError";

/// Writes the errno list and the exhaustive match from one source.
macro_rules! errnos {
    ($($name:ident),+ $(,)?) => {
        /// Every errno name this module has a class for.
        pub(crate) const NAMES: &[&str] = &[$(stringify!($name)),+];

        /// The errno name of one error, checked against the library's enum by
        /// the compiler.
        fn name_of(error: &Error) -> &'static str {
            match error {
                $(Error::$name(_) => stringify!($name),)+
            }
        }
    };
}

errnos!(
    EAGAIN,
    EFSM,
    ETERM,
    EHOSTUNREACH,
    EMFILE,
    EINVAL,
    EPROTONOSUPPORT,
    ENOCOMPATPROTO,
    EACCES,
    EADDRINUSE,
    EADDRNOTAVAIL,
    ENOTSOCK,
    EINTR,
    EMTHREAD,
    EMSGSIZE,
    ENOTSUP,
    ETIMEDOUT,
    ENOENT,
    EIO,
);

/// Creates the classes and adds them to the module. Called once, at import.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    ERRORS.install(module, BASE, NAMES)
}

/// The protocol-neutral form the bridge carries: the errno name and why.
///
/// Called on a reactor thread, with no GIL held — which is why it does not
/// build the exception itself.
pub fn errno_of(error: Error) -> Errno {
    Errno::new(name_of(&error), error.cause())
}

/// Turns an [`Errno`] into this module's exception. The
/// [`ErrnoMapper`](weida_py_core::ErrnoMapper) every [`Bridge`] here holds.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    ERRORS.error(py, errno)
}

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}
