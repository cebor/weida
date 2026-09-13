//! weida's failure vocabulary, as this module's exception classes.
//!
//! `weida.Untrusted`, `weida.UnknownEndpoint`, `weida.Indeterminate` and
//! eighteen more, each its own class under `weida.WeidaError`, each instance
//! carrying `errno` and `cause`. A caller writes the branch the failure model
//! is made of — `except weida.Indeterminate` for "the outcome is genuinely
//! unknown", `except weida.Rejected` for "the peer said no" — instead of
//! matching on the text of a `RuntimeError`.
//!
//! **The table cannot drift.** [`name_of`] is a `match` over
//! [`weida_core::Error`] written by the same macro that writes the name list,
//! so a variant added to the library that is not added here is a compile
//! error in this file rather than a failure that silently arrives as the base
//! class.
//!
//! One class is not like the others, and the difference is the point of
//! [FAILURE_MODEL.md](../../../../docs/FAILURE_MODEL.md):
//! `weida.Indeterminate` means the transfer may or may not have arrived, and
//! a caller that treats it as a definite failure is wrong. It is a sibling of
//! `ConnectionLost` rather than a kind of it, exactly as
//! `Error::is_definite_failure()` keeps it out of the definite set.

use pyo3::prelude::*;
use weida_core::Error;
use weida_py_core::{Errno, ErrorClasses};

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every failure of this module derives from.
const BASE: &str = "WeidaError";

/// Writes the name list and the exhaustive match from one source.
macro_rules! failures {
    ($($name:ident),+ $(,)?) => {
        /// Every failure name this module has a class for.
        pub(crate) const NAMES: &[&str] = &[$(stringify!($name)),+];

        /// The name of one error, checked against the library's enum by the
        /// compiler.
        ///
        /// `{ .. }` matches a unit, tuple and struct variant alike, so one
        /// arm shape serves all three.
        fn name_of(error: &Error) -> &'static str {
            match error {
                $(Error::$name { .. } => stringify!($name),)+
            }
        }
    };
}

failures!(
    Runtime,
    InvalidAddress,
    InvalidEndpointPath,
    InvalidFingerprint,
    AlreadyRegistered,
    NotConnected,
    ConnectionLost,
    Negotiation,
    Protocol,
    Rejected,
    UnknownEndpoint,
    Unsupported,
    NoParkedConnection,
    NoReply,
    Canceled,
    Indeterminate,
    LimitExceeded,
    Tls,
    Untrusted,
    Io,
    Transport,
);

/// Adds the base class and one class per failure name to the module.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    ERRORS.install(module, BASE, NAMES)
}

/// The name and cause of one library error, which
/// [`weida_py_core::Errno`] carries across the bridge.
///
/// `Display` rather than a hand-written message per variant: the library's
/// own `Display` is where a failure's wording lives, and a binding that
/// rephrased it would be a second vocabulary to keep in step.
pub fn errno_of(error: Error) -> Errno {
    Errno::new(name_of(&error), error.to_string())
}

/// The module's error class for one errno, which is also the
/// [`weida_py_core::ErrnoMapper`] the bridge is built with — a plain function
/// pointer, because the classes live in a `static`.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    ERRORS.error(py, errno)
}

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}
