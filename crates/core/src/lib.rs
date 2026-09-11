//! I/O-free core model of the weida messaging framework.
//!
//! This crate contains no networking, no `tokio` and no serialization: only the
//! vocabulary types (endpoint addresses, limits, trace context, errors) that
//! the wire codec and the transport share. Everything here is unit-testable
//! without a socket.
//!
//! The correlation and acknowledgement state machines that used to live here
//! are gone: Req/Rep rides one bidirectional QUIC stream, so the stream *is*
//! the correlation, and delivery is QUIC's own transport receipt rather than an
//! application-level ACK (`docs/ARCHITECTURE.md`, layer model).

pub mod addr;
pub mod error;
pub mod identity;
pub mod limits;
pub mod trace;

pub use addr::{
    Address, EndpointAddr, InprocAddr, MAX_BUS_BYTES, MAX_PATH_BYTES, MAX_SOCKET_PATH_BYTES,
    SCHEME, SCHEME_INPROC, SCHEME_UNIX, UnixAddr, validate_endpoint_path,
};
pub use error::{Error, ErrorCode, LossCause, Result, StopReason};
pub use identity::Fingerprint;
pub use limits::Limits;
pub use trace::{TraceContext, TraceError};
