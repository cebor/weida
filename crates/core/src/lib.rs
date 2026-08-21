//! I/O-free core model of the weida messaging framework.
//!
//! This crate contains no networking, no `tokio` and no serialization: only the
//! vocabulary types (identifiers, endpoint addresses, policies, limits, trace
//! context, errors) and the pure state machines that decide transfer outcomes.
//! Everything here is unit-testable without a socket, per Phase 1 of the master
//! architecture document.

pub mod addr;
pub mod error;
pub mod id;
pub mod limits;
pub mod policy;
pub mod state;
pub mod trace;

pub use addr::{EndpointAddr, MAX_PATH_BYTES, SCHEME, validate_endpoint_path};
pub use error::{Error, ErrorCode, Result, StopReason};
pub use id::TransferId;
pub use limits::Limits;
pub use policy::{AckMode, AckState, Outcome, Role};
pub use state::{
    Correlator, RecvAction, RecvEvent, RecvMachine, RecvState, ReplyDisposition, SendAction,
    SendEvent, SendMachine, SendState,
};
pub use trace::{TraceContext, TraceError};
