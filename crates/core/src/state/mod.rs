//! The core state machines.
//!
//! Master doc §64 and §16 of the agent rules: essential state machines must be
//! small, explicit and independently testable. Nothing here performs I/O; each
//! machine consumes events and returns the effects the transport must apply, so
//! the outcome rules in `docs/FAILURE_MODEL.md` are decided in one place and
//! tested without a socket.

pub mod correlation;
pub mod recv;
pub mod send;

pub use correlation::{Correlator, ReplyDisposition};
pub use recv::{RecvAction, RecvEvent, RecvMachine, RecvState};
pub use send::{SendAction, SendEvent, SendMachine, SendState};
