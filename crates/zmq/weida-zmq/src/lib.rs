//! A ZeroMQ implementation in Rust, usable with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements ZeroMQ for
//! applications that speak ZeroMQ, rather than bridging ZeroMQ into weida.
//! Nothing of weida's protocol is in it and nothing of it is in weida — the
//! two meet only in a forwarder that terminates both, which is a third crate
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4, §4.5).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver, `AF_UNIX` hygiene and name registry, and `weida-core`'s
//! `Error`/`LocalPrincipal` vocabulary at the boundary. `[dependencies]`
//! therefore names `weida-runtime` and `weida-core` and **never** `weida` or
//! `weida-protocol`: a ZeroMQ user must not link quinn, rustls and weida's
//! pattern layer to open a `tcp://` socket, and the dependency direction is
//! what keeps a ZeroMQ socket from reaching weida's types at all [0013 §4.2].
//!
//! # What exists so far
//!
//! - [`Context`] — the reactor, the `inproc://` namespace and the socket
//!   ceiling, created three ways ([`Context::new`], [`Context::with_handle`],
//!   [`Context::owned`]) and terminated with a **finite** close budget.
//! - [`Endpoint`] — `tcp://`, `ipc://` and `inproc://` parsed under libzmq's
//!   own length rules, with every other ZeroMQ transport named absent rather
//!   than called a typo.
//! - [`Message`] and [`Multipart`] — a frame and a whole message, the latter
//!   handed over as one value because ZMTP delivers "all frames or none", with
//!   `ZMQ_MAXMSGSIZE` judged from the declared length before a body is
//!   allocated.
//! - [`Pipe`] and [`Queue`] — the per-peer double queue every pattern RFC
//!   specifies, bounded by `ZMQ_SNDHWM`/`ZMQ_RCVHWM` in messages, with
//!   [`MuteAction`] carrying `zmq_socket(3)`'s "Action in mute state" column
//!   so that no socket type re-decides it.
//! - [`Error`] — libzmq's errno vocabulary, each error carrying a cause,
//!   with weida's `Error` converted at the boundary and never re-exported.
//!
//! The socket types — `ReqSocket`, `RepSocket`, `DealerSocket`,
//! `RouterSocket`, `PubSocket`, `SubSocket`, `XPubSocket`, `XSubSocket`,
//! `PushSocket`, `PullSocket`, `PairSocket` — are the following slices, as
//! are the connection engine with its reconnect, security and authorization,
//! and the option surface [0013 §5.3]. They are named here so that a reader
//! knows what this crate is for and what it does not do yet; nothing stands
//! in for them.
//!
//! # Rules this crate keeps
//!
//! - **Options are honoured or refused, never silently ignored** [0013 §4.4
//!   item 4]. A configuration this library cannot deliver fails where it is
//!   configured, with `EINVAL` and a message naming the value.
//! - **Two defaults deliberately differ from libzmq** [0013 §4.4 item 5]:
//!   the close budget is finite where `ZMQ_LINGER` is infinite (see
//!   [`DEFAULT_CLOSE_BUDGET`]), and `ZMQ_MAXMSGSIZE` has a real default where
//!   libzmq has "no limit" (see [`DEFAULT_MAX_MESSAGE_SIZE`]). Both are
//!   settable back; neither is silent.
//! - **Identity types stay apart** [0013 §4.4 item 6]. A ZeroMQ identity —
//!   a CURVE key, a routing id, a ZAP user id — and weida's proved
//!   `Fingerprint` are different claims about different things, so no `From`,
//!   `Into`, `AsRef` or `Deref` between the two groups exists in this
//!   workspace, and none may be added. `weida_core::LocalPrincipal` is
//!   shared, because `ipc://` peer credentials are the same kernel fact, and
//!   it is not a ZAP identity.
//! - **No remote input causes unbounded allocation** (`docs/INVARIANTS.md`),
//!   under libzmq's own names: `ZMQ_MAX_SOCKETS` here, `ZMQ_SNDHWM`,
//!   `ZMQ_RCVHWM`, `ZMQ_MAXMSGSIZE` and `ZMQ_BACKLOG` in the slices that
//!   introduce them.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from `docs/research/zeromq.md`,
//! which cites libzmq 4.3.x's manual pages, 37/ZMTP and the zguide. Section
//! references in this crate's documentation (`§2`, `§11`) are that sheet's.

#![warn(missing_docs)]

pub mod context;
pub mod endpoint;
pub mod error;
pub mod message;
pub mod pipe;

pub use context::{
    Context, ContextConfig, DEFAULT_CLOSE_BUDGET, DEFAULT_MAX_SOCKETS, InprocDial, SocketId,
    SocketSlot, Terminated,
};
pub use endpoint::{Endpoint, MAX_INPROC_NAME_BYTES, MAX_IPC_ENDPOINT_BYTES, TcpHost};
pub use error::{Cause, Error, Result};
pub use message::{DEFAULT_MAX_MESSAGE_SIZE, Decoded, Message, Multipart};
pub use pipe::{
    DEFAULT_RCVHWM, DEFAULT_SNDHWM, MuteAction, Pipe, PipeConfig, Queue, QueueConfig, Sent,
};
