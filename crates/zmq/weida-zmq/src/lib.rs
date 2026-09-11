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
//! - [`Engine`] and [`SocketOptions`] — one socket's binds, connects and
//!   reconnects, with `ZMQ_RECONNECT_IVL`/`_IVL_MAX` backoff,
//!   `ZMQ_HANDSHAKE_IVL`, `ZMQ_CONNECT_TIMEOUT`, `ZMQ_IMMEDIATE`,
//!   `ZMQ_BACKLOG` and `ZMQ_LAST_ENDPOINT`, handing each established
//!   connection to a [`Session`].
//! - [`ZmtpSession`] and [`Wire`] — the protocol itself over one connection:
//!   the greeting with its 3.0 downgrade, the NULL handshake, `READY` with
//!   `Socket-Type` and `Identity`, MORE/COMMAND framing, `PING`/`PONG` gated
//!   on the negotiated version, and `ERROR` sent and understood. Everything
//!   byte-exact in it is `weida-zmtp`'s.
//! - [`RoutingId`] — 1-255 self-asserted bytes with a nonzero first octet,
//!   and the type-level distance from weida's proved identities.
//! - [`SocketCore`] — what every socket type shares: the engine, the
//!   round-robin and fair-queue decisions, the `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`
//!   bound, and libzmq's thread rule as a type (`Send + !Sync`).
//! - [`ReqSocket`] and [`RepSocket`] — request-reply with its state machine:
//!   strict alternation reported as `EFSM`, the envelope prepended and
//!   stripped, round-robin out and last-peer in, a reply to a vanished
//!   requester discarded rather than blocking, `ZMQ_REQ_CORRELATE` and
//!   `ZMQ_REQ_RELAXED`.
//! - [`DealerSocket`] and [`RouterSocket`] — the same pattern without the
//!   lockstep: unrestricted in both directions, ROUTER addressing peers by
//!   the [`RoutingKey`] a peer announced or one it was given, with
//!   `ZMQ_ROUTER_MANDATORY`, `ZMQ_ROUTER_HANDOVER` and `ZMQ_PROBE_ROUTER`.
//! - [`PushSocket`] and [`PullSocket`] — the pipeline: round-robin over the
//!   workers with room, blocking rather than discarding, fair-queued at the
//!   sink.
//! - [`PairSocket`] — the exclusive pair: one peer, no auto-reconnect, and a
//!   further incoming connection terminated while one is live.
//! - [`Error`] — libzmq's errno vocabulary, each error carrying a cause,
//!   with weida's `Error` converted at the boundary and never re-exported.
//!
//! The remaining socket types — `PubSocket`, `SubSocket`, `XPubSocket`,
//! `XSubSocket` — are the following slices, as are security and
//! authorization and the rest of the option surface [0013 §5.3]. They are
//! named here so that a reader knows what this crate is for and what it does
//! not do yet; nothing stands in for them, and in particular there is no
//! default [`Session`] beyond [`ZmtpSession`]: a socket that handed its peers
//! to a no-op would be a ZeroMQ implementation that speaks nothing.
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
pub mod dealerrouter;
pub mod endpoint;
pub mod engine;
pub mod error;
pub mod identity;
pub mod message;
pub mod options;
pub mod pair;
pub mod pipe;
pub mod pipeline;
pub mod reqrep;
pub mod session;
pub mod socket;
pub mod transport;

pub use context::{
    Context, ContextConfig, DEFAULT_CLOSE_BUDGET, DEFAULT_MAX_SOCKETS, InprocDial, SocketId,
    SocketSlot, Terminated,
};
pub use dealerrouter::{DealerSocket, RouterSocket};
pub use endpoint::{Endpoint, MAX_INPROC_NAME_BYTES, MAX_IPC_ENDPOINT_BYTES, TcpHost};
pub use engine::{
    AnnouncedIdentity, Connection, Discarded, Engine, HandshakeGate, Peer, PeerId, Role, Session,
    SessionFuture,
};
pub use error::{Cause, Error, Result};
pub use identity::{MAX_ROUTING_ID_BYTES, RoutingId, RoutingKey};
pub use message::{
    DEFAULT_MAX_MESSAGE_FRAMES, DEFAULT_MAX_MESSAGE_SIZE, Decoded, Message, MessageLimits,
    Multipart,
};
pub use options::{
    DEFAULT_BACKLOG, DEFAULT_HANDSHAKE_IVL, DEFAULT_MAX_RESOLVED_ADDRESSES, DEFAULT_RECONNECT_IVL,
    SocketOptions,
};
pub use pair::PairSocket;
pub use pipe::{
    DEFAULT_RCVHWM, DEFAULT_SNDHWM, MuteAction, Pipe, PipeConfig, Queue, QueueConfig, Sent,
};
pub use pipeline::{PullSocket, PushSocket};
pub use reqrep::{RepSocket, ReqSocket};
pub use session::{Incoming, Negotiated, Wire, ZmtpSession};
pub use socket::{Delivered, SocketCore};
pub use transport::Stream;
