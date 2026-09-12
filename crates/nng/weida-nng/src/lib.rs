//! An implementation of the nanomsg Scalability Protocols in Rust, usable
//! with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements SP for
//! applications that speak SP, rather than bridging SP into weida. Nothing
//! of weida's protocol is in it and nothing of it is in weida — the two meet
//! only in a forwarder that terminates both, which is a third crate
//! ([decisions/0013](../../../docs/decisions/0013-competitor-libraries.md)
//! §4, §4.5).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver, `AF_UNIX` hygiene and name registry, and `weida-core`'s
//! `Error`/`LocalPrincipal` vocabulary at the boundary. `[dependencies]`
//! therefore names `weida-runtime`, `weida-core` and `weida-sp`, and
//! **never** `weida` or `weida-protocol`: an SP user must not link quinn,
//! rustls and weida's pattern layer to open a `tcp://` socket, and the
//! dependency direction is what keeps an SP socket from reaching weida's
//! types at all [0013 §4.2].
//!
//! # What exists so far
//!
//! - [`Context`] — the reactor, the `inproc://` namespace and the socket
//!   ceiling, created three ways ([`Context::new`], [`Context::with_handle`],
//!   [`Context::owned`]) and closed with a **finite** budget. NNG keeps these
//!   three things global and this crate cannot; see the module note for why,
//!   and for why this is not `nng_ctx`.
//! - [`Endpoint`] — `tcp://`, `tls+tcp://`, `ipc://` and `inproc://` parsed
//!   under NNG's own length rules — [`NNG_MAXADDRLEN`], the kernel's
//!   `sun_path` budget and the 122-bytes-including-NUL legacy IPC form —
//!   with every other NNG and SP transport named absent rather than called a
//!   typo.
//! - [`Protocol`] — one row per SP protocol: its wire identity from
//!   [`weida_sp`], whether it sends, whether it receives, and whether it
//!   holds the per-transaction state an `nng_ctx` is made of.
//! - [`Error`] — NNG's own `NNG_E*` vocabulary, each error carrying a cause,
//!   with weida's `Error` converted at the boundary and never re-exported.
//! - [`Message`] — a protocol header and an application body in separate
//!   storage, delivered whole or not at all, with `NNG_OPT_RECVMAXSZ`
//!   judged from the declared 64-bit length before a body is allocated.
//! - [`Queue`] and [`PipeId`] — `NNG_OPT_SENDBUF`/`NNG_OPT_RECVBUF` as
//!   depths in messages, where depth zero is NNG's rendezvous rather than
//!   libzmq's "unlimited", with [`FullAction`] carrying each protocol's
//!   behaviour at the bound so that no socket type re-decides it.
//!
//! # Rules this crate keeps
//!
//! - **Options are honoured or refused, never silently ignored** [0013 §4.4
//!   item 4]. A configuration this library cannot deliver fails where it is
//!   configured, with the NNG code that names the reason.
//! - **No remote input causes unbounded allocation** (`docs/INVARIANTS.md`).
//!   SP bounds nothing: a message may declare 2^64-1 octets and
//!   `NNG_OPT_RECVMAXSZ` is unlimited by default, the tag stack has no
//!   length field, and nothing limits how many connections a stranger opens
//!   (`docs/research/nanomsg-nng.md` §3, §5, §11). Every one of those gets a
//!   named, documented bound here.
//! - **Where NNG and the RFCs disagree, both numbers are published.** The
//!   PAIR v1 initial hop count and the `MAXTTL` ceiling are the two the
//!   sheet records (§3, §11); this crate says which it sends and accepts
//!   both, rather than choosing silently.
//! - [`Engine`], [`Dialer`] and [`Listener`] — one socket's dialling and
//!   listening endpoints, the pipes they create, the reconnect backoff of
//!   `NNG_OPT_RECONNMINT`/`RECONNMAXT`, and the three pipe events, with a
//!   synchronous dial that waits for the peer's protocol header without
//!   costing a thread and a named ceiling on how many pipes a socket
//!   admits ([`DEFAULT_MAX_PIPES`]).
//! - [`SocketOptions`] — every option under NNG's own name, honoured or
//!   refused where it is set, plus the three bounds SP leaves open and this
//!   library closes.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from
//! `docs/research/nanomsg-nng.md`, which cites the nanomsg RFCs and NNG
//! 1.10.0's manual pages. Section references in this crate's documentation
//! (`§2`, `§11`) are that sheet's.

#![warn(missing_docs)]

pub mod context;
pub mod endpoint;
pub mod engine;
pub mod error;
pub mod message;
pub mod options;
pub mod pipe;
pub mod protocol;
pub mod transport;

pub use context::{
    Closed, Context, ContextConfig, DEFAULT_CLOSE_BUDGET, DEFAULT_MAX_SOCKETS, SocketId, SocketSlot,
};
pub use endpoint::{
    Endpoint, MAX_INPROC_NAME_BYTES, MAX_LEGACY_IPC_PATH_BYTES, NNG_MAXADDRLEN, TcpHost,
};
pub use engine::{
    Admission, Connection, Dialer, DialerId, Engine, HandshakeGate, Listener, ListenerId,
    PipeCallback, PipeEvent, PipeInfo, Role, Session, SessionFuture,
};
pub use error::{Cause, Error, Result};
pub use message::{DEFAULT_RECV_MAX_SIZE, Message, RECV_MAX_SIZE_UNLIMITED};
pub use options::{
    DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_MAX_ADDRESSES, DEFAULT_MAX_PIPES, DEFAULT_RECONNECT_MAX,
    DEFAULT_RECONNECT_MIN, SocketOptions,
};
pub use pipe::{
    DEFAULT_RECV_DEPTH, DEFAULT_SEND_DEPTH, FullAction, MAX_QUEUE_DEPTH, PipeId, Queue,
    QueueConfig, Sent,
};
pub use pipe::{Discarded, Pipe, PipeConfig};
pub use protocol::{PROTOCOLS, Protocol, protocol};
pub use transport::Stream;
