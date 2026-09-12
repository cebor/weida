//! Bridges foreign ZeroMQ peers onto weida endpoints, and weida endpoints onto
//! foreign ZeroMQ peers.
//!
//! This is the **forwarder** of `docs/adapters/zmtp.md`, built on the
//! [`weida_zmq`] sockets rather than on a ZMTP session of its own
//! ([0013](https://git.doodleshnookie.net/hannes/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2). What each side speaks is somebody else's code: ZeroMQ is
//! `weida-zmq`'s, weida is `weida`'s, and what is here is the mapping between
//! them and nothing else.
//!
//! # A hop, not a tunnel
//!
//! The bridge terminates ZMTP and terminates weida. Nothing is forwarded
//! opaquely, which is what makes "all guarantees are defined against the
//! immediate next hop" checkable here (`docs/INVARIANTS.md`): the ZeroMQ peer's
//! only transfer point is `zmq_send` returning, the bridge's is
//! `Delivery::delivered()`, and there is no acknowledgement in ZMTP at all, so
//! the chain ends at this edge (`docs/adapters/zmtp.md` §7,
//! `docs/decisions/0006-guarantee-sets.md` §4.6).
//!
//! # What it maps
//!
//! One [`Inbound`] binds one ZeroMQ socket, presents one socket type to
//! whoever connects, and speaks to one weida endpoint:
//!
//! | Bridge presents | Foreign peer | weida side |
//! | --- | --- | --- |
//! | `REP` | `REQ`, `DEALER` | one Req/Rep exchange per message |
//! | `PULL` | `PUSH` | one Push transfer per message |
//! | `PUB` (an `XPUB` socket) | `SUB`, `XSUB` | a `Subscriber`, with the peer's subscriptions translated |
//!
//! One ZMTP message is one weida transfer and therefore one QUIC stream
//! (`docs/adapters/zmtp.md` §3).
//!
//! [`Outbound`] is the mirror: it **binds** on the weida side — Rep, Pull and
//! Pub bind (`docs/ARCHITECTURE.md` §6c.4) — and dials a foreign ZeroMQ peer,
//! as `DEALER` toward a `REP`/`ROUTER`, `PUSH` toward a `PULL`, or `SUB`
//! toward a `PUB`. Two things exist only there: a **correlation envelope**,
//! because a weida `Replier` accepts concurrent exchanges while a ZeroMQ `REP`
//! answers in order, and a **reply deadline**, because a ROUTER that cannot
//! route drops the request silently (loss L5) and the absence of a reply is
//! the only observation available.
//!
//! # What it refuses, and why it refuses rather than approximating
//!
//! * A **socket type** that may not talk to the one it presents. Refused by
//!   the socket at the handshake with an `ERROR` naming both types, which is
//!   the specification's table doing the work (§2).
//! * A **multipart** message where the pattern defines no envelope: ZMTP
//!   delivers multipart atomically and weida has no message-part concept, so
//!   concatenating would invent an application protocol. Loss L1, refused by
//!   default (§9.2). The envelope frames a pattern *does* define — REQ's empty
//!   delimiter — are the socket's business and never reach this crate.
//! * A **subscription** that no weida filter can express: a byte prefix ending
//!   mid-segment (L2) or containing weida's separator or wildcards (L4). The
//!   boundary-plus-local-refilter opt-in of §9.3 is
//!   [`MidSegment::BoundaryAndRefilter`], where the local re-filter is the
//!   ZeroMQ socket's own prefix match against the topic frame.
//! * A **payload beyond `max_message_bytes`**, in either direction: a ZeroMQ
//!   peer cannot be handed a body before it is complete, so the bound is
//!   `ZMQ_MAXMSGSIZE` on the socket and a declared length past it is refused
//!   before the body is read (§3).
//!
//! Refusals happen at configuration time where the configuration is wrong and
//! at message time where the message is, which is the rule at an adapter edge
//! (`docs/decisions/0006-guarantee-sets.md` §4.7).
//!
//! # What is not here any more
//!
//! `wire.rs` — the session, the framed reader, the command answers, the
//! sanitizer, the drop queue — and `outbound.rs`'s `Liveness` and
//! `subscriptions.rs`'s reference counting. Each was a piece of ZeroMQ with a
//! bridge's name on it, and each now lives where a socket can be checked
//! against libzmq for it: the parity table is
//! [`docs/libraries/zmq.md`](https://git.doodleshnookie.net/hannes/weida/blob/main/docs/libraries/zmq.md),
//! and the interop evidence is `weida-zmq`'s own matrices against libzmq 4.3.5
//! and the pure-Rust `zeromq` crate. `docs/adapters/zmtp.md` §3 lists the five
//! differences the rebuild made.

#![warn(missing_docs)]

mod error;
mod inbound;
mod outbound;
mod subscriptions;

pub use error::BridgeError;
pub use inbound::{Inbound, InboundConfig, Presenting};
pub use outbound::{Dialling, Outbound, OutboundConfig, SubscriptionForm};
pub use subscriptions::MidSegment;
