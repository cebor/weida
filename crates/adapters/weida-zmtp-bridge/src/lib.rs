//! Bridges foreign ZeroMQ peers onto weida endpoints.
//!
//! This is Phase B slice 2 of `docs/adapters/zmtp.md`: the **inbound**
//! direction, where ZeroMQ peers speak ZMTP to the bridge and the bridge
//! speaks weida onward. The codec it drives is [`weida_zmtp`], which has no
//! I/O and no weida dependency; everything protocol-shaped is decided there
//! and everything below is the hop.
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
//! One [`Inbound`] listens on a TCP address, presents one ZeroMQ socket type
//! to whoever connects, and speaks to one weida endpoint:
//!
//! | Bridge presents | Foreign peer | weida side |
//! | --- | --- | --- |
//! | `REP` | `REQ`, `DEALER` | one Req/Rep exchange per message |
//! | `PULL` | `PUSH` | one Push transfer per message |
//! | `PUB` | `SUB`, `XSUB` | a `Subscriber`, with the peer's subscriptions translated |
//!
//! One ZMTP message is one weida transfer and therefore one QUIC stream
//! (`docs/adapters/zmtp.md` §3).
//!
//! # What it refuses, and why it refuses rather than approximating
//!
//! * A **socket type** that may not talk to the one it presents: `ERROR`, then
//!   close (§2).
//! * A **multipart** message where the pattern defines no envelope: ZMTP
//!   delivers multipart atomically and weida has no message-part concept, so
//!   concatenating would invent an application protocol. Loss L1, refused by
//!   default (§9.2). The envelope frames a pattern *does* define — REQ's empty
//!   delimiter — are consumed, not forwarded (§2).
//! * A **subscription** that no weida filter can express: a byte prefix ending
//!   mid-segment (L2) or containing weida's separator or wildcards (L4). The
//!   boundary-plus-local-refilter opt-in of §9.3 is
//!   [`MidSegment::BoundaryAndRefilter`].
//! * A **payload beyond `max_message_bytes`**, in either direction: a ZeroMQ
//!   peer cannot be handed a body before it is complete, so the bridge must
//!   buffer whole messages and must bound what it buffers (§3).
//!
//! Refusals happen at configuration time where the configuration is wrong and
//! at message time where the message is, which is the rule at an adapter edge
//! (`docs/decisions/0006-guarantee-sets.md` §4.7).
//!
//! # The other direction
//!
//! [`Outbound`] is the mirror: it **binds** on the weida side — Rep, Pull and
//! Pub bind (`docs/ARCHITECTURE.md` §6c.4) — and dials a foreign ZeroMQ peer,
//! presenting `DEALER` toward a `REP`/`ROUTER`, `PUSH` toward a `PULL`, or
//! `SUB` toward a `PUB`. Two things exist only there: a **correlation
//! envelope**, because a weida `Replier` accepts concurrent exchanges while a
//! ZeroMQ `REP` answers in order, and a **reply deadline**, because a ROUTER
//! that cannot route drops the request silently (loss L5) and the absence of a
//! reply is the only observation available.
//!
//! # What is not here
//!
//! The interop bench against a real `zeromq` peer is slice 5
//! (`docs/adapters/zmtp.md` §10 items 3-6). Everything in this crate is tested
//! against a ZMTP peer built on [`weida_zmtp`] itself: faithful on the wire,
//! and not an independent implementation.

#![warn(missing_docs)]

mod error;
mod inbound;
mod outbound;
mod subscriptions;
mod wire;

pub use error::BridgeError;
pub use inbound::{Inbound, InboundConfig, Presenting};
pub use outbound::{Dialling, Outbound, OutboundConfig};
pub use subscriptions::MidSegment;
