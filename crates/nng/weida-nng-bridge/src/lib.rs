//! Bridges foreign nanomsg/NNG SP peers onto weida endpoints.
//!
//! This is Phase B slice 2 of `docs/adapters/nng.md`: the **inbound**
//! direction, where SP peers speak the TCP mapping to the bridge and the
//! bridge speaks weida onward. The codec it drives is [`weida_sp`], which has
//! no I/O and no weida dependency; everything protocol-shaped is decided
//! there and everything here is the hop.
//!
//! # A hop, not a tunnel
//!
//! The bridge terminates SP and terminates weida. Nothing is forwarded
//! opaquely, which is what makes "all guarantees are defined against the
//! immediate next hop" checkable here (`docs/INVARIANTS.md`). SP has **no
//! transfer point at all** - no application acknowledgement, no broker
//! receipt, no persistence signal (`docs/research/nanomsg-nng.md` §6) - so
//! the guarantee chain ends at this edge, at the bridge's own local queue,
//! which is the second case of
//! `docs/decisions/0006-guarantee-sets.md` §4.6 and what
//! `docs/adapters/nng.md` §7 says in those words.
//!
//! # What it maps
//!
//! One [`Inbound`] listens on a TCP address, presents one SP protocol to
//! whoever connects, and speaks to one weida endpoint:
//!
//! | Bridge presents | Foreign peer | weida side |
//! | --- | --- | --- |
//! | `REP` (`0x31`) | `REQ` | one Req/Rep exchange per message, concurrent |
//! | `PULL` (`0x51`) | `PUSH` | one Push transfer per message |
//! | `PUB` (`0x20`) | `SUB` | a `Subscriber` on the empty filter |
//!
//! One SP message is one weida transfer and therefore one QUIC stream
//! (`docs/adapters/nng.md` §3). The per-protocol headers are consumed at the
//! bridge and never forwarded as payload: the REQ/REP tag stack is kept
//! beside the exchange and written back onto the reply unchanged, and a
//! PAIR v1 hop count would be dropped after its check (§3).
//!
//! # What it refuses, and how a peer finds out
//!
//! **SP has no error frame.** There is no `ERROR` command, no reason string
//! and no code: the TCP mapping's only remedy is "the connection MUST be
//! closed immediately" [rfc-tcp §2]. So every refusal below is observable to
//! the peer as a **close**, which is loss L10 of `docs/adapters/nng.md` §8
//! and the sharpest difference from the ZMTP bridge next door, where a
//! refusal carries a reason.
//!
//! * An **endpoint type** that may not talk to the one presented: closed
//!   (§2). The peer sees what a real NNG socket would give it.
//! * A **protocol header** with wrong magic, an unknown version or a nonzero
//!   reserved field: closed [rfc-tcp §2].
//! * A **message beyond `max_message_bytes`**: closed, and the declaration is
//!   refused from the size field alone, before anything is allocated (§3).
//!   NNG would discard the message and keep the pipe; a bridge cannot, for
//!   the reason `weida_sp::error::MessageError::is_violation` documents.
//! * A **tag stack deeper than `max_hops`**, or a truncated one: closed
//!   [rfc-reqrep §5].
//!
//! Configuration refusals happen at [`Inbound::bind`], which is the rule at
//! an adapter edge (`docs/decisions/0006-guarantee-sets.md` §4.7).
//!
//! # What it deliberately does not do
//!
//! * **It does not suppress REQ retransmissions.** A cooked REQ resends on
//!   its own timer, on disconnect, or when a peer becomes available
//!   (`docs/research/nanomsg-nng.md` §4), and the bridge forwards the resend
//!   as a second exchange: that is loss L4, decided in
//!   `docs/adapters/nng.md` §7, and suppressing it would need a deduplication
//!   identity §9.3 refuses to invent.
//! * **It does not filter for a SUB peer.** SP filters at the subscriber
//!   (§4), a SUB socket cannot send, and so the bridge never learns what the
//!   peer wants: it subscribes to the empty filter on the weida side and
//!   sends every copy. Loss L1.
//! * **It does not retry the weida side, and it does not answer a failed
//!   exchange.** There is nothing to answer with; a REQ peer's own resend
//!   timer is its recovery (§8 L3, L10).
//!
//! # The other direction
//!
//! [`Outbound`] is the mirror: it **binds** on the weida side - Rep, Pull and
//! Pub bind (`docs/ARCHITECTURE.md` §6c.4) - and dials one foreign SP peer,
//! presenting `REQ` toward a `REP`, `PUSH` toward a `PULL`, or `SUB` toward a
//! `PUB`. Three things exist only there:
//!
//! * **A request tag this side allocates, and no resend timer.** A cooked REQ
//!   retransmits by itself (`docs/research/nanomsg-nng.md` §4); a bridge that
//!   did would be inventing at-least-once for a weida requester that asked
//!   for one attempt, so the outbound bridge speaks the **raw** REQ header
//!   shape - one 31-bit id per exchange with the terminal bit
//!   [rfc-reqrep §5] - and writes each request exactly once.
//! * **A deadline, because silence is all a peer can say.** SP has no way to
//!   decline and no error frame (L10), so an unanswered exchange is refused
//!   with `ERROR{NO_REPLY}` at [`OutboundConfig::reply_deadline`] - and
//!   immediately, without waiting it out, when the connection closes first.
//! * **A topic split, because SP has none.** A published body carries its
//!   topic in its leading bytes and nothing says where they end, so
//!   [`TopicSplit`] says: a delimiter, a fixed width, or a constant topic.
//!   The inbound direction writes the same delimiter through
//!   [`InboundConfig::topic_delimiter`], which is what lets the two be paired.
//!
//! # What is not here
//!
//! The interop bench against a real NNG peer is slice 5
//! (`docs/adapters/nng.md` §10). Everything in this crate is tested against
//! an SP peer built on [`weida_sp`] itself: byte-exact against the golden
//! vectors of §10.1, and therefore faithful on the wire - and not an
//! independent implementation, which is exactly why §10's `nng` run is still
//! owed.

#![warn(missing_docs)]

mod error;
mod inbound;
mod outbound;
mod wire;

pub use error::BridgeError;
pub use inbound::{Inbound, InboundConfig, Presenting};
pub use outbound::{Dialling, Outbound, OutboundConfig, TopicSplit};
