//! Bridges foreign nanomsg/NNG SP peers onto weida endpoints.
//!
//! This is the forwarder of
//! [0013](https://github.com/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §4.5: **configuration plus mapping plus refusal, and no protocol**. The
//! SP side is `weida-nng`'s sockets — the protocol header, the pairing
//! check, the 64-bit framing, the REQ tag stack, the pipe ceiling and the
//! local prefix match are all theirs — and the weida side is `weida`'s
//! patterns. What is left in this crate is which socket type faces which
//! weida pattern, what the bridge will hold, and what it refuses.
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
//! **SP itself.** `wire.rs` is gone: the handshake driving, the framing,
//! the tag-stack bookkeeping and the prefix matching moved into
//! `weida-nng`, where they are a library's behaviour rather than a
//! bridge's private code (0013 §5.2). `max_message_bytes` is
//! `NNG_OPT_RECVMAXSZ` on the socket, `max_hops` is `NNG_OPT_MAXTTL`, and
//! `max_connections` is the socket's pipe ceiling; this crate keeps its own
//! weida-side cap, which is the same number applied where a weida payload
//! is buffered whole.
//!
//! **The interop bench** is `weida-nng`'s, against the vendored C library,
//! and it runs there rather than here: a bridge test against a peer built
//! from the same codec would only ever agree with itself.

#![warn(missing_docs)]

mod error;
mod inbound;
mod outbound;

pub use error::BridgeError;
pub use inbound::{Inbound, InboundConfig, Presenting};
pub use outbound::{Dialling, Outbound, OutboundConfig, TopicSplit};
