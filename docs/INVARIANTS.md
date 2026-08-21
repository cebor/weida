# Invariants

This file is deliberately short so that an implementation agent can consult it repeatedly.

Every design change MUST be validated against this list. A change that violates an
invariant is rejected, not accommodated; if an invariant is genuinely wrong, the invariant
is amended here first, with the reasoning recorded, and only then is code changed.

## Invariant list

Reproduced verbatim from master doc §77.

- Endpoint paths are opaque identifiers.
- All user payloads may remain streams end-to-end.
- Core transport does not require payload materialization.
- One data flow maps naturally to one transport stream where the transport supports it.
- Replies and ACKs are distinct concepts.
- Transfer-related control messages do not require a permanent control stream.
- All guarantees are defined against the immediate next hop.
- Brokerless and brokered APIs share the same messaging concepts.
- A broker cluster appears as one logical broker.
- Raft coordinates control state, not bulk payload transport.
- Payload replication remains stream-oriented.
- Disabled guarantees should not participate in the hot path.
- No remote input can cause unbounded memory allocation.
- Protocol adapters may not silently invent guarantees their source protocol cannot provide.

## v0 mechanical checks

The invariants above span the whole project. The table below records, for the invariants
that this increment (Phases 0-2 plus the Phase 3 pattern increment, see
[IMPLEMENTATION.md](IMPLEMENTATION.md)) can already
enforce, where the enforcement lives. Invariants not listed here are not yet mechanically
checkable because the subsystem they constrain does not exist.

| Invariant | Enforced by |
| --- | --- |
| Endpoint paths are opaque identifiers | `EndpointAddr` in `weida-core` validates bytes and length only; the endpoint namespace in `weida` is a flat map keyed by the exact path string — no splitting, no prefix match, no wildcards. Pub/Sub **topics** are a separate namespace from endpoint paths and are matched by byte prefix; that prefix match is on topics only and never on paths ([PROTOCOL.md](PROTOCOL.md) §6.6) |
| All user payloads may remain streams end-to-end | DATA payload is opaque bytes until FIN, with no internal framing ([PROTOCOL.md](PROTOCOL.md) §4) |
| Core transport does not require payload materialization | `OutgoingTransfer` implements `AsyncWrite` and `IncomingTransfer` implements `AsyncRead`; `collect(max_bytes)` is an opt-in convenience with an explicit cap, never an internal step |
| One data flow maps naturally to one transport stream | one QUIC uni stream per transfer: request, reply, ACK, ERROR, CANCEL and HELLO each get their own stream |
| Replies and ACKs are distinct concepts | separate frame kinds `1` (DATA) and `2` (ACK) on separate streams; an ACK never substitutes for a reply and vice versa ([PROTOCOL.md](PROTOCOL.md) §9.2) |
| Transfer-related control messages do not require a permanent control stream | ACK, ERROR and CANCEL are short header-only uni streams; there is no multiplexed control stream anywhere in the implementation |
| All guarantees are defined against the immediate next hop | `ACK(accepted)` is defined strictly as "the peer read the payload to FIN and handed it to the application" ([GUARANTEES.md](GUARANTEES.md)) |
| Disabled guarantees should not participate in the hot path | `ack_mode = none` registers no pending-ACK entry and emits no control stream; the payload path is `write` to the quinn `SendStream` with no task hop and no lock |
| No remote input can cause unbounded memory allocation | `header_len` is compared against `max_header_bytes` **before** allocating ([PROTOCOL.md](PROTOCOL.md) §3.1); CBOR skip is iterative with `max_depth = 8`; QUIC `stream_receive_window`, `connection_receive_window` and `max_concurrent_uni_streams` bound buffered payload and concurrent stream state; `max_connections` bounds accepted connections; a peer's subscriptions are bounded by `max_subscriptions` filters per connection, each capped at 256 B, and dropped wholesale when the connection closes; payload queued for one subscriber is bounded by `subscriber_buffer_bytes`; worst-case hostile per-connection header memory is 32 MiB ([PROTOCOL.md](PROTOCOL.md) §10) |

Invariants deferred with their subsystems: brokerless/brokered API parity, broker cluster
as one logical broker, Raft scope, stream-oriented payload replication, and adapter
guarantee honesty. None of the v0 code may be shaped in a way that forecloses them; in
particular the wire protocol keeps ACK semantics hop-local and extensible precisely so that
`stored` and `replicated` can be added without changing the DATA frame layout.
