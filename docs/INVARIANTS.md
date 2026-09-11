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
that this increment (Phases 0-2, the Phase 3 pattern increment and the Phase 3 layered
re-architecture, see [IMPLEMENTATION.md](IMPLEMENTATION.md)) can already enforce, where the
enforcement lives. Invariants not listed here are not yet mechanically checkable because
the subsystem they constrain does not exist.

| Invariant | Enforced by |
| --- | --- |
| Endpoint paths are opaque identifiers | `EndpointAddr` in `weida-core` validates bytes and length only; the endpoint namespace in `weida` is a flat map keyed by the exact path string — no splitting, no prefix match, no wildcards. Pub/Sub **topics** are a separate namespace from endpoint paths and are matched by a segmented pattern with a one-segment and a trailing rest wildcard; that matching is on topics only and never on paths ([PROTOCOL.md](PROTOCOL.md) §6.4, [decisions/0007](decisions/0007-topic-namespace.md) §4.1-§4.2). The invariant was **considered for amendment and deliberately kept**: hierarchy lives in the topic namespace so that endpoint dispatch keeps exactly one answer per (stream kind, path) [0007 §4.1] |
| All user payloads may remain streams end-to-end | DATA payload is opaque bytes until FIN, with no internal framing ([PROTOCOL.md](PROTOCOL.md) §4) |
| One data flow maps naturally to one transport stream | one QUIC stream per data flow, and QUIC's two stream kinds are the only primitives: a one-way transfer is one unidirectional stream; a Req/Rep exchange is one bidirectional stream whose initiating half carries the request and whose reply half carries the reply or an ERROR; HELLO, SUBSCRIBE and UNSUBSCRIBE each get their own short stream ([PROTOCOL.md](PROTOCOL.md) §4). On the in-process transport the same holds by construction and more literally: a stream *is* a channel pair, minted when the stream is opened and dead with it ([decisions/0010](decisions/0010-local-transport.md) §4.2) |
| Replies and ACKs are distinct concepts | held by construction: the v0 core has no application acknowledgement to confuse a reply with. The only delivery signal is `Delivery`, a sender-side transport receipt backed by QUIC's fin-acknowledgement, which is never a frame on the wire and never arrives where a reply would. Accepted / Stored / Replicated / Processed are reserved for the L2 broker ([GUARANTEES.md](GUARANTEES.md) §6) |
| Transfer-related control messages do not require a permanent control stream | ERROR rides the reply half of the exchange it concerns and nothing else; SUBSCRIBE and UNSUBSCRIBE are short header-only unidirectional streams; cancellation is `RESET_STREAM`/`STOP_SENDING`, transport signalling rather than a message. There is no multiplexed control stream anywhere in the implementation. A **control connection** per peer ([decisions/0002](decisions/0002-control-and-bulk-separation.md), [PROTOCOL.md](PROTOCOL.md) §2.5) does not violate this: the invariant forbids a permanent multiplexed control *stream* inside a connection, where transfer frames would queue behind each other; a separate connection carries its own short streams and is what removes that coupling rather than creating it |
| All guarantees are defined against the immediate next hop | `Delivery::delivered()` is defined strictly as "the next hop's **transport** acknowledged every byte and the FIN", explicitly not "the application read it" — quinn's `stopped()` says "although not necessarily the processing of it" ([GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md) §4) |
| Disabled guarantees should not participate in the hot path | `finish()` is synchronous and the payload path is `write` to the quinn `SendStream` with no task hop and no lock. A fire-and-forget sender registers nothing per *write* and awaits nothing anywhere; what it does pay, once per transfer, is dropping the `Delivery`: the receipt is pushed onto the parked set of its own connection under one uncontended lock, so that `Runtime::drain` has something to wait on ([decisions/0009](decisions/0009-drain.md) §4.2). Nothing is polled, allocated or spawned there, and the cost is not resolvable in the header bench against that bench's run-to-run spread ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-032) |
| No remote input can cause unbounded memory allocation | `header_len` is compared against `max_header_bytes` **before** allocating ([PROTOCOL.md](PROTOCOL.md) §3.1); CBOR skip is iterative with `max_depth = 8`; QUIC `stream_receive_window`, `connection_receive_window`, `max_concurrent_uni_streams` and `max_concurrent_bidi_streams` bound buffered payload and concurrent stream state; `max_connections` bounds accepted connections per binding and `max_connections_per_peer` (64) bounds what **one** peer may hold there, counted by the fingerprint it proved and released when a connection closes — the bound one connection per dialled path makes necessary, since the dialling side then chooses the count; connections that proved no identity are each their own peer and are bounded only by `max_connections`, because two anonymous connections cannot be shown to be one peer; a peer's subscriptions are bounded by `max_subscriptions` filters per connection, each capped at 256 B, and dropped wholesale when the connection closes; payload queued for one subscriber is bounded by `subscriber_buffer_bytes`; the per-connection scope table of the gap detector and of the reassembler is bounded by `max_sequence_scopes` (1024), and at the cap a new scope is left untracked rather than inserted; the reassembly hold is bounded by `max_reorder_hold` (256) transfers over all scopes, enforced by releasing the oldest held transfer out of order with its gap reported, never by growing — a held transfer is an unread stream, so the bytes it pins are quinn's and are bounded again by `connection_receive_window`; the dedup window's identity table is bounded in time by the negotiated window **and** in count by `max_dedup_entries` (4096), evicting its oldest entry at the cap, so a peer that sends fast buys itself missed suppression rather than memory; a resolver answer — remote input, since whoever answers DNS chooses its length — is bounded by `max_resolved_addresses` (8), and a dial tries them in order with every attempt but the last bounded by `connect_attempt_timeout`; live transfers on one local connection are bounded by `max_local_streams` (255), and opening past the cap fails with `LimitExceeded` rather than queueing, because there the stream *is* the OS object ([decisions/0010](decisions/0010-local-transport.md) §4.2); worst-case hostile per-connection header memory is `max_header_bytes * (max_concurrent_uni_streams + max_concurrent_bidi_streams)` = **48 MiB** ([PROTOCOL.md](PROTOCOL.md) §10) |

Every bound this list names is now **implemented**: `max_connections_per_peer` with the
per-path connections of B-017, and `max_local_streams` with the first local transport of
[decisions/0010](decisions/0010-local-transport.md) (B-037). The habit stays — a bound is
named here before the allocation it caps exists, so that no implementation can land
without one — and the next entries will be the `AF_UNIX` and named-pipe socket paths of
[0010 §4.5] if they turn out to need more than `max_local_streams`.

The hot-path invariant binds all three structures that now exist: a connection that
negotiated `Ordering = None` and `Deduplication = None` — which is every connection that
declares nothing, since `core` is the default guarantee set — allocates none of them. That
is checked rather than asserted: the unit tests of `crates/weida/src/ordering.rs` and
`crates/weida/src/dedup.rs` drive a thousand calls through the disabled sequencer, detector,
reassembler and dedup window and assert that the backing tables' capacity is still zero
([GUARANTEES.md](GUARANTEES.md) §3, [PROTOCOL.md](PROTOCOL.md) §6.5).

The drain of [decisions/0009](decisions/0009-drain.md) needed **no new number**, which is
worth stating because "wait until things finish" is exactly the shape that usually grows a
queue. It does hold something, though, and the note's "holds nothing new" is sharpened
here: a finished transfer whose `Delivery` the application dropped has its receipt parked
**on its own connection**, because that receipt is the only handle to the acknowledgement.
Per connection, and not per runtime, for the hot-path invariant above: parking must not
touch a structure every connection and every worker thread shares. The set is capped by
that connection's stream budgets, `max_concurrent_uni_streams +
max_concurrent_bidi_streams` — what can be unacknowledged at once is what can be in flight
at once — and it is walked only when it is full, where settled receipts are reaped before
anything is thrown away, and once more at drain time. It is local either way: a peer cannot
grow it by sending, only this process can, by finishing transfers. A receipt evicted at the
cap is counted as outstanding by the next drain rather than assumed delivered.

A local peer is proved by the **kernel** rather than by a key, which is the one place the
identity invariant reads differently: `IncomingMeta::peer` carries a key or a local principal,
an in-process peer carries neither, and a PID is an observation that MUST NOT be authorized on
([ARCHITECTURE.md](ARCHITECTURE.md) §2, [decisions/0010](decisions/0010-local-transport.md)
§4.4). What does not change is that an identity is proved and never claimed. Both kinds now
exist: `IncomingMeta::peer` is `Option<PeerIdentity>`, `None` in process and for an
anonymous TLS client, `Key` on QUIC, and `Local { uid, gid, pid }` on `AF_UNIX`, taken from
the kernel at connect time. The `sha256:…@` userinfo is refused on every local scheme so
that no local address can look authenticated [0010 §4.8], and what binds a local peer's
several connections together is a group token *plus* those same credentials, never the
token alone ([decisions/0012](decisions/0012-local-connection-grouping.md) §4.2).

Invariants deferred with their subsystems: brokerless/brokered API parity, broker cluster
as one logical broker, Raft scope, stream-oriented payload replication, and adapter
guarantee honesty. None of the v0 code may be shaped in a way that forecloses them. The
acknowledgement vocabulary they need — Accepted, Stored, Replicated, Processed — is
reserved for the L2 broker rather than approximated on the v0 wire, precisely so that a
broker hop can define it against real responsibility transfer instead of inheriting a
brokerless ACK that only ever meant "arrived in RAM"
([IMPLEMENTATION.md](IMPLEMENTATION.md) §1, [GUARANTEES.md](GUARANTEES.md)).

Adapter guarantee honesty is deferred as *code* and not as a rule: it has a home from now on.
Every adapter's mapping document (`docs/adapters/<proto>.md`) states the foreign protocol's
transfer point, where the guarantee chain therefore ends, the named losses, and the
configurations the adapter refuses; that document, not the code, is what a reader checks the
invariant against ([decisions/0006](decisions/0006-guarantee-sets.md) §4.6-§4.9,
[docs/adapters/README.md](adapters/README.md)). The mechanical half is the refusal: a guarantee
set an edge cannot carry is rejected at configuration time, and a degradation exists only as a
named configuration entry [0006 §4.7]. The editorial half is the document, and it is required
before the bridge code of its protocol exists.
