# Invariants

This file is deliberately short so that an implementation agent can consult it repeatedly.

Every design change MUST be validated against this list. A change that violates an
invariant is rejected, not accommodated; if an invariant is genuinely wrong, the invariant
is amended here first, with the reasoning recorded, and only then is code changed.

## Invariant list

Reproduced verbatim from master doc §77, with one amendment marked where it stands: the fourth
item gained its second half with [decisions/0034](decisions/0034-late-is-lost.md) §4.11, which
the owner accepted before any code depended on it. The flow is still one stream in every sense
the invariant protects — lifetime, refusal, cancellation, accounting — and the datagrams are the
carrier QUIC provides for units a stream would deliver late.

- Endpoint paths are opaque identifiers.
- All user payloads may remain streams end-to-end.
- Core transport does not require payload materialization.
- One data flow maps naturally to one transport stream where the transport supports it; a flow
  of units that are worthless once late maps to one registration stream and the transport's
  datagrams (amended by [0034](decisions/0034-late-is-lost.md) §4.11).
- Replies and ACKs are distinct concepts.
- Transfer-related control messages do not require a permanent control stream.
- All guarantees are defined against the immediate next hop.
- Brokerless and brokered APIs share the same messaging concepts.
- A broker cluster appears as one logical broker.
- Raft coordinates control state, not bulk payload transport.
- Payload replication remains stream-oriented.
- Disabled guarantees should not participate in the hot path.
- No remote input can cause unbounded memory allocation.
- A managed Connector may claim only what its concrete source, Queue and sink can prove.

## v0 mechanical checks

The invariants above span the whole project. The table below records, for the invariants
that this increment (Phases 0-2, the Phase 3 pattern increment and the Phase 3 layered
re-architecture, see [IMPLEMENTATION.md](IMPLEMENTATION.md)) can already enforce, where the
enforcement lives. Invariants not listed here are not yet mechanically checkable because
the subsystem they constrain does not exist.

| Invariant | Enforced by |
| --- | --- |
| Endpoint paths are opaque identifiers | `EndpointAddr` in `weida-core` validates bytes and length only; the endpoint namespace in `weida` is a flat map keyed by the exact path string — no splitting, no prefix match, no wildcards. Pub/Sub **topics** are a separate namespace from endpoint paths and are matched by a segmented pattern with a one-segment and a trailing rest wildcard; that matching is on topics only and never on paths ([PROTOCOL.md](PROTOCOL.md) §6.4, [decisions/0007](decisions/0007-topic-namespace.md) §4.1-§4.2). The invariant was **considered for amendment and deliberately kept**: hierarchy lives in the topic namespace so that endpoint dispatch keeps exactly one answer per (stream kind, path) [0007 §4.1] |
| All user payloads may remain streams end-to-end | DATA payload is opaque bytes until FIN, with no internal framing ([PROTOCOL.md](PROTOCOL.md) §4). A datagram payload is materialized by definition and bounded by `max_datagram_size` (about a kilobyte); every payload larger than one packet still may be, and under RADIO is, a stream end to end ([decisions/0034](decisions/0034-late-is-lost.md) §4.11) |
| One data flow maps naturally to one transport stream | one QUIC stream per data flow, and QUIC's three carriers are the only primitives: a one-way transfer is one unidirectional stream; a Req/Rep exchange is one bidirectional stream whose initiating half carries the request and whose reply half carries the reply or an ERROR; HELLO, SUBSCRIBE and UNSUBSCRIBE each get their own short stream ([PROTOCOL.md](PROTOCOL.md) §4); a datagram flow is one FLOW stream that lives as long as the flow, with its units in QUIC DATAGRAM frames ([PROTOCOL.md](PROTOCOL.md) §6.8, §6.9). On the in-process transport the same holds by construction and more literally: a stream *is* a channel pair, minted when the stream is opened and dead with it ([decisions/0010](decisions/0010-local-transport.md) §4.2); on every local transport a flow's datagrams ride its FLOW stream as length-prefixed records ([PROTOCOL.md](PROTOCOL.md) §2.1) |
| Replies and ACKs are distinct concepts | held by construction: the v0 core has no application acknowledgement to confuse a reply with. The only delivery signal is `Delivery`, a sender-side transport receipt backed by QUIC's fin-acknowledgement, which is never a frame on the wire and never arrives where a reply would. Accepted / Stored / Replicated / Processed are reserved for the L2 broker ([GUARANTEES.md](GUARANTEES.md) §6) |
| Transfer-related control messages do not require a permanent control stream | ERROR rides the reply half of the exchange it concerns and nothing else; SUBSCRIBE and UNSUBSCRIBE are short header-only unidirectional streams; cancellation is `RESET_STREAM`/`STOP_SENDING`, transport signalling rather than a message. There is no multiplexed control stream anywhere in the implementation. A **control connection** per peer ([decisions/0002](decisions/0002-control-and-bulk-separation.md), [PROTOCOL.md](PROTOCOL.md) §2.5) does not violate this: the invariant forbids a permanent multiplexed control *stream* inside a connection, where transfer frames would queue behind each other; a separate connection carries its own short streams and is what removes that coupling rather than creating it |
| All guarantees are defined against the immediate next hop | `Delivery::delivered()` is defined strictly as "the next hop's **transport** acknowledged every byte and the FIN", explicitly not "the application read it" — quinn's `stopped()` says "although not necessarily the processing of it" ([GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md) §4) |
| Disabled guarantees should not participate in the hot path | `finish()` is synchronous and the payload path is `write` to the quinn `SendStream` with no task hop and no lock. A fire-and-forget sender registers nothing per *write* and awaits nothing anywhere; what it does pay, once per transfer, is dropping the `Delivery`: the receipt is pushed onto the parked set of its own connection under one uncontended lock, so that `Runtime::drain` has something to wait on ([decisions/0009](decisions/0009-drain.md) §4.2). Nothing is polled, allocated or spawned there, and the cost is not resolvable in the header bench against that bench's run-to-run spread ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-032) |
| No remote input can cause unbounded memory allocation | `header_len` is compared against `max_header_bytes` **before** allocating ([PROTOCOL.md](PROTOCOL.md) §3.1); CBOR skip is iterative with `max_depth = 8`; QUIC `stream_receive_window`, `connection_receive_window`, `max_concurrent_uni_streams` and `max_concurrent_bidi_streams` bound buffered payload and concurrent stream state; `max_connections` bounds accepted connections per binding and `max_connections_per_peer` (64) bounds what **one** peer may hold there, counted by the fingerprint it proved and released when a connection closes — the bound one connection per dialled path makes necessary, since the dialling side then chooses the count; connections that proved no identity are each their own peer and are bounded only by `max_connections`, because two anonymous connections cannot be shown to be one peer; a peer's subscriptions are bounded by `max_subscriptions` filters per connection, each capped at 256 B, and dropped wholesale when the connection closes; payload queued for one subscriber is bounded by `subscriber_buffer_bytes`; the per-connection scope table of the gap detector and of the reassembler is bounded by `max_sequence_scopes` (1024), and at the cap a new scope is left untracked rather than inserted; the reassembly hold is bounded by `max_reorder_hold` (256) transfers over all scopes, enforced by releasing the oldest held transfer out of order with its gap reported, never by growing — a held transfer is an unread stream, so the bytes it pins are quinn's and are bounded again by `connection_receive_window`; the dedup window's identity table is bounded in time by the negotiated window **and** in count by `max_dedup_entries` (4096), evicting its oldest entry at the cap, so a peer that sends fast buys itself missed suppression rather than memory; a resolver answer — remote input, since whoever answers DNS chooses its length — is bounded by `max_resolved_addresses` (8), and a dial tries them in order with every attempt but the last bounded by `connect_attempt_timeout`; live transfers on one local connection are bounded by `max_local_streams` (255), and an `open` at the cap waits for a live transfer to end rather than allocating past it — the same wait a QUIC `open` makes on the peer's stream budget, not a refusal (B-059) — because there the stream *is* the OS object ([decisions/0010](decisions/0010-local-transport.md) §4.2); a DATA header's report order is bounded at the decoder by `MAX_REPORT_LEVELS` (16) before the level vector is reserved, and a non-ascending or repeated level is refused rather than stored, so the order costs at most sixteen `u64` pairs however many a peer writes; a CURSOR stream's reader holds one 64-byte read buffer plus at most one partial record (`MAX_CURSOR_RECORD_LEN`, 16 B), because a record is self-delimiting and applied as soon as it is whole, and the set it fills is a **fixed** `[(u64, u64); 16]` that allocates nothing at all; a CURSOR stream naming a `report_id` this side never handed out allocates **no state whatsoever** — it is reset with `CANCELED` before the first record is read ([PROTOCOL.md](PROTOCOL.md) §6.7), which is what keeps the report table bounded by the `Cursors` handles this side holds rather than by what a peer sends; worst-case hostile per-connection header memory is `max_header_bytes * (max_concurrent_uni_streams + max_concurrent_bidi_streams)` = **48 MiB** ([PROTOCOL.md](PROTOCOL.md) §10) |

Every bound this list names is now **implemented**: `max_connections_per_peer` with the
per-path connections of B-017, `max_local_streams` with the first local transport of
[decisions/0010](decisions/0010-local-transport.md) (B-037), and `max_parked_reverse`
with the reverse pool of
[decisions/0012](decisions/0012-local-connection-grouping.md) §4.4 (B-048) — a parked
connection is a descriptor held open for a copy that may never come, so it is bounded
twice: by the pool's own ceiling and by `max_local_streams`, which counts it like any
other live local connection. The habit stays — a bound is named here before the
allocation it caps exists, so that no implementation can land without one. The named-pipe
instance count of [0010 §4.5] turned out to need nothing beyond `max_local_streams`: the
"1-255" of `CreateNamedPipe` is the range of its `nMaxInstances` argument, and 255 is
`PIPE_UNLIMITED_INSTANCES`, so the pipe is created unlimited and the per-peer stream slots
are the only ceiling, as on `AF_UNIX` (B-039). The pipe's chunk framing
([PROTOCOL.md](PROTOCOL.md) §2.1) allocates nothing for a chunk length: a reader takes at
most the caller's buffer per read, and the one buffer the transport owns — the 8 KiB
scratch of a draining reader — is a constant. The publisher's per-topic drop table
(B-067) is bounded by `max_sequence_scopes`, the same ceiling as the sequencer's per-topic
table: the topics are the local application's, not a peer's, but a table nobody bounds is a
table that grows for the life of the process, and at the cap a topic's drops count in the
aggregate only. A weida QUIC connection advertises no `max_datagram_frame_size` and buffers no
datagrams unless its profile enables flows (B-279).

**Datagram flows and RADIO/DISH are inside it with a cap each**
([decisions/0034](decisions/0034-late-is-lost.md)). `datagram_receive_bytes` (0 by default,
64 KiB when enabled) bounds what `quinn` holds unread per connection, oldest first;
`max_flows` (64) bounds the inbound flows one connection may hold, each a FLOW stream the peer
opened; `flow_queue_bytes` (16 KiB) bounds the unread datagrams of one flow, charged with a
per-entry overhead so a flood of empty datagrams is bounded by the same cap; `flow_early_bytes`
(4 KiB) and `flow_early_hold` (1 s) bound the one per-connection ring a peer can fill with
datagrams for ids it never registered; a local flow's record is refused above 1200 bytes, so a
reader's reassembly buffer is a constant. On the dish side the `newest` table that discards stale
segments holds at most `max_sequence_scopes` topics — an untracked topic is simply never stale —
and its queue is `endpoint_queue` deep and discards rather than blocks. On the radio side the
topic table is capped at `max_sequence_scopes` too and evicts only a topic with no copy in
flight, a copy holds at most 64 queued chunks, and a dish's chunks are charged against
`subscriber_buffer_bytes` exactly as a subscriber's are.

**The chain a peer presented is inside it with a cap of its own**
([decisions/0035](decisions/0035-keys-proved-not-judged.md) §4.2). A connection keeps the
certificate chain behind a proved key for its whole life and shares it into every arrival, so
it is bounded before it is kept: at most 8 certificates and 32 KiB together
(`MAX_PEER_CHAIN_CERTS`, `MAX_PEER_CHAIN_BYTES` in `crates/weida/src/tls.rs`). A binding that
requires `ClientTrust::AnyKey` refuses a larger chain in the handshake; a `Trusted` binding and
the dialling side keep none of it and know the peer by its fingerprint alone. The per-peer table
of the accept loop holds connection handles rather than a count since `Binding::disconnect`,
and is still bounded by `max_connections`.

**The sender outbox and the event stream of
[0031](decisions/0031-transparent-redial-and-the-sender-outbox.md) are inside it too, and
neither is remote input.** An outbox holds bodies the local application handed to `send`
while no peer was live, bounded in count by `RuntimeConfig::outbox_messages` (1000, ZeroMQ's
`ZMQ_SNDHWM`) and in bytes by `RuntimeConfig::outbox_bytes` (8 MiB — the bound ZeroMQ lacks),
with `outbox_full` deciding what a `send` does at either: block, drop and count, or fail. A
body larger than the byte bound is refused at the call rather than blocking forever. A peer
influences none of it: the numbers are the sender's, and what the peer's absence changes is
only *whether* the bound is reached. The `PeerEvent` stream is a broadcast channel of 64
events per endpoint; a reader that falls behind is told `Missed(n)` and the events are gone,
never queued. A slot's redial task holds the endpoint weakly and ends at the endpoint's
drop, so an application that connects and forgets holds one task per address and no more.

**An identity source of [0032](decisions/0032-identity-sources-and-the-handoff.md) holds one
identity and a 64-event stream, and reads nothing on a timer.** A `files` source checks two
modification times at most once per `FilesOptions::poll` (10 s), and only when the identity
is used — no task, no reactor, no inotify; the parse of a changed file happens then, once,
and a file that does not parse leaves the previous identity in service with `RenewalFailed`
on the stream. A rustls resolver caches one certified key per source generation, so an
unchanged source costs a handshake one atomic load. What `weida-openbao` adds runs as tasks
that hold the source and the client and end with them: one renewal per `PkiSign`, one
refresh per `PkiAnchor` and `Kv`, one token renewal per client; each sleeps between
rounds and none queues. A response from OpenBao is bounded by `reqwest`'s body read into a
`serde_json::Value`, which is the one place this crate trusts a server it authenticated to
with a token — the same trust the `bao` CLI extends.

**The cursor stream is inside this invariant, and the shape that keeps it there is worth
naming.** A report is the first thing a peer sends that is *about* a transfer rather than part
of one, so the tempting implementation is a table keyed by whatever the peer says. It is not
one: the key is a `report_id` **this** side allocated, so a stream naming an id nobody handed
out is reset with `CANCELED` and allocates nothing (B-233), and the table is bounded by the
`Cursors` handles the application holds rather than by transfers it has ever sent — each entry
is released when its handle drops. The three remote-controlled sizes inside a report are all
capped at the decoder: sixteen levels per order, sixteen bytes per record, one fixed
`[(u64, u64); 16]` per transfer that allocates nothing. A bus member's writer queue is the one
new bound that is *local* rather than remote input — the member's own `endpoint_queue` depth of
messages per peer, past which a copy is dropped and counted (B-238) — and it is named here
anyway, because an unbounded queue is an unbounded queue whoever fills it.

**Standalone foreign-protocol libraries are inside this invariant too**
([decisions/0013](decisions/0013-competitor-libraries.md)). Their bounds carry the foreign
protocol's own names, because users of those protocols must recognize them —
`ZMQ_SNDHWM`, `ZMQ_RCVHWM`, `ZMQ_MAXMSGSIZE`, `ZMQ_MAX_SOCKETS`, `ZMQ_BACKLOG`,
`ZMQ_HANDSHAKE_IVL`. Two defaults deliberately differ from libzmq's and say so in the parity
table: `ZMQ_MAXMSGSIZE` is finite because a ZMTP frame may declare 2^63-1 octets, and
`ZMQ_LINGER` is finite rather than infinite [0013 §4.4].

**A message count is not a memory bound**, and that is the one place a standalone library
adds a name of its own rather than the foreign protocol's. `ZMQ_SNDHWM` and `ZMQ_RCVHWM`
count messages, so at libzmq's own defaults one peer could hold `1000 × ZMQ_MAXMSGSIZE` per
direction; `weida-zmq`'s `QueueConfig::max_bytes` (`DEFAULT_QUEUE_BYTES`, 8 MiB per
direction per peer) bounds the bytes instead, with the message count left at exactly what
`zmq_setsockopt(3)` documents. A queue that is empty still accepts one message of any size,
because a bound that can refuse the only message in a queue is a deadlock and not a bound,
so the exposure is `max_bytes - 1 + max_message_size` and `ZMQ_MAXMSGSIZE` is what bounds
the single-message case (B-096, `docs/libraries/zmq.md` §9).

**And a payload-byte budget is not a bound either, if a message may be empty.** The L2
broker's queue is the case: `weida_broker::BrokerConfig::queue_bytes` (8 MiB per queue)
bounds what a queue holds, but a producer of zero-byte messages costs zero payload bytes and
one live `QueuedMessage` each, so the byte budget alone would bound nothing at all. Each
message is therefore charged `PER_MESSAGE_OVERHEAD` (256 B) plus its body's **allocation**
plus the labels the queue retains, which makes `queue_bytes / PER_MESSAGE_OVERHEAD` the
ceiling on the message count as well. The allocation rather than the payload length, because
a body arrives through `IncomingTransfer::collect`, which grows a `Vec` from empty in 64 KiB
reads: a 2 MiB + 64 KiB message comes to rest in a 4 MiB allocation, so charging its length
would let a queue hold close to twice `queue_bytes` of resident payload, with the factor
chosen by the producer's message size. One further bound belongs to that config rather than
to `Limits`, because a queue outlives every connection that touches it: `max_queues`,
checked before any path is claimed. `max_unsettled` bounds, **per subscription**, the
deliveries a queue has handed out and not seen reported — live since B-203, and enforced in
the delivery scan, which is the one place it can be: a subscription at its bound is not
eligible. It could not be enforced against a *cumulative* credit limit without capping a
subscription's lifetime delivery count rather than its outstanding one, which is why it is a
separate number rather than a clamp on credit. An unsettled delivery **keeps its charge**
against `queue_bytes`, so the two bounds compose: a consumer that stops reporting costs its
own slots and the queue's budget, and nothing else. The payload is read under the queue's
*remaining* budget,
never under the producer's advisory `content_len`, so an over-budget message is refused
rather than buffered and then rejected (B-201,
[decisions/0018](decisions/0018-minimal-broker.md) §4.8).

**A grant that arrives before its subscription is state a peer can create, so it is capped
too.** A CREDIT frame and the SUBSCRIBE it belongs to ride two separate unidirectional
streams handled by two independent tasks, so the credit can arrive first; dropping it left
the consumer starved in silence, so the broker holds it until the subscription lands. The
peer chooses both halves of the key — its connection and the filter — and a connection that
only ever grants credit registers no consumer route, so nothing ever tells the queue that it
went away. The table is therefore a fixed 64 entries with the oldest evicted rather than a
map that grows until an `Unsubscribed` that may never come, and eviction rather than refusal
because refusing the newest entry would let one peer's stale grants deny the mechanism to
every other consumer on the queue (`crates/broker/src/consumers.rs`).

**The queue's refusal is the opposite choice from `weida-zmq`'s, and deliberately so.** A
ZeroMQ queue that refused the only message in an empty queue would deadlock a pattern that
has nowhere else to put it; a broker that accepted a message larger than the queue it is
held in would break the bound it exists to keep, and the producer *does* have somewhere to
put the refusal — it is an exchange, and `{REJECTED}` is an answer. Same invariant, two
shapes, because the escape route differs.

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

Standalone foreign-protocol libraries terminate no weida guarantee chain and have no global
socket-to-pattern mapping. Future broker Connectors are inside the honesty invariant as concrete
managed resources: each must name its source or sink, Queue, conversion policy, limits and
achieved guarantee level, and resource application must refuse a composition it cannot honour.
One Connector's explicit policy never becomes a framework-wide equivalence between protocols
([decisions/0013](decisions/0013-competitor-libraries.md) §4.6).
