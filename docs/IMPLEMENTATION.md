# Implementation

Implementation status, process rules and the acceptance criteria of the current increment.

Related: [ARCHITECTURE.md](ARCHITECTURE.md), [PROTOCOL.md](PROTOCOL.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md).

---

## 1. Phase tracker

Phases are executed in dependency order (master doc §79). Work MUST NOT be spread
chaotically across phases.

| Phase | Name | Status |
| --- | --- | --- |
| 0 | Architecture/specification | done |
| 1 | Core model | done |
| 2 | Native QUIC transport | done |
| 3 | Brokerless messaging patterns | in progress |
| 4 | Reliability | not started |
| 5 | Persistence subsystem | not started |
| 6 | Standalone broker | not started |
| 7 | Broker clustering | not started |
| 8 | Web adapter | not started |
| 9 | Legacy adapters | in progress |
| 10 | Language bindings | not started |
| 11 | CLI and administration | first slice done |
| 12 | Documentation/site/stabilization | not started |

Phases 0, 1 and 2 are complete. Phase 3 is under way: its first increment (Push/Pull and
Pub/Sub) landed, its second re-founded the stack on the layer model — L0 stream core,
L1 patterns, L2 broker — see [ARCHITECTURE.md](ARCHITECTURE.md), and its third gave peers
identities and pinned the stream semantics with measured probes. Router/Dealer are answered
as emergent rather than implemented, see [ARCHITECTURE.md](ARCHITECTURE.md) §6a. Phases 4
and later remain out of scope; Phase 6 gained the acknowledgement vocabulary that used to
sit in the core.

Phase 9 is open out of order, deliberately and narrowly: the ZMTP codec
(`crates/zmq/weida-zmtp`, [adapters/zmtp.md](adapters/zmtp.md) §10.1) is a self-contained
foreign-protocol codec with no weida dependency, so it needs nothing from phases 4-8 and
building it now is what turns the adapter mapping document from a design into a checked
claim. The bridge slices, which do need the patterns, wait for their place in the order.

One increment belongs to no phase: **`weida-runtime` is extracted** (B-070,
`crates/runtime`). It holds `Exec` — `spawn`, `sleep`, `within`, `enter` and the capped
resolver with its IP-literal fast path — the three reactor-ownership constructors with the
background-shutdown discipline, the `AF_UNIX` bind hygiene and `peer_credentials` of
[0010](decisions/0010-local-transport.md) §4.5, a generic named-endpoint registry with a
byte budget, and the bounded close budget; `weida` uses it and its public API is unchanged
([0013](decisions/0013-competitor-libraries.md) §4.2, §5.3). It is the prerequisite of
0013's standalone libraries, which need a reactor, a resolver and socket hygiene and none of
weida's frames — so it is neither a phase nor a slice of one, but the crate boundary the two
product lines share. `weida-runtime` depends on `weida-core` and `tokio` and on nothing
else.

**Phase 11's first slice is the `weida` binary** (B-060, `crates/weida/src/bin/weida.rs`),
out of order for the same narrow reason Phase 9 is: it needs nothing from phases 4-10 and it
is what a reader of this tree hits first, because until it existed the fastest way to find
out whether an endpoint was reachable was to write and compile a program. Four verbs —
`serve` (`--echo`, `--sink`, `--pub`), `request`, `send`, `sub` — over the same library the
examples use, with the payload on stdin and stdout and nothing else on them, addresses and
receipts on stderr, and exit codes that name the error vocabulary a script can branch on
(`3` refused, `4` unknown endpoint, `5` no reply, `6` untrusted, `7` connection lost; `1`
for everything else, deliberately including `Indeterminate`, which must not become a
matchable number). Two things it does *not* do are decisions rather than omissions: a
**bind** address is parsed by the binary rather than by `Address::parse`, because a bind
address may say port `0` and carries no fingerprint — the one it prints is its output; and
`weida+inproc://` is refused with a sentence naming why, since an in-process bus cannot be
reached from another process and a CLI that pretended otherwise would only ever talk to
itself. Standard input and output go through `std`, not tokio's `io-std` and `fs`: those
features are not in the library's dependency set and a binary is not a reason to widen what
every downstream crate compiles. What remains of the phase — administration, introspection
of a running runtime, anything that needs a broker — is untouched.

### Phase 0 — Architecture/specification

Delivers this documentation set, and only it: no code.

- Terminology formalized ([ARCHITECTURE.md](ARCHITECTURE.md) §2).
- Core invariants finalized ([INVARIANTS.md](INVARIANTS.md)).
- Architecture documents produced (this set).
- Failure vocabulary defined ([FAILURE_MODEL.md](FAILURE_MODEL.md)).
- Guarantee vocabulary defined ([GUARANTEES.md](GUARANTEES.md)).
- Protocol envelope designed ([PROTOCOL.md](PROTOCOL.md) §3).
- Serialization selected (CBOR, see §5 below).
- Version/capability negotiation designed ([PROTOCOL.md](PROTOCOL.md) §2.3).
- Initial Rust public API designed ([ARCHITECTURE.md](ARCHITECTURE.md) §7).
- Deliberately unresolved questions identified (§6 below and
  [PROTOCOL.md](PROTOCOL.md) §11).

No broker implementation begins before these foundations are coherent.

### Phase 1 — Core model

Delivers `weida-core`: an I/O-free crate, unit-tested without any networking.

- `EndpointAddr` endpoint identifiers with the `weida://` parser and the `SCHEME` constant.
- `Error`, `ErrorCode` and `StopReason` with hand-written `Display`/`Error` impls.
- `Limits`.
- `TraceContext` with the W3C `traceparent` parser and formatter.
- Cancellation representation.

As first delivered the crate also carried `TransferId` and id allocation (`id.rs`), the
`AckMode`/`AckState`/`Outcome` policy types (`policy.rs`), and the send, receive and
correlation state machines as pure transition functions (`state/`). All three were deleted
by the Phase 3 re-architecture below; the surviving modules are `addr`, `error`, `limits`
and `trace`.

The Runtime, Listener, Binding and typed Endpoint abstractions named in master doc §79's
Phase 1 list are realized in crate `weida` in this increment, because their shape is
determined by the transport they front; their model-level types (`Limits`, `EndpointAddr`,
`Error`) live in `weida-core`.

### Phase 2 — Native QUIC transport

Delivers `weida-protocol` and crate `weida`.

- Quinn integration and connection establishment.
- Negotiation (HELLO exchange, version and capability intersection).
- The protocol envelope: varints, preamble, the frame headers, CBOR codec with
  cap-before-allocation and depth-limited skip.
- Endpoint routing through a flat namespace shared by all bindings of a Listener.
- Request/reply correlation over paired unidirectional streams, with ACK, ERROR and CANCEL
  as short header-only control streams. Superseded by the Phase 3 re-architecture: an
  exchange is now one bidirectional stream, ACK and CANCEL are gone from the wire, and
  ERROR rides the reply half.
- Cancellation and stream reset in both directions.
- Resource limits, all remote-input-bounded.
- Client connection pooling keyed by `(host, port)`. The key has since grown the dialling
  terms and the fingerprint the address names (third increment below).

Large streaming transfers are tested immediately, not deferred.

---

### Phase 3 — Brokerless messaging patterns

Master doc §79 asks at minimum for Push/Pull, Pub/Sub, Req/Rep and Router/Dealer
equivalents. §82 asks whether a smaller internal primitive set implements them cleanly.

**Delivered in the first increment:**

- The §82 answer: four primitives (P1 one-way transfer, P2 correlation, P3 peer set plus
  selection policy, P4 bounded inbound queue behind an opaque path), recorded with the
  Router/Dealer-is-emergent rationale in [ARCHITECTURE.md](ARCHITECTURE.md) §6a.
- Wire: the `topic` DATA key, the SUBSCRIBE and UNSUBSCRIBE frame kinds, and the limits
  `max_subscriptions` and `subscriber_buffer_bytes` — all normative in
  [PROTOCOL.md](PROTOCOL.md). (The numbers this increment assigned were compacted by the
  re-architecture below; current numbering is `topic = 5`, SUBSCRIBE `= 3`,
  UNSUBSCRIBE `= 4`.)
- Push/Pull: `Pusher` connects and round-robins over its peers, `Puller` binds. Both ride
  P1 and P3 unchanged.
- Pub/Sub: `Publisher` binds, `Subscriber` connects; byte-prefix topic filters; fan-out
  with a per-subscriber byte budget and explicit drops, so a slow subscriber never stalls
  the publisher.
- Refusal rather than reinterpretation when a pattern meets an endpoint that does not serve
  it: the connection stays intact and the sender learns the reason. (As first delivered
  that was an ERROR `UNSUPPORTED` on its own stream plus `STOP_SENDING(REJECTED)`; the
  re-architecture gave misrouting its own stop code, see below.)

**Delivered in the second increment — the layered re-architecture:**

The stack was re-founded on three layers ([ARCHITECTURE.md](ARCHITECTURE.md)): **L0 stream
core**, ZeroMQ's idea rebuilt on QUIC, whose primitives are exactly the two QUIC stream
kinds with exactly QUIC's guarantees; **L1 patterns**, the zmq/nanomsg family as thin
wrappers over L0; and **L2 broker**, the RabbitMQ analog, deferred to Phase 6.

Deleted from the core and from the wire: `FrameKind::Ack`, `FrameKind::Cancel`,
`AckHeader`, `CancelHeader`, the `ack_mode`, `role`, `transfer_id` and `correlation_id`
DATA keys, `AckMode`, `AckState`, `Outcome`, `Role`, `TransferId`, `Correlator`,
`SendMachine`, `RecvMachine`, `ReplyDisposition`, `PendingReply` and `Limits::max_pending`
— and with them the files `crates/core/src/id.rs`, `crates/core/src/policy.rs` and the
whole `crates/core/src/state/` directory.

Two reasons, both load-bearing:

1. **A brokerless application ACK means "arrived in RAM".** QUIC already retransmits, and
   already reports that the peer's transport holds every byte. An application ACK with no
   broker behind it therefore carried no information the transport did not already have:
   it bought RabbitMQ's vocabulary — Accepted / Stored / Replicated / Processed — without
   RabbitMQ's responsibility transfer. Nothing had taken responsibility for the message, so
   the words were unearned. That vocabulary is now reserved for the L2 broker hop, where
   responsibility genuinely changes hands, and has no v0 wire representation
   ([GUARANTEES.md](GUARANTEES.md)).
2. **The correlation machinery existed only because replies rode separate unidirectional
   streams.** Correlation ids, the `Correlator`, `ReplyArrived`, the CANCEL frame and the
   per-connection pending-reply table were all bookkeeping for one question: which reply
   stream answers which request stream. One bidirectional stream per exchange answers it
   structurally — the stream *is* the correlation. Nothing on the wire names an exchange,
   the requester abandoning a reply is `STOP_SENDING(CANCELED)` on the reply half rather
   than a CANCEL frame, and the actor that held the table is gone. P2 in the §82 taxonomy
   stopped being "correlation" and became "exchange": a bidirectional stream, with nothing
   behind it ([ARCHITECTURE.md](ARCHITECTURE.md) §6a).

What took their place:

- Req/Rep is one client-opened bidirectional stream: DATA (with `endpoint`) + payload + FIN
  on the initiating half; DATA (no `endpoint`) + payload + FIN, **or** ERROR + FIN, on the
  reply half. The halves are independent, so request and reply still stream simultaneously.
- `OutgoingTransfer::finish` returns a `Delivery` transport receipt instead of an ack
  outcome; `Delivery::delivered().await` resolves when the peer's *transport* holds every
  byte, explicitly not when the application read it
  ([FAILURE_MODEL.md](FAILURE_MODEL.md)).
- The L0 API is public: `Peer` (`connect`, `open`, `open_bi`) and `Acceptor`
  (`Listener::acceptor`, `accept() -> Incoming`) in `crates/weida/src/stream.rs`, proven by
  `crates/weida/tests/raw_streams.rs`. The patterns in `endpoint.rs` are now wrappers over
  it.
- Misrouting gained its own QUIC application error code, `UNSUPPORTED = 9`, because a
  unidirectional stream can no longer be refused with an ERROR frame of its own.
- Frame kinds and DATA keys were renumbered densely and every golden vector recomputed
  ([PROTOCOL.md](PROTOCOL.md) §4, §6, §8).

**Delivered in the third increment — identity, trust and stream semantics:**

Peers gained identities. `weida_core::Fingerprint` (`crates/core/src/identity.rs`) is the
SHA-256 digest of a leaf certificate's DER `SubjectPublicKeyInfo`, with text form
`sha256:<64 lowercase hex>`; the key is hashed rather than the certificate, so a pin survives
a renewal that reuses the key. `EndpointAddr` gained `peer: Option<Fingerprint>` and the
grammar `weida://[sha256:<hex>@]host:port/path`, so one string says where to dial and whom to
accept. The TLS configuration was split along the two questions it answers: `Identity`
(`generate`, `generate_for`, `from_pem`, `from_pem_files`, `from_pem_file`, `fingerprint`,
`certificate_pem`, `to_pem`) for *who am I*, and `Trust` (`by_address`, `pin`, `anchor`,
`anchor_file` plus the `and_*` builders) for *whom do I accept* — pinned fingerprints or CA
anchors, with an address-named fingerprint overriding both. `ClientTls { trust, identity }`
and `ServerTls { identity, client_trust }` combine them; `impl Into` at every call site lets
a bare `Trust` or `Identity` stand in, and the previous root-list client shape and
certificate-and-key server shape are both gone. A binding may now require client identity
(`ServerTls::require_client`), and whatever a peer proved in the handshake is surfaced as
`IncomingMeta::peer` on inbound transfers, requests and the requester's reply half — from the
handshake, never from a header, so it can only be proved and never claimed (master doc §47).
New errors: `Error::InvalidFingerprint` and the definite `Error::Untrusted(Fingerprint)`,
which names the identity that answered instead of the one that was expected. The connection
pool key became `(host, port, ClientTls, Option<Fingerprint> from the address)`.

Three defects the new tests exposed were fixed. `ConnCtx::negotiated()` now fails immediately
with the connection's close reason instead of waiting out the 10 s HELLO deadline, so a client
whose identity a binding refuses learns it at once rather than hanging for `hello_timeout_ms`.
`conn_error` maps `TimedOut` (idle timeout) and `Reset` (stateless reset) to
`Error::ConnectionLost` — definite, where it used to be `Error::Transport("timed out")` — and
a peer close carrying a TLS alert code to `Error::Tls`. `PeerSet::add` reaps entries whose
connection has closed, so a process that reconnects after every loss no longer accumulates
dead peers for the life of the process.

The increment is pinned by 10 tests in `crates/weida/tests/identity.rs` (an address pin under
an empty trust; a wrong pin refused with `Untrusted(what answered)` and no peer added; an
address pin overriding an anchor; `Trust::pin` accepting only the key; pins and anchors
composing; an anchor checking the name where a pin does not; a binding refusing anonymous and
untrusted clients, accepting a pinned one and seeing its fingerprint in `IncomingMeta::peer`;
an anonymous client reported as `None`; reply metadata naming the server; connections not
shared across different terms), by unit tests in `crates/weida/src/tls.rs`,
`crates/core/src/identity.rs` and `crates/core/src/addr.rs`, and by 10 stream probes in
`crates/weida/tests/streams.rs` that measure what the transport actually promises about
receipts, flow control, stream budgets, cancellation, connection loss and idle timeout (§4).
The examples lost their PEM handling: `push_pull` and `pub_sub` use `Identity::generate()`
plus a pinned URL and touch no files at all, `transform_server` prints its
`weida://sha256:…@host:port/…` addresses and takes `--identity PATH` for a fingerprint that
survives a restart and `--cert-out PATH` for publishing its certificate, while
`transform_client` and `large_stream` take an optional `--ca PATH` and otherwise trust what
the address names.

**Delivered in the fourth increment — DATA keys 6 and 7 in the codec (B-013):**

`DataHeader` gained `sequence: Option<u64>` (key `6`) and
`producer: Option<[u8; 32]>` (key `7`), encoded per
[decision 0008](decisions/0008-session-identity.md) §4.4: a minimal CBOR `uint` and a raw
32-byte `bstr`, both written only when set and both skipped by a peer that does not know
them. `limits::PRODUCER_BYTES` is an exact length rather than a cap — a `bstr` of any other
length is a framing violation, because a truncated digest names nobody
([PROTOCOL.md](PROTOCOL.md) §6.2) — and `MapReader::byte_array` is the strict reader behind
it. Two golden vectors pin the encoding in `docs/PROTOCOL.md` §8, in
`crates/protocol/tests/golden_vectors.rs` and in the header unit tests; the `roundtrip` and
`data_header` fuzz targets and the deterministic `fuzz_smoke` mirrors generate both fields.

**Nothing in `crates/weida` reads or writes either key.** The runtime's `data_header` sets
both to `None` and says why, so the wire is byte-identical to the previous increment and the
B-009 header-cost numbers below still describe what the tree sends. The keys exist so that
the ordering work of [0001](decisions/0001-sequence-field.md) has a wire to land on, not
because anything uses them yet.

**Delivered in the fifth increment — HELLO guarantee declarations (B-014):**

`Hello` gained `guarantees_offered` and `guarantees_required`, both
`Option<GuaranteeSet>` and both encoded as the nested map of
[PROTOCOL.md](PROTOCOL.md) §6.5. `GuaranteeSet` is ten `Copy` fields — delivery,
acknowledgement, the durability and replica axes, ordering, deduplication and its window,
backpressure, producer naming, control isolation — with `GuaranteeSet::CORE` as the default
and `is_core()` as the reason nothing is written for it. `negotiate()` now computes the
effective set as the weaker of the two offers (`GuaranteeSet::intersect`) and fails with
`NEGOTIATION_FAILED` when it does not reach either side's requirement
(`GuaranteeSet::reaches`); `Agreed` carries the result.

Three properties are pinned rather than asserted in prose. **A v0 peer that declares nothing
still negotiates `core`** (`two_v0_peers_negotiate_core_unchanged`), and a HELLO whose
declarations are explicitly `core` encodes to the same bytes as one with none
(`golden_hello_with_guarantees_frame`). **A required level the peer does not offer fails the
handshake**, in both directions, and over a real connection in
`crates/weida/tests/hostile.rs`. **A malformed declaration is a framing violation, not a
negotiation failure**: the hostile suite builds six illegal guarantee maps byte by byte —
the library's encoder cannot produce one — and each closes the connection with
`PROTOCOL_VIOLATION`.

No new bound was needed for [INVARIANTS.md](INVARIANTS.md): a guarantee set is fixed-size
`Copy` data whose enclosing map is already bounded by `max_header_bytes`, so it adds no
remote-influenced allocation. Nothing in `crates/weida` declares anything yet; the runtime
still sends `Hello::v0`, which is why the wire is unchanged.

**Delivered in the sixth increment — detector-mode ordering (B-015):**

`RuntimeConfig::guarantees` is the configuration surface: one set, offered *and* required,
so a peer that cannot match it fails the handshake and the local set is therefore the
effective one. Configured with `OrderingMode::PerProducerDetect`, a runtime numbers its
one-way transfers in DATA key `6` — per dialled path for `Peer::open` and Push, per **topic**
for a published message, assigned once before fan-out — and the receiving connection reports
what is missing through `IncomingMeta::gap` (`Gap { expected, seen }`, `missed()`), delivering
every message as it arrives. Exchanges are not numbered: a reply carries no endpoint and the
stream is the correlation.

Both halves live in `crates/weida/src/ordering.rs` (`Sequencer`, `GapDetector`) and both are
inert under `core`. The hot-path invariant is checked rather than claimed: two unit tests
drive a thousand calls through a disabled sequencer and a disabled detector and assert the
backing tables' `capacity() == 0`, so an endpoint that negotiated nothing allocates nothing.
The detector's scope table is remote-influenced — the peer names the paths and topics — so it
is capped by the new `Limits::max_sequence_scopes` (1024); at the cap a new scope is left
untracked rather than inserted, which a unit test pins.

The end-to-end proof is `a_dropped_fan_out_copy_shows_up_as_a_gap` in
`crates/weida/tests/pubsub.rs`: a subscriber whose byte budget is exhausted loses copies, and
the next copy it receives carries a gap whose `missed()` equals `Publisher::dropped()`. That
is the capability [0001](decisions/0001-sequence-field.md) §7.2 required and the wire could
not express before B-013.

**Deduplication is not part of this increment** — it landed in the next one, below.

**Delivered in the seventh increment — bounded deduplication (B-025):**

A runtime configured `Deduplication::Bounded` with a `dedup_window_ms` suppresses a
repeated `(producer, scope, sequence)` inside that window. The producer is the connection's
proved fingerprint unless DATA key `7` names another, per
[0008](decisions/0008-session-identity.md) §4.4; the scope is the topic for a published copy
and the endpoint path otherwise, the same scope the sequencer numbers. The window lives in
`crates/weida/src/dedup.rs` (`DedupWindow`) and sits between the wire and the application in
`handle_data`: a suppressed transfer is read to EOF and discarded rather than dropped, so the
sender sees an ordinary transport receipt and never reads the suppression as a refusal.
Suppressions are counted per runtime and exposed as `Runtime::suppressed_duplicates`, which
is where drops are already visible on the publishing side.

Bounded is bounded twice. In time by the negotiated window, and in count by the new
`Limits::max_dedup_entries` (4096), because a time window bounds how long an identity is kept
and not how many arrive within it — the reason [INVARIANTS.md](INVARIANTS.md) named this bound
before the code existed. At the cap the oldest entry is evicted, so a peer that floods buys
itself missed suppression rather than memory. Both bounds are pinned by unit tests, as is the
hot-path invariant: a thousand calls through a disabled window leave the table's
`capacity() == 0`.

The end-to-end proof is `crates/weida/tests/dedup.rs`, which replays from the raw wire —
the library's own sequencer is monotone and cannot produce a repeat. Three tests: the same
numbered transfer twice inside the window arrives once and increments the counter, with an
unsuppressed sentinel behind it proving suppression rather than delay; the same identity
after the window arrives again; and under `core` the same replay arrives twice, because
nothing was negotiated.

**One bug in B-015, found by thinking about replays.** `GapDetector::observe` advanced its
position to `max(expected, seen) + 1` on every arrival, including one *below* the expected
number. A late or repeated number therefore consumed a number that had not arrived and
silenced its gap: with `0, 2, 1` the detector reported the hole at 1 and then skipped 3
without a word. A below-expected arrival now moves the position nowhere, which is what the
method's own doc comment already claimed, and `an_out_of_order_arrival_is_not_a_gap` pins it
by asserting that the hole after the late arrival is still reported. The suppression check
is also placed ahead of the detector, so a duplicate the receiver never accepted cannot
shift the receiver's idea of where the producer is.

**Delivered in the eighth increment — reassemble-mode ordering (B-022):**

`OrderingMode::PerProducerReassemble` holds an arrival whose predecessors have not come yet
and releases the run in sequence order. `Reassembler<T>` in `crates/weida/src/ordering.rs`
is the whole mechanism: a `BTreeMap` per scope plus the position, generic in what is held so
that the ordering logic is unit-testable without a connection, and the connection instantiates
it as `Reassembler<Held>`. The gap detector is now enabled in detect mode *only* — under
reassemble the reassembler owns the position, and two components tracking it would report the
same hole twice.

**A held transfer is an unread stream.** `Held` carries the `RecvStream`, the metadata and
the path; no payload is read, so reassembly stays inside "core transport does not require
payload materialization" ([INVARIANTS.md](INVARIANTS.md)). What the hold pins is transport
memory: one stream of the peer's `max_concurrent_uni_streams` budget and up to
`stream_receive_window` bytes each, the total bounded by `connection_receive_window`. This is
a **documented deviation from [0002](decisions/0002-control-and-bulk-separation.md) §6.6**,
which proposed reading eagerly into an application-owned buffer so that transport credit is
released early: v0 cannot do that without materializing the payload, so the bytes stay in the
transport, where the receiver's own window already bounds them and the sender feels the hold
as backpressure. The pubsub test above had to be given a receive window large enough to park
two copies *and* accept the arrival that evicts them — the deviation is not theoretical.

**The bound is enforced by releasing, never by growing.** `Limits::max_reorder_hold` (256 —
B-010 measured the peak at N − 1 of the transfers in flight and 84 of 256 with no adversarial
pattern, so the cap is configuration, not an assumption about arrival order) counts held
transfers over all scopes. When an arrival does not fit, the oldest held transfer is released
out of order carrying `IncomingMeta::gap` — exactly what detect mode would have reported —
and whatever now follows it in order goes out behind it. The victim scope is the one holding
the most, so one stalled producer cannot spend the whole budget and push every other scope
into out-of-order release. Seven unit tests cover it: in-order release, gapless runs, the
forced release and its gap, the hold never exceeding the cap over 500 arrivals, the scope
table at its own cap, pass-through for unnumbered, untracked-scope and late arrivals, and
the repeat rule below.

The end-to-end proofs are `crates/weida/tests/reorder.rs` — sequences scrambled on the raw
wire arrive in sequence order; a hold of two releases out of order with the gap when the
third arrival does not fit; and the reverse-FIN probe of
`reverse_order_completion_measures_the_reorder_buffer` rerun at 16 and 256 transfers with
both `Batched` and `Sequenced` finishing, now asserting that the *runtime* delivers in
sequence order what the probe's application had to reorder for itself — and
`a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for` in `crates/weida/tests/pubsub.rs`,
which is the acceptance criterion that a subscriber in either mode reports a Pub/Sub drop
through `IncomingMeta`. Note what that test shows about reassemble mode: a hole a publisher
drop leaves is *never* filled, so the loss becomes visible only when the hold is full. Under
reassemble, `max_reorder_hold` is the delay a permanent loss costs.

**Two things review caught in the first cut of `admit`.** A repeated number at or above the
position went through `BTreeMap::insert`, which replaced the held transfer silently while
`held` was incremented again: the counter drifted upward for good, forced releases began
firing below the real bound, and the displaced `RecvStream` was dropped — resetting a stream
its sender still believed was in flight. The slot now belongs to whoever claimed it first
and the repeat is passed straight through (`a_repeated_number_is_delivered_and_does_not_move_the_hold`
asserts both the unchanged hold and that every item offered came back exactly once; it fails
on the old code with `held` at 2). Suppressing a repeat is deduplication's job and stays
there. Second, the in-order arrival — the common case, and the only one on a network that is
not reordering — allocated a map node just to take it out again; when the number is the
expected one and nothing is waiting, it is now released without touching the `BTreeMap`.

**Not bounded in time.** v0 bounds the hold in count only. A hole at the very end of a
producer's traffic holds its successors until the connection closes, because nothing arrives
to push the hold over its cap. A time bound on a held transfer is the obvious next knob and
is deliberately not invented here.

**Delivered in the ninth increment — the bounded drain (B-032):**

`Runtime::drain(Duration) -> Drained` is the counterpart of the abortive `shutdown`
([decisions/0009](decisions/0009-drain.md) §4.1). In order: admission stops, the finished
transfers are awaited to their transport receipt, and then the same close runs, bounded by
whatever is left of the same deadline (§4.4). The deadline is a plain `Duration` — no
`Option`, no configuration default that could grow into ZeroMQ's infinite linger (§4.3) —
and `Drained { delivered, outstanding }` is a pair of local counts. An expired drain is not
an error and returns no error (§4.6). Nothing was added to the wire (§4.8).

**Admission** is one flag in the per-runtime `Shared` state that every connection already
needs: a binding refuses an incoming handshake outright, and an inbound stream on an
existing connection is answered with `STOP_SENDING(SHUTDOWN)` — a bidirectional one also
has its reply half reset with the same code (§4.5). No frame announces it: a peer that had
to be told would have to answer, which is the application acknowledgement
[0005](decisions/0005-refusal-race.md) closed.

**What the drain waits on, and why it had to be parked.** The only completion signal L0 has
is `SendStream::stopped()`, and a fire-and-forget sender drops the `Delivery` that holds
it, taking the last handle to the acknowledgement with it. `Delivery::drop` now hands that
receipt to **its own connection's** parked set (`ConnDrain` in
`crates/weida/src/drain.rs`, beside the sequencer and the gap detector in `ConnCtx`)
instead of dropping it; a receipt the application awaits itself is never parked. Published
fan-out copies, which never had a `Delivery` at all, are parked the same way at `finish()`
in `pubsub::write_one`. Parking is a lock and a push and nothing else — the set is walked
only when it is full, where settled receipts are reaped with one `Waker::noop()` poll
before anything is thrown away, and once more at drain time, when `DrainState::take` walks
the connections the runtime registered (once per connection, never per message). The cap
is that connection's stream budgets from `Limits`, which is why
[INVARIANTS.md](INVARIANTS.md) needs no new number for it. A receipt evicted at that cap
is counted as outstanding by the next drain rather than assumed delivered. What the first
cut got wrong, and what the numbers for it are, is §4 (B-032).

**What a slow reader cannot show, and the test that says so.** QUIC acknowledges bytes into
the receive window whether or not the application reads them, so a slow *reader* never
leaves a finished transfer outstanding — what does is an acknowledgement that never comes.
The proof pair in `crates/weida/tests/drain.rs` is therefore built around a window smaller
than the payload: a 512 KiB transfer whose receiver starts reading late arrives whole under
`drain` and is cut short by `shutdown`, asserted in both directions so the pair cannot
drift. The expiry test gives the peer its own single-threaded reactor and blocks that
thread outright — socket open, connection up, nothing read or acknowledged — and a
200 ms drain returns at its deadline with `outstanding = 1`, `delivered = 0` and no error.

**The admission refusals, and what testing them needed (B-046).** Both are three lines in an
accept loop, and neither has an observable unless a drain is *in flight* — a drain with
nothing outstanding returns at once, so there is no window to race a connect against. The
`Draining` harness in `crates/weida/tests/drain.rs` makes one: a runtime that is a server
**and** a stalled sender, holding a binding plus one finished transfer to a frozen peer whose
receipt can never settle, so its drain runs for its whole deadline. Inside that window a new
dial is refused (the control is in the same test: the same client dials one path before the
drain and another during it, which one connection per dialled path makes a real handshake
rather than a reuse), and a new stream on the connection that already exists is refused with
`SHUTDOWN`. Both fail under mutation: disabling either `is_draining()` check makes exactly its
own test fail.

Writing them exposed a vocabulary gap. `STOP_SENDING(SHUTDOWN)` fell through
`codes::stop_reason` to `StopReason::Other(6)` and reached the sender as
`Error::Transport("peer stopped receiving with code 6")` — a refusal the application could only
recognize by parsing a string. `StopReason::ShuttingDown` now names it and maps to
`Error::Rejected`: the *outcome* is the same as any other refusal — nothing of the transfer was
taken and the peer will not take it later — and a second word for one outcome is what B-028's
`LossCause` exists to avoid.

**Delivered in the tenth increment — the transport boundary and inproc (B-037):**

The first non-QUIC transport, and therefore the first time the runtime had to say what a
transport *is*. `crates/weida/src/transport.rs` defines three types — `Link`,
`SendHalf`, `RecvHalf` — and everything above them is transport-blind: the frame readers,
the patterns, the guarantees and the drain are written once and run over either transport,
which is what makes [PROTOCOL.md](PROTOCOL.md) §2.1's "same frames, same HELLO, same
negotiation" checkable rather than merely stated. An enum, not a trait object: the set of
transports is closed and small, and the payload path must stay a direct call rather than a
vtable hop. `AF_UNIX` (B-038) and named pipes (B-039) are one variant each.

`crates/weida/src/inproc.rs` is the first of them. A process-global registry maps a bus
name — at most 256 bytes, libzmq's budget for the same thing — to a bound listener, and
`Listener::bind_inproc` returns a `LocalBinding` that unregisters the name when it drops.
A connection is a pair of queues, one per stream kind, and **a stream is one channel
pair**: opening mints a `tokio::io::duplex` pair whose capacity is `stream_receive_window`,
so the backpressure is the buffer's, as [0010](decisions/0010-local-transport.md) §4.2
expects where there is no connection window. That is also why the two accept queues are
separate: the uni and bidi accept loops are separate tasks, and one queue would let each
consume the other's stream — which is exactly the bug the first cut had, and the
`req_rep_over_inproc` failure that found it.

**No TLS, no credentials, nobody proved.** A local binding takes no identity and a local
dial ignores the trust it is handed, because there is no key to check [0010 §4.3]; the
address form rejects the `sha256:…@` userinfo so that it cannot look otherwise [0010 §4.8].
In process there is nothing to prove at all, so `IncomingMeta::peer` is `None` [0010 §4.4].
`weida_core::Address` parses both schemes and the scheme alone picks the transport: nothing
falls back from one to the other [0010 §4.6].

**Ordering, deduplication and the drain are not excluded**, because they are defined on
headers and receipts rather than on sockets: `guarantees_and_the_drain_work_over_inproc`
negotiates `PerProducer(detect)` and `Bounded` over a bus, asserts the sequence numbers on
the arriving transfers and then drains the client to zero outstanding. Two transport-level
details make that work: `LocalSend::stopped` resolves once the payload and its FIN are in
the peer's buffer — in process, that *is* "the peer's transport holds every byte" — and
`Runtime::shutdown` now closes the registered links as well as the endpoints, because a
local connection has no endpoint to close.

**`max_local_streams` (255) is enforced, not just named.** Live transfers on one local
connection are counted, and an `open` at the cap waits for one to end rather than
refusing (B-059, below); the number is Windows' named-pipe instance limit, the tightest
of the three platforms [0010 §4.2]. INVARIANTS' named-bounds table is down to one entry,
`max_connections_per_peer`.

**The tests are parametrized rather than copied.** `crates/weida/tests/transports.rs`
writes one Req/Rep body, one Push/Pull body and one Pub/Sub body and runs each over both
transports through a `Harness`; the address rules, local peer loss and the guarantee/drain
case are inproc-specific tests beside them. Nine tests in that file, and the pre-existing
QUIC suites are untouched — they cover what is QUIC-specific (trust, pooling, hostile
peers), which is the split that keeps the shared bodies honest.

**Not in this slice**, and named rather than discovered: no pooling for local connections
(each dial is a fresh pair, and the pool exists for the expensive case), no control/bulk
split locally (there is no shared window to separate), and the `AF_UNIX` and named-pipe
variants with their platform rules and their local-principal identity, which are B-038 and
B-039 with the acceptance already written in [0010](decisions/0010-local-transport.md) §4.5.

**Delivered in the eleventh increment — `AF_UNIX` (B-038), and the decision it needed
(0012):**

Implementing 0010's "the OS connection is the stream" on a socket exposed what that
sentence does not say: an accepted socket cannot be dialled back, and three things need a
stream from the accepting side — its own HELLO, Pub/Sub fan-out, and everything the runtime
holds per connection that is really per peer. Descriptor passing, the shape `ipc.md` §11
calls strictly better, is closed to us: Windows `AF_UNIX` has no ancillary data and named
pipes have none either, so B-039 could not inherit it, and `SCM_RIGHTS` is unstable in
`std` while this crate forbids `unsafe`. [0012](decisions/0012-local-connection-grouping.md)
decided the alternative and this increment implements §4.1-§4.3 of it.

**Connections become a peer by grouping.** The first connection a client opens is the
control connection: it carries the HELLO exchange both ways and owns everything per peer
(`Agreed`, the sequence scopes, the dedup window, the parked drain receipts). Every further
connection opens with `0x02` and the 16-byte group token the server issued on the control
connection, and is admitted only if the token names a live peer **and** the kernel
credentials match that peer's — uid always, pid where the platform reports one. An unbound
connection is dispatched nowhere, which
`a_transfer_connection_with_an_unknown_token_is_refused` pins by injecting a raw connection
with a zero token and observing that only the legitimate transfer arrives.

**Dispatch is by path**, exactly as [PROTOCOL.md](PROTOCOL.md) §2.1 already specified for
local transports: the accepted connection's frame is read, the path is looked up, and a
replier answers on that same connection while a puller path never writes back. That is one
new concept in the boundary — `Link::dispatch_by_path` and a `handle_local` beside
`handle_bi` — and it is what a transport with no stream kinds needs.

**Identity is now a sum.** `IncomingMeta::peer` is `Option<PeerIdentity>`: a `Key` proved by
TLS, or a `Local` principal proved by the kernel — `SO_PEERCRED` on Linux through tokio's
safe `peer_cred()`, `LOCAL_PEERCRED` on macOS with no PID. A PID is reported where it
exists and is documented as an observation that must not be authorized on [0010 §4.4].
`a_unix_peer_presents_the_principal_the_kernel_proved` asserts the shape: a principal, no
key, the uid of the process that owns the socket directory, and a PID on Linux.

**Socket hygiene is the caller's directory plus our two rules.** `bind_unix` removes a
stale socket node before binding and sets mode `0600` explicitly rather than inheriting
`umask`, and its doc comment states the obligation the code cannot discharge: unlink-then-bind
races unless the directory's ownership and permissions prevent substitution
(`docs/research/ipc.md` §1.2, §7). `a_stale_socket_file_does_not_stop_the_next_bind` leaves
a node behind, binds over it and checks the mode. The path budget is checked after
percent-decoding, against 107 bytes on Linux and 104 on macOS.

**What was not here, and landed next.** Pub/Sub over `AF_UNIX`: a publisher has no stream
to a peer that dialled it, so this increment refused it with `Unsupported` and filed the
reverse pool of [0012 §4.4] as **B-048**, which the increment below implements. No
pooling, and no control/bulk split, for the same reasons as inproc. The suite is
`#[cfg(unix)]`, so Windows builds compile the transport out entirely; B-039 adds the pipe
variant to the same shape.

**Delivered in the twelfth increment — the reverse pool (B-048), which closes 0012 §4.4:**

Fan-out over `AF_UNIX` was the one pattern B-038 could not serve, because a publisher has
no way to dial a peer that dialled it. It now rides connections the subscriber parks in
advance, which is the shape [0012 §4.4] proposed without precedent; this increment is the
evidence that decides it.

**The pool.** `Subscriber::connect` over a socket transport opens
`Limits::max_parked_reverse` connections (default 8), each carrying preamble `0x03` plus
the group token, and each admitted by exactly the rule a transfer connection is: the token
names a live peer and the kernel credentials match. The server keeps their *write* halves
in a `ReversePool` shared with that peer's link; the subscriber keeps their read halves,
and hands them to its own connection driver, where they wait like any other inbound
stream. Nothing new is on the wire: what the publisher writes is a DATA frame, and the
client dispatches it by the path in its header — `dispatch_by_path` of B-038, in the other
direction.

**Replenishment is driven by the first byte, not by the end of the copy.** A parked
connection is spent the moment the peer writes on it, so `LocalRecv` signals a deficit on
its first successful read (and on drop, for one that dies unused); a maintainer task per
subscribing connection parks a replacement per deficit. Refilling at the *end* of a copy
would have shrunk the pool for the whole duration of a long transfer.

**Both bounds are real.** A parked connection is capped twice: by `max_parked_reverse`,
the pool's own ceiling, and by `max_local_streams`, which counts it like any other live
local connection — on this transport a stream is a file descriptor, so the pool is a
descriptor budget. A permit of the connection's slot semaphore is that accounting, and
the pool takes its permits with `StreamSlot::try_acquire`: filling a pool is a refusal
when the budget is spent, where a transfer waits (B-059).

**An empty pool is a drop, not a stall and not a teardown.** `Link::open_uni` on the
accepting side returns the new `Error::NoParkedConnection`, and the per-subscriber writer
counts that copy in `Publisher::dropped` and carries on — the same answer an exhausted
subscriber byte budget already gets, and the reason 0012 gave for choosing a drop: a local
subscriber that replenishes slowly must not be able to stall a publisher serving remote
ones. `an_exhausted_reverse_pool_drops_the_copy_and_counts_it` runs a pool of one against
a subscriber that never reads, and asserts both halves: the drop is counted within a
handful of publishes — the other two drop causes need 1024 queued copies or 8 MiB of them,
so the cause is unambiguous — and a later copy still arrives, so the subscription survived.

**A subscriber that parks nothing is refused when it subscribes.** With
`max_parked_reverse = 0` there is no route for a copy at all, so `Subscriber::connect`
returns `Unsupported` rather than registering a subscription that would be silent
(`a_subscriber_that_parks_nothing_is_refused_at_connect`). This replaces B-038's blanket
refusal of Pub/Sub on this transport.

**The shared Pub/Sub body now runs over all three transports.** `pub_sub_over_unix` uses
the same fan-out body as QUIC and inproc, which is the strongest statement available that
the pool is not a special case: nothing above the transport knows it is there. 17 tests in
`transports.rs`.

**Deliberately deferred** (recorded now, not discovered later):

- Connecting publishers and binding pushers; v0 fixes Pub/Pull as binders and Sub/Push as
  connectors.
- Streaming fan-out — tee-ing one long stream to many subscribers needs its own drop and
  ordering design. **Delivered later as B-064** (`Publisher::open`, §4.1 of
  [PATTERNS.md](PATTERNS.md)): the drop design turned out to be the per-subscriber drop this
  increment already had, applied per transfer instead of per message.
- Per-producer ordering. Ordering is `None` for both new patterns; a sequence field would
  be a protocol addition, not an implementation detail.
- Coalescing backpressure (master doc §27); only `Block`, `Reject` and fan-out `Drop` exist.
  **Answered later as B-065** ([decisions/0016](decisions/0016-conflation.md)): the level stays
  reserved for the L2 broker with its definition written down — a keyed queue at a hop, the key
  being the DATA `topic` — and a fan-out coalescer at L1 is *refused*, because holding the
  superseded value per key per subscriber is the materialization B-064 exists to remove. v0's
  conflation is the producer's single slot over `Publisher::open` + `write_now`.
- PAIR, BUS and SURVEYOR/RESPONDENT: mapped onto L0 in
  [ARCHITECTURE.md](ARCHITECTURE.md), deliberately not implemented until a use case asks.
- Router/Dealer as first-class types, and the broker work they would actually require
  (master doc §47, §85, Phase 6).

**Delivered in the thirteenth increment — the SP codec (B-050):**

`crates/nng/weida-sp`, the Phase B2 counterpart of `weida-zmtp` and under the same
rule: an **empty `[dependencies]`**, not even `weida-core`, so the codec is checkable
against a foreign specification instead of against our reading of it
([ARCHITECTURE.md](ARCHITECTURE.md) §4). Four modules, one per decode entry point: the
8-octet TCP protocol header (`header`), the 64-bit message framing (`message`), the
REQ/REP and survey tag stack (`backtrace`), and PAIR v1's hop count (`pair`). Every
decode takes its bound as an argument — a byte cap for the message, a hop cap for the two
header stacks — and rejects from the declared value alone, before anything is allocated.
42 tests: 21 unit, 13 golden vectors, 7 hostile-input smoke tests, 1 doctest, plus five
`cargo fuzz` targets (`header`, `message`, `message_stream`, `backtrace`, `pair`).

**Four places where the specification, the research sheet and NNG disagree.** Each is a
decision here rather than a comment in the code, because each one changes what a real peer
will accept ([docs/adapters/nng.md](adapters/nng.md) §11 carries them as open questions the
interop bench settles):

1. **Endpoint type numbers are the implementation's.** The SP RFCs assign the 12-bit
   protocol IDs and delegate the 4-bit endpoint role to the per-protocol RFCs, which never
   published them (sp-protocol-ids-01 §1-§2). **Decision:** take the role halves from NNG's
   registry (`NNI_PROTO(major, minor) = major * 16 + minor`, `src/core/protocol.h`), which
   is what both NNG and mangos put on the wire, and publish all eleven as golden vectors so
   the assumption is visible rather than buried in a match arm.
2. **PAIR v1's initial hop count differs by one.** The research sheet says the counter is
   "initialized to one and incremented at each node"
   ([research/nanomsg-nng.md](research/nanomsg-nng.md) §4); NNG's current source appends
   `0` on a cooked send and increments on receipt (`src/sp/protocol/pair1/pair.c`).
   **Decision:** encode NNG's `0`, decode both without complaint — the difference is a
   count, not a format — and name the disagreement in the module documentation, in
   `pair::INITIAL_HOPS` and in the mapping document. A test against a real NNG peer is what
   would close it; until then this codec is interoperable with the implementation rather
   than with the prose.
3. **The hop ceiling has two values.** The sheet documents `MAXTTL` as 1-255 with 8 the
   common default (§11); NNG's `NNI_MAX_MAX_TTL` is **15** (`src/core/defs.h`), and its own
   comment says the buffer sizing is why. **Decision:** ship all three as named constants
   (`DEFAULT_MAX_HOPS = 8`, `NNG_MAX_HOPS = 15`, `SPEC_MAX_HOPS = 255`) and make the bound a
   parameter of `backtrace::decode`. A codec that picked 255 would accept stacks a real peer
   drops; one that picked 15 would silently narrow the specification. The caller chooses and
   the unit test asserts that the two ceilings are in fact different.
4. **An oversized message is fatal here and recoverable in NNG.** NNG discards a message
   beyond `RECVMAXSZ` and keeps the pipe (§8), which it can do because it owns the socket
   and can drain the declared octets. **Decision:** `MessageError::BodyTooLarge` reports
   `is_violation() == true`, because a sans-I/O decoder cannot drain anything — the next
   message begins after a body it refused to read — and draining is left to a caller that
   has the socket. The same reasoning already applies to `weida-zmtp`'s `BodyTooLarge`, for
   a different upstream reason (libzmq disconnects).

**What the golden vectors are for.** `docs/adapters/nng.md` §10.1 publishes 20 accept
vectors and 6 rejection vectors; `tests/golden_vectors.rs` asserts every accept vector in
**both** directions and every rejection vector on the decoder. Encoding alone would not
catch a decoder wrong in the same way, which is the failure mode that matters when the peer
is NNG and not us — and it is what B-043 showed pays, since the ZMTP vectors were what made
a foreign peer's disagreements interpretable rather than mysterious.

**Not in this slice:** no bridge, no I/O, no weida types, and no PUB/SUB topic handling —
SP has no topic field, only the leading bytes of a body, so the split is adapter
configuration and belongs to the bridge slice ([adapters/nng.md](adapters/nng.md) §6).

**Delivered in the fourteenth increment — the SP inbound bridge (B-052):**

`crates/nng/weida-nng-bridge`, the mirror of B-041 for SP: one `Inbound` terminates the
SP TCP mapping with `weida-sp` and speaks weida onward. `REP` in front of a weida
`Replier`, `PULL` in front of a `Puller`, `PUB` fed by a `Subscriber`. Ten integration
tests against an SP peer built on our own codec — no C library, nothing `#[ignore]`d — plus
the unit test that pins the endpoint-type table.

**Two things are structurally unlike the ZMTP bridge, and both come from the protocol.**

1. **Requests are concurrent, and the tag stack is what pairs them.** ZMTP's REQ is
   lockstep, so B-041's REP loop is too. A cooked SP REQ holds one outstanding request per
   *context* and a socket may own many
   ([research/nanomsg-nng.md](research/nanomsg-nng.md) §2), so the bridge splits the socket,
   serves each request in its own task and writes replies through one writer task. Order is
   not the correlation — the 32-bit tag stack is [rfc-reqrep §5] — and
   `several_requests_in_flight_are_answered_by_tag_not_by_order` proves it by making the
   weida replier answer four held requests in reverse: a sequential bridge deadlocks on that
   test rather than failing it slowly.
2. **The `PUB` side subscribes to everything.** SP filters at the *subscriber* and a SUB
   socket has no send operation at all (§4), so no subscription ever reaches the bridge and
   there is nothing to translate. It takes the empty-subscription row of
   [adapters/nng.md](adapters/nng.md) §6, and L1 stops being a sentence: the test observes a
   topic the peer would have filtered out arriving anyway. The topic is written as the
   leading bytes of the body with no separator, because SP has no topic field.

**Every bound is a factor of one stated product**, which is the B-043 arithmetic applied
before the review pass could find it rather than after. `max_message_bytes` defaults to
1 MiB — `stream_receive_window`, the same number and the same reason as the ZMTP bridge —
and the worst case one bridge holds is
`max_message_bytes × 2 × max_in_flight × max_connections` = 1 MiB × 2 × 4 × 64 = **512 MiB**,
twice `max_in_flight` because a request payload is held while its exchange runs and its
reply while it waits for the writer. `max_connections` exists because the listener would
otherwise accept without a ceiling; `max_hops` bounds the tag stack a peer can make the
bridge allocate. All four are configuration, and zero in any of them is refused at
`bind` — which is also where a guarantee set above `core` is refused (§9.2, §9.3).

**A refusal here carries no reason, and that is now L10 of the mapping document.** SP has
no error frame: the TCP mapping's only remedy is "the connection MUST be closed
immediately" [rfc-tcp §2]. A mismatched endpoint type, a malformed protocol header, an
oversized declaration and a too-deep tag stack are therefore all the same observation from
the peer's side — the bridge's header, then EOF — which two tests assert and which makes
the bridge's own log the only place the reason exists. The contrast with the ZMTP bridge,
where a refusal carries a printable reason, is the sharpest thing this slice learned.

**What is not here:** the outbound direction (slice 3) and the interop bench against a real
`nng` peer (slice 5), which is what would settle §11's PAIR v1 hop-count disagreement and
L10's "is a close really all a peer learns".

**Delivered in the fifteenth increment — the SP outbound bridge (B-063):**

The other direction of the same crate: `Outbound` binds the weida side and dials one
foreign SP peer — `REQ` toward a `REP`, `PUSH` toward a `PULL`, `SUB` toward a `PUB`. Nine
integration tests against a peer built on `weida-sp` over a plain TCP listener, plus five
unit tests for the topic split and the request-id sequence.

**The decision this slice existed to make: the bridge does not retransmit.** A cooked REQ
socket resends on its timer, on disconnect, or when a peer becomes available
([research/nanomsg-nng.md](research/nanomsg-nng.md) §4). Doing that here would manufacture
at-least-once for a weida requester that asked for one attempt, which is the adapter
inventing a guarantee its source protocol does not give *in the direction nobody asked for*
— L3 and L4 of [adapters/nng.md](adapters/nng.md) §8 arriving from the wrong side. So the
bridge speaks the **raw** REQ header shape: a 31-bit id per exchange, written once with the
terminal bit [rfc-reqrep §5], matched on the way back. The test asserts the absence —
exactly one request on the wire and nothing after it — which is the only way an
un-retransmission can be observed.

**How a weida requester learns that an SP peer will not answer**, the question the loop
asked for judgement on. SP has no error frame and no way to decline, so silence is the
whole vocabulary (§4, §6, [rfc-tcp §2]). Two answers rather than one:

1. **A per-exchange deadline**, B-042's answer for ZMTP, reused because the shape is the
   same even though the cause is not: there a ROUTER *dropped* an unroutable request, here
   the peer may be deliberately silent. On expiry the requester gets `ERROR{NO_REPLY}`.
2. **A close ends every pending exchange at once**, which B-042 did not have. A closed
   connection is the only statement SP can make, so waiting out the deadline after it would
   be inventing patience: the test parks an exchange behind a 600-second deadline, drops the
   peer, and expects `Error::NoReply` immediately. This is the slice's one deviation from
   the ZMTP answer, recorded here because it is a deviation.

**A wrong peer type is told to nobody and reported to everybody.** The dialled peer gets a
close — L10 again — but the weida side is already bound, so for `refusal_grace` the bridge
answers every exchange with `ERROR{UNSUPPORTED}`, which reaches a requester as
`Error::Unsupported` rather than as an endpoint that never existed. A `Puller` has no coded
refusal, so an arriving transfer is dropped, which resets its stream: weida's own
per-stream refusal.

**Bounds, again stated as a product.** `max_pending_exchanges × max_message_bytes` =
64 × 1 MiB = **64 MiB**, and each waiting exchange holds a weida request *and* its body,
which is why the ceiling is checked **before** the body is read — the cheaper refusal, and
the shape B-053 had to add to the ZMTP bridge after the fact.

**The topic split became configuration in both directions.** SP has no topic field, so
`TopicSplit` says what the leading bytes mean — a delimiter, a fixed width, or a constant —
and the inbound direction gained a matching `topic_delimiter`, so a pair of these bridges
can round-trip a topic. The default delimiter is `0x00`, the one octet a weida topic cannot
contain. The `SUB` role's prefixes are applied locally, because SP subscriptions never
reach the wire, and they are **not** derived from weida filters: that is new loss L11 in
the mapping document, with the boundary rule of
[decisions/0007](decisions/0007-topic-namespace.md) §4.5 as the reason only one shape could
ever translate.

**What is not here:** the interop bench against a real `nng` peer (slice 5), which is what
would settle §11's PAIR v1 hop-count disagreement, L10's "is a close really all a peer
learns", and this slice's own new question — whether an NNG `REP` minds a requester that
never retransmits.

**Delivered in the sixteenth increment — the cross-adapter test (B-057):**

`crates/interop/cross-tests` is Phase B slice 6 ([LOOP.md](LOOP.md) §9): a message enters
through one adapter and leaves through the other. It is its own crate with no library code,
because `weida-zmq-bridge` and `weida-nng-bridge` each know one foreign protocol and nothing
of the other — the property that keeps either checkable against its own specification
([ARCHITECTURE.md](ARCHITECTURE.md) §4) — so a chain belongs to neither.

**Nine tests, both foreign ends the real implementations**: `zeromq` in pure Rust and the
`nng` C library through its Rust binding, neither `#[ignore]`d. Six are the pattern chains —
REQ→REP with the reply returning, PUSH→PULL, PUB→SUB through both topic conventions, and the
three reversed — and what they assert is that **neither envelope crosses**: the 28/REQREP
delimiter is consumed on the ZMTP side and the 32-bit tag stack is written on the SP side, so
the far peer answering the right bytes is the proof that each protocol was terminated rather
than forwarded.

**The chain's honest guarantee is `BestEffort`, and it is asserted rather than described.**
A ZeroMQ send that returns has handed the message to a socket
([adapters/zmtp.md](adapters/zmtp.md) §7); SP has no transfer point at all
([adapters/nng.md](adapters/nng.md) §7). `the_chain_is_best_effort_end_to_end` sends with the
NNG puller closed and asserts the pair — the send succeeded, the message is gone — which is
what neither mapping document may be read as promising more than.

**The composed losses are the part no single document could state.** A ZMTP multipart is
refused at hop one, so the SP edge never sees it (L1 of zmtp.md §8); with a 4 KiB ZMTP cap
against the SP side's 1 MiB, **the smaller cap decides** and the far edge buffers nothing; and
an SP `MAXTTL` ceiling reached at the *second* hop reaches the first peer **as silence**,
because SP has no error frame (L10) and the ZMTP bridge has no reply to attach its `ERROR` to.
That conversion — a refusal at the far hop becoming silence at the near one — is what the
chain adds to either document alone. That one test's SP peer is raw TCP on `weida-sp`, because
a reply carrying three forwarder ids needs an `nng_device` topology that belongs to the interop
bench.

**What it settles from the increment above.** The `nng` run is no longer owed for the REQ
question: a real `Rep0` answers this bridge's raw requester, which never retransmits. PAIR is
not in any chain, so §11's hop-count disagreement is still open on the wire.

**Delivered in the seventeenth increment — named pipes on Windows (B-039), which closes
Phase A:**

The third local transport of [0010](decisions/0010-local-transport.md) §4.1, and the one
that waited for a runner rather than for a design. It was built and gated on a Windows 11
VM (`win11-geselle`, MSVC, stable 1.98.1) beside the Linux gate: the same 0012 grouping over
`\\.\pipe\<name>`, and the same three pattern bodies of `tests/transports.rs` run over
it unchanged, plus the principal, the unknown-token refusal, the slot tests and two claims
only a pipe can make.

**The grouping was written once, and this is the increment that proved it.** `unix.rs`
held 0012's whole mechanism bound to `tokio::net::UnixStream`; it is now
`grouped.rs`, generic over a `Stream` trait with the eight things a transport contributes
— dial, principal, split, same-peer, identity, `finish`, `reset`, `stop` — and `unix.rs`
is the eighty-line socket implementation. `Link`, `SendHalf` and `RecvHalf` gained a `Pipe`
variant over the same generic types, boxed so the enum stays the size of its QUIC
variant. The accept path is one generic `LocalAccept::serve`; the two loops around it —
`accept()` on a listener, `connect()` on an instance that is replaced before it is served
— are the only per-transport code in `listener.rs`.

**A pipe has no half-close, so the pipe carries its streams in chunks.** A socket ends a
stream by shutting down one direction; a pipe handle closes whole and would take the reply
with it. Everything after the local preamble on a pipe connection is therefore chunked —
`0x00` + length + payload, `0x01` FIN, `0x02` + code RESET ([PROTOCOL.md](PROTOCOL.md)
§2.1) — at five bytes per write and nothing per byte, written through the same
`AsyncWrite` polls as the payload. It carries what the socket could not, a reset code:
`an_abandoned_request_over_a_pipe_is_a_named_cancellation` cancels a request mid-payload
and the replier reads `Error::Canceled` rather than a truncated body. What it cannot carry
is the reader's refusal, because the only direction a reader could write on is the one the
reply owns; a stopped pipe reader **drains** the rest on the runtime instead, so the writer
never blocks on a pipe nobody reads and learns of the refusal from the reply. That is
0012 §4.7's named loss moved from the write to the receipt, and it is stated in the
protocol document rather than discovered. **Byte mode, not message mode**, against 0010
§4.5's letter and for its reason: the kernel's framing would go unused under 0012, and
`mio` reads a message-mode pipe into a fixed buffer where a longer message is
`ERROR_MORE_DATA` and an error.

**Identity is a SID.** `PeerIdentity::Windows(WindowsPrincipal { sid, pid })`: the client's
token SID through `ImpersonateNamedPipeClient` on the accepting side, read only after the
first byte because a client that has not written cannot be impersonated, and reverted on
every path by a guard; the pipe object's owner SID on the dialling side, which the
creator's token set. The SID is an `Arc<str>` so that `PeerIdentity` — copied into every
incoming transfer's metadata — stays a refcount bump to clone; `PeerIdentity` lost `Copy`
for it, and six call sites gained a `.clone()`. Clients open with
`SECURITY_IDENTIFICATION`, so a server may learn who a client is and may not act as it.
`a_pipe_peer_presents_the_principal_the_kernel_proved` asserts the shape and that the pid
is this process's.

**The hygiene is `weida-runtime`'s, as on unix.** `BoundPipe::bind` creates the first
instance with `FILE_FLAG_FIRST_PIPE_INSTANCE` — a name that exists is refused as
`InvalidAddress`, which is the answer to pipe squatting — an owner-only DACL built from
the process token's SID (`O:<sid>D:P(A;;GA;;;SY)(A;;GA;;;<sid>)`, the pipe's `0600`), and
`PIPE_REJECT_REMOTE_CLIENTS`; `next_instance` recreates the same after every accept, so a
client between accepts finds the pipe busy rather than absent; `connect_pipe` waits out
`ERROR_PIPE_BUSY` on the runtime's timer for five seconds instead of blocking a thread in
`WaitNamedPipe`, and reports a missing pipe as `ConnectionLost(PeerClosed)` so a caller
redials. Three runtime tests pin the SID agreement, the taken-name refusal and the absent
pipe. `PIPE_UNLIMITED_INSTANCES` turned out to be what "255" means, so the per-peer
`max_local_streams` is the only ceiling on every platform and INVARIANTS closes its
pending entry.

**One crate may use `unsafe`, and it is thirteen kilobytes.** No dependency exposes a
security descriptor on `CreateNamedPipe`, `ImpersonateNamedPipeClient` with a token read,
`GetSecurityInfo` or the pipe pids safely — `interprocess` comes closest and stops before
the SID — so `crates/winpipe` (`weida-winpipe`) wraps those fifteen Win32 calls from
`windows-sys` behind `OwnerOnlyDacl`, `create_instance`, `open_client`, `client_peer` and
`server_peer`, each `unsafe` block one call with its precondition stated,
`unsafe_op_in_unsafe_fn` forbidden, and the crate empty off Windows. Every other crate,
`weida-runtime` included, keeps `unsafe_code = "forbid"`.

**Verified.** Linux: fmt, clippy in both configurations, **1680 tests** with 37 ignored,
rustdoc with `-D warnings`. Windows, same commit: build, fmt, clippy in both
configurations, **1658 tests** across 119 binaries with 37 ignored, rustdoc. The address
form is checked on both: `weida+pipe://<name>/<path>`, name 1..=256 bytes, no backslash,
no `sha256:…@`, mapped to `\\.\pipe\<name>` and never a UNC path; on a platform without
pipes the dial refuses it by name, as `weida+unix://` is refused on Windows.

---

## 2. Mandatory development loop per phase

For every phase (master doc §80):

```text
design
    ↓
implement
    ↓
unit test
    ↓
failure test
    ↓
fuzz relevant boundaries
    ↓
benchmark relevant hot paths
    ↓
check invariants
    ↓
refactor as if architecture had always been designed this way
    ↓
continue
```

Architectural debt MUST NOT be deferred to a later phase. "A later phase will clean this up"
is not an accepted justification for accumulating it.

---

## 3. Agent implementation rules

Verbatim from master doc §81. The implementation agent must follow these rules:

1. Read the architectural documents before changing foundational components.
2. Do not duplicate messaging semantics between brokered and brokerless paths.
3. Do not introduce complete-payload materialization into core APIs.
4. Do not add a permanent control stream merely for convenience.
5. Do not interpret endpoint paths beyond opaque lookup.
6. Do not silently weaken requested guarantees.
7. Do not advertise end-to-end guarantees when only next-hop state is known.
8. Do not mix user replies with protocol ACK semantics.
9. Do not put bulk payload replication through Raft without an exceptional demonstrated reason.
10. Do not expose broker topology to normal clients as a requirement.
11. Do not make Web a second-class feature subset.
12. Do not make Rust typing the only representation of protocol roles.
13. Do not add large dependencies without justification.
14. Do not optimize by disabling correctness for configurations that request stronger guarantees.
15. Conversely, do not impose strong-guarantee overhead on weak-guarantee configurations.
16. Prefer clear explicit state machines over distributed implicit state.
17. Prefer bounded resources everywhere.
18. Treat all network input as hostile.
19. Keep observability available from the beginning.
20. Preserve the ability to refactor the `0.x` protocol when real-world findings justify it.

---

## 4. Current increment acceptance

The reference prototype of master doc §83 is the acceptance criterion for Phases 0-2:

```text
Process A:
    Runtime
    Endpoint<Req>

Process B:
    Runtime
    Listener
        QUIC binding
        /transform Endpoint<Rep>
```

Each numbered requirement below is verbatim from §83, mapped to the artifact that
demonstrates it. Requirement 9 is the one the project has since decided against; the row
says so rather than quietly claiming it.

| # | Requirement | Demonstrated by |
| --- | --- | --- |
| 1 | Establish QUIC connection. | `echo_roundtrip` in `crates/weida/tests/reqrep.rs`; examples `transform_server` + `transform_client` |
| 2 | Negotiate protocol. | `echo_roundtrip`; `hello_with_an_unsupported_version_fails_negotiation` in `crates/weida/tests/hostile.rs` asserting close code `NEGOTIATION_FAILED` |
| 3 | Open uni request stream. | `echo_roundtrip` — but on a **bidirectional** stream: the re-architecture replaced §83's paired unidirectional streams with one exchange ([PROTOCOL.md](PROTOCOL.md) §4). A unidirectional request stream is still the Push/Pull shape (`push_best_effort` in `crates/weida/tests/pushpull.rs`) and the raw L0 `Peer::open` |
| 4 | Send metadata header. | golden-vector tests in `crates/protocol/tests/golden_vectors.rs`; `echo_roundtrip` |
| 5 | Stream arbitrary-sized input. | `streaming_overlap` in `crates/weida/tests/reqrep.rs`; example `large_stream` |
| 6 | Server begins processing before FIN where possible. | `streaming_overlap` (handler consumes the first chunk before the request FIN) |
| 7 | Server opens correlated reply stream before request necessarily completes. | `streaming_overlap` (the handler replies on the exchange's reply half after the first chunk; the client withholds the remaining payload until at least one reply byte has arrived, making the overlap deterministic). The reply half exists from the moment the exchange is opened, so "opens" is now free — and the correlation is the stream, not an id |
| 8 | Stream reply simultaneously. | `streaming_overlap`; the two halves are independent QUIC streams |
| 9 | Send protocol ACK on a separate control stream. | **Deliberately not met.** The v0 core carries no application ACK: brokerless, it would only mean "arrived in RAM", which QUIC already guarantees (§1, Phase 3). What remains is the transport receipt, `push_delivery_receipt` in `crates/weida/tests/pushpull.rs` asserting `Delivery::delivered()`. Accepted / Stored / Replicated / Processed are reserved for the Phase 6 broker ([GUARANTEES.md](GUARANTEES.md)) |
| 10 | Propagate OpenTelemetry trace context. | `trace_propagation` in `crates/weida/tests/reqrep.rs`; `traceparent` fuzz target under `crates/protocol/fuzz`; the trace id printed by `transform_client` and logged by `transform_server` |
| 11 | Cancel mid-transfer. | `cancel_mid_transfer` and `reply_abort_when_reply_stream_dropped` in `crates/weida/tests/reqrep.rs`; `canceled_resolves_when_the_requester_walks_away` for the handler-side signal |
| 12 | Test connection loss. | `crates/weida/tests/hostile.rs`: `a_server_that_never_answers_yields_indeterminate` (closed after FIN) and `a_server_that_disappears_mid_stream_yields_connection_lost` |
| 13 | Test malformed headers. | `crates/weida/tests/hostile.rs`: garbage preamble and `header_len = 1 MiB` both assert close code `PROTOCOL_VIOLATION`; `weida-protocol` unit tests for duplicate keys, missing required keys (`missing_required_keys_are_rejected`) and oversized lengths |
| 14 | Fuzz parser. | fuzz targets `preamble`, `data_header`, `hello`, `traceparent`, `roundtrip` and `subscribe` under `crates/protocol/fuzz`, plus the deterministic `fuzz_smoke_*` tests in `crates/protocol/tests/fuzz_smoke.rs` |
| 15 | Benchmark small and large transfers. | `crates/protocol/benches/codec.rs` (header encode/decode); `crates/weida/benches/echo.rs` (`echo_1kib_rtt`, `echo_1kib_rtt_explicit`, `stream_throughput_64mib`) |
| 16 | Demonstrate bounded memory with a multi-GB generated stream. | `large_stream_bounded_memory` in `crates/weida/tests/large.rs` (ignored by default; run with `--ignored`), asserting checksum equality and peak RSS below 512 MiB for a 1 GiB echo; example `large_stream` for ad-hoc runs |

This prototype proves the central architecture before the feature surface expands.

### Verified results — Phases 0-2

Recorded from the run that closed that increment, on an AMD Ryzen 7 5800X, Linux, stable
Rust 1.97.1 (nightly 1.100.0 for the fuzz targets only). Left exactly as recorded: it
measures a wire that no longer exists, so its ACK rows are history, not documentation.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 182 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 58, `weida-protocol` 69 unit + 8 fuzz-smoke, `weida` 23 unit + 11 Req/Rep + 12 hostile + 1 doc |
| Two-process prototype | `transform_server` + `printf 'hello weida' \| transform_client --ack` | stdout `HELLO WEIDA`; stderr `outcome=Acked(Accepted)`; the printed trace id appears in the server's request log line |
| Bounded memory, in process | `cargo test --release -p weida --test large -- --ignored` | 1 GiB echoed each way, checksums equal, 905 MiB/s, peak RSS **24.0 MiB** against a 512 MiB ceiling |
| Bounded memory, two process | `large_stream --gib 4` against the release server | 4 GiB echoed byte for byte, 908.7 MiB/s both directions, peak RSS **14.4 MiB** |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | all five targets, zero crashes, zero OOMs |
| Header codec | `cargo bench -p weida-protocol` | DataHeader encode 100.2 ns / decode 60.8 ns minimal, 218.5 ns / 146.8 ns fully populated; ACK 32.8 ns / 20.2 ns; preamble parse 6.9 ns |
| End to end | `cargo bench -p weida` | 1 KiB echo round trip 82.9 us best effort, 113.5 us with an `Accepted` ACK; 64 MiB streaming echo at 1.04 GiB/s counting both directions |

Peak RSS two orders of magnitude below the payload size is the substantive result: no stage
of the path buffers a whole transfer, which is what requirement 16 exists to establish.

No thresholds are asserted on the benchmark numbers this increment; they are the baseline
for later comparison.

### Verified results — Phase 3, first increment

Same machine and toolchains. The Phase 0-2 numbers above are left as recorded; these are
the checks that closed the Push/Pull and Pub/Sub increment, and they too predate the
re-architecture — the `Accepted` ACK figures below no longer have a wire to run on.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 215 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 58, `weida-protocol` 76 unit + 9 fuzz-smoke, `weida` 26 unit + 11 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 16 hostile + 1 doc |
| Golden vectors | `cargo test -p weida-protocol` | the four new vectors byte-exact both directions: oneshot DATA, fan-out DATA with `topic`, SUBSCRIBE, UNSUBSCRIBE |
| Drop policy | `slow_subscriber_drops_not_blocks` | 100 x 32 KiB published against a 64 KiB per-subscriber budget: the whole publish loop completes inside the deadline, the draining subscriber receives all 100, `Publisher::dropped() > 0` |
| Filtering | `subscribe_prefix_filters_topics` | a non-matching topic published between two matching ones never arrives; proven by a FIFO sentinel rather than a sleep |
| Selection policy reuse | `push_round_robins_two_peers` | 8 pushes over 2 peers split 4/4 by destination path, each message delivered exactly once |
| Hostile subscriptions | `cargo test -p weida --test hostile` | 257-byte filter → `PROTOCOL_VIOLATION`; `max_subscriptions + 1` distinct filters → `LIMIT_EXCEEDED`; oneshot DATA ahead of HELLO is parked and then delivered |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | all six targets including the new `subscribe`, zero crashes, zero OOMs |
| Push | `cargo bench -p weida --bench patterns` | 1 KiB push 27.6 us best effort (35.4 MiB/s), 83.0 us with an `Accepted` ACK (11.8 MiB/s) |
| Fan-out | `cargo bench -p weida --bench patterns` | 1 KiB to 8 subscribers, published and fully drained by all 8: 65.2 us (119.8 MiB/s aggregate) |

The fan-out number is the one worth reading carefully: it measures publish **plus** eight
receives, so it is a whole-cycle figure, not the cost of `publish` itself, which is a
non-blocking enqueue.

No thresholds are asserted on these numbers either.

### Verified results — Phase 3, layered re-architecture

Same machine and toolchains. `cargo test --workspace` green and
`cargo fmt --all --check` clean. `cargo clippy --workspace --all-targets` emits no
diagnostic in any file this increment touched; the workspace's one remaining warning,
`chunks_exact` with a constant chunk size in the untouched `crates/core/src/trace.rs`,
predates it and is the only thing standing between the tree and a green
`clippy -D warnings`.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | 188 tests pass, 1 ignored (the 1 GiB memory test); `weida-core` 20, `weida-protocol` 70 unit + 6 golden-vector + 9 fuzz-smoke, `weida` 28 unit + 13 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 21 hostile + 2 raw-stream + 1 doc |
| Golden vectors | `cargo test -p weida-protocol --test golden_vectors` | all six vectors recomputed and byte-exact both directions: HELLO, DATA request, DATA reply (empty map `A0`), DATA fan-out with `topic`, ERROR `{code:5}`, SUBSCRIBE/UNSUBSCRIBE |
| Transport receipt is not an application ack | `push_delivery_receipt` | `Delivery::delivered()` resolves **before** the puller calls `recv()` — a 1 KiB payload fits the flow-control window, so the receipt demonstrably reports the peer's transport, not its application |
| Cancellation without a CANCEL frame | `reply_abort_when_reply_stream_dropped`, `canceled_resolves_when_the_requester_walks_away` | dropping the `ReplyStream` stops the reply half; the handler's `canceled()` future fires and its subsequent writes fail `Canceled`. The second test is verified by mutation: stubbing the future to `pending` makes it time out |
| Typed refusal on the reply half | `bidi_request_to_a_pull_path_is_refused_with_unsupported` | a raw bidirectional DATA to a puller path is answered with a real ERROR `{UNSUPPORTED}` frame and `STOP_SENDING(UNSUPPORTED)`; the connection survives |
| Public L0 API | `acceptor_receives_both_stream_kinds` in `crates/weida/tests/raw_streams.rs` | one `Acceptor` on `/raw` receives `Incoming::Stream` from `Peer::open` and `Incoming::Exchange` from `Peer::open_bi`, and the exchange reply round-trips |
| Two-process prototype | `transform_server` + `printf 'hello weida' \| transform_client` | stdout `HELLO WEIDA`; stderr `delivered sent=… received=…`; the printed trace id appears in the server's request log line |
| Fuzzing | `cargo +nightly fuzz run <target> -- -runs=200000 -max_len=20000` | all six targets against the new field set, zero crashes, zero OOMs |
| End to end | `cargo bench -p weida` | see the table below |

| Benchmark | Now | Previously recorded |
| --- | --- | --- |
| `echo/echo_1kib_rtt` | 58.2 us | 82.9 us over two uni streams plus control — **−30 %** |
| `echo/echo_1kib_rtt_explicit` | 59.3 us | new: `open_bi`/`finish`/`recv` on the raw handles |
| `stream/stream_throughput_64mib` | 130 ms | +7 % against the Phase 0-2 run |
| `push/push_1kib_best_effort` | 7.9 us | 27.6 us — **−72 %**, no registration round trip |
| `push/push_1kib_delivered` | 26.2 ms | replaces `push_1kib_acked` |
| `fanout/pub_1kib_8_subscribers` | 62.1 us | 65.2 us — unchanged within noise |

The echo number is the substantive one: collapsing an exchange from two unidirectional
streams plus correlation bookkeeping into one bidirectional stream took 30 % off the round
trip, which is what removing work rather than tuning it looks like.

`push_1kib_delivered` at ~26 ms is not a weida cost. On an idle loopback connection the
peer delays its acknowledgement up to QUIC's max ack delay, and `delivered()` waits for
exactly that acknowledgement. The receipt is a correctness signal, not a latency-sensitive
one, which is why `Pusher::send` discards it and callers who want it reach for
`Pusher::open` + `finish()` + `delivered()`.

No thresholds are asserted on these numbers either.

### Verified results — Phase 3, identity and stream semantics

Same machine as the runs above; every stream number was measured on loopback in a debug
build against `quinn 0.11.11` / `quinn-proto 0.11.17`. `cargo test --workspace` green four
times in a row, `cargo fmt --all --check` clean, `cargo clippy --workspace --all-targets
-D warnings` clean with and without the `generate` feature, `cargo doc` without warnings. The
byte counts below are observations; each test asserts the race-free bound around them, which
is noted where the two differ.

| Check | Command | Result |
| --- | --- | --- |
| Whole workspace | `cargo test --workspace` | green: 217 tests pass, 1 ignored (the 1 GiB memory test). Counted from the test files: `weida-core` 24, `weida-protocol` 70 unit + 6 golden-vector + 9 fuzz-smoke, `weida` 33 unit + 13 Req/Rep + 7 Push/Pull + 11 Pub/Sub + 21 hostile + 2 raw-stream + 10 identity + 10 stream + 1 doc |
| Identity and trust | `cargo test -p weida --test identity` | the 10 tests listed in §1: pinned addresses, a refused pin reported as `Untrusted` with the fingerprint that answered, an address pin overriding an anchor, a pinned trust accepting the key and only the key, pins and anchors composing, name checking on anchors only, a binding requiring client identity, an anonymous client as `None`, the server's fingerprint on the reply half, and no connection sharing across different terms |
| Two-process prototype | `transform_server --bind … [--identity PATH] [--cert-out PATH]` + `transform_client [--ca PATH] URL` | a pinned address round trip prints `HELLO WEIDA`; a wrong pin exits 1 with "presented sha256:…, which is not trusted"; a plain address without `--ca` fails with "nothing to trust" before any packet; restarting with `--identity` keeps the fingerprint, so the printed URL is unchanged; `--cert-out` plus `--ca` against a pinned address works |
| A receipt beyond the window implies the reader consumed | `a_receipt_beyond_the_window_implies_the_reader_consumed` | 160 KiB pushed into a 64 KiB stream receive window: no marker at all within 300 ms while nothing is read; with an 8 KiB-chunk reader, `write_all` returns at **131072 bytes consumed** and the receipt resolves at **163840** (the whole payload). The assertion is the flow-control bound, `payload - window = 98304` |
| The stream window is not a payload budget | `a_payload_the_size_of_the_stream_window_waits_for_the_reader` | a payload of exactly 65536 bytes against a 64 KiB window produces no `Wrote` within 300 ms although the header was already parsed; it completes as soon as the reader consumes one eighth of the window (8192 bytes). The DATA header spends the same window as the payload |
| The connection window is the shared resource | `a_stalled_stream_does_not_block_its_siblings` | 32 KiB payloads, 64 KiB stream window, 256 KiB connection window: siblings of an unread stream arrive and read back correctly, and the stall lands at **7 unread streams = 229376 of 262144 bytes**; reading one stream's 32 KiB releases the stalled writer. The test asserts the bound `1 + blocked_at <= connection_window / payload` and the unblocking, not the index |
| The stream budget is backpressure, and `open` is where it lands | `the_stream_budget_is_backpressure_not_an_error`, `a_deeper_endpoint_queue_does_not_raise_the_stream_budget` | with `max_concurrent_uni_streams = 2` the third transfer blocks in `Pusher::open` — not in `write_all`, not in the receipt — and never errors; reading one transfer to EOF releases it. `endpoint_queue = 8` changes nothing, because a transfer parked in the queue still owns its stream |
| Cancellation | `cancel_discards_unread_bytes_and_keeps_read_ones` | the 4 KiB already read still compares equal; after `cancel()` the reader is served the whole 4 KiB buffered remainder and only then fails, as `io::ErrorKind::ConnectionReset` on the `AsyncRead` and as `Error::Canceled` through `read_capped` — never EOF |
| Idle timeout is connection loss | `idle_timeout_reports_loss_within_the_window` | a 500 ms server idle timeout against the client's default 10 s keep-alive (asserted to be the longer of the two): after 1.5 s of silence the next request fails with `Error::ConnectionLost(LossCause::IdleTimeout)` — the cause, not only the outcome, since B-028 |
| No automatic reconnect | `after_the_server_restarts_the_pusher_must_reconnect` | the **first** send after the server closed fails with `ConnectionLost` and `peer_count()` drops to 0; nothing reconnects, and after `connect` to a replacement server carrying the same identity a send succeeds and `peer_count()` is 1 — the dead entry was reaped |
| Hot paths unchanged | `cargo bench -p weida` (3 s measurement, both trees on the same idle machine) | `echo_1kib_rtt` 63.5-67.3 µs over four runs of this tree against 62.9 µs for the tree before it; `push_1kib_best_effort` 7.9 µs against 7.9; `pub_1kib_8_subscribers` 71.7 µs against 70.3. All within the run-to-run spread of this machine: the verifier runs once per handshake and the per-stream path gained one `Option<[u8; 32]>` copy |

The two probes that resisted determinism are recorded as observations only: the buffered tail
delivered after `cancel()` is a genuine race between the reset frame and the reader's polls,
and the exact stall index in the connection-window probe depends on header sizes. Both tests
assert the race-free part instead, and no stream probe is `#[ignore]`d.

### Verified results — DATA header cost at a high message rate (B-009)

What the two DATA keys of [decision 0001](decisions/0001-sequence-field.md) cost per message.
No sender writes either key, so each was simulated by the existing key with its exact wire
shape: `content_len` for the `uint` sequence, and a `sha256:<64 hex>` `content_type` for the
producer identity. Both endpoint paths are six bytes, so the two keys are the only difference.
Same machine as the runs above, release profile, loopback, debug assertions off.

| Check | Command | Result |
| --- | --- | --- |
| Bytes per message | `cargo bench -p weida --bench patterns -- header` (printed by the bench) | minimal DATA frame **135 B** for a 64-byte payload; with the two keys **215 B**, i.e. **+80 B**, +59.3 % of the whole frame. The split is `content_len` 6 B (1 B key + 5 B `uint`) and the 71-byte producer string 74 B (1 B key + 2 B `tstr` prefix + 71 B) |
| Messages per second | `cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`, two runs | minimal `push_64b_keys0` 5.57 µs → **179.4 Kmsg/s** (5.43-5.71 µs); with two keys `push_64b_keys2` 6.14 µs → **162.9 Kmsg/s** (6.01-6.27 µs). Run-to-run change on the same tree was within ±3 % (p = 0.38-0.41), so the **−9 % throughput** between the two variants is larger than this machine's spread |

Two conclusions carried into the wire work. First, the cost is the bytes, not the encoding:
80 B more header for a 64-byte payload costs 9 % of the message rate, and the same two keys on
a 64 KiB payload are noise. Second, the 215 B figure is the **worst case**, an explicitly named
producer spelled as `sha256:<64 hex>`. Decision 0008 §4.4 settled the encoding from these
numbers: the receiver already knows the sending peer's fingerprint from the handshake, so the
producer key is omitted entirely in the default case and the default pays only the sequence's
6 B; where the producer is not the connection peer the key is a CBOR `bstr` holding the raw
32-byte digest, 35 B instead of 74 B, and the hex string stays presentation only. B-013
landed exactly that codec and its golden vectors; the measured frame above is unchanged,
because the runtime still sets both fields to `None` (§1, fourth increment).

### Verified results — what parking a receipt costs the send path (B-032)

The drain of [0009](decisions/0009-drain.md) parks the receipt of every finished transfer
nobody is waiting on, which puts work on a path B-009 measured. Review asked for the number
before the design was accepted, so here it is, on the same bench, same machine, release
profile, loopback: `cargo bench -p weida --bench patterns -- header --warm-up-time 1
--measurement-time 3`, two runs per tree, throughput in Kmsg/s.

| Tree | `push_64b_keys0` | `push_64b_keys2` |
| --- | --- | --- |
| Before the drain (`9a82f4c`) | 163.0, 162.5 | 152.3, 153.6 |
| First cut: one runtime-wide `Mutex<VecDeque>`, reaping under the lock | 163.9, 162.7 | 147.6, 149.6 |
| Per connection: one uncontended lock, push only | 155.7, 162.2 | 147.6, 152.1 |

The first cut was the wrong shape regardless of the number — a structure shared by every
connection and every worker thread, plus a poll of quinn's connection lock under it, for an
operation that runs once at shutdown — and the restructure moved the parked set onto the
connection beside the sequencer and the gap detector, leaving the runtime-wide state with
only the admission flag, the connection list and the eviction counter.

**What the numbers do and do not show.** The keys2 column shows the first cut ~3 % below
the baseline in both runs and the per-connection version back inside it; that is the signal
that motivated the restructure. Nothing sharper than that is available from this bench:
this machine's run-to-run spread today is **wider** than the ±3 % B-009 recorded — the same
tree produced 155.7 and 162.2 Kmsg/s in consecutive keys0 runs, and lengthening the
measurement to 10 s per run did not settle it (baseline 149.0 and 162.4; per-connection
160.9, 163.6, 156.7). So the honest statement is: **the residual cost of parking is not
resolvable against this bench's noise**, and the claim in the code is worded as what it
does — one uncontended lock and a push, once per transfer — not as "free".

### Verified results — reassembly buffer under cross-stream reordering (B-010)

What an application-side reorder buffer costs when transfers complete out of dispatch order,
the second unmeasured cost of [0001](decisions/0001-sequence-field.md) §8. The probe is
`reverse_order_completion_measures_the_reorder_buffer` in `crates/weida/tests/streams.rs`: it
opens N transfers, writes a dispatch sequence into each, finishes them from the last to the
first, and reports each transfer's sequence from its own task, so what is measured is
completion order rather than the order a single-threaded reader imposes on itself. The buffer
is then simulated exactly: hold what cannot be released, release as soon as the next sequence
is present, record the peak. Debug build, loopback, same machine as the runs above; every
figure below was identical across five runs.

| Case | Command | Result |
| --- | --- | --- |
| N = 16, FINs queued back to back | `cargo test -p weida --test streams reverse_order_completion -- --nocapture` | **0 of 16** out of dispatch position, peak reorder buffer **0**. Reversing the FIN order changes nothing the application can see: the FINs are queued in one burst and quinn transmits the pending streams in its own order, so arrival order follows dispatch order |
| N = 256, FINs queued back to back | same | **90 of 256** out of dispatch position, peak reorder buffer **84** transfers (33 % of the transfers in flight), first divergence at position **29**. The first 29 arrive in order; beyond that, concurrent per-transfer tasks are the reordering source, not the FIN order |
| N = 16, each FIN awaited to its transport receipt | same | **16 of 16** out of dispatch position, peak reorder buffer **15** = N − 1, arrivals exactly reversed (`[15, 14, 13, …]`). Serializing the FINs is what makes the sender's order reach the receiver, and then the buffer must hold every outstanding transfer but one |

Two results matter for the reassembly mode of 0001 §7.5. First, the bound is N − 1 and it is
reachable: an eager reassembler must be capped by construction, which is what the INVARIANTS
follow-up of 0001 §8 asks for — the cap is a configured number of held transfers, not a hope
about arrival order. Second, at 256 transfers in flight a third of them were already held
without any adversarial pattern, so the cap has to be chosen against normal concurrency rather
than against the worst case alone. The probe asserts only the invariants — every transfer
arrives exactly once, the buffer drains empty, the peak stays below N — because QUIC promises
no ordering across streams and a test that pinned 84 would pin an accident of this machine.

### Verified results — what a connection to a peer costs (B-011)

The figure [0002](decisions/0002-control-and-bulk-separation.md) needs: what the *second*
connection to a peer that is already connected costs, in latency and in memory. The bench is
`crates/weida/benches/connections.rs`, loopback, pinned trust, no client identity (mTLS off).

The pool keys on `(host, port, ClientTls, address fingerprint)`, so a second connection to one
server on the same terms cannot be dialled from one runtime today — which is precisely why
0002 needs a second pool tier. The memory figures therefore use one client runtime per
connection against one server, with both ends in this process, and they separate the two
deltas: runtimes are built and measured *before* anything is dialled, so the per-connection
number excludes the per-runtime quinn endpoint and UDP socket.

| Check | Command | Result |
| --- | --- | --- |
| Cold handshake | `cargo bench -p weida --bench connections -- --warm-up-time 1 --measurement-time 3` | `connect/cold_handshake` **1.04-1.10 ms** over three runs (fresh runtime per iteration, only `connect` timed) |
| Pooled dial | same | `connect/pooled_dial` **3.8 µs** (3.27-4.36 µs): dialling a peer the pool already holds does no handshake and is ~**280x** cheaper than one |
| Handshakes in series | same, printed by the bench | 64 handshakes in **65.6-73.9 ms**, i.e. ~1.0-1.2 ms each with no measurable degradation: the first is 1.2-1.9 ms and the sixty-fourth 0.81-0.97 ms |
| Memory per idle runtime | same | **0-4 KiB** RSS: a `Runtime` that has dialled nothing holds nothing worth counting |
| Memory per live connection | same | **488-596 KiB** at 2 connections, **750-850 KiB** at 64 (46-53 MiB for 64), covering **both** ends. The single-connection figure is 1196-1260 KiB because it also pays the one-off crypto and endpoint state |

What this says about a second connection per peer: it costs about **one millisecond of
handshake and under a megabyte of resident memory for both ends together**, and the handshake
is the whole cost — there is no per-connection cost that grows with the number of connections
held. Against that, the head-of-line coupling one connection per path removes is unbounded:
one slow reader stalls every writer on the connection ([PATTERNS.md](PATTERNS.md) §1.3). That
is why the per-path split is worth a connection each and was built (B-017). It is also why the
*control* tier is not: a millisecond is cheap for isolation and expensive for nothing, and
[decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3 shows the tier would carry
nothing in v0. These numbers also set the scale for the per-peer ceiling: 64 connections to
one peer is ~50 MiB of transport state on the pair, which is why
`max_connections_per_peer` (64) is a named bound ([INVARIANTS.md](INVARIANTS.md)) rather than
the path count being the only one.

RSS is read from `/proc/self/status` `VmRSS` and the allocator does not return everything
between measurements, so each row's baseline is the previous row's residue; the per-connection
deltas are the trustworthy part and the absolute totals are not.

### Verified results — connections per dialled path (B-012)

The same bench, extended with the per-path fan of
[0002](decisions/0002-control-and-bulk-separation.md) and the point at which a server refuses.
Two shapes are measured because they are two different systems: today the pool keys on
`(host, port, ClientTls, address fingerprint)` and **not** on the path, so every path a runtime
dials shares one connection; 0002's bulk tier makes it one connection per path.

| Check | Command | Result |
| --- | --- | --- |
| 16 paths, pooled (today) | `cargo bench -p weida --bench connections` | **1.13-1.15 ms** for all 16 dials: one handshake and fifteen pool hits, 16 `(connection, path)` peer entries over **one** QUIC connection |
| 256 paths, pooled (today) | same | **1.47 ms** for all 256 dials, 256 peer entries over **one** connection. The fan does not exist yet: dialling more paths costs microseconds, not connections |
| 16 paths, one connection each (0002) | same | **22.6-23.6 ms** total, **1.41-1.47 ms** per path |
| 256 paths, one connection each (0002) | same | **277.7 ms** total, **1.08 ms** per path — no degradation with the count, and 995 KiB of RSS per connection for both ends together, consistent with B-011's 750-850 KiB at 64 |
| The refusal point | same | with `max_connections = 8`, connections 0 through 7 are accepted and the ninth dial fails with `Error::LimitExceeded` — "resource limit exceeded". The server completes the handshake first and then closes with `LIMIT_EXCEEDED` on purpose, so a peer can tell overload from a routing mistake (`crates/weida/src/listener.rs`) |

The consequence, now implemented (B-017): **a per-path connection is affordable in time and
linear in memory, but it converts a path count into a connection count against
`max_connections`** — a default of 1024 accepted connections per binding is 1024 paths' worth
of fan from a *single* client if nothing else bounds it, which is why `max_connections_per_peer`
(64) exists and counts per proved fingerprint
([INVARIANTS.md](INVARIANTS.md), [PROTOCOL.md](PROTOCOL.md) §10). A client that dials 256
paths pays 278 ms of handshakes where it paid 1.5 ms with one pooled connection, so a path
dialled is not a path used: connections are created on the first dial of a path and never
pre-created, which is what the pool does. No control connection is added on top
([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3).

**One regression found and fixed (B-025).** `connect/cold_handshake` had moved from
1.02-1.10 ms to **1.18-1.24 ms** (+12 %, p = 0.00) against criterion's stored baseline, which
predated B-016. The cause was in the code rather than inferred: `Exec::resolve` allocated the
host string, spawned a task and awaited its join handle on **every** `connect`, including for a
literal `127.0.0.1` that needs no lookup at all. `Exec::resolve` now parses an IP literal in
place and keeps the task only for real hostnames, which `lookup_host` needs for its Tokio
context. Measured after the fix: **1.05-1.09 ms**, `change: −11.6 % (p = 0.00)` — back to the
pre-B-016 level. Every pinned deployment dials literals, because a pinned address names a key
rather than a name, so this was the common path and not an edge case.

### Verified results — segment matching in the fan-out path (B-021)

The number [0007](decisions/0007-topic-namespace.md) §6 asked for. The objection the decision
overrode was that a matching language belongs nowhere near a publisher's hot path
([0007](decisions/0007-topic-namespace.md) §4.6); this is what it costs.

The `filters` group of `crates/weida/benches/patterns.rs` gives one connection *N* filters that
the published topic matches **none** of, so `publish` returns 0 and the measurement is *N*
matcher calls plus the fixed cost of a publish — no enqueue, no copy, no fan-out. Running the
same shape at `N = 1` and `N = 64` subtracts the fixed cost out: the difference over 63 is what
one more filter costs.

| Case | Command | Result |
| --- | --- | --- |
| Literal mismatch | `cargo bench -p weida --bench patterns -- filters` | 93.3 ns at one filter, **1.020 µs** at 64 → **14.7 ns per filter** |
| `*` in the middle | same | 97.4 ns at one filter, **999 ns** at 64 → **14.3 ns per filter** |
| Trailing `#` | same | **1.047 µs** at 64 → **15.1 ns per filter** |
| The replaced matcher, for scale | same | 64 `topic.starts_with(filter)` comparisons in a tight loop: **29.3 ns**, i.e. **0.46 ns each** |
| One matched fan-out, for scale | `cargo bench -p weida --bench patterns -- fanout` | `pub_1kib_8_subscribers` **64.5 µs** for publish plus eight receives |

**The shape does not matter.** A walk that fails in the first segment, one that has to cross a
`*` to reach the third, and one that meets a trailing `#` differ by 5 % — inside the spread of
this machine. That is the answer to the objection: the per-filter cost is dominated by
iterating the subscription registry, not by the grammar, and `#` being legal only in the final
segment is what keeps it that way (no backtracking, so the walk is linear in the filter).

**The scale is what settles it.** Sixty-four non-matching filters cost about **1 µs** of a
publish, against **64.5 µs** for one matched fan-out to eight subscribers — under 2 % — and a
publisher hits `max_subscriptions = 256` long before that becomes visible. The byte prefix it
replaced would have been ~0.5 ns per comparison in isolation, so a hot-path-only argument could
have preferred it; what it could not do is express a segment boundary at all, which is the
trade 0007 recorded and these numbers price.

### Verified results — the dedup key's allocation per call (B-040)

`DedupWindow::is_duplicate` builds its lookup key with `scope: scope.into()`, which allocates
a `Box<str>` on **every** call — including the two paths that insert nothing: a duplicate that
is found, and the probe before an insert. The worker who wrote B-034 noticed it and refused to
restructure the key on suspicion; this is the number that decides it.

It cannot be measured through the public surface. `DedupWindow` is `pub(crate)` and its only
public path is a negotiated connection receiving DATA, where one loopback message costs
microseconds and would bury a nanosecond answer — so the `dedup_key` group of
`crates/weida/benches/patterns.rs` measures the two *shapes* as pure functions, the way B-021
measured the byte prefix it had replaced. Tables hold `max_dedup_entries = 4096` entries over
eight scopes, with a 19-byte scope string. Two runs, release, same idle machine.

| Shape | Miss | Hit |
| --- | --- | --- |
| **owned** — today: one flat `HashMap<Identity, _>`, key allocates per probe | **41.1-41.3 ns** | **43.8-48.1 ns** |
| the same table, key built once (the allocation removed, nothing else) | 31.4 ns | 35.1-35.6 ns |
| **borrowed** — the candidate: `HashMap<Box<str>, HashMap<(producer, sequence), _>>`, outer lookup borrows `&str` | 42.0-42.8 ns | 44.7-46.3 ns |

`cargo bench -p weida --bench patterns -- dedup_key --warm-up-time 1 --measurement-time 3`

**The allocation costs ~10 ns**, which the middle row isolates: 41.2 against 31.4 on a miss,
and the same gap on a hit.

**And removing it does not pay.** The candidate shape is *slower* on a miss — 42.0-42.8 ns
against 41.1-41.3 — and the same within noise on a hit, because a two-level table hashes twice
and the second hash costs what the allocation cost. That is the whole finding: the obvious
optimization buys nothing, so the flat key stays and `dedup.rs` now says so with the number
beside it. For scale, 10 ns is **0.13 %** of a 1 KiB push (~7.9 µs), and deduplication is
opt-in — a connection that negotiated `core` allocates nothing here at all, which the
capacity assertions in `dedup.rs` already prove.

---

### Verified results — what the ZMTP bridge costs, against no bridge (B-043)

The comparison [adapters/zmtp.md](adapters/zmtp.md) §10 item 6 asks for: the same work through
the adapter and with no adapter in it, the foreign peer being the pure-Rust `zeromq` crate
0.6 on both sides of the comparison. `direct` is zmq.rs to zmq.rs over loopback TCP with no
weida at all; `bridged` is zmq.rs to the inbound bridge to a weida endpoint — one TCP hop, one
QUIC hop and two protocol terminations. One process, one machine, so what the difference
contains is the bridge and the second transport rather than a network.

| Exchange | Payload | direct (zmq.rs ↔ zmq.rs) | bridged (zmq.rs → weida) | Ratio |
| --- | --- | --- | --- | --- |
| REQ/REP round trip | 1 KiB | **20.4-20.6 µs** | **80.9-81.4 µs** | 4.0× |
| REQ/REP round trip | 1 MiB | **437-441 µs** | **3.34-3.41 ms** | 7.7× |
| PUSH one-way | 1 KiB | **4.28-4.31 µs** | **5.16-5.28 µs** | 1.2× |
| PUSH one-way | 1 MiB | **202-224 µs** | **1.43-1.45 ms** | 6.8× |

`cargo bench -p weida-zmq-bridge --bench interop -- --warm-up-time 1 --measurement-time 3`

**What the one-way rows do and do not say.** A PUSH that returns is a message accepted by a
socket, not delivered to anybody: the direct 1 MiB row at 4.6 GiB/s is zmq.rs buffering into
its own queue, and the bridged row is bounded by the bridge actually reading the message and
forwarding it. So the honest reading of PUSH is the small-payload row — **+0.93 µs per
message** for a whole ZMTP termination, a weida DATA header and a QUIC stream — and the
round-trip rows are where a real end-to-end cost appears.

**The cost is linear in size with no cliff**, which is the finding the two open questions
needed. Between 1 KiB and 1 MiB the bridged round trip grows 41× for 1024× the bytes, and
nothing in the adapter changes behaviour at a threshold in between.

**§11's first open question, `max_message_bytes`, is therefore not a latency choice: it is a
memory one, and it is now 1 MiB** (was 8 MiB, a number borrowed from `subscriber_buffer_bytes`
for lack of anything better). The bridge holds at most one whole message per direction per
connection, because a ZeroMQ peer cannot be handed a body before it is complete — so the
exposure is `max_message_bytes × max_connections`, and at 8 MiB against the default 1024
connections that product was **8 GiB** nobody had chosen. 1 MiB is `stream_receive_window`,
the per-stream budget the bridge's own reads already live inside [PROTOCOL §10], and it is
three orders of magnitude above the payload size these patterns are for.

**The same arithmetic corrected a second bound the question had not asked about.** The PUB
side's queue was `queue_depth: 1024` *messages*, which multiplies by the cap into the real
figure: a slow subscriber could pin a gigabyte. It is now `queue_bytes: 8 MiB` — a byte budget,
dropping the oldest until the new message fits, with `subscriber_buffer_bytes` as the weida
neighbour it can be compared against. A message larger than the whole budget is still queued
alone, because it passed `max_message_bytes` and a ZMTP message cannot be split.

**The reply deadline of B-042 stays at 10 s, now with a ratio behind it.** The slowest exchange
the cap allows measures 3.37 ms, so the default is ~3000× the working range: far enough above
it that a merely slow peer cannot trip it, which is the only failure mode that matters — a
request lost to a silent ROUTER costs one exchange, a deadline that fires early costs correct
ones.

### Verified results — what the local transports cost against QUIC (B-059)

The number [0010](decisions/0010-local-transport.md) §4.2 chose "the OS connection **is** the
stream, one per transfer" without: the same patterns over all three transports, at a payload
where the overhead is the whole figure and at one where the copies are. The bench is
`crates/weida/benches/transports.rs`, same machine as the runs above, release profile, both
ends in this process, loopback for the QUIC rows. Two runs; the spread below is the two runs'
point estimates.

`cargo bench -p weida --bench transports -- --warm-up-time 1 --measurement-time 3`

| Pattern | Payload | QUIC (loopback) | inproc | `AF_UNIX` |
| --- | --- | --- | --- | --- |
| Req/Rep round trip | 1 KiB | **58.3-58.8 µs** | **13.2-13.5 µs** (4.4×) | **50.6-50.8 µs** (1.2×) |
| Req/Rep round trip | 1 MiB | **2.70-2.73 ms** | **144-145 µs** (18.6-19.0×) | **266-312 µs** (8.7-10.3×) |
| Push one-way | 1 KiB | **7.97-8.08 µs** | **2.03-2.07 µs** (3.9×) | **24.0-24.1 µs** (0.33×) |
| Push one-way | 1 MiB | **1.358-1.362 ms** | **57.7-79.5 µs** (17.1-23.6×) | **130-147 µs** (9.2-10.5×) |
| RSS per live connection | — | **995 KiB** (B-012) | **430-1761 B** | **0-655 B** |

The ratio in brackets is against the QUIC row; above 1 means the local transport is that much
faster. B-043's reading applies to the one-way rows and is the reason Req/Rep is the
comparison: a Push returns when the transport has taken the bytes, which locally is a kernel
buffer or a channel slot, so those rows compare enqueue rates and not delivery. The memory
row is the delta over 200 live transfer connections read from `VmRSS`, and it varies with
what the process already has: 1761 B (inproc) and 655 B (`AF_UNIX`) per connection when the
report runs alone, 430 B and 0 B after the timing rows have already grown the heap. Either
way it is **two to three orders of magnitude** under the 995 KiB a QUIC connection costs, and
at the low end it is below what RSS can resolve.

**What one connection per transfer costs, plainly.** Compare the two local transports at the
same payload: the only structural difference between them is that `AF_UNIX` mints a real OS
connection per transfer where inproc mints a channel pair. At 1 KiB that is **22 µs** per
one-way message (24.0 against 2.03) — a `connect`, a preamble write, an accept and the
credential check on the far side, all per message. At 1 MiB it is **~70 µs** against a figure
dominated by copying, i.e. the same fixed cost, no longer visible as a ratio. Holding the
connections open costs nothing worth counting: 200 live ones are 0-128 KiB of RSS in total.

**Where the answer changes is the small end, not the large one.** At 1 MiB the choice is free:
`AF_UNIX` beats loopback QUIC 9-10× on the round trip and inproc ~19×, and the per-transfer
connection is lost in the copies. At 1 KiB the same fixed 22 µs is most of a local round trip,
and an `AF_UNIX` round trip is only **1.2×** faster than a full QUIC one over loopback —
7.7 µs of margin against a transport that does TLS, congestion control and packet framing.
On the one-way row it has already crossed: a 1 KiB Push over `AF_UNIX` costs **3× a QUIC
one** (24.0 µs against 8.0), because QUIC opens a stream inside a connection it already has
while `AF_UNIX` opens a connection. So 0010 §4.2's structural argument holds for bulk and for
in-process work, and the transport to reach for at high message rates over `AF_UNIX` is the
one weida does not have — which is the price of not reimplementing QUIC's stream layer, now
with a number beside it.

**Two bugs, both found by running this bench rather than by reading.** First, a receipt
parked for the drain holds its send half, which locally is a descriptor, and the parked set
was sized by QUIC's stream budgets — so a sequential run exhausted `max_local_streams` with
nothing in flight (`81d70e7`, and the parked set is now bounded by half the local ceiling).
Second, the Push rows could not complete at all: a transfer holds its slot until both ends
are done with it, so a sender that outran its puller hit the ceiling and `open` **refused**
with `LimitExceeded` — `Reject` where [GUARANTEES.md](GUARANTEES.md) §6 promises `Block`. A
local `open` now waits for a slot exactly as a QUIC `open` waits on the peer's stream budget,
cancelled by dropping the future and bounded by the caller's own deadline;
`push_pull_waits_for_a_slot_over_{inproc,unix}` pins it and fails on the old code at the
256th send.

### Verified results — what a payload costs crossing into Python (B-112)

The number [0014](decisions/0014-parallel-libraries.md) §2 asks for by name: the `bytes`
boundary of `weida-py-core` "avoids a copy where PyO3 allows it and measures the copy where
it does not". Same machine as the runs above, release profile, `bytes` objects of four sizes,
the GIL held throughout as it is during a real conversion.

`cargo bench -p weida-py-core --bench bytes_boundary -- --warm-up-time 1 --measurement-time 3`

| Direction | 0 B | 64 B | 1 KiB | 1 MiB |
| --- | --- | --- | --- | --- |
| Python → Rust, **borrowed** (`payload`) | **16.5 ns** | **16.7 ns** | **17.0 ns** | **16.9 ns** |
| Python → Rust, owned (`payload_of`) | **11.0 ns** | **18.7 ns** | **28.9 ns** | **19.19 µs** |
| Rust → Python (`py_bytes`) | **6.5 ns** | **14.3 ns** | **42.8 ns** | **18.93 µs** |

**The borrowed row is flat, and that is the proof rather than the claim.** 16.5 ns at zero
bytes and 16.9 ns at a mebibyte is a reference count and a type check; nothing is copied, so
nothing grows with the payload. That is the path a binding takes when it only *reads* a
payload — a subscription prefix, a key, a length — and `weida-zmq-py` takes it for every
`subscribe`.

**The two copies that remain are forced, one by each language.** A `weida_zmq::Message` owns
a `Vec<u8>` because a frame outlives the call that queued it and is written by a reactor
thread long after, and CPython's allocation cannot become a Rust one: that is the owned row.
A CPython `bytes` object owns its storage inside its own allocation and no API — limited or
otherwise — adopts a foreign buffer: that is the Rust → Python row. At 1 MiB both are
~19 µs, i.e. **~55 GiB/s**, which is a `memcpy` and an allocation and nothing else; the
small sizes are the object, not the bytes.

**What was deliberately not done.** A `memoryview` over Rust-owned bytes would make the
Rust → Python direction zero-copy, and it is not used: the buffer protocol entered CPython's
limited API in 3.11 while B-058's wheel is `abi3` from 3.9, so it would be a zero-copy path
that exists on some interpreters and not others — a performance cliff rather than a feature.
The cost of not having it is the last column, 19 µs per mebibyte, against a 1 MiB local
round trip that the table above measures at 144 µs *inproc*: **about 13 %** of the cheapest
transport weida has, and proportionally less on anything that crosses a socket.

### Verified results — what a Python round trip costs against the Rust one (B-117)

The comparison B-117 asks for. Same machine as the runs above, release profile for both
halves, and the same shape on each side so that the difference is the language boundary and
not the measurement: one REQ/REP round trip — request out, reply back — over `inproc://` and
over loopback `tcp://`, at an empty payload and at 1 KiB, median of 2000 round trips.

```text
cargo run -p weida-zmq --release --example roundtrip_cost
.venv/bin/python crates/zmq/weida-zmq-py/roundtrip_cost.py
```

| Round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| `weida-zmq`, Rust | **16.5 µs** | **17.5 µs** | **32.4 µs** | **35.3 µs** |
| `weida_zmq.sync`, Python | **67.7 µs** (4.1×) | **72.3 µs** (4.1×) | **111 µs** (3.4×) | **116 µs** (3.3×) |
| `weida_zmq`, Python asyncio | **330 µs** (20×) | **334 µs** (19×) | **343 µs** (10.6×) | **346 µs** (9.8×) |
| `pyzmq` (libzmq 4.3.5) | **5.7 µs** | **6.0 µs** | **45.8 µs** | **47.2 µs** |

**The payload is not what costs.** 1 KiB adds 1-5 µs on every row, which is the copy
measured in B-112 above. Everything else in the gap is per-*operation* overhead, and a round
trip is four operations, so each row's excess divides by four for a per-call figure.

**The synchronous surface costs ~13 µs per operation over Rust** (67.7 µs against 16.5 µs,
four operations). That is a GIL release and re-acquire plus a thread handoff: the facade
parks the calling thread in `futures::executor::block_on` and a reactor worker has to wake
it. It is the price of a blocking API over an asynchronous library, and it is the same price
`weida-zmq`'s own `blocking` facade pays in Rust.

**The asyncio surface costs ~78 µs per operation, and the reason is one loop wakeup per
`await`.** Each awaited call creates a future on the running loop, the operation completes
on a reactor thread, and `loop.call_soon_threadsafe` writes to the loop's self-pipe so that
`epoll` wakes and the callback runs. That wakeup — not the GIL, not the copy, not the
protocol — is the 78 µs, and four of them are 313 µs of the 330. **There is a fast path this
did not take**, filed as B-179 rather than hinted at: an operation that can complete
without waiting (a `recv` whose message is already queued, a `send` with room) polled once
inline and handed back as an already-resolved future, which `await` consumes without
yielding to the loop at all.

**B-179 took it, and the saving is measured on the same script.** `Bridge::awaitable`'s
first step now polls the future once on the loop's thread, inside the reactor's context and
with a no-op waker; a ready outcome completes the delivery future in place and nothing is
spawned, a pending one is spawned as before and re-registers its task's waker on its next
poll. Measured on an idle machine — the table above was taken with four worktrees compiling,
so its absolute numbers are higher — before and after, release profile, median of 2000:

| `weida_zmq` asyncio round trip | inproc, 0 B | inproc, 1 KiB | tcp, 0 B | tcp, 1 KiB |
| --- | --- | --- | --- | --- |
| before, one wakeup per `await` | 145 µs | 144 µs | 157 µs | 155 µs |
| after, ready futures resolved inline | **78 µs** | **82 µs** | **125 µs** | **122 µs** |
| `weida_zmq.sync`, same runs | 17.8 µs | 19.1 µs | 39.1 µs | 40.4 µs |

A round trip is four operations and two of them are sends into a queue with room, so two
of the four wakeups go: **−67 µs on inproc, about 33 µs per wakeup saved on this machine**,
which is the per-`await` cost with the loop otherwise idle. The two receives still wait for
the peer and still pay the wakeup, which is why tcp saves less: the reply takes longer to
arrive than a queue takes to have room. Nothing pinned by B-111 and B-112 moved: the future
still starts at the first `await` (the inline poll runs inside the coroutine's first step,
after it has been scheduled), a cancelled `Task` still aborts the reactor task, a panic still
arrives as a `RuntimeError`, and the no-running-loop error is still raised before anything
is polled.

**Against `pyzmq` the honest reading has two halves.** Over `inproc://` libzmq is 12× faster
than this binding's synchronous surface and 58× faster than its asyncio one, because libzmq's
`inproc` is a memcpy between two threads of one process with no reactor in the middle. Over
`tcp://` the gap closes and reverses on the synchronous surface at the top end — 111 µs
against 45.8 µs — and the Rust library is *faster* than libzmq (32.4 µs against 45.8 µs),
which is where the implementation actually competes. The binding's overhead is a Python
problem to solve in the bridge, not a protocol one.

---

## 5. Decisions

### Serialization: CBOR via `minicbor` (Phase 0)

Master doc §82 leaves the choice between MessagePack, CBOR and another compact evolvable
format open, with criteria: parsing speed, encoding speed, allocations, unknown-field
support, multi-language implementations, dependency cost, fuzzability, and deterministic
representation where needed. Phase 0 mandates selecting one.

CBOR is selected, encoded and decoded with `minicbor` (feature `alloc`):

- **Integer-keyed maps** give compact headers and a stable key space, decoupling the wire
  from field names.
- **Unknown-field support** is straightforward: unknown uint keys are skipped, satisfying
  master doc §15's requirement that unknown optional fields be ignorable.
- **No transitive dependencies**, satisfying the dependency discipline of master doc §72.
- **Multi-language support** is broad and mature (RFC 8949), which matters for the Phase 10
  bindings and the Phase 8 browser client.
- **Fuzz-friendly**: the codec is a pure function over bytes, so it fuzzes without a socket.
- **Deterministic representation** is achievable within our own encoder rules: definite
  length, ascending keys, minimal integers ([PROTOCOL.md](PROTOCOL.md) §5).

Encoders and decoders are hand-written rather than derived, because the strictness the
protocol requires — duplicate-key rejection, per-key string caps, cap-before-allocation,
depth-limited skipping — is not expressible through a derive macro. If `minicbor`'s public
API proves unable to express these bounds, the fallback is a hand-rolled CBOR-subset codec
inside `weida-protocol`; the wire bytes and the golden vectors do not change either way.

### Other Phase 0 decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| URL scheme | `weida://` | The master doc's `mq://` is a working name (§82). Single `SCHEME` constant in `crates/core/src/addr.rs`. |
| ALPN token | `weida/0` | Couples the TLS-level protocol identity to the wire protocol version, so a version-0 peer cannot silently talk to a future version. |
| Stream magic | `0x57` (ASCII `W`) | Cheap first-byte rejection of non-weida streams. |
| Wire protocol version | `0` | Experimental per master doc §15; independent of the library version. |
| Library version | `0.1.0` | Independent of the wire version. |
| Client trust | pinned public keys **or** configured anchors | A peer is accepted for a pinned fingerprint or for a chain to an explicitly configured anchor that names the host dialled. No platform root store and no insecure-skip mode ships in v0, and `Trust::by_address()` trusts nothing beyond what an address names. |
| TLS material source | file **or** in-memory PEM (`Pem`) | Requiring a path would force callers holding a key from a secret store to write it to disk first. Both sources are first class; `Identity::from_pem` and `Trust::anchor` never touch the filesystem. |
| Credential placement | identity per **binding**, trust per **dialling endpoint** | Credentials are transport-specific, so they belong neither on the Listener (a namespace) nor on the Runtime (a resource container). Consequence: the connection pool keys on `(host, port, ClientTls, address fingerprint)` — sharing on authority alone would hand one endpoint a peer authenticated on another endpoint's terms. |
| Rust edition | `2024` | Current stable edition. |
| MSRV | `1.88` | Highest requirement among the pinned dependencies. |

### Phase 3 re-architecture decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Req/Rep transport | one client-opened **bidirectional** stream per exchange | The stream is the correlation. Deletes correlation ids, the per-connection pending table, the `Correlator` actor, `ReplyArrived` and the CANCEL frame in one move, and lets request and reply stream simultaneously on independent halves. |
| Application acknowledgements | **not in the core**; reserved for the L2 broker (Phase 6) | Brokerless, an application ACK only means "arrived in RAM" — QUIC already retransmits and already reports transport receipt. Accepted / Stored / Replicated / Processed describe responsibility transfer to a hop that has taken responsibility; without such a hop the words are unearned ([GUARANTEES.md](GUARANTEES.md)). |
| Delivery signal | `OutgoingTransfer::finish() -> Delivery`, `Delivery::delivered().await` | The honest signal QUIC can actually give: quinn's `stopped()` resolving `Ok(None)` means the peer acknowledged every byte "although not necessarily the processing of it". `finish()` is synchronous and dropping the `Delivery` is free, so fire-and-forget senders pay nothing ([FAILURE_MODEL.md](FAILURE_MODEL.md)). |
| Misroute refusal code | QUIC application error code `UNSUPPORTED = 9` | With ERROR frames confined to the reply half of an exchange, a unidirectional stream sent to a path that does not serve it has no frame to be refused with. A dedicated stop code keeps the refusal typed instead of collapsing it into `REJECTED` ([PROTOCOL.md](PROTOCOL.md) §7). |
| Raw stream API | `Peer` and `Acceptor` are public (`crates/weida/src/stream.rs`) | L0 is the product, not an implementation detail: if the patterns are the only way in, every unanticipated topology needs a new pattern. `Peer::open`/`open_bi` and `Acceptor::accept` expose the two QUIC stream kinds directly; the patterns are thin wrappers over them. |
| Stream limits | `max_pending` removed; `max_concurrent_bidi_streams = 1024` added | There is no pending-reply table left to bound. Concurrent exchanges are bounded by QUIC itself instead, and the bidirectional budget was previously hardcoded to `0`. Worst-case header memory becomes `max_header_bytes * (uni + bidi)` = **48 MiB**, up from 32 MiB ([INVARIANTS.md](INVARIANTS.md)). |

### Phase 3 identity and trust decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| Peer identity | SHA-256 of the leaf's DER `SubjectPublicKeyInfo` (`sha256:<64 hex>`) | Hashing the public key rather than the certificate keeps a pin valid across a renewal that reuses the key, and it is the same value `curl --pinnedpubkey` and HPKP pin, so an operator can compute and compare it without weida. |
| Address may name the peer | `weida://[sha256:<hex>@]host:port/path` | One string then carries both where to dial and whom to accept — which is exactly what a discovery record, a config line or a line pasted into a terminal has to survive as. Reaching a self-signed peer safely needs nothing else configured. |
| Address fingerprint overrides `Trust` | the named identity is the only one accepted on that connection | The most specific statement of intent wins. An operator who wrote down which peer must answer did not mean "or anybody else my CA vouches for". |
| Empty `Trust` dials nothing but pinned addresses | `Error::Tls("nothing to trust …")`, before any packet | The only alternative to failing here is trusting everything, which must never be reachable by omission. An endpoint with no trust configured stays usable — for addresses that name their peer, and for nothing else. |
| Client identity | optional on the dialling side; a binding may require it | Master doc §6 makes client identity optional and §46 asks for mTLS. `ClientTls` therefore carries `Option<Identity>`, and `ServerTls::require_client(trust)` refuses anonymous and untrusted peers at the handshake. Requiring an empty trust is rejected at bind time, since it would accept nobody. |
| Certificate generation | `rcgen`, behind default feature `generate` | `Identity::generate()` is what lets a pinned deployment handle no PEM at all, so it is on by default; behind a feature so a deployment that only loads issued certificates does not link a certificate builder. |
| Fingerprint dependencies | `rustls-webpki` (SPKI parsing) and `ring` (SHA-256) as direct dependencies | Both were already in the tree through rustls, so naming them directly adds no third-party code and no build time, which is what master doc §72's dependency discipline asks. Neither parsing a leaf's SPKI nor hashing it is something rustls exposes. |

### Runtime ownership decisions (B-016)

| Decision | Value | Rationale |
| --- | --- | --- |
| Three constructors | `Runtime::new` (ambient reactor), `Runtime::with_handle(handle, config)`, `Runtime::owned(config)` | `quinn` needs a Tokio reactor and nothing else in the crate does. Making the reactor something the runtime holds — borrowed, handed over, or created — removes "you must already be inside `#[tokio::main]`" from weida's contract, which is the prerequisite for the Phase 10 bindings, where the host language owns the thread the call arrives on. |
| `worker_threads` | `RuntimeConfig::worker_threads`, default 1, `0` rejected | A messaging runtime is I/O bound, and every extra worker is a thread a library takes from its host process without being asked. `0` is a configuration error rather than something to correct silently ([GUARANTEES.md](GUARANTEES.md) §4). |
| Owned-runtime teardown | `shutdown_background()` from `Drop`, not a plain drop | The last `Runtime` clone may go out of scope on one of that runtime's own worker threads, where dropping a Tokio runtime panics inside a destructor — which aborts rather than unwinds. |
| One runtime surface | `Exec` in `crates/weida/src/runtime.rs`: `spawn`, `sleep`, `resolve`, `enter` | One place to see what the crate asks of tokio, and the thing that makes a non-tokio caller possible at all. `grep -n 'tokio::spawn\|tokio::time\|lookup_host' crates/weida/src` matches `runtime.rs` and one comment. |
| Client handshake placement | spawned **on** the runtime, awaited by the caller as a join handle | Completing a `quinn::Connecting` spawns the connection driver from inside the poll, so the polling thread would need an ambient reactor. Spawning it keeps `Runtime::connect` pollable from any executor and keeps the future `Send`, which holding an `EnterGuard` across the await would not. |
| `futures-io` beside `tokio::io` | both trait pairs on `OutgoingTransfer`/`IncomingTransfer`, `futures-io` delegating to the tokio impl | A caller on `futures`, `smol` or `async-std` should not have to wrap a compat shim around a payload stream. Delegating keeps the end-of-payload bookkeeping in one place. |
| `shutdown_timeout` | `RuntimeConfig::shutdown_timeout`, default 1 s (B-031) | `shutdown` closes every endpoint and then waited for the sockets to go idle **without a bound**, so the length of a process's exit was decided by the path rather than by the caller: measured against a peer that has gone silent, the draining period alone is **96 ms** on loopback and grows with round-trip time and loss. One second is generous on any network where a clean close was possible at all. The bound is one budget for the whole shutdown, not one per endpoint, because what a caller waits for is the call ([decisions/0009](decisions/0009-drain.md) §4.4). |
| Address selection | try every resolved address in order, capped by `Limits::max_resolved_addresses` (8), each attempt but the last bounded by `RuntimeConfig::connect_attempt_timeout` (250 ms) (B-029) | Taking the first address made `weida://localhost:…` unreachable wherever `localhost` resolves to `::1` before `127.0.0.1`, which is the common Linux ordering and this machine's. Sequential attempts with a per-attempt bound rather than RFC 8305's parallel happy-eyeballs: the failure being fixed is an address that answers *nothing*, where QUIC has no refusal to observe, and 250 ms is RFC 8305's own Connection Attempt Delay for exactly that case. Measured on the regression test: **30 s before, 1.3 s after**. The last address keeps the full handshake budget, so an IP literal and a single-address name behave exactly as before. |
| Loss cause | `Error::ConnectionLost(LossCause)` — `IdleTimeout`, `PeerClosed`, `LocallyClosed`, `Reset`, `TransportError` (B-028) | `PeerSet::pick` reported a bare `ConnectionLost` for every closed peer, and `conn_error` mapped `TimedOut`, `Reset` and an unrecognized application close to the same value, so the distinction did not exist anywhere in the API. A payload rather than new top-level variants: the *outcome* is identical in all five cases — nothing in flight completed, `is_definite_failure()` stays true — and only the next action differs, so a second outcome vocabulary would have been the wrong shape ([FAILURE_MODEL.md](FAILURE_MODEL.md) §4). Five causes because each one changes what an application should do; `pick` now reports the cause of the peer it rejected. |

---

### ZMTP codec decisions (Phase 9 slice 1, B-030)

| Decision | Value | Rationale |
| --- | --- | --- |
| Dependencies | **none**, not even `weida-core` | The half of an adapter that can be checked against a foreign specification must not be able to reach for weida's types, limits or error vocabulary, or the check becomes a check against our reading of the specification. `Cargo.toml` has an empty `[dependencies]` on purpose, and the crate carries its own error vocabulary rather than borrowing `weida_core::Error`. |
| Where the cap lives | an argument to every decode entry point, never a constant | ZMTP grants no credit, a long frame may declare 2^63-1 octets, and `ZMQ_MAXMSGSIZE` is unlimited by default, so the local limit is the whole defence. Passing it in means a bridge can use the weida-side number it already has (`subscriber_buffer_bytes`, 8 MiB) instead of a second, unrelated default invented here — which is [adapters/zmtp.md](adapters/zmtp.md) §11's open question, left open rather than answered by accident. The effective cap is `min(argument, 2^63-1)`, so a caller cannot switch the check off. |
| What a decoder returns | borrowed slices (`Command<'a>`, `Metadata<'a>`) | A decoded frame body is a slice of the caller's buffer and a command points into that body, so the only allocation on the way in is a `READY`'s property list. It is also what lets a bridge hand a payload to a weida transfer without a copy. |
| Illegal flag combinations | unrepresentable | MORE "SHALL be zero on command frames", so `FrameKind` is `Message { more }` or `Command` — the combination has no value in either direction, rather than a runtime check on the way out. |
| Incomplete versus violated | separate per layer | A frame header can legitimately be short (read more); a command body arrives whole, so a field running past its end is a violation with nothing to wait for. Three error types, each answering `is_violation()`, rather than one type whose `Incomplete` means different things at different depths. |
| Where the specification contradicts itself | follow the ABNF and libzmq, and say so | Command names are length-prefixed, not null-separated; an `ERROR` reason may contain spaces although `VCHAR` excludes them. Both are recorded in [adapters/zmtp.md](adapters/zmtp.md) §10.1 and in the crate docs, because an interop bug found later must be traceable to a decision rather than to an accident. |

---

### ZMTP inbound bridge decisions (Phase 9 slice 2, B-041)

| Decision | Value | Rationale |
| --- | --- | --- |
| Where it lives | its own crate, `crates/zmq/weida-zmq-bridge` | A feature on the codec crate would put a weida dependency in the codec's manifest, and that manifest being empty is what keeps the codec checkable against 37/ZMTP rather than against our reading of it ([ARCHITECTURE.md](ARCHITECTURE.md) §4). |
| The handshake state machine | in the bridge, not the codec | The codec deliberately shipped without one (B-030), so this slice decided its shape: `Session::handshake` sends the full greeting, reads the peer's, exchanges `READY`, and refuses with `ERROR` before closing on two things — a socket type §2's table forbids, and a `READY` naming no socket type at all. The second is a `SHOULD` in the specification and a MUST here: a bridge that does not know which pattern it is translating cannot translate it. |
| One socket type per listener | `Presenting::{Rep, Pull, Pub}` | The three whose ZeroMQ counterparts bind rather than connect ([ARCHITECTURE.md](ARCHITECTURE.md) §6c.4). It also makes §9.1's refusal — a dropping policy bridged onto a blocking one — unrepresentable rather than checked: the pattern pair follows from the presented type, so there is nothing to misconfigure. |
| Envelope versus multipart | consume `[empty, body]`, refuse everything else | REQ puts an empty delimiter on the wire and REP strips it, so that frame is envelope and is consumed and mirrored onto the reply; a bare `[body]` is accepted too, because a DEALER peer leaves the envelope to its application. Any other frame count is a genuine multipart message, refused: concatenating it would invent an application protocol weida does not have (loss L1, §9.2). |
| The topic on the PUB side | its own frame, ahead of the payload | A SUB peer matches a byte prefix against the start of the message, so the topic has to be where that match lands. The zguide's envelope convention says the same thing and gives the reason: "the match won't cross a frame boundary" (§6). |
| Subscriptions | reference-counted per raw prefix **and** per translated filter | ZeroMQ's SUBSCRIBE is not idempotent and weida's is (L3), so the weida side is told only when a count crosses zero. Two counts rather than one, because under the §9.3 opt-in two different prefixes can translate to the same filter, and cancelling one must not unsubscribe the other. |
| A refused subscription | `ERROR` to the peer, connection stays up | The only non-fatal error in the bridge. A SUB peer with one untranslatable prefix and three good ones must keep the three, and a silently ignored subscription is a subscriber that waits forever for messages nobody will send. |
| The weida-side reconnect | three attempts, then let the ZMTP connection fail | A bridge that retried forever would hide a weida outage from the ZeroMQ side, whose own reconnect loop is better at it: a ZMTP peer reconnects automatically, and a fresh connection rebuilds everything from that peer's own subscriptions — which is what `Subscriber::connect` re-sending its filters gives us, and why reconnection is not re-registration ([decisions/0008](decisions/0008-session-identity.md) §4.5). |
| A slow SUB peer | bounded queue, drop the **oldest**, count and log it | Both sides already agree that fan-out drops rather than blocks. Oldest rather than newest because a subscriber that has fallen behind wants the freshest data it can still be given, and counted because ZeroMQ's PUB drops silently and the zguide names that as a debugging problem. |

**What writing the tests found.** The PUB loop selects over the socket and the weida
subscriber, so the framed reader's `fill` is dropped routinely — and the first version grew the
buffer with `resize` before the await and truncated after it. A cancelled read left the
zero-filled slack behind, which decodes as a stream of empty message frames: the bridge then
refused its own buffer with "a SUB peer sent a message, which its socket type cannot do". The
fix is to read into scratch and append only after the read completes, which is what makes the
claim "cancel-safe" true; the comment on `fill` now records the version that looks equivalent
and is not.

---

### ZMTP outbound bridge decisions (Phase 9 slice 3, B-042)

| Decision | Value | Rationale |
| --- | --- | --- |
| Which side binds | the **weida** side | The mirror of the inbound slice is not symmetric. Inbound the bridge binds on the ZeroMQ side because that is where the foreign peers dial; outbound it binds a weida endpoint and dials the foreign peer, because the endpoint path is what weida applications address and an endpoint that nobody can name is not reachable. One `Outbound` is therefore one weida path in front of one foreign peer. |
| Req/Rep dials `DEALER`, not `REQ` | `Dialling::{Dealer, Pull, Sub}` | A weida `Replier` accepts concurrent exchanges ([ARCHITECTURE.md](ARCHITECTURE.md) §6b) and REQ is lockstep — "send and then receive exactly one message at a time" [zeromq §4.2] — so a REQ socket would serialize the very concurrency this side offers. DEALER carries the 28/REQREP envelope instead, and the bridge synthesizes it: an id frame it assigns, an empty delimiter, the body. |
| What pairs a reply with its exchange | the id frame, in a table keyed by it | Not arrival order: a foreign ROUTER may answer out of order, and even against a REP peer nothing on the wire says the replies come back in the order the requests left. The test drives four exchanges at once and has the peer answer them in reverse; an id-blind bridge fails it. |
| A reply that never comes | a deadline per exchange, then `ERROR{NO_REPLY}` | `ZMQ_ROUTER_MANDATORY` is an option on the ROUTER socket, and outbound that socket belongs to the peer — the adapter cannot set it and the loss (L5) arrives as silence. A weida requester hanging forever on somebody else's dropped message is the one outcome worth ruling out, so the exchange is refused with a typed error instead. What the deadline should be is measurement the interop bench owes ([adapters/zmtp.md](adapters/zmtp.md) §11). |
| The refusal needed a public API | `IncomingRequest::refuse(code)` | The runtime already wrote `ERROR` frames for its own routing refusals and on drop (`NO_REPLY`), but an application could only refuse by dropping the handle — which says `NO_REPLY` and nothing else. [decisions/0005](decisions/0005-refusal-race.md) §4.3 says the ERROR frame is written by the application; until this slice needed it, nothing did. |
| Heartbeats | `ZMQ_HEARTBEAT_IVL` on the adapter's own socket, never translated | 37/ZMTP's PING/PONG is the only liveness the ZeroMQ side has, and TCP's is not a substitute (§3 of the mapping document). It stays local to that hop: weida's `keep_alive`/`idle_timeout` are not derived from it and it is not derived from them, because a timer that crosses the adapter would let one side's idea of "dead" close the other side's healthy connection. |

---

### ZMTP interop decisions (Phase 9 slice 5, B-043)

Every row here exists because a foreign implementation disagreed with us. Nothing in this
table could have been decided from the specification alone, which is the argument for the
slice.

| Decision | Value | Rationale |
| --- | --- | --- |
| A ZMTP **3.0** peer | accepted by downgrading, not refused | The codec's floor was 3.1 ("a peer MUST accept protocol versions greater or equal to 3.1") and the other half of the same rule permits a downgrade. Refusing 3.0 refuses the entire installed base of implementations that never adopted 3.1 — `zeromq` 0.6 announces 3.0 — and for a *bridge* that is not caution but uselessness. `Greeting::accept_downgrading` is a separate entry point rather than a relaxation of `accept`, so a caller that wants the strict floor still has it, and it **returns** the negotiated version instead of assuming one. |
| `PING`/`PONG` toward a 3.0 peer | suppressed, whatever the configuration says | PING/PONG are 3.1 commands. Sending one to a 3.0 peer is a protocol violation, and the price is exact: `zeromq` answers any command but `READY` with "Unknown command received" and drops the connection. So the configuration asks for a heartbeat and the negotiated version decides whether it can be honoured, logged once per connection rather than left as a silent difference. |
| The legacy subscription form | **accepted** on the PUB side, never sent by default | 3.x subscriptions are `SUBSCRIBE`/`CANCEL` commands; ZMTP 2.0's form is a one-frame message beginning `1` or `0`, which is also how libzmq presents subscriptions to an XPUB application. `zeromq` 0.6 sends and reads only the legacy form, so a bridge that accepts only commands has no subscribers from that implementation. Accepting both is unambiguous — a SUB peer may not send application messages at all — and sending is a configuration choice (`SubscriptionForm`) whose default stays the specified command, because libzmq is the reference and a knob is better than a guess. |
| An over-cap payload | refuses **that transfer**, not the connection | Both loops used to propagate `LimitExceeded` and end the ZMTP connection over one oversized message the peer never saw. weida keeps refusals per stream — "a refusal is per-stream, a violation ends the connection" ([PROTOCOL.md](PROTOCOL.md) §3) — and `collect` has already sent `STOP_SENDING(REJECTED)` and buffered nothing, so the loop continues; the exchange form also refuses the requester with `ERROR{REJECTED}` rather than leaving it for the deadline. |
| `max_message_bytes` | **1 MiB**, from the bench | The cost is linear in size, so the number bounds memory, not latency: one whole message per direction per connection against `max_connections`, which at the old 8 MiB was 8 GiB nobody had chosen. See the verified results above. |
| The PUB-side queue | **bytes** (`queue_bytes`, 8 MiB), not messages | A depth of 1024 messages multiplies by the cap into the exposure that matters. Bytes make the ceiling the number an operator cares about and give it a weida neighbour to be compared against, `subscriber_buffer_bytes`. |
| `max_pending_exchanges` (B-053) | 64, checked **before** the body is read | The review pass found the outbound DEALER loop holding one request *and* its body per waiting exchange with no ceiling at all, the count being whatever weida clients open — a bound the transport does not supply, since it limits concurrent streams per connection and not the sum across clients. 64 is `max_connections_per_peer`'s number, and the product it fixes is stated rather than implied: 64 MiB against a 1 MiB cap. Checked before the body because a request refused early costs this side nothing, and refused *immediately* rather than parked because a requester that will never be answered should not wait out the deadline to learn it. |
| The subscription table's ceilings (B-054) | 256 prefixes of at most 256 B, refused with `ERROR`; a **repeat** always accepted | ZeroMQ subscriptions are additive and non-idempotent, which the sheet records as a denial-of-service surface in libzmq itself [zeromq §11] — so what needs a ceiling is the number of *distinct* prefixes and the length of each, since a repeat only increments a counter. Both numbers are weida's own for the same thing (`max_subscriptions`, and the wire's 256 B cap on a SUBSCRIBE `filter`), which makes the exposure comparable across the hop: 64 KiB of prefixes per peer. A repeat is accepted even at the ceiling because loss L3's count is the thing being preserved: refusing the second SUBSCRIBE would make the first cancellable by one CANCEL where the peer sent two. |

---

### Connection-tier decisions (B-017)

| Decision | Value | Rationale |
| --- | --- | --- |
| Pool granularity | one connection per `(authority, terms, expected fingerprint, **path**)` | A QUIC connection's receive window is shared, so the only way two flows cannot stall each other is for them not to share a connection ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2). Measured as behaviour, not asserted: `a_stalled_path_does_not_stall_another_path` fills one path's window until a write parks and then sends on another path. Reverting the key to one connection per peer makes that test time out, which is the coupling the decision set out to remove. |
| The control tier | **not built, and now parked by decision** | In v0 a control connection would carry nothing: every connection performs its own HELLO (§2.2 of [PROTOCOL.md](PROTOCOL.md)), the credit frame of §11 does not exist, and SUBSCRIBE could not move there because a publisher writes fan-out on the connection the SUBSCRIBE arrived on and no field names a peer's other per-path connection. That question is settled by [decisions/0011](decisions/0011-answered-where-it-arrived.md): a frame naming a path rides that path's connection, which takes SUBSCRIBE/UNSUBSCRIBE *and* the reserved credit frame out of the tier's cargo and leaves it empty, so it is parked with a revival condition (0011 §4.3) rather than left blocked. The named residual coupling is a SUBSCRIBE behind payload on the *same* path (0011 §4.4). |
| `Limits` scope | a **per-connection** profile; runtime-level numbers moved to `RuntimeConfig` | 0002 §6.4 wants one profile per tier. Two profiles are only honest if every field means something in both, so `max_connections`, `endpoint_queue` and `max_resolved_addresses` — per binding, per endpoint, per dial — left `Limits`, and `keep_alive`/`idle_timeout` joined it, because those *are* per connection. The second profile itself waits for the tier: a `Limits::CONTROL` nothing reads would be five numbers nobody has to justify. |
| Peer binding | compare against the peer's **live** connections, refuse a mismatch with `Error::Untrusted(fp)` | The fingerprint is the only thing that binds a peer's connections ([decisions/0008](decisions/0008-session-identity.md) §4.2), and dialling a second path is dialling the same peer. Live connections rather than a remembered value, because a peer is this peer only while a connection to it lives: once the last one is gone, a replacement server with a new key is a new peer and nothing should still be objecting to it. Proved by swapping a raw server's identity between two handshakes on one socket — the load-balancer case, which two `quinn` endpoints cannot reproduce because they cannot share a port. |
| `max_connections_per_peer` | 64, counted per proved fingerprint, released on close | One connection per path lets the dialling side choose the count, so `max_connections` alone would let one peer fill a binding. 64 is the number B-011 measured (~50 MiB of transport state across both ends for 64 connections to one peer) and a sixteenth of the default `max_connections`. Anonymous connections are not counted together: two of them cannot be shown to be one peer, so counting them as one would refuse strangers for each other's traffic. |

---

## 6. Known debt and deferred work

Recorded deliberately, not discovered later.

- **OpenTelemetry SDK wiring deferred.** v0 propagates W3C `traceparent` and `tracestate` in
  transfer metadata and emits structured logs through `tracing`. No OTel exporter, no span
  export pipeline. That belongs to the observability work; the wire fields it needs are
  already carried, so enabling it does not change the protocol.
- **No retries.** A refused, lost or `Indeterminate` transfer is reported to the caller and
  never retried by the library ([GUARANTEES.md](GUARANTEES.md) §6). Retry policy arrives in
  Phase 4.
- **No persistence.** No payload store, no WAL, no recovery. Phase 5.
- **Deduplication is opt-in and bounded, not absent.** The entry here used to say weida had
  no dedup at all and no identifier to deduplicate on; both stopped being true with DATA keys
  6 and 7 ([decisions/0001](decisions/0001-sequence-field.md)). A connection that negotiated
  `Deduplication::Bounded(window)` suppresses a repeat of `(producer, scope, sequence)`
  inside `max_dedup_entries` (4096), and a `core` connection allocates nothing for it. What is
  still absent is an **application-level idempotency id** — the key is the producer's sequence,
  which only a sender that opted into per-producer ordering writes — so an `Indeterminate`
  result may still only be retried for idempotent operations, or by a sender that numbers its
  own transfers ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5).
- **Single reply per exchange.** The reply half carries one DATA transfer or one ERROR and
  then FIN. Answering one request several times needs either several exchanges or a framing
  convention inside the payload; v0 offers neither.
- **Capability codes unused.** HELLO carries `capabilities` and `required_capabilities`, but
  no code is assigned and the supported set is empty.
- **Application acknowledgements are absent, not partial.** Accepted, Stored, Replicated and
  Processed are reserved for the Phase 6 broker: no wire representation, no code point, no
  `UNSUPPORTED` answer, because there is nothing to ask for. The core's only delivery signal
  is the `Delivery` transport receipt ([GUARANTEES.md](GUARANTEES.md) §6).
- **No CI, no LICENSE, no publish metadata.** Not part of this increment; to be added on
  demand.
- **No synchronous API wrapper.** The async API is the only surface. A blocking facade is a
  binding-layer concern (Phase 10).
- **Fuzz runs need a nightly toolchain.** `cargo-fuzz` requires nightly, which is installed
  and was used: the `weida-protocol` targets ran 200 000 iterations each, the `weida-zmtp`
  targets and the five `weida-sp` targets 20 000 each, with no findings. On a host without
  nightly the targets are still committed and the deterministic `fuzz_smoke*` tests cover the
  same properties on stable, at lower depth.
- **`IncomingTransfer::read_capped` was added beyond the planned API surface.** The planned
  `collect(self, max_bytes)` cannot read a borrowed request body, so the borrowing form is
  the primitive and `collect` delegates to it.
- **A fan-out drop is visible to a subscriber that asked for detection.** This entry used to
  say the loss was invisible and would need a sequence field; the field arrived
  ([decisions/0001](decisions/0001-sequence-field.md)) and `PerProducer(detect)` now reports
  the gap to the subscriber, with `PerProducer(reassemble)` holding instead up to
  `max_reorder_hold`. What remains true is the **default**: a `core` subscriber past its byte
  budget at the publisher simply misses the message and only the publisher counts it
  (`Publisher::dropped`), because nothing on the wire reports a drop to a receiver that
  negotiated no ordering.
- **One subscriber per path per connection.** A `Subscriber` claims its path in the dialling
  connection's namespace, so two subscribers on one pooled connection asking for the same
  path collide with `AlreadyRegistered`. Fanning one subscription out to several in-process
  consumers is left to the application; silently multiplexing two subscribers onto one queue
  would be worse.
- **Publisher direction is fixed.** Pub and Pull bind; Sub and Push connect. The reverse
  directions wait for a use case that demands them.
- **No automatic reconnect.** A dead peer is never redialled by the library; the application
  calls `connect` again, and dead entries are reaped at that moment (`PeerSet::add`), not
  before.
- **Authorization hooks are not implemented.** Master doc §46's authorization surface does not
  exist. What ships is authentication plus the identity: applications decide on
  `IncomingMeta::peer`, and the only built-in allow list is a `Trust` pin list on a binding,
  which is connection-wide and all-or-nothing.
- **`stream_receive_window` is not a payload budget.** The DATA header spends the same window
  as the payload, and a receiver announces more window only per eighth of it, so a payload
  sized exactly to the window cannot be written until the application starts reading (§4).
  Correct QUIC behaviour that reads as a hang.
