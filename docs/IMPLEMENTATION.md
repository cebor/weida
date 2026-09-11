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
| 9 | Legacy adapters | not started |
| 10 | Language bindings | not started |
| 11 | CLI and administration | not started |
| 12 | Documentation/site/stabilization | not started |

Phases 0, 1 and 2 are complete. Phase 3 is under way: its first increment (Push/Pull and
Pub/Sub) landed, its second re-founded the stack on the layer model — L0 stream core,
L1 patterns, L2 broker — see [ARCHITECTURE.md](ARCHITECTURE.md), and its third gave peers
identities and pinned the stream semantics with measured probes. Router/Dealer are answered
as emergent rather than implemented, see [ARCHITECTURE.md](ARCHITECTURE.md) §6a. Phases 4
and later remain out of scope; Phase 6 gained the acknowledgement vocabulary that used to
sit in the core.

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

**Deliberately deferred** (recorded now, not discovered later):

- Connecting publishers and binding pushers; v0 fixes Pub/Pull as binders and Sub/Push as
  connectors.
- Streaming fan-out — tee-ing one long stream to many subscribers needs its own drop and
  ordering design.
- Per-producer ordering. Ordering is `None` for both new patterns; a sequence field would
  be a protocol addition, not an implementation detail.
- Coalescing backpressure (master doc §27); only `Block`, `Reject` and fan-out `Drop` exist.
- PAIR, BUS and SURVEYOR/RESPONDENT: mapped onto L0 in
  [ARCHITECTURE.md](ARCHITECTURE.md), deliberately not implemented until a use case asks.
- Router/Dealer as first-class types, and the broker work they would actually require
  (master doc §47, §85, Phase 6).
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
| Idle timeout is connection loss | `idle_timeout_reports_loss_within_the_window` | a 500 ms server idle timeout against the client's default 10 s keep-alive (asserted to be the longer of the two): after 1.5 s of silence the next request fails with `Error::ConnectionLost` |
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

What this says for 0002's two tiers: a control connection beside a bulk connection costs about
**one millisecond of handshake and under a megabyte of resident memory for both ends
together**, and the handshake is the whole cost — there is no per-connection cost that grows
with the number of connections held. Against that, the head-of-line coupling 0002 removes is
unbounded: one slow reader stalls every writer on the connection
([PATTERNS.md](PATTERNS.md) §1.3). The numbers therefore support 0002's default rather than
arguing for a lazily created control connection. They also set the scale for the `Limits`
profiles of B-017: 64 connections to one peer is ~50 MiB of transport state on the pair, so a
per-peer connection count belongs in the named bounds
([INVARIANTS.md](INVARIANTS.md)) rather than being left to the path count.

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

The consequence for B-017's `Limits` profiles: **a per-path bulk connection is affordable in
time and linear in memory, but it converts a path count into a connection count against
`max_connections`** — a default of 1024 accepted connections per binding is 1024 paths' worth
of fan from a *single* client if nothing else bounds it, which is why
`max_connections_per_peer` is a named bound before the code exists
([INVARIANTS.md](INVARIANTS.md), [PROTOCOL.md](PROTOCOL.md) §10.1). A client that dials 256
paths pays 278 ms of handshakes where it pays 1.5 ms today, so the bulk tier wants lazy
per-path connections — a path dialled is not a path used — while the control connection stays
eager per B-011.

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
- **No deduplication.** No idempotency ids, no dedup window — and since `transfer_id` left
  the wire there is not even an identifier a receiver could deduplicate on. This is why an
  `Indeterminate` result may only be retried for idempotent operations
  ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5).
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
  and was used: all six targets ran 200 000 iterations each with no findings. On a host
  without nightly the targets are still committed and the deterministic `fuzz_smoke_*` tests
  cover the same properties on stable, at lower depth.
- **`IncomingTransfer::read_capped` was added beyond the planned API surface.** The planned
  `collect(self, max_bytes)` cannot read a borrowed request body, so the borrowing form is
  the primitive and `collect` delegates to it.
- **Pub/Sub fan-out drops are invisible to the subscriber.** A subscriber past its byte
  budget at the publisher simply misses the message; nothing on the wire reports it. Only
  the publisher counts it (`Publisher::dropped`). Making loss observable to the receiving
  side would need a sequence field, which is the same prerequisite as per-producer
  ordering.
- **One subscriber per path per connection.** A `Subscriber` claims its path in the dialling
  connection's namespace, so two subscribers on one pooled connection asking for the same
  path collide with `AlreadyRegistered`. Fanning one subscription out to several in-process
  consumers is left to the application; silently multiplexing two subscribers onto one queue
  would be worse.
- **Publisher direction is fixed.** Pub and Pull bind; Sub and Push connect. The reverse
  directions wait for a use case that demands them.
- **The cause of a lost connection is erased at peer selection.** `PeerSet::pick` reports
  `Error::ConnectionLost` for any peer whose connection has closed, without consulting the
  `quinn::ConnectionError`, so an idle timeout, a peer SHUTDOWN and a transport error are
  indistinguishable there. `conn_error` has the richer mapping; through the pattern APIs it is
  unreachable once every peer entry is closed. An application deciding whether to reconnect or
  to give up cannot tell why it lost the peer. Filed as B-028.
- **No automatic reconnect.** A dead peer is never redialled by the library; the application
  calls `connect` again, and dead entries are reaped at that moment (`PeerSet::add`), not
  before.
- **Authorization hooks are not implemented.** Master doc §46's authorization surface does not
  exist. What ships is authentication plus the identity: applications decide on
  `IncomingMeta::peer`, and the only built-in allow list is a `Trust` pin list on a binding,
  which is connection-wide and all-or-nothing.
- **The resolver takes the first address.** `Exec::resolve` uses the first entry
  `lookup_host` returns, so `weida://localhost:…` on a host where `localhost` resolves to
  `::1` only cannot reach a server bound to `127.0.0.1`. Use the IP literal until address
  selection learns to try more than one — which is also the cheap path now, since a literal
  is resolved in place with no task at all (B-025). Filed as B-029.
- **`stream_receive_window` is not a payload budget.** The DATA header spends the same window
  as the payload, and a receiver announces more window only per eighth of it, so a payload
  sized exactly to the window cannot be written until the application starts reading (§4).
  Correct QUIC behaviour that reads as a hang.
