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
Pub/Sub) has landed; Router/Dealer are answered as emergent rather than implemented, see
[ARCHITECTURE.md](ARCHITECTURE.md) §6a. Phases 4 and later remain out of scope.

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
- Deliberately unresolved questions identified (§4 below and
  [PROTOCOL.md](PROTOCOL.md) §11).

No broker implementation begins before these foundations are coherent.

### Phase 1 — Core model

Delivers `weida-core`: an I/O-free crate, unit-tested without any networking.

- `TransferId` and id allocation.
- `EndpointAddr` endpoint identifiers with the `weida://` parser and the `SCHEME` constant.
- `Error` with hand-written `Display`/`Error` impls.
- `Limits`.
- `AckMode`, `AckState`, `Outcome` policy types.
- `TraceContext` with the W3C `traceparent` parser and formatter.
- Cancellation representation.
- The send, receive and correlation state machines, as pure transition functions matching
  [FAILURE_MODEL.md](FAILURE_MODEL.md) §4 exactly.

The Runtime, Listener, Binding and typed Endpoint abstractions named in master doc §79's
Phase 1 list are realized in crate `weida` in this increment, because their shape is
determined by the transport they front; their model-level types (`Limits`, `Outcome`,
`EndpointAddr`) live in `weida-core`.

### Phase 2 — Native QUIC transport

Delivers `weida-protocol` and crate `weida`.

- Quinn integration and connection establishment.
- Negotiation (HELLO exchange, version and capability intersection).
- The uni-stream protocol envelope: varints, preamble, the five frame headers, CBOR codec
  with cap-before-allocation and depth-limited skip.
- Endpoint routing through a flat namespace shared by all bindings of a Listener.
- Request/reply correlation.
- Short control streams: ACK, ERROR, CANCEL.
- Cancellation and stream reset in both directions.
- Resource limits, all remote-input-bounded.
- Client connection pooling keyed by `(host, port)`.

Large streaming transfers are tested immediately, not deferred.

---

### Phase 3 — Brokerless messaging patterns

Master doc §79 asks at minimum for Push/Pull, Pub/Sub, Req/Rep and Router/Dealer
equivalents. §82 asks whether a smaller internal primitive set implements them cleanly.

**Delivered in the first increment:**

- The §82 answer: four primitives (P1 one-way transfer, P2 correlation, P3 peer set plus
  selection policy, P4 bounded inbound queue behind an opaque path), recorded with the
  Router/Dealer-is-emergent rationale in [ARCHITECTURE.md](ARCHITECTURE.md) §6a.
- Wire: `role = 0` (oneshot) implemented, DATA key `9` (`topic`), frame kinds `5`
  (SUBSCRIBE) and `6` (UNSUBSCRIBE), limits `max_subscriptions` and
  `subscriber_buffer_bytes` — all normative in [PROTOCOL.md](PROTOCOL.md).
- Push/Pull: `Pusher` connects and round-robins over its peers, `Puller` binds. Both
  acknowledgement modes work unchanged, because they ride P1.
- Pub/Sub: `Publisher` binds, `Subscriber` connects; byte-prefix topic filters; fan-out
  with a per-subscriber byte budget and explicit drops, so a slow subscriber never stalls
  the publisher.
- Refusal rather than reinterpretation when a role meets an endpoint that does not serve
  it: ERROR `UNSUPPORTED` plus `STOP_SENDING(REJECTED)`, connection intact.

**Deliberately deferred** (recorded now, not discovered later):

- Connecting publishers and binding pushers; v0 fixes Pub/Pull as binders and Sub/Push as
  connectors.
- Streaming fan-out — tee-ing one long stream to many subscribers needs its own drop and
  ordering design.
- Per-producer ordering. Ordering is `None` for both new patterns; a sequence field would
  be a protocol addition, not an implementation detail.
- Coalescing backpressure (master doc §27); only `Block`, `Reject` and fan-out `Drop` exist.
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
demonstrates it.

| # | Requirement | Demonstrated by |
| --- | --- | --- |
| 1 | Establish QUIC connection. | `echo_roundtrip_with_ack` in `crates/weida/tests/reqrep.rs`; examples `transform_server` + `transform_client` |
| 2 | Negotiate protocol. | `echo_roundtrip_with_ack`; the `versions=[99]` case in `crates/weida/tests/hostile.rs` asserting close code `NEGOTIATION_FAILED` |
| 3 | Open uni request stream. | `echo_roundtrip_with_ack` |
| 4 | Send metadata header. | golden-vector unit tests in `weida-protocol`; `echo_roundtrip_with_ack` |
| 5 | Stream arbitrary-sized input. | `streaming_overlap` in `crates/weida/tests/reqrep.rs`; example `large_stream` |
| 6 | Server begins processing before FIN where possible. | `streaming_overlap` (handler consumes the first chunk before the request FIN) |
| 7 | Server opens correlated reply stream before request necessarily completes. | `streaming_overlap` (handler opens the reply after the first chunk; the client withholds the remaining payload until at least one reply byte has arrived, making the overlap deterministic) |
| 8 | Stream reply simultaneously. | `streaming_overlap` |
| 9 | Send protocol ACK on a separate control stream. | `echo_roundtrip_with_ack` asserting `finish()` == `Acked(Accepted)`; ACK golden vector in `weida-protocol` |
| 10 | Propagate OpenTelemetry trace context. | `trace_propagation` in `crates/weida/tests/reqrep.rs`; `traceparent` fuzz target under `crates/protocol/fuzz`; the trace id printed by `transform_client` and logged by `transform_server` |
| 11 | Cancel mid-transfer. | `cancel_mid_transfer` and `reply_abort_on_cancel_frame` in `crates/weida/tests/reqrep.rs` |
| 12 | Test connection loss. | `crates/weida/tests/hostile.rs`: raw server closing after FIN asserts `Err(Indeterminate)`; raw server closing mid-payload asserts `Err(ConnectionLost)` |
| 13 | Test malformed headers. | `crates/weida/tests/hostile.rs`: garbage preamble and `header_len = 1 MiB` both assert close code `PROTOCOL_VIOLATION`; `weida-protocol` unit tests for duplicate keys, missing keys and oversized lengths |
| 14 | Fuzz parser. | fuzz targets `preamble`, `data_header`, `hello`, `traceparent`, `roundtrip` under `crates/protocol/fuzz`, plus the deterministic `fuzz_smoke_*` unit tests in `weida-protocol` |
| 15 | Benchmark small and large transfers. | `crates/protocol/benches/codec.rs` (header encode/decode); `crates/weida/benches/echo.rs` (`echo_1kib_rtt`, `stream_throughput_64mib`) |
| 16 | Demonstrate bounded memory with a multi-GB generated stream. | `large_stream_bounded_memory` in `crates/weida/tests/large.rs` (ignored by default; run with `--ignored`), asserting checksum equality and peak RSS below 512 MiB for a 1 GiB echo; example `large_stream` for ad-hoc runs |

This prototype proves the central architecture before the feature surface expands.

### Verified results

Recorded from the run that closed this increment, on an AMD Ryzen 7 5800X, Linux, stable
Rust 1.97.1 (nightly 1.100.0 for the fuzz targets only).

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
the checks that closed the Push/Pull and Pub/Sub increment.

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

---

## 5. Decisions made in Phase 0

### Serialization: CBOR via `minicbor`

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

### Other decisions

| Decision | Value | Rationale |
| --- | --- | --- |
| URL scheme | `weida://` | The master doc's `mq://` is a working name (§82). Single `SCHEME` constant in `crates/core/src/addr.rs`. |
| ALPN token | `weida/0` | Couples the TLS-level protocol identity to the wire protocol version, so a version-0 peer cannot silently talk to a future version. |
| Stream magic | `0x57` (ASCII `W`) | Cheap first-byte rejection of non-weida streams. |
| Wire protocol version | `0` | Experimental per master doc §15; independent of the library version. |
| Library version | `0.1.0` | Independent of the wire version. |
| Client trust | explicit trust anchors only | No platform root store and no insecure-skip mode ships in v0. The prototype uses the server's self-signed certificate as its CA. |
| TLS material source | file **or** in-memory PEM (`Pem`) | Requiring a path would force callers holding a key from a secret store to write it to disk first. Both sources are first class; `ServerTls::from_pem` never touches the filesystem. |
| Credential placement | server identity per **binding**, trust per **dialling endpoint** | Credentials are transport-specific, so they do not belong on the Listener (a namespace) or the Runtime (a resource container). Consequence: the connection pool keys on `(host, port, ClientTls)` — sharing on authority alone would hand one endpoint a peer authenticated against another's CA. |
| Rust edition | `2024` | Current stable edition. |
| MSRV | `1.88` | Highest requirement among the pinned dependencies. |

---

## 6. Known debt and deferred work

Recorded deliberately, not discovered later.

- **OpenTelemetry SDK wiring deferred.** v0 propagates W3C `traceparent` and `tracestate` in
  transfer metadata and emits structured logs through `tracing`. No OTel exporter, no span
  export pipeline. That belongs to the observability work; the wire fields it needs are
  already carried, so enabling it does not change the protocol.
- **No retries.** Delivery is `BestEffort` only ([GUARANTEES.md](GUARANTEES.md) §6). Retry
  policy arrives in Phase 4.
- **No persistence.** No payload store, no WAL, no recovery. Phase 5.
- **No deduplication.** No idempotency ids, no dedup window; receiver-side `transfer_id`
  uniqueness is explicitly not enforced. This is why an `Indeterminate` outcome may only be
  retried for idempotent operations ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5).
- **Single reply per request.** Multiple correlated replies are a protocol capability the
  stream model was chosen to allow, but v0 resolves on the first reply and resets later
  same-correlation streams.
- **Capability codes unused.** HELLO carries `capabilities` and `required_capabilities`, but
  no code is assigned and the supported set is empty.
- **Ack modes beyond `accepted` unimplemented.** `stored`, `replicated` and `processed` are
  reserved code points answered with `UNSUPPORTED`.
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
