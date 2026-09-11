# Backlog

Maintained by the loop of [LOOP.md](LOOP.md). Ordered by priority; `ready` items first.
Statuses: `ready`, `in_progress`, `done <hash>`, `blocked: <reason>`, `parked`,
`dropped: <reason>`. Ids are monotonic and never reused.

### B-001 — Decision 0004: durability levels for Stored and Replicated
kind: research | size: 45 | status: done 56ead42 | needs: []
acceptance: `docs/decisions/0004-durability-levels.md`, Status accepted, defining `Stored(Written|Flushed)` and `Replicated(n, flushed: bool)` from rabbitmq-amqp091 §6d, kafka §6, nats §6, amqp10 §6.5; SYNTHESIS §8.3 marked closed.

### B-002 — Decision 0005: refusal race closed as documented behaviour
kind: research | size: 30 | status: done edabf00 | needs: []
acceptance: `docs/decisions/0005-refusal-race.md`, accepted; cites RFC 9000 §3.2 via quic-standards §12 item 8 and PATTERNS §1.6; SYNTHESIS §8.5 closed.

### B-003 — Decision 0006: guarantee sets and the adapter edge
kind: research | size: 45 | status: done 052e741 | needs: []
acceptance: `docs/decisions/0006-guarantee-sets.md`, accepted: a default guarantee set plus a configurable superset inside the weida network, validated at configuration time; at an adapter edge the chain ends at the protocol's transfer point (SYNTHESIS §4) and any degradation is named in configuration; SYNTHESIS §8.7 closed.

### B-004 — Decision 0007: segmented topics, opaque paths
kind: research | size: 45 | status: done c06c072 | needs: []
acceptance: `docs/decisions/0007-topic-namespace.md`, accepted: endpoint paths stay opaque (INVARIANTS), Pub/Sub filters become segmented patterns with a separator, a one-segment wildcard and a rest wildcard; ZeroMQ byte-prefix subscriptions map to a segment boundary as a named loss; mapping table for MQTT `+`/`#`, NATS `*`/`>`, AMQP `*`/`#`; SYNTHESIS §8.9 closed.

### B-005 — Decision 0008: session identity by fingerprint
kind: research | size: 30 | status: done 7d3d31b | needs: []
acceptance: `docs/decisions/0008-session-identity.md`, accepted: the proved fingerprint is the peer identity across connections (binds 0002's control and bulk connections; is the default producer name of 0001 §7.3); no L0 session state; subscription and sequence resumption is L2 work; the open item from 0001 §8 is closed.
note: 0008 §5 records that SYNTHESIS §8 never received the session entry 0001 §8 promised, so nothing is struck out there; the question and its answer live in 0001 §8 plus this note. Verified against the acceptance line, which asks only for the 0001 §8 item.

### B-006 — Spec sync: PROTOCOL.md on the decided state
kind: spec | size: 45 | status: done 191043c | needs: [B-001, B-004, B-005]
acceptance: reserved DATA keys 6 (sequence) and 7 (producer identity, encoding per 0008) in §6.2 with "optional, skipped by v0"; HELLO §2.3/§6.1 gains guarantee declarations and the intersection rule; §2 states the control connection and per-path bulk connections of 0002; §9 topic filter grammar per 0007; §10 split into control and bulk limits; §11 reserves the L2 credit frame kind (0003) and lists what is deferred; wire version stays 0 with the changes marked "spec ahead of code".
note: the filter grammar landed in §6.4, where `filter` is defined, with §9.5 pointing at it — §9 was the wrong home for a header rule. Two additions the acceptance line did not ask for but the decisions require: §6.5 defines the guarantee-set encoding (the wire keys 0006 §5 left to the wire work), and §9.2 states that a refusal is not ordered against the receipt per 0005 §4.3. The filter grammar is the one non-additive change — the same SUBSCRIBE bytes can now select a different topic set — so the Status section says the implementation is a defect against the document until B-020 lands, and the §8 SUBSCRIBE vector carries a note that `px.` does not match `px.eur` under the grammar.

### B-007 — Spec sync: GUARANTEES.md
kind: spec | size: 45 | status: done 30b8942 | needs: [B-001, B-003]
acceptance: §3 ordering `PerProducer(detect|reassemble)`, dedup `Bounded(window)`, `PerKey` L2-only; `Stored`/`Replicated` with durability levels; §6 rows for backpressure name the two L0 credit units and the absence of application credit; a new subsection on guarantee sets per 0006; §3 receipt paragraph cites RFC 9000 §3.2 per 0005.

### B-008 — Spec sync: PATTERNS.md, INVARIANTS.md, ARCHITECTURE.md
kind: spec | size: 45 | status: done 64ba40b | needs: [B-006, B-007]
acceptance: PATTERNS §1.3 narrowed to "bulk writers on the same path's connection", §1.4 states the stream budget is the L0 message credit, §1.6 cites 0005, §4 gains subscriber-side drop detection and segmented filters; INVARIANTS permits a control connection while forbidding a multiplexed control stream, adds the reassembly cap and per-peer connection count to the named bounds; ARCHITECTURE §5 describes the two pool tiers and two Limits profiles; SYNTHESIS §8 entries carry their decision numbers.
note: INVARIANTS gained a third named-but-unimplemented bound beside the reassembly hold and the per-peer connection count — the dedup window's identity count, which a time window does not bound — plus a paragraph naming the adapter mapping document as the home of the adapter-honesty invariant per 0006 §4.9. SYNTHESIS §8 also gained a preamble stating which seven entries are closed and that §8.6 and §8.8 remain open; the question text above each closing paragraph is left untouched, because a decision is only legible against its question.

### B-019 — Spec sync: FAILURE_MODEL.md per 0005
kind: spec | size: 30 | status: done 4343af2 | needs: []
acceptance: §4 keeps "a refusal can lose the race with the transport" and gains 0005 §4.4's consequence — no sender outcome exists for a refusal observed after the receipt resolved, and none is added — plus 0005 §4.3's two deterministic constructions (payload beyond the peer's stream receive window, or an exchange) with the note number; the three 2 MiB tests named in 0005 §2 get a doc-comment sentence saying their payload size is what makes the refusal deterministic; `grep` shows no document still calling the race an open question.
note: verified — the `docs/FAILURE_MODEL.md` §4 change states both deterministic constructions and that there is no third, adds "no sender outcome exists for a refusal observed after the receipt resolved", and makes the 2 MiB payload size load-bearing in prose and in the three test doc comments. The diff touches no executable line in `pushpull.rs` or `pubsub.rs`; the gate ran anyway because Rust files changed, and it is green (222 tests).

### B-009 — Measure: DATA header cost at high message rate
kind: measure | size: 60 | status: done 178a95c | needs: []
acceptance: a criterion bench in `crates/weida/benches/patterns.rs` pushing 64-byte payloads with a minimal header versus a header carrying two extra uint keys (simulated via `content_len` and `topic` today), messages per second and bytes per message on loopback; numbers in IMPLEMENTATION.md verified results.
note: `topic` is settable only by a publisher, so the second key is simulated by a `sha256:<64 hex>` `content_type` — the wire shape of the producer identity of 0008 — rather than by `topic`. The measured +80 B is therefore the worst case for DATA keys 6 and 7, and it is what 0008 §4.4 used to fix the encoding.

### B-010 — Measure: reassembly buffer under cross-stream reordering
kind: measure | size: 60 | status: done a236cbb | needs: []
acceptance: a probe in `crates/weida/tests/streams.rs` opening N transfers, finishing them in reverse order, and measuring at the puller how many arrive out of dispatch order and the peak count held back by an application-side reorder buffer keyed on a payload sequence; N = 16, 256; numbers recorded.
note: queueing the reverse FINs back to back does not reverse the arrival order — quinn transmits pending streams in its own order — so the probe carries a second mode that awaits each FIN's transport receipt, which does reverse it and reaches the N − 1 bound. Run at N = 16 for that mode; N = 16 and 256 for the batched one.

### B-011 — Measure: second handshake per peer
kind: measure | size: 45 | status: done 417a359 | needs: []
acceptance: a bench measuring `connect` latency and the runtime's RSS delta for 1 versus 2 versus 64 pooled connections to one server on loopback (mTLS off, pinned trust); numbers recorded.
note: "pooled connections to one server" cannot be dialled from one runtime today — the pool keys on `(host, port, ClientTls, address fingerprint)` — so the memory figures use one client runtime per connection and separate the per-runtime delta (0-4 KiB) from the per-connection one, both ends in one process. New bench target `crates/weida/benches/connections.rs`, which B-012 extends.

### B-012 — Measure: connections per dialled path
kind: measure | size: 45 | status: done b1ad06b | needs: [B-011]
acceptance: the same bench with one connection per path for 16 and 256 paths, against `max_connections`; handshake time, RSS, and the point where the server refuses with `LIMIT_EXCEEDED`; numbers recorded.
note: measures both shapes, because they are two systems: pooled today (256 paths share one connection, 1.47 ms for all 256 dials) and one connection per path as 0002 will have it (277.7 ms for 256, 1.08 ms each, 995 KiB per connection). The refusal point is confirmed at `max_connections`: the ninth dial against a ceiling of 8 fails with `Error::LimitExceeded`. The run also caught a +12 % `cold_handshake` regression that B-016 introduced — filed as B-025, not fixed here.

### B-013 — Wire: DATA keys 6 and 7 in weida-protocol
kind: code | size: 90 | status: done 2a43e70 | needs: [B-006]
acceptance: `DataHeader` gains `sequence: Option<u64>` and `producer: Option<...>` per 0008's encoding; encoder writes them only when set; decoder accepts, caps and skips per §5; golden vectors for both; fuzz targets extended; no runtime behaviour change yet.
note: delegated to the parallel worker on branch `b013-data-keys` in the worktree `../weida-b016` with its own `CARGO_TARGET_DIR`. Encoding is fixed by [PROTOCOL §6.2] and 0008 §4.4: `sequence` a `uint`, `producer` a `bstr` capped at 32 B and absent whenever the producer is the connection peer. Merged, gated and finished here on delivery, as B-016 was.
note: merged `--no-ff` as 2a43e70 and gated here (232 tests). `producer` is `Option<[u8; 32]>` rather than a capped `Vec`, which is the stronger shape: a wrong length cannot be constructed at all, and the decoder rejects a `bstr` of any other length. That sharpened PROTOCOL §6.2 from "cap 32 B" to *exactly* 32 B — a spec change I accepted and carried into the key table, because a truncated digest names nobody. §8 gained a sequenced and a relayed vector; the pinned v0 vectors are byte-identical, which is the "no runtime behaviour change" half of the acceptance.

### B-014 — Wire: HELLO guarantee declarations and intersection
kind: code | size: 90 | status: done 4d62746 | needs: [B-006, B-013]
acceptance: `Hello` carries ordering mode, dedup on/off plus window, producer naming, control-isolated flag; `negotiate()` computes the intersection and fails on a requested level the peer does not offer; `Agreed` exposes the result; hostile tests for mismatches; golden vector for the extended HELLO.
note: delegated to the parallel worker on branch `b014-hello-guarantees`, branched from `b020-segment-filter`. The wire is already specified: HELLO keys `5`/`6` and the guarantee-set map of [PROTOCOL §6.5], with the per-dimension intersection and the fail-rather-than-downgrade rule of §2.3 steps 5-7. Absent keys mean `core`, so a v0 HELLO must stay byte-identical — the existing golden vector is the regression guard.
note: merged `--no-ff` as 4d62746, gated here (253 tests at that point). **The acceptance line asked for two dimensions my PROTOCOL §6.5 did not have** — producer naming and a control-isolated flag — and the worker added them as guarantee-set keys `8` (unordered, like backpressure) and `9` (ordered, so the intersection is a logical AND), amending §6.5. Accepted: the item asked for them, `control_isolated` is exactly what SYNTHESIS §8.2 said an adapter multiplexing foreign sessions would need to require, and both default to the `core` value so a v0 HELLO stays byte-identical — the extended vector proves it. Two further rules the worker added are right and were missing: an encoder MUST NOT write a dimension left at its `core` level, so `core` and absent are indistinguishable on the wire; and when the intersection weakens `acknowledgement` below `Stored`, the `durability` and `replicas` axes it qualified are dropped rather than carried. The debt this creates is that GUARANTEES §3 still lists five dimensions — filed as B-035.

### B-015 — Runtime: detector-mode ordering and dedup window
kind: code | size: 90 | status: done f42b94a | needs: [B-014]
acceptance: a sending endpoint configured `PerProducer(detect)` numbers its transfers per (connection, path/topic); the receiver reports gaps through `IncomingMeta` (gap count, expected vs seen) without holding anything back; dedup with a time window drops repeats and counts them; both allocate nothing when negotiated off (INVARIANTS); tests for gap detection through a Pub/Sub drop.
note: delegated to the parallel worker on branch `b015-detect-dedup`, after B-014. Both structures it introduces are named bounds in INVARIANTS before they exist — the dedup identity count in particular, since a time window bounds how long an identity is kept and not how many arrive — and the "allocate nothing when negotiated off" half is the whole point: `core` is the default set, so the hot path must be untouched for every connection that does not ask.
note: merged `--no-ff` as f42b94a, gated here: **261 tests**, one conflict in `config.rs` where B-031's `shutdown_timeout` met the new `guarantees` field — both kept, and the `guarantees` doc comment's absolute GitHub URL replaced by the repo-relative form the rest of the file uses. Delivered as the **detect-ordering half only**, which is the right slice: `RuntimeConfig::guarantees`, `IncomingMeta::{sequence, gap}`, `Gap::missed()`, numbering per dialled path and per topic before fan-out, `Limits::max_sequence_scopes` (1024) as the named bound the scope table needs, `capacity() == 0` assertions for the allocation-free-when-off half, and `a_dropped_fan_out_copy_shows_up_as_a_gap` end to end. Deduplication is **not** in it and is B-034 per LOOP §1.3.

### B-034 — Runtime: bounded deduplication with a time window
kind: code | size: 90 | status: ready | needs: [B-015]
acceptance: a runtime configured `Deduplication::Bounded` with `dedup_window_ms` suppresses a repeated (producer, scope, sequence) within the window and counts the suppression on a per-connection metric; the window is bounded in count as well as time by a new named `Limits` field; the structure allocates nothing when `Deduplication::None` is negotiated, proved the way `ordering.rs` proves it (capacity assertion); a test replays one transfer twice inside the window and once after it, asserting suppression then delivery; GUARANTEES §6 dedup row loses its "spec ahead of code" marker and the INVARIANTS dedup-window bound moves from named to enforced.
note: the remainder of B-015, split off per LOOP §1.3 when the detect half landed green on its own.

### B-016 — Runtime ownership and centralized spawn/timer/DNS
kind: code | size: 90 | status: done 2741dfd | needs: []
acceptance: `Runtime::owned(config)` (multi-thread, `worker_threads` configurable, default 1) and `Runtime::with_handle`; all `tokio::spawn`, `tokio::time::sleep` and `lookup_host` calls go through `runtime.rs`; a test drives a full Req/Rep round trip under `futures::executor::block_on` with no `#[tokio::test]`; `futures-io` `AsyncRead`/`AsyncWrite` implemented beside the tokio traits.
note: built by the parallel worker on branch `b016-runtime` in the worktree `../weida-b016` (commits 6dee879 code, bfc5fee docs), merged here `--no-ff` as 2741dfd with no conflicts, and gated in this tree: fmt clean, clippy in both feature configurations, 222 tests pass with 1 ignored (the new `foreign_executor` suite among them), rustdoc with `-D warnings`. Verified against the acceptance line: `Runtime::owned` rejects `worker_threads: 0`, `Exec` in `runtime.rs` is the only place calling `tokio::spawn`, `tokio::time::sleep` or `lookup_host`, `futures_io::AsyncWrite`/`AsyncRead` are implemented on both transfer types, and `a_req_rep_round_trip_runs_without_a_tokio_executor` is a plain `#[test]`.

### B-017 — Control connection per peer, bulk per path
kind: code | size: 90 | status: ready | needs: [B-014, B-011, B-012]
acceptance: pool tiers per 0002 §7; HELLO on the control connection; bulk connections keyed by path and bound by fingerprint per 0008 §4.2, with a fingerprint mismatch refused and a test for that refusal in `crates/weida/tests/identity.rs`, and anonymous clients (no proved fingerprint) never treated as one peer; `Limits` profiles `control` and `bulk` in `RuntimeConfig` with defaults chosen from B-011/B-012 numbers; stream probes updated; `a_stalled_stream_does_not_block_its_siblings` extended to show a control frame crossing while bulk is stalled.

### B-018 — Research: ZMTP adapter mapping document
kind: research | size: 60 | status: done fecd996 | needs: [B-004]
acceptance: `docs/adapters/zmtp.md` derived from `docs/research/zeromq.md`: socket type to weida pattern table, stream mapping, HWM to credit, NULL/CURVE to Identity/Trust, transfer points, named losses (byte-prefix subscriptions, multipart), and the interop bench plan against the pure-Rust `zeromq` crate.
note: delivered with `docs/adapters/README.md` as well — the mapping-document template and its own table, so no row was needed in `docs/decisions/README.md`. Exceeds the acceptance line with ten named losses, six refused configurations and a per-loss test in the bench plan.

### B-020 — Segmented topic filter matching in code
kind: code | size: 90 | status: done 6250448 | needs: [B-006]
acceptance: `matches_filter` in `crates/weida/src/pubsub.rs` becomes the allocation-free, backtracking-free segment walker of 0007 §4.3, and its doc-comment objection is rewritten rather than deleted; filter grammar validation lands in `weida-protocol` beside the other header rules so an invalid filter (`*` not alone in its segment, `#` not final) is rejected at the codec boundary; `Subscriber::subscribe`'s documentation states the grammar; golden vectors for a literal filter, a middle-segment `*`, a trailing `#`, the empty filter and a topic containing a literal `*`; `subscribe_prefix_filters_topics` is renamed and extended, including the boundary case a byte prefix over-matched (`sensors.temp` must not select `sensors.temperature`).
note: delegated to the parallel worker on branch `b020-segment-filter` in the worktree `../weida-b016`, after B-013. It is the one item that makes the implementation agree with [PROTOCOL §6.4] again, so the §8 golden vectors and the `px.` note in that section are part of it; the existing `subscribe_prefix_filters_topics` must fail before the change and pass after it in its extended form.
note: merged `--no-ff` as 6250448 (it carried B-013 with it) and gated here: 244 tests. Verified: the walker is one left-to-right pass with no backtracking and no allocation, `weida_protocol::filter::validate` rejects `*` that does not own its segment and `#` that is not final, `SubscriptionHeader::decode` turns that into `HeaderError::InvalidFilter` at the codec boundary, `Subscriber::subscribe` refuses locally before the frame is written, and the renamed `subscribe_filters_topics_by_segment` asserts that `sensors.temp` does not select `sensors.temperature` — the assertion that fails on the old matcher. Three existing tests moved from `px.` to `px.#`: that is the contract changing, not a test being weakened, and the same change is why PROTOCOL's Status section now reads as history rather than as a pending defect (b4f77b3).

### B-021 — Measure: segment matching in the fan-out path
kind: measure | size: 45 | status: done 2c157ac | needs: [B-020]
acceptance: a criterion bench comparing byte-prefix `starts_with` against the segment walker at a realistic subscriber and filter count (the objection recorded in `crates/weida/src/pubsub.rs` asserts a cost without a number); publish-to-all-drained time per subscriber count for both matchers; numbers in IMPLEMENTATION.md verified results, which is what 0007 §6 asks for.
note: `matches_filter` is `pub(crate)` and B-020 documents why it lives beside the fan-out it serves, so the comparison is made through the public API rather than by moving code for a bench: one connection holds N filters that the topic matches none of, and running N = 1 against N = 64 subtracts every fixed cost of `publish` out. Per-filter cost is 14.3-15.1 ns and **shape-independent**, so the grammar is not the cost — registry iteration is. The byte prefix it replaced is measured as a pure function, since it no longer exists in the library.

### B-022 — Reassembly mode, capped, and subscriber-side drop detection
kind: code | size: 90 | status: ready | needs: [B-015]
acceptance: `PerProducer(reassemble)` holds out-of-order transfers and releases them in sequence order, with the hold bounded by a named `Limits` field and the bound enforced by refusing or releasing out of order rather than by growing — B-010 measured the peak at N − 1 of the transfers in flight, and 84 of 256 with no adversarial pattern, so the cap is a configured number and not an assumption about arrival order; the cap appears in INVARIANTS' named bounds; a subscriber in detect or reassemble mode reports a Pub/Sub drop through `IncomingMeta`; a test drives reordering with the two FIN modes of `reverse_order_completion_measures_the_reorder_buffer`.

### B-023 — Foreign-executor coverage for Push/Pull and Pub/Sub
kind: code | size: 45 | status: done 7ae9292 | needs: [B-016]
acceptance: `crates/weida/tests/foreign_executor.rs` gains a Push/Pull and a Pub/Sub round trip as plain `#[test]`s under `futures::executor::block_on`, so the claim "weida needs no ambient reactor" is proved for every pattern rather than for Req/Rep alone; each test bounds every await with the suite's deadline helper; a pattern that cannot run without a tokio context is a bug in `runtime.rs`, not a reason to skip the test. Proposed by the B-016 worker.
note: the Pub/Sub test needed one thing the tokio suites get for free — waiting for a subscription to land. It retries `publish` until it reaches one subscriber instead of sleeping, because a timer would need the very reactor the test refuses to have, and it subscribes with the empty filter so the test does not move when B-020 changes the matcher. 224 tests pass.

### B-024 — An example without `#[tokio::main]`
kind: code | size: 30 | status: done 11c31d0 | needs: [B-016]
acceptance: one existing example (or a new small one) drives a full exchange from a plain `fn main` using `Runtime::owned`, showing what a caller with no async runtime of its own writes; the README's build-and-run section names it; `cargo run -p weida --example <name>` works. Proposed by the B-016 worker, and it is the user-visible half of `Runtime::owned` — the API exists but nothing in the tree demonstrates it.
note: a new example, `crates/weida/examples/owned_runtime.rs`, rather than a converted one: the existing five all demonstrate a pattern and would have lost that focus. It joins the two halves instead of spawning them, because an executor with no reactor has nothing to spawn onto — which is the lesson the example exists to teach. The README gained a "No reactor of your own" section naming all three constructors. Verified by running it: it prints the pinned URL and `HELLO FROM A PLAIN MAIN`.

### B-025 — Do not resolve a literal address on every connect
kind: code | size: 30 | status: done 52ebbbc | needs: []
acceptance: `Exec::resolve` (or its caller in `crates/weida/src/pool.rs`) short-circuits a host that is already an IP literal — `host.parse::<IpAddr>()` — so no string is allocated, no task is spawned and no join handle is awaited for `weida://127.0.0.1:7443/x`; name resolution keeps its current shape for real hostnames, including the spawn that `lookup_host` needs for its Tokio context; `connect/cold_handshake` returns to ~1.05 ms in `cargo bench -p weida --bench connections` and the number replaces the regression paragraph in IMPLEMENTATION.md §4 (B-012). Found by B-012: the current path costs +12 % of a cold handshake (1.02-1.10 ms to 1.18-1.24 ms, p = 0.00) for a lookup that a literal address does not need.
note: no new test — `resolves_ip_literals_without_dns` in `runtime.rs` already pins the contract for both literal families and now exercises the short-circuit, and the whole suite dials `127.0.0.1`, so the branch is covered 222 times over. The proof is the bench: 1.05-1.09 ms, `change: −11.6 % (p = 0.00)`.

### B-026 — Decision 0009: a bounded drain at shutdown
kind: research | size: 45 | status: done dec2c62 | needs: []
acceptance: `docs/decisions/0009-drain.md`, accepted, closing SYNTHESIS §8.6: whether `Runtime::shutdown` gains a bounded drain and whether the drain belongs to the runtime or to L2, decided against the evidence the entry already names — ZeroMQ's `ZMQ_LINGER` with its infinite default that can hang forever, RabbitMQ's requeue-on-channel-close, AMQP 1.0's `drain`/`echo` quiescence point, NATS Lame Duck Mode, and D12's finding that none of them is a drain *acknowledgement*; a bound in wall-clock time is mandatory if a drain exists at all, because an unbounded one is the ZeroMQ failure mode; SYNTHESIS §8.6 closed, README row added, and the consequences name what PATTERNS §1.1 and the ZMTP mapping's loss L9 must then say.
note: the evidence turned up a defect while it was being gathered: `Runtime::shutdown` already waits on `wait_idle()` with **no bound**, so today's tree has ZeroMQ's failure mode in a place where it buys nothing. The note names it and B-031 fixes it separately, ahead of the drain feature itself (B-032) and its spec sync (B-033). The ZMTP mapping's loss L9 was updated in place, since "SYNTHESIS §8.6, still open" had become false.

### B-027 — Decision 0010: the local transport, per platform
kind: research | size: 60 | status: in_progress 2026-09-11T08:08Z | needs: []
acceptance: `docs/decisions/0010-local-transport.md`, accepted, closing SYNTHESIS §8.8 and unblocking roadmap A9: the Linux default (`AF_UNIX` `SOCK_STREAM` against `SOCK_SEQPACKET` and the abstract namespace), the macOS default, the Windows default (named pipes against Windows `AF_UNIX`), the fallback chain, and whether a bulk payload ever travels by handle passing rather than by bytes — each with the cost `docs/research/ipc.md` §11 already records; inproc binding is named as the first slice because it needs no platform decision at all; SYNTHESIS §8.8 closed and README row added.

### B-028 — Preserve why a connection was lost
kind: code | size: 45 | status: ready | needs: []
acceptance: `PeerSet::pick` stops flattening every closed peer into `Error::ConnectionLost`: the `quinn::ConnectionError` of the peer it rejected is mapped through the existing `conn_error` so an idle timeout, a peer-initiated close and a transport error are distinguishable through the pattern APIs; a test drives the idle-timeout case and asserts the specific error rather than `ConnectionLost`; the debt entry in IMPLEMENTATION.md §6 is removed rather than annotated. Found by the review pass in the debt list: an application deciding whether to redial cannot currently tell why it lost the peer.

### B-029 — Try more than the first resolved address
kind: code | size: 45 | status: ready | needs: [B-025]
acceptance: `Exec::resolve` returns the resolved addresses in order and the dialling path tries them until one connection succeeds, so `weida://localhost:7443/x` reaches a server bound to `127.0.0.1` on a host where `localhost` also resolves to `::1`; the number tried is bounded (a hostile resolver is remote input) and the bound is named in INVARIANTS; the last error is reported when all fail; the IP-literal short-circuit of B-025 is untouched; the debt entry in IMPLEMENTATION.md §6 is removed rather than annotated.

### B-030 — ZMTP codec: sans-I/O, golden vectors, fuzz target
kind: adapter | size: 90 | status: ready | needs: [B-018]
acceptance: `crates/adapters/weida-zmtp`, no I/O and no weida dependency in the codec itself: the 64-octet greeting, version negotiation, the NULL handshake with `READY` metadata, short and long frames with the MORE and COMMAND flags, `SUBSCRIBE`/`CANCEL`, `PING`/`PONG`; the golden vectors of `docs/adapters/zmtp.md` §10 byte-exact in both directions, including the 255/256-octet frame boundary; a fuzz target over the decoder that caps before allocating, since a ZMTP frame may declare up to 2^63-1 octets and `ZMQ_MAXMSGSIZE` is the only defence; no bridge, no sockets, no interop bench yet — those are slices 3 to 5.

### B-031 — Bound every wait in shutdown
kind: code | size: 30 | status: done 10b3120 | needs: []
acceptance: `Runtime::shutdown` stops awaiting `wait_idle()` without a bound — today it can hang on a peer's behaviour, which is the one failure mode the whole catalogue warns about ([0009](decisions/0009-drain.md) §4.4, [zeromq §12/P17]); the wait is capped (QUIC's own closing and draining periods are "at least three times the current PTO interval", so a cap in the hundreds of milliseconds is generous), the cap is a `RuntimeConfig` field with a stated default, and a test proves that shutdown returns even when the peer never acknowledges — a server whose process is suspended or a connection to a black hole. This is a defect in shipped code and does not wait for B-032.
note: the number that justifies the default was measured rather than guessed: against a peer that has gone silent, the draining period alone is **96 ms** on loopback, so a 1 s cap is generous where a clean close was possible at all. The test asserts **relatively** — a 1 ms cap must be at least twice as fast as a 10 s one, measured in the same run — because an absolute bound would pin this machine; verified by mutation, the unbounded version reports 106 ms against 95 ms and fails it. `shutdown`'s doc comment now also says *abortive*, which is the half of 0009 that needed no new API.

### B-032 — `Runtime::drain(Duration)`
kind: code | size: 90 | status: ready | needs: [B-031]
acceptance: `Runtime::drain(Duration)` per [0009](decisions/0009-drain.md) §4.1-§4.6: admission stops first (bindings accept no new connection, a new inbound stream on an existing connection is refused with `SHUTDOWN`), then transfers already `finish()`ed are awaited to their transport receipt, then the same close as `shutdown` runs; the deadline is a mandatory `Duration` with no infinite variant; the return value counts what reached the peer's transport and what was still outstanding, and an expired drain is not an error; `shutdown`'s documentation names itself **abortive**. Tests: a finished transfer that would be cut short by `shutdown` arrives under `drain`, and a drain against a peer that reads nothing returns at its deadline with a non-zero outstanding count.

### B-033 — Spec sync: the drain of 0009
kind: spec | size: 30 | status: ready | needs: [B-032]
acceptance: PATTERNS §1.1 keeps "the one thing that cuts a finished transfer short" for `shutdown` and gains `drain` as its counterpart; GUARANTEES §3 notes that the drain waits on the transport receipt and inherits its meaning, including that a drained transfer may still have been discarded by the peer's application (0005 §4.2); PROTOCOL §11 lists drain as a *local* operation with no wire representation, so nobody invents a quiescence frame; `grep` shows no document still calling §8.6 open.

### B-035 — Spec sync: the two guarantee dimensions B-014 added
kind: spec | size: 30 | status: ready | needs: []
acceptance: GUARANTEES §3 gains `ProducerNaming` (`Fingerprint` | `Stable`, unordered) and `ControlIsolated` (`No` | `Yes`, ordered) as dimensions beside the five it lists, each with its levels, its ordering and the decision it comes from ([0001](decisions/0001-sequence-field.md) §7.3 and [0008](decisions/0008-session-identity.md) §4.3 for the first, [0002](decisions/0002-control-and-bulk-separation.md) §6.1 for the second); the guarantee-set subsection stops saying a set has one level per dimension of the *five* and says what it now is; [0006](decisions/0006-guarantee-sets.md) §5 gains a consequence line recording that PROTOCOL §6.5 keys `8` and `9` extended the vocabulary with B-014, so "a guarantee set introduces no new words" reads as "no new words without a dimension entry"; `grep` shows no document listing five dimensions where there are seven. Found by reviewing B-014's delivery: the wire and the code carry two dimensions the guarantee vocabulary does not.
