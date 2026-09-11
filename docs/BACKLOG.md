# Backlog

Maintained by the loop of [LOOP.md](LOOP.md). Ordered by priority; `ready` items first.
Statuses: `ready`, `in_progress`, `done <hash>`, `blocked: <reason>`, `parked`,
`dropped: <reason>`. Ids are monotonic and never reused.

### B-001 — Decision 0004: durability levels for Stored and Replicated
kind: research | size: 45 | status: done 56ead42 | needs: []
acceptance: `docs/decisions/0004-durability-levels.md`, Status accepted, defining `Stored(Written|Flushed)` and `Replicated(n, flushed: bool)` from rabbitmq-amqp091 §6d, kafka §6, nats §6, amqp10 §6.5; SYNTHESIS §8.3 marked closed.

### B-002 — Decision 0005: refusal race closed as documented behaviour
kind: research | size: 30 | status: in_progress (delegated) 2026-09-11T02:05Z | needs: []
acceptance: `docs/decisions/0005-refusal-race.md`, accepted; cites RFC 9000 §3.2 via quic-standards §12 item 8 and PATTERNS §1.6; SYNTHESIS §8.5 closed.

### B-003 — Decision 0006: guarantee sets and the adapter edge
kind: research | size: 45 | status: ready | needs: []
acceptance: `docs/decisions/0006-guarantee-sets.md`, accepted: a default guarantee set plus a configurable superset inside the weida network, validated at configuration time; at an adapter edge the chain ends at the protocol's transfer point (SYNTHESIS §4) and any degradation is named in configuration; SYNTHESIS §8.7 closed.

### B-004 — Decision 0007: segmented topics, opaque paths
kind: research | size: 45 | status: in_progress (delegated) 2026-09-11T02:05Z | needs: []
acceptance: `docs/decisions/0007-topic-namespace.md`, accepted: endpoint paths stay opaque (INVARIANTS), Pub/Sub filters become segmented patterns with a separator, a one-segment wildcard and a rest wildcard; ZeroMQ byte-prefix subscriptions map to a segment boundary as a named loss; mapping table for MQTT `+`/`#`, NATS `*`/`>`, AMQP `*`/`#`; SYNTHESIS §8.9 closed.

### B-005 — Decision 0008: session identity by fingerprint
kind: research | size: 30 | status: in_progress (delegated) 2026-09-11T02:05Z | needs: []
acceptance: `docs/decisions/0008-session-identity.md`, accepted: the proved fingerprint is the peer identity across connections (binds 0002's control and bulk connections; is the default producer name of 0001 §7.3); no L0 session state; subscription and sequence resumption is L2 work; the open item from 0001 §8 is closed.

### B-006 — Spec sync: PROTOCOL.md on the decided state
kind: spec | size: 45 | status: ready | needs: [B-001, B-004, B-005]
acceptance: reserved DATA keys 6 (sequence) and 7 (producer identity, encoding per 0008) in §6.2 with "optional, skipped by v0"; HELLO §2.3/§6.1 gains guarantee declarations and the intersection rule; §2 states the control connection and per-path bulk connections of 0002; §9 topic filter grammar per 0007; §10 split into control and bulk limits; §11 reserves the L2 credit frame kind (0003) and lists what is deferred; wire version stays 0 with the changes marked "spec ahead of code".

### B-007 — Spec sync: GUARANTEES.md
kind: spec | size: 45 | status: ready | needs: [B-001, B-003]
acceptance: §3 ordering `PerProducer(detect|reassemble)`, dedup `Bounded(window)`, `PerKey` L2-only; `Stored`/`Replicated` with durability levels; §6 rows for backpressure name the two L0 credit units and the absence of application credit; a new subsection on guarantee sets per 0006; §3 receipt paragraph cites RFC 9000 §3.2 per 0005.

### B-008 — Spec sync: PATTERNS.md, INVARIANTS.md, ARCHITECTURE.md
kind: spec | size: 45 | status: ready | needs: [B-006, B-007]
acceptance: PATTERNS §1.3 narrowed to "bulk writers on the same path's connection", §1.4 states the stream budget is the L0 message credit, §1.6 cites 0005, §4 gains subscriber-side drop detection and segmented filters; INVARIANTS permits a control connection while forbidding a multiplexed control stream, adds the reassembly cap and per-peer connection count to the named bounds; ARCHITECTURE §5 describes the two pool tiers and two Limits profiles; SYNTHESIS §8 entries carry their decision numbers.

### B-009 — Measure: DATA header cost at high message rate
kind: measure | size: 60 | status: ready | needs: []
acceptance: a criterion bench in `crates/weida/benches/patterns.rs` pushing 64-byte payloads with a minimal header versus a header carrying two extra uint keys (simulated via `content_len` and `topic` today), messages per second and bytes per message on loopback; numbers in IMPLEMENTATION.md verified results.

### B-010 — Measure: reassembly buffer under cross-stream reordering
kind: measure | size: 60 | status: ready | needs: []
acceptance: a probe in `crates/weida/tests/streams.rs` opening N transfers, finishing them in reverse order, and measuring at the puller how many arrive out of dispatch order and the peak count held back by an application-side reorder buffer keyed on a payload sequence; N = 16, 256; numbers recorded.

### B-011 — Measure: second handshake per peer
kind: measure | size: 45 | status: ready | needs: []
acceptance: a bench measuring `connect` latency and the runtime's RSS delta for 1 versus 2 versus 64 pooled connections to one server on loopback (mTLS off, pinned trust); numbers recorded.

### B-012 — Measure: connections per dialled path
kind: measure | size: 45 | status: ready | needs: [B-011]
acceptance: the same bench with one connection per path for 16 and 256 paths, against `max_connections`; handshake time, RSS, and the point where the server refuses with `LIMIT_EXCEEDED`; numbers recorded.

### B-013 — Wire: DATA keys 6 and 7 in weida-protocol
kind: code | size: 90 | status: ready | needs: [B-006]
acceptance: `DataHeader` gains `sequence: Option<u64>` and `producer: Option<...>` per 0008's encoding; encoder writes them only when set; decoder accepts, caps and skips per §5; golden vectors for both; fuzz targets extended; no runtime behaviour change yet.

### B-014 — Wire: HELLO guarantee declarations and intersection
kind: code | size: 90 | status: ready | needs: [B-006, B-013]
acceptance: `Hello` carries ordering mode, dedup on/off plus window, producer naming, control-isolated flag; `negotiate()` computes the intersection and fails on a requested level the peer does not offer; `Agreed` exposes the result; hostile tests for mismatches; golden vector for the extended HELLO.

### B-015 — Runtime: detector-mode ordering and dedup window
kind: code | size: 90 | status: ready | needs: [B-014]
acceptance: a sending endpoint configured `PerProducer(detect)` numbers its transfers per (connection, path/topic); the receiver reports gaps through `IncomingMeta` (gap count, expected vs seen) without holding anything back; dedup with a time window drops repeats and counts them; both allocate nothing when negotiated off (INVARIANTS); tests for gap detection through a Pub/Sub drop.

### B-016 — Runtime ownership and centralized spawn/timer/DNS
kind: code | size: 90 | status: ready | needs: []
acceptance: `Runtime::owned(config)` (multi-thread, `worker_threads` configurable, default 1) and `Runtime::with_handle`; all `tokio::spawn`, `tokio::time::sleep` and `lookup_host` calls go through `runtime.rs`; a test drives a full Req/Rep round trip under `futures::executor::block_on` with no `#[tokio::test]`; `futures-io` `AsyncRead`/`AsyncWrite` implemented beside the tokio traits.

### B-017 — Control connection per peer, bulk per path
kind: code | size: 90 | status: ready | needs: [B-014, B-011, B-012]
acceptance: pool tiers per 0002 §7; HELLO on the control connection; bulk connections keyed by path and bound by fingerprint; `Limits` profiles `control` and `bulk` in `RuntimeConfig` with defaults chosen from B-011/B-012 numbers; stream probes updated; `a_stalled_stream_does_not_block_its_siblings` extended to show a control frame crossing while bulk is stalled.

### B-018 — Research: ZMTP adapter mapping document
kind: research | size: 60 | status: ready | needs: [B-004]
acceptance: `docs/adapters/zmtp.md` derived from `docs/research/zeromq.md`: socket type to weida pattern table, stream mapping, HWM to credit, NULL/CURVE to Identity/Trust, transfer points, named losses (byte-prefix subscriptions, multipart), and the interop bench plan against the pure-Rust `zeromq` crate.
