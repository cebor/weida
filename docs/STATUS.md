# Status

A one-page picture of where weida stands. Written for a reader who wants to see the shape of
the project before reading the detail; every number and status here is taken from
[NIGHTLOG.md](NIGHTLOG.md), [BACKLOG.md](BACKLOG.md) and [IMPLEMENTATION.md](IMPLEMENTATION.md)
at the commit named below, and those files remain the source of truth. The diagrams live in
`docs/status/` and are plain SVG; regenerate them by hand when the picture changes.

**Snapshot:** main `e357a4e`, 2026-09-11 ~20:00 UTC. Tree clean, gate green, 494 tests
(218 at the start of the session). Twelve decision notes (0001–0012), all `accepted`.
Seven crates: `weida-core`, `weida-protocol`, `weida`, `weida-zmtp`, `weida-zmtp-bridge`,
`weida-sp`, `weida-sp-bridge` (plus `cross-tests` on a branch).

## 1. The roadmap

![Roadmap](status/roadmap.svg)

Reading it: **Phase A is done** except two deliberate gaps — the control-connection tier
(A5) is parked because after decision 0011 no frame needs it, and named pipes (A9) are
blocked on a Windows runner rather than on a design. **Phase B has one protocol complete**
(ZMTP, all six slices, interop against the real `zeromq` crate) and one at five of six (SP:
codec, document, both bridges merged; the cross-adapter test is green on
`b057-cross-adapter` and waiting for its merge). MQTT is next and starts with a document,
because it is the first protocol with a session and weida deliberately has none. Phase C's
prerequisite (`Runtime::owned`) exists and the Python slice is a `ready` item.

## 2. What exists, layer by layer

![Architecture](status/architecture.svg)

The load-bearing line is the **transport boundary**: `Link`, `SendHalf`, `RecvHalf` as enums in
`crates/weida/src/transport.rs`. Everything above it — frames, HELLO, negotiation, the three
patterns, ordering, dedup, drain — is transport-blind, and `tests/transports.rs` proves that by
running one Req/Rep, one Push/Pull and one Pub/Sub body over QUIC, inproc and `AF_UNIX`
unchanged. Everything below it is one variant each; Windows named pipes would be a fourth.

The **adapters** sit outside the core on purpose. Each codec crate has an empty
`[dependencies]` section so that it can be checked against its RFC rather than against our
reading of it; each bridge is its own crate so that the codec never learns what weida is. The
review pass of B-051 found the only two unbounded remote-influenced allocations of the session
in a bridge, not in the core — the invariant sweep now covers `crates/adapters` too.

## 3. The first message across two protocols

![Cross-adapter chain](status/cross-adapter-chain.svg)

This is roadmap slice B(6) and the reason both mapping documents were written in one shape.
The chain's honest guarantee is `BestEffort`, and the test asserts that rather than implying
more: ZMTP's `delivered()` proves the ZeroMQ hop's transport, SP has no transfer point at all,
so a ZeroMQ send succeeds with the NNG side closed and nothing arrives. The composed losses —
a multipart refused at hop one, the smaller `max_message_bytes` deciding — are each one test.

## 4. Progress by the numbers

![Tests over time](status/tests-over-time.svg)

Every point is a merge into main behind the four-step gate of [LOOP.md](LOOP.md) §6. Two things
the chart does not show: three review findings were caught **before** their merge by reading
the branch (a reassembler counter that drifted on repeated sequence numbers, a runtime-wide
mutex on the fire-and-forget path that cost 3 % in the header bench, a parked receipt that
held an OS descriptor), and three of the four findings of the ZMTP interop run were mistakes in
our own code that no unit test could have seen without an independent implementation on the
other end.

## 5. The measurements that decided something

| Question | Number | What it decided |
| --- | --- | --- |
| Two extra DATA keys per message | +80 B, −9 % msg/s (B-009) | producer identity travels as a 32-byte `bstr`, not hex (0008 §4.4) |
| Reorder buffer under reverse completion | peak N − 1; 84 of 256 unforced (B-010) | `max_reorder_hold` is a configured number, default 256 |
| Second connection per peer | 1.05 ms handshake, ~1 MiB per pair (B-011/012) | one connection per dialled path is affordable (B-017) |
| Segment matcher in the fan-out path | 14 ns per filter, shape-independent (B-021) | the grammar is not the cost; registry iteration is |
| Parking a drain receipt on drop | first cut −3 %, per-connection inside noise (B-032) | per-connection parking; "dropping a Delivery is free" rewritten |
| Dedup key allocation | ~10 ns; the fix measures the same (B-040) | key left alone, with the number beside it |

![Transport latency](status/transport-latency.svg)

## 6. What needs a human

In the order [NIGHTLOG.md](NIGHTLOG.md) lists them:

1. **B-059, the local Push/Pull bound.** A queued local transfer holds a descriptor, so a
   fire-and-forget sender hits `max_local_streams` at the 384th message. Block on every
   transport, or document Reject locally. The measurement that found it is otherwise done.
2. **B-039, named pipes.** Blocked on a Windows runner by the loop's judgement: code the gate
   cannot compile is code nobody verified. Decision 0012 already covers its shape.
3. **A stray document** at the repository root, `weida-sample-transport.md`, committed as
   recovered; move it under `docs/` or turn its five requests into items.
4. **A deviation from 0002 §6.6:** reassembly holds transfers as unread streams (pinning the
   QUIC window) instead of reading eagerly into an application buffer, because the core forbids
   materializing payloads. Documented in GUARANTEES §3; a decision could still overrule it.
5. **Process:** two backlog ids were assigned in two places at once during the session
   (B-051/B-052, B-057/B-063). Ids now come from the backlog owner only.

## 7. Where the loop stopped

- `loop-owner` stopped inside B-059 with the green part committed (`81d70e7`) and a note in
  the backlog; `decisions-helper` stopped with B-057 committed and mergeable on
  `b057-cross-adapter` (`14c27fd`, main already merged in).
- Resuming is one instruction to the loop: merge `b057-cross-adapter`, write the two
  "cross-adapter" lines the mapping documents owe (text in the worker's last report), then
  B-059 with the decision above.
