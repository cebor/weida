# Status

A one-page picture of where weida stands. Written for a reader who wants to see the shape of
the project before reading the detail; every number and status here is taken from
[NIGHTLOG.md](NIGHTLOG.md), [BACKLOG.md](BACKLOG.md) and [IMPLEMENTATION.md](IMPLEMENTATION.md)
at the commit named below, and those files remain the source of truth. The diagrams live in
`docs/status/` and are plain SVG; regenerate them by hand when the picture changes.

**Snapshot:** main `bd40383`, 2026-09-11 ~13:10 UTC. Tree clean, gate green, 505 tests
(218 at the start of the session). Twelve decision notes (0001–0012), all `accepted`.
Eight crates: `weida-core`, `weida-protocol`, `weida`, `weida-zmtp`, `weida-zmtp-bridge`,
`weida-sp`, `weida-sp-bridge` and `cross-tests`.

## 1. The roadmap

![Roadmap](status/roadmap.svg)

Reading it: **Phase A is complete** except named pipes (A9), blocked on a Windows runner
rather than on a design. The control-connection tier (A5) is not a gap: decision 0011 parks
it with a revival condition, because no existing or reserved frame is peer-scoped. The last
open question in it, what a
local `open` does at its ceiling, is answered: it **waits for a slot**, so `Block` means the
same thing on every transport (B-059). **Phase B has two protocols complete** — ZMTP and SP,
all six slices each, both interoperating with the real upstream (`zeromq`, and the `nng` C
library), and the sixth slice is one test crate they share. MQTT is next and starts with a
document, because it is the first protocol with a session and weida deliberately has none.
Phase C's prerequisite (`Runtime::owned`) exists; its first slice is parked until MQTT.
AMQP 0-9-1 (RabbitMQ) is deliberately not a Phase B adapter: its clients come with the broker (D2), and B4's AMQP 1.0 client already reaches RabbitMQ 4.x.

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
| Local transports against QUIC | Req/Rep 1 KiB 13 µs inproc, 51 µs unix, 58 µs QUIC; Push 1 KiB 24 µs unix against 8 µs QUIC (B-059) | a local `open` waits for a slot, and 0010 §4.2's "the connection is the stream" is priced: free at 1 MiB, the whole cost at 1 KiB |

![Transport latency](status/transport-latency.svg)

## 6. What needs a human

One thing, and it is the same one [NIGHTLOG.md](NIGHTLOG.md) leads with:

1. **B-039, named pipes.** Blocked on a Windows runner by the loop's judgement: code the gate
   cannot compile is code nobody verified. Decision 0012 already covers its shape. A Windows
   route is being prepared outside this tree; `wine` here is a possible later smoke-test path
   and not a substitute for a runner.

## 7. Where the loop stands

- **Paused, not stopped, and nothing is in flight.** Both branches of the session are merged
  (`525db84` the cross-adapter test, `6ca89ad` the local `Block` fix), the gate is green at
  505 tests and the tree is clean. No worktree holds unmerged work.
- Resuming is one instruction: take the first `ready` item of [BACKLOG.md](BACKLOG.md). That
  is B-060, the `weida` CLI; B-062, the MQTT mapping document, is the one that moves the
  roadmap.
