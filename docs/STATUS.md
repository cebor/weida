# Status

A one-page picture of where weida stands. Written for a reader who wants to see the shape of
the project before reading the detail; every number and status here is taken from
[NIGHTLOG.md](NIGHTLOG.md), [BACKLOG.md](BACKLOG.md) and [IMPLEMENTATION.md](IMPLEMENTATION.md)
at the commit named below, and those files remain the source of truth. The diagrams live in
`docs/status/` and are plain SVG; regenerate them by hand when the picture changes.

**Snapshot:** main `742d905`, 2026-09-11 ~23:34 UTC. Tree clean, gate green, **764 tests**
(218 at the start of the session), plus 228 under `weida-zmq`'s non-default `blocking` feature
and 18 in the libzmq interop matrix that runs `#[ignore]`d against the system library.
Thirteen decision notes (0001–0013), all `accepted`. Eleven crates in one directory per
protocol family: `weida-core`, `weida-protocol`, `weida-runtime`, `weida`, then
`crates/zmq/{weida-zmtp, weida-zmq, weida-zmq-bridge}`,
`crates/nng/{weida-sp, weida-nng-bridge}` and `crates/interop/cross-tests`.

## 1. The roadmap

![Roadmap](status/roadmap.svg)

Reading it: **Phase A is complete** except named pipes (A9), blocked on a Windows runner
rather than on a design. The control-connection tier (A5) is not a gap: decision 0011 parks
it with a revival condition, because no existing or reserved frame is peer-scoped. The last
open question in it, what a
local `open` does at its ceiling, is answered: it **waits for a slot**, so `Block` means the
same thing on every transport (B-059). **Phase B's first protocol is complete as a library**:
`weida-zmq` is [decision 0013](decisions/0013-competitor-libraries.md)'s definition of done,
all twenty-six items B-070..B-095 merged — eleven socket types against `zmq_socket(3)`'s twenty
rows, three transports, NULL/PLAIN/CURVE with the ZAP dialog, a 98-row option table decided
row by row, the monitor and the devices, a non-default blocking facade, nine zguide recipes,
interop in both roles against `zeromq` 0.6 and against libzmq 4.3.5, and a parity table where
every row carries its evidence. The forwarder was rebuilt on it and lost 1352 lines. **B2 has
its codec, its mapping document and its bridge in both directions** — all interoperating with
the `nng` C library — and reopens as `weida-nng` under the same six slices; its items are not
filed yet. MQTT is next among the mapping documents, because it is the first protocol with a
session and weida deliberately has none.
Phase C's prerequisite (`Runtime::owned`) exists; its first slice is parked until MQTT.
AMQP 0-9-1 (RabbitMQ) is deliberately not a Phase B adapter: its clients come with the broker (D2), and B4's AMQP 1.0 client already reaches RabbitMQ 4.x.

## 2. What exists, layer by layer

![Architecture](status/architecture.svg)

The load-bearing line is the **transport boundary**: `Link`, `SendHalf`, `RecvHalf` as enums in
`crates/weida/src/transport.rs`. Everything above it — frames, HELLO, negotiation, the three
patterns, ordering, dedup, drain — is transport-blind, and `tests/transports.rs` proves that by
running one Req/Rep, one Push/Pull and one Pub/Sub body over QUIC, inproc and `AF_UNIX`
unchanged. Everything below it is one variant each; Windows named pipes would be a fourth.

The **reactor is its own crate** now: `weida-runtime` holds `Exec`, the three reactor-ownership
constructors, the capped resolver, the `AF_UNIX` hygiene and the name registry, and it depends
on `weida-core` and `tokio` and nothing else — which is what lets a ZeroMQ user open a socket
without linking quinn, rustls and weida's pattern layer. Above it the tree is **one directory
per protocol family** (`crates/zmq/`, `crates/nng/`, `crates/interop/`) rather than one
`adapters/` bucket, so the directory list answers "does this repository ship a ZeroMQ?". Each
codec crate still has an **empty `[dependencies]`** section, so it can be checked against its
RFC rather than against our reading of it; each library depends on the codec and the runtime
and never on `weida`; each forwarder is the only crate that names both sides. The review pass
of B-051 found the session's first two unbounded remote-influenced allocations in a bridge
rather than in the core, and B-097 found two more in the new library — so the invariant sweep
covers the foreign-protocol crates too, and every bound they added is named where it is taken.

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

- **Paused, not stopped, and nothing is in flight.** Every branch of the session is merged —
  the last was `b095-crate-move` as `bcdfdff` — the gate is green at 764 tests with one
  ignored, and the tree is clean. No worktree holds unmerged work.
- **14 items are `ready`, one is `blocked`** (B-039, named pipes, §6) and **two are `parked`**
  (A5's control tier by 0011 §4.3, the Python binding by your decision until MQTT).
- Resuming is one instruction, and the choice is yours: **`weida-nng` as a library**, which is
  B2's half of [decision 0013](decisions/0013-competitor-libraries.md) and needs its items
  filed first — the ZeroMQ line is the template, twenty-six items in one day — or **B-062, the
  MQTT mapping document**, which is the one that moves the roadmap, because MQTT is the first
  protocol with a session and weida deliberately has none.
- Five small items are ready and would cost under two hours together: B-104 (write the
  pipe-pairing diagnosis where somebody would try it again), B-106 (a regression test for the
  `Queue` lost wakeup that landed without one), B-107 (one sentence so LOOP §6's gate compiles
  the non-default `blocking` feature), B-109 (the codec's `MESSAGE` encoder still frames a
  command) and B-110 (the interop port probe races).
