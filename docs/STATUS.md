# Status

A one-page picture of where weida stands. Written for a reader who wants to see the shape of
the project before reading the detail; every number and status here is taken from
[NIGHTLOG.md](NIGHTLOG.md), [BACKLOG.md](BACKLOG.md) and [IMPLEMENTATION.md](IMPLEMENTATION.md)
at the commit named below, and those files remain the source of truth. The diagrams live in
`docs/status/` and are plain SVG; regenerate them by hand when the picture changes.

**Snapshot:** main `626d5eb`, 2026-09-13 ~20:45 UTC. Tree clean, gate green on **two
platforms**: Linux **1682 tests** (218 at the start of the session, 764 before the four
parallel workstreams) and a Windows 11 VM at the same commit with 1660 across 119 binaries,
each with 37 ignored — the interop suites that need a library or a broker the machine does
not have — plus the non-default feature runs (`weida-zmq` and `weida-mqtt`'s `blocking`,
`weida-nng`'s `blocking` and `nng-interop`) and the four bindings' Python suites: **45**
(MQTT), **36** (NATS) and **17** (AMQP) re-run here on merge, ZeroMQ's and SP's in their own
items, and **every one of the four wheels built and smoke-tested with no Rust toolchain on
`PATH`**. Fourteen decision notes (0001–0014), all `accepted`. **Twenty-four `[workspace]
members`**, one directory per protocol family, read off `cargo metadata` rather than a
hand-kept list: `weida-core`, `weida-protocol`, `weida-runtime`, `weida-winpipe`, `weida`,
`crates/py/weida-py-core`, then
`crates/zmq/{weida-zmtp, weida-zmq, weida-zmq-bridge, weida-zmq-py}`,
`crates/nng/{weida-sp, weida-nng, weida-nng-bridge, weida-nng-py}`,
`crates/mqtt/{weida-mqtt-codec, weida-mqtt, weida-mqtt-py}`,
`crates/amqp/{weida-amqp-codec, weida-amqp, weida-amqp-py}`,
`crates/nats/{weida-nats-codec, weida-nats, weida-nats-py}` and `crates/interop/cross-tests`.

![Product line](status/product-line.svg)

The two halves of that picture are the two products of
[0013](decisions/0013-competitor-libraries.md) §4: weida on the left, with the guarantee
dimensions, transports and patterns it offers, and the five foreign-protocol libraries on the
right, each with what was measured against a real peer and what is still pending — joined only
by the two forwarders in the middle and by the shared foundation both stand on.

## 1. The roadmap

![Roadmap](status/roadmap.svg)

Reading it: **Phase A is complete.** Its last slice, named pipes (A9, B-039), waited for a
Windows runner rather than for a design, and landed the day one existed: the same 0012
grouping over `\\.\pipe\`, gated on the VM beside the Linux gate. The control-connection
tier (A5) is not a gap: decision 0011 parks it with a revival condition, because no existing
or reserved frame is peer-scoped. The last open question in it, what a local `open` does at
its ceiling, is answered: it **waits for a slot**, so `Block` means the same thing on every
transport (B-059).

**Phase B is complete across all four slices, and Phase C's Python row with it** — sixty-six
items of [0014](decisions/0014-parallel-libraries.md) in four parallel workstreams, each
library the definition of done of
[0013](decisions/0013-competitor-libraries.md) §4.7 and each binding following its own
library, asyncio first and synchronous second:

- **B1 ZeroMQ** — `weida-zmq` and `weida-zmq-py`: eleven socket types against
  `zmq_socket(3)`'s twenty rows, three transports, NULL/PLAIN/CURVE with the ZAP dialog, a
  98-row option table decided row by row, the monitor, the devices, nine zguide recipes,
  interop in both roles against libzmq 4.3.5, and a forwarder rebuilt on the library that lost
  1352 lines.
- **B2 nanomsg SP** — `weida-nng` and `weida-nng-py`: one socket type per protocol, the
  endpoint engine, a 48-row option table, TLS and IPC credentials, **eleven interop tests
  against NNG 1.4.0-rc.0 through the `nng` crate**, and the bridge rebuilt on the library —
  780 insertions against 731 deletions, which is what a rebuild should look like. That rebuild
  found two library bugs no unit test had.
- **B3 MQTT 5** — `weida-mqtt` and `weida-mqtt-py`, a **client**: the server half is Phase D
  ([0014](decisions/0014-parallel-libraries.md) §2), so its parity table adds a fourth verdict
  and says why. Interop ran against **two** brokers, `rumqttd` 0.20.0 and `rmqtt` 0.23.1, and
  measured **six** disagreements with the specification.
- **B4 AMQP 1.0 and Core NATS** — `weida-amqp`, `weida-nats` and both bindings: nine
  performatives, both credit schemes, the settle modes and dispositions, interop in both roles
  against `fe2o3-amqp` 0.17.0 — and NATS interop **written and not run**, because no
  `nats-server` was reachable, which its parity table records as a clause **not met** rather
  than rounding up.

What that leaves: **Phase C's Java and Node rows**, untouched and unfiled; **Phase D**, the L2
broker, which is also where MQTT's and AMQP's server halves live; and the five parity
documents' own open questions, each named in its document rather than here.
AMQP 0-9-1 (RabbitMQ) is deliberately not a Phase B adapter: its clients come with the broker (D2), and B4's AMQP 1.0 client already reaches RabbitMQ 4.x.

## 2. What exists, layer by layer

![Architecture](status/architecture.svg)

The load-bearing line is the **transport boundary**: `Link`, `SendHalf`, `RecvHalf` as enums in
`crates/weida/src/transport.rs`. Everything above it — frames, HELLO, negotiation, the three
patterns, ordering, dedup, drain — is transport-blind, and `tests/transports.rs` proves that by
running one Req/Rep, one Push/Pull and one Pub/Sub body over QUIC, inproc, `AF_UNIX` and, on
Windows, named pipes unchanged. Below it the two kernel-mediated transports share one
implementation of the 0012 grouping, generic over what a socket and a pipe differ in; the
pipe's one addition is a chunk framing, because a pipe has no half-close.

The **reactor is its own crate** now: `weida-runtime` holds `Exec`, the three reactor-ownership
constructors, the capped resolver, the `AF_UNIX` and named-pipe hygiene and the name
registry, and it depends on `weida-core`, `tokio` and — on Windows only — `weida-winpipe`,
the one crate in the tree that may use `unsafe`, fifteen Win32 calls behind a safe surface —
which is what lets a ZeroMQ user open a socket
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

Two things, the first one changed shape:

1. **B-039 is done.** The Windows VM that unblocked it (`ssh win11-geselle`) is not in any
   CI; the Windows gate is run by hand (`C:\work\gate.ps1`) beside the Linux one, and B-061's
   CI would be where it stops being manual. The VM's storage threw `STATUS_IN_PAGE_ERROR`
   three times mid-compile during the slice, each a corrupted artifact cleared by hand.
2. **B-184, the gate's doc step, needs a change to [LOOP.md](LOOP.md) §6 that only you make.**
   The loop does not edit its own standing instructions. Two holes, both paid for tonight: the
   gate runs no per-crate `--no-default-features` rustdoc, so an intra-doc link to a
   `#[cfg(feature = ...)]` item is a hard error **only** in the configuration nobody runs —
   one such link shipped in B-147 and another in B-165, and each survived three or four merges
   invisibly; and the doc step can run against a stale target, which is why a worker's own
   green run was silent on a break that was really there. The item names the commands; adding
   them to §6 is the part that is yours.

## 7. Where the loop stands

- **Nothing is in flight and every branch is merged.** The four workstreams of
  [0014](decisions/0014-parallel-libraries.md) are drained — sixty-six filed items, **68 merge
  commits** since `184e439`, checked branch by branch rather than taken on report — the gate is
  green at **1680 tests** with 37 ignored on Linux and at 1658 on Windows, and the tree is
  clean. The tree has **no known red**, on either platform, in any configuration, including
  the four that are not in the gate yet.
- **9 items are `ready`, one is `blocked`** (B-061, CI: the Forgejo host cannot run this gate,
  a runner is a decision) and **one is `parked`** (A5's control tier by 0011 §4.3). Of 190
  filed items, 179 are `done`; B-189, the NNG survey interop hang on
  Windows, closed the same evening as a test race, not a library bug.
- **Of the six the night left behind**, four are closed this session: **B-182** (with B-188:
  the NNG survey flake was a library bug the second platform exposed every time), **B-183**
  and **B-185** (the two binding parity documents, `nng-py.md` and `mqtt-py.md`, now beside
  the other three), **B-176** (the adapters index says what the tree has) and **B-180** (the
  drain tests order the late stream by a poll of the drain, not a sleep). One stands:
  **B-184**, the gate change above (§6). One was filed and closed the same hour: **B-190**,
  `weida-nng-py`'s wheel, built and run with no toolchain like the four others' — the gap
  its own parity document found.
- **The older loose ends**: B-060, B-064..B-066 and B-068 (requirement-driven work; B-067,
  the per-topic drop counters, closed tonight), B-096 and B-099 (a byte ceiling for the
  per-peer queues and the peer count that is its other half), B-107 (a LOOP §6 sentence, yours
  like B-184), and B-177 (concurrent send and receive on one `weida-zmq` socket). Closed
  tonight beside them: B-104, B-106, B-109, B-110 (the four small ZeroMQ-family items, two
  worktrees in parallel), B-178 and B-179.
- **What moves the roadmap next** is yours to choose: **Phase D**, the L2 broker, which is also
  where MQTT's and AMQP's server halves live and where the credit of 0003 gets a consumer on
  both ends; or **Phase C's Java row**, now that the Python row is four bindings wide and
  `weida-py-core` has proved the shape a per-language foundation takes.
