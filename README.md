# weida

weida is a messaging framework for Rust built from first principles around QUIC. Every user
data flow maps to one QUIC stream, so a 40-byte transfer and a 40-GB transfer use the same
protocol semantics and neither requires the payload to be materialized in memory.

It is built in layers. **L0** is a stream core: ZeroMQ's idea rebuilt on QUIC, whose
primitives are unidirectional and bidirectional streams with exactly the guarantees QUIC
gives — ordered bytes within a stream, none across streams, flow control, a transport
delivery receipt, and cancellation by reset. **L1** is the ZeroMQ/nanomsg pattern family as
thin wrappers over L0: Req/Rep, Push/Pull, Pub/Sub. **L2** will be a RabbitMQ-analog broker
with queues, publisher confirms and consumer acknowledgements; it does not exist yet, and
its guarantee vocabulary is deliberately kept out of the socket layer until it does.

**Status:** alpha. Wire protocol version `0` (experimental, breaking changes permitted
within `0.x`). Phases 0-2 implemented: docs, core model, native QUIC transport with Req/Rep.
Phase 3 in progress: Push/Pull, Pub/Sub and the raw L0 stream API have landed.

## Documentation

| Document | Contents |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | layer model, terminology, addressing, crate map, runtime internals, public API v0 |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | normative wire protocol v0: framing, frame headers, golden vectors, limits |
| [docs/GUARANTEES.md](docs/GUARANTEES.md) | guarantee vocabulary and what is actually implemented in v0 |
| [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md) | failure scope, required scenarios, sender outcome rules, `Indeterminate` |
| [docs/INVARIANTS.md](docs/INVARIANTS.md) | the short invariant list every change is checked against |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | phase tracker, development loop, agent rules, acceptance, known debt |
| [Master Architecture and Implementation Plan — QUIC-native Messaging Framework.md](Master%20Architecture%20and%20Implementation%20Plan%20%E2%80%94%20QUIC-native%20Messaging%20Framework.md) | the normative architectural source of truth for the whole project |

## Crates

| Path | Package | Responsibility |
| --- | --- | --- |
| `crates/core` | `weida-core` | I/O-free model: errors, endpoint addresses, limits, trace context |
| `crates/protocol` | `weida-protocol` | wire codec, no I/O: varints, framing, CBOR headers, negotiation, error codes |
| `crates/weida` | `weida` | runtime, native QUIC transport, the raw stream core, and the Req/Rep, Push/Pull and Pub/Sub patterns |

## Try the prototype

Two terminals:

```
cargo run -p weida --example transform_server -- --bind 127.0.0.1:7443 --cert-out /tmp/weida-cert.pem
```

```
printf 'hello weida' | cargo run -p weida --example transform_client -- --ca /tmp/weida-cert.pem weida://127.0.0.1:7443/transform
```

Expected: stdout is exactly `HELLO WEIDA`; stderr shows `delivered` — QUIC's transport
receipt for the request, not an application acknowledgement — and a
trace id that also appears in the server's log line for the request.

## Other patterns

Push/Pull and Pub/Sub each have a self-contained example: both halves run in one
process on loopback, so there is nothing to configure.

```
cargo run -p weida --example push_pull   # fire-and-forget, and one transport receipt
cargo run -p weida --example pub_sub     # prefix-filtered topics, two subscribers
```

Messages arrive out of order in both. Each transfer is its own QUIC stream, so ordering
is `None` for these patterns — see [docs/GUARANTEES.md](docs/GUARANTEES.md) §6.

## Build and test

```
cargo test --workspace
cargo test -p weida --test large -- --ignored     # 1 GiB echo, asserts bounded peak RSS
cargo bench                                      # codec and loopback QUIC throughput
```

No license file yet.
