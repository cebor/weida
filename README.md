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
Phase 3 in progress: Push/Pull, Pub/Sub, the raw L0 stream API, peer identity by public-key
fingerprint, opt-in per-producer ordering and bounded deduplication, one connection per
dialled endpoint path, a bounded `drain`, and an in-process transport beside QUIC have
landed. The ZeroMQ adapter has its first two slices: a standalone ZMTP 3.1 codec
(`crates/adapters/weida-zmtp`) and an inbound bridge that puts foreign ZeroMQ peers onto
weida endpoints (`crates/adapters/weida-zmtp-bridge`).

## Documentation

| Document | Contents |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | layer model, terminology, addressing, crate map, runtime internals, public API v0 |
| [docs/PATTERNS.md](docs/PATTERNS.md) | the pattern reference: per-pattern tables in the shape of `zmq_socket(3)`, and what QUIC streams do underneath, measured |
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
| `crates/weida` | `weida` | runtime, the QUIC and in-process transports, the raw stream core, and the Req/Rep, Push/Pull and Pub/Sub patterns |
| `crates/adapters/weida-zmtp` | `weida-zmtp` | ZMTP 3.1 codec — greeting, framing, commands, metadata — with no I/O and no dependency on weida at all |
| `crates/adapters/weida-zmtp-bridge` | `weida-zmtp-bridge` | bridges foreign ZeroMQ peers onto weida endpoints, terminating both protocols |

## Identity in one line

A weida peer is its public key. The server generates an identity and prints an address that
names it; the client trusts that address and nothing else:

```
$ cargo run -p weida --example transform_server -- --bind 127.0.0.1:7443
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/transform
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo
```

```
$ printf 'hello weida' | cargo run -p weida --example transform_client -- 'weida://sha256:22ed30a8…9f25@127.0.0.1:7443/transform'
HELLO WEIDA
```

No certificate file changes hands, no CA exists, and a peer with any other key is refused with
`Untrusted(sha256:…)` naming the key that answered. `--identity PATH` keeps the server's key
across restarts so the address stays stable; `--cert-out PATH` plus `transform_client --ca
PATH` is the same exchange through a trusted certificate and a plain address instead. In
code: `Identity::generate()`, `Trust::by_address()` / `Trust::pin(fp)` / `Trust::anchor(pem)`,
`ServerTls::require_client(trust)` for mutual identity, and `IncomingMeta::peer` to see who
sent what.

Expected on stderr: `delivered` — QUIC's transport receipt for the request, not an application
acknowledgement — and a trace id that also appears in the server's log line for the request.

## Other patterns

Push/Pull and Pub/Sub each have a self-contained example: both halves run in one
process on loopback, so there is nothing to configure.

```
cargo run -p weida --example push_pull   # fire-and-forget, and one transport receipt
cargo run -p weida --example pub_sub     # prefix-filtered topics, two subscribers
```

Messages arrive out of order in both. Each transfer is its own QUIC stream, so ordering
is `None` for these patterns — see [docs/PATTERNS.md](docs/PATTERNS.md) §1.7.

## No reactor of your own

weida does not require you to be inside `#[tokio::main]`. `quinn` needs a Tokio reactor and
nothing else in the library does, so `Runtime::owned` creates and owns one — one worker
thread by default — and hands back ordinary futures that any executor can drive. Transfers
implement both the `tokio::io` and the `futures-io` trait pairs for the same reason.

```
cargo run -p weida --example owned_runtime   # a full exchange from a plain `fn main`
```

`Runtime::new` takes the ambient reactor and fails with `Error::Runtime` when there is none;
`Runtime::with_handle` takes somebody else's. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §5.

## Build and test

```
cargo test --workspace
cargo test -p weida --test streams               # QUIC stream mechanics, measured
cargo test -p weida --test transports            # one pattern suite over QUIC and inproc
cargo test -p weida --test identity              # pins, anchors, addresses, client identity
cargo test -p weida --test large -- --ignored     # 1 GiB echo, asserts bounded peak RSS
cargo test -p weida-zmtp                         # ZMTP golden vectors and hostile input
cargo test -p weida-zmtp-bridge                  # ZeroMQ peers against the bridge
cargo bench                                      # codec and loopback QUIC throughput
```

No license file yet.
