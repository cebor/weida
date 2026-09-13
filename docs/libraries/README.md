# Library parity documents

One document per foreign protocol this repository implements as a **standalone library** —
a crate usable with no weida in the picture ([0013](../decisions/0013-competitor-libraries.md)
§4, §5.5). The document is the parity table: it states, row by row, what the library does with
every inventory item of the protocol's reference implementation.

## A library document is not an adapter document

The two are easy to confuse and answer different questions, so the distinction is the first
thing this index says.

| | `docs/adapters/<proto>.md` | `docs/libraries/<proto>.md` |
| --- | --- | --- |
| Subject | a **bridge** between weida and a foreign network | a **library** that speaks the foreign protocol and nothing else |
| Question it answers | what does a weida guarantee become on the other side, and what is lost | does this implementation have what the reference implementation has |
| Vocabulary | both, lined up — the document exists to line them up | the foreign protocol's only; weida is not mentioned except where a weida crate replaces a foreign construct |
| Invariant it carries | adapter honesty: "protocol adapters may not silently invent guarantees their source protocol cannot provide" ([INVARIANTS.md](../INVARIANTS.md), [0006](../decisions/0006-guarantee-sets.md) §4.9) | parity honesty: every inventory row is present, refused with a reason, or absent with a reason — and **no row says "partial" without naming what is missing** (0013 §4.7 clause 6) |
| Named loss | what the guarantee chain cannot carry across the edge | nothing at all: a library terminates no weida guarantee, so it has no edge and no loss list |
| When it is written | Phase B slice 2, before the bridge code ([LOOP.md](../LOOP.md) §9) | last, after the library's slices are done, because it is the record of what they came to |

A protocol may have both, and ZeroMQ does: [`zmq.md`](zmq.md) is the parity table for the
`weida-zmq` library, and [`../adapters/zmtp.md`](../adapters/zmtp.md) maps the bridge that
sits between a weida endpoint and a ZeroMQ socket. Neither replaces the other, and neither
is the protocol's research sheet — a sheet in [`../research/`](../research/README.md)
describes a protocol on its own terms and cites its specifications; a parity document cites
the sheet and the code.

## Required sections

Every parity document answers all of these, in this order. A section that does not apply says
so in one line rather than being omitted.

```
# <Protocol> (<reference implementation and version>) — feature parity

## 1. What a row means
The three verdicts, the rule against "partial", and the versions every measured claim was
measured against.

## 2. <Primitive> types
Every socket/link/channel kind of the reference implementation's own table, with the rows that
table has, and where each one is implemented or why it is absent.

## 3. Transports
Every transport the sheet inventories, present or absent-with-reason.

## 4. Mechanisms and authentication
Every security mechanism, and the authentication dialog if the protocol has one.

## 5. Options
Every configuration option of the reference implementation, honoured under the name this
library gives it or refused with the reason.

## 6. Observability
The event set, counters or tracing surface, and what a row's value carries where it differs.

## 7. Devices and helpers
The intermediaries the reference implementation ships as API.

## 8. Interop evidence
Which implementations were run against this one, in which roles, how many pairings, and what
disagreed — each disagreement measured rather than inferred.

## 9. Deliberate deviations and bounds the reference implementation lacks
Where this library is on purpose not identical, and every ceiling it adds.

## 10. The definition of done
The clauses this library was built against, each with its verdict and the section that proves
it.

## 11. Sources
The research sheet with section numbers, the decisions, and the code paths the table's
verdicts were read from.
```

Two rules keep these documents honest:

1. **Every row carries its evidence.** A "present" row names the module or the test; a
   "refused" row names the reason the code gives at configuration time; an "absent" row names
   what is missing. A row whose verdict cannot be checked from the repository does not belong
   here.
2. **No aggregate verdicts.** "Mostly complete", "partial support" and "planned" are not
   verdicts. Either the row is present, or the document says what a caller who wants it does
   not get.

## Documents

| File | Library crates | Reference implementation | Sheet | Status |
| --- | --- | --- | --- | --- |
| [`zmq.md`](zmq.md) | `weida-zmtp` (codec), `weida-zmq` (implementation) | libzmq 4.3.5, plus the pure-Rust `zeromq` 0.6.0 | [`zeromq.md`](../research/zeromq.md) | complete against 0013 §4.7's six clauses |
| [`zmq-py.md`](zmq-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-zmq-py` (the binding) | pyzmq 27.2.0 over libzmq 4.3.5 | [`zeromq.md`](../research/zeromq.md) | covers: all eleven socket types, the three transports, the 98-row option table, PLAIN/CURVE/ZAP with a handler in Python, the monitor, the devices, and a synchronous surface beside the asyncio one. Does not cover: `zmq.STREAM` and the DRAFT socket types, `zmq.Poller`/`sock.fd`, `send_string`/`send_json`/`Frame`, pyzmq's `zmq.auth` policy framework, and **concurrent send and recv on one socket** — see §9.1, which is the library's `&mut self` and is filed against `weida-zmq` |
| [`nng.md`](nng.md) | `weida-sp` (codec), `weida-nng` (implementation) | NNG 1.4.0-rc.0, through the `nng` crate 1.0.1 | [`nanomsg-nng.md`](../research/nanomsg-nng.md) | complete against 0013 §4.7's six clauses, read for SP |
| [`nng-py.md`](nng-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-nng-py` (the binding) | pynng 0.9.0 over NNG 1.11.0 — a **second C release** beside the Rust suite's 1.4.0-rc.0, which §1 states per claim | [`nanomsg-nng.md`](../research/nanomsg-nng.md) | covers: all eleven protocol classes and three context classes, `tcp`/`ipc`/`inproc`, the 50-row option table with 33 honoured and 17 refused by name, 23 client failures as exception classes, six interop pairings against pynng, and a synchronous surface over `weida-nng`'s `blocking` feature. Does not cover: `tls+tcp` (the library has the transport, this surface has no certificate API), raw sockets and `nng_device`, pipe callbacks and per-endpoint options; the wheel is built and run with no Rust toolchain by `package.sh`, as for the four other bindings (§10) |
| [`mqtt.md`](mqtt.md) | `weida-mqtt-codec` (codec), `weida-mqtt` (client) | rumqttd 0.20.0 and rmqtt 0.23.1 | [`mqtt5.md`](../research/mqtt5.md) | complete against 0013 §4.7's six clauses, read for MQTT. The first document here whose protocol is **asymmetric**: it is a client, the server is Phase D ([0014](../decisions/0014-parallel-libraries.md) §2), and §0 draws that line before the first row so a reader learns why there is no server column |
| [`mqtt-py.md`](mqtt-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-mqtt-py` (the binding) | paho-mqtt 2.x as the documented reference, **not pinned and never run**; measured against rumqttd 0.20.0 | [`mqtt5.md`](../research/mqtt5.md) | covers: connect with 19 keyword options each honoured or refused by name, the session, publish returning a `Completion`, subscriptions as iterators of `Delivery`, all four CONNACK-declared limits readable, 51 exception classes (36 reason codes plus 14 without a byte plus the base), and a synchronous surface sharing every value class with the asyncio one. Does not cover: TLS — absent by construction, the library takes the caller's `rustls::ClientConfig` and `is_encrypted` is always `False` (§3, §9.1) — WebSockets, multi-step AUTH from Python, paho's `loop*` family, and interop against paho itself, which §10 records as not met |
| [`amqp.md`](amqp.md) | `weida-amqp-codec` (codec), `weida-amqp` (client) | `fe2o3-amqp` 0.17.0, the pure-Rust peer, through its `acceptor`; RabbitMQ 4.x **not run**, its binary absent | [`amqp10.md`](../research/amqp10.md) | complete against 0013 §4.7's six clauses, read for AMQP. Covers: all nine performatives, all nine message sections, both credit schemes, the settle modes and the five delivery states, `ANONYMOUS`/`PLAIN`/`EXTERNAL` and both routes to TLS. Does not cover: Part 4 transactions, link resumption, any interpretation of the three filter families (the field is carried), the WebSocket binding, and a broker — the standard defines no node model |
| [`amqp-py.md`](amqp-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-amqp-py` (the binding) | python-qpid-proton 0.40.0; the interop peer is `fe2o3-amqp` 0.17.0 | [`amqp10.md`](../research/amqp10.md) | covers: connection, session and link as objects, `await send` returning the delivery's terminal state, an async iterator of deliveries with all seven section accessors, credit granted explicitly, 28 conditions plus 16 client failures as exception classes, and a synchronous surface beside the asyncio one. Does not cover: TLS - the library takes the caller's `rustls::ClientConfig`, which has no Python spelling and is filed rather than guessed at (§9.5) - Proton's container and handler classes, the AMQP type wrappers, Part 4 transactions, and a listener |
| [`nats.md`](nats.md) | `weida-nats-codec` (codec), `weida-nats` (client) | `async-nats` 0.50 as the peer, **through a `nats-server` that was not available**: interop is written and `#[ignore]`d, not run | [`nats.md`](../research/nats.md) | five of 0013 §4.7's six clauses met; **clause 4, interop, is explicitly not met** and says so, because `nats-server` is absent and `async-nats` is a client rather than a substitute for one. Covers: all twelve verbs, both wildcards, queue groups, request-reply over an inbox with the 503 kept distinct from a timeout, all five credential forms, TCP and TLS. Does not cover: JetStream, a reconnect loop, the WebSocket transport, UNIX-domain sockets, and any cryptographic implementation of NKey or JWT signing — the caller supplies a closure |
| [`nats-py.md`](nats-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-nats-py` (the binding) | nats-py 2.15.0 | [`nats.md`](../research/nats.md) | covers: connect with 24 refused-by-name options, publish with headers, subscriptions as async iterators with the `sid` and the `NATS/1.0` status readable, queue groups asserted on the wire, `unsubscribe_after`, `request` with a mandatory timeout, `request_many`, the 503 as its own class beside the timeout, 24 client failures as exception classes, and a synchronous surface nats-py does not have at all. Does not cover: TLS - the library takes the caller's `rustls::ClientConfig`, which has no Python spelling (§8.5) - JetStream, `nats.micro`, push callbacks, drain, a server pool and any reconnect loop. **Interop is not met**: `nats-server` is absent, so the two tests that need one are skipped |
| [`weida-py.md`](weida-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-py` (the binding) | **none — this is weida's own binding**, so the reference is the **Rust API of `crates/weida`** it binds, which its §1 states before the first row: weida has no other Python client and the protocol has one implementation | none; the normative specification is [`PROTOCOL.md`](../PROTOCOL.md) | covers: `Runtime` on a reactor of its own, QUIC `bind`, all three patterns in both the whole-payload and the streamed form (`Requester`/`Replier`, `Pusher`/`Puller`, `Publisher`/`Subscriber` with `FanOut`'s two write forms), 25 `#[pyclass]`es and 41 names in `__all__`, 22 exception classes (21 library variants plus the base, one renamed — `Error::Runtime` → `weida.RuntimeFailure`, because `weida.Runtime` is the runtime), an **explicit receive ceiling on every receive** and no default for it, and a synchronous surface of nine classes over `weida::blocking`. Does not cover: the raw L0 surface, `Peer` and `Acceptor` (§9.2), a Python *server* on the `unix`/`pipe`/`inproc` transports, eight of `RuntimeConfig`'s nine fields and all of `Limits`, streaming in `weida.sync` (§9.3), `Runtime.shutdown` and `Binding.close` on the asyncio surface, and type stubs. It has **no interop section**, and §2 says why rather than counting pairings: there is no second implementation to pair against |

Every binding this repository ships has its document above; there is no planned entry.
