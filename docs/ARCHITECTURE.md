# Architecture

This document describes the shape of the system: the layer model, the terminology, the
crate boundaries, and the v0 runtime design. It derives from master doc §0, §3, §4, §48,
§73 and §74.

Related: [PROTOCOL.md](PROTOCOL.md), [GUARANTEES.md](GUARANTEES.md),
[FAILURE_MODEL.md](FAILURE_MODEL.md), [INVARIANTS.md](INVARIANTS.md),
[IMPLEMENTATION.md](IMPLEMENTATION.md).

---

## 1. Layer model

All components derive from one coherent model:

```text
                         APPLICATIONS
                              │
                    idiomatic language APIs
                              │
                    messaging patterns/policies
                              │
                    Endpoint / Transfer core
                              │
              ┌───────────────┴───────────────┐
              │                               │
         brokerless                       brokered
              │                               │
              └───────────────┬───────────────┘
                              │
                    native protocol model
                              │
              ┌───────────────┴────────────────────────┐
              │                                        │
          native QUIC                              adapters
                                                  Web
                                                  ZeroMQ
                                                  MQTT
                                                  AMQP 0-9-1
                                                  ...
```

The central idea is:

> The framework provides one messaging model. Brokerless peer-to-peer operation and brokered
> operation use the same patterns and semantics. A broker adds discovery, shared state,
> persistence, load distribution, replication and consistency; it does not introduce a second
> programming model.

The v0 implementation occupies the left branch of both forks: brokerless, native QUIC. It
MUST NOT acquire a second messaging model when the brokered branch is built.

---

## 2. Terminology

The term `Node` is deliberately not used; it is too overloaded.

### Runtime

A process-level execution/resource container. The asynchronous Rust implementation is
primary. The Runtime owns or coordinates asynchronous execution, timers, DNS, QUIC runtime
integration, connection pooling, buffer pools, transport/adapter registrations, shutdown,
common resource limits and observability infrastructure.

A process MAY create multiple Runtimes, especially in tests or deliberately isolated
applications.

### Listener

A Listener represents one logical externally reachable messaging namespace.

A Listener is **not** equivalent to one OS socket. A Listener may have several physical
bindings:

```text
Listener
 ├── QUIC IPv4 :7443
 ├── QUIC IPv6 :7443
 ├── Web :443
 └── potentially other adapters
```

All bindings belonging to the same Listener expose the same endpoint namespace:

```text
Listener
  bindings:
      quic://0.0.0.0:7443
      quic://[::]:7443
      web://0.0.0.0:443

  endpoints:
      /jobs
      /events
      /users
```

Different network interfaces required purely for IPv4/IPv6/network-segment reachability
belong to one Listener. If two externally reachable interfaces are meant to be
fundamentally isolated and expose different namespaces or security policies, use two
Listeners.

### Binding

A Binding represents one concrete externally reachable transport/protocol binding. Native
QUIC is the reference transport; Web, ZeroMQ, MQTT, AMQP and future protocols are adapter
bindings. This semantic division between Listener and Binding is mandatory.

### Endpoint

An Endpoint is the actual typed messaging object: `Endpoint<Req>`, `Endpoint<Rep>`,
`Endpoint<Push>`, `Endpoint<Pull>`, `Endpoint<Pub>`, `Endpoint<Sub>`, and later the rest.

Rust uses strong types rather than one dynamically configured monomorphic socket object.
The user does not manipulate a generic `Socket` concept merely because ZeroMQ historically
did. Public pattern names are not overfitted before protocol semantics are finalized.

### Transfer

A Transfer is one user data flow. Each Transfer maps to exactly one transport stream
(master doc §7, §8): a protocol header followed by an opaque user byte stream terminated by
FIN. The fundamental API is `open transfer` / `write or read stream` / `finish`; small
message convenience APIs are built on top of it and never the reverse. A 40-byte transfer
and a 40-GB transfer use the same protocol semantics.

Req/Rep is built from **correlated** transfers: the request transfer has a `transfer_id`,
the reply transfer has its own `transfer_id` plus a `correlation_id` naming the request
(master doc §12). The reply is its own user-data stream, so request bytes and reply bytes
can flow simultaneously. Reliability ACKs are separate from user replies: an ACK is protocol
state, a reply is application data, and the two are never conflated.

---

## 3. Addressing

The address syntax is a URL:

```text
weida://host:port/path
```

The master doc writes this as `mq://`. `weida://` is the concrete scheme chosen for this
implementation; per master doc §82 the names in the master doc are working names. The scheme
exists as a single constant `SCHEME` in `crates/core/src/addr.rs`.

Path rules:

- The path is an **opaque endpoint identifier**.
- It MUST start with `/`.
- It MUST be 1..=512 bytes long.
- It MUST NOT contain any byte below `0x20`.

The core MUST NOT interpret the path beyond opaque lookup (master doc §81 rule 5):

- `/foo/bar` is **not** a hierarchy.
- There are no routing wildcards on endpoint paths.
- Path components carry no topic semantics.

Pattern-specific filtering — for example Pub/Sub subscription matching — happens after an
Endpoint has been reached and has nothing to do with endpoint routing.

Port is required. IPv6 literals use bracket form, e.g. `weida://[::1]:7443/x`.

---

## 4. Crate map

```text
crates/
    core/       →  weida-core       I/O-free model
    protocol/   →  weida-protocol   wire codec, no I/O
    weida/      →  weida            runtime + native QUIC transport + Req/Rep
```

### `weida-core`

The I/O-free model. No tokio, no sockets, no quinn. Contains `Error`, `TransferId`,
`EndpointAddr` and the `SCHEME` constant, `Limits`, `AckMode`/`AckState`/`Outcome`,
`TraceContext` with its W3C traceparent codec, and the send/receive/correlation state
machines expressed as pure transition functions. Because it is I/O-free, every rule in
[FAILURE_MODEL.md](FAILURE_MODEL.md) §4 is unit-testable without a network.

### `weida-protocol`

The wire codec, also with no I/O: RFC 9000 varints, the stream preamble with its
cap-before-allocation check, the five frame headers with hand-written CBOR encoders and
decoders, the negotiation function, and the QUIC application error code constants. Being
I/O-free makes it directly fuzzable: a fuzz target feeds it arbitrary bytes with no socket
in the way.

### `weida`

The runtime and the native QUIC transport: configuration, TLS setup, the `Runtime`, the
client connection pool, the per-connection driver, the Listener/Binding/namespace machinery,
the typed endpoints, and the transfer handles. The QUIC-specific code lives in module
`transport`.

### Dependency direction

```text
core
 ↑
protocol
 ↑
weida
```

`weida-core` MUST NOT depend on `weida-protocol` or `weida`. `weida-protocol` MUST NOT
depend on `weida`. Circular architectural dependencies are prohibited (master doc §74). The
core must not depend on transport, runtime or, later, broker implementation details.

### Why runtime and transport are one crate

Master doc §73 lists `runtime/` and `transport-quic/` as separate crates. They are merged
into `crates/weida` for this increment, on the authority of §73's own rule:

> Do not create dozens of tiny crates prematurely.
> Start with strong module boundaries and split crates where the dependency/ownership
> boundary is real.

The boundary is not real yet. Exactly one transport exists, so a `runtime` crate would have
exactly one consumer and a `transport-quic` crate exactly one dependent; the split would
buy no isolation and no independent versioning while adding a public API surface between two
halves of one design. The QUIC code is confined to module `transport` inside `crates/weida`,
so extracting it later is a mechanical move: the module boundary that a crate split needs
already exists and is already enforced by the module system. The split becomes worthwhile
when a second transport or adapter binding appears — that is, at Phase 8 (Web adapter) at
the earliest.

---

## 5. Runtime internals, v0

### Connection driver

There is **one `ConnDriver` actor task per connection**, running identical code on both
sides. Server-side connections carry an `Arc<Namespace>`; client-side connections carry the
correlation and ACK tables. Both are the same struct, with an empty namespace on the client.

The driver owns plain `HashMap`s, not `Arc<Mutex<HashMap>>` (master doc §48):

```text
pending_acks:     HashMap<u64, oneshot::Sender<AckOrError>>
pending_replies:  HashMap<u64, oneshot::Sender<IncomingTransfer>>
active_requests:  HashMap<u64, watch::Sender<bool>>     // cancel signals
```

Sole ownership by one task removes the lock entirely; API handles reach the tables by
sending a `Ctl` message on a bounded `mpsc` channel (depth 1024) and awaiting a oneshot
reply.

### Payload bytes never traverse the actor

This is a hard rule. Handles own their quinn `SendStream` and `RecvStream` directly, so the
hot path from `write_all` to the wire has **no task hop and no lock**. The actor sees only
control events: registrations, ACKs, ERRORs, CANCELs, reply arrivals. Routing payload
through the actor would reintroduce exactly the per-transfer heavyweight actor that
master doc §49 forbids.

### Register before open

`open()` reserves a `transfer_id` from a shared `AtomicU64`, sends `Ctl::Register` and
awaits the actor's oneshot confirmation **before** opening the uni stream. This eliminates
the ACK-arrives-before-registration race: by the time the peer can possibly have seen the
DATA header, the pending-ACK entry already exists.

### Accept dispatch

Per connection an `accept_uni` loop spawns one task per incoming uni stream; concurrency is
bounded by the QUIC `max_concurrent_uni_streams` limit rather than by an unbounded spawn.
Each task reads the preamble and header — with the length cap checked before allocation —
and then dispatches by kind:

- **HELLO** → negotiate, publish the result to a `watch<Option<Agreed>>`.
- **DATA, role = request** → namespace lookup, then `mpsc.send(IncomingRequest).await` into
  the Replier queue; or the `UNKNOWN_ENDPOINT` / `UNSUPPORTED` handling of
  [PROTOCOL.md](PROTOCOL.md) §9.4.
- **DATA, role = reply** → `Ctl::ReplyArrived`; the actor resolves `pending_replies` and
  hands over the still-streaming `IncomingTransfer`.
- **ACK, ERROR, CANCEL** → a `Ctl` message to the actor.

DATA tasks park on the negotiated-`watch` until the peer HELLO has been processed. This is
the parking rule of [PROTOCOL.md](PROTOCOL.md) §2.2, and it is why an early DATA stream is
never a protocol violation.

### ACK emission

When an `IncomingTransfer` reaches EOF — the application read it to FIN — and its header
carried `ack_mode = Accepted`, the handle fires `Ctl::SendAck { re }` and does not wait. The
actor opens a uni stream, writes the ACK frame and FINs it.

### Client connection pool

The `Runtime` holds one lazily-bound client `quinn::Endpoint` (`[::]:0`, falling back to
`0.0.0.0:0` where IPv6 is unavailable) and a map keyed by `(host, port)`. `connect()`
resolves the host, dials, uses the host string as written for TLS server-name verification,
and waits for the HELLO exchange before returning — so a returned connection is always
negotiated. A `Requester` stores `(connection, path)` per `connect()` call and round-robins
across them on `open()`.

### TLS

Server: PEM certificate chain and key are loaded from disk, `alpn_protocols` is set to
exactly `[b"weida/0"]`, and the transport configuration applies the stream and connection
windows, the uni-stream limit and the idle timeout from `Limits`.

Client: trust anchors come **only** from explicitly configured PEM root files. There are no
platform roots and no insecure-skip-verification mode in v0. Same ALPN, plus the 10 s
keep-alive.

---

## 6. One Req/Rep exchange

Five streams are involved: two HELLOs, the request DATA, the reply DATA, and the ACK. Each
is a separate QUIC uni stream; there is no permanent control stream anywhere.

```text
  Requester                                                     Responder
      │                                                             │
      │  QUIC handshake, ALPN "weida/0"                             │
      │◄═══════════════════════════════════════════════════════════►│
      │                                                             │
      │  uni stream 1:  HELLO, FIN                                  │
      │────────────────────────────────────────────────────────────►│
      │                                  uni stream 2:  HELLO, FIN  │
      │◄────────────────────────────────────────────────────────────│
      │           negotiate() on both sides -> Agreed               │
      │                                                             │
      │  uni stream 3:  DATA{endpoint:"/transform", transfer_id:1,  │
      │                     role:request, ack_mode:accepted}        │
      │────────────────────────────────────────────────────────────►│
      │  payload bytes ...                                          │  namespace lookup,
      │────────────────────────────────────────────────────────────►│  hand to application
      │                                                             │
      │                 uni stream 4:  DATA{transfer_id:1,          │  reply opened BEFORE
      │                     role:reply, correlation_id:1}           │  request FIN
      │◄────────────────────────────────────────────────────────────│
      │  ... more request payload  ────────────────────────────────►│
      │  ◄──────────────────────────────  ... reply payload ...     │
      │  FIN (request)  ───────────────────────────────────────────►│  read to FIN,
      │                                                             │  handed to app
      │                          uni stream 5:  ACK{re:1, state:1}  │
      │◄────────────────────────────────────────────────────────────│
      │  finish() -> Acked(Accepted)                                │
      │  ◄─────────────────────────────  FIN (reply)                │
      │  recv() -> IncomingTransfer                                 │
      │                                                             │
```

Stream 5 (ACK) and stream 4 (reply) are independent: the ACK does not wait for the reply and
the reply does not imply the ACK. Stream numbering above is illustrative ordering, not QUIC
stream ids; uni streams are unordered relative to one another.

---

## 6a. Pattern taxonomy

Master doc §82 leaves open "whether a smaller internal primitive set can implement the
patterns cleanly". Answered from the built system: yes. Req/Rep decomposes into four
primitives, and the other patterns are compositions of the same four, not new machinery.

| # | Primitive | Where it lives | Used by |
| --- | --- | --- | --- |
| P1 | one-way transfer: register → open uni → DATA header → payload → outcome | `OutgoingTransfer`, `ConnCtx::register` | Req, Push, Pub (per copy) |
| P2 | correlation: pending-reply table keyed by transfer id | `Correlator`, the connection actor | Req/Rep only |
| P3 | peer set plus a selection policy | `PeerSet` in `endpoint.rs`; fan-out in `SubRegistry` | Req, Push (round-robin), Sub (all peers), Pub (fan-out) |
| P4 | bounded inbound queue behind an opaque path | `Namespace` + per-endpoint mpsc | Rep, Pull, Sub |

So: **Req = P1 + P2**, **Push = P1**, **Pull = P4**, **Sub = P4 + P3**, and
**Pub = P1 per matching subscriber under a fan-out policy**. The only genuinely new code
Phase 3 required is the fan-out selection policy and the subscription registry that feeds
it; acknowledgement modes, cancellation, outcome vocabulary and backpressure are all
pattern-independent because they live in P1 and P4.

This is what "patterns are orthogonal to guarantees" (master doc §16) buys concretely:
"Push/Pull with best effort" and "Push/Pull with accepted ACK" needed no reliability code
of their own.

### Router/Dealer are emergent, not missing

ZeroMQ needs Dealer and Router because one socket is one ordered pipe: Dealer exists to
multiplex unsynchronized requests onto that pipe, Router to address replies back to a
specific peer identity.

Neither constraint exists here.

- **Unsynchronized multiplexed requests**: every request is already its own QUIC stream
  with an explicit `correlation_id`. `Requester::open()` permits unlimited concurrent
  in-flight requests with no lockstep. That is what Dealer provides.
- **Identity-addressed replies**: a `Replier` answers over the connection the request
  arrived on, so peer identity is implicit in the connection rather than carried in an
  envelope. That is what Router provides for the direct-peer case.

What Router adds *beyond* that — forwarding to third parties, explicit identity envelopes,
routing tables — is broker work (master doc §47, §85), belongs to Phase 6, and would be a
new component rather than a new socket type. Introducing `Endpoint<Router>` in v0 would
name a distinction the transport does not have.

Pair patterns are skipped for the same reason: a Pair is a Req/Rep or Push/Pull peer with a
narrower API, architecturally identical.

### Why oneshot fan-out is unordered

P1 is one transfer per stream, and QUIC does not order streams relative to one another. A
publisher's per-subscriber writer is serialized, so copies are handed to the transport in
publication order — but that is a property of one hop's implementation, not a guarantee.
Making per-producer ordering real requires a sequence number in the DATA header and
reassembly on the receiving side; that is a deliberate later protocol addition rather than
something to imply from the current behaviour ([GUARANTEES.md](GUARANTEES.md) §6).
---

## 7. Public API v0

The surface of crate `weida`.

```rust
// re-exports from weida-core: Error, Outcome, AckState, AckMode, Limits, TraceContext, EndpointAddr
pub struct RuntimeConfig { pub limits: Limits, pub client_tls: Option<ClientTls>,
    pub keep_alive: Duration /*10s*/, pub idle_timeout: Duration /*30s*/ }   // Default impl
pub enum Pem { Bytes(Vec<u8>), File(PathBuf) }              // TLS material need not be a file
pub struct ClientTls { pub roots_pem: Vec<Pem> }            // explicit trust anchors, required for connect()
impl ClientTls { pub fn from_pem_file(p) -> Self; pub fn from_pem(bytes) -> Self; }
pub struct ServerTls { pub cert_chain_pem: Pem, pub key_pem: Pem }
impl ServerTls { pub fn new(cert_path, key_path) -> Self; pub fn from_pem(cert, key) -> Self; }

pub struct Runtime;                                          // Clone (Arc inner); needs ambient tokio
impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error>;   // Error::Runtime if no tokio handle
    pub async fn listener(&self, tls: ServerTls) -> Result<Listener, Error>;
    pub fn requester(&self) -> Requester;
    pub fn pusher(&self) -> Pusher;                          // Push connects, Pull binds
    pub fn subscriber(&self) -> Subscriber;                  // Sub connects, Pub binds
    pub async fn shutdown(self);                             // close all conns/bindings code SHUTDOWN, wait_idle
}
pub struct Listener;                                         // owns Namespace shared by all bindings
impl Listener {
    pub async fn bind_quic(&self, addr: SocketAddr) -> Result<Binding, Error>;
    pub fn replier(&self, path: &str) -> Result<Replier, Error>;   // Error::InvalidEndpointPath / AlreadyRegistered
    pub fn puller(&self, path: &str) -> Result<Puller, Error>;     // same path-uniqueness rule
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error>;
}
pub struct Binding; impl Binding { pub fn local_addr(&self) -> SocketAddr; }

pub struct Endpoint<P: Pattern>;             // Pattern sealed; markers Req, Rep, Push, Pull, Pub, Sub
pub type Requester = Endpoint<Req>; pub type Replier = Endpoint<Rep>;
pub type Pusher = Endpoint<Push>;   pub type Puller = Endpoint<Pull>;
pub type Publisher = Endpoint<Pub>; pub type Subscriber = Endpoint<Sub>;

impl Requester {                                             // multi-peer: connects append; open() round-robins
    pub async fn connect(&self, url: &str) -> Result<(), Error>;   // weida://host:port/path; pooled per host:port
    pub async fn open(&self, meta: TransferMeta) -> Result<(OutgoingTransfer, PendingReply), Error>;
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error>; // open+write_all+finish+recv
}
impl Replier { pub async fn accept(&self) -> Result<IncomingRequest, Error>; }

impl Pusher {                                                // same peer set and round-robin as Requester
    pub async fn connect(&self, url: &str) -> Result<(), Error>;
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error>; // no reply slot
    pub async fn send(&self, body: &[u8]) -> Result<Outcome, Error>;                 // open+write_all+finish
}
impl Puller { pub async fn recv(&self) -> Result<IncomingTransfer, Error>; }

impl Publisher {                                             // synchronous: never awaits a subscriber
    pub fn publish(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error>;
    pub fn subscriber_count(&self) -> usize;                 // ops metrics
    pub fn filter_count(&self) -> usize;
    pub fn dropped(&self) -> u64;                            // messages lost to slow subscribers
}
impl Subscriber {
    pub async fn connect(&self, url: &str) -> Result<(), Error>;   // claims path in the client conn's namespace
    pub async fn subscribe(&self, filter: &str) -> Result<(), Error>;   // byte prefix; "" = everything
    pub async fn unsubscribe(&self, filter: &str) -> Result<(), Error>;
    pub async fn recv(&self) -> Result<IncomingTransfer, Error>;   // topic on IncomingMeta::topic
}

#[derive(Default, Clone)] pub struct TransferMeta { pub content_type: Option<String>,
    pub content_len: Option<u64>, pub ack_mode: AckMode, pub trace: Option<TraceContext> } // None → generate ids

pub struct OutgoingTransfer;                                 // impl tokio::io::AsyncWrite
impl OutgoingTransfer {
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error>;
    pub async fn finish(self) -> Result<Outcome, Error>;     // Ok(SentBestEffort|Acked(..)); Err(Indeterminate|Rejected|..)
    pub fn cancel(self);                                     // RESET_STREAM(CANCELED)
}
pub struct PendingReply;                                     // Drop before recv → CANCEL frame
impl PendingReply { pub async fn recv(self) -> Result<IncomingTransfer, Error>; }
pub struct IncomingTransfer;                                 // impl tokio::io::AsyncRead
impl IncomingTransfer { pub fn meta(&self) -> &IncomingMeta; // endpoint, ids, ack_mode, content_*, trace
    pub async fn collect(self, max_bytes: usize) -> Result<Vec<u8>, Error>; } // Error::LimitExceeded over cap
pub struct IncomingRequest;
impl IncomingRequest { pub fn meta(&self) -> &IncomingMeta;
    pub fn body(&mut self) -> &mut IncomingTransfer;
    pub async fn reply(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error>; // correlated uni stream, callable pre-body-FIN; trace defaults to request's context
    pub fn canceled(&self) -> tokio::sync::watch::Receiver<bool>; } // flips on CANCEL frame
```

Type by type:

- **`RuntimeConfig`** — everything a `Runtime` needs: resource limits, optional client trust
  anchors, and the two timers. Has a `Default`.
- **`ClientTls`** — explicit PEM trust anchors. Required to `connect()`; there is no
  platform-root or skip-verification path in v0.
- **`ServerTls`** — PEM certificate chain and private key paths for a server binding.
- **`Runtime`** — the process-level container of §2. `Clone`, sharing an `Arc` inner. Needs
  an ambient tokio runtime; construction fails with `Error::Runtime` if there is none.
- **`Listener`** — one logical messaging namespace, owning the endpoint `Namespace` shared by
  all of its bindings.
- **`Binding`** — one concrete QUIC binding; exposes its resolved local address, which is how
  tests learn an ephemeral port.
- **`Endpoint<P>`** — the typed messaging object. `Pattern` is a sealed trait; `Req` and `Rep`
  are the v0 markers.
- **`Requester`** — `Endpoint<Req>`. Multi-peer: each `connect()` appends a peer and `open()`
  round-robins across them, preserving the ZeroMQ multi-peer property.
- **`Replier`** — `Endpoint<Rep>`. `accept()` yields inbound requests from the endpoint queue.
- **`TransferMeta`** — per-transfer outbound metadata. A `None` trace means the runtime
  generates fresh trace and span ids.
- **`OutgoingTransfer`** — the write half of a transfer, an `AsyncWrite`. `finish()` returns
  the outcome per [FAILURE_MODEL.md](FAILURE_MODEL.md) §4; `cancel()` resets the stream with
  `CANCELED`.
- **`PendingReply`** — the requester's claim on a reply. Dropping it before `recv()` sends a
  CANCEL frame, so an abandoned request does not leave the responder streaming into nothing.
- **`IncomingTransfer`** — the read half of a transfer, an `AsyncRead`, plus its metadata.
  `collect(max_bytes)` is the opt-in materialization convenience with a mandatory cap; it is
  never used internally.
- **`IncomingRequest`** — an accepted inbound request: its metadata, its body as an
  `IncomingTransfer`, `reply()` to open the correlated reply stream (callable before the body
  reaches FIN, and defaulting the reply's trace context to the request's), and `canceled()`
  to observe the peer's CANCEL frame.
