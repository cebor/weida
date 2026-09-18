# Master Architecture and Implementation Plan

## 0. Purpose of this document

Design and implement a modern messaging framework inspired by ZeroMQ, nanomsg/NNG and brokered systems such as RabbitMQ, but designed from first principles around QUIC, Rust, streaming payloads, explicit reliability guarantees, browser clients, brokerless operation, distributed brokers, standalone foreign-protocol libraries and explicitly managed broker connectors.

This document is normative. Treat it as the architectural source of truth for the repository.

The project must not become a collection of loosely connected features. All components must derive from one coherent model:

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
              ┌───────────────┴───────────────┐
              │                               │
          native QUIC                    Web binding
```

Standalone ZeroMQ, NNG, MQTT, AMQP and NATS libraries are a separate product line. They keep
their own semantics and are not hidden behind the framework's patterns.

The central idea is:

> The framework provides one messaging model. Brokerless peer-to-peer operation and brokered operation use the same patterns and semantics. A broker adds discovery, shared state, persistence, load distribution, replication and consistency; it does not introduce a second programming model.

---

# 1. Project goals

The framework should cover the spectrum from extremely lightweight peer-to-peer messaging to durable distributed message processing.

Primary goals:

1. Provide a ZeroMQ-like high-level networking abstraction designed natively for Rust and QUIC.
2. Preserve QUIC's stream semantics rather than rebuilding a message protocol on top of long-lived byte streams.
3. Support both brokerless and brokered architectures using the same public concepts.
4. Allow applications to explicitly trade performance against reliability and consistency.
5. Make guarantees precise rather than implying vague notions such as "reliable".
6. Support streaming payloads of arbitrary size without requiring materialization.
7. Make browser clients first-class participants.
8. Provide native interoperability with widely deployed messaging protocols.
9. Support a distributed broker cluster that appears to clients as one logical broker.
10. Produce first-class APIs for Rust and idiomatic bindings for many other languages.
11. Include CLI, tracing, metrics, inspection and debugging as first-class capabilities.
12. Minimize overhead added above QUIC.
13. Keep the dependency set deliberately small.

This is not merely a queue implementation. It should support the broad family of messaging patterns traditionally covered by ZeroMQ/NNG as well as queue/broker semantics traditionally associated with RabbitMQ and similar systems.

---

# 2. Non-goals and scope boundaries

Do not turn the framework into:

- a general IAM system;
- a distributed database with arbitrary tables and queries;
- a Byzantine-fault-tolerant consensus system;
- an application framework;
- an HTTP replacement;
- a mandatory broker architecture;
- a system that always enables the strongest possible guarantees.

The framework must explicitly support weak guarantees when those are desirable.

High-frequency trading, telemetry and low-latency systems may prefer:

```text
no persistence
no ACK
minimal allocations
minimal state
best effort
```

while business-critical workflows may prefer:

```text
durable outgoing state
remote persistence
replication
deduplication
processed ACK
```

Both must be valid first-class configurations.

---

# 3. Fundamental terminology

Avoid overloading the concept `Node`.

The main abstractions are:

## Runtime

A process-level execution/resource container.

The asynchronous Rust implementation is primary.

The Runtime owns or coordinates resources such as:

- asynchronous execution;
- timers;
- DNS;
- QUIC runtime integration;
- connection pooling;
- buffer pools;
- transport/binding registrations;
- shutdown;
- common resource limits;
- observability infrastructure.

Example conceptual API:

```rust
let runtime = Runtime::new(config);
```

A process may create multiple runtimes, especially in tests or deliberately isolated applications.

---

## Listener

A Listener represents one logical externally reachable messaging namespace.

A Listener is **not** equivalent to one OS socket.

A Listener may have several physical bindings:

```text
Listener
 ├── QUIC IPv4 :7443
 ├── QUIC IPv6 :7443
 ├── Web :443
 └── potentially other protocol-preserving bindings
```

All bindings belonging to the same Listener expose the same endpoint namespace.

Example:

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

This distinction is important.

Different network interfaces required purely for IPv4/IPv6/network-segment reachability may belong to one Listener.

If two externally reachable interfaces are meant to be fundamentally isolated and expose different namespaces or security policies, use two Listeners.

---

## Binding

A Binding represents one concrete reachable transport of the weida protocol.

Conceptually:

```rust
listener.bind(Quic, "[::]:7443").await?;
listener.bind(Web, "[::]:443").await?;
```

The exact Rust syntax remains open, but this semantic division is mandatory. Native QUIC is
the reference transport. Web is a binding because it preserves the same namespace and protocol
model over browser transports.

Foreign-protocol libraries are not bindings. Their addressing and semantics remain their own;
broker integration uses explicitly configured Connector resources instead.

---

## Endpoint

An Endpoint is the actual typed messaging object.

Examples conceptually:

```text
Endpoint<Pub>
Endpoint<Sub>
Endpoint<Push>
Endpoint<Pull>
Endpoint<Req>
Endpoint<Rep>
```

The exact names of patterns are subject to design, but Rust should use strong types instead of one dynamically configured monomorphic socket object.

The user should generally not manipulate a generic `Socket` concept simply because ZeroMQ historically did.

Possible implementation:

```rust
Endpoint<Pub>
```

with ergonomic constructors such as:

```rust
listener.publisher("/events");
runtime.producer();
```

Do not overfit the public names before the protocol semantics are finalized.

---

# 4. Endpoint addressing

The address syntax should resemble a URL:

```text
mq://hostname:port/system-messages
```

The path is an **opaque endpoint identifier**.

This is critical.

Do not interpret:

```text
/foo/bar
```

as a hierarchy in the transport core.

Do not implement routing wildcards on endpoint paths.

Do not overload path components with topic semantics.

For the core:

```text
"/foo/bar"
```

is simply an identifier.

Pattern-specific filtering happens after an Endpoint has been reached.

Example Pub/Sub:

```rust
let sub = runtime.subscriber();
sub.connect("mq://host:7443/system-messages").await?;
sub.subscribe("important*").await?;
```

Here:

```text
/system-messages
```

addresses the Endpoint.

```text
important*
```

belongs to Pub/Sub semantics and has nothing to do with endpoint routing.

---

# 5. Multiple peers and bind/connect semantics

Endpoints may connect to multiple peers.

This property from ZeroMQ is valuable and must be retained.

Conceptually:

```rust
endpoint.connect(peer_a).await?;
endpoint.connect(peer_b).await?;
endpoint.connect(peer_c).await?;
```

The messaging pattern determines what multiple peers mean.

Examples:

- Pub may fan out.
- Push may load-balance.
- Sub may aggregate several sources.
- Req may distribute requests.
- Router-like patterns may perform explicit routing.

`bind` versus `connect` must not define the messaging role.

They primarily determine which side initiates transport establishment.

After establishment, the messaging pattern determines behavior.

---

# 6. Native transport: QUIC

QUIC is intentionally chosen despite having more computational overhead than raw TCP or specialized transports.

The framework accepts this cost in exchange for:

- TLS encryption by default;
- authenticated server identity;
- optional client identity;
- multiplexing;
- stream-level flow control;
- connection-level flow control;
- connection migration;
- modern congestion control;
- reliable ordered streams;
- many cheap concurrent streams.

The framework must minimize **additional** overhead above QUIC.

The hot path must avoid:

- unnecessary allocations;
- unnecessary copies;
- full-message materialization;
- global locks;
- unnecessary task hops;
- redundant framing.

---

# 7. Fundamental stream model

The central protocol primitive is not a "message inside a long-running stream".

Instead:

> Every user data flow maps naturally to a transport stream.

For native QUIC:

```text
QUIC stream:

[protocol metadata/header]
[user bytes....................]
FIN
```

After the framework's metadata header, the user payload remains an opaque byte stream until FIN.

Do not introduce:

```text
[length][payload][length][payload]
```

inside a persistent data stream unless a specific adapter requires it.

---

# 8. Streams rather than materialized messages

The framework's fundamental API should support:

```text
open transfer
write/read stream
finish
```

Small-message convenience APIs should be built on top.

For example:

```rust
producer.send(bytes).await?;
```

may internally use:

```rust
let mut transfer = producer.open(meta).await?;
transfer.write_all(bytes).await?;
transfer.finish().await?;
```

but the reverse must never be true.

A 40-byte transfer and a 40-GB transfer use the same fundamental protocol semantics.

---

# 9. Streaming routers

Routing infrastructure must not require complete payloads.

A forwarding component should be capable of:

```text
upstream stream
      │
      ▼
parse small metadata
      │
      ▼
routing decision
      │
      ▼
open downstream stream
      │
      ▼
incremental forwarding
```

Conceptually:

```text
read chunk
   ↓
optional hash/inspection
   ↓
write chunk
```

A router handling a 40-GB transfer must require bounded memory.

Full kernel-level zero-copy may not always be achievable because QUIC encryption/decryption is involved, but the design must strive for:

- buffer reuse;
- bounded buffering;
- minimal user-space copies;
- no `Vec<u8>` containing the whole payload.

Backpressure should naturally propagate through QUIC flow control wherever possible.

---

# 10. QUIC stream directions

QUIC supports both uni- and bidirectional streams.

The native protocol should primarily use **unidirectional streams for individual protocol transfers**.

Example:

```text
request:
client ─────────────► server

reply:
client ◄───────────── server
```

Request and reply are independent streams correlated by protocol metadata.

This is preferable to assuming Req/Rep must consume opposite halves of one bidirectional stream because it enables:

- response generation before request completion;
- simultaneous streaming request and response;
- multiple correlated replies;
- independent cancellation;
- independent retries;
- independent routing;
- future fanout patterns.

Bidirectional data streams remain available if future patterns genuinely benefit from them, but they should not be required by the fundamental model.

---

# 11. No permanent control stream

Do **not** maintain a permanent multiplexed control stream merely to transport control messages.

QUIC already provides cheap multiplexed streams.

Control events should generally be self-contained protocol streams themselves.

Examples:

```text
ACK
CANCEL
ERROR
PARTIAL_ACK
NEGOTIATION
CAPABILITY
```

Each may be a short uni-stream:

```text
[control header][optional metadata][FIN]
```

This avoids rebuilding a second multiplexing/framing/state-machine layer on top of QUIC.

---

# 12. Request/reply and correlation

Req/Rep is built from correlated transfers:

```text
request transfer:
transfer_id = 42

reply transfer:
transfer_id = 93
correlation_id = 42
```

The response is its own user-data stream.

This permits:

```text
request bytes ──────────────────────►
              ◄──────────────────── response bytes
```

simultaneously.

Reliability ACKs are separate from user replies.

An ACK is protocol state.

A reply is application data.

Never conflate these concepts.

---

# 13. Connection negotiation

When a transport connection is first established, peers perform protocol negotiation.

This negotiation may exchange:

- supported wire-protocol versions;
- supported capabilities;
- resource limits;
- allowed messaging semantics;
- supported ACK modes;
- supported persistence/reliability features;
- optional authentication information;
- adapter-specific constraints.

There is no requirement to keep a negotiation/control stream open afterwards.

Negotiation is an exchange performed during connection establishment.

---

# 14. Wire protocol

The wire protocol must remain deliberately small.

A data stream should look conceptually like:

```text
[magic/version envelope]
[header length]
[serialized protocol header]
[opaque user byte stream]
[FIN]
```

The exact serialization format must be evaluated early.

Candidates include:

- MessagePack;
- CBOR;
- another compact self-describing representation.

Do not prematurely invent a custom serialization format unless benchmarks or protocol constraints justify it.

The serialized header must support forward-compatible extension.

Likely conceptual fields:

```text
protocol version
stream/operation kind
endpoint identifier
transfer id
correlation id
role/pattern
per-transfer policies
content metadata
trace context
extension metadata
```

The final set must remain minimal.

---

# 15. Protocol versioning

Protocol evolution is one of the highest-risk parts of the project.

Release strategy:

```text
0.1
0.x    protocol explicitly experimental; breaking changes permitted
1.0    first stable wire protocol
2.0    expected eventually if fundamental mistakes must be corrected
```

Library version and protocol version must be independent.

Example:

```text
library 0.8.4 → wire protocol 0.3
library 3.2.0 → wire protocol 1
```

Protocol evolution rules:

- unknown optional fields must be ignorable;
- required unsupported capabilities cause explicit rejection;
- optional capabilities must be negotiated;
- new features should not require new major versions unless they fundamentally alter existing wire semantics;
- never silently downgrade a requested guarantee;
- incompatibility must produce a clear protocol-level error.

Protocol 1.0 should be reached only after substantial alpha feedback.

Do not prematurely freeze the protocol solely to claim stability.

---

# 16. Messaging patterns

The framework intends to support essentially the useful family of ZeroMQ/NNG messaging patterns.

Examples include:

```text
Pub / Sub
Push / Pull
Req / Rep
Dealer / Router
Pair-like patterns
advanced Pub/Sub variants
pipeline patterns
fanout
competing consumers
```

However:

> Patterns define communication topology and behavior. Reliability guarantees are orthogonal.

Do not hard-code ZeroMQ's historical reliability assumptions into these patterns.

For example:

```text
Push/Pull + best effort
Push/Pull + at least once
Push/Pull + remote persisted ACK
```

should be conceptually possible where semantics permit.

---

# 17. Reference use cases

Design guarantees using concrete workflows.

At minimum evaluate:

## Web API → queue → worker

```text
HTTP/API server
   ↓
Producer
   ↓
queue/broker
   ↓
Consumer worker
```

Questions:

- when may API return success?
- what survives server crash?
- what survives broker crash?
- what if worker crashes?
- what if worker processed the job but ACK disappears?

---

## Brokerless producer → worker

Evaluate:

- no persistence;
- local outbox persistence;
- remote inbox persistence;
- both;
- retries;
- process crashes;
- restart behavior.

---

## Req/Rep with side effects

Classic ambiguous case:

```text
request arrives
server commits business side effect
reply is lost
```

Client must not be told that the operation definitely failed.

---

## Pub/Sub telemetry

Optimize for:

- low latency;
- no persistence;
- optional drops;
- coalescing;
- many consumers.

---

## Event log / audit stream

Optimize for:

- ordered durable records;
- replay;
- no coalescing;
- durable consumer positions.

---

## Streaming processing

Example:

```text
40 GB input ─────► processor
         ◄─────── streaming output
```

Neither side must wait for the entire stream.

---

## Fanout

One transfer must be delivered to several independent recipients.

Define what "success" means.

---

## Competing consumers

Exactly one currently active consumer should normally take work, with re-delivery according to selected guarantees.

---

# 18. Reliability as transfer of responsibility

Reliability must be formulated around **responsibility transfer**.

An ACK means:

> The next hop has reached a precisely defined state at which the previous hop is allowed to release some or all responsibility.

Possible next-hop completion states include conceptually:

```text
Accepted
Stored
Replicated(n)
Processed
```

Avoid one vague universal `Committed` state.

Each term must have exact semantics.

Example:

### Accepted

The next hop has accepted responsibility in memory according to the selected policy.

### Stored

The next hop has persisted sufficient state to survive the documented failure domain.

### Replicated(n)

The next hop guarantees that configured replication criteria have been met.

### Processed

The next hop's application-level consumer has explicitly reported successful processing.

The actual protocol names should be chosen carefully.

---

# 19. Guarantees are hop-local

This is a hard invariant.

> All delivery and completion guarantees refer to the immediate next hop.

Example:

```text
Producer → Broker A → Broker B → Consumer
```

If Broker A returns `Stored`, this means Broker A has met the documented storage guarantee.

It does **not** inherently mean:

```text
Consumer eventually processed the message.
```

Long end-to-end guarantees arise only through explicit composition of hop-local guarantees.

Do not make hidden claims about downstream systems.

---

# 20. Guarantee dimensions

Do not collapse everything into one enum such as `Reliable`.

Model independent dimensions where appropriate.

Potential dimensions:

## Delivery

```text
BestEffort
AtMostOnce
AtLeastOnce
```

Exactly-once delivery must not be casually promised.

Exactly-once **effects** may be achievable with additional mechanisms such as durable idempotency/inbox/outbox cooperation.

---

## Acknowledgement/completion

Potential conceptual levels:

```text
None
Accepted
Stored
Replicated(...)
Processed
```

---

## Ordering

Potential levels:

```text
None
PerProducer
PerKey
Total
```

The framework must document the performance implications.

---

## Deduplication

```text
None
Bounded
Durable
```

---

## Backpressure behavior

Potential policies:

```text
Block
Reject
Drop
Spill
Coalesce
```

These names are provisional; semantics matter more than naming.

---

# 21. Configuration of guarantees

The framework must support the full performance/reliability spectrum.

Examples:

## Very fast

```text
persistence = none
ACK = none
ordering = none
dedup = none
backpressure = drop/coalesce
```

## Reliable job processing

```text
outgoing responsibility retained
remote persistence
at-least-once
deduplication
consumer ACK
```

## Strong broker persistence

```text
replication factor = 3
ACK after N persisted replicas
durable deduplication
consumer processed ACK
```

The framework must validate invalid combinations.

A requested guarantee must never be silently weakened.

---

# 22. Unknown outcomes

Distributed systems contain states where the caller cannot know what happened.

This must be represented explicitly.

Example:

```text
server persisted operation
ACK sent
network disappeared before caller received ACK
```

The truthful result is not necessarily:

```text
Failed
```

It may be:

```text
Indeterminate / UnknownOutcome
```

The public API must preserve this distinction.

---

# 23. Persistence in brokerless mode

Persistence is not a broker-only concept.

A producer may use a durable outgoing spool:

```text
application
    ↓
local durable outbox
    ↓
network
```

A consumer may use a durable incoming spool:

```text
network
    ↓
durable inbox
    ↓
application
```

Both may be enabled.

However, many workers are intentionally stateless and should not pay this cost.

Therefore:

> Persistence is an optional helper/module layered on the streaming abstraction.

The core transport must not require it.

---

# 24. Streaming persistence

Persistence must preserve stream semantics.

Do not require:

```rust
Vec<u8>
```

for durable storage.

Conceptually:

```text
RecvStream
    ├── incremental hash
    └── storage writer
```

and later:

```text
storage reader
    ↓
SendStream
```

The framework itself should never need the entire 40-GB payload in memory.

---

# 25. Storage architecture

Separate large payload storage from small state/control journaling.

Conceptually:

```text
Metadata/control WAL
    transfer state
    ownership changes
    ACK state
    consumer state
    tombstones
    replication state

Payload store
    streaming bytes
    segmented storage
    large blobs
```

Do not necessarily create one physical WAL file per transfer.

For millions of small transfers this would be undesirable.

Prefer abstractions supporting:

```text
shared segmented state log
+
stream-oriented payload store
```

Large transfers may internally use separate files or segments.

Small transfers may be inlined as an optimization.

These are storage implementation details, not protocol semantics.

---

# 26. Content hashing

QUIC already supplies cryptographic transport integrity.

Content hashes are therefore not required merely to know whether QUIC corrupted bytes.

They may nevertheless provide important higher-level functions:

- persisted-content integrity;
- content identity;
- deduplication;
- resumable transfers;
- replication verification;
- content-addressed storage.

Hashing should be optional and streamable.

---

# 27. Queue coalescing

TTL is not sufficient for many state-update queues.

Introduce a concept of **coalescing**.

Example:

```text
pending:
device:123 -> state v17
```

New update:

```text
device:123 -> state v18
```

may replace v17 instead of appending another stale update.

Thus under overload the queue converges toward:

```text
latest pending state per key
```

Potential modes:

```text
FIFO
TTL
Coalescing(key)
Coalescing(key) + TTL
```

Do not call these "duplicates"; payloads are different versions of the same logical state.

This can enable eventual convergence under clearly documented assumptions but must not casually claim unconditional "eventual consistency".

---

# 28. Broker architecture

A broker is built using the same framework.

It is not a separate networking stack.

Brokerless:

```text
Producer ───────────► Consumer
```

Brokered:

```text
Producer ─► Broker ─► Consumer
```

Patterns remain conceptually the same.

The broker adds:

- discovery;
- named durable/shared messaging objects;
- queue/channel ownership;
- load distribution;
- persistence;
- retries;
- deduplication;
- consumer groups;
- replication;
- cluster consistency;
- monitoring.

The broker is analogous to a database infrastructure service, except it manages queues/channels/messaging state rather than arbitrary relational tables.

---

# 29. Broker-side logical objects

The broker may internally expose objects such as:

```text
/jobs
   Queue

/events
   PubSubChannel

/rpc
   Routing state
```

Clients attach with appropriate roles.

Example:

```text
Producer → /jobs
Consumer ← /jobs
```

Do not force the entire broker implementation into a single `Endpoint<Queue>` abstraction if a cleaner internal model exists.

The client-facing semantics remain Endpoint/Pattern based.

---

# 30. Broker cluster as one logical broker

Hard design rule:

> A cluster of brokers appears to clients as one logical broker.

Conceptually:

```text
mq://broker.example.com/jobs
```

not:

```text
broker-a
broker-b
broker-c
```

Clients should not normally need to know:

- which broker owns a shard;
- where replicas are;
- which broker is current leader;
- where a transfer was physically stored.

Bootstrap may use:

- DNS;
- multiple seed addresses;
- SRV;
- an external load balancer;
- another discovery mechanism.

After connection, topology knowledge and routing optimizations may be learned transparently.

Topology may optionally become visible for advanced affinity/locality use cases, but it must not be required by normal applications.

---

# 31. Broker scaling versus fault tolerance

These are independent concerns.

## Placement / sharding

Determines where work is handled.

Examples:

```text
round robin
random
hash(key)
affinity(key)
partitioned
single-owner
```

## Replication

Determines how many copies exist.

```text
replication factor = N
```

## Commit policy

Determines what state must be reached before the upstream hop is acknowledged.

Examples:

```text
local accept
N accepts
N durable stores
quorum
all replicas
```

These must be independently configurable where meaningful.

Example:

```text
32 inferred shards
replication factor = 3
ACK after 2 persisted replicas
```

---

# 32. Partitioning should normally be derived

Do not force users to manually reason about raw partition IDs like Kafka unless they explicitly need to.

Prefer high-level requirements:

```text
parallelism = 32
ordering = per_key
replication = 3
placement = scalable
```

and derive internal partitioning.

Advanced users may eventually need lower-level controls, but partitions should be an implementation mechanism first.

---

# 33. Ordering and placement

Ordering constraints affect sharding.

Examples:

```text
Ordering::None
    freely distribute

Ordering::PerKey
    same key must map consistently

Ordering::Total
    expensive coordination/sequencing required
```

Never claim stronger ordering while distributing work in a way that cannot provide it.

---

# 34. Raft usage

Raft should coordinate the broker **control plane**, not blindly replicate every payload through the Raft log.

Likely Raft-controlled state:

```text
cluster membership
queue/channel ownership
partition map
ownership epochs
configuration
consumer group metadata
leases
broker status
```

Payload replication uses a separate streaming replication mechanism.

This preserves streaming and avoids turning large data transfers into consensus-log entries.

---

# 35. Payload replication

Payload replication should itself stream.

Example:

```text
Producer
    ↓
Broker A
  ↙     ↘
B       C
```

Broker A may stream the incoming transfer to replicas without first materializing the whole payload.

Configured acknowledgement determines when upstream responsibility transfers.

---

# 36. Ownership epochs

Cluster ownership changes require monotonic generations/epochs.

An old broker returning after a network partition must not be allowed to resume writes merely because it once owned a shard.

State should include concepts such as:

```text
cluster epoch
partition ownership epoch
consumer generation
```

Safety must not depend on synchronized wall clocks.

---

# 37. Rebalancing

Rebalancing should normally be online.

Conceptually:

```text
old owner
    ↓ streaming state
new owner catches up
    ↓
ownership epoch changes
    ↓
new traffic routed to new owner
    ↓
old owner drains
```

Precise behavior must be modeled and tested.

---

# 38. Browser support

Browser clients are first-class clients.

They should expose essentially the same messaging patterns and reliability semantics as native clients where the browser environment permits them.

However browser transport is not the native QUIC transport implementation.

Architecture:

```text
common protocol/pattern semantics
       ┌────────────┴────────────┐
native QUIC                   Web binding
                              WebTransport
                              WebSocket
```

WebTransport may transparently use QUIC underneath, but the browser does not expose raw Quinn/QUIC semantics directly to us.

Therefore Web is a protocol-preserving binding.

---

# 39. Web listener binding

A Listener may expose the same namespace via both native QUIC and Web.

Example:

```rust
listener.bind(Quic, ":7443").await?;
listener.bind(Web, ":443").await?;
```

Both may expose:

```text
/jobs
/events
/rpc
```

The Web binding handles the transport-specific mapping.

---

# 40. Browser API

JavaScript/TypeScript must be a first-class supported API.

Conceptually:

```js
const producer = runtime.producer();

await producer.connect(...);

await producer.send(data);
```

For large data, use native Web Streams where possible:

```text
ReadableStream
WritableStream
```

Do not force large browser payloads to become ArrayBuffers containing the entire transfer.

---

# 41. WebSocket mapping

WebSocket does not provide the same independent stream abstraction as native QUIC.

Therefore the WebSocket binding may need to implement logical stream multiplexing/framing.
This complexity must remain inside the Web binding crate.

The common protocol model must not expose WebSocket-specific compromises to normal Endpoint APIs.

WebTransport should preserve streaming more naturally where available.

---

# 42. SharedWorker / ServiceWorker / browser persistence

Optional browser integrations may include:

- SharedWorker for sharing one broker connection across tabs;
- ServiceWorker integration where appropriate;
- IndexedDB outbox/inbox helpers;
- reconnect support.

Do not promise server-like execution guarantees from browser lifecycle behavior.

Browsers may suspend or terminate workers.

Durable local storage can guarantee persistence under documented assumptions, not timely background delivery.

---

# 43. Standalone foreign-protocol libraries

ZeroMQ, nanomsg/NNG, MQTT, AMQP and NATS are implemented as standalone libraries. Each exposes
its own protocol's native primitives, addresses, state machines, security, options and
completion states. It may share reactor and OS plumbing with weida but must not depend on
weida's protocol or pattern layer.

There is no framework-wide socket-to-pattern mapping and no general bridge/forwarder product
category. Applications may compose two public libraries explicitly and own the conversion.
Protocol-native devices remain native; for example, a ZeroMQ proxy moves ZeroMQ messages
between ZeroMQ sockets.

---

# 44. Managed broker connectors

Foreign-protocol integration owned by the broker is modeled as a long-lived **Connector**
resource, not as a `Listener` binding or an `Endpoint` backend. A Connector is attached to one
named Queue and describes one concrete source or sink.

The control plane owns Connector lifecycle:

```text
create/apply Connector specification
        ↓
commit desired generation in control Raft
        ↓
controller reconciles placement and runtime
        ↓
observed generation and status advance
```

A committed specification means desired state is durable, not that the connector is already
ready. Delete marks the resource for drain and removal; it is asynchronous and observable.
When control quorum is unavailable, existing connectors keep their last committed assignment
while creation, update, deletion and replacement placement stop.

---

# 45. Connector semantics

A Connector specification must name:

```text
protocol and direction
foreign address and authentication
source or sink Queue
resource limits
one explicit payload/topic conversion policy
required and achieved guarantee level
```

The configuration is validated as one concrete composition. Unsupported combinations are
rejected; no protocol similarity silently creates an equivalence. Durability comes from the
attached Queue's store or replicated group, never from a socket buffer or translation task.
Protocol-specific Connector controllers are optional broker integration components. They may
depend on the broker resource API and one standalone foreign library; neither depends back on
them, and they expose no free-standing forwarding product.

---

# 46. Security

Keep Authentication and Authorization separate.

## Authentication

First-class framework responsibility.

Potential mechanisms:

- mTLS identities;
- TLS server authentication;
- bearer tokens;
- JWT verification;
- anonymous mode;
- custom authenticators.

The authenticated result should produce a common Identity/Principal context.

Conceptually:

```text
Identity {
    principal
    authentication_method
    trusted claims
    peer metadata
}
```

---

## Authorization

Provide hooks and useful simple policies, but do not build an IAM platform.

Potential primitives:

```text
AllowAll
DenyAll
ACL
predicate/callback
custom policy provider
```

Authorization may be Endpoint-aware and operation-aware.

---

# 47. Identity propagation

Authenticated identity may need to survive routing through a trusted broker.

Example:

```text
browser user
    ↓
broker
    ↓
worker
```

The worker may need the original principal.

Never trust arbitrary user-supplied metadata claiming an identity.

Delegated identity requires a trust model, e.g. broker-attested metadata.

This should be designed explicitly rather than improvised later.

---

# 48. Runtime concurrency

The Rust implementation is async-first.

Tokio is the likely runtime integration for the reference implementation, particularly because of Quinn.

However:

> Tokio is an implementation choice, not a wire-protocol concept.

Prefer ownership-based task design.

Example:

```text
Runtime
 ├── listener tasks
 ├── connection owners
 ├── endpoint state owners
 ├── persistence workers
 ├── broker/cluster workers
 └── maintenance/timers
```

Avoid broad shared mutable state such as:

```rust
Arc<Mutex<HashMap<...>>>
```

on hot paths.

Use ownership and bounded channels where they simplify concurrency.

---

# 49. Per-transfer cost

Transfers should remain lightweight.

Do not build a heavyweight actor for every data stream unless benchmarks prove it worthwhile.

Hot paths should avoid:

- locks;
- heap allocations;
- context switches;
- task hops;

unless required by selected guarantees.

A best-effort transfer should not traverse persistence, replication or deduplication code paths that are disabled.

---

# 50. Bounded memory

No internal queue may be unbounded unless explicitly documented and justified.

All remote-controlled memory allocation must have defensive limits.

This is both a reliability and security requirement.

Remote peers must not be able to create:

- unlimited concurrent state;
- unlimited header allocation;
- unlimited pending transfer metadata;
- recursion bombs;
- arbitrary giant control objects.

---

# 51. Sync API

The async implementation is canonical.

The synchronous API is a wrapper around it.

Conceptually:

```text
SyncRuntime
    ↓
dedicated async runtime thread(s)
    ↓
same core implementation
```

Do not duplicate:

- transfer semantics;
- state machines;
- protocol implementation;
- retry logic.

---

# 52. Public Rust API

Rust receives the reference API and should feel natively Rust-like.

Use:

- strong types;
- builders where useful;
- async/await;
- Rust streams/readers/writers;
- ownership;
- traits where they genuinely improve extensibility.

Avoid forcing all patterns through a dynamic `SocketType` enum.

An internal generic core like:

```rust
Endpoint<P>
```

is attractive.

Ergonomic constructors may hide the generic parameter most of the time.

---

# 53. Other language APIs

The protocol and core object model must remain language-neutral.

Do not encode semantics only through Rust's type system.

Bindings must map concepts idiomatically:

```text
Rust:
AsyncRead / AsyncWrite

Python:
async iterators / async file-like objects
sync convenience API

JavaScript/TypeScript:
ReadableStream / WritableStream / Promise

Go:
io.Reader / io.Writer

Java:
InputStream/channels/reactive APIs as appropriate
```

Do not mechanically clone Rust syntax into every language.

Rust is the reference implementation, not the only first-class language.

---

# 54. CLI

The CLI is a first-class client, not merely an administrative tool.

It should support brokerless and brokered use.

Conceptual examples:

```bash
mq send mq://host:7443/jobs < payload

mq recv mq://host:7443/jobs

mq pub mq://host:7443/events --topic system < event.json

mq sub mq://host:7443/events --filter 'important*'

mq req mq://host:7443/rpc < request.json
```

Streaming shell use must work naturally:

```bash
cat huge.tar | mq send ...
mq recv ... > huge.tar
```

The CLI must use the same public library API normal users use.

---

# 55. Broker administration

Application messaging configuration should primarily live in clients.

Producer/consumer/channel options are supplied by applications and negotiated when connecting/attaching.

Do not require all queues to be preconfigured manually in a broker GUI.

Broker static/infrastructure configuration includes things such as:

- listeners/bindings;
- certificates/authentication;
- storage paths;
- resource limits;
- cluster membership/bootstrap;
- infrastructure-wide defaults and caps.

---

# 56. Admin interface

The administrative interface is primarily operational and observational.

Likely capabilities:

```text
list active endpoints
inspect producers/consumers
queue depth
lag
replication state
cluster topology
throughput
latency
failed transfers
disconnect/block peer
purge stale data
drain endpoint
trigger rebalance
inspect transfer
retry/dead-letter where semantics allow
```

The CLI is more important than a GUI.

A graphical admin application may later be built in Rust using iced/iced-nodegraph and possibly compiled to WebAssembly.

It must consume the same administrative/inspection APIs rather than depending on broker internals.

---

# 57. Observability

Observability is first-class.

The framework must include:

- metrics;
- structured events;
- distributed tracing;
- inspection APIs.

Tracing is not an afterthought.

---

# 58. OpenTelemetry

OpenTelemetry is an explicitly accepted dependency in the otherwise restrictive dependency set.

Distributed tracing context should be integrated directly into the transfer metadata model.

Use interoperable standards such as W3C Trace Context/OpenTelemetry semantics rather than inventing proprietary trace formats.

Potential propagated context:

```text
trace id
span context
flags
baggage as appropriate
```

The exact propagation mechanism should follow established OTel conventions.

---

# 59. Asynchronous trace relationships

Messaging does not always fit strict synchronous parent-child spans.

Support concepts such as span links when appropriate.

Examples:

- one published event processed by multiple subscribers;
- delayed queue consumption;
- retried delivery;
- fanout;
- aggregation.

The library should make distributed processing easy to trace correctly.

---

# 60. Metrics

Core metrics should include at least:

```text
connections
streams opened/completed/reset
bytes sent/received
transfers started/completed/failed
ACK state counts
retries
drops
coalesces
queue depth
consumer lag
persistence bytes
replication lag
quorum failures
broker health
partition ownership
rebalances
```

Tracing depth may be configurable for performance, but trace propagation must remain easy and standard.

---

# 61. Failure model

Model failures explicitly.

Primary scope:

- process crash;
- host crash;
- network partition;
- packet loss/delay;
- peer disappearance;
- disk failure/full disk;
- corrupted persisted data;
- leader/owner loss;
- reconnect;
- retries;
- ACK loss.

Do not initially attempt Byzantine fault tolerance.

Authenticated transport protects communication but does not make the cluster a BFT consensus system.

---

# 62. Failure analysis method

For every reliability mode, ask:

```text
Who currently owns responsibility?

What state is durable?

What has the next hop acknowledged?

May the current hop safely discard local state?

May the operation be retried?

Can a retry create a duplicate?

Can a duplicate create a second effect?

Is the final outcome known or indeterminate?
```

---

# 63. Required failure scenarios

The design/test suite must cover at minimum:

```text
producer crashes before local acceptance

producer crashes after local persistence before sending

network disappears during stream

receiver crashes during stream

receiver gets entire transfer but crashes before ACK

receiver persists transfer but ACK is lost

broker persists but crashes before replication

replicated quorum completes but broker dies before upstream ACK

consumer receives but crashes before processing

consumer processes successfully then crashes before processed ACK

processed ACK reaches broker but broker crashes before storing state

network partition splits broker cluster

old shard owner returns after epoch changed

disk becomes full during stream persistence

persisted state becomes corrupt

client reconnects through another broker

retry reaches another replica

request side effect succeeds but reply disappears
```

Every guarantee mode must have explicitly documented outcomes.

---

# 64. Formal state machines

Essential state machines must be small, explicit and independently testable.

Examples:

```text
transfer responsibility state
ACK state
retry/dedup state
replication state
ownership/epoch state
consumer group state
```

Avoid one enormous implicit distributed state machine spread across callbacks.

---

# 65. Formal/model-based validation

Normal unit/integration tests are the primary testing mechanism.

Critical concurrency/state-machine components should additionally use systematic techniques where practical.

Evaluate Rust tooling such as:

- Loom;
- Shuttle;
- Kani/Creusot/Prusti or other suitable formal/model-checking tooling.

Do not force the entire production system into a theorem prover.

Use formal techniques where small state spaces protect high-value invariants.

---

# 66. Core invariants to validate

At minimum:

```text
Responsibility cannot silently disappear under a guarantee that forbids loss.

An ACK cannot report a stronger state than actually achieved.

A stale ownership epoch cannot regain authority.

At-least-once cannot silently become at-most-once.

Retry after indeterminate outcome cannot corrupt protocol state.

The parser never interprets arbitrary remote input as unbounded allocation.

Disabled features must not unexpectedly participate in the hot path.
```

Expand this list during design.

---

# 67. Fuzzing

Fuzzing is mandatory because multiple interfaces process untrusted content, especially Web bindings and foreign-protocol codecs.

Use `cargo-fuzz`/libFuzzer or equivalent for Rust components.

Targets should include:

```text
wire header parser
negotiation parser
capability messages
ACK/CANCEL/ERROR state transitions
WebSocket framing/multiplexing
WebTransport binding boundaries
MQTT codec
ZeroMQ codec
AMQP codec
persistence/WAL recovery
authentication metadata parsing
```

Properties include:

```text
decode(encode(x)) == x

arbitrary input never panics

invalid lengths cannot allocate unbounded memory

malformed states are rejected

unknown optional fields remain safe

parser recursion remains bounded

WAL recovery never creates impossible state
```

---

# 68. Performance philosophy

QUIC's inherent overhead is accepted.

The objective is:

> Minimize framework overhead above the chosen transport.

QUIC encryption does not make avoiding extra allocations/copies irrelevant.

Zero-copy/buffer-reuse techniques remain valuable for every protocol.

---

# 69. Performance profiles

Benchmarks must represent different guarantee profiles.

At minimum:

```text
native QUIC best effort
reliable brokerless
durable brokerless
volatile broker
durable broker
replicated broker
browser WebTransport
browser WebSocket
standalone ZeroMQ library
standalone MQTT library
one named managed Connector configuration
```

Measure:

```text
p50/p95/p99 latency
messages/transfers per second
bytes per second
CPU per byte
CPU per transfer
allocations
copies
memory per transfer
memory per connection
concurrent streams
persistence cost
replication cost
```

---

# 70. Disabled-feature principle

Hard performance rule:

> Features that are disabled should impose close to zero runtime cost.

A best-effort transfer must not pay for:

- persistent WAL machinery;
- hashes;
- dedup lookup;
- replication;
- expensive tracing;
- consumer transaction state;

unless those capabilities are enabled.

---

# 71. Comparative benchmarks

Benchmark against relevant established systems where comparison is meaningful:

```text
ZeroMQ / NNG
NATS
RabbitMQ
popular MQTT implementations
```

The goal is not to manufacture a benchmark where this project wins everything.

The goal is to understand and document:

> What does each stronger guarantee cost?

For equivalent standalone-library behavior, strive to be at least competitive with common implementations.

---

# 72. Dependencies

Dependencies should remain unusually restricted.

Explicitly expected/accepted major dependencies include:

```text
quinn
OpenTelemetry ecosystem
```

A serialization dependency may be added after deliberate evaluation.

Consensus/Raft implementation requires careful evaluation: either a high-quality dependency or a tightly scoped implementation if justified.

Do not add dependencies merely for convenience.

Each major dependency should have an architectural justification.

---

# 73. Repository architecture

Initial conceptual structure:

```text
crates/
    core/
    protocol/
    runtime/
    weida/
    broker/
    raft/
    web/                         optional protocol-preserving binding
    zmq/                         codec, standalone library, bindings
    nng/                         codec, standalone library, bindings
    mqtt/                        codec, standalone library, bindings
    amqp/                        codec, standalone library, bindings
    nats/                        codec, standalone library, bindings
    connectors/                  optional broker Connector implementations
    site/
```

Do not create dozens of tiny crates prematurely.

Start with strong module boundaries and split crates where the dependency/ownership boundary is real.

---

# 74. Dependency direction

Avoid circular architectural dependencies.

Conceptually:

```text
core
 ↑
protocol/runtime
 ↑
native transport / patterns
 ↑
persistence
 ↑
broker / cluster
 ↑
applications
```

Foreign-protocol libraries depend only on protocol-neutral foundations. A broker deployment's optional Connector controller may depend on the broker resource API and one foreign library; neither side depends back on it, and it is not a free-standing product.

The broker uses the public/core messaging machinery.

The core must not depend on broker implementation details.

---

# 75. Documentation and website

Documentation is a major product feature.

Build a polished documentation website comparable in ambition to ZeroMQ's pattern documentation, but broader.

It should explain:

- conceptual architecture;
- protocol;
- guarantees;
- tradeoffs;
- messaging patterns;
- brokerless examples;
- broker examples;
- streaming examples;
- browser examples;
- failure semantics;
- performance profiles;
- interoperability.

Every major example should eventually be shown in many supported languages rather than only C/Python.

---

# 76. Protocol documentation

Maintain a normative protocol specification separate from implementation documentation.

Recommended top-level artifacts:

```text
ARCHITECTURE.md
PROTOCOL.md
GUARANTEES.md
INVARIANTS.md
FAILURE_MODEL.md
IMPLEMENTATION.md
```

Potential additional documents:

```text
SECURITY.md
PERFORMANCE.md
CONTRIBUTING.md
ADAPTERS.md
```

---

# 77. INVARIANTS.md

This file should be short enough that an agent can repeatedly consult it.

Example invariants:

```text
Endpoint paths are opaque identifiers.

All user payloads may remain streams end-to-end.

Core transport does not require payload materialization.

One data flow maps naturally to one transport stream where the transport supports it.

Replies and ACKs are distinct concepts.

Transfer-related control messages do not require a permanent control stream.

All guarantees are defined against the immediate next hop.

Brokerless and brokered APIs share the same messaging concepts.

A broker cluster appears as one logical broker.

Raft coordinates control state, not bulk payload transport.

Payload replication remains stream-oriented.

Disabled guarantees should not participate in the hot path.

No remote input can cause unbounded memory allocation.

A managed Connector may claim only what its concrete source, Queue and sink can prove.
```

The implementation agent must validate design changes against these invariants.

---

# 78. Protocol release strategy

The project should initially publish as alpha:

```text
0.1
```

During the `0.x` lifecycle:

- collect real user feedback;
- test interoperability;
- fuzz heavily;
- benchmark;
- discover semantic mistakes;
- intentionally allow protocol-breaking fixes.

Before `1.0`:

- audit the wire protocol;
- audit extension rules;
- audit all failure semantics;
- audit capability negotiation;
- audit security boundaries;
- provide cross-version compatibility tests.

`1.0` means the wire protocol becomes a serious long-term compatibility promise.

---

# 79. Implementation phases

The agent must proceed in dependency order.

Do not implement everything chaotically in parallel.

## Phase 0 — Architecture/specification

Before major implementation:

- formalize terminology;
- finalize core invariants;
- produce architecture documents;
- define failure vocabulary;
- define guarantee vocabulary;
- design protocol envelope;
- select serialization;
- design version/capability negotiation;
- design initial Rust public API;
- identify deliberately unresolved questions.

No broker implementation should begin before these foundations are coherent.

---

## Phase 1 — Core model

Implement:

- IDs;
- endpoint identifiers;
- Runtime abstractions;
- Listener;
- Binding abstraction;
- typed Endpoint model;
- Transfer model;
- correlation;
- policy types;
- errors;
- cancellation;
- core state machines.

Unit-test heavily.

No networking should be required to test the state machines.

---

## Phase 2 — Native QUIC transport

Implement:

- Quinn integration;
- connection establishment;
- negotiation;
- uni-stream protocol envelope;
- endpoint routing;
- request/reply correlation;
- short control streams;
- cancellation/reset;
- resource limits;
- connection pooling.

Initially support simple brokerless transfers.

Test large streaming transfers immediately.

---

## Phase 3 — Brokerless messaging patterns

Implement the core messaging family.

At minimum begin with:

```text
Push/Pull
Pub/Sub
Req/Rep
Router/Dealer equivalents
```

Then complete the broader ZeroMQ/NNG-inspired pattern set.

Guarantees should initially remain simple where necessary.

Avoid premature broker coupling.

---

## Phase 4 — Reliability

Implement explicit responsibility semantics:

- ACK states;
- retry policy;
- indeterminate outcomes;
- Transfer IDs;
- idempotency IDs;
- deduplication;
- configurable outgoing persistence;
- configurable incoming persistence;
- coalescing helper;
- recovery.

All reliability behavior must have failure tests.

---

## Phase 5 — Persistence subsystem

Implement:

- streaming payload store;
- metadata/control WAL;
- crash recovery;
- optional hashing;
- segment cleanup;
- durable outbox/inbox helpers.

Ensure very large transfers never require materialization.

---

## Phase 6 — Standalone broker

Implement one broker instance using the same core.

Responsibilities:

- Endpoint/queue/channel namespace;
- discovery;
- routing;
- consumer assignment;
- optional persistence;
- queue state;
- inspection API;
- metrics/tracing.

The single-node broker must already use abstractions compatible with later clustering.

---

## Phase 7 — Broker clustering

Implement:

- one control Raft for cluster membership and managed Queue/Connector resources;
- cluster membership;
- ownership maps;
- epochs;
- placement;
- one independent Raft group per replicated Queue;
- payload streaming outside every Raft log;
- configurable N-accept/quorum behavior;
- failover and online rebalance;
- topology discovery and resource reconciliation.

The client must still perceive one logical broker.

---

## Phase 8 — Web binding

Implement Web as a first-class protocol-preserving binding:

- browser server binding;
- WebTransport where useful/available;
- WebSocket fallback;
- transfer mapping;
- JS/TS client;
- Web Streams support;
- browser authentication;
- optional SharedWorker support;
- optional IndexedDB persistence helper.

Semantics should remain as close as possible to native clients.

---

## Phase 9 — Standalone foreign-protocol libraries

Implement independently:

1. ZeroMQ
2. nanomsg/NNG
3. MQTT
4. AMQP
5. NATS

For each:

- sans-I/O codec where the wire format warrants one;
- native typed API and bounded resource model;
- protocol conformance and hostile-input tests;
- interop against named upstream implementations;
- fuzzing and performance benchmarks;
- language bindings after the native library.

No general weida bridge or global pattern mapping is part of this phase.

---

## Phase 10 — Language bindings

Priority should be determined by ecosystem usefulness.

Likely:

```text
Python
JavaScript/TypeScript
Go
Java
...
```

Each binding gets idiomatic sync/async/streaming integration.

Do not expose an awkward Rust-shaped FFI directly as the public language API.

---

## Phase 11 — CLI and administration

Implement first-class CLI:

- send;
- receive;
- publish;
- subscribe;
- request;
- broker inspection;
- cluster inspection;
- streaming stdin/stdout.

Then add richer administration.

GUI may use Rust + iced/iced-nodegraph and potentially WASM.

---

## Phase 12 — Documentation/site/stabilization

Build polished documentation.

Before considering protocol 1.0:

- external testing;
- interoperability testing;
- heavy fuzzing;
- soak tests;
- failure injection;
- protocol audit;
- security review;
- API review;
- benchmark suite;
- compatibility tests.

---

# 80. Mandatory development loop per phase

For every phase:

```text
design
    ↓
implement
    ↓
unit test
    ↓
failure test
    ↓
fuzz relevant boundaries
    ↓
benchmark relevant hot paths
    ↓
check invariants
    ↓
refactor as if architecture had always been designed this way
    ↓
continue
```

Do not accumulate architectural debt with the excuse that a later phase will clean it up.

---

# 81. Agent implementation rules

The implementation agent must follow these rules:

1. Read the architectural documents before changing foundational components.
2. Do not duplicate messaging semantics between brokered and brokerless paths.
3. Do not introduce complete-payload materialization into core APIs.
4. Do not add a permanent control stream merely for convenience.
5. Do not interpret endpoint paths beyond opaque lookup.
6. Do not silently weaken requested guarantees.
7. Do not advertise end-to-end guarantees when only next-hop state is known.
8. Do not mix user replies with protocol ACK semantics.
9. Do not put bulk payload replication through Raft without an exceptional demonstrated reason.
10. Do not expose broker topology to normal clients as a requirement.
11. Do not make Web a second-class feature subset.
12. Do not make Rust typing the only representation of protocol roles.
13. Do not add large dependencies without justification.
14. Do not optimize by disabling correctness for configurations that request stronger guarantees.
15. Conversely, do not impose strong-guarantee overhead on weak-guarantee configurations.
16. Prefer clear explicit state machines over distributed implicit state.
17. Prefer bounded resources everywhere.
18. Treat all network input as hostile.
19. Keep observability available from the beginning.
20. Preserve the ability to refactor the `0.x` protocol when real-world findings justify it.

---

# 82. Questions deliberately left open

Do not guess these prematurely. Research/prototype them.

### Serialization

Choose between MessagePack, CBOR or another compact evolvable format.

Criteria:

- parsing speed;
- encoding speed;
- allocations;
- unknown-field support;
- multi-language implementations;
- dependency cost;
- fuzzability;
- deterministic representation where needed.

### Exact public names

Concepts are settled more strongly than names.

Names such as:

```text
Runtime
Listener
Binding
Endpoint<P>
Transfer
```

are working names and may be improved if a clearly superior vocabulary emerges.

### Exact pattern taxonomy

Support the useful ZeroMQ/NNG patterns, but investigate whether a smaller internal primitive set can implement them cleanly.

The public API may remain familiar even if internals are generalized.

### Exact ACK vocabulary

Define precise names only after the failure model is formalized.

Avoid ambiguous words such as `Committed` unless their scope is explicit.

### Raft implementation

Evaluate existing Rust implementations versus a dedicated implementation with the project's dependency constraints.

### QUIC datagrams

Investigate whether QUIC DATAGRAM support provides meaningful value for genuinely unreliable ultra-low-latency modes.

Do not add it merely because QUIC exposes it.

### Resumable huge streams

Investigate checkpointing/content addressing/resume semantics after the basic protocol is proven.

Do not complicate protocol v0.1 unnecessarily.

---

# 83. Initial architectural prototype

Before attempting the full broker, create one deliberately small end-to-end reference:

```text
Process A:
    Runtime
    Endpoint<Req>

Process B:
    Runtime
    Listener
        QUIC binding
        /transform Endpoint<Rep>
```

Requirements:

1. Establish QUIC connection.
2. Negotiate protocol.
3. Open uni request stream.
4. Send metadata header.
5. Stream arbitrary-sized input.
6. Server begins processing before FIN where possible.
7. Server opens correlated reply stream before request necessarily completes.
8. Stream reply simultaneously.
9. Send protocol ACK on a separate control stream.
10. Propagate OpenTelemetry trace context.
11. Cancel mid-transfer.
12. Test connection loss.
13. Test malformed headers.
14. Fuzz parser.
15. Benchmark small and large transfers.
16. Demonstrate bounded memory with a multi-GB generated stream.

This prototype should prove the central architecture before the feature surface explodes.

---

# 84. Second reference prototype

Build a brokerless durable Push/Pull workflow:

```text
Producer
   ↓
optional durable outbox
   ↓
Consumer
```

Exercise:

- best effort;
- at-most-once;
- at-least-once;
- remote accepted ACK;
- remote stored ACK;
- lost ACK;
- process crash;
- restart;
- duplicate delivery;
- deduplication;
- indeterminate result.

This prototype should prove the responsibility model.

---

# 85. Third reference prototype

Build a simple broker:

```text
Web/API producer
    ↓
Broker
    ↓
stateless worker
```

Then demonstrate:

```text
producer ACK after broker memory accept
producer ACK after broker persistence
worker processed ACK
worker crash before ACK
broker restart
```

This establishes the foundation for the distributed broker.

---

# 86. Design philosophy summary

When making difficult decisions, use these priorities:

```text
clarity
explicit semantics
streaming
correctness according to selected guarantees
low framework overhead
composability
interoperability
ergonomic APIs
```

Do not maximize consistency at all costs.

Do not maximize raw speed at all costs.

Instead:

> Make the tradeoff explicit and let the application select it.

The framework should be capable of behaving like a lightweight high-performance ZeroMQ-style networking layer in one configuration and like a durable replicated brokered queueing platform in another, while retaining one coherent programming model.

The project succeeds if a user can begin with:

```text
process A ↔ process B
```

and later evolve the architecture to:

```text
browser
   ↓
distributed broker cluster
   ↓
workers
   ↓
explicitly configured managed connectors to MQTT / ZeroMQ / RabbitMQ systems
```

without replacing the fundamental messaging abstraction.

That continuity is the core product.