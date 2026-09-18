# Synthesis: the same problems, answered side by side

## 0. How to read this

The eight sheets in this directory were written independently, each in its own protocol's
vocabulary, and each answers the problem catalogue P1-P18 of [README.md](README.md) in section
12. This document is the join. It adds no research: every cell and every claim carries a
reference to the sheet it comes from, and every claim about weida carries a reference to a
weida document.

**Architecture status.** The side-by-side comparisons remain research evidence. Any
socket-to-pattern mapping, adapter table or general forwarder recommendation in §§7-8 predates
the revised [0013](../decisions/0013-competitor-libraries.md) and is superseded by it. Similar
protocol operations are not globally equivalent; a managed Connector names one concrete
conversion policy.

Reference form. `[zeromq §12/P4]` is the ZeroMQ sheet, section 12, problem 4;
`[amqp10 §6.2]` is the AMQP 1.0 sheet, section 6.2. Sheet stems are `zeromq`,
`nanomsg-nng`, `rabbitmq-amqp091`, `amqp10`, `mqtt5`, `nats`, `kafka`, `ipc`. Weida
references are the document plus section: `[GUARANTEES §6]`, `[PATTERNS §1.2]`,
`[FAILURE_MODEL §4]`, `[ARCHITECTURE §6a]`, `[INVARIANTS]`.

Rows in §1 are the eight protocols in a fixed order: ZeroMQ, NNG, RabbitMQ/AMQP 0-9-1,
AMQP 1.0, MQTT 5, NATS core, NATS JetStream, Kafka. NATS core and JetStream are separate rows
throughout, because they answer most problems differently and the sheet answers them
separately. Where a sheet says a protocol has no answer, the cell says "no answer in this
protocol" rather than inventing one.

`ipc.md` is not a protocol sheet: it describes operating-system mechanisms and uses its own
layout. It appears only in P18's table, as the local transports available to any of these
protocols, and in §8, where its section 11 candidates are open decisions.

Two cautions carried over from the sheets. First, guarantees in every one of these protocols
are **hop-scoped**, and several sheets say so in nearly the same words
([zeromq §6], [mqtt5 §6], [rabbitmq-amqp091 §6], [GUARANTEES §2]) — comparing them requires
naming which hop. Second, the same word means different things in different sheets; §3 and the
contradiction notes in §2 are where that is recorded rather than smoothed over.

---

## 1. The matrix

### P1 — A request or message may be lost: how is loss detected and how is a safe retry made?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Nothing in ZMTP; application recipes (Lazy Pirate poll-and-resend, broker reassignment, Titanic persist-then-retry, Freelance blast-to-all) | One hop delivers atomically, at most once, in order, with no acknowledgement | `zmq_send` returning means only "queued on the socket and 0MQ has assumed responsibility"; a lost request is invisible until the application times out; REQ needs close-and-reopen after `EFSM` | [zeromq §12/P1], [zeromq §6] |
| NNG | REQ resends on `REQ_RESENDTIME`, peer disconnect, or a peer becoming available | Eventual processing while the requester lives; not exactly-once effects | The retry is ambiguous: the REP may already have acted, so requests must be idempotent. Other patterns have no safe retry at all | [nanomsg-nng §12/P1], [nanomsg-nng §6] |
| RabbitMQ / AMQP 0-9-1 | `confirm.select`, a sequence-number-to-message map, republish anything unconfirmed after a timeout | At-least-once publisher-to-queue, with the per-queue-type meaning of the confirm | Republication duplicates whenever the confirm was sent but lost; nothing on the wire identifies a retry, and `redelivered` is a hint clients MUST NOT rely on | [rabbitmq-amqp091 §12/P1] |
| AMQP 1.0 | Absence of a `disposition`; retry made safe by *link resumption*, not by timeout — both ends publish unsettled maps and a three-way rule decides | Tags both ends hold MUST be resumed; tags only the receiver holds are settled; tags only the sender holds MAY be resumed | A tag the receiver has no record of MUST be resent without `resume`, which is the duplicate-admitting case | [amqp10 §12/P1], [amqp10 §6.3] |
| MQTT 5 | Per hop, per QoS: QoS 1 detects a missing PUBACK, QoS 2 a missing PUBREC/PUBCOMP | Retransmission only on reconnect with Clean Start 0 and a session present, original Packet Identifiers, DUP 1 | "Clients and Servers MUST NOT resend messages at any other time"; QoS 0 offers no detection; duplicate suppression exists only inside one hop | [mqtt5 §12/P1] |
| NATS core | None | No delivery receipt at all | A client timeout or disconnect leaves delivery unknown, and a retry can duplicate application work | [nats §12/P1] |
| NATS JetStream | Publish acknowledgement plus `Nats-Msg-Id` de-duplication inside the stream's duplicate window; explicit consumer acknowledgements | Publish accepted and committed per the configured replication quorum; duplicate publishes suppressed within the window | An identifier reused after the window expires is not suppressed; a missing publish ack is ambiguous and must be retried with a stable id | [nats §12/P1], [nats §6] |
| Kafka | The Produce response, or its absence; idempotent producer (producer ID plus monotonic sequence) | At-least-once log delivery; with idempotence a retry is deduplicated and out-of-order sequence is detected | A Produce timeout is ambiguous — the broker may have appended before the response was lost; without idempotence the retry appends a duplicate | [kafka §12/P1], [kafka §6] |

### P2 — A peer may be dead or unreachable: how is liveness detected, in what time, and what happens to its state?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | ZMTP 3.1 `PING`/`PONG` with a TTL in tenths of a second; libzmq `ZMQ_HEARTBEAT_IVL` family, off by default; application heartbeats in the guide's three designs | Detection time is whatever the application configures, from "as low as 10 msecs" to 30 s | TCP alone can take "roughly 30 minutes". No session, no last will: on disconnect the socket destroys its per-peer double queue and discards its messages; DRAFT `ZMQ_DISCONNECT_MSG`/`ZMQ_ROUTER_NOTIFY` are the closest thing to a will | [zeromq §12/P2] |
| NNG | Dialer reconnect after pipe closure, with `RECONNMINT`..`RECONNMAXT` backoff; ZeroTier alone specifies ping-based death detection | Pipes are restored; nothing else is | No SP last-will or session state exists; queued messages and subscriptions are not durable protocol objects | [nanomsg-nng §12/P2] |
| RabbitMQ / AMQP 0-9-1 | Heartbeat frames, 60 s default negotiated, sent every half interval, dead after two missed intervals; Raft components add an adaptive failure detector | Detection within roughly two heartbeat intervals; 5-20 s recommended | No last will, no session: channels, consumers, exclusive and auto-delete queues are destroyed and all unacknowledged deliveries requeued. A stuck-but-connected consumer is caught only by `consumer_timeout`, default 30 min | [rabbitmq-amqp091 §12/P2] |
| AMQP 1.0 | `open.idle-time-out` in milliseconds, per direction and independent; an idle peer emits an empty frame or a `flow` | Liveness within the advertised interval; the advertised value SHOULD be half the local threshold | No last will and no session takeover. What survives is decided by the terminus: `expiry-policy` starts a `timeout`, `durable` decides what is kept; in-flight deliveries of unknown state are decided by `source.default-outcome` | [amqp10 §12/P2] |
| MQTT 5 | Keep Alive (client-chosen, max 65,535 s) with the server closing after 1.5 x Keep Alive; PINGREQ/PINGRESP the other way with no defined timeout | Worst-case detection 1.5 x Keep Alive | The Will is published after Will Delay or at session end, whichever comes first. Session state survives the Session Expiry Interval; in-flight QoS > 0 stays in session state and is resent, QoS 0 is lost | [mqtt5 §12/P2] |
| NATS core | `PING`/`PONG` with server `ping_interval` (2 min default) and `max_pings_out` (2) | Stale connection detected after the configured unanswered-ping threshold | Core subscriptions vanish with the connection and have no stored session | [nats §12/P2], [nats §11] |
| NATS JetStream | Same connection liveness; durable consumer state is server-side | Durable consumer delivery state survives the client | Ephemeral consumers have an inactivity lifetime and are not a durable reconnect contract | [nats §12/P2], [nats §2] |
| Kafka | Consumer-group heartbeats with `session.timeout.ms` (broker-controlled under the new consumer protocol); `max.poll.interval.ms` bounds progress; KRaft broker metadata fetches double as broker heartbeats | A missed session timeout removes the member and triggers reassignment | A fenced broker ceases serving client RPCs and is omitted from client metadata; partitions can become unavailable if no eligible leader remains | [kafka §12/P2], [kafka §1] |

### P3 — Work must be spread over several workers by capacity, not by turn

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Socket level: round-robin over peers whose outgoing queue is not full. Real answer: the load-balancing broker — workers send `ready` on start and after each task, the broker keeps them least-recently-used | Unit of credit is one task per `ready` signal; MDP formalizes "least recently used… for a given service" | Plain round-robin "becomes inefficient when tasks do not take approximately the same time"; the broker is a single point of failure and a restarted queue does not know its workers | [zeromq §12/P3] |
| NNG | PUSH round-robins only among pullers currently able to accept a message; REQ spreads requests among REP peers | Load distribution by immediate acceptance | "readiness rather than declared capacity" — no advertised capacity or service-rate protocol | [nanomsg-nng §12/P3] |
| RabbitMQ / AMQP 0-9-1 | Many consumers on one queue plus `basic.qos`; unit is messages (unacknowledged deliveries) | With `prefetch_count`, a consumer stops receiving once its window is full | Without prefetch, dispatch is blind round-robin: RabbitMQ "blindly dispatches every n-th message to the n-th consumer" without looking at outstanding deliveries | [rabbitmq-amqp091 §12/P3] |
| AMQP 1.0 | Link credit: each receiver independently advertises `link-credit` on its own link; `distribution-mode=move` makes a message ACQUIRED and ineligible on other links | A fast worker replenishes sooner and therefore receives more; unit is one message | Competing consumers cannot double-take under `move`; Dispatch's `balanced` balances "according to rate of settlement" rather than by turn | [amqp10 §12/P3] |
| MQTT 5 | Shared subscriptions, `$share/{ShareName}/{filter}`; unit is one Application Message | Each matching message reaches exactly one member of each group | No capacity signal: "the Server implementation is free to choose, on a message by message basis, which Session to use and what criteria it uses". EMQX defaults to round robin, HiveMQ distributes randomly so "faster consumers typically get more" | [mqtt5 §12/P3] |
| NATS core | Queue groups: one eligible member of the named group receives each matching publication | Documented goal is distributed load balancing among group members | A worker crash loses any Core message already sent to it and offers no retry | [nats §12/P3], [nats §4] |
| NATS JetStream | Pull consumers: workers request the next batch when they have capacity | Workers choose batch size and request expiration | Concurrency is bounded by `max_ack_pending`; unacknowledged work is redelivered after `ack_wait` | [nats §12/P3], [nats §5] |
| Kafka | Conventional consumer group assigns whole partitions, one active consumer per partition; share groups distribute individually acquired records | Partitions bound parallelism; share groups may exceed partition count | Assignment granularity is the partition, not the record; share-group records are re-offered after lock expiry or release | [kafka §12/P3], [kafka §4] |

### P4 — A consumer is slower than the producer: where does the backlog sit, what is its bound, what happens at the bound?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Per-peer pipes on both sides plus kernel socket buffers; bound is `ZMQ_SNDHWM`/`ZMQ_RCVHWM`, 1000 messages per peer | A local, inexact bound: "simply don't rely on the exact HWM value", and the effective limit "may be as much as 90% lower" | At the bound, per socket type: PUB, XPUB, XSUB, RADIO and ROUTER drop; PUSH, PULL, REQ, DEALER, PAIR, CLIENT, SCATTER, CHANNEL block; SERVER, PEER, STREAM return `EAGAIN`; a receiving SUB/XSUB silently discards. No credit ever returns to the publisher — "one setting, which is *full-speed*" | [zeromq §12/P4] |
| NNG | Socket `SENDBUF`/`RECVBUF`, 0-8192 **messages**, plus independent transport buffering | Bounded queue depth where the protocol supports buffering | PUSH and PAIR block or time out; BUS drops the undeliverable copy without blocking; SUB drops its oldest queued message by default (`PREFNEW=true`) or rejects the new one | [nanomsg-nng §12/P4], [nanomsg-nng §5] |
| RabbitMQ / AMQP 0-9-1 | The backlog sits in the queue on the broker, moved to disk aggressively; bounds are opt-in `x-max-length`/`x-max-length-bytes` | Backpressure without loss until a bound or an alarm is reached | At the bound: `drop-head` (default) silently discards the oldest; `reject-publish` nacks the publisher; unbounded growth ends in a memory or disk alarm that blocks every publishing connection cluster-wide while consumers keep running. Internal credit flow shows as `flow` before that | [rabbitmq-amqp091 §12/P4] |
| AMQP 1.0 | Backlog stays at the sending node and its size is advertised: `flow.available` is "the number of messages awaiting credit at the link sender endpoint" | Bound is `link-credit` per link and the session `incoming-window` per session | At the bound the sender **blocks**: "a sender MUST NOT send more messages" — no drop, no disconnect, no error. The only errors punish rule-breaking: `transfer-limit-exceeded`, `session:window-violation` | [amqp10 §12/P4] |
| MQTT 5 | Backlog sits in the server's session state for that client; `Receive Maximum` bounds the in-flight window only | The queue behind the window has **no protocol bound** | Implementation-defined and divergent: Mosquitto silently drops subsequent QoS 1/2 messages at `max_queued_messages`; EMQX evicts the oldest QoS 0 message; HiveMQ defaults to `discard` (drop new). The publisher gets no signal, its PUBACK having already been returned | [mqtt5 §12/P4] |
| NATS core | Server queues pending outbound bytes per client connection up to `max_pending` (64 MiB default) | A byte bound per connection | Beyond it the server reports a slow consumer and **disconnects that client**; its queued messages are lost and the publisher receives no delivery proof. `write_deadline` can cut the connection too | [nats §12/P4], [nats §5] |
| NATS JetStream | Stream retains messages; consumer delivery pauses at `max_ack_pending` (1000 default); pull batches and expirations let workers bound requested work | Delivery stops until acknowledgements advance the count | Stream limits and the discard policy decide overflow: `DiscardNew` rejects the write, `DiscardOld` evicts older retained messages | [nats §12/P4], [nats §11] |
| Kafka | Unread records stay in the partition log as consumer lag; the bound is retention by age and/or size | Backlog is bounded by retention, not by consumer memory | Once the committed position precedes the log start the fetch position is out of range and `auto.offset.reset` (earliest/latest/by_duration/none) decides; deleted records cannot be replayed | [kafka §12/P4] |

### P5 — A late joiner needs current state (last value, snapshot, replay)

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Nothing in the protocol. Application answers: synchronized startup over a second REQ/REP flow; Last Value Caching in an XSUB/XPUB proxy; CHP snapshot-then-stream (`ICANHAZ?` / `KVSYNC` / `KTHXBAI`) | LVC gives the cached last value per topic; CHP gives a snapshot plus a strictly increasing update sequence | "the subscriber will always miss the first messages that the publisher sends"; pub-sub "will lose messages arbitrarily". Replay of a gap is explicitly refused — the client should stop and not restart until someone checks the cause | [zeromq §12/P5] |
| NNG | No answer in this protocol | — | No retained publication, snapshot or replay exists in SP PUB/SUB | [nanomsg-nng §12/P5] |
| RabbitMQ / AMQP 0-9-1 | Nothing in AMQP 0-9-1 — delivery is once and destructive. The answer is a stream (`x-queue-type = stream`) read with `x-stream-offset` = first/last/next/offset/timestamp/interval | A stream is an append-only, non-destructively read log retained by `max-age`/`max-length-bytes` rather than by consumption | A message published with no queue bound is gone; no retained message and no last-value cache in the core protocol. Partial answers: the Recent History exchange plugin, MQTT retained messages | [rabbitmq-amqp091 §12/P5] |
| AMQP 1.0 | No answer in the core protocol. Nearest primitives: `distribution-mode=copy` and `source.filter` | A source MUST NOT resend a message already transferred to an ACCEPTED state | No last-value, retained-message, snapshot or replay concept; replay is a broker feature reached through filters (RabbitMQ streams, Service Bus `peek-message`) | [amqp10 §12/P5] |
| MQTT 5 | Retained messages: one per exact Topic Name, sent to a new non-shared subscription with RETAIN 1, governed by Retain Handling and Retain As Published | A last-value cache — "a snapshot of the last-known state" | Not a replay log: no history, no snapshot-plus-delta, no offset. Retained messages are never sent to shared subscriptions, and QoS 0 retained messages MAY be discarded at any time. The alternative is subscribing with a session *before* publication so the server queues | [mqtt5 §12/P5] |
| NATS core | No answer in this protocol | — | Core NATS stores neither a publication nor subscriber delivery state for replay | [nats §6], [nats §12/P5] |
| NATS JetStream | Consumer deliver policy: all, last, new, by start sequence, by start time, or last per subject; a KV bucket exposes the latest revision plus a watchable history | A consumer can start at a chosen point in the retained stream | Bounded by stream limits and retention; a KV reader must handle revision and watch semantics | [nats §12/P5] |
| Kafka | Replay retained offsets; compacted topics retain the latest record per key | Compaction retains offsets and order while removing obsolete keyed records | "not an immediate snapshot guarantee": consumers must tolerate older versions and tombstones until cleaner progress and retention remove them. A null key is invalid for compaction | [kafka §12/P5], [kafka §3] |

### P6 — A broker or peer fails over to another: what does the client do, what is re-established automatically?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Automatic reconnect to the *same* endpoint (`ZMQ_RECONNECT_IVL`, with optional doubling). Failover to a different endpoint is application logic: Freelance models, Binary Star, MDP's stateless broker | Subscriptions are re-established automatically because XSUB re-sends them; `ZMQ_XPUB_WELCOME_MSG` is also sent on reconnect | Reconnection is not re-registration: a worker "must close/reopen after broker death to re-register; ZeroMQ auto-reconnect is insufficient". Binary Star's explicit non-goal is state replication — "applications recreate all server-side state after failover" | [zeromq §12/P6] |
| NNG | Dialer reconnects pipes; REQ can resend to an available peer | Pipes are restored | Subscriptions, sessions, queues and in-flight state are not durable or re-established protocol objects | [nanomsg-nng §12/P6] |
| RabbitMQ / AMQP 0-9-1 | The client reconnects; the protocol re-establishes nothing. Libraries replay connection, channels, `basic.qos`/confirm settings, exchanges, queues, bindings, consumers | Any node may be connected to, and quorum-queue operations are routed to the leader transparently | Not re-established: in-flight publishes (lost, not buffered), `basic.get`, publisher sequence numbers (they restart), unacknowledged deliveries (the broker requeues them). Java, .NET and Bunny automate topology recovery; Pika does not | [rabbitmq-amqp091 §12/P6] |
| AMQP 1.0 | `amqp:connection:redirect` and `amqp:link:redirect` carry `hostname`, `network-host`, `port` and, for links, `address`. Explicit recovery: resume a link by name against surviving termini | Recovery is possible from near-total local state loss — link name and direction suffice, the remote terminus supplies the rest; `container-id` must match | Nothing is re-established automatically, and survival depends on `terminus-durability` and `expiry-policy`. No retry limit, loop detection or backoff is specified | [amqp10 §12/P6] |
| MQTT 5 | Redirection, not failover: CONNACK or DISCONNECT 0x9C (temporary) / 0x9D (permanent), optionally with a space-separated `Server Reference` | The client is told where to go, and MAY ignore it | Nothing is re-established automatically: session state is not transferred, subscriptions are not migrated, in-flight messages are not carried over. On the new server the client is a new session unless that server independently holds state for the ClientID | [mqtt5 §12/P6] |
| NATS core | `INFO.connect_urls` tells the client about cluster topology, including asynchronously; client libraries reconnect by their own policy | Topology discovery is in the client protocol | Core subscriptions must be re-established by the reconnecting library; reconnect policy is not a wire guarantee | [nats §12/P6] |
| NATS JetStream | Durable consumers can be rebound to retained server-side state | Delivery state survives the client reconnect | Memory storage and non-durable consumer state give no such persistence; an unconfirmed publish stays ambiguous | [nats §12/P6], [nats §8] |
| Kafka | Clients refresh metadata after a socket or leadership error and reconnect to the new partition leader; group membership and offsets are coordinator-managed | A committed record remains available under the stated ISR/failure model; availability pauses during election | The application must tolerate replay after an unclean work handoff; KIP-848's worst case remains at-least-once. Unclean leader election trades committed data for availability | [kafka §12/P6], [kafka §8] |

### P7 — Delivery must survive a restart: what is persisted, who decides, what acknowledgement certifies it?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Nothing in the protocol; every queue is in memory and discarded when its peer disconnects. The one durable recipe is Titanic, a specialized MDP worker writing requests to disk | The client decides by sending through Titanic; `titanic.request` returns a UUID, `titanic.close` certifies the reply may be wiped | Stated as a condition, not a promise: "As long as requests are fully committed to safe storage, work can't get lost". Faster variants trade it away: `fsync` "every N milliseconds with accepted last-M-message loss" | [zeromq §12/P7] |
| NNG | No answer in this protocol | — | No SP persistence and no durable-delivery acknowledgement exists | [nanomsg-nng §12/P7] |
| RabbitMQ / AMQP 0-9-1 | Durable queue **and** `delivery-mode = 2`; quorum queues always persist | The certificate is `basic.ack` in confirm mode: after the disk write (classic), after a majority wrote *and flushed* (quorum), after quorum replication but no explicit `fsync` (stream) | The publisher decides persistence per message, the declarer decides queue durability, and both must line up. Transient messages are "discarded on recovery even from durable queues"; without confirms there is no certificate and loss is silent | [rabbitmq-amqp091 §12/P7], [rabbitmq-amqp091 §6d] |
| AMQP 1.0 | Two orthogonal knobs: per-message `header.durable`, and `terminus-durability` (`none` / `configuration` / `unsettled-state`) | `header.durable=true` is a hard demand — a target that cannot honour it MUST NOT accept the message and MUST reject with `amqp:precondition-failed` | `disposition(state=accepted)` means "successfully processed" and explicitly not "on disk"; durability comes from the `durable` contract, not from the acknowledgement. `unsettled-state` is what makes exactly-once survive a restart | [amqp10 §12/P7], [amqp10 §6.5] |
| MQTT 5 | Entirely the implementation's choice. The protocol requires session state to be retained for the Session Expiry Interval but never requires stable storage | **No acknowledgement certifies durability.** The spec puts the decision on the solution developer — volatile memory for meter readings, non-volatile writes before transmission for payments | 4.1.1 explicitly contemplates loss: "hardware or software failures may result in loss or corruption of Session State". No source consulted states whether an acknowledgement precedes or follows a durable write | [mqtt5 §12/P7] |
| NATS core | None | — | Core publications and subscription state do not survive a restart | [nats §12/P7] |
| NATS JetStream | File-backed streams and durable consumer state, subject to configured storage and RAFT quorum | The publish acknowledgement certifies the committed stream write, following the leader's quorum commit rather than mere reception | Memory-storage streams and non-durable consumers give no such persistence; below quorum the RAFT group cannot commit and an unacknowledged publish is ambiguous | [nats §12/P7], [nats §6] |
| Kafka | Partition logs and committed group offsets persist; replication factor plus `acks=all` plus `min.insync.replicas` | A committed record is one applied by all ISR replicas; `acks=all` certifies replication to the ISR required for the committed boundary | `acks` does **not** make a broker fsync each record before acknowledging; durability rests on replica recovery and failure assumptions. `acks=1` can lose an acknowledged record on leader failover | [kafka §12/P7], [kafka §6] |

### P8 — What ordering is guaranteed and within which scope?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Per-connection in-order delivery between directly connected peers; frames within a message ordered by construction and the message atomic | "All messages between two immediate peers SHALL be delivered in order" — scope: one connection, one hop | Nothing across peers (fair-queuing interleaves arbitrarily), across hops, across sockets, or across a failover. Wider scope needs a single ordering point plus application sequence numbers, as CHP and ZRE do | [zeromq §12/P8] |
| NNG | None generic | No generic ordering guarantee; the contract permits dropped or reordered messages | PULL explicitly leaves ties among simultaneously ready peers undefined | [nanomsg-nng §12/P8] |
| RabbitMQ / AMQP 0-9-1 | Publication order along a single content path | One publishing channel, one exchange, one queue, one consuming channel — strengthened since 2.7.0 to "always held in the queue in publication order" even under requeue or channel closure | Several consumers on one queue can still observe reordering because others requeue; priorities, `basic.get`, redelivery and prefetch perturb it. No ordering across queues, across publishing channels, or per routing key | [rabbitmq-amqp091 §12/P8] |
| AMQP 1.0 | Connection frames arrive in order with no gaps; a session is a sequential conversation; within one link deliveries MUST NOT interleave | A multi-frame message's frames are contiguous; that is the whole promise | Nothing across links (even on one session), across sessions, or per key or partition — there is no partition concept. `header.priority` explicitly licenses reordering; redelivery after `released`/`modified` gives no positional guarantee | [amqp10 §12/P8] |
| MQTT 5 | Ordered Topic: the server MUST forward per topic and per QoS in the order received from a given client, and every topic is Ordered by default on non-shared subscriptions | Scope: same publishing client, same topic, same QoS, non-shared subscription | Nothing holds across topics, publishers, QoS levels or shared subscriptions. The spec's own legal receive order after a reconnect is 1,2,3,2,3,4; Receive Maximum 1 tightens it to 1,2,3,3,4 at the cost of all pipelining | [mqtt5 §12/P8] |
| NATS core | None documented | No global ordering across subjects, servers or queue-group members | A single subscriber sees the order its connected server writes; failures and topology changes give no replay or recovery order. A queue group is not an ordering or affinity primitive | [nats §12/P8], [nats §7] |
| NATS JetStream | Stream sequence numbers assigned in append order | Stream stored order; ordered consumers repair detected gaps | Redelivery can repeat a message after later deliveries; ordered consumers trade durable acknowledgement state and load sharing for the ordered view | [nats §12/P8] |
| Kafka | Offsets within a partition | Total append order within one partition; all replicas share the same offsets and order after convergence | No total order across partitions or topics. Key order exists only while a stable key-to-partition mapping holds — changing partition count or partitioner defeats it. Rebalances transfer ownership and may replay from the committed offset | [kafka §12/P8] |

### P9 — Duplicates: where do they arise, what suppresses them, what does "exactly once" mean?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Wire-level: "A message SHALL NOT be delivered more than once to any peer"; CurveZMQ disconnects a peer that reuses a short nonce, blocking wire replay | No wire duplication; suppression above it is application state (client id plus message number, with the server caching the reply) | Duplicates come from application retries and deliberate fan-out (Freelance Model Two duplicates by design). "Exactly once" is not offered and not claimed anywhere; MDP simply assumes "Workers are idempotent" | [zeromq §12/P9] |
| NNG | None | No deduplication and no exactly-once guarantee | REQ retransmission duplicates requests, including when only the reply was lost; SURVEYOR explicitly permits duplicate responses in some topologies | [nanomsg-nng §12/P9] |
| RabbitMQ / AMQP 0-9-1 | Detection aids only: `redelivered`, `x-delivery-count`, `x-acquired-count`, `x-death` | Explicitly not offered: "RabbitMQ does not guarantee exactly-once delivery because retransmission can duplicate" | Sources: lost confirms, automatic requeue, requeue loops, quorum leader election, at-least-once dead-lettering, an RPC server dying between reply and ack. The one real facility is Stream-protocol publisher deduplication on producer name plus a strictly increasing publishing ID — opt-in, one concurrent producer per name | [rabbitmq-amqp091 §12/P9] |
| AMQP 1.0 | `snd-settle-mode=unsettled` plus `rcv-settle-mode=second`, plus link resumption, plus a terminus durable enough to make resumption possible | "Exactly once" is a precise construction, not a mode name: the sender settles only after the receiver reaches a terminal state, the receiver only after learning the sender settled | Duplicates arise when the receiver settles before the sender, and from resumption ambiguity — a tag the receiver does not remember MUST be resent. Detection aids: `header.delivery-count`, `first-acquirer`, `message-id` (a broker MAY discard a duplicate). No surveyed broker implements `second` | [amqp10 §12/P9], [amqp10 §13] |
| MQTT 5 | QoS 2's two-phase PUBLISH/PUBREC/PUBREL/PUBCOMP handshake, with the receiver required to suppress duplicate onward delivery of a repeated Packet Identifier before PUBREL | Exactly once **strictly per hop** | Not end-to-end: publisher-to-broker QoS 2 with broker-to-subscriber QoS 1 is legal and common, and the subscriber then sees duplicates. Eight distinct duplicate sources are enumerated; DUP suppresses nothing and is not propagated | [mqtt5 §12/P9], [mqtt5 §7] |
| NATS core | None | At-most-once; no deduplication | An active matching subscriber receives a publication at most once; retries duplicate application work | [nats §6] |
| NATS JetStream | `Nats-Msg-Id` within the stream's duplicate window, plus the consumer double-acknowledgement | JetStream documentation calls the combination "exactly once semantics" | The window is bounded in time, so an id reused after it is not suppressed; consumer redelivery after `ack_wait` is normal. External side effects remain application work | [nats §12/P9], [nats §6] |
| Kafka | Idempotent producer (producer ID, epoch, sequence per partition); transactions plus `read_committed` isolation | Idempotence gives exactly-once *log append per producer sequence*; transactions give exactly-once processing "within its stated transactional boundary" | Duplicates from non-idempotent retries and from post-processing pre-commit crashes. `read_committed` is a visibility mode, not a guarantee about external side effects; Kafka alone cannot atomically commit arbitrary external effects | [kafka §12/P9], [kafka §6] |

### P10 — Request/reply: how is a reply correlated and routed back through intermediaries?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | The reply envelope: zero or more reply addresses, an empty delimiter frame, then the body. Each ROUTER hop prepends the arriving connection's identity and strips the first frame on send; REP stores and replays the whole envelope | Replies return along the recorded address stack; the thread-safe family uses a 32-bit routing id per message instead | REQ correlation is positional — "each reply received is matched with the last issued request" — unless `ZMQ_REQ_CORRELATE` adds a request-id frame. ROUTER learns an identity only after the peer speaks, so an application "can really reply, but cannot spontaneously talk to a peer" | [zeromq §12/P10] |
| NNG | A backtrace stack of big-endian 32-bit IDs: the final request ID has its MSB set, preceding forwarder peer IDs do not | Devices push their local peer ID on the way in and pop it on the way back | The pipe-local IDs route the response backwards without supplying a globally meaningful peer identity; a new REQ send discards the earlier reply but cannot withdraw the request | [nanomsg-nng §12/P10] |
| RabbitMQ / AMQP 0-9-1 | An exclusive server-named callback queue per client, `reply_to` naming it, a unique `correlation_id`; the responder replies via the default exchange with `routing_key = props.reply_to` | Correlation is pure convention: the broker interprets neither property | The exception is Direct Reply-To, where the broker rewrites `reply_to` and routes the reply over the requester's own channel with no queue — at-most-once, auto-ack only, dropped if the requester has gone, and never returned as unroutable even with `mandatory` | [rabbitmq-amqp091 §12/P10] |
| AMQP 1.0 | `properties.reply-to` names the reply node, `correlation-id` ties response to request, `reply-to-group-id` routes replies to a group; all live in the immutable bare message | Intermediaries cannot rewrite them. The reply node is commonly created on demand with `source.dynamic=true` and `lifetime-policy=delete-on-close` | Recipients SHOULD prioritize replying over the connection the request arrived on, then fall back to the `reply-to` network endpoint; any network endpoint in a *link* address MUST be ignored | [amqp10 §12/P10] |
| MQTT 5 | `Response Topic` names where the reply goes, `Correlation Data` is opaque binary copied into the reply | The broker forwards both properties unaltered and otherwise treats them as ordinary messages | The requester must already be subscribed to the Response Topic or the reply is dropped; a self-chosen topic is often unauthorized, so the server can hand the client a namespace via Response Information. There may be zero responders or several | [mqtt5 §12/P10], [mqtt5 §4.3] |
| NATS core | A reply-to inbox subject (conventionally `_INBOX.`) that the requester subscribes to; the responder publishes there | The reply is routed by ordinary subject interest, not by a hidden correlation field | A timeout cannot distinguish absent responders, network loss, slow processing or a lost reply; multiple responders can all answer the same inbox (scatter-gather). `no_responders` gives a fast negative | [nats §12/P10] |
| NATS JetStream | The same inbox mechanism: a JetStream publish is a Core publish to a stream-covered subject plus a request for the publish acknowledgement | The acknowledgement reports stream name, stream sequence and duplicate status | Because the ack is itself a reply on a subject, a lost ack is indistinguishable from a lost publish and must be retried with `Nats-Msg-Id` | [nats §6], [nats §12/P1] |
| Kafka | The wire protocol correlates a response to a request by connection plus request-header correlation ID | Responses return in send order on one connection | Kafka supplies no general application request/reply routing pattern — request/reply between applications is not a Kafka topology | [kafka §12/P10] |

### P11 — Topology: who binds and who connects, brokered vs brokerless, discovery

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | "ZMTP is by default a peer-to-peer protocol that makes no distinction between the clients and servers"; all sockets bind and connect, and one socket may do both to many endpoints (except PAIR and CHANNEL) | Both sides can be either. Brokerless with easy intermediaries: `zmq_proxy()` gives shared queue, forwarder and streamer in one call | The guide still prescribes a bias — binder at a well-known address, connector dynamic. Discovery: none in ZMTP; static configuration, a Freelance name service, or 36/ZRE's UDP beacons on port 5670 | [zeromq §12/P11] |
| NNG | Either pattern role may dial, listen, or both simultaneously | Brokerless; endpoint direction does not prescribe the application role | No discovery service; BUS fan-out is one hop, so a fully connected mesh must be established by the application | [nanomsg-nng §12/P11] |
| RabbitMQ / AMQP 0-9-1 | Brokered always; clients are always the initiators. Roles are symmetric in that any connection may publish and consume | Topology is defined by applications, not administrators: "AMQP 0-9-1 entities and routing schemes are primarily defined by applications themselves" | No brokerless or peer-to-peer mode; Federation and Shovel are broker-side plugins, not protocol features. Discovery: none in the protocol; cluster formation uses static config or DNS/AWS/Kubernetes/Consul/etcd plugins | [rabbitmq-amqp091 §12/P11] |
| AMQP 1.0 | Symmetric by design: "AMQP does not differentiate between clients and servers, but only knows communicating peers"; either peer may initiate `attach` in either role | Brokered and brokerless are both native — a broker is just a container holding distribution nodes, and Dispatch demonstrates a router that "never assumes ownership of a message" | The only asymmetries are TCP-level (the TCP client sends its header first, and is the TLS and SASL client) plus the convention that the peer holding addressable nodes resolves addresses | [amqp10 §12/P11] |
| MQTT 5 | Strictly brokered and strictly asymmetric: the server binds and accepts, the client connects | A client can never listen; there is no brokerless mode and no client-to-client path | Broker-to-broker federation is outside the protocol — done by running a client on one broker, which is what No Local and Retain As Published were added for. Discovery: none; `Server Reference` is the only location hint | [mqtt5 §12/P11] |
| NATS core | Clients connect to servers; servers form clusters with routes, superclusters with gateways, and leaf nodes extend a remote edge server | Interest-based routing: cross-cluster traffic is sent only where there is remote interest | `INFO.connect_urls` is the server-to-client topology discovery mechanism; a disconnected leaf partitions edge and upstream interest | [nats §12/P11] |
| NATS JetStream | The same client-to-server topology; JetStream metadata and replicated stream/consumer state use RAFT groups | For a replicated stream the leader appends and a quorum commit precedes the publish acknowledgement | A mirror is read-only and cannot accept independent local writes; a partition below quorum blocks commits | [nats §4], [nats §9] |
| Kafka | Brokered: clients try configured bootstrap addresses, issue Metadata, then connect directly to partition leaders or eligible followers | Bootstrap is brokered discovery; cached metadata is refreshed after network or leadership errors | A bootstrap list need not contain every broker, but multiple addresses tolerate one being down; `NOT_LEADER_OR_FOLLOWER` is retriable because metadata may be stale | [kafka §12/P11] |

### P12 — Flow-control credit: unit, grantor, default, exhaustion behaviour

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | High-water marks. Unit: **messages**. Grantor: nobody — there is no credit exchange in ZMTP 3.1 | `ZMQ_SNDHWM`/`ZMQ_RCVHWM` default 1000 each, a purely local bound applied per peer | At exhaustion: block, drop or `EAGAIN` by socket type. Kernel buffers underneath make the bound larger and imprecise. Credits appear in 37/ZMTP only under "Topics for Discussion" — "Credit could be octets, or messages" | [zeromq §12/P12] |
| NNG | Local socket buffer depths. Unit: **messages**, 0-8192. Grantor: nobody | No receiver-granted credit; PUSH selection is governed by peer availability | Exhaustion blocks or times out, or follows the protocol-specific drop policy. `RECVMAXSZ` is a separate byte bound, not a queue limit | [nanomsg-nng §12/P12] |
| RabbitMQ / AMQP 0-9-1 | Consumer prefetch. Unit: **messages** (unacknowledged deliveries). Grantor: the consumer, via `basic.qos` | Default 0 meaning no limit, with a server-side `default_consumer_prefetch` | At exhaustion the broker stops delivering on that channel until an ack frees a slot — no error, no drop, no disconnect. `prefetch_size` is unimplemented; the `global` flag's scoping diverges from the spec; publisher-side credit is not exposed, surfacing only as `flow` and `connection.blocked`. Streams use chunk credits instead | [rabbitmq-amqp091 §12/P12] |
| AMQP 1.0 | Two simultaneous schemes. Link: unit **one message**, grantor the receiver alone, initial 0. Session: unit **one transfer frame**, grantor the receiver via `incoming-window`, mandatory on `begin` | Credit is communicated as an absolute delivery-limit (`delivery-count + link-credit`), not an increment, which makes `flow` idempotent | At the bound the sender MUST NOT send more — it blocks. Violating the session window MUST end the session with `amqp:session:window-violation`. No per-connection credit exists. `drain` forces the sender to consume all outstanding credit and report | [amqp10 §12/P12], [amqp10 §5] |
| MQTT 5 | Send quota. Unit: **one QoS 1 or QoS 2 PUBLISH packet**. Grantor: the receiver, via `Receive Maximum` in CONNECT or CONNACK, independently per direction | Default 65,535; 0 is a Protocol Error. Replenished by one per PUBACK/PUBCOMP regardless of the code carried, never above the initial value | Exhaustion blocks QoS > 0 PUBLISH only; all other packet types MUST still be processed and answered. QoS 0 has no credit mechanism at all. Violation earns DISCONNECT 0x93. The quota is per connection and explicitly not session state | [mqtt5 §12/P12] |
| NATS core | None | No publisher credit, publisher acknowledgement or publisher-side flow control | The practical bound is the receiving connection's `max_pending` byte limit, after which the server disconnects the slow consumer | [nats §12/P12] |
| NATS JetStream | `max_ack_pending` (messages, default 1000); pull batch size and expiration; optional push flow-control status requests awaiting a client reply; idle heartbeats | Consumer-level bound on unacknowledged delivered messages | At the bound delivery pauses until acknowledgements advance it. `max_waiting` (default 512) bounds outstanding pull requests | [nats §12/P12], [nats §5] |
| Kafka | No per-message consumer credit: consumers pull byte-bounded Fetch responses; producers accumulate into `buffer.memory` | `fetch.max.bytes` 50 MiB and `max.partition.fetch.bytes` 1 MiB are soft — the first oversized batch is returned anyway to guarantee progress | A producer exhausting buffer capacity blocks until `max.block.ms`, then fails. Broker quotas throttle rather than create record credit | [kafka §12/P12] |

### P13 — Large messages and streaming bodies

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Frame body up to 2^63-1 octets by grammar; `ZMQ_MAX_MSGSZ` defaults to and caps at `INT_MAX`; `ZMQ_MAXMSGSIZE` is the per-socket inbound limit | Atomic delivery: "all frames or none"; the first part goes on the wire only when the last is sent | A body **cannot** be delivered before it is complete, and multipart does not reduce memory — the guide says to split large files into separate single-part messages. Exceeding `ZMQ_MAXMSGSIZE` disconnects the sender. PGM is the one place a message spans transport frames | [zeromq §12/P13] |
| NNG | `RECVMAXSZ` bounds an accepted remote message in bytes; zero disables the limit | NNG delivers a message wholly or not at all | No body streaming and no partial-message delivery; an oversized message is discarded. `inproc` accepts but ignores `RECVMAXSZ` | [nanomsg-nng §12/P13] |
| RabbitMQ / AMQP 0-9-1 | Body frames of at most the negotiated `frame-max` (131072 default), total carried as a 64-bit `body-size`; `max_message_size` 16 MiB default, 512 MiB maximum | The wire streams; the API does not — a receiver MUST reassemble the frames as a single set | RabbitMQ "always delivers the message in full to the client" even when rejected mid-send; cancelling a partially sent content is marked "planned". No chunked or resumable transfer, no content offset | [rabbitmq-amqp091 §12/P13] |
| AMQP 1.0 | Multi-frame delivery with `more=true` on all but the last transfer; `attach.max-message-size` unset or zero means unlimited | **Yes, a body can be delivered before it is complete**: the transport hands payload up as it arrives, `received(section-number, section-offset)` exists precisely to name a partial position, and Proton exposes `Delivery.partial` | Deliveries on one link MUST NOT interleave, so a partial delivery blocks its link; `aborted=true` on the last transfer tells the receiver to discard everything so far and implicitly settles. Oversize gives `amqp:link:message-size-exceeded` | [amqp10 §12/P13], [amqp10 §3] |
| MQTT 5 | One PUBLISH per whole Application Message; ceiling 268,435,455 bytes from the Remaining Length encoding, either peer may declare a lower `Maximum Packet Size` | A body cannot be delivered before it is complete: no fragmentation, no continuation packet, no chunked mode | A message too large for a subscriber is silently discarded and the server "behave[s] as if it had completed sending". Practical limits are far lower: Mosquitto 2,000,000 bytes, EMQX 1 MB | [mqtt5 §12/P13] |
| NATS core | `PUB`/`HPUB` frame the complete byte count before routing; `INFO.max_payload` advertises the ceiling, default 1 MiB | The payload is complete before routing begins | No partial-body streaming delivery primitive; an oversized message is rejected with a protocol error or disconnect | [nats §12/P13] |
| NATS JetStream | The same server payload ceiling; the object store chunks objects into stream messages | Large objects become many stream messages | Chunking is an abstraction above the protocol, not a streaming body | [nats §12/P13], [nats §2] |
| Kafka | Records travel as complete record batches; `message.max.bytes`/`max.message.bytes` bound accepted batches, producer `max.request.size` bounds a request | Batches, not streamed bodies | Oversize is rejected with `MESSAGE_TOO_LARGE`; a broker disconnects a client whose request exceeds the maximum request size. A first oversized batch is still returned on fetch, so the consumer must be able to hold it | [kafka §12/P13], [kafka §11] |

### P14 — Identity: authentication, per-message visibility, authorization granularity

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | Per connection at handshake: NULL, PLAIN or CURVE, exactly one mechanism per socket and no negotiation; authorization delegated to a ZAP handler over an in-process REQ/REP dialog | Security is "assertive": all peers on a socket share the same required level, which "prevents downgrade attacks" | An authenticated identity is **not** visible per message: the CURVE public key and the ZAP user id are per-connection facts held by the server; the only per-message label is the ROUTER/SERVER routing id, self-asserted when the peer chose it. Authorization granularity is per connection; the ZAP domain is the only scoping string | [zeromq §12/P14] |
| NNG | TLS authenticates transport peers; IPC can expose OS-derived UID/GID/PID, described as non-forgeable at connection time | Transport-level authentication only | The SP pattern protocols define no message-level authentication, authorization or authenticated sender identity | [nanomsg-nng §12/P14] |
| RabbitMQ / AMQP 0-9-1 | SASL during the handshake (`PLAIN`, `AMQPLAIN`, `ANONYMOUS`, `RABBIT-CR-DEMO`, or `EXTERNAL` mapping an X.509 subject) | An identity is visible per message only if the publisher sets `user-id`, which RabbitMQ validates against the connection's user | The `impersonator` tag allows forging `user-id`. Granularity: vhost admission, then `configure`/`write`/`read` regexes over entity names, plus optional topic routing-key permissions. There is no per-message authorisation | [rabbitmq-amqp091 §12/P14] |
| AMQP 1.0 | SASL in a dedicated protocol layer, mechanisms in server preference order; TLS client certificates at the TLS layer, with SNI selecting back end and validation domain | Exactly one per-message identity field: `properties.user-id`, client-set, "MAY be authenticated by intermediaries", immutable because it is in the bare message | The core standard has no authorization model at all — only `amqp:unauthorized-access`, `amqp:not-allowed`, `amqp:resource-locked` and the ability to refuse an `attach` with a null terminus. Brokers supply granularity | [amqp10 §12/P14] |
| MQTT 5 | User Name and Password in CONNECT, TLS client certificates, or the enhanced AUTH exchange (SASL-style, repeatable in-connection) | Authorization granularity: per topic name for publish, per topic filter for subscribe, reported as reason codes 0x87/0x8F | An authenticated identity is **not** visible per message: PUBLISH has no sender field, so identity reaches subscribers only via payload, User Property or topic. The authorization mechanism is non-normative | [mqtt5 §12/P14] |
| NATS core | Per connection, before authorized activity: token, user/password, TLS client certificates, NKeys (signing a server nonce), JWT operator mode, or an authorization callout | An account is an isolated subject namespace and tenant boundary; publish/subscribe permissions are subject-pattern grants and denies | The authenticated connection identity is not a standard per-message field; a per-message business identity must be carried by the application or an agreed header convention | [nats §12/P14], [nats §10] |
| NATS JetStream | The same connection authentication; account limits scope JetStream resources | Stream and consumer resources are bounded per account | The same absence of per-message identity | [nats §12/P14], [nats §12/P15] |
| Kafka | TLS (optionally mutual) and SASL authenticate the connection; ACLs authorize principals at cluster, topic, group, transactional-ID and delegation-token scopes | Authorization is evaluated for the client request that reads or writes a record | A record does not carry an authenticated Kafka principal as a native per-message field; `client.id` is a logging and quota label, not authentication | [kafka §12/P14], [kafka §10] |

### P15 — Resource bounds: what a hostile or buggy peer can make the other side allocate

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | `ZMQ_MAXMSGSIZE`, HWMs, `ZMQ_MAX_SOCKETS` (1023), `ZMQ_BACKLOG` (100), `ZMQ_HANDSHAKE_IVL` (30 s), heartbeat and timeout options; 37/ZMTP advises allocating memory only after the handshake and capping in-progress handshakes per IP | Without `ZMQ_MAXMSGSIZE` a frame may declare up to 2^63-1 octets | Residual exposures: subscriptions are additive and non-idempotent so repeated SUBSCRIBEs accumulate; `ZMQ_ROUTER_HANDOVER` lets a peer claiming an in-use identity evict the incumbent; on `ipc` a local process can steal a bound endpoint; `ZMQ_CONFLATE` on an unread socket grows without bound | [zeromq §12/P15] |
| NNG | `RECVMAXSZ` per endpoint, set before the endpoint starts; message queue depths 0-8192 | Per-endpoint limits allow different bounds at different trust boundaries | `RECVMAXSZ` zero remains unbounded and `inproc` ignores the option entirely | [nanomsg-nng §12/P15] |
| RabbitMQ / AMQP 0-9-1 | `max_message_size`, negotiated `frame-max`, `channel_max`, per-vhost connection and queue limits, operator policies that override client arguments | Oversized frames give 501 `FRAME_ERROR` and kill the connection | Unacknowledged deliveries with `prefetch_count = 0` or auto-ack are unbounded memory on both sides; unbounded queues grow to the alarm; leaked channels, connections, consumers, queues and vhosts cost node RAM and replicated metadata. Two documented holes: the memory watermark is a hint a node can exceed, and "MUST NOT discard a persistent message on queue overflow" is contradicted by `drop-head` | [rabbitmq-amqp091 §12/P15] |
| AMQP 1.0 | `max-frame-size`, `channel-max`, `handle-max`, `max-message-size`, `idle-time-out`, plus the mandatory session windows | Only three limits are safe by construction: `incoming-window` and `outgoing-window` are mandatory, and `link-credit` starts at 0 | Accepting the defaults means agreeing to a 4 GiB frame, 65536 sessions, 2^32 links per session, unbounded message size and a connection that never times out. The unsettled map is unbounded and persists for a durable terminus; `incomplete-unsettled` bounds the frame, not the map. Link names are "arbitrarily long without a significant penalty" | [amqp10 §12/P15], [amqp10 §11] |
| MQTT 5 | `Receive Maximum`, `Maximum Packet Size`, `Topic Alias Maximum`, `Maximum QoS` (refuse QoS 2 entirely), `Server Keep Alive`, availability flags | Declarative per-connection limits, one exchange each way | A hostile peer can hold session state for up to 0xFFFFFFFF seconds, an unbounded queue behind it (the protocol sets no queue bound), up to 65,535 concurrent QoS 2 receiver states, alias-table entries, arbitrarily many subscriptions, and 256 MiB per packet where no maximum was declared | [mqtt5 §12/P15] |
| NATS core | `max_payload` (1 MiB), `max_control_line`, `max_connections` (65,536), `max_pending` (64 MiB), `ping_interval`/`max_pings_out`, `write_deadline` (10 s) | Server-side caps on payload, line size, connections, pending bytes and write time | A hostile client can consume connection slots, subscriptions, pending buffers and pull requests until the corresponding server or account limits intervene; the monitoring port is unauthenticated by default | [nats §12/P15], [nats §11] |
| NATS JetStream | Stream limits (messages, bytes, age, per-subject, consumers, storage), consumer `max_ack_pending` and `max_deliver`, pull `max_waiting`, account quotas | The discard policy decides overflow: `DiscardNew` rejects, `DiscardOld` evicts | Configured JetStream storage is itself an allocation a client can consume up to the account limit | [nats §12/P15], [nats §11] |
| Kafka | Request-size, message-size, fetch-size, producer-buffer, retention, session, acquisition-lock and quota settings | Broker request-size limits disconnect an oversized request before unbounded parsing | Fetch byte bounds are soft (a first oversized batch is returned regardless); open transactions delay the last stable offset and can withhold later offsets; quotas bound a client's share of capacity rather than its retained data | [kafka §12/P15], [kafka §11] |

### P16 — Observability: what a party can measure about delivery

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | No confirms, receipts, counters or tracing headers in the protocol. `zmq_socket_monitor()` delivers two-frame connection events on an `inproc` PAIR; `zmq_proxy()`'s capture socket (Espresso) mirrors all traffic including subscription frames | Connection-level lifecycle visibility plus optional full traffic capture | Monitoring works only on TCP, IPC and TIPC; `ZMQ_EVENT_ALL` is warned against for applications relying on a fixed event sequence. Message-level visibility is application convention: sequence numbers for gaps, timestamps for latency | [zeromq §12/P16] |
| NNG | Pipe add/remove callbacks, source-pipe handles on messages, statistics snapshots | Connection lifecycle and per-message source pipe | No delivery receipts and no tracing header is defined | [nanomsg-nng §12/P16] |
| RabbitMQ / AMQP 0-9-1 | Publisher confirms with sequence numbers and `multiple`, `basic.return`, consumer acknowledgements, `redelivered`, `queue.declare-ok` counts, `connection.blocked`/`unblocked`, server-sent `basic.cancel`; message-level `x-death`, `x-delivery-count`, `x-acquired-count` | In-protocol per-message delivery signals in both directions | No tracing headers: the broker MUST NOT modify existing message information and adds only dead-letter headers. Out-of-band metrics such as consumer capacity are "merely a hint" | [rabbitmq-amqp091 §12/P16] |
| AMQP 1.0 | `disposition` is the receipt; wire counters `flow.available`, `flow.delivery-count`, `flow.link-credit`, `header.delivery-count`; `flow.echo` demands the partner's state | A disposition certifies the delivery's state at the issuing endpoint, and with `settled=true` that the endpoint has forgotten it | Tracing has no defined field; `message-annotations` with `x-opt-` keys are the extension point, must be ignored if not understood, and intermediaries MUST propagate them | [amqp10 §12/P16] |
| MQTT 5 | The reason code on PUBACK or PUBREC is the only in-protocol delivery receipt, certifying ownership transfer for one hop | Code 0x10 (No matching subscribers) is the one signal that a message reached nobody | 0x10 is optional — the server "MAY use this Reason Code instead of 0x00". Reason String "SHOULD NOT be parsed" and is suppressed if the client set Request Problem Information to 0 or if it would exceed the peer's Maximum Packet Size. `User Property` is the only general tracing carrier | [mqtt5 §12/P16] |
| NATS core | `+OK` in verbose mode acknowledges a well-formed protocol operation only; `-ERR` reports protocol or authorization errors; HTTP monitoring endpoints `/varz`, `/connz`, `/routez`, `/healthz` | Protocol-level and operational visibility, not delivery visibility | `+OK` is explicitly not application processing or subscriber receipt; `/varz` exposes a `slow_consumers` counter and `/connz` per-connection `pending_bytes` | [nats §12/P16], [nats §6] |
| NATS JetStream | Publish acknowledgements expose stream name, stream sequence and duplicate status; consumer acknowledgements and delivery metadata expose progress and redelivery state | Per-message publish receipt with a sequence identity | `max_deliver` exhaustion surfaces as an advisory rather than a successful acknowledgement | [nats §12/P16] |
| Kafka | Produce responses, offsets, committed group offsets, protocol error codes labelled by retriability, `client.id`, broker and client metrics; record headers can carry tracing context | An offset is a durable, comparable position; a Produce response names the appended offset | Offset commit is a checkpoint of the next position, not proof that every prior record's business effect succeeded; a delivery timeout is a producer deadline, not evidence that no append occurred. Kafka does not interpret tracing semantics | [kafka §12/P16], [kafka §11] |

### P17 — Shutdown: what happens to in-flight and queued messages on orderly close?

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | `ZMQ_LINGER` per socket: default -1 (infinite), 0 discards immediately, positive is a millisecond bound. `zmq_ctx_term()` interrupts blocking calls with `ETERM`, then blocks until every socket is closed and every sent message is transferred or its linger expired | Linger certifies transfer to the network, not receipt by the application | ZMTP has no close handshake — either peer may close at any moment. With the default infinite linger "`zmq_ctx_destroy()` will by default wait forever if there are pending connects or sends". A partially sent multipart message is cancelled only by closing the socket | [zeromq §12/P17] |
| NNG | Closing a pipe removes it; after `REM_POST` communication over it is impossible | None | No generic in-flight drain, linger or outcome guarantee is specified; an outstanding REQ may resend after the disconnect | [nanomsg-nng §12/P17] |
| RabbitMQ / AMQP 0-9-1 | `channel.close`/`close-ok` and `connection.close`/`close-ok`, after which received methods other than close are discarded | No linger and no drain | Closing a channel requeues every unacknowledged delivery on it — also the documented way to *deliberately* requeue, since `basic.cancel` does not — and acks sent immediately before closing may never reach the queue. Auto-delete and exclusive queues are deleted synchronously; messages written to the socket are not guaranteed to have arrived | [rabbitmq-amqp091 §12/P17] |
| AMQP 1.0 | Teardown is link, then session, then connection, each explicit; `close` MUST be the last thing written and the sender SHOULD keep reading until the partner's `close` | A `detach` without `closed` leaves deliveries "live": a `disposition` MAY refer to deliveries on links no longer attached, as long as they were not closed or detached with an error, and the state MUST be applied | Draining is done separately, before teardown: `flow(link-credit=0, echo=true)` or `flow(drain=true)` provides the quiescence point. An errored detach destroys the endpoint and later input on that handle MUST end the session | [amqp10 §12/P17], [amqp10 §9] |
| MQTT 5 | DISCONNECT 0x00, after which the sender MUST send nothing more and MUST close; the server MUST discard the stored Will without publishing it | No linger and no drain — anything in flight is abandoned on the wire | The fate of those messages is decided by the session, not the shutdown: with Session Expiry > 0 unacknowledged QoS > 0 messages and unanswered PUBRELs remain session state and are resent; with Session Expiry 0 everything queued and in flight is discarded. QoS 0 in flight is lost either way | [mqtt5 §12/P17] |
| NATS core | `UNSUB` removes a subscription (optionally after a message count); ending the transport ends the connection and its subscriptions. Lame Duck Mode notifies clients with `ldm`, stops admitting new connections, drains existing clients, then shuts down | Lame Duck Mode is a server-side drain | Closing a Core connection loses its outstanding ephemeral delivery state; in-flight Core delivery remains non-durable during the Lame Duck drain | [nats §12/P17], [nats §1] |
| NATS JetStream | A pull request that expires ends with the messages already received or a status response | It does not acknowledge delivered messages | Unacknowledged deliveries remain eligible for redelivery after `ack_wait` | [nats §8], [nats §12/P17] |
| Kafka | Controlled broker shutdown migrates partition leadership and flushes data; producer `close`/`delivery.timeout.ms` and consumer commits determine client-side draining | Leadership migration keeps partitions available across a planned shutdown | Uncommitted processing may replay after restart; a delivery timeout is a producer deadline, not evidence that no append occurred | [kafka §12/P17], [kafka §11] |

### P18 — Transports

| Protocol | Mechanism | Guarantee | Failure behaviour | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | ZMTP is specified over "a connected transport layer such as TCP". libzmq implements `tcp`, `ipc`, `inproc`, `pgm`/`epgm`, `udp` (RADIO/DISH only), `vmci`, `tipc`, `vsock`, plus DRAFT `ws`/`wss` | Multipart framing is identical across transports | What changes: PGM breaks message-to-datagram alignment and needs an offset field; UDP restricts you to RADIO/DISH; `inproc` shares buffers so "real HWM is the sum of both sides' configured HWMs"; monitoring works only on TCP/IPC/TIPC; CURVE and ZAP are documented "when using TCP transport"; PAIR/CHANNEL are effectively `inproc`-only; filter placement is transport-dependent | [zeromq §12/P18] |
| NNG | SP RFCs define TCP, IPC, TLS, UDP and WebSocket mappings; NNG 1.10.0 exposes inproc, IPC, TCP, TLS-over-TCP, WebSocket/WSS and experimental ZeroTier | Pattern semantics remain socket protocol semantics across transports | Security and framing differ by transport; `inproc` ignores `RECVMAXSZ`; ZeroTier setup can take about a minute in extreme cases | [nanomsg-nng §12/P18] |
| RabbitMQ / AMQP 0-9-1 | The specification defines TCP only, port 5672. RabbitMQ adds TLS on 5671, PROXY protocol v1/v2, and a separate binary Stream protocol on 5552/5551 | One port carries AMQP 0-9-1 and AMQP 1.0, distinguished by protocol header | No AMQP 0-9-1 over WebSocket, no QUIC, no IPC, no in-process transport. Enabling PROXY protocol requires *all* clients to arrive through a proxy; the Stream protocol changes the delivery model entirely | [rabbitmq-amqp091 §12/P18] |
| AMQP 1.0 | TCP 5672 plus TLS-first 5671; TLS (`%d2`), SASL (`%d3`) and bare AMQP (`%d0`) are nesting protocol-id layers. UDP and SCTP ports are reserved but undefined | WebSocket is a separate Committee Specification: one WebSocket message per protocol header, then AMQP frames as binary payloads | Over WebSocket there is *no* mapping between AMQP frames and WebSocket messages in either direction, and mechanisms that themselves provide encryption are not supported | [amqp10 §12/P18] |
| MQTT 5 | Normatively any transport providing "an ordered, lossless, stream of bytes" both ways; none mandatory. TCP 1883, TLS 8883; WebSocket has the only normative binding | The WebSocket binding changes framing and handshake, not semantics | Connectionless transports such as UDP are declared "not suitable on their own because they might lose or reorder data". **QUIC is not an MQTT 5.0 transport**: EMQX ships it and states it "is not yet an MQTT standard protocol", and its multi-stream design changes the guarantees | [mqtt5 §12/P18] |
| NATS core | TCP/IP by default, TLS over the connection, WebSocket client connections, and UNIX-domain sockets when the server is embedded in Go | The client protocol is an ASCII control protocol with length-prefixed opaque payloads on any of them | MQTT and WebSocket listeners are server integration options whose protocol semantics are not Core NATS semantics | [nats §12/P18] |
| NATS JetStream | The same client transports; JetStream is an API over subjects rather than a transport | No transport of its own | Replication is a server-side RAFT concern, not a client transport concern | [nats §12/P18], [nats §4] |
| Kafka | The binary protocol over TCP, optionally protected by SSL/TLS and authenticated with SASL; PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL are listener security protocols, not alternative transports | Ordered request/response per connection | Kafka does not define QUIC, WebSocket, IPC or in-process transports. TLS disables the `sendfile` zero-copy path because TLS processing happens in user space | [kafka §12/P18], [kafka §11] |
| IPC (local transports available to any of the above) | Linux `AF_UNIX` `SOCK_STREAM`/`SOCK_SEQPACKET` (filesystem or abstract), pipes/FIFOs, memfd plus futex; macOS `AF_LOCAL` `SOCK_STREAM`, Mach ports, XPC, POSIX SHM; Windows named pipes (message mode), `AF_UNIX` (stream only), file mappings, mailslots, ALPC; loopback TCP everywhere; in-process channels | Kernel framing exists only as Linux `SOCK_SEQPACKET` and Windows message-mode pipes; macOS has none and requires application framing | Differences that matter: stale endpoint after crash (pathname yes, abstract no, Windows pipes no, Windows `AF_UNIX` yes); handle passing (`SCM_RIGHTS` on Unix, `DuplicateHandle` on Windows, none on Windows `AF_UNIX`); readiness (epoll/kqueue) versus completion (IOCP), which "does not unify"; no kernel zero-copy path for `AF_UNIX`, so bulk payloads need shared memory | [ipc §4], [ipc §11], [ipc §8] |

---

## 2. Common denominators

Recurring shapes across all eight sheets. Each is a claim about several protocols at once, with
the references that support it.

**D1 — The unit of flow-control credit is almost never bytes.** Messages: ZeroMQ HWM
[zeromq §12/P12], NNG socket buffers [nanomsg-nng §12/P12], RabbitMQ consumer prefetch
[rabbitmq-amqp091 §12/P12], AMQP 1.0 link credit [amqp10 §12/P12], JetStream
`max_ack_pending` [nats §12/P12]. Packets: MQTT's send quota, which counts QoS > 0 PUBLISH
packets only [mqtt5 §12/P12]. Frames: AMQP 1.0's session window, which the spec itself
describes as byte control in disguise "since frames have a maximum size for a given
connection" [amqp10 §5.2]. Bytes appear only as side limits — NATS `max_pending`
[nats §12/P12], Kafka fetch byte bounds [kafka §12/P12], RabbitMQ's unimplemented
`prefetch_size` [rabbitmq-amqp091 §12/P12] — or as a *separate* size cap (`RECVMAXSZ`,
`max_payload`, `Maximum Packet Size`, `max-message-size`). Only AMQP 1.0 runs two credit
schemes at once with different units [amqp10 §5.3].

**D2 — Credit is receiver-granted or it does not exist.** Receiver-granted: AMQP 1.0 link
credit and session window [amqp10 §12/P12], MQTT `Receive Maximum` [mqtt5 §12/P12], RabbitMQ
`basic.qos` [rabbitmq-amqp091 §12/P12], JetStream `max_ack_pending` and pull batches
[nats §12/P12]. No grantor at all — purely local queues: ZeroMQ, where 37/ZMTP lists credit
under "Topics for Discussion" [zeromq §12/P12]; NNG [nanomsg-nng §12/P12]; NATS core
[nats §12/P12]. Kafka inverts the question by making the consumer pull [kafka §12/P12].

**D3 — Four states an acknowledgement can certify, and every protocol picks a subset.**
*Ownership transferred for this hop*: MQTT PUBACK/PUBREC [mqtt5 §6], AMQP 1.0
`disposition(accepted)` [amqp10 §6.4], ZeroMQ's `zmq_send` returning "0MQ has assumed
responsibility" [zeromq §6]. *Stored*: RabbitMQ `basic.ack` for a persistent message in a
durable classic queue [rabbitmq-amqp091 §6d]. *Replicated*: RabbitMQ quorum confirm after a
majority wrote and flushed [rabbitmq-amqp091 §6d], JetStream publish ack after quorum commit
[nats §6], Kafka `acks=all` at the ISR boundary [kafka §6]. *Processed*: only the
application-level reply — MDP's `FINAL` [zeromq §6], an NNG REQ reply [nanomsg-nng §6], a NATS
reply on an inbox [nats §12/P10] — plus JetStream's `+ACK`, documented as confirming
"successful processing" [nats §6]. Three sheets state in nearly identical words that *no*
acknowledgement in their protocol certifies that a consumer processed anything:
[rabbitmq-amqp091 §6] ("that needs an application-level reply"), [mqtt5 §6] (PUBCOMP means
"the handshake terminated, not that any subscriber saw anything"), [nats §6] ("It does not
certify that any consumer received or processed the message").

**D4 — Ordering scope is always narrower than a topic.** Per connection or hop: ZeroMQ
[zeromq §12/P8], AMQP 1.0's connection guarantee [amqp10 §12/P8]. Per link or channel path:
AMQP 1.0 link non-interleaving [amqp10 §12/P8], RabbitMQ's single content path
[rabbitmq-amqp091 §12/P8]. Per (client, topic, QoS): MQTT's Ordered Topic [mqtt5 §12/P8]. Per
partition: Kafka [kafka §12/P8]. Per stream: JetStream [nats §12/P8]. None: NNG
[nanomsg-nng §12/P8], NATS core [nats §12/P8]. Only Kafka has a key concept, and even there
key order is emergent from a stable key-to-partition mapping rather than promised
[kafka §12/P8]. Four sheets independently note that competing consumers or fan-out break
whatever order exists: [rabbitmq-amqp091 §7], [mqtt5 §7], [nats §7], [amqp10 §7].

**D5 — Liveness is a heartbeat with an application-chosen timeout, everywhere.** ZeroMQ
`PING`/`PONG` with TTL, off by default in libzmq [zeromq §12/P2]; RabbitMQ heartbeat frames,
60 s, dead after two missed [rabbitmq-amqp091 §12/P2]; AMQP 1.0 `idle-time-out` with empty
frames [amqp10 §12/P2]; MQTT Keep Alive with the 1.5x rule [mqtt5 §12/P2]; NATS `PING`/`PONG`
with `ping_interval`/`max_pings_out` [nats §12/P2]; Kafka group heartbeats plus KRaft broker
heartbeats [kafka §12/P2]. NNG has no common SP heartbeat at all — only TCP keepalive and
ZeroTier's own probing [nanomsg-nng §1]. Two sheets give the same rationale almost verbatim:
connections "go stale and die without reporting TCP errors" [zeromq §1], and untuned TCP
detection takes about 11 minutes [rabbitmq-amqp091 §1] or "roughly 30 minutes"
[zeromq §12/P2]. Three sheets add the same refinement: treat *any* incoming traffic as a sign
of life [zeromq §1], [rabbitmq-amqp091 §1], [nats §12/P2].

**D6 — Authenticated identity is per connection everywhere; per-message identity is optional,
self-asserted, or absent.** Per-connection only, nothing per message: ZeroMQ
[zeromq §12/P14], NNG [nanomsg-nng §12/P14], MQTT [mqtt5 §12/P14], NATS [nats §12/P14], Kafka
[kafka §12/P14]. One optional per-message field: RabbitMQ `user-id`, validated against the
connection user but forgeable with the `impersonator` tag [rabbitmq-amqp091 §12/P14]; AMQP 1.0
`properties.user-id`, client-set and immutable, which intermediaries "MAY" authenticate
[amqp10 §12/P14]. No sheet reports per-message authorization anywhere: granularity is per
connection (ZeroMQ's ZAP domain), per entity name (RabbitMQ), per subject pattern (NATS), per
topic (MQTT), or per resource scope (Kafka ACLs).

**D7 — "Exactly once" always means something narrower than it sounds, and every sheet says so
in its own words.** MQTT: exactly once is QoS 2 and "strictly per hop", broken end-to-end by
the legal downgrade rule [mqtt5 §12/P9]. AMQP 1.0: a construction of settlement modes plus
link resumption plus durable termini, and "the guarantee is *not* a property of the settlement
modes alone" [amqp10 §6.2]. Kafka: idempotence is "exactly-once *log append per producer
sequence*, not atomic consume-process-produce", and transactions cover "between Kafka topics
only" [kafka §6]. JetStream: the documentation calls publish de-duplication plus double
acknowledgement "exactly once semantics", with the window bounded in time and external side
effects excluded [nats §12/P9]. RabbitMQ refuses the term outright: "retransmission can
duplicate" [rabbitmq-amqp091 §12/P9]. ZeroMQ never claims it and answers with "Idempotency is
not something you take a pill for" [zeromq §12/P9]. NNG states plainly that no deduplication
or exactly-once guarantee exists [nanomsg-nng §12/P9]. The unanimous residue: external side
effects need an application-level idempotency key — stated independently by [zeromq §12/P9],
[rabbitmq-amqp091 §12/P9], [kafka §6], [nats §6], [amqp10 §12/P9], [nanomsg-nng §9].

**D8 — Nobody re-establishes application state on reconnect, and several sheets warn about the
same mistake.** Reconnect restores a transport, not a registration: ZeroMQ says a worker "must
close/reopen after broker death to re-register; ZeroMQ auto-reconnect is insufficient"
[zeromq §12/P6]; RabbitMQ says the protocol re-establishes nothing and publisher sequence
numbers restart [rabbitmq-amqp091 §12/P6]; MQTT says session state is not transferred on
redirection [mqtt5 §12/P6]; NNG says subscriptions and queues are not durable protocol objects
[nanomsg-nng §12/P6]. Only two mechanisms in the whole set genuinely resume application state:
AMQP 1.0 link resumption against a durable terminus [amqp10 §12/P6] and MQTT session
resumption keyed on ClientID [mqtt5 §1] — plus JetStream's durable consumers, which keep the
state server-side so there is nothing to resume [nats §12/P6].

**D9 — Late-joiner state is not a protocol feature.** No answer at all: NNG
[nanomsg-nng §12/P5], NATS core [nats §6], AMQP 1.0 core [amqp10 §12/P5], AMQP 0-9-1
[rabbitmq-amqp091 §12/P5]. A last-value cache: MQTT retained messages [mqtt5 §12/P5] and, as
an application recipe, ZeroMQ's Last Value Caching proxy [zeromq §12/P5]. A log: Kafka
retention and compaction [kafka §12/P5], JetStream deliver policies [nats §12/P5], RabbitMQ
streams [rabbitmq-amqp091 §12/P5]. The split falls exactly between protocols that own storage
and protocols that do not.

**D10 — Overload is answered either by blocking the producer or by dropping, and the choice is
made per pattern rather than per protocol.** Block: ZeroMQ PUSH/PULL/REQ/DEALER/PAIR
[zeromq §12/P4], NNG PUSH and PAIR [nanomsg-nng §12/P4], RabbitMQ prefetch exhaustion
[rabbitmq-amqp091 §12/P12], AMQP 1.0 at zero credit [amqp10 §12/P4], MQTT at zero send quota
[mqtt5 §12/P12], Kafka producer at `buffer.memory` [kafka §12/P12]. Drop: ZeroMQ
PUB/XPUB/XSUB/RADIO/ROUTER [zeromq §12/P4], NNG BUS and SUB [nanomsg-nng §12/P4], RabbitMQ
`drop-head` [rabbitmq-amqp091 §12/P4], MQTT implementation queues [mqtt5 §12/P4]. Disconnect:
NATS core's slow-consumer rule, the only protocol in the set that answers overload by cutting
the peer off [nats §12/P4]. Retain-and-lag: Kafka, which turns the problem into retention
[kafka §12/P4].

**D11 — A body cannot usually be delivered before it is complete.** Whole-message only:
ZeroMQ ("all frames or none") [zeromq §12/P13], NNG ("wholly or not at all")
[nanomsg-nng §12/P13], MQTT (one PUBLISH per message) [mqtt5 §12/P13], NATS (complete byte
count before routing) [nats §12/P13], Kafka (complete record batches) [kafka §12/P13],
RabbitMQ (the wire streams, the API does not) [rabbitmq-amqp091 §12/P13]. AMQP 1.0 is the sole
exception, with partial deliveries observable and a `received(section-number, section-offset)`
state to name the position [amqp10 §12/P13].

**D12 — Shutdown has no drain acknowledgement anywhere.** ZeroMQ's linger certifies transfer
to the network, not receipt [zeromq §12/P17]; RabbitMQ has "no linger, no drain" and requeues
on channel close [rabbitmq-amqp091 §12/P17]; MQTT abandons in-flight messages on the wire
[mqtt5 §12/P17]; NNG specifies no drain at all [nanomsg-nng §12/P17]. The two mechanisms that
come closest are AMQP 1.0's `flow(drain=true)`/`flow(link-credit=0, echo=true)` quiescence
point [amqp10 §9] and NATS Lame Duck Mode's server-side client drain [nats §12/P17].

---

## 3. Where the protocols genuinely disagree

Places where sheets record *opposite* choices, with the reason each sheet gives.

**3.1 Drop versus block on overload.** ZeroMQ's PUB "is used mainly for transient event
distribution where stability of the network (e.g. consistently low memory usage) is more
important than reliability of traffic" and therefore "SHALL silently drop the message if the
queue for a subscriber is full. SHALL NOT block on sending" [zeromq §4.3]; its PUSH does the
opposite and blocks, because the pipeline pattern "will not discard messages unless a node
disconnects unexpectedly" [zeromq §4.4]. AMQP 1.0 refuses the drop option entirely at the
credit layer — the sender blocks, and the sheet stresses "no drop, no disconnect, no error"
[amqp10 §12/P4] — because credit is an absolute delivery-limit the receiver owns
[amqp10 §5.1]. NATS core disagrees with both: it neither blocks the publisher nor drops
selectively, it disconnects the slow consumer after buffering up to `max_pending`
[nats §12/P4]. ZeroMQ's chapter 5 enumerates all four possible policies and adds a fifth —
Suicidal Snail, where the subscriber measures its own lateness and exits, with the stated
rationale "Abort today, and the problem will be fixed. Allow late data to flow downstream, and
the problem may cause wider damage" [zeromq §12/P4].

**3.2 Where the queue lives: broker owns state versus peers own state.** RabbitMQ, Kafka and
JetStream put the backlog in the broker and make it the durable hop
[rabbitmq-amqp091 §12/P4], [kafka §12/P4], [nats §12/P4]. ZeroMQ and NNG put it in per-peer
pipes on both sides and persist nothing, with the stated preference for "simple stateless
message switches over complex/stateful central brokers" [zeromq §12/P11]. AMQP 1.0 refuses to
choose: brokered and brokerless are both native, a broker being "just a container holding
distribution nodes", and Qpid Dispatch demonstrates the brokerless case as a router that
"never assumes ownership of a message" and needs a broker "only when store-and-forward is
genuinely required" [amqp10 §12/P11], [amqp10 §9]. MQTT is at the opposite pole from ZeroMQ:
strictly brokered, strictly asymmetric, and a client "can never listen" [mqtt5 §12/P11].

**3.3 Pull versus push.** Kafka makes fetching pull-based even though production is pushed to
the leader, so "slow consumers accumulate lag in retained partition logs rather than receive
unsolicited records" [kafka §5]. JetStream offers both shapes explicitly: a push consumer with
a delivery subject, or a pull consumer that "waits for `CONSUMER.MSG.NEXT` requests"
[nats §4]. AMQP 1.0 makes push the default but gives the receiver the absolute say through
credit, and turns pull into a pattern: `flow(link-credit=1)` then block is the spec's own
"synchronous get" [amqp10 §5.1]. MQTT has no pull at all — the server pushes and the only
lever is `Receive Maximum` [mqtt5 §12/P12]. ZeroMQ's REQ/REP is push with lockstep, and the
guide's own load-balancing broker reintroduces pull by making workers send an explicit `ready`
message, precisely because "round robin becomes inefficient when tasks do not take
approximately the same time" [zeromq §12/P3].

**3.4 Redelivery counting versus none.** RabbitMQ quorum queues count failed redeliveries in
`x-delivery-count` and enforce a `delivery-limit` (default 20), because a message that always
fails would otherwise be requeued forever [rabbitmq-amqp091 §9]; classic queues have no
equivalent, so consumers must count themselves [rabbitmq-amqp091 §9]. AMQP 1.0 has
`header.delivery-count`, incremented by `rejected` and by `modified{delivery-failed=true}` but
not by `accepted` or `released`, where "if this value is non-zero it can be taken as an
indication that the delivery might be a duplicate" [amqp10 §7]. JetStream bounds attempts with
`max_deliver` and surfaces exhaustion as an advisory [nats §6]. Kafka share groups count
delivery attempts [kafka §7]. MQTT deliberately has no counter and says its nearest flag is
useless for the purpose: a receiver of DUP 1 "cannot assume that it has seen an earlier copy",
the flag is not propagated, and it refers to the packet rather than the message [mqtt5 §6].
NNG has no dedup key exposed to the REP "beyond its routing header" [nanomsg-nng §7], and
ZeroMQ's protocol declines requeueing altogether because it "results in out-of-order messages,
and is not robust against messages lost in transit" [zeromq §12/P1].

**3.5 Explicit versus implicit acknowledgement.** RabbitMQ makes consumer acknowledgement
explicit and calls auto-ack mode "unsafe", noting that without acknowledgements only
at-most-once is guaranteed [rabbitmq-amqp091 §6]. JetStream recommends `AckExplicit` for
at-least-once and offers `AckNone`/`AckAll` as the weaker alternatives [nats §6]. Kafka has no
per-record consumer acknowledgement at all in conventional groups: a committed group offset is
"the durable restart position for a group and partition, not an acknowledgement on each
record", and auto-commit "can commit a fetched batch before processing finishes"
[kafka §2], [kafka §6] — share groups add per-record acknowledgement precisely because the
offset checkpoint does not express it [kafka §6]. MQTT's PUBACK is implicit ownership transfer
that "need not have completed onward delivery first" [mqtt5 §6]. AMQP 1.0 keeps both:
settlement policy "may be fixed for a link, allowing optimized endpoint choices, or decided
per delivery" [amqp10 §6.2].

**3.6 Where a subscription is matched.** ZeroMQ moved filtering to the publisher in 3.x, so
"filtering SHALL happen at the publisher side" and a payload nobody subscribed to never
crosses the network [zeromq §4.3]. NNG, from the same author lineage, does the opposite: "PUB
broadcasts every message to every connected SUB; each SUB filters locally by prefix
subscription, so subscriptions do not reduce link bandwidth" [nanomsg-nng §4]. RabbitMQ
matches in the broker by binding [rabbitmq-amqp091 §4]; MQTT matches in the server by topic
filter [mqtt5 §4.1]; NATS matches by interest in the server and routes between servers only
where interest exists [nats §4]; Kafka does not match at all — a consumer names partitions and
offsets [kafka §4].

**3.7 Whether the protocol is symmetric.** AMQP 1.0: "AMQP does not differentiate between
clients and servers, but only knows communicating peers", either peer may `attach` in either
role [amqp10 §12/P11]. ZeroMQ: peer-to-peer by default, every socket may bind and connect
[zeromq §12/P11]. NNG: either role may dial or listen, or both [nanomsg-nng §12/P11]. MQTT:
strictly asymmetric, no client can listen [mqtt5 §12/P11]. RabbitMQ: brokered always, "clients
are always the initiators" [rabbitmq-amqp091 §12/P11]. Kafka: brokered, with the extra
asymmetry that clients must reach the *leader* of each partition [kafka §12/P11].

**3.8 Whether an oversized message kills the connection or the message.** ZeroMQ disconnects
the sender: "If a peer sends a message larger than ZMQ_MAXMSGSIZE it is disconnected", with no
error command defined for it [zeromq §8]. RabbitMQ splits the two cases: an oversized *frame*
is a connection exception 501, an oversized *message* is simply rejected
[rabbitmq-amqp091 §8]. AMQP 1.0 splits them the same way: `framing-error` closes the
connection, `message-size-exceeded` detaches the link [amqp10 §8]. MQTT closes with DISCONNECT
0x95 when receiving one, but *silently discards* when it cannot forward one, then behaves "as
if it had completed sending" [mqtt5 §12/P13]. NNG simply discards [nanomsg-nng §12/P13].

---

## 4. Responsibility transfer points per protocol

The exact moments at which responsibility for a message moves, and the signal that marks each,
from each sheet's section 6. This is the table a bridge has to line up: two protocols can only
be joined where their transfer points can be made to coincide, or where the bridge itself
becomes a transfer point.

| Protocol | Transfer point(s) | Signal that marks it | What the previous hop may then discard | Sheet ref |
| --- | --- | --- | --- | --- |
| ZeroMQ | (1) Application to local socket queue. (2) Nothing else in the protocol. (3) Application-protocol completion, where one exists | (1) `zmq_send` returning — "0MQ has assumed responsibility for the message". (3) MDP `FINAL`, Titanic's UUID from `titanic.request` and its `titanic.close`, FLP's echoed Client Control Frame | (1) The message buffer, but not the retry obligation: a disconnecting peer "SHALL destroy its double queue and SHALL discard any messages it contains". (3) With Titanic, the request, once `titanic.close` confirms the reply is stored or processed | [zeromq §6] |
| NNG | (1) Application to socket queue/pipe. (2) REQ's reply | (1) Send completion, or readiness of the send descriptor. (2) Receipt of a matching reply, which stops periodic retransmission | (2) The request state; nothing certifies that a remote side effect occurred exactly once | [nanomsg-nng §6] |
| RabbitMQ / AMQP 0-9-1 | (1) Publisher to node and queue leader. (2) Queue to consumer. Explicitly "entirely orthogonal and unaware of each other" | (1) `basic.ack` in confirm mode; `basic.nack` means the broker "refuses responsibility"; `basic.return` means routed nowhere and arrives *before* the ack. (2) `basic.ack` from the consumer; `basic.reject`/`basic.nack` hand it back | (1) The publisher may discard on the ack, whose meaning ranges from "accepted by all the queues" to "a majority wrote and flushed". (2) The queue deletes the message on the consumer ack | [rabbitmq-amqp091 §6], [rabbitmq-amqp091 §6d] |
| AMQP 1.0 | Settlement, in up to four steps: the sender creates an unsettled entry, the receiver reaches a terminal state, the sender settles, the receiver settles | `transfer(settled=…)`, then `disposition(role=receiver, state=accepted/rejected/released/modified)`, then `disposition(role=sender, settled=true)` | Settling *is* forgetting: idempotent, irreversible, one-way. At-most-once settles before sending; at-least-once has the receiver settle first; exactly-once has each wait for the other | [amqp10 §6.1], [amqp10 §6.2] |
| MQTT 5 | One per hop, and publisher-to-broker and broker-to-subscriber are separate hops with independently chosen QoS | QoS 1: PUBACK "having accepted ownership". QoS 2: PUBREC after "all checks for conditions which might result in a forwarding failure", then PUBREL (the sender will never resend) and PUBCOMP (the identifier is released) | On PUBACK/PUBREC the sender discards the message; on PUBREL the receiver may drop duplicate-suppression state. Durability of the ownership transfer is never required by the spec | [mqtt5 §6] |
| NATS core | None. A socket write "only establishes that the client sent bytes toward its connected server" | `+OK` in verbose mode, which acknowledges a well-formed protocol operation and nothing more | Nothing may safely be discarded: there is no publisher delivery acknowledgement and no consumer acknowledgement | [nats §6] |
| NATS JetStream | (1) Publisher to stream, after quorum commit. (2) Stream to consumer | (1) The publish acknowledgement, carrying stream name, stream sequence and duplicate status. (2) `+ACK`; `-NAK` requests redelivery, `+WPI` extends the timer, `+TERM` ends delivery without success | (1) The publisher may discard once the ack arrives; it certifies nothing about consumers. (2) The consumer ack advances consumer state; the double-ack variant waits for the server's ack of the ack | [nats §6] |
| Kafka | (1) Producer to partition leader or ISR. (2) Consumer position to committed offset — a checkpoint, not a per-record ack. (3) Share groups: per-record acquisition | (1) The Produce response under `acks=0`/`1`/`all`. (2) `OffsetCommit`, optionally inside a transaction via `sendOffsetsToTransaction`. (3) acknowledge / release / reject within a bounded acquisition lock | (1) The producer discards on the response, whose meaning depends on `acks` and `min.insync.replicas`. (2) Nothing is deleted by a commit — retention decides; the commit only moves the restart position | [kafka §6], [kafka §2] |
| weida v0, for comparison | One: sender to the peer's **transport**. There is no application-level transfer point in v0 | `Delivery::delivered()` resolving `Ok(())` — the peer's transport holds every byte and the FIN, "explicitly not the application read it". In Req/Rep the reply itself proves more | Nothing may be discarded on the strength of a receipt beyond "the bytes arrived". `Accepted`, `Stored`, `Replicated` and `Processed` belong to a hop that owns the message; since B-233 they have a carrier — a CURSOR stream (kind `6`) reporting absolute byte offsets — and `weida-broker` reports `Accepted` on it, while the brokerless core still produces none of them | [GUARANTEES §3], [GUARANTEES §6], [ARCHITECTURE §1] |

Two structural observations follow from the table and matter for §7.

- **The number of transfer points differs by protocol, not just their meaning.** ZeroMQ, NNG
  and NATS core have one weak point each (the local queue handoff); MQTT, RabbitMQ, JetStream
  and Kafka have two (in and out of a store); AMQP 1.0 has up to four inside a single hop
  [amqp10 §6.1]. A bridge between a one-point protocol and a two-point protocol has to
  manufacture the missing point or refuse the configuration.
- **Only AMQP 1.0 makes forgetting explicit.** Settlement is "idempotent and irreversible" and
  is the only mechanism in the set that lets two ends reconcile who still owns what after a
  failure, through the unsettled-map comparison [amqp10 §6.3]. Everywhere else the in-doubt set
  is reconstructed by convention: RabbitMQ's sequence-number map [rabbitmq-amqp091 §9], MQTT's
  session state [mqtt5 §1], Kafka's producer sequences [kafka §6].

---

## 5. What weida already answers, and how

From the weida documents only. Where a weida document does not state something, this section
says "not stated" rather than inferring it. Everything below describes the **L0 stream core**
and the **L1 patterns**; the L2 broker layer, where the completion vocabulary acquires meaning,
is Phase 6 [GUARANTEES §6], [ARCHITECTURE §1].

- **P1 — loss and retry.** Loss is reported, never repaired: "v0 performs no retries. A failed
  or indeterminate transfer is reported to the application, which decides" [GUARANTEES §6].
  Detection is per operation and typed: `write_all` fails with `ConnectionLost` before the FIN
  (a definite failure), `delivered()` yields `Indeterminate` after the FIN, and a typed refusal
  (`Rejected`, `UnknownEndpoint`, `Unsupported`, `Canceled`) is definite [FAILURE_MODEL §4].
  Safe retry is the application's, with an explicit constraint: retry only if the operation is
  idempotent, because v0 provides no deduplication [FAILURE_MODEL §5]. Durable idempotency
  helpers are named as Phase 4/5 work [FAILURE_MODEL §5].
- **P2 — liveness.** Idle timeout (`RuntimeConfig::idle_timeout`, 30 s default, the smaller of
  the two peers' values governing both) plus keep-alives sent by the **dialling** side only
  (`RuntimeConfig::keep_alive`, 10 s default) [PATTERNS §1.8]. Loss surfaces as
  `Error::ConnectionLost` from the next operation, without the cause: an idle timeout, a peer
  SHUTDOWN and a transport failure are indistinguishable at the API [FAILURE_MODEL §4]. No
  session, no last will, no peer state to preserve: **nothing reconnects**, the application
  calls `connect` again, and a `Subscriber` re-sends its filters on `connect` [PATTERNS §1.8].
- **P3 — capacity spreading.** Round-robin over live peers, one message or one exchange per
  pick, with a peer skipped only once its connection is closed; "a peer that is merely slow
  keeps receiving its share and eventually stalls the pusher through its windows". Spreading
  work by capacity rather than by turn is explicitly deferred: "broker work (L2)"
  [PATTERNS §3].
- **P4 — slow consumer.** Backpressure is `Block`, `Reject` and `Drop` [GUARANTEES §6].
  `Block` is QUIC's own: per-stream and per-connection windows, the concurrent-stream budgets,
  and bounded internal channels. The budget lands on `open`, and a transfer parked in an accept
  queue still holds its stream, so a deeper `endpoint_queue` does not raise it [PATTERNS §1.4].
  Per stream, transfers are isolated; per connection, one slow reader stalls every writer until
  it consumes at least an eighth of the window [PATTERNS §1.3]. `Reject` is
  `read_capped` refusing a payload past its cap with `STOP_SENDING(REJECTED)` before buffering
  it [GUARANTEES §6]. `Drop` is confined to publisher fan-out: a copy that does not fit in
  `subscriber_buffer_bytes` for that subscriber is dropped and counted, and the publisher never
  blocks [PATTERNS §4]. **The end dropped is the arriving copy, not a queued one** — drop-new,
  where conflation is drop-new at depth one *per key*, which is why the two look alike and are
  not ([decisions/0016](../decisions/0016-conflation.md) §2). There is no high-water-mark
  setting; the windows plus `endpoint_queue` do that job "by exerting backpressure rather than
  by discarding" [ARCHITECTURE §6c]. A payload too large for the budget is not dropped at all
  but streamed, one stream per subscriber, with the budget bounding a chunk
  [PATTERNS §4.1].
- **P5 — late joiner.** Nothing is retained: `publish` returns `0` with no subscribers and
  "nothing is queued for a subscriber that does not exist yet" [PATTERNS §4]. No last-value
  cache, snapshot or replay mechanism is stated in any weida document.
- **P6 — failover.** Not stated as a mechanism, and the absence is deliberate: nothing
  reconnects, and the application re-dials with the same or a new address, the dead entry being
  reaped then [PATTERNS §1.8]. `after_the_server_restarts_the_pusher_must_reconnect` is the
  named test [PATTERNS §1.8]. "client reconnects through another broker" is listed out of scope
  until Phase 6 [FAILURE_MODEL §3].
- **P7 — restart survival.** Nothing is persisted; there is no persistence subsystem. The
  scenarios that need one — "producer crashes after local persistence before sending",
  "receiver persists transfer but ACK is lost", "disk becomes full during stream persistence",
  "persisted state becomes corrupt" — are out of scope until Phase 5, and the replication ones
  until Phase 7 [FAILURE_MODEL §3]. No acknowledgement certifies storage: `Stored` "MUST NOT be
  reported for an in-memory buffer" and is reserved for L2 [GUARANTEES §1].
- **P8 — ordering.** `None` [GUARANTEES §6]. QUIC orders bytes within one stream and nothing
  else; a one-way transfer is one stream and each half of an exchange is one stream, so a
  single payload is ordered end to end, and "across streams there is no ordering guarantee of
  any kind" [GUARANTEES §6], [PATTERNS §1.7]. A publisher's per-subscriber writer enqueues
  copies in publication order, but that is "a property of one hop's implementation, not a
  guarantee" [ARCHITECTURE §6a]. The one ordered channel available is a long-lived raw stream
  [PATTERNS §5]. Per-producer ordering "requires a sequence number in the DATA header and
  reassembly on the receiving side; that is a deliberate later protocol addition"
  [ARCHITECTURE §6a].
- **P9 — duplicates.** `None`: "No idempotency ids, no dedup window. Nothing on the wire names
  a transfer — correlation is the stream itself — so a receiver could not deduplicate even if
  it wanted to" [GUARANTEES §6]. Exactly-once delivery "MUST NOT be casually promised";
  exactly-once *effects* are "a property of a composed system, not of a transport"
  [GUARANTEES §3]. v0 creates no duplicates of its own, because it never retries
  [GUARANTEES §6].
- **P10 — request/reply.** One bidirectional stream per exchange: the requester writes on its
  half and reads the reply or an ERROR on the other, and "the stream is the correlation;
  nothing on the wire names an exchange" [PATTERNS §2]. The correlation table, pending-reply
  map, per-transfer identifier and cancellation frame of an earlier design were deleted
  [ARCHITECTURE §1], [ARCHITECTURE §6a]. Routing back through intermediaries is not stated —
  there is exactly one hop in v0 [GUARANTEES §2], and forwarding to third parties with explicit
  identity envelopes is named as broker work for Phase 6 [ARCHITECTURE §6a].
- **P11 — topology.** Brokerless, one direct QUIC connection per peer pair [GUARANTEES §2].
  bind/connect is fixed per pattern in v0: Rep, Pull and Pub bind; Req, Push and Sub connect,
  and either-side flexibility is deferred "until a use case asks for it" [ARCHITECTURE §6c]. A
  Listener is one logical namespace over possibly several Bindings; an Endpoint is addressed by
  an opaque path inside it [ARCHITECTURE §2]. Discovery is not stated.
- **P12 — flow-control credit.** Units are mixed: **bytes** for the windows
  (`stream_receive_window`, `connection_receive_window`, `subscriber_buffer_bytes`),
  **streams** for the concurrent-stream budgets (`max_concurrent_uni_streams`,
  `max_concurrent_bidi_streams`), and **messages** for `endpoint_queue`
  [GUARANTEES §6], [PATTERNS §1.3], [PATTERNS §1.4]. Grantor: the receiving peer, through
  QUIC's transport parameters. Exhaustion: `open` waits — "It does not fail, and `write_all`
  and `finish` are never reached" [PATTERNS §1.4]. There is no application-level credit frame
  of any kind on the wire.
- **P13 — large messages and streaming.** Payloads may remain streams end to end;
  `OutgoingTransfer` implements `AsyncWrite`, `IncomingTransfer` implements `AsyncRead`, and
  `collect(max_bytes)` is "an opt-in convenience with an explicit cap, never an internal step"
  [INVARIANTS]. "A 40-byte transfer and a 40-GB transfer use the same protocol semantics"
  [ARCHITECTURE §2]. So a body is delivered before it is complete, by construction. No protocol
  maximum message size is stated; the caps that exist are `read_capped`'s explicit cap, which
  refuses with `STOP_SENDING(REJECTED)` before buffering, and `subscriber_buffer_bytes` for
  Pub/Sub, where a larger payload is `LimitExceeded` before fan-out
  [GUARANTEES §6], [PATTERNS §4].
- **P14 — identity.** Per connection, proved in the TLS handshake, and surfaced per transfer:
  every inbound transfer carries the sending peer's SHA-256 public-key fingerprint in
  `IncomingMeta::peer` (`None` for an anonymous client), and "it comes from the TLS handshake,
  never from a header, so it can be authorized on but not claimed"
  [GUARANTEES §6], [PATTERNS §1.9]. `Trust` states whom a dialling endpoint accepts — pins,
  anchors, or only what the address names; a binding may require a client identity with
  `ServerTls::require_client`; an address that names a fingerprint overrides the endpoint's
  `Trust` [ARCHITECTURE §2]. Authorization beyond "is this key trusted at all" is the
  application's decision on the fingerprint [GUARANTEES §6]. This is the one dimension where
  weida is *stronger* than every sheet's answer to P14 (D6): the per-transfer identity is
  proved rather than self-asserted.
- **P15 — resource bounds.** "No remote input can cause unbounded memory allocation" is a hard
  invariant with named enforcement: `header_len` is checked against `max_header_bytes` before
  allocation; CBOR skip is iterative with `max_depth = 8`; the QUIC windows and stream budgets
  bound buffered payload and concurrent stream state; `max_connections` bounds accepted
  connections; a peer's subscriptions are bounded by `max_subscriptions` filters per
  connection, each capped at 256 B and dropped wholesale when the connection closes; payload
  queued for one subscriber is bounded by `subscriber_buffer_bytes` [INVARIANTS]. Byzantine
  fault tolerance is out of scope, and a peer that completes the handshake "has proved that it
  holds the key its identity names, and nothing more: … its wire input remains hostile"
  [FAILURE_MODEL §1].
- **P16 — observability.** The transport receipt (`Delivery`), typed errors including the
  first-class `Indeterminate`, `Publisher::dropped` and the count returned by
  `Publisher::publish`, and `Peer::peer_count`
  [GUARANTEES §6], [PATTERNS §4], [PATTERNS §1.8]. `Indeterminate` must be distinguished in
  logs and metrics from both success and failure: "Folding indeterminate outcomes into an error
  counter destroys exactly the information §22 exists to preserve" [FAILURE_MODEL §5]. Two
  blind spots are documented rather than hidden: a Pub/Sub drop is invisible to the subscriber,
  which "cannot distinguish 'nothing was published' from 'a message was dropped for me'"
  [FAILURE_MODEL §4]; and the cause of a connection loss does not survive to the API
  [FAILURE_MODEL §4]. No tracing header exists on the wire.
- **P17 — shutdown.** `finish()` is a commitment: once the FIN is queued the payload arrives
  with no local handle, and "`Runtime::shutdown` is the one thing that cuts a finished transfer
  short" [PATTERNS §1.1]. No linger setting and no drain handshake are stated. Cancellation is
  transport state, not a message: `RESET_STREAM`/`STOP_SENDING`, and `cancel()` "guarantees an
  outcome, not a retraction" — the receiver can never mistake the transfer for a complete one,
  but may still read bytes already buffered [FAILURE_MODEL §4], [PATTERNS §1.5].
- **P18 — transports.** Native QUIC is the reference transport; a Listener may carry several
  Bindings, and the Web adapter exposes the same namespace over WebSocket or WebTransport
  [ARCHITECTURE §2]. A Binding "must be able to carry weida's addressing model: an opaque
  endpoint path inside a namespace, several endpoints per binding", which is why
  legacy-protocol adapters (ZeroMQ, NNG, MQTT, AMQP 0-9-1) are separate crates rather than
  Bindings [ARCHITECTURE §2]. No local, IPC or in-process transport is stated anywhere in the
  documents read.

**Deferred explicitly, with the phase named:** the L2 broker layer and its acknowledgement
vocabulary, Phase 6 [ARCHITECTURE §1], [GUARANTEES §1]; producer-side local acceptance and
consumer-crash scenarios, Phase 4; persistence, disk-full and corruption scenarios, Phase 5;
replication, quorum and shard-epoch scenarios, Phase 7 [FAILURE_MODEL §3]. Also recorded as
deferrals rather than gaps: streaming fan-out, connecting publishers and binding pushers
[PATTERNS §6], and PAIR/BUS/SURVEYOR-RESPONDENT, which are mapped onto L0 but not built
[ARCHITECTURE §6b].

---

## 6. Candidates weida could take from each protocol

Candidates, not decisions. Each names the sheet, the problem it addresses, and the cost the
sheet itself records. Nothing here is a recommendation; §8 lists what still has to be decided.

**From ZeroMQ.**

- Could adopt the **load-balancing broker's explicit readiness signal** ([zeromq §12/P3]) for
  P3, where weida round-robins by turn and defers capacity to L2 [PATTERNS §3]. Cost per
  sheet: a central queue that is "one real weakness", hard to manage and a single point of
  failure, and a restarted queue does not know its workers until they re-announce.
- Could adopt **`PING` with a TTL the sender declares** ([zeromq §1]) for P2, where weida's
  keep-alive is one-directional (dialling side only) and the idle timeout is the smaller of two
  independently configured values [PATTERNS §1.8]. Cost per sheet: PONG storms if pings are
  sent without replies, and false timeouts under congestion — "Heartbeating is difficult".
- Could adopt **`ZMQ_LINGER`-style shutdown bounding** ([zeromq §12/P17]) for P17, where
  `Runtime::shutdown` cuts finished transfers short with no linger [PATTERNS §1.1]. Cost per
  sheet: the default infinite linger means termination "will by default wait forever if there
  are pending connects or sends".
- Could adopt **Last Value Caching as an intermediary** ([zeromq §12/P5]) for P5, where weida
  retains nothing [PATTERNS §4]. Cost per sheet: an intermediary in the path, per-topic
  storage, and a production LVC needs verbose subscription notification because duplicate
  subscriptions are hidden by default.

**From NNG.**

- Could adopt **`RECVMAXSZ` set per endpoint rather than per process** ([nanomsg-nng §11]) for
  P15, which "permits different limits for different trust boundaries", where weida's
  `read_capped` cap is per read and `Limits` are per runtime [GUARANTEES §6]. Cost per sheet:
  zero still means unbounded, and the option must be set before the endpoint starts.
- Could adopt **PAIR v1's hop count (`MAXTTL`, 1-255)** ([nanomsg-nng §4]) for a future L2
  forwarding topology: a forwarder increments the counter and checks its own limit. Cost per
  sheet: each node chooses its own limit so the bound is not global, and BUS's raw exclusion
  "only prevents a message returning immediately to the pipe from which that raw socket
  received it".
- Could adopt **REQ's three retry triggers — timer, peer disconnect, peer becoming available**
  ([nanomsg-nng §4]) for P1, where weida performs no retries at all [GUARANTEES §6]. Cost per
  sheet: "idempotent request effects are required because duplicates are possible", and a newer
  request cannot withdraw work already performed by a replier.

**From RabbitMQ / AMQP 0-9-1.**

- Could adopt **publisher confirms with a sequence-number map and a `multiple` flag**
  ([rabbitmq-amqp091 §9]) for P1 and P16, once an L2 hop exists to own the message
  [GUARANTEES §1]. Cost per sheet: bookkeeping plus "hundreds of milliseconds of ack latency
  for persistent messages under load", and acks can arrive out of order relative to
  publication, so applications must not depend on confirm ordering.
- Could adopt **`basic.return` for "routed nowhere"** ([rabbitmq-amqp091 §6]) for P16: an
  explicit signal that routing was evaluated and produced an empty destination list, arriving
  *before* the confirm. weida's nearest equivalent is `Publisher::publish` returning `0` plus a
  drop the subscriber cannot see [PATTERNS §4]. Cost per sheet: with `mandatory = false` the
  loss is invisible and "silence looks like success"; the code RabbitMQ uses (312) is absent
  from the 0-9-1 constant table.
- Could adopt **`connection.blocked`/`unblocked` as an explicit overload notification**
  ([rabbitmq-amqp091 §5]) for P4, where weida's overload is expressed only as blocked writes
  [PATTERNS §1.3]. Cost per sheet: it is coarse (per connection), a second alarm before
  unblocking produces no second notification, and mixing publishing and consuming on one
  connection defeats the design.
- Could adopt **`x-delivery-count` plus a `delivery-limit`** ([rabbitmq-amqp091 §9]) for P9 and
  a future L2 redelivery path. Cost per sheet: messages are discarded at the limit; "with
  prefetch above 1 a collectively requeued batch can all be discarded at once"; and the counter
  increments on some failure paths but not others — not on `basic.nack`, not on an
  intra-cluster partition, not on consumer timeout.
- Could adopt **`consumer_timeout` for a stuck-but-connected consumer**
  ([rabbitmq-amqp091 §12/P2]) for P2, a case weida's idle timeout cannot detect because a
  connection with traffic is live by definition [PATTERNS §1.8]. Cost per sheet: on expiry the
  channel closes and *all* following deliveries on it, from all its consumers, are requeued;
  the default is 30 minutes and values under 5 minutes are not recommended.

**From AMQP 1.0.**

- Could adopt **credit as an absolute delivery-limit rather than an increment**
  ([amqp10 §5.1]) for P12: because the receiver communicates `delivery-count + link-credit`,
  `flow` frames are idempotent and lost or reordered grants cannot inflate credit. Cost per
  sheet: the sender must decrease `link-credit` by exactly what it advances `delivery-count`
  by, and reducing credit while transfers are in flight leaves the receiver a choice it need
  not announce — handle the excess, or detach with `transfer-limit-exceeded`.
- Could adopt **`drain` and `flow(link-credit=0, echo=true)`** ([amqp10 §9]) for P17 and P4:
  drain "converts 'wait for a message' into 'wait for a definite answer'", and the echoed flow
  marks the point after which no further transfer will come — exactly the quiescence signal
  weida lacks at shutdown [PATTERNS §1.1]. Cost per sheet: one extra round trip; in-flight
  transfers still arrive until the sender processes the new state; answering echo with echo
  loops forever.
- Could adopt **the unsettled-map comparison on resume** ([amqp10 §6.3]) for P1 and P6, the
  only mechanism in any sheet that resolves the in-doubt set after a connection failure — which
  is precisely weida's `Indeterminate` case [FAILURE_MODEL §5]. Cost per sheet: durable
  unsettled state at both termini, application-visible delivery-tag management, `attach` frames
  that may not fit in one frame (hence `incomplete-unsettled` and a mandatory detach/reattach
  cycle), and the sheet's own record that no surveyed broker implements the receiver mode this
  machinery exists to serve [amqp10 §13].
- Could adopt **`flow.available`, the sender advertising its own backlog** ([amqp10 §12/P4])
  for P4 and P16: it tells the receiver how much is waiting, which weida has no field for. Cost
  per sheet: the sender MAY transfer even when `available` is zero, so the receiver must floor
  its own calculation at zero — the value is advisory.
- Could adopt **`received(section-number, section-offset)` as a resumable position inside a
  body** ([amqp10 §12/P13]) for P13, where weida streams bodies but has no way to name how much
  of one arrived, and `cancel()` "guarantees an outcome, not a retraction" [PATTERNS §1.5].
  Cost per sheet: it is meaningful only on a resumed delivery or in an unsettled map, and a
  receiver position preceding the sender's earliest resumable point cannot be completed at all
  — the only option is `aborted=true` plus a fresh delivery.

**From MQTT 5.**

- Could adopt **a receiver-declared `Receive Maximum` in messages, distinct from byte windows**
  ([mqtt5 §12/P12]) for P12, where weida's message-level bound is `endpoint_queue` and does not
  raise the stream budget [PATTERNS §1.4]. Cost per sheet: the quota bounds the in-flight
  window only and the queue behind it is unbounded by the protocol; violation is answered by
  disconnect (0x93), not by an error on the operation.
- Could adopt the **rule that a blocked publish path never stalls other packet types**
  ([mqtt5 §5]) for P4: at zero quota both sides MUST continue to process and answer every other
  packet type, so "acknowledgements, subscriptions and pings never stall behind a blocked
  publish path". weida's per-connection window does the opposite — one slow reader stalls every
  writer on the connection [PATTERNS §1.3]. Cost per sheet: it requires the credit unit to be
  message-typed rather than byte-generic.
- Could adopt **Last Will with a Will Delay Interval** ([mqtt5 §4.5]) for P2, where weida has
  no way to announce a peer's death to third parties. Cost per sheet: detection is at best
  1.5 x Keep Alive; a graceful DISCONNECT discards the Will, so an "offline" state after a
  clean exit must be published by the client itself; and a server MAY defer Will publication
  until after a restart, so a Will can arrive long after the failure [mqtt5 §8].
- Could adopt **an optional human Reason String beside the machine code** ([mqtt5 §1.9]) for
  P16, where weida carries typed codes but no text, and the *reason* for a connection loss does
  not survive to the API at all — an idle timeout, a peer SHUTDOWN and a transport failure are
  indistinguishable [FAILURE_MODEL §4]. Cost per sheet: the string "SHOULD NOT be parsed" and
  is the first thing suppressed when it would exceed the peer's maximum packet size —
  "diagnostics are the first casualty of a small limit".
- Could adopt **`Maximum Packet Size` declared per connection by each side** ([mqtt5 §12/P13])
  for P13 and P15, where weida's caps are local (`read_capped`, `subscriber_buffer_bytes`) and
  not announced to the peer [GUARANTEES §6]. Cost per sheet: a message too large to forward is
  silently discarded and the server behaves "as if it had completed sending" — a loss mode with
  no signal at all.

**From NATS core.**

- Could adopt **subject-token hierarchy with `*` and `>` wildcards** ([nats §4]) for P11, where
  weida endpoint paths are opaque identifiers with no splitting or prefix match, and only
  Pub/Sub *topics* are prefix-matched [INVARIANTS]. Cost per sheet: routing becomes
  interest-based, which is a different addressing model from an opaque path — and weida's
  invariant "endpoint paths are opaque identifiers" is what would have to change first
  [INVARIANTS].
- Could adopt **`INFO` with `connect_urls`, including asynchronous updates** ([nats §12/P11])
  for P6 and P11, where weida has no discovery and no failover mechanism [PATTERNS §1.8]. Cost
  per sheet: a capable client must handle later `INFO` messages outside the initial handshake,
  and reconnect policy remains a client-library concern rather than a wire guarantee.
- Could adopt **`no_responders` as a fast negative answer** ([nats §4]) for P10, where a weida
  request to an unregistered path already gets `UNKNOWN_ENDPOINT` on the reply half
  [FAILURE_MODEL §4] but a registered path with no worker has no equivalent. Cost per sheet: it
  requires headers and an opt-in capability, and it answers "no responders now" rather than
  "nobody will ever respond".
- Could adopt **Lame Duck Mode: notify, stop admitting, drain, then shut down**
  ([nats §12/P17]) for P17, where `Runtime::shutdown` cuts finished transfers short
  [PATTERNS §1.1]. Cost per sheet: clients must reconnect elsewhere, and in-flight non-durable
  delivery is still not preserved by the drain.

**From NATS JetStream.**

- Could adopt **`Nats-Msg-Id` with a bounded duplicate window** ([nats §12/P1]) for P9, where
  weida has no dedup and nothing on the wire names a transfer [GUARANTEES §6] — this is exactly
  `Bounded` in weida's own deduplication vocabulary [GUARANTEES §3]. Cost per sheet: the window
  is bounded in time, so an id reused after it is not suppressed.
- Could adopt **`+WPI` (work-in-progress) to extend an acknowledgement deadline** ([nats §6])
  for a future L2 consumer path: it resets the ack timer while processing continues, which is
  the missing piece in every fixed-timeout redelivery scheme. Cost per sheet: it is meaningful
  only with `ack_wait` and `max_deliver` configured, and `max_deliver` exhaustion surfaces as
  an advisory rather than a successful acknowledgement.
- Could adopt **`max_ack_pending` as a consumer-level pause** ([nats §5]) for P4 and P12:
  delivery pauses at the bound and resumes as acknowledgements advance, which is credit in
  units the application understands. Cost per sheet: it applies only to explicit/all-ack
  consumers, and the documented default of 1,000 messages is a memory commitment.
- Could adopt **the ordered consumer — an ephemeral view that recreates itself on a detected
  gap** ([nats §7]) for P8, where weida's ordering is `None` across streams [GUARANTEES §6].
  Cost per sheet: it trades durable acknowledgement state and load sharing for the gap-repaired
  ordered view.
- Could adopt **`DiscardNew` versus `DiscardOld` as an explicit overload policy** ([nats §11])
  for P4, where weida's one drop site (Pub/Sub fan-out) has a fixed policy [PATTERNS §4]. Cost
  per sheet: `DiscardNew` rejects the write at full limits, which turns a silent drop into a
  producer-visible failure — the opposite of the trade weida made.

**From Kafka.**

- Could adopt **producer ID plus per-partition sequence numbers** ([kafka §6]) for P1 and P9: a
  retry is deduplicated and out-of-order sequence is *detected*
  (`OUT_OF_ORDER_SEQUENCE_NUMBER`), which is more than dedup — it is a gap alarm. Cost per
  sheet: it requires `acks=all`, retries greater than zero and at most five in-flight requests
  per connection, and it "protects only a producer's writes to Kafka, not arbitrary effects
  performed after consuming a record".
- Could adopt **an offset as a durable, comparable position** ([kafka §12/P8]) for P5 and P8,
  where weida has no sequence field at all and therefore cannot express replay, gap detection
  or per-producer order [ARCHITECTURE §6a]. Cost per sheet: offsets are per partition, not
  topic-wide identities; key order holds only under a stable key-to-partition mapping; and
  increasing partitions "does not redistribute existing records".
- Could adopt **`acks` as an explicit per-request durability level** ([kafka §6]) for P7,
  mapping onto weida's reserved `Stored` and `Replicated(n)` [GUARANTEES §1]. Cost per sheet:
  `acks=1` can lose an acknowledged record on leader failover; `acks=all` is subject to
  `min.insync.replicas` and is rejected with insufficient-replica errors below it; and
  `NOT_ENOUGH_REPLICAS_AFTER_APPEND` means the append happened but the condition was not met —
  an ambiguous outcome, which is exactly weida's `Indeterminate` [FAILURE_MODEL §5].
- Could adopt **share groups' per-record acquisition lock with acknowledge/release/reject**
  ([kafka §4]) for P3, where weida's round-robin cannot express capacity and spreading by
  capacity is deferred [PATTERNS §3]. Cost per sheet:
  `group.share.partition.max.record.locks` bounds acquired records per group and partition, and
  exhaustion yields no further records until acknowledgements or lock expiry reduce it.
- Could adopt **retention as the backlog bound** ([kafka §12/P4]) for P4, an alternative to
  both blocking and dropping: the backlog becomes bounded lag in a log. Cost per sheet: records
  past retention cannot be replayed at all, "retention deletion can remove whole old segments
  regardless of whether a group has committed beyond them", and an out-of-range position forces
  an `auto.offset.reset` decision.

---

## 7. Interoperability chains

Three chains, using §4's transfer points to find the weakest guarantee dimension in each. Only
what the sheets support is stated; where a chain needs a weida behaviour that no weida document
describes, that is said explicitly.

### 7.1 RabbitMQ ↔ weida ↔ ZeroMQ

**Weakest link: the acknowledgement.** RabbitMQ has two responsibility transfers, in and out of
a queue, each with a wire signal [rabbitmq-amqp091 §6]. ZeroMQ has one, and it is local:
`zmq_send` returning means "queued on the socket and 0MQ has assumed responsibility"
[zeromq §6], and a disconnecting peer discards its whole per-peer queue [zeromq §12/P2]. weida
in between has exactly one transfer point and it is transport-level [GUARANTEES §3].

Where a bridge must refuse or take responsibility itself:

- **RabbitMQ → weida → ZeroMQ.** The bridge is the consumer of a RabbitMQ queue, so it owes a
  `basic.ack`, and `basic.ack` "certifies only that this consumer took responsibility"
  [rabbitmq-amqp091 §6]. Downstream it has nothing better than a transport receipt, which says
  the bytes arrived and explicitly not that an application read them [GUARANTEES §3], and
  beyond that a ZeroMQ hop with no acknowledgement at all. So the bridge must either
  acknowledge on the strength of a transport receipt — which for a payload inside the peer's
  stream receive window resolves before the peer's application has called `recv`
  [PATTERNS §1.2] — or **store the message before acking**, becoming the durable hop itself.
  There is no third option that preserves at-least-once, because unacked deliveries on a closed
  channel are automatically requeued [rabbitmq-amqp091 §6] and weida performs no retries
  [GUARANTEES §6].
- **ZeroMQ → weida → RabbitMQ.** The reverse direction is easier for delivery and harder for
  overload. The bridge can use publisher confirms upstream of RabbitMQ [rabbitmq-amqp091 §9],
  but it cannot propagate a `basic.nack` back to a ZeroMQ PUSH peer, because ZMTP has no
  acknowledgement frame of any kind [zeromq §6]. A configuration a bridge must refuse: a
  ZeroMQ PUB source feeding a RabbitMQ quorum queue with confirms, because PUB drops silently
  at its HWM and "the publisher is not told" which peer lost what [zeromq §8], so the confirm
  chain would certify only what survived the drop.
- **Overload directions collide.** ZeroMQ PUSH blocks at its HWM and PUB drops [zeromq §12/P4];
  weida blocks everywhere except Pub/Sub fan-out, where it drops per subscriber and counts it
  [PATTERNS §4]; RabbitMQ blocks the whole publishing connection cluster-wide on a resource
  alarm [rabbitmq-amqp091 §12/P4]. A bridge that maps ZeroMQ PUB onto weida Pub/Sub gets a
  matching policy (drop, silent to the subscriber in both); a bridge that maps ZeroMQ PUSH onto
  weida Push/Pull gets matching backpressure; mixing them silently converts one into the other,
  which is exactly what weida's invariant against adapters inventing guarantees forbids
  [INVARIANTS].
- **Ordering.** All three promise per-connection or per-stream order and nothing wider —
  ZeroMQ "between two immediate peers" [zeromq §12/P8], RabbitMQ along one content path
  [rabbitmq-amqp091 §12/P8], weida within one stream [PATTERNS §1.7]. A bridge that fans one
  RabbitMQ queue onto many weida streams loses the queue's order, because weida does not order
  streams relative to each other [PATTERNS §1.7].

### 7.2 MQTT ↔ weida ↔ AMQP 1.0

**Weakest link: duplicate suppression, and the fact that MQTT's strongest mode is per hop.**
MQTT QoS 2 is exactly-once "strictly per hop" and the downgrade rule makes an end-to-end claim
illegal [mqtt5 §12/P9]. AMQP 1.0's exactly-once is a different construction — settlement modes
plus link resumption plus a durable terminus [amqp10 §6.2]. weida between them has
deduplication `None` and cannot deduplicate even in principle, because "nothing on the wire
names a transfer" [GUARANTEES §6].

Where a bridge must refuse or take responsibility itself:

- **MQTT QoS 2 → weida → AMQP 1.0 `rcv-settle-mode=second`.** This configuration must be
  refused as an end-to-end exactly-once claim. Lining up §4: MQTT's PUBREC transfers ownership
  after the receiver has completed "all checks for conditions which might result in a
  forwarding failure" [mqtt5 §6]; AMQP 1.0's exactly-once requires the *sender* to retain an
  unsettled entry until the receiver reaches a terminal state and then to settle
  [amqp10 §6.1]. A bridge in the middle would have to hold the MQTT delivery unacknowledged
  until the AMQP receiver settled — that is, act as the durable hop — and weida gives it no
  storage and no way to defer an acknowledgement, since there is no application acknowledgement
  on the v0 wire at all [GUARANTEES §3]. Note also that the settlement mode the AMQP side of
  this chain needs is unsupported by every broker the AMQP sheet surveyed [amqp10 §13].
- **The credit units do not line up.** MQTT's quota counts QoS > 0 PUBLISH packets and is
  re-initialised each connection [mqtt5 §12/P12]; AMQP 1.0 runs link credit in messages *and*
  a session window in frames [amqp10 §12/P12]; weida's are byte windows plus stream budgets
  [PATTERNS §1.3], [PATTERNS §1.4]. A bridge cannot translate credit arithmetically; it can
  only bound its own buffer and let each side's mechanism act on that bound. The one behaviour
  it must preserve is MQTT's rule that a blocked publish path never stalls acknowledgements,
  subscriptions or pings [mqtt5 §5] — which weida's *connection*-level window violates by
  construction, since one slow reader stalls every writer on the connection [PATTERNS §1.3].
  The bridge therefore needs one weida connection per MQTT session, or separate connections for
  control and data.
- **Session state has no weida counterpart.** MQTT keeps unacknowledged QoS > 0 messages and
  unanswered PUBRELs in session state for the Session Expiry Interval and resends them on
  reconnect [mqtt5 §12/P17]; AMQP 1.0 keeps an unsettled map on a durable terminus
  [amqp10 §12/P7]. weida has no session, and nothing reconnects [PATTERNS §1.8]. A bridge that
  offers MQTT session resumption owns that state itself; a bridge that does not must refuse
  Clean Start 0 with a non-zero expiry, or document that it maps to Clean Start 1.
- **Streaming is the one dimension where weida and AMQP 1.0 agree and MQTT cannot follow.**
  Both hand a body up before it is complete — AMQP 1.0 via multi-frame transfers and
  `Delivery.partial` [amqp10 §12/P13], weida via `AsyncRead`/`AsyncWrite` [INVARIANTS] — while
  MQTT has one PUBLISH per whole message and no fragmentation [mqtt5 §12/P13]. A bridge from a
  streaming AMQP delivery to MQTT must buffer the whole body and must therefore enforce a size
  cap; MQTT's own answer to an unforwardable oversize is a silent discard that behaves "as if it
  had completed sending" [mqtt5 §12/P13], which an honest bridge should not imitate.
- **Identity.** Both foreign protocols carry at most a self-asserted per-message identity —
  MQTT none at all [mqtt5 §12/P14], AMQP 1.0 the client-set `properties.user-id`
  [amqp10 §12/P14] — while weida carries a *proved* fingerprint per transfer
  [PATTERNS §1.9]. A bridge can populate `user-id` from the proved weida fingerprint in one
  direction, but it must not present a foreign `user-id` as a weida peer identity, since
  `IncomingMeta::peer` "comes from the handshake, never from a header" [GUARANTEES §6].

### 7.3 Kafka ↔ weida ↔ NATS JetStream

**Weakest link: ordering scope, and the absence of any position identifier in weida.** Kafka's
unit of order and of parallelism is the partition, totally ordered by offset [kafka §12/P8];
JetStream's is the stream sequence [nats §12/P8]; weida's is a single QUIC stream, with no
order across streams and no sequence field on the wire
[PATTERNS §1.7], [ARCHITECTURE §6a].

Where a bridge must refuse or take responsibility itself:

- **Kafka → weida → JetStream.** A Kafka consumer group gives one active consumer per partition
  and preserves per-partition order [kafka §12/P3]. To preserve that order across weida, the
  bridge must use one long-lived raw stream per partition, because that is the only ordered
  channel weida has [PATTERNS §5]; mapping a partition onto one transfer per record loses the
  order [PATTERNS §1.7]. Any configuration that fans one partition across several weida streams
  must be refused if downstream order matters.
- **Offsets and commits have no weida representation.** Kafka's committed offset is "a
  checkpoint of the next record position, not proof that every prior record's business effect
  succeeded" [kafka §11], and JetStream's publish ack certifies a committed stream write
  [nats §6]. The bridge sits between them with only a transport receipt [GUARANTEES §3]. It can
  therefore commit its Kafka offsets only after the JetStream publish ack — becoming the party
  that owns the in-doubt window — or it commits early and accepts at-most-once, which Kafka
  itself documents as the consequence of committing before processing [kafka §6].
- **Both ends offer deduplication and weida offers none, which decides where dedup must live.**
  Kafka's idempotent producer keys on producer ID, epoch, partition and sequence [kafka §11];
  JetStream's keys on `Nats-Msg-Id` within a bounded window [nats §12/P1]. A bridge retrying an
  ambiguous JetStream publish must set a stable `Nats-Msg-Id` derived from something durable —
  the Kafka partition and offset are the obvious candidates, since an offset is a durable
  position [kafka §12/P8] — and it must keep the retry inside the stream's duplicate window,
  after which the id is no longer suppressed [nats §12/P9]. weida contributes nothing here: it
  neither duplicates (no retries) nor deduplicates (no ids) [GUARANTEES §6].
- **Backlog policies are three different things.** Kafka bounds the backlog by retention and
  makes overrun an out-of-range fetch [kafka §12/P4]; JetStream bounds it by stream limits with
  `DiscardNew` or `DiscardOld` [nats §11]; weida blocks (except in fan-out)
  [GUARANTEES §6]. A bridge that blocks on the weida hop turns a Kafka lag problem into
  consumer-group liveness risk, because a Kafka member that stops polling within
  `max.poll.interval.ms` is removed and its partitions reassigned [kafka §1]. The bridge must
  therefore bound its own buffer and either pause the Kafka consumer explicitly or accept
  reassignment.
- **Transports do not overlap at all.** Kafka defines only TCP with optional TLS/SASL and no
  QUIC [kafka §12/P18]; JetStream's client transports are TCP, TLS, WebSocket and embedded
  UNIX-domain sockets [nats §12/P18]; weida is QUIC with a Web binding [ARCHITECTURE §2]. Every
  chain in this section is therefore a protocol bridge in a separate process or crate, which is
  precisely why weida's legacy adapters are separate crates and "not bindings"
  [ARCHITECTURE §2].

---

## 8. Open decisions

Questions the evidence leaves open, phrased as decisions to make. Each names what the evidence
constrains and what it does not.

**Every question in this section is now decided**, and each carries a **Closed by** paragraph
naming the note: §8.1 by [0003](../decisions/0003-credit-unit.md), §8.2 by
[0002](../decisions/0002-control-and-bulk-separation.md), §8.3 by
[0004](../decisions/0004-durability-levels.md), §8.4 by
[0001](../decisions/0001-sequence-field.md), §8.5 by
[0005](../decisions/0005-refusal-race.md), §8.6 by [0009](../decisions/0009-drain.md), §8.7 by
[0006](../decisions/0006-guarantee-sets.md), §8.8 by
[0010](../decisions/0010-local-transport.md), §8.9 by
[0007](../decisions/0007-topic-namespace.md). The question text above each closing paragraph is
left exactly as it was written, because a decision is only legible against the question it
answered. A tenth question — a session concept and a stable producer name — was raised in
[0001](../decisions/0001-sequence-field.md) §8 for this section and never added here; it is
answered by [0008](../decisions/0008-session-identity.md) in that file instead, and is not
created retroactively merely to strike it out.

**8.1 What unit does weida's credit have, and is any of it receiver-granted at the application
level?** Today the units are mixed — bytes for the windows, streams for the concurrent-stream
budgets, messages for `endpoint_queue` [GUARANTEES §6], [PATTERNS §1.3], [PATTERNS §1.4] — and
all of it is transport-granted. The evidence: messages are the near-universal choice
(D1), receiver-granted is the majority (D2), and AMQP 1.0 runs both units simultaneously with
no guidance on relative sizing, "which is where brokers differ most" [amqp10 §5.3]. The
decision is whether an L2 hop needs a message-unit credit signal of its own, or whether QUIC's
byte windows plus the stream budget are the whole answer for a stream-native protocol. Related
sub-decision: if credit is ever put on the wire, absolute-limit form (`delivery-count +
link-credit`) makes it idempotent [amqp10 §5.1], which is a choice to make once.

**Closed by [0003](../decisions/0003-credit-unit.md):** L0 carries no application credit at
all — QUIC's byte windows are the byte credit and the concurrent-stream budget is the message
credit, both receiver-granted and both already absolute and idempotent. L2 gets an explicit
per-subscription message credit on the control connection, in the absolute delivery-limit form
this entry named as the choice to make once; bytes stay with the transport.

**8.2 Does the per-connection window's head-of-line coupling need a fix?** weida's connection
window is shared, so one slow reader stalls every writer on the connection [PATTERNS §1.3].
MQTT forbids exactly this for its own control traffic — acknowledgements, subscriptions and
pings "never stall behind a blocked publish path" [mqtt5 §5]. The decision is whether control
and bulk traffic must be separated (separate connections, or a reserved window), and it becomes
load-bearing for any adapter that multiplexes many foreign sessions onto one weida connection
(§7.2).

**Closed by [0002](../decisions/0002-control-and-bulk-separation.md):** separated, as
connections rather than as a reserved window — one control connection per peer and one bulk
connection per dialled path, bound together by the proved fingerprint
([0008](../decisions/0008-session-identity.md) §4.2). The cost is measured: ~1.1 ms of
handshake and under a megabyte of resident state for the pair
([IMPLEMENTATION.md](../IMPLEMENTATION.md) §4, B-011), against head-of-line coupling that is
unbounded.

**Amended by [0011](../decisions/0011-answered-where-it-arrived.md):** the separation that was
built is the per-path one, and it is what the coupling above needed. The per-peer control
connection is parked: 0011 §4.1 decides that a side writes traffic it originates on the
connection the peer's registration arrived on, so a frame naming a path rides that path's
connection — which leaves the control tier with no cargo in v0, HELLO included, since every
connection performs its own. What is left coupled is one case, named rather than hidden: an
endpoint that publishes *and* subscribes on the same path [0011 §4.4].

**8.3 Which acknowledgement states does L2 actually need, and what exactly certifies each?**
`Accepted`, `Stored`, `Replicated(n)` and `Processed` are reserved with precise definitions and
no wire representation [GUARANTEES §1]. The sheets show three distinct certification points in
use: after a disk write (RabbitMQ classic), after a majority wrote *and flushed* (RabbitMQ
quorum), after quorum replication without an explicit `fsync` (RabbitMQ streams)
[rabbitmq-amqp091 §6d] — and Kafka's warning that `acks` "does not make a broker fsync each
record before acknowledgement" [kafka §6]. The decision is which of these `Stored` means, and
whether `Replicated(n)` counts acceptance or durable flush.

**Closed by [0004](../decisions/0004-durability-levels.md):** `Stored` gains a durability level
naming the failure domain it survives — `Stored(Written)` the broker process, `Stored(Flushed)`
power loss on that node — and `Replicated(n, flushed: bool)` counts the replicas that reached at
least `Stored(Written)`, leader included, with the flag certifying that all of them flushed. The
two axes are a partial order, `n` is achieved rather than configured, and a level that cannot be
honoured is refused rather than degraded.

**8.4 Does weida need a sequence field, and at what scope?** Ordering is `None` and
deduplication is `None` because nothing on the wire names a transfer
[GUARANTEES §6]; per-producer ordering is recorded as "a deliberate later protocol addition"
requiring a sequence number in the DATA header and receiver-side reassembly
[ARCHITECTURE §6a]. The same field would make three other things possible at once: Pub/Sub drop
detection, which today "needs a sequence field the wire does not have"
[PATTERNS §4]; bounded deduplication in the shape of `Nats-Msg-Id` [nats §12/P1]; and gap
detection in the shape of Kafka's `OUT_OF_ORDER_SEQUENCE_NUMBER` [kafka §11]. The decision is
the scope — per producer, per key, per stream — and whether it belongs on the L0 wire or only
in L2.

**Closed by [0001](../decisions/0001-sequence-field.md):** two separate L0 DATA keys, not one —
a monotone per-producer sequence scoped to (producer, endpoint or topic) for ordering and gap
detection, and a distinct producer identity for bounded deduplication; `PerProducer` in a detect
and a reassemble mode, `PerKey` closed as L2-only. Pub/Sub drop detection is the requirement
that puts the counter at L0 rather than in L2. The producer key's encoding was left open there
and is settled by [0008](../decisions/0008-session-identity.md) §4.4: absent by default, a raw
32-byte `bstr` otherwise. Both keys are now in [PROTOCOL §6.2], marked spec ahead of code.

**8.5 Should a refusal ever be guaranteed to beat the transport receipt?** Today a one-way
transfer small enough to fit in flight can be acknowledged by the peer's transport before its
application refuses it, so `delivered()` resolves `Ok` for a discarded transfer — truthfully,
"since a receipt says nothing about the application, including that it said no"
[PATTERNS §1.6]. Every foreign protocol with an application acknowledgement avoids this by
construction, because the acknowledgement *is* the application's. The decision is whether to
leave the race documented (the current position) or to make refusal deterministic for small
payloads, which requires an application-level signal the v0 wire deliberately lacks
[GUARANTEES §3].

**Closed by [0005](../decisions/0005-refusal-race.md):** the race stays as documented
behaviour. No application-level signal is added to the L0 wire; a refusal is guaranteed to be
observed only where the payload exceeds the peer's stream receive window or where the pattern
is Req/Rep, and the deterministic counterpart arrives, if ever, as the reserved `Accepted`
state at an L2 broker hop [GUARANTEES §3].

**8.6 Is a quiescence signal needed at shutdown?** `Runtime::shutdown` is "the one thing that
cuts a finished transfer short" and there is no linger and no drain [PATTERNS §1.1]. The
evidence spans the full range: ZeroMQ's `ZMQ_LINGER` with an infinite default that can hang
forever [zeromq §12/P17]; RabbitMQ's requeue-on-channel-close [rabbitmq-amqp091 §12/P17];
AMQP 1.0's `drain`/`echo` quiescence point [amqp10 §9]; NATS Lame Duck Mode [nats §12/P17]; and
D12's finding that none of them is a drain *acknowledgement*. The decision is whether weida
needs a bounded drain, and whether it belongs to the runtime or to L2.

**Closed by [0009](../decisions/0009-drain.md):** yes, and it is the runtime's. `shutdown`
stays abortive and is named as such; a separate `drain(Duration)` stops admission, waits for
transfers already `finish()`ed to reach the peer's *transport* — the only completion signal L0
has — and then closes. The deadline is mandatory and finite: no infinite variant exists,
because ZeroMQ's `-1` default is the failure the catalogue unanimously records. The outcome is
a local count, never an acknowledgement, since D12 says nobody has one; and L2's queue drain is
a different operation that may not be presented as this one. The unbounded `wait_idle()` in
today's `shutdown` is named as a defect by the same note.

**8.7 What does a bridge do when it cannot honour the source protocol's guarantee?** The
invariant is already written — "Protocol adapters may not silently invent guarantees their
source protocol cannot provide" [INVARIANTS] — and §7 shows the three concrete shapes: refuse
the configuration, store before forwarding, or degrade with an explicit statement. The decision
is which of the three is the default, and where the refusal surfaces: at configuration time,
which is what the guarantee-validation rule requires ("Invalid combinations MUST be rejected…
at configuration time, not silently at runtime" [GUARANTEES §4]), or per message.

**Closed by [0006](../decisions/0006-guarantee-sets.md):** guarantees become a set over the
dimensions of [GUARANTEES §3], with `core` as the default and configured sets only ever
supersets of it, declared in HELLO and intersected per dimension with failure instead of
downgrade. Refusal at configuration time is the default at an adapter edge, where the chain ends
at the foreign protocol's transfer point (§4) — at the adapter's own queue where the protocol has
none; degradation is available only as a named configuration entry, and store-before-forwarding
is refused until the L2 durable hop of 0004 exists.

**8.8 Local transport: does weida get one, and which?** No weida document read here states any
local, IPC or in-process transport [ARCHITECTURE §2]. `ipc.md` §11 lists the candidates with
their trade-offs and makes no choice, and the decisions it leaves open are:

- **Linux default.** `AF_UNIX` `SOCK_STREAM` on a filesystem path is what every surveyed system
  uses [ipc §11], at the cost of needing framing, a stale socket file after a crash forcing
  unlink-then-bind and its race, a 107-byte path budget and a umask-derived default mode.
  `SOCK_SEQPACKET` removes the framing layer but is unavailable on macOS and Windows and
  unsupported by Tokio's own types; the abstract namespace removes stale state entirely but has
  "no access control whatsoever" and is invisible across network namespaces, which breaks it in
  containers [ipc §11].
- **macOS default.** `AF_LOCAL` `SOCK_STREAM`, with framing mandatory (no `SOCK_SEQPACKET`), a
  104-byte path budget that collides with App Group container paths, and `LOCAL_PEERCRED`
  carrying no PID. XPC is the candidate for a supervised service specifically — the only
  surveyed mechanism with launchd-supervised on-demand start, crash restart and documented
  code-signing requirements on the peer — at the cost of being macOS-only and opaque
  [ipc §11].
- **Windows default.** Named pipes in message mode, which give kernel framing, no stale
  endpoint and an impersonation-based peer identity, at the cost of a default security
  descriptor granting Everyone read, a required `PIPE_REJECT_REMOTE_CLIENTS`, two accept-loop
  races to handle, and a completion-based model that "does not unify" with readiness. Windows
  `AF_UNIX` is the single-code-path alternative and costs handle passing, peer credentials,
  stale-state freedom, AppContainer support and Rust `std`/Tokio support [ipc §11].
- **Fallback chains and bulk payloads.** The sheet records a per-platform fallback chain ending
  in loopback TCP with a token file, notes that "an inherited unnamed endpoint is strictly
  better than a named one" for a spawned-child topology, and records that there is no kernel
  zero-copy path for `AF_UNIX`, so passing a memfd, Mach memory entry or file-mapping handle is
  "the only route to zero-copy" [ipc §11]. Each of these is a decision weida has not made.

**Closed by [0010](../decisions/0010-local-transport.md):** yes — inproc first, then `AF_UNIX`
`SOCK_STREAM` on a permission-protected filesystem path (Linux and macOS, path budget checked
after decoding), then named pipes in message mode with an explicit DACL and
`PIPE_REJECT_REMOTE_CLIENTS`. `SOCK_SEQPACKET`, the abstract namespace, XPC as a transport and
Windows `AF_UNIX` are each rejected with their reason. The shape that makes it cheap: **the OS
connection is the stream**, one per transfer, so no framing or multiplexing layer is invented —
at the price of a named `max_local_streams` bound. No TLS locally, so the kernel is the prover
and `IncomingMeta::peer` becomes a sum of key and local principal, which amends 0008 §4.1
without touching its rule. No automatic fallback to loopback TCP, because that would change
who can connect silently; and bulk zero-copy by memfd, Mach entry or file-mapping handle is
named as the only route and deferred to its own decision.

**8.9 Does the endpoint-path namespace stay opaque?** "Endpoint paths are opaque identifiers"
is an invariant, and the topic prefix match is deliberately confined to Pub/Sub topics
[INVARIANTS]. NATS subjects with `*`/`>` [nats §4], MQTT topic filters [mqtt5 §4.1] and
RabbitMQ topic exchanges [rabbitmq-amqp091 §4] all assume hierarchical matching on the
addressing namespace itself. The decision — required before any adapter maps a foreign
hierarchical namespace onto weida endpoints — is whether the invariant is amended (with the
reasoning recorded first, as [INVARIANTS] itself requires) or whether adapters keep their
hierarchy entirely inside their own crate.

**Closed by [0007](../decisions/0007-topic-namespace.md):** the invariant is not amended.
Endpoint paths stay opaque and exactly matched; the hierarchy lives in the Pub/Sub topic
namespace, whose filters become segmented patterns — separator `.`, `*` for exactly one
segment, trailing `#` for zero or more — and a ZeroMQ byte-prefix subscription maps to a
segment boundary as a named loss. The note carries the mapping table for MQTT `+`/`#`,
NATS `*`/`>` and AMQP 0-9-1 `*`/`#`.
