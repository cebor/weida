# Guarantees

This document defines the guarantee vocabulary. It is normative for what a weida
acknowledgement is permitted to mean. It derives from master doc §18-22.

Related: [PROTOCOL.md](PROTOCOL.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md), [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Reliability as transfer of responsibility

Reliability is formulated around **responsibility transfer**, not around the word
"reliable".

An ACK means:

> The next hop has reached a precisely defined state at which the previous hop is allowed to
> release some or all responsibility.

There is deliberately no vague universal `Committed` state. Each completion state has exact
semantics:

### Accepted

The next hop has accepted responsibility in memory according to the selected policy.

### Stored

The next hop has persisted sufficient state to survive the documented failure domain.

### Replicated(n)

The next hop guarantees that configured replication criteria have been met.

### Processed

The next hop's application-level consumer has explicitly reported successful processing.

An implementation MUST NOT report one of these states unless the exact condition above
holds. In particular, `Accepted` MUST NOT be reported before the payload has actually been
handed to the application, and `Stored` MUST NOT be reported for an in-memory buffer.

These four states describe a responsibility transfer **to a broker hop**. They are reserved
for the **L2 broker layer** (Phase 6) and carry no v0 wire representation: with no broker in
the topology there is nobody to transfer responsibility *to*, and an acknowledgement that
only says "the bytes are in the peer's RAM" is a claim QUIC already makes for free (§3).

---

## 2. Guarantees are hop-local

This is a hard invariant.

> All delivery and completion guarantees refer to the immediate next hop.

Consider:

```text
Producer → Broker A → Broker B → Consumer
```

If Broker A returns `Stored`, this means Broker A has met the documented storage guarantee.
It does **not** inherently mean:

```text
Consumer eventually processed the message.
```

Long end-to-end guarantees arise only through explicit composition of hop-local guarantees.
An implementation MUST NOT make hidden claims about downstream systems, and MUST NOT
present a hop-local acknowledgement as an end-to-end result (master doc §81 rule 7).

In v0 there is exactly one hop — a direct QUIC connection between requester and
responder — so hop-local and end-to-end coincide. This coincidence is an accident of the
brokerless topology and MUST NOT be relied on by API design or documentation wording.

---

## 3. Independent guarantee dimensions

Guarantees MUST NOT be collapsed into a single enum such as `Reliable`. The dimensions are
independent.

### Delivery

```text
BestEffort
AtMostOnce
AtLeastOnce
```

Exactly-once delivery MUST NOT be casually promised. Exactly-once **effects** may be
achievable with additional mechanisms such as durable idempotency/inbox/outbox cooperation;
that is a property of a composed system, not of a transport.

### Acknowledgement/completion

```text
None
TransportReceipt          (v0 core)
Accepted                  (reserved, L2 broker)
Stored                    (reserved, L2 broker)
Replicated(...)           (reserved, L2 broker)
Processed                 (reserved, L2 broker)
```

The v0 stream core offers exactly one delivery signal, the **transport receipt**.
`OutgoingTransfer::finish` hands back a `Delivery`, and `Delivery::delivered().await`
resolves `Ok(())` when the peer's transport holds every byte of the payload and the FIN.
quinn documents the underlying condition as the local side finishing the stream and the peer
then acknowledging receipt of all stream data *"(although not necessarily the processing of
it)"*. That parenthesis is the whole distinction: a transport receipt says the bytes
arrived, never that an application read them, still less that it acted on them.

`Accepted`, `Stored`, `Replicated` and `Processed` are reserved for the L2 broker layer and
are deliberately absent from the v0 wire ([PROTOCOL.md](PROTOCOL.md) §11). Earlier drafts
carried application ACK frames in the core and they were removed: without a broker in the
topology, an application ACK means "arrived in RAM at the other end", which is precisely
what QUIC already guarantees by retransmitting until the peer acknowledges. It bought
RabbitMQ's vocabulary without RabbitMQ's responsibility transfer — the one thing that makes
the vocabulary worth having (§1). The words return when a hop exists that can own the
message.

The receipt is a correctness signal, not a latency-sensitive one. On an idle loopback
connection `delivered()` resolves in ~26 ms, because the peer delays its acknowledgement up
to QUIC's max ack delay; the same 1 KiB push without the receipt costs ~7.9 µs. This is why
`Pusher::send` finishes the transfer and drops the `Delivery` — zmq pipeline semantics —
while callers who want the receipt use `Pusher::open` plus `finish()` and `delivered()`
themselves. Dropping a `Delivery` is free and observes no outcome at all.

What the receipt implies about the peer's application depends on the payload size, and the
dependency is worth stating precisely. Inside the peer's stream receive window the receipt
resolves before the application has called `recv`, and even while the transfer is still
parked in the peer's accept queue. Beyond the window it cannot resolve until the application
has consumed at least `payload - window` bytes, because QUIC will not accept more than the
window without that. A receipt for a large transfer is therefore evidence of application
progress; a receipt for a small one is not, and no API distinguishes the two cases
([PATTERNS.md](PATTERNS.md) §1.2, `a_receipt_beyond_the_window_implies_the_reader_consumed`).

The same asymmetry cuts the other way for refusals: a one-way transfer that fits in flight may
be acknowledged by the peer's transport before the peer's application refuses it, so
`delivered()` may resolve `Ok(())` for a transfer the application then discarded. That is not
a defect of the receipt but its definition ([PATTERNS.md](PATTERNS.md) §1.6).

### Ordering

```text
None
PerProducer
PerKey
Total
```

The performance implications of each level MUST be documented.

### Deduplication

```text
None
Bounded
Durable
```

### Backpressure behavior

```text
Block
Reject
Drop
Spill
Coalesce
```

These names are provisional; semantics matter more than naming.

---

## 4. Configuration of guarantees

The framework must support the full performance/reliability spectrum. Illustrative
configurations — only the first is reachable in the v0 core, the other two describe the L2
broker layer:

**Very fast**

```text
persistence = none
ACK = none
ordering = none
dedup = none
backpressure = drop/coalesce
```

**Reliable job processing**

```text
outgoing responsibility retained
remote persistence
at-least-once
deduplication
consumer ACK
```

**Strong broker persistence**

```text
replication factor = 3
ACK after N persisted replicas
durable deduplication
consumer processed ACK
```

Two rules govern configuration:

- Invalid combinations MUST be rejected. Validation is explicit and happens at
  configuration time, not silently at runtime.
- A requested guarantee MUST NEVER be silently weakened (master doc §81 rule 6). If a peer
  or a build cannot honour a requested guarantee, the operation MUST fail visibly. The v0
  core has no acknowledgement knob to weaken — it offers the transport receipt or nothing —
  so the rule shows up in routing instead: a stream addressed to an endpoint whose pattern
  cannot serve it is refused with `UNSUPPORTED` rather than quietly treated as something
  the endpoint does understand (see [PROTOCOL.md](PROTOCOL.md) §9).

---

## 5. Unknown outcomes

Distributed systems contain states where the caller cannot know what happened. This MUST be
represented explicitly, as a first-class result rather than as an error or a success.

Example:

```text
replier read the request to FIN and acted on it
reply written
connection disappeared before the requester saw it
```

The truthful result is not necessarily `Failed`. It may be `Indeterminate` /
`UnknownOutcome`. The public API MUST preserve this distinction. Collapsing
`Indeterminate` into either `Ok` or `Failed` is a correctness defect, because it invites the
application to retry a possibly-applied operation or to discard a possibly-applied one.

The v0 rules that produce `Indeterminate`, and what an application may do with it, are
normative in [FAILURE_MODEL.md](FAILURE_MODEL.md).

---

## 6. Implementation status in v0

Everything below describes the **L0 stream core** and the **L1 patterns** built on it. The
L2 broker layer, where the completion states of §1 acquire meaning, is Phase 6.

| Dimension | v0 support | Notes |
| --- | --- | --- |
| Acknowledgement | transport receipt only | `Delivery::delivered()` resolves `Ok(())` when the peer's **transport** holds every byte and the FIN — explicitly not "the application read it" (§3). There is no application acknowledgement anywhere in the v0 core: `Accepted`, `Stored`, `Replicated` and `Processed` are reserved for the L2 broker layer and have no wire representation, not even a reserved code point ([PROTOCOL.md](PROTOCOL.md) §11). |
| Delivery | `BestEffort` only | v0 performs no retries. A failed or indeterminate transfer is reported to the application, which decides. `AtMostOnce` and `AtLeastOnce` require retry and dedup machinery that does not exist yet. |
| Ordering | `None` | QUIC guarantees byte order **within** one stream. A one-way transfer is one stream, and each half of an exchange is one stream, so a single payload is ordered end to end. Across streams there is no ordering guarantee of any kind. `PerProducer`, `PerKey` and `Total` are not implemented. |
| Deduplication | `None` | No idempotency ids, no dedup window. Nothing on the wire names a transfer — correlation is the stream itself — so a receiver could not deduplicate even if it wanted to. |
| Backpressure | `Block`, `Reject`, `Drop` | `Block`: QUIC stream and connection flow control, the concurrent-stream budgets (`max_concurrent_uni_streams`, `max_concurrent_bidi_streams`) and bounded internal channels (`endpoint_queue`, the actor control channel) make senders await capacity; this is what Req/Rep and Push/Pull use. The budget lands on `open`, and a transfer parked in an accept queue still holds its stream, so a deeper queue does not raise it ([PATTERNS.md](PATTERNS.md) §1.4). `Reject`: `IncomingTransfer::read_capped` refuses a payload past its cap with `STOP_SENDING(REJECTED)` and `LimitExceeded` before buffering it, and `Publisher::publish` rejects a payload larger than `subscriber_buffer_bytes` locally. `Drop`: publisher fan-out only — a subscriber past `subscriber_buffer_bytes` loses the message rather than stalling the publisher. `Spill` and `Coalesce` are not implemented. |
| Indeterminate outcomes | implemented | First-class: `Error::Indeterminate` is deliberately excluded from `Error::is_definite_failure()`. See [FAILURE_MODEL.md](FAILURE_MODEL.md). |
| Peer identity | implemented | Every inbound transfer carries the sending peer's proved public-key fingerprint in `IncomingMeta::peer` (`None` for an anonymous client). It comes from the TLS handshake, never from a header, so it can be authorized on but not claimed (master doc §47). Trust is stated per dialling endpoint (`Trust`: pins, anchors, or only what the address names) and optionally required of clients per binding (`ServerTls::require_client`). Authorization beyond "is this key trusted at all" is the application's decision on the fingerprint. |
| Hop-locality | implemented, trivially | Exactly one hop exists in v0 (direct connection). No composition of hops is possible yet. |

### Per pattern

| Pattern | Delivery | Acknowledgement | Ordering |
| --- | --- | --- | --- |
| Req/Rep | `BestEffort` | the reply itself | `None` across exchanges |
| Push/Pull | `BestEffort` | optional transport receipt | `None` |
| Pub/Sub | `BestEffort`, with per-subscriber drop | none | `None` |

Three points deserve emphasis, because each is easy to assume otherwise:

- **Req/Rep needs no receipt.** A reply is written by the peer's application after it read
  the request to FIN and dispatched it, so it proves strictly more than a transport receipt
  ever could. `Requester::request` therefore drops the `Delivery` of the request half and
  waits on the reply; an ERROR frame on the reply half is equally conclusive. The receipt
  remains available to callers who drive the halves themselves with `Requester::open`.
- **Pub/Sub drops are silent to the subscriber.** A subscriber whose byte budget at the
  publisher is exhausted simply does not receive that message; nothing on the wire tells it
  so. The publisher counts the drop locally (`Publisher::dropped`). This is the one place
  where weida answers overload by discarding, and it is confined to fan-out (master doc
  §17).
- **Ordering is `None` for the new patterns, not "usually ordered".** Each message is its
  own stream and QUIC does not order streams relative to each other. A publisher's
  per-subscriber writer enqueues copies in publication order, but that is an implementation
  property of one hop, not a guarantee an application may rely on. Per-producer ordering
  requires a sequence field that v0 does not have.
