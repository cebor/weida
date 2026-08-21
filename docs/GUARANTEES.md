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
Accepted
Stored
Replicated(...)
Processed
```

Semantics per §1.

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
configurations:

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
  or a build cannot honour a requested guarantee, the operation MUST fail visibly. In v0
  this is why a reserved `ack_mode` yields an ERROR frame with code `UNSUPPORTED` rather
  than being downgraded to `accepted` or `none`
  (see [PROTOCOL.md](PROTOCOL.md) §6.2).

---

## 5. Unknown outcomes

Distributed systems contain states where the caller cannot know what happened. This MUST be
represented explicitly, as a first-class result rather than as an error or a success.

Example:

```text
server persisted operation
ACK sent
network disappeared before caller received ACK
```

The truthful result is not necessarily `Failed`. It may be `Indeterminate` /
`UnknownOutcome`. The public API MUST preserve this distinction. Collapsing
`Indeterminate` into either `Ok` or `Failed` is a correctness defect, because it invites the
application to retry a possibly-applied operation or to discard a possibly-applied one.

The v0 rules that produce `Indeterminate`, and what an application may do with it, are
normative in [FAILURE_MODEL.md](FAILURE_MODEL.md).

---

## 6. Implementation status in v0

| Dimension | v0 support | Notes |
| --- | --- | --- |
| Acknowledgement | `None`, `Accepted` | `Accepted` means exactly "the peer read the complete payload to FIN and handed it to the application". `Stored`, `Replicated`, `Processed` exist only as reserved `ack_mode` code points `2`, `3`, `4`; requesting one is answered with an ERROR frame code `UNSUPPORTED` plus `STOP_SENDING(REJECTED)` and is never downgraded. |
| Delivery | `BestEffort` only | v0 performs no retries. A failed or indeterminate transfer is reported to the application, which decides. `AtMostOnce` and `AtLeastOnce` require retry and dedup machinery that does not exist yet. |
| Ordering | `None` | QUIC guarantees byte order **within** one stream, and each transfer is one stream, so a single transfer's payload is ordered. Across transfers there is no ordering guarantee of any kind. `PerProducer`, `PerKey` and `Total` are not implemented. |
| Deduplication | `None` | No idempotency ids, no dedup window. Receiver-side `transfer_id` uniqueness is explicitly not enforced ([PROTOCOL.md](PROTOCOL.md) §6.2). |
| Backpressure | `Block`, `Reject` | `Block`: QUIC stream and connection flow control plus bounded internal channels (`endpoint_queue`, the actor control channel) make senders await capacity. `Reject`: exceeding `max_pending` fails `open()` locally with `LimitExceeded` without touching the connection. `Drop`, `Spill` and `Coalesce` are not implemented. |
| Indeterminate outcomes | implemented | First-class: `Outcome`/`Error` distinguish `Indeterminate` from definite failure. See [FAILURE_MODEL.md](FAILURE_MODEL.md). |
| Hop-locality | implemented, trivially | Exactly one hop exists in v0 (direct connection). No composition of hops is possible yet. |
