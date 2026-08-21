# Failure model

Failures are modelled explicitly. This document defines the failure vocabulary
(master doc §61-63) and the normative v0 outcome rules that map observable transport events
to the results the application sees.

Related: [GUARANTEES.md](GUARANTEES.md), [PROTOCOL.md](PROTOCOL.md),
[INVARIANTS.md](INVARIANTS.md), [IMPLEMENTATION.md](IMPLEMENTATION.md).

---

## 1. Failure scope

Primary scope, verbatim from master doc §61:

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

Byzantine fault tolerance is **out of scope**. Authenticated transport protects
communication but does not make the cluster a BFT consensus system. A peer that completes
the TLS handshake is trusted to be who its certificate says, and nothing more: its wire
input remains hostile ([INVARIANTS.md](INVARIANTS.md)).

---

## 2. Failure analysis method

For every reliability mode, ask (verbatim from master doc §62):

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

Any new reliability mode MUST answer all eight questions in writing before it is
implemented.

---

## 3. Required failure scenarios

Verbatim from master doc §63, each annotated with its status in the current increment.
Scenarios are covered now only where they are reachable without persistence and without a
broker; the rest are deferred to the phase that introduces the subsystem they exercise
(see [IMPLEMENTATION.md](IMPLEMENTATION.md)).

```text
producer crashes before local acceptance                                [out of scope until Phase 4]
producer crashes after local persistence before sending                 [out of scope until Phase 5]
network disappears during stream                                        [covered in v0]
receiver crashes during stream                                          [covered in v0]
receiver gets entire transfer but crashes before ACK                    [covered in v0]
receiver persists transfer but ACK is lost                              [out of scope until Phase 5]
broker persists but crashes before replication                          [out of scope until Phase 7]
replicated quorum completes but broker dies before upstream ACK         [out of scope until Phase 7]
consumer receives but crashes before processing                         [out of scope until Phase 4]
consumer processes successfully then crashes before processed ACK        [out of scope until Phase 4]
processed ACK reaches broker but broker crashes before storing state    [out of scope until Phase 6]
network partition splits broker cluster                                 [out of scope until Phase 7]
old shard owner returns after epoch changed                             [out of scope until Phase 7]
disk becomes full during stream persistence                             [out of scope until Phase 5]
persisted state becomes corrupt                                         [out of scope until Phase 5]
client reconnects through another broker                                [out of scope until Phase 6]
retry reaches another replica                                           [out of scope until Phase 7]
request side effect succeeds but reply disappears                       [partially covered in v0]
```

Notes on the covered ones:

- **network disappears during stream** — exercised by the hostile-peer test that closes the
  connection after reading half the request; the sender observes `ConnectionLost` while
  still pre-FIN.
- **receiver crashes during stream** — indistinguishable at the wire from the previous case;
  same rule, same outcome.
- **receiver gets entire transfer but crashes before ACK** — exercised by the hostile-peer
  test where a raw server reads the request to FIN, sends nothing, then closes with
  `NO_ERROR`; the sender observes `Indeterminate`. This is the canonical §22 case.
- **request side effect succeeds but reply disappears** — *partially* covered: the
  disappearing-reply half is covered (the requester resolves `Indeterminate`), but there is
  no side-effect durability to reason about in v0, so the scenario cannot be closed until
  persistence exists.

Every guarantee mode must have explicitly documented outcomes. In v0 there are two
acknowledgement modes (`none`, `accepted`), and §4 documents the outcomes of both
exhaustively.

---

## 4. v0 sender outcome rules

Normative. These rules are implemented as pure transition functions in `weida-core` so they
can be tested without networking.

| Event | Local state | Outcome | Rationale |
| --- | --- | --- | --- |
| Connection lost | before local FIN | `Failed(ConnectionLost)` | The receiver discards partial transfers on reset or connection loss ([PROTOCOL.md](PROTOCOL.md) §9.3), so the payload was definitely not delivered. Definite failure, not indeterminate. |
| Connection lost | after local FIN, awaiting ACK or reply | `Indeterminate` | The peer may have read the payload to FIN and handed it to the application, and the ACK or reply may have been lost. The caller cannot know. |
| ERROR frame received | any | `Failed(code)` | The peer explicitly reported refusal or failure. `code` maps to `UnknownEndpoint`, `Rejected`, `Unsupported`, `Internal` or `NoReply`. |
| `STOP_SENDING` observed | any | `Failed(Rejected)`, `Failed(UnknownEndpoint)` or `Failed(Canceled)`, selected by the QUIC application error code | The receiver refused the inbound payload. The refusal is explicit, so the outcome is definite. |
| ACK received | awaiting ACK | `Acked(Accepted)` | The peer read the complete payload to FIN and handed it to the application ([GUARANTEES.md](GUARANTEES.md) §1). |
| Local FIN completes | `ack_mode = none` | `SentBestEffort` | No acknowledgement was requested; the only assertion is that the local side finished writing. It is explicitly not a delivery claim. |

### Precedence

An ERROR frame observed before an ACK has been delivered to the application **wins**: the
transfer resolves as `Failed(code)`, not as `Acked`. An implementation MUST NOT report
success and then report the error afterwards.

### Receiver-side rules

| Event | Receiver action |
| --- | --- |
| Peer `RESET_STREAM` before FIN | Discard all partial state for that transfer. Send **no** ACK and **no** ERROR frame. Surface a `Canceled` error to any application read in progress. |
| Receiver refuses inbound payload | `STOP_SENDING` with the appropriate application error code: `UNKNOWN_ENDPOINT` for an unregistered path, `REJECTED` when the application drops the body before FIN or a reserved header value was requested, `CANCELED` when a local cancellation caused the refusal. |
| Application drops a request without opening a reply | Send ERROR `{re, NO_REPLY}` so the requester does not hang until the idle timeout. |

### Connection teardown

When a connection fails, every pending table entry on that connection MUST be resolved by
these rules — pending ACK waiters, pending reply waiters and active inbound request cancel
signals alike. No pending operation may be left to time out silently.

---

## 5. What `Indeterminate` means for the application

`Indeterminate` is not a synonym for failure and MUST NOT be treated as one.

It means: **the operation may or may not have taken effect.** The peer may have delivered
the payload to its application and acted on it, or may have died before doing so. No
observation available to the local side distinguishes these.

Consequences for the application:

- Retry only if the operation is **idempotent**. v0 provides no deduplication
  ([GUARANTEES.md](GUARANTEES.md) §6), so a retry of a non-idempotent operation can produce
  a second effect.
- Do not report success to a user. Do not report definite failure either.
- If the operation is not idempotent and the outcome matters, reconcile out of band — query
  the peer for the effect, or defer to an application-level idempotency key. The framework
  will grow durable idempotency helpers in Phase 4/5; until then reconciliation is the
  application's responsibility.
- Distinguish it in logs and metrics from both success and failure. Folding indeterminate
  outcomes into an error counter destroys exactly the information §22 exists to preserve.
