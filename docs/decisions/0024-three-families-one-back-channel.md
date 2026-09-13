# 0024: Three pattern families on one surface, and one back channel

Status: provisional
Date: 2026-09-13
Relates to: the owner's restatement of the model; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §6a, §6b, §7;
[0018](0018-minimal-broker.md) §4.3, §4.6; [0023](0023-completion-is-a-cursor.md) §4.2-§4.5;
[PATTERNS.md](../PATTERNS.md) §1.2, §1.11; [PROTOCOL.md](../PROTOCOL.md) §4.1, §6.5;
[GUARANTEES.md](../GUARANTEES.md) §1, §2; [INVARIANTS.md](../INVARIANTS.md).

## 1. The question

[ARCHITECTURE.md](../ARCHITECTURE.md) §1 opens with "weida is three layers plus adapters" and an
ASCII stack: L0 stream core, L1 patterns, L2 broker semantics. The owner named what is wrong with
that as a description of the thing being built: "was ich ein wenig kompliziert wieder zu geben
finde ist die komplizierte **zwiebelung**", and then gave the shape that replaces it — streams as
the base and "ein neues **gesamtset** stream patterns und message patterns und viele davon sind
ueberlappend", with broker patterns as a third set, and: "das ist die weida user api stream,
message, broker **nach freier wahl**".

Two more things came with it, and both are decisions rather than presentation. The back channel:
"async datagrams als back channel… am ende sende ich halt mit jeder nachricht mit, hey, ich
interessiere mich fuer updates ueber write, sync, process, custom-xyz, in cursor form… optional
mit nagle und optional **fin-only**". And the point that makes streaming the headline rather than
a feature: a video upload must arrive whole at the end, but the sender "kann eigentlich zufrieden
sein sobald mir der loadbalancer, der ja der erste hop ist, sein read fin ack ueber standard quic
uebermittelt… und der loadbalancer muss dafuer **nicht die gesamte nachricht materialisieren**".

So: is the layer model the right story, where does the cursor back channel live, and what does the
system actually claim about materialization.

## 2. The evidence, condensed

**The onion is real in one direction only.** The dependency direction is genuine and worth
keeping: `weida-broker`'s "whole dependency list is `weida`", nothing in the core may depend on
it, "and a brokerless" program links none of it [ARCHITECTURE §4]. But the documents already deny
that this makes a stack the user climbs: "The framework provides one messaging model. Brokerless
peer-to-peer operation and brokered operation use the same patterns and semantics… it does not
introduce a second programming model", and §1's own gloss, "**L2 is a layer, not a fork**: it adds
guarantees to the same patterns rather than a second way of writing programs". A reader who is
told "three layers" and then told "but not a stack you climb" has been given work to do that the
design does not require.

**The overlap the owner describes is already visible in the pattern table.** [ARCHITECTURE §6b]
maps the family — REQ/REP, DEALER/ROUTER, PUSH/PULL, PUB/SUB, PAIR, BUS, SURVEYOR/RESPONDENT —
onto weida constructs, and every row is expressed in terms of the *two stream kinds*: "one
exchange", "one-way transfer, round-robin out", "one-way fan-out plus SUBSCRIBE". So a pattern is
not a layer above streams; it is a **vocabulary over the same two primitives**, which is exactly
why §6b could call building one "a decision about API surface… and not about the protocol".

**A message is already defined as a stream, not as a different thing.** [PATTERNS §1.11]:
"**a message is a stream that reached FIN**", so a queue's unit is a completed stream, and "half a
frame is not a frame" [PATTERNS §4.1] is the general rule. Nothing in the message world needs a
representation the stream world lacks.

**QUIC datagrams exist, and their loss policy is the one an absolute cursor wants.** quinn
0.11.11 `Connection::send_datagram` transmits "an unreliable, unordered application datagram…
They may be lost or delivered out of order, and `data` must both fit inside a single QUIC packet
and be smaller than the maximum dictated by the peer", and — the sentence that matters —
"**Previously queued datagrams which are still unsent may be discarded to make space for this
datagram, in order of oldest to newest**". `max_datagram_size()` "Returns `None` if datagrams are
unsupported by the peer or disabled locally… if the peer's limit is large this is guaranteed to be
a little over a kilobyte at minimum", and `datagram_receive_buffer_size(None)` disables incoming
datagrams entirely, with the peer "forbidden to send single datagrams larger than this size".

Read that against [0023 §4.3b]: cursors are **absolute**, so "a later cursor for a level
supersedes every earlier one". Dropping the oldest unsent datagram to make room for the newest is
therefore not a loss at all — it discards exactly the information that the newer datagram makes
redundant. The transport's congestion behaviour and the cursor's algebra agree without either
being designed for the other.

**And the receipt the owner wants to be satisfied by is already the L0 primitive.** `Delivery` is
"the transport receipt — QUIC's own fin-acknowledgement" [ARCHITECTURE §1], and
[PATTERNS §1.2] documents what it implies beyond the flow-control window: for a payload of `p`
bytes against a window of `w`, it cannot resolve until the reader consumed at least `p - w` bytes.
Nothing about it requires the peer to have *kept* the bytes.

## 3. Options considered

| Option | Story told to a user | Named loss |
| --- | --- | --- |
| **A — three families on one surface: stream, message, broker, chosen freely; the cursor is one mechanism across all of them** | "weida has two stream kinds and three vocabularies over them. Pick the vocabulary your problem has. Ask for the cursors you care about." | The dependency direction stops being visible in the headline, so §1 must state it separately (it does, in §4's crate map, and this note keeps a one-line version) |
| B — keep the L0/L1/L2 onion as the primary story | "learn the stack" | Contradicts the project's own "one messaging model, not a second programming model", and misleads twice: it suggests messages are *above* streams (they are the same streams, finished) and that a broker user is further from the metal (a broker is an ordinary weida user) |
| C — two products: a stream API and a message API | "choose a library" | Precisely the fork [ARCHITECTURE §1] forbids, and it would duplicate every pattern |
| D — message patterns only, streams as an implementation detail | ZeroMQ's story | Throws away the differentiator. The video case shows why: a message API forces materialization at some hop, and the whole point is that no hop has to |

## 4. Decision

**Option A**, plus two mechanism decisions that follow from it.

**4.1 The user API is one surface with three vocabularies, chosen freely.** Not a stack to climb:

| Vocabulary | What you get | What it is made of |
| --- | --- | --- |
| **stream** | `Peer`, `Acceptor`, one-way transfers, exchanges, `Delivery`, cancel | QUIC's two stream kinds, nothing invented |
| **message** | the full ZeroMQ/nanomsg family — Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS | the same two stream kinds, with a selection policy and a name |
| **broker** | queues, confirms, subscriptions, redelivery | an ordinary weida process, using the two above plus a store |

The sets **overlap on purpose**: Req/Rep is a stream pattern *and* a message pattern, because a
request is a stream and a completed request is a message. What distinguishes the message
vocabulary is not a layer boundary but the assumption that the payload is **whole** before it is
used — and [PATTERNS §1.11] already says what that means: a message is a stream that reached FIN.

The dependency direction survives as a single sentence rather than a diagram: **`weida` never
depends on the broker, and a brokerless program links none of it.** That is a packaging fact, not
a user's mental model.

**4.2 The cursor back channel is per-message, requested by the sender, and changes no pattern.**
A sender states, with the message, the levels it wants reported — `written`, `synced`,
`processed`, and application stages above 15 ([0023 §4.3a]) — and the reporting side honours
them at a granularity of its own. This is the owner's framing and it is a **feature, not a
semantic**: without it an application that wants progress builds an explicit back-propagation
stream and a correlation scheme by hand; with QUIC it is nearly free. No pattern changes shape
because cursors were added, and a pattern used without them behaves exactly as it does today.

**4.3 Three modes, and `fin-only` is what makes the classic model a configuration.**

| Mode | What is reported | When it is right |
| --- | --- | --- |
| `per_chunk` | a cursor whenever the reporter's own unit advances | progress that a human or a scheduler watches |
| `coalesced { bytes, interval }` | the same, batched — "every *N* bytes or every *T* ms" | the default: cheap, and lossless because cursors are absolute |
| `fin_only` | exactly one cursor, at the end | a message-shaped application that wants a verdict and nothing else |

`fin_only` is the important one politically: the whole-message acknowledgement that RabbitMQ, AMQP
and MQTT all have is **this system's degenerate case**, not a different mechanism. A migrating
application asks for `fin_only` and gets what it had; nothing in the design has to argue that
whole-message verdicts are obsolete.

**4.4 The back channel is a reliable stream, and a transfer that orders cursors is an exchange.**
The owner's requirement decides this: "auf die ack muss man sich trotzdem verlassen koennen,
wenigstens auf das fin" — and a verdict that may be lost is not a verdict. So the terminal cursor
rides a **stream**, and the cheapest stream is the one the transfer already has.

- **An exchange's reply half carries every cursor and the terminal verdict**, reliably and in
  order. It is already the correlation [0023 §4.3], so nothing on the wire names the transfer,
  and the shape already exists in the implementation: the broker's confirm *is* an exchange reply
  carrying the achieved level as DATA key `8` (B-201).
- **A transfer that orders cursors is therefore sent as an exchange**, not as a one-way transfer.
  That is one bidirectional stream instead of one unidirectional stream — the same single stream,
  a direction wider — and it changes **no pattern's semantics**, because Push/Pull's defining
  property is that the *application* does not answer, while a cursor is a report rather than a
  reply. Asking for a reliable answer is asking for a stream that can carry one.
- **A producer that orders nothing keeps its one-way transfer** and gets the transport receipt and
  nothing more, exactly as [0018 §4.6] says.

**Datagrams are rejected, and the reason is worth keeping.** An earlier draft of this note put
progress cursors on QUIC datagrams, and the analysis stands as far as it went: an absolute cursor
can afford loss, and quinn discards the *oldest* unsent datagram, which is the one a newer cursor
makes redundant. But weigh what that bought. A datagram is not on a stream, so it needed the
connection-local stream id anyway — **the addressing cost was identical**. What it saved was
per-cursor overhead; what it cost was the one property the mechanism must have. When the only
advantage is cost and the requirement is reliability, the reliable carrier wins outright.

**The fallback, if per-transfer reply halves ever cost too much.** At very high fan-out — millions
of small messages, each ordering cursors — a reply half per transfer is a reader per in-flight
transfer. The alternative is then a **single long-lived back-channel stream per connection**
carrying cursor frames tagged with the connection-local stream id they report about, which
amortizes stream setup and keeps reliability. It is deliberately *not* the default, because it
reintroduces addressing that [ARCHITECTURE §1] removed on purpose: "the correlation machinery
existed only because replies rode separate streams… machinery whose entire job was to undo a
choice made one layer down". A separate back-channel stream makes that choice again, so it needs a
measurement to justify it (§4.8), not a preference.

Two rules survive from the datagram draft unchanged, because they are about the cursor and not
about its carrier:

1. **Nothing waits on a cursor.** No guarantee is expressed through one, and a peer that receives
   none behaves correctly — it merely learns less. This is what keeps the back channel out of the
   critical path however it travels.
2. **Coalescing is free**, because the cursors are absolute: a reporter may drop every
   intermediate cursor and the receiver still ends at the same offset per level [0023 §4.3b].
   Reliability and batching are therefore not in tension — the stream delivers what is sent, and
   the reporter decides how much that is.

**4.5 Materialization is a per-hop choice, and this is the headline.** The video case states the
property exactly: the uploader is satisfied by the **first hop's** transport receipt, and that hop
— a load balancer — "does not have to materialize the whole message". If every hop in the chain
keeps working in streams, the payload may **never** be materialized anywhere, and the guarantee
each hop gives is still honest, because guarantees are hop-local [GUARANTEES §2],
[0018 §4.3].

So the positioning follows: **weida is first-class messaging, and streaming is the thing to
highlight.** A message system forces a materialization boundary at every hop that wants to
acknowledge; this one does not, because its acknowledgements are cursors over bytes rather than
verdicts about objects. The invariant that payloads "may remain streams end-to-end"
[INVARIANTS] stops being a constraint the implementation must respect and becomes the feature
the project leads with.

**4.6 The synergy, stated because it is the reason the two halves belong together.** A
message-shaped user would normally get one acknowledgement, after FIN, because that is when a
message exists. Because the same mechanism reports cursors, that user gets **progress for free**:
an upload's bar, a queue's drain position, a consumer's stage — with no extra API, no extra stream
and no correlation scheme. That is what makes the back channel worth a frame: it serves the stream
world's necessity and the message world's convenience with one mechanism.

**4.7 What does not change.** No pattern's semantics, no guarantee, no wire vocabulary for
patterns: PAIR, SURVEY and BUS are still API surface over the same two stream kinds (B-236,
B-237, B-238). `Delivery` keeps its exact meaning, transport state stays hidden
[0023 §4.1], and adapter honesty is untouched — a foreign protocol that has one whole-message
ack maps to `fin_only` and says so.

**4.8 Status is `provisional`, and what would change it.** The carrier is the part with a
measurement attached. §4.4's fallback becomes the default if a reply half per transfer proves too
expensive at high fan-out; the numbers to measure are memory and scheduling cost per in-flight
exchange against the same workload sent as one-way transfers, at a fan-out where the reader count
is the dominant term. Until that measurement exists, the default is the stream the transfer
already has.

## 5. Consequences and follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §1** is rewritten around §4.1: three vocabularies on one
  surface, the dependency direction as one sentence, the L0/L1/L2 names kept only where they
  describe crate boundaries.
- **[PATTERNS.md](../PATTERNS.md)** gains the sentence that the families overlap and why, next to
  §1.11's "a message is a stream that reached FIN".
- **[PROTOCOL.md](../PROTOCOL.md)** states that cursors travel only on an exchange's reply half,
  that a transfer ordering cursors is therefore an exchange, and that nothing waits on a cursor.
  No datagram extension is used, so §4's stream-shape table stays as it is.
- **[0023](0023-completion-is-a-cursor.md) §4.3** keeps its rule — a one-way transfer gets no
  cursors — and gains the consequence: ordering cursors makes the transfer an exchange.
- **Backlog.** Three items, links relative to `docs/BACKLOG.md`.

### B-239 — The three ack modes, including `fin_only`
kind: code | size: 60 | status: blocked | needs: [B-233]
acceptance: a sender states per message which levels it wants and in which mode — `per_chunk`, `coalesced { bytes, interval }`, `fin_only` ([0024](decisions/0024-three-families-one-back-channel.md) §4.3) — and the reporting side honours it, with `coalesced` proved lossless: a reporter that coalesces 100 advances into 3 frames leaves the receiver with the same final offset per level as one that reports all 100, because cursors are absolute. `fin_only` produces **exactly one** cursor frame per level and is the mode an adapter with a whole-message ack maps to; a test asserts that `fin_only` plus a `Stored` level is byte-for-byte the classic confirm, so the migration story is real rather than asserted.
note: the mode is configuration, not protocol: the frame of B-233 is unchanged by all three.

### B-240 — Ordering cursors makes a transfer an exchange
kind: code | size: 60 | status: blocked | needs: [B-239]
acceptance: a send that orders cursors opens a **bidirectional** stream and reads its reply half, where a one-way send opens a unidirectional one — decided by the request, not by the pattern ([0024](decisions/0024-three-families-one-back-channel.md) §4.4). Proved on Push/Pull, the pattern where it is surprising: the same `push` call with cursors ordered yields the same delivery semantics, the same round-robin selection and the same refusal behaviour, plus a reliable terminal cursor; with none ordered it is byte-for-byte today's one-way transfer. A test asserts the stream **kind** in both cases, because that is the whole mechanism, and one asserts that a stalled or unread back channel blocks no transfer — nothing waits on a cursor.
note: no datagram extension and no new addressing: the reply half is already the correlation, and the broker's existing confirm (DATA key `8` on a reply half, B-201) is the shape this generalizes.

### B-241 — ARCHITECTURE §1: three vocabularies instead of an onion
kind: spec | size: 30 | status: done | needs: []
acceptance: [ARCHITECTURE.md](ARCHITECTURE.md) §1 no longer opens with "weida is three layers" and a stack diagram, but with the surface a user actually chooses from — stream, message, broker — the overlap between the families and the reason for it, and the dependency direction as a single sentence rather than a picture. The L0/L1/L2 names survive only where they name crate boundaries, which is what they are.
note: done. The onion was never wrong about dependencies and always wrong as a user's mental model, and the document said so itself two screens further down: "L2 is a layer, not a fork".

## 6. What this note does not decide

- **Whether the per-connection back-channel stream of §4.4 is ever built.** It is the named
  fallback with a measurement attached (§4.8), not a plan, and it is the only place a
  connection-local stream id would appear on the wire.
- **Whether a broker ever *requires* cursors.** It prescribes which levels it reports; whether it
  refuses a producer that asks for none is a broker policy question, and queue admission
  [0018 §4.6] does not need one.
- **Backpressure on the back channel.** A reporter that produces cursors faster than the peer
  consumes them is bounded by the datagram send buffer today; whether it should also be bounded by
  a rate is §4.8's measurement.
- **Anything about the stream primitives.** They are unchanged, and that is the point of calling
  them the base.

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §4, §6a, §6b, §7;
[PATTERNS.md](../PATTERNS.md) §1.2, §1.11, §4.1; [PROTOCOL.md](../PROTOCOL.md) §4.1, §6.2, §6.5;
[GUARANTEES.md](../GUARANTEES.md) §1, §2; [INVARIANTS.md](../INVARIANTS.md);
[0018](0018-minimal-broker.md) §4.3, §4.6; [0023](0023-completion-is-a-cursor.md) §4.1-§4.5.

Research sheets: [zeromq.md](../research/zeromq.md) §1, §4 (the pattern family and its socket
types); [nanomsg-nng.md](../research/nanomsg-nng.md) (the SP set: PAIR, REQREP, PUBSUB, PIPELINE, SURVEY,
BUS); [amqp10.md](../research/amqp10.md) §6 (whole-delivery settlement, the `fin_only` analogue).

External, read 2026-09-13: quinn 0.11.11 `src/connection.rs` — `send_datagram` ("unreliable,
unordered… may be lost or delivered out of order", oldest-first discard of unsent datagrams),
`max_datagram_size` (`None` when unsupported or disabled, "a little over a kilobyte at minimum"
when the peer's limit is large); quinn-proto 0.11 `src/config/transport.rs` —
`datagram_receive_buffer_size(None)` disables incoming datagrams, `datagram_send_buffer_size`
drops older datagrams when full. Read while evaluating datagrams as the back channel and
**recorded because the option was rejected**: the properties are real, they are simply not worth a
verdict that may be lost (§4.4).
