# 0029 — A report is relayed, a certificate is not

- **Status:** provisional
- **Date:** 2026-09-15
- **Items:** B-263, and it unblocks B-203
- **Supersedes:** one sentence of [0018](0018-minimal-broker.md) §4.3 and one paragraph of
  [0023](0023-completion-is-a-cursor.md) §4.4
- **Related:** [GUARANTEES.md](../GUARANTEES.md) §1, §2, §4;
  [0023](0023-completion-is-a-cursor.md) §4.1-§4.5;
  [0024](0024-three-families-one-back-channel.md) §4.4a;
  [0004](0004-durability-levels.md) §5; [0006](0006-guarantee-sets.md) §4.4;
  [0009](0009-drain.md) §4.7; [PROTOCOL.md](../PROTOCOL.md) §6.2, §6.7

## 1. The question

Three things were decided separately and one of them is wrong when the other two are read
together.

[0023](0023-completion-is-a-cursor.md) §4.4: "`Processed` … keeps its code point and loses its
end-to-end reading." [0018](0018-minimal-broker.md) §4.3: "The broker **MUST NOT relay** it:
guarantees are hop-local." And [GUARANTEES.md](../GUARANTEES.md) §2, the hard invariant behind
both: "An implementation MUST NOT make hidden claims about downstream systems, and MUST NOT
present a hop-local acknowledgement as an end-to-end result."

Against that, the shape the broker actually has. It exists to extend the patterns of
[PATTERNS.md](../PATTERNS.md) with a **persistent queue**: a producer pushes to a path and a
consumer subscribes to the same path, so producer and consumer are the two ends of *one*
pattern and the queue is its durable middle. Under that reading a producer that orders
`Processed` is not asking the broker to certify a foreign hop; it is asking to be told what the
far end of its own pattern reported, with the queue carrying the message and the report the way
it carries the payload.

Both readings cannot stand. Either the producer cannot learn what a consumer reported at all —
in which case the ordering mechanism [0023](0023-completion-is-a-cursor.md) built is available
only to a direct peer and a queue in the middle destroys it — or the relay exists and
GUARANTEES §2 has to say precisely what it forbids, because it currently forbids more than it
means.

## 2. The evidence, condensed

**What a cursor already is.** [0023](0023-completion-is-a-cursor.md) §4.5 draws the line this
note needs: **whether** levels are reported is the negotiated `acknowledgement` dimension of
[PROTOCOL.md](../PROTOCOL.md) §6.5 — a guarantee, failed at negotiation when a peer cannot meet
it [0006 §4.4] — while a per-message report order is a **request**, and "a cursor is never
load-bearing". The two are different objects: one is what a peer promises, the other is what a
peer says happened. GUARANTEES §2's invariant is about the first. Nothing in it is about
carrying the second.

**What `Processed` is defined as.** [GUARANTEES.md](../GUARANTEES.md) §1: "the next hop's
application-level consumer has explicitly reported successful processing." The definition is
already a statement **about somebody else's application** rather than about the reporting
transport — which is why [0004](0004-durability-levels.md) §5 maps AMQP 1.0's
`disposition(accepted)` onto it. A queue hop cannot mean anything else by it: a queue does not
process messages, it holds them, so `Processed` from a queue can only be the report of the
consumer it delivered to. That is not a reinterpretation; it is the level's own definition read
at the only hop where it is not ambiguous.

**What the broker's own certificate is.** `Accepted` — "the next hop has taken responsibility"
— emitted at admission, on the reply half as DATA key `8` for an exchange (B-201) and as a
cursor for a one-way transfer ([0024](0024-three-families-one-back-channel.md) §4.4a, B-240).
It is the thing the broker is answerable for, and nothing in this note changes it.

**What the wire carries.** A cursor record is a `(level, offset)` pair of QUIC varints, at most
16 bytes, on a stream whose head frame names a `report_id` the **sending side allocated on that
connection** ([PROTOCOL.md](../PROTOCOL.md) §6.7). There is no field naming who reported, and
there is no field naming which hop the offset is measured on.

**What already works.** The level space is open: values below `CursorLevel::APPLICATION_FLOOR`
are weida's ladder, values at or above it are an application's own stages, "carried and ordered,
never interpreted" [0023 §4.4]. A producer orders levels with DATA keys `9`-`11`. A level nobody
reports is **absent** from the final snapshot, which is implemented and tested
(`crates/weida/tests/cursors.rs`). None of that needed a change for this note; all of it is
what the note is about.

## 3. The three answers, with their costs

**(a) No relay, as 0018 §4.3 says today.** A producer learns `Accepted` and nothing more; if it
wants to know a consumer processed its message it builds an application reply. Honest, and it
makes the queue the one place in weida where the completion mechanism stops working: the
producer's order for `Processed` is silently unanswerable, and the reason is not a property of
the message but of the topology. It also pushes every user of a queue into building the
back-channel weida has, by hand, beside it.

**(b) Relay as the broker's own claim.** The broker emits `Processed` and says nothing about
where it came from. Cheapest on the wire and exactly what GUARANTEES §2 forbids: the producer
reads a hop-local level as an end-to-end result with nothing to warn it, and a chain of two
queues would let a claim travel arbitrarily far from what anybody observed.

**(c) Relay as the consumer's report, bounded to one hop.** The broker re-emits the consumer's
`(level, offset)` toward the producer, may not fabricate one, may not relay a report it itself
received as a relay, and keeps `Accepted` as its own certificate. Costs one rule per bullet
below and no wire vocabulary. The claim the producer can then draw is exactly "the consumer of
the queue I sent to reported this", which is what it asked for, and it cannot grow past one
queue because the relay stops there.

A fourth was considered and refused in §5: attribution **on the record**, a field naming the
reporting hop.

## 4. The decision

**4.1 (c). A queue relays its consumer's report; a queue never relays its own certificate.**
The producer's certificate from the broker stays `Accepted` however the consumer answers, and
`Processed` reaching the producer is the consumer's statement travelling, not the broker's
claim. 0018 §4.3's "MUST NOT relay" is amended to the rule it meant: **a hop MUST NOT present
another hop's report as its own guarantee**, which is GUARANTEES §2 unchanged.

**4.2 The levels are the user's, and a name never reaches the wire.** `Application(u64)` at or
above the floor carries any stage an application defines; weida orders and carries it and
interprets nothing. Turning a number into a label is each binding's API problem — `weida.ACCEPTED`
is a module constant in Python, not a header field — and the wire stays varints. An undefined
value *below* the floor remains a protocol violation, because the reserved range is where a
later version of this specification puts its own stages.

**4.3 A relay is a re-emission, and the mechanics follow from the wire.** A queued message is
**two transfers**, so it has two report orders, and the broker is the only party that sees both:

1. The broker reads the producer's order (keys `9`-`11`). Levels it can answer itself — the
   `Stored` axis of [0004](0004-durability-levels.md) — it answers itself. Levels only a
   consumer can answer it **orders on the delivery**, from its own side, at its own report id.
2. When a consumer's record arrives, the broker re-emits the same `(level, offset)` pair on the
   producer-facing cursor stream, under the report id **the producer allocated**. A report id is
   per connection and per allocating side, so the pair is copied and the frame is new.
3. Relay is honest only while the body is **byte-identical across the hop**, because a cursor is
   an absolute offset into the payload. A queue that ever re-encoded a message MUST stop
   relaying offsets rather than renormalise them silently — a renormalised offset is a number
   nobody measured.

**4.4 A broker MUST NOT fabricate a report, and MUST NOT relay a relay.** It emits a relayed
level exactly when it received that level for that message from a consumer. It does not relay a
report that itself arrived as a relay, so a claim can travel at most one queue away from the
consumer that made it — which is what keeps "hidden claims about downstream systems" impossible
rather than merely discouraged. A chain of queues is named in §5 as unsettled.

**4.5 An ordered level nobody reports stays silence.** A producer that orders a level no hop
implements sees it absent from its final snapshot. There is no refusal, no capability check and
no error: it is the rule 0023 §4.5 states from the reporter's side ("more often is always
allowed, less often never") read from the reader's, and it is implemented. **No slice may invent
a validation handshake for it**: a producer that must know in advance what a hop can report is
asking a deployment question, and the answer is the deployment's.

**4.6 No timer, because there are three failure modes and three answers.** For a delivery the
broker has handed out and not seen settled:

| Failure | Answer |
| --- | --- |
| A packet was lost | QUIC's retransmission. weida adds nothing. |
| The consumer's process died | Requeue when its subscription goes, which the connection's end already signals. |
| The consumer holds the delivery and reports nothing | Nothing — and the bound is `max_unsettled`, so it costs that consumer's own slot. |

A visibility timeout as SQS has one would add a fourth answer to a question that has three, and
its cost is duplicates **weida invented**: a slow consumer's message delivered twice because a
number expired. It stays available as an addition and is not in the first slice. The observable
consequence of choosing this is stated where a caller reads it: a queue delivers at least once,
and a repeat is visible as `delivery_attempt` (DATA key `12`, B-203).

**4.7 The vocabulary of the slice that implements this** — settled here so B-203 does not decide
it in code: the state of a delivery that is out is `Unsettled` and its bound is
**`max_unsettled`**, not `max_unacked`, because nothing here is an ack; a repeat carries
**`delivery_attempt`**, a count rather than a flag, because a count names a poison message and
costs the same varint; and the queue drain is **`retire(deadline)`** returning
`Retired { settled, unsettled, requeued }`, because [0009](0009-drain.md) §4.7 forbids
presenting it as the L0 drain and a shared name would be exactly that.

**4.8 No new client type.** A consumer is a `weida::Subscriber` that grants credit, and
settlement is the `Reporter` that the delivery's `IncomingTransfer` already hands out. The
cursor stream is what removes the need for a delivery to be an exchange, so nothing about the
client surface changes.

## 5. Alternatives refused, and why

**Attribution on the record.** A field in the cursor record or its head frame naming the hop
that reported — a fingerprint, or an opaque origin id. It would make §4.1's distinction visible
in bytes rather than in the level's definition, and it was refused for three reasons: a record
is at most 16 bytes and self-delimiting, and an id would roughly double it for a fact
`Processed`'s own definition already carries; the head frame is per report, not per record, so a
mixed report of own and relayed levels would need the id on the record, which is the hot path;
and an attribution field is an invitation to relay chains, which §4.4 forbids on purpose. If a
chain is ever wanted, the field is where that decision starts — not before.

**A restatement at an application level.** The broker reporting, say, `Application(33)` meaning
"a consumer told me it processed this". Keeps `Processed` strictly hop-local and makes the
producer's order for `Processed` unanswerable through a queue — so a producer would have to know
it is talking to a queue, and order a broker-specific number, to learn a standard thing. That is
a worse trade than amending one sentence of 0018.

**Relaying through a chain of queues.** Refused for the first slice by §4.4 rather than left
open in code. Producer → A → B → consumer, with both relaying, gives the producer a `Processed`
whose distance from the observation it cannot know, which is the hidden claim GUARANTEES §2 is
written against.

## 6. What this changes

- **[0018](0018-minimal-broker.md) §4.3**: the sentence "The broker MUST NOT relay it" becomes
  the rule of §4.1 above, with the certificate/report distinction named and this note linked.
- **[0023](0023-completion-is-a-cursor.md) §4.4**: the paragraph that has `Processed` "lose its
  end-to-end reading" is amended — the level keeps its definition and a queue hop relays it
  under §4.4's two prohibitions.
- **[GUARANTEES.md](../GUARANTEES.md) §2**: gains the distinction it needs now that a second hop
  exists — a guarantee is hop-local, a report may be carried, and a hop may never present a
  carried report as its own guarantee.
- **[PROTOCOL.md](../PROTOCOL.md) §6.2**: DATA key `12` is named as `delivery_attempt` and
  reserved for B-203, so the numbering cannot be spent twice.
- **B-203** is unblocked, with §4.6 and §4.7 as its acceptance rather than as choices it makes.

## 7. What this does not decide

**Whether a hop can declare the levels it implements.** Today the answer to "can you report
`Stored`?" is silence, by §4.5. A HELLO key or endpoint metadata advertising a hop's vocabulary
is a real question for the store phase (B-234) and is not this note's: adding it now would be
wire vocabulary in advance of a user.

**Broker-specific persistence stages.** `Stored(Written)` and `Stored(Flushed)` are the ladder's
and suffice for B-203. A store with stages the ladder does not have — "segment sealed",
"indexed" — names them in the application range under §4.2, and which stages exist is the store
slice's question.

**Chains.** See §5. One queue, one relay.

**Anything about the L0 drain.** `retire` is a queue's operation and `Runtime::drain` is the
transport's; §4.7 fixes the names so that neither can be read as the other, and 0009 stays as it
is.
