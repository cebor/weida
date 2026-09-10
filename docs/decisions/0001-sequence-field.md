# 0001: Sequence field — scope and layer

Status: draft, awaiting decision
Date: 2026-09-10
Relates to: SYNTHESIS §8.4; P8, P9, P5, P16

## 1. The question

weida's ordering is `None` and its deduplication is `None`, and both for the same reason:
"Nothing on the wire names a transfer — correlation is the stream itself — so a receiver could
not deduplicate even if it wanted to" ([GUARANTEES.md](../GUARANTEES.md) §6). Per-producer
ordering is already recorded as a planned protocol change: making it real "requires a sequence
number in the DATA header and reassembly on the receiving side; that is a deliberate later
protocol addition rather than something to imply from the current behaviour"
([ARCHITECTURE.md](../ARCHITECTURE.md) §6a).

SYNTHESIS §8.4 turns that into an open decision with two axes: **what the number is scoped to**
— per producer, per key, per stream — and **which layer carries it**, the L0 wire or L2 only
[SYNTHESIS §8.4]. This note lays out the options with their sourced costs; it makes no
recommendation.

## 2. What the wire has today

The DATA header (kind 1) carries exactly six keys, all optional at the decoder, in ascending
key order ([PROTOCOL.md](../PROTOCOL.md) §6.2): `0` `endpoint` (`tstr`, cap 512 B), `1`
`content_len` (`uint`, advisory), `2` `content_type` (`tstr`, 256 B), `3` `traceparent`
(`tstr`, 128 B), `4` `tracestate` (`tstr`, 512 B), `5` `topic` (`tstr`, 256 B). Nothing else.

The specification states the absence directly: "There is no `transfer_id`, no `role` and no
`correlation_id`. The stream carries all three: its kind says whether a reply is expected, its
direction says which side initiated, and its identity is the correlation. Nothing on the wire
names an exchange" [PROTOCOL §6.2]. The same holds for ERROR, which "needs no reference to what
it answers: the stream is the reference. The `re` key of earlier drafts is gone" [PROTOCOL §6.3].

Three encoding facts bound any addition [PROTOCOL §5]: map keys are CBOR unsigned integers in
strictly ascending order; unknown unsigned-integer keys MUST be skipped by the decoder, which
is what makes a new key forward-compatible with a v0 peer; and "the key space `0..=63` is
reserved for this specification. Extensions MUST use keys `64` and above" — so key `6` is the
next reserved slot. A field that must be *understood* rather than tolerated goes through
HELLO's `required_capabilities`, where "any non-empty `required_capabilities` from the peer
MUST fail negotiation" against a v0 peer [PROTOCOL §2.3].

What the transport contributes is also fixed: QUIC gives ordered bytes within one stream and
nothing across streams [quic-standards §12 item 2], and there are "no message boundaries inside
a stream… No active draft adds them" [quic-standards §12 item 4], the attempts at unreliable
streams and partial reliability having all expired without adoption [quic-standards §8.5]. So
one transfer is one stream, "the within-stream order is the only order there is"
([PATTERNS.md](../PATTERNS.md) §1.7), and any wider scope must be invented by weida.

One identifier does already exist and is *not* in a header: every inbound transfer carries the
sending peer's proved public-key fingerprint in `IncomingMeta::peer`, which "comes from the TLS
handshake, never from a header, so it can be authorized on but not claimed" [GUARANTEES §6],
[PATTERNS §1.9] — a producer name available without a new field, for as long as the connection
lasts (§6).

## 3. What a sequence field would make possible

SYNTHESIS §8.4 names four capabilities that one field would unlock at once:

1. **Per-producer ordering.** The `PerProducer` dimension exists in the vocabulary and is
   unimplemented [GUARANTEES §3], [GUARANTEES §6]; realizing it needs the header number plus
   receiver-side reassembly [ARCHITECTURE §6a]. Real system: JetStream assigns "a monotonically
   advancing stream sequence to each stored message", with ordered consumers giving a
   gap-repaired view [nats §12/P8], [nats §7]; Kafka's offset "is the sequential identity and
   position of a record in that partition" [kafka §3].
2. **Pub/Sub drop detection.** Today "a subscriber cannot detect a drop; making loss observable
   needs a sequence field the wire does not have" [PATTERNS §4], the drop being silent on the
   wire while the publisher counts it locally [GUARANTEES §6]. Real system: ZeroMQ's Clone adds
   server-side sequence numbers so "clients detect holes from congestion or overflow", under
   CHP's rule that "the client MUST discard any KVPUB commands whose sequence numbers are not
   strictly greater than the last KTHXBAI or KVPUB command received" [zeromq §7], [zeromq
   §12/P8].
3. **Bounded deduplication.** The `Bounded` level exists and is unimplemented [GUARANTEES §3].
   Real system: JetStream "can suppress duplicate publish attempts when producers set the same
   `Nats-Msg-Id` header within the stream's duplicate window" [nats §7], [nats §12/P1].
4. **Gap detection.** Real system: Kafka's idempotent producer "sends a broker-assigned producer
   ID and monotonically checked sequence numbers, so a retry is deduplicated and out-of-order
   sequence is detected" [kafka §6], surfaced as `OUT_OF_ORDER_SEQUENCE_NUMBER` [kafka §11] —
   "more than dedup — it is a gap alarm" [SYNTHESIS §6]. D7's caution applies to all of it:
   "exactly once" always means something narrower than it sounds, and external side effects
   still need an application-level idempotency key [SYNTHESIS D7].

The prior-art sheet adds three more, each observed in a QUIC-native system:

5. **Relay caching.** MOQT relays "terminate Transport Sessions in order to have visibility of
   MOQT Object metadata", and caching relays key on the (full track name, group ID, object ID)
   triple [prior-art §4 identity, §4 delivery guarantees]. Zenoh's roadmap is the same shape:
   `SourceInfo{source_id, source_sn}` plus a bounded-history cache [prior-art §1 delivery
   guarantees].
6. **Replay-safe 0-RTT.** Because MOQT objects are immutable and named, "processing the same
   Objects multiple times is idempotent", which makes 0-RTT replay harmless [prior-art §8
   lesson 12], [prior-art §4] — the general rule being that "0-RTT is only worth the analysis if
   the application layer is already idempotent" [prior-art §8 lesson 5], QUIC supplying no
   replay protection for 0-RTT application data [quic-standards §12 item 15].
7. **Resumption by range.** `iroh-blobs` addresses everything by BLAKE3 hash, "so an interrupted
   transfer resumes by requesting missing chunk ranges rather than by protocol state" [prior-art
   §8 lesson 12], [prior-art §2]. Zenoh does it with numbers: a subscriber that detects a gap
   re-queries the range with a `_sn=<start>..<end>` selector [prior-art §1 delivery guarantees].

## 4. Options

### Option A — no field; ordering and dedup stay L2-only

Keep the DATA header as it is, and keep `Ordering = None` / `Deduplication = None` as the
honest statement of what one stream per transfer can promise [GUARANTEES §6]. "An application
that needs message order must carry it in the payload or keep one long-lived stream"
[PATTERNS §1.7], [PATTERNS §5].

- **Problems answered:** none of P8, P9, P5; P16 unchanged.
- **Wire cost:** zero bytes, no new key, no capability negotiation. **Receiver state:** zero —
  no reassembly buffer, no seen-set, no window.
- **Invariants:** untouched — "no remote input can cause unbounded memory allocation" stays
  trivially satisfied ([INVARIANTS.md](../INVARIANTS.md)), and there is no guarantee to disable.
- **Precedent:** NNG, with "no generic ordering guarantee; the contract permits dropped or
  reordered messages" and no deduplication or exactly-once guarantee [nanomsg-nng §12/P8],
  [nanomsg-nng §12/P9]; ZeroMQ, whose ZMTP "defines no message id or sequence number" and
  leaves it to application protocols [zeromq §3].
- **Named loss:** weida can never make loss observable to a subscriber [PATTERNS §4], and a
  bridge can contribute nothing to either end's dedup: "weida contributes nothing here: it
  neither duplicates (no retries) nor deduplicates (no ids)" [SYNTHESIS §7.3].

### Option B — per-producer sequence in the L0 DATA header

Add one monotonically increasing `uint` to the DATA header, scoped to (sending peer, endpoint or
topic), with the producer named by the already-proved connection fingerprint rather than by a new
field [GUARANTEES §6]. The receiver buffers out-of-order arrivals up to a bound and delivers in
sequence — exactly the "sequence number in the DATA header and reassembly on the receiving side"
that [ARCHITECTURE §6a] describes.

- **Problems answered:** P8 at `PerProducer`, P9 as `Bounded` dedup, P16 gap detection, and P5
  only as far as "you know you missed something".
- **Wire cost:** key `6`, one CBOR uint — 1 byte of key plus 1 byte (values 0-23), 2 (to 255),
  3 (to 65535), 5 (to 2^32-1) or 9 for a full `u64`, so 2-10 bytes per DATA header under
  [PROTOCOL §5]'s minimal-integer rule. Forward-compatible with a v0 peer, which skips the
  unknown key [PROTOCOL §5]; making the receiver's reordering *mandatory* instead needs a
  capability code, and requiring one a v0 peer lacks fails negotiation [PROTOCOL §2.3].
- **Receiver state:** a reassembly buffer plus a next-expected counter per (peer, scope), and
  for dedup a seen-window. Both are remote-controlled, so both need explicit caps under "no
  remote input can cause unbounded memory allocation" [INVARIANTS]; the precedent shape is
  JetStream's window, "bounded in time, so an identifier reused after its window is not
  suppressed" [nats §7].
- **Interaction with invariants:** paths stay opaque (the number is not parsed as structure)
  and payload materialization is untouched, the field being header-only while DATA payload
  stays "opaque bytes until FIN, with no internal framing" [INVARIANTS], [PROTOCOL §4]; the
  guarantee is hop-local [INVARIANTS]. Two frictions: reassembly makes one message wait on an
  earlier one, head-of-line blocking above a transport chosen to avoid it [prior-art §8 lesson
  2]; and "disabled guarantees should not participate in the hot path" [INVARIANTS] requires
  that a sender not requesting ordering write no key and such a receiver keep no table.
- **Precedent:** Kafka's idempotent producer — producer ID, epoch and "monotonically checked
  sequence numbers, so a retry is deduplicated and out-of-order sequence is detected"
  [kafka §6]; Zenoh's `SourceInfo{source_id, source_sn}` plus a bounded history cache is the
  pub/sub shape [prior-art §1 delivery guarantees]. ZRE is the strict variant: on a gap it
  "SHALL treat the peer as invalid, and disconnect the peer" [zeromq §7].
- **Named loss:** it can never express order *between* producers, or per key. Kafka's own limit:
  "offsets are not topic-wide identities", and "key-based ordering is obtained only by
  consistently mapping a key to one partition" [kafka §3], [kafka §12/P8]. Nor is it a durable
  position — it names a place in one producer's output, not in a stored log — so it answers P5
  only by detecting a hole, not by filling it, and ZeroMQ's guide refuses even that, because
  holes come from network stress and "asking for more messages makes it worse" [zeromq §12/P5].

### Option C — per-stream-group sequence (MoQ-style group/object addressing)

Name a *group* and a position within it: two header keys, so a transfer is addressed as (peer,
endpoint/topic, group, object). MOQT's subgroup is "a sequence of one or more objects from the
same group in ascending order by Object ID" whose objects "are sent on a single stream whenever
possible" [prior-art §4 message-to-transport mapping] — lesson 1's shape, "a stream per message
is not the design; a stream per ordered group of messages is" [prior-art §8 lesson 1], which
maps onto weida's long-lived raw stream [PATTERNS §5].

- **Problems answered:** P8 within a group, P9 by (group, object) identity, P5 via a group as a
  join point — MOQT: "a Group provides a join point for subscriptions" [prior-art §4].
- **Wire cost:** two keys (`6`, `7`), two CBOR uints: 4-20 bytes per DATA header by the same
  minimal-integer arithmetic [PROTOCOL §5], plus a grouping convention the application must
  choose — MOCHA Chat's is wall-clock minutes, `group_id = floor(ntp_timestamp_seconds / 60)`,
  giving "natural time-based partitioning" and a caching unit [prior-art §4].
- **Receiver state:** per open group, an expected-object counter and whatever the group's
  in-flight window allows; groups are remote-created state and must be capped [INVARIANTS].
  MOQT answers a subscriber that cannot keep up with termination, not buffering — `PUBLISH_DONE`
  with error `TOO_FAR_BEHIND` [prior-art §4 flow control] — and its partial reliability is a
  stream reset with `DELIVERY_TIMEOUT`, the pattern lesson 6 records across projects [prior-art
  §4 delivery guarantees], [prior-art §8 lesson 6].
- **Interaction with invariants:** the group is a second addressing dimension beside the
  endpoint path, so "endpoint paths are opaque identifiers" must be restated — MOQT's key is a
  hierarchical (track namespace, track name, group ID, object ID) [prior-art §4], the same
  pressure SYNTHESIS §8.9 records for foreign hierarchical namespaces. "One data flow maps
  naturally to one transport stream" survives only if a group maps to a stream, which MOQT binds
  both ways: "Objects from two subgroups MUST NOT be sent on the same stream, and Objects from
  the same Subgroup MUST NOT be sent on different streams" [prior-art §4].
- **Precedent:** MOQT and moq-lite (Session > Broadcast > Track > Group > Frame, datagrams
  "deduplicated by group sequence") [prior-art §4]; EMQX in weaker form, where the client groups
  topics onto streams and "if there are two topics whose data is correlated and ordering is
  crucial, they should be mapped to the same stream" [prior-art §3 message-to-transport mapping].
- **Named loss:** no order across groups, and a gap means nothing on its own — "a gap in the
  observed Object IDs does not by itself convey any information about the skipped Objects"
  [prior-art §4]. Its dedup is bought with immutability, which weida would inherit: an object's
  "contents MUST NOT change over time", and a duplicate object with a different payload,
  priority or subgroup "MUST treat the track as Malformed" [prior-art §4]. Mutation becomes a
  new object, as MOCHA Chat's `replaces` does [prior-art §4].

### Option D — content-addressed identity (hash) instead of a sequence

Carry a digest of the payload — the iroh-blobs shape, where everything is "addressed by BLAKE3
hash and verifies incrementally, so an interrupted transfer resumes by requesting missing chunk
ranges rather than by protocol state" [prior-art §8 lesson 12], [prior-art §2].

- **Problems answered:** P9 (identity-based dedup), P5 (resumption by range) and cache
  validity; not P8.
- **Wire cost:** one key plus a byte string: 1 + 2 + 32 = 35 bytes for a 256-bit digest, against
  the header cap of `max_header_bytes` default 16384 [PROTOCOL §3.1].
- **Receiver state:** a seen-digest set bounded by count or time like any dedup window
  [nats §7], plus verification state at 1024-byte BLAKE3 chunk granularity [prior-art §2].
- **Interaction with invariants:** this option collides head-on with "core transport does not
  require payload materialization" [INVARIANTS]. A digest of the whole payload cannot be
  written into a header that precedes it without buffering the payload first, while weida's
  `OutgoingTransfer` is an `AsyncWrite` whose payload is "opaque bytes until FIN, with no
  internal framing" [INVARIANTS], [PROTOCOL §4]. A verified-streaming tree is the way out, and
  it is a payload framing — exactly what the invariant forbids the core from requiring
  [prior-art §2]. weida and AMQP 1.0 are also the only two systems that hand a body up before
  it is complete [SYNTHESIS D11].
- **Precedent:** `iroh-blobs`; and `iroh-gossip`, retaining message ids for 90 s against a 30 s
  cache retention "to not accidentally receive messages multiple times" [prior-art §2].
- **Named loss:** it cannot express order at all, and cannot distinguish two legitimately
  identical messages: "identical records may legitimately occupy distinct offsets, so content
  is not Kafka's duplicate identity" [kafka §3]. Nor can it separate a *retry* from a *repeat*,
  which the JetStream and Kafka producer-scoped ids do [nats §7], [kafka §6].

### Option E — L2-only sequence with a reserved L0 key

Leave L0 semantically unchanged, but reserve the key number now — the way `Accepted`, `Stored`,
`Replicated(n)` and `Processed` are reserved with precise definitions and no wire representation
[GUARANTEES §1], [GUARANTEES §3]. The broker layer of Phase 6 assigns and interprets the number;
L0 carries it as an opaque passthrough, or not at all until then.

- **Problems answered:** P8, P9 and P5 at the broker hop only, once Phase 6 exists
  [ARCHITECTURE §1 (L2)]; nothing at an L0 hop.
- **Wire cost:** zero today; later the same 2-10 bytes as Option B, but reserved out of the
  `0..=63` specification space now rather than negotiated later [PROTOCOL §5]. **Receiver
  state:** zero in the core; whatever the broker's store needs at L2.
- **Interaction with invariants:** it is the position the layering argument already produced —
  earlier drafts "mixed RabbitMQ semantics into the socket layer… and got less"
  [ARCHITECTURE §1] — and it keeps "all guarantees are defined against the immediate next hop"
  clean, because a broker hop can own the message [INVARIANTS], [GUARANTEES §1]. The risk is the
  adapter invariant: adapters "may not silently invent guarantees their source protocol cannot
  provide" [INVARIANTS], yet a Kafka-to-JetStream adapter has nothing to carry an id in, so it
  must own dedup or refuse [SYNTHESIS §7.3].
- **Precedent:** AMQP 1.0's split between layers — `delivery-id` is "a session-scoped 32-bit
  RFC-1982 serial number, a wire shorthand for the tag" while `properties.message-id` is
  "application-level, globally unique by convention" and only "a broker MAY discard a duplicate"
  [amqp10 §3], [amqp10 §12/P9]. RabbitMQ likewise keeps publisher deduplication out of the core:
  a Stream-protocol feature on producer name plus a strictly increasing publishing ID, opt-in
  [rabbitmq-amqp091 §12/P9].
- **Named loss:** a brokerless deployment gets nothing, which is every deployment weida has
  today — v0 is brokerless with exactly one hop [GUARANTEES §2] — so Pub/Sub drop detection, an
  L0-hop problem [PATTERNS §4], stays unsolved. A reserved key that no code writes is also a
  claim nothing enforces: the golden vectors of [PROTOCOL §8] cannot pin a nonexistent field.

## 5. Consequences per option

| Option | P8 ordering scope gained | P9 dedup gained | P5 late-joiner/replay gained | Wire bytes | Receiver state | Breaks which invariant | Precedent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A — no field | none; stays per stream [PATTERNS §1.7] | none [GUARANTEES §6] | none [SYNTHESIS §5 P5] | 0 | none | none | NNG [nanomsg-nng §12/P8]; ZeroMQ ZMTP [zeromq §3] |
| B — per-producer seq | `PerProducer` per (peer, endpoint/topic) [ARCHITECTURE §6a] | `Bounded`, window-scoped [nats §7] | gap *detection* only, no refill [zeromq §12/P5] | 2-10 B (key + CBOR uint) [PROTOCOL §5] | reassembly buffer + next-expected + seen-window per (peer, scope) | none if capped; pressures "no remote input can cause unbounded memory allocation" and "disabled guarantees should not participate in the hot path" [INVARIANTS] | Kafka idempotent producer [kafka §6]; Zenoh `SourceInfo` [prior-art §1] |
| C — group/object | total within a group, none across groups [prior-art §4] | by (group, object) identity, needs immutability [prior-art §4] | group as join point + relay cache [prior-art §4] | 4-20 B (two keys + two uints) [PROTOCOL §5] | per-group expected-object counter; group table must be capped | pressures "endpoint paths are opaque identifiers" (second addressing dimension) and "one data flow maps naturally to one transport stream" [INVARIANTS] | MOQT / moq-lite [prior-art §4]; EMQX stream grouping [prior-art §3] |
| D — content hash | none | by digest; cannot separate legitimate repeats [kafka §3] | resumption by range [prior-art §2] | ~35 B (key + 32-byte digest) | seen-digest set + verification state | "core transport does not require payload materialization" [INVARIANTS], [PROTOCOL §4] | `iroh-blobs` [prior-art §2]; `iroh-gossip` id retention [prior-art §2] |
| E — L2-only, key reserved | none at L0; broker-scoped later [GUARANTEES §1] | none at L0; broker-scoped later | none at L0; broker-scoped later | 0 now; 2-10 B later [PROTOCOL §5] | none at L0 | none; leaves the adapter-honesty invariant load-bearing [INVARIANTS], [SYNTHESIS §7.3] | AMQP 1.0 `delivery-id` vs `message-id` [amqp10 §3]; RabbitMQ Stream dedup [rabbitmq-amqp091 §12/P9] |

## 6. What the evidence does not settle

- **Header cost at high message rates on QUIC.** Unmeasured: for long-lived bidirectional
  messaging "there is no published measurement, academic or vendor" [quic-standards §11.9]. Only
  adjacent facts exist — an application cannot control how writes become frames and packets
  [quic-standards §3.8], and per-packet CPU cost dominates the fast-link results
  [quic-standards §11.2].
- **Reassembly-buffer cost under reordering across streams.** Unmeasured anywhere. What exists
  is the shape: a receiver "must buffer out-of-order data up to the flow control limit" *within*
  a stream [quic-standards §2.2] while nothing is ordered across streams [quic-standards §12
  item 2], plus the warning that an ordering layer above the transport reintroduces head-of-line
  blocking, which "the application sees as a liveness failure, not as slowness" [prior-art §8
  lesson 2].
- **Whether a per-producer sequence survives a reconnect.** "weida has no session, and nothing
  reconnects" [SYNTHESIS §7.2], [PATTERNS §1.8], and the sheets agree this is what breaks:
  reconnect restores a transport, not a registration; RabbitMQ's publisher sequence numbers
  restart; only AMQP 1.0 link resumption and MQTT session resumption resume application state
  [SYNTHESIS D8]. EMQX measured the same on QUIC — "session state is not preserved", and an
  unexpectedly closed data stream loses QoS 1 and QoS 2 message state [prior-art §3 delivery
  guarantees], [prior-art §8 lesson 10]. What a weida sequence would mean across a reconnect
  is stated nowhere, today's producer name being the connection's fingerprint [GUARANTEES §6].
- **What the dedup window should be bounded by.** JetStream bounds it in time [nats §7];
  Artemis by a circular cache, `id-cache-size` default 20000 [amqp10 §7]; MQTT by the
  65,535-identifier space, EMQX capping awaiting-release at 100 [mqtt5 §11]. No sheet compares
  them or gives a rule.
- **Whether ordering is per transfer or configured per connection.** The vocabulary lists
  `None`/`PerProducer`/`PerKey`/`Total` and requires each level's performance implications to
  be documented [GUARANTEES §3]; whether the level is negotiated in HELLO, chosen per endpoint
  or set per transfer is unstated. Also unsettled: whether a relay reuses the producer's field
  or writes its own — MOQT relays read object metadata [prior-art §4], weida's L2 broker is
  Phase 6 [ARCHITECTURE §1].

## 7. Questions for the decider

1. **Is the field's purpose ordering, or identity?** Ordering means a monotone counter, so
   Option B or C [ARCHITECTURE §6a]. Identity — dedup, caching, replay-safe 0-RTT — needs only
   a name, which admits Option D and the MOQT triple [prior-art §8 lesson 12]. Both means the
   wire pays for two mechanisms.
2. **Must a Pub/Sub subscriber be able to detect a drop?** Today it cannot, by construction
   [PATTERNS §4]. Yes forces the field onto the L0 wire and rules out Option E, since the drop
   happens at the publisher's L0 fan-out; no leaves drop counting a publisher-local metric
   [GUARANTEES §6].
3. **What names the producer across a reconnect?** If the connection fingerprint suffices,
   Option B costs one uint and no new identity concept [GUARANTEES §6]. If the sequence must
   survive a reconnect, weida needs a session or a stable producer name — the part every sheet
   says breaks [SYNTHESIS D8] — a larger decision than a header key.
4. **Is per-key ordering in scope, or only per-producer?** `PerKey` is in the vocabulary
   [GUARANTEES §3], but only Kafka has a key concept and there "key order exists only while a
   stable key-to-partition mapping holds" [kafka §12/P8], [SYNTHESIS D4]. Yes implies a
   partition-like binding weida does not have; no closes `PerKey` as unreachable in the core.
5. **May a receiver hold a message back to deliver it in order?** Yes means reassembly:
   head-of-line blocking above QUIC [prior-art §8 lesson 2] plus a bounded buffer [INVARIANTS].
   No makes the field a *detector* — the `OUT_OF_ORDER_SEQUENCE_NUMBER` shape, an alarm rather
   than a repair [kafka §11].
6. **What bounds receiver state, and what happens at the bound?** A time window like
   JetStream's, where "an identifier reused after its window is not suppressed" [nats §7]; a
   count like Artemis's 20000-entry cache [amqp10 §7]; or a disconnect like ZRE's, which "SHALL
   treat the peer as invalid, and disconnect the peer" on a gap [zeromq §7]. Each is a different
   promise when the bound is hit.
7. **Reserved key space `0..=63`, or extension space `64+`?** Reserved makes the field part of
   the specification and of the golden vectors [PROTOCOL §5], [PROTOCOL §8]; the extension space
   keeps it optional. If a receiver must *understand* it rather than skip it, that is a
   `required_capabilities` code, which by construction fails negotiation against a v0 peer
   [PROTOCOL §2.3].
8. **Which guarantee dimension moves off `None`, and is that stated as a guarantee?** A
   requested guarantee "MUST NEVER be silently weakened" and invalid combinations "MUST be
   rejected… at configuration time, not silently at runtime" [GUARANTEES §4], so the choice
   must be a configuration the runtime can validate and refuse — including when the peer
   does not support it.

## 8. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §2.3, §3.1, §4, §5, §6.2, §6.3, §8;
[GUARANTEES.md](../GUARANTEES.md) §1, §2, §3, §4, §6; [PATTERNS.md](../PATTERNS.md) §1.7, §1.8,
§1.9, §4, §5; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §6a; [INVARIANTS.md](../INVARIANTS.md).

Research sheets: [SYNTHESIS.md](../research/SYNTHESIS.md) §1 (P5, P8, P9), §2 (D4, D7, D8, D11), §5,
§6, §7.2, §7.3, §8.4, §8.9; [prior-art.md](../research/prior-art.md) §1 (Zenoh), §2 (iroh), §3
(EMQX), §4 (MoQ), §8 lessons 1, 2, 5, 6, 10, 12; [quic-standards.md](../research/quic-standards.md)
§2.2, §3.8, §8.5, §11.2, §11.9, §12 items 2, 4, 15; [zeromq.md](../research/zeromq.md) §3, §7,
§12/P5, §12/P8; [nats.md](../research/nats.md) §7, §12/P1, §12/P8;
[kafka.md](../research/kafka.md) §3, §6, §11, §12/P8; [mqtt5.md](../research/mqtt5.md) §11;
[amqp10.md](../research/amqp10.md) §3, §7, §12/P9; [nanomsg-nng.md](../research/nanomsg-nng.md)
§12/P8, §12/P9; [rabbitmq-amqp091.md](../research/rabbitmq-amqp091.md) §12/P9.
