# 0034 — Late is lost: datagram flows, expiring streams, and RADIO/DISH

- **Status:** accepted
- **Date:** 2026-09-29
- **Items:** B-279 to B-292
- **Answers:** [requirements/griasdi-voice.md](../requirements/griasdi-voice.md) (all four
  requests and its open questions 1 and 4), and the lossy half of
  [requirements/zeughaus-video.md](../requirements/zeughaus-video.md) that its stage 3 and its
  open question 2 point at
- **Amends:** [ARCHITECTURE.md](../ARCHITECTURE.md) §1 ("its primitives are exactly the two
  stream kinds"), [PROTOCOL.md](../PROTOCOL.md) §11 ("QUIC datagrams. Only streams are used";
  "no capability code is assigned"), [0016](0016-conflation.md) §4.2 (what the refusal covers)
- **Related:** [0002](0002-control-and-bulk-separation.md) §6.2,
  [0005](0005-refusal-race.md), [0006](0006-guarantee-sets.md),
  [0011](0011-answered-where-it-arrived.md) §4.1, [0012](0012-local-connection-grouping.md) §4.4,
  [0013](0013-competitor-libraries.md), [0021](0021-consensus-openraft.md),
  [0024](0024-three-families-one-back-channel.md) §4.4, [0031](0031-transparent-redial-and-the-sender-outbox.md),
  [PATTERNS.md](../PATTERNS.md) §1.3, §1.7, §1.11, §4, §4.1, §6.2, §6.3,
  [GUARANTEES.md](../GUARANTEES.md) §3, §4, §6, [INVARIANTS.md](../INVARIANTS.md)

## 1. The question

Two applications arrived with the same shape of problem. griasdi, a chat and voice application
whose every byte travels over weida, needs voice: 50 datagrams a second per speaker, 80-250
bytes each, and "a 20 ms audio frame that arrives after its playout time is worthless"
[griasdi-voice, Context]. zeughaus needs video: its stage 1 runs on standing feeds today, but
stage 3 is fan-out to several viewers and its open question 2 is the day remote viewing needs
compression, which is the day a frame depends on the frames before it.

griasdi's requirement asks for an L0 primitive and explicitly **not** a pattern: "griasdi does
not need RADIO/DISH-style group semantics from weida, because fan-out is the SFU's job and weida
forwards on nobody's behalf" [griasdi-voice, "What weida would need", item 1]. The owner's
position is wider, in his words: "ich will das einen echten teil von weida machen, mit weida
sollte man solche architekturen mit abbilden koennen, dafuer braucht es ggf andere patterns."
So the question is not only "which bytes go on the wire" but: **which mechanisms make "late is
lost" a property weida states, and which pattern gives the architectures built on it — a
selective forwarding unit, a media fan-out, a relay — a name in weida's own vocabulary.**

## 2. The evidence, condensed

**2.1 What weida delivers today: all of a unit, or a named failure.** Within a connection QUIC
retransmits until FIN or reset ([PATTERNS.md](../PATTERNS.md) §1.11), and the one place weida
discards on purpose is fan-out, which drops the *arriving* copy at a subscriber's byte budget
([PATTERNS.md](../PATTERNS.md) §4, [0016](0016-conflation.md) §2). §1.11's own table already
names the right answer for "a video frame, a live tail, a snapshot that is already stale":
**cancel**. What is missing is anything that cancels *on time* — today the application has to
notice the deadline and call `cancel` itself.

**2.2 What weida already gets right for media.** One stream per message means a lost packet
delays only its own message: "Per stream, transfers are isolated" ([PATTERNS.md](../PATTERNS.md)
§1.3). That is the property griasdi's rejected "one long-lived stream per flow" lacks
[griasdi-voice, Rejected alternatives]. `Publisher::open` + `write_now` already drops a
subscriber that cannot keep up without holding anything ([PATTERNS.md](../PATTERNS.md) §4.1), and
it is zeughaus' conflation today [0016 §4.3].

**2.3 Datagrams, read from `quinn` 0.11.11 / `quinn-proto` 0.11.17.** The send side is in
[0024](0024-three-families-one-back-channel.md) §2: unreliable, unordered, must fit one packet,
"previously queued datagrams which are still unsent may be discarded … in order of oldest to
newest", `max_datagram_size()` is `None` when disabled. Three further facts, read for this note:

- **The receive side discards oldest-first too.** "If the aggregate size of all datagrams that
  have been received from the peer but not consumed by the application exceeds this value, old
  datagrams are dropped" (`quinn-proto` `config/transport.rs:280-285`), implemented as a loop
  that pops the front until the new one fits (`connection/datagrams.rs:132-136`). Both ends of
  the carrier already have the policy a stale frame wants.
- **Datagrams outrank every stream on their connection.** `populate_packet` writes pending
  DATAGRAM frames into a packet before any stream frame (`connection/mod.rs:3314-3325`). A
  datagram flow therefore needs no priority of its own against streams on the same connection.
  This is an implementation property of `quinn`, not of RFC 9221, and is recorded as such.
- **Every weida QUIC connection advertises datagrams today, and nothing reads them.**
  `TransportConfig::default()` sets `datagram_receive_buffer_size = Some(STREAM_RWND)`, and
  `STREAM_RWND` is 12 500 000 B/s × 100 ms = **1 250 000 bytes** (`config/transport.rs:359-363,
  388`); the QUIC transport parameter `max_datagram_frame_size` is derived from it, capped at
  65 535 (`transport_parameters.rs:170-172`). weida's `transport_config` never touches either
  (`crates/weida/src/tls.rs:426-445`, used for both roles at `:572` and `:633`). So a peer may
  send datagrams to any weida connection and `quinn` buffers up to ~1.2 MB of them per
  connection — bounded by drop-oldest, but a remote-influenced allocation
  [INVARIANTS.md](../INVARIANTS.md) does not name, and at `max_connections_per_peer` (64) about
  80 MB per peer that no code ever consumes. This answers griasdi's "whether quinn's defaults
  leave datagrams enabled on weida connections is not verified here" [griasdi-voice, "What weida
  provides today"]: they do.

**2.4 Stream priority exists and is unused.** `SendStream::set_priority(i32)` (`quinn`
`send_stream.rs:220`) orders the streams of **one connection**; weida never calls it. Because a
dialled path is its own connection ([0002](0002-control-and-bulk-separation.md) §6.2), a priority
can order streams within a path and never across paths. And a reset is accepted after `finish`
until every byte is acknowledged — `reset` refuses only a stream whose state is gone or already
reset (`quinn-proto` `connection/streams/mod.rs:313-325`) — so a deadline can still cancel a
finished transfer whose tail is being retransmitted.

**2.5 What the precedents do** ([research/prior-art.md](../research/prior-art.md)).

- **MOQT** delivers "while still useful": partial reliability is two delivery timeouts that
  reset streams and drop datagrams, a subgroup is one stream, an object may be one datagram, a
  group is the join point, priorities are per subscription and per track, and "implementations
  SHOULD minimize the amount of data buffered at the underlying transport layer, as any data
  buffered at this layer can no longer be timed out" (lines 546-575, 630-652).
- **moq-lite** is the same idea smaller: one uni stream per group, a datagram is a single-frame
  group of at most 1200 bytes, expired group streams are reset "to avoid consuming flow control",
  and "a subscriber MUST handle gaps, potentially caused by congestion" (lines 584-586, 623-626,
  665-667, 1267).
- **ZeroMQ's own lossy fan-out is RADIO/DISH** (48/RADIO-DISH): exact-match groups, RADIO "fans
  out, drops on a full queue, never blocks", DISH joins groups and "discards on a full queue"
  ([research/zeromq.md](../research/zeromq.md) §4.6, lines 308-310). `weida-zmq` lists it as absent,
  "and additionally the group family is defined over `udp`" ([libraries/zmq.md](../libraries/zmq.md)
  line 62).
- **Zenoh** sends best-effort traffic as QUIC DATAGRAM and maps priority classes, not messages,
  onto streams; **Mosh** skips obsolete frames rather than retransmitting them; **WebTransport**'s
  incoming datagram queue "drops from the head when full" (lines 1263, 1277, 621-623).
- **Relays are part of the architecture** in every system that reaches the internet — the
  catalogue's recurring lesson 3 (lines 1300-1305).

**2.6 The shape they share.** Each unit has a **useful life**; after it, delivery is waste. A
newer unit **supersedes** an older one of the same kind. Loss is expected and must be
**visible** as a gap, never as silence and never as a stall. And the carrier follows the unit's
size: a unit that fits one packet and depends on nothing is a **datagram**; a unit larger than a
packet, or a chain whose later parts depend on earlier ones (a video GOP), is **one stream per
chain**, reset when its life ends or its successor starts.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — datagram flows at L0 and nothing else | griasdi's request verbatim | Every application rebuilds expiry, supersession and the stale-drop, and the video half — larger-than-a-packet units with dependencies — gets nothing. The owner's "Architekturen abbilden" is not met: an SFU is expressible only as bytes, not as a pattern |
| B — streams only: expiry and priority, a datagram modelled as a one-frame stream | moq-lite without its datagram | 50 streams a second per speaker per listener, each paying an open and a DATA header, and a lost packet is retransmitted until the deadline reset lands — at least an RTT of useless traffic per loss. moq-lite itself kept the datagram for exactly this case |
| C — implement MOQT or moq-lite as weida's media layer | interoperate with the IETF work | A second namespace, a second session model and a second identity scheme beside weida's, on drafts that churned from -01 to -05 and interoperate "only on the feature intersection" (prior-art line 1267). The right home for it is a standalone library in [0013](0013-competitor-libraries.md)'s shape, not weida's core |
| D — RTP/UDP or WebRTC beside weida | the telephony stack | Already rejected by the requirement: a second identity, a second encryption layer, a second NAT mapping, no proved peer [griasdi-voice, Rejected alternatives] |
| **E — one carrier, two stream mechanisms, one pattern pair** | a datagram flow at L0; expiry and priority on the streams that exist; RADIO/DISH at L1 over both; a relay is an ordinary program | Adds a third carrier to a core that was defined as "exactly the two stream kinds", a frame kind, a capability code and two invariant amendments. Each is paid for below |

## 4. Decision

**Option E.** Late is lost, and weida says so in three places: the stream vocabulary gets the
carrier QUIC has and weida did not use, the streams it already has learn to stop on time, and
the message vocabulary gets the pattern ZeroMQ already named for lossy fan-out.

### 4.1 The principle: a unit's life is the sender's, and no guarantee dimension is added

A deadline is a **local decision about how long a unit is worth sending**, exactly as a survey's
deadline is a local decision about how long to wait ([PATTERNS.md](../PATTERNS.md) §6.2): it is
not negotiated and the receiver never needs to learn it. So the guarantee-set list stays closed
([GUARANTEES.md](../GUARANTEES.md) §3): a flow and a RADIO copy are delivery `BestEffort`, as a
fan-out copy is, and what distinguishes them is a pattern row in [GUARANTEES.md](../GUARANTEES.md)
§6, not a level two peers intersect. What *is* negotiated is only whether the carrier exists at
all (§4.5), because that is a capability, not a strength.

### 4.2 L0: the datagram flow, a third carrier

QUIC has three carriers — uni streams, bidi streams, datagrams — and weida used two. A **flow**
is registered once, reliably, and its units travel as datagrams:

- **Registration is a stream, and the stream is the flow's lifetime.** The sender opens a uni
  stream of new frame kind **`7` FLOW**: a header with the same keys DATA uses where the meaning
  is the same (`endpoint`, `content_type`, `traceparent`, `tracestate`, `topic`) plus a required
  `flow` id, and **no FIN** until the flow ends. FIN is an orderly close, `RESET_STREAM` an
  abandoned flow, `STOP_SENDING` with `UNKNOWN_ENDPOINT`, `UNSUPPORTED` or `REJECTED` a refusal —
  [PROTOCOL.md](../PROTOCOL.md) §7's existing codes, so authorization keeps
  [0015](0015-peer-authorization.md)'s shape: the acceptor decides on `(proved peer, path)` once,
  when the flow opens. No FLOW_CLOSE frame, no reply frame, no correlation: the stream is all of
  them, which is the lesson [ARCHITECTURE.md](../ARCHITECTURE.md) §1 draws from Req/Rep.
- **A datagram is `varint flow` followed by opaque payload.** The id is allocated by the
  **sender**, per connection and direction. Unlike [0024](0024-three-families-one-back-channel.md)
  §4.4's `report_id`, the receiver's table is not keyed by a number it never issued in an
  unbounded way: every live entry holds an open FLOW stream, so the table is bounded by the
  stream budget and, separately, by `Limits::max_flows` (§4.5).
- **A datagram naming an unknown id is held briefly, then dropped and counted — never a
  protocol error.** Streams and datagrams are unordered relative to each other, and `quinn`
  packs a datagram ahead of stream data (§2.3), so a flow's first datagrams can arrive before its
  FLOW header has been dispatched. They wait in one per-connection ring of
  `Limits::flow_early_bytes` for at most `flow_early_hold`; a header claiming the id adopts them in
  order, anything else ages out as `unknown_flow`. A datagram for a closed flow is the same case.
- **Either side opens flows**, on whichever connection exists: the bound side toward a peer that
  dialled it rides the connection that peer's registration arrived on
  ([0011](0011-answered-where-it-arrived.md) §4.1), which is what a server-to-client voice flow
  behind NAT needs [griasdi-voice, "What weida provides today"].
- **Sending never waits.** `Flow::send(Bytes)` is synchronous. It returns `Ok` when the datagram
  was handed to the connection and a named error otherwise: `TooLarge { max }` against
  `max_datagram_size()` minus the id's varint, `DatagramsUnavailable`, or, on a closed flow, the
  error that closed it — `Rejected`, `UnknownEndpoint`, `Unsupported`, `LimitExceeded`,
  `Canceled` or `ConnectionLost(cause)`; there is no separate "flow closed" error. Queued
  datagrams beyond `Limits::datagram_send_bytes` are discarded oldest-first — `quinn`'s policy.
  On QUIC those discards are **not attributable per flow**, because `quinn` reports none; on the
  local transports (§4.4) weida's own writer discards and counts them per flow.
- **Receiving never blocks the connection.** One reader per connection demultiplexes into a
  per-flow ring of `Limits::flow_queue_bytes`; a slow consumer loses its **oldest** datagrams,
  counted per flow, and stalls neither other flows nor the reader.
- **The refusal race is [0005](0005-refusal-race.md)'s, unchanged.** Datagrams sent before a
  refusal arrives are dropped at the receiver; the sender learns the refusal from its flow
  closing with the code, and nothing is invented to order the two.
- **Refusal and release are stop codes.** `IncomingFlow::refuse()` stops the FLOW stream with
  `REJECTED`; dropping an accepted `IncomingFlow` stops it with `CANCELED`; a FLOW header beyond
  `Limits::max_flows` inbound flows is stopped with `LIMIT_EXCEEDED`. The FLOW header's keys are
  `0` endpoint, `1` flow id, `2` content_type, `3` traceparent, `4` tracestate, `5` topic.

The surface, as a sketch rather than a signature:

```rust
let mic = peer.open_flow(FlowMeta::default()).await?;   // FLOW header written, no answer awaited
mic.send(opus_frame)?;                                  // never waits; TooLarge, DatagramsUnavailable, or the closing error
match acceptor.accept().await? {
    Incoming::Flow(flow) => while let Some(dg) = flow.recv().await { /* ... */ },
    Incoming::Stream(t) | Incoming::Exchange(t) => { /* unchanged */ }
}
```

### 4.3 L0: the streams that exist learn to stop on time

Two sender-local mechanisms, neither on the wire:

- **Expiry.** `OutgoingTransfer::expire_at(Instant)` resets the stream with `CANCELED` if its
  bytes are not all acknowledged by then — on QUIC including after `finish`, which §2.4 shows
  `quinn` permits, so it covers MOQT's subgroup timeout as well as its object timeout. On the
  local transports expiry acts until `finish`. The sender sees `Error::Expired`, which is not a
  definite failure, because the peer may have read every byte before the reset landed; the
  reader sees what it sees for any cancellation, `Canceled` and never EOF
  ([PATTERNS.md](../PATTERNS.md) §1.5). The sender's counters distinguish an expiry from a
  cancel; the wire does not need to, because to the reader a partial unit is useless either way.
- **Priority.** `OutgoingTransfer::set_priority(i32)`, `quinn`'s own scale. It orders streams of
  **one connection**, which after [0002](0002-control-and-bulk-separation.md) §6.2 means one path —
  so media that must be ordered against each other share a path, and §4.6's pattern puts a whole
  broadcast under one. Datagrams outrank every stream on their connection already (§2.3). On a
  local transport, where each stream is its own OS connection, priority is a no-op and says so.

### 4.4 The local transports carry the same contract

The contract a flow states is: a datagram may be lost, is never sent late on purpose, never
blocks its sender, and at a bound the oldest goes. Each local transport meets it without a new
kernel mechanism:

- **On every local transport** — in process, over `AF_UNIX` and over named pipes — the flow's
  registration is its own stream ([0010](0010-local-transport.md) §4.2), and the FLOW stream
  itself carries the datagrams after its header, each as a QUIC varint length followed by that
  many bytes, at most `LOCAL_MAX_DATAGRAM` = 1200, so boundaries survive without the payload
  chunk framing and without tuning `SO_SNDBUF`. The sender's writer drains a drop-oldest ring of
  `datagram_send_bytes`; the receiver's reader task drains the socket into the flow's own
  drop-oldest ring, so a kernel buffer holds stale data only while the receiving process is
  stalled as a whole. Locally nothing is lost in transit; the only losses are overload drops,
  counted as such.
- **Named loss: a local refusal is not learned by the sender.** Over `AF_UNIX` and named pipes a
  receiver that refuses or releases a flow stops reading by draining, and the sender does not
  learn the stop code.
- **A flow toward a peer that dialled** needs a parked reverse connection
  ([0012](0012-local-connection-grouping.md) §4.4) and holds it for the flow's life, so over a
  socket transport a subscriber can receive at most `max_parked_reverse` concurrent flows. That
  bound is stated, not hidden, and a flow that finds the pool empty is a counted drop.

### 4.5 Negotiation and configuration: off by default, and the first capability code

- **Capability code `1` is `datagram`**, the first code [PROTOCOL.md](../PROTOCOL.md) §6.1
  assigns. A side lists it in HELLO key `3` when its profile enables flows, and MAY require it
  in key `4`. A FLOW stream or a datagram MUST NOT be sent unless both HELLOs listed code `1`,
  which is what makes kind `7` safe: [PROTOCOL.md](../PROTOCOL.md) §4 makes an unknown kind
  connection-fatal, and a peer that never listed the code never sees one. This answers
  griasdi's open question 1 for a frame kind rather than a reserved path.
- **The QUIC transport parameter follows the capability.** `Limits` gains
  `datagram_receive_bytes` (default **0**, which sets `quinn`'s `datagram_receive_buffer_size`
  to `None`, so `max_datagram_frame_size` is not advertised at all), `datagram_send_bytes`,
  `max_flows`, `flow_queue_bytes`, `flow_early_bytes` and `flow_early_hold`. Per profile, so a
  path that carries voice can enable it and a bulk path need not.
- **Default off, and that fixes §2.3 on its own.** A connection that enables nothing advertises
  no datagram support, buffers none and runs no reader — the hot-path invariant. B-279 makes
  that true before any flow exists, because today's state is an allocation nothing names.
- **No silent downgrade.** A flow opened where the capability was not agreed fails with
  `DatagramsUnavailable`; weida never substitutes a stream, because a stream is the one carrier
  that would deliver the unit *late* ([GUARANTEES.md](../GUARANTEES.md) §4, rule 2). Whether to
  fall back is the application's decision [griasdi-voice, "No silent downgrade"].

### 4.6 L1: RADIO/DISH, lossy fan-out of segments

ZeroMQ named this pattern and weida adopts the name, because weida's message vocabulary is the
ZeroMQ family ([ARCHITECTURE.md](../ARCHITECTURE.md) §1): RADIO fans out, drops, never blocks;
DISH joins. What weida adds is the unit a media stream actually has.

A **segment** is a unit whose parts may depend on each other and on nothing earlier: a voice
frame, a raw preview frame, a video GOP. It is MOQT's group and HLS's segment, and it is the
**join point**. Segments are numbered per `(radio, topic)` in new DATA key **`13`** `segment`.

| | `Radio` | `Dish` |
| --- | --- | --- |
| Compatible peer | `Dish` | `Radio` |
| Direction | binds | connects |
| Send/receive pattern | `segment(topic)` → `write` chunks → `finish`; `datagram(topic, bytes)` for a one-packet segment | `join(filter)` / `leave(filter)`, then `recv` → a segment as a streamed `IncomingTransfer`, or a datagram with its topic and segment number |
| Topics | [0007](0007-topic-namespace.md)'s namespace and filter grammar, unchanged — ZeroMQ's "group" is weida's topic | |
| Outgoing routing | every dish whose filter matches, one copy each; the dish set is fixed when a segment opens, as `FanOut`'s is ([PATTERNS.md](../PATTERNS.md) §4.1) | — |
| Carrier | a stream segment: one uni DATA stream per dish; a datagram segment: a flow per `(dish, topic)`, opened lazily on the connection the join arrived on | |
| Supersession | opening segment *n+1* on a topic **resets every copy of segment *n* on that topic that is still unacknowledged**, with `CANCELED` | a segment older than the newest one it has delivered on that topic is discarded before delivery, so streams that arrive out of order never go backwards |
| Expiry | per dish: its `max_age`, measured on the radio's clock from the segment's open — the dish needs no clock of its own; when several of one dish's matching filters carry a `max_age`, the smallest applies | states `max_age` when it joins (SUBSCRIBE key `2`) |
| Backpressure | never blocks: a dish without room for a chunk loses that segment (`write` is `write_now`) | a dish that stops reading loses segments at the radio and never stalls it |
| Delivery | `BestEffort`; a lost segment is a gap in the segment numbers the dish sees, and the radio counts it per topic and cause: budget, superseded, expired, too large for the dish's datagram size, no datagram capability | |
| Late joiner | receives the next segment; nothing is retained for it | |

Five rules a caller can get wrong:

1. **Supersession holds nothing.** It discards bytes that are already in flight — the
   transport's — rather than keeping a value to send instead, which is the difference from the
   coalescer [0016](0016-conflation.md) §4.2 refused (§4.7).
2. **A late joiner waits for the next segment**, because a segment is the only point a decoder
   can start from and retaining the current one for newcomers is MQTT's retained message, a
   late-joiner store [0016 §3 option E] this note does not add. The application chooses how
   often a join point comes, which for video is the keyframe interval.
3. **Topics under one radio share one connection per dish**, so priority works among them:
   audio as datagrams outranks video as streams by `quinn`'s packing (§2.3), and video tiers can
   be ordered with §4.3's priority. Topics on different paths cannot be prioritized against each
   other at all.
4. **A datagram segment is never turned into a stream segment.** A dish whose connection did not
   agree capability `1` loses datagram segments with the cause named; it is not sent them late
   instead (§4.5).
5. **Joins are subscriptions.** SUBSCRIBE and UNSUBSCRIBE carry them with the new optional key,
   they ride the path's connection ([0011](0011-answered-where-it-arrived.md) §4.2), and a
   redialling dish re-sends them exactly as a subscriber does
   ([0031](0031-transparent-redial-and-the-sender-outbox.md) §4.6). A join has no reply half,
   so [0017](0017-subscription-verdict.md)'s silence applies unchanged.

### 4.7 What this does to 0016 and to ordering

- **[0016](0016-conflation.md) §4.2 refused a fan-out coalescer because it must hold the
  superseded value** — a per-subscriber copy — and because for a streamed payload there is no
  value to hold. Supersession by reset holds nothing and is defined for exactly the streamed
  case. It also delivers the one thing 0016 §4.4 said only a key could buy, "several keys
  multiplexed over one subscription": a new segment supersedes only its own topic, so a fast
  topic cannot starve a slow one through supersession. The refusal of the coalescer stands;
  this note narrows what it covers to what it argued. `Coalesce` stays reserved for the L2 keyed
  queue.
- **A segment number is not a `PerProducer` sequence.** It is intrinsic to the pattern, written
  whatever ordering was negotiated, and scoped per `(radio, topic)`. Segments never carry DATA
  key `6`, and the reassembler passes an arrival without a sequence number straight through
  (`crates/weida/src/ordering.rs:602-606`), so reassembly never holds a segment for a
  predecessor that was superseded on purpose.

### 4.8 The two architectures, expressed

**griasdi.** The client dials the server's `/voice` path. Its microphone is an L0 flow toward the
server; what it hears is a dish joined to the room's topics. The server is a relay: it accepts
flows, authorizes each on `(proved peer, path)`, and republishes their payload — SFrame
ciphertext it cannot read — as datagram segments on its radio. weida still forwards on nobody's
behalf ([PATTERNS.md](../PATTERNS.md) §6.3): the SFU is an ordinary weida program, as the broker
is, and its forwarding decision is application code.

```rust
// server: the whole forwarding path of an SFU
while let Incoming::Flow(mic) = voice.accept().await? {
    let topic = speaker_topic(mic.info())?;         // authorize here, or mic.refuse()
    let radio = radio.clone();
    exec.spawn(async move {
        while let Some(frame) = mic.recv().await { radio.datagram(&topic, frame); }
    });
}
```

**zeughaus.** A runner binds a radio per sample path; each tier of a node's output is a topic
(`node.17.pin.0.t480`). A raw preview frame is one stream segment, so supersession is the
conflation 0016 §4.3 asked the producer to build by hand, and a viewer's `max_age` replaces its
frame-rate ceiling's latency half. When compression arrives, a GOP is one segment whose chunks
are the encoded frames, read by the dish as they arrive — the payload is never materialized at
the radio, at a relay, or at the dish, which keeps [0024](0024-three-families-one-back-channel.md)
§4.5's headline true for video.

A relay between a radio and many dishes is the same program with a dish on one side: it forwards
stream segments chunk by chunk and datagram segments one by one, and counts the ones too large
for a downstream connection's datagram size — MOQT's multi-hop hazard (prior-art lines 571-575),
made a named drop cause rather than a silence.

### 4.9 Statistics are passed through, in weida's own types

griasdi sizes its jitter buffer and its Opus bitrate from the path [griasdi-voice, "Enough
statistics"]. A flow exposes `PathStats` — `rtt` (smoothed), `cwnd`, `congestion_events`,
`lost_packets`, `sent_packets` and the current `max_datagram_size` — and `FlowStats`: sent, too
large, discarded before send (local transports), not live, received, dropped on overflow.
`quinn` 0.11 exposes no RTT variation (`quinn-proto` `connection/stats.rs:136-151`), so none is
passed through. These are weida's structs filled from `quinn`'s `Connection::stats`; no `quinn`
type appears in a public signature, which is the encapsulation
[0021](0021-consensus-openraft.md) applies to openraft and weida already applies to `quinn`.

### 4.10 A redial re-opens flows; a gap loses what was sent into it

griasdi's open question 4. A flow opened through a **dialling** endpoint belongs to the address
slot, not to the connection ([0031](0031-transparent-redial-and-the-sender-outbox.md) §4.1): the
runtime re-registers it on the redialled connection under a **new id on the new connection**, and a `send` while
no connection is live is dropped and counted as `not_live` — never queued, because a queued voice
frame is a late one. A re-registration that is refused ends the flow with the code. A flow opened
by the **bound** side ends with its connection, and a dish's datagram segments resume because its
joins are re-sent and the radio opens new flows lazily.

### 4.11 Two invariants are amended — by the owner, before any code depends on it

[INVARIANTS.md](../INVARIANTS.md) says a genuinely wrong invariant is amended there first, with
the reasoning. Two are affected; the owner accepted this text with the note, and B-291 writes it
into INVARIANTS.md:

- "One data flow maps naturally to one transport stream where the transport supports it"
  becomes "… where the transport supports it; **a flow of units that are worthless once late
  maps to one registration stream and the transport's datagrams**". The flow is still one
  stream in every sense the invariant protects — lifetime, refusal, cancellation, accounting —
  and the datagrams are the carrier QUIC provides for units a stream would deliver late.
- "All user payloads may remain streams end-to-end" is **unchanged in text** and gains a
  sentence in its enforcement row: a datagram payload is materialized by definition and bounded
  by `max_datagram_size` (about a kilobyte); every payload larger than one packet still may be,
  and under RADIO is, a stream end to end.

### 4.12 What does not change

No existing pattern changes shape or semantics, no existing frame changes, a connection that
enables nothing is byte-identical to today's on the wire apart from **not** advertising
`max_datagram_frame_size`, and [0024](0024-three-families-one-back-channel.md) §4.4's rejection of
datagrams **for cursors** stands: a cursor must arrive, a voice frame must not arrive late, and
the carrier follows the requirement both times.

## 5. Consequences and follow-ups

Documents, once the owner accepts the note (B-291): [ARCHITECTURE.md](../ARCHITECTURE.md) §1's
L0 list gains the flow and its table a row; [PATTERNS.md](../PATTERNS.md) gains §1.12 (expiry and
priority), §5's flow, §6.4 RADIO/DISH, and two "Choosing" rows; [GUARANTEES.md](../GUARANTEES.md)
§6 gains the RADIO/DISH and flow rows; [PROTOCOL.md](../PROTOCOL.md) gains kind `7`, the datagram
payload, capability code `1`, DATA key `13`, SUBSCRIBE key `2`, and loses two lines of §11;
[INVARIANTS.md](../INVARIANTS.md) takes §4.11's text and names the six new bounds. Backlog, links
relative to `docs/BACKLOG.md`:

### B-279 — Stop advertising datagrams nobody reads
kind: code | size: 30 | status: ready | needs: []
acceptance: `transport_config` sets `datagram_receive_buffer_size(None)` on both roles, so a weida QUIC connection no longer sends `max_datagram_frame_size` and `quinn` buffers nothing for a peer's datagrams ([0034](decisions/0034-late-is-lost.md) §2.3). A test over a real QUIC pair asserts `max_datagram_size()` is `None` on both sides — and fails on today's code, where it is `Some`. [INVARIANTS.md](INVARIANTS.md)'s bound list says datagrams are refused at the transport parameter until B-282 names their bounds.
note: found while writing 0034: `quinn`'s default is `Some(1 250 000)` bytes per connection, drop-oldest, never read by weida — bounded, unnamed, and about 80 MB per peer at `max_connections_per_peer`.

### B-280 — PROTOCOL: the datagram capability, FLOW, and the two new keys
kind: spec | size: 45 | status: ready | needs: []
acceptance: [PROTOCOL.md](PROTOCOL.md) specifies capability code `1` `datagram` (§6.1, and the rule that neither kind `7` nor a DATAGRAM frame is sent unless both HELLOs listed it, a FLOW from a peer that did not being a `PROTOCOL_VIOLATION`), frame kind `7` FLOW (§4: uni, header only on QUIC, held open until FIN; §6.8: its key table, the sender-chosen flow id, refusal by `STOP_SENDING` with `UNKNOWN_ENDPOINT`, `UNSUPPORTED`, `REJECTED` or `LIMIT_EXCEEDED`, release with `CANCELED`), the DATAGRAM payload `varint flow` + opaque bytes and the early-hold rule (§6.9), the local-transport mapping of `varint length || bytes` records of at most 1200 bytes after the FLOW header (§2.1), DATA key `13` `segment` and SUBSCRIBE key `2` `max_age_ms`, golden vectors for FLOW, both keys, a datagram payload and a HELLO listing code `1` (§8), and the six new `Limits` rows (§10); §11 loses "QUIC datagrams. Only streams are used" and the empty-capability line, and `grep` finds no stale statement that the capability set is empty.

### B-281 — The codec for FLOW, the flow-id prefix and the two keys
kind: code | size: 60 | status: ready | needs: [B-280]
acceptance: `weida-protocol` encodes and decodes the FLOW header, the datagram prefix, DATA key `13`, SUBSCRIBE key `2` and capability code `1`, with golden vectors in [PROTOCOL.md](PROTOCOL.md) §8's shape and the fuzz targets extended; kind `7` from a peer whose HELLO did not list code `1` is a `PROTOCOL_VIOLATION`.

### B-282 — Datagram flows over QUIC
kind: code | size: 90 | status: ready | needs: [B-279, B-281]
acceptance: [0034](decisions/0034-late-is-lost.md) §4.2 and §4.5 in `crates/weida`, one mechanism for QUIC and the local transports: the six `Limits` fields with defaults that keep flows off, capability `1` advertised exactly when enabled, `Peer::open_flow`, `Incoming::Flow`, `Flow::send` synchronous with `TooLarge`, `DatagramsUnavailable`, or the error that closed the flow, one reader per connection with per-flow drop-oldest rings, the early hold, refusal by stop code. Tests in `crates/weida/tests/flows.rs`: a flow carries datagrams from a dialling peer to an acceptor; a flow to an unregistered path fails its sends with `UnknownEndpoint`; a stalled flow drops its oldest and counts them while a sibling flow keeps receiving; a runtime without the capability fails `open_flow` with `DatagramsUnavailable` and never falls back to a stream; a payload one byte over `max_payload()` is `TooLarge`; a unit test shows a datagram for an unknown id allocates nothing beyond the early ring. The bound side's flow toward a peer that dialled is proved by B-286's datagram segments.

### B-283 — Flows over the local transports
kind: code | size: 90 | status: ready | needs: [B-282]
acceptance: [0034](decisions/0034-late-is-lost.md) §4.4: B-282's flow scenario passes over QUIC, in process, over `AF_UNIX` and over named pipes in `crates/weida/tests/transports.rs`, with the datagrams carried on the FLOW stream as `varint length || bytes` records behind a drop-oldest writer ring.

### B-284 — Expiry and priority on an outgoing transfer
kind: code | size: 60 | status: ready | needs: []
acceptance: [0034](decisions/0034-late-is-lost.md) §4.3: `OutgoingTransfer::expire_at` resets an unacknowledged stream with `CANCELED` at the deadline, on QUIC **including after `finish`**, and until `finish` on the local transports; the sender sees `Error::Expired`, which is not a definite failure, and the reader sees `Canceled`, never EOF; `Runtime::expired_transfers` counts expiries; `set_priority` passes `quinn`'s priority to the stream and is a documented no-op locally. Test: a transfer whose reader is stalled past its deadline fails with `Expired` and the counter says so. No priority test, because it would test `quinn`'s scheduler.

### B-285 — RADIO/DISH with stream segments
kind: code | size: 90 | status: ready | needs: [B-281, B-284]
acceptance: [0034](decisions/0034-late-is-lost.md) §4.6's table for stream segments: `Radio::segment`, `Dish::join` with `max_age`, segment numbers in DATA key `13`, supersession resets the unacknowledged copies of the previous segment on the same topic and no other, expiry per dish on the radio's clock (the smallest `max_age` of the matching filters), the dish discards a segment older than the newest it delivered, drops counted per topic and cause. Tests: a stalled dish loses segments by supersession while a fast one receives every segment; a segment on topic A never resets one on topic B; a joiner receives the next segment and nothing earlier; a dish's `max_age` expires its copy while a draining dish receives the same segment whole.

### B-286 — RADIO datagram segments over flows
kind: code | size: 60 | status: ready | needs: [B-282, B-285]
acceptance: `Radio::datagram` sends a one-packet segment on a flow per `(dish, topic)`, opened lazily on the connection the join arrived on and closed on `leave`; a dish without capability `1` and a datagram larger than a dish's `max_datagram_size` are counted drops with their causes, never a stream.

### B-287 — `PathStats` and `FlowStats`
kind: code | size: 45 | status: ready | needs: [B-282]
acceptance: [0034](decisions/0034-late-is-lost.md) §4.9: `PathStats` (`rtt`, `cwnd`, `congestion_events`, `lost_packets`, `sent_packets`, `max_datagram_size`) on `Flow` and `IncomingFlow`, filled from `quinn` with no `quinn` type in a public signature, `None` on the local transports; `FlowStats` ships with B-282. No permanent test, because the struct is a pass-through; a throwaway example prints the values of one loopback flow.

### B-288 — A flow outlives a redial
kind: code | size: 60 | status: ready | needs: [B-282]
acceptance: [0034](decisions/0034-late-is-lost.md) §4.10: a flow opened through a dialling endpoint is re-registered on the redialled connection under a new id, sends during the gap are counted `not_live` and never queued, a refused re-registration ends the flow with its code; tests in `crates/weida/tests/reconnect.rs`' shape.

### B-289 — Voice beside bulk: what the path and the controller cost
kind: measure | size: 60 | status: ready | needs: [B-282]
acceptance: one-way latency and loss of a 50 Hz, 200-byte flow while a bulk upload runs to the same host, in four configurations — flow on the bulk path's connection or on its own path, NewReno/Cubic or BBR on the bulk connection — on a shaped link (`tc netem` with a stated rate and queue), numbers into [IMPLEMENTATION.md](IMPLEMENTATION.md) §4. It decides whether a media profile needs its own congestion controller ([0034](decisions/0034-late-is-lost.md) §6).

### B-290 — The SFU and the relay as programs, and a guide section
kind: code | size: 60 | status: ready | needs: [B-286]
acceptance: an example SFU (flows in, datagram segments out, payload opaque) and a relay (dish in, radio out, stream segments forwarded chunk by chunk), each asserted by a test the way the zguide recipes are, and a [GUIDE.md](GUIDE.md) section that teaches "late is lost" with them.

### B-291 — Documents for 0034
kind: spec | size: 45 | status: ready | needs: [B-285]
acceptance: §5's edits to ARCHITECTURE, PATTERNS, GUARANTEES and INVARIANTS, the index rows, and the two requirement documents pointing at what shipped.

### B-292 — Flows and RADIO/DISH in `weida::blocking` and `weida-py`
kind: code | size: 90 | status: ready | needs: [B-286]
acceptance: RADIO/DISH in `weida::blocking` (flows stay async-only there, because `blocking` mirrors patterns and not `Peer`/`Acceptor`) and in both Python surfaces, with a `datagrams` runtime option; one stream segment and one datagram round trip per Python surface.

## 6. What this note does not decide

- **The congestion controller for media.** One connection per path means voice and a bulk
  upload to the same host run two controllers against one bottleneck queue, and no QUIC priority
  reaches across them; a loss-based controller on the bulk side fills that queue and the voice
  pays for it. Whether a media profile or the bulk profile needs a delay-based controller is
  B-289's number, not an argument. **B-289 measured it** ([IMPLEMENTATION.md](../IMPLEMENTATION.md)
  §4): at 20 Mbit/s behind a 64 KiB drop-tail queue, BBR on separate paths cut voice p95 by 92 %
  and 48 % in two runs, past the 30 % set in advance, at the price of about twice the voice
  loss. So a bulk profile that shares a bottleneck with media runs `Congestion::Bbr`; the
  default stays `Cubic`.
- **DSCP marking.** Per-connection marking fits per-path connections; griasdi's open question 2
  is right that a shared socket for hole punching makes it per packet. Both wait for a user.
- **NAT traversal and client admission.** griasdi's adjacent requirements — a
  client-verification hook, device certificates, one socket for dialling and listening, the
  observed remote address — are real and independent of lossy transport. Each gets its own note
  when taken.
- **Interoperating with MOQT or moq-lite.** Option C's home is a standalone library in
  [0013](0013-competitor-libraries.md)'s shape, and nothing here forecloses it: segment, topic and
  datagram map onto group, track and datagram.
- **Forward error correction, and framing inside a segment.** Both are the application's: Opus
  carries its own redundancy, and a segment's payload stays opaque, as every DATA payload is.
- **A connecting radio.** Connecting publishers are a recorded deferral
  ([PROTOCOL.md](../PROTOCOL.md) §11); the upstream direction is served by an L0 flow until a user
  needs a dialling radio.

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §1; [PATTERNS.md](../PATTERNS.md) §1.3,
§1.5, §1.7, §1.11, §4, §4.1, §6.2, §6.3; [GUARANTEES.md](../GUARANTEES.md) §3, §4, §6;
[PROTOCOL.md](../PROTOCOL.md) §2.1, §4, §6.1, §6.2, §6.4, §7, §11;
[INVARIANTS.md](../INVARIANTS.md); decisions 0002, 0005, 0007, 0010-0013, 0015-0017, 0021, 0024,
0031; [requirements/griasdi-voice.md](../requirements/griasdi-voice.md);
[requirements/zeughaus-video.md](../requirements/zeughaus-video.md);
[research/prior-art.md](../research/prior-art.md) lines 546-652, 1263-1305;
[research/zeromq.md](../research/zeromq.md) §4.6; [libraries/zmq.md](../libraries/zmq.md) §2.

Code read for this note: `crates/weida/src/tls.rs:416-445, 571-573, 632-634`;
`crates/core/src/limits.rs`; `quinn` 0.11.11 `src/connection.rs` (`send_datagram`,
`max_datagram_size`, `datagram_send_buffer_space`, `rtt`, `stats`) and `src/send_stream.rs:220`
(`set_priority`); `quinn-proto` 0.11.17 `src/config/transport.rs:280-303, 357-389`,
`src/connection/datagrams.rs:111-140`, `src/connection/mod.rs:3314-3329`,
`src/connection/streams/mod.rs:291-352`, `src/transport_parameters.rs:170-172`.
