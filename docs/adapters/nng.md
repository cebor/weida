# NNG / SP v1 — adapter mapping

Status: mapping document only. No adapter crate exists yet; the SP codec is Phase B slice 1
(`crates/adapters/weida-sp`) and the two bridge directions follow it
([LOOP.md](../LOOP.md) §9 Phase B, [0006](../decisions/0006-guarantee-sets.md) §4.9).
Date: 2026-09-11
Derived from: [docs/research/nanomsg-nng.md](../research/nanomsg-nng.md) (SP v1 RFCs
revision 01; NNG 1.10.0). Every SP or NNG claim below carries that sheet's section; every
weida claim carries the weida document or decision it comes from. Three kinds of byte-level
detail the sheet states only in prose — the octets of the TCP protocol header, the numeric
endpoint type IDs, and the exact shape of the PAIR v1 header — are taken from the SP RFCs
and the NNG source and are marked `[rfc-*]` / `[nng-src]` in §12; §11 records that as a gap
in the sheet rather than hiding it. Where SP and weida cannot be made to coincide, §8 names
the loss.

## 1. Scope

Two directions, built as separate slices [LOOP §9 Phase B]:

- **Inbound.** Foreign SP peers (NNG, libnanomsg or mangos) speak SP to the adapter; the
  adapter speaks weida to the weida network.
- **Outbound.** weida endpoints reach a foreign SP peer through the adapter.

The adapter is a **hop**, not a tunnel. It terminates SP — the 8-byte protocol header, the
64-bit-prefixed message framing, and the per-protocol headers (request-ID backtrace, survey
ID, PAIR v1 hop count) — and it terminates weida. Nothing is forwarded opaquely, which is
what makes "all guarantees are defined against the immediate next hop"
([INVARIANTS.md](../INVARIANTS.md)) checkable here, and what lets §7 name the exact point at
which the chain ends.

The adapter presents **cooked** SP semantics on the foreign side: the state machines, header
transfer, matching and retry the protocol defines [nanomsg-nng §4 "Cooked versus raw"]. Raw
sockets deliberately omit that state and hand it to the application [nanomsg-nng §2, §4], so
a raw peer is a peer of the adapter like any other, but the adapter never *is* a raw socket:
it has no application to delegate the omitted state to. `nng_device()` forwarding is
explicitly out of scope for the same reason — it requires raw sockets and only forwards
[nanomsg-nng §4].

Out of scope for the first slices: the UDP and WebSocket/WSS mappings and the experimental
ZeroTier transport [nanomsg-nng §0, §12/P18]; `inproc`, which is an in-process transport of
NNG's own and has no wire [nanomsg-nng §0]; PAIR v1 polyamorous mode, which the manual itself
deprecates [nanomsg-nng §4]; and the nanomsg-1.0 compatibility API, which is an API and not a
wire [nanomsg-nng §13].

## 2. SP protocol to weida pattern

weida's own mapping of this family is [ARCHITECTURE.md](../ARCHITECTURE.md) §6b — which names
nanomsg in its first column — with the direction rule of [ARCHITECTURE §6c.4]: Rep, Pull and
Pub bind; Req, Push and Sub connect. SP itself has no such rule: "either role may listen,
dial, or do both" [nanomsg-nng §1], so the adapter's configuration fixes which side binds and
that choice is the adapter's, not the protocol's (L8).

| SP protocol (endpoint type) | weida counterpart | Faithful? | What the adapter must do |
| --- | --- | --- | --- |
| REQ v0 (`0x30`) | `Requester` / one exchange [ARCHITECTURE §6a P2, §6b] | no — retry is not weida's | A cooked REQ **retransmits automatically** on its resend timer, on peer disconnect, or when a peer becomes available [nanomsg-nng §4 "REQ retry triggers", §12/P1]. weida's Req/Rep never retries: one exchange is one bidirectional stream and a failure is reported, not retried ([PATTERNS.md](../PATTERNS.md) §1.2, [FAILURE_MODEL.md](../FAILURE_MODEL.md) §4). Inbound, the adapter MUST treat a retransmitted request as a **new** request unless deduplication is configured (§7, L4) |
| REP v0 (`0x31`) | `Replier` [ARCHITECTURE §6b] | yes, with the envelope consumed | A cooked REP may send only after receiving its request, one pending receive per context, `NNG_ESTATE` otherwise [nanomsg-nng §4]. weida's reply half has the same one-reply-per-exchange shape [PATTERNS §1.2]. The backtrace stack is consumed at the adapter and copied back onto the reply [nanomsg-nng §3, §4], never forwarded as payload |
| PUSH v0 (`0x50`) | `Pusher` [ARCHITECTURE §6b] | yes on the policy | PUSH round-robins over the pullers that can accept *now* and waits or times out when none can [nanomsg-nng §4, §5]; weida's Push/Pull backpressure is `Block` ([GUARANTEES.md](../GUARANTEES.md) §6). Both are readiness-based, neither declares capacity [nanomsg-nng §12/P3] |
| PULL v0 (`0x51`) | `Puller` [ARCHITECTURE §6b] | yes | PULL fair-queues arrivals with no defined order between simultaneously ready peers [nanomsg-nng §4, §7]; weida's `Puller` is one bounded queue behind an opaque path [ARCHITECTURE §6a P4] and its ordering is `None` [GUARANTEES §6] |
| PUB v0 (`0x20`) | `Publisher` [ARCHITECTURE §6b] | no — the filter is on the wrong side | PUB offers **every** message to **every** connected subscriber without testing subscriptions [nanomsg-nng §4 "PUB/SUB filtering locus"]; weida filters at the publisher [ARCHITECTURE §6c.2]. §6 and L1 |
| SUB v0 (`0x21`) | `Subscriber` [ARCHITECTURE §6b] | yes on delivery, no on the filter | SUB matches the initial body bytes locally; an empty subscription admits everything [nanomsg-nng §3, §4]. The adapter owns the local match when bridging inbound (§6) |
| SURVEYOR v0 (`0x62`) | fan-out of exchanges with a deadline — *mapped, unimplemented* [ARCHITECTURE §6b] | no counterpart exists yet | A survey is one send to every respondent plus at most one reply each within `SURVEYTIME`, started at send [nanomsg-nng §4]. weida has no deadline-scoped fan-out of exchanges. Until §6b's row is built, the adapter refuses this pairing (§9.4); the honest interim shape is a weida `Publisher` for the survey plus a `Puller` for the answers, which is **not** the same object and MUST NOT be presented as one |
| RESPONDENT v0 (`0x63`) | `Replier` per survey | partly | A respondent may simply not answer, and the surveyor cannot tell silence from slowness [nanomsg-nng §4 "Survey time boundary"]. weida's `IncomingRequest` must be answered or refused; "no answer" is a deadline at the requester, not a protocol state |
| PAIR v0 (`0x10`) | one connection, one exchange or one one-way transfer each way — *mapped, unimplemented* [ARCHITECTURE §6b] | n/a | PAIR v0 has no protocol header and is the legacy interoperable form [nanomsg-nng §4]. Its exclusivity — a peer rejects a second connection while paired [nanomsg-nng §4 matrix] — has no weida equivalent (L6) |
| PAIR v1 (`0x11`) | as PAIR v0 | n/a | Adds a 32-bit hop-count header bounded by `MAXTTL` (1-255, commonly 8) [nanomsg-nng §3, §4, §11]. weida has no forwarding layer in v0, so the count is consumed and never propagated |
| PAIR v1 polyamorous | not mapped | no | The destination is a *pipe handle*, not an address; an unavailable directed pipe discards silently and cannot route through devices [nanomsg-nng §4]. weida addresses by opaque endpoint path [INVARIANTS]; there is no handle to carry. Deprecated upstream [nanomsg-nng §4] |
| BUS v0 (`0x70`) | n peers, each a `Peer` plus an `Acceptor` on the same path — *mapped, unimplemented* [ARCHITECTURE §6b] | no | BUS is one hop to *directly connected* peers only, and needs a fully connected mesh to be a bus [nanomsg-nng §4]. weida has no mesh membership concept. Refused until §6b's row is built (§9.5) |
| Raw variants of all of the above | — | n/a | Raw preserves the wire headers and moves state machines, retries, matching and loop control to the application [nanomsg-nng §4, §11]. The adapter speaks cooked (§1); a raw *peer* is fine, a raw adapter is not |

## 3. Stream mapping

**One SP message is one weida transfer, and therefore one QUIC stream** — "one data flow maps
naturally to one transport stream" [INVARIANTS], P1/P2 of [ARCHITECTURE §6a]. A weida transfer
is a DATA header followed by opaque payload bytes until FIN [PROTOCOL.md](../PROTOCOL.md) §4.

**The SP wire, exactly.** On the TCP mapping both sides send an 8-byte protocol header
immediately after the TCP handshake and MUST wait for the peer's before proceeding
[nanomsg-nng §1]; the octets are `0x00 0x53 0x50 <version>`, then a 16-bit big-endian endpoint
type, then 16 bits of zero reserved, and a peer whose first four bytes differ or whose
reserved field is nonzero MUST be disconnected [rfc-tcp §2]. Each message is a 64-bit
big-endian size followed by exactly that many payload bytes [rfc-tcp §3]. The endpoint type is
a 12-bit protocol ID plus a 4-bit role [rfc-ids §1], which gives the constants in §2's first
column [nng-src]. There is no multiplexing: the mapping says independent message streams are
separate TCP connections [rfc-tcp §4] — the same choice weida made per dialled path
([0002](../decisions/0002-control-and-bulk-separation.md) §6.2), for a different reason.

**The SP connection is not one weida connection.** A socket owns many endpoints, each creating
pipes that map 1:1 to TCP connections [nanomsg-nng §1, §2]; the adapter holds one weida
connection per dialled endpoint path [0002 §6.2] and the connections of one peer are bound by
the proved fingerprint ([0008](../decisions/0008-session-identity.md) §4.2). One SP socket
therefore becomes several weida connections, and the round-robin over them is `PeerSet::pick`
[ARCHITECTURE §5] — the same selection rule PUSH and REQ already use [nanomsg-nng §4]. No
separate control connection is needed: a frame that names a path rides that path's connection
([0011](../decisions/0011-answered-where-it-arrived.md) §4.2).

**Protocol headers are consumed, never forwarded.** An `nng_msg` keeps protocol header and
application body in separate storage [nanomsg-nng §2], but on the wire the header is simply
the first bytes of the message. The adapter strips:

- **REQ/REP.** A stack of 32-bit big-endian tags, MSB clear for forwarder peer IDs and MSB set
  on the final request ID [nanomsg-nng §3], which is how the reply is routed back by popping
  [nanomsg-nng §4 "REQ reverse routing"]. The adapter keeps the stack per in-flight exchange
  and writes it back onto the reply; weida needs none of it, because the reply half of the
  same stream *is* the correlation [PATTERNS §1.2].
- **SURVEYOR/RESPONDENT.** The same stack with a survey ID [nanomsg-nng §3, §4].
- **PAIR v1.** One 32-bit hop count [nanomsg-nng §3], checked against the local `MAXTTL`
  [nanomsg-nng §4] and then dropped.
- **BUS.** Cooked BUS has no header; raw BUS carries the ingress pipe ID [nanomsg-nng §3].
- **PUB/SUB.** Nothing to strip: the topic *is* the leading bytes of the body [nanomsg-nng §3].
  See §6 — this is the one place where stripping would be wrong.

**Sizes.** A message may declare up to 2^64-1 bytes [rfc-tcp §3]; the only inbound defence is
`NNG_OPT_RECVMAXSZ`, zero meaning unlimited, an oversized message being discarded
[nanomsg-nng §5, §8], and it should be set per endpoint before the endpoint starts
[nanomsg-nng §1, §9]. The adapter therefore MUST carry its own `max_message_bytes`, set
`RECVMAXSZ` on its own endpoints to it, and cap **before** allocating, which is the same rule
weida's own decoder follows for `max_header_bytes` [PROTOCOL §3.1], [INVARIANTS]. On the weida
side the neighbours are `subscriber_buffer_bytes` (8 MiB) [PROTOCOL §10] and
`stream_receive_window` (1 MiB) [PROTOCOL §10].

**Streaming does not survive.** NNG "delivers a message wholly or not at all" and exposes no
streaming body [nanomsg-nng §2, §12/P13]; weida's payloads "may remain streams end-to-end"
[INVARIANTS]. Outbound the adapter MUST buffer a whole weida transfer before it can emit the
64-bit size, bounded by `max_message_bytes`, and refuse beyond it with
`STOP_SENDING(REJECTED)` [PROTOCOL §9.3] rather than truncate. Named loss L2.

**Liveness and reconnect.** SP has no common heartbeat: TCP keepalive is a transport option
and only ZeroTier documents ping-based death detection [nanomsg-nng §1, §12/P2]. weida has
`idle_timeout` (30 s) and a keep-alive from the dialling side (10 s) [PATTERNS §1.8]. A dialer
redials after a pipe closes with exponential backoff between `RECONNMINT` and `RECONNMAXT`
[nanomsg-nng §1]; weida does not reconnect by itself — the application calls `connect` again
and a `Subscriber` re-sends its filters then [PATTERNS §1.8]. Neither timer is translated into
the other: they bound different hops. Nothing is resumed on either side — SP specifies no
session resumption, durable session or last will [nanomsg-nng §1, §12/P6], and weida's proved
identity "carries no session" [0008 §4.4], which is the one place the two protocols agree
exactly.

## 4. Credit and backpressure mapping

| Side | Unit | Granted by | At the bound |
| --- | --- | --- | --- |
| SP socket queues | **messages**, `SENDBUF`/`RECVBUF`, 0-8192, not all protocols support them [nanomsg-nng §5, §11] | the local socket, not the peer — "no receiver-granted credit" [nanomsg-nng §12/P12] | protocol-specific: PUSH/PAIR block or time out, BUS drops, SUB drops oldest or rejects newest [nanomsg-nng §5, §8] |
| SP inbound size | **bytes**, `RECVMAXSZ`, zero = unlimited [nanomsg-nng §5] | the receiver, locally | the message is discarded [nanomsg-nng §8] |
| weida L0 | **bytes** (`stream_receive_window`, `connection_receive_window`) and **streams** (`max_concurrent_uni_streams`, `max_concurrent_bidi_streams`) | the receiver, through QUIC transport parameters ([0003](../decisions/0003-credit-unit.md) §4.1) | the sender stalls; there is no application credit frame [0003 §4.1] |

Three consequences, each of them a mapping rule:

1. **SP's message-depth queue is not a credit signal and MUST NOT be presented as one.** It is
   a local buffer depth [nanomsg-nng §5, §12/P12]; weida's message credit is the peer-granted
   concurrent-stream budget [0003 §4.1]. The adapter may *size* its stream budget from
   `RECVBUF`, but the number it grants is its own.
2. **`RECVMAXSZ` is the byte pairing, and it is the one that transfers cleanly**: both sides
   bound a single inbound unit in bytes before allocating [nanomsg-nng §5], [PROTOCOL §3.1].
3. **Overload policy must pair like with like** [0006 §4.7]. `Block` pairs with PUSH/PULL and
   PAIR, which block [nanomsg-nng §5]; `Drop` pairs with PUB/SUB and BUS, which discard
   [nanomsg-nng §5]. Bridging a dropping source onto a blocking weida pattern converts a
   deliberate drop into backpressure on the publisher, and the reverse converts backpressure
   into silent loss; both are refused (§9.1).

## 5. Identity, security and authorization

- **SP itself has none.** "The SP pattern protocols define no message-level authentication,
  authorization, or authenticated sender identity" [nanomsg-nng §10, §12/P14]. There is
  therefore nothing to map onto weida's `IncomingMeta::peer`, and no SP field may be presented
  as one: an identity weida reports is one that was *proved*, never one that was claimed
  [0008 §4.4], [GUARANTEES §6].
- **TLS transport.** TLS 1.2 over TCP with configurable auth mode, CA file, certificate and
  key, verification result, peer common name and alternative names [nanomsg-nng §10]; WSS
  exposes the same family [nanomsg-nng §10]. This authenticates the *transport peer of the
  adapter's own SP connection* and terminates at the adapter. weida's identity is a proved
  public-key fingerprint over its own TLS 1.3 handshake [GUARANTEES §6], and the two are
  different hops: the adapter MAY authorize on the SP-side certificate, and MUST NOT forward
  it as a weida peer identity.
- **IPC peer credentials.** The IPC transport can expose OS-derived UID, GID, PID and zone ID,
  described as non-forgeable at connection time [nanomsg-nng §10]. That is exactly the local
  principal weida's own `AF_UNIX` transport proves
  ([0010](../decisions/0010-local-transport.md) §4.4) — the one SP identity mechanism with a
  faithful weida counterpart, and only for a local hop. A PID remains an observation and must
  not be authorized on [0010 §4.4].
- **Pipe-add-pre callbacks** may reject a pipe before it joins the socket; the manual calls
  this application policy, not SP authorization [nanomsg-nng §10]. In the adapter it is where
  a configured allow-list runs, and it is named as adapter policy in the configuration, not as
  a protocol guarantee.
- **ZeroTier** admission states and optional persistent node identity [nanomsg-nng §10] are
  out of scope with the transport (§1).

## 6. Topic or address mapping

[0007](../decisions/0007-topic-namespace.md) §5 has **no row group for SP**: its ZMTP rows
cover publisher-side byte-prefix subscriptions [0007 §5], and SP's are neither on that side nor
a wire construct at all. The rows below are what an SP adapter may claim, in 0007 §5's shape;
adding them to that table is a follow-up for the decision (§11).

| SP construct | SP semantics | weida filter | Exact? | Named loss / adapter obligation |
| --- | --- | --- | --- | --- |
| SUB subscription, prefix ending at a `.` boundary | an arbitrary byte prefix of the body, matched **at the subscriber** [nanomsg-nng §3, §4] | `P.#` | yes, for a boundary-aligned `P` | none beyond the boundary assumption itself, which SP does not make — the adapter must know the publisher's topic convention, and the configuration states it |
| SUB subscription, prefix ending mid-segment | same [nanomsg-nng §4] | — | no | the loss 0007 §4.5 already names for ZMTP: subscribe at the enclosing boundary and re-apply the byte prefix locally, or refuse |
| SUB empty subscription | admits all publications [nanomsg-nng §4] | `""` or `#` | yes | none [PROTOCOL §6.4] |
| Several subscriptions on one SUB socket | a local set, no documented count limit [nanomsg-nng §11] | one weida filter each, `max_subscriptions` = 256 per connection [PROTOCOL §10] | no | a socket with more subscriptions than the weida cap is refused at configuration time, never silently truncated |
| The topic itself | the leading bytes of the body; no separate field and no typed metadata [nanomsg-nng §3] | `topic` in the DATA header [PROTOCOL §6.4] | no | **outbound** the adapter prepends the topic to the body (and the far side must agree where it ends); **inbound** it must split body into topic and payload by the configured convention. Either way the split is adapter configuration, not SP |
| Where matching happens | at the subscriber, after the publisher has sent every publication to every subscriber link [nanomsg-nng §4] | at the publisher [ARCHITECTURE §6c.2] | no | L1: bridging inbound moves work and bandwidth *off* the wire, which is fine; bridging outbound to SP subscribers means the adapter must send everything a SUB peer might want and let it filter |
| Endpoint addressing | URLs per transport, bounded by `NNG_MAXADDRLEN`; legacy IPC paths ≤ 122 bytes [nanomsg-nng §11] | opaque endpoint paths ≤ 512 bytes [INVARIANTS], [PROTOCOL §10] | no | an SP URL is not an endpoint path; the adapter's configuration maps one to the other explicitly and MUST NOT derive a path from a URL |

## 7. Transfer points and guarantee mapping

**SP has no transfer point.** It defines no application acknowledgement, no broker receipt, no
transaction and no persistence signal [nanomsg-nng §6, §12/P7]; BUS, PUB/SUB, PAIR and
PUSH/PULL explicitly promise best effort [nanomsg-nng §6]. This is [0006 §4.6]'s second case,
in that decision's own words: the chain ends at the adapter's own local queue, and this
document says so. Concretely:

- **Inbound (SP → weida).** Responsibility passes to weida when the adapter's weida-side
  transfer reports its transport receipt [GUARANTEES §1]; the SP sender learns nothing of that
  and never will. The set the edge carries is `core` [0006 §4.2].
- **Outbound (weida → SP).** The weida guarantee chain ends when the adapter has handed the
  message to its SP socket — the moment SP's own manual calls best effort [nanomsg-nng §6].
  A weida `Delivery` the adapter awaits proves the *weida* hop only.

**REQ's reply is the one exception, and it is not an acknowledgement.** Receipt of a matching
reply stops retransmission and is the only built-in completion signal, and it "does not prove
a remote side effect occurred exactly once" [nanomsg-nng §6]. It maps onto weida's rule that a
reply proves strictly more than a receipt [PATTERNS §1.2], with one difference that matters:
weida never resends, SP does.

**Duplicates.** REQ retransmits on timer, disconnect or peer availability, and a reply lost
after the REP acted produces a duplicate request with no deduplication key beyond the routing
header [nanomsg-nng §6, §7, §12/P9]. Surveys can duplicate responses in some topologies
[nanomsg-nng §7]. So:

- Inbound, the adapter's weida side is `Deduplication = None` under `core` [GUARANTEES §6],
  which means **the duplicate reaches the weida application**. An operator who needs
  suppression configures `Bounded` deduplication on the weida side and the adapter supplies
  the identity, which it must synthesize (the SP request ID is 31 bits, unique only per
  requester context and restarted at random [rfc-reqrep §5]) — that synthesis is named in the
  configuration (§9.3), never implicit.
- The honest default is the one SP's own recipes give: make the request idempotent
  [nanomsg-nng §9].

**Ordering.** SP's generic contract permits drops and reordering [nanomsg-nng §7, §12/P8];
weida's `core` ordering is `None` [GUARANTEES §6]. Equal, and equally weak — nothing is lost
in the mapping. A weida side configured `PerProducer` cannot be fed faithfully from SP, because
SP has no producer sequence to carry (§9.2).

## 8. Named losses

1. **Subscription filtering changes sides.** SP filters at the subscriber, after every copy has
   crossed every link [nanomsg-nng §4]; weida filters at the publisher [ARCHITECTURE §6c.2].
   Inbound this is an improvement and still a change: a foreign PUB expects its bandwidth to be
   spent, and an operator measuring links will see different numbers. Outbound, the adapter
   must deliver everything its SUB peers could want, so a weida filter cannot reduce SP-side
   traffic.
2. **Streaming does not survive.** NNG delivers whole messages only [nanomsg-nng §2]; a weida
   payload may be a stream [INVARIANTS]. The adapter buffers whole messages under
   `max_message_bytes` (§3).
3. **REQ's automatic retry has no weida counterpart.** weida reports a failed exchange; SP
   silently tries again [nanomsg-nng §4, §6]. Outbound, a weida requester's single attempt
   becomes a single SP request — the adapter does not retry on its behalf, because a retry is
   an application decision weida deliberately leaves to the application [FAILURE_MODEL §4].
4. **REQ retries arrive as duplicate requests.** Under `core` they are delivered
   [nanomsg-nng §6], [GUARANTEES §6]; see §7 and §9.3.
5. **A survey deadline is not carried.** `SURVEYTIME` starts when the survey is sent and a late
   reply is discarded, which makes "no answer" indistinguishable from slow, unreachable or
   deliberately silent [nanomsg-nng §4]. weida has no deadline-scoped fan-out of exchanges
   [ARCHITECTURE §6b], so the adapter must impose the deadline itself and report expiry as its
   own observation.
6. **PAIR's exclusivity is not representable.** A PAIR peer rejects a second connection while
   paired [nanomsg-nng §4]; weida's `Acceptor` admits every peer that reaches the path, bounded
   only by `max_connections_per_peer` [PROTOCOL §10]. An adapter that presents a PAIR socket
   must enforce the one-peer rule itself and name it as adapter policy.
7. **BUS is one hop, and weida has no mesh.** BUS reaches only directly connected peers and
   needs a fully connected mesh to behave like a bus, with delivery to "some, all, or none"
   [nanomsg-nng §4]. Nothing in weida reproduces that membership model; §9.5 refuses it.
8. **Who binds is the adapter's choice.** SP lets either role listen or dial [nanomsg-nng §1];
   weida fixes bind/connect per pattern in v0 [ARCHITECTURE §6c.4]. An SP topology that relies
   on the opposite direction needs the adapter to bind on both sides, which is a configuration,
   not a translation.
9. **`RECVMAXSZ = 0` cannot be honoured.** Zero means unlimited [nanomsg-nng §5, §11]; weida
   may not allocate on unbounded remote input [INVARIANTS]. The adapter always has a finite cap
   (§9.6).

## 9. Configurations the adapter refuses

Refusal is at configuration time, which is both the guarantee rule [GUARANTEES §4] and the
default at an adapter edge [0006 §4.7]. Degradation exists only as an explicitly named
configuration entry [0006 §4.7]. The adapter rejects, naming the reason:

1. **A dropping SP source bridged onto a blocking weida pattern, or the reverse.** PUB/SUB and
   BUS drop [nanomsg-nng §5]; Push/Pull and PAIR block [nanomsg-nng §5]; weida's `core`
   backpressure is `Block` with `Drop` for fan-out only [GUARANTEES §6]. Backpressure levels
   are behaviours, not strengths, and two sides must state the same one [0006 §4.3].
2. **A weida side requiring `Ordering = PerProducer` or stronger.** SP carries no producer
   sequence and permits reordering [nanomsg-nng §7]; there is nothing to derive the number
   from, and inventing one would be the adapter claiming a guarantee its source cannot provide
   [INVARIANTS].
3. **Deduplication of REQ retries without a named identity source.** `Bounded` deduplication
   needs an identity [GUARANTEES §6]; the SP request ID is 31 bits, per-context and randomly
   seeded [rfc-reqrep §5], so the adapter's synthesized identity — and its window — must be
   spelled out in the configuration or the configuration is refused.
4. **SURVEYOR/RESPONDENT onto weida patterns**, until [ARCHITECTURE §6b]'s row is built: the
   deadline-scoped fan-out of exchanges does not exist, and approximating it with a publisher
   plus a puller loses the per-respondent reply correlation (L5).
5. **BUS onto weida**, for the same reason plus the mesh (L7).
6. **`RECVMAXSZ = 0` or no `max_message_bytes`.** Unbounded inbound size is refused
   [INVARIANTS], [nanomsg-nng §5].
7. **Any configuration that makes the adapter the durable hop** — storing before forwarding —
   which is L2 work and refused by [0006 §4.8]. SP has no persistence to inherit anyway
   [nanomsg-nng §12/P7].
8. **Raw sockets on the adapter's own side** (§1), and `nng_device()` forwarding
   [nanomsg-nng §4].

## 10. Interop bench plan

Built as Phase B slice 5, after the codec (slice 1) and the two bridge directions [LOOP §9].

**Upstream under test — and the problem with it.** The sheet names three Rust options and all
three are *bindings*: `nng` (binding for the C library), `runng` (async wrapper) and
`nanomsg-rs` (legacy libnanomsg) — "these are bindings, not independent SP wire
specifications" [nanomsg-nng §13]. There is no pure-Rust SP implementation in the sheet. The
one independent implementation it does name is **mangos**, the Go implementation NNG itself
calls conforming for the common SP subset [nanomsg-nng §13].

Consequences, following [LOOP §2] and [LOOP §5]:

- The bench's default upstream is the `nng` crate as an **optional dev-dependency**; every test
  that needs it is `#[ignore]` with the build requirement in its doc comment, because it builds
  the C library. The `#[ignore]` reason is the honest one: "requires the NNG C library".
- Any long-running upstream process — a mangos peer, or an NNG example daemon — runs under the
  process supervisor with a `ready` condition and is stopped in the same item, never as a
  stray background job [LOOP §2].
- Because the primary peer in CI is therefore this repository's own codec, the bench states
  what that cannot prove: a codec that is byte-exact against §10.1 is a faithful SP peer and
  still shares every assumption with the code under test. The `nng` and mangos runs are what
  turn "we agree with ourselves" into interoperability.

**The bench.**

1. **Golden vectors, no I/O** — slice 1: §10.1 below, asserted byte-exact in both directions in
   `crates/adapters/weida-sp/tests/golden_vectors.rs`, the shape [PROTOCOL §8] already requires
   of weida's own codec.
2. **Fuzz target** over the SP decoder, cap-before-allocate on the declared 64-bit size — a
   message may declare up to 2^64-1 bytes [rfc-tcp §3] and `RECVMAXSZ` is the only defence
   [nanomsg-nng §5] — mirroring `max_header_bytes`'s rule [PROTOCOL §3.1]. A stable-Rust smoke
   test runs the same properties under `cargo test`.
3. **Inbound matrix.** NNG REQ → adapter → weida `Replier` (including a forced retransmission,
   observed as a duplicate, L4); NNG PUSH → adapter → `Puller`; NNG SUB ← adapter ← weida
   `Publisher` with a boundary-aligned prefix and a refused mid-segment prefix (§6).
4. **Outbound matrix.** The same three reversed: a weida `Requester` through a foreign REP with
   concurrent exchanges (SP contexts, one request each [nanomsg-nng §2]); a `Pusher` through a
   foreign PULL; a `Subscriber` fed by a foreign PUB with the topic split of §6.
5. **A test per observable named loss.** L1 (filter side: a SUB peer receives publications it
   did not subscribe to when the adapter is outbound), L2 (an oversized payload refused, not
   truncated), L4 (a retransmitted request arrives twice), L5 (a survey deadline expires at the
   adapter), L9 (`RECVMAXSZ = 0` refused at configuration).
6. **Numbers to record.** Round-trip latency and throughput for Req/Rep and Push/Pull through
   the adapter against the same patterns native on both sides, and the message rate at which
   the SP side starts dropping under PUB/SUB — the drop being the protocol's documented answer
   [nanomsg-nng §5], the number being ours.

### 10.1 Golden vectors

Bytes, hexadecimal, most significant first. The protocol header is [rfc-tcp §2] with the type
IDs of [rfc-ids §1] and [nng-src]; the message framing is [rfc-tcp §3]; the REQ/REP and survey
tag stacks are [nanomsg-nng §3] and [rfc-reqrep §5]; the PAIR v1 header is [nanomsg-nng §3]
and [nng-src].

| # | What | Bytes |
| --- | --- | --- |
| 1 | Protocol header, REQ v0 (`0x0030`) | `00 53 50 00 00 30 00 00` |
| 2 | Protocol header, REP v0 (`0x0031`) | `00 53 50 00 00 31 00 00` |
| 3 | Protocol header, PUB v0 (`0x0020`) | `00 53 50 00 00 20 00 00` |
| 4 | Protocol header, SUB v0 (`0x0021`) | `00 53 50 00 00 21 00 00` |
| 5 | Protocol header, PUSH v0 (`0x0050`) | `00 53 50 00 00 50 00 00` |
| 6 | Protocol header, PULL v0 (`0x0051`) | `00 53 50 00 00 51 00 00` |
| 7 | Protocol header, SURVEYOR v0 (`0x0062`) | `00 53 50 00 00 62 00 00` |
| 8 | Protocol header, RESPONDENT v0 (`0x0063`) | `00 53 50 00 00 63 00 00` |
| 9 | Protocol header, BUS v0 (`0x0070`) | `00 53 50 00 00 70 00 00` |
| 10 | Protocol header, PAIR v0 (`0x0010`) | `00 53 50 00 00 10 00 00` |
| 11 | Protocol header, PAIR v1 (`0x0011`) | `00 53 50 00 00 11 00 00` |
| 12 | Empty message | `00 00 00 00 00 00 00 00` |
| 13 | Message, body `hi` | `00 00 00 00 00 00 00 02 68 69` |
| 14 | REQ message, request ID 1, body `ping` | `00 00 00 00 00 00 00 08 80 00 00 01 70 69 6e 67` |
| 15 | REQ message through one device: peer ID 7, request ID 1, body `ping` | `00 00 00 00 00 00 00 0c 00 00 00 07 80 00 00 01 70 69 6e 67` |
| 16 | REP reply to #14 (same tag, body `pong`) | `00 00 00 00 00 00 00 08 80 00 00 01 70 6f 6e 67` |
| 17 | SURVEYOR message, survey ID 42, body `who` | `00 00 00 00 00 00 00 07 80 00 00 2a 77 68 6f` |
| 18 | PAIR v1 message as sent by a cooked socket (hop count 0), body `hi` | `00 00 00 00 00 00 00 06 00 00 00 00 68 69` |
| 19 | PAIR v1 message after one forwarding hop, body `hi` | `00 00 00 00 00 00 00 06 00 00 00 01 68 69` |
| 20 | PUB message, topic `px.eur` inline, body `px.eur120` | `00 00 00 00 00 00 00 09 70 78 2e 65 75 72 31 32 30` |

Vector 20 is the point of §6 in bytes: there is no topic field, only body [nanomsg-nng §3].

Rejection vectors, for the decoder and the fuzz target:

| # | What | Bytes | Required behaviour |
| --- | --- | --- | --- |
| R1 | Wrong magic | `00 53 51 00 00 30 00 00` | close the connection [rfc-tcp §2] |
| R2 | Nonzero reserved | `00 53 50 00 00 30 00 01` | close the connection [rfc-tcp §2] |
| R3 | Unknown version | `00 53 50 01 00 30 00 00` | close the connection [rfc-tcp §2] |
| R4 | Declared size beyond the cap | `ff ff ff ff ff ff ff ff …` | refuse before allocating [PROTOCOL §3.1], [nanomsg-nng §5] |
| R5 | REQ tag stack with no terminator (MSB never set) | `00 00 00 00 00 00 00 04 00 00 00 07` | malformed; ignore the message [rfc-reqrep §5] |
| R6 | Reply shorter than one tag | `00 00 00 00 00 00 00 02 80 00` | malformed; ignore the message [rfc-reqrep §5] |

## 11. Open questions

- **The sheet has no byte-level layouts.** It states the 8-byte greeting, the header stacks and
  the PAIR hop count in prose [nanomsg-nng §1, §3] but gives neither the octets nor the numeric
  endpoint type IDs, so §3 and §10.1 had to go to the SP RFCs and the NNG source directly. The
  research item that settles this is an update to
  [nanomsg-nng.md](../research/nanomsg-nng.md) §1/§3 carrying the octets with their RFC
  sections.
- **PAIR v1's initial hop count.** The sheet says the counter is "initialized to one and
  incremented at each node" [nanomsg-nng §4]; NNG's current source appends `0` on a cooked send
  and increments on receipt [nng-src], which is what vectors 18 and 19 encode. The two differ by
  one, and interoperability depends on which the peer implements. Settled by testing against a
  real NNG peer (§10 item 3), which is the first thing the bench should check.
- **No row group in [0007](../decisions/0007-topic-namespace.md) §5.** §6's rows are proposed
  here; whether receiver-side prefix filtering deserves its own rows in that table, given that
  it differs from ZMTP's only in *where* matching happens, is the decision owner's call.
- **Which upstream the bench treats as normative** when the `nng` C library and mangos disagree.
  The sheet names mangos as conforming for the "common SP subset" [nanomsg-nng §13] without
  saying what that subset excludes, so a disagreement has no tie-breaker today.
- **Whether the adapter should ever present a raw socket.** §1 says no, because the adapter has
  no application to delegate omitted state to; a device-like weida deployment might argue
  otherwise, and that is an L2 question [nanomsg-nng §4], [ARCHITECTURE §1].

## 12. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §3.1, §4, §6.4, §8, §9.3, §10;
[PATTERNS.md](../PATTERNS.md) §1.2, §1.8; [GUARANTEES.md](../GUARANTEES.md) §1, §4, §6;
[ARCHITECTURE.md](../ARCHITECTURE.md) §1, §5, §6a, §6b, §6c.2, §6c.4;
[FAILURE_MODEL.md](../FAILURE_MODEL.md) §4; [INVARIANTS.md](../INVARIANTS.md);
[LOOP.md](../LOOP.md) §2, §5, §9.

Decisions: [0002](../decisions/0002-control-and-bulk-separation.md) §6.2;
[0003](../decisions/0003-credit-unit.md) §4.1;
[0006](../decisions/0006-guarantee-sets.md) §4.2, §4.3, §4.6, §4.7, §4.8, §4.9;
[0007](../decisions/0007-topic-namespace.md) §4.5, §5;
[0008](../decisions/0008-session-identity.md) §4.2, §4.4;
[0010](../decisions/0010-local-transport.md) §4.4;
[0011](../decisions/0011-answered-where-it-arrived.md) §4.2.

Research sheet: [nanomsg-nng.md](../research/nanomsg-nng.md) §0, §1, §2, §3, §4, §5, §6, §7,
§8, §9, §10, §11, §12 (P1, P2, P3, P6, P7, P8, P9, P12, P13, P14, P18), §13.

Primary sources consulted directly, because the sheet carries them only in prose (§11):

- `[rfc-tcp]` — "TCP mapping for Scalability Protocols", sp-tcp-mapping-01, §2 (protocol
  header), §3 (message delimitation), §4 (no multiplexing):
  https://github.com/nanomsg/nanomsg/blob/master/rfc/sp-tcp-mapping-01.txt
- `[rfc-ids]` — "List of SP protocol IDs", sp-protocol-ids-01, §1-§2 (12-bit protocol ID plus
  4-bit endpoint role; pair 1, pubsub 2, reqrep 3, pipeline 5, survey 6, bus 7):
  https://github.com/nanomsg/nanomsg/blob/master/rfc/sp-protocol-ids-01.txt
- `[rfc-reqrep]` — "Request/reply protocol", sp-request-reply-01, §5 (32-bit tag stack, MSB set
  on the final request ID, 31-bit request IDs seeded at random, malformed-reply rules):
  https://github.com/nanomsg/nanomsg/blob/master/rfc/sp-request-reply-01.txt
- `[nng-src]` — NNG `src/core/protocol.h` (`NNI_PROTO(major, minor) = major * 16 + minor` and
  the protocol registry table) and `src/sp/protocol/*/[a-z]*.c` (`REQ0_SELF 0x30`,
  `REP0_SELF 0x31`, `SURVEYOR0_SELF 0x62`, `PAIR1_SELF 0x11`, and the PAIR v1 hop-count
  handling): https://github.com/nanomsg/nng
