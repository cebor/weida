# ZMTP 3.1 — adapter mapping

Status: mapping document. No adapter crate exists yet; this is the design a
`crates/adapters/weida-zmtp` must implement, and the contract its documentation owes its user
([LOOP.md](../LOOP.md) §9 Phase B slice 2, [0006](../decisions/0006-guarantee-sets.md) §4.9).
Date: 2026-09-11
Derived from: [docs/research/zeromq.md](../research/zeromq.md) (ZMTP 3.1, libzmq 4.3.x, the
zguide, CURVE/ZAP). Every ZeroMQ claim below carries that sheet's section; every weida claim
carries the weida document or decision it comes from. Where the two cannot be made to
coincide, §8 names the loss instead of hiding it.

## 1. Scope

Two directions, built as separate slices [LOOP §9 Phase B]:

- **Inbound.** Foreign ZeroMQ peers speak ZMTP to the adapter; the adapter speaks weida to the
  weida network.
- **Outbound.** weida endpoints reach a foreign ZeroMQ peer or proxy through the adapter.

The adapter is a **hop**, not a tunnel. It terminates ZMTP — greeting, mechanism, framing,
subscriptions — and it terminates weida; nothing is forwarded opaquely. That is what makes
"all guarantees are defined against the immediate next hop"
([INVARIANTS.md](../INVARIANTS.md)) checkable at this edge, and it is why §7 can name exactly
where the chain ends.

Out of scope for the first slices: the draft thread-safe socket family, `pgm`/`epgm`, `udp`,
`vmci`, `tipc`, `vsock` and `ws`/`wss` [zeromq §12/P18]; `ZMQ_STREAM` (a raw-TCP shim, not a
ZMTP peer) [zeromq §4.6]; and any ZeroMQ application protocol (MDP, CHP, FLP, ZRE), which
sits above ZMTP and is the application's business [zeromq §0].

## 2. Socket type to weida pattern

weida's own mapping of the family is [ARCHITECTURE.md](../ARCHITECTURE.md) §6b; this table is
that mapping made concrete for a bridge, with the direction rule of
[ARCHITECTURE §6c.4] — Rep, Pull and Pub bind; Req, Push and Sub connect.

| ZeroMQ socket | weida counterpart | Faithful? | What the adapter must do |
| --- | --- | --- | --- |
| `REQ` | one exchange: `Requester` / `Peer::open_bi` [ARCHITECTURE §6a P2] | yes, and weida is stricter about nothing | REQ is lockstep — "SHALL send and then receive exactly one message at a time", `EFSM` otherwise [zeromq §4.2]. Inbound, the bridge holds one exchange per REQ peer at a time; it MUST NOT use weida's concurrency to accept a second request from the same REQ socket, because that socket cannot produce one |
| `REP` | `Replier` [ARCHITECTURE §6b] | yes | REP discards a reply whose originator vanished [zeromq §4.2]; weida's reply half fails with `Error::Canceled` when the requester dropped it ([FAILURE_MODEL.md](../FAILURE_MODEL.md) §4). Equivalent behaviour, different observability: weida tells the replier, ZeroMQ does not |
| `DEALER` | `Requester::open`, unlimited concurrent exchanges [ARCHITECTURE §6a] | yes, without the envelope | DEALER exists because one ZeroMQ socket is one ordered pipe [ARCHITECTURE §6b]; weida needs no DEALER type. The bridge consumes DEALER's empty-delimiter envelope and MUST NOT forward it as payload [zeromq §4.2] |
| `ROUTER` | `Replier` plus the connection identity [ARCHITECTURE §6b] | partly | ROUTER's identity frame is a local connection handle [zeromq §10]; weida answers on the reply half of the same stream, so the envelope is consumed at the bridge. Third-party routing — ROUTER forwarding to a peer that did not ask — is broker work and is **not** in this adapter [ARCHITECTURE §6b] |
| `PUSH` | `Pusher` [ARCHITECTURE §6b] | yes | Both block rather than drop: PUSH blocks at its HWM and "SHALL NOT discard" [zeromq §4.4], weida's Push/Pull backpressure is `Block` ([GUARANTEES.md](../GUARANTEES.md) §6) |
| `PULL` | `Puller` [ARCHITECTURE §6b] | yes | PULL fair-queues its peers; weida's `Puller` is one bounded queue behind an opaque path [ARCHITECTURE §6a P4] |
| `PUB` | `Publisher` [ARCHITECTURE §6b] | yes on the overload policy, no on the filter | Both drop per slow subscriber and never block: PUB "SHALL silently drop the message if the queue for a subscriber is full" [zeromq §4.3], weida drops past `subscriber_buffer_bytes` and counts it [GUARANTEES §6], [PATTERNS.md](../PATTERNS.md) §4. Filter semantics differ — §6 |
| `SUB` | `Subscriber` [ARCHITECTURE §6b] | yes on delivery, no on the filter | Both filter at the publisher [zeromq §4.3], [ARCHITECTURE §6c.2] |
| `XPUB` | `Publisher` — **no counterpart for the subscription stream** | no | XPUB delivers subscription commands to the application and synthesizes an unsubscribe when a subscriber drops [zeromq §4.3]. weida exposes `Publisher::filter_count` and `peer_count` and no subscription event ([PATTERNS §4], `crates/weida/src/endpoint.rs`). Named loss L7 |
| `XSUB` | `Subscriber` | partly | XSUB forwards subscriptions upstream and re-sends them on reconnect [zeromq §4.3]; weida's `Subscriber` re-sends its filters on the next `connect` [PATTERNS §1.8], which is the same behaviour for the proxy case |
| `PAIR` | mapped, unimplemented [ARCHITECTURE §6b] | n/a | PAIR is `inproc`-shaped and does not auto-reconnect [zeromq §4.5]; a bridge for it waits until weida ships the type |
| `RADIO`/`DISH` | `Publisher`/`Subscriber` with a wildcard-free filter | yes | Groups are exact-match strings [zeromq §4.6] and map to a weida filter containing no `*` and no `#` ([0007](../decisions/0007-topic-namespace.md) §5). libzmq caps a group at 16 bytes including the null [zeromq §11], well inside weida's 256 B filter cap ([PROTOCOL.md](../PROTOCOL.md) §6.4) |
| `SCATTER`/`GATHER` | `Pusher`/`Puller` | yes | "the thread-safe version of the pipeline pattern", same round-robin and fair-queue rules [zeromq §4.6] |
| `CLIENT`/`SERVER` | `Requester`/`Replier` | partly | CLIENT/SERVER forbid multipart outright [zeromq §2], which removes loss L1 for this pair; the 32-bit routing id is the connection handle and is consumed at the bridge [zeromq §4.6] |
| `PEER`/`CHANNEL` | not mapped | no | PEER mixes bind and connect on one socket and lets either side speak first [zeromq §4.6]; weida fixes bind/connect per pattern in v0 [ARCHITECTURE §6c.4]. Revisit when weida ships PAIR/BUS |
| `ZMQ_STREAM` | out of scope | n/a | Raw TCP peers, not ZMTP [zeromq §4.6] |

## 3. Stream mapping

**One ZMTP message is one weida transfer, and therefore one QUIC stream** — "one data flow
maps naturally to one transport stream" [INVARIANTS], P1 and P2 of [ARCHITECTURE §6a]. A
weida transfer is a DATA header followed by opaque payload bytes until FIN [PROTOCOL §4].

**The ZMTP connection is not one weida connection.** After [0002](../decisions/0002-control-and-bulk-separation.md)
§6.2-§6.3 the adapter holds one control connection per weida peer and one bulk connection per
dialled path, bound together by the proved fingerprint
([0008](../decisions/0008-session-identity.md) §4.2). A ZeroMQ socket that connects to several
endpoints [zeromq §2] therefore maps to several weida bulk connections, and the adapter's
round-robin over them is `PeerSet::pick` [ARCHITECTURE §5], which is the same selection rule
PUSH and REQ use [zeromq §4.1].

**Multipart does not survive.** ZMTP multipart is "multiple sequential ZMTP messages, where
all but the last message has the MORE flag set", delivered atomically, "all frames or none"
[zeromq §3]. weida has "no message-part concept anywhere in v0" and deliberately so: a QUIC
stream is already a framed ordered byte sequence [ARCHITECTURE §6c.3]. The adapter's rule:

- **ZMTP → weida.** A multipart message is refused, or concatenated under an
  adapter-owned framing the far side must also understand — which is an application protocol
  the adapter invents, not weida semantics. The default is refusal (§9). Envelope frames the
  pattern itself defines (REQ's empty delimiter, ROUTER's identity frame) are *consumed*, not
  forwarded [zeromq §4.2].
- **weida → ZMTP.** One transfer becomes one single-part ZMTP message. Because ZeroMQ cannot
  hand up a body before it is complete — delivery is atomic and "the first part goes on the
  wire only when the last is sent" [zeromq §12/P13] — the adapter MUST buffer the whole
  payload, and MUST therefore carry its own `max_message_bytes` cap: buffering a remote-sized
  payload without a named bound violates "no remote input can cause unbounded memory
  allocation" [INVARIANTS]. Payloads beyond the cap are refused with
  `STOP_SENDING(REJECTED)` [PROTOCOL §9.3], never truncated.

**Sizes.** ZMTP allows a frame body of 2^63-1 octets by grammar, with `ZMQ_MAXMSGSIZE`
(default -1, no limit) as the only inbound defence, and exceeding it *disconnects* the peer
[zeromq §3], [zeromq §11]. The adapter sets `ZMQ_MAXMSGSIZE` on its own sockets to its
`max_message_bytes`, and maps the weida side's caps the other way: `subscriber_buffer_bytes`
(8 MiB) bounds a published payload [PROTOCOL §9.5], `max_header_bytes` (16384) bounds a header
[PROTOCOL §3.1].

**Liveness.** ZMTP 3.1 `PING`/`PONG` with a TTL in tenths of a second, off by default in
libzmq [zeromq §1], [zeromq §11]; weida has `idle_timeout` (30 s) and a keep-alive sent by the
dialling side only (10 s) [PATTERNS §1.8]. The adapter enables `ZMQ_HEARTBEAT_IVL` on its
ZeroMQ sockets rather than relying on TCP, whose timeout "can be roughly 30 minutes"
[zeromq §8], and it does not translate either timer into the other: they bound different
hops.

**Reconnect.** ZeroMQ reconnects automatically to the same endpoint [zeromq §1]; weida does
not reconnect at all — "the application calls `connect` again" [PATTERNS §1.8]. The adapter
owns the weida-side reconnect loop. It must also honour the rule both sheets state
independently: reconnection is not re-registration [zeromq §12/P6],
[SYNTHESIS](../research/SYNTHESIS.md) §2 D8, and weida has no session to resume [0008 §4.5].
On a weida reconnect the adapter re-sends its subscriptions, exactly as `Subscriber` does
[PATTERNS §1.8].

## 4. Credit and backpressure mapping

**Neither protocol has credit on the wire.** ZMTP 3.1 lists credits under "Topics for
Discussion" and not as a feature; what exists is a purely local per-peer high-water mark,
`ZMQ_SNDHWM`/`ZMQ_RCVHWM`, 1000 messages each way by default and inexact in both directions
because kernel buffers sit underneath [zeromq §5], [zeromq §12/P12]. weida likewise carries no
application credit at L0: QUIC's byte windows are the byte credit and the concurrent-stream
budget is the message credit, both receiver-granted by transport parameters
([0003](../decisions/0003-credit-unit.md) §4.1), and `max_concurrent_uni_streams` *is* the
prefetch a consumer grants [0003 §5]. So there is nothing to translate, and the adapter
MUST NOT pretend otherwise.

| ZeroMQ | weida | Mapping |
| --- | --- | --- |
| `ZMQ_SNDHWM`/`ZMQ_RCVHWM`, messages per peer, default 1000 [zeromq §11] | `max_concurrent_uni_streams` / `max_concurrent_bidi_streams`, default 1024 each [PROTOCOL §10] | Both are message-unit bounds per peer, and the numbers are coincidentally close. Not a translation: weida's budget is granted to the peer by transport parameters and blocks `open` [PATTERNS §1.4], the HWM is local and blocks or drops by socket type [zeromq §5] |
| Kernel `ZMQ_SNDBUF`/`ZMQ_RCVBUF` under the HWM, "strange double-buffering" [zeromq §5] | `stream_receive_window` 1 MiB, `connection_receive_window` 16 MiB [PROTOCOL §10] | The weida side bounds bytes, which the ZeroMQ side cannot express at all |
| Block at the bound: PUSH, PULL, REQ, DEALER, PAIR, CLIENT, SCATTER, CHANNEL [zeromq §12/P4] | `Block` [GUARANTEES §6] | Matching. Push/Pull and Req/Rep bridges keep backpressure end to end |
| Drop at the bound: PUB, XPUB, XSUB, RADIO, ROUTER [zeromq §12/P4] | `Drop`, publisher fan-out only [GUARANTEES §6] | Matching **only** for PUB/XPUB/RADIO onto weida Pub/Sub. ROUTER's drop has no weida counterpart — loss L5 |
| `EAGAIN` at the bound: SERVER, PEER, STREAM [zeromq §12/P4] | `Reject`: `read_capped` past its cap, `publish` past `subscriber_buffer_bytes` [GUARANTEES §6] | Different trigger, same shape: a visible local error rather than a silent loss |
| No credit signal anywhere [zeromq §12/P12] | No L0 application credit; L2 credit is per subscription on the control connection [0003 §4.2] | When the L2 broker exists, a ZeroMQ peer still has nothing to grant or consume, so the adapter is the credit endpoint and must bound its own buffer |

The rule SYNTHESIS §7.1 states for this chain is normative here: "a bridge that maps ZeroMQ
PUB onto weida Pub/Sub gets a matching policy… a bridge that maps ZeroMQ PUSH onto weida
Push/Pull gets matching backpressure; mixing them silently converts one into the other, which
is exactly what weida's invariant against adapters inventing guarantees forbids"
[SYNTHESIS §7.1], [INVARIANTS]. Crossing the two is a refused configuration (§9).

## 5. Identity, security and authorization

| ZeroMQ | weida | Mapping |
| --- | --- | --- |
| `NULL`: no authentication, no confidentiality, "SHOULD NOT be used on public infrastructure without transport-level security" [zeromq §10] | `ClientTls` dialling anonymously; `IncomingMeta::peer` is `None` [PATTERNS §1.9] | Permitted only on a trusted local transport. The adapter MUST NOT present a NULL peer as any weida identity, and the two-tier control/bulk binding of [0002] is unavailable for anonymous peers [0008 §4.2] |
| `PLAIN`: username and password in clear text, "not robust against even the simplest traffic snooping" [zeromq §10] | no counterpart | Credentials terminate at the adapter. They MAY select which weida `Identity` the adapter dials with; they are never forwarded |
| `CURVE`: Curve25519, permanent keys C and S plus transient C'/S', forward secrecy, three security models from "no client check" to "each client has its own key" [zeromq §10] | `Trust` (pins, anchors, or what the address names) and `ServerTls::require_client` [PATTERNS §1.9], [ARCHITECTURE §5 TLS] | Structurally the same idea — key-as-identity — but **no key is convertible**: a CURVE permanent key is a 32-byte Curve25519 public key [zeromq §10], a weida `Fingerprint` is the SHA-256 of the peer's TLS public key [PATTERNS §1.9]. The adapter is the trust boundary and holds two independent trust configurations. Loss L6 |
| `ZAP`: authorization delegated to an in-process handler over REQ/REP, answering 200/300/400/500 with a user id, scoped only by a domain string [zeromq §10] | "Authentication is not authorization": the application decides on `IncomingMeta::peer` per endpoint, topic or payload [ARCHITECTURE §5] | A ZAP 200 admits the ZMTP connection to the adapter and nothing more. The ZAP user id is a per-connection fact held by the server [zeromq §12/P14] and is never presented as a weida peer identity |
| `Identity` metadata / ROUTER routing id: self-asserted, 1-255 bytes, chosen by the peer [zeromq §10] | proved fingerprint, "from the handshake, never from a header, so it can be authorized on but not forged" [PATTERNS §1.9] | **The adapter MUST NOT map one onto the other** [0008 §5 adapters], [INVARIANTS]. A routing id may be carried as adapter-local state or as application payload; it is not an identity |

## 6. Topic filter mapping

[0007](../decisions/0007-topic-namespace.md) turns weida's Pub/Sub filter from a byte prefix
into a segmented pattern: separator `.`, `*` for exactly one whole segment, `#` for zero or
more trailing segments and only as the final segment [0007 §4.2]. The ZMTP row group of
[0007 §5], reproduced here as this document owes it:

| ZMTP construct | ZMTP semantics | weida filter | Exact? | Adapter obligation |
| --- | --- | --- | --- | --- |
| `SUBSCRIBE` prefix ending at a segment boundary | "A subscription of 'A' SHALL match all messages starting with 'A'" [zeromq §4.3] | `P.#` | yes | none beyond the boundary discipline the zguide already recommends |
| `SUBSCRIBE` prefix ending mid-segment | same, with no notion of a boundary [zeromq §4.3] | — | no | **Loss L2.** Subscribe at the enclosing segment boundary and re-apply the byte prefix locally before handing the message to the ZeroMQ peer, or refuse the subscription |
| Empty subscription | "An empty subscription SHALL match all messages" [zeromq §4.3] | `""`, equivalently `#` | yes | none [PROTOCOL §6.4] |
| RADIO/DISH group | exact-match groups instead of prefix topics [zeromq §4.6] | a filter with no wildcard | yes | none |
| Repeated `SUBSCRIBE` for one filter | additive and **non-idempotent**: "Subscribing to 'A' and 'A' counts as two subscriptions, and would require two CANCEL commands to undo" [zeromq §2], [zeromq §7] | one filter | no | **Loss L3.** weida's SUBSCRIBE "for a filter already held on that connection and path is idempotent" [PROTOCOL §6.4], so the adapter reference-counts subscriptions per ZeroMQ peer and sends UNSUBSCRIBE only when its count reaches zero |
| A topic containing `.`, `*` or `#` | no character is special in a ZMTP subscription [zeromq §4.3] | those bytes are special in a *filter* only [0007 §4.2] | no | **Loss L4.** A ZeroMQ prefix containing weida's separator or wildcard bytes cannot be expressed as a literal filter; the adapter refuses such a subscription rather than silently widening it |

The zguide's envelope advice is what makes L2 tolerable in practice: put the key in its own
frame, because "Subscription is a prefix match" and the envelope prevents accidental payload
matches since "the match won't cross a frame boundary" [zeromq §4.3]. An envelope frame is a
segment boundary by construction, so a bridge that requires the envelope convention loses
nothing. It MUST say so in its configuration rather than assume it.

## 7. Transfer points and guarantee mapping

**The transfer points, lined up** [SYNTHESIS §4]:

| Hop | Transfer point | Signal | What may then be discarded |
| --- | --- | --- | --- |
| ZeroMQ application → its socket | the only one the protocol has | `zmq_send()` returning — "0MQ has assumed responsibility for the message" [zeromq §6] | the buffer, but not the retry obligation: a disconnecting peer "SHALL destroy its double queue and SHALL discard any messages it contains" [zeromq §6] |
| adapter → weida peer | sender to the peer's **transport** [SYNTHESIS §4] | `Delivery::delivered()` resolving `Ok(())` [GUARANTEES §3] | nothing beyond "the bytes arrived" |
| weida → ZeroMQ peer | none | there is no acknowledgement in ZMTP: "No frame, command or field acknowledges a message" [zeromq §6] | nothing is certified at all |

**Therefore, per [0006] §4.6, the guarantee chain ends at the adapter edge**, and per
[0005](../decisions/0005-refusal-race.md) §4.5 the adapter may not dress a weida transport
receipt as a ZeroMQ-side ownership transfer — there is no ZeroMQ-side signal to dress it as.
The concrete consequences:

- **weida → ZMTP.** Once the adapter has queued a message on its ZeroMQ socket, it knows
  nothing more, and it cannot tell the weida sender anything more either. If the ZeroMQ peer
  disconnects, the queued messages are destroyed silently [zeromq §6]. A weida sender that
  needs to know a refusal happened must use Req/Rep, whose ERROR frame is written by the
  application [0005 §4.3] — and even then the ERROR is the *adapter's* statement, never the
  ZeroMQ application's.
- **ZMTP → weida.** The adapter's `delivered()` proves the next weida hop's transport holds
  the bytes and nothing about its application [GUARANTEES §3]; a small transfer can even be
  acknowledged before the peer's application refuses it [PATTERNS §1.6], [0005 §4.1]. There is
  no ZeroMQ-side acknowledgement to report it to in any case.

**Guarantee set at this edge** [0006 §4.1-§4.2]: the weida side runs `core` — delivery
`BestEffort`, acknowledgement `TransportReceipt`, ordering `None`, deduplication `None`,
backpressure `Block` with `Drop` for fan-out. The ZeroMQ side is, read against its own three
rules — atomic, never duplicated, in order between immediate peers [zeromq §6] — at-most-once
per hop with in-order no-duplicate delivery of whatever arrives [zeromq §6 inference]. The two
sets line up on every dimension, which is why this adapter needs no degradation entry for the
default configuration; a superset requested on the weida side (ordering, dedup) has no ZeroMQ
counterpart and is refused (§9), not silently dropped [0006 §4.7].

**Ordering.** Both sides promise one hop and nothing wider: ZeroMQ "between two immediate
peers" [zeromq §12/P8], weida within one stream [PATTERNS §1.7]. A bridge that fans one
ordered ZeroMQ pipe onto several weida transfers loses the order, because weida does not order
streams relative to one another [PATTERNS §1.7], [SYNTHESIS §7.1] — loss L8.

**Duplicates.** Neither side duplicates on the wire [zeromq §7], [GUARANTEES §6]; neither side
suppresses duplicates either, and the residue both catalogues state is the same: external side
effects need an application-level idempotency key [SYNTHESIS §2 D7], [zeromq §12/P9]. The
adapter adds nothing and claims nothing.

## 8. Named losses

Each is a thing the adapter cannot carry. The mapping document is where they live, and the
adapter's configuration must surface them rather than absorb them silently [INVARIANTS],
[0006 §4.9].

- **L1 — Multipart.** ZMTP's atomic multi-frame message has no weida representation
  [zeromq §3], [ARCHITECTURE §6c.3]. Default: refuse. Optional: an explicitly configured
  adapter-owned concatenation, which is an application protocol both ends must know (§3).
- **L2 — Byte-prefix subscriptions.** A prefix that ends mid-segment cannot be expressed as a
  weida filter [zeromq §4.3], [0007 §4.5]. Subscribe at the boundary and re-filter locally, or
  refuse (§6).
- **L3 — Subscription non-idempotence.** ZMTP counts duplicate subscriptions, weida collapses
  them [zeromq §2], [PROTOCOL §6.4]. The adapter reference-counts (§6).
- **L4 — Reserved filter bytes.** A ZeroMQ prefix containing `.`, `*` or `#` cannot be a
  literal weida filter [0007 §4.6]. Refuse (§6).
- **L5 — ROUTER's silent drop.** ROUTER drops an unroutable or over-HWM message silently by
  default — "remarkably easy to lose messages by accident" [zeromq §8], [zeromq §12/P4] —
  while weida refuses an unknown path explicitly with `STOP_SENDING(UNKNOWN_ENDPOINT)`
  [PROTOCOL §9.4]. The adapter MUST set `ZMQ_ROUTER_MANDATORY` so the loss becomes
  `EHOSTUNREACH` and can be reported; it MUST NOT map a weida refusal onto a silent drop.
- **L6 — Key identity is not transferable.** CURVE keys and weida fingerprints are different
  key material [zeromq §10], [PATTERNS §1.9]; the adapter is the trust boundary and cannot
  extend either side's authentication across itself (§5).
- **L7 — Subscription visibility.** XPUB's subscription events and its synthesized unsubscribe
  on premature disconnect [zeromq §4.3] have no weida counterpart: a publisher sees
  `filter_count` and `peer_count`, not events [PATTERNS §4]. A bridge that needs XPUB
  semantics keeps the subscription table itself.
- **L8 — Order across a fan-out.** One ordered ZeroMQ pipe onto many weida transfers is
  unordered at the far end [PATTERNS §1.7], [SYNTHESIS §7.1].
- **L9 — Drain.** ZeroMQ's `ZMQ_LINGER` certifies transfer to the network, not receipt, and
  defaults to infinite, so `zmq_ctx_term()` can block forever [zeromq §12/P17]; weida's
  `Runtime::shutdown` "is the one thing that cuts a finished transfer short" [PATTERNS §1.1],
  and [0009](../decisions/0009-drain.md) answers the other half: a separate `drain(Duration)`
  waits for already-finished transfers to reach the peer's **transport**, under a deadline that
  is mandatory and finite. The adapter therefore sets a finite linger on its ZeroMQ sockets and
  maps its own shutdown onto that drain — and still MUST NOT present either side's shutdown as
  a drain acknowledgement, because neither protocol has one [SYNTHESIS §2 D12], [0009 §4.6].
- **L10 — No application acknowledgement in either direction.** ZMTP has none [zeromq §6];
  weida has only the transport receipt [GUARANTEES §3]. Nothing in this adapter can certify
  that a message was processed (§7).

## 9. Configurations the adapter refuses

Refusal is at configuration time, which is both the guarantee rule ([GUARANTEES §4]) and the
default at an adapter edge ([0006 §4.7]). The adapter rejects, naming the reason:

1. **A ZeroMQ PUB/XPUB/RADIO source bridged onto a weida pattern with `Block` backpressure**
   (Push/Pull, Req/Rep), or a ZeroMQ PUSH source bridged onto weida Pub/Sub. Mixing a dropping
   policy with a blocking one silently converts one into the other [SYNTHESIS §7.1],
   [INVARIANTS].
2. **Multipart traffic without an explicitly configured concatenation** (L1).
3. **A subscription whose byte prefix ends mid-segment or contains `.`, `*` or `#`**, unless
   the configuration opts into boundary-subscribe-plus-local-refilter (L2, L4).
4. **A weida-side guarantee set above `core`** — any ordering, deduplication or completion
   level beyond `TransportReceipt` — because the ZeroMQ side has no mechanism to carry it
   [0006 §4.3], [0006 §4.7], [zeromq §6].
5. **`NULL` security on a non-local transport** paired with a weida binding that requires a
   client identity: the adapter would be asserting an identity nobody proved
   [zeromq §10], [0008 §4.2].
6. **Any configuration that would make the adapter the durable hop** — storing before
   forwarding to cover a ZeroMQ peer's disconnect — since that is `Stored(...)` and L2 work
   [0006 §4.8], [0004](../decisions/0004-durability-levels.md).

## 10. Interop bench plan

Built as Phase B slice 5, after the codec (slice 1) and the two bridge directions
[LOOP §9]. Upstream is pure Rust first [LOOP §9].

**Upstream under test.** The `zeromq` crate (zmq.rs, MIT): "A native Rust implementation of
ZeroMQ", TCP and IPC transports, patterns REQ, REP, DEALER, ROUTER, PUB, SUB, XPUB, XSUB,
PUSH, PULL, runtime selectable with `tokio` as the default [zeromq §13]. It carries its own
disclaimer — "This codebase does not implement all of ZeroMQ's feature set", with basic ZMTP
"working and tested against the reference implementation" [zeromq §13] — which bounds what the
bench can prove.

**What the pure-Rust upstream can cover.** Greeting and version negotiation, the NULL
handshake with `READY` metadata, framing including MORE and COMMAND flags, `SUBSCRIBE`/`CANCEL`,
`PING`/`PONG`, and the six socket pairings of the table above that both sides implement
[zeromq §1], [zeromq §3], [zeromq §13].

**What it cannot, and what happens then.** PAIR, the thread-safe draft family
(CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER/CHANNEL), `ws`/`wss` and everything beyond
TCP/IPC are absent from zmq.rs [zeromq §13]. Those cases go against libzmq through the `zmq`
crate (rust-zmq, C bindings, tracks libzmq releases [zeromq §13]) as an optional dev-dependency,
and are `#[ignore]` with the install command in the doc comment where libzmq is absent
[LOOP §2], [LOOP §5]. A libzmq daemon or example process, if one is needed, runs only under
the process supervisor with a `ready` condition and is stopped in the same item [LOOP §2].

**The bench itself.**

1. **Golden vectors, no I/O.** The 64-octet greeting, a NULL `READY` with `Socket-Type`
   metadata, short and long frames at the 255/256-octet boundary, a two-frame multipart
   message, `SUBSCRIBE`/`CANCEL`, `PING` with a TTL and its `PONG` echo [zeromq §1],
   [zeromq §3]. Byte-exact in both directions, the shape [PROTOCOL §8] already requires of the
   weida codec.
2. **Fuzz target** over the ZMTP decoder, cap-before-allocate on the declared frame size —
   a frame may declare up to 2^63-1 octets and `ZMQ_MAXMSGSIZE` is the only defence
   [zeromq §11] — mirroring `max_header_bytes`'s rule [PROTOCOL §3.1].
3. **Inbound matrix.** zmq.rs REQ → adapter → weida `Replier`; zmq.rs PUSH → adapter →
   `Puller`; zmq.rs SUB ← adapter ← weida `Publisher`, including a boundary-aligned prefix and
   a rejected mid-segment prefix (L2).
4. **Outbound matrix.** The same four with the directions reversed, plus DEALER/ROUTER against
   weida's concurrent exchanges [ARCHITECTURE §6b].
5. **Loss assertions, not just happy paths.** Each named loss of §8 that is observable gets a
   test: a multipart message is refused rather than flattened (L1); a duplicate SUBSCRIBE is
   reference-counted, and one CANCEL does not unsubscribe (L3); `ZMQ_ROUTER_MANDATORY` yields
   `EHOSTUNREACH` rather than a silent drop (L5); a weida payload beyond `max_message_bytes`
   is refused rather than truncated (§3).
6. **Numbers.** Round-trip latency and messages per second for REQ/REP and PUSH/PULL through
   the adapter against a direct zmq.rs pair on loopback, recorded in
   [IMPLEMENTATION.md](../IMPLEMENTATION.md) verified results with the command that produced
   them [LOOP §5 measure].
7. **Cross-adapter test** (Phase B slice 6, once a second adapter exists): a message enters
   through ZMTP and leaves through the other protocol, with the guarantees of both mapping
   documents asserted [LOOP §9].

## 11. Open questions

- **Where the adapter's `max_message_bytes` default comes from.** Nothing is measured; the
  weida-side neighbours are `subscriber_buffer_bytes` 8 MiB and `stream_receive_window` 1 MiB
  [PROTOCOL §10], and the ZeroMQ side has no default at all (`ZMQ_MAXMSGSIZE` is -1)
  [zeromq §11].
- **Whether a weida-native ZMTP codec should implement CURVE at all**, given that the weida
  side is already authenticated and encrypted by TLS and the adapter is the trust boundary
  (§5, L6). CURVE is documented "when using TCP transport" only [zeromq §10].
- **XPUB semantics** (L7): whether weida should expose subscription events at all is a weida
  question, not an adapter one, and belongs in a decision note if an adapter needs it.
- **Shutdown** (L9) is no longer open: SYNTHESIS §8.6 was closed by
  [0009](../decisions/0009-drain.md), and the drain it decided exists as
  `Runtime::drain(Duration)`. What is left for this adapter is the mapping, which §8 L9
  already states: a finite `ZMQ_LINGER` on the ZeroMQ sockets, the adapter's own shutdown
  onto `drain(timeout)`, and neither side's shutdown presented as a drain acknowledgement.

## 12. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §3.1, §4, §6.4, §8, §9.3, §9.4, §9.5, §10;
[PATTERNS.md](../PATTERNS.md) §1.1, §1.4, §1.6, §1.7, §1.8, §1.9, §4;
[GUARANTEES.md](../GUARANTEES.md) §3, §4, §6; [ARCHITECTURE.md](../ARCHITECTURE.md) §5, §6a,
§6b, §6c; [FAILURE_MODEL.md](../FAILURE_MODEL.md) §4; [INVARIANTS.md](../INVARIANTS.md);
[LOOP.md](../LOOP.md) §2, §5, §9; decisions [0002](../decisions/0002-control-and-bulk-separation.md)
§6.2, §6.3, [0003](../decisions/0003-credit-unit.md) §4.1, §4.2, §5,
[0004](../decisions/0004-durability-levels.md), [0005](../decisions/0005-refusal-race.md) §4.1,
§4.3, §4.5, [0006](../decisions/0006-guarantee-sets.md) §4.1-§4.3, §4.6-§4.9,
[0007](../decisions/0007-topic-namespace.md) §4.2, §4.5, §4.6, §5,
[0008](../decisions/0008-session-identity.md) §4.2, §4.5, §5.

Research: [zeromq.md](../research/zeromq.md) §0, §1, §2, §3, §4.1-§4.6, §5, §6, §7, §8, §10,
§11, §12/P4, §12/P6, §12/P8, §12/P9, §12/P12, §12/P13, §12/P14, §12/P17, §12/P18, §13;
[SYNTHESIS.md](../research/SYNTHESIS.md) §2 (D7, D8), §4, §7.1, §8.6.
