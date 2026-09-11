# ZMTP 3.1 — adapter mapping

Status: mapping document; slice 1 (the codec) implemented as `crates/zmq/weida-zmtp` and
slices 2, 3 and 5 (both bridge directions and the interop run against an independent ZeroMQ)
as `crates/zmq/weida-zmq-bridge`. What is left is the contract the adapter's
documentation owes its user
([LOOP.md](../LOOP.md) §9 Phase B, [0006](../decisions/0006-guarantee-sets.md) §4.9).
Date: 2026-09-11
Derived from: [docs/research/zeromq.md](../research/zeromq.md) (ZMTP 3.1, libzmq 4.3.x, the
zguide, CURVE/ZAP). Every ZeroMQ claim below carries that sheet's section; every weida claim
carries the weida document or decision it comes from. Where the two cannot be made to
coincide, §8 names the loss instead of hiding it.

**What the interop slice changed, and it is the reason the slice exists.** Three things this
document assumed were wrong against a real implementation, and all three were wrong in *our*
code rather than in the reading of the specification:

1. **A ZMTP 3.0 peer was refused.** The codec's 3.1 floor is what the specification asks a
   peer to accept, and the same sentence permits a downgrade — which a bridge must take,
   because the pure-Rust `zeromq` crate announces 3.0 and it is not alone. The negotiated
   version is now returned by the handshake and carried through §3's heartbeat.
2. **`PING` was sent to a peer that cannot decode it.** PING/PONG are 3.1 commands, and a 3.0
   peer that meets one answers "Unknown command received" and closes. The heartbeat is now
   gated on the negotiated version, whatever the configuration asked for.
3. **Subscriptions arrived in a form the bridge refused.** Real subscribers send ZMTP 2.0's
   one-frame `1`/`0` form rather than the 3.x `SUBSCRIBE` command, so a PUB-side bridge that
   accepts only commands has no subscribers at all (§6 records both forms now).

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

**What the inbound slice built** (`crates/zmq/weida-zmq-bridge`): one `Inbound` listens
on a TCP address, presents one ZeroMQ socket type, and speaks to one weida endpoint —
`REP` for a `REQ`/`DEALER` peer, `PULL` for a `PUSH` peer, `PUB` for a `SUB`/`XSUB` peer. It
drives the greeting and the NULL handshake, checks the peer's socket type against §2's table
and answers a mismatch with `ERROR` before the close, consumes the envelope frames a pattern
defines and refuses the multipart messages it does not (L1), translates subscriptions per §6
with the refusals of §9.3 and ZeroMQ's reference counting (L3), and bounds what it buffers
with its own `max_message_bytes` (§3). The tests drive it with a ZMTP peer built on the
codec, which is faithful on the wire and is *not* an independent implementation — that is
what slice 5's bench against the pure-Rust `zeromq` crate is still owed for (§10 items 3-6).

**What the outbound slice built** (the same crate): one `Outbound` binds a weida endpoint and
dials one foreign ZeroMQ peer — a weida `Replier` in front of a foreign `REP`/`ROUTER`, a
`Puller` in front of a foreign `PULL`, and a weida `Publisher` fed by a foreign `PUB`. The
mirror is deliberately not symmetric: inbound the bridge binds on the ZeroMQ side, outbound
it binds on the weida side, because the side that owns the endpoint path is the side weida
applications address. Req/Rep dials as `DEALER` rather than `REQ`, since a weida `Replier`
accepts concurrent exchanges ([ARCHITECTURE.md](../ARCHITECTURE.md) §6b) and `REQ` is a
lockstep socket [zeromq §4.2]: the 28/REQREP envelope (identity frame, empty delimiter) is
what pairs a reply with its exchange, and it is consumed rather than forwarded. A peer that
drops a request silently — `ZMQ_ROUTER_MANDATORY` off, the L5 loss — reaches the weida
requester as `ERROR{NO_REPLY}` after a deadline rather than as a hang, which is what
`IncomingRequest::refuse` exists for. `ZMQ_HEARTBEAT_IVL` runs on the adapter's own sockets
(§3) and neither side's liveness timer is translated into the other's.

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

**The protocol below this document is `weida-zmq`'s, not the bridge's** (B-094,
[0013](../decisions/0013-competitor-libraries.md) §5.2). Everything §3 states about framing and
everything §6 states about the two subscription wire forms is now the behaviour of a
`weida-zmq` socket, which the bridge **inherits rather than implements**: the greeting and its
3.0 downgrade, the socket-type table checked at the handshake, the frame headers and the
`ZMQ_MAXMSGSIZE` refusal from a declared length, the envelope a pattern defines, `PING`/`PONG`
with its version gate, and both subscription forms with 37/ZMTP's non-idempotent counting. This
document still maps the **forwarder** — what a weida guarantee becomes on the other side, and
what is lost — and the rules it states are now checkable in two places: here, and in the
library's own tests. Six differences the rebuild made, each because a socket is not a
connection driver:

1. **A message the bridge cannot carry ends the socket, not one connection.** Refusing loss
   L1's multipart used to close that peer's connection; a ZeroMQ socket has no API to drop one
   peer — libzmq has none either — so the refusal ends the bridge's socket and every connection
   on it. The refusal is unchanged; its blast radius is larger, and a supervisor that restarts
   the bridge is what a ZeroMQ application already does.
2. **A `REP` peer's request must carry the delimiter.** A `DEALER` peer that sent a bare
   `[body]` used to be accepted; the socket now discards a message with no envelope delimiter,
   which is what libzmq's REP does and what 28/REQREP specifies.
3. **The outbound direction reconnects.** Dialling is the socket's, so a foreign peer that goes
   away is re-dialled with `ZMQ_RECONNECT_IVL` backoff and the queued messages wait for it,
   where the run used to end.
4. **A refused subscription still reaches the peer as an `ERROR`**, through the one capability
   `weida-zmq` has and libzmq's API does not — `XPubSocket::refuse`, recorded in
   [libraries/zmq.md](../libraries/zmq.md) §9. Without it §9.3's refusal would have become a
   silent drop, which is the one thing that section forbids.
5. **`queue_bytes` is a byte budget over a message-counting high-water mark.** `ZMQ_SNDHWM`
   counts messages, so the configuration's byte budget becomes `queue_bytes /
   max_message_bytes` messages per peer, at least one, and the product is what a slow
   subscriber pins.
6. **`max_message_bytes` is a payload budget and `ZMQ_MAXMSGSIZE` counts frame headers**, so
   the socket is configured with 32 octets more — three frames' worth of header, more than any
   message this bridge maps carries. Without it a payload of exactly the configured cap would
   be refused by its own header, which is not what a caller who set that number meant; the
   interop bench, which sends exactly 1 MiB against the 1 MiB default, is what found it.

**One ZMTP message is one weida transfer, and therefore one QUIC stream** — "one data flow
maps naturally to one transport stream" [INVARIANTS], P1 and P2 of [ARCHITECTURE §6a]. A
weida transfer is a DATA header followed by opaque payload bytes until FIN [PROTOCOL §4].

**The ZMTP connection is not one weida connection.** The adapter holds **one weida connection
per dialled endpoint path** ([0002](../decisions/0002-control-and-bulk-separation.md) §6.2,
implemented), and the connections of one peer are bound together by the proved fingerprint
([0008](../decisions/0008-session-identity.md) §4.2). A ZeroMQ socket that connects to several
endpoints [zeromq §2] therefore maps to several weida connections, and the adapter's
round-robin over them is `PeerSet::pick` [ARCHITECTURE §5], which is the same selection rule
PUSH and REQ use [zeromq §4.1]. There is no separate control connection to hold: 0002 §6.3's
per-peer tier is parked, because a frame that names a path rides that path's connection
([0011](../decisions/0011-answered-where-it-arrived.md) §4.2-§4.3). What the adapter gets from
that is exactly what SYNTHESIS §7.2 asked for — one weida connection per foreign session is one
connection per path — so it can promise MQTT's no-stall rule for a session without inventing
anything.

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

**But only with a 3.1 peer.** `PING`/`PONG` arrived in 3.1, so the heartbeat is gated on the
version the greeting negotiated and a 3.0 peer gets none — configuration asks, the version
decides, and the suppression is logged once rather than left silent. The slice-5 run is why
this is stated rather than assumed: `zeromq` 0.6 announces 3.0 and answers any command but
`READY` with "Unknown command received", then closes, so an ungated heartbeat does not
degrade liveness detection — it *is* the connection loss it was meant to detect.

**Version negotiation.** A 3.0 peer is accepted by downgrading, which the specification
permits ("a peer MAY downgrade to a lower protocol version") and a bridge has to take: the
alternative is refusing every implementation that never adopted 3.1. Below major 3 there is
no downgrade, because 2.0 and 1.0 have different framing and a different subscription form,
and the specification detects them by abusing the padding field rather than by the version
octets [zeromq §1].

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
| No credit signal anywhere [zeromq §12/P12] | No L0 application credit; L2 credit is per subscription, on the connection of the path that subscription names [0003 §4.2, [0011](../decisions/0011-answered-where-it-arrived.md) §4.3] | When the L2 broker exists, a ZeroMQ peer still has nothing to grant or consume, so the adapter is the credit endpoint and must bound its own buffer |

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

**Two wire forms, and a bridge meets both.** 3.x carries a subscription in the
`SUBSCRIBE`/`CANCEL` **commands** [zeromq §1]; ZMTP 2.0 carried it as a one-frame **message**
beginning `%x01` to subscribe or `%x00` to cancel, which is also the shape libzmq presents to
an XPUB *application* — "byte 1 (for subscriptions) or byte 0 (for unsubscriptions) followed
by the subscription body" [zeromq §6]. The slice-5 run found that the distinction is not
historical: `zeromq` 0.6 announces ZMTP 3.0 and sends **and reads only the legacy form**, so
a PUB-side bridge that accepts commands alone has no subscribers from that implementation at
all. The adapter therefore **accepts both and sends the command**, with the legacy form
available as configuration. Accepting both is unambiguous rather than generous: a SUB or XSUB
peer may not send application messages, so a message from one is a subscription or an error.

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
  [PROTOCOL §9.4]. `ZMQ_ROUTER_MANDATORY` lives on the ROUTER socket, so the adapter can set
  it only where the ROUTER is **its own**; a weida endpoint dialling somebody else's ROUTER
  cannot set an option on a socket it does not own, and the loss reaches it as a reply that
  never comes. The outbound bridge therefore holds each exchange against a deadline and
  refuses it with `ERROR{NO_REPLY}` when the deadline passes, so the loss surfaces as a typed
  error rather than as a hang; it MUST NOT map a weida refusal onto a silent drop.
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

**What the pure-Rust upstream can cover — and what the run found it cannot.** In plan:
greeting and version negotiation, the NULL handshake with `READY` metadata, framing including
MORE and COMMAND flags, `SUBSCRIBE`/`CANCEL`, `PING`/`PONG`, and the six socket pairings of
the table above that both sides implement [zeromq §1], [zeromq §3], [zeromq §13]. In fact
`zeromq` 0.6 announces ZMTP **3.0** and decodes exactly one command, `READY`: `PING`, `PONG`
and `ERROR` are all "Unknown command received" and end the connection, and subscriptions
travel in ZMTP 2.0's message form rather than as commands. So the run covers the greeting, the
handshake, framing, both subscription paths and the socket pairings, and it cannot cover
`PING`/`PONG` at all — the heartbeat's 3.1 gate is the *consequence* of that, tested by
holding a connection open across several suppressed intervals rather than by observing a PONG.

**What it cannot cover, and what happens then.** PAIR, the thread-safe draft family
(CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER/CHANNEL), `ws`/`wss` and everything beyond
TCP/IPC are absent from zmq.rs [zeromq §13]. Those cases would go against libzmq through the
`zmq` crate (rust-zmq, C bindings [zeromq §13]) as an optional dev-dependency, `#[ignore]`
with the install command in the doc comment where libzmq is absent [LOOP §2], [LOOP §5] —
not built here, because none of the patterns this adapter maps needs them. The pure-Rust
upstream is a **library dev-dependency**, so the bench needs no supervised process, no install
step and no ignored tests: it runs in `cargo test` and `cargo bench` like everything else,
which is the outcome [LOOP §2]'s supervisor rule was there to make safe.

**The bench itself.**

1. **Golden vectors, no I/O** — *done, slice 1*: §10.1 below publishes the octets and
   `crates/zmq/weida-zmtp/tests/golden_vectors.rs` asserts every one of them in both
   directions [zeromq §1], [zeromq §3]. Byte-exact both ways, the shape [PROTOCOL §8] already
   requires of the weida codec.
2. **Fuzz target** over the ZMTP decoder, cap-before-allocate on the declared frame size —
   a frame may declare up to 2^63-1 octets and `ZMQ_MAXMSGSIZE` is the only defence
   [zeromq §11] — mirroring `max_header_bytes`'s rule [PROTOCOL §3.1]. *Done, slice 1*: five
   `cargo fuzz` targets under `crates/zmq/weida-zmtp/fuzz` (frame, frame stream, command,
   metadata, greeting) and a stable-Rust `fuzz_smoke.rs` that runs the same properties in
   `cargo test`, including a long header with an arbitrary 64-bit length and no body.
3. **Inbound matrix.** zmq.rs REQ → adapter → weida `Replier`; zmq.rs PUSH → adapter →
   `Puller`; zmq.rs SUB ← adapter ← weida `Publisher`, including a boundary-aligned prefix and
   a rejected mid-segment prefix (L2). *Done, slice 5*
   (`crates/zmq/weida-zmq-bridge/tests/interop.rs`), against the pure-Rust `zeromq`
   crate as an independent implementation; slice 2's own matrix stays, against a peer built on
   this repository's codec, because the two catch different things — a faithful peer pins the
   bridge's behaviour, and a foreign one pins its assumptions. Three of those assumptions were
   wrong and are listed at the head of this document.
4. **Outbound matrix.** The same four with the directions reversed, plus DEALER/ROUTER against
   weida's concurrent exchanges [ARCHITECTURE §6b]. *Done, slice 5*: a weida `Requester`
   through a real `RepSocket`, a `Pusher` through a real `PullSocket`, a `Subscriber` fed by a
   real `PubSocket`, and a real `RouterSocket` that drops a request silently. Slice 3's
   four-concurrent-exchange correlation test stays where it is, since `zeromq`'s REP socket is
   lockstep and cannot produce out-of-order replies to measure against.
5. **Loss assertions, not just happy paths.** Each named loss of §8 that is observable gets a
   test: a multipart message is refused rather than flattened (L1); a byte prefix that stops
   mid-segment selects nothing and is refused (L2); a duplicate SUBSCRIBE is reference-counted,
   and one CANCEL does not unsubscribe (L3); a ROUTER that drops a request silently reaches
   the requester as `ERROR{NO_REPLY}` rather than as a hang (L5); a weida payload beyond
   `max_message_bytes` is refused rather than truncated (§3). *Done*: L1, L2, L5 and the cap
   against the independent peer in slice 5, L3 against the faithful peer in slice 2 — `zeromq`
   0.6 collapses duplicate subscriptions in its own client before they reach the wire, so it
   cannot exercise a reference count.
6. **Numbers.** Round-trip latency and messages per second for REQ/REP and PUSH/PULL through
   the adapter against a direct zmq.rs pair on loopback, recorded in
   [IMPLEMENTATION.md](../IMPLEMENTATION.md) verified results with the command that produced
   them [LOOP §5 measure]. *Done, slice 5*:
   `cargo bench -p weida-zmq-bridge --bench interop`, and the two numbers §11 was holding
   open are decided there.
7. **Cross-adapter (slice 6).** `crates/interop/cross-tests` runs a message in through one
   adapter and out through the other against `zeromq` and `nng`: the three pattern chains in
   both directions, the composed losses (a ZMTP multipart refused at hop one — L1, so the
   second protocol never sees it — the smaller `max_message_bytes` deciding, an SP hop-count
   ceiling arriving as silence), and `BestEffort` end to end asserted rather than described.
   *Done, slice 6*: nine tests, both foreign ends the real implementations. The claim is the
   composition of §7 and [nng.md](nng.md) §7 — `delivered()` proves the ZeroMQ hop's
   transport, SP has no transfer point at all — so a ZeroMQ send succeeds with the NNG end
   closed and nothing arrives, which is the test rather than the caveat [LOOP §9].

### 10.1 The vectors

Published here so that a reader can check an implementation — this one, libzmq, or a
reimplementation — rather than trust it. Hex; the frame header and the body are separate
columns, and a run of equal octets is written `00×8`.

Four places where a ZeroMQ RFC disagrees with itself or with its reference implementation were
found while writing the codec, and the vectors take a side:

- **Command names are length-prefixed.** The prose says a command contains "a printable
  command name, a null octet separator, and data"; the ABNF says
  `command-name = short-size 1*255command-name-char`, and libzmq puts `05 READY` on the wire.
  The grammar and the reference implementation agree against the prose, so the vectors use the
  length octet and no separator.
- **An `ERROR` reason may contain spaces.** `error-reason = short-size 0*255VCHAR` excludes
  the space octet, while libzmq's own reasons read like "Unknown mechanism". The codec accepts
  printable ASCII including space and refuses everything else, in both directions.
- **CURVE's `HELLO` padding is 72 octets, not 70.** 26/CURVEZMQ's ABNF says `hello-padding =
  72%x00`; its prose says "This SHALL be 70 octets, all zero." Only 72 adds up to the
  specification's own 200-octet `HELLO`: 6 + 2 + 72 + 32 + 8 + 80 = 200, where 70 gives 198.
  The vectors use 72 in both directions, and the 198-octet reading is published as a row of
  its own because refusing it is the behaviour that matters — a reader that accepted it would
  take the signature box two octets out of phase. The arithmetic is in
  [zeromq.md](../research/zeromq.md) §10.
- **A CURVE `MESSAGE` is framed as a message, not as a command.** 26/CURVEZMQ calls it a
  command and gives it a command body — `%d7 "MESSAGE"`, the short nonce, the box — but
  libzmq 4.3.5 puts a **message** frame header in front of it and **closes the connection** on
  the command-framed form. The reference implementation wins here because it is the only
  reading two implementations can share: the vectors use the message header, a reader accepts
  either kind since what matters is whether the box opens, and the four handshake commands stay
  command frames — so the two forms are mixed inside one connection. Measured rather than read:
  a CURVE handshake with libzmq completes and the first command-framed `MESSAGE` ends it
  (`crates/zmq/weida-zmq/tests/interop_libzmq.rs`, [zeromq.md](../research/zeromq.md) §13
  source [41]).

| Vector | Frame header | Body |
| --- | --- | --- |
| Greeting, NULL, as-server 0 | — | `FF 00×8 7F` `03 01` `4E 55 4C 4C 00×16` `00` `00×31`, 64 octets: signature, version, mechanism, as-server, filler |
| Partial greeting (version sniff) | — | `FF 00×8 7F 03`, 11 octets |
| `READY`, `Socket-Type=REQ` | `04 19` | `05 READY 0B "Socket-Type" 00 00 00 03 "REQ"` |
| `READY`, no properties | `04 06` | `05 READY` |
| `ERROR "bad socket type"` | `04 16` | `05 ERROR 0F "bad socket type"` |
| `SUBSCRIBE "px.eur"` | `04 10` | `09 SUBSCRIBE "px.eur"` |
| `CANCEL "px.eur"` | `04 0D` | `06 CANCEL "px.eur"` |
| `SUBSCRIBE ""` (matches everything) | `04 0A` | `09 SUBSCRIBE` |
| `PING`, TTL 300 (30.0 s), context `ctx` | `04 0A` | `04 PING 01 2C "ctx"` |
| `PONG`, context `ctx` | `04 08` | `04 PONG "ctx"` |
| Message frame, 255-octet body | `00 FF` | 255 octets — the largest short frame |
| Message frame, 256-octet body | `02 00 00 00 00 00 00 01 00` | 256 octets — the smallest long frame |
| Multipart `A`, `B` | `01 01` then `00 01` | `41`, then `42` — MORE on all but the last |
| Long command frame | `06` + eight-octet size | a `READY` beyond 255 octets |
| `HELLO`, user `admin`, password `secret` (PLAIN) | `04 13` | `05 HELLO 05 "admin" 06 "secret"` |
| `HELLO`, both fields empty | `04 08` | `05 HELLO 00 00` — the length octets stay |
| `WELCOME` (PLAIN) | `04 08` | `07 WELCOME` — no data at all |
| `INITIATE`, `Socket-Type=DEALER` (PLAIN) | `04 1F` | `08 INITIATE 0B "Socket-Type" 00 00 00 06 "DEALER"` |
| `HELLO` (CURVE), 200 octets | `04 C8` | `05 HELLO` `01 00` `00×72` `C1×32` `00×7 01` `5A×80` — version, padding, C', nonce counter, `Box [64 * %x0](C'->S)` |
| `HELLO` written from the prose, 198 octets | `04 C6` | the same with `00×70` — **refused**, `CURVE HELLO is 200 octets, not 198` |
| `WELCOME` (CURVE), 168 octets | `04 A8` | `07 WELCOME` `11×16` `B0×144` — long nonce, `Box [S' + cookie](S->C')` |
| `INITIATE` (CURVE), 257 octets | `06` `00×6 01 01` | `08 INITIATE` `22×16` `CB×80` `00×7 02` `1B×144` — cookie (nonce and box), nonce counter, `Box [C + vouch + metadata](C'->S')`; always a long frame |
| `READY` (CURVE), 30 octets | `04 1E` | `05 READY` `00×7 03` `BD×16` — the smallest box there is: sealing costs 16 octets |
| `MESSAGE` (CURVE), 33 octets | `00 21` | `07 MESSAGE` `00×7 04` `E7×17` — a **message** frame header, not a command one (see above); the box holds the flags octet, so it is never empty |
| `INITIATE` box plaintext, 128+ octets | — | `C1×32` `33×16 BC×80` `0B "Socket-Type" 00 00 00 06 "DEALER"` — C, vouch, metadata |
| Z85, the RFC's test vector | — | `86 4F D2 6F B5 59 F7 5B` ↔ `HelloWorld` |
| Z85, a 40-character key | — | `C1×32` ↔ `.ni$7.ni$7.ni$7.ni$7.ni$7.ni$7.ni$7.ni$7` |

The four PLAIN rows are 24/ZMTP-PLAIN's grammar rather than 37/ZMTP's: `hello = command-size
%d5 "HELLO" username password` with a one-octet length before each field, `welcome =
command-size %d7 "WELCOME"` carrying nothing, and `initiate = command-size %d8 "INITIATE"
metadata` carrying what NULL puts in `READY`. They live in the codec because they are octets;
the mechanism's own warning — PLAIN is "not robust against even the simplest traffic snooping
or spoofing attacks" — is a property of the mechanism and not of the encoding, and the codec
still depends on nothing at all.

The six CURVE rows and the `INITIATE` plaintext are 26/CURVEZMQ's layouts with **every
cryptographic box an opaque range**: a length and a stated content, never a computed value.
That is why they are in the codec at all. The boxes are `crypto_box` output, so a vector
containing one would be a vector of somebody's key material; what can be published, and
checked against libzmq with a hex dump, is where each box starts and how long it is. The nonces
are the other half of the layout: a short nonce is the eight-octet counter shown here behind a
16-octet fixed prefix (`CurveZMQHELLO---`, `CurveZMQINITIATE`, `CurveZMQREADY---`,
`CurveZMQMESSAGEC` and `CurveZMQMESSAGES`), and a long one is the 16 octets shown here behind
`WELCOME-`, `COOKIE--` or `VOUCH---`. The two Z85 rows are 32/Z85, which is how a key is
written down rather than sent: four octets to five characters, so 32 octets to 40. Sealing and
opening happen in `weida-zmq`, which takes the one cryptographic dependency; `weida-zmtp`'s
`[dependencies]` is still empty.

The encoder always picks the shortest size field, which is what the specification recommends;
the decoder accepts a long size for a short body, because a peer that sends one is odd rather
than wrong. That asymmetry is itself a vector: `02 00 00 00 00 00 00 00 01 78` decodes to the
one-octet body `x`.

## 11. Open questions

- **`max_message_bytes`** is no longer open: the slice-5 bench measured the bridge's cost as
  **linear in message size with no cliff** (1 KiB round trip 81 µs, 1 MiB 3.37 ms, against
  20.5 µs and 439 µs for a direct ZeroMQ pair), so the number bounds **memory** and not
  latency. The default is **1 MiB** = `stream_receive_window` [PROTOCOL §10], because the
  bridge holds one whole message per direction per connection and the exposure is therefore
  the cap times `max_connections` — 8 GiB at the 8 MiB this document used to suggest. The same
  arithmetic moved the PUB-side queue from 1024 *messages* to 8 MiB of *bytes*, which is
  `subscriber_buffer_bytes`: a depth multiplies by the cap, and nobody chose the product.
  Recorded in [IMPLEMENTATION.md](../IMPLEMENTATION.md) verified results with the command.
- **The outbound reply deadline** is no longer open either, and the answer is that the
  existing number survives: **10 s**, now justified as ~3000× the slowest exchange the cap
  allows (3.37 ms at 1 MiB). A deadline that far above the working range cannot fire on a
  merely slow peer, and that asymmetry is the whole argument — a request lost to a silent
  ROUTER costs one exchange, a deadline that fires early costs correct ones.
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
