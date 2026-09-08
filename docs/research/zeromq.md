# ZeroMQ (ZMTP 3.1, libzmq 4.3.x)

## 0. Identity card

- **Name.** ZeroMQ, also ØMQ and 0MQ. Wire protocol: ZeroMQ Message Transport Protocol, ZMTP [1].
- **Versions in use.** ZMTP 3.1 (37/ZMTP, marked *draft*) on the wire, with backwards detection of ZMTP 3.0, 2.0 and 1.0 [1]. API version researched: libzmq 4.3.x; 4.3.5 stable released 2023/10/09, completing relicensing from
  LGPL-3.0+ to MPL-2.0 [36].
- **Governing body.** The ZeroMQ community. Specs live in the ZeroMQ RFC repository under the Digital Standards Organization's Consensus-Oriented Specification System (COSS); each is GPLv3-licensed prose with a named editor,
  mostly Pieter Hintjens [1][2][11].
- **Specification documents.** 37/ZMTP (framing, greeting, heartbeats, metadata) [1]; pattern RFCs 28/REQREP [2], 29/PUBSUB [3], 30/PIPELINE [4], 31/EXPAIR [5], 41/CLIENTSERVER [6], 48/RADIO-DISH [7]; security RFCs
  24/ZMTP-PLAIN [8], 25/ZMTP-CURVE [9], 26/CURVEZMQ [10], 27/ZAP [11]; WebSocket mapping 45/ZWS [12]. Application protocols on top: 18/MDP [13], 12/CHP [14], 10/FLP [15], 36/ZRE [16].
- **Year of the version researched.** 37/ZMTP carries copyright 2009-2015 [1]; pattern and security RFCs 2011-2015; RADIO-DISH and ZWS 2020 [7][12]; libzmq 4.3.5 is 2023 [36].
- **Reference implementation.** libzmq, "ZeroMQ core engine in C++, implements ZMTP/3.1" [36]. CurveZMQ's own reference is libcurve [10]; ZAP's is `src/spec_27.c` in the RFC repository [11].
- **Wire type.** Binary: flags octet, size field, opaque body [1]. Clear-text connections send message data as message frames; encrypted connections encode message data as commands "so that wire analysis is not possible", but
  "command names SHALL be visible and command frames SHALL be printable" [1].
- **Transports the specification defines.** ZMTP is specified over "a connected transport layer such as TCP" [1]. libzmq 4.3.x implements `tcp`, `ipc`, `inproc`, `pgm`/`epgm`, `udp` (RADIO/DISH only), `vmci`, `tipc`, `vsock`,
  and DRAFT `ws`/`wss` [17][24][25][26][27][28][29][30][36].

## 1. Connection and session lifecycle

**Stages.** Version and mechanism agreement; security handshake; metadata as a final handshake command; then messages either way. "Either peer may at any moment close the connection." [1]

**Greeting.** Fixed 64 octets: signature `%xFF` + 8 padding + `%x7F`, major `%x03`, minor `%x01`, 20-octet null-padded mechanism name, one-octet `as-server`, 31 zero filler octets [1]. Negotiation is asymmetric: a peer may
send only the first 11 octets (signature + major) to sniff the peer's version, or send all 64 and demand 64 back [1]. Normative: padding carries no meaning and MUST NOT be validated; a peer MUST accept versions >= 3.1; a peer
always uses its own protocol against an equal-or-higher peer, MAY downgrade to a lower one, and MUST close if it cannot [1]. Two documented downgrade strategies exist, one detecting only ZMTP 2.0 and one detecting 1.0 and 2.0
by abusing the padding field as a ZMTP 1.0 identity-frame length; if the mechanism is anything other than NULL and a ZMTP 1.0/2.0 peer is detected, "it MUST immediately close the connection" [1].

**Mechanism agreement.** "A peer announces precisely one security mechanism, unlike SASL… Security in ZMTP is *assertive* in that all peers on a given socket have the same, required level of security. This prevents downgrade
attacks and simplifies implementations." [1] A received mechanism that does not exactly match the sent one MUST cause a close. A peer that reads a full greeting including mechanism MUST also send one, to avoid deadlock [1].

**Authentication step.** NULL: client sends `READY`, waits for `READY`; server SHOULD parse and MAY validate; either peer MAY close on failure; messages may flow once `READY` has been both sent and received [1]. PLAIN:
`C:HELLO`(user,pass) → `S:WELCOME`|`S:ERROR`, `C:INITIATE`(metadata) → `S:READY`|`S:ERROR` [8]. CURVE: `C:HELLO` → `S:WELCOME` → `C:INITIATE` → `S:READY`, with `S:ERROR` possible at either server step [10]. The `as-server`
field picks the client and server roles for PLAIN and CURVE [8][9]; for NULL it MUST be zero and "the peer that binds SHALL be the server, and connecting peer SHALL be the client" [1].

**Metadata.** A key/value dictionary per direction, names case-insensitive and 1-255 characters, values 0 to 2^31-1 octets with a four-octet network-order size. Defined: `Socket-Type` (SHOULD), `Identity` (MAY), `Resource`
(MAY, new in 3.1); `X-` names are reserved for applications [1]. Metadata is sent *after* the handshake specifically so that version fingerprinting is harder [1]. libzmq exposes application properties via `ZMQ_METADATA` [18].

**Socket-type validation.** The peer SHOULD enforce a valid peer socket type; 37/ZMTP gives the legal table (REQ: REP, ROUTER; REP: REQ, DEALER; DEALER: REP, DEALER, ROUTER; ROUTER: REQ, DEALER, ROUTER; PUB: SUB, XSUB; XPUB:
SUB, XSUB; SUB: PUB, XPUB; XSUB: PUB, XPUB; PUSH: PULL; PULL: PUSH; PAIR: PAIR; CLIENT: SERVER; SERVER: CLIENT; RADIO: DISH; DISH: RADIO; SCATTER: GATHER; GATHER: SCATTER; PEER: PEER; CHANNEL: CHANNEL). On mismatch it SHOULD
send `ERROR` and disconnect [1]. libzmq bounds the whole handshake with `ZMQ_HANDSHAKE_IVL`, default 30000 ms, 0 for no limit, not applicable to `ZMQ_STREAM` [18].

**Keep-alive / heartbeat.** ZMTP 3.1 adds `PING`/`PONG`. `PING` carries a 16-bit `ping-ttl` in tenths of a second (max 6553.5 s) and a `ping-context` of at most 16 octets, which `PONG` echoes [1]. Motivation: "Network
connections can go stale and die without reporting TCP errors" and "Processes can become blocked, especially if they run out of memory" [1]. A peer SHOULD consider the connection dead if it sent a `PING` and got no traffic
within a timeout, or received a `PING` with non-zero TTL and then no traffic within that TTL; "Since PONG replies may be arbitrarily delayed behind already queued traffic, a peer SHOULD treat any incoming traffic (not just a
PONG reply) as a sign of life"; and it should not send many PINGs without replies, to avoid PONG storms [1]. libzmq: `ZMQ_HEARTBEAT_IVL` (default 0, off), `ZMQ_HEARTBEAT_TIMEOUT` (default 0, or `HEARTBEAT_IVL` if set; "any
received traffic will cancel the timeout"), `ZMQ_HEARTBEAT_TTL` (default 0, max 6553599 ms, rounded to deciseconds, values under 100 have no effect) [18].

**Idle behaviour.** No idle timeout beyond heartbeats. For encrypted connections the spec recommends sending "random garbage data ('noise') when there is no other traffic" against presence analysis [1]; libzmq exposes no such
option [18] [inference].

**Orderly close.** ZMTP has no close handshake [1]. At the API, `ZMQ_LINGER` governs the drain: -1 (default) is infinite, and `zmq_ctx_term()` "shall block until all pending messages have been sent to a peer"; 0 discards
immediately; a positive value is a millisecond bound after which pending messages are discarded [18]. `zmq_ctx_term()` first makes blocking calls return `ETERM` and all further calls except `zmq_close()` fail with `ETERM`,
then blocks until every socket is closed and every sent message is transferred or its linger expired [20]. `ZMQ_BLOCKY` false gives new sockets a zero linger [19]. The guide states the hazards: "if you leave any sockets open,
the `zmq_ctx_destroy()` function will hang forever", and "even if you close all sockets, `zmq_ctx_destroy()` will by default wait forever if there are pending connects or sends unless you set the LINGER to zero on those
sockets before closing them" [31].

**Abortive close.** Signalled by closing the transport: "An implementation SHOULD signal any other error, e.g. overloaded, temporarily refusing connections, etc. by closing the connection. The peer SHALL treat an unexpected
connection close as a temporary error, and SHOULD reconnect." An incoming `ERROR` command is fatal: close and do not reconnect with the same credentials [1]. CurveZMQ repeats the split, and adds that a client peer SHALL NOT
send `ERROR` to the server [10].

**Reconnection rules.** All sockets "SHALL establish connections opportunistically, that is: they connect to an endpoint asynchronously, and if the connection is broken, SHOULD reconnect after a suitable delay" [1]. "To avoid
connection storms, peers should reconnect after a short and possibly randomized interval. Further, if a peer reconnects more than once, it should increase the delay between reconnects." [1] libzmq: `ZMQ_RECONNECT_IVL` default
100 ms, -1 disables, "may be randomized by 0MQ to prevent reconnection storms"; `ZMQ_RECONNECT_IVL_MAX` default 0 (no backoff), otherwise doubling up to that ceiling [18]. DRAFT `ZMQ_RECONNECT_STOP` can stop on
`ECONNREFUSED`, on handshake failure, or after `zmq_disconnect()` [18]. `ZMQ_PAIR` and `ZMQ_CHANNEL` "do not implement functionality such as auto-reconnection" [17].

## 2. Primitives

**Context.** `zmq_ctx_new()`; holds the I/O thread pool and the `inproc` namespace. Options settable only before the first socket: `ZMQ_IO_THREADS` (default 1, may be 0 for `inproc`-only), `ZMQ_MAX_SOCKETS` (default 1023),
`ZMQ_MAX_MSGSZ` (default and maximum `INT_MAX`), `ZMQ_BLOCKY`, `ZMQ_IPV6`, thread scheduling policy/priority/affinity/name prefix, DRAFT `ZMQ_ZERO_COPY_RECV` [19]. The guide: "Call `zmq_ctx_new()` once at the start of a
process, and `zmq_ctx_destroy()` once at the end"; two contexts are two separate ZeroMQ instances; with `fork()` create the context after the fork; rule of thumb one I/O thread per gigabyte/s each way [31][32]. Lifetime ends
at `zmq_ctx_term()`, which blocks per §1.

**Socket.** `zmq_socket(ctx, type)`, an opaque handle. "The newly created socket is initially unbound, and not associated with any endpoints." A socket may connect to many endpoints and bind many at once, "thus allowing
many-to-many relationships", except `ZMQ_PAIR` and `ZMQ_CHANNEL` [17]. There is no `zmq_accept()`: a bound endpoint accepts automatically, and "application code cannot manipulate individual underlying connections" [32]. State
held: type, options, per-peer queues, subscription/group state, the routing-id table (ROUTER, SERVER, PEER, STREAM), and the REQ/REP state machine. Cardinality per context is bounded by `ZMQ_MAX_SOCKETS` [19].

**Thread/ownership rules.** "0MQ has both thread safe socket type and *not* thread safe socket types. Applications MUST NOT use a *not* thread safe socket from multiple threads under any circumstances. Doing so results in
undefined behaviour." Thread-safe: `ZMQ_CLIENT`, `ZMQ_SERVER`, `ZMQ_DISH`, `ZMQ_RADIO`, `ZMQ_SCATTER`, `ZMQ_GATHER`, `ZMQ_PEER`, `ZMQ_CHANNEL` [17]. 37/ZMTP gives the reason and the price: "For thread-safety the sockets API
MUST be atomic, a call to send MUST send the entire message and a call to receive MUST receive the entire message. Therefore thread-safe sockets disallow multipart messages." Such a socket "MUST disallow the sending of
multipart messages and MUST discard any multipart messages received from the wire"; routing-id or group travels as message metadata instead [1]. The guide: "If you're sharing sockets across threads, don't. It will lead to
random weirdness, and crashes." [32]

**Per-peer queue ("pipe", "double queue").** Every pattern RFC specifies the same object: one queue (or a double queue, one per direction) per connected peer; created when initiating an outgoing connection "and SHALL maintain
the double queue whether or not the connection is established"; created when a peer connects; and on that peer's disconnect destroyed, discarding any messages it contains; with sizes constrained to "a runtime-configurable
limit" [2][3][4][5][6][7]. The guide calls it the pipe: PUB/PUSH have send buffers only, SUB/PULL/REQ/REP receive buffers only, DEALER/ROUTER/PAIR both [32].

**Endpoint.** A `transport://address` string. Most transports cannot bind the same endpoint twice; `ipc` can, and "any existing binding to the same endpoint shall be overridden" [25][32]. Wildcard binds require reading back
`ZMQ_LAST_ENDPOINT` before `zmq_unbind()` [24][25].

**Subscription.** Held by SUB/XSUB, mirrored publisher-side. "Subscriptions SHALL be additive and SHALL NOT be idempotent. That is, subscribing to 'A' and '' is the same as subscribing to '' alone. Subscribing to 'A' and 'A'
counts as two subscriptions, and would require two CANCEL commands to undo." [1] libzmq: a new SUB "shall filter out all incoming messages"; an empty subscription takes everything; a message is accepted if it matches at least
one filter [18].

**Group (RADIO/DISH).** Strings of 0-255 bytes, characters `%d1-255`, matched exactly [7]. libzmq is narrower: "Groups are null terminated strings limited to 16 chars length (including null). The intention is to increase the
length to 40 chars (including null). The encoding of groups shall be UTF8." [17] Joined with `zmq_join()`, read with `zmq_msg_group()` [17].

**Routing id.** Two different things share the name. ROUTER/STREAM: a binary string of 1-255 bytes whose first octet must not be zero, optionally chosen by the peer through the `Identity` metadata property, otherwise
generated [1][2][18]. SERVER/PEER: "a non-zero 32-bit unsigned integer value" that the socket assigns and the peer "SHALL NOT" choose [6][17].

**Monitor socket.** `zmq_socket_monitor()` creates a `ZMQ_PAIR` bound to an `inproc://` endpoint; the application connects its own `ZMQ_PAIR` to collect events. It "supports only connection-oriented transports, that is, TCP,
IPC, and TIPC" [23].

**Proxy.** `zmq_proxy(frontend, backend, capture)` runs a bidirectional shuttle in the calling thread and "returns only if/when the current context is closed" [22].

## 3. Message model

**Framing.** After the greeting everything is a frame: one flags octet, a size field of one or eight octets, then the body. "The size does not include the flags field, nor itself, so an empty frame has a size of zero." Short
bodies are 0-255 octets, long bodies 0 to 2^63-1 [1]. Flags: bits 7-3 reserved and MUST be zero; bit 2 COMMAND; bit 1 LONG (64-bit network-order size); bit 0 MORE, which "SHALL be zero on command frames" [1].

**Commands versus messages.** "Commands are used by the ZMTP implementation and not generally visible to the application except in some cases. Commands always consist of one frame, containing a printable command name, a null
octet separator, and data." [1] Defined commands: `READY`, `ERROR`, `SUBSCRIBE`, `CANCEL`, `PING`, `PONG`, plus `JOIN`/`LEAVE` for RADIO/DISH; mechanisms may add their own. Command bodies use a short size field up to 255
octets and a long one beyond, and "the flags octet and the size field is always in clear text" while the body may be encrypted [1].

**Multipart.** "A multipart message is multiple sequential ZMTP messages, where all but the last message has the MORE flag set." [1] Delivery is atomic: "A message SHALL be sent or received atomically; that is, all frames or
none. On sending, the peer SHALL queue all frames of a message in memory until the final frame is sent." [1] The API agrees: "0MQ ensures atomic delivery of messages: peers shall receive either all *message parts* of a
message or none at all. The total number of message parts is unlimited except by available memory." [21] The guide's corollaries: "When you send a multipart message, the first part (and all following parts) are only actually
sent on the wire when you send the final part"; "If you are using `zmq_poll()`, when you receive the first part of a message, all the rest has also arrived"; "You will receive all parts of a message, or none at all"; and only
closing the socket cancels a partially sent message. Multipart does not reduce memory, and the guide tells you to split large files into separate single-part messages [32].

**Size limits.** 2^63-1 octets per frame by grammar [1]; `ZMQ_MAX_MSGSZ` per context defaults to and caps at `INT_MAX` [19]; `ZMQ_MAXMSGSIZE` per socket inbound: "If a peer sends a message larger than ZMQ_MAXMSGSIZE it is
disconnected. Value of -1 means 'no limit'", default -1 [18].

**Headers/properties.** None per message on the wire; all named properties are connection metadata exchanged once [1]. Per-message side data exists only in the thread-safe family: routing-id (SERVER, PEER) and group (RADIO,
DISH) as API attributes, with RADIO sending group and body as a two-part message on the wire [1][17].

**Payload typing.** None. "ZeroMQ strings are length-specified and are sent on the wire *without* a trailing null"; applications own all serialization, and the guide points at Protocol Buffers, msgpack or JSON [31][32].
libzmq "delivers whole messages with wire framing; 10K sent is 10K received" [31].

**Message identity.** ZMTP defines no message id or sequence number. The nearest socket feature is `ZMQ_REQ_CORRELATE`, which prefixes outgoing REQ messages with a request-id frame, making the message `(request id, 0, user
frames...)`, and discards incoming messages not starting with those two frames [18]. Everything else is application convention: the CHP sequence number [14], MDP's frame layout [13], FLP's Client Control Frame ("Clients MAY
use the CCF for any purpose, including request sequence numbering") [15], and ZRE's cyclic sequence, where "when a peer detects gaps in the sequence, or an out-of-sequence message, it SHALL treat the peer as invalid, and
disconnect the peer" [16].

**What the protocol interprets.** MORE, COMMAND, sizes, and the greeting. Beyond that the first frame is pattern-specific: PUB/SUB "SHALL perform a binary comparison of the subscription against the start of the first frame of
the message" [3]; ROUTER/STREAM strip or prepend a routing-id frame [2][17]; REQ prepends and REP strips an empty delimiter [2]. Otherwise bodies are opaque - PUB, SUB, XPUB, XSUB, DEALER, ROUTER, PUSH, PULL and PAIR "SHALL
not filter or modify" messages [2][3][4][5].

**Resource property (new in 3.1).** Lets services share one interface and port: `zmq_bind(s, "tcp://eth0:6000/system/name-service/test")` makes the resource `system/name-service/test`. "The implementation SHALL accept all
resource requests, as a resource may become available at an arbitrary time after the connection has been established." Because resources resolve *after* the security handshake, all services on one endpoint must share
credentials [1]. libzmq 4.3.x does not document it in `zmq_bind`/`zmq_tcp` [24] [inference].

## 4. Patterns and topologies

### 4.1 The socket-type tables from zmq_socket(3)

Reproduced from `zmq_socket(3)`, libzmq master, page last updated 2026-07-26 [17]. "N/A" and "See text" are the manual's own entries; an em dash means the manual's table for that type omits the row.

| Socket | Compatible peers | Direction | Send/receive pattern | Outgoing routing | Incoming routing | Action in mute state |
| --- | --- | --- | --- | --- | --- | --- |
| `ZMQ_REQ` | REP, ROUTER | Bidirectional | Send, Receive, Send, Receive, … | Round-robin | Last peer | Block |
| `ZMQ_REP` | REQ, DEALER | Bidirectional | Receive, Send, Receive, Send, … | Last peer | Fair-queued | — |
| `ZMQ_DEALER` | ROUTER, REP, DEALER | Bidirectional | Unrestricted | Round-robin | Fair-queued | Block |
| `ZMQ_ROUTER` | DEALER, REQ, ROUTER | Bidirectional | Unrestricted | See text | Fair-queued | Drop (see text) |
| `ZMQ_PUB` | SUB, XSUB | Unidirectional | Send only | Fan out | N/A | Drop |
| `ZMQ_SUB` | PUB, XPUB | Unidirectional | Receive only | N/A | Fair-queued | — |
| `ZMQ_XPUB` | SUB, XSUB | Unidirectional | Send messages, receive subscriptions | Fan out | N/A | Drop |
| `ZMQ_XSUB` | PUB, XPUB | Unidirectional | Receive messages, send subscriptions | N/A | Fair-queued | Drop |
| `ZMQ_PUSH` | PULL | Unidirectional | Send only | Round-robin | N/A | Block |
| `ZMQ_PULL` | PUSH | Unidirectional | Receive only | N/A | Fair-queued | Block |
| `ZMQ_PAIR` | PAIR | Bidirectional | Unrestricted | N/A | N/A | Block |
| `ZMQ_CLIENT` | SERVER | Bidirectional | Unrestricted | Round-robin | Fair-queued | Block |
| `ZMQ_SERVER` | CLIENT | Bidirectional | Unrestricted | See text | Fair-queued | Return EAGAIN |
| `ZMQ_RADIO` | DISH | Unidirectional | Send only | Fan out | N/A | Drop |
| `ZMQ_DISH` | RADIO | Unidirectional | Receive only | N/A | Fair-queued | — |
| `ZMQ_SCATTER` | `ZMQ_SCATTER` (as printed) | Unidirectional | Send only | Round-robin | N/A | Block |
| `ZMQ_GATHER` | `ZMQ_GATHER` (as printed) | Unidirectional | Receive only | N/A | Fair-queued | — |
| `ZMQ_PEER` | PEER | Bidirectional | Unrestricted | See text | Fair-queued | Return EAGAIN |
| `ZMQ_CHANNEL` | CHANNEL | Bidirectional | Unrestricted | N/A | N/A | Block |
| `ZMQ_STREAM` | none | Bidirectional | Unrestricted | See text | Fair-queued | EAGAIN |

The SCATTER and GATHER peer rows are printed that way in the manual; 37/ZMTP's table says "SCATTER: GATHER" and "GATHER: SCATTER", which is the sane reading [1][17]. The manual also omits the mute-state row for REP, SUB, DISH
and GATHER while giving it for PULL.

### 4.2 Request-reply (28/REQREP)

"Intended for service-oriented architectures of various kinds. It comes in two basic flavors: synchronous (REQ and REP), and asynchronous (DEALER and ROUTER), which may be mixed in various ways." [2] Usual topology: bind
REP/ROUTER, connect REQ/DEALER [33].

- **REQ.** Any number of REP or ROUTER peers; "SHALL send and then receive exactly one message at a time". Outgoing: prefix an empty delimiter, round-robin, "SHALL block on sending, or return a suitable error, when it has no
  connected peers", "SHALL NOT discard messages that it cannot send". Incoming: "SHALL accept an incoming message only from the last peer that it sent a request to. SHALL discard silently any messages received from other
  peers." [2] The guide: "The REQ-REP socket pair is in lockstep"; any other order returns -1 [31]; the API reports `EFSM` [21].
- **REP.** Any number of REQ or DEALER peers; "SHALL receive and then send exactly one message at a time". Incoming: fair-queue, "SHALL remove and store the address envelope, including the delimiter", pass the rest up.
  Outgoing: prepend the stored envelope, deliver to the originator, "SHALL silently discard the reply, or return an error, if the originating peer is no longer connected", "SHALL not block on sending" [2]. `zmq_socket(3)`:
  "If the original requester does not exist any more the reply is silently discarded" [17].
- **DEALER.** "DEALER works as an asynchronous replacement for REQ." Double queue per peer; a peer is available "only when it has a outgoing queue that is not full"; round-robin over available peers; block or error when none;
  "SHALL not accept further messages when it has no available peers"; never discard; fair-queue inbound [2]. Talking to REP, each message must start with an empty delimiter part [17].
- **ROUTER.** "ROUTER works as an asynchronous replacement for REP." Identifies each double queue by a unique identity string and "SHOULD allow the peer to specify its identity explicitly through the Identity metadata
  property". Incoming: fair-queue, prefix the identity frame. Outgoing: remove the first frame as the queue identity, route if that queue exists and has space, "SHALL either silently drop the message, or return an error,
  depending on configuration, if the queue does not exist, or is full", and "SHALL NOT block on sending" [2].

**The reply envelope.** "The ZeroMQ reply envelope formally consists of zero or more reply addresses, followed by an empty frame (the envelope delimiter), followed by the message body (zero or more frames)." REQ sending
`Hello` produces `[empty][Hello]`. "The REP socket does the matching work: it strips off the envelope, up to and including the delimiter frame, saves the whole envelope, and passes the 'Hello' string up the application." With
a ROUTER-DEALER proxy between and a REQ identity of `ABC`, the broker reads `[ABC][empty][Hello]` and forwards all three frames; REP strips the whole envelope; on the return path ROUTER consumes `ABC` and sends
`[empty][World]`, and REQ checks and discards the delimiter [33]. ROUTER itself "does not interpret the complete envelope and knows nothing about the empty delimiter"; its only concern is the single identity frame [33].

**Identities.** "An identity (also called an address) is a binary string whose only meaning is 'this is a unique handle to the connection.'" ZeroMQ 2.2 and earlier used UUIDs; v3.0 and later generate 5 bytes, `0` plus a
random 32-bit integer [33]. A peer sets `ZMQ_IDENTITY` before bind/connect so ROUTER uses a logical address instead [33]; libzmq deprecates that name for `ZMQ_ROUTING_ID`, while chapter 3 names only `ZMQ_IDENTITY` [18][33].
Key limitation: ROUTER learns an identity only after that peer has sent something, so "an application can really reply, but cannot spontaneously talk to a peer" - true even when ROUTER is the connecting side [33].
`ZMQ_PROBE_ROUTER` closes the gap by making REQ/DEALER/ROUTER send an empty message on every new connection, giving "the ROUTER application with an event signaling the arrival of a new peer"; the application must filter
those, and the option must not be set against other socket types [18]. `ZMQ_CONNECT_ROUTING_ID` assigns the routing id of the next `zmq_connect()`, "useful when connecting ROUTER to ROUTER, or STREAM to STREAM, as it allows
for immediate sending to peers" [18]. Chapter 3's only identity caution is unrelated to ROUTER: do not set identities on subscribers, because connecting to a running broker then yields outdated state [33].

**Unroutable messages.** "ROUTER sockets do have a somewhat brutal way of dealing with messages they can't send anywhere: they drop them silently." Since v3.2, `ZMQ_ROUTER_MANDATORY` makes that `EHOSTUNREACH` [33]. Full
libzmq rule: 0 discards silently "when it cannot be routed or the peers SNDHWM is reached"; 1 returns `EHOSTUNREACH` if unroutable or `EAGAIN` if the SNDHWM is reached under `ZMQ_DONTWAIT`, and without `ZMQ_DONTWAIT` blocks
until `ZMQ_SNDTIMEO` or space appears. It also changes polling: with the option set, `ZMQ_POLLOUT` fires only if some peer is sendable; without it "the socket will generate a `ZMQ_POLLOUT` event on every call to `zmq_poll`"
[18]. `ZMQ_ROUTER_HANDOVER` resolves duplicate identities: default 0 rejects the newcomer, 1 "shall hand-over the connection to the new client and disconnect the existing one" [18].

**Legal and illegal combinations, as chapter 3 tabulates them** [33]:

| Combination | Guide's own verdict |
| --- | --- |
| REQ → REP | Basic strict request-reply; REQ must start; REP cannot send first, `EFSM` if attempted. |
| DEALER → REP | Asynchronous client to many REP servers, but must emulate REQ's envelope exactly. |
| REQ → ROUTER | Asynchronous server for many REQ clients; the ROUTER application must know the `[identity][empty][data]` shape. |
| DEALER → ROUTER | "Most powerful pairing": both sides asynchronous and format-controlling, so the application must design a protocol. |
| DEALER → DEALER | Only when the DEALER speaks to exactly one peer; the worker becomes asynchronous and may emit any number of replies. "Tricky and rarely needed." |
| ROUTER → ROUTER | Sounds like N-to-N, is "the most difficult pairing"; avoid until well advanced. |
| REQ → REQ | Invalid: both want to send first. |
| REQ → DEALER | Invalid in practice: a second REQ breaks it because DEALER cannot identify the original peer for a reply. |
| REP → REP | Invalid: each waits for the other. |
| REP → ROUTER | Theoretically possible, "messy and gives nothing over DEALER→ROUTER". |

Memory aid: "DEALER is like an asynchronous REQ; ROUTER is like an asynchronous REP." [33]

**Load-balancing broker (LRU worker queue).** The problem: "round robin becomes inefficient when tasks do not take approximately the same time"; the post-office analogy strands fast customers behind slow ones at fixed
counters, where a single queue feeds whichever counter frees up. The broker needs both a readiness notification and a least-recently-used list. Mechanism: "workers send a `ready` message when they start and after they finish
each task"; the broker reads those, takes the identity from ROUTER, and sends the next task to that worker. "This twists request-reply: broker sends task as the reply; a task result is sent back as a new request." Adding a
frontend ROUTER gives the full broker; the envelope walk is `[CLIENT][empty][Hello]` inbound, `[WORKER][empty][CLIENT][empty][Hello]` to the backend, and the reverse back. Poll rule: always poll the backend, poll the frontend
only while a worker is available [33]. `zmq_proxy()` with ROUTER frontend and DEALER backend gives the plain shared queue: "Requests shall be fair-queued from frontend connections and distributed evenly across backend
connections. Replies shall automatically return to the client that made the original request." [22]

**Asynchronous client/server.** DEALER clients to one ROUTER server: clients may send many requests without waiting, each gets zero or more replies, and the server may send many replies. Its internal dialogue to the worker
pool is DEALER→DEALER, chosen over load-balancing ROUTER→DEALER "because it lowers per-request latency; trade-off is greater risk of unbalanced work distribution". Envelope trace: client sends `[body]`, ROUTER receives
`[client-identity][body]`, both frames go to a worker, the worker returns both, the server routes the second by the first [33].

**Inter-broker routing (Peering 1-3).** Each broker owns six sockets: `localfe`/`localbe`, `cloudfe`/`cloudbe`, `statefe`/`statebe`. State flows over PUB/SUB, each broker publishing its available-worker count, with the sender
address carried explicitly because it is needed to send tasks back; cloud task and reply flows use two asynchronous ROUTER sockets so requests need not be distinguished from replies by inspection. Federation - brokers
pretending to be each other's clients and workers - is rejected for load balancing because the emulated lock-step permits one task at a time, but kept as suitable for service-name routing. The rule against extra buffers:
"ZeroMQ sockets *are* queues already". Recorded build failures: ROUTER silently dropped unroutable frames and froze clients; reading more than one ready socket per loop lost the first message; `zmsg` encoded UUIDs as C
strings and corrupted UUIDs containing zero bytes. Peering 3 does not detect a departed cloud peer, so others keep routing to its advertised capacity and lose requests [33].

### 4.3 Publish-subscribe (29/PUBSUB)

"Intended for event and data distribution, usually from a small number of publishers to a large number of subscribers, but also from many publishers to a few subscribers. For many-to-many use-cases the pattern provides raw
socket types (XPUB, XSUB) to construct distribution proxies, also called brokers." [3]

- **PUB.** "PUB is used mainly for transient event distribution where stability of the network (e.g. consistently low memory usage) is more important than reliability of traffic." One outgoing queue per subscriber; "SHALL
  silently discard any messages that subscribers send it"; MAY send to all or only to matching subscribers depending on transport; "SHALL perform a binary comparison of the subscription against the start of the first frame of
  the message"; "SHALL silently drop the message if the queue for a subscriber is full"; "SHALL NOT block on sending"; subscription commands are not delivered to the application [3].
- **SUB.** Receive only. "SHALL silently discard messages if the queue for a publisher is full"; fair-queues publishers; MAY prefix-filter depending on transport [3].
- **XPUB.** As PUB plus a double queue per subscriber, inbound messages fair-queued to the application, subscription commands delivered to the application, optional normalization "so that multiple identical subscriptions
  result in a single command only", and "SHALL, if the subscriber peer disconnects prematurely, generate a suitable unsubscribe request for the calling application" [3].
- **XSUB.** As SUB plus sending messages and subscriptions upstream; "SHALL send all messages to all connected publishers"; silently drops on a full outgoing queue; never blocks; and "When closing a connection to a publisher
  SHOULD send unsubscribe requests for all subscriptions" [3].

Wire form: filtering "SHALL happen at the publisher side (the PUB or XPUB socket)". `SUBSCRIBE`/`CANCEL` carry a binary subscription; "A subscription of 'A' SHALL match all messages starting with 'A'. An empty subscription
SHALL match all messages." [1] At the API, XPUB/XSUB expose subscriptions as messages: "byte 1 (for subscriptions) or byte 0 (for unsubscriptions) followed by the subscription body. Messages without a sub/unsub prefix are
also received, but have no effect on subscription status." [17] Options: `ZMQ_XPUB_VERBOSE`, `ZMQ_XPUB_VERBOSER`, `ZMQ_XPUB_MANUAL`, DRAFT `ZMQ_XPUB_MANUAL_LAST_VALUE`, `ZMQ_XPUB_WELCOME_MSG` (sent on connect and reconnect),
`ZMQ_XPUB_NODROP` (return `EAGAIN` instead of dropping at SNDHWM, for XPUB and PUB), `ZMQ_INVERT_MATCHING` (send to all except matching subscribers; must be set on both sides for SUB) [18].

**Who binds.** "Practical default direction: bind PUB, connect SUB, unless topology prevents it" [31]. The pub-sub proxy binds XSUB and XPUB at well-known endpoints, both publishers and subscribers connect to it, and the
proxy must forward subscriptions from the XPUB side to the XSUB side [32]. `zmq_proxy()` with XSUB frontend and XPUB backend is the built-in forwarder and "may be used to bridge networks transports, e.g. read on tcp:// and
forward on pgm://" [22].

**Nobody to talk to.** "If a publisher has no connected subscribers, it drops all messages." [31] The Espresso trace shows it: after the subscriber's unsubscriptions the publisher thread keeps sending and "the PUB socket
silently drops those messages" [35].

**Slow joiner.** "the subscriber will always miss the first messages that the publisher sends… This is because as the subscriber connects to the publisher (something that takes a small but non-zero time), the publisher may
already be sending messages out." The arithmetic: 5 ms of connection setup against 1 M messages/s means 1000 messages take 1 ms, so all of them can go out during setup. Sleeping is "extremely fragile as well as inelegant and
slow"; the chapter 2 answer is a second REQ/REP flow where subscribers announce readiness and the publisher waits for the expected count [31][32].

**Pub-sub envelopes.** Optional: put the key in its own frame. "Subscription is a prefix match", and the envelope prevents accidental payload matches because "the match won't cross a frame boundary". The filter accepts or
rejects the whole multipart message, never one part [32].

### 4.4 Pipeline (30/PIPELINE)

"Intended for task distribution, typically in a multi-stage pipeline where one or a few nodes push work to many workers, and they in turn push results to one or a few collectors. The pattern is mostly reliable insofar as it
will not discard messages unless a node disconnects unexpectedly. It is scalable in that nodes can join at any time." [4] PUSH: one outgoing queue per peer; a peer is available only when its queue is not full; round-robin
over available peers; block or error when none; never discard. PULL: fair-queue [4]. `zmq_proxy()` with PULL frontend and PUSH backend is the streamer [22].

Topology: the ventilator and sink are stable and bind; workers are dynamic and connect upstream and downstream, "so workers are dynamic and can be added without endpoints/configuration changes". PUSH distributes evenly - load
balancing - only once all workers are connected; PULL at the sink is fair-queuing. The pipeline has its own slow joiner: "the first PULL socket to connect will grab an unfair share of messages. The accurate rotation of
messages only happens when all PULL sockets are successfully connected, which can take some milliseconds. As an alternative to PUSH/PULL, for lower data rates, consider using ROUTER/DEALER and the load balancing pattern."
[31][32]

### 4.5 Exclusive pair (31/EXPAIR)

"PAIR is not a general-purpose socket but is intended for specific use cases where the two peers are architecturally stable. This usually limits PAIR to use within a single process, for inter-thread communication." At most
one peer, a double queue, block or error when the peer is unavailable, never discard [5]. `zmq_socket(3)` warns: "their inability to auto-reconnect coupled with the fact new incoming connections will be terminated while any
previous connections (including ones in a closing state) exist makes them unsuitable for TCP in most cases" [17].

### 4.6 Draft patterns

- **CLIENT/SERVER (41/CLIENTSERVER).** "the thread-safe alternative of the router-dealer pattern… All flows are initiated by the CLIENT", and explicitly "This pattern is meant to deprecate and eventually replace the
  request-reply pattern." CLIENT: round-robin out, fair-queue in, blocks rather than dropping, must not send multipart, "MUST discard any part of a multipart message" received and "MAY disconnect a peer that is sending
  multipart messages". SERVER: a non-zero 32-bit routing id per queue that the peer "SHALL NOT" set; "SHALL return an error if the queue does not exist"; blocks on a full queue unless configured otherwise; never discards [6].
  libzmq: an unspecified or unknown routing id gives `EHOSTUNREACH`; a full buffer blocks or gives `EAGAIN` under `ZMQ_DONTWAIT`; and "replies from a `ZMQ_SERVER` socket will go to the first client thread that calls
  `zmq_msg_recv`", so per-thread replies need one CLIENT socket per thread [17].
- **RADIO/DISH (48/RADIO-DISH).** "the thread-safe alternative of the pubsub pattern", with exact-match groups instead of prefix topics. RADIO fans out, drops on a full queue, never blocks, and discards anything a dish sends
  it; DISH joins groups, fair-queues, discards on a full queue, and attaches the group to each delivered message [7]. On the wire RADIO sends group then body as a two-part message, and DISH sends `JOIN`/`LEAVE` [1]. UDP works
  only with RADIO/DISH: DISH binds, RADIO connects, unicast or multicast [28].
- **SCATTER/GATHER.** "the thread-safe version of the pipeline pattern", same round-robin and fair-queue rules [17].
- **PEER/CHANNEL.** PEER "talks to a set of PEER sockets… Peer can both connect and bind and mix both of them with the same socket", for peer-to-peer networks; `zmq_connect_peer()` returns the new peer's routing id so a PEER
  can speak first, `zmq_disconnect_peer()` drops one; unknown routing id gives `EHOSTUNREACH`, a full buffer blocks or gives `EAGAIN`, nothing is dropped. CHANNEL is "the thread-safe version of the exclusive pair pattern"
  with PAIR's single-peer and no-reconnect caveats [17].
- **STREAM.** For non-ZeroMQ TCP peers: prepends a routing-id part on receive, strips one on send; unroutable sends give `EHOSTUNREACH` or `EAGAIN`; a zero-length message signals connect and disconnect; sending routing id
  plus a zero-length message closes a connection; "You must send one routing id frame followed by one data frame." [17]

## 5. Flow control and backpressure

**Unit of credit.** Messages, not bytes. `ZMQ_SNDHWM` and `ZMQ_RCVHWM` are "a hard limit on the maximum number of outstanding messages 0MQ shall queue in memory for any single peer that the specified socket is communicating
with. A value of zero means no limit." Both default to 1000 [18].

**Who grants it.** Nobody. There is no peer-to-peer credit signalling in ZMTP 3.1; the high-water mark is a purely local bound on a local queue, applied per peer. 37/ZMTP lists credit-based flow control under "Topics for
Discussion", not as a feature: "Credits, as a signaling mechanism for asynchronous flow control. This allows fine control over the amount of queued data. In this scenario, the receiving peer (DEALER, PULL, REP) would send
credit which would be used up by messages routed to it. The minimal use-case is for PUSH/DEALER-to-PULL/DEALER round-robin routing. Credit could be octets, or messages." [1] The pattern RFCs only require that queue sizes be
"constrain[ed]… to a runtime-configurable limit" [2][3][4][5][6][7].

**Where the backlog sits.** In per-peer pipes on both sides, plus the kernel socket buffers. `zmq_tcp(7)`: "you may expect that setting ZMQ_SNDHWM to 100 on a socket using TCP transport will have the effect of blocking the
transmission of the 101-th message if the receiver is slow. This is very unlikely when using TCP transport since OS TCP buffers will typically provide enough buffering to allow you sending much more than 100 messages… simply
don't rely on the exact HWM value." [24] `zmq_setsockopt(3)` adds that "the actual limit may be as much as 90% lower depending on the flow of messages on the socket" [18], and the guide says effective capacity "can be as low
as half because of queue implementation" [32]. For `inproc`, "sender and receiver share buffers; real HWM is the sum of both sides' configured HWMs" [32]. `ZMQ_SNDBUF`/`ZMQ_RCVBUF` set the kernel buffers separately; the
nanomsg comparison calls the result "strange double-buffering behaviour… if you want to limit the amount of outgoing data, you have to set both ZMQ_SNDBUF and ZMQ_SNDHWM" [18][37].

**What happens on exhaustion.** Per socket type, per the mute-state column in §4.1. The guide's shorthand: "When your socket reaches its HWM, it will either block or drop data depending on the socket type. PUB and ROUTER
sockets will drop data if they reach their HWM, while other socket types will block." [32] That is incomplete against `zmq_socket(3)`, where XPUB, XSUB and RADIO also drop and SERVER/PEER/STREAM return `EAGAIN` [17], and
against 29/PUBSUB, where a *receiving* SUB or XSUB "SHALL silently discard messages if the queue for a publisher is full" [3]. `ZMQ_DONTWAIT` turns a blocking send into `EAGAIN`; `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` bound the wait
[18][21]. `ZMQ_XPUB_NODROP` converts a PUB/XPUB drop into `EAGAIN`, `ZMQ_ROUTER_MANDATORY` a ROUTER drop into `EAGAIN`/`EHOSTUNREACH` [18].

**Queues that fill before a connection exists.** "By default queues will fill on outgoing connections even if the connection has not completed. This can lead to 'lost' messages on sockets with round-robin routing (REQ, PUSH,
DEALER). If this option is set to 1, messages shall be queued only to completed connections. This will cause the socket to block if there are no other connections, but will prevent queues from filling on pipes awaiting
connection." - `ZMQ_IMMEDIATE`, default 0 [18].

**Conflation.** `ZMQ_CONFLATE` keeps only the last message per queue and "Ignores ZMQ_RCVHWM and ZMQ_SNDHWM options. Does not support multi-part messages"; for PULL, PUSH, SUB, PUB, DEALER; default off. Caveat: "If recv is
not called on the inbound socket, the queue and memory will grow with each message received." [18]

**Multicast and batching.** PGM/EPGM sockets "are rate limited by default" via `ZMQ_RATE` and `ZMQ_RECOVERY_IVL`; the transport page recommends raising `ZMQ_RATE`, `ZMQ_SNDBUF` and `ZMQ_RCVBUF` for high rates [27]. DRAFT
`ZMQ_IN_BATCH_SIZE`/`ZMQ_OUT_BATCH_SIZE` bound batch sizes [18].

## 6. Delivery guarantees and acknowledgement

**There is no acknowledgement in ZMTP.** No frame, command or field acknowledges a message. The only round-trip primitive is `PING`/`PONG`, which certifies connection liveness, not delivery; `PONG` echoes only the 16-octet
`ping-context`, not a message identifier [1].

**What a send returns.** "A successful invocation of `zmq_send()` does not indicate that the message has been transmitted to the network, only that it has been queued on the socket and 0MQ has assumed responsibility for the
message." [21]

**What the protocol does promise.** Three rules for all sockets: "A message SHALL be sent or received atomically; that is, all frames or none"; "A message SHALL NOT be delivered more than once to any peer"; "All messages
between two immediate peers SHALL be delivered in order." [1] All three are hop-scoped - "immediate peers", not end to end.

**At-most / at-least / exactly-once.** ZeroMQ does not use these terms. Read against the rules above, one ZMTP hop is at-most-once with in-order, no-duplicate delivery of whatever arrives [inference]; anything stronger is an
application protocol. Per-socket nuances that bear on it: PUB/XPUB/RADIO drop on a full peer queue and never block, and PUB drops everything with no subscribers [3][7][31]; ROUTER drops silently when a destination is unknown
or full unless `ZMQ_ROUTER_MANDATORY` [2][18]; REP "SHALL silently discard the reply… if the originating peer is no longer connected" [2]; REQ silently discards anything from a peer other than the last one it sent to [2];
PUSH, PULL, DEALER, PAIR, CLIENT, SCATTER, GATHER and CHANNEL "SHALL NOT discard messages that it cannot queue" and block instead [2][4][5][6][17]; a disconnecting peer takes its queued messages with it, because the socket
"SHALL destroy its double queue and SHALL discard any messages it contains" [2][3][4][5][6][7]; and exceeding `ZMQ_MAXMSGSIZE` disconnects the sender [18].

**Application-level acknowledgements the guides define.** MDP: the worker "SHALL send zero or more PARTIAL commands for a single REQUEST, followed by exactly one FINAL command"; "There is no response to a READY. The worker
SHOULD assume the registration succeeded until or unless it receives a DISCONNECT, or it detects a broker failure through heartbeating"; and MDP assumes "Workers are idempotent, i.e. it is safe to execute the same request
more than once" [13]. CHP: `KTHXBAI` terminates a snapshot carrying the highest sequence number sent, and `HUGZ` is an idle liveness beat whose absence the client "MAY treat… as an indicator that the server has crashed" [14].
Titanic: `titanic.close` is the client's confirmation that the reply is stored or processed - the only recipient-side commit acknowledgement in the whole guide, and it certifies that the reply may be wiped [34]. FLP: `PING`
is answered by `PONG`, and a request's Client Control Frame comes back unmodified [15].

**Redelivery rules.** None in the protocol. Redelivery is client retry (Lazy Pirate), broker resend to another worker ("Allow the broker to recover from dead or disconnected workers by resending requests to other workers"
[13]), or Titanic's indefinite retry [34]. 37/ZMTP raises requeueing as an open question and rejects it in the same breath: "Do we want DEALER and PUSH sockets to requeue undeliverable messages to other peers? This can
improve reliability but results in out-of-order messages, and is not robust against messages lost in transit, or already delivered to the other peer." [1]

## 7. Ordering and duplicates

**Promised order.** "All messages between two immediate peers SHALL be delivered in order." [1] Scope: one connection between two directly connected peers. No promise across peers, hops, or sockets.

**Where order is not promised.** A socket with several peers fair-queues inbound and round-robins outbound, so the interleaving of two peers' streams is unspecified [2][3][4]. Through an intermediary, order is whatever the
intermediary produces: CHP's server "centralizes every change and imposes one sequence in arrival order", and chapter 5 rejects direct client publication "because it loses consistent ordering: competing writes to the same key
can leave clients with different values" [35]. Clone Model Six assumes "multiple clients do not update the same hash key simultaneously", because "two servers can receive client updates in different orders, so backup may
apply pending updates in an order different from primary"; the safe case is that "updates from one client reach both servers in same order" [35]. REQ with `ZMQ_REQ_RELAXED` resets the state machine and sends to the next
available peer, so a late reply to an aborted request "can be reported as the reply to the superseding request" unless `ZMQ_REQ_CORRELATE` is also set [18]. Within a message, frames are ordered by construction and the message
is atomic [1][21].

**Ordering aids the protocols add.** CHP: "The sequence number MUST be strictly incremental. The client MUST discard any KVPUB commands whose sequence numbers are not strictly greater than the last KTHXBAI or KVPUB command
received." [14] ZRE: "The first message from a peer (HELLO) MUST have sequence number 1, and every message must have a strictly incrementing sequence number. When a peer detects gaps in the sequence, or an out-of-sequence
message, it SHALL treat the peer as invalid, and disconnect the peer." [16] Clone's server-added sequence numbers let clients detect holes from congestion or overflow [35].

**Where duplicates arise.** Not from the protocol: "A message SHALL NOT be delivered more than once to any peer" [1]. They come from retries and fan-out. Chapter 4 names the hard case: "Death while sending reply is
problematic: server believes work completed, client retries after lost reply/network failure, and work runs twice." Freelance Model Two blasts one request to every server, so "A server may receive duplicate request" by design
[34]. PUB sends the message once per subscriber over TCP, so each subscriber sees one copy [3].

**What suppresses duplicates.** Only application state. Chapter 4's recipe: "Client stamps every request with unique client identifier and unique message number. Before replying, server stores reply keyed by client ID +
message number. On repeat request for that key, server does not process; it resends stored reply." The idempotency taxonomy: idempotent - stateless task distribution, name lookup; not idempotent - logging, services with
downstream effects, shared-data mutation such as a debit "unless extra work makes it idempotent". "Idempotency is not something you take a pill for." [34] `ZMQ_REQ_CORRELATE` suppresses stale replies at the socket level, not
duplicate processing [18]. CurveZMQ suppresses *wire* replay: every box uses a unique nonce, the short nonce is an incrementing integer, and "The server SHALL verify that a client connection does use correctly incrementing
short nonces, and SHALL disconnect clients that reuse a short nonce" [10].

**Subscription non-idempotence.** "Subscribing to 'A' and 'A' counts as two subscriptions, and would require two CANCEL commands to undo." [1] libzmq's default XPUB behaviour hides that - "only the first subscription to each
filter will be passed" - unless `ZMQ_XPUB_VERBOSE` is set, and 29/PUBSUB permits either behaviour ("MAY, depending on configuration, normalize commands") [3][18].

## 8. Failure behaviour

| Event | What the sending/publishing side observes | What the receiving side observes | What is lost | What is ambiguous |
| --- | --- | --- | --- | --- |
| Peer process crash | TCP error or nothing at all; with `ZMQ_HEARTBEAT_IVL` set, an unanswered `PING` times the connection out [1][18]; monitor gives `ZMQ_EVENT_DISCONNECTED` [23]; reconnect begins after `ZMQ_RECONNECT_IVL` [18]; DRAFT `ZMQ_DISCONNECT_MSG`/`ZMQ_ROUTER_NOTIFY` can synthesize an application-visible disconnect [18] | Nothing | Everything queued for that peer: the socket "SHALL destroy its double queue and SHALL discard any messages it contains" [2] | Whether an in-flight request was processed; chapter 4 flags server death while sending the reply as exactly this case [34] |
| Network partition | Silence. Without heartbeats, "TCP timeout can be roughly 30 minutes, so it can be impossible to distinguish peer death, disconnection, or prolonged absence" [34]; ZMTP's stated motive for `PING` is that "Network connections can go stale and die without reporting TCP errors" [1] | Silence; a SUB cannot tell "good silence (no data) from bad silence (peer died)" [34] | Messages sent during the outage: "ZeroMQ will automatically reconnect in such cases, but in the meantime, messages may get lost" [34] | Liveness. Binary Star's split brain lives here: a partition can let both servers see client votes and both go active [34] |
| Reconnect | `ZMQ_EVENT_CONNECT_RETRIED` with the recalculated interval, then `CONNECTED` and `HANDSHAKE_SUCCEEDED` [23]; interval grows to `ZMQ_RECONNECT_IVL_MAX` and may be randomized [18] | `ZMQ_EVENT_ACCEPTED`; a new ROUTER identity unless the peer set one; XPUB sees subscriptions resent | Whatever was discarded on disconnect | Whether this is the same logical peer, absent an explicit identity. Chapter 4: automatic reconnection does **not** re-register a worker at a restarted broker; the worker must destroy and recreate its socket [34] |
| HWM reached, PUB/XPUB/RADIO | Send succeeds, message dropped for that peer: "SHALL silently drop the message if the queue for a subscriber is full. SHALL NOT block on sending." [3][7] With `ZMQ_XPUB_NODROP` and `ZMQ_DONTWAIT`, `EAGAIN` [18] | A gap, detectable only if the application numbers messages [35] | Messages for the slow peer only | Which peer lost what; the publisher is not told |
| HWM reached, ROUTER | Dropped silently by default; with `ZMQ_ROUTER_MANDATORY`, `EAGAIN` under `ZMQ_DONTWAIT`, else block until `ZMQ_SNDTIMEO` [18] | A gap | The routed message | Nothing if `ZMQ_ROUTER_MANDATORY` is set; everything if not - "they drop them silently" [33] |
| HWM reached, PUSH/DEALER/REQ/PAIR/CLIENT/SCATTER/CHANNEL | `zmq_send()` blocks until the mute state ends, or `EAGAIN` with `ZMQ_DONTWAIT`; "messages are not discarded" [17][21] | Nothing | Nothing | Nothing; backpressure is visible to the sender |
| HWM reached, SERVER/PEER/STREAM | Blocks, or `EAGAIN` with `ZMQ_DONTWAIT`; "shall not drop messages in any case" [17] | Nothing | Nothing | Nothing |
| HWM reached, SUB/XSUB/PULL receive side | Not visible to the sender | SUB/XSUB "SHALL silently discard messages if the queue for a publisher is full" [3]; PULL's table says "Block" [17], i.e. backpressure to PUSH | For SUB/XSUB, the discarded messages | For SUB, whether a gap was network loss or local overflow |
| Oversized message | Peer disconnects the sender: "If a peer sends a message larger than ZMQ_MAXMSGSIZE it is disconnected" [18] | Disconnect; monitor `ZMQ_EVENT_DISCONNECTED` [23] | The message and everything queued on that connection | The reason: no error command is defined for this, only a close |
| Unroutable ROUTER message | Default: nothing, message gone. `ZMQ_ROUTER_MANDATORY=1`: `EHOSTUNREACH` [18][33] | Nothing | The message | Default config makes the loss invisible; the guide calls it "remarkably easy to lose messages by accident" [32] |
| CURVE auth failure | Server sends `ERROR` and closes, or closes silently: "If the client does not pass authentication, the server SHALL not respond except by closing the connection" [10]; monitor gives `HANDSHAKE_FAILED_AUTH` with the ZAP status code, or `HANDSHAKE_FAILED_PROTOCOL` with a `ZMQ_PROTOCOL_ERROR_*` value [23] | Client treats `ERROR` as fatal and "SHALL NOT try to reconnect using the same credentials"; a silent close is a soft error it MAY retry [10] | Nothing was sent yet: metadata comes only after the handshake [1] | Whether a silent close was rejection or overload; 37/ZMTP says signal other errors "by closing the connection" and treat an unexpected close "as a temporary error" [1] |
| ZAP handler down or absent | ZAP requires that "The handler SHALL start before any server starts", one handler per process bound to `inproc://zeromq.zap.01` [11]; a failed dialog gives `HANDSHAKE_FAILED_PROTOCOL` with `ZMQ_PROTOCOL_ERROR_ZAP_UNSPECIFIED`, `_MALFORMED_REPLY`, `_BAD_REQUEST_ID`, `_BAD_VERSION`, `_INVALID_STATUS_CODE` or `_INVALID_METADATA` [23] | Connection fails | The connection | ZAP status 300 (temporary) versus 500 (internal) is the handler's judgement; the RFC defines the codes but not the server's retry policy [11] |
| Context termination with pending messages | All blocking calls return `ETERM`, all further calls except `zmq_close()` fail with `ETERM`, then `zmq_ctx_term()` blocks until every socket is closed and every sent message is "physically transferred to a network peer, or the socket's linger period… has expired" [20] | Receives whatever drained within the linger | Anything still queued when a finite linger expires; everything if `ZMQ_LINGER` is 0 [18] | With the default infinite linger there is no ambiguity but also no bound: "`zmq_ctx_destroy()` will by default wait forever if there are pending connects or sends" [31] |
| Duplicate ROUTER identity | Default: the new client is rejected. `ZMQ_ROUTER_HANDOVER=1`: "the ROUTER socket shall hand-over the connection to the new client and disconnect the existing one" [18] | The displaced client sees a disconnect | The displaced client's queued messages | Which of two peers claiming one identity is legitimate; the protocol has no answer |
| Bind conflict on `ipc` | Bind succeeds and steals the endpoint: "if a second process binds to an endpoint already bound by a process, this will succeed and the first process will lose its binding. In this behaviour, the 'ipc' transport is not consistent with the 'tcp' or 'inproc' transports." On Linux the `@` abstract namespace fails instead [25] | Loses its binding | New connections | Which process owns the endpoint |
| Slow consumer | Nothing; PUB has "one setting, which is *full-speed*" [35] | Growing delay, then gaps once the publisher's HWM is hit | Messages beyond the HWM | Whether the subscriber is temporarily peaky or permanently too slow - the question Suicidal Snail answers by having the subscriber exit [35] |

## 9. Reliability recipes

The framing first. "What is 'Reliability'?" answers itself: "if we can handle a certain set of well-defined and understood failures, then we are reliable with respect to those failures. No more, no less." The working
definition is "keeping things working properly when code freezes or crashes", shortened to "dies" [34]. The failure list, in the guide's words and roughly descending probability: application code, which "can crash and exit,
freeze and stop responding to input, run too slowly for its input, exhaust all memory"; system code, which "can die for the same reasons as application code… and especially run out of memory if it tries to queue messages for
slow clients"; message queues, which "can overflow… When a queue overflows, it starts to discard messages. So we get 'lost' messages"; networks, which "can fail (e.g., WiFi gets switched off or goes out of range). ZeroMQ will
automatically reconnect in such cases, but in the meantime, messages may get lost"; hardware; exotic network failures; and whole data centres. The chapter covers the first five, "as covering 99.9% of real-world requirements
outside large companies" [34]. Reliability is analysed per pattern: in request-reply a dead server leaves a client with no answer; in pub-sub the publisher learns nothing, because there is no back channel; in a pipeline the
ventilator cannot see a dead worker, but the collector can notice a missing task and ask for a resend [34].

**Heartbeating**, on which most of the recipes rest [34]. *Problem:* is the peer alive, when TCP will not say for up to half an hour. *Mechanism:* three options - (1) "Shrugging It Off", no heartbeat; (2) one-way heartbeats,
each peer emitting one per second or so, nothing for several seconds meaning dead, with "Treat any incoming data as a heartbeat, not just special heartbeat messages", and the only option for pub-sub since SUB cannot talk
back; (3) ping-pong, a payload-free ping answered by a payload-free pong, uncorrelated, working for all ROUTER-based brokers, with "treat any incoming data as pong, and ping only when otherwise not sending data". *Guarantee:*
a detection time the application chooses. *Cost:* "Heartbeating is difficult" - roughly five hours to get Paranoid Pirate's right versus perhaps ten minutes for the rest of the chain. Build and test the heartbeat exchange
under simulated failures *before* the rest of the message flow, because retrofitting is much harder. Settings must be configurable and are usually negotiated; intervals range "as low as 10 msecs" to "as high as 30 seconds";
with unequal intervals the poll timeout must be the lowest interval and never infinite. Heartbeat on the message socket so it doubles as a keep-alive against firewalls. *Failure modes:* false failures are the characteristic
risk; large data delays heartbeats and can cause false timeouts under congestion; PUSH and DEALER queue heartbeats to a dead peer, so on return it "can receive thousands of stale heartbeats", where PUB-SUB drops them; one
network-wide timeout suits neither aggressive detection nor power-saving peers; and heartbeat-less applications tracking ROUTER peers "leak per-peer resources as disconnect/reconnect occurs and get slower". *Present
recommendation:* "if designing this today, I'd probably try a ping-pong approach instead." Chapter 4 never mentions ZMTP's `PING`/`PONG`; its heartbeats are application messages on the application's own socket [34]. ZMTP
3.1's is the wire-level equivalent [1], implemented by libzmq's `ZMQ_HEARTBEAT_IVL` family [18].

**Lazy Pirate** - client-side reliable request-reply [34]. *Problem:* "A blocking REQ client hangs forever if its server crashes, or request/reply is lost." *Mechanism:* poll the REQ socket and receive only when a reply has
arrived; resend on timeout; give up after several attempts. Because REQ enforces strict alternation and yields `EFSM` otherwise, "The brute-force remedy is close and reopen the REQ socket after an error." *Guarantee:* the
client gets an in-order reply or abandons, never blocking indefinitely. *Cost:* trivial to add; extra latency; duplicated work on retry; socket churn. *Failure modes:* "doesn't failover to backup or alternate servers"; a
permanently dead server is not solved; one server remains a poor architecture. Do not casually swap REQ for DEALER - you must recreate the envelope behaviour or receive unexpected replies.

**Simple Pirate** - reliable queuing via the load-balancing broker [34]. *Problem:* Lazy Pirate cannot fail over to another worker. *Mechanism:* an unchanged Lazy Pirate client in front of the chapter 3 load-balancing broker;
the server becomes a stateless worker signalling `ready` with REQ. *Guarantee:* any number of clients and workers; workers may crash and restart repeatedly while the queue runs; client retries recover from a dead worker.
*Cost:* almost nothing beyond composing two patterns; introduces a central queue. *Failure modes:* the central queue is "one real weakness", hard to manage and a single point of failure; a restarted queue has not received the
workers' `ready` messages, so it does not know them; a worker that dies while idle is unnoticed until work is sent to it, and the client then retries uselessly.

**Paranoid Pirate** - reliable queuing with heartbeating [34]. *Problem:* Simple Pirate survives neither a queue restart nor an idle dead worker. *Mechanism:* heartbeats both ways between queue and worker; the worker switches
REQ to DEALER so it can send and receive at any time and manages envelopes explicitly; the client stays Lazy Pirate; the worker retries forever with exponential backoff. Liveness: `HEARTBEAT_LIVENESS` starts at 3 ("3-5 is
reasonable"); poll once per `HEARTBEAT_INTERVAL` (1000 ms in the example); any message resets liveness to three and resets the reconnect interval; silence decrements it; at zero the queue is dead, whereupon the worker sleeps
the reconnect interval, doubles it up to 32 seconds, destroys and recreates its socket, and resets liveness. The queue keeps a per-worker expiry instead of one `heartbeat_at`. *Guarantee:* workers and clients reconnect across
a queue restart; the queue evicts lost workers instead of discovering them through a failed request; the client never receives an out-of-order reply. *Cost:* explicit envelope management; the five-hour heartbeat debugging; a
failure-simulation harness "dangerous to reuse". *Failure modes:* false failures if heartbeats are not sent properly; PPP is not interoperable with Simple Pirate; and the worker must actively recreate its socket, because
"automatic ZeroMQ reconnect alone does not re-register it at a restarted broker". The contract lesson: "Lack of contracts is a sure sign of a disposable application."

**Majordomo (MDP)** - service-oriented reliable queuing [34][13]. *Problem:* Paranoid Pirate routes work but has no named service, and no application should have to implement the protocol details itself. *Mechanism:* MDP adds
a service name to client requests and per-service worker registration, giving the broker one request queue and one worker queue per service. It splits into MDP/Client (`REQUEST`, `PARTIAL`, `FINAL`) and MDP/Worker (`READY`,
`REQUEST`, `PARTIAL`, `FINAL`, `HEARTBEAT`, `DISCONNECT`), each command carrying a six-byte protocol header so reconnects can be validated. The broker MUST use ROUTER and MAY use one socket for both sub-protocols; clients
MUST use DEALER. Stated goals: route by abstract service name; let both peers detect disconnection by heartbeating; "Allow the broker to implement a 'least recently used' pattern for task distribution to workers for a given
service"; "Allow the broker to recover from dead or disconnected workers by resending requests to other workers." Assumptions: "Workers are idempotent", each handles at most one request at a time and issues exactly one reply
per successful request. Heartbeat rules: any received command except `DISCONNECT` acts as a heartbeat; both sides beat at agreed intervals; a peer is disconnected if none arrives "within some multiple of that interval
(usually 3-5)". On `DISCONNECT` the worker MUST close its socket and reconnect on a new one - "This mechanism allows workers to re-register after a broker failure and recovery." *Guarantee:* named services reach registered
workers, and because broker state is essentially service presence, "Majordomo supports live-live broker failover indirectly, by keeping no significant state in a broker. Actual failover to alternate brokers is handled by the
clients and workers, not the protocol." *Cost:* a framework and API are needed for sane use; the example broker is nearly 500 lines and took two days to make somewhat robust; the APIs are single-threaded, deliberately without
background heartbeats, so a stuck worker stops beating and the broker stops giving it work; throughput is "tens of thousands, not millions, of request-reply transactions per second due to round-trip costs and the extra
latency of a broker-based approach". *Failure modes:* MDP's own "Known Weaknesses" - "The heartbeat rate must be set to similar values in broker and worker, or false disconnections will occur. A better heartbeat design will
be developed later", plus a performance cost from per-frame command encoding. The guide adds: the reference API does no exponential backoff and asserts on unexpected messages; empty service queues are never deleted; and MDP
"does not implement any authentication, access control, or encryption mechanisms and should not be used in any deployment where these are required". *Asynchronous Majordomo* splits client send and receive and uses DEALER with
a hand-built envelope: 173,010 calls/sec versus 9,057 synchronous, and 100K requests in 8.730 s (one worker) or 3.863 s (ten workers) versus 14.088 s. The cost is that the client loses automatic retries and "it cannot survive
a broker crash without more work" - proper reconnect needs request numbering, client-side retention of all outstanding requests, and resend to the failover broker [34]. *MMI* layers service discovery on top without changing
MDP: names beginning `mmi.` are handled internally, and `mmi.service` answers "200" if workers are registered and "404" otherwise; its weakness is that a vanished worker can leave a service looking present [34].

**Titanic** - disk-based, disconnected reliable queuing [34]. *Problem:* Pirate requires the client to wait for a real-time answer; sporadically connected clients and workers need state in the middle. *Mechanism:* not a
broker change but a specialized MDP worker that is also a client, writing requests to disk "to ensure they never get lost, no matter how sporadically clients and workers are connected". Three services: `titanic.request`
persists a request and returns a UUID; `titanic.reply` fetches the reply for a UUID; `titanic.close` confirms it is stored or processed. The client keeps its UUIDs. Titanic consults `mmi.service` before dispatching and
retries indefinitely. The example uses three threads, one file per message, a separate request queue, and `inproc` notification so the dispatcher need not rescan the directory. *Guarantee:* worker crash mid-processing, lost
reply, a client that missed a reply, and Titanic's own crash are all covered by retry or re-ask. "As long as requests are fully committed to safe storage, work can't get lost." Operationally: "You can stop and restart any
piece *except the client* and nothing will get lost." *Cost:* an extra network hop plus disk; a heavier client protocol that should be hidden behind an API; performance "surely terrible"; and components that "need
management/repair". Faster variants: one large file, a circular buffer, an in-memory rebuilt index, `fsync` per message or every N milliseconds "with accepted last-M-message loss", SSD, preallocation. The guide warns against
a database or key-value store unless performance is unimportant, because the abstraction may cost "ten to a thousand times" versus raw disk. *Failure modes:* disk-backed reliability is restricted to "an asynchronous
disconnected network", since most Pirate cases can avoid the disk when workers are stateless and idempotent; the sample MMI discovery is poor, so dispatching to an apparently running service "won't work all the time"; the
client must not be stopped; and durability is stated as a condition, not a promise.

**Binary Star** - primary/backup failover pair [34]. *Problem:* catastrophic failure of a server, its hardware, or a network segment. *Mechanism:* two servers, active and passive; the passive does no work and monitors the
active, taking over only after the active has been absent for a configured time **and** clients ask it to connect. A finite state machine drives it on three events: "Peer Active", "Client Request", and "Client Vote" (a client
request while the peer has been silent for two heartbeats). State is exchanged over PUB-SUB only, because "PUSH/DEALER block if peer is not ready; PAIR does not reconnect after peer disappearance/return; ROUTER needs peer
address before send". The reusable form is the Binary Star reactor wrapping CZMQ's `zloop`, with voter registration and active/passive handlers, run by `bstar_start` until a callback returns -1 or a signal arrives.
*Guarantee:* at most one active server; automatic protection against catastrophic disappearance; "failover reliably when needed, and only when needed"; target failover under 60 seconds, preferably under 10. The three-way
rule: "server will not become active until it receives application connection requests and cannot see peer." *Cost:* a fully redundant backup that is usually idle but must carry the full load; clients must know both
addresses, detect failure, retry primary then backup with a delay of at least the failover timeout, recreate server state, and retransmit anything lost - usually behind a client API. Recovery is manual by design, and each
failover or recovery may interrupt service 10-30 seconds; the example timeout is 2000 ms and must exceed any wrapper-script restart time. *Preconditions:* exactly one backup; equally capable peers; no load balancing between
them; explicit manual configuration known to the applications; matching failover response values; a dedicated high-speed peering route. *Explicit non-goals:* not for active backup or load balancing; no persistent messages or
transactions; no automatic discovery; no state or message replication - "applications recreate all server-side state after failover"; a server cannot belong to more than one pair; configuration cannot change at runtime.
*Failure modes:* automatic recovery is undesirable because it creates a second outage and ambiguity; shutdown order matters, stopping the passive first or both within seconds; too short a timeout activates the backup when the
primary would have restarted. The split-brain warning verbatim: "We must not split a Binary Star architecture into two islands, each with a set of applications. While this may be a common type of network architecture, you
should use federation, not high-availability failover, in such cases." The dangerous topology is a pair split across two buildings with applications in both and one link between; losing the link gives two client groups and
two active servers. The mitigation is a dedicated peer link on the same switch or a crossover cable, better still two private interconnects on separate NICs.

**Freelance (FLP)** - brokerless reliable request-reply [34][15]. *Problem:* multi-server reliability with no central broker, demonstrated as name resolution, which must stay available because an unavailable name service
blocks the whole application network. FLP's goals: an N-to-N peer network, "To operate without an intermediary broker or devices", multi-threaded servers, and server failover and recovery. *Mechanism, three models.* Model
One, "Simple Retry and Failover": Lazy Pirate over several endpoints - one server means retry, several means try each once. Model Two, "Brutal Shotgun Massacre": a DEALER connected to every server, each request blasted once
per server, first reply taken, requests numbered so stale replies are ignored, and an empty frame prepended to build a valid REP envelope. Model Three, "Complex and Nasty": ROUTER-to-ROUTER with the server's identity fixed to
its public endpoint, which FLP formalizes ("Server identities are their public endpoints… e.g. 'tcp://192.168.55.162:5055'"; "Clients use transient sockets, and MUST not set an identity. Servers use durable sockets and MUST
set an identity"), plus ping-pong liveness ("Clients MAY send ping commands at regular intervals. A client SHOULD consider a server 'disconnected' if no 'pong' arrives within some multiple of that interval (usually 2-3)"),
plus a background agent acting as a mini-broker and a tickless poll timer. *Guarantee:* a live server in the pool answers; Model Two fails over in "about 60 microseconds to one server and 80 microseconds to three" on the test
box; Model Three can target servers known to be alive. *Cost:* Model One is "Poor at scale: if primary is down and many client sockets connect, each incurs painful timeout"; Model Two duplicates all traffic, cannot prioritize
primary over secondary, and "the server can do at most one request at a time, period"; Model Three "becomes large, nearly Majordomo broker complexity". *Failure modes:* the ROUTER-to-ROUTER bootstrap paradox -
post-`zmq_connect` availability is nondeterministic and neither anonymous ROUTER can speak first, so the fixed endpoint identity is explicitly "a cheat", and reversing it so the server knows arbitrary clients "would be
insane, on top of complex and nasty". The chapter's conclusion: the two patterns that stand out for production use are Majordomo (brokered) and Freelance (brokerless) [34].

**Chapter 5's pub-sub recipes** [35]. *Espresso (pub-sub tracing).* Problem: no visibility into a pub-sub network. Mechanism: `zmq_proxy()`'s third capture socket, fed to a listener thread over an `inproc` PAIR pair; the
trace shows subscription frames (`0141`, `0142`), the data, and the unsubscriptions (`0041`, `0042`). Guarantee: all bridged traffic, control and data, is observable. Cost: a proxy in the path. *Last Value Caching.* Problem:
a new subscriber to a rarely updated topic waits for the next update - with 1000 topics and one update per second, "a new subscriber waits 500 seconds on average for data". Mechanism: an application-programmable proxy where a
PGM switch would sit - XSUB upstream, XPUB downstream, latest message cached per topic, republished on an XPUB subscription notification. Guarantee: immediate catch-up to the cached last value per subscribed topic, not the
historical stream. Cost: an intermediary, per-topic storage, and a TCP-scale topology rather than switch-level multicast. Failure mode: XPUB does not report duplicate subscriptions by default, so a production LVC proxy needs
`ZMQ_XPUB_VERBOSE`; libzmq also offers DRAFT `ZMQ_XPUB_MANUAL_LAST_VALUE`, which "changes the XPUB socket behaviour to send the first message to the last subscriber… This prevents duplicated messages when using last value
caching(LVC)" [18][35]. *Suicidal Snail (slow subscriber detection).* Problem: a permanently slow subscriber, where queuing at the publisher risks its memory, queuing at the subscriber only defers the problem, HWM drops
create gaps, and disconnecting the subscriber is impossible because "ZeroMQ publisher applications cannot see individual subscribers". Mechanism: the subscriber detects its own lateness and exits. Sequence numbers detect gaps
but break with multiple publishers (needs a publisher id) and with SUB filters (filtered streams have legitimate gaps); timestamps are the general solution - compare receive time against the message timestamp and croak past
the threshold. Guarantee: an assertion about maximum latency. Rationale verbatim: "Aborting a subscriber may not seem like a constructive way to guarantee a maximum latency, but it's the assertion model. Abort today, and the
problem will be fixed. Allow late data to flow downstream, and the problem may cause wider damage and take longer to appear on the radar." Failure mode: it sacrifices the slow subscriber's availability and repairs nothing.
*Black Box (high-speed subscribers).* Problem: market-data-scale streams where "even slight per-message work makes a subscriber unable to catch up", and publisher and subscriber top out near 6M messages/second after tuning.
Mechanism: separate I/O from work (a SUB thread pushing to PULL workers over `inproc`), then shard - two I/O threads, two NICs each bound to one, two subscriber threads pinned to cores, two SUB sockets with partitioned
subscription keys, remaining cores for workers connected to both PUSH sockets. Guarantee and limit: "Ideally, we want to match the number of fully-loaded threads in our architecture with the number of cores"; beyond that
"There would be no benefit, for example, in creating more I/O threads." Failure mode: it cannot beat CPU oversubscription or a single thread's ceiling. *Clone / CHP (reliable pub-sub state replication).* Six models. One:
state as key-value pairs published as `kvmsg` (key, 64-bit network-order sequence, body), correct only if all clients start before the server and never crash. Two: out-of-band snapshot - subscribe first, request state on a
DEALER, do not read the SUB socket while waiting (ZeroMQ queues the updates), receive the snapshot, resume reading, discard updates at or below the snapshot sequence, apply the rest. Three: clients generate updates and send
them to the server, which becomes a stateless broker but remains the single ordering point. Four: subtrees, a path hierarchy requested identically in the snapshot request and the subscription. Five: ephemeral values with a
`ttl` property, chosen over sessions on the principle of "not invent concepts that are not absolutely essential", plus a `zloop` reactor with one handler per concern. Six: Binary Star underneath, client updates fanned to both
servers over PUB/SUB, heartbeats so clients detect a dead primary, snapshot requests doubling as Binary Star votes, a client-generated UUID on every update, and a passive-side pending list ordered oldest to newest. Guarantee:
continuity while at least one server runs. Assumptions: "At least one server will keep running. If both servers crash, we lose all server state and there's no way to recover it", and clients do not update the same key
simultaneously. The protocol is 12/CHP: the server binds ROUTER at P, PUB at P+1, SUB at P+2; the client connects DEALER to P, SUB to P+1, optionally PUB to P+2; `ICANHAZ?` plus subtree is answered by zero or more `KVSYNC`
and then `KTHXBAI` carrying the highest sequence; `KVPUB` carries updates under the strict-increment discard rule; `HUGZ` beats about once a second when idle; `KVSET` comes from clients, with an empty value meaning delete and
a `ttl` property meaning expire. CHP "does not specify the mechanisms used for this failover but the Binary Star pattern from the Guide may be helpful" [14][35].

## 10. Security and identity

**Mechanisms.** Exactly one per socket, announced in the greeting: "Security in ZMTP is *assertive* in that all peers on a given socket have the same, required level of security. This prevents downgrade attacks and simplifies
implementations." [1] Defined: NULL (no authentication, no confidentiality; "SHOULD NOT be used on public infrastructure without transport-level security (e.g. over a VPN)"), PLAIN, CURVE. Private mechanisms are allowed;
public ones "SHALL be defined as 0MQ RFCs", names of uppercase letters, digits, hyphens and underscores, assigned first-come first-served [1]. libzmq also ships GSSAPI options [18].

**PLAIN.** Username and password in clear text, for "multiple services on the same network (for instance, development servers and production servers)" and "minimal authentication of clients (to avoid errors in
configuration)". "Previously, applications have tried to do such authentication using socket identities. These do not work for all socket types, nor do they map cleanly to user identities." It is "not robust against even the
simplest traffic snooping or spoofing attacks" [8]. libzmq: `ZMQ_PLAIN_SERVER` (default 0; setting 0 "shall reset the socket security to NULL"), `ZMQ_PLAIN_USERNAME`, `ZMQ_PLAIN_PASSWORD` [18].

**CURVE.** Curve25519, 32-octet keys, 24-octet nonces, boxes 16 octets larger than their plaintext; "These sizes are not configurable; they are enforced by the underlying cryptography library and act as universal constants
for CurveZMQ implementations." [10] Four keys per connection: permanent C and S, transient C' and S'. Flow: `HELLO` (200 octets, carrying C' and anti-amplification padding so HELLO exceeds WELCOME) → `WELCOME` (168 octets,
carrying S' and a cookie encrypting C' and s' under a short-lived cookie key the server "MUST discard… after a short interval, for example 60 seconds, or as soon as the client sends a valid INITIATE") → `INITIATE` (257+
octets, carrying C, a vouch box and metadata) → `READY` (30+ octets, server metadata) → `MESSAGE` either way. The server keeps no state for an unauthenticated client: "It's generated a keypair, sent that back to the client in
a way only the client can read, and thrown it away." Claimed properties: perfect forward secrecy ("Session keys are held in memory and destroyed when the connection is closed") and client-identity protection ("the client
permanent public key is not sent in clear-text"). Named defences: eavesdropping, fraudulent data, altered data, replay, amplification, man-in-the-middle, key theft, client identification, certain denial-of-service. Deviations
from CurveCP include the vouch box being `Box[C',S](C->S')` rather than `Box[C'](C->S)` "to reduce the risk of client impersonation", a version number in HELLO, a READY command, and unbounded message payloads. Stated
limitations: "CurveCP makes no attempt to defend against traffic analysis attacks"; "CurveCP does not explain how keys are exchanged"; "CurveCP enforces a 1-to-many relationship from servers to clients"; and a dependency on
NaCl/libsodium [10]. The three security models named: "Where the server does not check client keys at all… Where all clients share the same public key, that the server checks… Where each client has its own key, that the
server checks. In this case the server can grant access to clients according to their authenticated identity." [10] libzmq: `ZMQ_CURVE_SERVER` (a server "does not need to know its own public key"), `ZMQ_CURVE_PUBLICKEY`,
`ZMQ_CURVE_SECRETKEY`, `ZMQ_CURVE_SERVERKEY`, keys as 32 binary bytes or 40-character Z85 plus null, "all, when using TCP transport" [18].

26/CURVEZMQ contradicts itself on the HELLO padding field: the ABNF says `hello-padding = 72%x00`, while the prose says "An anti-amplification padding
field. This SHALL be 70 octets, all zero." The stated 200-octet HELLO total (6 + 2 + padding + 32 + 8 + 80) only works with 72, so the grammar is the
correct reading and the prose is an error [10] [inference].

**Authorization: ZAP.** The decision is delegated to a handler over an in-process request-reply dialog. "ZAP uses an *inprocess bridge* design. That is, ZAP itself requires that the handler run as a thread within the same
process as the servers. When an application uses multiple processes, each process will contain a ZAP handler." Rules: the server uses REQ or DEALER, the handler REP or ROUTER, the handler binds `inproc://zeromq.zap.01`, one
handler per process, any number of servers, and "The handler SHALL start before any server starts." Request frames: delimiter, version "1.0", request id, non-empty domain, address (client IP as IPv4 dotted or IPv6 canonical
string), identity (the `Identity` metadata property, at most 255 bytes), mechanism, credentials. Reply frames: delimiter, version, echoed request id, status code, status text, user id, metadata. Status codes: "200" success,
"300" temporary error, "400" authentication failure, "500" internal error. "user id: this MAY provide the user identity in case of a 200 status, for use by applications. For other statuses, it SHALL be empty." Credentials per
mechanism: NULL has none and "provides no security credentials but allows a server to filter bogus clients on the basis of IP address"; PLAIN has username and password frames; CURVE has "a 32-byte long-term public key of the
peer being authenticated". A handler MAY proxy to external handlers over TCP, and a terminal handler behind unknown intermediaries "MUST accept an address envelope consisting of N routing ID frames followed by an empty
address delimiter frame… and MUST send the same envelope back with the reply. This is what a REP socket does". ZAP defines no discovery: "ZAP does not define how to discover handlers on the network." [11] libzmq: "A ZAP
domain must be specified to enable authentication. When the ZAP domain is empty, which is the default, ZAP authentication is disabled" - and this "is not compatible with previous versions of libzmq", governed by
`ZMQ_ZAP_ENFORCE_DOMAIN`, "for now… disabled by default, but in a future version it will be enabled by default" [18].

**Authorization granularity.** Per connection, at handshake time. The domain string is the only scoping handle, and its meaning is left open: "The significance of domains are an application issue and not relevant to ZAP."
[11] No per-message, per-topic, per-service or per-operation authorization exists anywhere in the specifications [inference].

**Does identity travel with a message?** Not as an authenticated attribute. The ZMTP `Identity` property is per connection, meaningful only for REQ/DEALER/ROUTER peers connecting to a ROUTER ("For all other socket types, the
Identity property shall be ignored"), 0-255 octets whose first octet must not be zero, and chosen by the peer [1]. ROUTER exposes it as a prepended frame that the application may carry along, which is exactly what MDP's
"Client address (envelope stack)" frame and CHP's client identity prefix do [2][13][14]. A ZAP handler's `user id` goes to the *server*, not onto messages [11]. So the wire carries an unauthenticated per-connection label and
an authenticated per-connection key, and nothing per message [inference].

**Transport security.** CURVE is the in-protocol answer, documented "when using TCP transport" [18]. `wss://` is TLS around ZWS, requiring GnuTLS, with `ZMQ_WSS_KEY_PEM`, `ZMQ_WSS_CERT_PEM`, `ZMQ_WSS_TRUST_PEM`,
`ZMQ_WSS_HOSTNAME`, `ZMQ_WSS_TRUST_SYSTEM` [36]. ZWS itself "makes no attempt at encryption of the wire" and offers only ZWS2.0 (no mechanism), ZWS2.0/NULL, ZWS2.0/PLAIN and an unimplemented ZWS2.0/BEARER; "Load balancer
might terminate the SSL before arriving on the server." [12] Deprecated transport filters remain: `ZMQ_TCP_ACCEPT_FILTER` (CIDR allow-list) and `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID`, both carrying "This option is deprecated,
please use authentication via the ZAP API" [18]. `ZMQ_SOCKS_PROXY` with optional basic authentication covers outbound TCP [18].

**Security considerations 37/ZMTP raises.** Guard against downgrade attacks including crafted ZMTP 1.0/2.0 headers; log and possibly block repeated failed connections; "allocate memory to a connection only after the security
handshake is complete, and MAY limit the number and cost of in-progress handshakes"; limit in-progress handshakes per originating IP against amplification; send metadata after the handshake; pad encrypted messages "to a
randomized minimum size" against size analysis; and send noise when idle against presence analysis [1].

## 11. Limits and resource bounds

| Limit | Where | Default | Notes |
| --- | --- | --- | --- |
| `ZMQ_SNDHWM` / `ZMQ_RCVHWM` | per socket, per peer, messages | 1000 each | 0 means no limit; actual limit "may be as much as 90% lower"; TCP buffers can make it far larger [18][24] |
| `ZMQ_MAXMSGSIZE` | per socket, inbound bytes | -1 (no limit) | Exceeding it disconnects the peer [18] |
| `ZMQ_MAX_MSGSZ` | per context | `INT_MAX` | Also the maximum [19] |
| `ZMQ_MAX_SOCKETS` | per context | 1023 | Ceiling via `ZMQ_SOCKET_LIMIT`; `zmq_socket()` gives `EMFILE` at the limit [17][19] |
| `ZMQ_IO_THREADS` | per context | 1 | May be 0 for `inproc`-only; guide's rule of thumb one per Gb/s [19][32] |
| `ZMQ_LINGER` | per socket, ms | -1 (infinite) | 0 discards immediately [18] |
| `ZMQ_BACKLOG` | per socket, connections | 100 | Connection-oriented transports only [18] |
| `ZMQ_HANDSHAKE_IVL` | per socket, ms | 30000 | 0 means no limit; not for `ZMQ_STREAM` [18] |
| `ZMQ_RECONNECT_IVL` / `_IVL_MAX` | per socket, ms | 100 / 0 | -1 disables reconnect; 0 max means no backoff [18] |
| `ZMQ_HEARTBEAT_IVL` / `_TIMEOUT` / `_TTL` | per socket, ms | 0 / 0 or IVL / 0 | TTL max 6553599 ms, decisecond granularity [18] |
| `ZMQ_SNDTIMEO` / `ZMQ_RCVTIMEO` | per socket, ms | -1 (block) | [18] |
| `ZMQ_CONNECT_TIMEOUT` | per socket, ms | 0 (OS default) | [18] |
| `ZMQ_TCP_MAXRT`, `ZMQ_TCP_KEEPALIVE*` | per socket | OS defaults | [18] |
| `ZMQ_SNDBUF` / `ZMQ_RCVBUF` | per socket, kernel bytes | 0 (OS default) | Bounded by host limits [18][27] |
| `ZMQ_RATE`, `ZMQ_RECOVERY_IVL`, `ZMQ_MULTICAST_HOPS`, `ZMQ_MULTICAST_MAXTPDU` | multicast | rate-limited by default | [18][27] |
| Identity length | ROUTER routing id | n/a | 1-255 bytes, first octet must not be zero [1][18] |
| Group length | RADIO/DISH | n/a | RFC: 0-255 bytes. libzmq: "limited to 16 chars length (including null)" [7][17] |
| Subscription length | SUB/XSUB | n/a | Unbounded in the grammar (`subscription = *OCTET`) [1] |
| `inproc` name length | endpoint | n/a | "may be up to 256 characters" [26] |
| `ipc` path length | endpoint | n/a | "On Linux, the maximum is 113 characters including the 'ipc://' prefix" [25] |
| Frame body | wire | n/a | Short 0-255, long 0 to 2^63-1 octets [1] |
| Metadata value | handshake | n/a | 0 to 2^31-1 octets [1] |
| CURVE commands per connection | wire | n/a | "SHALL NOT send more than 2^64-1 commands in one connection" [10] |

**What an unbounded resource looks like in practice.** Four shapes. ZeroMQ 2.x, where "the HWM was infinite by default. This was easy but also typically fatal for high-volume publishers" [32] - the 4.x default of 1000 is the
fix, and with it "publisher crashes are rarer unless the HWM is deliberately made infinite" [35]. An application-side hash table keyed by ROUTER identity, which "leak[s] per-peer resources as disconnect/reconnect occurs and
get[s] slower" without heartbeats [34]. `ZMQ_CONFLATE` on an inbound socket that is never read: "the queue and memory will grow with each message received" [18]. And the default infinite `ZMQ_LINGER`, which bounds time rather
than memory: termination waits forever [18][31]. Multipart is not an escape either - all frames are held until the last is sent, and the guide says to split large payloads into separate single-part messages [21][32].

**What a hostile peer can force.** 37/ZMTP's own list with its mitigations: repeated connection attempts (log and block per IP); holding connections open to exhaust memory (allocate only after the handshake, cap in-progress
handshakes); amplification via spoofed source addresses (cap in-progress handshakes per IP; in CURVE, pad HELLO larger than WELCOME) [1][10]. Beyond that, a peer can declare a frame of up to 2^63-1 octets, against which
`ZMQ_MAXMSGSIZE` is the only defence [18]; can open subscriptions, which are additive and non-idempotent, so N repeated `SUBSCRIBE` commands cost N entries [1]; and can claim an in-use ROUTER identity, which by default is
rejected but with `ZMQ_ROUTER_HANDOVER` evicts the incumbent [18]. On `ipc`, a local process can steal a bound endpoint [25].

## 12. Answers to the problem catalogue

**P1 - loss detection and safe retry.** Not in the protocol: one ZMTP hop delivers atomically, at most once, in order, with no acknowledgement [1]. Detection and retry are the application's, and the guide's recipes are Lazy
Pirate (poll, resend on timeout, abandon after N attempts, close and reopen the REQ socket after `EFSM`), Simple/Paranoid Pirate (retry through a broker that reassigns to another worker), MDP ("Allow the broker to recover
from dead or disconnected workers by resending requests to other workers"), Titanic (persist first, retry indefinitely), and Freelance Model Two (blast to every server, take the first reply) [34][13]. Duplicates are the
accepted cost - MDP simply assumes "Workers are idempotent". Where they cannot be, the suppression recipe is a client id plus message number, with the server storing the reply under that key and resending it on a repeat
[34][13]. At the socket level, `ZMQ_REQ_CORRELATE` plus `ZMQ_REQ_RELAXED` stop a late reply to an aborted request from being taken as the reply to its successor [18]. 37/ZMTP considered and declined automatic requeueing
because it "results in out-of-order messages, and is not robust against messages lost in transit, or already delivered to the other peer" [1].

**P2 - liveness, in what time, and what happens to peer state.** Two layers. ZMTP 3.1: `PING` with a TTL in tenths of a second, answered by `PONG`, any incoming traffic counting as a sign of life, and the connection dead
after a timeout "usually be a small multiple of the PING interval" [1]; libzmq's `ZMQ_HEARTBEAT_IVL`/`_TIMEOUT`/`_TTL`, off by default [18]. Application layer: the guide's three heartbeat designs, with Paranoid Pirate's
three-strike liveness at one-second intervals and configured intervals from "as low as 10 msecs" to "as high as 30 seconds" [34]. Detection time is therefore whatever the application configures; TCP alone can take "roughly 30
minutes" [34]. Peer state: no session and no last will. On disconnect the socket "SHALL destroy its double queue and SHALL discard any messages it contains" [2], the ROUTER identity vanishes unless the peer set an explicit
one, and XPUB "SHALL, if the subscriber peer disconnects prematurely, generate a suitable unsubscribe request for the calling application" [3]. The closest thing to a last will is DRAFT `ZMQ_DISCONNECT_MSG` on
ROUTER/SERVER/PEER and `ZMQ_HICCUP_MSG` on DEALER/CLIENT/PEER, which synthesize a locally configured message when a peer drops, plus DRAFT `ZMQ_ROUTER_NOTIFY` for zero-length connect/disconnect notifications [18]. In-flight
messages are lost, and whether the request was processed is exactly the ambiguity chapter 4 flags [34].

**P3 - spreading work by capacity, not by turn.** The socket-level answer is not by capacity: PUSH, DEALER, CLIENT, REQ and SCATTER round-robin, but only over peers whose outgoing queue is not full, which gives crude capacity
awareness ("SHALL consider a peer as available only when it has a outgoing queue that is not full") [2][4][6]. The real answer is the load-balancing pattern: workers send an explicit `ready` message "when they start and after
they finish each task", and the broker keeps them in least-recently-used order and hands the next task to the identity at the head [33]. The unit of credit is one task per ready signal. MDP formalizes it - "Allow the broker
to implement a 'least recently used' pattern for task distribution to workers for a given service", with the broker free to use "any basis, including round robin and least-recently used" [13]. The justification for the extra
machinery: round robin "becomes inefficient when tasks do not take approximately the same time" [33]. A worker's initial `ready` can carry capability data so the broker picks the fastest rather than the oldest [33].

**P4 - slower consumer: where the backlog sits, its bound, and behaviour at the bound.** The backlog sits in per-peer pipes on both sides plus the kernel socket buffers; the bound is `ZMQ_SNDHWM`/`ZMQ_RCVHWM`, 1000 messages
per peer by default and inexact both ways [18][24][32]. At the bound the socket enters the mute state and blocks or drops per §4.1: PUB, XPUB, XSUB, RADIO and ROUTER drop; PUSH, PULL, REQ, DEALER, PAIR, CLIENT, SCATTER and
CHANNEL block; SERVER, PEER and STREAM return `EAGAIN` [17]; a receiving SUB or XSUB silently discards [3]. `ZMQ_XPUB_NODROP` and `ZMQ_ROUTER_MANDATORY` convert drops into errors [18]. Chapter 5 enumerates the four possible
policies - queue at the publisher (risks its memory), queue at the subscriber (ZeroMQ's default, good for peaky overload), stop queuing (HWM, creates gaps), disconnect the slow subscriber (impossible, since a PUB application
cannot see individual subscribers) - and then adds the fifth that ZeroMQ makes possible: Suicidal Snail, where the subscriber measures its own lateness and exits [35]. No credit ever returns to the producer, so a publisher
has "one setting, which is *full-speed*" [35].

**P5 - late joiner needing current state.** Nothing in the protocol; the pattern loses those messages by design. "the subscriber will always miss the first messages that the publisher sends" [31], and "The ZeroMQ pub-sub
pattern will lose messages arbitrarily when a subscriber is connecting, when a network failure occurs, or just if the subscriber or network can't keep up with the publisher" [35]. Three application answers: (a) synchronized
startup, where subscribers announce readiness on a second REQ/REP flow and the publisher waits for the expected count [32]; (b) Last Value Caching, an XSUB/XPUB proxy caching the last message per topic and republishing on
subscription notification, needing `ZMQ_XPUB_VERBOSE` in production, with DRAFT `ZMQ_XPUB_MANUAL_LAST_VALUE` and `ZMQ_XPUB_WELCOME_MSG` supporting the same shape [18][35]; (c) snapshot then stream, CHP's `ICANHAZ?` on a
DEALER/ROUTER pair answered by `KVSYNC` records and a `KTHXBAI` carrying the highest sequence, with the client subscribing *first*, queuing updates while it waits, and discarding those at or below the snapshot sequence
[14][35]. Replay of a gap is explicitly refused: holes are caused by network stress and asking for more messages makes it worse, so "All the client can do is warn its users that it is 'unable to continue', stop, and not
restart until someone has manually checked the cause of the problem." [35]

**P6 - failover to another broker or peer.** No protocol support; there is no session to resume. Automatic reconnection to the *same* endpoint is built in [1][18], but failover to a different endpoint is application logic,
and chapter 4 stresses that reconnection is not re-registration: a worker "must close/reopen after broker death to re-register; ZeroMQ auto-reconnect is insufficient" [34]. What the recipes re-establish: Lazy Pirate nothing,
and it does not fail over at all; Freelance Model One tries each endpoint once, Model Two blasts all of them, Model Three pings to find live ones [34][15]; MDP relies on the broker holding no significant state, so "Actual
failover to alternate brokers is handled by the clients and workers, not the protocol" [13]; Binary Star gives a primary/backup pair whose clients "must know both addresses, detect failure… retry primary then backup with
delay at least failover timeout, recreate server state, and retransmit failover-lost messages if required", and whose explicit non-goal is state replication [34]; Clone Model Six re-requests a full snapshot from the backup
after heartbeat loss, the snapshot request doubling as the Binary Star vote [35]. Subscriptions themselves are re-established automatically on reconnect, since XSUB "SHALL send subscribe and unsubscribe requests to
publishers" [3] [inference], and `ZMQ_XPUB_WELCOME_MSG` is "also sent on reconnecting" [18].

**P7 - surviving a restart.** ZeroMQ persists nothing: every queue is in memory and is discarded when its peer disconnects [2]. The only durable recipe is Titanic, which writes requests to disk "to ensure they never get lost,
no matter how sporadically clients and workers are connected", assigns a UUID the client is expected to keep safely, and exposes `titanic.close` as the client's confirmation that the reply may be wiped [34]. Who decides: the
client, by choosing to send through Titanic rather than directly. What certifies it: the UUID from `titanic.request` for acceptance, and `titanic.close` for completion. The guarantee is stated as a condition, not a promise -
"As long as requests are fully committed to safe storage, work can't get lost", and operationally "You can stop and restart any piece *except the client* and nothing will get lost" [34]. Faster variants trade it away
explicitly: `fsync` "every N milliseconds with accepted last-M-message loss", or memory-only requests where a Titanic crash loses them [34]. CHP persists nothing either, and Clone Model Six's assumption is "At least one
server will keep running. If both servers crash, we lose all server state and there's no way to recover it." [35]

**P8 - ordering guarantee and its scope.** "All messages between two immediate peers SHALL be delivered in order." [1] Scope: one connection, one hop. Not across peers, since fair-queuing interleaves them arbitrarily; not
across hops; not across sockets; not across a failover. Within a message, frames are ordered and the message is atomic [1][21]. Applications needing a wider scope funnel through a single ordering point and number the result:
CHP's server "centralizes every change and imposes one sequence in arrival order", with "The sequence number MUST be strictly incremental. The client MUST discard any KVPUB commands whose sequence numbers are not strictly
greater than the last KTHXBAI or KVPUB command received." [14][35] ZRE requires strictly incrementing per-peer sequence numbers and disconnects a peer on a gap [16].

**P9 - duplicates.** The wire does not duplicate: "A message SHALL NOT be delivered more than once to any peer" [1], and CurveZMQ additionally disconnects a peer that reuses a short nonce, blocking wire replay [10].
Duplicates come from application retries and deliberate fan-out: Lazy Pirate's resend, a broker's reassignment, Titanic's indefinite retry, and Freelance Model Two, where "A server may receive duplicate request" by design
[34]. Suppression is application-level only: the client-id plus message-number reply cache [34], or `ZMQ_REQ_CORRELATE` to reject stale replies at the socket [18]. "Exactly once" is not offered and not claimed anywhere in the
specifications or the guide; the nearest statements are MDP's assumption that "Workers are idempotent, i.e. it is safe to execute the same request more than once" [13] and chapter 4's flat "Idempotency is not something you
take a pill for." [34] Note also that subscriptions are deliberately non-idempotent at the ZMTP level [1].

**P10 - reply correlation and routing back through intermediaries.** By envelope, not by id. "The ZeroMQ reply envelope formally consists of zero or more reply addresses, followed by an empty frame (the envelope delimiter),
followed by the message body (zero or more frames)." REQ prepends the delimiter; each ROUTER hop prepends the identity of the connection a message arrived on and strips the first frame of a message it sends; REP saves the
whole envelope through the delimiter and replays it with the reply [2][33]. REQ correlation is positional - "each reply received is matched with the last issued request" [17] - unless `ZMQ_REQ_CORRELATE` adds an explicit
request-id frame [18]. For the thread-safe family it is an integer: SERVER and PEER expose a 32-bit routing id per message via `zmq_msg_routing_id`/`zmq_msg_set_routing_id`, and an unknown one gives `EHOSTUNREACH` [6][17];
note that a SERVER's reply "will go to the first client thread that calls `zmq_msg_recv`", so per-thread correlation needs one CLIENT socket per thread [17]. Application protocols carry the envelope in their own frames: MDP's
`REQUEST`/`PARTIAL`/`FINAL` each contain a "Client address (envelope stack)" frame followed by an empty delimiter [13]; ZAP messages "contain the request-reply address envelope so that requests can be sent along multiple
intermediaries, and replies will return to the right server" [11]; FLP returns the Client Control Frame unmodified [15]. The asymmetry worth naming: ROUTER learns an identity only after the peer speaks, so "an application can
really reply, but cannot spontaneously talk to a peer" - the gap that `ZMQ_PROBE_ROUTER`, `ZMQ_CONNECT_ROUTING_ID`, `ZMQ_HELLO_MSG` and `zmq_connect_peer()` exist to close [17][18][33].

**P11 - topology.** "ZMTP is by default a peer-to-peer protocol that makes no distinction between the clients and servers", and it "allows this but also the opposite topology in which the client binds, and the server
connects" [1]. "All sockets SHALL accept connections (binding to an address) and make connections" [1], and at the API a socket may bind several endpoints and connect several more at once, except PAIR and CHANNEL [17]. So
both sides can be either. The guide still prescribes a bias: "binder is a server at a well-known address; connector is a client with arbitrary/unknown address", and "server/static topology component binds fixed endpoints;
client/dynamic component connects. The chances that it will 'just work' are much better like that" [32]; chapter 3's valid/invalid table follows the same bias [33]. Brokerless versus brokered: "ZeroMQ doesn't come with a
message broker as such, but it lets us build intermediaries quite easily", with a preference for "simple stateless message switches over complex/stateful central brokers" [32]. `zmq_proxy()` gives the three canonical
intermediaries - shared queue (ROUTER/DEALER), forwarder (XSUB/XPUB), streamer (PULL/PUSH) - in one call [22]. Discovery: none in ZMTP. The RFCs answer it three ways: static configuration (Freelance, Binary Star, CHP's
P/P+1/P+2 convention) [14][15][34]; a name service that is itself a Freelance pool [34]; and UDP beacons in 36/ZRE, which listens and broadcasts on IANA-assigned UDP port 5670 with a 22-octet beacon of `ZRE` + version +
16-octet UUID + mailbox port, where a zero port means the peer is leaving [16]. ZAP explicitly declines: "ZAP does not define how to discover handlers on the network." [11] 37/ZMTP's `Resource` property is the port-sharing
counterpart, letting several services live behind one interface and port [1].

**P12 - flow-control credit.** Unit: messages. Grantor: nobody - there is no credit exchange in ZMTP 3.1. Credits appear only under "Topics for Discussion", sketched as "the receiving peer (DEALER, PULL, REP) would send
credit which would be used up by messages routed to it… Credit could be octets, or messages" [1]. What exists instead is a local per-peer high-water mark, 1000 messages each way by default, whose exhaustion blocks, drops or
returns `EAGAIN` by socket type [17][18]. Kernel buffers sit underneath and make the bound both larger and imprecise: "simply don't rely on the exact HWM value" [24]. The nanomsg comparison names this as a design complaint:
"Both the outgoing and incoming data is stored in a message queue **and** in TCP's tx/rx buffers… Given that there's no semantic difference between the two, nanomsg uses only TCP's (or equivalent's) buffers" [37].

**P13 - large messages and streaming bodies.** Maximum frame body is 2^63-1 octets by grammar [1]; in libzmq the practical ceiling is `ZMQ_MAX_MSGSZ`, defaulting to and capped at `INT_MAX` [19], with `ZMQ_MAXMSGSIZE` as the
per-socket inbound limit whose breach disconnects the sender [18]. A body cannot be delivered before it is complete: delivery is atomic ("all frames or none"; "On sending, the peer SHALL queue all frames of a message in
memory until the final frame is sent"), the first part goes on the wire only when the last is sent, receiving the first part means all parts arrived, and only closing the socket cancels a partially sent message [1][21][32].
Multipart is not streaming and does not reduce memory: "single or multipart data must fit in memory; multipart does not lower memory consumption. Split arbitrarily large files into separate single-part messages." [32]
Zero-copy exists on send (`zmq_msg_init_data()` with a free callback) but "There is no zero-copy receive", and it pays off only for large frequent blocks [32]; the nanomsg note calls it "zero-copy till the message gets to the
kernel boundary" [37]. The thread-safe socket family cannot use multipart at all [1]. PGM is the one place a message spans transport frames: "a single 0MQ message may span several PGM datagrams", each datagram carrying a
16-bit offset to the first message boundary, or `0xFFFF` for a pure continuation, so late joiners can resynchronize [27].

**P14 - identity.** Authentication is per connection at handshake time, by one of NULL, PLAIN or CURVE, with exactly one mechanism per socket and no negotiation [1][8][10]. The authorization decision may be delegated to a ZAP
handler receiving the mechanism, domain, client IP address, connection `Identity` property and mechanism credentials, and answering 200/300/400/500 plus a user id and metadata [11]. An authenticated identity is *not* visible
per message: the CURVE permanent public key and the ZAP user id are per-connection facts held by the server, and the only per-message label is the ROUTER/SERVER routing id, a local connection handle which - where the peer
chose it via the `Identity` property - is self-asserted and unauthenticated [1][2][11]. Authorization granularity is per connection; the ZAP domain is the only scoping string and its meaning is left to the application [11].
No per-topic, per-service or per-operation authorization exists in any specification [inference]. libzmq's older per-transport filters (`ZMQ_TCP_ACCEPT_FILTER`, `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID`) are documented as deprecated
in favour of ZAP [18].

**P15 - what a hostile or buggy peer can make you allocate.** 37/ZMTP names three attacks with mitigations: connection storms (log and block per IP); memory exhaustion by holding connections open (allocate memory only after
the handshake, cap the number and cost of in-progress handshakes); amplification with a spoofed source address (cap in-progress handshakes per IP; CURVE pads HELLO larger than WELCOME so the server cannot be used as an
amplifier) [1][10]. Concrete limits available: `ZMQ_MAXMSGSIZE` (otherwise a frame may declare up to 2^63-1 octets), `ZMQ_SNDHWM`/`ZMQ_RCVHWM`, `ZMQ_MAX_SOCKETS`, `ZMQ_BACKLOG`, `ZMQ_HANDSHAKE_IVL`, `ZMQ_HEARTBEAT_*`,
`ZMQ_CONNECT_TIMEOUT`, `ZMQ_TCP_MAXRT`, and the deprecated address filters [18][19]. Residual exposures: subscriptions are additive and non-idempotent, so repeated `SUBSCRIBE` commands accumulate state [1];
`ZMQ_ROUTER_HANDOVER` lets a peer claiming an in-use identity evict the incumbent [18]; on `ipc` a local process can steal a bound endpoint [25]; `ZMQ_CONFLATE` on an unread inbound socket grows without bound [18]; and the
application's own per-peer bookkeeping is unbounded unless it expires peers, which is why chapter 4 warns that heartbeat-less ROUTER applications "leak per-peer resources… and get slower" [34].

**P16 - observability.** No confirms, receipts, counters or tracing headers in the protocol [1]. What exists is connection-level and local. `zmq_socket_monitor()` delivers two-frame events on an `inproc` PAIR socket -
`CONNECTED`, `CONNECT_DELAYED`, `CONNECT_RETRIED` (value = the recalculated reconnect interval), `LISTENING`, `BIND_FAILED`, `ACCEPTED`, `ACCEPT_FAILED`, `CLOSED`, `CLOSE_FAILED`, `DISCONNECTED`, `MONITOR_STOPPED`,
`HANDSHAKE_SUCCEEDED`, `HANDSHAKE_FAILED_NO_DETAIL`, `HANDSHAKE_FAILED_PROTOCOL` (with a `ZMQ_PROTOCOL_ERROR_*` code covering both ZMTP and ZAP faults) and `HANDSHAKE_FAILED_AUTH` (with the ZAP status code) - for TCP, IPC and
TIPC only [23]. Its warning: "as new events are added, the catch-all value will start returning them. An application that relies on a strict and fixed sequence of events must not use ZMQ_EVENT_ALL" [23]. `zmq_getsockopt`
exposes `ZMQ_EVENTS`, `ZMQ_FD` and `ZMQ_LAST_ENDPOINT`; the nanomsg comparison flags the `ZMQ_FD` semantics as a known usability problem - "the descriptor is edge-triggered… nanomsg uses level-triggered file descriptors
instead" [37]. For message-level visibility the answer is Espresso: `zmq_proxy()`'s capture socket, which "shall send all messages, received on both frontend and backend, to the capture socket", including subscription control
frames [22][35]. Everything else is application convention: sequence numbers for gaps, timestamps for latency (Suicidal Snail), `HUGZ` as an idle beat, MDP heartbeats, and the guide's advice to "Use simple console tracing;
dump messages and incrementally number them to reveal gaps" [14][34][35].

**P17 - shutdown.** Governed by `ZMQ_LINGER`, per socket. Default -1: pending messages are not discarded and `zmq_ctx_term()` blocks until all have been sent. 0: pending messages are discarded immediately on
`zmq_disconnect()` or `zmq_close()`. Positive: a millisecond bound, after which pending messages are discarded [18]. `zmq_ctx_term()` first interrupts every blocking call with `ETERM` and makes every subsequent call except
`zmq_close()` fail with `ETERM`, then blocks until all sockets are closed and every sent message is "physically transferred to a network peer, or the socket's linger period… has expired" [20]. `ZMQ_BLOCKY` false gives every
new socket a zero linger, presented as the easy way to get the effect of the usual handshake-then-terminate discipline [19]. The guide's rules: close sockets then destroy the context, in the owning thread; "if you leave any
sockets open, the `zmq_ctx_destroy()` function will hang forever"; for a socket with outstanding requests set a low linger such as one second before closing; do not destroy the same context twice [31]. There is no drain
acknowledgement from the peer - linger certifies transfer to the network, not receipt by the application [inference]. A partially sent multipart message is cancelled only by closing the socket [32]. ZMTP itself has no close
handshake: either peer may close at any moment [1].

**P18 - transports.** ZMTP is specified over "a connected transport layer such as TCP" [1]. libzmq 4.3.x implements: `tcp` ("an ubiquitous, reliable, unicast transport… will likely be your first choice"), with wildcard
interface and port, optional source endpoint, and HWM interacting with kernel buffers [24]; `ipc`, UNIX domain sockets only, where a later bind may steal the endpoint, `@` selects the Linux abstract namespace, and the path is
limited to about 107 characters [25]; `inproc`, which "passes messages via memory directly between threads sharing a single 0MQ context", involves no I/O threads, allows names up to 256 characters, and since 4.0 no longer
requires bind before connect [26]; `pgm` and `epgm` for reliable multicast, PUB and SUB only, rate limited by default, with `pgm` needing raw-socket privileges and a 16-bit message-boundary offset per datagram [27]; `udp`,
RADIO and DISH only, DISH binding and RADIO connecting, unicast or multicast, IPv4 or IPv6 [28]; `vmci` between VMware guests and host [29]; `tipc`, "a cluster IPC protocol with a location transparent addressing scheme" with
`{type,lower,upper}` port names where overlapping ranges round-robin incoming connections [30]; `vsock` [24]; and DRAFT `ws`/`wss` per 45/ZWS [12][36]. What changes between them: multipart framing is identical, but PGM breaks
message-to-datagram alignment and needs the offset field [27]; UDP restricts you to RADIO/DISH [28]; `inproc` shares buffers, so "real HWM is the sum of both sides' configured HWMs" [32]; `zmq_socket_monitor()` works only on
TCP, IPC and TIPC [23]; CURVE, PLAIN, the ZAP domain and the CURVE key options are documented "when using TCP transport" [18]; ZWS replaces the greeting with a WebSocket `Sec-WebSocket-Protocol` negotiation, maps each ZeroMQ
frame to one binary WebSocket message with a flag byte (0x00 final, 0x01 more, 0x02 command), and implements XPUB/XSUB as API constructs only, "at the protocol level as PUB and SUB sockets" [12]; and PAIR/CHANNEL are
effectively `inproc`-only because they do not auto-reconnect [17]. Filtering placement is transport-dependent: v3.x filters at the publisher for connected `tcp`/`ipc`, at the subscriber for `epgm` [31].

## 13. Ecosystem

**Reference core.** libzmq, C++, "implements ZMTP/3.1" [36]. 4.3.5 (2023/10/09) completed relicensing from LGPL-3.0+ with exceptions to MPL-2.0, collecting grants from all relevant authors and clean-room reimplementing what
could not be relicensed, and tagged sources with SPDX identifiers; it also added `ZMQ_BUSY_POLL` and `ZMQ_HICCUP_MSG` [36]. "WebSockets support is disabled by default if DRAFT APIs are disabled" [36], which is the practical
reason `ws`/`wss` and the whole thread-safe socket family are missing from many distribution builds [inference].

**Bindings and higher-level stacks named by the primary sources.** CZMQ, whose `zloop` reactor the guide uses for the Clone server, the Binary Star reactor and the Freelance agent [34][35]. The RFC-side reference
implementations: `majordomo` for MDP/0.2 [13], `libcurve` for CurveZMQ [10], `spec_27.c` for ZAP [11], and the chapter 5 C99 Clone examples, which "act as the prime reference implementation for CHP" [14]. cppzmq and zmqpp
(C++), PyZMQ (Python), zeromq.js (Node) and zwssock (CZMQ WebSockets) appear in the project's own documentation index [36].

**Rust crates.**
- `zmq` (rust-zmq, `erickt/rust-zmq`, Apache-2.0/MIT): "The `zmq` crate provides bindings for the `libzmq` library… The API exposed by `zmq` should be safe (in the usual Rust sense), but it follows the C API closely, so it is
  not very idiomatic." "The aim of this project is to track latest zmq releases as close as possible", CI-tested on current stable Rust. It ships compile-fail tests including `socket-thread-unsafe.rs`, enforcing libzmq's
  thread-safety rule at the type level, plus a large `examples/zguide/` tree [39].
- `zeromq` (zmq.rs, `zeromq/zmq.rs`, MIT): "A native Rust implementation of ZeroMQ", with "DISCLAIMER: This codebase does not implement all of ZeroMQ's feature set." Status: "Basic ZMTP implementation is working and tested
  against the reference implementation." Transports: TCP and IPC (unix only). Patterns: REQ, REP, DEALER, ROUTER, PUB, SUB, XPUB, XSUB, PUSH, PULL. Runtime selectable between `tokio` (default), `async-std` and
  `async-dispatcher` [38].

**Known incompatibilities** (only those the sources state).
- Feature coverage: zmq.rs implements neither PAIR nor the thread-safe draft family, and no transport beyond TCP and IPC [38]; a peer using CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER/CHANNEL or `ws`/`wss` therefore needs
  libzmq or another complete implementation [inference].
- Draft status: CLIENT/SERVER, RADIO/DISH, PEER, CHANNEL, `ws`/`wss`, `ZMQ_RECONNECT_STOP`, `ZMQ_ROUTER_NOTIFY`, `ZMQ_XPUB_MANUAL_LAST_VALUE` and `ZMQ_ZERO_COPY_RECV` are all marked "in DRAFT state, not yet available in
  stable releases" or "still in draft phase" [17][18][19].
- ZMTP version skew: 1.0 and 2.0 peers can only request NULL security, and a peer configured for anything else "MUST immediately close the connection" [1]. ZRE has none at all: "There is no mechanism for backwards
  interoperability." [16]
- Application-protocol skew: "MDP/0.2 is not compatible with MDP/0.1", differing by the replacement of `REPLY` with `PARTIAL`/`FINAL` and the removal of leading empty frames [13]. Paranoid Pirate "is not interoperable with
  Simple Pirate because PPP has heartbeats" [34].
- ZAP domain semantics: libzmq's default of an empty domain disabling authentication "is not compatible with previous versions of libzmq" and contradicts RFC 27's requirement that a domain always be set;
  `ZMQ_ZAP_ENFORCE_DOMAIN` toggles it and is currently off [11][18].
- Group length: 0-255 bytes per 48/RADIO-DISH versus 16 characters including the null per `zmq_socket(3)` [7][17].
- ZWS: "ZWS implementations don't have to implement the all sockets type and can choose which socket type to implement", and XPUB/XSUB do not exist on the wire [12].

**ZeroMQ's own stated weaknesses.** The most candid catalogue is the nanomsg comparison, written by the original author of both libraries and carrying the caveat "Much has changed since this document was written, both in
nanomsg and ZeroMQ." Its claims about ZeroMQ: the threading model is "One of the big architectural blunders I've done in ZeroMQ… Each individual object is managed exclusively by a single thread… it becomes a trouble for
objects managed by user threads", with the named consequences "inability to implement request resending in REQ/REP protocol, PUB/SUB subscriptions not being applied while application is doing other work"; "REQ socket in
ZeroMQ cannot be really used in real-world environments, as they get stuck if message is lost due to service failure or similar. Users have to use XREQ instead and implement the request re-trying themselves";
bind-then-connect and auto-reconnect did not work for `inproc` (since fixed in libzmq 4.0 [26][32]); "the way in which ZeroMQ sockets failed randomly in such circumstances proved to be painful and hard to debug"; "the
incomprehensible shutdown mechanism as seen in ZeroMQ"; no formal API for plugging in new transports or protocols, so "there were no new transports added since 2008"; synchronous DNS, where "when DNS was unavailable, the
whole library, including the sockets that haven't used DNS, just hung"; edge-triggered `ZMQ_FD`; the double-buffering of HWM plus kernel buffers; zero-copy that is only "zero-copy till the message gets to the kernel
boundary"; simple tries for subscription matching, "intended for up to 10,000 subscriptions" against users with 150,000,000; and BSD sockets rather than IOCP on Windows [37]. The guide's own admissions are quieter: pub-sub
"will lose messages arbitrarily" [35]; ROUTER's silent drops make "debugging hard" [33]; "Heartbeating is difficult" [34]; and MDP's known-weaknesses list [13].

**Notable deployments.** None of the primary sources this sheet is built on names a deployment. `zmq_socket(3)`'s PEER section cites "zyre, bitcoin, torrent" as examples of the peer-to-peer style it targets [16][17].

## 14. Sources

All sources were read on 2026-09-08, in full or in the cited ranges.

1. **37/ZMTP - ZeroMQ Message Transport Protocol.** https://rfc.zeromq.org/spec/37/ - ZMTP 3.1, status draft, copyright 2009-2015 iMatix, editor Pieter Hintjens. Greeting and version negotiation, framing, commands, NULL
   mechanism, metadata and the Socket-Type/Identity/Resource properties, socket semantics rules, subscription and JOIN/LEAVE commands, thread-safe family, PING/PONG, backwards interoperability, security considerations, and
   the open "Topics for Discussion" on credits and requeueing.
2. **28/REQREP - ZeroMQ Request-Reply.** https://rfc.zeromq.org/spec/28/ - stable, 2013. REQ/REP/DEALER/ROUTER normative behaviour, double queues, envelope format, drop/block/error rules.
3. **29/PUBSUB - ZeroMQ Publish-Subscribe.** https://rfc.zeromq.org/spec/29/ - stable, 2013-2014. PUB/XPUB/SUB/XSUB behaviour, publisher-side prefix matching, drop-on-full rules, XPUB normalization and premature-disconnect
   unsubscribe.
4. **30/PIPELINE - ZeroMQ Pipeline.** https://rfc.zeromq.org/spec/30/ - stable, 2013. PUSH/PULL behaviour and the "mostly reliable" framing.
5. **31/EXPAIR - ZeroMQ Exclusive Pair.** https://rfc.zeromq.org/spec/31/ - stable, 2013. PAIR behaviour and intended scope.
6. **41/CLIENTSERVER - ZeroMQ Client-Server.** https://rfc.zeromq.org/spec/41/ - draft, 2015, editors Pieter Hintjens and Doron Somech. CLIENT/SERVER behaviour, 32-bit routing id, multipart prohibition, the deprecation intent
   toward request-reply.
7. **48/RADIO-DISH - ZeroMQ Radio-Dish.** https://rfc.zeromq.org/spec/48/ - draft, 2020, editor Doron Somech. RADIO/DISH behaviour, exact group matching, group length.
8. **24/ZMTP-PLAIN - ZMTP PLAIN.** https://rfc.zeromq.org/spec/24/ - stable, 2013. The PLAIN handshake grammar, its purpose, its stated limitations.
9. **25/ZMTP-CURVE - ZMTP CURVE.** https://rfc.zeromq.org/spec/25/ - stable, 2013. The CURVE mechanism's use of `as-server` and its delegation to 26/CURVEZMQ.
10. **26/CURVEZMQ - CurveZMQ.** https://rfc.zeromq.org/spec/26/ - CurveZMQ 1.0, stable, 2013. HELLO/WELCOME/INITIATE/READY/MESSAGE/ERROR, key and nonce sizes, cookie handling, the three security models, nonce replay
    protection, differences from CurveCP, the specific-defences and known-issues lists.
11. **27/ZAP - ZeroMQ Authentication Protocol.** https://rfc.zeromq.org/spec/27/ - ZAP 1.0, stable, 2013. The inproc bridge design, socket and endpoint rules, request and reply frames, status codes, per-mechanism credentials,
    proxy handlers, absence of discovery.
12. **45/ZWS - ZeroMQ WebSocket Protocol 2.0.** https://rfc.zeromq.org/spec/45/ - draft, 2020, editor Doron Somech. WebSocket subprotocol negotiation, ZWS framing flags, socket compatibility, XPUB/XSUB as API-only constructs,
    the security note.
13. **18/MDP - Majordomo Protocol.** https://rfc.zeromq.org/spec/18/ - MDP 0.2, draft, 2012. Goals, topology, ROUTER addressing, command layouts, connection open/close rules, heartbeating, the reliability failure list,
    scalability figures, the security disclaimer, "Known Weaknesses".
14. **12/CHP - Clustered Hashmap Protocol.** https://rfc.zeromq.org/spec/12/ - stable, 2011. Three-port architecture, ICANHAZ/KVSYNC/KTHXBAI/KVPUB/HUGZ/KVSET, the strict-increment discard rule, TTL semantics, the Binary Star
    pointer, the security disclaimer.
15. **10/FLP - Freelance Protocol.** https://rfc.zeromq.org/spec/10/ - stable, 2011. Goals, PING/PONG and request commands, the Client Control Frame, transient versus durable sockets, endpoint-as-identity, the
    server-reliability heartbeat multiple.
16. **36/ZRE - ZeroMQ Realtime Exchange Protocol.** https://rfc.zeromq.org/spec/36/ - ZRE 2, stable, 2009-2014. UDP beacon discovery on port 5670, the ROUTER/DEALER interconnection model, strict sequence numbering and
    disconnect-on-gap, PING/PING-OK, absence of backwards interoperability.
17. **zmq_socket(3).** https://libzmq.readthedocs.io/en/latest/zmq_socket.html - libzmq master, page last updated 2026-07-26 12:58:15 UTC. The per-socket-type tables in §4.1, thread-safety list, mute-state behaviour,
    ROUTER_MANDATORY prose, STREAM semantics, group length, SERVER/PEER routing-id errors, the PAIR/CHANNEL TCP caveat.
18. **zmq_setsockopt(3).** https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html - libzmq master, last updated 2026-07-26. Every option default and semantic cited in §1, §5, §8, §10, §11 and §12: SNDHWM/RCVHWM, LINGER,
    IMMEDIATE, CONFLATE, HEARTBEAT_IVL/TIMEOUT/TTL, RECONNECT_IVL/_MAX/_STOP, ROUTER_MANDATORY/HANDOVER, XPUB_VERBOSE/VERBOSER/MANUAL/MANUAL_LAST_VALUE/WELCOME_MSG/NODROP, PROBE_ROUTER, CONNECT_ROUTING_ID,
    REQ_CORRELATE/RELAXED, MAXMSGSIZE, HANDSHAKE_IVL, INVERT_MATCHING, CURVE_*, PLAIN_*, ZAP_DOMAIN/ZAP_ENFORCE_DOMAIN, HELLO_MSG/DISCONNECT_MSG/HICCUP_MSG, ROUTER_NOTIFY, the deprecated TCP/IPC filters.
19. **zmq_ctx_set(3).** https://libzmq.readthedocs.io/en/latest/zmq_ctx_set.html - libzmq master, last updated 2026-07-26. IO_THREADS, MAX_SOCKETS, MAX_MSGSZ, BLOCKY, ZERO_COPY_RECV, thread scheduling options.
20. **zmq_ctx_term(3).** https://libzmq.readthedocs.io/en/latest/zmq_ctx_term.html - libzmq master, last updated 2026-07-26. The two-step termination procedure and its interaction with LINGER.
21. **zmq_send(3).** https://libzmq.readthedocs.io/en/latest/zmq_send.html - libzmq master, last updated 2026-07-26. DONTWAIT and SNDMORE semantics, the queued-not-transmitted note, atomic multipart delivery, the error list
    including EFSM, EHOSTUNREACH, EINVAL, ETERM.
22. **zmq_proxy(3).** https://libzmq.readthedocs.io/en/latest/zmq_proxy.html - libzmq master, last updated 2026-07-26. The capture socket and the shared queue, forwarder and streamer configurations.
23. **zmq_socket_monitor(3).** https://libzmq.readthedocs.io/en/latest/zmq_socket_monitor.html - libzmq master, last updated 2026-07-26. Event list, two-frame event encoding, protocol-error codes, transport restriction, the
    ZMQ_EVENT_ALL warning.
24. **zmq_tcp(7).** https://libzmq.readthedocs.io/en/latest/zmq_tcp.html - libzmq master, last updated 2026-07-26. TCP addressing, wildcard binds, source endpoints, the HWM-versus-kernel-buffer discussion, the `vsock`
    cross-reference.
25. **zmq_ipc(7).** https://libzmq.readthedocs.io/en/latest/zmq_ipc.html - libzmq master, last updated 2026-07-26. UNIX-domain-only availability, bind stealing, the Linux abstract namespace, the path length limit.
26. **zmq_inproc(7).** https://libzmq.readthedocs.io/en/latest/zmq_inproc.html - libzmq master, last updated 2026-07-26. No I/O threads, the 256-character name limit, the 4.0 change making bind/connect order irrelevant.
27. **zmq_pgm(7).** https://libzmq.readthedocs.io/en/latest/zmq_pgm.html - libzmq master, last updated 2026-07-26. pgm versus epgm, the PUB/SUB restriction, default rate limiting, the datagram offset field, tuning
    recommendations.
28. **zmq_udp(7).** https://libzmq.readthedocs.io/en/latest/zmq_udp.html - libzmq master, last updated 2026-07-26. The RADIO/DISH restriction and the bind/connect asymmetry.
29. **zmq_vmci(7).** https://libzmq.readthedocs.io/en/latest/zmq_vmci.html - libzmq master, last updated 2026-07-26. The VMCI transport's scope and addressing.
30. **zmq_tipc(7).** https://libzmq.readthedocs.io/en/latest/zmq_tipc.html - libzmq master, last updated 2026-07-26. TIPC port-name ranges and round-robin distribution of connection requests.
31. **ZeroMQ - The Guide, Chapter 1: Basics.** https://zguide.zeromq.org/docs/chapter1/ - read 2026-09-08. The "sockets on steroids" framing, REQ/REP lockstep, pub-sub basics, the slow joiner explanation and arithmetic, the
    pipeline example, context lifecycle, the clean-exit and LINGER hazards, the brokerless framing, the string-handling rules, transport-dependent filter placement.
32. **ZeroMQ - The Guide, Chapter 2: Sockets and Patterns.** https://zguide.zeromq.org/docs/chapter2/ - read 2026-09-08. Bind-versus-connect guidance, the transport list and I/O thread rule of thumb, multipart rules,
    intermediaries and `zmq_proxy()`, zero-copy, pub-sub envelopes, the high-water-mark section including the v2.x-versus-v3.x defaults and the block-or-drop statement, and the nine-point "Missing Message Problem Solver".
33. **ZeroMQ - The Guide, Chapter 3: Advanced Request-Reply Patterns.** https://zguide.zeromq.org/docs/chapter3/ - read 2026-09-08. The reply envelope definition and walk-throughs, ROUTER identities and their history, the
    silent-drop statement, the socket recap, the legal and invalid combination tables, the load-balancing broker, the asynchronous client/server pattern, the six-socket inter-broker peering design.
34. **ZeroMQ - The Guide, Chapter 4: Reliable Request-Reply Patterns.** https://zguide.zeromq.org/docs/chapter4/ - read 2026-09-08. The reliability definition and failure list, per-pattern reliability analysis,
    Lazy/Simple/Paranoid Pirate, the three heartbeating designs and the implementation advice, contracts, Majordomo and Asynchronous Majordomo and MMI, idempotency and duplicate detection, Titanic, Binary Star including the
    split-brain warning and non-goals, Freelance models one to three.
35. **ZeroMQ - The Guide, Chapter 5: Advanced Pub-Sub Patterns.** https://zguide.zeromq.org/docs/chapter5/ - read 2026-09-08. The pub-sub pros and cons list and the loss statement, Espresso, Last Value Caching, Suicidal
    Snail, Black Box, Clone models one to six and the CHP summary, the refusal to replay gaps.
36. **libzmq 4.3.5 release notes and repository description.** https://github.com/zeromq/libzmq/releases/tag/v4.3.5 - released 2023/10/09; read 2026-09-08. The version and date, the MPL-2.0 relicensing, ZMQ_BUSY_POLL and
    ZMQ_HICCUP_MSG, the WS/WSS DRAFT status and GnuTLS requirement, the ZMQ_WSS_* option names, the "implements ZMTP/3.1" description, and the documentation index naming cppzmq, zmqpp, PyZMQ, zeromq.js and zwssock.
37. **Differences between nanomsg and ZeroMQ.** https://nanomsg.org/documentation-zeromq.html - last updated 2018-02-07, by the original author of both libraries, carrying its own "Much has changed since this document was
    written" caveat. Used only for §13's catalogue of ZeroMQ's stated weaknesses and for the double-buffering, zero-copy, edge-triggered-FD and subscription-trie remarks.
38. **zmq.rs README.** https://github.com/zeromq/zmq.rs - `zeromq` crate, MIT, read 2026-09-08. The native-implementation disclaimer, supported transport and socket lists, async runtime feature flags.
39. **rust-zmq README.** https://github.com/erickt/rust-zmq - `zmq` crate, Apache-2.0/MIT, read 2026-09-08. The binding's stated scope and non-idiomatic API, the version-tracking aim, the thread-safety compile-fail tests and
    zguide examples.

Not read, therefore not cited for content: 23/ZMTP (ZMTP 3.0), 13/ZMTP (2.0), 15/ZMTP (1.0), 49/SCATTERGATHER, 51/P2P, 52/CHANNEL, 6/PPP, 7/MDP 0.1, 8/MMI, 20/ZRE-DISC. Their existence and titles come from 37/ZMTP's
related-specifications list and chapter 4's references [1][34].

Two further internal inconsistencies in the sources, recorded here rather than in the body. 37/ZMTP cites the client-server pattern as
`rfc.zeromq.org/spec:41/CLIENTSERVER` in its related-specifications list but as `rfc.zeromq.org/spec:47/CLIENTSERVER` in the Client-Server socket-semantics
section; only spec 41 exists [1]. And `zmq_ws(7)`/`zmq_wss(7)` man pages are absent from the readthedocs set and from the libzmq `doc/` tree at master, so
the only normative description of the WebSocket transport is 45/ZWS plus the release notes [12][36].
