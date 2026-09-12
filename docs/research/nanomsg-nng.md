# nanomsg / NNG Scalability Protocols (SP v1; NNG 1.10.0)

## 0. Identity card

- **Name.** The nanomsg Scalability Protocols (SP) are protocol-pattern specifications; NNG is a C implementation and successor-compatible implementation of them. [1][2]
- **Versions researched.** SP protocol IDs and the listed pattern RFCs are revision 01; the implementation reference is NNG 1.10.0 (latest manual at research date 2026-09-08). [1][3]
- **Governing body.** There is no standards body or IETF RFC; the nanomsg project publishes the RFC repository. [3]
- **Reference implementations.** libnanomsg implements the original API; NNG is wire-compatible with SP and supplies a nanomsg-1.0 compatibility API. [2][4]
- **Wire type.** SP is binary, message-oriented framing; WebSocket SP traffic uses binary frames. [2][14]
- **Defined mappings/transports.** SP RFCs define TCP, IPC, TLS, UDP, and WebSocket mappings; NNG 1.10.0 exposes inproc, IPC, TCP, TLS-over-TCP, WebSocket/WSS, and experimental ZeroTier transports. [3][2]

## 1. Connection and session lifecycle

- A socket owns zero or more listener and dialer endpoints; endpoints create pipes, which are message-oriented connected streams and commonly map 1:1 to TCP or IPC connections. [2]
- Either role may listen, dial, or do both; endpoint direction does not prescribe request/reply or other application role. [2][7]
- `nng_dial()` is synchronous by default: a refused first connection is returned immediately and no retry is started. `NNG_FLAG_NONBLOCK` makes that first attempt asynchronous, after which failures retry periodically. [7]
- Observed: the synchronous `nng_dial()` returns only after the peer's 8-octet protocol header has arrived, not when the TCP connection is established — the call blocks through the greeting exchange. A program that calls it from the same thread that must answer a greeting therefore deadlocks until the socket timeout fires. [30]
- After a dialer pipe closes, its dialer attempts reconnection; retry delay begins at `NNG_OPT_RECONNMINT` and grows exponentially to `NNG_OPT_RECONNMAXT` when the latter is nonzero. [7][6]
- The SP TCP mapping opens one full-duplex TCP connection and begins with an 8-octet greeting carrying SP version and protocol identifiers; incompatible peers must disconnect. [3]
- TCP keepalive is a transport option, not a common SP heartbeat; ZeroTier alone documents ping-time/ping-tries liveness probing. [12][15]
- TLS performs the TLS 1.2 handshake over TCP and can validate the server name from the dial URL; WSS combines HTTP/WebSocket negotiation with TLS when enabled. [13][14]
- A pipe is removed when its peer, owning dialer/listener, or `nng_pipe_close()` closes it. `NNG_PIPE_EV_REM_POST` occurs after removal and communication over that pipe is then impossible. [8][9]
- No SP session-resumption, durable session, last-will, or cross-restart state is specified. [3]

## 2. Primitives

- **Socket.** A protocol-specific socket implements exactly one SP protocol, has endpoint sets, and can send and/or receive only as that protocol permits. [2]
- **Dialer/listener.** A dialer initiates a connection to a URL; a listener accepts it. Both are associated with one socket and create pipes. [7][8]
- **Pipe.** An opaque, value-passed handle for one connection, associated with exactly one creating dialer or listener and therefore one socket. [8]
- **Message.** An `nng_msg` has separate application body and protocol header storage; application data is opaque to most protocols. [2][5]
- **Context.** `nng_ctx` shares its socket, endpoints, and pipes but owns stateful protocol state such as a request ID and retry timer, enabling independent concurrent transactions. [10]
- Context support is protocol-specific; raw sockets do not support contexts because protocol state is intentionally absent. [10]
- Pipe callbacks expose add-before, add-after, and remove-after events. The callback runs under the socket lock and must not access the socket. [9]

## 3. Message model

- NNG delivers a message wholly or not at all; it does not expose partial-message delivery or streaming bodies. [2]
- Generic NNG does not promise delivery or ordering; a protocol may add retry/matching semantics. [2]
- `NNG_OPT_RECVMAXSZ` limits an accepted remote message by bytes; zero disables the limit, and an excess message is discarded. [6]
- `RECVMAXSZ` should be set before endpoint creation, ideally per listener/dialer; future transports may negotiate it during connection setup. [6]
- `inproc` accepts but deliberately ignores `RECVMAXSZ`, because peers share an address space. [16]
- PUB/SUB uses the initial bytes of the body as a topic; they are neither a separate wire field nor typed metadata. [21]
- REQ/REP headers are a stack of big-endian 32-bit IDs: final request ID has MSB set, preceding forwarder peer IDs have MSB clear. [5]
- SURVEY headers use the equivalent stack with a final survey ID (MSB set) and preceding peer IDs. [22]
- PAIR v1 has one 32-bit header whose low-order byte is a hop count, "initialized to one and incremented at each node" [18]. **Settled by observation: the RFC's prose is what a running node does.** A reading of `src/sp/protocol/pair1/pair.c` suggested the counter starts at `0`, and it was recorded here as a disagreement; a cooked PAIR v1 send by NNG 1.4.0-rc.0 puts `00 00 00 01` on the wire, so the count originates at one and each receiving node increments. A raw send whose count is already `0xff` is still rejected as malformed. A decoder should accept both readings on receipt, because the difference is a count and not a format. [18][30][31]
- BUS cooked messages have no protocol header; a raw BUS receive carries the incoming pipe ID in its sole header element. [17]

### Wire layouts, octet by octet

The RFCs state the greeting, the framing and the header stacks in prose; the octets below are
those layouts written out, taken from the RFCs and confirmed against what NNG 1.4.0-rc.0 puts
on a TCP connection. [3][30]

**Protocol header**, 8 octets, sent by both sides immediately after the TCP handshake and
awaited by both before anything else:

```
0x00 0x53 0x50 | version | type-hi type-lo | 0x00 0x00
     "SP"          0x00      16-bit type      reserved
```

A peer whose first three octets, version or reserved field differ is disconnected, and the
disconnection is the whole error report: there is no reply, no reason code and no round trip
in the mapping at all. [3][30]

**Endpoint type ids.** The 16-bit type field is a 12-bit protocol ID plus a 4-bit endpoint
role. The RFCs assign the protocol IDs and leave the role nibble to the per-protocol RFCs,
which never published it; the values below are NNG's registry
(`NNI_PROTO(major, minor) = major * 16 + minor`, `src/core/protocol.h`) and are what NNG and
mangos put on the wire. [30]

| Endpoint | Protocol, role | Type | Full header |
| --- | --- | --- | --- |
| PAIR v0 | 1, 0 | `0x0010` | `00 53 50 00 00 10 00 00` |
| PAIR v1 | 1, 1 | `0x0011` | `00 53 50 00 00 11 00 00` |
| PUB | 2, 0 | `0x0020` | `00 53 50 00 00 20 00 00` |
| SUB | 2, 1 | `0x0021` | `00 53 50 00 00 21 00 00` |
| REQ | 3, 0 | `0x0030` | `00 53 50 00 00 30 00 00` |
| REP | 3, 1 | `0x0031` | `00 53 50 00 00 31 00 00` |
| PUSH | 5, 0 | `0x0050` | `00 53 50 00 00 50 00 00` |
| PULL | 5, 1 | `0x0051` | `00 53 50 00 00 51 00 00` |
| SURVEYOR | 6, 2 | `0x0062` | `00 53 50 00 00 62 00 00` |
| RESPONDENT | 6, 3 | `0x0063` | `00 53 50 00 00 63 00 00` |
| BUS | 7, 0 | `0x0070` | `00 53 50 00 00 70 00 00` |

Surveyor and respondent are roles 2 and 3 rather than 0 and 1 because the first two are
retired. [30]

**Message framing.** A message is a big-endian 64-bit octet count followed by exactly that
many octets of body, with nothing else on the wire: no type tag, no flags, no continuation
bit and therefore no multipart form. An empty message is the eight zero octets and nothing
after them. The count is declared before any of the body arrives, which is what makes
`RECVMAXSZ` (§11) checkable from the length field alone. [3][30]

**REQ/REP and survey tag stacks** are big-endian 32-bit words carried before the body: zero or
more forwarder peer IDs with the most significant bit clear, then the request or survey ID
with that bit set. A stack with no terminating word is malformed. A reply repeats the stack it
arrived with; each forwarder pops its own word to choose the reverse pipe, so the originator
sees only its own ID. [5][22][30]

**PAIR v1** prefixes the body with one big-endian 32-bit word whose low-order octet is the hop
count described above. PAIR v0 has no header at all. [18][30]

## 4. Patterns and topologies

- **BUS v0.** Each node sends to every *directly connected* peer; a mesh must therefore be fully connected for all nodes to see a publication. Send is best-effort, nonblocking, and discards when a peer cannot receive; delivery may reach some, all, or none. [17]
- BUS raw mode excludes the incoming pipe ID when re-broadcasting, preventing immediate reflection in a one-socket device; it is not a network-wide loop detector. [17]
- **PAIR v0.** A one-to-one peer relationship; it normally blocks when no peer can receive. v0 has no protocol header and is the interoperable legacy choice. [18]
- **PAIR v1.** Also one-to-one by default; it adds hop-count loop protection across devices. `NNG_OPT_MAXTTL` is 1–255, commonly default 8 where supported; a forwarder checks its own limit. [18][6]
- **PAIR v1 polyamorous.** `nng_pair1_open_poly()` permits multiple direct peers. The sender chooses a pipe with `nng_msg_set_pipe()` (often from received `nng_msg_get_pipe()`); without one it selects any available peer. A directed unavailable pipe discards silently to avoid head-of-line blocking, and cannot route through devices. This deprecated mode should not be chosen for new designs. [18]
- **PUB/SUB v0.** PUB broadcasts every message to every connected SUB; each SUB filters locally by prefix subscription, so subscriptions do not reduce link bandwidth. PUB cannot receive and SUB cannot send. [20][21]
- With an empty subscription a SUB accepts all messages. A subscriber queue full condition drops the oldest message by default (`SUB_PREFNEW=true`) or rejects the new one when false. [21]
- Observed: the consequence of receiver-side filtering is visible on the link rather than in the API. A SUB socket that has subscribed to nothing is still sent every publication and discards each one after inspecting its prefix, and a SUB that subscribes to one prefix receives the others too. A publisher's egress is therefore the message size times the number of connected subscribers regardless of what they asked for, and a subscription is a receiver-side cost saving only. [21][30]
- **PUSH/PULL v0.** A PUSH selects one connected puller able to receive, round-robin among available peers; unavailable peers are excluded by flow control. With no eligible peer, the send waits or times out. [19]
- PULL receives as messages arrive. If two peers have messages ready, their order is undefined; PULL cannot send and PUSH cannot receive. [19]
- **REQ/REP v0.** A cooked REQ sends one outstanding request per socket context and normally spreads requests among peer REP sockets; the selected REP receives then replies. REQ automatically resends until reply or timeout. [5][23]
- A cooked REP may send only after receiving its corresponding request and may have only one pending receive per context; REQ may receive only after a request. Violations return `NNG_ESTATE`. [5][23]
- A new REQ send cancels the earlier request locally and discards its later reply, but cannot cancel processing already performed by a REP. [5]
- REQ contexts each carry one independent outstanding request, retry configuration, and request ID; REP contexts likewise each process one independent request. [5][23][10]
- Observed: a cooked REP socket accepts and answers requests from a peer that never retransmits. Nothing in the exchange makes a REP aware of the requester's resend timer — the tag stack and the reply are the entire contract — so a peer that sends each request exactly once is served normally. [30]
- REQ/REP forwarding: a device prepends its local peer ID on request reception; the REP copies the header to the reply; each forwarder pops its ID to select the reverse pipe; the original REQ finally sees only its request ID. [5]
- **SURVEYOR/RESPONDENT v0.** A surveyor broadcasts a survey to every respondent, then accepts at most one response per respondent; a respondent may decline by not replying. Duplicates remain possible in some topologies. [22][24]
- Each cooked surveyor context permits one active survey. Starting another cancels its prior survey; its `SURVEYTIME` starts on send, late replies are discarded, blocked receive expires as `NNG_ETIMEDOUT`, and later receives with no survey return `NNG_ESTATE`. [22]
- Respondent contexts route each incoming survey to one context and return its reply toward the latest survey received there. [24]
- Survey forwarding uses the same stack-and-pop reverse routing method as REQ/REP, with survey IDs rather than request IDs. [22]
- **Cooked versus raw.** Cooked sockets enforce the pattern state machines and headers; raw constructors bypass them, leaving send/receive and header semantics to the application. `nng_device()` requires raw sockets but just forwards messages. [2][6]

### Per-pattern operational matrix

| Protocol | Send selection | Receive selection | Mute-state behaviour | Sources |
| --- | --- | --- | --- | --- |
| BUS v0 | Considers every directly connected pipe. | Receives from connected peers only; no broker or indirect route is implied. | A peer that cannot receive misses its copy, while the originating send succeeds without blocking. | [17] |
| PAIR v0 | One active one-to-one peer relationship. | The paired peer receives messages. | A peer rejects another connection when already actively paired. | [18] |
| PAIR v1 | One-to-one by default; a device increments the hop-count byte at every node. | The receiving node applies its local maximum TTL before forwarding. | The normal one-to-one admission rule applies. | [18][6] |
| PAIR v1 polyamorous | The destination is a local pipe handle rather than a routable identity. | A directly connected selected pipe receives it. | A disconnected directed destination silently discards, without sender error. | [18] |
| PUB/SUB v0 | PUB offers every subscriber connection a copy without testing subscription prefixes first. | SUB tests initial body bytes against local subscriptions; a match admits and no match discards. | PUB has no receive operation; SUB has no send operation. | [20][21] |
| PUSH/PULL v0 | PUSH round-robins among connected pullers that can accept and does not select an unavailable puller merely to preserve turn order. | PULL accepts incoming messages; simultaneously ready peers have no defined order. | PUSH has no receive operation; PULL has no send operation. | [19] |
| REQ/REP v0 | A cooked REQ generally spreads work among available repliers and retains one active request per context. | REP receives a request then may send only its matching reply; independent concurrent processing uses contexts. | REQ receive without an active request, or a second concurrent receive, returns `NNG_ESTATE`; REP send before a request and a second simultaneous receive are likewise rejected. | [5][23][10] |
| SURVEYOR/RESPONDENT v0 | Each survey is broadcast to respondent peers; a respondent context returns toward the surveyor of its most recently received survey. | A surveyor accepts only replies for its outstanding, unexpired survey; each incoming respondent survey goes to exactly one context, while others can receive other surveys concurrently. | A surveyor needs an active survey to receive; a respondent needs a received survey to reply. | [22][24] |

### Context concurrency boundaries

- Contexts share a socket’s endpoint and pipe set rather than creating isolated connections. REQ contexts independently retain request ID and retry state, allowing one concurrent request per context; REP contexts likewise permit several requests to be processed in parallel over one socket. [10][5][23]
- SURVEYOR contexts can overlap surveys with individual deadlines, but high concurrent survey activity can lose outgoing surveys or incoming replies because the pattern is best effort rather than a queueing guarantee. [22]
- Raw sockets cannot use contexts: raw mode makes the application responsible for the omitted protocol state. [10][2]

### Routing and topology detail

**BUS connectivity.** BUS fan-out is a one-hop operation, not flooding: a node
must have a direct pipe to receive a given send. Consequently, an application
that expects every participant to observe every BUS message must establish a
fully connected mesh itself. [17]

**BUS loss boundary.** BUS may deliver a message to some, all, or none of its
directly connected peers. Its nonblocking send semantics make the application,
rather than the protocol, responsible for any retry or membership-aware
republication policy. [17]

**BUS raw rebroadcasting.** A raw BUS receive records its ingress pipe ID in
the header, and a resend excludes that pipe. This prevents immediate echo in
the documented single-socket device arrangement; it does not identify a
message globally or prevent a larger forwarding cycle. [17]

**PAIR v0 interoperability.** PAIR v0 contains no protocol header and is the
legacy wire form recommended when communicating with libnanomsg or mangos.
Its one-to-one topology is therefore a connection admission constraint, not
an application address-selection facility. [18]

**PAIR v1 forwarding.** PAIR v1 initializes its hop counter to one and
increments it when a new node receives the message. A forwarder’s local
`MAXTTL` policy bounds forwarding even though different nodes may choose
different limits. [18][6]

**PAIR v1 addressing.** Polyamorous PAIR’s pipe handle selects only a direct
neighbour. Applications commonly retain the pipe from an incoming message to
reply, but a device proxy cannot carry that directed-send choice beyond the
adjacent pipe. [18]

**PAIR delivery caveat.** Ordinary PAIR back pressure can make the pattern
appear reliable, but devices and raw sockets can discard messages. The manual
therefore directs applications requiring a delivery semantic to REQ or an
application acknowledgement layer. [18]

**PUB/SUB filtering locus.** Topic matching occurs at the subscriber after
the publisher has delivered every publication to every subscriber link. A
subscription is an arbitrary byte prefix of the body; an empty prefix admits
all publications. [20][21]

**PUB/SUB queue policy.** When the local subscriber queue is full, the default
policy removes its oldest queued message to make room. Selecting
`SUB_PREFNEW=false` instead preserves old queued messages by rejecting the
new message. [21]

**Pipeline eligibility.** PUSH selects only a puller capable of accepting a
message, so its rotation is over the ready subset rather than a static worker
list. This is load distribution by immediate acceptance, not an advertised
capacity or service-rate protocol. [19]

**Pipeline loss caveat.** The pipeline manual says flow control attempts to
avoid drops but gives no delivery guarantee and no acknowledgement. A process
that needs confirmation of completed work must add it above the pattern or
use REQ/REP where its duplicate semantics are acceptable. [19]

**REQ cancellation scope.** Sending a newer request cancels the requester’s
interest in its earlier reply and causes a late old reply to be discarded.
It does not withdraw the earlier request from a replier or undo work already
performed there. [5]

**REQ retry triggers.** An outstanding request is resent after its resend
timer elapses, when the original peer disconnects, or when a peer becomes
available while it is waiting. The resend clock’s default check granularity
is one second and is shared by the socket’s contexts. [5]

**REQ reverse routing.** The request ID occupies the final position in the
backtrace and has its high bit set; device-local peer IDs precede it with that
bit clear. These pipe-local IDs route the response backwards without supplying
a globally meaningful peer identity. [5]

**REP state isolation.** A REP context provides the state-machine unit for
one received request and its response. Multiple contexts retain independent
request state, rather than relaxing the cooked socket’s ordering rule for a
single context. [23][10]

**Survey time boundary.** Survey time starts when the survey is sent, not when
a particular respondent receives it. A response after expiry is discarded,
which makes nonresponse indistinguishable from a slow, unreachable, or
deliberately silent respondent. [22][24]

**Survey response count.** A surveyor normally expects at most one response
from each respondent, but the manual warns that some topologies can duplicate
responses. The pattern therefore supports collection within a deadline, not a
quorum-certified membership result. [22]

**Survey reverse routing.** A forwarding node prepends its local peer ID to
the survey header, and the response returns by popping those IDs. When it
reaches the initiating surveyor, only the MSB-marked survey ID should remain.
[22]

**Cooked protocol ownership.** Cooked sockets supply state-machine checks,
header transfer, matching, and retry where the chosen protocol defines them.
Raw sockets preserve the wire headers but transfer responsibility for those
operations to the application. [2][6]

**Device scope.** `nng_device()` operates on raw sockets and forwards messages
without adding application processing. Protocol header designs such as
REQ/REP backtraces and PAIR v1 TTL supply the routing or loop information that
a forwarding topology needs. [2][5][18]

## 5. Flow control and backpressure

- NNG socket `SENDBUF` and `RECVBUF` are depths in **messages**, each configurable from 0 through 8192, not byte credit. Transports may also buffer independently. [6][19]
- A full or zero-depth send path blocks until it can queue/hand off, unless the socket send timeout yields `NNG_ETIMEDOUT`; the send readiness FD is readable only while an immediate nonblocking send is possible. [6]
- `RECVBUF` holds transport-arrived messages until application delivery; not every protocol supports it, notably REQ can handle one reply per context. [6]
- `SENDBUF` is likewise unavailable for protocols that permit only one outstanding transaction per context, notably REQ. [6]
- PUSH has an explicitly documented default `SENDBUF=0`; it waits for an eligible puller, while a positive depth (at most 8192) admits that many intermediate messages. [19]
- PAIR normally blocks when no peer can receive, but device/raw topologies can still discard. [18]
- BUS never blocks and drops an undeliverable copy. [17]
- PUB broadcasts best-effort; SUB decides queue-full retention (`PREFNEW` old-drop or new-reject). [20][21]
- REQ retry is protocol recovery, not flow-control credit; it can duplicate a request after a missing reply. [5]
- `RECVMAXSZ` is a separate byte bound against a peer claiming a huge message; it is not a queue limit. [6]
- Observed while writing an implementation from these pages: the buffer defaults the manual publishes cannot be applied uniformly. `SENDBUF=0` is documented for PUSH and means what it says there — the send waits for an eligible puller — but the same zero for the protocols whose answer to a full queue is to discard (BUS, PUB, SURVEYOR) would discard every message that arrives while a connection's writer is still busy with the previous one, which is "almost never" rather than "best effort"; the manual's own note that a transport may buffer independently is what a real node relies on here. A dropping protocol therefore needs a queue of its own, and the depth is a decision the manual leaves to the implementer rather than a value it publishes. [6][19][17][20] [inference]
- Observed, same source: `PREFNEW`'s two policies (drop oldest, reject newest) belong to the queue of **admitted** publications on the socket and not to the pipe's receive queue. A pipe queue that dropped a subscribed message to make room for one the subscriber never asked for would be applying a subscriber's retention policy to traffic it does not want, and the filter is receiver-side and runs after the queue. A pipe's own full-queue behaviour is therefore to stop reading the connection, which is the only backpressure SP has — it grants no credit on the wire. [21][6] [inference]
- Observed, same source: the receive buffer being the **socket's** rather than the pipe's is load-bearing and not an implementation detail. A REP peer that answers a request and closes immediately — the ordinary shape of a short-lived responder — has its reply on the wire before the close, and an implementation that held arrived messages in the pipe would discard the reply when the pipe went. The manual's wording for `RECVBUF` is a socket receive buffer, and that is the only placement under which the close of an answered exchange is not a lost answer. Anything held there outlives the pipe it came from and so needs a bound of its own: the pipe's is gone with the pipe, and a peer may reconnect and disconnect without limit. [6][8] [inference]

## 6. Delivery guarantees and acknowledgement

- SP has no transport-independent application acknowledgement, broker receipt, transaction, or persistence acknowledgement. [2][3]
- BUS, PUB/SUB, PAIR, and PUSH/PULL provide no delivery acknowledgement; their manuals explicitly describe best effort or lack of a guarantee. [17][18][19][20]
- REQ’s reply is the only built-in completion signal: receipt of a matching reply stops periodic retransmission. It does not prove a remote side effect occurred exactly once. [5]
- A reply lost after the REP processed the request causes REQ retransmission; requests therefore need idempotent semantics. [5]
- Survey responses are best effort, optional, and not acknowledgements of broadcast delivery. [22][24]
- Observed: there is no refusal, error or reject frame anywhere in the mapping. A peer that will not or cannot take a message has exactly two ways to say so — close the pipe, or stay silent — and the two are indistinguishable from a sender that is waiting for something. A message dropped for size, for hop count, or for a protocol state violation is dropped without any signal to its sender at all. Every diagnosis on the sending side is therefore an inference from a close or from a timeout. [3][30]

## 7. Ordering and duplicates

- NNG’s generic contract permits dropped or reordered messages. [2]
- PUSH selects eligible peers round-robin, but PULL order across simultaneously ready peers is undefined. [19]
- PUB/SUB offers no replay or ordering guarantee; local queue replacement can remove old messages. [20][21]
- REQ retransmission creates duplicate requests, including when a reply is lost; the protocol has no deduplication key exposed to the REP beyond its routing header. [5]
- SURVEYOR normally gets at most one response per respondent but explicitly permits duplicates in some topologies. [22]
- PAIR v1 hop count constrains device forwarding loops; BUS raw exclusion only prevents a message returning immediately to the pipe from which that raw socket received it. [18][17]

## 8. Failure behaviour

| Event | Observation and loss/ambiguity |
| --- | --- |
| Dialer cannot connect | Default synchronous `nng_dial()` returns the connection failure and does not continue; `NNG_FLAG_NONBLOCK` retries asynchronously. A later pipe closure also triggers redial. [7] |
| Peer crash mid-REQ | REQ resends on peer disconnection, retry timer, or a newly available peer. The REP may already have acted, so the retry is ambiguous and can duplicate work. [5] |
| BUS peer/backlog full | The undeliverable peer copy is discarded; BUS send never blocks. [17] |
| PAIR peer/backlog unavailable | Normal PAIR blocks; polyamorous directed PAIR v1 instead silently discards for that unavailable/closed pipe. [18] |
| PUSH/PULL buffer full | PUSH considers only pullers that can accept; with no candidate it waits or times out. Buffered sends fail with timeout when they cannot queue. [19][6] |
| PUB/SUB subscriber buffer full | Each SUB removes oldest by default, or rejects a new message with `SUB_PREFNEW=false`; publisher has no receipt. [21] |
| Oversized remote message | `RECVMAXSZ` discards it; limit zero accepts unlimited size. inproc does not enforce this option. [6][16] |
| Survey deadline expires | Later responses are discarded; waiting receive returns `NNG_ETIMEDOUT`, then a receive without an outstanding survey is `NNG_ESTATE`. [22] |
| Reply past the local hop ceiling | Dropped on receipt, the pipe kept, and nothing is sent back: the originator observes only the absence of a reply until its own timeout. A ceiling reached at a forwarder is invisible to the peers on either side of it. [18][30] |
| Pipe closes in flight | The pipe is removed and cannot communicate after `REM_POST`; generic NNG gives no delivery outcome, while outstanding REQ is resent. [9][5] |
| TLS handshake/auth failure | Dial can report `NNG_EPEERAUTH` or `NNG_EPROTO`; no pipe is usable until connection/negotiation succeeds. [7][9] |
| WebSocket handshake/frame violation | The SP protocol needs binary frames; a compliant WebSocket peer discards invalid UTF-8 TEXT data and breaks the connection. [14] |
| ZeroTier liveness failure | After configured unanswered pings, the transport assumes peer death and closes the connection; dial attempts have configurable count/interval. [15] |

## 9. Reliability recipes

- **Idempotent REQ service.** Make request effects repeat-safe, because REQ retries after lost replies/disconnects. This delivers eventual processing while a requester remains alive, not exactly-once effects. [5][25]
- **REQ/REP device load balancing.** Connect multiple REP peers; REQ normally spreads requests among them, and a device can form intermediaries. Cost: no per-operation capacity declaration, and retries can duplicate work. [5]
- **PUSH/PULL work distribution.** Use PUSH’s ready-peer round robin for pull workers. It reacts to a peer able to accept but does not promise capacity-weighted distribution, persistence, or acknowledgement. [19]
- **Surveyor voting/service discovery.** Broadcast survey, collect responses only until `SURVEYTIME`, and treat missing responses as absent rather than failed. Cost: best effort and topology duplicates. [22][24]
- **Fully connected BUS mesh.** Explicitly connect every participant to every other participant, because BUS only broadcasts one hop. Cost: aggregate traffic increases loss likelihood. [17]
- **Reconnect tuning.** Use nonblocking dial plus reconnect min/max bounds; this restores pipes but does not restore a durable subscription/session or queued messages. [7][6]
- **Bound hostile input.** Set `RECVMAXSZ` before the endpoint starts, preferably per endpoint, to prevent oversized-message allocation attacks. [6]

## 10. Security and identity

- The SP pattern protocols define no message-level authentication, authorization, or authenticated sender identity. [3]
- TLS transport provides TLS 1.2 over TCP with configurable authentication mode, CA file, certificate/key file, verification result, peer common name, and peer alternative names. [13][14]
- WSS uses TLS support and exposes the same TLS configuration family; a shared HTTP server instance can share one TLS configuration. [14]
- IPC can expose OS-derived peer UID, GID, PID, and (where applicable) zone ID; the UID/GID values are described as non-forgeable at connection time. [11][6]
- A pipe-add-pre callback may reject a pipe before it enters the socket, for example after local authorization logic; this is application policy, not SP authorization. [9]
- ZeroTier network admission has UP, CONFIG, DENIED, NOTFOUND, ERROR, OBSOLETE, and UNKNOWN status, and persistent node identity is optional via `ZT_HOME`. [15]


## 11. Limits and resource bounds

- Socket send and receive queues are individually bounded to 0–8192 messages when supported. Positive buffers do not eliminate additional transport buffering. [6]
- `RECVMAXSZ` is unlimited at zero; a nonzero maximum is the primary inbound-size defence, except on trusted inproc. [6][16]
- `MAXTTL` is 1–255; supported forwarding protocols commonly default to 8, but each node checks its own setting. [6]
- Observed: the `MAXTTL` range the manual documents and the range the implementation accepts are not the same. `NNI_MAX_MAX_TTL` is **15** (`src/core/defs.h`), and the source comment gives buffer sizing as the reason, so a stack that the specification's 255 permits is refused by a real node at 16. An implementation that takes 255 as the bound accepts stacks its peers drop; one that takes 15 silently narrows the specification. **Still open after the 2026-09-12 run**, which exercised hop counts only up to a device's own ceiling and never offered a real node a sixteenth hop. What would settle it: a raw PAIR v1 send to a cooked NNG socket whose count is 15, and a sixteen-element REQ tag stack to a cooked NNG REP socket, observing in each case whether the message is delivered, dropped, or the connection closed. [6][30][31]
- SUB topics are arbitrary-size byte arrays and are maintained locally; the manual gives no subscription-count limit. [21]
- A REQ/REP context has at most one active request; a surveyor context has one active survey; creating contexts multiplies their independent timers/state. [5][22][23]
- A raw mode application owns headers, retries, matching, and loop controls itself; this removes protocol safeguards rather than adding resource bounds. [2][10]
- ZeroTier’s MTU includes 20 bytes of transport overhead plus normally at most 16 bytes of protocol overhead, and the transport is experimental. [15]

### Endpoint-specific bounds

- A socket-level maximum can be overridden on an individual dialer or listener.
  This permits different limits for different trust boundaries.
  [6]

- `NNG_OPT_RECVFD` and `NNG_OPT_SENDFD` cannot be mixed with contexts on one socket.
  The manual calls that combination unsupported and unpredictable.
  [6][10]

- An endpoint URL is limited to `NNG_MAXADDRLEN` for TCP and TLS forms.
  The exact macro value is implementation-defined rather than an SP wire limit.
  [12][13]

- IPC path compatibility with legacy nanomsg requires at most 122 bytes including NUL.
  This is a legacy URL representation constraint.
  [11]

- ZeroTier initial connection setup can take up to about one minute in extreme cases.
  It is therefore unsuited to short-lived programs.
  [15]

## 12. Answers to the problem catalogue

- **P1 — loss and safe retry.** REQ detects a missing reply via `REQ_RESENDTIME`, peer disconnect, or availability of a peer and retransmits; idempotent request effects are required because duplicates are possible. Other patterns have no safe retry protocol. [5]
- **P2 — dead/unreachable peer.** Dialers reconnect after a pipe closes and nonblocking dials retry failed attempts with configured backoff. ZeroTier alone specifies ping-based death detection; no SP last-will or session state exists. [7][6][15]
- **P3 — capacity-based work spread.** PUSH chooses round-robin only among pullers capable of accepting a message, which is readiness rather than declared capacity. REQ normally spreads requests among REP peers. [19][5]
- **P4 — slow receiver.** Backlog sits in supported socket message queues and possibly transports; bound it with `SENDBUF`/`RECVBUF` (0–8192). PUSH/PAIR block, BUS drops, and SUB drops old or rejects new locally. [6][19][18][17][21]
- **P5 — late joiner state.** No retained publication, snapshot, or replay exists in SP PUB/SUB. [20][21]
- **P6 — failover.** A dialer reconnects pipes, and REQ can resend to an available peer. Subscriptions, sessions, queues, and in-flight state are not durable/re-established protocol objects. [7][5][3]
- **P7 — survive restart.** No SP persistence or durable-delivery acknowledgement exists. [3][2]
- **P8 — ordering.** No generic ordering guarantee; PULL explicitly leaves ties among ready peers undefined. [2][19]
- **P9 — duplicates/exactly once.** REQ and survey topologies can duplicate; SP supplies no deduplication or exactly-once guarantee. [5][22]
- **P10 — request/reply routing.** REQ/REP uses the 32-bit request-ID/peer-ID backtrace stack; devices push incoming peer IDs and pop them for replies. [5]
- **P11 — topology/discovery.** Either pattern role can dial or listen, both simultaneously; the protocols are brokerless and make no discovery service. BUS needs a fully connected mesh. [2][7][17]
- **P12 — flow credit.** No receiver-granted credit; the unit is queued messages in local socket buffers. Availability controls PUSH selection; exhaustion blocks/times out or follows protocol-specific drop policy. [6][19]
- **P13 — large/streaming bodies.** `RECVMAXSZ` bounds whole messages; NNG delivers wholly or not at all and has no body streaming. [6][2]
- **P14 — identity.** SP has no per-message authenticated identity or authorization. TLS authenticates transport peers; IPC can expose OS peer credentials. [3][13][11]
- **P15 — hostile resource use.** Configure per-endpoint `RECVMAXSZ` before connection; configure message queue depths. Zero remains unbounded and inproc ignores the size limit. [6][16]
- **P16 — observability.** Pipe add/remove callbacks report connection lifecycle; pipe/message handles reveal source pipe, and statistics snapshots exist in NNG. No delivery receipts or tracing header is defined. [9][8][2]
- **P17 — shutdown.** Closing a pipe removes it; no generic in-flight drain, linger, or outcome guarantee is specified. REQ may resend outstanding work after disconnect. [8][9][5]
- **P18 — transports.** SP RFCs specify TCP, IPC, TLS, UDP, and WebSocket mappings; NNG provides inproc, IPC, TCP, TLS, WS/WSS, and experimental ZeroTier. Security and framing differ by transport, while pattern semantics remain socket protocol semantics. [3][2][12][13][14][15][16]

## 13. Ecosystem

- **NNG.** Active C implementation, NNG 1.10.0 at the researched manual; wire-compatible with SP/libnanomsg when both sides select mutually supported protocol and transport. [1][2]
- **nanomsg/libnanomsg.** Original C implementation with nanomsg 1.0 API; NNG documents its compatibility layer as a transition aid and discourages its use for new applications. [4]
- **mangos.** Go implementation specifically named by NNG as a conforming interoperable implementation for the common SP subset. [2]
- **Rust.** `nng` is the Rust binding for NNG; `runng` is an asynchronous Rust binding/wrapper; `nanomsg-rs` targets the legacy nanomsg library. These are bindings, not independent SP wire specifications. [26][27][28]
- **What the `nng` crate is, measured.** Version 1.0.1 is a thin binding over `nng-sys` 1.4.0-rc.0, which builds the NNG C library from vendored sources: the wire behaviour under it is the C implementation's, not the crate's, and the library version is therefore the one to cite. It builds with a C toolchain and no system NNG package. Its socket calls are blocking, `Socket::recv` included, so a program driving it beside an event loop must keep those calls off the loop's threads; `RecvTimeout` and `SendTimeout` are the options that turn a stalled exchange into an error instead of a hang. [30]
- **A binding is not a second opinion.** `nng`, `runng` and `nanomsg-rs` all terminate in a C library, so agreement between two of them says nothing about the wire. mangos remains the only independent conforming implementation the project names, and what "the common SP subset" excludes is stated nowhere, so a disagreement between NNG and mangos has no published tie-breaker. [2][26][30]
- **What a full pairing run established, measured.** All eleven protocol ids and both role nibbles of §3's registry are what a running NNG 1.4.0-rc.0 puts in the 8-octet header and accepts from a peer: REQ/REP, PUSH/PULL, PUB/SUB, PAIR v0, PAIR v1, SURVEYOR/RESPONDENT and BUS each exchanged messages with an independent implementation in **both roles**, NNG listening and NNG dialling, over `tcp://`, with no adjustment on either side beyond the PAIR v1 hop count of §3. The REQ/REP tag stack survives a round trip through a cooked NNG REP socket byte for byte, including the terminal-bit rule, and a device written from the RFCs forwards between two NNG raw sockets in both directions. What this does *not* establish: anything about mangos, anything about a later NNG release, and anything about the `MAXTTL` ceiling of §11. [31]
- **Other bindings.** NNG directs non-C users to the nanomsg site’s bindings list; binding maintenance is independent of NNG’s C release cadence. [2][29]
- **Compatibility boundary.** PAIR v0 is recommended for legacy nanomsg/mangos interoperability; PAIR v1, polyamorous mode, NNG-specific URL forms, inproc, and ZeroTier are not automatically portable. [18][12][15][16]
- **Historical note.** The nanomsg comparison page is maintainer-written historical material (last updated 2018-02-07), not a current normative specification; it says nanomsg’s REQ retry was designed to avoid the named limitation of ZeroMQ REQ. [25]

## 14. Sources

1. NNG Reference Manual index, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/.
2. `nng(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng.7.html.
3. nanomsg RFC repository, master branch, accessed 2026-09-08: https://github.com/nanomsg/nanomsg/tree/master/rfc.
4. NNG Reference Manual index, compatibility API section, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/index.html.
5. `nng_req(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_req.7.html.
6. `nng_options(5)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_options.5.html.
7. `nng_dial(3)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_dial.3.html.
8. `nng_pipe(5)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_pipe.5.html.
9. `nng_pipe_notify(3)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_pipe_notify.3.html.
10. `nng_ctx(5)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_ctx.5.html.
11. `nng_ipc(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_ipc.7.html.
12. `nng_tcp(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_tcp.7.html.
13. `nng_tls(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_tls.7.html.
14. `nng_ws(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_ws.7.html.
15. `nng_zerotier(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_zerotier.7.html.
16. `nng_inproc(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_inproc.7.html.
17. `nng_bus(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_bus.7.html.
18. `nng_pair(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_pair.7.html.
19. `nng_push(7)` and `nng_pull(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_push.7.html; https://nng.nanomsg.org/man/v1.10.0/nng_pull.7.html.
20. `nng_pub(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_pub.7.html.
21. `nng_sub(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_sub.7.html.
22. `nng_surveyor(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_surveyor.7.html.
23. `nng_rep(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_rep.7.html.
24. `nng_respondent(7)`, NNG 1.10.0, 2026-09-08: https://nng.nanomsg.org/man/v1.10.0/nng_respondent.7.html.
25. Martin Sústrik, “Differences between nanomsg and ZeroMQ”, maintainer-written historical article, updated 2018-02-07: https://nanomsg.org/documentation-zeromq.html.
26. `nng` Rust crate, crates.io, accessed 2026-09-08: https://crates.io/crates/nng.
27. `runng` Rust crate, crates.io, accessed 2026-09-08: https://crates.io/crates/runng.
28. `nanomsg-rs` Rust crate, crates.io, accessed 2026-09-08: https://crates.io/crates/nanomsg.
29. nanomsg bindings list, accessed 2026-09-08: https://nanomsg.org/documentation.html.
30. The `nng` Rust crate 1.0.1 over `nng-sys` 1.4.0-rc.0 (vendored NNG C library 1.4.0-rc.0), exercised on Linux x86-64 in 2026-09 against an independent SP implementation written from the RFCs: `Req0`/`Rep0`, `Push0`/`Pull0` and `Pub0`/`Sub0` sockets over `tcp://`, with the C sources `src/core/protocol.h`, `src/core/defs.h` and `src/sp/protocol/pair1/pair.c` read for the values the manual does not publish. Used for: the dial behaviour of §1, the wire layouts and endpoint type ids of §3, the fan-out and REQ observations of §4, the refusal behaviour of §6 and §8, the hop ceiling of §11, and the crate facts of §13. Everything attributed to this source is what NNG 1.4.0-rc.0 did on that machine, not a claim about the specification or about a later release.
31. The `nng` Rust crate 1.0.1 over `nng-sys` 1.4.0-rc.0 (vendored NNG C library 1.4.0-rc.0), exercised on Linux x86-64 on 2026-09-12 against an independent SP implementation written from the RFCs, in a suite of eleven tests run in both roles over `tcp://`: `Req0`/`Rep0`, `Push0`/`Pull0`, `Pub0`/`Sub0`, `Pair0`, `Pair1`, `Surveyor0`/`Respondent0` and `Bus0` pairings; the eleven protocol ids and both role nibbles compared against an encoder written from the RFC registry; a REQ tag stack sent through a cooked NNG REP socket and compared byte for byte on return; a raw `Pair1` send captured to read the hop count NNG originates; and a device forwarding between two NNG raw sockets. Used for: the PAIR v1 hop count of §3, which this run settled against the earlier source reading, the protocol-id registry of §3, and the pairing and device observations of §13. Everything attributed to this source is what NNG 1.4.0-rc.0 did on that machine and date, not a claim about the specification or about a later release.