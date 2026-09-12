# MQTT 5.0 — adapter mapping

Status: mapping document, written before any MQTT code. It is the document the MQTT **client**
library — `crates/mqtt/weida-mqtt-codec` and `crates/mqtt/weida-mqtt` — is read against, which
is the exception [0014](../decisions/0014-parallel-libraries.md) §3 item 4 makes for this file:
every other forwarder's mapping document is filed after the library it forwards, this one comes
first because it was already filed and because MQTT is the first protocol in the catalogue whose
mapping is genuinely hard. **The broker half of every rule below is marked deferred, never
absent.** An MQTT server is a broker — sessions that outlive connections, retained-message
storage, shared-subscription dispatch — and the broker is Phase D
([LOOP.md](../LOOP.md) §9, [0014](../decisions/0014-parallel-libraries.md) §2). Where a rule has
two halves, the client half is stated and checkable today and the server half says *deferred to
Phase D* with what it will owe.
Date: 2026-09-12
Derived from: [docs/research/mqtt5.md](../research/mqtt5.md) (MQTT Version 5.0, OASIS Standard,
07 March 2019; 3.1.1 differences in that sheet's §1.9). Every MQTT claim below carries that
sheet's section and, where the specification numbers it, the conformance statement
`[MQTT-x.y-z]`; every weida claim carries the weida document or decision it comes from. Where
MQTT and weida cannot be made to coincide, §8 names the loss and says how it would be observed.

## 1. Scope

**What is being built, and what is not.** MQTT is the first protocol in this repository whose
topology is strictly asymmetric: "the server binds and accepts, the client connects", a client
"can never listen", and there is no brokerless mode and no client-to-client path
[mqtt5 §12/P11]. That single fact decides the scope of this document.

- **Built now — the client.** `weida-mqtt-codec`, sans-I/O with an empty `[dependencies]`
  ([0013](../decisions/0013-competitor-libraries.md) §4.3), and `weida-mqtt` on `weida-runtime`
  ([0013](../decisions/0013-competitor-libraries.md) §4.2), in the shape
  [0013](../decisions/0013-competitor-libraries.md) §4.4 gave `weida-zmq`: typed surface, async
  first, options honoured or refused at configuration time, identity types kept apart at the
  type level.
- **Deferred to Phase D — the server.** Accepting MQTT clients means holding sessions across
  connections, storing retained messages outside any session and dispatching shared
  subscriptions. Those are broker responsibilities and weida has no broker hop yet
  ([GUARANTEES.md](../GUARANTEES.md) §6, [PROTOCOL.md](../PROTOCOL.md) §11).
- **A forwarder is not filed by this document either**
  ([0014](../decisions/0014-parallel-libraries.md) §3 item 4). What §2-§9 state is the mapping a
  `weida-mqtt-bridge` would have to obey; the backlog gains that crate after the client exists.

**Both forwarder directions are nevertheless expressible with a client, and that is not a
loophole.** MQTT's own answer to broker-to-broker federation is "running a client on one broker
that connects to another", which is exactly what No Local and Retain As Published were added for
[mqtt5 §12/P11], [mqtt5 §4.6]. So:

- **Inbound (MQTT → weida)** is our client *subscribing* on a foreign broker and publishing what
  arrives onto a weida endpoint.
- **Outbound (weida → MQTT)** is our client *publishing* to a foreign broker what a weida
  endpoint produced.

What is **not** expressible with a client, and is therefore deferred rather than worked around,
is a weida-side peer reaching *MQTT clients*: that needs us to be the server.

**The adapter would be a hop, not a tunnel.** It terminates MQTT — the one-byte type-and-flags
header, the Variable Byte Integer Remaining Length, the property framework, the QoS handshakes,
the session — and it terminates weida. Nothing is forwarded opaquely, which is what makes "all
guarantees are defined against the immediate next hop" ([INVARIANTS.md](../INVARIANTS.md))
checkable here and what lets §7 name the exact point at which each chain ends.

**The client library is a client of a broker, and the broker is the far side of every guarantee
in this document.** This is structurally unlike ZMTP and SP, where the adapter's peer was
another endpoint of the same rank. Every MQTT guarantee is between our client and one broker,
and the broker's own onward hop to a subscriber is a *second* hop with an independently chosen
QoS [mqtt5 §6]. No claim in this document reaches past the broker.

Out of scope for the client, each with its reason:

- **MQTT 3.1.1 and 3.1 as spoken versions.** The client sends Protocol Version 5 and nothing
  else. 3.1.1 has no properties, no reason codes, no server-initiated DISCONNECT, no AUTH, no
  Session Expiry, no shared subscriptions and no subscription options beyond maximum QoS
  [mqtt5 §1.9] — half the surface this library is built to carry. The interop consequence is
  §10: `rumqttd` 0.20.0 is 3.1.1-only [mqtt5 §13], so a 5.0 client meets it only where the two
  versions agree, and the rest needs a 5.0 broker.
- **The WebSocket binding** (chapter 6 of the specification, the only normative transport
  binding). It changes framing and handshake, not semantics [mqtt5 §12/P18]; TCP 1883 and TLS
  8883 come first because they are what both interop peers listen on.
- **MQTT over QUIC.** Not an MQTT 5.0 transport: EMQX ships it and states it "is not yet an MQTT
  standard protocol", it binds a port that is not MQTT-registered, and its multi-stream design
  changes the guarantees [mqtt5 §12/P18]. The temptation here is obvious and is refused — weida
  is QUIC-native, and "MQTT on QUIC" would be a protocol nobody else speaks.
- **MQTT-SN**, which the sheet itself excludes [mqtt5 §0].

## 2. MQTT primitive to weida pattern

weida's own mapping of this protocol family is [ARCHITECTURE.md](../ARCHITECTURE.md) §6b, whose
first column names ZeroMQ and nanomsg; MQTT appears in §4 of that document as an adapter
protocol rather than a row of §6b, because MQTT's primitives are *broker* primitives and §6b's
are patterns between peers. That mismatch is the content of this table.

| MQTT primitive | weida counterpart | Faithful? | What the client or a forwarder must do |
| --- | --- | --- | --- |
| Network Connection (3.1, 4.2) | one QUIC connection to one peer [ARCHITECTURE §2] | no — one byte stream against many streams | MQTT multiplexes every packet of a session onto one "ordered, lossless stream of bytes" [mqtt5 §12/P18]; weida gives each transfer its own stream and separates control from bulk ([0002](../decisions/0002-control-and-bulk-separation.md)). §3 and L11 |
| Session, keyed by Client Identifier (4.1) | **no counterpart** — "There is no L0 session state. Nothing is retained across a connection" [0008 §4.5] | no | The client owns the session state itself (§7, L1). A forwarder MUST NOT present it as a weida property: weida subscriptions "stay per connection and are dropped wholesale when it closes" [0008 §4.5]. *Server half deferred:* the broker owes the server-side inventory of [mqtt5 §2] — subscriptions, queued messages, the Will and the session-end time |
| Client Identifier (3.1.3.1) | **not an identity** [0008 §4.1] | no | A self-asserted UTF-8 string that is also the key to session state [mqtt5 §2]. Never presented as `IncomingMeta::peer`; §5 and L14 |
| Subscription: filter plus options plus optional identifier (4.8.1) | `Subscriber` + SUBSCRIBE/UNSUBSCRIBE [ARCHITECTURE §6b] | partly | Per-filter maximum QoS, No Local, Retain As Published and Retain Handling have no weida equivalent [PROTOCOL §6.4]; the filter itself maps per §6. Re-subscribing a filter MUST replace the subscription without losing messages ([MQTT-3.8.4-3]) [mqtt5 §2], which weida's idempotent SUBSCRIBE already does [PROTOCOL §6.4] |
| Shared Subscription `$share/{ShareName}/{filter}` (4.8.2) | **no counterpart** — "one weida subscription per group member selects the same set, and single-delivery-per-group is the L2 credit and queue work of 0003 §4.2, not topic matching" [0007 §5] | no | Joined **as a client** [0014 §2]; never reduced to a weida fan-out. §6 and L5. *Server half deferred:* the dispatch choice, which MQTT leaves entirely free — "free to choose, on a message by message basis, which Session to use" [mqtt5 §4.2] |
| Application Message: payload, QoS, properties, Topic Name (1.2) | one **one-way transfer** — P1 of [ARCHITECTURE §6a] | yes on the payload, no on the properties | The payload is opaque bytes both sides [mqtt5 §3], [PROTOCOL §4]. The properties have no weida header fields; a forwarder carries them as payload framing it defines, or refuses (§9.6, L7) |
| PUBLISH QoS 0 | `Publisher`/`Pusher` with the `Delivery` dropped [PATTERNS §1.2] | yes | "the message arrives at the receiver either once or not at all" [mqtt5 §6]; weida's `core` delivery is `BestEffort` [GUARANTEES §6]. Equal, and equally weak |
| PUBLISH QoS 1 + PUBACK | one-way transfer whose `Delivery` is awaited [PATTERNS §1.2] | no — the two receipts certify different things | §7. PUBACK certifies ownership transfer by the *broker application*; a weida receipt certifies the peer's *transport* holds the bytes [GUARANTEES §3]. L2 |
| PUBLISH QoS 2 + PUBREC/PUBREL/PUBCOMP | **no counterpart** | no | weida has deduplication `None` and "nothing on the wire names a transfer" [GUARANTEES §6], so the four-packet handshake cannot be carried at all. L3, and the weakest link of [SYNTHESIS §7.2] |
| SUBSCRIBE / SUBACK, per-filter reason codes (3.8, 3.9) | SUBSCRIBE on the path's connection [0011 §4.1] | no — weida's SUBSCRIBE has no reply half | A weida SUBSCRIBE "arrives on a uni stream with no reply half" [0007 §6], so a per-filter granted QoS or 0x87 has nowhere to go inbound. The client surfaces it as its own typed result; a forwarder cannot propagate it. L18 |
| Packet Identifier, 1..65,535 per direction per session (2.2.1) | **nothing** — an exchange is its own correlation [ARCHITECTURE §6a P2] | n/a | weida needs no identifier because "the bidirectional stream is the correlation" [ARCHITECTURE §6a]. The client owns the identifier space; it is never exported. L12 |
| Topic Alias (3.3.2.3.4) | **nothing** | n/a | A per-connection compression of the Topic Name, never carried across a reconnect ([MQTT-3.3.2-7]) [mqtt5 §2]. Purely local to one MQTT connection; L8 |
| Retained message (3.3.1.3) | **no counterpart** — weida has no last-value cache [SYNTHESIS §2 D8] | no | Read and written **as a client**; never synthesized. L4. *Server half deferred:* storage outside any session, one per exact Topic Name, surviving session end [mqtt5 §2] |
| Will (3.1.2.5, 3.1.3.2.2) | **no counterpart** — weida has no presence signal [PATTERNS §1.8] | no | Declared in CONNECT by the client and fired by the broker. L6. *Server half deferred:* the trigger list and the Will Delay timer |
| Request/response: Response Topic + Correlation Data (4.10) | one **exchange** — P2 of [ARCHITECTURE §6a] | no — the shapes differ in kind | weida's exchange is one bidirectional stream and needs no correlation table [ARCHITECTURE §6a]; MQTT's is two independent publishes through a broker that "treats them as ordinary messages" [mqtt5 §12/P10], with zero or many responders and a reply that may reach a client that is not the requester [mqtt5 §4.3]. L9 |
| Keep Alive + PINGREQ/PINGRESP (3.1.2.10, 3.12) | `RuntimeConfig::keep_alive` / `idle_timeout` [ARCHITECTURE §7] | partly | Both are liveness timers; MQTT's is asymmetric (PINGREQ is client-to-server only [mqtt5 §1]) and the server's threshold is fixed at 1.5x by the specification ([MQTT-3.1.2-22]) [mqtt5 §1], while QUIC's is a transport parameter |
| DISCONNECT with a reason code, either direction (3.14) | `Error` + stop codes [PROTOCOL §9.4] | partly | 29 server-to-client reason codes [mqtt5 §1]; the client surfaces each as a named error rather than as a closed socket. weida's `ErrorCode` vocabulary is smaller and does not grow for this. L18 |
| AUTH, method + data, re-authenticable (4.12) | TLS handshake only [ARCHITECTURE §5] | no | weida authenticates once, with a proved key, and has no in-connection re-authentication. The client implements AUTH; a forwarder terminates it. §5 |
| `$SYS/` and `$`-prefixed topics (4.7.2) | **nothing reserved** [0007 §5] | no | weida has no reserved topic prefix, so `#` matches `$`-prefixed topics too; an MQTT-facing adapter must exclude them itself and subscribe twice where MQTT would [0007 §5]. L16 |

## 3. Stream mapping

**One Application Message is one weida transfer, and therefore one QUIC stream** — "one data
flow maps naturally to one transport stream" [INVARIANTS], P1 of [ARCHITECTURE §6a]. A weida
transfer is a DATA header followed by opaque payload bytes until FIN
([PROTOCOL.md](../PROTOCOL.md) §4).

**The MQTT wire, exactly.** A fixed header of one byte — packet type in bits 7-4, type-specific
flags in bits 3-0 — then Remaining Length as a Variable Byte Integer of 1 to 4 bytes covering
variable header plus payload and excluding itself [mqtt5 §3]. Variable Byte Integers MUST use
the minimum number of bytes ([MQTT-1.5.5-1]), so the encoding is canonical and a non-minimal one
is a Malformed Packet. UTF-8 Encoded Strings and Binary Data are two-byte-length-prefixed and
therefore capped at 65,535 bytes each [mqtt5 §3]. Thirteen of the fifteen packet types end their
variable header with a Variable Byte Integer Property Length followed by identifier/value pairs,
and CONNECT carries a second such set — Will Properties — inside its payload [mqtt5 §3].

**Three numbers bound every allocation, and the codec takes the third as an argument.** The
protocol ceiling on a packet is 268,435,455 bytes, just under 256 MiB, from the Remaining Length
encoding [mqtt5 §3]; a string or binary field is at most 65,535 bytes; and either peer may
declare a lower `Maximum Packet Size`, where zero is a Protocol Error and absence means no limit
below the encoding ceiling [mqtt5 §11]. "No remote input can cause unbounded memory allocation"
[INVARIANTS] therefore has a precise reading here: **nothing is allocated before a declared
length has been checked against a caller-supplied maximum packet size**, which is the same rule
`weida-zmtp` follows for a ZMTP frame's 64-bit size prefix
([0013](../decisions/0013-competitor-libraries.md) §4.4 item 5). The maximum is an argument to
every decode, not a constant in the codec, because it is a per-connection negotiated value.

**One connection carries everything, and that is the structural difference.** MQTT's transport
requirement is a single ordered lossless byte stream in both directions [mqtt5 §12/P18], so
CONNECT, PUBLISH, PUBACK, SUBSCRIBE and PINGREQ all interleave on it. weida gives each transfer
its own stream and separates the control connection from one bulk connection per dialled path
([0002](../decisions/0002-control-and-bulk-separation.md)). Two consequences, both of which a
forwarder must honour rather than discover:

1. **MQTT's "a blocked publish path never stalls acknowledgements, subscriptions or pings"**
   ([MQTT-4.9.0-3], [MQTT-3.3.4-8], [MQTT-3.3.4-10]) [mqtt5 §5] is violated by a weida
   *connection*-level receive window by construction, since one slow reader stalls every writer
   on that connection [PATTERNS §1.3]. A forwarder therefore needs **one weida connection per
   MQTT session**, or separate connections for control and data — which is what 0002 already
   gives it [SYNTHESIS §7.2].
2. **A single MQTT reader task owns the connection.** Every ordering obligation MQTT states is
   per connection and per topic [mqtt5 §7], and the specification says nothing about threads
   [mqtt5 §2], so an implementation fanning one connection across tasks must reimpose that order
   itself. The client does not fan it out: one reader, one writer, and the ordering obligations
   of [MQTT-4.6.0-2] to [MQTT-4.6.0-4] — PUBACK in receipt order, PUBREC in receipt order,
   PUBREL in PUBREC-receipt order — fall out of that rather than being enforced by a sorter.

**A body cannot be delivered before it is complete.** One PUBLISH carries one whole Application
Message: no multipart, no fragmentation, no continuation packet, no chunked mode [mqtt5 §3].
weida's payload is an `AsyncRead`/`AsyncWrite` stream [INVARIANTS], and the two cannot be
reconciled — L11.

**Liveness and reconnect.** Keep Alive is a two-byte second count, maximum 65,535, zero
disables; absent other traffic the client MUST send PINGREQ ([MQTT-3.1.2-20]) and a server that
receives nothing for 1.5x Keep Alive MUST close "as if the network had failed"
([MQTT-3.1.2-22]) — so the Will fires — and SHOULD send DISCONNECT 0x8D first [mqtt5 §1]. The
reverse direction is weaker by specification: a client seeing no PINGRESP "within a reasonable
amount of time" SHOULD close, with no number given [mqtt5 §1]. **That unquantified number is a
configuration value in this library with a real default, not a silent forever**, for the same
reason `ZMQ_LINGER` is finite here ([0013](../decisions/0013-competitor-libraries.md) §4.4 item
5) and for the reason [LOOP.md](../LOOP.md) §2 exists: an unbounded wait is a hang with a
rationale. MQTT defines no reconnect policy at all — "backoff, jitter and retry limits are
absent" and implementations fill the gap with values that disagree by a factor of two
[mqtt5 §1] — so the client's policy is its own, configured, and documented as not being the
protocol's.

## 4. Credit and backpressure mapping

| Side | Unit | Granted by | At the bound |
| --- | --- | --- | --- |
| MQTT send quota (4.9) | **one QoS 1 or QoS 2 PUBLISH packet** — not bytes, not QoS 0, not any other packet type [mqtt5 §5] | the receiver, declaring `Receive Maximum` in CONNECT or CONNACK; directions independent; default 65,535; 0 is a Protocol Error [mqtt5 §11] | the sender MUST NOT send further QoS > 0 PUBLISH packets ([MQTT-4.9.0-2]); it MAY keep sending QoS 0; it MUST keep processing and answering every other packet type ([MQTT-4.9.0-3]) [mqtt5 §5]. Exceeding the peer's value earns DISCONNECT 0x93 |
| MQTT `Maximum Packet Size` (3.1.2.11.4) | bytes, per packet | either peer | a hard limit, not backpressure: a receiver that gets an oversized packet sends DISCONNECT 0x95 and closes [mqtt5 §5] |
| MQTT `Topic Alias Maximum` (3.1.2.11.5) | alias-table entries | the receiver; default 0 | a memory bound, not a rate bound; the sender MUST NOT exceed it ([MQTT-3.3.2-9]) [mqtt5 §5] |
| MQTT QoS 0 | **nothing** | nobody | "There is no mechanism to limit the QoS 0 publications that the Server might try to send", stated twice [mqtt5 §5]. The only answers are DISCONNECT 0x96/0x97 or dropping the socket |
| weida stream and connection windows | bytes | the receiver, as QUIC flow control | the writer blocks — `Backpressure = Block` [GUARANTEES §6], [PATTERNS §1.3] |
| weida `Limits::endpoint_queue` | queued inbound transfers per endpoint | the local process | blocks, by not reading [ARCHITECTURE §6a P4] |
| weida `Limits::subscriber_buffer_bytes` | bytes per subscriber | the local publisher | **drops**, and counts the drop — the one place in v0 where overload is answered by discarding [ARCHITECTURE §6c.5], [GUARANTEES §6] |

**The units do not line up and cannot be translated arithmetically.** MQTT counts packets, weida
counts bytes [SYNTHESIS §7.2]. A forwarder can only bound its own buffer and let each side's
mechanism act on that bound; the bound is named configuration
(`max_in_flight`, `max_message_bytes`), never implicit, and its product with the connection
count is the memory a peer can make the process hold — stated in the configuration's own
documentation, as the SP bridge already does [nng §1].

**Three mappings are faithful and one is not.**

- MQTT's exhausted send quota and weida's blocked writer are the same behaviour: both block, and
  the sheet's cross-protocol row puts them in the same column [SYNTHESIS §2 D10].
- MQTT's `Maximum Packet Size` and weida's `Limits` message caps are the same kind of hard
  bound, and both refuse rather than truncate [PROTOCOL §9.4].
- MQTT's QoS 0, with no credit at all, and weida's `subscriber_buffer_bytes` drop are the same
  admission of defeat, differently placed: MQTT drops at the broker's implementation bound with
  no signal to the publisher [mqtt5 §12/P4], weida drops at the publisher **and counts it**
  [GUARANTEES §6]. The weida side is strictly more observable, which is a difference a forwarder
  must not hide in the other direction (L15).
- **Not faithful: scope.** MQTT's quota is per connection and is re-initialised each connection
  — explicitly not session state [mqtt5 §5] — and "MQTT 5.0 has no per-channel, per-subscription
  or per-topic credit, so a single slow subscription cannot be throttled independently of the
  rest of the session" [mqtt5 §5]. weida's windows are per stream *and* per connection
  [PATTERNS §1.3], so weida can throttle one transfer without the others and MQTT cannot. L17.

**Broker-side queue limits are entirely implementation-defined**, and the divergence is
load-bearing rather than trivia: at its bound Mosquitto silently drops subsequent QoS 1/2
messages, EMQX evicts the *oldest* QoS 0 message, HiveMQ defaults to discarding new arrivals
[mqtt5 §5], [mqtt5 §12/P4]. The publisher gets no signal in any of the three, its PUBACK having
already been returned. A client cannot observe which policy its broker has, and this document
does not let the library claim otherwise: the number recorded in §10 is measured per named broker
version.

## 5. Identity, security and authorization

- **The Client Identifier is self-asserted, and is never a weida peer identity.**
  [0008](../decisions/0008-session-identity.md) §4.1 closed its Option B — a claimed identifier
  in HELLO — and named this protocol as the reason: "a claimed identifier would be the only
  unproved identity in the system and brings MQTT's authorization rule and takeover ambiguity
  with it" [0008 §4.1]. The rule that survives is *proved and never claimed*, on every transport
  [0008 §4.1]. MQTT's own specification agrees about the risk from the other side: authorization
  must cover the right to use a given ClientID, "since the ClientID is the key to session state
  and a colliding identifier gives access to another client's session" [mqtt5 §10].
- **Nothing MQTT carries is presented as `IncomingMeta::peer`.** That field "comes from the
  handshake, never from a header" [GUARANTEES §6]: a proved fingerprint over QUIC, a kernel-proved
  local principal over `AF_UNIX`, `None` for an anonymous client [0010 §4.4]. User Name,
  Password, ClientID and any AUTH outcome are none of those.
- **Identity types stay apart at the type level**, which is
  [0013](../decisions/0013-competitor-libraries.md) §4.4 item 6 applied to this protocol.
  `weida_mqtt::ClientId` is a 1..65,535-byte UTF-8 string whose first 23 bytes of `[0-9a-zA-Z]`
  every server MUST accept ([MQTT-3.1.3-5]) [mqtt5 §2]; `weida_mqtt::UserName` is a self-asserted
  UTF-8 string; `weida_mqtt::AuthMethod` is a scheme name, "commonly a SASL mechanism" but not
  restricted to registered ones [mqtt5 §10]. `weida_core::Fingerprint` is the SHA-256 of a proved
  TLS public key and `weida_core::LocalPrincipal` is what the kernel said. **No `From`, `Into`,
  `AsRef` or `Deref` between the two groups exists anywhere in the workspace, and none may be
  added.** Where they must meet — a forwarder — the meeting is an explicit configuration
  decision by a human, never a conversion.
- **What each MQTT mechanism authenticates, and where it terminates.** Basic: User Name (UTF-8)
  and Password (binary, "can be used to carry any credential information"), with 5.0 permitting a
  Password with no User Name, which 3.1.1 forbade [mqtt5 §10]. TLS client certificates, which may
  authenticate the client in addition to or instead of User Name and Password [mqtt5 §10].
  Enhanced: the AUTH exchange of 4.12, SASL-shaped, repeatable in-connection for credential
  rotation [mqtt5 §10]. All three authenticate **one MQTT connection to one broker** and
  terminate at the adapter. None of them travels with a message.
- **MQTT is not trust-symmetrical, and says so.** "When using basic authentication, there is no
  mechanism for the Client to authenticate the Server. Some forms of extended authentication do
  allow for mutual authentication" [mqtt5 §10]. weida's handshake is mutual by construction when
  a client identity is required [PATTERNS §1.9], [ARCHITECTURE §5]. A client configuration that
  uses User Name and Password over plain TCP therefore authenticates in one direction only, and
  the library's documentation says that at the point of configuration rather than in a chapter
  nobody reads.
- **No identity travels with a message on either side, and that is the one row that lines up.**
  PUBLISH has no authenticated-sender field and the server adds nothing but Subscription
  Identifiers [mqtt5 §12/P14]; a weida DATA header carries no producer identity in the default
  case either, because the receiver already has the proved fingerprint from the handshake
  [0008 §4.4]. The consequence MQTT states explicitly — "Clients connected to a Server have a
  transitive trust relationship with other Clients connected to the same Server and who have
  authority to publish data on the same topics" [mqtt5 §10] — is the reason a forwarder must not
  present a broker-delivered message as coming from a weida peer.
- **Authorization is per topic name for publish and per topic filter for subscribe**, reported as
  reason codes 0x87 and 0x8F, and the mechanism is non-normative [mqtt5 §10]. weida's position is
  unchanged: "a completed handshake says which key answered and nothing about what that peer may
  do" [ARCHITECTURE §5], and identity remains authentication, not authorization [0008 §4.7]. The
  client surfaces 0x87/0x8F as named errors; it never retries them, because a reason code
  >= 0x80 on PUBACK or PUBREC means the message "is dead with no protocol recourse" and MUST NOT
  be retransmitted ([MQTT-4.4.0-2]) [mqtt5 §6].
- **TLS is on 8883 and is the transport, not the identity.** The specification's chapter 5 is
  non-normative and recommends TLS on the `secure-mqtt` port, avoiding NULL-encryption cipher
  suites, and SNI for multi-hostname servers [mqtt5 §10]. CRL and OCSP checking are optional
  there; in this library the verification result is whatever the TLS stack reports, and nothing
  turns it into a sender identity for the same reason `weida-zmq` refuses to
  ([0013](../decisions/0013-competitor-libraries.md) §4.4 item 6).
- **One named attack the specification documents, carried into the client's design.** 5.4.9.2
  describes a publisher-driven denial of service against a *subscriber*: if the broker does not
  validate Disallowed Unicode code points or the Payload Format Indicator but the subscriber
  does, a publisher can make that subscriber close its connection repeatedly, and at QoS 1/2 the
  message is redelivered so it disconnects again [mqtt5 §10]. The remedies the specification
  offers are to change the broker or to make the subscriber library tolerant. This library takes
  the second: a Payload Format Indicator of 1 with a payload that is not well-formed UTF-8 is
  surfaced as a typed property of the delivery, **not** as a connection-closing error, and
  answering it with 0x99 is the application's explicit choice. That is a deliberate reading of a
  MAY and is recorded as such in §10's numbers.

## 6. Topic or address mapping

[0007](../decisions/0007-topic-namespace.md) §5's MQTT row group, reproduced verbatim as
[adapters/README.md](README.md) requires, with what the adapter must do per row.

| Foreign construct | Foreign semantics | weida filter | Exact? | Named loss / adapter obligation |
| --- | --- | --- | --- | --- |
| MQTT level separator `/` | levels of a topic name [mqtt5 §4.1] | `.` | no | the adapter translates `/` to `.`; an MQTT level containing `.` (or a topic containing weida's wildcard bytes) has no faithful translation and MUST be refused or escaped by the adapter, not silently flattened |
| MQTT `+` | exactly one level, must occupy a whole level ([MQTT-4.7.1-2]) [mqtt5 §4.1] | `*` | yes | none |
| MQTT `#` | zero or more trailing levels, including the parent; last and alone ([MQTT-4.7.1-1]) [mqtt5 §4.1] | `#` | yes | none |
| MQTT `$`-prefixed topics | "A server MUST NOT match a filter beginning with a wildcard against a Topic Name beginning with `$`" ([MQTT-4.7.2-1]) [mqtt5 §4.1] | — | no | weida has no reserved topic prefix: `#` matches `$`-prefixed topics too. An MQTT-facing adapter MUST exclude them itself, and MUST subscribe twice (`#` and `$SYS.#`) where MQTT would [mqtt5 §4.1] |
| MQTT `$share/{name}/{filter}` | shared subscription, a work queue over a filter [mqtt5 §4.2] | — | no | not a filter question: one weida subscription per group member selects the same set, and single-delivery-per-group is the L2 credit and queue work of 0003 §4.2, not topic matching |
| MQTT filter length | up to 65,535 bytes [mqtt5 §11] | ≤ 256 B [PROTOCOL §6.4] | no | a longer filter MUST be refused at configuration time; it cannot be carried |

**Three things the client owes beyond that table**, because the table is about a forwarder's
filter translation and the client's job is MQTT's own matcher:

1. **The specification's worked examples are the test, not a paraphrase of them.** `sport/#`
   matches bare `sport`; `sport/tennis/player1/#` matches `sport/tennis/player1` itself as well
   as its children; `sport/tennis/+` matches `sport/tennis/player1` but not
   `sport/tennis/player1/ranking`; `sport/+` does **not** match `sport` but does match
   `sport/`; `/finance` matches `+/+` and `/+` but not `+`; and a filter beginning with a
   wildcard never matches a `$` topic [mqtt5 §4.1]. Each is a case where a plausible
   implementation gets the opposite answer.
2. **Matching is byte-for-byte with no normalization** ([MQTT-4.7.3-4]) [mqtt5 §3], which is the
   same rule weida's segment walker follows — "no normalization, no case folding, no escape
   character; empty segments are permitted and match only empty segments" [0007 §4.2]. Equal,
   and the one place the two namespaces agree without qualification.
3. **`$share` and the ShareName are not considered when matching publications**, ShareName must
   be at least one character and contain none of `/`, `+` or `#`, and No Local MUST NOT be set on
   a shared subscription ([MQTT-3.8.3-4]) [mqtt5 §4.2]. The client validates all three before
   the filter reaches the wire.

*Server half deferred:* a broker additionally owes the `$SYS/` convention, whose content is
implementation-defined — Mosquitto publishes counters and gauges on a `sys_interval` default of
10 s, EMQX publishes under `$SYS/brokers/{node}/...` [mqtt5 §4.1] — and the authorization
recommendation to "consider limiting access to Topic Filters that have broad scope, such as the
`#` Topic Filter" [mqtt5 §10].

## 7. Transfer points and guarantee mapping

**MQTT has a transfer point and weida v0 does not.** This is the reverse of every mapping
document written so far: ZMTP's and SP's foreign sides had no transfer point at all
[nng §7], [zmtp §7], so the chain ended at the adapter's local queue. MQTT's PUBACK and PUBREC
are real responsibility transfers between two applications, and they are the case
[0006](../decisions/0006-guarantee-sets.md) §4.6 means by an edge where the *foreign* side is
stronger.

**What each MQTT packet certifies, from the specification's own footnotes.**

- **PUBACK** — "transfer of ownership for that hop, nothing else. Not that a subscriber received
  the message, not that it was persisted, not that it was processed" [mqtt5 §6]. The receiver
  "does not need to complete delivery of the Application Message before sending the PUBACK"
  ([MQTT-4.3.2-4] and the footnote to figure 4.2) [mqtt5 §6].
- **PUBREC** — the receiver has accepted ownership *and* completed "all checks for conditions
  which might result in a forwarding failure (e.g. quota exceeded, authorization, etc.)"
  [mqtt5 §6]. Strictly more than PUBACK, and still not onward delivery or persistence.
- **PUBREL** — the sender will never send this PUBLISH again, so the receiver may release its
  duplicate-suppression state [mqtt5 §6].
- **PUBCOMP** — the identifier is released and "Publication of QoS 2 message is complete": the
  handshake terminated, not that any subscriber saw anything [mqtt5 §6].
- **Durability is certified by nothing.** "No acknowledgement certifies durability"
  [mqtt5 §12/P7]: the specification puts the choice on the solution developer, contrasting
  volatile memory for meter readings against non-volatile writes before transmission for
  parking-meter payments, and 4.1.1 explicitly contemplates loss [mqtt5 §6]. No source consulted
  states whether an acknowledgement precedes or follows a durable write, and Mosquitto's
  persistence promises only writes at close, at `autosave_interval` (default 1800 s) or on
  SIGUSR1, with no fsync ordering [mqtt5 §6].

**Where that lands in weida's vocabulary** ([GUARANTEES.md](../GUARANTEES.md) §1, §3). PUBACK and
PUBREC are `Accepted`-shaped: "the next hop has accepted responsibility in memory according to
the selected policy" [GUARANTEES §1] is precisely what "having accepted ownership" means. But
`Accepted` is **reserved for the L2 broker layer and deliberately absent from the v0 wire**
[GUARANTEES §3], [PROTOCOL §11], and the only signal v0 offers is the **transport receipt**,
which "says the bytes arrived, never that an application read them, still less that it acted on
them" [GUARANTEES §3]. So:

| Direction | Where the chain ends | The set the edge carries |
| --- | --- | --- |
| **Inbound (MQTT → weida)** | Responsibility passes to weida when the adapter's weida-side transfer reports its transport receipt [GUARANTEES §1]. The adapter must therefore decide **when to send the PUBACK**: before the weida receipt (and own the message it may then lose) or after it (and hold the MQTT quota open). Either is defensible and the choice is named configuration, never implicit — §9.7 | `core` [0006 §4.2]: delivery `BestEffort`, acknowledgement `TransportReceipt`, ordering `None`, deduplication `None`, backpressure `Block` with `Drop` for fan-out |
| **Outbound (weida → MQTT)** | The weida guarantee chain ends when the broker's PUBACK or PUBCOMP arrives — which is **stronger** than the weida side asked for, and must not be reported back as more than weida can express. A weida `Delivery` the adapter awaits proves the weida hop only [GUARANTEES §3] | `core` on the weida side; on the MQTT side the ownership transfer above, explicitly not `Stored` |
| **Client, no forwarder** | The chain ends at the broker's acknowledgement, and the library reports exactly that: a typed completion carrying the reason code, documented as ownership transfer for one hop | n/a — no weida hop is involved |

**The one thing a forwarder must never do is report `Stored`.** MQTT never requires the
ownership transfer to be durable [mqtt5 §6], so a PUBACK cannot be presented as
`Stored(Written)` or anything above it; that would be "inventing guarantees the source protocol
cannot provide" [INVARIANTS]. It is refused in §9.2.

**QoS 2 is exactly-once strictly per hop, and the chain cannot be extended.** MQTT defines
exactly-once as the two-phase handshake with duplicate suppression by Packet Identifier before
PUBREL ([MQTT-4.3.3-10]), and it is explicitly not end-to-end: publisher-to-broker QoS 2 with
broker-to-subscriber QoS 1 is legal and common, and then the subscriber sees duplicates
[mqtt5 §12/P9]. weida between two such hops has deduplication `None` and "cannot deduplicate
even in principle, because nothing on the wire names a transfer" [SYNTHESIS §7.2],
[GUARANTEES §6]. A forwarder in the middle would have to hold the MQTT delivery unacknowledged
until its far side settled — that is, act as the durable hop — and weida gives it no storage and
no way to defer an acknowledgement, since there is no application acknowledgement on the v0 wire
at all [SYNTHESIS §7.2]. Refused in §9.3.

**Duplicates.** Eight sources, all enumerated by the specification: QoS 1 by construction; QoS 2
downgraded to QoS 1; QoS 1 downgraded to QoS 0, where the server is *explicitly permitted* to
duplicate; overlapping subscriptions on one client; a shared plus a non-shared subscription
matching the same message; two overlapping shared subscriptions; reconnect retransmission; and a
shared-subscription QoS 1 message reassigned after the first member's session terminated
[mqtt5 §7]. Inbound, the adapter's weida side is `Deduplication = None` under `core`
[GUARANTEES §6], which means **the duplicate reaches the weida application**. There is no
deduplication key to supply: "no identifier survives the hop", the Packet Identifier is per hop
per direction per session and reused, and a client can receive the same Application Message twice
with DUP 0 under different identifiers [mqtt5 §3]. DUP is not that key either — it is not
propagated, its outgoing value "MUST be determined solely by whether the outgoing PUBLISH packet
is a retransmission" ([MQTT-3.3.1-3]), and a receiver of DUP 1 "cannot assume that it has seen an
earlier copy of this packet" [mqtt5 §6]. The honest answer is the one MQTT's own ecosystem gives:
carry an application identifier in the payload, in `Correlation Data` or in a `User Property`
[mqtt5 §7]. L13.

**Ordering.** MQTT promises per (publishing client, topic, QoS) on non-shared subscriptions and
nothing wider — "nothing is promised across topics, across publishers, across QoS levels, or
through a shared subscription", and there is no partition or key concept [mqtt5 §7]. weida's
`core` ordering is `None` [GUARANTEES §6]. Inbound, MQTT's narrow promise is **lost**, because
weida has nothing to carry it in; `PerProducer(detect|reassemble)` needs a sequence in the DATA
header [GUARANTEES §3] and MQTT has no sequence to supply [mqtt5 §7]. The one in-protocol lever
MQTT offers for strict ordering is Receive Maximum 1 on both sides, which turns the
specification's own legal receive order 1,2,3,2,3,4 into 1,2,3,3,4 and costs all pipelining
[mqtt5 §7]; the client exposes it as exactly that trade and does not default to it. L17.

## 8. Named losses

Each is a thing the client or a forwarder cannot carry. Phase B1's lesson is that naming a loss
is not enough: the two ZMTP losses the interop bench later corrected were the two nobody had
checked against a real implementation ([adapters/zmtp.md](zmtp.md) §8). So every entry says
**how it would be observed** — the experiment that shows the loss is real — and **what would
show this document wrong**. The bench items of §10 are those experiments.

1. **L1 — MQTT has a session; weida has none, and the client owns the difference.** MQTT session
   state is keyed by Client Identifier, spans consecutive connections and "lasts as long as the
   latest Network Connection plus the Session Expiry Interval" [mqtt5 §2]. weida's position is
   the opposite and is decided rather than accidental: "There is no L0 session state. Nothing is
   retained across a connection… no expiry interval, no session table and no reconnect logic
   enters the runtime" [0008 §4.5], because "a retained session is remote-controlled state that
   the stream core has no owner for" [INVARIANTS]. The client therefore holds the **client half**
   of the session itself — QoS 1 and 2 messages sent but not fully acknowledged, QoS 2 messages
   received but not fully acknowledged [mqtt5 §2] — and that is explicitly permitted: "a client
   library may hold the *client half* of a server-side concept where the protocol defines it for
   a client — an MQTT session with its expiry… and that is not a broker" [0014 §2]. A forwarder
   may not export it.
   *Observed as:* a client that publishes QoS 1 with Clean Start 0 and a non-zero Session Expiry
   Interval, loses its connection before the PUBACK, reconnects, and sees the same PUBLISH go out
   again with the **original Packet Identifier and DUP 1** — while a weida `Subscriber` in the
   same situation re-sends its filters on the next `connect` [PATTERNS §1.8] and has no unsent
   transfer to resume at all. Count the retransmissions on the MQTT wire; there is nothing to
   count on the weida one. *This document is wrong if* a weida reconnect turns out to resume a
   transfer — it does not, and the invariant says so [INVARIANTS].
   *Server half deferred:* the broker's inventory — the session's existence, subscriptions with
   their identifiers, queued messages, the Will and Will Delay, and the session-end time
   [mqtt5 §2].
2. **L2 — a PUBACK and a transport receipt are not the same signal, and the stronger one cannot
   be represented.** PUBACK is ownership transfer by the broker's application [mqtt5 §6]; a weida
   `Delivery` is QUIC's fin-acknowledgement, "although not necessarily the processing of it"
   [GUARANTEES §3]. Inbound, the adapter has a signal weida cannot express (`Accepted` is
   reserved, [GUARANTEES §3]); outbound, it has a weaker one than the MQTT side would like.
   *Observed as:* a weida `Pusher` whose payload fits the peer's stream receive window sees
   `delivered()` resolve **before the peer application has called `recv`** [PATTERNS §1.2],
   whereas a broker's PUBACK cannot be sent before the broker has accepted ownership. The
   experiment is the existing weida test
   `a_receipt_beyond_the_window_implies_the_reader_consumed` read in reverse: below the window,
   the receipt proves nothing about the application. *This document is wrong if* a v0 weida
   acknowledgement appears that an application controls — it will not; that is `Accepted` and it
   is reserved [PROTOCOL §11].
3. **L3 — QoS 2's exactly-once does not survive the hop boundary.** Per hop by construction, and
   the downgrade rule makes an end-to-end claim illegal [mqtt5 §12/P9]. weida has deduplication
   `None` and no transfer name [GUARANTEES §6].
   *Observed as:* publish QoS 2 to the broker, subscribe with maximum QoS 1, and drop the
   subscriber connection between PUBLISH and PUBACK: the message arrives twice, with the broker
   having completed a perfectly correct QoS 2 handshake on the inbound hop. Two deliveries, one
   exactly-once publication — the whole loss in one capture. *This document is wrong if* a broker
   refuses the downgrade; [MQTT-3.8.4-8] requires it, and §10's Mosquitto run measures whether
   `upgrade_outgoing_qos` (default false, and marked contrary to the rule in Mosquitto's own man
   page [mqtt5 §6]) is off as documented.
4. **L4 — retained messages have no weida counterpart, in either direction.** One retained
   message per exact Topic Name, held outside any session and surviving session end [mqtt5 §2]; a
   last-value cache, "not a replay log — no history, no snapshot-plus-delta, no offset"
   [mqtt5 §12/P5]. weida has neither, and the catalogue's own split puts the reason plainly:
   every mechanism that genuinely retains state owns storage [SYNTHESIS §2 D8], and storage is
   the L2 layer's job [ARCHITECTURE §1].
   *Observed as:* an inbound forwarder that subscribes and immediately receives a retained
   message with RETAIN 1 — and a weida `Subscriber` that receives **nothing** on subscribing to a
   topic whose last publication predates it, because a weida `Publisher` stores no copy at all
   [PATTERNS §1.8]. The retained delivery and the live one are distinguishable on the MQTT wire by
   the RETAIN flag and indistinguishable after the hop, since a weida DATA header has no such
   field [PROTOCOL §4]. *This document is wrong if* a weida subscriber can be shown to receive a
   pre-subscription publication.
   *Server half deferred:* the store itself, expiry per Message Expiry Interval, and the rule
   that a zero-byte retained payload deletes and is itself not stored ([MQTT-3.3.1-6],
   [MQTT-3.3.1-7]) [mqtt5 §4.4].
5. **L5 — a shared subscription is a work queue, and weida's fan-out is not one.**
   `$share/{ShareName}/{filter}` is scoped to the server; each matching message reaches exactly
   one member of each group [mqtt5 §2]. A weida `Publisher` copies to every matching subscriber
   [ARCHITECTURE §6b], and "single-delivery-per-group is the L2 credit and queue work of
   0003 §4.2, not topic matching" [0007 §5]. The client
   joins one as a member; it never presents one to weida.
   *Observed as:* two of our clients joined to `$share/g/t/+` and two weida `Subscriber`s on
   `t.*`: publish one message and count deliveries — one on the MQTT side, two on the weida side.
   *This document is wrong if* a weida `Publisher` gains a selection policy that delivers to one
   subscriber; it has none, and adding one is L2 work [0003 §4.2].
   *Server half deferred:* the selection, which MQTT leaves entirely free and which
   implementations answer differently enough to be a portability hazard — EMQX seven strategies
   with `round_robin` default, HiveMQ random and "explicitly not round-robin"
   [mqtt5 §4.2], [mqtt5 §13].
6. **L6 — the Will is a presence signal weida does not have.** An Application Message stored with
   the session and published when the connection closes abnormally, removed by DISCONNECT 0x00
   ([MQTT-3.1.2-10]), optionally delayed by Will Delay Interval and suppressed entirely if a new
   connection to that session arrives first ([MQTT-3.1.3-9]) [mqtt5 §4.5]. weida has no
   equivalent: a closed connection drops its subscriptions and publishes nothing [INVARIANTS].
   *Observed as:* kill a client process without DISCONNECT and watch the Will arrive on its topic
   at the broker; do the same to a weida `Peer` and observe that no third party learns anything —
   the acceptor sees a connection close and has no configured message to emit. Detection latency
   is at best 1.5x Keep Alive [mqtt5 §9]. *This document is wrong if* a weida acceptor can be
   configured to publish on peer loss; it cannot in v0.
   *Server half deferred:* the trigger list, the delay timer, and the takeover interaction — on
   takeover the Will fires or does not depending on Will Delay, Clean Start and the old Session
   Expiry [mqtt5 §9].
7. **L7 — the properties have nowhere to go.** Payload Format Indicator, Content Type, Response
   Topic, Correlation Data, Message Expiry Interval, Subscription Identifier, User Property and
   the rest are a typed, extensible per-message vocabulary the broker MUST forward unaltered and
   in order ([MQTT-3.3.2-4], [MQTT-3.3.2-15] to [MQTT-3.3.2-20]) [mqtt5 §3]. A weida DATA header
   has no such field and gains none: the v0 header is fixed [PROTOCOL §4], [PROTOCOL §11].
   *Observed as:* publish with a User Property and a Content Type, forward it, and read the weida
   side: the payload bytes are identical and every property is gone. A forwarder that wants them
   must define its own payload framing, which makes the weida payload no longer the MQTT payload
   — the choice is named configuration or a refusal (§9.6). *This document is wrong if*
   [PROTOCOL.md](../PROTOCOL.md) gains a user-metadata field; §11 of that document reserves the
   space and does not spend it.
8. **L8 — a Topic Alias is per connection and cannot cross anything.** Two bytes standing in for
   a Topic Name, bounded by the receiver's Topic Alias Maximum (default 0), established by
   sending a non-zero-length Topic Name with the alias; a receiver MUST NOT carry mappings across
   connections ([MQTT-3.3.2-7]) and alias 0 is forbidden ([MQTT-3.3.2-8]) [mqtt5 §2]. It is a
   compression of one direction of one connection, so there is nothing to carry and the loss is
   that a forwarder must **resolve** it rather than pass it on.
   *Observed as:* a broker that declares Topic Alias Maximum 10 (Mosquitto's `max_topic_alias`
   default [mqtt5 §11]) and a client that publishes the same topic 1,000 times: the first packet
   carries the full Topic Name and the rest two bytes, and the weida side sees the full topic
   1,000 times because the alias was resolved at the adapter. Reconnect and watch the table reset.
   *This document is wrong if* a broker is observed honouring an alias established on a previous
   connection — that is a protocol violation and §10 records it against the version.
9. **L9 — request/response through a broker is not an exchange.** weida's exchange is one
   bidirectional stream, and "correlation needs no identifier and no table, because an exchange is
   one stream and a stream cannot be mistaken for another one" [ARCHITECTURE §2]. MQTT's is two
   independent publishes plus `Response Topic` and `Correlation Data`, where the specification's
   own caveats are that there may be zero responders or several, zero or several subscribers to
   the Response Topic *including clients other than the requester*, and that a response published
   before the requester subscribed is dropped [mqtt5 §4.3]. There is no timeout, no retry and no
   reply-once semantics anywhere in it [mqtt5 §9].
   *Observed as:* two responders subscribed to the same request topic produce two replies on one
   Response Topic with identical Correlation Data; a weida `Requester` receives exactly one reply
   on the stream it opened, or a named failure [PATTERNS §1.2]. Counting replies is the whole
   experiment. *This document is wrong if* MQTT gains a reply-once rule; 4.10.1 states the
   opposite.
10. **L10 — the QoS downgrade rule means the delivered QoS is not the published QoS.** "The QoS
    level used to deliver an Application Message outbound to the Client could differ from that of
    the inbound Application Message", and the rule is the minimum of the published QoS and the
    granted maximum ([MQTT-3.8.4-8]) — downgraded, never upgraded [mqtt5 §6]. weida has no QoS
    dimension to downgrade, so an inbound forwarder cannot express "this arrived at QoS 1 having
    been published at QoS 2".
    *Observed as:* subscribe at maximum QoS 0 and publish at QoS 2: the delivery arrives with QoS
    0 and no DUP, and the server "is explicitly permitted to send duplicate copies" when the
    original was QoS 1 and the granted maximum 0 [mqtt5 §6]. The SUBACK reason code (0x00/0x01/
    0x02 = Granted QoS 0/1/2) is the only place the client learns what it will get [mqtt5 §6],
    and the library surfaces it rather than assuming the requested value. *This document is wrong
    if* a broker upgrades — Mosquitto's `upgrade_outgoing_qos` and EMQX's `mqtt.upgrade_qos` do
    exactly that, both default false and both marked non-standard by their own documentation
    [mqtt5 §6]; §10 measures the default.
11. **L11 — streaming does not survive.** One PUBLISH per whole Application Message, no
    fragmentation [mqtt5 §12/P13]; a weida payload may be a stream [INVARIANTS], and streaming is
    "the one dimension where weida and AMQP 1.0 agree and MQTT cannot follow" [SYNTHESIS §7.2].
    The adapter buffers whole messages under a named cap.
    *Observed as:* a weida sender that writes the first half of a payload and then pauses —
    nothing appears on the MQTT socket until FIN, because Remaining Length cannot be written
    before the length is known [mqtt5 §3]. Measurable as first-byte latency equal to
    whole-payload latency, and as a refusal rather than a truncation when the payload passes the
    cap. *This document is wrong if* MQTT gains a continuation packet; it has none.
12. **L12 — concurrency is capped at 65,535 per direction, and the cap is a protocol fact.** A
    Packet Identifier is a two-byte non-zero integer from one unified space per direction per
    session shared across PUBLISH (QoS > 0), SUBSCRIBE and UNSUBSCRIBE ([MQTT-2.2.1-3]), freed on
    PUBACK, PUBCOMP, a PUBREC of 0x80 or above, or SUBACK/UNSUBACK [mqtt5 §2]. weida's concurrency
    bound is `max_concurrent_bidi_streams` and is a transport parameter, not a 16-bit space
    [ARCHITECTURE §6a].
    *Observed as:* exhaust the space with 65,535 unacknowledged QoS 1 publishes and observe the
    client refuse the next one **locally**, before the wire, rather than reusing an identifier.
    In practice `Receive Maximum` bites first — the peer's value, default 65,535 but 10 on
    HiveMQ's `server-receive-maximum` and 16 in Azure's archived preview [mqtt5 §11] — so the
    test asserts which bound triggered. *This document is wrong if* a broker accepts a duplicate
    in-use identifier; the answer is PUBACK/PUBREC 0x91 (Packet identifier in use) [mqtt5 §8].
13. **L13 — there is no message identity, and DUP is not one.** Detailed in §7.
    *Observed as:* the same Application Message received twice with DUP 0 and *different* Packet
    Identifiers, which the specification states outright is possible [mqtt5 §3]. Construct it with
    two overlapping subscriptions on one client, where the server MAY send one copy per matching
    subscription ([MQTT-3.3.4-2]) [mqtt5 §4.1]. *This document is wrong if* DUP turns out to be
    propagated by a broker; [MQTT-3.3.1-3] forbids it and §10 checks it.
14. **L14 — the Client Identifier is not an identity and takeover is ambiguous.** A second CONNECT
    with the same ClientID takes the session over and the incumbent gets DISCONNECT 0x8E with its
    connection closed ([MQTT-3.1.4-3]) [mqtt5 §2]. Whether that was the same application restarting
    or a rogue client reusing the identifier is undecidable in the protocol, and the specification
    files it as an authorization concern [mqtt5 §8]. weida's peer name is proved and cannot be
    claimed [0008 §4.1].
    *Observed as:* connect twice with one ClientID from two processes; the first is evicted with
    0x8E and the second inherits its session and its queued messages. Do the same with two weida
    peers holding different keys and they are two peers, by construction [0008 §4.1] — there is no
    eviction to observe. *This document is wrong if* a broker refuses the second CONNECT; the
    specification requires the takeover.
15. **L15 — an unforwardable oversize message is discarded silently, and the publisher is told it
    succeeded.** Where the server cannot send a message because it exceeds the subscriber's
    Maximum Packet Size it "MUST discard it without sending it and then behave as if it had
    completed sending that Application Message" ([MQTT-3.1.2-25]) [mqtt5 §5]. The publisher's
    PUBACK already reported success, so the failure is invisible on both sides. weida's answer to
    an over-cap payload is a refusal with a code [PROTOCOL §9.4], and its one dropping path counts
    the drop [GUARANTEES §6]. **An honest forwarder does not imitate the silent discard**
    [SYNTHESIS §7.2].
    *Observed as:* a subscriber declaring Maximum Packet Size 256 and a publisher sending 1 KiB at
    QoS 1: the publisher gets PUBACK 0x00 and the subscriber gets nothing, with no reason code
    anywhere in the capture. That absence is the observation. *This document is wrong if* a broker
    sends DISCONNECT 0x95 to the subscriber instead of discarding — permitted for a received
    oversize packet, not for an unsendable one; §10 records which each broker does.
16. **L16 — `$` topics are excluded there and not here.** Reproduced in §6 from [0007 §5]. `#`
    receives nothing under `$` while `$SYS/#` does, and a client wanting both must subscribe to
    both [mqtt5 §4.1]; weida has no reserved prefix, so a weida `#` filter would match a
    `$`-prefixed topic.
    *Observed as:* subscribe to `#` on a broker publishing `$SYS/broker/uptime` and receive
    nothing from it; subscribe to `#` in weida against a publisher whose topic begins with `$`
    and receive it. The two matchers genuinely disagree, and the adapter's exclusion is the fix.
    *This document is wrong if* a broker delivers `$SYS/...` to a bare `#`; [MQTT-4.7.2-1] forbids
    it.
17. **L17 — credit scope and ordering scope are both narrower than they look, and neither maps.**
    Detailed in §4 and §7. MQTT's quota is per connection with no per-subscription or per-topic
    credit [mqtt5 §5]; its ordering is per (client, topic, QoS) on non-shared subscriptions
    [mqtt5 §7]; weida's windows are per stream and per connection [PATTERNS §1.3] and its `core`
    ordering is `None` [GUARANTEES §6].
    *Observed as:* two topics on one MQTT connection, one subscriber slow: the whole connection's
    QoS > 0 publish path stalls at zero quota while PINGREQ and SUBSCRIBE keep flowing
    ([MQTT-4.9.0-3]). The weida mirror is `a_stalled_path_does_not_stall_another_path`
    [GUARANTEES §6] — different paths are independent there, and that is exactly why a forwarder
    needs one connection per session (§3). *This document is wrong if* MQTT is found to have a
    per-subscription credit; it has none.
18. **L18 — the reason-code vocabulary is richer than weida's error vocabulary and is not
    translated.** Every acknowledgement carries a reason code plus an optional human Reason String
    [mqtt5 §1.9], with per-filter verdicts on SUBACK and UNSUBACK and 29 codes on a
    server-initiated DISCONNECT [mqtt5 §1]. weida's `ErrorCode` set is small and closed
    [PROTOCOL §9.4], and a weida SUBSCRIBE has no reply half to carry a per-filter verdict at all
    [0007 §6]. The client surfaces every code as a named error — that is B-141's acceptance — and
    a forwarder cannot propagate them.
    *Observed as:* a SUBSCRIBE with two filters where one is unauthorized: SUBACK carries 0x01 for
    the first and 0x87 for the second, and the connection continues [mqtt5 §8]. Nothing in weida
    can express half a subscription succeeding. *This document is wrong if*
    [PROTOCOL.md](../PROTOCOL.md) gains a subscription reply; §6 of that document keeps SUBSCRIBE
    one-way.
19. **L19 — against a 3.1.1 peer, most of this document is unreachable.** No properties, no
    reason codes beyond CONNACK's six return codes and SUBACK's four, no server-initiated
    DISCONNECT, no AUTH, no Session Expiry, no Message Expiry, no Will Delay, no Topic Alias, no
    Maximum Packet Size, no flow control, no shared subscriptions, no subscription options beyond
    maximum QoS, no request/response, no Server Keep Alive, no Assigned Client Identifier
    [mqtt5 §1.9]. This is not a mapping loss but an interop one, and it decides §10's split into
    two items.
    *Observed as:* the client against `rumqttd` 0.20.0, whose own workspace checklist leaves MQTT
    5 unchecked [mqtt5 §13]. What that broker cannot exercise is named in the test file rather
    than worked around. *This document is wrong if* `rumqttd` 0.20.0 accepts Protocol Version 5
    and honours properties — §10 item 1 is precisely that measurement, and the answer is recorded
    against the version either way.

## 9. Configurations the adapter refuses

Refusal is at configuration time, which is both the guarantee rule [GUARANTEES §4] and the
default at an adapter edge [0006 §4.7]. Degradation exists only as an explicitly named
configuration entry, never as a silent fallback.

1. **A weida side asking for more than `core`.** Delivery above `BestEffort`, acknowledgement
   above `TransportReceipt`, ordering above `None`, deduplication above `None`, or producer naming
   `Stable` — refused when the forwarder is configured, exactly as the existing bridges do
   [0006 §4.7], [0013 §4.5].
2. **Any configuration that reports a PUBACK as `Stored` or above.** §7. MQTT never requires the
   ownership transfer to be durable [mqtt5 §12/P7], so the state does not exist to report.
3. **QoS 2 presented as end-to-end exactly-once.** §7 and [SYNTHESIS §7.2]. A forwarder may
   *speak* QoS 2 on its MQTT hop — that is the client's job — and may not claim the property past
   the hop. Named configuration where an operator wants it anyway: none; this one has no
   degradation entry, because there is nothing weaker to degrade to that would still carry the
   name.
4. **A topic filter longer than 256 bytes, or an MQTT level containing weida's separator or
   wildcard bytes.** §6, [0007 §5]. Refused rather than flattened.
5. **A `$share/...` filter presented to the weida side as a fan-out subscription**, and No Local
   on a shared subscription, which the protocol itself forbids ([MQTT-3.8.3-4]) [mqtt5 §4.2].
6. **A property set the configuration does not say what to do with.** Either the forwarder is
   configured to carry named properties in a payload framing it defines — which makes the weida
   payload not the MQTT payload, stated at the point of configuration — or the properties are
   dropped with the loss named (L7). Silence is not an option.
7. **An unstated PUBACK ordering.** §7's inbound row: whether the MQTT acknowledgement precedes
   or follows the weida transport receipt changes what can be lost, so the configuration must say
   which, and the library has no default that hides the question.
8. **Clean Start 0 with a non-zero Session Expiry Interval on a forwarder that does not own the
   session state.** It must refuse, or document that it maps to Clean Start 1 [SYNTHESIS §7.2].
   The *client* does own that state (L1), so this refusal is the forwarder's alone.
9. **Using a feature the server declared unavailable.** Retain with `Retain Available` 0,
   wildcards with `Wildcard Subscription Available` 0, a Subscription Identifier with
   `Subscription Identifiers Available` 0, `$share/` with `Shared Subscription Available` 0, a QoS
   above `Maximum QoS`, an alias above `Topic Alias Maximum`, or a packet above the peer's
   `Maximum Packet Size`. Each is a Protocol Error with a specific code — 0x9A, 0xA2, 0xA1, 0x9E,
   0x9B, 0x94, 0x95 [mqtt5 §1], [mqtt5 §8] — and **this client's own error before the packet
   reaches the wire**, which is B-141's acceptance. The five availability flags are new in 5.0 and
   replace the 3.1.1 habit of refusing a feature by claiming the client is not authorized
   [mqtt5 §1]; honouring them locally is what makes that replacement worth anything.
10. **Session Expiry Interval non-zero on DISCONNECT when CONNECT carried zero.** A Protocol Error
    ([MQTT-3.14.2-2] and 3.14.2.2.2) [mqtt5 §1], enforced before sending rather than discovered
    from the broker's DISCONNECT.
11. **`Receive Maximum` 0, `Maximum Packet Size` 0, Topic Alias 0, Packet Identifier 0, Retain
    Handling 3, or a Subscription Identifier of 0.** Each is a Protocol Error or Malformed Packet
    in the specification [mqtt5 §11], [mqtt5 §4.4], [mqtt5 §2] and is refused at construction, not
    at encode time — a value that cannot be sent is not a value.
12. **QUIC as the MQTT transport.** §1. Refused with the reason named: not an MQTT 5.0 transport,
    and the one implementation that ships it says so itself [mqtt5 §12/P18].
13. **Protocol Version 3.1.1 or 3.1.** §1. Refused with the reason named: L19's list is what would
    silently stop working.

## 10. Interop bench plan

Two items, because one broker cannot cover the surface and an item that pretended otherwise would
be dishonest [0014 §3 item 3].

1. **`rumqttd` 0.20.0, the 3.1.1-compatible half.** Pure Rust, so it builds where the toolchain
   is, and it is started through the process supervisor with a `ready` condition and stopped in
   the same item on success and on failure alike ([LOOP.md](../LOOP.md) §2). Covers what its own
   workspace checklist marks complete: MQTT 3.1.1, QoS 0/1/2, TLS, retransmission, Will and
   retained messages [mqtt5 §13]. **Our client on both sides of the broker**, so publisher and
   subscriber roles are both ours. What it cannot exercise — properties, reason codes, Session
   Expiry, shared subscriptions, topic aliases, Retain Handling, the availability flags — is named
   in the test file, not worked around (L19).
2. **A 5.0 broker (Mosquitto) for the rest**, under the supervisor and stopped in the same item,
   `#[ignore]` with the install command in the doc comment where it is absent
   ([LOOP.md](../LOOP.md) §2). Every disagreement is recorded as measured against a named version
   rather than inferred, which is what makes the row in `docs/libraries/mqtt.md` a measurement.

Fuzz: one target over the whole packet surface, seeded from §10.1's vectors, asserting that every
input either decodes or is refused with a named error and that a decoded packet re-encodes to the
same bytes. The codec's `[dependencies]` stays empty [0013 §4.3], so the fuzz target lives beside
it exactly as `weida-protocol`'s does.

### 10.1 Golden vectors

Bytes, hexadecimal, most significant first, from the specification's own field order. **CONNECT's
payload follows the normative body's order — Client Identifier, Will Properties, Will Topic, Will
Payload, User Name, Password (3.1.3) — and not Appendix B's**, which is the one place the
specification contradicts itself about this packet and the reason this list exists rather than a
prose description.

| # | Packet | Bytes | What it pins |
| --- | --- | --- | --- |
| 1 | CONNECT, clean start, keep alive 60, ClientID `a`, no properties | `10 0E 00 04 4D 51 54 54 05 02 00 3C 00 00 01 61` | the fixed four-byte protocol name `MQTT` at fixed offset [mqtt5 §0], Protocol Version 5, Connect Flags `0x02` = Clean Start, an empty property set as a single `0x00` ([MQTT-2.2.2-1]) |
| 2 | CONNECT with Will and credentials: ClientID `c`, Will Delay 10, Will Topic `d`, Will Payload `01`, User Name `u`, Password `70`, Will QoS 1 | `10 20 00 04 4D 51 54 54 05 CE 00 3C 00 00 01 63 05 18 00 00 00 0A 00 01 64 00 01 01 00 01 75 00 01 70` | the payload order above; Connect Flags `0xCE` = User Name + Password + Will QoS 1 + Will Flag + Clean Start; Will Properties as a property set *inside* the payload [mqtt5 §3] |
| 3 | CONNACK, Session Present 0, Success, no properties | `20 03 00 00 00` | the acknowledge-flags byte, then the reason code, then the property length |
| 4 | CONNACK, Session Present 1, Success, Receive Maximum 10 | `20 06 01 00 03 21 00 0A` | property `0x21` as a two-byte integer; Session Present 1, which a client with no state MUST answer by closing ([MQTT-3.2.2-4]) |
| 5 | PUBLISH QoS 0, topic `a/b`, payload `hi` | `30 08 00 03 61 2F 62 00 68 69` | no Packet Identifier at QoS 0; the property length is still present |
| 6 | PUBLISH QoS 1, DUP 0, RETAIN 0, topic `a/b`, id 10, payload `hi` | `32 0A 00 03 61 2F 62 00 0A 00 68 69` | the QoS bits at 2-1 and the identifier's position after the Topic Name |
| 7 | PUBLISH QoS 0 using Topic Alias 1 with a zero-length Topic Name | `30 08 00 00 03 23 00 01 68 69` | property `0x23`; a zero-length Topic Name is legal only with an established alias, else DISCONNECT 0x82 [mqtt5 §8] |
| 8 | PUBACK id 10, Success, no properties — short form | `40 02 00 0A` | the Reason Code and Property Length "can be omitted if the Reason Code is 0x00 and there are no Properties" (3.4.1) |
| 9 | PUBACK id 10, Success, explicit reason and empty properties — long form | `40 04 00 0A 00 00` | that both forms decode to the same value, which is the encoder's choice and the decoder's obligation |
| 10 | PUBREC id 10 / PUBREL id 10 / PUBCOMP id 10, short form | `50 02 00 0A` / `62 02 00 0A` / `70 02 00 0A` | PUBREL's reserved flags `0010`, which are not optional |
| 11 | SUBSCRIBE id 1, filter `a/+`, maximum QoS 1, no options | `82 09 00 01 00 00 03 61 2F 2B 01` | SUBSCRIBE's reserved flags `0010`; the per-filter options byte |
| 12 | SUBSCRIBE id 1, Subscription Identifier 5, filter `a/+`, maximum QoS 1 | `82 0B 00 01 02 0B 05 00 03 61 2F 2B 01` | property `0x0B` as a Variable Byte Integer, range 1..268,435,455 [mqtt5 §11] |
| 13 | SUBACK id 1, Granted QoS 1 | `90 04 00 01 00 01` | one reason code per filter, in request order |
| 14 | UNSUBSCRIBE id 2, filter `a/+` | `A2 08 00 02 00 00 03 61 2F 2B` | no options byte, unlike SUBSCRIBE |
| 15 | UNSUBACK id 2, Success | `B0 04 00 02 00 00` | that 5.0 gives UNSUBACK reason codes where 3.1.1 gave none [mqtt5 §1.9] |
| 16 | PINGREQ / PINGRESP | `C0 00` / `D0 00` | no variable header, no properties, no payload |
| 17 | DISCONNECT, Normal disconnection, short form | `E0 00` | the reason code is omissible when 0x00 with no properties (3.14.1); this is the form that discards the Will ([MQTT-3.14.4-3]) |
| 18 | DISCONNECT with Will Message (0x04), Session Expiry 30 | `E0 07 04 05 11 00 00 00 1E` | 0x04 asks for the Will anyway; property `0x11` as a four-byte integer, revising the session's lifetime at close |
| 19 | AUTH, Continue authentication (0x18), method `SCRAM-SHA-1` | `F0 10 18 0E 15 00 0B 53 43 52 41 4D 2D 53 48 41 2D 31` | packet type 15, Reserved/Forbidden in 3.1.1 [mqtt5 §1.9]; property `0x15`; every AUTH and any successful CONNACK MUST repeat the same method ([MQTT-4.12.0-5]) |
| 20 | Malformed: a Variable Byte Integer of `81 00`, or `80 00` | — | a non-minimal encoding MUST be refused ([MQTT-1.5.5-1]). Both decode arithmetically — to 1 and to 0 — and both spend two bytes on a value with a one-byte encoding. The check is exactly "more than one byte and the last byte is `0x00`", because a value whose most significant encoded byte is zero fits in one byte fewer. `80 01` is **not** an example: it is the minimal encoding of 128 |
| 21 | Malformed: Remaining Length `FF FF FF FF 7F` | — | more than four bytes is not a Variable Byte Integer [mqtt5 §3] |
| 22 | Malformed: a repeated Session Expiry Interval property | — | repetition is a Protocol Error for every property except User Property and Subscription Identifier [mqtt5 §3] |
| 23 | Malformed: property identifier `0x7F` on CONNECT | — | an unknown identifier or a wrong type is a Malformed Packet answered with 0x81 [mqtt5 §3] |

### 10.2 The matrices

| Exercise | `rumqttd` 0.20.0 | Mosquitto |
| --- | --- | --- |
| QoS 0, 1, 2 publish and subscribe, both roles ours | yes | yes |
| Retained message, delivery at subscribe, zero-byte delete | yes | yes |
| Will on abortive close; discarded by DISCONNECT 0x00 | yes | yes |
| Session resumption: Clean Start 0, Session Present 1, retransmission with DUP 1 | yes | yes |
| Retransmission happens on reconnect **only** — nothing inside a live connection ([MQTT-4.4.0-1]) | yes | yes |
| Properties end to end (User Property, Content Type, Correlation Data) | no — L19 | yes |
| Reason codes on every acknowledgement; per-filter SUBACK verdicts | no — L19 | yes |
| Session Expiry Interval, and its revision on DISCONNECT | no — L19 | yes |
| Shared subscriptions, two members, one delivery per group | no — L19 | yes |
| Topic aliases, per direction, reset on reconnect | no — L19 | yes |
| Retain Handling 0/1/2 and Retain As Published | no — L19 | yes |
| The five availability flags honoured locally | no — L19 | yes |
| AUTH exchange and re-authentication | no — L19 | measured; Mosquitto's support is what §11 asks about |
| TLS on 8883 | yes | yes |

### 10.3 Numbers to record

In `docs/libraries/mqtt.md` and, where they are performance figures, in
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4, each against a named broker version:

- The declared limits each broker returns in CONNACK: Receive Maximum, Maximum QoS, Maximum
  Packet Size, Topic Alias Maximum, Server Keep Alive, and the five availability flags. Mosquitto
  documents `max_packet_size` 2,000,000 since 2.1, `max_topic_alias` 10, `max_qos` 2 and
  `max_keepalive` 0 [mqtt5 §11]; the run measures whether the defaults are what ships.
- Whether `upgrade_outgoing_qos` is off as documented (L10), and whether a bare `#` receives
  `$SYS/...` (L16).
- Whether DUP is propagated (L13), and what code a duplicate in-use identifier draws (L12).
- What each broker does with a message too large for a subscriber: silent discard or DISCONNECT
  0x95 (L15).
- Whether `rumqttd` 0.20.0 answers Protocol Version 5 with CONNACK 0x84 (Unsupported Protocol
  Version) or closes the socket (§11).
- Round-trip cost of QoS 0 against QoS 1 against QoS 2 on loopback, since EMQX's claim that QoS 2
  "usually has about half the throughput" of QoS 0/1 is a vendor generalisation with no benchmark
  behind it [mqtt5 §6].

## 11. Open questions

- **How `rumqttd` 0.20.0 refuses Protocol Version 5.** CONNACK 0x84 is the specification's
  answer; a 3.1.1-only server may instead send a 3.1.1 return code `0x01` or simply close. The
  sheet records the version as 3.1.1-only [mqtt5 §13] and says nothing about the refusal shape.
  Settled by §10 item 1.
- **Whether Mosquitto implements AUTH at all.** The sheet records that Mosquitto claims "full
  MQTT v5.0 support" while listing features it does not itself use — Server Redirection and
  Reason String are named [mqtt5 §13] — and that NanoMQ and AWS IoT Core explicitly do **not**
  support AUTH [mqtt5 §13]. AUTH is where the sheet records "the failure mode… for two large
  deployments" being a hang rather than a reason code, which is B-147's acceptance. Settled by
  §10 item 2, and if Mosquitto does not, the AUTH path needs a third peer or a stub server named
  as such.
- **Whether a PUBACK ever precedes a durable write anywhere.** "No source consulted states
  whether an acknowledgement precedes or follows a durable write" [mqtt5 §6]. This is not a
  question the client can settle by observation from outside; it would need a broker whose
  documentation says so, and none of the five surveyed does. It stays open, and the consequence is
  already decided: §9.2 refuses to report `Stored`.
- **What "a reasonable amount of time" is for a missing PINGRESP.** Unquantified by the
  specification and by HiveMQ's guide alike [mqtt5 §1]. §3 makes it configuration with a real
  default; whether that default should be 1.5x Keep Alive by symmetry with the server's rule, or
  the Keep Alive interval itself, is a judgement this document does not pretend the sources make
  for it. The measurement that would settle it is what the two interop brokers actually tolerate.
- **Whether weida should gain a per-message metadata field, and if so where.** L7's loss is total
  today. [PROTOCOL.md](../PROTOCOL.md) §11 reserves the space without spending it, and three
  protocols in the catalogue now want it — MQTT properties, AMQP 1.0 application properties, NATS
  headers. This is a weida decision, not an MQTT one, and it belongs in a decision note rather
  than in an adapter.
- **Whether the client should offer Receive Maximum 1 as a named "ordered" mode.** It is the only
  in-protocol lever for strict ordering and it costs all pipelining [mqtt5 §7]. Offering it as a
  named configuration makes a real guarantee reachable; offering it as a default would make the
  library slow for a property most callers do not need. Deferred to B-147's options table, where
  every option is honoured or refused with its reason.

## 12. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §4, §6.2, §6.4, §9.4, §11;
[PATTERNS.md](../PATTERNS.md) §1.2, §1.3, §1.8, §1.9; [GUARANTEES.md](../GUARANTEES.md) §1, §3,
§4, §6; [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §2, §4, §5, §6a, §6b, §6c, §7;
[INVARIANTS.md](../INVARIANTS.md); [LOOP.md](../LOOP.md) §2, §9;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §4.

weida decisions: [0002](../decisions/0002-control-and-bulk-separation.md);
[0003](../decisions/0003-credit-unit.md) §4;
[0006](../decisions/0006-guarantee-sets.md) §4.2, §4.6, §4.7, §4.9;
[0007](../decisions/0007-topic-namespace.md) §4.2, §4.5, §5, §6;
[0008](../decisions/0008-session-identity.md) §4.1, §4.4, §4.5, §4.7;
[0010](../decisions/0010-local-transport.md) §4.4;
[0011](../decisions/0011-answered-where-it-arrived.md) §4.1;
[0013](../decisions/0013-competitor-libraries.md) §4.2, §4.3, §4.4, §4.5, §4.7;
[0014](../decisions/0014-parallel-libraries.md) §2, §3.

Research: [mqtt5.md](../research/mqtt5.md) §0 (identity card, transports), §1 (connection,
negotiation, keep-alive, close, reconnection), §1.9 (what 3.1.1 lacks), §2 (primitives), §3
(framing, size limits, properties, message identity), §4.1-§4.7 (fan-out, shared subscriptions,
request/response, retained, Will, bridging, topic design), §5 (flow control), §6 (delivery
guarantees, the downgrade rule, DUP, retransmission), §7 (ordering and duplicates), §8 (failure
table), §9 (reliability recipes), §10 (security and identity), §11 (limits), §12/P1-P18 (the
problem catalogue), §13 (ecosystem, brokers, known incompatibilities);
[SYNTHESIS.md](../research/SYNTHESIS.md) §2 D1, D2, D3, D6, D7, D8, D10, §4, §7.2.

Sibling mapping documents, for the shape and for the losses this one inherits:
[zmtp.md](zmtp.md) §7, §8; [nng.md](nng.md) §1, §7, §8.
