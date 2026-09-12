# MQTT 5.0 (with 3.1.1 differences)

MQTT 5.0 is the subject. MQTT 3.1.1 appears in section 1.9 and where a difference changes an answer. MQTT-SN is out of scope.
Bracketed numbers are sources in section 14; parenthesised numbers are sections of [1] unless stated otherwise; `[MQTT-x.y-z]` are its
numbered conformance statements. Author inferences are marked `[inference]`.

## 0. Identity card

- **Name.** MQTT. On the wire the protocol name is the four-byte UTF-8 string `MQTT` at fixed offset and length, which "will not be
  changed by future versions" (3.1.2.1) [1].
- **Versions in use.** 5.0 (Protocol Version byte 5), 3.1.1 (byte 4), 3.1 (byte 3). One server MAY serve several on one port and
  discriminates on that byte (3.1.2.2, 3.1.4) [1].
- **Governing body.** OASIS MQTT Technical Committee [1].
- **Specifications.** *MQTT Version 5.0*, OASIS Standard, 07 March 2019, editors Banks, Briggs, Borgendale, Gupta; the `.docx` is
  authoritative and HTML/PDF derived [1]. Predecessor *MQTT Version 3.1.1*, OASIS Standard, 29 October 2014 [2]. Year researched: 2019.
- **ISO status.** 3.1.1 is also ISO/IEC 20922:2016, published 2016-06 and confirmed 2025 [3]; [1] records this in 1.8.1. No ISO/IEC
  publication of 5.0 was found [3] — absence of evidence, not proof of absence.
- **Reference implementation.** The specification names none. Eclipse Paho (clients) and Eclipse Mosquitto (broker) serve as de-facto
  references; Mosquitto claims "full MQTT v5.0 support" while listing features it does not itself use [32].
- **Wire type.** Binary: a one-byte type/flags header plus a Variable Byte Integer Remaining Length, then optional variable header and
  payload (2.1) [1].
- **Transports the specification defines.** Normatively only "an ordered, lossless, stream of bytes" in both directions ([MQTT-4.2-1])
  (4.2) [1]. TCP, TLS and WebSocket are named non-normatively; chapter 6 gives the only normative binding (WebSocket). UDP-style
  transports are called unsuitable on their own (4.2) [1]. IANA: TCP 1883 plain, TCP 8883 TLS (`secure-mqtt`) (4.2, 5.1) [1]. QUIC is
  not an MQTT 5.0 transport; see 12/P18 and 13.

## 1. Connection and session lifecycle

**Handshake.** The client's first packet MUST be CONNECT ([MQTT-3.1.0-1]); a second CONNECT is a Protocol Error and the server MUST
close ([MQTT-3.1.0-2]) (3.1) [1]. The server sends exactly one CONNACK and MUST send CONNACK 0x00 before any packet other than AUTH
([MQTT-3.2.0-1], [MQTT-3.2.0-2]) (3.2) [1]. Clients MAY pipeline after CONNECT; if the server rejects the CONNECT it MUST NOT process
anything after it except AUTH ([MQTT-3.1.4-6]), and an early publisher risks violating limits it has not yet learned (3.1.4) [1].
Missing CONNECT or CONNACK "within a reasonable amount of time" SHOULD cause a close; neither timeout is quantified (3.1.4, 3.2) [1].

**Negotiation** is one exchange of declarative properties, not a round trip; each side states its own limits and there is no
counter-offer. Client in CONNECT (3.1.2.11): Session Expiry Interval, Receive Maximum, Maximum Packet Size, Topic Alias Maximum,
Request Response Information, Request Problem Information, User Property, Authentication Method, Authentication Data. Server in CONNACK
(3.2.2.3): Session Expiry Interval, Receive Maximum, Maximum QoS, Retain Available, Maximum Packet Size, Assigned Client Identifier,
Topic Alias Maximum, Reason String, User Property, Wildcard Subscription Available, Subscription Identifiers Available, Shared
Subscription Available, Server Keep Alive, Response Information, Server Reference, Authentication Method, Authentication Data [1]. The
five availability flags are new in 5.0 and replace the 3.1.1 habit of refusing a feature by claiming the client is not authorized
(Appendix C) [1]; using an unavailable feature is a Protocol Error with a specific code (0x9B, 0x9A, 0xA2, 0xA1, 0x9E) (4.13.1) [1].
Server Keep Alive is the only property that overrides the client: present, the client MUST use it ([MQTT-3.2.2-21]); absent, the server
MUST use the client's ([MQTT-3.2.2-22]) (3.2.2.3.14) [1].

**Authentication step.** Basic: `User Name` (UTF-8) and `Password` (binary) in the CONNECT payload; 5.0 permits a Password with no User
Name, which 3.1.1 forbade (3.1.2.9) [1]. Enhanced: `Authentication Method` in CONNECT starts an AUTH challenge/response exchange
(4.12) [1]; a client that sets it MUST send nothing but AUTH or DISCONNECT until CONNACK ([MQTT-3.1.2-30]).

**Keep-alive.** Two-byte second count, max 65,535 (18h 12m 15s), 0 disables (3.1.2.10) [1]. It bounds the gap from finishing one client
packet to starting the next; absent other traffic the client MUST send PINGREQ ([MQTT-3.1.2-20]). With non-zero Keep Alive, a server
receiving nothing for **1.5 x** Keep Alive MUST close "as if the network had failed" ([MQTT-3.1.2-22]) — so the Will fires — and SHOULD
signal DISCONNECT 0x8D (Keep Alive timeout) (3.14.2.1) [1]. The reverse is weaker: a client seeing no PINGRESP "within a reasonable
amount of time" SHOULD close, with no number given; HiveMQ's guide repeats the same non-quantified advice [9]. There is no
server-initiated ping; PINGREQ is client-to-server only (3.12) [1].

**Idle behaviour.** Idling is legal while Keep Alive is honoured. The server MAY disconnect for its own reasons — shutdown, inactivity,
administrative action, connect-time limit — via DISCONNECT 0x8B, 0x98, 0xA0 (3.1.2.10, 3.14.2.1) [1].

**Orderly close.** Client sends DISCONNECT 0x00; the server MUST then discard the stored Will without publishing it
([MQTT-3.14.4-3]). After sending DISCONNECT the sender MUST send nothing more and MUST close ([MQTT-3.14.4-1], [MQTT-3.14.4-2]); the
receiver SHOULD close (3.14.4) [1]. Two variants matter: DISCONNECT 0x04 (Disconnect with Will Message) asks the server to publish the
Will anyway, and the client MAY set a new Session Expiry Interval on DISCONNECT, so a session's lifetime can be shortened or extended at
close. A non-zero value when CONNECT carried zero is a Protocol Error, and the *server* MUST NOT send Session Expiry Interval on
DISCONNECT ([MQTT-3.14.2-2]) (3.14.2.1, 3.14.2.2.2) [1].

**Abortive close.** New in 5.0, DISCONNECT also flows server-to-client with one of 29 reason codes (3.14.2.1) [1]. A server MUST NOT
send DISCONNECT before a CONNACK with code < 0x80 ([MQTT-3.14.0-1]); earlier failures go on CONNACK. Where a code >= 0x80 applies the
connection MUST be closed whether or not the DISCONNECT was sent ([MQTT-4.13.2-1]) (4.13.2) [1]. A server detecting a Malformed Packet
or Protocol Error MUST close ([MQTT-4.13.1-1]); a client only SHOULD (4.13.1) [1]. Dropping the socket is always available and triggers
the Will.

**Reconnection rules the protocol defines.**

- Session identity is the Client Identifier. Clean Start 1 discards any existing session ([MQTT-3.1.2-4]); Clean Start 0 resumes one if
  it exists, else creates one ([MQTT-3.1.2-5], [MQTT-3.1.2-6]) (3.1.2.4) [1].
- `Session Present` in CONNACK reports which happened. A client with no state that receives 1 MUST close ([MQTT-3.2.2-4]); a client with
  state that receives 0 MUST discard it ([MQTT-3.2.2-5]) (3.2.2.1.1) [1].
- Retransmission on reconnect is the *only* required retransmission: with Clean Start 0 and a session present, both sides MUST resend
  unacknowledged PUBLISH (QoS > 0) and PUBREL packets with their original Packet Identifiers, and "MUST NOT resend messages at any other
  time" ([MQTT-4.4.0-1]) (4.4) [1]. No timer-driven retransmission exists inside a live connection. HiveMQ's 2015 QoS guide claims the
  opposite (retransmit after "a reasonable time frame") [5]; HiveMQ's 2024 correction quotes the 5.0 rule and withdraws it [14].
- Subscriptions survive a resumed session because they are server-side session state (4.1) [1]; they do not survive Clean Start 1.
  Clients commonly resubscribe on every connect anyway — Paho's Python and C guidance puts SUBSCRIBE in the connect callback [37][34].
- No reconnect policy. Backoff, jitter and retry limits are absent; DISCONNECT reason codes are offered as input to the client's choice
  "whether to retry the connection, and how long it should wait" (3.14.2.1, non-normative) [1]. Implementations fill the gap: Paho C
  1..60 s doubling, off unless `automaticReconnect` is set [34]; Paho Python `reconnect_on_failure=True`, 1..120 s doubling [37]; Paho
  Java 1 s doubling to 2 minutes [39]; Mosquitto bridges decorrelated jitter, base 5 s cap 30 s [31].
- Takeover: if the ClientID is already connected, the server sends DISCONNECT 0x8E (Session taken over) to the incumbent and MUST close
  its connection ([MQTT-3.1.4-3]) (3.1.4) [1].

### 1.9 What 3.1.1 lacks

All present in 5.0, absent from 3.1.1 ([1] Appendix C, [2]):

- **Properties.** No property mechanism at all: no User Property, Content Type, Payload Format Indicator, Response Topic, Correlation
  Data, Subscription Identifier, Topic Alias.
- **Reason codes.** CONNACK carries a six-value *return code* (0x00 Accepted, 0x01 unacceptable protocol version, 0x02 identifier
  rejected, 0x03 Server unavailable, 0x04 bad user name or password, 0x05 not authorized) (3.2.2.3 of [2]). PUBACK, PUBREC, PUBREL,
  PUBCOMP and UNSUBACK carry no status at all; SUBACK has only 0x00/0x01/0x02 and 0x80 Failure (3.9.3 of [2]). 5.0 puts a reason code on
  every acknowledgement plus an optional human Reason String.
- **Server-initiated DISCONNECT.** 3.1.1 has 14 packet types and DISCONNECT is client-to-server only (2.2.1 of [2]). A 3.1.1 server
  reports errors by closing: "if either the Server or Client encounters a protocol violation, it MUST close the Network Connection"
  (4.8 of [2]). The client must guess why.
- **AUTH packet.** Type 15 is Reserved/Forbidden (2.2.1 of [2]): no enhanced or mutual authentication above TLS, no re-authentication.
- **Session Expiry.** Only the binary `CleanSession` flag; with CleanSession 0 the session lasts indefinitely and the decision cannot be
  revised at disconnect (3.1.2.4 of [2]). 5.0 splits it into Clean Start plus Session Expiry Interval, where Clean Start 1 + expiry 0 is
  exactly CleanSession 1 (3.1.2.11.2) [1]. Brokers papered over the gap non-standardly: Mosquitto `persistent_client_expiration`, marked
  "not standard" for 3.1 [31]; EMQX `mqtt.session_expiry_interval` default `2h` applied to 3.1/3.1.1 sessions [18].
- **Message Expiry Interval**, **Will Delay Interval** (in 3.1.1 the Will fires whenever the connection closes, 3.1.2.5 of [2], so every
  network blip produces one), **Topic Alias**, **Maximum Packet Size** (only the 268,435,455-byte Remaining Length bound, 2.2.3 of [2]),
  and **flow control** (no Receive Maximum; in-flight limits exist only as configuration — Mosquitto `max_inflight_messages` 20 [31],
  EMQX `mqtt.max_inflight` 32 [18]).
- **Shared subscriptions.** "In earlier versions of MQTT all Subscriptions are Non-shared" (4.8) [1]: no load balancing.
- **Subscription options** beyond maximum QoS — no No Local, Retain As Published, Retain Handling or Subscription Identifier; the 3.1.1
  SUBSCRIBE payload byte is QoS only (3.8.3 of [2]). Bridging therefore cannot be expressed in the protocol and needs broker-specific
  loop suppression such as Mosquitto's `try_private` [31].
- **Request/response**, **Server Keep Alive** (Mosquitto instead rejects a 3.1.1 client above `max_keepalive` with an
  identifier-rejected CONNACK [31]), **Server Reference**, and the **feature-availability flags**.
- **Assigned Client Identifier.** A 3.1.1 client with a zero-length ClientID MUST also set CleanSession 1 and is never told what
  identifier it received ([MQTT-3.1.3-7], [MQTT-3.1.3-8] of [2]); 5.0 returns Assigned Client Identifier and drops the restriction
  (3.1.3.1) [1].

3.1.1 keeps: three QoS levels, the same QoS 1/2 flows, retained messages, Will messages, wildcards, `$`-topic exclusion, Session
Present, the same ordering rules, and the same retransmission-only-on-reconnect rule ([MQTT-4.4.0-1] of [2]).

## 2. Primitives

**Network Connection.** One transport connection per client; one CONNECT and one CONNACK each. Lifetime: transport open to close. Holds
Topic Alias mappings per direction, the send quota, and the negotiated limits — none of which is session state (3.3.2.3.4, 4.9) [1].

**Session.** Stateful client/server interaction keyed by Client Identifier, spanning zero or more consecutive connections; "it lasts as
long as the latest Network Connection plus the Session Expiry Interval" (4.1) [1]. At most one live session per ClientID per server: a
second CONNECT with the same ClientID takes it over and evicts the incumbent ([MQTT-3.1.4-3]). State (4.1) [1]:

- *Client:* QoS 1 and QoS 2 messages sent but not fully acknowledged; QoS 2 messages received but not fully acknowledged.
- *Server:* the existence of the session even when otherwise empty; the client's subscriptions including Subscription Identifiers; QoS
  1/2 messages sent but not fully acknowledged; QoS 1/2 messages pending transmission and OPTIONALLY QoS 0 messages pending
  transmission; QoS 2 messages received but not fully acknowledged; the Will Message and Will Delay Interval; and, while disconnected,
  the time at which the session will end.

Neither side may discard session state while the connection is open ([MQTT-4.1.0-1]); the server MUST discard it once closed and the
expiry interval has passed ([MQTT-4.1.0-2]) (4.1.1) [1]. Retained messages are explicitly not session state and survive session end
(4.1) [1]. HiveMQ's and EMQ's guides restate the same server-side inventory, HiveMQ stressing that new QoS 1/2 messages missed while
offline are part of it [6] and EMQ that the Will Message and Will Delay Interval are [27].

**Client Identifier.** Mandatory UTF-8 string, first field of the CONNECT payload. Servers MUST accept 1..23 bytes of `[0-9a-zA-Z]` and
MAY accept more ([MQTT-3.1.3-5]). Zero length makes the server assign one and return it as Assigned Client Identifier
([MQTT-3.1.3-6], [MQTT-3.2.2-16]) (3.1.3.1) [1]. It is the key to session state, so authorization must cover the right to use a given
ClientID or one client can take another's session (5.4.2) [1].

**Subscription.** A Topic Filter plus Subscription Options (maximum QoS, No Local, Retain As Published, Retain Handling) and an optional
Subscription Identifier. A non-shared subscription belongs to exactly one session, and a session cannot hold two with the same filter,
so the filter is the key (4.8.1) [1]. Re-subscribing the same filter MUST replace the subscription without losing messages
([MQTT-3.8.4-3], [MQTT-3.8.4-4]) (3.8.4) [1]. Lifetime: until UNSUBSCRIBE, session end, or replacement.

**Shared Subscription.** Scoped to the *server*, not a session; identified by `$share/{ShareName}/{filter}` ([MQTT-4.8.2-1],
[MQTT-4.8.2-2]). Multiple sessions attach; each matching message reaches exactly one. It survives its creator unsubscribing and ends —
deleting undelivered messages — when the last session detaches (4.8.2) [1]. A session holding both a shared and a non-shared
subscription that match will receive two copies (4.8.2) [1].

**Application Message.** Payload bytes, a QoS, a property collection, and a Topic Name (1.2) [1].

**Packet Identifier.** Two-byte non-zero integer. Client and server each keep their own single unified space per session, shared across
PUBLISH (QoS > 0), SUBSCRIBE and UNSUBSCRIBE ([MQTT-2.2.1-3], [MQTT-2.2.1-4]); the spaces are independent, so both peers may have 0x1234
in flight at once (2.2.1) [1]. An identifier frees on PUBACK, PUBCOMP, a PUBREC with code >= 0x80, or SUBACK/UNSUBACK — capping
concurrent outstanding operations at 65,535 per direction per session.

**Topic Alias.** Two-byte integer standing in for a Topic Name, bounded by the receiver's Topic Alias Maximum, established by sending a
non-zero-length Topic Name with the alias. Per direction and per connection; a receiver MUST NOT carry mappings across connections
([MQTT-3.3.2-7]) and alias 0 is forbidden ([MQTT-3.3.2-8]) (3.3.2.3.4) [1].

**Retained message.** At most one per exact Topic Name, held outside any session (3.3.1.3, 4.1) [1].

**Ownership/threading.** The specification says nothing about threads or object ownership; it constrains only the byte stream. Ordering
obligations (4.6) are per connection and per topic, so an implementation fanning one connection across threads must reimpose that order
itself [inference].

## 3. Message model

**Framing.** Fixed header: one byte of packet type (bits 7-4) plus type-specific flags (bits 3-0), then Remaining Length as a Variable
Byte Integer of 1..4 bytes (2.1) [1]. Remaining Length covers variable header plus payload and excludes itself; "the packet size is the
total number of bytes in an MQTT Control Packet" = fixed header + Remaining Length (2.1.4) [1]. Variable Byte Integers MUST use the
minimum number of bytes ([MQTT-1.5.5-1]), so encodings are canonical.

**Size limits.** Remaining Length maxes at 268,435,455 bytes (0xFF 0xFF 0xFF 0x7F), just under 256 MiB, and that is the protocol ceiling
for a packet (1.5.5, 2.1.4) [1]. UTF-8 Encoded Strings and Binary Data are two-byte-length-prefixed, hence capped at 65,535 bytes each
(1.5.4, 1.5.6) [1]; Topic Names and Filters MUST NOT encode to more than 65,535 bytes ([MQTT-4.7.3-3]). Either peer may declare a lower
`Maximum Packet Size`; zero is a Protocol Error, absence means no limit beyond the encoding (3.1.2.11.4, 3.2.2.3.6) [1].

**Single-part.** No multipart, fragmented or streaming message: one PUBLISH carries one complete Application Message. Over WebSocket a
single data frame may contain multiple or partial control packets and receivers MUST NOT assume packet/frame alignment
([MQTT-6.0.0-2]) — transport framing, not message segmentation (6) [1].

**Properties.** CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT and
AUTH each end their variable header with a Variable Byte Integer Property Length (zero if none, [MQTT-2.2.2-1]) followed by
identifier/value pairs; CONNECT also carries a Will Properties set inside its payload (2.2.2) [1]. Identifiers are Variable Byte
Integers, though all 27 defined ones are single-byte (2.2.2.2) [1]. Order between different identifiers is insignificant; an unknown
identifier or wrong type is a Malformed Packet answered with 0x81. Repetition is a Protocol Error for every property except `User
Property` and `Subscription Identifier` (2.2.2, 3.1.2.11.8, 3.3.2.3.8) [1]. The server MUST forward Payload Format Indicator, Response
Topic, Correlation Data, Content Type and all User Properties in order, unaltered ([MQTT-3.3.2-4], [MQTT-3.3.2-15] to [MQTT-3.3.2-20])
[1]; it rewrites Message Expiry Interval downwards for waiting time ([MQTT-3.3.2-6]) and inserts Subscription Identifiers, which a
client-to-server PUBLISH MUST NOT contain ([MQTT-3.3.4-6]) [1].

**Payload typing.** Opaque bytes; a zero-length payload is valid (3.3.3) [1]. Two optional properties describe without constraining:
`Payload Format Indicator` (0 unspecified bytes, 1 UTF-8 character data) and `Content Type` (arbitrary UTF-8, MIME by convention, with
"MQTT performs no validation of the string" beyond UTF-8 well-formedness) (3.3.2.3.2, 3.3.2.3.9) [1]. A receiver MAY validate and reject
with 0x99 (Payload format invalid) — or MAY not, and asymmetric validation between broker and subscriber lets a publisher knock a
subscriber off the network (5.4.9.2) [1].

**Message identity.** No identifier survives the hop. The Packet Identifier is per hop, per direction, per session and is reused, so
"it is possible for a Client to receive a PUBLISH packet with DUP flag set to 0 that contains a repetition of an Application Message
that it received earlier, but with a different Packet Identifier" (3.3.1.1) [1]. Application identity must ride in the payload,
`Correlation Data` or a `User Property`; EMQX's own guide advises sequence numbers where order matters [26].

**What the protocol interprets.** Topic Name (matched character for character with no normalization, [MQTT-4.7.3-4]), RETAIN, QoS, DUP,
Packet Identifier, and the properties of table 2-4. Payload, Correlation Data, User Property values and Content Type strings are opaque
(2.2.2, 3.3.2) [1].

## 4. Patterns and topologies

MQTT has one topology: the server binds and accepts, the client connects (1.2) [1]. No peer-to-peer mode, no listening client, no discovery. The patterns below are those the specification and the maintainer guides build on that base.

### 4.1 Fan-out via topic filters

A publisher PUBLISHes to a Topic Name; the server copies it to every session whose Topic Filter matches (4.5, 4.8.1) [1]. `/` separates levels, `+` matches exactly one level, `#` matches the parent level and any number of child levels. `#` must be last and alone in its level ([MQTT-4.7.1-1]); `+` must occupy a whole level ([MQTT-4.7.1-2]). The spec's worked examples (4.7.1) [1]: `sport/tennis/player1/#` matches `sport/tennis/player1` itself as well as `.../ranking` and `.../score/wimbledon`; `sport/#` matches bare `sport`; `#` receives everything; `sport/tennis/+` matches `sport/tennis/player1` but not `sport/tennis/player1/ranking`; `sport/+` does not match `sport` but does match `sport/`; `/finance` matches `+/+` and `/+` but not `+`; `sport/tennis#` and `sport/tennis/#/ranking` are invalid. Topics are case sensitive, may contain spaces, and a leading or trailing `/` makes a distinct topic (4.7.3) [1].

Every non-shared subscriber gets its own copy, so "Non-shared Subscriptions cannot be used to load-balance Application Messages across multiple consuming Clients" (4.8.1) [1]. When one client's own subscriptions overlap, the server MUST deliver respecting the maximum QoS of all matching subscriptions and MAY additionally send one copy per matching subscription ([MQTT-3.3.4-2]) — overlapping wildcards are a duplication source the protocol permits rather than resolves (3.3.4) [1]. Subscription Identifiers exist so the client can tell which subscription caused a delivery: a single copy carries all matching identifiers ([MQTT-3.3.4-4]), multiple copies one each ([MQTT-3.3.4-5]) (3.3.4) [1].

**`$` topics.** A server MUST NOT match a filter beginning with a wildcard against a Topic Name beginning with `$` ([MQTT-4.7.2-1]). Non-normatively: "`$SYS/` has been widely adopted as a prefix to topics that contain Server-specific information or control APIs"; applications cannot use `$` for their own purposes; `#` receives nothing under `$` while `$SYS/#` does; a client wanting both must subscribe to `#` *and* `$SYS/#` (4.7.2) [1]. Content is implementation-defined: Mosquitto publishes byte/message counters, client and heap gauges, retained/store/subscription counts and bridge state, static topics once on subscribe and the rest every `sys_interval` (default 10 s, 0 disables) [32][31]; EMQX publishes under `$SYS/brokers/{node}/...` and by default allows `$SYS` subscriptions only from localhost [30]. HiveMQ notes `$SYS` "has no official standardization" and should not be published to [4].

**Nobody to talk to.** A PUBLISH with no matching subscriber is dropped. QoS 0 gives no signal. At QoS 1/2 the server MAY report 0x10 (No matching subscribers) in PUBACK/PUBREC — a success code, not an error (3.4.2.1, 3.5.2.1) [1]. That optional code is the only in-protocol "delivered to nobody" signal.

### 4.2 Shared subscriptions as work queues

Filter `$share/{ShareName}/{filter}`; ShareName is at least one character and contains none of `/`, `+`, `#` ([MQTT-4.8.2-1], [MQTT-4.8.2-2]), and neither `$share` nor the ShareName is considered when matching publications (4.8.2) [1]. Shared subscriptions are server-scoped, so `$share/consumer1/sport/tennis/+` and `$share/consumer2/sport/tennis/+` are independent groups: one message produces one copy for a member of the first, one for a member of the second, and a further copy for every non-shared subscriber to `sport/tennis/+` (4.8.2, worked example) [1]. Rules the spec fixes (4.8.2) [1]:

- No retained messages are ever sent to a shared subscription, neither on creation nor on later joins.
- Selection is unconstrained: "the Server implementation is free to choose, on a message by message basis, which Session to use and what criteria it uses to make this selection."
- Members may hold different granted QoS and the server MUST respect each member's ([MQTT-4.8.2-3]). No Local MUST NOT be set on a shared subscription ([MQTT-3.8.3-4]).
- A second SUBSCRIBE to a shared subscription already held does not increase the attachment count; one UNSUBSCRIBE detaches.
- Recovery differs by QoS. For a QoS 2 message in flight to a member whose connection breaks, the server MUST complete delivery to *that* client on reconnect ([MQTT-4.8.2-4]), and if the session terminates first MUST NOT give it to anybody else ([MQTT-4.8.2-5]) — the message is lost. For QoS 1 the server MAY wait for the same client, SHOULD hand it to another member if the session terminates, and MAY reassign as soon as the connection drops. On a negative acknowledgement (code >= 0x80) the server MUST discard the message and MUST NOT try another subscriber ([MQTT-4.8.2-6]).

Implementations diverge on selection. EMQX offers `mqtt.shared_subscription_strategy` with `random`, `round_robin` (default), `round_robin_per_group`, `sticky`, `hash_clientid`, `hash_topic`, `local` [20]. HiveMQ's current documentation says distribution is random and "a round-robin algorithm is not used", so faster consumers get more — contradicting HiveMQ's own 2019 article [16][11]. EMQX also accepts a non-standard `$queue/<topic>` group-less form and now deprecates it [20]; no `$queue` form appears in HiveMQ's documentation [16]. Mosquitto supports shared subscriptions and counts them at `$SYS/broker/shared_subscriptions/count` [32].

### 4.3 Request/response

Formalised in 5.0 (4.10) [1]. The requester publishes a Request Message carrying `Response Topic`, optionally with `Correlation Data`; a responder subscribed to the request topic acts and publishes to the Response Topic, copying the Correlation Data across; the response carries no Response Topic. The server forwards both properties unaltered and otherwise treats these as ordinary messages ([MQTT-3.3.2-15], [MQTT-3.3.2-16]) [1]. The guide's own caveats (4.10.1) [1]: there may be zero responders or several; there may be zero or several subscribers to the Response Topic, so a response can reach a client that is not the requester; the requester "normally subscribes to the Response Topic before publishing", and a response sent with nothing subscribed is delivered to nobody; any QoS may be used and "it is common to send Request Messages at QoS 0 and only when the Responder is expected to be connected"; a responder pool may use a shared subscription, "note however that when using Shared Subscriptions that the order of message delivery is not guaranteed between multiple Clients". Authorization is the applications' problem in both directions. HiveMQ restates the same shape and the same subscribe-first advice [12].

Because a self-chosen Response Topic is usually not authorized, 5.0 adds `Request Response Information` in CONNECT and `Response Information` in CONNACK: the server hands the client a string to build its Response Topic from, typically "a globally unique portion of the topic tree which is reserved for this Client for at least the lifetime of its Session" (3.2.2.3.15, 4.10.2) [1]. The specification deliberately defines neither the string's content nor how to derive a topic from it, and the server MAY decline even when asked (3.1.2.11.6) [1].

### 4.4 Retained messages as last-value cache

RETAIN = 1 replaces any existing retained message for that exact topic ([MQTT-3.3.1-5]); a zero-byte payload deletes it and is itself not stored ([MQTT-3.3.1-6], [MQTT-3.3.1-7]); RETAIN = 0 neither stores nor removes anything ([MQTT-3.3.1-8]) (3.3.1.3) [1]. On a new non-shared subscription the last retained message on each matching topic is sent with RETAIN = 1, subject to Retain Handling [1]. HiveMQ frames this as "a snapshot of the last-known state" and stresses one retained message per topic, that it is the most recent *retained* publication rather than the most recent publication, and that retained state is independent of persistent sessions [7]; Mosquitto calls it a "last known good" mechanism [33]. Two 5.0 subscription options make it controllable (3.8.3.1) [1]:

- **Retain Handling**: 0 send retained at subscribe; 1 send only if the subscription did not already exist; 2 never send at subscribe. Value 3 is a Protocol Error. The spec's rationale: 1 is "useful when a reconnect is done and the Client is not certain whether the subscriptions were completed in the previous connection", 2 "where a Client wishes to receive change notifications and does not need to know the initial state".
- **Retain As Published**: 0 the server clears RETAIN when forwarding ([MQTT-3.3.1-12]); 1 it forwards the flag as published ([MQTT-3.3.1-13]).

QoS 0 retained messages are weaker: the server SHOULD store one but "MAY choose to discard it at any time" (3.3.1.3) [1]. Retained messages expire per Message Expiry Interval (3.3.1.3) [1]; Mosquitto only scans for expired entries when `retain_expiry_interval` is set, otherwise noticing expiry lazily on access, replacement or a matching subscription [31].

EMQX's documentation restates the same three rules — only the latest retained message per topic is kept, a new matching subscription receives it immediately, and publishing an empty retained payload clears it — and adds that by default retained messages never expire unless deleted or configured to expire [28].

### 4.5 Presence via Last Will and Testament

The Will is an Application Message stored with the session at CONNECT (Will Properties, Will Topic, Will Payload, plus Will QoS and Will Retain in the Connect Flags) and published when the connection closes abnormally ([MQTT-3.1.2-7], [MQTT-3.1.2-8]) (3.1.2.5) [1]. The spec's trigger list: server-detected I/O or network failure; Keep Alive expiry; the client closing without DISCONNECT 0x00; the server closing without having received DISCONNECT 0x00. It is removed once published or once DISCONNECT 0x00 arrives ([MQTT-3.1.2-10]). `Will Delay Interval` (5.0) delays publication until the interval elapses or the session ends, whichever is first, and the server MUST NOT publish it if a new connection to that session arrives first ([MQTT-3.1.3-9]) (3.1.3.2.2) [1] — its stated purpose is "to avoid publishing Will Messages if there is a temporary network disconnection and the Client succeeds in reconnecting". Setting Will Delay longer than Session Expiry turns the Will into a session-expiry notification (3.1.2.11.2) [1]. HiveMQ's presence recipe: Will retained, payload `Offline`, topic `client1/status`, plus a retained `Online` published after connecting [8] — the retained message gives late joiners current liveness, the Will gives live transitions.

EMQX's documentation restates the mechanism from the broker side: the configured Will Message is published to matching subscribers on an accidental disconnect, and a retained Will additionally becomes the retained message for its topic [29].

### 4.6 Bridging between brokers

The protocol supplies two subscription options "primarily defined to allow for message bridge applications" (Appendix C) [1]: **No Local** (messages MUST NOT be forwarded to a connection whose ClientID equals the publishing connection's, [MQTT-3.8.3-3]) and **Retain As Published**. Together they let a bridge client republish onward without immediately receiving its own traffic back and without flattening retained state (3.8.3.1) [1]. Everything else about bridging is outside the specification.

Mosquitto's bridge is the semantics most often cited [31]. A `connection <name>` block plus `address <host[:port]>` (port defaults 1883) defines a bridge; topics are mapped with `topic <pattern> [[[out|in|both] qos-level] local-prefix remote-prefix]`, direction defaulting to `out` and QoS to 0. For `in` the bridge subscribes remotely to `remote-prefix + pattern`, strips the remote prefix on delivery and prepends the local one; for `out` it subscribes locally to `local-prefix + pattern`, strips the local prefix and prepends the remote one. `""` marks an empty prefix. `bridge_protocol_version` selects `mqttv50`, `mqttv311` (default) or `mqttv31`. Client identifiers default to `<connection-name>.<hostname>` remotely and `local.<remote_clientid>` locally, which is why a self-bridge needs explicit distinct identifiers. `try_private` (default true) asks the remote broker to treat the connection as a bridge, which makes loop detection more effective and propagates retained messages correctly; topic mappings can still be configured into a loop and the man page puts that on the operator. `bridge_attempt_unsubscribe` (default true) sends a remote UNSUBSCRIBE when a topic flips from `in` to `out`. `notifications` (default true) publishes retained `1`/`0` at `$SYS/broker/connection/<remote_clientid>/state` using the bridge's own Will. `start_type` is `automatic` (default, restarts after failure), `lazy` (starts above `threshold`, default 10 queued; stops after `idle_timeout`, default 60 s) or `once` (never restarts). `restart_timeout` defaults to decorrelated jitter base 5 s cap 30 s; `round_robin` (default false) prefers the first address and periodically returns to it. Note `cleansession` defaults to false on a bridge, so remote subscriptions survive a dropped link; setting it true can cause retained messages to be re-sent on every reconnect.

### 4.7 Topic design guidance

The spec's own guidance is thin: topics may be predefined by an administrator or created dynamically on first subscribe or first publish, and a server MAY authorize per topic (4.7.3) [1]; there is no limit on level count beyond the string bound and the server performs no normalization ([MQTT-4.7.3-4]). HiveMQ's named best practices, a vendor guide rather than the spec [4]: "Have at least one character"; "MQTT Topics are Case sensitive"; "Avoid Leading Forward Slash"; "Never use spaces in an MQTT Topic"; "Keep MQTT topics short and concise"; "Use only ASCII characters, and avoid non-printable characters"; "Embed a unique identifier or the Client Id in topics"; "Avoid Subscribing to Wildcards (#)"; "Use specific topics, not general ones". The embedded-identifier rule is the one with protocol teeth: it is what makes per-client authorization and No Local usable.

## 5. Flow control and backpressure

**Unit of credit.** One QoS 1 or QoS 2 PUBLISH packet — not bytes, not QoS 0, not any other packet type (4.9) [1].

**Grantor.** The receiver, by declaring `Receive Maximum` (client in CONNECT, server in CONNACK). The directions are independent. The
value "applies only to the current Network Connection"; quota and Receive Maximum are re-initialised each connection and are explicitly
not session state (3.1.2.11.3, 3.2.2.3.3, 4.9) [1].

**The two values are two numbers, and an implementation that holds one field for both loses a bound.** The directions being independent
means a client's own `Receive Maximum` from CONNECT and the server's from CONNACK are separate quantities that happen to share a name
eleven pages apart; the first bounds what may arrive, the second is the sender's initial send quota by [MQTT-4.9.0-1]. Because absence
means 65,535, a client that stored both in one field would silently raise its own inbound ceiling to 65,535 whenever a CONNACK merely
omitted the property, and would then never earn the DISCONNECT 0x93 below for a server that exceeded what it had declared. Measured
against a broker declaring 8 while the client declared 200: the send quota is 8 and the inbound ceiling stays 200 [66]. Section 13
records this as a tension of the specification's own prose rather than of any implementation.

**Default.** 65,535 when absent; 0 is a Protocol Error [1]. HiveMQ states the same default [13].

**Mechanics** (4.9) [1]. The sender sets an initial send quota, non-zero and not exceeding the peer's Receive Maximum
([MQTT-4.9.0-1]). Each QoS > 0 PUBLISH decrements it; at zero the sender MUST NOT send further QoS > 0 PUBLISH packets
([MQTT-4.9.0-2]). It is incremented by one per PUBACK or PUBCOMP received — "regardless of whether the PUBACK or PUBCOMP carried an
error code" — and per PUBREC with code >= 0x80, and never above the initial value, which matters because a PUBREL retransmitted on a new
connection would otherwise inflate it.

**Exhaustion.** Blocks QoS > 0 PUBLISH only. The sender MAY keep sending QoS 0 or suspend that too. Both sides MUST continue to process
and respond to all other packet types at zero quota ([MQTT-4.9.0-3]) and MUST NOT delay non-PUBLISH packets because of a full window
([MQTT-3.3.4-8], [MQTT-3.3.4-10]) — acknowledgements, subscriptions and pings never stall behind a blocked publish path (3.3.4, 4.9) [1].
Exceeding the peer's Receive Maximum earns DISCONNECT 0x93 (Receive Maximum exceeded) [1][13]. There is no negotiation and no soft
warning.

**QoS 0 has no flow control at all.** The spec says so twice: "There is no mechanism to limit the QoS 0 publications that the Server
might try to send" (3.1.2.11.3), and the mirror sentence for the client (3.2.2.3.3) [1]. A QoS 0 flood can only be answered with
DISCONNECT 0x96 (Message rate too high) or 0x97 (Quota exceeded), or by dropping the socket.

**Maximum Packet Size** is byte-oriented and a hard limit rather than backpressure. Neither side may send a packet exceeding the peer's
maximum ([MQTT-3.1.2-24], [MQTT-3.2.2-15]); a receiver that gets one uses DISCONNECT 0x95 (Packet too large). Where the server cannot
send a message because it is too large it "MUST discard it without sending it and then behave as if it had completed sending that
Application Message" ([MQTT-3.1.2-25]) — a silent drop from the subscriber's view; for a shared subscription it may discard for everyone
or route to a member that can take it (3.1.2.11.4) [1]. A dead-letter queue is mentioned once as explicitly out of scope. Reason String
and User Property must be suppressed whenever they would push a packet over the peer's maximum ([MQTT-3.2.2-19], [MQTT-3.4.2-2] and
equivalents) [1], so diagnostics are the first casualty of a small limit.

**Topic Alias Maximum** bounds the alias table the receiver must hold: default 0, so no aliases unless requested; the sender MUST NOT
exceed it ([MQTT-3.3.2-9], [MQTT-3.3.2-11]) and the receiver MUST accept every alias from 1 to the value it published
([MQTT-3.3.2-10], [MQTT-3.3.2-12]) (3.1.2.11.5, 3.2.2.3.8) [1]. A memory bound, not a rate bound.

**Scope.** All of the above is per connection. MQTT 5.0 has no per-channel, per-subscription or per-topic credit, so a single slow
subscription cannot be throttled independently of the rest of the session.

**Broker-side queue limits are entirely implementation-defined.** The spec acknowledges only that implementations "will of course have
limits in terms of capacity and may be subject to administrative policies" (4.1.1) [1]. Concretely: Mosquitto `max_inflight_messages`
20 (0 unlimited, 1 guarantees ordering), `max_inflight_bytes` 0, `max_queued_messages` 1000 per client, `max_queued_bytes` 0, queueing
stopping at whichever bound is hit first with subsequent QoS 1/2 messages "silently dropped" and no eviction of older ones documented
[31]; EMQX `mqtt.max_inflight` 32 (range 1..65,535), `mqtt.max_mqueue_len` 1000, `mqtt.mqueue_store_qos0` true, and at a per-priority
limit eviction of the *oldest* QoS 0 message at that priority — the opposite end from Mosquitto [18]; HiveMQ `max-queue-size` 1000 per
client with `strategy` default `discard` (drop new arrivals) and optional `discard-oldest`, plus a 500,000-message per-shared-
subscription queue [17][16]. EMQX `mqtt.max_awaiting_rel` 100 bounds received QoS 2 messages awaiting PUBREL and answers 0x93 when
full, with `mqtt.await_rel_timeout` 300 s [18].

## 6. Delivery guarantees and acknowledgement

The delivery protocol is symmetric and hop-scoped: "The delivery protocol is concerned solely with the delivery of an application
message from a single sender to a single receiver. When the Server is delivering an Application Message to more than one Client, each
Client is treated independently" (4.3) [1]. There is no end-to-end guarantee from publisher to subscriber.

**QoS 0, at most once.** The sender MUST send PUBLISH with QoS 0 and DUP 0 ([MQTT-4.3.1-1]). No response, no retry, no stored state;
"the message arrives at the receiver either once or not at all", and the receiver accepts ownership on receipt (4.3.1) [1]. HiveMQ calls
it fire-and-forget: the sender neither stores nor retransmits [5].

**QoS 1, at least once** (4.3.2, figure 4.2) [1]:

```
Sender                                  Receiver
store message
PUBLISH QoS=1, DUP=0, PacketId=N  ----->
                                        initiate onward delivery
                                  <----- PUBACK PacketId=N [reason code]
discard message
```

The sender MUST assign an unused Packet Identifier per new message ([MQTT-4.3.2-1]), send with DUP 0 ([MQTT-4.3.2-2]) and treat the
PUBLISH as unacknowledged until the matching PUBACK ([MQTT-4.3.2-3]). The receiver MUST respond with PUBACK carrying that identifier
"having accepted ownership of the Application Message" ([MQTT-4.3.2-4]), and need not have completed onward delivery first — the
footnote to figure 4.2 is explicit: "The receiver does not need to complete delivery of the Application Message before sending the
PUBACK." After sending PUBACK the receiver MUST treat any later PUBLISH with the same identifier as a new message, whatever DUP says
([MQTT-4.3.2-5]).

*What PUBACK certifies:* transfer of ownership for that hop, nothing else. Not that a subscriber received the message, not that it was
persisted, not that it was processed. The spec never requires the ownership transfer to be durable; 4.1.1 and 4.1.2 leave volatile
versus non-volatile storage to the implementer, contrasting an electricity-meter solution that accepts volatile memory with a
parking-meter payment application requiring "all data be written to non-volatile memory before it is transmitted across the network"
[1]. No source consulted states whether an acknowledgement precedes or follows a durable write; Mosquitto's persistence promises only
writes at close, at `autosave_interval` (default 1800 s) or on SIGUSR1, with no fsync ordering [31].

*PUBACK reason codes* (3.4.2.1) [1]: 0x00 Success; 0x10 No matching subscribers (server only, a success); 0x80 Unspecified error; 0x83
Implementation specific error; 0x87 Not authorized; 0x90 Topic Name invalid; 0x91 Packet identifier in use; 0x97 Quota exceeded; 0x99
Payload format invalid. A code >= 0x80 means the PUBLISH counts as acknowledged and MUST NOT be retransmitted ([MQTT-4.4.0-2]) (4.4)
[1] — the message is dead with no protocol recourse.

**QoS 2, exactly once** (4.3.3, figure 4.3) [1] — four packets, two round trips:

```
Sender                                        Receiver
store message
PUBLISH QoS=2, DUP=0, PacketId=N      ----->
                                              store PacketId, initiate onward delivery
                                      <-----  PUBREC PacketId=N, reason code
discard message, store "PUBREC received"
PUBREL PacketId=N                     ----->
                                              discard PacketId
                                      <-----  PUBCOMP PacketId=N
discard stored state
```

Sender: assign an unused identifier ([MQTT-4.3.3-1]); send with DUP 0 ([MQTT-4.3.3-2]); treat the PUBLISH as unacknowledged until PUBREC
([MQTT-4.3.3-3]); on a PUBREC with code < 0x80 send PUBREL with the same identifier ([MQTT-4.3.3-4]); treat PUBREL as unacknowledged
until PUBCOMP ([MQTT-4.3.3-5]); MUST NOT resend the PUBLISH once PUBREL has been sent ([MQTT-4.3.3-6]); MUST NOT apply message expiry
once the PUBLISH has been sent ([MQTT-4.3.3-7]) [1].

Receiver: respond with PUBREC "having accepted ownership" ([MQTT-4.3.3-8]); until PUBREL arrives, answer any repeat PUBLISH with the
same identifier by another PUBREC and MUST NOT cause a duplicate onward delivery ([MQTT-4.3.3-10]); respond to PUBREL with PUBCOMP
([MQTT-4.3.3-11]); after PUBCOMP treat the identifier as free and a later PUBLISH with it as new ([MQTT-4.3.3-12]); continue the
acknowledgement sequence even if message expiry has been applied ([MQTT-4.3.3-13]); having sent a PUBREC with code >= 0x80, treat a
later PUBLISH with that identifier as new ([MQTT-4.3.3-9]) [1].

*What each packet certifies.* **PUBREC**: the receiver has accepted ownership *and* completed every check that could cause a forwarding
failure — the footnote to figure 4.3 is precise: "the receiver needs to perform all checks for conditions which might result in a
forwarding failure (e.g. quota exceeded, authorization, etc.) before accepting ownership. The receiver indicates success or failure
using the appropriate Reason Code in the PUBREC." It does not certify onward delivery or persistence. **PUBREL**: the sender will never
send this PUBLISH again, so the receiver may release its duplicate-suppression state. **PUBCOMP**: the identifier is released and
"Publication of QoS 2 message is complete" (3.7.2.1) [1] — the handshake terminated, not that any subscriber saw anything.

*Reason codes.* PUBREC shares PUBACK's list (3.5.2.1). PUBREL and PUBCOMP have only 0x00 Success and 0x92 Packet Identifier not found,
which the spec notes "is not an error during recovery, but at other times indicates a mismatch between the Session State on the Client
and Server" (3.6.2.1, 3.7.2.1) [1].

*Cost.* Four packets against QoS 1's two, plus per-message identifier state on both peers held across two round trips. EMQX calls QoS 2
the highest-overhead and slowest level and claims it "usually has about half the throughput" of QoS 0/1 — a vendor generalisation, not a
benchmark [26]. HiveMQ presents it as a four-step handshake and advises QoS 1 where its duplicate risk is tolerable [5], without stating
a round-trip count; two round trips is arithmetic off the flow [inference]. The receiver-side state is what EMQX bounds with
`max_awaiting_rel` 100 and `await_rel_timeout` 300 s [18].

**The DUP flag.** DUP 0 means first attempt, DUP 1 "might be re-delivery of an earlier attempt to send the packet". It MUST be 1 when
re-delivering ([MQTT-3.3.1-1]) and MUST be 0 for all QoS 0 messages ([MQTT-3.3.1-2]) (3.3.1.1) [1]. Three limits, all stated by the spec
itself, make it useless for deduplication: it is not propagated, and an outgoing value "MUST be determined solely by whether the
outgoing PUBLISH packet is a retransmission" ([MQTT-3.3.1-3]); a receiver of DUP 1 "cannot assume that it has seen an earlier copy of
this packet"; and it refers to the packet, not the message, so the same message can arrive twice with DUP 0 under different identifiers.
Deduplication happens by Packet Identifier inside the QoS 2 handshake, not by DUP. HiveMQ's description that DUP is "not processed by the
broker or client" overstates it — it is a protocol flag; it is applications that need not read it [5].

**Retransmission and session-resumed rules** (4.4) [1]. On reconnect with Clean Start 0 and a session present, both sides MUST resend,
with the original Packet Identifiers: unacknowledged PUBLISH packets with QoS > 0, DUP set to 1, in the order the originals were sent
([MQTT-4.6.0-1]); and PUBREL packets not yet answered by PUBCOMP. **Not** resent: QoS 0 PUBLISH packets, ever; a PUBLISH whose PUBREL has
already been sent ([MQTT-4.3.3-6]); a PUBLISH already answered with a code >= 0x80 ([MQTT-4.4.0-2]); SUBSCRIBE and UNSUBSCRIBE, which are
not in the retransmission list at all. Nothing is retransmitted on a timer inside a live connection: "This is the only circumstance
where a Client or Server is REQUIRED to resend messages. Clients and Servers MUST NOT resend messages at any other time"
([MQTT-4.4.0-1]). Brokers add timer retry regardless — EMQX `mqtt.retry_interval` defaults to `30s` [18] — and Paho's Python client
documents republishing QoS > 0 messages after a network reconnect even with `clean_session=True`, calls that non-compliant itself, and
warns QoS 2 messages can therefore arrive twice [38].

**Publisher-to-broker versus broker-to-subscriber: the downgrade rule.** Two separate hops, each with its own QoS; "the QoS level used
to deliver an Application Message outbound to the Client could differ from that of the inbound Application Message" (4.3) [1]. The rule:
"The QoS of Application Messages sent in response to a Subscription MUST be the minimum of the QoS of the originally published message
and the Maximum QoS granted by the Server" ([MQTT-3.8.4-8]) (3.8.4) [1]. QoS is only downgraded, never upgraded. The server may grant
less than requested and reports what it granted in the SUBACK code (0x00/0x01/0x02 = Granted QoS 0/1/2) (3.9.3) [1]. The spec's own
consequences (3.8.4) [1]: granted QoS 1 with published QoS 0 delivers at QoS 0, at most one copy, while published QoS 2 is downgraded to
QoS 1 so the subscriber may see duplicates; granted QoS 0 with published QoS 2 may lose the message but "the Server should never send a
duplicate", while published QoS 1 may be lost or duplicated; and where the original was QoS 1 and the granted maximum QoS 0, the server
is explicitly permitted to send duplicate copies. "Subscribing to a Topic Filter at QoS 2 is equivalent to saying 'I would like to
receive Messages matching this filter at the QoS with which they were published'" — the publisher sets the ceiling, the subscriber its
own cap. HiveMQ states the same min() rule and that a broker cannot upgrade [14]; Mosquitto's `mqtt(7)` gives the same worked examples
[33] — yet Mosquitto ships `upgrade_outgoing_qos` (default false) which makes delivery QoS equal subscription QoS and whose own man page
marks it contrary to the MQTT rule [31], and EMQX has `mqtt.upgrade_qos`, default false [18]. Server-side, `Maximum QoS` in CONNACK caps
what the *client* may publish; exceeding it is a Protocol Error answered with DISCONNECT 0x9B, and a server that does not support QoS
1/2 MUST still accept SUBSCRIBE with requested QoS 0, 1 or 2 ([MQTT-3.2.2-9] to [MQTT-3.2.2-11]) (3.2.2.3.4) [1].

## 7. Ordering and duplicates

**What is promised.** Client obligations (4.6) [1]: resend PUBLISH packets in original order ([MQTT-4.6.0-1]); send PUBACK in the order
the PUBLISH packets were received ([MQTT-4.6.0-2]); send PUBREC in that same order ([MQTT-4.6.0-3]); send PUBREL in the order PUBRECs
were received ([MQTT-4.6.0-4]). Server side: an **Ordered Topic** is one where the client can be certain that messages "in that Topic
from the same Client and at the same QoS are received are in the order they were published", and for one the server MUST forward, per
topic and per QoS, in the order received from any given client ([MQTT-4.6.0-5]). Every topic is an Ordered Topic by default when
forwarding on non-shared subscriptions ([MQTT-4.6.0-6]), and a server MAY offer an administrative exemption (4.6) [1].

**Scope.** Same publishing client, same topic, same QoS, non-shared subscription. Nothing is promised across topics, across publishers,
across QoS levels, or through a shared subscription. There is no partition or key concept; the topic is the only ordering scope. HiveMQ
restates this and adds a product-level claim: order holds for same client, same topic and same QoS class, with QoS 1 and QoS 2 ordered
together but not ordered against parallel QoS 0 traffic [15]. EMQX's v5.0 design docs use the same Ordered Topic language [24].

**Where duplicates arise.** (1) QoS 1 by construction (4.3.2) [1]. (2) QoS 2 downgraded to QoS 1 for a subscriber granted maximum 1
(3.8.4) [1]. (3) QoS 1 downgraded to QoS 0, where the server is explicitly permitted to duplicate (3.8.4) [1]. (4) Overlapping
subscriptions on one client, where the server MAY send one copy per matching subscription ([MQTT-3.3.4-2]) [1]. (5) A shared plus a
non-shared subscription matching the same message (4.8.2) [1]. (6) Two overlapping shared subscriptions, processed separately by both
(4.8.2) [1]. (7) Reconnect retransmission (4.4) [1]. (8) A shared-subscription QoS 1 message reassigned to another member after the
first member's session terminated, when the first had in fact received it (4.8.2) [1].

**Ordering and duplicates interact.** The spec's own example: a publisher sends 1,2,3,4 and a subscriber may receive 1,2,3,2,3,4 if the
connection drops after message 3 — a retransmitted earlier message arriving after a later one. Receive Maximum 1 on both sides restricts
this to 1,2,3,3,4: "no QoS 1 message will be received after any later one even on re-connection" (4.6) [1]. That is the only in-protocol
lever for strict ordering and it costs all pipelining. EMQX gives the same advice — in-flight window 1, accepting reduced throughput
[24] — and Mosquitto notes `max_inflight_messages 1` "guarantees ordering" [31].

**Detection.** Within a hop, the Packet Identifier plus the QoS 2 handshake suppresses duplicate onward delivery
([MQTT-4.3.3-10]) [1]. Across hops there is no sequence number, no offset and no message identifier, and DUP is unreliable for the
reasons above, so applications must carry their own identifier in the payload, in `Correlation Data` or in a `User Property` — the same
advice EMQX gives [26].

**Shared subscriptions deliberately break ordering.** The spec warns that "the order of message delivery is not guaranteed between
multiple Clients" for a responder pool (4.10.1) [1], and [MQTT-4.6.0-6] limits the Ordered Topic obligation to non-shared subscriptions.
HiveMQ states the trade-off directly: no single member receives the complete stream [15].

## 8. Failure behaviour

| Event | Sending peer observes | Receiving peer observes | Lost | Ambiguous |
| --- | --- | --- | --- | --- |
| Client crash, session present (expiry > 0) | Nothing; process gone | Will published once Will Delay elapses or the session ends, whichever first ([MQTT-3.1.2-8]) (3.1.2.5, 3.1.3.2.2) [1] | QoS 0 in flight; client-side session state if volatile (4.1.1) [1] | Whether the client processed a message whose PUBACK never arrived; it is resent with DUP 1 on reconnect (4.4) [1] |
| Keep Alive expiry | Nothing | Server MUST close after 1.5 x Keep Alive "as if the network had failed" ([MQTT-3.1.2-22]), SHOULD send DISCONNECT 0x8D; Will fires because no DISCONNECT 0x00 preceded the close (3.1.2.5) [1] | QoS 0 in flight | Whether the client is dead or merely silent — the protocol cannot tell |
| Network partition | Writes fail or hang; no protocol signal | Keep Alive expiry as above | QoS 0 in flight | Everything in flight at QoS > 0 until reconnect resolves it |
| Reconnect before Will Delay elapses | — | Server MUST NOT send the Will ([MQTT-3.1.3-9]) [1] | Nothing | Nothing — the designed use of Will Delay |
| Session taken over | Incumbent gets DISCONNECT 0x8E and its connection MUST close ([MQTT-3.1.4-3]) | New connection sees Session Present per Clean Start | Incumbent's QoS 0 in flight. Its Will is published if Will Delay is 0, or Clean Start is 1, or the old Session Expiry was 0 (3.1.4, 3.1.3.2.2) [1] | Whether this was the same application restarting or a rogue client reusing the ClientID; the spec flags it as an authorization concern (5.4.2) [1] |
| Broker restart, persistent sessions | Reconnect fails until it returns | Session Present shows whether state survived; a client with state that sees 0 MUST discard it ([MQTT-3.2.2-5]) | Whatever was not persisted. The spec says only that "hardware or software failures may result in loss or corruption of Session State" (4.1.1) [1]. EMQX's default sessions are RAM-only and lost on node restart; durable sessions (v5.7.0+, off by default) persist to disk [19] | Whether an unacknowledged QoS 1 message was delivered before the crash. Also: the server MAY defer Will publication until after restart, so a Will can arrive long after the failure (3.1.2.5) [1] |
| Message expires while queued | No signal | Nothing arrives; if expiry passes before onward delivery starts the server MUST delete its copy for that subscriber ([MQTT-3.3.2-5]), and delivered copies carry the remaining interval ([MQTT-3.3.2-6]) [1] | The message, silently | Publisher cannot tell expiry from no-subscriber from drop. Once a QoS 2 PUBLISH has been sent, expiry MUST NOT be applied ([MQTT-4.3.3-7]) and the handshake MUST complete anyway ([MQTT-4.3.3-13]) — so PUBCOMP can certify an expired message |
| Session expires while offline | — | All queued messages go with the session state ([MQTT-4.1.0-2]) [1]; HiveMQ notes session expiry overrides per-message expiry [10] | Every queued message regardless of its own Message Expiry Interval | The client learns only from Session Present 0 on its next connect |
| Oversized packet received | DISCONNECT 0x95 (Packet too large), connection closed (3.1.2.11.4, 3.2.2.3.6) [1] | — | The packet and the connection | — |
| Oversized packet cannot be sent onward | — | Nothing arrives | The message: the server MUST discard it "and then behave as if it had completed sending that Application Message" ([MQTT-3.1.2-25]) | Total silence — the publisher's PUBACK already reported success. For a shared subscription the server may drop it for all or route to a member that can take it (3.1.2.11.4) [1] |
| Unauthorized publish | QoS 0: no signal, or DISCONNECT 0x87. QoS 1: PUBACK 0x87. QoS 2: PUBREC 0x87, and the message MUST NOT be retransmitted ([MQTT-4.4.0-2]) (3.4.2.1, 3.5.2.1) [1] | Nothing arrives | The message | At QoS 0 the publisher cannot distinguish "not authorized" from "delivered" |
| Unauthorized subscribe | SUBACK 0x87 or 0x8F for that filter only; other filters in the same SUBSCRIBE may succeed (3.9.3) [1] | — | That subscription | — |
| Authentication failure | CONNACK 0x86, 0x87 or 0x8C — or nothing at all: the spec recommends silence on public networks so as not to reveal that an MQTT server is present (3.1.4, 3.2.2.2, 4.12) [1] | — | The connection | Whether the server exists at all, by design |
| Invalid Topic Alias | DISCONNECT 0x94 for alias 0 or above the peer's maximum; 0x82 for a zero-length Topic Name with no established mapping (3.3.4) [1] | — | The connection and every alias mapping on it (mappings never cross connections, [MQTT-3.3.2-7]) | — |
| QoS 2 state lost on one side | PUBREL answered PUBCOMP 0x92 (Packet Identifier not found) | PUBLISH answered PUBREC/PUBACK 0x91 (Packet identifier in use) | Possibly the message, or a duplicate delivery | The spec's own answer is inconclusive: 0x92 "is not an error during recovery, but at other times indicates a mismatch between the Session State on the Client and Server" (3.6.2.1) [1]. For 0x91 the suggested remedy is to fix the state, reconnect with Clean Start 1, or decide an implementation is defective (2.4) [1] |
| Shared-subscription member crash with unacked messages | — | QoS 2: server MUST complete delivery to that same client on reconnect ([MQTT-4.8.2-4]); if the session terminates first it MUST NOT send to any other subscriber ([MQTT-4.8.2-5]). QoS 1: server MAY wait, SHOULD reassign if the session terminates, MAY reassign as soon as the connection drops (4.8.2) [1] | A QoS 2 message whose member's session died | Whether a reassigned QoS 1 message was already processed by the crashed member. EMQX documents redispatch only on persistent-session *expiry* and discards everything pending when the last member's session expires [20] |
| Member sends a negative acknowledgement | PUBACK/PUBREC >= 0x80 | — | The message: the server MUST discard it and MUST NOT try another subscriber ([MQTT-4.8.2-6]) [1] | — |
| Slow consumer | No signal — its PUBACK already succeeded | Backlog grows in the server's session state, bounded only by implementation limits (4.1.1) [1] | Messages dropped at the implementation bound: new arrivals in Mosquitto and HiveMQ's default, oldest QoS 0 in EMQX [31][17][18] | The protocol gives the publisher no visibility; even DISCONNECT 0x97 goes to the consumer, not the producer |
| Server redirection | CONNACK or DISCONNECT 0x9C or 0x9D, optionally with `Server Reference`; connection closes (4.11) [1] | — | The connection; session state is not transferred by the protocol | Whether the target holds the session. No session migration is defined, and the client SHOULD but need not follow the reference (4.11) [1] |
| Malformed packet | Server MUST close ([MQTT-4.13.1-1]); client SHOULD. DISCONNECT 0x81 or 0x82 SHOULD precede it (4.13.1) [1] | — | The connection. "There are no consequences for other Sessions" (4.13.1) [1] | — |

## 9. Reliability recipes

**LWT presence / online-offline state** [8]. *Problem:* subscribers need to know whether a client is alive. *Mechanism:* Will with
retain true, payload `Offline`, topic `client1/status`; publish retained `Online` after connecting; add Will Delay Interval to tolerate
blips (3.1.3.2.2) [1]. *Guarantee:* late joiners read current state from the retained message, a hard death produces the transition.
*Cost:* one retained message per client plus a Will per session. *Failure modes:* detection is at best 1.5 x Keep Alive (3.1.2.10) [1];
a server MAY defer Will publication across its own restart so the transition can arrive arbitrarily late (3.1.2.5) [1]; on takeover the
Will fires or does not depending on Will Delay, Clean Start and the old Session Expiry (3.1.3.2.2) [1]; a normal DISCONNECT 0x00
discards the Will ([MQTT-3.14.4-3]), so `Offline` after a graceful exit must be published by the client itself.

**Retained state as last-value cache** [7]. *Problem:* a late joiner needs current state from an irregular publisher. *Mechanism:*
RETAIN = 1; delivery at subscribe; zero-byte retained publish clears. *Guarantee:* exactly one value per exact topic, delivered on
subscribe. *Cost:* server storage outside any session, surviving session end (4.1) [1]. *Failure modes:* a wildcard subscription gets
one retained message per matching topic, which can be a large burst; QoS 0 retained messages MAY be discarded at any time
(3.3.1.3) [1]; retained messages are never sent to shared subscriptions (4.8.2) [1] — a rule `rmqtt` 0.23.1 does **not** honour, so a
`$share/` subscriber there receives the stored value at subscribe time with RETAIN 1 and a client cannot rely on the exclusion [66];
Retain As Published 0 hides from the subscriber
that a message was retained (3.8.3.1) [1]; AWS IoT Core stores per exact topic but does not deliver an existing retained message to a
wildcard subscription [60], and Azure IoT Hub does not persist RETAIN at all, converting it to an `mqtt-retain` property [61].

**Request/response with correlation** [12], (4.10) [1]. *Problem:* route a reply back through a broker with no notion of a reply.
*Mechanism:* Response Topic plus Correlation Data; the responder copies the correlation value into a publish to the Response Topic; use
Request Response Information / Response Information to obtain an authorized namespace. *Guarantee:* the requester can match asynchronous
replies to requests. *Cost:* a subscription per requester plus authorization on both topics in both directions. *Failure modes:* the
spec's own list — zero or many responders, zero or many subscribers to the Response Topic including clients other than the requester,
and a response dropped entirely if the requester has not subscribed yet (4.10.1) [1]; no timeout, retry or reply-once semantics
anywhere; Correlation Data should be encrypted or hashed if responder tampering would break the requester (3.3.2.3.6) [1].

**Shared subscriptions for consumer scaling** [11][16], (4.8.2) [1]. *Problem:* spread work over several workers. *Mechanism:* every
worker subscribes to `$share/{ShareName}/{filter}`; the server picks one member per message. *Guarantee:* one member per group receives
each message. *Cost:* per-topic ordering is given up. *Failure modes:* selection criteria are unspecified, so capacity awareness is
implementation-dependent — EMQX offers seven strategies with `round_robin` default [20], HiveMQ is random and explicitly not round-robin
[16]; a QoS 2 message in flight to a member whose session dies is lost and MUST NOT be reassigned ([MQTT-4.8.2-5]); a QoS 1 message may
be reassigned after the first member may already have processed it; no retained messages are delivered; a disconnected persistent member
keeps accumulating messages, which EMQX warns can overflow its queue and for which it recommends Clean Session or a short expiry [20];
HiveMQ downgrades shared QoS 2 to QoS 1 because it "cannot guarantee QoS 2 for shared subscriptions" [16].

**Session expiry for offline delivery** [10], (3.1.2.11.2, 4.1) [1]. *Problem:* an intermittently connected client must not lose
messages. *Mechanism:* Clean Start 0 plus a non-zero Session Expiry Interval; the server queues QoS 1/2 messages matching the session's
subscriptions while it is offline; the interval can be revised on DISCONNECT. *Guarantee:* QoS 1/2 messages published during the outage
are delivered on reconnect within the interval. *Cost:* server memory or disk per disconnected session — EMQX warns the population is
roughly disconnect rate x expiry interval [18]. *Failure modes:* QoS 0 messages are only optionally queued (4.1 lists them as OPTIONAL
[1]; Mosquitto queues them only with the non-standard `queue_qos0_messages` [31], EMQX by default via `mqueue_store_qos0` [18]); the
queue is bounded by implementation limits and overflows silently; a long interval on a client that never returns is an orphaned session,
which is why the spec advises disconnecting with expiry 0 when done (3.1.2.11.2) [1]; a server MAY discard session state
administratively at any time (4.1.1) [1]; session expiry discards queued messages regardless of their own Message Expiry Interval [10].

**Message expiry to bound staleness** [10], (3.3.2.3.3) [1]. *Problem:* queued telemetry becomes worthless. *Mechanism:* Message Expiry
Interval per PUBLISH; the server deletes its copy if the interval passes before onward delivery starts and rewrites the interval
downwards on delivery. *Guarantee:* subscribers do not receive messages older than the interval. *Cost:* per-copy arrival-time tracking.
*Failure modes:* once a QoS 2 PUBLISH has been sent expiry MUST NOT be applied ([MQTT-4.3.3-7]) and the handshake completes anyway
([MQTT-4.3.3-13]); expiry is silent to the publisher; Mosquitto reclaims expired retained entries only if `retain_expiry_interval` is
configured [31].

**Flow control to bound in-flight work** [13], (4.9) [1]. *Problem:* a peer must not be swamped by concurrent reliable messages.
*Mechanism:* Receive Maximum per direction per connection. *Guarantee:* at most that many unacknowledged QoS > 0 PUBLISH packets.
*Cost:* pipelining; Receive Maximum 1 serialises the reliable path. *Failure modes:* QoS 0 is entirely uncovered (3.1.2.11.3) [1]; the
quota is per connection, not per subscription or topic; breaching it costs the whole connection via DISCONNECT 0x93.

**Ordering by an in-flight window of one** (4.6) [1]. *Problem:* a retransmitted QoS 1 message can arrive after a later one.
*Mechanism:* Receive Maximum 1 on both sides. *Guarantee:* no QoS 1 message arrives after a later one even across reconnection —
1,2,3,3,4 rather than 1,2,3,2,3,4. *Cost:* one message in flight at a time. *Failure modes:* nothing is fixed across topics,
publishers, QoS levels or shared subscriptions. EMQX [24] and Mosquitto [31] document the same lever.

**QoS choice per use case.** The spec's own examples: QoS 0 for "ambient sensor data where it does not matter if an individual reading
is lost as the next one will be published soon after"; QoS 2 for "billing systems where duplicate or lost messages could lead to
incorrect charges being applied" (Abstract) [1]; and its storage pair, QoS 1 in volatile memory for meter readings against non-volatile
writes before transmission for parking-meter payments (4.1.2) [1]. HiveMQ adds that QoS 1 is faster where its duplicate risk is
tolerable [5]; EMQX calls QoS 2 the slowest and highest-overhead level [26]. The two hops are chosen independently: publisher ceiling,
subscriber cap (section 6).

**Bridging for locality** [31], (4.6 above). *Problem:* connect brokers without flooding either with the other's traffic. *Mechanism:* a
bridge client using No Local and Retain As Published (3.8.3.1) [1] plus per-topic direction and prefix rewriting. *Guarantee:* selected
topics cross, the rest stay local. *Cost:* a client session per bridge and duplicated retained state. *Failure modes:* Mosquitto's man
page states that topic mappings can be configured into loops and that `try_private` only works if the remote broker supports bridge
mode [31].

**No recipe in the protocol for** failover pairs, state replication, disk-backed delivery guarantees, dead-letter queues (mentioned once
as explicitly out of scope, 3.1.2.11.4), service-broker discovery, or transactions [1]. Server Reference is the nearest thing to
failover and it transfers no state (4.11) [1].

## 10. Security and identity

**Authentication mechanisms.** Basic: `User Name` (UTF-8) and `Password` (binary data, "can be used to carry any credential
information") in the CONNECT payload (3.1.3.5, 3.1.3.6) [1]; 5.0 permits a Password with no User Name (3.1.2.9) [1]. Enhanced (4.12)
[1]: `Authentication Method` names a scheme — "commonly a SASL mechanism, and using such a registered name aids interchange", though not
restricted to registered SASL mechanisms — and AUTH packets carry `Authentication Data` both ways with reason code 0x18 until the server
sends CONNACK 0x00. Non-normative SCRAM-SHA-1 and GS2-KRB5 traces are given. All AUTH packets and any successful CONNACK MUST repeat the
same method ([MQTT-4.12.0-5]); an unsupported method earns CONNACK 0x8C or 0x87 and the connection MUST close ([MQTT-4.12.0-1]).
Enhanced authentication is OPTIONAL for both parties, and absent a client-named method the server MUST NOT send AUTH ([MQTT-4.12.0-6]).

**Re-authentication** (4.12.1) [1]: a client that named a method may send AUTH 0x19 at any time after CONNACK using the same method
([MQTT-4.12.1-1]); other traffic continues on the previous authentication during the exchange; on failure both sides SHOULD send
DISCONNECT and MUST close ([MQTT-4.12.1-2]). A server can bound what re-authentication may change by rejecting attempts that, say, alter
the User Name. This is the protocol's answer to credential rotation on long-lived connections, and 5.4.10 also suggests periodic forced
re-authentication [1].

**Mutual authentication.** "The MQTT protocol is not trust symmetrical. When using basic authentication, there is no mechanism for the
Client to authenticate the Server. Some forms of extended authentication do allow for mutual authentication" (5.4.3) [1]. TLS server
certificates are the usual answer, with SNI recommended for multi-hostname servers (5.4.3) [1].

**Authorization model.** Non-normative and left to implementations (5.4.2) [1]. The spec recommends: check authorization after
successful authentication; base it on User Name, client host or IP, or the authentication outcome; **check that the client is authorized
to use its Client Identifier**, since the ClientID is the key to session state and a colliding identifier gives access to another
client's session; apply post-CONNECT controls on which topics may be published to and which filters subscribed with; and "consider
limiting access to Topic Filters that have broad scope, such as the `#` Topic Filter" (5.4.2) [1]. Section 4.7.3 adds that a server MAY
use a security component to authorize particular actions on a topic resource for a given client [1].

**Granularity.** Per connection for authentication; per topic name and per topic filter for authorization, at publish and subscribe
time. In-protocol verdict carriers: CONNACK 0x87, SUBACK/UNSUBACK 0x87 or 0x8F per filter, PUBACK/PUBREC 0x87 per message, DISCONNECT
0x87 (2.4) [1]. Request/response authorization is symmetric and applicational — the requester must be authorized to publish the request
and subscribe the Response Topic, the responder the reverse, and "While topic authorization is outside of this specification, it is
recommended that Servers implement such authorization" (4.10.1) [1].

**Does an identity travel with a message?** No. PUBLISH has no authenticated-sender field. The server forwards Payload Format Indicator,
Content Type, Response Topic, Correlation Data and User Properties unaltered ([MQTT-3.3.2-4], [MQTT-3.3.2-15] to [MQTT-3.3.2-20]) [1] and
adds nothing but Subscription Identifiers. A publisher's identity reaches a subscriber only if the application puts it in a User
Property, the payload, or the topic — which is what HiveMQ's "embed a unique identifier or the Client Id in topics" rule buys [4]. The
spec is explicit about the consequence: "Clients connected to a Server have a transitive trust relationship with other Clients connected
to the same Server and who have authority to publish data on the same topics" (5.4.10) [1]. Non-repudiation is named as something
application designers "might need to consider" and is not provided (5.4.6) [1].

**Transport security** (non-normative chapter 5) [1]. TLS is recommended on port 8883 (`secure-mqtt`) and NULL-encryption cipher suites
are to be avoided (5.1, 5.4.5). TLS client certificates may authenticate the client in addition to or instead of User Name and Password
(5.4.3, 5.4.12.3). CRL and OCSP checking are optional (5.4.7, 5.4.10); session resumption is suggested for constrained devices and
renegotiation for long-lived connections (5.4.10). Three profiles are sketched — clear communication, secured network (VPN or physically
secure network), secured transport (TLS) — plus industry-specific profiles (5.4.12). Applications may encrypt payloads independently,
which protects the payload at rest and in flight but "would not provide privacy for other Properties of the Application Message such as
Topic Name" (5.4.5). SOCKSv5 support is recommended for proxied clients, with a warning that SOCKS authentication may be plaintext and
should not reuse MQTT credentials (5.4.11).

**Attack surface the spec names** (5.1) [1]: compromised devices; data at rest in clients and servers; timing attacks; denial of
service; interception, alteration, re-routing or disclosure; injection of spoofed control packets. Suggested detections (5.4.8):
repeated connection or authentication attempts, abnormal terminations, topic scanning, sending undeliverable messages, clients that
connect but send nothing; with responses including dynamic block lists by IP or ClientID and network-level rate limiting. A server may
also "limit reading from the Network Connection or close the Network Connection if the Client sends too much data before authentication
is complete... as a way of avoiding denial of service attacks" (3.1.4) [1]. Finally, 5.4.9.2 documents a publisher-driven denial of
service against a *subscriber*: if the server does not validate Disallowed Unicode code points or the Payload Format Indicator but a
subscribing client does, a publisher can make that subscriber close its connection repeatedly — and at QoS 1/2 the message is
redelivered, so the subscriber disconnects again. The suggested remedies are to change the server to reject such strings, or the
subscriber library to tolerate them (5.4.9.3) [1].

## 11. Limits and resource bounds

| Limit | Declared by | Default | Bounds |
| --- | --- | --- | --- |
| `Receive Maximum` (3.1.2.11.3, 3.2.2.3.3) | both, per direction | 65,535 | unacknowledged QoS 1/2 PUBLISH packets; 0 is a Protocol Error |
| `Maximum Packet Size` (3.1.2.11.4, 3.2.2.3.6) | both | absent = no limit below 268,435,455 bytes | total packet bytes; 0 is a Protocol Error |
| `Topic Alias Maximum` (3.1.2.11.5, 3.2.2.3.8) | both | 0 — no aliases permitted | highest usable alias, hence the alias table |
| `Maximum QoS` (3.2.2.3.4) | server | absent = 2 | highest QoS the client may publish |
| `Retain Available` (3.2.2.3.5) | server | absent = available | whether the client may set RETAIN |
| `Wildcard Subscription Available` (3.2.2.3.11) | server | absent = available | whether filters may contain `+` or `#` |
| `Subscription Identifiers Available` (3.2.2.3.12) | server | absent = available | whether SUBSCRIBE may carry an identifier |
| `Shared Subscription Available` (3.2.2.3.13) | server | absent = available | whether `$share/` filters are accepted |
| `Server Keep Alive` (3.2.2.3.14) | server | absent = client's value stands | overrides the client's Keep Alive |
| `Session Expiry Interval` (3.1.2.11.2, 3.2.2.3.2) | client proposes, server may override | absent = 0, session ends with the connection | session-state retention; 0xFFFFFFFF never expires |
| `Keep Alive` (3.1.2.10) | client, server may override | none; 0 disables | maximum silence before the server closes at 1.5x |
| `Request Problem Information` (3.1.2.11.7) | client | 1 | if 0, no Reason String or User Property except on PUBLISH, CONNACK, DISCONNECT ([MQTT-3.1.2-29]) |
| `Request Response Information` (3.1.2.11.6) | client | 0 | if 0, the server MUST NOT return Response Information ([MQTT-3.1.2-28]) |
| subscription `Maximum QoS` (3.8.3.1) | client per filter, server grants | — | delivery QoS to that subscription |
| `Message Expiry Interval` (3.3.2.3.3) | publisher per message | absent = never expires | how long the server holds an undelivered copy |
| `Will Delay Interval` (3.1.3.2.2) | client per session | 0 | delay before the Will is published |

Protocol-fixed ceilings, not configurable: packet 268,435,455 bytes (2.1.4); UTF-8 string and Binary Data 65,535 bytes (1.5.4, 1.5.6);
Topic Name and Filter 65,535 bytes ([MQTT-4.7.3-3]); Packet Identifier 1..65,535 (2.2.1); Subscription Identifier 1..268,435,455
(3.8.2.1.2); Keep Alive 65,535 s; Session Expiry and Message Expiry 0..4,294,967,295 s, about 136 years as HiveMQ notes [10] [1].

**What an unbounded resource looks like in practice.** The specification bounds almost nothing by default and says so: 4.1.1 offers only
that implementations "will of course have limits in terms of capacity and may be subject to administrative policies", that stored state
can be discarded administratively, and that "it is prudent to evaluate the storage capabilities of the Client and Server to ensure that
they are sufficient" [1]. Four resources are unbounded by the protocol:

1. **Session count and lifetime.** A client may request Session Expiry 0xFFFFFFFF and never return. Servers cap it out of band: EMQX
   `max_session_expiry_interval`, default `infinity` [18]; HiveMQ overrides an excessive request in CONNACK [17]; Mosquitto's
   non-standard `persistent_client_expiration`, which never expires by default [31].
2. **Queued messages per session.** Bounded only by implementation: Mosquitto 1000 (`max_queued_messages`) dropping new arrivals [31];
   EMQX 1000 (`mqtt.max_mqueue_len`) evicting the oldest QoS 0 message at a priority [18]; HiveMQ 1000 with `discard` (new) default
   [17]. EMQX's durable sessions have "no upper limit on undelivered messages" at the cost of throughput and latency [19].
3. **Subscriptions and topics.** No limit on subscription count, topic count or tree depth, only on string length. Azure's archived MQTT
   5 preview capped clients at 50 subscriptions [62].
4. **QoS 2 receiver state.** Every received-but-unreleased QoS 2 message pins a Packet Identifier and its duplicate-suppression state
   until PUBREL; the protocol's only cap is the 65,535-identifier space per direction. EMQX bounds it at `max_awaiting_rel` 100 with
   `await_rel_timeout` 300 s, answering 0x93 when full [18].

The reason codes provided for these situations — 0x97 Quota exceeded on CONNACK, PUBACK, PUBREC, SUBACK and DISCONNECT; 0x96 Message
rate too high; 0x9F Connection rate exceeded; 0xA0 Maximum connect time (2.4) [1] — report enforcement without defining it.

Selected implementation defaults, for scale: Mosquitto `max_packet_size` 2,000,000 bytes since 2.1 (previously unlimited),
`message_size_limit` 0 (accept everything valid), `max_keepalive` 0, `max_qos` 2, `max_topic_alias` 10, `max_connections` -1,
`autosave_interval` 1800 s [31]; EMQX `mqtt.max_packet_size` 1 MB, `mqtt.session_expiry_interval` 2h for 3.1/3.1.1 [18]; HiveMQ
`max-packet-size` 268,435,460 bytes and `server-receive-maximum` 10 [17]; Azure's archived MQTT 5 preview declared Maximum QoS 1,
Maximum Packet Size 256 KiB, Topic Alias Maximum 10, Receive Maximum 16, Keep Alive 19 minutes [62].

## 12. Answers to the problem catalogue

**P1 — Loss detection and safe retry.** Per hop and per QoS. QoS 0 offers none; QoS 1 detects loss by a missing PUBACK, QoS 2 by a missing PUBREC or PUBCOMP (4.3) [1]. Retry is not timer-driven: the only required retransmission is on reconnect with Clean Start 0 and a session present, resending unacknowledged QoS > 0 PUBLISH packets and unanswered PUBREL packets with their original Packet Identifiers and DUP 1, and "Clients and Servers MUST NOT resend messages at any other time" ([MQTT-4.4.0-1]) (4.4) [1]. Duplicate suppression exists only inside a hop: the QoS 2 handshake makes the receiver answer a repeat PUBLISH with another PUBREC and forbids duplicate onward delivery ([MQTT-4.3.3-10]) [1]. Idempotency is not addressed; DUP is explicitly unreliable (3.3.1.1) [1] and no identifier survives the broker, so idempotency keys must ride in the payload, Correlation Data or a User Property [inference, matching EMQ's advice [26]].

**P2 — Dead or unreachable peer.** Keep Alive: the server MUST close on receiving nothing for 1.5 x Keep Alive ([MQTT-3.1.2-22]), so worst-case detection is 1.5 x Keep Alive, chosen by the client (max 65,535 s) or overridden by Server Keep Alive (3.1.2.10, 3.2.2.3.14) [1]; Keep Alive 0 disables detection. The client's view of the server rests on PINGREQ/PINGRESP with no defined timeout (3.12, 3.13) [1]. On detection the server publishes the Will after Will Delay or at session end, whichever is first ([MQTT-3.1.2-8]) [1]. Session state survives for the Session Expiry Interval and MUST then be discarded ([MQTT-4.1.0-2]) [1]. In-flight QoS > 0 messages stay in session state and are resent on reconnect; QoS 0 is lost; queued messages die with the session.

**P3 — Spreading work by capacity.** Shared subscriptions; unit is one Application Message. Members attach to `$share/{ShareName}/{filter}` and each matching message reaches exactly one (4.8.2) [1]. There is no capacity signal: "the Server implementation is free to choose, on a message by message basis, which Session to use and what criteria it uses to make this selection" (4.8.2) [1]. The nearest thing is Receive Maximum, which stops a member accepting more than N unacknowledged messages (4.9) [1]; whether the server reads it as a capacity hint is its own business. EMQX offers seven strategies with `round_robin` default [20]; HiveMQ distributes randomly, so "faster consumers typically get more" [16]. Capacity-proportional distribution is achievable but is not a protocol guarantee.

**P4 — Slow consumer.** The backlog sits in the server's session state for that client — "QoS 1 and QoS 2 messages pending transmission to the Client and OPTIONALLY QoS 0 messages pending transmission" (4.1) [1]. The protocol bound is Receive Maximum, which bounds the *in-flight* window only, not the queue behind it (4.9) [1]; the queue has no protocol bound. At the implementation bound behaviour diverges: Mosquitto silently drops subsequent QoS 1/2 messages at `max_queued_messages` (1000) with no documented eviction of older ones [31]; EMQX evicts the oldest QoS 0 message at that priority at `max_mqueue_len` (1000) [18]; HiveMQ defaults to `discard` (drop new) with `discard-oldest` available [17]. The publisher gets no signal, its PUBACK having been returned when the broker took ownership. Codes 0x97 (Quota exceeded) and 0x96 (Message rate too high) exist for enforcement against the offending peer (2.4) [1]. Backpressure never reaches the producer through the broker.

**P5 — Late joiner needing current state.** Retained messages: one per exact Topic Name, sent to a new non-shared subscription with RETAIN 1, governed by Retain Handling 0/1/2 and Retain As Published (3.3.1.3, 3.8.3.1) [1]. That is a last-value cache, not a replay log — no history, no snapshot-plus-delta, no offset, no "last N messages". A late joiner needing more must use the offline-delivery path instead: subscribe with Clean Start 0 and a non-zero Session Expiry Interval *before* the messages are published, so the server queues them (4.1) [1]. Retained messages are never sent to shared subscriptions (4.8.2) [1], and QoS 0 retained messages MAY be discarded at any time (3.3.1.3) [1].

**P6 — Failover to another broker.** Redirection, not failover: CONNACK or DISCONNECT with 0x9C (Use another server, temporary) or 0x9D (Server moved, permanent), optionally with `Server Reference` — "a space separated list of references", recommended format `name[:port]` with IPv6 literals in square brackets, examples `myserver.xyz.org:8883` and `[fe80::9610:3eff:fe1c]:1883` (4.11) [1]. The server MAY never send one and the client MAY ignore it. Nothing is re-established automatically: session state is not transferred, subscriptions are not migrated, in-flight messages are not carried over. On the new server the client is a new session unless that server independently holds state for its ClientID, which it learns from Session Present (3.2.2.1.1) [1]. Clustered brokers solve this outside the protocol — HiveMQ claims a persistent QoS 1/2 session reconnecting to another cluster node receives its queued messages in order [15], EMQX replicates durable-session state with Raft [19]. Discovery appears only as the note that a Server Reference name is "commonly a host name, DNS name [RFC1035], SRV name [RFC2782], or literal IP address" (4.11) [1].

**P7 — Surviving a restart.** What is persisted is entirely the implementation's choice. The protocol requires session state to be retained while the connection is open ([MQTT-4.1.0-1]) and after close for the Session Expiry Interval ([MQTT-3.1.2-23]), and enumerates what session state is (4.1) [1] — but never requires stable storage, and 4.1.1 explicitly contemplates loss: "It is possible that hardware or software failures may result in loss or corruption of Session State stored by the Client or Server" [1]. Section 4.1.2 puts the decision on the solution developer, contrasting volatile storage for meter readings with "all data be written to non-volatile memory before it is transmitted across the network" for payments [1]. **No acknowledgement certifies durability:** PUBACK and PUBREC certify transfer of ownership, PUBCOMP certifies handshake completion (4.3.2, 4.3.3) [1], and none of them mentions disk. No source consulted states whether an acknowledgement precedes or follows a durable write; Mosquitto's built-in persistence promises only writes at close, at `autosave_interval` (1800 s) or on SIGUSR1, with no fsync ordering [31]. The client learns after the fact from Session Present 0, at which point it MUST discard its own state ([MQTT-3.2.2-5]) [1]. EMQX's default sessions are RAM-only and lost on node restart; its durable sessions (v5.7.0+, off by default) write session state and routed messages to RocksDB with Raft replication [19].

**P8 — Ordering guarantees and scope.** Scope: same publishing client, same topic, same QoS, non-shared subscription. For an Ordered Topic the server MUST forward, per topic and per QoS, in the order received from any given client ([MQTT-4.6.0-5]), and every topic is Ordered by default on non-shared subscriptions ([MQTT-4.6.0-6]), with a MAY-level administrative exemption (4.6) [1]. Clients must acknowledge in receipt order and resend in original order ([MQTT-4.6.0-1] to [MQTT-4.6.0-4]) [1]. Nothing holds across topics, publishers, QoS levels or shared subscriptions; the spec's own example gives 1,2,3,2,3,4 as a legal receive order after a reconnect, and Receive Maximum 1 on both sides tightens it to 1,2,3,3,4 (4.6) [1]. There is no partition or key concept — the topic is the only scope.

**P9 — Duplicates and "exactly once".** MQTT defines exactly once as QoS 2, strictly per hop: the two-phase PUBLISH/PUBREC/PUBREL/PUBCOMP handshake, with the receiver required to suppress duplicate onward delivery of a repeated Packet Identifier before PUBREL ([MQTT-4.3.3-10]) [1]. It is not end-to-end: publisher-to-broker QoS 2 with broker-to-subscriber QoS 1 is legal and common, and then the subscriber sees duplicates — the downgrade rule permits exactly this ([MQTT-3.8.4-8]) [1]. The eight duplicate sources are enumerated in section 7. Suppression exists only within a hop and only at QoS 2. DUP suppresses nothing: a receiver of DUP 1 "cannot assume that it has seen an earlier copy", the flag is not propagated ([MQTT-3.3.1-3]), and the same message can arrive twice with DUP 0 under different identifiers (3.3.1.1) [1].

**P10 — Request/reply.** `Response Topic` names where the reply goes; `Correlation Data` (opaque binary) is copied by the responder into the reply so the requester can match it (3.3.2.3.5, 3.3.2.3.6, 4.10) [1]. Routing back through the intermediary is just another publish: the broker treats request and reply as ordinary messages and only guarantees to forward Response Topic and Correlation Data unaltered ([MQTT-3.3.2-15], [MQTT-3.3.2-16]) [1]. The requester must already be subscribed to the Response Topic or the reply is dropped (4.10.1) [1]. Because a self-chosen response topic is often unauthorized, the server can hand the client a namespace via Request Response Information / Response Information, whose content and derivation rule the spec deliberately leaves undefined (4.10.2) [1]. There is no request timeout, no reply-once semantics and no correlation state in the broker.

**P11 — Topology.** Strictly brokered and strictly asymmetric: the server binds and accepts, the client connects (1.2) [1]. A client can never listen; there is no brokerless mode and no client-to-client path. Both sides may send PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP and DISCONNECT, but CONNECT, SUBSCRIBE, UNSUBSCRIBE and PINGREQ are client-to-server only and CONNACK, SUBACK, UNSUBACK and PINGRESP server-to-client only (2.1.2) [1]. Broker-to-broker federation is not in the protocol: it is done by running a client on one broker that connects to another, which is what No Local and Retain As Published were added for (Appendix C) [1] and what Mosquitto's bridge implements [31]. Discovery: none. The only in-protocol hint of a location is `Server Reference`, and only after a failed or terminated connection (4.11) [1].

**P12 — Flow-control credit.** Unit: one QoS 1 or QoS 2 PUBLISH packet. Grantor: the receiver, via `Receive Maximum` in CONNECT or CONNACK, independently per direction. Default 65,535; 0 is a Protocol Error. Exhaustion: the sender MUST stop sending QoS > 0 PUBLISH packets, MAY continue or suspend QoS 0, and MUST keep processing and answering every other packet type ([MQTT-4.9.0-2], [MQTT-4.9.0-3]) [1]. The quota is replenished by one per PUBACK or PUBCOMP received regardless of the code carried, and per PUBREC with code >= 0x80, never above the initial value (4.9) [1]. Violation earns DISCONNECT 0x93. The quota is per connection and re-initialised each connection — explicitly not session state (4.9) [1]. QoS 0 has no credit mechanism at all (3.1.2.11.3) [1].

**P13 — Large messages and streaming bodies.** Maximum packet size is 268,435,455 bytes, just under 256 MiB, from the Remaining Length encoding (2.1.4) [1], and either peer may declare a lower `Maximum Packet Size` (3.1.2.11.4, 3.2.2.3.6) [1]. A body cannot be delivered before it is complete: no fragmentation, no continuation packet, no chunked mode, one PUBLISH per whole Application Message. A message too large for a subscriber is silently discarded by the server, which then behaves "as if it had completed sending that Application Message" ([MQTT-3.1.2-25]) [1]. Practical limits are far below the ceiling: Mosquitto `max_packet_size` 2,000,000 bytes since 2.1 [31], EMQX 1 MB [18], Azure's archived MQTT 5 preview 256 KiB [62]. Large payloads therefore need out-of-band transfer with a reference in the message [inference].

**P14 — Identity.** Peers authenticate with User Name and Password in CONNECT, with TLS client certificates, or with the enhanced AUTH exchange, which supports SASL-style challenge/response and mutual authentication and can be repeated in-connection (3.1.3.5, 3.1.3.6, 4.12, 5.4.1, 5.4.3) [1]. An authenticated identity is **not** visible per message: PUBLISH has no sender field and the server adds nothing but Subscription Identifiers, so an identity reaches subscribers only if the application puts it in the payload, a User Property or the topic (section 10). Authorization granularity is per topic name for publish and per topic filter for subscribe, with verdicts as reason codes 0x87 and 0x8F per filter or per message; the mechanism is non-normative (5.4.2, 3.9.3, 3.4.2.1) [1]. The spec's own summary of the trust model: clients on one server "have a transitive trust relationship" with every other client authorized to publish on the same topics (5.4.10) [1].

**P15 — Resource bounds against a hostile peer.** A hostile peer can make the other side allocate: session state surviving disconnection for up to 0xFFFFFFFF seconds via a long Session Expiry Interval; an unbounded queue of QoS 1/2 messages for such a session, since the protocol sets no queue bound; up to 65,535 concurrent QoS 2 receiver states, each pinning an identifier and duplicate-suppression state until PUBREL; up to `Topic Alias Maximum` alias entries; arbitrarily many subscriptions and topics; and up to 256 MiB per packet where no Maximum Packet Size was declared. Protocol defences: `Receive Maximum`, `Maximum Packet Size`, `Topic Alias Maximum`, `Maximum QoS` (refuse QoS 2 entirely), `Server Keep Alive` (force liveness), and the availability flags (refuse wildcards, shared subscriptions, retained messages, subscription identifiers). Everything else is out of band: 4.1.1 concedes only that implementations have limits and may discard state administratively, and 3.1.4 suggests limiting reads before authentication completes [1]. Enforcement codes: 0x97 Quota exceeded, 0x96 Message rate too high, 0x9F Connection rate exceeded, 0xA0 Maximum connect time, 0x8A Banned, 0x98 Administrative action (2.4) [1]. Note also the subscriber-directed attack of 5.4.9.2: Disallowed Unicode code points or a mismatched Payload Format Indicator can make a strict subscriber disconnect repeatedly, with QoS 1/2 redelivery turning it into a loop [1].

**P16 — Observability.** Per message: the reason code on PUBACK or PUBREC, the only in-protocol delivery receipt, certifying ownership transfer for one hop. Code 0x10 (No matching subscribers) is the one signal that a message reached nobody, and it is optional — the server "MAY use this Reason Code instead of 0x00 (Success)" (3.4.2.1) [1]. Reason String carries human diagnostics, "SHOULD NOT be parsed", and is suppressed entirely if the client set Request Problem Information to 0 or whenever it would exceed the peer's Maximum Packet Size ([MQTT-3.1.2-29], [MQTT-3.4.2-2]) [1]. Tracing: `User Property` is the only general carrier, forwarded unaltered and in order on PUBLISH ([MQTT-3.3.2-17], [MQTT-3.3.2-18]) with no MQTT-defined meaning (3.3.2.3.7) [1]; `Subscription Identifier` tells a client which subscription caused a delivery (3.3.2.3.8) [1]; `Correlation Data` can carry a trace identifier. There are no protocol counters, no delivery statistics and no management interface; `$SYS` is a convention with "no official standardization" [4] whose content differs per broker — Mosquitto publishes byte/message/client/heap/store counters at `sys_interval` (10 s) [32][31], EMQX under `$SYS/brokers/{node}/...` restricted by default to localhost subscribers [30].

**P17 — Shutdown.** On orderly close the client sends DISCONNECT 0x00 and the server MUST discard the Will without publishing it ([MQTT-3.14.4-3]) [1]. There is no linger and no drain: after sending DISCONNECT the sender MUST send nothing more and MUST close ([MQTT-3.14.4-1], [MQTT-3.14.4-2]) [1], so anything in flight is abandoned on the wire. What becomes of those messages depends on the session, not the shutdown: with Session Expiry > 0 unacknowledged QoS > 0 messages and unanswered PUBREL packets remain session state and are resent next connect (4.4) [1]; with Session Expiry 0 the session ends and everything queued and in flight is discarded ([MQTT-4.1.0-2]) [1]. QoS 0 in flight is lost either way. The client can adjust the outcome at the last moment with a new Session Expiry Interval on the DISCONNECT (3.14.2.2.2) [1], and can request the Will be published anyway with code 0x04 (3.14.2.1) [1]. On the subscribe side UNSUBSCRIBE is the closest thing to a drain: the server MUST stop adding new matching messages ([MQTT-3.10.4-2]), MUST complete delivery of QoS 1/2 messages it has already started sending ([MQTT-3.10.4-3]), and MAY continue delivering already-buffered messages (3.10.4) [1]. Server shutdown is announced with DISCONNECT 0x8B (3.14.2.1) [1], and a server MAY defer Will publication until after a restart (3.1.2.5) [1].

**P18 — Transports.** Normatively, any transport providing "an ordered, lossless, stream of bytes" both ways ([MQTT-4.2-1]) (4.2) [1]; none is mandatory. Non-normatively TCP/IP, TLS and WebSocket are named, and connectionless transports such as UDP are declared "not suitable on their own because they might lose or reorder data" (4.2) [1]. IANA ports: TCP 1883 plain, TCP 8883 TLS (`secure-mqtt`) (4.2, 5.1) [1]. Only WebSocket has a normative binding (chapter 6) [1], and it changes framing and handshake, not semantics: control packets MUST travel in WebSocket binary data frames and any other frame type forces a close ([MQTT-6.0.0-1]); a frame may hold multiple or partial control packets and receivers MUST NOT assume alignment ([MQTT-6.0.0-2]); the client MUST offer and the server MUST select the subprotocol name `mqtt` ([MQTT-6.0.0-3], [MQTT-6.0.0-4]); and "the WebSocket URI used to connect the Client and Server has no impact on the MQTT protocol". TLS changes nothing in the protocol but enables client-certificate and server authentication (5.4.3, 5.4.12.3) [1].

**P18, continued — QUIC is not an MQTT 5.0 transport.** EMQX ships MQTT over QUIC and states plainly that it "is not yet an MQTT standard protocol" while pursuing OASIS standardization [21]. Its design changes the guarantees and is worth recording. The QUIC listener is off by default and binds `0.0.0.0:14567` in EMQX's own configuration, which is not an MQTT-registered port [23]. Single-stream mode carries all packets in one bidirectional QUIC stream and preserves send order. Multi-stream mode makes the first client-created stream a *control stream* carrying CONNECT, CONNACK and PINGREQ/PINGRESP, then lets the client open one or more *data streams* for publishing and subscribing, mapped as it likes — per topic, per QoS, or separate uplink and downlink; QoS 1 PUBACK and the QoS 2 packets travel on the stream that carried the PUBLISH, and subscription deliveries leave on the stream that carried the SUBSCRIBE [22]. The motivation is head-of-line blocking: independent QUIC streams mean one stalled message does not block the others [22][25]. The cost is ordering — it holds per stream only, and EMQX states directly that it is not assured across streams, so correlated topics must map to the same stream [25]. EMQX documents no negotiation packet, property or ALPN identifier for selecting multi-stream mode; the client simply opens streams [22]. Its listed limitations: session state is not preserved, a reconnecting client must resubscribe its data-stream topics, and QoS 1/2 state is lost if a data stream closes unexpectedly [21]. NanoMQ implements QUIC as a *bridge* mode to EMQX 5, off unless compiled with `-DNNG_ENABLE_QUIC=ON`, with TCP fallback [59].

## 13. Ecosystem

**Rust crates.**

| Crate | Latest | Role | MQTT | Notes |
| --- | --- | --- | --- | --- |
| `rumqttc` | 0.25.1, 2025-11-21 [43] | client | 3.1.1, 5 [42] | Tokio event loop behind sync and async APIs; the caller must keep polling `eventloop.poll()` or `connection.iter()` or the connection stops progressing, and outgoing-packet throttling is listed "todo" [44]. v5 grew over time: properties APIs in 0.21.0, AUTH packet and session-expiry options in 0.25.0, QoS 2 identifier tracking reworked onto `FixedBitSet` in 0.25.0 [43] |
| `rumqttd` | 0.20.0 [42] | embeddable broker | 3.1.1 **and 5** [65] | The workspace checklist marks MQTT 3.1.1, QoS 0/1/2, TLS, retransmission, will and retained messages complete and leaves **MQTT 5 unchecked** [42] — but the checklist is out of date, and this is the one place in this sheet where a measurement overrides a project's own documentation. `rumqttd generate-config` enables a `[v5.1]` listener by default, and the build accepts a 5.0 CONNECT, answers a 5.0 CONNACK **with properties**, enforces a `Topic Alias Maximum` of 4096 and echoes `Subscription Identifier` on every delivery it causes; four rules it does not implement are in the incompatibility list below [65] |
| `paho-mqtt` | 0.14.0, 2026-03-26 [46] | client | 5, 3.1.1, 3.1 [45] | Safe wrapper over Paho C >= 1.3.16, by default building the bundled C with a C compiler and CMake — not pure Rust [45]. Runtime-agnostic, tested with Tokio and smol; its futures start I/O when called rather than when awaited. Ships persistence, automatic reconnect and offline buffering |
| `ntex-mqtt` | 8.2.1, 2026-06-19 [48] | client **and** server framework | 3.1.1, 5 [47] | Apache-2.0, separate codec/client/server modules per version [47]; a framework for building a broker, not a broker |
| `rmqtt` | 0.23.1 [66] | broker | 3.1, 3.1.1, 5 [66] | Apache-2.0, plugin-based. The only broker in this table that **declares** a full capability set in CONNACK — `Maximum QoS` 2, `Retain Available`, `Receive Maximum`, `Maximum Packet Size`, `Topic Alias Maximum` and all three subscription-availability flags — which is what makes a client's refusal paths reachable at all. Two of those flags are plugin state rather than listener settings and so are global to the process; see the incompatibility list below for two rules it does not implement, and note that its plugin registry panics at startup if a compiled-in plugin's configuration file is absent [66] |
| `rust-mqtt` | 0.5.1, 2026-04-10 [49] | client, `no_std` | 5.0 only [50] | On `embedded_io_async`; deliberately omits automatic reconnect, keep-alive loops, retry policy and background tasks, leaving session and QoS delivery control to the caller [50] |
| `mqtt-protocol` | 0.12.0, 2024-03-13 [51] | codec | not established by the crate page | Protocol library, neither client nor broker; the stale release date is the maintenance signal |
| `mqtt5-protocol` | 0.15.0, 2026-09-06 [52] | codec | 5 [52] | Packets, encoding and validation |

The practical consequence for Rust: a mature MQTT 5 *client* story (rumqttc, paho-mqtt, rust-mqtt for embedded), and **two pure-Rust
MQTT 5 brokers that a client can actually reach**, both measured rather than read off a checklist — `rumqttd` 0.20.0, whose checklist
says otherwise, and `rmqtt` 0.23.1, which declares the full capability set [65][66]. `ntex-mqtt` remains a framework for building a
broker rather than one [47]. What neither Rust broker offers is 4.12's AUTH exchange, so that part of the protocol has no pure-Rust
peer to be measured against at all [65][66].

**Brokers.**

| Broker | Language | Licence | Coverage | Notable gaps |
| --- | --- | --- | --- | --- |
| Eclipse Mosquitto (v2.1.2 current [53]) | C | see repository licence files | 5.0, 3.1.1, 3.1 [53]; "full MQTT v5.0 support" claimed [32] | Supports but "does not make use of" Server Redirection, and does not use Reason String [32]. Ships the non-standard `upgrade_outgoing_qos` and `queue_qos0_messages`, both so marked in its own man page [31] |
| EMQX (6.x) | Erlang | Business Source License 1.1 from 5.9.0 — not Apache-2.0 [54] | 5.0, 3.1.1, 3.1, plus MQTT-SN, CoAP, LwM2M, MQTT over QUIC; masterless clustering [54] | Durable sessions off by default; regular sessions RAM-only and lost on node restart [19]. QUIC mode does not preserve session state [21] |
| HiveMQ CE (2026.5) | Java | Apache-2.0 [55] | "all MQTT 3.1/3.1.1/5.0 features" over TCP, TLS, WebSocket, secure WebSocket; embeddable [55] | Downgrades shared-subscription QoS 2 to QoS 1 because it "cannot guarantee QoS 2 for shared subscriptions" [16] |
| VerneMQ (2.2.0, 2026-08-09) | Erlang/OTP | Apache-2.0 [56] | 3.1/3.1.1/5.0, QoS 0/1/2, clustering, shared subscriptions; MQTT 5 inventory covers enhanced auth, expiration, will delay, shared subscriptions, request/response, topic aliases, flow control, subscription flags, subscription identifiers, all property types [56] | Actively released — 2.2.0 carries OTP 28/29 work [57] — so the recurring claim that VerneMQ is abandoned is not supported by its release history |
| NanoMQ (LF Edge) | C on NNG | MIT [58] | "full MQTT 3.1/3.1.1 and 5.0 support" [58] | README explicitly lists MQTT 5 **AUTH** and **Server Redirection** as unsupported [58] — a hard boundary for enhanced-authentication clients. QUIC bridging needs a compile-time flag [59] |

**History and documented adoption.** Arlen Nipper (Arcom Control Systems) and Andy Stanford-Clark (IBM) sketched the first version of
MQTT at the start of 1999 to replace proprietary industrial poll/response protocols [63]; the specification thanks both as "the original
inventors" (Appendix A) [1]. Facebook announced MQTT in its Messenger app in 2011, citing low bandwidth and battery use [64][63]. AWS
IoT Core and Azure IoT Hub both expose MQTT endpoints [60][61].

**Known incompatibilities between implementations.**

- **AWS IoT Core** supports MQTT 3.1.1 and MQTT 5 with documented differences and **QoS 0 and 1 only** — no PUBREC, PUBREL or PUBCOMP,
  and QoS 2 publish and subscribe are rejected. It also does not support MQTT 5 **AUTH** or **server redirection**. RETAIN is supported
  with one retained message per exact topic, but a wildcard subscription does **not** receive an existing retained message on subscribe.
  An MQTT 3 persistent session cannot be resumed as MQTT 5 or vice versa [60].
- **Azure IoT Hub** currently documents MQTT v3.1.1 and v3.1.1-over-WebSocket only, and Microsoft states directly that IoT Hub "is not a
  full-featured MQTT broker". QoS 2 is unsupported and a QoS 2 device publish closes the connection; RETAIN is not persisted but
  converted into an `mqtt-retain` application property [61]. The MQTT 5 preview is deprecated and lacked subscription identifiers,
  shared subscriptions, RETAIN, assigned client identifiers and response information, capping Maximum QoS at 1, Maximum Packet Size at
  256 KiB, Topic Alias Maximum at 10, Receive Maximum at 16, Keep Alive at 19 minutes and each client at 50 subscriptions [62].
- **Shared-subscription semantics differ substantively:** selection strategy (EMQX seven strategies, `round_robin` default [20] versus
  HiveMQ random and explicitly not round-robin [16]); filter syntax (EMQX additionally accepts and now deprecates `$queue/<topic>` [20],
  undocumented by HiveMQ [16]); QoS ceiling (HiveMQ silently downgrades shared QoS 2 to QoS 1 [16]); and redispatch of unacknowledged
  messages (EMQX documents it only on persistent-session *expiry*, not on member disconnect [20], where the specification permits
  immediate reassignment for QoS 1, 4.8.2 [1]).
- **Deliberate spec deviations shipped as configuration:** Mosquitto's `upgrade_outgoing_qos` makes delivery QoS equal subscription QoS,
  contradicting the downgrade-only rule, and its own man page says so; `queue_qos0_messages` queues QoS 0 for offline clients, also
  marked non-standard [31]. Paho's Python client republishes QoS > 0 messages after a network reconnect even with `clean_session=True`,
  states that this contradicts the standard, and warns that QoS 2 messages can therefore arrive twice [38].
- **Client-side session durability is often absent:** Paho Python's clean-session-false session is memory-only, so a process restart
  loses incomplete inbound QoS 2 and unacknowledged outbound QoS 1/2 [38]; Paho Java's `MemoryPersistence` is documented as unsuitable
  where reliability across restart is required [41]; Paho C's `MQTTCLIENT_PERSISTENCE_NONE` is memory-based "and can lose messages"
  [36]. Where a store is used, its contract is coarse: Paho Java's `MqttClientPersistence` holds inbound and outbound in-flight
  messages, and a failed `put` means the data is assumed absent while a failed `remove` means it is assumed still stored [40].
- **Automatic reconnect is off by default in Paho C** and must be enabled with `automaticReconnect` [34], and disconnected publishing
  additionally needs `sendWhileDisconnected` plus a bounded `maxBufferedMessages` (default 100) [35] — a client that assumes buffering
  gets none.
- **MQTT over QUIC is not interoperable across brokers** in any standard sense: an EMQX transport [21] and a NanoMQ bridge to EMQX [59].
- **Two Rust brokers disagree with the specification in six measured places**, each observed on 2026-09-12 against a named version
  over loopback TCP by a client written against [1] and exercising both roles [65][66]. `rumqttd` 0.20.0: (1) a delivery arrives at the
  subscription's **granted maximum** rather than at the minimum of that and the publish's QoS, so a QoS 0 publication reaching a QoS 2
  subscription arrives at QoS 2 — an *upgrade*, which [MQTT-3.8.4-8] forbids in either direction; the downgrade direction is correct,
  which is what shows the implementation applies the granted value rather than taking a minimum [65]. (2) Retain Handling 2, "do not
  send retained messages at the time of the subscribe" (3.8.3.1), sends them [65]. (3) DISCONNECT 0x04 (Disconnect with Will Message)
  publishes nothing, while 0x00's discard works — so that build implements one half of [MQTT-3.14.4-3]'s pair [65]. (4) An UNSUBACK
  carries **one** reason code however many Topic Filters the UNSUBSCRIBE carried, and none at all where nothing was removed, against
  [MQTT-3.11.3-1]'s one per filter in the order sent; since the acknowledgement carries no filters, position is the only binding between
  a code and a filter and a disagreeing count is unreadable rather than merely surprising [65]. `rmqtt` 0.23.1: (5) a retained message
  **is** sent to a shared subscription, which 4.8.2 forbids [66]. (6) A Client Identifier takeover closes the older connection's
  transport with no DISCONNECT at all, so the reason code [MQTT-3.1.4-3] exists to deliver — 0x8E, Session taken over — is exactly what
  is missing; that is the 3.1.1 behaviour of section 1.9 from a broker claiming full 5.0 support [66].
- **Capability declarations differ enormously between implementations claiming the same version.** `rumqttd` 0.20.0's CONNACK declares
  `Topic Alias Maximum` 4096 and nothing else, so every other §11 default applies by absence; its own README checklist leaves MQTT 5
  unchecked while its generated configuration enables a v5 listener by default and it honours `Subscription Identifier` on every
  delivery [65]. `rmqtt` 0.23.1 declares `Maximum QoS` 2, `Retain Available` 1, `Receive Maximum` 8, `Maximum Packet Size` 1,048,576,
  `Topic Alias Maximum` 16, all three subscription-availability flags and `Server Keep Alive` 30 s [66]. A client cannot tell an absent
  property from one equal to its default, so the *refusal* paths of the availability flags are unreachable against a broker that
  declares nothing — which makes the declarations themselves an interoperability surface rather than a formality [65][66].
- **Two of `rmqtt` 0.23.1's availability flags are not listener settings but plugin state.** `Retain Available` and
  `Shared Subscription Available` are reported 0 unless the `rmqtt-retainer` and `rmqtt-shared-subscription` plugins are started, so
  they are global to the broker process rather than per listener and cannot differ between two listeners of one broker; `Maximum QoS`
  and `Topic Alias Maximum` are per listener [66].
- **Neither Rust broker offers an enhanced-authentication mechanism**, so 4.12's AUTH exchange is unexercised against both.
  `rmqtt` 0.23.1 ships JWT and HTTP authentication plugins, and both authenticate the CONNECT's User Name and Password rather than
  running the AUTH dialog [66]. Neither offers `Response Information`, so the Response Topic namespace of 4.10 is unmeasured against
  either [65][66].
- **Installing either broker from crates.io fails in opposite directions on rustc 1.98.** `cargo install rumqttd --version 0.20.0`
  fails **with** `--locked`, because its `Cargo.lock` pins a `metrics` version whose registry accessor no longer passes borrow checking
  (E0521); without `--locked` the resolver picks a newer `metrics` and it builds [65]. `cargo install rmqttd --version 0.23.1` fails
  **without** `--locked`, because a resolved `pulsar` gained a field its egress-bridge plugin does not set (E0063); with `--locked` it
  builds [66].

**Two internal inconsistencies in [1] itself,** recorded because implementers hit them. First, Appendix B's restatement of
[MQTT-3.3.1-10] inverts the Retain Handling = 1 rule: it says retained messages MUST be sent "if the subscription did already exist" and
MUST NOT be sent "if the subscription did not exist", whereas the normative body in 3.3.1.3 says the opposite, and 3.8.3.1 agrees with
the body ("1 = Send retained messages at subscribe only if the subscription does not currently exist"). The body is authoritative; the
Appendix B row is erroneous. Second, Appendix B's [MQTT-3.1.3-1] gives the CONNECT payload field order as "Client Identifier, Will Topic,
Will Message, User Name, Password", omitting Will Properties, while section 3.1.3 requires "Client Identifier, Will Properties, Will
Topic, Will Payload, User Name, Password" — the appendix retains 3.1.1 wording. Appendix B is labelled non-normative and chapter 7 is
"a definitive list of conformance requirements", so both discrepancies resolve in favour of the body [1].

**Both are now confirmed by an implementation that had to choose between the readings** [65][66]. A client written against the
normative body sends CONNECT with the payload order of 3.1.3 — Client Identifier, Will Properties, Will Topic, Will Payload, User Name,
Password — and both `rumqttd` 0.20.0 and `rmqtt` 0.23.1 accept it and answer CONNACK 0x00, including with a Will carrying all four of
its properties; a CONNECT built to Appendix B's order would have placed the Will Topic where the Will Properties' length byte belongs
and could not have been parsed [65][66]. And Retain Handling 1 read as the body reads it — send the retained message only if the
subscription did *not* already exist — is what `rmqtt` 0.23.1 does: a first subscribe to a filter over a stored value delivers it and a
second subscribe of the same filter on the same session delivers nothing [66]. Appendix B's inverted row would predict the opposite
order of events. Neither discrepancy is therefore a live ambiguity in practice; both are appendix errata.

**Two further tensions that only writing a client exposes**, both in the normative body rather than in an appendix.

First, **[MQTT-3.2.2-4] is unusable read against 4.1's enumeration.** The conformance statement obliges a client that "receives Session
Present 1 where it has no Session State" to close the connection, and 4.1's list of *client-side* session state is only the
unacknowledged QoS 1 and 2 exchanges — the outbound ones awaiting acknowledgement and the inbound QoS 2 identifiers awaiting release.
Read literally, a client that connected with Clean Start 0 and a non-zero Session Expiry Interval, published nothing, disconnected and
reconnected receives Session Present 1 with no session state by 4.1's definition, and is obliged to close on its own correct
resumption. An implementation must therefore track something 4.1 does not list — whether a session exists on the server at all, which
follows from whether the last CONNECT declared a non-zero Session Expiry Interval — and only then does the statement discriminate: a
session that declared no expiry ends with its connection (3.1.2.11.2), so Session Present 1 afterwards really is the forbidden row
[65]. The practical consequence is not a corner case: a client process that restarts with no persisted session state and reconnects
with Clean Start 0 finds the broker legitimately still holding its session, has nothing to match it against, and must close. Measured
against `rumqttd` 0.20.0, which holds the session and reports Session Present 1 truthfully; the only available answer is Clean Start 1
[65]. This is the client-side counterpart of the durability gap section 13 records for the Paho families [38][40][41].

Second, **`Receive Maximum` is two independent numbers under one name, and reading it as one dissolves a bound.** 3.1.2.11.3's
`Receive Maximum` in CONNECT is what the *server* may have in flight toward the client; 3.2.2.3.3's in CONNACK is what the *client* may
have in flight toward the server, and [MQTT-4.9.0-1] makes only the second the sender's initial send quota. An implementation holding
one field for both loses the inbound ceiling whenever a CONNACK merely omits the property: absence means 65,535 (3.2.2.3.3), so a
client that declared 2 and received a CONNACK without the property would silently raise its own limit from 2 to 65,535 and no longer
earn the DISCONNECT 0x93 of section 5 for a server that exceeded it. The two are measurably distinct against a broker that declares
one: `rmqtt` 0.23.1 configured with `max_inflight` 8 declares `Receive Maximum` 8, and a client declaring 200 in its CONNECT ends the
handshake with a send quota of 8 and an inbound ceiling of 200 [66]. Nothing in [1] flags the collision; the two uses are simply eleven
pages apart.

## 14. Sources

Official specification first, official guide second, maintainer-written material third. HiveMQ and EMQ blog articles are
maintainer-written vendor material and are marked as such where cited.

1. *MQTT Version 5.0*, OASIS Standard, 07 March 2019. <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html> — every normative statement in this sheet, plus Appendix B and Appendix C.
2. *MQTT Version 3.1.1*, OASIS Standard, 29 October 2014. <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html> — the 3.1.1 baseline in section 1.9.
3. *ISO/IEC 20922:2016 — MQTT v3.1.1*, ed. 1, published 2016-06, confirmed 2025. <https://www.iso.org/standard/69466.html> — ISO/IEC 20922 covers 3.1.1, not 5.0.
4. HiveMQ, *MQTT Topics, Wildcards, & Best Practices — MQTT Essentials Part 5*, 2019-08-20. <https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/> — named topic-design rules; the `$SYS` convention.
5. HiveMQ, *What is MQTT Quality of Service (QoS) 0,1, & 2? — MQTT Essentials Part 6*, 2015-02-16. <https://www.hivemq.com/blog/mqtt-essentials-part-6-mqtt-quality-of-service-levels/> — QoS explanations, DUP, QoS 2 cost. Contains the outdated retransmission claim corrected by [14].
6. HiveMQ, *Understanding Persistent Sessions and Clean Sessions — MQTT Essentials Part 7*, 2015-02-23. <https://www.hivemq.com/blog/mqtt-essentials-part-7-persistent-session-queuing-messages/> — persistent-session state; offline queueing.
7. HiveMQ, *What are Retained Messages in MQTT? — MQTT Essentials Part 8*, 2015-03-02. <https://www.hivemq.com/blog/mqtt-essentials-part-8-retained-messages/> — retained messages as last-known-state snapshot; caveats.
8. HiveMQ, *What is MQTT Last Will and Testament (LWT)? — MQTT Essentials Part 9*, 2015-03-09. <https://www.hivemq.com/blog/mqtt-essentials-part-9-last-will-and-testament/> — the retained-LWT presence recipe.
9. HiveMQ, *What Is MQTT Keep Alive and Client Take-Over? — MQTT Essentials Part 10*, 2015-03-16. <https://www.hivemq.com/blog/mqtt-essentials-part-10-alive-client-take-over/> — Keep Alive behaviour, the 1.5x rule, client-side advice.
10. HiveMQ, *MQTT Session Expiry and Message Expiry Intervals — MQTT 5 Essentials Part 4*, 2019-10-16. <https://www.hivemq.com/blog/mqtt5-essentials-part4-session-and-message-expiry/> — expiry semantics; session expiry overrides message expiry; the 4,294,967,295 s ceiling.
11. HiveMQ, *MQTT Shared Subscriptions — MQTT 5 Essentials Part 7*, 2019-11-05. <https://www.hivemq.com/blog/mqtt5-essentials-part7-shared-subscriptions/> — shared subscriptions; its round-robin claim is superseded by [16].
12. HiveMQ, *MQTT Request-Response Pattern — MQTT 5 Essentials Part 9*, 2019-11-21. <https://www.hivemq.com/blog/mqtt5-essentials-part9-request-response-pattern/> — Response Topic, Correlation Data, Response Information.
13. HiveMQ, *MQTT Flow Control — MQTT 5 Essentials Part 12*, 2020-06-16. <https://www.hivemq.com/blog/mqtt5-essentials-part12-flow-control/> — Receive Maximum, default 65,535, DISCONNECT 0x93.
14. HiveMQ, *Debunking Common MQTT QoS Misconceptions*, 2024-10-10. <https://www.hivemq.com/blog/debunking-common-mqtt-qos-misconceptions/> — MQTT 5 forbids retransmission during a live session; the min() downgrade rule.
15. HiveMQ, *Understanding MQTT Message Ordering*, 2025-08-06. <https://www.hivemq.com/blog/understanding-mqtt-message-ordering/> — ordering scope, QoS-class ordering, shared-subscription ordering loss, clustered-session claims.
16. HiveMQ, *Shared Subscriptions in MQTT* (product docs), undated, accessed 2026-09. <https://docs.hivemq.com/hivemq/latest/user-guide/shared-subscriptions.html> — random (not round-robin) distribution; 500,000-message shared queue; QoS 2 to QoS 1 downgrade.
17. HiveMQ, *HiveMQ MQTT Broker Configuration*, undated, accessed 2026-09. <https://docs.hivemq.com/hivemq/latest/user-guide/configuration.html> — `max-queue-size` 1000 with `discard` default, `max-packet-size` 268,435,460, `server-receive-maximum` 10, expiry caps.
18. EMQX, *MQTT Configuration*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/configuration/mqtt.html> — `max_inflight` 32, `retry_interval` 30s, `max_awaiting_rel` 100, `await_rel_timeout` 300s, `max_mqueue_len` 1000, `mqueue_store_qos0` true, `mqueue_priorities` disabled, `session_expiry_interval` 2h, `max_session_expiry_interval` infinity, `upgrade_qos` false, `max_packet_size` 1MB.
19. EMQX, *MQTT Durable Sessions*, latest docs (durable sessions introduced v5.7.0), undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/durability/durability_introduction.html> — RAM-only regular sessions; RocksDB plus Raft durable storage; no undelivered-message ceiling; throughput and latency cost.
20. EMQX, *MQTT Shared Subscription*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/messaging/mqtt-shared-subscription.html> — `$share` and deprecated `$queue` syntax; seven dispatch strategies, `round_robin` default; redispatch on persistent-session expiry; queue-growth warning.
21. EMQX, *MQTT over QUIC* (introduction), latest docs (states EMQX 5.0), undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/introduction.html> — not an MQTT standard; session state and QoS 1/2 state not preserved.
22. EMQX, *MQTT over QUIC: Features and Benefits*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/features-mqtt-over-quic.html> — single- and multi-stream modes; control versus data streams; packet-to-stream binding; no documented negotiation.
23. EMQX, *Use MQTT over QUIC*, latest docs (Docker example EMQX 5.8.8), undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/getting-started.html> — listener off by default, `0.0.0.0:14567`, TCP fallback.
24. EMQX, *Message Retransmission* (EMQX 5.0 design docs), undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/v5.0/design/retransmission.html> — Ordered Topic language; in-flight window 1 for strict ordering; retry-induced duplicate interleaving.
25. EMQ, *How Multi-Stream of QUIC Could Mitigate the HOL Blocking Issue of MQTT Connection*, 2025-04-18. <https://www.emqx.com/en/blog/multi-stream-of-mqtt-over-quic> — per-stream ordering; order not assured across streams.
26. EMQ, *MQTT QoS 0, 1, 2 Explained: A Quickstart Guide*, 2026-05-13. <https://www.emqx.com/en/blog/introduction-to-mqtt-qos> — QoS 2 packet count and overhead; the "about half the throughput" vendor claim; sequence numbers for strict ordering.
27. EMQ, *MQTT Persistent Session and Clean Session Explained*, 2022-12-27. <https://www.emqx.com/en/blog/mqtt-session> — server-side session-state inventory.
28. EMQX, *MQTT Retained Message*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/messaging/mqtt-retained-message.html> — one retained message per topic; delivery on matching subscribe; empty payload clears.
29. EMQX, *MQTT Will Message*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/messaging/mqtt-will-message.html> — will on accidental disconnect; a retained will becomes a retained message.
30. EMQX, *System Topic*, latest docs, undated, accessed 2026-09. <https://docs.emqx.com/en/emqx/latest/observability/mqtt-system-topics.html> — `$SYS/brokers/{node}/...`; localhost-only default ACL.
31. *mosquitto.conf(5)* man page, published 2026-02-09. <https://mosquitto.org/man/mosquitto-conf-5.html> — every Mosquitto limit and default cited in sections 5, 8, 11 and 13; persistence and autosave; the complete bridge directive set; the non-standard `upgrade_outgoing_qos` and `queue_qos0_messages`; `sys_interval`; `retain_expiry_interval`; `max_topic_alias`; `bridge_receive_maximum`; `persistent_client_expiration`; `max_keepalive`.
32. *mosquitto(8)* man page, published 2026-02-02. <https://mosquitto.org/man/mosquitto-8.html> — supported versions; the MQTT 5.0 feature list including shared subscriptions; the statements that Mosquitto does not make use of Server Redirection or Reason String; the `$SYS` hierarchy including `shared_subscriptions/count`.
33. *mqtt(7)* man page, published 2026-01-29. <https://mosquitto.org/man/mqtt-7.html> — Mosquitto's QoS downgrade examples; "last known good" retained framing; durable connections.
34. Eclipse Paho, *Asynchronous MQTT C Client Library: Automatic Reconnect*, generated 2018-09-13. <https://eclipse.dev/paho/files/mqttdoc/MQTTAsync/html/auto_reconnect.html> — reconnect off unless enabled; 1..60 s doubling; connected callback for resubscription.
35. Eclipse Paho, *Asynchronous MQTT C Client Library: Publish While Disconnected*, generated 2018-09-13. <https://eclipse.dev/paho/files/mqttdoc/MQTTAsync/html/offline_publish.html> — `sendWhileDisconnected`, `maxBufferedMessages` default 100.
36. Eclipse Paho, *MQTTClientPersistence.h File Reference*, generated 2018-09-13. <https://eclipse.dev/paho/files/mqttdoc/MQTTAsync/html/_m_q_t_t_client_persistence_8h.html> — persistence modes; `NONE` is memory-based and can lose messages.
37. Eclipse Paho, *paho-mqtt Python: client module*, undated, accessed 2026-09. <https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html> — `reconnect_on_failure` default true; 1..120 s doubling; clean-start handling; what `publish()` success means; `manual_ack`; subscribe in `on_connect`.
38. Eclipse Paho, *paho.mqtt.python README*, undated, accessed 2026-09. <https://github.com/eclipse-paho/paho.mqtt.python> — memory-only session state; the self-declared non-compliant republish behaviour and its QoS 2 duplicate warning.
39. Eclipse Paho, *MqttConnectOptions* (Java v3 Javadoc), 2016-09-01. <https://eclipse.dev/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttConnectOptions.html> — reconnect backoff 1 s doubling to 2 minutes; constructor defaults.
40. Eclipse Paho, *MqttClientPersistence* (Java v3 Javadoc), 2016-09-01. <https://eclipse.dev/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttClientPersistence.html> — what the store holds; its failure contract.
41. Eclipse Paho, *MemoryPersistence* (Java v3 Javadoc), 2016-09-01. <https://eclipse.dev/paho/files/javadoc/org/eclipse/paho/client/mqttv3/persist/MemoryPersistence.html> — unsuitable where reliability across restart is required.
42. bytebeamio, *rumqtt* repository, undated, accessed 2026-09. <https://github.com/bytebeamio/rumqtt> — workspace roles; Apache-2.0; the checklist showing rumqttd without MQTT 5 and with QoS 2 complete.
43. *rumqttc CHANGELOG*, rumqttc 0.25.1, 2025-11-21. <https://github.com/bytebeamio/rumqtt/blob/main/rumqttc/CHANGELOG.md> — v5 evolution across 0.21.0 and 0.25.0; AUTH packet support; QoS 2 identifier tracking.
44. *rumqttc* crate documentation, undated, accessed 2026-09. <https://docs.rs/rumqttc/latest/rumqttc/> — Tokio event loop; the polling obligation; outgoing-packet throttling listed "todo".
45. *Eclipse Paho MQTT Rust Client Library*, undated, accessed 2026-09. <https://github.com/eclipse-paho/paho.mqtt.rust> — wraps Paho C >= 1.3.16; MQTT 5/3.1.1/3.1; QoS 0/1/2; runtime-agnostic; build requirements.
46. *paho.mqtt.rust Release Version 0.14.0*, 2026-03-26. <https://github.com/eclipse-paho/paho.mqtt.rust/releases/tag/v0.14.0> — latest release; v5 reason-code error handling fixes.
47. ntex-rs, *ntex-mqtt* repository, undated, accessed 2026-09. <https://github.com/ntex-rs/ntex-mqtt> — Apache-2.0 client and server framework with v3.1.1 and v5 codecs.
48. *ntex-mqtt Release v8.2.1*, 2026-06-19. <https://github.com/ntex-rs/ntex-mqtt/releases/tag/v8.2.1> — latest release.
49. *rust-mqtt* on crates.io, 0.5.1, 2026-04-10. <https://crates.io/crates/rust-mqtt> — `no_std` async client on `embedded_io_async`; MIT OR Apache-2.0.
50. obabec, *rust-mqtt* repository, undated, accessed 2026-09. <https://github.com/obabec/rust-mqtt> — MQTT 5.0 only; deliberate omission of reconnect, keep-alive loops, retry and background tasks.
51. *mqtt-protocol* on crates.io, 0.12.0, 2024-03-13. <https://crates.io/crates/mqtt-protocol> — codec-only crate and its release date.
52. *mqtt5-protocol* on crates.io, 0.15.0, 2026-09-06. <https://crates.io/crates/mqtt5-protocol> — MQTT v5 packets, encoding, validation.
53. *Eclipse Mosquitto* repository, undated, accessed 2026-09. <https://github.com/eclipse-mosquitto/mosquitto> — C server for MQTT 5.0/3.1.1/3.1 plus client library and CLI clients; v2.1.2 listed current.
54. *EMQX* repository, undated, accessed 2026-09. <https://github.com/emqx/emqx> — Erlang; MQTT-SN, CoAP, LwM2M and QUIC alongside MQTT; masterless clustering; Business Source License 1.1 from 5.9.0.
55. *HiveMQ Community Edition* repository, undated, accessed 2026-09. <https://github.com/hivemq/hivemq-community-edition> — Apache-2.0 Java broker; all MQTT 3.1/3.1.1/5.0 features; TCP/TLS/WebSocket; embedded mode; 2026.5 current.
56. *VerneMQ* repository, undated, accessed 2026-09. <https://github.com/vernemq/vernemq> — Apache-2.0 Erlang/OTP distributed broker; MQTT 3.1/3.1.1/5.0; the MQTT 5 feature inventory.
57. *VerneMQ Release 2.2.0*, 2026-08-09. <https://github.com/vernemq/vernemq/releases/tag/2.2.0> — active maintenance in 2026, including OTP 28/29 work.
58. LF Edge, *NanoMQ* repository, undated, accessed 2026-09. <https://github.com/nanomq/nanomq> — MIT C edge broker on NNG; full MQTT 3.1/3.1.1/5.0 claim; MQTT 5 AUTH and Server Redirection listed unsupported.
59. NanoMQ, *MQTT over QUIC Bridge*, undated, accessed 2026-09. <https://nanomq.io/docs/en/latest/bridges/quic-bridge.html> — QUIC bridging requires `-DNNG_ENABLE_QUIC=ON`, off by default, bridges to EMQX 5, hybrid TCP fallback.
60. AWS, *MQTT — AWS IoT Core Developer Guide*, undated, accessed 2026-09. <https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html> — QoS 0 and 1 only; no PUBREC/PUBREL/PUBCOMP, AUTH or server redirection; retained-message semantics including no retained delivery to wildcard subscriptions; no cross-version session resumption.
61. Microsoft, *Use MQTT to communicate with Azure IoT Hub*, 2025-03-19, updated 2026-08-27. <https://learn.microsoft.com/en-us/azure/iot-hub/iot-mqtt-connect-to-iot-hub> — MQTT v3.1.1 and v3.1.1-over-WebSocket only; "not a full-featured MQTT broker"; no QoS 2; no RETAIN persistence, converted to `mqtt-retain`.
62. Microsoft, *Azure IoT Hub MQTT 5 support (preview)*, version 2.0, dated 2024-04-08, archived, updated 2025-03-19. <https://learn.microsoft.com/en-us/previous-versions/azure/iot/iot-mqtt-5-preview> — deprecated preview; its declared limits and missing features.
63. HiveMQ, *The Origin of MQTT* (history series, written by Eclipse Paho project lead Ian Craggs), 2024-06-20. <https://www.hivemq.com/blog/the-history-of-mqtt-part-1-the-origin/> — 1999 origin by Arlen Nipper and Andy Stanford-Clark; the poll/response replacement motivation; Facebook Messenger adoption.
64. Eclipse Mosquitto blog, *Facebook using MQTT*, 2011-08-17. <https://mosquitto.org/blog/2011/08/facebook-using-mqtt/> — contemporaneous record of the Facebook Messenger announcement and its stated bandwidth and battery motivation.
65. Measurement record, *MQTT 5.0 client against `rumqttd` 0.20.0*, 2026-09-12, Linux x86-64, rustc 1.98, loopback TCP. The broker is bytebeamio's `rumqttd` [42], installed from crates.io and configured with a single `[v5.1]` listener; the client is an independent MQTT 5.0 implementation written against [1], exercising publisher and subscriber roles simultaneously across thirteen scenarios. Used for: what that build declares in CONNACK; QoS 0, 1 and 2 completing end to end; the granted-maximum-versus-minimum delivery QoS; Retain Handling 0 and 2; retained storage and the zero-byte delete through all three states; the Will on an abnormal close and its discard on an orderly one; DISCONNECT 0x04; session resumption and Clean Start discard; `Subscription Identifier` echoed on deliveries; the `+` and `#` matching rules including `#` matching the parent level; the UNSUBACK reason-code count; the [MQTT-3.2.2-4] restart case; and the `--locked` build failure. Every claim attributed to this source was observed in that run and not inferred.
66. Measurement record, *MQTT 5.0 client against `rmqtt` 0.23.1*, 2026-09-12, Linux x86-64, rustc 1.98, loopback TCP. The broker is rmqtt, installed from crates.io and configured with two TCP listeners declaring different capabilities plus the `rmqtt-retainer` and `rmqtt-shared-subscription` plugins; the client is the same implementation as [65], across twelve scenarios. Used for: the full set of CONNACK declarations; `Receive Maximum` as the send quota against a differing client declaration; shared subscriptions splitting deliveries between group members; a retained message reaching a shared subscription; Topic Aliases in both directions against a declared maximum; Retain Handling 1 separated from 0; Session Expiry as a timer; every PUBLISH property forwarded unaltered and in order including repeated User Property names; SUBACK and UNSUBACK per-filter codes; the availability flags as refusals against the second listener; the Client Identifier takeover closing without a DISCONNECT; the plugin-versus-listener scope of two flags; and the `--locked` build requirement. Every claim attributed to this source was observed in that run and not inferred.
