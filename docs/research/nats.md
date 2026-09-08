# NATS Core and JetStream (server 2.14, client protocol)

## 0. Identity card

- **Name:** NATS is a subject-addressed messaging system; JetStream is its persistence layer. [1][2]
- **Versions researched:** NATS Server documentation 2.14 and the current client protocol reference, accessed 2026-09-08. [3][4]
- **Governing body:** NATS.io publishes the server, documentation, client protocol, and Architecture and Design Records (ADRs). [3][5]
- **Specification documents:** the client protocol reference and JetStream API/configuration documentation are the normative operational references used here. [3][6]
- **Reference implementation:** `nats-server` implements Core NATS, clustering, security, and JetStream. [3][7]
- **Wire type:** the client protocol is an ASCII text control protocol with length-prefixed opaque byte payloads. [4]
- **Primary client transport:** a client normally connects through a TCP/IP socket. [4]
- **Other client transports:** the server supports WebSocket client connections and, when embedded in Go, UNIX-domain sockets. [4]
- **TLS:** TLS can protect a client connection and can be required by the server. [4][17]
- **Related server protocols:** server configuration can expose MQTT and WebSocket listeners; these are server features rather than Core NATS wire commands. [18]

## 1. Connection and session lifecycle

- A client first establishes its transport connection to a NATS server. [4]
- The server sends `INFO` after accepting the connection. [4]
- `INFO` reports server identity, version, host/port, `max_payload`, protocol level, and feature/security fields. [4]
- An `INFO` can also carry `connect_urls`, permitting clients that support protocol level 1 to learn cluster topology changes asynchronously. [4]
- A server may send later asynchronous `INFO` messages; a capable client must handle them outside the initial handshake. [4]
- The client sends `CONNECT` with client metadata and selected capabilities. [4]
- `CONNECT` can carry token, user/password, JWT, NKey public key, and a signature of the server nonce. [4]
- If the server advertises TLS as required, the client must complete TLS before ordinary protocol exchange. [4]
- A server that gives a nonce expects the NKey client to sign that nonce in `CONNECT`. [4]
- `PING` and `PONG` are bidirectional protocol operations used for keep-alive. [4]
- Server configuration supplies `ping_interval` and `max_pings_out`; after too many outstanding client pings the server closes the stale connection. [7]
- A normal client protocol close has no dedicated `CLOSE` verb; ending the transport ends the connection and its subscriptions. [4]
- `UNSUB` removes a subscription before its connection closes, optionally after a message count. [4]
- Core NATS does not define durable client sessions or server-side resumption of subscriptions. [1][4]
- Client reconnection policy is a client-library policy, not a Core NATS wire guarantee. [4][20]
- JetStream durable consumers preserve delivery state at the server, so a reconnecting client can bind and continue that consumer. [2][6]
- Ephemeral consumers are server-created state with an inactivity lifetime and are not an application-durable reconnect contract. [6]
- In Lame Duck Mode, the server marks later `INFO` notifications with `ldm: true`, then drains clients before shutdown. [4][13]

## 2. Primitives

- A **server** accepts client connections and routes Core NATS subject interest. [1][4]
- A **connection** owns the client/server transport and carries `CONNECT`, subscriptions, publications, and pings. [4]
- A **subject** is a dot-tokenized name used for publish and subscription routing. [8]
- A **subscription** is connection-local interest identified on the wire by client-selected `sid`. [4]
- A subscription may be ordinary or a member of a named **queue group**. [4][10]
- A queue group is not a broker queue: it is a set of subscriptions sharing one eligible delivery per publication. [10]
- A **message** contains subject, optional reply subject, optional headers, and opaque payload bytes. [4]
- An **inbox** is a reply subject, conventionally under `_INBOX.`, on which a requester subscribes. [9]
- A **stream** is a server-side JetStream message store bound to one or more subject patterns. [2][6]
- A stream assigns a sequence number when it appends a matching published message. [2]
- A stream configuration holds storage, retention, limits, replica count, discard policy, and source or mirror settings. [6]
- A **consumer** is a stateful server-side view of one stream, with delivery position and acknowledgement state. [2][6]
- Consumers have independent positions over the same stored stream messages. [2]
- A durable consumer has a stable server-side name; an ephemeral consumer is named by the server and is removed after inactivity. [6]
- A push consumer has a delivery subject; a pull consumer has no delivery subject and is driven by client requests. [6][14]
- A pull consumer is durable in the documented pull-subscribe model. [14]
- A **key-value bucket** is a JetStream-based materialized key/value view, not an independent storage engine. [15]
- An **object store** is a JetStream-based object abstraction that chunks objects into stream messages. [16]

## 3. Message model

- `PUB <subject> [reply-to] <bytes>` frames a Core NATS payload with a byte count followed by CRLF-delimited bytes. [4]
- `HPUB` adds a header byte count and total byte count ahead of the header block and payload. [4]
- `MSG` delivers subject, `sid`, optional reply-to subject, byte count, and payload to a subscription. [4]
- `HMSG` is the header-bearing counterpart of `MSG`. [4]
- Header blocks begin with `NATS/1.0` and then use HTTP-like `name: value` lines, including multiple values for one name. [4][12]
- Header names preserve case from publisher to receiver. [4]
- The protocol assigns no payload schema or content type: payload bytes are application-defined. [4]
- A Core NATS message has no broker-assigned universal message identifier. [4]
- A JetStream stream assigns a monotonically advancing stream sequence to each stored message. [2][6]
- JetStream delivery carries an acknowledgement reply subject encoding delivery metadata for the consumer. [14]
- The server advertises its accepted payload ceiling through `INFO.max_payload`. [4]
- `max_payload` is a server configuration limit and defaults to 1 MiB in the server documentation. [7]
- Headers count within the `HPUB` total size and therefore within the server’s accepted message size. [4]
- A payload is complete before `PUB` or `HPUB` routing begins; Core NATS has no streaming-body delivery primitive. [4]
- `Nats-Msg-Id` is a JetStream publish header used for server-side duplicate publication detection. [11]

## 4. Patterns and topologies

### Subject publish/subscribe

- A publisher sends a message to one subject without naming a subscriber. [1][8]
- Every active ordinary subscription matching that subject receives one copy. [1]
- Subject tokens are separated by dots, such as `orders.created`. [8]
- `*` matches exactly one complete subject token. [8]
- `>` matches one or more trailing subject tokens and must be the final token. [8]
- Thus `orders.*` matches `orders.created` but not `orders.eu.created`; `orders.>` matches both. [8]
- Routing is interest-based: a publication with no matching active interest has no Core NATS recipient. [1]
- A JetStream stream can independently capture a publication when its configured subject pattern matches. [2][6]

### Queue groups

- Subscribers join a queue group by placing the group name between subject and `sid` in `SUB`. [4]
- For each publication, the server selects one eligible member from each matching queue group. [10]
- Multiple queue groups each receive one copy, while ordinary subscriptions each receive one copy. [10]
- The documented selection goal is distributed load balancing among group members. [10]
- Queue membership is tied to the subscription and ends when it unsubscribes or its connection ends. [4][10]

### Request/reply and scatter-gather

- A requester subscribes to a unique inbox and publishes a request whose reply-to field is that inbox. [4][9]
- A responder receives the reply subject alongside the request and publishes its response to that subject. [4][9]
- The reply is therefore routed by ordinary subject interest, not by a hidden correlation field. [4]
- Client request APIs create and manage an `_INBOX` reply subscription and impose a caller-selected timeout. [9][20]
- A requester can receive replies from multiple responders during its collection window; NATS documents this as scatter-gather. [9]
- A request with no responders can receive a fast no-responder status when the client enables `no_responders` and headers. [4][9]

### JetStream stream and consumer topologies

- A stream binds one or more subject patterns to one physical or memory-backed message store. [2][6]
- Limits retention retains messages until configured limits remove them. [6]
- Interest retention removes messages once no consumer needs them. [6]
- Work-queue retention removes a message after the consuming acknowledgement, and overlapping consumer filters are constrained to prevent multiple workers taking it. [6]
- A stream’s discard policy chooses whether a full stream rejects new messages (`DiscardNew`) or evicts old messages (`DiscardOld`). [6]
- A consumer may filter one subject or configured multiple filter subjects, making one stateful view selective. [6][5]
- Deliver policies choose the initial point: all, last, new, by start sequence, by start time, or last per subject. [6]
- A push consumer sends to its delivery subject whenever it has eligible messages. [6]
- A pull consumer waits for `CONSUMER.MSG.NEXT` requests and replies with up to the requested batch. [14]
- A mirror maintains a read-only copy of one origin stream. [6][19]
- A source imports messages from another stream into a destination stream; unlike a mirror, a stream can have multiple sources and local messages. [6][19]

### Server, cluster, gateway, and leaf-node topologies

- A Core NATS cluster is a set of servers connected by routes that distribute subscription interest and messages. [7][18]
- Clients connect to a server; the cluster routes according to distributed interest rather than having clients bind directly to peers. [4][18]
- JetStream metadata and replicated stream/consumer state use RAFT groups. [18][19]
- For a replicated stream, the leader appends a write and a quorum commit is required before the publish acknowledgement represents the replicated write. [18]
- A supercluster joins clusters across geographic or administrative boundaries through gateway connections. [18]
- Gateways exchange interest so cross-cluster traffic is sent only when there is remote interest. [18]
- A leaf node is a remote NATS server connection that extends subjects to edge clients while retaining local connectivity. [18]
- Leaf nodes can be deployed where edge clients or constrained links should connect locally and reach a central NATS system. [18]
- Server MQTT and WebSocket listeners bridge clients speaking those protocols into configured NATS server services. [18]

## 5. Flow control and backpressure

### Core NATS

- Core NATS has no publisher credit, publisher acknowledgement, or publisher-side flow-control protocol. [1][4]
- A successful socket write only establishes that the client sent bytes toward its connected server; it is not a delivery confirmation. [4]
- For each client connection, the server queues pending outbound data while it attempts to write to that client. [7]
- `max_pending` bounds pending outbound bytes per client connection; its documented default is 64 MiB. [7]
- When a slow consumer exceeds the pending limit, the server reports it as a slow consumer and disconnects that client. [7][21]
- The unaffected publisher is not backpressured by a Core NATS protocol credit grant. [1][7]
- A connection can also be cut by the configured `write_deadline` if writes cannot complete in time. [7]
- The server’s `/varz` reports a `slow_consumers` counter, while `/connz` exposes per-connection `pending_bytes`. [21]

### JetStream consumers

- `max_ack_pending` bounds unacknowledged delivered messages for explicit or all-ack consumers. [6]
- When that consumer-level count is reached, further delivery pauses until acknowledgements advance it. [6]
- The default documented `max_ack_pending` is 1,000 messages. [6]
- Pull consumers let workers choose the requested batch and the request expiration. [14][22]
- A fetch returns when its batch fills or its expiration passes, whichever occurs first. [22]
- A no-wait pull returns immediately with a message or a status such as no messages. [14]
- `max_waiting` bounds outstanding pull requests; the documented default is 512. [14]
- Push consumers can enable flow control, under which the server sends a flow-control status request and waits for the client reply before continuing. [6]
- Push consumers can send idle-heartbeat status messages when no regular messages arrive. [6][23]
- Flow control and idle heartbeats apply to push consumers, not ordinary Core NATS subscriptions. [6]
- Consumer delivery is also bounded by stream limits, consumer `max_deliver`, acknowledgement policy, and client/server pending buffers. [6][7]

## 6. Delivery guarantees and acknowledgement

### Core NATS

- Core NATS is at-most-once: an active matching subscriber receives a publication at most once. [1]
- Core NATS stores neither a publication nor subscriber delivery state for replay. [1]
- `+OK` in verbose mode acknowledges a well-formed protocol operation, not application processing or subscriber receipt. [4]
- `-ERR` reports a protocol or authorization error and can be followed by disconnect. [4]
- Core NATS has no publisher delivery acknowledgement and no consumer acknowledgement. [1][4]

### JetStream publish acknowledgement

- A JetStream publish is a Core NATS publish to a stream-covered subject plus a request for the JetStream publish acknowledgement. [2][6]
- The publish acknowledgement reports stream name, stream sequence, and duplicate status. [6][11]
- With stream replication, the acknowledgement follows the stream leader’s quorum commit, not merely reception by the client’s connected server. [18]
- A publish acknowledgement certifies JetStream accepted and committed that stream message according to the configured replication quorum. [18][19]
- It does not certify that any consumer received or processed the message. [2][6]

### JetStream consumer acknowledgement

- `AckNone` needs no consumer acknowledgement and treats delivery as complete on delivery. [6]
- `AckAll` acknowledges the received message and all earlier pending messages for that consumer. [6]
- `AckExplicit` requires individual acknowledgement and is the recommended policy for at-least-once processing. [6]
- `+ACK` confirms successful processing and advances the consumer acknowledgement state. [6]
- `-NAK` asks for redelivery; an optional delay postpones that redelivery. [6]
- `+WPI` is an in-progress acknowledgement that resets the acknowledgement timer while processing continues. [6]
- `+TERM` terminates delivery of that message to the consumer without marking it successfully processed. [6]
- If a required acknowledgement does not arrive before `ack_wait`, JetStream schedules redelivery. [2][6]
- `max_deliver` bounds delivery attempts; exhaustion is surfaced as an advisory rather than automatic successful acknowledgement. [6]
- Redelivery and failed acknowledgement transmissions make duplicate delivery normal for at-least-once consumers. [2][6]

### De-duplication and NATS exactly-once terminology

- JetStream can suppress duplicate publish attempts when producers set the same `Nats-Msg-Id` header within the stream’s duplicate window. [11][6]
- The duplicate window is stream configuration and is bounded in time, so an identifier reused after its window is not suppressed. [6][11]
- JetStream documentation calls the combination of publish de-duplication and double acknowledgements “exactly once semantics.” [6][11]
- The consumer double-ack mechanism waits for the server’s acknowledgement of the client acknowledgement before the client treats it as confirmed. [6]
- Those mechanisms cover accepted publish attempts and acknowledged consumer delivery state, not arbitrary external side effects. [6][11]
- [inference] An application writing to an external database still needs an idempotency or transactional strategy keyed by its own operation identity. [6][11]

## 7. Ordering and duplicates

- Core NATS preserves no documented global ordering across subjects, servers, or queue-group members. [1][4]
- A single Core NATS subscriber observes messages in the order its connected server writes eligible deliveries on that connection; failures and topology changes give no replay or recovery order. [4][7]
- A JetStream stream assigns sequence numbers in append order, providing the stream’s stored order. [2][6]
- A consumer advances through its selected stream view and filter subject(s) according to its delivery state. [6]
- A queue group changes which worker gets a publication, so it is not an ordering or affinity primitive. [10]
- JetStream redelivery can repeat a previously delivered message after later deliveries have occurred. [6]
- Multiple consumers intentionally receive independent copies of the same stream messages. [2]
- Producer retry after an uncertain publish acknowledgement can create duplicate stored messages without `Nats-Msg-Id` de-duplication. [11]
- Consumer retries can create duplicate processing even when the server sees an acknowledgement late or not at all. [6]
- `Nats-Msg-Id` detects duplicate publishes only within the configured duplicate window. [6][11]
- An ordered consumer is an ephemeral, no-ack consumer that recreates itself on a detected gap and resumes from the expected sequence. [24]
- Ordered consumers trade durable acknowledgement state and load sharing for a gap-repaired ordered view. [24]

## 8. Failure behaviour

| Event | What participants observe | Loss and ambiguity |
| --- | --- | --- |
| Core subscriber crash | Its connection and subscriptions disappear; later Core publications have no delivery to that subscriber. [1][4] | Messages published while absent are lost to it and cannot be replayed. [1] |
| Slow Core subscriber | The server accumulates per-connection pending bytes up to `max_pending`, reports slow-consumer state, then disconnects it. [7][21] | Messages queued for that disconnected connection are lost; publisher receives no delivery proof. [1][7] |
| Server restart without JetStream | Client TCP connections and Core subscriptions end. [1][4] | Core messages and subscription state are not persisted. [1] |
| Server restart with JetStream | Clients reconnect through their libraries; persisted streams and durable consumer state can resume from the server. [2][6] | Memory storage and non-durable consumer state do not provide the same persistence; an unconfirmed action remains ambiguous. [6] |
| JetStream RAFT leader change | A replica election replaces the leader; clients may see temporary request or publish disruption. [18][19] | A publish without its acknowledgement is ambiguous and must be retried with an idempotent message id if duplicate storage matters. [11][18] |
| Consumer `ack_wait` expiry | JetStream treats an explicit/all-policy delivery as unacknowledged and redelivers it, subject to `max_deliver`. [6] | The receiver may already have processed it, so processing is ambiguous and duplicate-capable. [6] |
| Message larger than `max_payload` | The server rejects a client protocol message exceeding the configured accepted payload size. [4][7] | It is not delivered or stored; the client receives a protocol error or disconnect behavior. [4] |
| Publish with no Core subscribers | The server has no active interest and the Core message goes nowhere. [1] | The publisher cannot infer a recipient from the normal publish; request no-responder support is a distinct opt-in path. [4][9] |
| Permission violation | The server denies a publish or subscription outside the authenticated user’s subject permissions. [17] | The operation is rejected; `-ERR` may be sent and the connection may be closed depending on the error. [4][17] |
| Lame Duck Mode | The server sends `INFO` with `ldm`, rejects new client connections, and drains existing clients before shutdown. [13][4] | A client must reconnect elsewhere; in-flight Core delivery remains non-durable. [1][13] |
| Network partition below JetStream quorum | The affected RAFT group cannot commit writes until a quorum is available. [18][19] | A client lacking a publish acknowledgement cannot know whether to retry; retries need de-duplication for safe storage. [11][18] |
| Pull request expires | A pull fetch ends with the messages already received or a timeout/status response. [14][22] | It does not acknowledge delivered messages; unacknowledged deliveries remain eligible for redelivery. [6][14] |

## 9. Reliability recipes

### Request/reply with timeouts

- **Problem:** a requester needs bounded waiting for a service response. [9]
- **Mechanism:** subscribe to an `_INBOX` reply subject, publish with that reply-to subject, and use a client-side timeout. [4][9]
- **Guarantee:** replies arriving while the inbox subscription exists route through the normal subject mechanism. [4][9]
- **Cost/failure:** a timeout cannot distinguish absent responders, network loss, slow processing, or a lost reply. [1][9]

### Scatter-gather

- **Problem:** collect independent answers from multiple responders. [9]
- **Mechanism:** publish one request with an inbox reply subject and keep collecting responses until the caller’s deadline. [9]
- **Guarantee:** every responding active subscription can reply to the same inbox. [4][9]
- **Cost/failure:** response count and completion are application decisions; a late response can miss the collection window. [9]

### Queue groups for scaling

- **Problem:** spread one subject’s work across workers. [10]
- **Mechanism:** make workers subscribe to the same subject and queue-group name. [4][10]
- **Guarantee:** one eligible member per group receives each live Core publication. [10]
- **Cost/failure:** a worker crash loses any Core message already sent to it and offers no retry. [1][10]

### JetStream explicit acknowledgement

- **Problem:** make messages survive an offline consumer and retry unacknowledged work. [2]
- **Mechanism:** store the subject in a stream and use a durable consumer with `AckExplicit`, `ack_wait`, and `max_deliver`. [2][6]
- **Guarantee:** stored messages can be replayed and unacknowledged deliveries are redelivered. [2][6]
- **Cost/failure:** consumers must tolerate duplicate delivery and configure bounded outstanding acknowledgements. [6]

### Idempotent JetStream publishing

- **Problem:** retry a publish after a lost acknowledgement without storing it twice. [11]
- **Mechanism:** set a stable `Nats-Msg-Id` and retry during the stream duplicate window. [6][11]
- **Guarantee:** JetStream identifies duplicate message IDs within that configured window. [11]
- **Cost/failure:** the window expires, and IDs do not make external consumer side effects idempotent. [6][11]

### Key-value last-value state

- **Problem:** expose the latest value for a named key. [15]
- **Mechanism:** use a JetStream-backed KV bucket, which stores revisions under key subjects and supports watching. [15]
- **Guarantee:** a reader can retrieve the latest stored revision and watchers can receive updates. [15]
- **Cost/failure:** it is not a general transaction system and readers must handle revision and watch semantics. [15]

### Mirrors for disaster recovery/read locality

- **Problem:** retain a read-only copy of a stream in another placement or domain. [19]
- **Mechanism:** configure a stream mirror of the origin stream. [6][19]
- **Guarantee:** the mirror tracks source stream messages as a read-only replica stream abstraction. [19]
- **Cost/failure:** replication lag and source reachability affect freshness; a mirror cannot accept independent local writes. [19]

### Leaf nodes for edge

- **Problem:** let edge deployments connect locally while extending central subjects. [18]
- **Mechanism:** connect an edge NATS server as a leaf node to the upstream NATS system. [18]
- **Guarantee:** leaf connectivity extends subject communication across that link. [18]
- **Cost/failure:** a disconnected leaf partitions edge and upstream interest; Core messages during the disconnection are not persisted merely by being Core NATS. [1][18]

## 10. Security and identity

- Authentication is per connection, before ordinary authorized publish/subscribe activity. [4][17]
- Supported server authentication mechanisms include token, username/password, TLS client certificates, NKeys, and JWT-based operator mode. [4][17]
- An NKey client signs the server-provided nonce without sending its private key. [4][17]
- JWTs identify operators, accounts, and users in the NATS decentralized security model. [17]
- An account is an isolated subject namespace and tenant boundary. [17]
- A user authenticates into one account. [17]
- Publish and subscribe permissions are subject-pattern grants and denies for that user/account. [17]
- Accounts can explicitly export and import selected subjects for cross-account communication. [17]
- An authorization callout allows the server to ask an external service to authenticate a client connection. [5][17]
- TLS protects transport confidentiality and integrity; mutual TLS can authenticate the client certificate. [17]
- The Core message wire format does not attach the authenticated connection identity to every delivered message. [4][17]
- [inference] If a receiver needs a per-message business identity, it must be carried and protected by the application or an agreed header convention, not inferred from Core message framing. [4][12][17]

## 11. Limits and resource bounds

- `max_payload` limits the maximum client payload accepted by a server and defaults to 1 MiB. [7]
- `max_pending` limits pending outbound bytes for each client and defaults to 64 MiB. [7]
- `max_connections` limits simultaneous client connections; the documented default is 65,536. [7]
- `write_deadline` bounds how long the server allows a client write to take; the documented default is 10 seconds. [7]
- `ping_interval` sets how often a server sends client pings; the documented default is 2 minutes. [7]
- `max_pings_out` sets the allowed unanswered pings before stale disconnect; the documented default is 2. [7]
- `max_control_line` bounds text protocol line size before payload framing. [7][4]
- Subscription limits and account limits can cap per-account connections, subscriptions, payloads, and JetStream resources. [7][17]
- Stream limits include maximum messages, bytes, age, messages per subject, consumers, and storage. [6]
- A stream’s storage can be memory or file, so capacity and restart behavior depend on the selected storage type. [6]
- Stream replica count is configurable; placement requires eligible JetStream servers and quorum availability. [6][18]
- `DiscardNew` rejects a write at full limits while `DiscardOld` removes older retained messages to admit a new one. [6]
- Consumer `max_ack_pending` bounds delivered unacknowledged messages. [6]
- Consumer `max_deliver` bounds attempts for one message. [6]
- Pull `max_waiting` bounds outstanding pull requests and defaults to 512. [14]
- The monitoring port is unauthenticated by default unless operators protect network access or add protection. [21]
- [inference] A hostile client can consume connection slots, subscriptions, pending buffers, pull requests, and configured JetStream storage until the corresponding server/account limits intervene. [6][7][14][17]

## 12. Answers to the problem catalogue

### P1 — Loss detection and safe retry

- Core NATS has no delivery receipt; a client timeout or disconnect leaves publication delivery unknown, and retry can duplicate application work. [1][4]
- JetStream supplies publish acknowledgements and `Nats-Msg-Id` de-duplication within the configured duplicate window; consumer applications use explicit acknowledgements and idempotent processing. [6][11]

### P2 — Dead or unreachable peer

- `PING`/`PONG`, configured `ping_interval`, and `max_pings_out` detect a stale client connection; the server disconnects after the configured unanswered-ping threshold. [4][7]
- Core subscriptions vanish with the connection and have no stored session; durable JetStream consumer state remains server-side. [1][2]

### P3 — Capacity-based work spreading

- Queue groups distribute each matching Core publication to one eligible member of the named group. [10]
- Pull consumers let workers request the next message or batch when they have capacity. [14][22]

### P4 — Slow consumer

- Core NATS buffers pending outbound bytes per connection up to `max_pending` and then disconnects the slow consumer. [7][21]
- JetStream retains stream messages and pauses a consumer at `max_ack_pending`; pull batches and expirations let workers bound requested work. [6][14]

### P5 — Late joiner/current state

- A JetStream consumer can start at all, last, new, a sequence, a time, or last-per-subject according to its deliver policy. [6]
- A KV bucket exposes the latest revision for a key and a watchable update history. [15]

### P6 — Broker failover

- Clients learn cluster `connect_urls` through `INFO`, and client libraries reconnect according to their own policies. [4][20]
- Core subscriptions must be re-established by the reconnecting client library; durable JetStream consumers can be rebound to retained state. [2][4][6]

### P7 — Restart survival

- Core NATS publications and subscription state do not survive a restart. [1]
- File-backed JetStream streams and durable consumer state persist subject to configured storage and RAFT quorum; a publish acknowledgement certifies the committed stream write. [6][18]

### P8 — Ordering scope

- JetStream stream sequence defines append order in that stream, while Core NATS promises no global order over subjects, servers, or queue workers. [2][4]
- Ordered consumers repair detected gaps for an ephemeral ordered view rather than supplying a durable shared worker ordering contract. [24]

### P9 — Duplicates and exactly once

- Consumer redelivery and uncertain publish retries cause duplicates; `Nats-Msg-Id` suppresses matching publish IDs during the duplicate window. [6][11]
- NATS calls publish de-duplication plus confirmed/double consumer acknowledgements “exactly once semantics”; external side effects remain application work. [6][11]

### P10 — Request/reply correlation

- The request carries a reply-to inbox subject and the responder publishes the answer to that subject. [4][9]
- The inbox subscription and caller timeout correlate and bound the reply collection. [9]

### P11 — Topology and discovery

- Clients connect to NATS servers; subscriptions create interest, clusters use routes, superclusters use gateways, and leaf nodes extend a remote edge server. [4][18]
- `INFO.connect_urls` is the server-to-client topology discovery mechanism in the client protocol. [4]

### P12 — Flow-control credit

- Core NATS has no publisher credit; its practical bound is the receiver connection’s pending-byte limit followed by disconnect. [1][7]
- JetStream uses `max_ack_pending`, pull batch/expiration, and optional push flow-control replies and idle heartbeats. [6][14]

### P13 — Large messages and body streaming

- Server `max_payload` bounds a complete published payload, defaulting to 1 MiB. [7]
- `PUB` and `HPUB` frame the complete byte count before routing; Core NATS provides no partial-body streaming delivery. [4]

### P14 — Identity and authorization

- Connections authenticate with configured credentials, NKeys/JWTs, TLS certificates, or auth callout, then are authorized by account and subject permissions. [17]
- The authenticated connection identity is not a standard per-message field. [4][17]

### P15 — Resource bounds

- Server limits cap payload, control line, connections, pending bytes, ping liveness, and write time; JetStream caps stream and consumer resources. [6][7]
- Account limits scope many of these quotas for multi-tenant operation. [17]

### P16 — Observability

- JetStream publish acknowledgements expose stream sequence and duplicate detection; consumer acknowledgements and delivery metadata expose progress/redelivery state. [6][11][14]
- HTTP monitoring endpoints expose `/varz`, `/connz`, `/routez`, `/jsz`, and `/healthz` snapshots. [21]

### P17 — Shutdown

- `UNSUB` removes a subscription; closing a Core connection loses its outstanding ephemeral delivery state. [1][4]
- Lame Duck Mode notifies clients, stops admitting new connections, drains existing clients, and then shuts down. [13]

### P18 — Transports

- The client protocol’s default transport is TCP/IP; it also supports TLS over the connection, WebSockets, and embedded-server UNIX-domain sockets. [4][17]
- MQTT and WebSocket listeners are server integration options; their protocol semantics are not Core NATS client protocol semantics. [18]

## 13. Ecosystem

- **Rust:** `async-nats` is the asynchronous Rust client and includes Core NATS and JetStream APIs. [25]
- **Rust:** `nats.rs` is the earlier Rust client repository; `async-nats` is the maintained successor described by its project. [26]
- **Go:** `nats.go` is the official Go client, including JetStream APIs and reconnect support. [20]
- **Python:** `nats.py` is the official asyncio Python client with JetStream support. [27]
- **Java:** `nats.java` is the official Java client with JetStream management and consumption APIs. [28]
- **CLI:** the `nats` CLI creates and inspects streams/consumers and publishes, subscribes, and requests. [29]
- The documentation uses all of Rust, Go, Python, Java, JavaScript/TypeScript, C, C#, and CLI examples for the same JetStream concepts. [2][22]
- Client libraries add ergonomics such as reconnect loops, request timeout APIs, inbox management, and consumer fetch APIs over the Core protocol. [9][20][22]
- [inference] Interoperability is strongest at the shared server protocol and JetStream API; retry timing and callback/concurrency behavior remain client-library-specific. [4][20][25][27][28]

## 14. Sources

1. NATS Docs, “Core NATS Deep Dive,” current documentation, accessed 2026-09-08. https://docs.nats.io/learn/core-nats/
2. NATS Docs, “JetStream,” current documentation, accessed 2026-09-08. https://docs.nats.io/concepts/jetstream
3. NATS Docs, “Configuration,” NATS Server 2.14 documentation, accessed 2026-09-08. https://docs.nats.io/reference/config/
4. NATS Docs, “Client Protocol,” current protocol reference, accessed 2026-09-08. https://docs.nats.io/reference/protocols/client
5. nats-io, “NATS Architecture and Design,” ADR index, repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design
6. NATS Docs, “Consumers” and JetStream configuration reference, current documentation, accessed 2026-09-08. https://docs.nats.io/nats-concepts/jetstream/consumers
7. NATS Docs, “Server Configuration” properties, NATS Server 2.14 documentation, accessed 2026-09-08. https://docs.nats.io/running-a-nats-service/configuration
8. NATS Docs, “Subjects,” current documentation, accessed 2026-09-08. https://docs.nats.io/nats-concepts/subjects
9. NATS Docs, “Request-Reply,” current documentation, accessed 2026-09-08. https://docs.nats.io/nats-concepts/core-nats/reqreply
10. NATS Docs, “Queue Groups,” current documentation, accessed 2026-09-08. https://docs.nats.io/nats-concepts/core-nats/queue
11. NATS Docs, “JetStream Model Deep Dive: Exactly Once Semantics,” current documentation, accessed 2026-09-08. https://docs.nats.io/nats-concepts/jetstream/model-deep-dive
12. nats-io, ADR-4, “NATS Message Headers,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-4.md
13. nats-io, ADR-5, “Lame Duck Notification,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-5.md
14. nats-io, ADR-13, “Pull Subscribe internals,” 2021-07-20, accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-13.md
15. nats-io, ADR-8, “JetStream based Key-Value Stores,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-8.md
16. nats-io, ADR-20, “JetStream based Object Stores,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-20.md
17. NATS Docs, “Security Deep Dive,” current documentation, accessed 2026-09-08. https://docs.nats.io/learn/security/
18. NATS Docs, “Clustering & Replication Deep Dive” and topology documentation, current documentation, accessed 2026-09-08. https://docs.nats.io/learn/clustering/
19. nats-io, ADR-59, “JetStream Stream Sourcing and Mirroring,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-59.md
20. nats-io, `nats.go` repository, repository state accessed 2026-09-08. https://github.com/nats-io/nats.go
21. NATS Docs, “Monitoring endpoints,” current documentation, accessed 2026-09-08. https://docs.nats.io/learn/monitoring/monitoring-endpoints
22. NATS Docs, “Pull consumers in depth,” current documentation, accessed 2026-09-08. https://docs.nats.io/learn/jetstream/pull-consumers
23. nats-io, ADR-9, “JetStream Consumer Idle Heartbeats,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-9.md
24. nats-io, ADR-17, “Ordered Consumer,” repository state accessed 2026-09-08. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-17.md
25. nats-io, `async-nats` repository, repository state accessed 2026-09-08. https://github.com/nats-io/nats.rs
26. nats-io, `nats.rs` repository, repository state accessed 2026-09-08. https://github.com/nats-io/nats.rs
27. nats-io, `nats.py` repository, repository state accessed 2026-09-08. https://github.com/nats-io/nats.py
28. nats-io, `nats.java` repository, repository state accessed 2026-09-08. https://github.com/nats-io/nats.java
29. nats-io, `natscli` repository, repository state accessed 2026-09-08. https://github.com/nats-io/natscli
