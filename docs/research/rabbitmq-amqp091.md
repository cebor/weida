# AMQP 0-9-1 and RabbitMQ 4.3

## 0. Identity card

- **Name.** AMQP 0-9-1, Advanced Message Queuing Protocol, revision 0-9-1: a messaging protocol "that enables conforming client applications to
  communicate with conforming messaging middleware brokers" [5].
- **Versions in use.** 0-8, 0-9 and 0-9-1 are accepted on the same port; 0-9-1 is the one in use [4]. AMQP 1.0 is "a completely different messaging
  protocol and not a revision of the same idea", shares nothing at the wire level and uses different client libraries [4][64]. RabbitMQ 4.x speaks
  both natively on 5672, selected by protocol header [64].
- **Governing body.** The AMQP Working Group published 0-9-1 (spec PDF and machine-readable XML, copyright 2009, AMQP license) [1][2] then moved to
  AMQP 1.0 under OASIS/ISO; 0-9-1 is not maintained further. RabbitMQ is the de facto steward, publishing the spec archive, an errata document and a
  conformance table [1][3][4].
- **Specification documents.** `amqp0-9-1.pdf` (specification), `amqp-xml-doc0-9-1.pdf` (generated reference), `amqp0-9-1.xml` / `.stripped.xml`
  (machine-readable, for parser generation), `.extended.xml` adding the RabbitMQ extension methods [1][2][4].
- **Year researched.** Protocol 0-9-1, XML copyright 2009 [2]. Broker RabbitMQ 4.3; latest patch 4.3.5 released 17 Aug 2026, community support to 30
  Nov 2026 [54]. All documentation read at version selector 4.3.
- **Reference implementation.** RabbitMQ (Erlang/OTP). The conformance page marks `connection`, `channel`, `exchange`, `queue` and `basic` "ok" and
  `tx` "partial" [4].
- **Wire type.** Binary. A frame is `type` (octet), `channel` (short), `size` (long), payload, `frame-end` `0xCE` [1][2].
- **Transports the specification defines.** TCP only, default port 5672, in the spec XML root element [2]. RabbitMQ adds TLS on 5671, PROXY protocol
  v1/v2 for both, and a separate binary Stream protocol on 5552/5551 [15][35]. There is no AMQP 0-9-1 over WebSocket; the WebSocket plugins carry
  STOMP and MQTT [9].

## 1. Connection and session lifecycle

**Protocol header.** The client writes the 8-octet header: literal `AMQP`, then `0`, then major `0`, minor `9`, revision `1` [1]. If the server does
not recognise the first five octets or the requested version, it MUST write a valid protocol header back, flush, and close the socket [4].

**Handshake.** `connection.start` (server) -> `start-ok` -> optionally `connection.secure` / `secure-ok` (a repeatable SASL challenge) ->
`connection.tune` (server) -> `tune-ok` -> `connection.open` (client, carrying the virtual host) -> `open-ok` [2][33]; all ten methods are implemented
[4]. `start` carries `version-major`, `version-minor`, a `server-properties` table, the space-separated `mechanisms` list and `locales`, and the
server MUST support at least `en_US`; `start-ok` carries `client-properties`, the chosen `mechanism`, the SASL `response` and `locale`, and a
mechanism the server did not propose means the server MUST close the connection without further data [2][4]. Client properties carry an optional
`capabilities` table and a `connection_name`; RabbitMQ's server capabilities advertise `exchange_exchange_bindings`, `consumer_cancel_notify`,
`basic.nack` and `publisher_confirms` [33]. Before `open`/`open-ok` there is no error handshake at all: a peer detecting an error MUST close the
socket silently [4], which is why authentication failures are invisible unless the client advertises `authentication_failure_close`, in which case
RabbitMQ sends `connection.close` with `ACCESS_REFUSED` (403) [29].

**Negotiation.** `tune` proposes `channel-max`, `frame-max`, `heartbeat`; `tune-ok` answers. Until `frame-max` is agreed both peers MUST accept frames
of `frame-min-size` = 4096 octets, also the minimum negotiable value [2][4]. RabbitMQ deviates twice: no limit on `channel-max`, accepting any
`tune-ok` value, while it does limit `frame-max` and requires `tune-ok <= server value`; and it ignores `frame-max` for method and content-header
frames, which cannot be split [3]. Server defaults: `channel_max` 2047 (channel 0 reserved), `frame_max` 131072 (128 KiB), `initial_frame_max` 8192,
`heartbeat` 60 s [43].

**Authentication step.** SASL, inside `start-ok`/`secure-ok`. Built in: `PLAIN`, `AMQPLAIN` (a non-standard PLAIN variant kept for compatibility),
`ANONYMOUS`, `RABBIT-CR-DEMO` (a demonstration challenge-response, off by default); `EXTERNAL` comes from `rabbitmq_auth_mechanism_ssl`. The default
offered list is exactly `PLAIN`, `AMQPLAIN`, `ANONYMOUS` [44].

**Keep-alive.** Heartbeat frames are type 8, channel 0, zero-length payload, `0xCE` terminator: eight bytes [2][3]. RabbitMQ suggests 60 s; core
clients negotiate "if either side proposes 0, take the larger, otherwise the smaller" [32]. Frames go out roughly every `timeout/2`; after two missed
heartbeats the peer is unreachable and the TCP connection is closed [32]. Any traffic counts as activity, including publishes and acknowledgements
[32]. 5-20 s is recommended: below 5 s false positives are likely, at 1 s very likely, from transient congestion or short-lived server flow control
[32]. Heartbeats are *deactivated* while a resource alarm is in effect [38]. Both sides must send 0 to disable them, discouraged unless TCP keepalives
are tuned everywhere; untuned Linux detection is about 11 minutes [32].

**Idle behaviour.** Connections are meant to be long-lived, one client connection per TCP connection [33]; heartbeats also stop proxies and load
balancers closing idle connections [32]. Churn consistently above ~100/s indicates misuse [33].

**Orderly close.** `connection.close` (either peer, carrying `reply-code`, `reply-text`, `class-id`, `method-id`) -> `close-ok`. After sending
`close`, all received methods except `close` and `close-ok` MUST be discarded, and receiving a `close` after sending one is answered with `close-ok`
[2][4]. Closing a connection closes all its channels [5][34]. A connection-level exception ("hard error") is unrecoverable instead: RabbitMQ sends the
error and closes the connection [33]. A peer detecting socket closure without a `close-ok` SHOULD log it; only the server keeps a log [4].

**Reconnection rules the protocol defines.** None. There is no session resumption, no durable subscription state and no client identifier. Everything
a connection held dies with it: channels, consumers, unacknowledged deliveries (requeued by the broker), publisher sequence numbers, exclusive and
auto-delete queues. Reconnection and topology re-declaration are client-library or application concerns [10][33][42]; see section 9.

## 2. Primitives

**Connection.** One TCP connection, one virtual host chosen in `connection.open`; the server MUST enforce full separation of exchanges, queues and
associated entities per vhost [2][4][46]. Holds the negotiated `channel-max`/`frame-max`/`heartbeat`, the authenticated user, client properties and
capabilities, the open channels, and ownership of its exclusive queues. Costs one file handle plus TCP buffers [33].

**Channel.** A "lightweight connection that shares a single TCP connection" [5]. Every client operation happens on a channel and every method frame
carries the channel number [5]. Channels exist only inside a connection; usable numbers are 1..`channel-max`, so channel 0 is outside the range and
reserved [34][43]. Holds: confirm-mode flag and publisher sequence counter, transaction mode, `basic.qos` settings, its consumers, its unacknowledged
delivery tags, and the last server-generated queue name for empty-string references [12]. Delivery and consumer tags are channel-scoped: acknowledging
on another channel is an "unknown delivery tag" error that closes the channel [8], and a consumer created on one channel MUST NOT be used on another
[4]. By convention channels are not thread-safe — one per thread or process, never shared for publishing, since concurrent publishing on a shared
channel produces incorrect framing and kills the connection [9][34]. They should be long-lived, single digits per connection, with churn above ~100/s
worth investigating [34].

**Exchange.** A named routing table [9]. Properties: name, type, `durable`, `auto-delete`, `internal`, argument table [5][11]. Durable exchanges
survive restart; auto-deleted ones vanish when their last binding is removed, and only if a binding ever existed [11]. The spec asks for at least 16
per vhost and ideally no limit [4]. `amq.*` names are reserved and cannot be declared or deleted in RabbitMQ [31], where the spec only says clients
MAY declare them with `passive` set [4]. Pre-declared per vhost: the nameless default exchange, `amq.direct`, `amq.fanout`, `amq.topic`, `amq.match`
(plus `amq.headers`), and the system exchanges `amq.rabbitmq.log` and `amq.rabbitmq.event` [5][11].

**Queue.** Stores messages for consumption [5]. Properties: name (up to 255 bytes UTF-8 in RabbitMQ), `durable`, `exclusive`, `auto-delete`,
`x-arguments` [12]. `exclusive` means one connection may use it and it is deleted when that connection closes; `auto-delete` means a queue that has
had at least one consumer is deleted when the last consumer unsubscribes — a queue that never had a consumer is never auto-deleted, and `basic.get`
does not count as a consumer [3][12]. Declaration is create-if-absent and idempotent only when every attribute matches; otherwise channel error 406
`PRECONDITION_FAILED` [5][12]. Declaring the empty string yields a server-named `amq.gen-*` queue which the channel remembers [5][12]; an
`amq.`-prefixed declaration raises 403 `ACCESS_REFUSED` [12]. Types, by the immutable `x-queue-type` argument: `classic` (default, non-replicated —
mirroring was removed in 4.x), `quorum` (Raft-replicated, always durable, never exclusive, no server-named names), `stream` (append-only replicated
log) [12][13][14]. Every replica's hot path is bound to one CPU core, so a single queue is an anti-pattern for throughput [10][12].

**Binding.** Source exchange name, destination name, destination type (`queue` for queues and streams, `exchange` for exchange-to-exchange), optional
routing key, optional argument table used by some exchange types [11]. Duplicate bindings MUST be ignored rather than erroring, and a message MUST NOT
be delivered twice to a queue even if several bindings match [4]. Durability is inherited from source and destination; semi-durable and fully
transient bindings are slated for removal [11]. Every declared queue is automatically bound to the default exchange with its own name as routing key
[4][11].

**Consumer (subscription).** Registered with `basic.consume` on a queue, identified by a channel-scoped consumer tag, cancelled with `basic.cancel`;
consumers are "transient requests for messages" lasting as long as their channel [5][10][65]. Flags: `no-local` (accepted and ignored [4]), `no-ack`,
`exclusive`, `no-wait`, plus arguments such as `x-priority` and `x-stream-offset` [2][14][23]. Consuming from a non-existent queue is a 404
`NOT_FOUND` channel error [10]. Cancelling stops future deliveries but neither discards nor requeues in-flight ones; only closing the channel does
[10].

**Publisher.** Not a protocol object: a channel used with `basic.publish`, optionally in confirm mode [9]. **Virtual host.** A named container for
exchanges, queues, bindings, permissions and policies, default `/`, giving logical rather than physical separation, with per-vhost limits on
concurrent connections and total queues. No cross-vhost routing: an application must connect to both [5][46].

## 3. Message model

**Framing.** Four frame types: `frame-method` 1, `frame-header` 2, `frame-body` 3, `frame-heartbeat` 8; `frame-end` 206 (`0xCE`); `frame-min-size`
4096 [2]. An unknown frame type, or an invalid `frame-end`, is a fatal protocol error and the connection MUST be closed without further data [4]. A
content-bearing method (`basic.publish`, `basic.deliver`, `basic.get-ok`, `basic.return`) is one method frame, one content header frame, then zero or
more body frames [1][4]. The content header's `class-id` MUST match the method's class or the peer raises 501 `FRAME_ERROR`; content frames MUST NOT
use channel 0 or the peer raises 504 `CHANNEL_ERROR`; heartbeats and connection-class frames MUST use channel 0 or the peer raises 503
`COMMAND_INVALID` [4].

**Size limits.** A peer MUST NOT send frames larger than the negotiated `frame-max`, and one receiving an oversized frame MUST signal a connection
exception with 501 `FRAME_ERROR` [4]. The body is a sequence of body frames and "can be any size" at protocol level [4], the total carried as a 64-bit
`body-size` in the content header [1]. RabbitMQ bounds it operationally: `max_message_size` defaults to 16777216 bytes (16 MiB), maximum allowed 512
MiB, larger rejected [43]. There is no multipart message: a body split across frames MUST be reassembled into a single set and may be re-fragmented
differently on delivery [4], while method and content-header frames cannot be split at all [3].

**Properties.** The `basic` content class defines 14 properties plus one reserved field, in order: `content-type`, `content-encoding`, `headers`
(field table), `delivery-mode`, `priority`, `correlation-id`, `reply-to`, `expiration`, `message-id`, `timestamp`, `type`, `user-id`, `app-id`,
`reserved` [2]. All are optional except that RabbitMQ documents `delivery-mode` as required (1 transient, 2 persistent) [9]. What the broker actually
interprets: `delivery-mode` decides restart survival [3][9]; `expiration`, a decimal string of milliseconds, is per-message TTL [17]; `priority` is
used by classic priority queues and, in 4.3, by quorum queues [19]; `user-id`, if set, is validated against the connection's authenticated user [28];
and `headers` entries `CC` and `BCC` are read as additional routing keys, `BCC` being stripped before delivery — the only case where RabbitMQ modifies
properties after routing [4][22]. `reply-to` and `correlation-id` are pure convention except that `amq.rabbitmq.reply-to*` is special-cased by Direct
Reply-To [25]. `content-type`, `content-encoding`, `type`, `app-id`, `timestamp` and `message-id` are never validated or used by core RabbitMQ
[9][10]. Delivery metadata is not part of the message and is set at routing time: delivery tag, `redelivered`, exchange, routing key, consumer tag
[9][10].

**Payload typing.** The body is an opaque byte array; the broker will not inspect or modify it and MUST NOT modify content bodies it passes on [4][5].
A message may have properties and no body [5].

**Field tables.** Field names MUST start with a letter, `$` or `#` [4]. RabbitMQ deliberately does *not* use the 0-9-1 type tags: it keeps the
Qpid/Rabbit tags, where `s` is a signed 16-bit integer (0-9-1 says short string) and `l` is signed 64-bit (0-9-1 says unsigned), and adds `x` for byte
arrays; decimals are treated as signed [3]. `field-array` is prefixed with the total encoded byte length treated as `long-uint`, assuming the
grammar's `long-int` is a typo [3].

**Message identity.** None the broker enforces; `message-id` and `correlation-id` are free-form strings [9]. Protocol identity is per-hop and
per-channel: the publisher sequence number in confirm mode (starting at 1 on `confirm.select`) and the delivery tag (monotonically increasing 64-bit,
never zero — zero means "all messages so far received") [4][8]. Neither travels with the message. Only the Stream protocol has a real deduplication
identity: producer name plus a strictly increasing `publishing ID` [14].

## 4. Patterns and topologies

AMQP 0-9-1 is "a programmable protocol": entities and routing schemes are defined by applications, not by an administrator, so every pattern is a
matter of who declares and who binds [5]. Exchanges take a message and route it to zero or more queues, by exchange type and bindings [5].

### Exchange types

- **Default exchange.** A pre-declared direct exchange named by the empty string. Every queue is automatically bound to it with its own name as
  routing key, so publishing with `routing_key = <queue name>` appears to address the queue directly [5][11]. RabbitMQ prohibits every other access —
  no bind, unbind, delete or redeclare, and it may be neither source nor destination of an exchange-to-exchange binding [3][11]. It is `amq.default`
  in permission contexts, a name that cannot be used when publishing [11].
- **Direct.** Routes to every queue whose binding key K equals the routing key R: unicast, or multicast when several queues share K [5][11].
  **Fanout.** Routes a copy to every bound queue, stream or exchange; the routing key is completely ignored [5][11].
- **Topic.** Routing and binding keys are dot-delimited word lists — the spec requires "zero or more words delimited by dots" [4]. `*` matches exactly
  one word, `#` zero or more [11][48]. `regions.na.cities.*` matches `regions.na.cities.toronto` but not `regions.na.cities`; `audit.events.#` matches
  `audit.events` and `audit.events.users.signup`; a binding of `#` makes the exchange a fanout for that binding [11].
- **Headers.** Ignores the routing key and matches the `headers` table. `x-match` selects `any` or `all`; headers whose names start with `x-` are
  excluded unless `x-match` is `any-with-x` or `all-with-x` [5]. Pre-declared as `amq.match` and `amq.headers` [5].
- **Plugin types shipped with the broker.** Consistent Hashing, Random, Recent History, Local Random, Modulus Hash, JMS Topic [9][11]. Consistent
  hashing and the sharding plugin are the documented way past the one-core-per-queue limit [10]; `x-modulus-hash` partitions an ordered workload
  across several single-active-consumer queues [12]. The spec requires all non-normative exchange types to be named starting with `x-` [4] — a rule
  RabbitMQ's own type names do not follow.
- **Exchange-to-exchange bindings** (`exchange.bind`, a RabbitMQ extension): both endpoints are exchanges, flow is source -> destination, binding
  semantics follow each exchange's type. Routing detects and eliminates cycles and guarantees each destination queue receives exactly one copy over
  the whole transitive topology, so the destination exchange's inbound rate metric is not updated [11][21].
- **Alternate exchanges.** An exchange declared with the `alternate-exchange` argument or policy key delegates messages it could not route — no bound
  queues, or no matching binding — to another exchange. Chains are followed until routing succeeds, the chain ends, or an already-attempted exchange
  recurs. A message routed via an AE counts as routed for `mandatory` purposes [20].

### Behaviour when there is nobody to talk to

Publishing to a non-existent *exchange* is a 404 `NOT_FOUND` channel error that closes the channel [9]. Publishing to an existing exchange that routes
nowhere has three outcomes [9][20]: with `mandatory = false` (the default) and no alternate exchange the message is silently dropped, visible only in
an "unroutable dropped" metric [9]; with an alternate exchange it is republished there [20]; with `mandatory = true` it returns as `basic.return`
carrying a reply code and text, sent *before* the confirm [8][9]. The code RabbitMQ uses is 312 "no route", which the 0-9-1 constant table omits — the
errata says section 1.2 "ought to define" it [3]. The `immediate` flag ("deliver only if a consumer is ready") is **not** implemented; the documented
substitute is a per-message TTL of 0, which expires the message on arrival unless it can be delivered immediately, produces no `basic.return`, and
dead-letters if a DLX is configured [4][17]. The spec's 313 `NO_CONSUMERS` code exists but has no RabbitMQ path [2][4].

### The six tutorial topologies

The tutorials target RabbitMQ 4.x; quoted from the Python series [48].

1. **Hello World.** Both `send.py` and `receive.py` declare queue `hello` with `durable=True` and `arguments={'x-queue-type': 'quorum'}` — declaration
   is idempotent, so whoever runs first wins and the identical second declaration is a no-op. No exchange is declared and no binding created: the
   publisher uses `exchange=''`, `routing_key='hello'`, relying on the default exchange's automatic queue-name binding. The consumer uses
   `auto_ack=True`. A message sent to a non-existent location is "just dropped".
2. **Work Queues.** Queue `task_queue`, durable and quorum, declared by producer and worker alike; default exchange, `routing_key='task_queue'`;
   producer sets `delivery_mode=Persistent` (value 2); worker uses manual acknowledgement (the Pika default) and acks after the work. Dispatch is
   round-robin: RabbitMQ hands out messages as they enter the queue, does not look at how many unacknowledged deliveries a consumer holds, and
   "blindly sends every n-th message to the n-th consumer" — turn-based, not capacity-based, so with alternating heavy and light tasks one worker
   stays busy while the other idles. The prescribed fix is exactly `channel.basic_qos(prefetch_count=1)`. A worker dying before acknowledging causes
   requeue and redelivery to another consumer; under `auto_ack=True` the message is lost. The persistence caveat is explicit: marking a message
   persistent tells RabbitMQ to save it to disk but does not fully guarantee it, because there is a window after acceptance and RabbitMQ does not
   `fsync(2)` per message — for a stronger guarantee it points at publisher confirms.
3. **Publish/Subscribe.** Both sides declare `exchange='logs'`, type `fanout`. The producer declares no queue and publishes with `routing_key=''`,
   which fanout ignores. Each consumer declares `queue=''` with `exclusive=True` — a server-named queue such as `amq.gen-JzTY20BRgKO-HjmUJj0wLg`,
   deleted when its connection closes — and creates `queue_bind(exchange='logs', queue=queue_name)` with no binding key. The tutorial states plainly
   that messages are lost when no queue is bound yet, and argues this is acceptable because with no listener the message can safely be discarded.
4. **Routing.** Both sides declare `exchange='direct_logs'`, type `direct`. The publisher publishes with `routing_key=severity` (`info`, `warning`,
   `error`). Each consumer declares an exclusive server-named queue and one binding per severity wanted. A direct exchange delivers to queues whose
   binding key exactly equals the routing key; other routing keys are discarded. Several queues may share a binding key, and matching messages then go
   to all of them.
5. **Topics.** Both sides declare `exchange='topic_logs'`, type `topic`; consumers bind an exclusive server-named queue with one or more binding keys;
   routing keys may be up to 255 bytes. Worked example: Q1 binds `*.orange.*`, Q2 binds `*.*.rabbit` and `lazy.#`. Then `quick.orange.rabbit` reaches
   Q1 and Q2; `lazy.orange.elephant` both; `quick.orange.fox` only Q1; `lazy.brown.fox` only Q2; `lazy.pink.rabbit` reaches Q2 exactly once although
   both its bindings match; `quick.brown.fox` matches nothing and is discarded. Breaking the three-word contract, `orange` and
   `quick.orange.new.rabbit` match nothing and are lost, while `lazy.orange.new.rabbit` still matches `lazy.#`.
6. **RPC.** The server declares `rpc_queue`, durable and quorum, sets `basic_qos(prefetch_count=1)` so several server processes share work, and
   acknowledges the request after publishing the reply. The client declares `queue=''` with `exclusive=True` as its single callback queue — one per
   client, not one per request — and sends `reply_to=callback_queue`, `correlation_id=uuid4()`; request and reply both go through the default
   exchange, the reply using `routing_key=props.reply_to`. `correlation_id` exists because one callback queue carries replies to many requests; the
   recommended handling of an unknown `correlation_id` is to discard rather than fail. Duplicates are expected: a server can die after sending the
   reply but before acknowledging the request and will reprocess it after restart, so RPC should ideally be idempotent. The tutorial explicitly does
   not solve no server running, client-side RPC timeouts, propagating server exceptions, or validating malformed requests, and advises an asynchronous
   pipeline when in doubt.
7. **Publisher Confirms** exists as tutorial 7 for Java, Kotlin, Ruby, C#, PHP and Go but not Python [48]; its content is the three confirm strategies
   of section 9.

**Direct Reply-To** replaces the tutorial-6 callback queue when no durability is wanted: the client consumes the pseudo-queue `amq.rabbitmq.reply-to`
with `no_ack=true` without declaring it, and publishes its request with `reply_to` set to that literal value, on the same connection and channel.
RabbitMQ rewrites the forwarded `reply_to` to `amq.rabbitmq.reply-to.<opaque-suffix>`; the responder publishes to the default exchange with that value
as routing key. The pseudo-queue is not a real queue — it cannot be deleted, does not appear in management or `list_queues`, holds no buffer, is
at-most-once and may drop. Replies must be automatically acknowledged, are not fault-tolerant, and are dropped if the requester has gone; with
`mandatory` set, a reply to a departed requester still counts as routed and produces no `basic.return`. At most one such consumer per channel [25].

## 5. Flow control and backpressure

**1. Consumer prefetch (`basic.qos`).** The unit of credit is *messages*: the maximum number of unacknowledged deliveries. At the limit RabbitMQ stops
delivering on that channel until an acknowledgement frees a slot, and a multi-ack releases several at once [8]. `prefetch_count = 0` means no limit
[8]. Because deliveries and acks are asynchronous, changing prefetch mid-flight can temporarily exceed the new value [8]. `prefetch_size` (a byte
window) is **not implemented** [4], and the concepts guide states RabbitMQ supports neither connection-level nor size-based prefetching [5]. The
`global` flag is reinterpreted: the spec makes `prefetch_count` channel-wide, RabbitMQ applies it "separately to each new consumer on the channel"
[24]. So `basic.qos(10, global=false)` gives each subsequently created consumer its own budget of 10, `basic.qos(15, global=true)` sets a channel-wide
budget, and both can be set and are then enforced independently — 10 per consumer and 15 across the channel — at the cost of coordination; the spec
does not explain repeated `basic.qos` calls with differing `global` values [24]. The server-side default is `default_consumer_prefetch`, e.g.
`{default_consumer_prefetch, {false, 250}}` [24]. `basic.qos` has no effect on `basic.get` [8]. 100-300 is described as usually optimal; 1 is the most
conservative and significantly reduces throughput, especially with high consumer latency [8]. Quorum queues and streams reject global QoS with a
channel error and require per-consumer QoS [13][14]. Consumer priorities interact: a consumer at its prefetch limit counts as "blocked" and RabbitMQ
then delivers to lower-priority consumers rather than waiting [23].

**2. Internal credit flow.** Between broker components. "RabbitMQ will reduce the speed of connections which are publishing too quickly for queues to
keep up. No configuration is required." Such a connection shows `flow` in `rabbitmqctl`, the management UI and the HTTP API, meaning it is blocked and
unblocked several times a second; from the client's point of view it should merely look like reduced bandwidth. Channels, queues and other components
can also be in `flow` and propagate it back to publishers [36].

**3. Resource alarms and `connection.blocked`.** Two watermarks — memory above its limit, free disk below its limit. On either, RabbitMQ blocks
connections that publish by suspending reads from the socket [38]. Consuming-only connections are not blocked and deliveries continue; heartbeats are
deactivated while an alarm is in effect [38]. Management state is `blocking` for a connection that has not attempted to publish and `blocked` for one
that has and is paused [38]. Alarms are cluster-wide: an alarm on one node blocks publishing connections on all nodes until every alarm clears
[9][38]. Because the connection is the unit, mixing publishing and consuming on one connection defeats the design, so separate connections are advised
[38]. Clients advertising the `connection.blocked` capability receive `connection.blocked` (with a reason) and later `connection.unblocked`; a second
alarm before unblocking produces no second notification, and `unblocked` arrives only after every alarm has cleared [30]. Defaults:
`vm_memory_high_watermark.relative = 0.6`, i.e. 60% of detected RAM, with an absolute form recommended for containers [37]; `disk_free_limit` 50 MB
free on the database partition, checked every 10 seconds and up to 10 times a second near the threshold [39]. Setting the memory watermark to 0 blocks
all publishing immediately [9][37].

**4. TCP backpressure.** Blocking is implemented by not reading the socket, so a blocked publisher's writes eventually time out or fail with an I/O
error; publishers must handle that and use confirms to learn what was accepted [9][38]. Since 4.1 TCP buffer sizes auto-adjust from message rate and
size, typically settling at 88-128 KiB on Linux; `tcp_listen_options.sndbuf`/`.recbuf` trade per-connection RAM against throughput, values below 8 KiB
are discouraged and asymmetric values are called dangerous [35]. Nagle is disabled by default [35].

**5. Not a mechanism.** `channel.flow` with `active=false` is not supported by the server; the conformance table says "Limiting prefetch with
basic.qos provides much better control", and neither server nor clients ever issue a `channel.flow` automatically [4].

**Streams** use a different unit. Stream-protocol consumers subscribe with *chunk* credits — a chunk is one to several thousand messages — and the
broker decrements one credit per delivered chunk and stops at zero until the client issues more; at least 2 initial credits plus one per new chunk is
the documented rule of thumb [15]. Stream-protocol publishers get `stream.initial_credits` = 50000 outstanding unconfirmed messages, unblocking at
`stream.credits_required_for_unblocking` = 12500, publishers only [15]. A stream consumed through AMQP 0-9-1 instead requires QoS prefetch and
acknowledgements, where acks act as credit to advance the consumer's offset rather than deleting messages [14].

## 6. Delivery guarantees and acknowledgement

Consumer acknowledgements and publisher confirms are "entirely orthogonal and unaware of each other": confirms cover only the publisher's interaction
with the node it is connected to and the queue or stream leader replica, acknowledgements only the broker-to-consumer hop [8]. Neither is end-to-end.
Both are explicitly modelled on TCP [8].

**a-b. Publish, then confirm mode.** Writing `basic.publish` plus header and body to the socket certifies nothing: "a client that's written a protocol
frame or a set of frames to its socket cannot assume that the message has reached the server and was successfully processed" [8], and `basic.publish`
has no reply method [5]. `confirm.select` / `confirm.select-ok` put the channel into confirm mode, after which broker and client both count published
messages from 1; a transactional channel cannot enter confirm mode and vice versa [8].

**c. `basic.return` (broker -> publisher)**, if `mandatory` was set and routing found no queue. It carries a reply code and text — 312 "no route" in
RabbitMQ [3] — plus the full message, and certifies that routing was evaluated and produced an empty queue list. It arrives *before* the `basic.ack`
for the same message and does not mean rejection: an unroutable mandatory message is returned *and* confirmed [8]. A message routed to an alternate
exchange counts as routed and is not returned [20], nor is a reply to a vanished Direct Reply-To requester [25].

**d. `basic.ack` (broker -> publisher), the publisher confirm.** `delivery-tag` is the publisher sequence number, `multiple` means everything up to
and including it [8]. What it certifies depends entirely on the destination:

| Case | What `basic.ack` certifies |
| --- | --- |
| Unroutable | The exchange verified the message routes to no queue at all [8] |
| Routable, generally | "The message has been accepted by all the queues" it was routed to [8] |
| Persistent message, durable classic queue | Accepted **and persisted to disk** [8] |
| Transient message (`delivery-mode = 1`) | Accepted into the queue only; a node restart discards it even from a durable queue [8][12] |
| Persistent message, transient queue | Nothing durable: the queue and all its messages are discarded at node boot [12] |
| Quorum queue | A quorum of replicas accepted and confirmed to the elected leader [8]; quorum queues always persist regardless of delivery mode and confirm only after a majority wrote *and flushed* to disk [13] |
| Stream | Replicated to a quorum — but streams do not explicitly `fsync`, relying on the OS page cache, so an uncontrolled shutdown can in theory lose confirmed data [14] |
| Multi-queue publish via `CC`/`BCC` | Routed to at least one queue and every such queue confirmed; a subset failing to route does not prevent the ack [22] |

The classic message store persists in batches after a few hundred milliseconds to reduce `fsync(2)` calls, or when a queue goes idle, so under
constant load `basic.ack` latency for persistent messages can reach hundreds of milliseconds — hence the advice to handle confirms asynchronously or
in batches [8].

**e-f. `basic.nack` (broker -> publisher), and silent removal.** Fields mean what they do in `basic.ack` and `requeue` must be ignored; by nacking,
the broker "indicates that it was unable to process the messages and refuses responsibility for them" and the client may republish. Every published
message is confirmed or nacked exactly once, never both, with no promise about how soon [8]. Once queued, a message can still be removed without
reaching any consumer — TTL expiry [17], a length limit dropping from the head [18], queue deletion, or `queue.purge`, which MUST NOT purge messages
already delivered but unacknowledged [4] — and none of this is signalled to the publisher, whose ack was sent long before.

**g-j. Delivery and consumer acknowledgement.** `basic.deliver` carries consumer tag, delivery tag, `redelivered`, exchange and routing key;
`basic.get-ok` is the polling equivalent and adds `message-count`, `basic.get-empty` says the queue is empty [2]. In `no-ack` mode the message counts
as successfully delivered "immediately after it is sent out (written to a TCP socket)", so if the consumer's connection or channel closes before it
arrives it is lost [8]. `basic.ack` from the consumer records the delivery and discards the message; `multiple = true` acknowledges every outstanding
tag up to and including the given one, so with 5,6,7,8 outstanding, acking 8 with `multiple` clears all four while without it 5,6,7 remain [8]. It
certifies only that this consumer took responsibility. `basic.reject` carries one delivery tag plus `requeue`: `false` discards the message or
dead-letters it if a DLX is configured, `true` puts it back [2][8]; semantically it means "not processed but should still be deleted" against
`basic.ack`'s "processed successfully" [8], and in RabbitMQ it increments the quorum poison-message `delivery-count` whereas `basic.nack` does not
[26]. `basic.nack` from a consumer, a RabbitMQ extension, is `basic.reject` plus a `multiple` flag — the whole reason it exists, since `basic.reject`
cannot batch — with fields `delivery_tag` (64-bit), `multiple`, `requeue` [8][26].

**k-l. Requeue placement, `redelivered`, and automatic requeueing.** A requeued message returns to its original position if possible, otherwise nearer
the head, because of concurrent deliveries and acks from other consumers [8]; since 2.7.0 messages are always held in publication order even under
requeueing or channel closure [6]. Redeliveries carry `redelivered = true`, first deliveries `false`, and a consumer can receive a message previously
delivered to a different consumer [8]. The spec is emphatic that clients MUST NOT rely on this field and must track duplicates themselves; RabbitMQ's
own duplicate tracking is marked "planned" [4], and the reliability guide restates that `redelivered=true` is only a hint that a delivery may have
been seen while unset guarantees it has not [42]. Any delivery not acked is automatically requeued when its channel or connection closes — TCP loss,
consumer process failure, or a channel-level protocol exception [8] — detection taking time [8][32], and acks sent immediately before closing a
channel may be lost with the deliveries requeued anyway [34].

**m-n. Timeout and client errors.** A consumer that does not ack within `consumer_timeout` (default 30 minutes, evaluated once a minute, values under
1 minute unsupported and under 5 not recommended) has its channel closed with `PRECONDITION_FAILED`, and *all* following deliveries on that channel,
from all its consumers, are requeued [10][43]; as of 4.3 only quorum queues enforce it, configurable per node, per policy (`consumer-timeout`) or per
queue (`x-consumer-timeout`) [10]. Acking the same tag twice, an unknown tag, or acking on a channel other than the one the delivery arrived on all
produce `PRECONDITION_FAILED - unknown delivery tag` and close the channel [8]; the errata notes the spec never said what `basic.reject` with an
unknown tag should do and suggests the same treatment [3].

**What none of them certify.** No acknowledgement tells a publisher that a consumer received or processed the message; that needs an application-level
reply [8]. A confirm does not survive its own loss: after recovery the publisher must retransmit everything unconfirmed, duplicating messages whose
confirm was sent but lost [42]. Without confirms, persistence guarantees nothing usable — publish a persistent message to a durable queue, restart the
node, and it is gone, whereas with confirms the publisher would simply never have received the ack [8]. `tx.commit-ok` for acknowledgements means the
acks were *received*, not processed or persisted, so a later server-side failure can "resurrect" acknowledged messages and redeliver them [6]. And
dead-lettering under the default `at-most-once` strategy is not confirmed at all: the source message is removed immediately, so a clustered DLX target
that cannot accept it loses the message [16].

**At-most / at-least / exactly-once, as the documentation defines them.** *At-most-once*: automatic acknowledgement mode ("fire-and-forget"),
explicitly "should be considered unsafe"; without acknowledgements only at-most-once is guaranteed [8][42]. *At-least-once*: consumer acknowledgements
plus publisher confirms plus durable or replicated queues plus persistent messages — what the acknowledgement mechanisms provide [42]. *Exactly-once*:
not offered. "RabbitMQ does not guarantee exactly-once delivery because retransmission can duplicate"; consumers should deduplicate or be idempotent
[42]. The only deduplication primitive in the product is Stream-protocol publisher deduplication (section 7).

## 7. Ordering and duplicates

**What is promised.** Spec section 4.7: the server MUST preserve the order of contents flowing through a single content processing path, unless
`redelivered` is set — messages published on one channel, through one exchange, one queue and one outgoing channel are received in the order they were
sent [4][6]. RabbitMQ promises more since 2.7.0: messages are always held in the queue in publication order, even under requeueing or channel closure,
whereas before 2.7.0 requeued messages went to the back [6].

**Scope.** One publishing channel, one queue. Publishing on one channel enqueues in publishing order in every queue the message reaches; concurrent
publishing from several channels or connections interleaves arbitrarily [12]. There is no ordering across queues and no key-, partition- or
topic-level ordering concept: the routing key selects destinations, it does not define an ordering scope.

**What breaks it.** The spec lists multiple readers, client transactions, priority fields, message selectors and implementation-specific optimisations
[4]. Concretely: **multiple consumers on one queue** — "it is still possible for individual consumers to observe messages out of order if the queue
has multiple subscribers", because other subscribers requeue [6]; **requeue and redelivery** of any kind [12]; **priorities**, which replace FIFO —
classic queues cycle through sub-queues to avoid starvation while the 4.3 quorum implementation is strict and can delay low priorities indefinitely,
returned quorum messages go to a returns queue and requeue in exact return order rather than by priority, and prefetch can hand a consumer a
low-priority message before a higher-priority one arrives because priority only orders what is already waiting [19]; **`basic.get`**, which the
ordering advice says to avoid [12]; **`basic.cancel`**, where closing the channel is preferred because cancel leaves in-flight deliveries un-requeued
[10][12]; and **concurrent consumer dispatch** — Java and .NET guarantee that deliveries on one channel are *dispatched* in order regardless of pool
size, but concurrent processing then races [10].

**The documented way to get ordering.** Use a stream, whose offset is fixed at publish time; or use a single active consumer plus a quorum delivery
limit, return messages in receipt order, avoid `basic.get`, and prefer channel close to cancel; and partition an ordered workload across several SAC
queues with `x-modulus-hash` [12]. A super stream preserves order within each partition via single active consumer [14].

**Where duplicates arise.** Publisher retransmission after a lost or missing confirm — the confirm may have been sent and lost [42]. Automatic requeue
of unacked deliveries after consumer or connection failure [8]. Consumer-side requeue loops: if every consumer requeues on a transient condition they
create a requeue/redelivery loop, costly in bandwidth and CPU [8]. Leader election in a quorum queue or a partition, where unacknowledged delivered
messages are requeued and redelivered "so consumers can receive duplicates" [41]. `at-least-once` dead-lettering, whose retries against an unroutable,
missing, unavailable or rejecting target "can create duplicates" [16]. And the RPC pattern's own case, a server dying between reply and ack [48].

**What the protocol offers to detect them.** The `redelivered` flag, a hint only [4][42]; the quorum `x-delivery-count` header and, from 4.3,
`x-acquired-count` for consumer assignment count [13]; and the `x-death` history added by dead-lettering [16]. Nothing else — deduplication is the
application's job [42]. The one real facility is Stream-protocol publisher deduplication keyed on producer name plus a strictly increasing publishing
ID: opt-in, off by default in maintained clients, valid only with a single concurrent producer per name and stream, tolerant of gaps, and queryable
after a restart [14].

## 8. Failure behaviour

| Event | Publisher observes | Consumer observes | What is lost | What is ambiguous |
| --- | --- | --- | --- | --- |
| Publisher connection lost **before** confirm | I/O write error or timeout, possibly nothing until two heartbeat intervals pass [32][33]; messages already written to the socket are not guaranteed to arrive [9] | Nothing | Possibly the message, possibly nothing | Yes. "Messages that were not confirmed should be considered undelivered after a period of time" and may be republished if safe [9] |
| Publisher connection lost **after** confirm | Connection error only | Nothing | Nothing, subject to what that confirm meant (section 6d) | No |
| Consumer crash with unacked deliveries | Nothing | Connection gone | Nothing | No, but redelivery is certain: every unacked delivery on the closed channel or connection is automatically requeued [8]. Detection is delayed by heartbeat timing [32] |
| Consumer stops acking but stays connected (quorum queue) | Nothing | Channel closed with `PRECONDITION_FAILED` after `consumer_timeout`, default 30 min | Nothing | No. All following deliveries on that channel, from all its consumers, are requeued [10] |
| Broker node restart, **classic** queue | Blocked or failed publishes while down | Connection loss, then 404 if the queue is gone | Transient messages always; persistent messages in a durable queue survive; a transient queue and all its contents are discarded at boot [12]. Classic queues are not replicated — mirroring was removed in 4.x [12] | Anything published but not confirmed |
| Broker node restart, **quorum** queue | Confirms delayed, or `basic.nack`, while no leader exists [41] | Deliveries pause; `basic.consume`/`basic.get` block or time out until a leader is reachable [41] | Nothing, provided a majority of replicas is not permanently lost: a confirmed message "should not be lost" [13] | Publishes awaiting confirm at the moment of election |
| Loss of a quorum-queue majority (2 of 3 gone permanently) | Publishes fail or time out | Cannot consume | The queue is permanently unavailable and must be force-deleted and recreated [13] | No |
| Network partition, majority side | Writes rejected until a leader is elected; pending confirms may return as `basic.nack`; delay generally a few seconds [41] | Acks, nacks and rejects are buffered and replayed; deliveries resume after election [41] | Nothing for quorum queues | Confirms in flight during the election |
| Network partition, minority side | Cannot progress: Raft consensus needs a reachable majority, work is retained until recovery or fails with timeout [41] | Quorum deliveries pause; Khepri serves local cached reads; stream consumers may still read locally available data [41] | Nothing new; the side stalls | Yes, until the partition heals |
| Length limit reached, `x-overflow = drop-head` (default) | Nothing: the publish is accepted and confirmed | Never sees the dropped messages | The **oldest** messages, dead-lettered with reason `maxlen` if a DLX exists [16][18] | No |
| Length limit reached, `x-overflow = reject-publish` | `basic.nack` for that publish, if confirms are on [18] | Nothing | The **newest** message; `reject-publish-dlx` dead-letters it instead [18] | Partly: if the publish was routed to several queues and only one rejects, the channel gets `basic.nack` while the other queues still enqueue it [18] |
| TTL expiry | Nothing, already confirmed | Never sees it: expired messages are never delivered in `basic.deliver` or `basic.get-ok` [17] | The message, dead-lettered with reason `expired` if a DLX exists [16] | Yes, in one narrow race: expiry can happen after the socket write but before the consumer receives it [17]. Also, retroactive per-queue TTL only discards at the head, so expired messages behind unexpired ones still consume resources and appear in statistics [17] |
| Unroutable message, `mandatory = false` | Nothing; only an "unroutable dropped" metric moves, and a confirm still arrives [8][9] | Nothing | The whole message, unless an alternate exchange catches it [20] | No — and that is the danger: silence looks like success |
| Unroutable message, `mandatory = true` | `basic.return` with 312 "no route", then `basic.ack` [3][8] | Nothing | Nothing: the message comes back | No |
| Channel-level error (406 redeclare mismatch, 403 unauthorised, 404 missing resource, 405 exclusive queue held elsewhere) | The channel closes and cannot publish; notification is asynchronous, so the causing operation may not fail synchronously [34] | Same channel closes; its unacked deliveries are requeued [8][34] | Nothing | Which operation caused it, if several were in flight [34] |
| Connection-level error (501, 502, 503, 504, 505, 506, 530, 540, 541) | Connection closed after the error method; hard errors are unrecoverable [33] | Same | In-flight publishes; unacked deliveries are requeued | No |
| Oversized frame (larger than negotiated `frame-max`) | Connection exception 501 `FRAME_ERROR` [4] | — | The connection | No, but note RabbitMQ ignores `frame-max` for method and content-header frames [3] |
| Message larger than `max_message_size` (16 MiB default) | Rejected [43] | — | The message | No |
| Exceeding negotiated `channel_max` | Connection closed with a fatal `not_allowed` [34] | — | The connection | No |
| Resource alarm (memory or disk) | Publishing connections blocked cluster-wide; `connection.blocked` if the capability was advertised; writes eventually time out or fail [9][30][38] | Unaffected: consuming continues, heartbeats deactivated [38] | Nothing | Whether a write in flight was accepted — hence confirms [38] |
| Authentication failure | Socket closed abruptly, or `connection.close` with 403 `ACCESS_REFUSED` if `authentication_failure_close` was advertised [29] | — | The connection | Without the capability, yes: the cause is invisible to the client, though the server logs it [29] |
| Queue deleted, or its hosting node becomes unavailable | Publishes become unroutable | Server-sent `basic.cancel`, if `consumer_cancel_notify` was advertised; otherwise no notification at all [27] | The queue's contents | Without the capability, yes |
| Slow consumer | Eventually `flow` state, then an alarm blocks the connection [36][38] | Backlog grows in the queue | Nothing until a limit or alarm is hit | No |

## 9. Reliability recipes

**Publisher confirms, three strategies** [9][48]. *Problem:* a socket write proves nothing. *Mechanism:* `confirm.select`, then (a) **streaming
confirms** — map each sequence number to its message, remove on ack, republish on nack; asynchronous and nearly free; (b) **batch publishing** —
publish a batch, wait for all outstanding confirms, republish on nack or timeout, larger batches reducing the penalty; or (c) **publish-and-wait**,
documented "primarily for completeness", an anti-pattern with "a very significant negative effect on throughput". *Guarantee:* at-least-once publisher
to queue, with the per-queue-type meaning of section 6d. *Cost:* bookkeeping plus hundreds of milliseconds of ack latency for persistent messages
under load. *Failure modes:* acks can arrive out of order relative to publication, so applications must not depend on confirm ordering [8]; the
transactional alternative is "unnecessarily heavyweight", cutting throughput "by a factor of 250" [8].

**Durability pairing.** *Problem:* surviving a restart. *Mechanism:* durable queue **and** `delivery-mode = 2`; neither alone suffices, and publishing
to a durable exchange or into a durable queue does not make a message persistent [5]. *Guarantee:* persistent messages in durable classic queues are
recovered at boot, transient ones discarded even from durable queues [12]. *Cost:* disk I/O. *Failure mode:* the window between acceptance and the
batched disk write — exactly what confirms close [8][48].

**Quorum queues for HA.** *Problem:* a node hosting a classic queue takes the queue with it. *Mechanism:* `x-queue-type = quorum`, Raft with `(N/2)+1`
majority, default group size 3, one member per node, odd sizes recommended, `queue-leader-locator` for placement (`client-local` default, `balanced`
under 1000 queues) [13][40]. *Guarantee:* a confirmed message should not be lost while a majority of hosting nodes is not permanently unavailable; a
follower is elected on leader loss and rejoining followers resume where they stopped [13]. *Cost:* always-persistent writes; a per-node WAL capped at
512 MiB with node memory recommended at 3-4x that; at least 32 bytes of metadata per message and 1 MiB per 30,000; throughput falls with message size
and member count [13]. *Failure modes:* losing the majority makes the queue permanently unavailable, membership changes need a quorum, global QoS is
rejected; 3 members tolerate 1 failure, 5 tolerate 2 [13].

**Poison-message handling and retry counting.** *Problem:* a message that always fails, requeued forever. *Mechanism:* quorum queues count failed
redeliveries in `x-delivery-count` and enforce `delivery-limit`, default 20 since 4.0 (`-1` unlimited); exceeding it drops or dead-letters [13][43].
The count increments on `basic.reject` and on client crash or connection loss, but *not* on `basic.nack`, an intra-cluster partition, or consumer
timeout; from 4.3 `x-acquired-count` separately tracks consumer assignments and is the recommended exposure measure [13]. *Cost:* messages are
discarded. *Failure mode:* with prefetch above 1 a collectively requeued batch can all be discarded at once [13]. Classic queues have no equivalent,
so consumers must count redeliveries themselves [8].

**Dead-letter exchanges.** *Problem:* somewhere to put rejected, expired and dropped messages. *Mechanism:* `x-dead-letter-exchange` plus optional
`x-dead-letter-routing-key`, or the `dead-letter-exchange` policy key; arguments override policy; same vhost; declaration needs `configure` and `read`
on the queue and `write` on the DLX [16]. Exactly four reasons: `rejected`, `expired`, `maxlen`, `delivery_limit`; queue *expiry* does not dead-letter
contents [16]. *Guarantee:* by default at-most-once — republishing is unconfirmed, the source message is removed immediately, a target that cannot
accept it loses the message, and a missing target exchange drops silently [16]. Quorum queues can opt into `dead-letter-strategy = at-least-once`,
which also requires `overflow = reject-publish` and a configured DLX, retaining source messages until the target confirms at the price of duplicates
on retry; switching back deletes anything unconfirmed [13][16]. *Observability:* the `x-death` array (newest first, compressed by `{queue, reason}`)
with `queue`, `reason`, `count`, `time`, `exchange`, `routing-keys`, optional `original-expiration`, plus immutable
`x-first-death-{queue,reason,exchange}` and per-event `x-last-death-*` [16]. *Failure modes:* cycle detection drops a message revisiting the same
queue if no rejection occurred in the cycle; the original TTL is stripped so it does not expire again in the target; `CC` is dropped when the routing
key is replaced, `BCC` always [16].

**Delayed retry via TTL plus DLX.** *Problem:* an immediate requeue produces a hot loop [8]. *Mechanism:* dead-letter the failure into a holding queue
carrying a message TTL whose own DLX points back at the work exchange; the TTL is the retry delay and `x-death.count` the attempt counter.
*Guarantee:* bounded retries with a delay. *Cost:* one queue per delay tier. *Failure modes:* classic queues only expire at the head, so a long-TTL
message in front of a short-TTL one delays it [17], and the path inherits at-most-once dead-lettering unless the quorum at-least-once strategy is used
[16]. **[inference]** The composition is mine; the docs supply the primitives (`x-message-ttl`, `expiration`, DLX, `x-death`) and tell consumers to
"schedule requeueing after a delay" [8], but no cited page names this recipe. The quorum feature matrix does list "delayed retry" as supported [13].

**Idempotent consumers.** *Problem:* redelivery and republication both duplicate. *Mechanism:* application-side deduplication or idempotent effects.
*Guarantee:* effective exactly-once processing on at-least-once delivery. Treated as mandatory: "consumers must be prepared to handle redeliveries and
otherwise be implemented with idempotence in mind" [8], and "RabbitMQ does not guarantee exactly-once delivery" [42].

**Single Active Consumer for ordering.** *Problem:* several consumers on one queue reorder work. *Mechanism:* `x-single-active-consumer = true`, a
queue argument only, because a policy could be removed and silently re-enable parallel processing; the first registered consumer becomes active and
another is promoted automatically on cancellation or death [10]. *Guarantee:* one consumer at a time with automatic failover, unlike an `exclusive`
consumer where re-registering is the application's job [10]. *Cost:* the throughput of one consumer. *Failure modes:* SAC and exclusive consumers are
mutually exclusive; classic queues pick the initial active consumer randomly, ignoring consumer priorities, while quorum queues stop delivering to the
active consumer when a higher-priority one registers and promote it once everything is acknowledged; quorum queues ignore `exclusive` on
`basic.consume`; and an AMQP 0-9-1 client cannot enable SAC on a stream, which needs a native Stream client [10].

**Heartbeating.** *Problem:* undetected dead peers and proxies closing idle connections. *Mechanism:* the negotiated heartbeat, 5-20 s recommended,
two missed intervals to declare death [32]. *Cost:* false positives under congestion at low values [32]. *Failure mode:* heartbeats are deactivated
during resource alarms, so an alarm masks liveness detection [38].

**Connection and topology recovery in clients.** *Problem:* the protocol re-establishes nothing. *Mechanism:* reconnect, restore connection listeners,
re-open channels, restore channel listeners, restore `basic.qos`, confirm and transaction settings; then re-declare exchanges (except pre-defined
ones) and queues, recover bindings, recover consumers last [9][10]. Java, .NET and Bunny implement it; others leave it to the application [9][10].
*Failure modes:* auto-recovering connections using exclusive or auto-delete queues must use server-named queues [10]; `basic.get` is not recovered
[50]; messages published while down are lost and not buffered [51]; publisher sequence numbers restart [50]; the broker resets delivery tags, so
clients adjust them and suppress stale acknowledgements [51]; explicitly closed channels and channels closed by a channel-level exception are not
recovered [51].

**Alternate exchanges as a safety net.** *Problem:* silently dropped unroutable messages. *Mechanism:* `alternate-exchange` catching what an exchange
could not route; documented uses are detecting clients that publish unroutable messages, and "or else" routing where a generic handler takes the rest
[11][20]. *Failure mode:* a missing AE only logs a warning [20]. **Federation and Shovel.** *Problem:* linking brokers reliably. *Mechanism:* both
recover, retransmit, and use confirms and acknowledgements by default; Federation needs several upstream URIs or an available load balancer, Shovel
can list several endpoints and retries after a configurable delay [42]. Federation clears the upstream `user-id` unless `trust-user-id` is set
upstream [28].

## 10. Security and identity

**Authentication mechanisms.** SASL, advertised in `connection.start`'s `mechanisms`; the client must pick one the server proposed or the server
closes the connection without further data [2][4]. Built in: `PLAIN`, `AMQPLAIN`, `ANONYMOUS`, `RABBIT-CR-DEMO`; `EXTERNAL` via
`rabbitmq_auth_mechanism_ssl` [44]. The default `auth_mechanisms` list is exactly `PLAIN`, `AMQPLAIN`, `ANONYMOUS` in decreasing preference [44].
`ANONYMOUS` authenticates unauthenticated clients as `anonymous_login_user`/`anonymous_login_pass`, both defaulting to `guest`, and the docs say
production deployments should remove it [44]. On a blank node RabbitMQ creates vhost `/` and user `guest`/`guest` with full access to `/`, but `guest`
is restricted to loopback interfaces for every protocol via `loopback_users`; `loopback_users = none` lifts that and is strongly discouraged [44].
Credentials travel in clear text without TLS [33]. `update-secret` is a RabbitMQ extension method for renewing credentials on a live connection when
they can expire [7].

**Authorization model.** Two layers: the connection's user must have permissions for the target vhost, then each operation is checked against
per-vhost regular-expression triples — `configure` (create, destroy, alter), `write` (inject messages), `read` (retrieve messages). An empty pattern
equals `^$` and matches no non-empty name. Results may be cached per connection or channel, so permission changes can take effect only on
reconnection. For checks, the blank default-exchange name maps to `amq.default` [44]. The documented table [44]:

| Operation | Required permissions |
| --- | --- |
| `exchange.declare` | `configure` on the exchange |
| `exchange.declare` with an alternate exchange | `configure` + `read` on the exchange, `write` on the AE |
| `exchange.delete` | `configure` on the exchange |
| `queue.declare` | `configure` on the queue |
| `queue.declare` with a DLX | `configure` + `read` on the queue, `write` on the DLX |
| `queue.delete` | `configure` on the queue |
| `exchange.bind` / `exchange.unbind` | `write` on the destination exchange, `read` on the source exchange |
| `queue.bind` / `queue.unbind` | `write` on the queue, `read` on the exchange |
| `basic.publish` | `write` on the exchange |
| `basic.get` / `basic.consume` / `queue.purge` | `read` on the queue |

Since 4.3.1 a *passive* `exchange.declare`/`queue.declare` requires at least one of the three permissions on the target; a non-passive declare still
requires `configure` [44]. **Topic authorisation** adds routing-key-level permissions on topic exchanges: for publishing, the resource `write` check
runs first and the topic check only if it passes, while for AMQP 0-9-1 consumers, which consume queues, the binding routing keys between a topic
exchange and the queue are checked instead. Under the internal backend, absent topic permissions mean access is allowed — it is opt-in. Patterns
expand `{username}`, `{vhost}` and `{client_id}` (MQTT only) [44].

**Does identity travel with a message?** Only if the publisher chooses. Setting the `user-id` property makes RabbitMQ validate it against the
connection's authenticated user; unset, the publisher's identity stays private and nothing is validated [28]. The `impersonator` tag permits forging
`user-id`; no user has it by default and `administrator` does not imply it [28]. There is no per-message authorisation model: the documented
granularity is vhost admission, entity-name regex per operation, and topic routing key [28][44].

**Backends.** `auth_backends` defaults to `internal`; LDAP and HTTP provide authentication and authorisation, OAuth 2 provides JWT-based both, and a
cache backend can be combined. Split entries are possible (`auth_backends.1.authn = ldap`, `auth_backends.1.authz = internal`); in a chain the first
positive authentication result is final [44]. With OAuth 2 the access token is passed as the password and the username field ignored; the plugin
validates the signature and requires its `resource_server_id` in the `aud` claim, translating scopes such as `rabbitmq.configure:*/*` and
`rabbitmq.tag:administrator` into permissions [47].

**Transport security.** TLS on `listeners.ssl`, conventionally 5671, with `ssl_options.cacertfile`/`certfile`/`keyfile`; `listeners.tcp = none`
disables plain listeners [45]. `ssl_options.verify = verify_peer` verifies a presented client certificate and `fail_if_no_peer_cert = true`
additionally rejects clients presenting none; together they are mandatory client certificate verification, and the default chain verification depth is
1 [45]. Erlang 27.x/26.x enable TLS 1.3 and 1.2 by default; TLS 1.3 shares no cipher suites with earlier versions and a 1.3-only listener rejects
older clients; `honor_cipher_order` and `honor_ecc_order` are recommended for 1.2 and must be disabled with 1.3 [45]. TLS may instead be terminated by
a proxy, which is why RabbitMQ supports the PROXY protocol [35][45]. Client-certificate identity maps to a username through `ssl_cert_login_from`:
`distinguished_name` (default, full RFC 4514 subject DN), `common_name`, or `subject_alternative_name` with a selectable SAN type and index; with
`EXTERNAL` any client-supplied password is ignored [44][45]. Inter-node and CLI traffic can use `-proto_dist inet_tls`, after which unencrypted CLI
connections stop working [45].

## 11. Limits and resource bounds

Negotiated between the peers in `connection.tune`/`tune-ok` [2][43]:

| Parameter | Server default | Notes |
| --- | --- | --- |
| `channel_max` | 2047 | Channel 0 reserved; 16-128 recommended. Exceeding the negotiated value closes the connection with a fatal `not_allowed` [34][43] |
| `frame_max` | 131072 (128 KiB) | "Should not be changed". Spec minimum 4096; RabbitMQ ignores it for method and content-header frames [3][43] |
| `initial_frame_max` | 8192 | Pre-tune value; clients overriding `frame_max` must use at least 8192 [43][55] |
| `heartbeat` | 60 s | Two missed intervals means dead [32][43] |

Timeouts and per-connection limits [43]: `handshake_timeout` 10000 ms, `ssl_handshake_timeout` 5000 ms, `session_max_per_connection` 1 and
`link_max_per_session` 10 (AMQP 1.0 only). Per node [43]: `consumer_timeout` 1800000 ms (30 minutes), `connection_max` unlimited,
`channel_max_per_node` unlimited, `ranch_connection_max` unlimited, `stream.max_connections` unlimited, `max_message_size` 16777216 (16 MiB, maximum
allowed 512 MiB), `management.http.max_body_size` 20971520. Per channel: `consumer_max_per_channel`, unlimited by default [10][43]. Per cluster:
`vhost_max`, `cluster_exchange_limit`, `cluster_queue_limit`, all unlimited by default and required to be set identically on every node [43]. Per
vhost: `max-connections` and `max-queues`, where `{"max-connections": 0}` blocks all client connections and `-1` in preconfigured `default_limits`
means unlimited [46]. Per user: maximum concurrent connections, and maximum concurrent channels across them [43].

Per queue, by client argument or policy — a client argument beats a user policy, an operator policy beats both, and for numeric values the lower of
the two wins [11][12]: `x-max-length` (ready messages), `x-max-length-bytes` (body bytes only, properties excluded), `x-overflow` `drop-head`
(default) / `reject-publish` / `reject-publish-dlx`, `x-message-ttl` and per-message `expiration`, `x-expires` (queue TTL, positive milliseconds,
"unused" meaning no consumers, no redeclare and no `basic.get` for the period, with redeclaration renewing the lease), `x-max-priority` 1..255 for
classic queues, `delivery-limit` for quorum queues (default 20), `x-consumer-timeout`, `x-quorum-initial-group-size` (default 3) [13][17][18][19][43].
Length limits count only ready messages, never unacknowledged ones [18]. Per stream: `x-max-length-bytes` and `x-max-age` retention (units
`Y M D h m s`, evaluated per segment, always keeping at least one non-empty segment), `x-stream-max-segment-size-bytes` default 500000000,
`x-stream-filter-size-bytes` 16..255 default 16, `x-initial-cluster-size` [14].

Resource ceilings: the memory high watermark `vm_memory_high_watermark.relative = 0.6` is a footprint *hint*, not a hard cap, so a node can exceed it;
an absolute form is recommended in containers, and if OS memory cannot be detected RabbitMQ assumes 1024 MB [37]. `disk_free_limit` is 50 MB [39]. At
OS level `ERL_MAX_PORTS` is usually 65536, with a rough file-handle requirement of connections x 1.5 [35]. Spec-level minima a conforming broker
SHOULD offer, all marked "does" by RabbitMQ: 16 exchanges per vhost, 256 queues per vhost, 4 bindings per queue, 16 consumers per queue, ideally with
no limit beyond available resources [4].

**What an unbounded resource looks like in practice.** Every "unlimited" default is a lever a client can pull. `prefetch_count = 0` lets one consumer
accumulate the whole queue in its own heap until the OS kills it [8][58]; automatic acknowledgement mode has no window by definition [8]; unbounded
queues page to disk and slow down, and a very large backlog is called one of RabbitMQ's hardest cases [57][59]; leaked channels and connections
exhaust node RAM and CPU [33][34]. The stated purpose of the configurable limits is exactly this: guardrails for operators who do not control the
applications [43].

## 12. Answers to the problem catalogue

Each answer is the protocol's own; the mechanics behind it are in the sections above.

**P1 — Loss and safe retry.** Loss surfaces as a missing publisher confirm: `confirm.select`, keep a sequence-number to message map, treat anything
unconfirmed after a timeout as undelivered, republish [8][9]. Republication duplicates whenever the confirm was sent but lost, so the receiver must be
idempotent [42]. Consumer-side loss is detected by the broker, not the consumer: unacked deliveries on a closed channel are requeued automatically
[8]. Nothing identifies a retry; the only detection aid is the `redelivered` hint, which the spec says clients MUST NOT rely on [4][42].

**P2 — Dead or unreachable peer.** Heartbeats: 60 s default negotiated, frames every half interval, dead after two missed intervals, then the
connection is closed; 5-20 s recommended against untuned TCP keepalive detection of ~11 minutes [32]. Raft components add an adaptive failure detector
that can notice unavailability earlier [41]. No last will, no session: channels, consumers, exclusive and auto-delete queues are destroyed and all
unacknowledged deliveries requeued [3][8]. A stuck-but-connected consumer is caught by `consumer_timeout` (30 min, quorum queues only in 4.3) [10]; a
consumer whose queue disappears is told by a server-sent `basic.cancel` only if it advertised `consumer_cancel_notify` [27].

**P3 — Spreading work by capacity.** Many consumers on one queue plus `basic.qos`, unit *messages* (unacknowledged deliveries). Without prefetch,
dispatch is blind round-robin: RabbitMQ "blindly dispatches every n-th message to the n-th consumer" without looking at outstanding deliveries —
turn-based, not capacity-based [48]. With `prefetch_count`, a consumer stops receiving once its window is full: tutorial 2 prescribes 1 for strictly
fair dispatch, the confirms guide 100-300 for throughput [8][48]. Consumer priorities layer on top, "blocked" including having hit the prefetch limit
[23].

**P4 — Consumer slower than producer.** The backlog sits in the queue on the broker, moved to disk aggressively [12]. Bounds are opt-in
(`x-max-length`, `x-max-length-bytes`): at the bound `drop-head` (default) discards the oldest silently, `reject-publish` discards the newest and
nacks a confirming publisher, `reject-publish-dlx` also dead-letters [18]. Unbounded, growth continues until a memory or disk alarm blocks every
publishing connection cluster-wide while consumers keep running [38]; before that, internal credit flow throttles publishers into `flow` state [36].

**P5 — Late joiner needing current state.** Nothing in AMQP 0-9-1: delivery is once and destructive, and a message published with no queue bound is
gone [48]. No retained message, no last-value cache, no replay. The answer is a stream — an append-only, non-destructively read log
(`x-queue-type = stream`) consumed with `x-stream-offset` = `first`, `last`, `next`, an offset, a timestamp (POSIX seconds, one-second accuracy over
AMQP 0-9-1) or an interval, clamped when unavailable, retained by `max-age`/`max-length-bytes` rather than by consumption; broker-side offset tracking
is Stream-plugin-only [14]. Partial classic answers: the Recent History exchange plugin, and MQTT retained messages [9][11].

**P6 — Failover to another broker.** The client reconnects; the protocol re-establishes nothing. Any node may be connected to and quorum-queue
operations are routed to the leader transparently, streams excepted [12][40]. Libraries re-establish connection, listeners, channels,
`basic.qos`/confirm/transaction settings, then exchanges, queues, bindings, consumers [9][10]. Not re-established: in-flight publishes (lost, not
buffered), `basic.get`, publisher sequence numbers (they restart), and unacknowledged deliveries, which the broker requeues [50][51]. Java, .NET and
Bunny automate it; Pika does not [50][51][52].

**P7 — Surviving a restart.** Persisted: durable queue and exchange metadata, bindings between durable endpoints, and `delivery-mode = 2` messages in
durable queues [4][5][12]; quorum queues always persist [13]. Not persisted: transient messages, "discarded on recovery even from durable queues", and
everything in a transient queue [12]. The publisher decides persistence per message, the declarer queue durability, and both must line up [5]. The
certificate is `basic.ack` in confirm mode — after the disk write (classic), after a majority wrote and flushed (quorum), after quorum replication but
no explicit `fsync` (stream) [8][13][14]. Without confirms there is no certificate and loss is silent [8].

**P8 — Ordering guarantees and scope.** One publishing channel, one exchange, one queue, one consuming channel: publication order [4][6], strengthened
since 2.7.0 to "always held in the queue in publication order" even under requeue or channel closure [6]. No ordering across queues, across publishing
channels, or per routing key. Several consumers on one queue can still observe reordering because others requeue [6], and priorities, `basic.get`,
redelivery and prefetch perturb it [12][19]. Streams give an immutable publish-time offset order; super streams preserve it per partition via SAC
[14].

**P9 — Duplicates and "exactly once".** Sources: retransmission after a lost confirm, automatic requeue, requeue loops, quorum leader election and
partitions, `at-least-once` dead-letter retries, and an RPC server dying between reply and ack [8][16][41][42][48]. Suppression: none. Detection aids
only: `redelivered`, `x-delivery-count`, `x-acquired-count`, `x-death` [4][13][16][42]. Exactly-once is explicitly not offered — "RabbitMQ does not
guarantee exactly-once delivery because retransmission can duplicate" [42]. The one real facility is Stream-protocol publisher deduplication on
producer name plus a strictly increasing publishing ID: opt-in, off by default, one concurrent producer per name, gaps allowed [14].

**P10 — Request/reply.** An exclusive server-named callback queue per client, `reply_to` naming it and a unique `correlation_id`; the server replies
via the default exchange with `routing_key = props.reply_to`, copying `correlation_id` back, and the client discards unknown ones [48]. Correlation is
pure convention: the broker interprets neither property, except in Direct Reply-To, where it rewrites `reply_to` to `amq.rabbitmq.reply-to.<suffix>`
and routes the reply over the requester's own channel with no queue — at-most-once, auto-ack only, dropped if the requester has gone, never returned
as unroutable even with `mandatory` [25]. Routing back is ordinary routing; nothing carries a reply path.

**P11 — Topology.** Brokered always; no brokerless or peer-to-peer mode, and clients are always the initiators — Federation and Shovel are broker-side
plugins, not protocol features [42]. Roles are symmetric: any connection may publish and consume [9][10]. Topology is defined by applications, not
administrators — "AMQP 0-9-1 entities and routing schemes are primarily defined by applications themselves" — so declaration conflicts are an
application concern [5]. Discovery: none in the protocol; clients get endpoint lists, cluster formation uses static config or
DNS/AWS/Kubernetes/Consul/etcd plugins, and Stream clients can query a stream's leader and replicas [15][40]. `queue.declare-ok`'s name, message count
and consumer count is the only topology feedback, and its `consumer-count` is all consumers, not only active ones [2][4].

**P12 — Flow-control credit.** Unit: messages (unacknowledged deliveries); grantor: the consumer, via `basic.qos`; default 0 meaning no limit, with a
server-side `default_consumer_prefetch` [8][24]. On exhaustion the broker stops delivering on that channel until an acknowledgement frees a slot — no
error, no drop, no disconnect [8]. Scoping diverges from the spec: channel-wide there, per new consumer in RabbitMQ, `global = true` selecting the
channel-wide reading, both coexisting independently [24]. `prefetch_size` is unimplemented; `basic.qos` does not affect `basic.get` [4][8].
Publisher-side credit is not exposed — internal flow control surfaces only as `flow` [36], the coarse control being `connection.blocked`/`unblocked`
[30], and `channel.flow(active=false)` is unsupported [4]. Streams are the exception, with chunk credits and a 50000-message publisher window [15].

**P13 — Large messages and streaming bodies.** The body travels in body frames of at most `frame-max` (131072 default), the total as a 64-bit
`body-size` [1][43], capped operationally by `max_message_size` at 16 MiB default, 512 MiB maximum [43]. A body "can be any size, and MAY be broken
into several (or many) chunks" [4], but a receiver MUST reassemble the frames as a single set [4]: the wire format streams, the API does not, and
RabbitMQ never hands a partial body to an application — cancelling a partially-sent content would need a size-1 body frame, which RabbitMQ marks
"planned", stating "the message is always delivered in full to the client" even when rejected mid-send [4]. No chunked or resumable transfer, no
content offset, no size negotiation. Quorum queues call about 1 MiB and up "large" and note the disk footprint [13].

**P14 — Identity.** SASL during the handshake: `PLAIN`, `AMQPLAIN`, `ANONYMOUS`, `RABBIT-CR-DEMO`, or `EXTERNAL` mapping an X.509 subject DN, CN or
SAN to a username [44][45]. An identity is visible per message only if the publisher sets `user-id`, which RabbitMQ validates against the connection's
user; otherwise the publisher is anonymous to consumers, and the `impersonator` tag allows forging it [28]. Granularity: vhost admission, then
per-operation `configure`/`write`/`read` regexes over entity names, plus optional topic routing-key permissions with
`{username}`/`{vhost}`/`{client_id}` expansion [44]. There is no per-message authorisation.

**P15 — What a hostile or buggy peer can make the broker allocate.** Unacknowledged deliveries with `prefetch_count = 0` or auto-ack: unbounded memory
on both sides [8]. Unbounded queues: memory then disk, up to the alarm [12][38]. Leaked channels and connections: node RAM, CPU and Erlang processes,
a file handle plus TCP buffers each [33][34]. Leaked consumers, queues, exchanges and vhosts: metadata replicated to every node [43]. Oversized
messages are capped by `max_message_size`; oversized frames give 501 `FRAME_ERROR` and kill the connection [4][43]. Quorum metadata costs at least 32
bytes per message and 1 MiB per 30,000, plus a 512 MiB default WAL [13]. Counter-measures: the section 11 limits plus operator policies, which
override client arguments and force the lower numeric value [11][43]. Two holes: the memory watermark is a hint a node can exceed [37], and "the
server MUST NOT discard a persistent basic message in case of a queue overflow", recorded as satisfied [4], is contradicted by `drop-head` [18].

**P16 — Observability of delivery.** In-protocol: publisher confirms with sequence numbers and `multiple`, `basic.return`, consumer acknowledgements,
`redelivered`, `queue.declare-ok`'s message and consumer counts, `basic.get-ok`'s `message-count`, `connection.blocked`/`unblocked`, server-sent
`basic.cancel`, and close methods carrying reply code, text, class id and method id [2][8][9][27][30]. Message-level: `x-death` with
per-`{queue, reason}` counts and timestamps, `x-first-death-*`, `x-last-death-*`, `x-delivery-count`, `x-acquired-count` [13][16]. Out of band:
management UI, HTTP API and Prometheus metrics — publish and confirm rates, unroutable-dropped and unroutable-returned rates, connection and channel
churn, and consumer capacity, explicitly "merely a hint" [9][10]. No tracing headers: the broker MUST NOT modify existing message information and adds
only the dead-letter headers [4][16].

**P17 — Shutdown.** Orderly close is a handshake (`channel.close`/`close-ok`, `connection.close`/`close-ok`), after which received methods other than
close and close-ok MUST be discarded [4]. No linger, no drain. Closing a channel requeues every unacknowledged delivery on it — also the documented
way to *deliberately* requeue in-flight deliveries, since `basic.cancel` does not [8][10] — and acks sent immediately before closing may never reach
the queue [34]. Auto-delete queues are deleted synchronously with `basic.cancel`, channel close and connection close; exclusive queues with channel
and connection close [3]. Durable-queue contents stay; transient and exclusive queue contents die with the queue [12]. `queue.delete` discards
contents, the spec's dead-letter-on-delete suggestion being marked "doesn't" [4]. Messages already written to the socket are not guaranteed to have
arrived, which is what confirms are for [9].

**P18 — Transports.** The specification defines TCP, port 5672 [2]. RabbitMQ adds TLS on 5671 [35][45]; PROXY protocol v1 and v2 in front of either,
which when enabled requires *all* clients to arrive through a proxy [35]; and, for streams only, a separate binary protocol on 5552, TLS 5551 [15]. No
AMQP 0-9-1 over WebSocket, no QUIC, no IPC, no in-process transport. TLS adds `ssl_handshake_timeout` (5 s), enables X.509 identity through
`EXTERNAL`, and restricts clients on a TLS-1.3-only listener [43][45]; the Stream protocol changes the delivery model entirely — chunk credits,
offsets, server-side offset tracking, publisher deduplication, super streams, its own single active consumer — and requires connecting to a node
holding a replica [14][15]. One port carries AMQP 0-9-1 and AMQP 1.0, distinguished by protocol header, with 0-8/0-9/0-9-1 all accepted [4][64].

## 13. Ecosystem

**Officially supported clients.** The developer-tools page marks with a tick those supported by Team RabbitMQ and VMware; absence means community
status, not incompatibility [49]. AMQP 0-9-1: Java, .NET/C#, Go (`amqp091-go`), Python (`pika`), PHP (`php-amqplib`), Erlang, Ruby (Bunny), Swift 6
(`bunny-swift`); JavaScript/Node (`amqplib`) carries no tick. AMQP 1.0 clients now shipped: Java, .NET, Go, Python, Erlang. Stream clients: Java,
.NET, Go, Rust, Python `rstream` [49].

| Rust crate | Version / date | Status and shape |
| --- | --- | --- |
| `lapin` [60] | 4.11.0, 2026-09-08 | Async AMQP 0-9-1 client, the de facto Rust choice; latest release and latest `main` commit both 2026-09-08. Runtime-agnostic with exactly one feature-selected runtime: `tokio` (default), `smol` or `async-global-executor`, plus custom `async_rs::Runtime`. Publisher confirms supported (`basic_publish(...).await?.await?`, with a `publisher_confirms.rs` example). `.enable_auto_recover()` reconnects and replays exchanges, queues, bindings and consumers; a recoverable channel error is handled with `channel.wait_for_recovery(error).await`. TLS via exactly one of `rustls` (default), `native-tls` or `openssl`. Not marked officially supported [49] |
| `amqprs` [61] | 2.1.5, 2026-03-19; `main` 2026-05-24 | Async, deliberately lock-free AMQP 0-9-1 client, Tokio-based, API modelled on the Python client, MSRV 1.71. Confirms via `Channel::confirm_select`; `ServerCapabilities::publisher_confirms()` exposes the negotiated capability. Optional `tls` and `urispec` features; v1 unmaintained. Automatic reconnect and topology replay are **not** documented, so recovery is application-managed. Not marked officially supported [49] |
| `amq-protocol` [62] | 10.6.3, 2026-08-03 | Not a client: AMQP 0-9-1 codec, types, URI parser, TCP/TLS glue and a code generator driven by the RabbitMQ spec XML. Used by `lapin`; same runtime and TLS feature matrix. Confirms and recovery are the caller's problem |
| `amqp_serde` [66] | 0.4.3, 2025-10-27 | Serde implementation for AMQP 0-9-1 types, shipped from the `amqprs` repository. Types and serialisation only |
| `rabbitmq-stream-client` [63] | 0.11.0, 2026-03-18 | The **Stream protocol** client, not AMQP 0-9-1, and the only Rust client with the official tick [49]. Three publish APIs: async `send` with internal buffering and a confirmation callback, async `batch_send`, synchronous `send_with_confirm`. Supports super streams, hash and routing-key producers, single active consumer, server-side Bloom-filter filtering (3.13+), load-balancer mode. TLS via `TlsConfiguration`. Examples use `#[tokio::main]`; automatic recovery is not documented |

**Client conventions for reconnect and topology recovery.** Java and .NET recover in the order given in section 9 and are explicit about the gaps:
`basic.get` is not recovered because it is not a subscription, unacknowledged deliveries are requeued by the broker instead, and publisher sequence
numbers restart and must not be assumed continuous [50]. .NET enables it with `AutomaticRecoveryEnabled`, has `TopologyRecoveryEnabled` on by default,
retries at `NetworkRecoveryInterval` (5 s), triggers on I/O-loop exceptions, socket read timeouts and missed heartbeats but not on an initial
connection failure, an application close or a channel-level exception, loses anything published while down because there is no outgoing buffer, and
adjusts delivery tags to stay monotonic while suppressing stale acknowledgements [51]. Pika provides no automatic topology recovery: its official
example shuffles a host list, builds a fresh `BlockingConnection`, redeclares the queue and re-registers `basic_consume` after each
`AMQPConnectionError`, and deliberately does not recover from `AMQPChannelError` [52].

**Known incompatibilities between implementations** [3][53]. AMQP 0-9-1 and AMQP 1.0 are different protocols with different client libraries and no
wire-level overlap [4][64]. Field type tags: RabbitMQ keeps the Qpid/Rabbit tags where `s` is signed 16-bit and `l` signed 64-bit, against 0-9-1's
short-string `s` and unsigned `l`, and adds `x` for byte arrays [3] — any client generated strictly from the published spec will mis-decode RabbitMQ
tables. RabbitMQ declares the `amq.*` exchanges durable while the Qpid Java client forcibly re-declares them non-durable, producing
`PRECONDITION_FAILED` and "breaking interoperability almost completely" [3][53]. Qpid 0.6 Java had a negotiation bug and defaulted to AMQP 0-10, so it
could not connect to 0-8 or 0-9-1 brokers; Qpid Ruby/.NET 0.6 defaulted to 0-8 while sending 0-9-1 methods, causing framing errors; the Qpid C++ 0.6
broker spoke only 0-10, so no RabbitMQ client could reach it [53]. Against the Qpid Java broker, RabbitMQ clients lose `exchange.delete(if-unused)`,
auto-delete exchanges and alternate exchanges, hit transaction and QoS semantic differences, and cannot use vhost `/`, their own default [53]. The
interoperability page has no ActiveMQ AMQP 0-9-1 findings at all [53].

**Deployment scale.** No cited source makes a general production claim. The authoritative figures are RabbitMQ's own 2020 cluster-sizing case studies:
7x16 and 9x8 clusters reaching 65-70k msg/s under ideal conditions and 20k msg/s (1.7 billion messages/day) under adverse ones, with backlogs near 6-7
million messages still handled. The authors call very large backlogs one of RabbitMQ's hardest cases and note their tests ran only 10 minutes to 1 h
40 [57].

## 14. Sources

Official specification first, official guide second, maintainer-written material third, everything else marked. All read 2026-09-08; the version for
`rabbitmq.com/docs` pages is the page's own version badge.

1. AMQP 0-9-1 specification PDF, AMQP Working Group, rev 0-9-1, AMQP license — https://github.com/rabbitmq/amqp-0.9.1-spec/blob/main/pdf/amqp0-9-1.pdf — framing, protocol header, content, body size.
2. AMQP 0-9-1 machine-readable spec `amqp0-9-1.stripped.xml`, copyright 2009 AMQP Working Group, BSD-licensed (RabbitMQ's copy, which also carries `connection.blocked`/`unblocked`) — https://raw.githubusercontent.com/rabbitmq/amqp-0.9.1-spec/main/xml/amqp0-9-1.stripped.xml — constants, reply codes and soft/hard class, domains, all classes and methods with fields and indices, `basic` properties.
3. AMQP 0-9-1 Errata, RabbitMQ, no version metadata — https://www.rabbitmq.com/amqp-0-9-1-errata — 30 numbered deviations.
4. Compatibility and Conformance, 4.3 — https://www.rabbitmq.com/docs/specification — class/method status, conformance table.
5. AMQP 0-9-1 Model Explained, tutorials, current — https://www.rabbitmq.com/tutorials/amqp-concepts — the model, exchange types, entity attributes, 406/403, channels, vhosts, ack modes.
6. Broker Semantics, 4.3 — https://www.rabbitmq.com/docs/semantics — `tx` semantics, ordering since 2.7.0, exclusive queues.
7. AMQP 0-9-1 Protocol Extensions, 4.3 — https://www.rabbitmq.com/docs/extensions — extension inventory, `update-secret`.
8. Consumer Acknowledgements and Publisher Confirms, 4.3 — https://www.rabbitmq.com/docs/confirms — delivery tags, ack modes, prefetch, automatic requeueing, confirm mode, what confirms certify, ack latency.
9. Publishers, 4.3 — https://www.rabbitmq.com/docs/publishers — publishing model, properties, unroutable handling, confirm strategies, recovery steps, alarm effects, metrics.
10. Consumers, 4.3 — https://www.rabbitmq.com/docs/consumers — lifecycle, recovery order, prefetch, ack timeout, exclusivity, single active consumer, priorities, queue parallelism.
11. Exchanges, 4.3 — https://www.rabbitmq.com/docs/exchanges — exchange types, default exchange, bindings, precedence, E2E.
12. Queues, 4.3 — https://www.rabbitmq.com/docs/queues — names, properties, equivalence, FIFO, durability, 4.3.0 changes.
13. Quorum Queues, 4.3 — https://www.rabbitmq.com/docs/quorum-queues — Raft, confirms, feature matrix, `delivery-limit`, overflow, at-least-once dead lettering, WAL sizing.
14. Streams, 4.3 — https://www.rabbitmq.com/docs/streams — append-only log, `x-stream-offset`, retention, replication, confirms without `fsync`, deduplication, super streams.
15. Stream Plugin, 4.3 — https://www.rabbitmq.com/docs/stream — ports, chunk and publisher credits, `stream.frame_max`.
16. Dead Letter Exchanges, 4.3 — https://www.rabbitmq.com/docs/dlx — reasons, keys, strategies, `x-death` headers.
17. Time-To-Live and Expiration, 4.3 — https://www.rabbitmq.com/docs/ttl — message and queue TTL, TTL 0, head-of-queue.
18. Queue Length Limit, 4.3 — https://www.rabbitmq.com/docs/maxlength — limits, ready-only counting, overflow modes.
19. Priority Queue Support, 4.3 — https://www.rabbitmq.com/docs/priority — classic and quorum priorities, ordering effects.
20. Alternate Exchanges, 4.3 — https://www.rabbitmq.com/docs/ae — argument and policy key, chaining, `mandatory`.
21. Exchange to Exchange Bindings, 4.3 — https://www.rabbitmq.com/docs/e2e — `exchange.bind`, cycles, one copy per queue.
22. Sender-Selected Distribution, 4.3 — https://www.rabbitmq.com/docs/sender-selected — `CC`/`BCC` semantics.
23. Consumer Priorities, 4.3 — https://www.rabbitmq.com/docs/consumer-priority — `x-priority`, meaning of "blocked".
24. Consumer Prefetch, 4.3 — https://www.rabbitmq.com/docs/consumer-prefetch — the `global` reinterpretation table.
25. Direct Reply-to, 4.3 — https://www.rabbitmq.com/docs/direct-reply-to — pseudo-queue semantics and limitations.
26. `basic.nack`, 4.3 — https://www.rabbitmq.com/docs/nack — fields, batching, effect on quorum `delivery-count`.
27. Consumer Cancellation Notification, 4.3 — https://www.rabbitmq.com/docs/consumer-cancel — capability and triggers.
28. Validated User-ID, 4.3 — https://www.rabbitmq.com/docs/validated-user-id — validation, `impersonator`, federation.
29. Authentication Failure Notification, 4.3 — https://www.rabbitmq.com/docs/auth-notification — capability, 403.
30. Blocked Connection Notifications, 4.3 — https://www.rabbitmq.com/docs/connection-blocked — methods and capability.
31. AMQP 0-9-1 spec differences, 4.3 — https://www.rabbitmq.com/docs/spec-differences — undeprecations, `amq.` naming.
32. Heartbeats, 4.3 — https://www.rabbitmq.com/docs/heartbeats — 60 s default, negotiation, two missed intervals.
33. Connections, 4.3 — https://www.rabbitmq.com/docs/connections — lifecycle, capabilities, exception classes, churn.
34. Channels, 4.3 — https://www.rabbitmq.com/docs/channels — multiplexing, exceptions, `channel_max`, requeue on close.
35. Networking, 4.3 — https://www.rabbitmq.com/docs/networking — ports, PROXY protocol, TCP tuning, handshake timeout.
36. Flow Control, 4.3 — https://www.rabbitmq.com/docs/flow-control — internal credit flow and the `flow` state.
37. Memory Threshold and Limit, 4.3 — https://www.rabbitmq.com/docs/memory — watermark 0.6, absolute form, fallback.
38. Memory and Disk Alarms, 4.3 — https://www.rabbitmq.com/docs/alarms — blocking scope, cluster-wide effect, backpressure.
39. Disk Alarms, 4.3 — https://www.rabbitmq.com/docs/disk-alarms — `disk_free_limit` 50 MB, check interval.
40. Clustering, 4.3 — https://www.rabbitmq.com/docs/clustering — replicas, leader routing, locator, ports, node counts.
41. Network Partitions, 4.3, reworked for 4.3.0 — https://www.rabbitmq.com/docs/partitions — Khepri, removal of Mnesia-era strategies, detection, leader and follower disconnection, duplicates.
42. Reliability Guide, 4.3 — https://www.rabbitmq.com/docs/reliability — failure taxonomy, at-least-once, non-guarantees.
43. Configurable Limits and Timeouts, 4.3 — https://www.rabbitmq.com/docs/limits — every default cited in section 11.
44. Access Control, 4.3 — https://www.rabbitmq.com/docs/access-control — mechanisms, `guest`, the permission table, topic authorisation, `auth_backends`, credential rotation.
45. TLS Support, 4.3 — https://www.rabbitmq.com/docs/ssl — listeners, peer verification, versions, `ssl_cert_login_from`.
46. Virtual Hosts, 4.3 — https://www.rabbitmq.com/docs/vhosts — separation and per-vhost limits.
47. OAuth 2 Support, 4.3 — https://www.rabbitmq.com/docs/oauth2 — JWT as password, `aud`, scope translation.
48. RabbitMQ tutorials 1-6, Python, current, targeting 4.x — https://www.rabbitmq.com/tutorials/tutorial-one-python through `-six-python` — who declares what, bindings, routing, acks, round-robin unfairness, durability caveat, topics, RPC. Tutorial 7 (Publisher Confirms) exists for Java, Kotlin, Ruby, C#, PHP and Go, not Python.
49. Client Libraries and Developer Tools, current — https://www.rabbitmq.com/client-libraries/devtools — support ticks.
50. Java Client API Guide, recovery, current — https://www.rabbitmq.com/client-libraries/java-api-guide#recovery — what automatic recovery restores and what it does not.
51. .NET Client API Guide, recovery, client 7.0 — https://www.rabbitmq.com/client-libraries/dotnet-api-guide#recovery — recovery flags, 5 s interval, triggers, lost publishes, delivery-tag adjustment.
52. Pika docs, blocking consume with recovery over multiple hosts, stable — https://pika.readthedocs.io/en/stable/examples/blocking_consume_recover_multiple_hosts.html — caller-written recovery.
53. Interoperability, current — https://www.rabbitmq.com/client-libraries/interoperability — Qpid and OpenAMQ findings.
54. Release Information, read 2026-09-08 — https://www.rabbitmq.com/release-information — 4.3.5 on 17 Aug 2026; 4.3.0 on 23 Apr 2026.
55. RabbitMQ 4.1.0 release notes, 2025-04-15 — https://www.rabbitmq.com/blog/2025/04/15/rabbitmq-4.1.0-is-released — `frame_max` 131072, initial frame 4096 -> 8192, `max_message_size` 16 MiB.
56. Jack Vanlightly, "RabbitMQ gets an HA upgrade", official RabbitMQ blog, 2020-04-20 — https://www.rabbitmq.com/blog/2020/04/20/rabbitmq-gets-an-ha-upgrade — maintainer-written mirrored-queue data-loss modes, partition trade-offs, Raft quorum queues; 3.8-era, mirroring removed in 4.x [12].
57. RabbitMQ cluster sizing case studies, official blog, 2020-06-18/06-20/06-22 — https://www.rabbitmq.com/blog/2020/06/18/cluster-sizing-and-other-considerations plus both case-study parts — maintainer-written throughput and backlog figures.
58. THIRD-PARTY: CloudAMQP, "How to optimize the RabbitMQ prefetch count", 2020-08-19 — https://www.cloudamqp.com/blog/how-to-optimize-the-rabbitmq-prefetch-count.html — prefetch heuristics and failure mode.
59. THIRD-PARTY: CloudAMQP, "13 common RabbitMQ mistakes", updated 2025-01-17 — https://www.cloudamqp.com/blog/part4-rabbitmq-13-common-errors.html — churn costs, short queues, queues per core.
60. `lapin` crate 4.11.0, 2026-09-08 — https://crates.io/crates/lapin, https://github.com/amqp-rs/lapin — Rust client.
61. `amqprs` crate 2.1.5, 2026-03-19 (`main` 2026-05-24) — https://crates.io/crates/amqprs, https://github.com/gftea/amqprs — Rust client capabilities and documented gaps.
62. `amq-protocol` crate 10.6.3, 2026-08-03 — https://crates.io/crates/amq-protocol, https://github.com/amqp-rs/amq-protocol — codec and transport building blocks.
63. `rabbitmq-stream-client` crate 0.11.0, 2026-03-18 — https://crates.io/crates/rabbitmq-stream-client, https://github.com/rabbitmq/rabbitmq-stream-rust-client — Stream protocol client features.
64. Protocols, and AMQP 1.0 in RabbitMQ, 4.3 — https://www.rabbitmq.com/docs/protocols, https://www.rabbitmq.com/docs/amqp — AMQP 1.0 versus 0-9-1, both on 5672 by protocol header.
65. AMQP 0-9-1 Quick Reference, spec archive `main` branch, read 2026-09-08 — https://raw.githubusercontent.com/rabbitmq/amqp-0.9.1-spec/main/docs/amqp-0-9-1-quickref.md — method signatures and support levels; `basic.nack` marked a RabbitMQ extension.
66. `amqp_serde` crate 0.4.3, 2025-10-27 — https://crates.io/crates/amqp-serde — Serde types for AMQP 0-9-1.

### Recorded disagreements between sources

- **Name length.** The spec's `queue-name` and `exchange-name` domains assert a maximum length of 127 [2]; the queues and concepts guides say up to
  255 bytes of UTF-8 [5][12]. The spec's character-set assertion `^[a-zA-Z0-9-_.:]*$` [2] is, RabbitMQ says, "not enforced by the server" [4].
- **When `basic.nack` reaches a publisher.** The confirms guide says it "will only be delivered if an internal error occurs in the Erlang process
  responsible for a queue" [8], but the queue-length guide says a `reject-publish` overflow produces `basic.nack` [18] and the partitions guide says
  confirms pending during a Raft election "can receive negative acknowledgement" [41]. The "only" is wrong for 4.3.
- **Quorum prefetch ceiling.** The confirms guide states quorum queues cap consumer prefetch at 2,000 to limit Raft log growth [8]; the quorum-queues
  guide mentions no such ceiling and only requires per-consumer QoS [13].
- **Stream protocol frame size.** The limits guide calls `frame_max` (131072) the maximum AMQP 1.0, AMQP 0-9-1 *and* RabbitMQ Stream Protocol frame
  size [43], while the Stream plugin guide gives the Stream protocol's default maximum frame size as 1 MiB via `stream.frame_max` [15].
- **Network-partition handling.** The clustering guide still refers to partition-handling modes [40], while the partitions guide, reworked for 4.3.0,
  says the Mnesia-era strategies (`ignore`, `pause_minority`, `pause_if_all_down`, `autoheal`) were removed with Mnesia [41]; the maintainer blog
  explaining them predates the change [56]. The 4.3 partitions guide is current, and no successor setting replaces them.
- **Transient classic queues.** The classic-queues guide says transient-queue and global-QoS support "will be removed in RabbitMQ 4.0"; the current
  queues guide says such queues became disabled by default in 4.3.0, re-enabled with `deprecated_features.permit.transient_nonexcl_queues = true`
  [12]. The former is stale.
- **Field type tags.** 0-9-1 assigns `s` to short string and `l` to unsigned 64-bit; RabbitMQ keeps the earlier Qpid/Rabbit meanings, signed 16-bit
  and signed 64-bit, and adds `x` [2][3].
- **Heartbeat frame type.** The specification PDF says frame type 4, the XML says 8; RabbitMQ, Qpid and OpenAMQ all send type 8 [3].
- **Persistent messages and overflow.** The conformance table records "the server MUST NOT discard a persistent basic message in case of a queue
  overflow" as satisfied [4], but `drop-head` discards regardless of delivery mode [18].
- **Quorum `delivery-limit` spelling.** The quorum policy table lists `delivery-limit` while its prose names the queue argument `x-delivery-limit`
  [13].
- **`x-max-priority` guidance.** The priority guide recommends single-digit maxima [19], the limits guide says 1-5 [43]; the valid range 1..255 is the
  same in both.
- **Quorum replication factor.** The 2020 maintainer blog says the default is 5, constrained down to cluster size [56]; the 4.3 guide says the default
  initial group size is 3 [13].
- **Prefetch heuristics.** CloudAMQP recommends 1 for many or slow consumers and round-trip time divided by processing time for few fast ones [58];
  the official guide recommends 100-300 [8]. Neither is a guarantee.
