# Patterns

The reference for what each weida pattern does, in the shape of `zmq_socket(3)`: one table
per pattern naming its compatible peer, direction, routing strategy and behaviour when it has
nowhere to send, followed by what happens at every failure. Where ZeroMQ's tables describe
what its threads and queues do, these describe what the **transport** does, because a weida
pattern is a thin wrapper over transport streams and inherits its behaviour from them.

Three transports carry those streams today ([decisions/0010](decisions/0010-local-transport.md),
[0012](decisions/0012-local-connection-grouping.md)): QUIC, an in-process channel pair, and
`AF_UNIX`, where the OS connection **is** the stream. Every statement below is written for
QUIC unless it says otherwise, because QUIC is the transport whose flow control the patterns
were designed against; §1.10 says which of them mean something different locally, and the
failure tables name the transport where the two diverge.

Every statement below that could be false is defended by a test named in the text. The
measured numbers are from `crates/weida/tests/streams.rs` on loopback with `quinn 0.11`; the
statements hold on any link, the numbers do not. `crates/weida/tests/transports.rs` runs one
Req/Rep, Push/Pull and Pub/Sub body over all three transports, which is what makes the
pattern semantics a fact about weida rather than about QUIC.

Related: [ARCHITECTURE.md](ARCHITECTURE.md) (layer model, primitives P1-P4),
[GUARANTEES.md](GUARANTEES.md) (the vocabulary), [FAILURE_MODEL.md](FAILURE_MODEL.md)
(outcome rules), [PROTOCOL.md](PROTOCOL.md) (the wire).

---

## 1. Common ground: what a stream is

Every user data flow is one stream: one QUIC stream over the network, one channel pair in
process, one `AF_UNIX` connection locally — "the OS connection **is** the stream" [0010 §4.2].
A Push message, a published copy, the request half of an exchange and its reply half are each
a stream of their own. What follows is true of all of them, whichever pattern opened them and
whichever transport carries them, except where §1.10 says a local transport differs.

### 1.1 `finish()` is a commitment

Once `OutgoingTransfer::finish` has queued the FIN, the payload arrives without any local
handle. The transfer is consumed, the `Delivery` may be dropped, the endpoint may go out of
scope; only the connection has to live, and the runtime's pool holds it.
`Runtime::shutdown` is the one thing that cuts a finished transfer short. Its counterpart is
`Runtime::drain(deadline)`: it stops admitting work, gives the transfers that were already
finished until the deadline to reach the peer's **transport**, and only then performs the
same close ([decisions/0009](decisions/0009-drain.md) §4.1-§4.6). What comes back is a pair
of counts, `Drained { delivered, outstanding }`, not a promise — and an expired drain with
something still outstanding is not an error. The deadline is mandatory and finite: there is
no infinite variant to set by accident [0009 §4.3].

*`a_finished_transfer_needs_no_local_handle_to_arrive`,
`a_finished_transfer_that_shutdown_cuts_short_arrives_under_drain`.*

### 1.2 The receipt, inside and beyond the window

`Delivery::delivered()` resolves when the peer's **transport** holds every byte and the FIN.
Inside the peer's stream receive window that is all it means: the receipt resolves before the
peer's application has called `recv`, and even while the transfer is still parked in the
peer's accept queue.

Beyond the window it means more, because QUIC cannot accept more bytes than the window until
the application has consumed some. For a payload of `p` bytes against a window of `w`, the
receipt cannot resolve until the reader has consumed at least `p - w` bytes, rounded up to the
next eighth of a window (quinn announces window credit in eighths). So a receipt for a large
transfer is evidence that the application is reading it; a receipt for a small one is not.

*`push_delivery_receipt` (1 KiB, resolves before `recv`);
`a_receipt_beyond_the_window_implies_the_reader_consumed` (160 KiB against a 64 KiB window:
the write completed once 131072 bytes were consumed, the receipt at 163840);
`the_stream_budget_is_backpressure_not_an_error` (receipt resolves for a transfer still in
the accept queue).*

### 1.3 Two windows, one shared

Flow control is per stream and per connection, and the two behave differently:

- **Per stream, transfers are isolated.** A stream nobody reads does not delay its siblings.
- **Per connection, one slow reader stalls every other writer on that connection** — and
  "that connection" is now one **endpoint path**. The connection window is shared by every
  stream on the connection. Unread streams consume it; when it is spent, every writer on the
  connection blocks until the slow reader consumes at least an eighth of the window. Since
  one connection per dialled path
  ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2), "everyone" means
  the writers on that path and nothing else: another path is another connection and another
  window. What is *not* isolated is a path's own control traffic: a SUBSCRIBE rides the
  connection of the path it names, because that is the only route back to the subscriber
  ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1-§4.2), so an endpoint
  that publishes *and* subscribes on one path can queue its own subscription behind its own
  payload. A pure subscriber writes nothing there and a pure publisher sends no SUBSCRIBE,
  so neither is affected [0011 §4.4].
- **The header spends the window too.** The DATA header rides on the transfer's own stream, so
  the payload that fits in one window is `stream_receive_window - header`, and the last eighth
  only clears once the application reads. A payload sized exactly to the window therefore
  blocks until the reader starts. `stream_receive_window` is a buffer size, not a message size.

*`a_stalled_stream_does_not_block_its_siblings` (64 KiB stream window, 256 KiB connection
window, 32 KiB per stream: siblings flow past an unread stream; the seventh unread stream
stalls the connection at 229376 bytes; reading the first one releases it);
`a_stalled_path_does_not_stall_another_path` (the same numbers on two paths: one path's window
is filled until a write parks, and a send on the other path arrives anyway);
`a_payload_the_size_of_the_stream_window_waits_for_the_reader`.*

### 1.4 The stream budget is backpressure, and it lands on `open`

A peer grants `max_concurrent_uni_streams` and `max_concurrent_bidi_streams`. A transfer holds
its stream until it has been read to EOF, dropped or refused — including while it sits in a
bounded accept queue. When the budget is spent, the next `open` waits. It does not fail, and
`write_all` and `finish` are never reached. A deeper `endpoint_queue` changes nothing: a queued
transfer still owns its stream.

*`the_stream_budget_is_backpressure_not_an_error`,
`a_deeper_endpoint_queue_does_not_raise_the_stream_budget`,
`a_replier_that_stops_accepting_stalls_requesters_after_the_queue_fills` (bidi budget 2,
queue 1: exactly two exchanges complete, the third waits in `open`, one `accept` releases it).*

**The stream budget *is* weida's message credit at L0.** There is no application credit, at
L0 or on the wire: QUIC's byte windows are the byte credit and the concurrent-stream budget is
the message credit, both receiver-granted through transport parameters and both absolute and
idempotent ([decisions/0003](decisions/0003-credit-unit.md) §4.1). A consumer that wants a
prefetch of *n* messages grants `max_concurrent_uni_streams = n` on the connection it reads
from — one per dialled path, per §1.3 — and that is the whole mechanism
[0003 §5]. A per-subscription message credit arrives with the L2 broker and travels on the
connection of the path its subscription names, never at L0 [0003 §4.2, as amended by
[0011](decisions/0011-answered-where-it-arrived.md) §4.3].

### 1.5 Cancel: never EOF, not a retraction

`OutgoingTransfer::cancel` (and dropping an unfinished transfer) resets the stream. The reader
never observes the transfer as complete: `AsyncRead` fails with `io::ErrorKind::ConnectionReset`,
`read_capped`/`collect` with `Error::Canceled`, never `Ok(0)`. Bytes the reader already took
are unaffected. Bytes already buffered at the receiver may still be read before the reset is
processed: cancellation guarantees the peer cannot mistake the transfer for a whole one, not
that the peer saw fewer bytes.

*`cancel_discards_unread_bytes_and_keeps_read_ones`, `push_cancel_mid_transfer`,
`cancel_mid_transfer`.*

### 1.6 Refusal of a one-way transfer can lose the race to the receipt

A peer refuses a one-way transfer with `STOP_SENDING` and a code (`UNKNOWN_ENDPOINT`,
`UNSUPPORTED`, `REJECTED`). That is an application act, and it races the transport
acknowledgement: a payload that fits in flight can be acknowledged by the peer's transport
before its application refuses it, and `delivered()` then resolves `Ok` — truthfully, since a
receipt says nothing about the application, including that it said no. **This is a decided
position, not an open question** ([decisions/0005](decisions/0005-refusal-race.md)): no
application-level signal is added to the L0 wire to order a refusal ahead of the receipt, and
the deterministic counterpart is the reserved `Accepted` of an L2 broker hop. A refusal is
guaranteed to be observed in exactly two constructions [0005 §4.3]: a payload beyond the peer's
stream receive window, where flow control forces the application to act before the write can
finish, and an exchange, whose ERROR frame is written by the application on the reply half and
takes precedence over the request half's receipt. Once the receipt has resolved, a later
refusal reaches **no observer at all** — the `Delivery` is consumed, and no counter, metric or
late error is added for it [0005 §4.4].

*`push_to_an_unknown_path_is_reported`, `push_to_rep_path_is_unsupported`,
`a_publisher_path_refuses_inbound_transfers` (all with 2 MiB payloads for this reason);
`unknown_endpoint_is_reported`, `request_to_pull_path_is_unsupported` (Req/Rep).*

### 1.7 Ordering is per stream and nothing else

Bytes within a stream arrive in order. Streams arrive in no particular order relative to each
other: a peer that opens A then B may see B dispatched first. Every pattern's ordering is
therefore `None` across messages ([GUARANTEES.md](GUARANTEES.md) §6), and the within-stream
order is the only order there is. An application that needs message order must carry it in
the payload or keep one long-lived stream (§5). The sequence key of
[PROTOCOL.md](PROTOCOL.md) §6.2 will let a receiver *detect* a gap or reassemble in order, and
the cost of reassembling is measured: reverse-order completion forces a reorder buffer of
N − 1 of the transfers in flight, and 84 of 256 were held with no adversarial pattern at all
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-010). Ordering is not free at the receiver, which
is why it is negotiated rather than default ([GUARANTEES.md](GUARANTEES.md) §3).

### 1.8 Liveness: idle timeout, keep-alive, no reconnect

- A connection with no traffic is declared dead after `RuntimeConfig::idle_timeout` (30 s
  default), the smaller of the two peers' values governing both.
- Only the **dialling** side sends keep-alives (`RuntimeConfig::keep_alive`, 10 s default). A
  binding with an idle timeout shorter than its clients' keep-alive interval drops them.
- Loss surfaces as `Error::ConnectionLost(cause)` from the next operation, and the cause is
  kept rather than flattened: `IdleTimeout`, `PeerClosed`, `LocallyClosed`, `Reset` or
  `TransportError`. It is not a second outcome — the failure is definite whichever it is —
  but it is what tells an application whether redialling makes sense. `Peer::peer_count`
  stops counting the dead peer.
- **Nothing reconnects.** The application calls `connect` again, with the same or a new
  address; the dead entry is reaped then. A `Subscriber` re-sends its filters on `connect`.

*`idle_timeout_reports_loss_within_the_window` (server idle timeout 500 ms, client
keep-alive 10 s, `ConnectionLost(IdleTimeout)` after 1.5 s of silence);
`after_the_server_restarts_the_pusher_must_reconnect`.*

### 1.9 Identity: who is on the other side

A peer is named by the SHA-256 fingerprint of its public key (`Fingerprint`, text form
`sha256:<64 hex>`). The dialling side states whom it accepts with `Trust` — pins, anchors, or
only what the address names (`weida://sha256:…@host:port/path`) — and a binding may require a
client identity with `ServerTls::require_client`. Whatever arrives on a stream carries the
peer's identity in `IncomingMeta::peer`: `PeerIdentity::Key(fingerprint)` over QUIC, `None`
for an anonymous client, and `PeerIdentity::Local { uid, gid, pid }` over `AF_UNIX`, where
the **kernel** is the prover instead of TLS and a PID is an observation that must not be
authorized on [0010 §4.4]. In process there is no identity at all, because there is no
boundary to prove anything across. Whichever it is, it comes from the transport and never
from a header, so it can be authorized on but not forged. A peer outside the terms fails
`connect` with `Error::Untrusted(fingerprint)`, carrying what
answered so an operator can pin it after checking it out of band.

*`crates/weida/tests/identity.rs`, all ten.*

### 1.10 What a local transport changes

Everything above is written against QUIC. Over the two local transports of
[0010](decisions/0010-local-transport.md) five of those statements mean something else, and
the difference is always the same cause: there is no QUIC connection to share, because one
transfer is one channel pair (inproc) or one OS connection (`AF_UNIX`).

- **§1.2's window arithmetic does not apply.** A receipt still means the peer's transport
  holds the bytes, but "beyond the window" has no local analogue: inproc hands over a buffer
  and a socket has the kernel's own buffer, so a receipt is never evidence that the
  application is reading. The weaker reading — a transport receipt is not an application
  read — is the one that holds everywhere.
- **§1.3's two windows collapse into one.** There is no connection window to share, so the
  head-of-line coupling the per-path connections of
  [0002](decisions/0002-control-and-bulk-separation.md) exist to remove cannot arise:
  `a_stalled_path_does_not_stall_another_path` is a statement about QUIC, and locally it is
  true by construction. What replaces it is a descriptor count — every live transfer is a
  file descriptor, bounded by `max_local_streams` (255, the number Windows' pipe instance cap
  fixes) rather than by a stream budget.
- **§1.4's stream budget becomes that descriptor count.** `open` waits for a descriptor at
  the local ceiling exactly as it waits for a QUIC stream at the peer's budget: the
  backpressure is a wait on every transport, bounded by the caller's own deadline and
  cancelled by dropping the future (B-059). A descriptor comes free when a transfer ends,
  which locally means when both ends are done with it — so a consumer that stops reading
  slows its producer down instead of failing it. What `LimitExceeded` still means locally is
  a refusal somebody decided on, the reverse pool of §4 below, and never a busy transport.
- **§1.8's liveness is the kernel's.** There is no idle timeout and no keep-alive locally: a
  peer that goes away closes its socket or drops its channel, which arrives as
  `ConnectionLost(PeerClosed)` on the next operation. That is a *better* signal than a timer,
  and it is why the two timers are not simulated — an invented local heartbeat would only be
  able to say what the kernel already said.
- **§4's fan-out needs a parked connection.** A publisher on a socket transport writes each
  copy on a reverse connection the subscriber parked in advance
  ([0012](decisions/0012-local-connection-grouping.md) §4.4), bounded by
  `Limits::max_parked_reverse` (8). An exhausted pool is a **second drop cause** beside the
  subscriber byte budget of §4: the copy is dropped, counted in `Publisher::dropped`, and the
  subscription survives. A subscriber that parks nothing is refused at `connect` rather than
  silently receiving nothing.

*`crates/weida/tests/transports.rs`: the three pattern bodies over all three transports,
`a_unix_peer_presents_the_principal_the_kernel_proved`,
`a_local_peer_that_goes_away_is_reported_as_connection_loss`,
`an_exhausted_reverse_pool_drops_the_copy_and_counts_it`,
`a_subscriber_that_parks_nothing_is_refused_at_connect`.*

---

## 2. Req/Rep

One bidirectional stream per exchange. The requester writes the request on its half and reads
the reply, or an ERROR, on the other. The stream is the correlation; nothing on the wire names
an exchange.

| | `Requester` (`Req`) | `Replier` (`Rep`) |
| --- | --- | --- |
| Compatible peer | `Replier`, `Acceptor` | `Requester`, `Peer::open_bi` |
| Direction | connects | binds |
| Send/receive pattern | any number of concurrent exchanges, each `open` → write → `finish` → `recv` | `accept` → read body → `reply` → write → `finish`, or `refuse(code)`, or drop for `NO_REPLY` |
| Incoming routing | the reply half of the exchange that asked | fair, bounded queue per path (`endpoint_queue`) |
| Outgoing routing | round-robin over live peers, one exchange per pick | the exchange that asked |
| Action with no peer | `Error::NotConnected` immediately; `ConnectionLost` if every peer died | `accept` waits |
| Transport | one client-opened bidirectional stream | |
| Ordering | `None` across exchanges; request and reply each in order | |
| Delivery signal | the reply itself; the request's receipt is available from `open` but proves less than the reply | |
| Backpressure | `max_concurrent_bidi_streams` on `open`, then both halves' windows | a full queue stalls the requester's `open` |
| Cancellation | drop the `ReplyStream`: `STOP_SENDING(CANCELED)` on the reply half, `IncomingRequest::canceled` fires | drop the request: ERROR `NO_REPLY` + `STOP_SENDING(REJECTED)` |

Failure modes, from the requester's side:

| Event | Result of `recv` / `request` |
| --- | --- |
| Path unknown at the peer | `Error::UnknownEndpoint` (ERROR frame) |
| Path serves another pattern | `Error::Unsupported` (ERROR frame) |
| Replier dropped the request | `Error::NoReply`; the request may have had an effect |
| Replier refused on purpose | the code it chose: `Error::Rejected`, or `Error::NoReply` where the request was taken and nobody will answer it — an adapter whose far side dropped it silently |
| Connection lost before the request FIN | `Error::ConnectionLost` from the write: definitely not delivered |
| Connection lost after the FIN, no reply seen | `Error::Indeterminate`: the replier may have acted |
| Replier reset the reply mid-stream | `Error::Canceled` from the read |

Request and reply stream simultaneously: the replier may `take_body` and `reply` before the
request has finished, and a requester writing a large request must drain the reply
concurrently or it stalls the replier and therefore itself
(*`streaming_overlap`*). Router/Dealer are not separate types: unlimited concurrent exchanges
give Dealer's multiplexing, and the reply riding the originating stream gives Router's
addressing for free ([ARCHITECTURE.md](ARCHITECTURE.md) §6a).

---

## 3. Push/Pull

One unidirectional stream per message. Fire-and-forget with an optional transport receipt.

| | `Pusher` (`Push`) | `Puller` (`Pull`) |
| --- | --- | --- |
| Compatible peer | `Puller`, `Acceptor` | `Pusher`, `Peer::open` |
| Direction | connects | binds |
| Send/receive pattern | `send` (returns at FIN, receipt dropped) or `open` → write → `finish` → `delivered` | `recv` → read |
| Incoming routing | — | fair, bounded queue per path (`endpoint_queue`) |
| Outgoing routing | round-robin over live peers, one message per pick | — |
| Action with no peer | `Error::NotConnected` immediately; `ConnectionLost` if every peer died — never blocks, unlike ZeroMQ's PUSH | `recv` waits |
| Transport | one client-opened unidirectional stream | |
| Ordering | `None` | |
| Delivery signal | none (`send`) or the transport receipt (§1.2) | none; EOF is EOF |
| Backpressure | `max_concurrent_uni_streams` on `open` (§1.4), then the windows (§1.3) | a puller that stops reading stalls its pushers after the budget; nothing is dropped |
| Cancellation | `cancel` or drop: the puller's read fails, never EOF (§1.5) | drop an unread transfer: `STOP_SENDING(REJECTED)` |

Failure modes, from the pusher's side:

| Event | `send` | `delivered()` |
| --- | --- | --- |
| Path unknown / wrong pattern | `UnknownEndpoint` / `Unsupported`, or `Ok` if the transport acknowledged first (§1.6) | same |
| Connection lost before FIN | `ConnectionLost` | — |
| Connection lost after FIN | `Ok` (the FIN was queued) | `Indeterminate` |
| Puller refused mid-transfer | `Rejected` | `Rejected` |

Round-robin is per message, and a peer is skipped only once its connection is closed; a peer
that is merely slow keeps receiving its share and eventually stalls the pusher through its
windows. Spreading work by capacity rather than by turn is broker work (L2).
*`push_round_robins_two_peers`*.

---

## 4. Pub/Sub

One unidirectional stream per subscriber per message, opened by the publisher's per-subscriber
writer. Filters are **segmented patterns** carried in SUBSCRIBE/UNSUBSCRIBE frames: segments
separated by `.`, `*` for exactly one whole segment, a trailing `#` for zero or more segments,
everything else literal ([PROTOCOL.md](PROTOCOL.md) §6.4,
[decisions/0007](decisions/0007-topic-namespace.md) §4.2). `sensors.*.temp` selects one
segment, `sensors.#` selects `sensors` and everything under it, and `sensors.temp` does *not*
select `sensors.temperature` — which a byte prefix did, and which is the reason the grammar
changed. Endpoint **paths** are untouched by any of this: they stay opaque and are matched
exactly [0007 §4.1].

| | `Publisher` (`Pub`) | `Subscriber` (`Sub`) |
| --- | --- | --- |
| Compatible peer | `Subscriber` | `Publisher` |
| Direction | binds | connects |
| Send/receive pattern | `publish(topic, bytes)`: synchronous, returns the number of subscribers reached | `subscribe`/`unsubscribe`, then `recv` |
| Incoming routing | — | one bounded queue (`endpoint_queue`) over every peer |
| Outgoing routing | fan-out to every subscriber whose filter matches, one copy each | — |
| Action with no peer | `publish` returns `0`; nothing is queued for a subscriber that does not exist yet | `recv` waits; `peer_count` is the only sign that the publisher is gone |
| Transport | one server-opened unidirectional stream per (subscriber, message) | |
| Ordering | `None`; one subscriber's copies are enqueued in publication order, but that is not a guarantee | |
| Delivery signal | none, and none is possible: `publish` never awaits a subscriber | none |
| Backpressure | `Drop`: a copy that does not fit in `subscriber_buffer_bytes` for that subscriber is dropped and counted in `dropped()`; the publisher never blocks | a subscriber that stops reading fills its budget at the publisher and then loses messages |
| Payload | whole `Bytes`, at most `subscriber_buffer_bytes`; larger is `LimitExceeded` before fan-out | |

Failure modes:

| Event | Publisher | Subscriber |
| --- | --- | --- |
| Slow subscriber | drops for that subscriber only, `dropped()` grows | silently misses messages: nothing on the wire says so |
| Subscriber's connection lost | its filters and writer are removed | `recv` keeps waiting; `peer_count` drops |
| Publisher's connection lost | — | `recv` keeps waiting; filters are remembered and re-sent on the next `connect` |
| Too many filters on one connection | closes it with `LIMIT_EXCEEDED` | `connect`/`subscribe` fails |
| Message beyond the budget | `LimitExceeded`, nothing sent | — |

This is the one place weida answers overload by discarding, and it is confined to fan-out
([GUARANTEES.md](GUARANTEES.md) §6). Today a subscriber cannot detect a drop; **subscriber-side
drop detection is what the sequence key of [PROTOCOL.md](PROTOCOL.md) §6.2 exists for**
([decisions/0001](decisions/0001-sequence-field.md) §7.2). A subscriber that has negotiated
the detect level of `PerProducer` sees the gap — how many messages were missed, expected
against seen — without anything being held back, which is the honest answer to a policy that
drops on purpose. The reassemble level holds messages instead, bounded by
`Limits::max_reorder_hold` and releasing the oldest held transfer with its gap reported at
the cap, at the buffer cost measured in [IMPLEMENTATION.md](IMPLEMENTATION.md) §4 (B-010).
Both are **on the wire and implemented**: DATA keys 6 and 7 carry the sequence and the
producer ([PROTOCOL.md](PROTOCOL.md) §6.2), and a fan-out drop reaches a detecting
subscriber as a `Gap`. Streaming fan-out — a publisher that hands out a stream per
subscriber instead of a `Bytes` — is a recorded deferral.
*`slow_subscriber_drops_not_blocks`, `subscribe_filters_topics_by_segment`,
`a_dropped_fan_out_copy_shows_up_as_a_gap`,
`a_full_hold_reports_the_pub_sub_drop_it_was_waiting_for`.*

---

## 5. Raw streams: `Peer` and `Acceptor`

The L0 core, for topologies the patterns do not cover. A `Peer` dials and opens either stream
kind; an `Acceptor` binds one path and receives both kinds as `Incoming::Stream` or
`Incoming::Exchange`. Everything in §1 applies without translation, and nothing else is added:
no selection policy beyond round-robin over peers, no fan-out, no filters.

Two things the patterns cannot express are natural here:

- **A long-lived stream.** Open once, write many messages with a framing of your own, and the
  transport orders them for you — the only ordered channel weida has, and one every transport
  provides, since a stream is ordered bytes wherever it runs. Over QUIC it keeps its window
  and its place in the budget for as long as it is open, and its reader's pace is its writer's
  pace (§1.3); locally it keeps a descriptor instead (§1.10). A standing feed of frames to one
  viewer is this shape.
- **Both stream kinds on one path.** A control exchange and a bulk one-way stream to the same
  endpoint, dispatched by one accept loop.

*`acceptor_receives_both_stream_kinds`.*

---

## 6. Mapped, not implemented

PAIR, BUS and SURVEYOR/RESPONDENT are mapped onto L0 in [ARCHITECTURE.md](ARCHITECTURE.md)
and deliberately not shipped until a use case asks. Router/Dealer are emergent (§2). Connecting
publishers and binding pushers are recorded deferrals.

---

## 7. Choosing

| You need | Use | Because |
| --- | --- | --- |
| an answer per message | Req/Rep | the reply is the strongest signal weida has (§2) |
| work distributed over workers, nothing lost under load | Push/Pull | backpressure is `Block`; the only losses are explicit refusals and `Indeterminate` after a loss (§3) |
| the newest of a feed, many readers, laggards may lose | Pub/Sub | drops are per subscriber and counted (§4) |
| ordered messages to one peer | a raw stream | QUIC orders bytes within a stream and nowhere else (§1.7, §5) |
| a signal larger than `subscriber_buffer_bytes` to many readers | a raw stream per reader, until streaming fan-out exists | Pub/Sub materializes a copy per subscriber (§4) |
| proof the peer's application acted | Req/Rep, or an L2 broker (Phase 6) | a transport receipt never says that (§1.2); `Accepted`/`Stored`/`Processed` are reserved for a hop that owns the message |
| to know whether the receiver accepted it | Req/Rep | a one-way refusal can lose the race with the receipt and then reaches no observer (§1.6, [decisions/0005](decisions/0005-refusal-race.md) §4.3) |
