# weida wire protocol, version 0

## Status and scope

Wire protocol version: `0`.
Status: **experimental**. Per master doc §15, `0.x` protocol versions are explicitly
experimental and breaking changes are permitted. Implementations MUST NOT assume any
compatibility guarantee across `0.x` releases. The library version (`0.1.0`) and the wire
protocol version (`0`) are independent.

This version is a deliberate break from earlier `0.x` drafts: ACK and CANCEL frames are
gone, the DATA header lost `transfer_id`, `role`, `correlation_id` and `ack_mode`, and
Req/Rep moved onto bidirectional streams. Old and new binaries do not interoperate. The
ALPN token stays `weida/0` because `0.x` carries no compatibility promise to preserve.

This document is the single normative source for the weida wire format. Anything not stated
here is unspecified in v0. Implementations MUST NOT infer wire behaviour from
implementation source; where this document and an implementation disagree, this document is
authoritative and the implementation is a defect.

Related documents: [ARCHITECTURE.md](ARCHITECTURE.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md), [IMPLEMENTATION.md](IMPLEMENTATION.md).

---

## 1. Notation

- The keywords MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted as
  described in RFC 2119.
- Byte values are written in hexadecimal, e.g. `0x57`.
- Byte sequences in code blocks are space-separated hexadecimal octets in wire order.
- "QUIC varint" means the variable-length integer encoding of RFC 9000 §16.
- "uni stream" means a QUIC unidirectional stream; "bidi stream" means a QUIC bidirectional
  stream.
- An **exchange** is one bidi stream. Its **initiating half** is the direction opened by the
  requester; its **reply half** is the other direction.
- "FIN" means the QUIC stream final offset, i.e. clean end of stream.
- "tstr" and "uint" are the CBOR (RFC 8949) major types 3 (text string) and 0 (unsigned
  integer) respectively.
- "peer" means the other side of a QUIC connection, regardless of which side is the QUIC
  client and which the QUIC server.

---

## 2. Connection establishment

### 2.1 Transport

The transport is QUIC. The reference implementation uses `quinn`.

The TLS ALPN token MUST be exactly `weida/0`. A peer MUST offer this token and MUST NOT
accept a connection that negotiated any other token. An ALPN mismatch MUST fail the TLS
handshake; it is not signalled at the weida protocol layer.

### 2.2 HELLO exchange

Immediately after the QUIC handshake completes, each side MUST open exactly one uni stream
carrying a single HELLO frame and MUST then FIN that stream.

A side MUST NOT process any frame other than HELLO before it has processed the peer's HELLO
frame.

QUIC streams are unordered relative to one another: a DATA stream — uni or bidi — MAY be
accepted before the peer's HELLO stream. Receiving DATA before HELLO is **not** a protocol
violation. The receiver MUST park such a stream (retain it, unread beyond its preamble,
without emitting any error) until the peer HELLO has been processed and negotiation has
succeeded, and MUST then process the parked stream normally. In the reference
implementation the parked task awaits a `tokio::sync::watch<Option<Agreed>>`.

If the peer HELLO has not arrived within `hello_timeout` (default 10 s, see §10), the local
side MUST close the connection with application error code `NEGOTIATION_FAILED`.

### 2.3 Negotiation

Negotiation is a pure function of the two HELLO frames. The reference signature is:

```text
negotiate(ours: &Hello, theirs: &Hello) -> Result<Agreed, NegotiateError>
```

The algorithm is:

1. Compute the intersection of `ours.versions` and `theirs.versions`.
2. If the intersection is empty, negotiation fails.
3. The effective version is the maximum element of the intersection.
4. If any code in `theirs.required_capabilities` is outside the local supported capability
   set, negotiation fails. The v0 supported capability set is empty, therefore any non-empty
   `required_capabilities` from the peer MUST fail negotiation.
5. On success the result is

```text
Agreed {
    version: u64,                 // the effective version from step 3
    send_max_header_bytes: u64,   // = theirs.max_header_bytes
}
```

`send_max_header_bytes` is the **peer's** advertised `max_header_bytes`; it bounds the
headers this side may send. The local receive limit remains the local
`limits.max_header_bytes` and is not affected by the peer's advertisement.

Negotiation failure MUST close the connection with `CONNECTION_CLOSE`, application error
code `NEGOTIATION_FAILED`. All in-flight local operations on that connection then resolve
per the connection-loss rules in [FAILURE_MODEL.md](FAILURE_MODEL.md).

### 2.4 Timers

| Timer | Value | Applies to |
| --- | --- | --- |
| Keep-alive | 10 s | client side only |
| Idle timeout | 30 s | both sides |
| `hello_timeout` | 10 s | both sides, until peer HELLO is processed |

---

## 3. Stream framing

Every stream, of either kind and in both directions, MUST begin with the following
preamble. On a bidi stream this holds for **each half independently**: the initiating half
opens with a DATA preamble and the reply half opens with a DATA or ERROR preamble.

```text
[0x57][kind: u8][header_len: QUIC varint][CBOR header: exactly header_len bytes]
```

- `0x57` is the stream magic byte (ASCII `W`).
- `kind` is a single octet, see §4.
- `header_len` is a QUIC varint (RFC 9000 §16). Any of the four varint encoding lengths
  MUST be accepted for a given value; a non-minimal encoding is **not** an error at this
  layer.
- The CBOR header occupies exactly `header_len` bytes.

### 3.1 Header length check

If `header_len` is greater than the local `limits.max_header_bytes` (default `16384`), the
receiver MUST close the connection with `PROTOCOL_VIOLATION` **before allocating** any
buffer for the header. The check MUST precede allocation; allocating and then rejecting is
a defect because it lets a remote peer choose the allocation size.

### 3.2 Conditions that MUST close the connection with PROTOCOL_VIOLATION

- Magic byte not equal to `0x57`.
- Unknown `kind` value, i.e. anything in `5..=255`.
- `header_len` greater than the local `limits.max_header_bytes` (§3.1).
- CBOR parse failure of the header.
- A required key missing from the header.
- A duplicate key in the header map.
- A map key that is not a CBOR unsigned integer.
- A value whose CBOR type does not match the type required for its key.
- A text string longer than the cap defined for its key.
- A frame kind used on a stream kind where §4 does not permit it.
- A DATA frame without `endpoint` on a stream that initiates a transfer (§6.2).

These rules are symmetric: they apply identically to streams received by the QUIC client
and by the QUIC server. All network input is hostile (master doc §81 rule 18); neither role
is trusted.

Where a condition below is specified to produce an ERROR frame or a `STOP_SENDING` rather
than a connection close, that more specific rule governs. Everything in this section is a
connection-fatal framing violation.

---

## 4. Frame kinds

| Kind | Name | Stream shape | Permitted on |
| --- | --- | --- | --- |
| `0` | HELLO | header only, FIN directly after the header | uni |
| `1` | DATA | header followed by opaque payload bytes until FIN | uni, and both halves of a bidi stream |
| `2` | ERROR | header only, FIN directly after the header | **reply half of a bidi stream only** |
| `3` | SUBSCRIBE | header only, FIN directly after the header | uni |
| `4` | UNSUBSCRIBE | header only, FIN directly after the header | uni |

Kinds `5..=255` are reserved and MUST close the connection with `PROTOCOL_VIOLATION`. This
is not a forward-compatibility hook: a receiver cannot know whether an unknown stream kind
carries payload it would have to drain.

HELLO, ERROR, SUBSCRIBE and UNSUBSCRIBE are header-only frames: the sender MUST FIN the
stream immediately after the header. Receiver handling of bytes appearing after the header
on a header-only stream is unspecified in v0; a receiver MAY ignore them and MAY stop
reading the stream after the header.

For DATA, everything after the header up to FIN is opaque user payload. The protocol
imposes no internal framing on the payload: there is no `[length][payload]` chunking inside
a DATA stream.

### 4.1 Which frame may open which stream

- A **uni stream** MUST open with HELLO, DATA, SUBSCRIBE or UNSUBSCRIBE. An ERROR frame on a
  uni stream is a violation (§3.2): an ERROR is the alternative to a reply, and it therefore
  has meaning only where a reply would have gone.
- A **bidi stream** MUST open with DATA on its initiating half. Any other kind there is a
  violation.
- The **reply half** of a bidi stream MUST carry either DATA or ERROR, exactly one frame,
  followed by FIN.

---

## 5. CBOR header encoding rules

Encoder requirements:

- The header MUST be a CBOR **definite-length** map. Indefinite-length maps MUST NOT be
  emitted and MUST be rejected on decode.
- Map keys MUST be CBOR unsigned integers.
- Keys MUST be emitted in ascending numeric order.
- Integers MUST use minimal-length encoding.
- Absent optional keys MUST be omitted entirely. A key MUST NOT be present with a null or
  placeholder value to mean "absent".
- There are no keyed defaults in v0. Every DATA field is genuinely optional and is either
  written or omitted; nothing is encoded to restate a default.

Decoder requirements:

- Unknown unsigned-integer keys MUST be skipped, preserving forward compatibility
  (master doc §15: unknown optional fields must be ignorable).
- Skipping MUST be performed by an iterative, depth-limited skip with `max_depth = 8`;
  exceeding the depth limit is a CBOR parse failure. A recursive skip is a stack-exhaustion
  vector on hostile input and MUST NOT be used.
- Duplicate keys MUST be rejected (§3.2), for every key including extension keys the
  decoder skips.
- Keys MUST be strictly ascending on decode as well as on encode. Rejecting a key that is
  not greater than its predecessor makes duplicate detection complete in constant space; a
  set of seen extension keys would itself be remote-controlled allocation (§10).
- Non-uint keys MUST be rejected (§3.2).
- Indefinite-length byte strings, text strings, arrays and maps MUST be rejected anywhere
  in a header, not only at the top level.
- CBOR tags, half-precision floats and simple values other than `false`, `true`, `null` and
  `undefined` MUST be rejected. Extensions carry plain data items only.
- Bytes remaining after the header map MUST be rejected: `header_len` describes exactly one
  CBOR map.
- A list-valued field MUST NOT declare more than 64 items, and a decoder MUST NOT reserve
  memory from a declared length before that check. Without this bound a peer could pin
  `max_concurrent_uni_streams` worth of large lists by opening many HELLO streams.

The key space `0..=63` is reserved for this specification. Extensions MUST use keys `64`
and above.

---

## 6. Frame headers

### 6.1 HELLO (kind 0)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `[uint]` | `versions` | yes | — | wire protocol versions supported by the sender; v0 sends `[0]` |
| `1` | `uint` | `max_header_bytes` | yes | — | largest header size the sender is willing to receive |
| `2` | `uint` | `max_transfers` | yes | — | maximum concurrent inbound transfers; **advisory in v0**, not enforced |
| `3` | `[uint]` | `capabilities` | yes | — | optional capability codes supported; v0 sends `[]` |
| `4` | `[uint]` | `required_capabilities` | yes | — | capability codes the sender requires the peer to support; v0 sends `[]` |

All five keys are required. A missing key is a framing violation per §3.2.

Capability code assignment is unspecified in v0: no codes are defined and the v0 supported
set is empty.

### 6.2 DATA (kind 1)

| Key | CBOR type | Name | Required at the decoder | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | no | 512 B | endpoint path being addressed |
| `1` | `uint` | `content_len` | no | — | payload length in bytes; **advisory**, not enforced |
| `2` | `tstr` | `content_type` | no | 256 B | opaque media type label |
| `3` | `tstr` | `traceparent` | no | 128 B | W3C Trace Context `traceparent` |
| `4` | `tstr` | `tracestate` | no | 512 B | W3C Trace Context `tracestate`, opaque passthrough |
| `5` | `tstr` | `topic` | no | 256 B | Pub/Sub topic; opaque bytes, matched by byte prefix (§9.5) |

**Every key is optional at the decoder, and that is deliberate.** A decoder sees a byte
slice, not a stream: it cannot tell an initiating half from a reply half, so it cannot
enforce a rule that depends on which one it is looking at. The conditional requirement lives
one layer up, at dispatch, which does know:

- On a stream that **initiates** a transfer — any uni DATA stream, and the initiating half of
  a bidi stream — `endpoint` is REQUIRED. Its absence MUST close the connection with
  `PROTOCOL_VIOLATION` (§3.2). There is nowhere to route the stream and nothing meaningful
  to answer with.
- On the **reply half** of a bidi stream, `endpoint` carries no meaning and MUST be ignored
  if present. The minimal reply header is therefore the empty CBOR map `A0`.

There is no `transfer_id`, no `role` and no `correlation_id`. The stream carries all three:
its kind says whether a reply is expected, its direction says which side initiated, and its
identity is the correlation. Nothing on the wire names an exchange.

`topic` is meaningful only for the fan-out copies a publisher emits (§9.5). It is opaque
bytes: weida never parses it, and no character in it is special.

`content_len` is advisory: the receiver MUST NOT reject a payload for disagreeing with it,
and MUST NOT size an allocation from it.

`traceparent` and `tracestate` carry W3C Trace Context. `tracestate` is opaque to weida and
MUST be forwarded unmodified where trace context is propagated.

### 6.3 ERROR (kind 2)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `code` | yes | — | error code, see below |
| `1` | `tstr` | `message` | no | 1024 B | human-readable detail, not machine-interpreted |

An ERROR frame is legal **only on the reply half of a bidi stream**, where it is the
alternative to a reply (§4.1). It therefore needs no reference to what it answers: the
stream is the reference. The `re` key of earlier drafts is gone.

Error codes:

| Code | Name | Meaning |
| --- | --- | --- |
| `1` | `UNKNOWN_ENDPOINT` | no endpoint is registered for the requested path |
| `2` | `REJECTED` | the receiving side declined to accept the transfer |
| `3` | `UNSUPPORTED` | the endpoint exists but does not serve this stream kind |
| `4` | `INTERNAL` | the receiving side failed internally |
| `5` | `NO_REPLY` | the request was accepted but no reply will be produced |

An ERROR frame resolves the exchange as failed. Codes other than `1..=5` are unspecified in
v0; because an ERROR resolves the exchange regardless of code, a receiver SHOULD treat an
unrecognised code as a generic failure of that exchange rather than as a framing violation.

### 6.4 SUBSCRIBE (kind 3) and UNSUBSCRIBE (kind 4)

Both kinds carry the same header. Only the kind byte distinguishes registering interest
from withdrawing it.

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | yes | 512 B | publisher endpoint path the subscription applies to |
| `1` | `tstr` | `filter` | yes | 256 B | topic prefix; the empty string matches every topic |

Both keys are required. `filter` is required even when empty: an absent key and an empty
string would otherwise be indistinguishable, and the empty filter is the "every topic"
subscription.

Receiver behaviour:

- The filter is a **byte prefix**, not a pattern. A topic matches when `filter` is a prefix
  of `topic` compared byte for byte. No character is special, there is no wildcard syntax,
  and there is no case folding.
- SUBSCRIBE for a filter already held on that connection and path is idempotent.
- UNSUBSCRIBE naming an unknown filter, path or connection MUST be ignored.
- SUBSCRIBE for a path no publisher has registered yet MUST still be recorded: a subscriber
  may connect before the publisher exists, and the subscription is bounded by
  `max_subscriptions` either way.
- When accepting the frame would take the connection past `max_subscriptions` filters
  summed over all paths, the receiver MUST close the connection with `LIMIT_EXCEEDED`. A
  SUBSCRIBE arrives on a uni stream and so has no reply half to carry an ERROR; the
  connection is the only granularity available.
- All subscriptions held by a connection are dropped when that connection closes.
- Like every non-HELLO frame, a SUBSCRIBE that arrives before the peer's HELLO is parked
  until negotiation completes; it is not a violation (§2.2).
- A peer that registers no publishers MAY ignore these frames. Subscribing to a side that
  publishes nothing is useless, not hostile.

---

## 7. QUIC application error codes

These codes are used for `CONNECTION_CLOSE`, `RESET_STREAM` and `STOP_SENDING`. They are
distinct from the ERROR frame codes of §6.3.

| Code | Name | Typical use |
| --- | --- | --- |
| `0` | `NO_ERROR` | orderly close |
| `1` | `INTERNAL` | local failure the peer cannot act on |
| `2` | `PROTOCOL_VIOLATION` | any condition in §3.2 |
| `3` | `CANCELED` | transfer abandoned by either side |
| `4` | `NEGOTIATION_FAILED` | §2.3 failure, or `hello_timeout` expiry |
| `5` | `LIMIT_EXCEEDED` | connection refused because a local limit is reached |
| `6` | `SHUTDOWN` | runtime shutting down |
| `7` | `REJECTED` | receiver declines the inbound payload |
| `8` | `UNKNOWN_ENDPOINT` | requested endpoint path is not registered |
| `9` | `UNSUPPORTED` | endpoint exists but does not serve this stream kind |

`UNSUPPORTED` is new in this revision. A misrouted uni stream has no reply half to carry an
ERROR frame, so the refusal has to be a stop code; without a distinct code, "wrong pattern"
would be indistinguishable from "declined".

---

## 8. Golden test vectors

Implementations MUST encode exactly these bytes for these inputs, and MUST decode these
bytes back to these field sets. Byte-exact conformance in both directions is a requirement,
not a convenience: these vectors define the encoding rules of §5 operationally.

```text
DATA   {endpoint:"/t"}                       (initiating half)
       57 01 05  A1 00 62 2F 74

DATA   {}                                    (reply half)
       57 01 01  A0

HELLO  {versions:[0], max_header_bytes:16384, max_transfers:1024, caps:[], req_caps:[]}
       57 00 10  A5 00 81 00 01 19 40 00 02 19 04 00 03 80 04 80

ERROR  {code:5}                              (NO_REPLY, on a reply half)
       57 02 03  A1 00 05

DATA   {endpoint:"/md", topic:"px.eur"}      (publisher fan-out copy)
       57 01 0E  A2 00 63 2F 6D 64 05 66 70 78 2E 65 75 72

SUB    {endpoint:"/md", filter:"px."}
       57 03 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

UNSUB  {endpoint:"/md", filter:"px."}
       57 04 0B  A2 00 63 2F 6D 64 01 63 70 78 2E
```

Decoded field lists:

**Initiating DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x05` (5 bytes),
CBOR map of 1 entry: key `0` `endpoint = "/t"`. Every other DATA key is absent and therefore
omitted. This is the smallest legal request or push header.

**Reply DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x01` (1 byte), the
empty CBOR map. The bidi stream is the correlation, so a reply that carries no metadata
carries no header fields either.

**Fan-out DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x0E` (14 bytes),
CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `5` `topic = "px.eur"`. This is the
shape of a copy a publisher writes to one subscriber. A real fan-out copy additionally
carries `content_len` and `traceparent`; they are omitted here to keep the vector minimal.

**SUBSCRIBE and UNSUBSCRIBE vectors** — magic `0x57`, kind `0x03` / `0x04`,
`header_len = 0x0B` (11 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `1`
`filter = "px."`. The two frames differ in exactly one byte, the kind.

**HELLO vector** — magic `0x57`, kind `0x00` (HELLO), `header_len = 0x10` (16 bytes),
CBOR map of 5 entries: key `0` `versions = [0]`, key `1` `max_header_bytes = 16384`,
key `2` `max_transfers = 1024`, key `3` `capabilities = []`, key `4`
`required_capabilities = []`.

**ERROR vector** — magic `0x57`, kind `0x02` (ERROR), `header_len = 0x03` (3 bytes), CBOR
map of 1 entry: key `0` `code = 5` (`NO_REPLY`).

---

## 9. Operational semantics

### 9.1 Request/reply is one bidirectional stream

A requester opens a bidi stream and writes, on the initiating half: a DATA header carrying
`endpoint`, then the request payload, then FIN.

The responder answers on the reply half with exactly one of:

- a DATA header (no `endpoint`), the reply payload, and FIN; or
- an ERROR header and FIN.

**Correlation is the stream.** There is no correlation id, no per-connection pending table
and no ordering requirement between exchanges. Two consequences follow directly, and both
are intended:

- Unlimited concurrent exchanges on one connection, bounded only by
  `max_concurrent_bidi_streams`. This is what DEALER/ROUTER exists for in socket-oriented
  systems; here it is emergent rather than a separate pattern.
- Exactly one reply per exchange, structurally. A second reply is not representable, so the
  "reset a duplicate correlation" rule of earlier drafts has nothing left to guard.

The responder MAY write the reply header at any time, including **before** the request half
has reached FIN. The two halves of a QUIC bidi stream are independent, so simultaneous
streaming in both directions is an intended and required capability (master doc §10, §12).

A peer learns of a bidi stream only when its first bytes arrive. Because the DATA header is
always written first, a receiver never sees a bidi stream it cannot classify.

### 9.2 Delivery is QUIC's transport receipt

There is no application-level acknowledgement in v0. The only delivery signal is the one
QUIC already provides: the peer's acknowledgement of every stream byte and the FIN.

In the reference implementation `OutgoingTransfer::finish()` marks the FIN and returns a
`Delivery`; awaiting `Delivery::delivered()` resolves when the peer's transport has
acknowledged the whole payload. quinn's own wording for that condition is that the peer
"acknowledges receipt of all stream data (although not necessarily the processing of it)".

That parenthesis is the entire semantic content:

> The receipt means the peer's **transport** holds every byte. It says nothing about the
> peer's application having read, stored or processed them.

Dropping the receipt is legal and free; it is the fire-and-forget path.

The acknowledgement vocabulary of Accepted / Stored / Replicated / Processed is reserved for
a broker layer (§11) and has no v0 wire representation. See
[GUARANTEES.md](GUARANTEES.md) for why a brokerless application ACK would restate what QUIC
already guarantees.

### 9.3 Cancellation

Cancellation uses QUIC's own stream teardown throughout. No frame carries it.

| Situation | Mechanism |
| --- | --- |
| Sender abandons its own outgoing payload | `RESET_STREAM(CANCELED)` on the sending half |
| Receiver refuses inbound payload | `STOP_SENDING(REJECTED)` on the receiving half |
| Receiver refuses because the path is unknown | `STOP_SENDING(UNKNOWN_ENDPOINT)` |
| Receiver refuses because the path serves another stream kind | `STOP_SENDING(UNSUPPORTED)` |
| Requester abandons the reply | `STOP_SENDING(CANCELED)` on the reply half |
| Replier will not answer | ERROR `{NO_REPLY}` + FIN on the reply half |

A receiver observing a reset before FIN MUST discard all partial state for that transfer.

The requester's stop on the reply half is the direct replacement for the CANCEL frame of
earlier drafts. In the reference implementation it is what dropping a `ReplyStream` before
`recv()` does; the responder observes it as its reply half's `stopped()` future resolving,
and as a failure on its next write.

### 9.4 Endpoint dispatch and refusal

Dispatch is a function of the stream kind and the addressed path. A refusal is never a
connection error: the connection survives all of it.

**Uni stream carrying DATA:**

| Registered at the path | Action |
| --- | --- |
| a puller or subscriber | accept and queue the transfer |
| a raw acceptor | accept and queue the transfer |
| a replier | `STOP_SENDING(UNSUPPORTED)` |
| a publisher | `STOP_SENDING(UNSUPPORTED)` |
| nothing | `STOP_SENDING(UNKNOWN_ENDPOINT)` |

**Bidi stream (an exchange):**

| Registered at the path | Action |
| --- | --- |
| a replier | accept and queue the exchange |
| a raw acceptor | accept and queue the exchange |
| a puller or subscriber | ERROR `{UNSUPPORTED}` + FIN on the reply half, `STOP_SENDING(UNSUPPORTED)` on the request half |
| a publisher | same as above |
| nothing | ERROR `{UNKNOWN_ENDPOINT}` + FIN on the reply half, `STOP_SENDING(UNKNOWN_ENDPOINT)` on the request half |

A receiver MUST NOT reinterpret a misrouted stream as something the path does serve.

The `NO_REPLY` rule is mandatory: an application that takes an exchange and drops it without
answering MUST cause ERROR `{NO_REPLY}` on the reply half. Without it a requester would hang
until the idle timeout.

### 9.5 Push/Pull and Pub/Sub

Both are one-way transfers: a DATA frame on a fresh uni stream, addressed to a path, with no
reply half and nothing to correlate. They differ only in who selects the recipients.

**Push/Pull.** One DATA frame per message, addressed to the puller's path. A sender with
several peers selects one per transfer; the reference implementation uses round-robin over
live connections, but the selection policy is local and not part of the wire contract. The
sender MAY await the transport receipt (§9.2) or discard it.

**Pub/Sub.** A subscriber sends SUBSCRIBE frames (§6.4) naming the publisher's path and a
topic prefix. Publishing a message means writing one DATA frame per matching subscriber,
each on its own uni stream, each carrying `topic` (key `5`). Every copy is independent.

Delivery to a subscriber is best effort with **explicit drops**. A publisher bounds the
payload bytes it will hold queued for one subscriber (`subscriber_buffer_bytes`); a message
that does not fit is dropped for that subscriber alone, and the publisher continues. A slow
consumer therefore cannot stall a publisher or its other subscribers. Publishing a payload
larger than that bound fails locally rather than being dropped for everyone.

**Ordering is `None` for both patterns in v0.** Every message is its own unidirectional
stream, and QUIC does not order streams relative to each other. The per-pipe ordering of
socket-oriented messaging systems does not carry over. A publisher's per-subscriber writer
is serialized, so copies are *enqueued* in publication order, but the receiving application
MUST NOT rely on observing them in that order. Per-producer ordering needs an explicit
sequence field and is deliberately absent from v0.

Ordering between exchanges is likewise `None`, for the same reason.

---

## 10. Resource limits

All limits live in `weida-core::Limits`. Every limit exists to bound memory or state that a
remote peer can cause to be allocated (master doc §50, §81 rule 17).

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_header_bytes` | `16384` | largest header a peer may make this side buffer; checked before allocation (§3.1) |
| `max_concurrent_uni_streams` | `2048` | QUIC `TransportConfig::max_concurrent_uni_streams`; bounds parked/in-flight one-way stream tasks per connection |
| `max_concurrent_bidi_streams` | `1024` | QUIC `TransportConfig::max_concurrent_bidi_streams`; bounds live exchanges per connection |
| `stream_receive_window` | 1 MiB | per-stream QUIC flow-control window; bounds unread payload buffered per stream |
| `connection_receive_window` | 16 MiB | per-connection QUIC flow-control window; bounds unread payload buffered per connection |
| `max_connections` | `1024` | connections accepted per server binding; excess connections are closed immediately with `LIMIT_EXCEEDED` |
| `endpoint_queue` | `256` | depth of the accept channel per registered endpoint |
| `hello_timeout` | 10 s | time a connection may exist without a processed peer HELLO |
| `max_subscriptions` | `256` | subscription filters one peer connection may hold, summed over paths; exceeding it closes the connection with `LIMIT_EXCEEDED` (§6.4) |
| `subscriber_buffer_bytes` | 8 MiB | payload bytes a publisher will hold queued for one subscriber; a message that does not fit is dropped for that subscriber (§9.5) |

Worst-case hostile per-connection header memory is bounded by

```text
max_header_bytes * (max_concurrent_uni_streams + max_concurrent_bidi_streams)
        = 16 KiB * (2048 + 1024) = 48 MiB
```

Both stream budgets count: a peer may open its full unidirectional *and* bidirectional
allowance, and every accepted stream starts with one header. This is the governing number
for hostile-peer memory sizing in v0.

`max_concurrent_bidi_streams` replaces the previous hardcoded `0`, which refused bidi
streams outright. It is also the concurrency ceiling for inbound requests: a peer cannot
hold more live exchanges than this, which is what the deleted `max_pending` used to bound
locally — except that it is now enforced by the transport, on the side that pays for it.

`endpoint_queue` produces natural backpressure: when the queue is full, the inbound stream
task awaits queue capacity, which stops reading the payload, which closes the QUIC
flow-control window back to the sender.

`max_subscriptions` is bounded per connection rather than per path because a subscriber
chooses both: without the sum, one connection could hold `max_subscriptions` filters on
each of unboundedly many paths.

`subscriber_buffer_bytes` is the one limit that answers overload by **discarding** rather
than by backpressure. That is deliberate and confined to fan-out: a publisher that blocked
on its slowest subscriber would let one consumer degrade every other (master doc §17).

---

## 11. Not specified in v0

The following are deliberately absent from wire protocol version 0. Implementations MUST
NOT invent wire representations for them; they will be specified in later protocol
versions.

- **Application-level acknowledgements.** Accepted, Stored, Replicated and Processed are
  broker-layer semantics — a transfer of responsibility to a broker hop — and are scheduled
  for Phase 6. The v0 core deliberately carries none: without a broker to take
  responsibility, such an acknowledgement would mean "arrived in RAM", which QUIC's own
  transport receipt (§9.2) already states more honestly.
- **Router/Dealer equivalents.** Not needed as wire constructs: an exchange is a stream, so
  unlimited concurrent unsynchronized requests and correctly matched replies both fall out
  of §9.1. What Router adds beyond that — forwarding to third parties, identity envelopes —
  is broker work and has no v0 representation. Pair patterns are likewise unspecified.
- **Connecting publishers and binding pushers.** In v0 Rep, Pull and Pub bind while Req,
  Push and Sub connect. The reverse directions have no v0 representation.
- **Streaming fan-out.** A publisher sends whole messages (§9.5). Tee-ing one long stream
  to many subscribers needs its own drop and ordering design and is not specified.
- **Per-producer ordering.** Ordering is `None` across streams (§9.5). A sequence field
  would be required and is deliberately absent.
- **Multiple replies per exchange.** Exactly one reply or one ERROR per reply half; a second
  is not representable.
- **Persistence.** No wire concept of durability, storage acknowledgement or recovery.
- **Deduplication.** No idempotency ids, no inbox/outbox, no dedup window. With transfer ids
  gone there is not even an identifier to deduplicate on.
- **Capability codes.** The capability negotiation mechanism exists (HELLO keys `3` and
  `4`), but no capability code is assigned and the supported set is empty.
- **QUIC datagrams.** Only streams are used.
- **Resumable or checkpointed streams.** No offsets, content addressing or resume semantics
  (master doc §82 leaves this for after the basic protocol is proven).
- **Retries.** The protocol carries no retry or attempt metadata; retry is entirely an
  application concern in v0.
- **Authentication beyond TLS.** No application-level authentication fields in HELLO.
