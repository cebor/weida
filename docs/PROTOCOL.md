# weida wire protocol, version 0

## Status and scope

Wire protocol version: `0`.
Status: **experimental**. Per master doc §15, `0.x` protocol versions are explicitly
experimental and breaking changes are permitted. Implementations MUST NOT assume any
compatibility guarantee across `0.x` releases. The library version (`0.1.0`) and the wire
protocol version (`0`) are independent.

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
- "uni stream" means a QUIC unidirectional stream.
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

A side MUST NOT process DATA frames before it has processed the peer's HELLO frame.

QUIC uni-streams are unordered relative to one another: a DATA stream MAY be accepted
before the peer's HELLO stream. Receiving DATA before HELLO is **not** a protocol
violation. The receiver MUST park such a stream (retain it, unread beyond its preamble and
header, without emitting any error) until the peer HELLO has been processed and negotiation
has succeeded, and MUST then process the parked stream normally. In the reference
implementation the parked task awaits a `tokio::sync::watch<Option<Agreed>>`.

If the peer HELLO has not arrived within `hello_timeout` (default 10 s, see §9), the local
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

Every uni stream, in both directions, MUST begin with the following preamble:

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
- Unknown `kind` value.
- `header_len` greater than the local `limits.max_header_bytes` (§3.1).
- CBOR parse failure of the header.
- A required key missing from the header.
- A duplicate key in the header map.
- A map key that is not a CBOR unsigned integer.
- A value whose CBOR type does not match the type required for its key.
- A text string longer than the cap defined for its key.

These rules are symmetric: they apply identically to streams received by the QUIC client
and by the QUIC server. All network input is hostile (master doc §81 rule 18); neither role
is trusted.

Where a condition below is specified to produce an ERROR frame or a STOP_SENDING rather
than a connection close, that more specific rule governs. Everything in this section is a
connection-fatal framing violation.

---

## 4. Frame kinds

| Kind | Name | Stream shape |
| --- | --- | --- |
| `0` | HELLO | header only, FIN directly after the header |
| `1` | DATA | header followed by opaque payload bytes until FIN |
| `2` | ACK | header only, FIN directly after the header |
| `3` | ERROR | header only, FIN directly after the header |
| `4` | CANCEL | header only, FIN directly after the header |
| `5` | SUBSCRIBE | header only, FIN directly after the header |
| `6` | UNSUBSCRIBE | header only, FIN directly after the header |

HELLO, ACK, ERROR, CANCEL, SUBSCRIBE and UNSUBSCRIBE are header-only frames: the sender
MUST FIN the stream immediately after the header. Receiver handling of bytes appearing
after the header on a
header-only stream is unspecified in v0; a receiver MAY ignore them and MAY stop reading the
stream after the header.

For DATA, everything after the header up to FIN is opaque user payload. The protocol
imposes no internal framing on the payload: there is no `[length][payload]` chunking inside
a DATA stream.

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
- `ack_mode` (DATA key `4`) is the one keyed default in v0: when absent it means `0 = none`,
  and a sender SHOULD omit it rather than encode the zero. A decoder MUST nevertheless
  accept an explicitly encoded `0`.

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

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | required iff `role ∈ {oneshot, request}` | 512 B | endpoint path being addressed |
| `1` | `uint` | `transfer_id` | yes | — | sender's transfer id on this connection (see below) |
| `2` | `uint` | `role` | yes | — | `0 = oneshot`, `1 = request`, `2 = reply`; `3..` reserved |
| `3` | `uint` | `correlation_id` | required iff `role = reply` | — | the peer's request `transfer_id` this reply answers |
| `4` | `uint` | `ack_mode` | no, default `0` | — | `0 = none`, `1 = accepted`; `2 = stored`, `3 = replicated`, `4 = processed` reserved |
| `5` | `uint` | `content_len` | no | — | payload length in bytes; **advisory**, not enforced |
| `6` | `tstr` | `content_type` | no | 256 B | opaque media type label |
| `7` | `tstr` | `traceparent` | no | 128 B | W3C Trace Context `traceparent` |
| `8` | `tstr` | `tracestate` | no | 512 B | W3C Trace Context `tracestate`, opaque passthrough |
| `9` | `tstr` | `topic` | no | 256 B | Pub/Sub topic; opaque bytes, matched by byte prefix (§9.6) |

`transfer_id` rules:

- It is a per-connection, per-sender counter starting at `1`.
- A `transfer_id` of `0` MUST close the connection with `PROTOCOL_VIOLATION`.
- Uniqueness is **NOT** enforced by the receiver in v0. Transfer ids only correlate control
  frames with transfers; receiver state keyed by `transfer_id` is bounded independently of
  id reuse, so a duplicated id cannot exhaust receiver memory. Implementations MUST NOT
  rely on receiver-side uniqueness checking in v0.

`role` rules:

- `0 = oneshot`, `1 = request` and `2 = reply` are the values a v0 sender may emit.
- A `oneshot` transfer addresses an endpoint path and expects no reply: `endpoint` is
  required, `correlation_id` MUST NOT be set (a receiver tolerates and ignores it, per the
  general leniency of §5), and `ack_mode` MAY be `0` or `1`. It is the wire form of both
  Push/Pull and the copies a publisher fans out.
- Values `3..` are reserved. A v0 receiver receiving a reserved role MUST answer with an
  ERROR frame carrying code `UNSUPPORTED` and MUST refuse the payload with
  `STOP_SENDING(REJECTED)`. It MUST NOT close the connection.
- A role the addressed endpoint does not serve — a `request` aimed at a path bound by a
  puller, a `oneshot` aimed at a path bound by a replier, or either aimed at a path bound
  by a publisher — is refused the same way: ERROR `UNSUPPORTED` plus
  `STOP_SENDING(REJECTED)`, connection intact. The receiver MUST NOT reinterpret the
  sender's stated role.

`topic` is meaningful only for the fan-out copies a publisher emits (§9.6). It is opaque
bytes: weida never parses it, and no character in it is special.

`ack_mode` rules:

- `0 = none` is the default and means no acknowledgement is requested.
- `1 = accepted` requests an ACK frame with `state = 1` (see §6.3).
- `2 = stored`, `3 = replicated`, `4 = processed` are reserved. A v0 receiver receiving a
  reserved value MUST answer with an ERROR frame carrying code `UNSUPPORTED` and MUST
  refuse the payload with `STOP_SENDING(REJECTED)`. It MUST NOT close the connection, and
  it MUST NOT silently downgrade the request to a weaker ack mode
  (see [GUARANTEES.md](GUARANTEES.md)).

`content_len` is advisory: the receiver MUST NOT reject a payload for disagreeing with it,
and MUST NOT size an allocation from it.

`traceparent` and `tracestate` carry W3C Trace Context. `tracestate` is opaque to weida and
MUST be forwarded unmodified where trace context is propagated.

### 6.3 ACK (kind 2)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `re` | yes | — | the **recipient's** outgoing `transfer_id` being acknowledged |
| `1` | `uint` | `state` | yes | — | `1 = accepted` |

Emission is direction-agnostic. Whichever side fully reads a DATA stream whose header
carried `ack_mode = 1` — meaning the application consumed the payload to FIN — MUST send an
ACK frame on its own fresh uni stream. The ACK is not tied to the client or server role.

`re` is interpreted in the **recipient's** id space: it names a transfer that the receiver
of the ACK sent.

An ACK naming an id for which the local side has no waiting transfer MUST be ignored. This
races legitimately with cancellation and with connection teardown; implementations SHOULD
log it at debug level and MUST NOT treat it as an error.

Values of `state` other than `1` are unspecified in v0.

### 6.4 ERROR (kind 3)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `re` | yes | — | the **recipient's** outgoing `transfer_id` this error refers to |
| `1` | `uint` | `code` | yes | — | error code, see below |
| `2` | `tstr` | `message` | no | 1024 B | human-readable detail, not machine-interpreted |

Error codes:

| Code | Name | Meaning |
| --- | --- | --- |
| `1` | `UNKNOWN_ENDPOINT` | no endpoint is registered for the requested path |
| `2` | `REJECTED` | the receiving side declined to accept the transfer |
| `3` | `UNSUPPORTED` | a reserved or unsupported header value was requested |
| `4` | `INTERNAL` | the receiving side failed internally |
| `5` | `NO_REPLY` | the request was accepted but no reply will be produced |

An ERROR frame resolves the referenced outgoing transfer, and any pending reply correlated
to it, as failed.

An ERROR naming an unknown `re` MUST be ignored.

Codes other than `1..=5` are unspecified in v0. Because an ERROR frame resolves the
referenced transfer as failed regardless of code, a receiver SHOULD treat an unrecognised
code as a generic failure of that transfer rather than as a framing violation.

### 6.5 CANCEL (kind 4)

| Key | CBOR type | Name | Required | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `uint` | `id` | yes | — | the **sender's own** request `transfer_id` whose replies are no longer wanted |

Note the asymmetry with ACK and ERROR: `re` in ACK/ERROR is in the recipient's id space,
whereas `id` in CANCEL is in the sender's own id space.

Receiver behaviour on CANCEL:

- Flip the cancel signal of the active inbound request with that `transfer_id`, so the
  application handling it observes cancellation.
- RESET any reply streams already open for that request with application error code
  `CANCELED`.
- An unknown `id` MUST be ignored.

### 6.6 SUBSCRIBE (kind 5) and UNSUBSCRIBE (kind 6)

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
  summed over all paths, the receiver MUST close the connection with `LIMIT_EXCEEDED`.
  SUBSCRIBE carries no `transfer_id`, so there is nothing for an ERROR frame to name.
- All subscriptions held by a connection are dropped when that connection closes.
- Like every non-HELLO frame, a SUBSCRIBE that arrives before the peer's HELLO is parked
  until negotiation completes; it is not a violation (§2.2).
- A peer that registers no publishers MAY ignore these frames. Subscribing to a side that
  publishes nothing is useless, not hostile.

---

## 7. QUIC application error codes

These codes are used for `CONNECTION_CLOSE`, `RESET_STREAM` and `STOP_SENDING`. They are
distinct from the ERROR frame codes of §6.4.

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

---

## 8. Golden test vectors

Implementations MUST encode exactly these bytes for these inputs, and MUST decode these
bytes back to these field sets. Byte-exact conformance in both directions is a requirement,
not a convenience: these vectors define the encoding rules of §5 operationally.

```text
DATA   {endpoint:"/t", transfer_id:1, role:request, ack_mode:accepted}
       57 01 0B  A4 00 62 2F 74 01 01 02 01 04 01

HELLO  {versions:[0], max_header_bytes:16384, max_transfers:1024, caps:[], req_caps:[]}
       57 00 10  A5 00 81 00 01 19 40 00 02 19 04 00 03 80 04 80

ACK    {re:1, state:accepted}
       57 02 05  A2 00 01 01 01

CANCEL {id:1}
       57 04 03  A1 00 01

DATA   {endpoint:"/t", transfer_id:1, role:oneshot}
       57 01 09  A3 00 62 2F 74 01 01 02 00

DATA   {endpoint:"/md", transfer_id:1, role:oneshot, topic:"px.eur"}
       57 01 12  A4 00 63 2F 6D 64 01 01 02 00 09 66 70 78 2E 65 75 72

SUB    {endpoint:"/md", filter:"px."}
       57 05 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

UNSUB  {endpoint:"/md", filter:"px."}
       57 06 0B  A2 00 63 2F 6D 64 01 63 70 78 2E
```

Decoded field lists:

**DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x0B` (11 bytes),
CBOR map of 4 entries: key `0` `endpoint = "/t"`, key `1` `transfer_id = 1`, key `2`
`role = 1` (request), key `4` `ack_mode = 1` (accepted). Keys `3`, `5`, `6`, `7`, `8` are
absent and therefore omitted.

**Oneshot DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x09` (9 bytes),
CBOR map of 3 entries: key `0` `endpoint = "/t"`, key `1` `transfer_id = 1`, key `2`
`role = 0` (oneshot). `ack_mode` is `0`, its default, and is therefore omitted — the role
is not, because it selects behaviour rather than restating a default.

**Fan-out DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x12` (18 bytes),
CBOR map of 4 entries: key `0` `endpoint = "/md"`, key `1` `transfer_id = 1`, key `2`
`role = 0` (oneshot), key `9` `topic = "px.eur"`. This is the shape of a copy a publisher
writes to one subscriber.

**SUBSCRIBE and UNSUBSCRIBE vectors** — magic `0x57`, kind `0x05` / `0x06`,
`header_len = 0x0B` (11 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `1`
`filter = "px."`. The two frames differ in exactly one byte, the kind.

**HELLO vector** — magic `0x57`, kind `0x00` (HELLO), `header_len = 0x10` (16 bytes),
CBOR map of 5 entries: key `0` `versions = [0]`, key `1` `max_header_bytes = 16384`,
key `2` `max_transfers = 1024`, key `3` `capabilities = []`, key `4`
`required_capabilities = []`.

**ACK vector** — magic `0x57`, kind `0x02` (ACK), `header_len = 0x05` (5 bytes), CBOR map
of 2 entries: key `0` `re = 1`, key `1` `state = 1` (accepted).

**CANCEL vector** — magic `0x57`, kind `0x04` (CANCEL), `header_len = 0x03` (3 bytes),
CBOR map of 1 entry: key `0` `id = 1`.

---

## 9. Operational semantics

### 9.1 Request/reply

A request is a DATA frame with `role = request` on a fresh uni stream.

Each reply is a DATA frame with `role = reply` and `correlation_id` set to the request's
`transfer_id`, carried on its own fresh uni stream opened by the responder.

The responder MAY open the reply stream at any time, including **before** the request
stream has reached FIN. Simultaneous streaming of request and reply in both directions is
an intended and required capability (master doc §10, §12).

In v0 there is exactly one reply per request. The requester MUST resolve its pending
correlation on the **first** reply header it receives for a given `correlation_id`, and
MUST RESET, with application error code `CANCELED`, any later stream carrying the same
`correlation_id`. This MUST NOT be treated as a connection error: it races legitimately
with cancellation, and such races are legal.

Multiple replies per request are unspecified in v0.

### 9.2 ACK and reply are distinct

An ACK is protocol state. A reply is application data. They MUST NOT be conflated
(master doc §12, §81 rule 8).

- A reply never substitutes for an ACK.
- An ACK never substitutes for a reply.
- An ACK is always carried on its own ACK frame stream, never inside a DATA stream.

The exact v0 meaning of `ACK(accepted)` is:

> the receiving side read the complete payload to FIN and handed it to the application.

It asserts nothing about persistence, replication or processing, and nothing about any hop
beyond the immediate peer (see [GUARANTEES.md](GUARANTEES.md)).

### 9.3 Cancellation

Cancellation is available to both directions of every transfer:

- **Sender abandons its own outgoing DATA mid-stream**: it MUST
  `RESET_STREAM(CANCELED)`. A receiver observing a reset before FIN MUST discard all
  partial state for that transfer, and MUST NOT send an ACK or an ERROR frame for it.
- **Receiver refuses inbound payload**: it MUST `STOP_SENDING` with the appropriate
  application error code — `REJECTED`, `CANCELED` or `UNKNOWN_ENDPOINT`.
- **Requester abandons an expected reply**: it MUST send a CANCEL frame naming its own
  request `transfer_id`. In the reference implementation this happens when `PendingReply`
  is dropped before `recv()`.

### 9.4 Responder-side failure handling

| Situation | Responder action |
| --- | --- |
| Requested endpoint path is not registered | `STOP_SENDING(UNKNOWN_ENDPOINT)` **and** ERROR `{re, UNKNOWN_ENDPOINT}` |
| Application drops the request body before FIN | `STOP_SENDING(REJECTED)` |
| Application drops the whole request without ever opening a reply | ERROR `{re, NO_REPLY}` |
| Reserved `role` or `ack_mode` value received | ERROR `{re, UNSUPPORTED}` **and** `STOP_SENDING(REJECTED)` |
| `role` the addressed endpoint does not serve | ERROR `{re, UNSUPPORTED}` **and** `STOP_SENDING(REJECTED)` |

The `NO_REPLY` rule is mandatory: without it a requester awaiting a reply would hang until
the idle timeout.

### 9.5 Sender outcome precedence

An ERROR frame observed before an ACK has been delivered to the application wins: the
transfer resolves as failed with the ERROR code, not as acknowledged. The full outcome
rules are normative in [FAILURE_MODEL.md](FAILURE_MODEL.md).

### 9.6 Push/Pull and Pub/Sub

Both patterns are `role = oneshot` DATA frames; they differ only in who selects the
recipients.

**Push/Pull.** A push is one `oneshot` DATA frame on a fresh uni stream, addressed to the
puller's path. There is no reply and no correlation. `ack_mode` applies unchanged:
`0` settles at FIN as best effort, `1` settles when the receiving application has read the
payload to FIN. A sender with several peers selects one per transfer; the reference
implementation uses round-robin over live connections, but the selection policy is local
and not part of the wire contract.

**Pub/Sub.** A subscriber sends SUBSCRIBE frames (§6.6) naming the publisher's path and a
topic prefix. Publishing a message means writing one `oneshot` DATA frame per matching
subscriber, each on its own uni stream, each carrying the `topic` (key `9`) and each with
`ack_mode = 0`: the publisher waits on nobody, so there is nothing for an acknowledgement
to resolve. Every copy is independent — a `transfer_id` is per connection, so headers
cannot be shared between subscribers.

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

---

## 10. Resource limits

All limits live in `weida-core::Limits`. Every limit exists to bound memory or state that a
remote peer can cause to be allocated (master doc §50, §81 rule 17).

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_header_bytes` | `16384` | largest header a peer may make this side buffer; checked before allocation (§3.1) |
| `max_concurrent_uni_streams` | `2048` | QUIC `TransportConfig::max_concurrent_uni_streams`; bounds the number of parked/in-flight stream tasks per connection |
| `stream_receive_window` | 1 MiB | per-stream QUIC flow-control window; bounds unread payload buffered per stream |
| `connection_receive_window` | 16 MiB | per-connection QUIC flow-control window; bounds unread payload buffered per connection |
| `max_connections` | `1024` | connections accepted per server binding; excess connections are closed immediately with `LIMIT_EXCEEDED` |
| `max_pending` | `4096` | locally registered awaiting-ACK plus awaiting-reply entries; exceeding it fails `open()` locally with `LimitExceeded` |
| `endpoint_queue` | `256` | depth of the accept channel per registered endpoint |
| `hello_timeout` | 10 s | time a connection may exist without a processed peer HELLO |
| `max_subscriptions` | `256` | subscription filters one peer connection may hold, summed over paths; exceeding it closes the connection with `LIMIT_EXCEEDED` (§6.6) |
| `subscriber_buffer_bytes` | 8 MiB | payload bytes a publisher will hold queued for one subscriber; a message that does not fit is dropped for that subscriber (§9.6) |

Worst-case hostile per-connection header memory is bounded by
`max_concurrent_uni_streams * max_header_bytes` = 2048 x 16 KiB = **32 MiB**. This is the
governing number for hostile-peer memory sizing in v0.

`max_pending` is **local backpressure, not a protocol violation**: exceeding it MUST fail
the local `open()` call with `LimitExceeded` and MUST NOT close or reset the connection. A
local application opening too many concurrent transfers is not peer misbehaviour.

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

- **Router/Dealer equivalents.** Not specified, and in v0 not needed as separate wire
  constructs: every request is its own stream carrying an explicit `correlation_id`, so
  concurrent unsynchronized requests and identity-addressed replies both fall out of
  Req/Rep. What Router adds beyond that — forwarding to third parties, identity envelopes —
  is broker work and has no v0 representation. Pair patterns are likewise unspecified.
- **Connecting publishers and binding pushers.** In v0 Pub and Pull bind while Sub and Push
  connect. The reverse directions have no v0 representation.
- **Streaming fan-out.** A publisher sends whole messages (§9.6). Tee-ing one long stream
  to many subscribers needs its own drop and ordering design and is not specified.
- **Per-producer ordering.** Ordering is `None` for Push/Pull and Pub/Sub (§9.6). A
  sequence field would be required and is deliberately absent.
- **Acknowledgement modes beyond `accepted`.** `stored`, `replicated` and `processed` are
  reserved code points only; receiving them yields `UNSUPPORTED`.
- **Persistence.** No wire concept of durability, storage acknowledgement or recovery.
- **Deduplication.** No idempotency ids, no inbox/outbox, no dedup window. Receiver-side
  `transfer_id` uniqueness is explicitly not enforced.
- **Capability codes.** The capability negotiation mechanism exists (HELLO keys `3` and
  `4`), but no capability code is assigned and the supported set is empty.
- **Multiple replies per request.** Exactly one reply is defined; later same-correlation
  streams are reset.
- **QUIC datagrams.** Only streams are used.
- **Resumable or checkpointed streams.** No offsets, content addressing or resume semantics
  (master doc §82 leaves this for after the basic protocol is proven).
- **Retries.** The protocol carries no retry or attempt metadata; retry is entirely an
  application concern in v0.
- **Authentication beyond TLS.** No application-level authentication fields in HELLO.
- **Bidirectional data streams.** All v0 transfers use uni streams.
