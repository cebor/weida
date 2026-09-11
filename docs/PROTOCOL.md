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

**Wire version 0 is unchanged by the decided-but-unbuilt parts of this document.** Sections
marked *spec ahead of code* — §2.5, HELLO keys `5` and `6` (§6.1), the guarantee set of §6.5,
the profiles of §10.1 — specify behaviour that the accepted decision notes in
[decisions/](decisions/) have settled and that no v0 implementation yet produces. They are
written here rather than left to the implementation for two reasons: a key number and an
encoding must be fixed once, by the specification, before two implementations can disagree
about them; and every one of them is already load-bearing for an adapter mapping document.
They change nothing for a v0 peer, because every one of them is either an optional key that an
encoder omits and a decoder skips (§5), or a statement about how many connections a pair
holds. The version stays `0`.

DATA keys `6` and `7` (§6.2) are a narrower case and no longer only on paper: the codec
encodes and decodes both and §8 pins their vectors, while no v0 *sender* sets either, so they
are implemented and unused rather than unimplemented.

One change was not merely additive: the topic filter grammar of §6.4 replaced the byte-prefix
match of earlier drafts, so the same SUBSCRIBE bytes can select a different set of topics than
they used to ([decisions/0007](decisions/0007-topic-namespace.md) §4.2). The implementation
follows it as of B-020 — the matcher is a segment walk and an invalid filter is rejected at
the codec boundary — so this is history rather than a pending defect. `0.x` carries no
compatibility promise, which is what made fixing the grammar before the adapters exist
cheaper than fixing it after.

Related documents: [ARCHITECTURE.md](ARCHITECTURE.md),
[GUARANTEES.md](GUARANTEES.md), [FAILURE_MODEL.md](FAILURE_MODEL.md),
[INVARIANTS.md](INVARIANTS.md), [IMPLEMENTATION.md](IMPLEMENTATION.md), and the accepted
decision notes in [decisions/](decisions/README.md), which are normative for the sections
marked *spec ahead of code*.

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
- "peer" means the other side of a connection, regardless of which side opened it. On the
  network transport that is a QUIC connection; on a local transport it is the socket, pipe or
  channel of §2.1.

---

## 2. Connection establishment

### 2.1 Transport

**The network transport is QUIC.** The reference implementation uses `quinn`.

The TLS ALPN token MUST be exactly `weida/0`. A peer MUST offer this token and MUST NOT
accept a connection that negotiated any other token. An ALPN mismatch MUST fail the TLS
handshake; it is not signalled at the weida protocol layer.

**A local transport carries the same protocol without TLS**
([decisions/0010](decisions/0010-local-transport.md)). In-process channels, `AF_UNIX`
`SOCK_STREAM` sockets and Windows named pipes in message mode carry the same frames (§4), the
same headers (§6), the same HELLO exchange (§2.2) and the same negotiation (§2.3). Three
differences, and only three:

- **There is no TLS and therefore no ALPN.** The version fence moves to where the real work
  was always done: the `versions` intersection of §2.3. A local peer MUST still send HELLO and
  MUST still fail the connection with `NEGOTIATION_FAILED` on an empty intersection.
- **The OS connection is the stream.** A local transport has no stream multiplexing, so one
  transfer is one local connection and a transfer's lifetime is that connection's
  [0010 §4.2]. Nothing in §3-§9 changes: a preamble and a header still open every stream,
  because the stream *is* the connection. There is consequently no per-connection window, so
  the shared-window coupling of §10 does not arise locally.

  One consequence is worth stating rather than deriving. A local connection is bidirectional
  by nature, so it carries no equivalent of QUIC's stream kind, and the dispatch of §9.4 —
  today a function of the stream kind *and* the addressed path — is locally a function of the
  **path alone**: the pattern registered there says whether a reply is expected. A replier path
  answers on the same connection; a puller or publisher path never writes back, and the
  initiator MUST NOT wait for a reply on it. The mismatch case is unchanged and already
  specified: a transfer addressed to a path whose pattern cannot serve it is refused with
  `UNSUPPORTED` (§9.4).
- **The peer is proved by the kernel, not by a key.** `SO_PEERCRED` on Linux, `LOCAL_PEERCRED`
  on macOS — which carries no PID — and the client's token through
  `ImpersonateNamedPipeClient` on Windows; an in-process peer has no identity at all, because
  there is nobody else to prove [0010 §4.4]. A PID is an observation and MUST NOT be
  authorized on.

A local transport is named by its own URL scheme, never by `weida://`
([ARCHITECTURE.md](ARCHITECTURE.md) §3): the transport is part of the address, and there is no
automatic fallback from one to another, because that would change who may connect and what
proves them without saying so [0010 §4.6], [0010 §4.8].

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
5. Compute the effective guarantee set, dimension by dimension, as the **weaker** of
   `ours.guarantees_offered` and `theirs.guarantees_offered`, an absent declaration meaning
   `core` (§6.1, §6.5). For a dimension whose levels are not ordered — `backpressure`, and
   the two independent axes of a durability level (§6.5) — "weaker" is not defined, so the
   two declarations MUST be equal or negotiation fails.
6. If the effective set does not reach `theirs.guarantees_required` on **every** dimension,
   negotiation fails. There is no downgrade path: a level the peer does not offer is a failed
   handshake, never a quieter success
   ([decisions/0006](decisions/0006-guarantee-sets.md) §4.4). Because a v0 peer declares
   nothing, it offers and requires `core`, and the step is a no-op between two v0 peers.
7. On success the result is

```text
Agreed {
    version: u64,                 // the effective version from step 3
    send_max_header_bytes: u64,   // = theirs.max_header_bytes
    guarantees: GuaranteeSet,     // the effective set from step 5; spec ahead of code
}
```

`send_max_header_bytes` is the **peer's** advertised `max_header_bytes`; it bounds the
headers this side may send. The local receive limit remains the local
`limits.max_header_bytes` and is not affected by the peer's advertisement.

Negotiation is still a pure function of the two HELLO frames: the effective set is computed,
not agreed in a second round trip, and both sides compute the same one from the same two
frames.

Negotiation failure MUST close the connection with `CONNECTION_CLOSE`, application error
code `NEGOTIATION_FAILED`. All in-flight local operations on that connection then resolve
per the connection-loss rules in [FAILURE_MODEL.md](FAILURE_MODEL.md).

### 2.4 Timers

| Timer | Value | Applies to |
| --- | --- | --- |
| Keep-alive | 10 s | client side only |
| Idle timeout | 30 s | both sides |
| `hello_timeout` | 10 s | both sides, until peer HELLO is processed |

### 2.5 Connections per peer

A peer pair holds more than one QUIC connection
([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2-§6.3):

- one **connection per dialled endpoint path**, carrying that path's transfers and the control
  frames that name it. **Implemented**;
- one **control** connection per peer, for peer-scoped frames.
  **Not implemented, and parked** ([decisions/0011](decisions/0011-answered-where-it-arrived.md)
  §4.3): no frame that v0 has or reserves is peer-scoped enough to need it.

The point is head-of-line coupling: a QUIC connection's receive window is shared, so one slow
reader can stall every writer on that connection ([PATTERNS.md](PATTERNS.md) §1.3). One
connection per path means two paths share no window and therefore cannot stall each other,
which is asserted end to end by `a_stalled_path_does_not_stall_another_path` in
`crates/weida/tests/streams.rs`. Each connection is an ordinary weida connection: it performs
its own HELLO (§2.2) and its own negotiation (§2.3), and nothing on the wire distinguishes one
from another.

**Traffic a side originates for a peer's registration MUST be written on the connection that
carried the registration** ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.1).
No header field selects a connection. A publisher therefore writes fan-out on the connection
the SUBSCRIBE arrived on, which is what every protocol in the catalogue does — EMQX sends a
server-initiated PUBLISH "on the stream where it received that topic's subscription", NATS
answers a `SUB` on its own connection, a RabbitMQ consumer tag is channel-scoped.

**A frame that names a path is path-scoped and rides that path's connection; a frame that names
only the peer is peer-scoped** [0011 §4.2]. SUBSCRIBE and UNSUBSCRIBE name an endpoint path
(§6.4), so they ride that path's connection and are **not** control-tier traffic: moving them
to a per-peer connection would separate a subscription from the only route to its subscriber.
The reserved credit frame of §11 is granted per subscription, so it is path-scoped for the same
reason [0011 §4.3].

What remains coupled is stated rather than hidden: since a path's connection carries both its
payload and its subscriptions, an endpoint that publishes *and* subscribes on one path can
queue its own SUBSCRIBE behind its own payload. A pure subscriber writes nothing there and a
pure publisher sends no SUBSCRIBE, so neither is affected [0011 §4.4].

**What binds a peer's connections is the proved fingerprint, and nothing else**
([decisions/0008](decisions/0008-session-identity.md) §4.2). No HELLO field names a peer's
other connections. A connection belongs to the peer that proved the same fingerprint under the
same authority and terms; a connection whose fingerprint differs is a different peer and MUST
NOT be bound to it — a dialling side MUST refuse it rather than serve a transfer on it. Two
connections that proved no fingerprint at all — an anonymous client — MUST NOT be treated as
one peer, which is why `max_connections_per_peer` (§10) counts only connections that proved an
identity and a deployment that wants that bound requires a client identity.

Nothing is retained between connections: there is no session, no subscription resumption and
no sequence resumption at wire version 0 (§11). A peer that reconnects is the same *peer* and
starts again.

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
- Unknown `kind` value, i.e. anything in `5..=255`. Kind `5` is *reserved* for the L2 credit
  frame (§11) and is unknown at wire version 0 like any other: a reservation is a promise not
  to reuse the number, not a permission to send it.
- `header_len` greater than the local `limits.max_header_bytes` (§3.1).
- CBOR parse failure of the header.
- A required key missing from the header.
- A duplicate key in the header map.
- A map key that is not a CBOR unsigned integer.
- A value whose CBOR type does not match the type required for its key.
- A text or byte string longer than the cap defined for its key.
- A frame kind used on a stream kind where §4 does not permit it.
- A DATA frame without `endpoint` on a stream that initiates a transfer (§6.2).
- A guarantee set whose value or dimension combination §6.5 forbids.

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
carries payload it would have to drain. Kind `5` additionally carries a *name* already —
the L2 credit frame of [decisions/0003](decisions/0003-credit-unit.md) §4.2, on the control
connection of §2.5 — so that nothing else claims the number; it is still unknown, and still
fatal, at wire version 0 (§11).

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
- A **nested** definite-length map is permitted only where a key's type says so — in v0 that
  is the guarantee set of §6.5 and nothing else — and every rule above applies to it
  unchanged: uint keys, strictly ascending, no duplicates, unknown keys skipped. Nesting is
  one level deep by specification; the `max_depth = 8` skip bound is what makes an
  unspecified deeper nesting harmless rather than fatal to the decoder.

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
| `5` | `map` | `guarantees_offered` | no | §6.5 | guarantee set the sender can honour; **optional, absent in v0** |
| `6` | `map` | `guarantees_required` | no | §6.5 | guarantee set the sender requires of the peer; **optional, absent in v0** |

Keys `0` to `4` are required. A missing one of them is a framing violation per §3.2.

Capability code assignment is unspecified in v0: no codes are defined and the v0 supported
set is empty.

**Keys `5` and `6` are specified ahead of code** (§11) and are the only optional HELLO keys.
They declare guarantee sets per [decisions/0006](decisions/0006-guarantee-sets.md) §4.4,
encoded as §6.5. An absent key means the default set `core` — which is exactly what every v0
peer offers and requires — so a v0 HELLO is unchanged on the wire and a v0 decoder skips both
keys by the rule of §5. `guarantees_required` MUST be a subset-or-equal of the sender's own
`guarantees_offered` on every dimension: requiring what you cannot yourself honour is a
configuration error, not a negotiation position.

### 6.2 DATA (kind 1)

| Key | CBOR type | Name | Required at the decoder | Cap | Meaning |
| --- | --- | --- | --- | --- | --- |
| `0` | `tstr` | `endpoint` | no | 512 B | endpoint path being addressed |
| `1` | `uint` | `content_len` | no | — | payload length in bytes; **advisory**, not enforced |
| `2` | `tstr` | `content_type` | no | 256 B | opaque media type label |
| `3` | `tstr` | `traceparent` | no | 128 B | W3C Trace Context `traceparent` |
| `4` | `tstr` | `tracestate` | no | 512 B | W3C Trace Context `tracestate`, opaque passthrough |
| `5` | `tstr` | `topic` | no | 256 B | Pub/Sub topic; opaque bytes, selected by the filter grammar of §6.4 |
| `6` | `uint` | `sequence` | no | — | per-producer sequence number; coded, **written by no v0 sender** |
| `7` | `bstr` | `producer` | no | exactly 32 B | producer identity, the raw digest; coded, **written by no v0 sender** |

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

**Keys `6` and `7` are coded but unused** (§11). They carry exact semantics so that nothing
else takes the numbers and so that the two capabilities they enable have one definition
rather than one per implementation. `weida-protocol` encodes and decodes both, and §8 pins
their bytes; what no v0 *sender* does is set them, because the guarantee levels that give
them meaning are not negotiated yet (§6.5). A decoder that meets one accepts it — the
specification defines it — and a decoder that meets an unknown key still skips it under §5.

`sequence` is a monotonically increasing `uint` scoped to (producer, endpoint or topic)
([decisions/0001](decisions/0001-sequence-field.md) §7.1). It is not a transfer identifier and
it does not correlate anything: an exchange is still correlated by its stream (§9.1). Its
purpose is ordering and gap detection, and a receiver that has not negotiated a `PerProducer`
level (§6.5) MUST ignore it.

`producer` names the producer when the producer is **not** the connection peer — a relay, an
L2 hop forwarding another producer's output, or a stable name supplied by an L2 subscription.
It is **absent** in the default case, because the receiver already knows the sending peer's
proved fingerprint from the handshake and a claimed name could not be trusted anyway
([decisions/0008](decisions/0008-session-identity.md) §4.4). Where present it is the raw
32-byte digest as a `bstr`; the `sha256:<64 hex>` spelling is presentation only and MUST NOT
appear on the wire. The length is exact rather than merely capped — a `bstr` of any other
length is a framing violation (§3.2), because a truncated digest names nobody — and the
absent default is also what keeps a connection that negotiated no ordering from paying for
one: the measured cost of writing the text form was +80 B and −9 % of the message rate at a
64-byte payload ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-009).

`sequence` and `producer` are independent: either may appear without the other. Ordering and
deduplication are separate guarantee dimensions and neither implies the other [0001 §7.1].

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
| `1` | `tstr` | `filter` | yes | 256 B | topic filter; the grammar below. The empty string matches every topic |

Both keys are required. `filter` is required even when empty: an absent key and an empty
string would otherwise be indistinguishable, and the empty filter is the "every topic"
subscription.

Receiver behaviour:

- The filter is a **segmented pattern**, not a byte prefix
  ([decisions/0007](decisions/0007-topic-namespace.md) §4.2). A topic and a filter are byte
  strings split on `.` (U+002E, one byte) into segments, and the filter matches a topic when
  every segment matches:
  - `*` alone in a segment matches exactly one whole segment. `*` MUST occupy a whole
    segment; a segment that merely contains it (`a*b`) is a grammar violation.
  - `#` alone in the **final** segment matches zero or more trailing segments, so `a.#`
    matches `a`, `a.b` and `a.b.c`. `#` MUST be the last segment and MUST be alone in it.
  - Every other byte is literal, compared byte for byte. There is no escape character, no
    normalization and no case folding. Empty segments are permitted and match only empty
    segments.
  - The empty filter matches every topic and is equivalent to the single-segment filter `#`.
  - A **`topic` is never a pattern**: `*` and `#` are special only inside a filter, so a
    published topic containing them is matched literally.
  Matching is a single left-to-right walk over both strings — `#` only in final position is
  what removes backtracking — and allocates nothing, so it is bounded by the 256 B filter cap.
- A filter that violates the grammar (`*` not alone in its segment, `#` not final or not
  alone) MUST close the connection with `PROTOCOL_VIOLATION`. Like an oversized filter, it is
  malformed content on a uni stream with no reply half to answer on, so the connection is the
  only granularity available; and unlike `max_subscriptions`, it is not an overload but a
  peer sending something the grammar does not permit.
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

### 6.5 Guarantee set encoding (spec ahead of code)

A **guarantee set** is the unit of configuration and of negotiation: one level per guarantee
dimension of [GUARANTEES.md](GUARANTEES.md) §3, carried as one CBOR map
([decisions/0006](decisions/0006-guarantee-sets.md) §4.1). It appears only in HELLO keys `5`
and `6` (§6.1); no DATA frame carries a guarantee level, because a set is a property of the
connection and not of a message.

| Key | CBOR type | Name | Values |
| --- | --- | --- | --- |
| `0` | `uint` | `delivery` | `0` BestEffort, `1` AtMostOnce, `2` AtLeastOnce |
| `1` | `uint` | `acknowledgement` | `0` None, `1` TransportReceipt, `2` Accepted, `3` Stored, `4` Replicated, `5` Processed |
| `2` | `uint` | `durability` | `0` Written, `1` Flushed; permitted only with `acknowledgement` `3` or `4` |
| `3` | `uint` | `replicas` | the replica count `n`, leader included; permitted only with `acknowledgement` `4`, and MUST be ≥ 2 |
| `4` | `uint` | `ordering` | `0` None, `1` PerProducer detect, `2` PerProducer reassemble, `3` PerKey, `4` Total |
| `5` | `uint` | `deduplication` | `0` None, `1` Bounded, `2` Durable |
| `6` | `uint` | `dedup_window_ms` | window length in milliseconds; REQUIRED with `deduplication` `1`, forbidden otherwise |
| `7` | `uint` | `backpressure` | `0` Block, `1` Reject, `2` Drop, `3` Spill, `4` Coalesce |
| `8` | `uint` | `producer_naming` | `0` fingerprint, `1` stable; how a sequenced transfer's producer is named ([decisions/0001](decisions/0001-sequence-field.md) §7.3, [decisions/0008](decisions/0008-session-identity.md) §4.3) |
| `9` | `uint` | `control_isolated` | `0` no, `1` yes: control traffic cannot stall behind bulk ([decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.1) |

Rules:

- **An absent key means the `core` level for that dimension**: `delivery` BestEffort,
  `acknowledgement` TransportReceipt, `ordering` None, `deduplication` None, `backpressure`
  Block, `producer_naming` fingerprint, `control_isolated` no [0006 §4.2]. An empty map is
  therefore exactly `core`, and so is an absent HELLO key. An encoder MUST NOT write a
  dimension left at its `core` level, so a `core` declaration is indistinguishable from no
  declaration on the wire.
- The map is bounded by `max_header_bytes` like every other header, and every value is a
  `uint`, so a guarantee set introduces no new allocation a peer can influence.
- A key whose value is outside the list above, or a dimension combination the table forbids
  (`durability` without `Stored`/`Replicated`, `replicas` without `Replicated`, `replicas`
  of `1`, a missing `dedup_window_ms` under `Bounded`) is a framing violation (§3.2). The
  levels reserved for the L2 broker — `acknowledgement` `2` to `5` — are legal to *declare*
  and impossible to honour in v0, so a peer that requires one gets a failed negotiation
  (§2.3), never a quieter success.
- The state set is **not a ladder**. Persistence level and replica count are independent axes,
  so `Stored(Flushed)` and `Replicated(3, flushed: false)` are incomparable and comparison is
  per axis ([decisions/0004](decisions/0004-durability-levels.md) §4.4). `backpressure` and
  `producer_naming` are not ordered at all: their values are behaviours and names, not
  strengths, so two peers either state the same one or fail to negotiate. Every other
  dimension *is* a ladder in the order its values are listed above, `control_isolated`
  included — isolation is strictly stronger than none, so the intersection of §2.3 is the
  logical AND.
- When the intersection weakens `acknowledgement` below `Stored`, the `durability` and
  `replicas` axes it qualified are **dropped** rather than carried: dropping is what "weaker"
  means here, and keeping them would produce a set this section forbids. The same holds for
  `dedup_window_ms` when `deduplication` weakens to `None`.
- An unknown map key MUST be skipped, per §5. That is how a later version adds a dimension
  without breaking this one — and it is also why a peer MUST NOT infer agreement from a key it
  skipped: what binds is the intersection of §2.3, computed over the dimensions both sides
  know.

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

HELLO  {…, guarantees_offered:{ordering:1}, guarantees_required:{ordering:1}}
       57 00 18  A7 00 81 00 01 19 40 00 02 19 04 00 03 80 04 80 05 A1 04 01 06 A1 04 01

ERROR  {code:5}                              (NO_REPLY, on a reply half)
       57 02 03  A1 00 05

DATA   {endpoint:"/md", topic:"px.eur"}      (publisher fan-out copy)
       57 01 0E  A2 00 63 2F 6D 64 05 66 70 78 2E 65 75 72

DATA   {endpoint:"/t", sequence:1}           (key 6; no v0 sender writes it)
       57 01 07  A2 00 62 2F 74 06 01

DATA   {endpoint:"/t", sequence:1, producer:<32-byte digest>}   (keys 6 and 7)
       57 01 2A  A3 00 62 2F 74 06 01 07 58 20
                 9F 86 D0 81 88 4C 7D 65 9A 2F EA A0 C5 5A D0 15
                 A3 BF 4F 1B 2B 0B 82 2C D1 5D 6C 15 B0 F0 0A 08

SUB    {endpoint:"/md", filter:"px."}
       57 03 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

UNSUB  {endpoint:"/md", filter:"px."}
       57 04 0B  A2 00 63 2F 6D 64 01 63 70 78 2E

DATA   {endpoint:"/md", topic:"px.*"}        (a topic is never a pattern)
       57 01 0C  A2 00 63 2F 6D 64 05 64 70 78 2E 2A

SUB    {endpoint:"/md", filter:"px.eur"}     (literal filter)
       57 03 0E  A2 00 63 2F 6D 64 01 66 70 78 2E 65 75 72

SUB    {endpoint:"/md", filter:"sensors.*.temp"}   (one-segment wildcard)
       57 03 16  A2 00 63 2F 6D 64 01 6E 73 65 6E 73 6F 72 73 2E 2A 2E 74 65 6D 70

SUB    {endpoint:"/md", filter:"ctl.#"}      (rest wildcard, final segment)
       57 03 0D  A2 00 63 2F 6D 64 01 65 63 74 6C 2E 23

SUB    {endpoint:"/md", filter:""}           (every topic)
       57 03 08  A2 00 63 2F 6D 64 01 60
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

**Sequenced DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x07` (7 bytes),
CBOR map of 2 entries: key `0` `endpoint = "/t"`, key `6` `sequence = 1`. The sequence is a
plain minimal `uint`, so the whole field costs two bytes here and six at `u64::MAX`. The
vector fixes the encoding; no v0 sender writes the key (§6.2).

**Relayed DATA vector** — magic `0x57`, kind `0x01` (DATA), `header_len = 0x2A` (42 bytes),
CBOR map of 3 entries: key `0` `endpoint = "/t"`, key `6` `sequence = 1`, key `7` `producer`
= the 32 bytes `9f86…0a08`, which is SHA-256 of `"test"` — the digest the address examples
of §4 already use. The `bstr` header is `58 20`: major type 2, one-byte length `0x20` = 32.
This is the shape a relay or an L2 hop writes when the producer is **not** the connection
peer; the `sha256:<64 hex>` spelling never appears on the wire, and a value of any other
length is a framing violation (§6.2).

**SUBSCRIBE and UNSUBSCRIBE vectors** — magic `0x57`, kind `0x03` / `0x04`,
`header_len = 0x0B` (11 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `1`
`filter = "px."`. The two frames differ in exactly one byte, the kind.

These two vectors fix an *encoding*, not a match: under the filter grammar of §6.4 the byte
string `px.` is a two-segment filter `["px", ""]`, so it does **not** select the topic
`px.eur` of the fan-out vector above — `px.*` or `px.#` does. The vector predates the grammar
and its bytes are still exactly what an encoder must produce for that filter string.

**Filter grammar vectors** — four SUBSCRIBE frames, one per construct of §6.4, all on
`endpoint = "/md"` and differing only in key `1`: the literal `px.eur`
(`header_len = 0x0E`), the one-segment wildcard `sensors.*.temp` (`0x16`), the rest wildcard
`ctl.#` (`0x0D`), and the empty filter (`0x08`, value `0x60` — the empty text string, present
because absent and empty must stay distinguishable). UNSUBSCRIBE carries the same header
under kind `0x04`. Each fixes an encoding; what each *selects* is the matcher's business, and
the pairs are pinned together in `crates/weida/src/pubsub.rs`.

**Literal-wildcard topic vector** — magic `0x57`, kind `0x01` (DATA),
`header_len = 0x0C` (12 bytes), CBOR map of 2 entries: key `0` `endpoint = "/md"`, key `5`
`topic = "px.*"`. A **`topic` is never a pattern** (§6.2, §6.4): the `*` here is an ordinary
byte, and the filter `px.*` selects this topic exactly as it selects `px.eur`.

Every vector §8 once deferred has now landed with its codec. The DATA key `6` and `7`
vectors and the extended HELLO below pin encodings rather than describe traffic:
`weida-protocol` reads and writes all three, while no v0 sender sets the DATA keys and no v0
peer declares a guarantee set.

**HELLO vector** — magic `0x57`, kind `0x00` (HELLO), `header_len = 0x10` (16 bytes),
CBOR map of 5 entries: key `0` `versions = [0]`, key `1` `max_header_bytes = 16384`,
key `2` `max_transfers = 1024`, key `3` `capabilities = []`, key `4`
`required_capabilities = []`. Keys `5` and `6` are absent, which is the declaration every v0
peer makes: offering and requiring the default guarantee set `core` (§6.1, §6.5).

**HELLO-with-guarantees vector** — the same five keys plus key `5`
`guarantees_offered = {4: 1}` and key `6` `guarantees_required = {4: 1}`, each a nested
one-entry map declaring `ordering = PerProducer detect`; `header_len = 0x18` (24 bytes).
Two things it pins. A dimension left at `core` is **not** written, so a HELLO whose
declarations are explicitly `core` is byte-identical to the v0 HELLO above — that is what
makes the declarations free for a peer that wants none. And the nested map obeys every rule
of §5 unchanged: uint keys, strictly ascending, no duplicates, unknown keys skipped.

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

**A refusal is not ordered against the receipt, and no frame will be added at wire version 0
to order it** ([decisions/0005](decisions/0005-refusal-race.md) §4.1-§4.3). A `STOP_SENDING`
refusal (§9.3, §9.4) is an application act, while the receipt is the transport's, so a payload
small enough to fit in flight can be acknowledged before the peer's application refuses it:
`delivered()` then resolves `Ok` for a transfer that was discarded, truthfully, since the
receipt never claimed anything about the application. A refusal is *guaranteed* to be observed
in exactly two constructions: a payload larger than the peer's `stream_receive_window`, where
flow control forces the application to act before the write can finish, and an exchange, whose
ERROR frame on the reply half is written by the receiving application and takes precedence
over the request half's receipt (§9.1). An application that must observe a refusal uses
Req/Rep.

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
topic **filter** (§6.4). Publishing a message means writing one DATA frame per matching subscriber,
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
MUST NOT rely on observing them in that order. Per-producer ordering needs the sequence key
of §6.2, which is specified ahead of code and written by no v0 implementation (§11); a
subscriber that has negotiated the detect level of `PerProducer` (§6.5) can then observe a
drop instead of missing it silently, which is the whole reason the key exists
([decisions/0001](decisions/0001-sequence-field.md) §7.2).

Ordering between exchanges is likewise `None`, for the same reason.

---

## 10. Resource limits

Limits live in two places, split by what they bound. `weida-core::Limits` is a
**per-connection** profile: every field applies to one connection, which is what lets a
runtime hold one profile per connection tier when the control tier of §2.5 arrives. Numbers
that belong to a runtime rather than to a connection live on `RuntimeConfig`. Every limit
exists to bound memory or state that a remote peer can cause to be allocated (master doc §50,
§81 rule 17).

Per connection (`Limits`):

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_header_bytes` | `16384` | largest header a peer may make this side buffer; checked before allocation (§3.1) |
| `max_concurrent_uni_streams` | `2048` | QUIC `TransportConfig::max_concurrent_uni_streams`; bounds parked/in-flight one-way stream tasks per connection |
| `max_concurrent_bidi_streams` | `1024` | QUIC `TransportConfig::max_concurrent_bidi_streams`; bounds live exchanges per connection |
| `stream_receive_window` | 1 MiB | per-stream QUIC flow-control window; bounds unread payload buffered per stream |
| `connection_receive_window` | 16 MiB | per-connection QUIC flow-control window; bounds unread payload buffered per connection |
| `keep_alive` | 10 s | QUIC keep-alive interval; sent by the dialling side only, so a binding's value is not read (§2.4) |
| `idle_timeout` | 30 s | QUIC idle timeout, applied in both directions (§2.4) |
| `hello_timeout_ms` | 10 s | time a connection may exist without a processed peer HELLO |
| `max_subscriptions` | `256` | subscription filters one peer connection may hold, summed over paths; exceeding it closes the connection with `LIMIT_EXCEEDED` (§6.4) |
| `subscriber_buffer_bytes` | 8 MiB | payload bytes a publisher will hold queued for one subscriber; a message that does not fit is dropped for that subscriber (§9.5) |
| `max_sequence_scopes` | `1024` | producer scopes — paths and topics — a receiver tracks per connection for gap detection or reassembly under `PerProducer` ordering; the peer names the scopes, so at the cap a new one is simply not tracked |
| `max_reorder_hold` | `256` | transfers a receiver holds back at once, over all scopes, under `PerProducer(reassemble)`; at the cap the oldest held transfer is released out of order with its gap reported (§6.5, [GUARANTEES.md](GUARANTEES.md) §3). A held transfer is an unread stream, so the bytes it pins are bounded again by `stream_receive_window` and `connection_receive_window` |
| `max_dedup_entries` | `4096` | identities a receiver remembers per connection under `Bounded` deduplication; the negotiated window bounds how long an identity is kept and this bounds how many, evicting the oldest at the cap (§6.5) |
| `max_local_streams` | `255` | live transfers on one **local** connection (§2.1), where the stream is the OS object and there is no multiplexing; opening past the cap fails with `LIMIT_EXCEEDED` locally rather than queueing. The number is Windows' named-pipe instance limit, the tightest of the three platforms [0010 §4.2] |

Per runtime (`RuntimeConfig`):

| Field | Default | Bound enforced |
| --- | --- | --- |
| `max_connections` | `1024` | connections accepted per server binding; excess connections are closed immediately with `LIMIT_EXCEEDED` |
| `max_connections_per_peer` | `64` | connections one **peer** may hold on one binding, counted by the fingerprint it proved; the excess connection is closed with `LIMIT_EXCEEDED`. It exists because one connection per dialled path (§2.5) lets the dialling side choose the count, and 64 connections to one peer measured about 50 MiB of transport state on the pair ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-011). Connections that proved no identity are each their own peer and are bounded only by `max_connections` (§2.5) |
| `endpoint_queue` | `256` | depth of the accept channel per registered endpoint |
| `max_resolved_addresses` | `8` | addresses a dialling endpoint will try for one hostname, in the resolver's order; a resolver answer is remote input, so its length needs a ceiling |
| `connect_attempt_timeout` | 250 ms | how long a dial waits on one resolved address before trying the next. Every address but the last is bounded by it; an IP literal and a single-address name keep the full handshake budget. The value is RFC 8305's Connection Attempt Delay, and it exists because an address that answers nothing gives QUIC no refusal to observe |
| `shutdown_timeout` | 1 s | how long `Runtime::shutdown` waits for closed sockets to go idle before returning anyway ([decisions/0009](decisions/0009-drain.md) §4.4) |

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

### 10.1 Control and bulk profiles

*Spec ahead of code, for the control half only.*
[decisions/0002](decisions/0002-control-and-bulk-separation.md) §6.2-§6.3 gives a peer pair one
**control** connection and one **bulk** connection per dialled path (§2.5). The bulk half is
implemented; the control half is not, and §2.5 says why. The two carry different traffic —
control frames are small, latency-sensitive and few; bulk streams are large, many and
throughput-sensitive — so one set of numbers cannot size both, and `Limits` is already a
per-connection profile so that a second one can be added without moving anything:

| Profile | Sized for | Fields that would differ from the table above |
| --- | --- | --- |
| `control` | a handful of short frames at a time, never a payload | small `stream_receive_window` and `connection_receive_window`; a `max_concurrent_uni_streams` budget that only has to cover HELLO, SUBSCRIBE, UNSUBSCRIBE and the reserved credit frame of §11; `max_concurrent_bidi_streams` may be `0` |
| `bulk` | payload transfers on one path | the windows and stream budgets of the table above, which are also the byte and message credit a consumer grants ([decisions/0003](decisions/0003-credit-unit.md) §4.1) |

The control numbers are not chosen here, and deliberately not in the code either: a profile
nothing reads would be a number nobody has to justify. They are chosen with the tier, from the
measured cost of a connection — a cold handshake of ~1.1 ms and 750-850 KiB of resident state
per live connection counting both ends ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-011) — and
from the per-path fan measurement that follows it. What is normative now is that a peer MUST
NOT be charged twice for the same limit: profiles are separate budgets, not one budget shared,
and `max_connections_per_peer` bounds the connections of a peer whatever tier they belong to.

Both profiles are advertised the same way: `max_header_bytes` in HELLO applies to the
connection the HELLO arrived on (§2.3), so a control connection may advertise a smaller header
limit than a bulk connection to the same peer.

---

## 11. Not specified in v0

The following are deliberately absent from wire protocol version 0. Implementations MUST
NOT invent wire representations for them; they will be specified in later protocol
versions.

- **Application-level acknowledgements.** `Accepted`, `Stored(Written|Flushed)`,
  `Replicated(n, flushed)` and `Processed` are broker-layer semantics — a transfer of
  responsibility to a broker hop — with the exact conditions each certifies now fixed
  ([decisions/0004](decisions/0004-durability-levels.md) §4.1-§4.4,
  [GUARANTEES.md](GUARANTEES.md) §1) and scheduled for Phase 6. They are declarable in a
  guarantee set (§6.5) and impossible to honour here, so a peer that requires one fails
  negotiation (§2.3). The v0 core deliberately carries no such frame: without a broker to take
  responsibility, such an acknowledgement would mean "arrived in RAM", which QUIC's own
  transport receipt (§9.2) already states more honestly. That the receipt therefore cannot be
  ordered against an application refusal is a decided position, not an omission
  ([decisions/0005](decisions/0005-refusal-race.md), §9.2).
- **Router/Dealer equivalents.** Not needed as wire constructs: an exchange is a stream, so
  unlimited concurrent unsynchronized requests and correctly matched replies both fall out
  of §9.1. What Router adds beyond that — forwarding to third parties, identity envelopes —
  is broker work and has no v0 representation. Pair patterns are likewise unspecified.
- **Connecting publishers and binding pushers.** In v0 Rep, Pull and Pub bind while Req,
  Push and Sub connect. The reverse directions have no v0 representation.
- **Streaming fan-out.** A publisher sends whole messages (§9.5). Tee-ing one long stream
  to many subscribers needs its own drop and ordering design and is not specified.
- **Ordering and deduplication beyond what is negotiated.** The sequence and producer keys
  of §6.2 are coded, pinned (§8) and now acted on: a peer that negotiated `PerProducer`
  numbers its one-way transfers and, in `detect`, reports gaps, or, in `reassemble`, holds
  arrivals back up to `max_reorder_hold` ([decisions/0001](decisions/0001-sequence-field.md)
  §7.5, [GUARANTEES.md](GUARANTEES.md) §3). What stays unspecified: `PerKey` ordering has no
  wire representation at all and is L2 work [0001 §7.4], `Total` is unspecified, and
  `Durable` deduplication needs a store and belongs to the broker [0001 §7.6]. None of these
  adds a frame: the guarantee set of §6.5 is the whole wire surface for them.
- **A quiescence signal.** `Runtime::drain(deadline)` is a **local** operation and has no
  wire representation: it stops admitting work, waits on transport receipts the connection
  already produces and then closes with `SHUTDOWN` like any other close. A peer observes
  exactly what it observes today — refused streams and a close — and nothing announces the
  drain. The alternative, a frame the peer answers when it has taken everything, is an
  application acknowledgement and is closed by
  [decisions/0005](decisions/0005-refusal-race.md); no protocol in the catalogue offers one
  either ([decisions/0009](decisions/0009-drain.md) §4.8, §3 option E). Implementations MUST
  NOT invent a quiescence frame for it.
- **Session state.** No session identifier, no subscription resumption and no sequence
  resumption. The peer's proved fingerprint identifies it across connections and carries no
  retained state; resumption is L2 work
  ([decisions/0008](decisions/0008-session-identity.md) §4.5, §4.6).
- **The L2 credit frame.** Frame kind `5` is **reserved** for the broker-layer credit frame of
  [decisions/0003](decisions/0003-credit-unit.md) §4.2-§4.3: an absolute delivery limit per
  subscription, idempotent under loss or duplication. It names a subscription, so it is
  path-scoped and rides that path's connection rather than a control connection, which amends
  0003 §4.2 ([decisions/0011](decisions/0011-answered-where-it-arrived.md) §4.3). Its
  fields are fixed with the Phase 6 broker design. A wire-version-0 receiver has no such frame
  and MUST therefore treat kind `5` as unknown and close with `PROTOCOL_VIOLATION` (§3.2); the
  reservation only forbids anyone else from taking the number.
- **Multiple replies per exchange.** Exactly one reply or one ERROR per reply half; a second
  is not representable.
- **Persistence.** No wire concept of durability, storage acknowledgement or recovery.
- **Deduplication as a behaviour.** The producer-identity key of §6.2 is coded and pinned in
  §8; `Bounded(window)` and the receiver-side window that would use it are decided
  [0001 §7.6], [0008 §4.4] and unbuilt, so no v0 peer suppresses a duplicate. `Durable`
  deduplication is L2 work.
- **Capability codes.** The capability negotiation mechanism exists (HELLO keys `3` and
  `4`), but no capability code is assigned and the supported set is empty.
- **QUIC datagrams.** Only streams are used.
- **Resumable or checkpointed streams.** No offsets, content addressing or resume semantics
  (master doc §82 leaves this for after the basic protocol is proven).
- **Retries.** The protocol carries no retry or attempt metadata; retry is entirely an
  application concern in v0.
- **Authentication beyond TLS.** No application-level authentication fields in HELLO.
