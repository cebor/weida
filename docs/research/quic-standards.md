# QUIC: standards, extensions and literature

## 0. Scope

This sheet describes QUIC as a transport, in the vocabulary of the documents that define it: packets, frames, streams, flow
control credit, connection IDs, paths, transport parameters. It does not describe a messaging protocol, because QUIC is not one;
it describes what QUIC hands to whatever protocol runs on top of it, and what it does not hand over. The layout deviates from the
catalogue template in `README.md` because the template's sections (patterns, delivery guarantees, reliability recipes) presuppose
an application protocol.

Documents covered, with the version researched:

- QUIC version 1: RFC 8999 (invariants), RFC 9000 (transport), RFC 9001 (TLS), RFC 9002 (loss detection and congestion control),
  all Standards Track / Informational, May 2021.
- QUIC version 2: RFC 9369, May 2023.
- Extensions: RFC 9221 (unreliable datagrams, March 2022), RFC 9297 (HTTP datagrams and the capsule protocol, August 2022), and
  the active drafts listed in section 8.
- Applications over QUIC: RFC 9114 (HTTP/3, June 2022), RFC 9204 (QPACK, June 2022), RFC 9220 (extended CONNECT, June 2022), RFC
  9218 (extensible prioritization, June 2022), WebTransport drafts, RFC 9298 and RFC 9484 (MASQUE).
- Operational: RFC 9308 (applicability, September 2022), RFC 9312 (manageability, September 2022), RFC 9368 (compatible version
  negotiation, May 2023).

Everything below is sourced. Section numbers refer to the cited document. Statements that are the author's reading rather than the
document's own words are marked `[inference]`. Vendor benchmarks and preprints are marked as such in section 11.

## 1. QUIC core (RFC 8999, 9000, 9001, 9002)

### 1.1 What is fixed across all versions

RFC 8999 defines the subset of QUIC that cannot change between versions: UDP encapsulation, the Header Form bit, the Version
field, connection IDs in long headers, and the Version Negotiation packet (RFC 8999 §5, §6). "Unless specifically prohibited in
this document, any aspect of the protocol can change between different versions" (RFC 8999 §2). A connection ID is "an opaque
field of arbitrary length" (RFC 8999 §5.3); the invariants place no meaning on its content. Everything else in this sheet is QUIC
version 1 unless stated otherwise.

### 1.2 Connections, packets, frames

A QUIC connection is "shared state between a client and a server" (RFC 9000 §5). UDP datagrams carry one or more QUIC packets;
packets carry frames. Packets that are declared lost are not retransmitted: "QUIC packets that are determined to be lost are not
retransmitted whole. The same applies to the frames that are contained within lost packets. Instead, the information that might be
carried in frames is sent again in new frames as needed" (RFC 9000 §13.3). Section 13.3 enumerates, frame type by frame type, what
is resent and when sending stops: STREAM data until acknowledged or until RESET_STREAM is sent; RESET_STREAM until the sending
part reaches "Reset Recvd" or "Data Recvd"; STOP_SENDING until the receiving part reaches "Data Recvd" or "Reset Recvd";
MAX_DATA/MAX_STREAM_DATA/MAX_STREAMS by resending the current value, not the lost one; CONNECTION_CLOSE is not resent on loss at
all (RFC 9000 §13.3, §10.2.1 covers its re-emission rules instead).

Packet numbers are per packet number space (Initial, Handshake, Application Data). 0-RTT and 1-RTT share the Application Data
space (RFC 9001 §4.9.3). Packet numbers are monotonically increasing per space and are never reused, which decouples loss
detection from retransmission (RFC 9000 §12.3); the exception is a packet retransmitted from the closing state (RFC 9000 §10.2.1,
note).

### 1.3 Handshake, ALPN, transport parameters

QUIC uses TLS 1.3 and only TLS 1.3 (RFC 9001 §4.2). Handshake messages travel in CRYPTO frames, not on a stream, and CRYPTO data
"is not flow controlled in the same way as stream data" (RFC 9000 §4, RFC 9001 §4). ALPN is mandatory: "Unless another mechanism
is used for agreeing on an application protocol, endpoints MUST use ALPN for this purpose", and failure to negotiate one is a
connection error 0x0178 `no_application_protocol` at both client and server (RFC 9001 §8.1).

Transport parameters are carried in a TLS extension (RFC 9001 §8.2) and are therefore authenticated by the handshake, exchanged
once, and not renegotiable during the connection. The full list is RFC 9000 §18.2. Defaults that matter later: `max_ack_delay`
defaults to 25 ms and values of 2^14 or greater are invalid; `ack_delay_exponent` defaults to 3; `active_connection_id_limit` MUST
be at least 2 and defaults to 2; `max_idle_timeout` of 0 or absent means no idle timeout from that endpoint (RFC 9000 §18.2).
There is no mechanism in version 1 to raise `initial_max_data`-style parameters other than through the corresponding MAX_* frames.

The client's first Initial datagram MUST be at least 1200 bytes of UDP payload (RFC 9000 §8.1, §14.1). The server MUST NOT send
more than three times the bytes it has received before validating the client address (RFC 9000 §8.1); see section 9.

### 1.4 Version negotiation

RFC 9000 §6 defines the Version Negotiation packet but leaves the reaction to it unspecified; RFC 9368 supplies both an
incompatible mechanism (a round trip, applicable to all versions) and a compatible one (no round trip, applicable when the server
can convert the client's first flight) (RFC 9368 §2, §2.2, §2.3). Compatibility is not symmetric (RFC 9368 §2.2). QUIC version 2
(RFC 9369) changes the long-header version value to 0x6b3343cf, all long-header packet type codepoints, the Initial salt, the HKDF
labels and the Retry integrity key/nonce, and nothing else: "QUIC version 2 provides no change from QUIC version 1 for the
capabilities available to applications" (RFC 9369 §3, §7). TLS session tickets and NEW_TOKEN tokens are version-specific and MUST
NOT be carried across versions (RFC 9369 §5).

## 2. Streams: semantics, limits, ordering, cancellation

### 2.1 Types and identifiers

"Streams in QUIC provide a lightweight, ordered byte-stream abstraction to an application" (RFC 9000 §2). A stream ID is a 62-bit
integer; bit 0x01 identifies the initiator (0 = client, 1 = server) and bit 0x02 distinguishes bidirectional (0) from
unidirectional (1), giving four independent ID spaces: 0x00 client-bidi, 0x01 server-bidi, 0x02 client-uni, 0x03 server-uni (RFC
9000 §2.1, Table 1). IDs within a type are consumed in increasing order, and "a stream ID that is used out of order results in all
streams of that type with lower-numbered stream IDs also being opened" (RFC 9000 §2.1). Stream IDs are never reused within a
connection (RFC 9000 §2.1). A single STREAM frame can open, carry data for, and close a stream (RFC 9000 §2).

### 2.2 What the byte stream is and is not

Within a stream, delivery is ordered and reliable: endpoints "MUST be able to deliver stream data to an application as an ordered
byte stream" and must buffer out-of-order data up to the flow control limit (RFC 9000 §2.2). Across streams there is nothing:
"QUIC does not provide any means of ensuring ordering between bytes on different streams" (RFC 9000 §2).

There is no message framing. "Streams are an ordered byte-stream abstraction with no other structure visible to QUIC. STREAM frame
boundaries are not expected to be preserved when data is transmitted, retransmitted after packet loss, or delivered to the
application at a receiver" (RFC 9000 §2.2). An application that needs message boundaries inside a stream defines them itself; RFC
9308 §4 offers the alternative of one message per stream, where "resetting the stream to expire an unacknowledged message can be
used to emulate partial reliability for that message" (RFC 9308 §4).

Out-of-order delivery within a stream is permitted as an implementation option but is not part of the contract: "QUIC makes no
specific allowances for delivery of stream data out of order. However, implementations MAY choose to offer the ability to deliver
data out of order to a receiving application" (RFC 9000 §2.2). RFC 9308 §4.3 warns that a sender that unilaterally changes to
non-contiguous sending "might encounter performance issues or deadlocks", because receivers commonly withhold flow control credit
until contiguous data is delivered.

### 2.3 Priority

"QUIC does not provide a mechanism for exchanging prioritization information. Instead, it relies on receiving priority information
from the application" (RFC 9000 §2.3). Implementations SHOULD offer an API for relative stream priority (RFC 9000 §2.3), but
nothing about priority appears on the wire. RFC 9308 §4.2 restates this: "Stream prioritization is not exposed to either the
network or the receiver." An application protocol that wants signalled priority defines it itself; RFC 9218 (Extensible
Prioritization Scheme for HTTP) is the HTTP-specific answer and is a separate document from HTTP/3.

### 2.4 The operations an application gets

RFC 9000 §2.4 does not define an API but does define the operations an application protocol may assume. On the sending part: write
data "understanding when stream flow control credit has successfully been reserved"; end the stream cleanly (STREAM frame with
FIN); reset the stream (RESET_STREAM). On the receiving part: read data; abort reading, "possibly resulting in a STOP_SENDING
frame". An application "can also request to be informed of state changes on streams, including when the peer has opened or reset a
stream, when a peer aborts reading on a stream, when new data is available, and when data can or cannot be written to the stream
due to flow control" (RFC 9000 §2.4). Note what is absent from this list: there is no operation that reports how much of the
stream the peer's application has read.

### 2.5 Stream states and what "acknowledged" means

The sending part of a stream (RFC 9000 §3.1) moves Ready -> Send -> Data Sent -> Data Recvd, with a side branch to Reset Sent ->
Reset Recvd.

- "Data Sent" is entered when the application has finished writing and a STREAM frame with FIN has been sent. From here the
  endpoint "only retransmits stream data as necessary" (RFC 9000 §3.1).
- "Once all stream data has been successfully acknowledged, the sending part of the stream enters the 'Data Recvd' state, which is
  a terminal state" (RFC 9000 §3.1).
- "Once a packet containing a RESET_STREAM has been acknowledged, the sending part of the stream enters the 'Reset Recvd' state,
  which is a terminal state" (RFC 9000 §3.1).

The names are misleading in a way that matters. "Data Recvd" on the *sending* side means every byte was acknowledged by the peer's
QUIC layer; it says nothing about the peer's application. The receiving side (RFC 9000 §3.2) has its own distinct states: Recv ->
Size Known -> Data Recvd -> Data Read, and Reset Recvd -> Reset Read. Only "Data Read" means the application consumed the bytes,
and the specification states plainly that the sender cannot see it: "the receiving part of a stream tracks the delivery of data to
the application, some of which cannot be observed by the sender" (RFC 9000 §3.2). There is no frame that carries "Data Read" back.
The strongest signal a sender gets is a QUIC-level acknowledgement plus, indirectly, the arrival of MAX_STREAM_DATA/MAX_DATA
credit, which a receiver typically emits as the application consumes data (RFC 9000 §3.2, §4.2).

### 2.6 RESET_STREAM and STOP_SENDING: who observes what

RESET_STREAM aborts the sending direction. "RESET_STREAM terminates one direction of a stream abruptly. For a bidirectional
stream, RESET_STREAM has no effect on data flow in the opposite direction" (RFC 9000 §4.4). On receipt "an endpoint will tear down
state for the matching stream and ignore further data arriving on that stream" (RFC 9000 §4.4).

What the receiver observes after a reset is deliberately loose: "Sending a RESET_STREAM means that an endpoint cannot guarantee
delivery of stream data; however, there is no requirement that stream data not be delivered if a RESET_STREAM is received. An
implementation MAY interrupt delivery of stream data, discard any data that was not consumed, and signal the receipt of the
RESET_STREAM. A RESET_STREAM signal might be suppressed or withheld if stream data is completely received and is buffered to be
read by the application" (RFC 9000 §3.2). So after a reset, a receiving application may see all of the data, some of it, or none
of it, and may or may not see the reset itself. That is implementation-defined behaviour inside the standard.

STOP_SENDING is a request, not a command over data already sent: it "requests that the receiving endpoint send a RESET_STREAM
frame", and the peer MUST comply if the stream is in "Ready" or "Send", MAY defer if in "Data Sent" (RFC 9000 §3.5). Sending
STOP_SENDING "typically indicates that the receiving application is no longer reading data it receives from the stream, but it is
not a guarantee that incoming data will be ignored", and "STREAM frames received after sending a STOP_SENDING frame are still
counted toward connection and stream flow control" (RFC 9000 §3.5). The error code SHOULD be copied from STOP_SENDING into the
RESET_STREAM, but the peer "can use any application error code" (RFC 9000 §3.5). To kill both directions an endpoint sends
RESET_STREAM and STOP_SENDING (RFC 9000 §3.5).

Permitted frames per state are tabulated in RFC 9000 §3.3: a sender emits only STREAM, STREAM_DATA_BLOCKED and RESET_STREAM; a
receiver emits only MAX_STREAM_DATA and STOP_SENDING; "a receiver could receive any of these three frames in any state, due to the
possibility of delayed delivery of packets carrying them".

### 2.7 Final size

"The final size is the amount of flow control credit that is consumed by a stream" — one more than the largest offset sent, or
zero (RFC 9000 §4.5). It is communicated reliably however the stream ends: as Offset+Length of the FIN-bearing STREAM frame, or as
the Final Size field of RESET_STREAM. "This guarantees that both endpoints agree on how much flow control credit was consumed by
the sender on that stream" (RFC 9000 §4.5). Once known it cannot change; a contradiction SHOULD be a FINAL_SIZE_ERROR, though
"generating these errors is not mandatory, because requiring that an endpoint generate these errors also means that the endpoint
needs to maintain the final size state for closed streams" (RFC 9000 §4.5). This is the one accounting invariant QUIC does
guarantee across an abrupt cancellation: not delivery, but byte count.

### 2.8 Stream limits as a resource bound

Concurrency is capped by the peer: "Only streams with a stream ID less than `(max_streams * 4 + first_stream_id_of_type)` can be
opened", set initially by `initial_max_streams_bidi` / `initial_max_streams_uni` and raised by MAX_STREAMS; limits are cumulative
over the connection's lifetime and can never be lowered (RFC 9000 §4.6). Exceeding the peer's limit is a connection error
STREAM_LIMIT_ERROR. A blocked endpoint SHOULD send STREAMS_BLOCKED, but "an endpoint MUST NOT wait to receive this signal before
advertising additional credit" (RFC 9000 §4.6).

This is the primary defence against the stream commitment attack: "An adversarial endpoint can open a large number of streams,
exhausting state on an endpoint... on a new connection, opening stream 4000000 opens 1 million and 1 client-initiated
bidirectional streams" (RFC 9000 §21.8). The related stream fragmentation attack — withholding a prefix so the receiver buffers
the rest — is mitigated by not overcommitting memory, bounding tracking structures, delaying reassembly, or heuristics on
reassembly holes (RFC 9000 §21.7).

Because the limit is cumulative and monotonic, it is also a shutdown mechanism, and RFC 9308 §4.5 treats it as one: an endpoint
can "stop sending increases to stream limits and allow the connection to naturally terminate once remaining streams are consumed",
but "the period of time it takes to do so is dependent on the peer, and an unpredictable closing period might not fit application
or operational needs". RFC 9308 recommends an application-layer graceful-close mechanism instead, of which HTTP/3 GOAWAY is the
example.

## 3. Flow control and congestion control

### 3.1 Two levels of credit

Flow control is limit-based, not window-based on the wire: a receiver advertises "the limit of total bytes it is prepared to
receive on a given stream or for the entire connection" (RFC 9000 §4.1). Stream-level credit is MAX_STREAM_DATA, an absolute byte
offset for that stream. Connection-level credit is MAX_DATA, "the maximum of the sum of the absolute byte offsets of all streams".
Senders MUST NOT exceed either; violation is FLOW_CONTROL_ERROR. Re-advertising a smaller limit is not an error but has no effect,
and senders MUST ignore non-increasing MAX_* frames (RFC 9000 §4.1).

A blocked sender SHOULD send STREAM_DATA_BLOCKED or DATA_BLOCKED, and SHOULD do so periodically when it has no ack-eliciting
packets in flight, otherwise the receiver may idle-timeout a connection whose sender has data to send (RFC 9000 §4.1).

### 3.2 The receiver's obligations

RFC 9000 §4.2 puts the burden on the receiver, deliberately: "A receiver MUST NOT wait for a STREAM_DATA_BLOCKED or DATA_BLOCKED
frame before sending a MAX_STREAM_DATA or MAX_DATA frame; doing so could result in the sender being blocked for the rest of the
connection." Guidance, not requirements, follows: send updates more than once per round trip or early enough to survive loss;
avoid frequent small increments; autotune from the RTT estimate and the rate at which the application consumes data; piggyback
credit on packets that carry other frames.

The performance consequence is stated flatly: "If an endpoint cannot ensure that its peer always has available flow control credit
that is greater than the peer's bandwidth-delay product on this connection, its receive throughput will be limited by flow
control" (RFC 9000 §4.3). And loss interacts with it: "Packet loss can cause gaps in the receive buffer, preventing the
application from consuming data and freeing up receive buffer space" (RFC 9000 §4.3).

Credit accounting survives cancellation: after RESET_STREAM both ends still agree on consumption via the final size (RFC 9000
§4.4, §4.5), and for a bidirectional stream "both endpoints MUST maintain flow control state for the stream in the unterminated
direction until that direction enters a terminal state" (RFC 9000 §4.4).

### 3.3 Deadlocks

RFC 9308 §4.4 is the most useful text in any of these documents for a protocol designer, and it is worth reproducing in outline.
Deadlock is possible "for any protocol that uses QUIC". The named cases:

1. A message larger than the available credit, where the recipient does not release credit until the whole message is received and
   delivered. "This is possible even where stream flow control limits are not reached because connection flow control limits can
   be consumed by other streams."
2. Length-prefixed formats make this easier to hit, because a consumer can look at the prefix and leave the body unread in the
   transport buffer, withholding credit. RFC 9308 notes a length prefix also makes the deadlock detectable, and suggests
   "reserving flow control credit for the entire message atomically".
3. Interdependent data on different streams: if stream A's data is unread because it depends on stream B, and stream B is blocked
   because A's unread data withholds connection credit, both stop. The recommendation is that "the sender should ensure that
   dependent data is not sent until the data it depends on has been accounted for in both stream- and connection-level flow
   control credit".

Mitigations offered: read eagerly into application buffers (with the caveat that the receiver then needs another way to hold the
peer accountable for that memory), or cancel affected streams with STOP_SENDING/RESET_STREAM, "Canceling some streams results in
the connection being terminated in some protocols" (RFC 9308 §4.4).

### 3.4 Acknowledgements, ack delay, max_ack_delay

Endpoints acknowledge all packets they receive and process, but only ack-eliciting packets force an ACK within a bounded time (RFC
9000 §13.2). `max_ack_delay` "declares an explicit contract: an endpoint promises to never intentionally delay acknowledgments of
an ack-eliciting packet by more than the indicated value. If it does, any excess accrues to the RTT estimate and could result in
spurious or delayed retransmissions from the peer" (RFC 9000 §13.2.1). Initial and Handshake ack-eliciting packets MUST be
acknowledged immediately. An endpoint MUST NOT send more than one ACK-only packet in response to an ack-eliciting packet, and MUST
NOT send a non-ack-eliciting packet in response to a non-ack-eliciting packet (RFC 9000 §13.2.1). Reordering and gaps SHOULD
trigger an immediate ACK, as SHOULD a CE-marked packet (RFC 9000 §13.2.1).

The default cadence is TCP's: "A receiver SHOULD send an ACK frame after receiving at least two ack-eliciting packets" (RFC 9000
§13.2.2). The measured delay is reported in the ACK Delay field, scaled by `ack_delay_exponent`; when the actual delay exceeds
`max_ack_delay` the endpoint SHOULD report the real value (RFC 9000 §13.2.5). ACK-only packets are not congestion controlled and
do not count as bytes in flight (RFC 9002 §7), which is what makes ACK volume a real cost — RFC 9308 §7 notes that "generating and
processing QUIC acknowledgments consumes resources at a sender and receiver" and points at the ack-frequency extension (see
section 8).

### 3.5 Loss detection and PTO

Loss detection uses a packet threshold (recommended kPacketThreshold = 3) and a time threshold (RFC 9002 §6.1.1, §6.1.2). The
Probe Timeout covers tail loss and ack loss:

```
PTO = smoothed_rtt + max(4*rttvar, kGranularity) + max_ack_delay
```

(RFC 9002 §6.2.1). `max_ack_delay` is set to 0 for the Initial and Handshake spaces. A PTO expiration "does not indicate packet
loss and MUST NOT cause prior unacknowledged packets to be marked as lost"; it sends one or two ack-eliciting probes. PTO backoff
doubles on each expiry and is reset on acknowledgement, with an exception for a client that is not yet sure the server validated
its address. "The total length of time over which consecutive PTOs expire is limited by the idle timeout" (RFC 9002 §6.2, §6.2.1).
With no prior RTT the initial RTT SHOULD be 333 ms, giving a 1 s initial PTO (RFC 9002 §6.2.2).

### 3.6 Congestion control

RFC 9002 specifies NewReno and explicitly permits anything else: "The signals QUIC provides for congestion control are generic and
are designed to support different sender-side algorithms. A sender can unilaterally choose a different algorithm to use, such as
CUBIC" — subject to conforming to RFC 8085 §3.1 (RFC 9002 §7). Congestion control is per path (RFC 9002 §7, RFC 9000 §9.4).
Initial window: 10 * max_datagram_size, capped at max(14720, 2 * max_datagram_size); minimum window 2 * max_datagram_size (RFC
9002 §7.2). Persistent congestion collapses the window to the minimum after loss of all packets across a duration derived from the
PTO (RFC 9002 §7.6). Pacing is a SHOULD (RFC 9002 §7.7).

CUBIC and BBR are implementation choices, not QUIC specifications; see section 13 for what each implementation ships and defaults
to.

Application-limited senders get an explicit rule, which matters for a workload of small, bursty messages: "When bytes in flight is
smaller than the congestion window and sending is not pacing limited, the congestion window is underutilized... When this occurs,
the congestion window SHOULD NOT be increased in either slow start or congestion avoidance" (RFC 9002 §7.8). A sender delayed only
by its own pacer SHOULD NOT count itself application limited. RFC 9002 §7.8 points at RFC 7661 for alternative behaviour after
idle periods but does not specify one.

### 3.7 ECN

QUIC can set ECT codepoints and treats a reported CE increase as congestion (RFC 9002 §7.1, RFC 9000 §13.4). ECN counts are echoed
in ACK frames (RFC 9000 §13.4.1) and an endpoint must run a validation procedure, disabling ECN on that path if it fails (RFC 9000
§13.4.2, §13.4.2.2). Reading the ECN field "is not possible on all platforms" (RFC 9000 §13.4.1), so ECN support is a deployment
property, not a guarantee.

### 3.8 Packetization latency

An application generally cannot control how its writes become frames and packets. "By default, many implementations will try to
pack STREAM frames from one or more streams into each QUIC packet, in order to minimize bandwidth consumption and computational
costs. If there is not enough data available to fill a packet, an implementation might wait for a short time to optimize bandwidth
efficiency instead of latency" (RFC 9308 §5). RFC 9308 recommends that implementations expose a way to say "send now" or to
suggest a bundling delay, but this is guidance to implementers, not a wire feature. Padding is likewise available to
implementations for length hiding (RFC 9308 §5).

## 4. Datagrams (RFC 9221) and HTTP datagrams (RFC 9297)

### 4.1 The DATAGRAM frame

RFC 9221 (March 2022) adds frame types 0x30 and 0x31 (LEN bit is 0x01) carrying opaque application bytes (RFC 9221 §4). Support is
advertised by the `max_datagram_frame_size` transport parameter (0x20), whose default is 0, meaning unsupported; it is a
unidirectional declaration, so datagrams may be negotiated in one direction only (RFC 9221 §3). Sending a DATAGRAM without having
received a non-zero value, or larger than the received value, is a PROTOCOL_VIOLATION. RFC 9221 recommends advertising 65535 "to
indicate that this endpoint will accept any DATAGRAM frame that fits inside a QUIC packet" (RFC 9221 §3).

### 4.2 One packet, no fragmentation

"DATAGRAM frames cannot be fragmented; therefore, application protocols need to handle cases where the maximum datagram size is
limited by other factors" — those factors being `max_datagram_frame_size`, the peer's `max_udp_payload_size`, and the path MTU
(RFC 9221 §5). The usable payload is therefore path-dependent and can shrink mid-connection.

### 4.3 No flow control

"DATAGRAM frames do not provide any explicit flow control signaling and do not contribute to any per-flow or connection-wide data
limit" (RFC 9221 §5.3). The stated consequence is that a receiver that cannot commit resources "MAY be dropped by the receiver if
the receiver cannot process them". There is no backpressure signal of any kind for datagrams.

### 4.4 Congestion control applies

"DATAGRAM frames employ the QUIC connection's congestion controller. As a result, a connection might be unable to send a DATAGRAM
frame generated by the application until the congestion controller allows it. The sender MUST either delay sending the frame until
the controller allows it or drop the frame without sending it (at which point it MAY notify the application)" (RFC 9221 §5.4).
Implementations may optionally support a per-datagram send expiry (RFC 9221 §5.4).

### 4.5 Acknowledgement semantics

DATAGRAM frames are not retransmitted, but they are ack-eliciting (RFC 9221 §5.2). An implementation MAY report loss and MAY
report acknowledgement to the application. The crucial sentence: "acknowledgement of a DATAGRAM frame only indicates that the
transport-layer handling on the receiver processed the frame and does not guarantee that the application on the receiver
successfully processed the data. Thus, this signal cannot replace application-layer signals that indicate successful processing"
(RFC 9221 §5.2). Receivers SHOULD delay ACKs for datagram-only packets within `max_ack_delay`, since the sender takes no action on
them (RFC 9221 §5.2).

### 4.6 No multiplexing identifier

"DATAGRAM frames belong to a QUIC connection as a whole and are not associated with any stream ID at the QUIC layer" (RFC 9221
§5.1). Any demultiplexing is the application protocol's job; RFC 9221 recommends a leading variable-length integer as a flow
identifier. Prioritisation between datagrams and streams is an implementation API concern, not a wire feature (RFC 9221 §5.1).

### 4.7 Datagrams and 0-RTT

A client may store the server's `max_datagram_frame_size` and send DATAGRAM frames in 0-RTT packets; the server must then
advertise a value at least as large as before (RFC 9221 §3). Because datagrams carry application data, an application protocol
that sends them in 0-RTT "require[s] a profile that defines acceptable use of 0-RTT" (RFC 9221 §6).

### 4.8 HTTP datagrams (RFC 9297)

RFC 9297 (August 2022) binds datagrams to HTTP requests. Over HTTP/3, the QUIC DATAGRAM payload is `Quarter Stream ID (i)`
followed by the payload; the quarter stream ID is the associated client-initiated bidirectional stream ID divided by four (RFC
9297 §2.1). Use is gated on SETTINGS_H3_DATAGRAM (0x33) being both sent and received with value 1 (RFC 9297 §2.1.1). Datagrams for
a not-yet-created stream are dropped or briefly buffered "on the order of a round trip"; after the receive side closes they are
silently dropped (RFC 9297 §2.1). Prioritisation of HTTP/3 datagrams is explicitly undefined (RFC 9297 §2.1).

Where QUIC datagrams are unavailable — HTTP/2 or HTTP/1.x — the same payloads travel as DATAGRAM capsules (type 0x00) on the
request's data stream via the Capsule Protocol (RFC 9297 §2.2, §3.5). This restores reliability and ordering and removes the
unreliability the application asked for: over HTTP/2 "demultiplexing is provided by the HTTP/2 framing layer, but unreliable
delivery is unavailable", and over HTTP/1.x neither is available (RFC 9297 §2). A capsule is a type-length-value on the data
stream, and the `Capsule-Protocol` header field signals its use (RFC 9297 §3.2, §3.4). RFC 9297 §4 notes that advertising
SETTINGS_H3_DATAGRAM "sticks out" on the wire and recommends always sending it if supported.

## 5. Connection lifecycle: handshake, 0-RTT, idle, migration, closing

### 5.1 Handshake and its confirmation points

TLS 1.3 runs in CRYPTO frames. "Handshake complete" is when the TLS stack says so; "handshake confirmed" is, at the server, when
the handshake completes, and at the client, when it receives HANDSHAKE_DONE (RFC 9001 §4.1.1, §4.1.2). Several rules key off
confirmation: an endpoint MUST NOT initiate migration before it (RFC 9000 §9), MUST NOT arm the Application Data PTO before it
(RFC 9002 §6.2.1), MAY initiate a key update only after it (RFC 9001 §6), and MUST send CONNECTION_CLOSE in a 1-RTT packet after
it (RFC 9000 §10.2.3).

### 5.2 0-RTT and what the application must do

0-RTT reuses parameters from a previous connection carried in a TLS session ticket (RFC 9001 §4.6). TLS 1.3 caps the age of the
ticket at seven days (RFC 9001 §4.6). Certain transport parameters MUST NOT be remembered: `ack_delay_exponent`, `max_ack_delay`,
`initial_source_connection_id`, `original_destination_connection_id`, `preferred_address`, `retry_source_connection_id`,
`stateless_reset_token` (RFC 9000 §7.4.1). A server that accepts 0-RTT MUST NOT reduce most flow-control and stream limits
relative to the remembered values (RFC 9000 §7.4.1).

The replay problem is delegated upward, in unusually direct language. "STREAM, RESET_STREAM, STOP_SENDING, and CONNECTION_CLOSE
frames are potentially unsafe for use with 0-RTT as they carry application data... A client therefore MUST NOT use 0-RTT for
application data unless specifically requested by the application that is in use. An application protocol that uses QUIC MUST
include a profile that defines acceptable use of 0-RTT; otherwise, 0-RTT can only be used to carry QUIC frames that do not carry
application data" (RFC 9001 §5.6). And: "Ultimately, the responsibility for managing the risks of replay attacks with 0-RTT lies
with an application protocol... Disabling 0-RTT entirely is the most effective defense against replay attack" (RFC 9001 §9.2).
QUIC's own frame processing is idempotent and not replay-vulnerable (RFC 9001 §9.2).

RFC 9308 §3.1 adds the operational reading: idempotent operations "might be safe", but "it is also possible to combine
individually idempotent operations into a non-idempotent sequence of operations", and "once a server accepts 0-RTT data, there is
no means of selectively discarding data that is received".

What 0-RTT resumes is the TLS session and the transport parameters. It does not resume streams, stream IDs, or any application
state; QUIC "does not depend on any state being retained when resuming a connection unless 0-RTT is also used", and application
state across resumption is the application protocol's business (RFC 9001 §4.5). Session resumption also lets a server link the two
connections, which is a privacy consideration for the client (RFC 9001 §4.5, §9.1).

### 5.3 Idle timeout arithmetic and keep-alive

If either endpoint advertises `max_idle_timeout`, "the connection is silently closed and its state is discarded when it remains
idle for longer than the minimum of the max_idle_timeout value advertised by both endpoints" — or the sole non-zero value if only
one advertises (RFC 9000 §10.1). The timer restarts on receiving and processing a packet, and on sending an ack-eliciting packet
if none has been sent since the last receive. "Endpoints MUST increase the idle timeout period to be at least three times the
current Probe Timeout (PTO)" (RFC 9000 §10.1).

Keep-alive is a PING frame or any other ack-eliciting frame; QUIC has no dedicated keep-alive and "application protocols that use
QUIC SHOULD provide guidance on when deferring an idle timeout is appropriate" (RFC 9000 §10.1.2). Middlebox state is the real
constraint: "Though REQ-5 in [RFC4787] recommends a 2-minute timeout interval, experience shows that sending packets every 30
seconds is necessary to prevent the majority of middleboxes from losing state for UDP flows" (RFC 9000 §10.1.2). RFC 9308 §3.2
cites the 2010 Hatonen study for the claim that "UDP applications can assume that any NAT binding or other state entry can expire
after just thirty seconds of inactivity", notes RFC 8085 requires a minimum keep-alive interval of 15 seconds and recommends
larger, and warns that pinging more often than every 30 s "may result in excessive unproductive traffic in some situations and
unacceptable power usage for power-constrained (mobile) devices". Its three options for an application are: ignore the issue,
avoid long idle periods, or reconnect with 0-RTT after a long idle period — the last "only valid in cases in which it is safe to
use 0-RTT and when the client is the restarting peer" (RFC 9308 §3.2).

### 5.4 Migration, NAT rebinding, path validation

A connection survives an address change because packets are matched by connection ID (RFC 9000 §9). Constraints:

- No migration before the handshake is confirmed (RFC 9000 §9).
- `disable_active_migration` forbids the peer from using a new local address; a violating peer's packets must be dropped or
  path-validated, never answered with a stateless reset or a close, because that would let third parties break connections (RFC
  9000 §9, §18.2).
- "This document limits migration of connections to new client addresses... Clients are responsible for initiating all migrations.
  Servers do not send non-probing packets toward a client address until they see a non-probing packet from that address" (RFC 9000
  §9).
- NAT rebinding is not distinguished from deliberate migration by intent, only by procedure: "An endpoint MUST perform path
  validation if it detects any change to a peer's address, unless it has previously validated that address" (RFC 9000 §9).

Path validation is a PATH_CHALLENGE with an unpredictable payload echoed in a PATH_RESPONSE (RFC 9000 §8.2, §8.2.1, §8.2.2).
PATH_CHALLENGE, PATH_RESPONSE, NEW_CONNECTION_ID and PADDING are "probing frames"; a packet with only those is a probing packet
(RFC 9000 §9.1). Failure to validate a path "does not cause the connection to end unless there are no valid alternative paths
available" (RFC 9000 §9.1). Congestion control and RTT state are reset on migration: packets on the old path "MUST NOT contribute"
to the new path's controller (RFC 9000 §9.4). RFC 9308 §9 summarises the cost: "Path validation takes at least one RTT, and
congestion control will also be reset after path migration. Therefore, migration usually has a performance impact."

Connection ID rotation exists for privacy, not for routing: "An endpoint MUST NOT reuse a connection ID when sending from more
than one local address" and likewise for more than one destination address (RFC 9000 §9.5). Endpoints SHOULD supply fresh
connection IDs before peers migrate, because "an endpoint that exhausts available connection IDs cannot probe new paths or
initiate migration" (RFC 9000 §9.5). A zero-length connection ID makes migration pointless or impossible: an endpoint "SHOULD NOT
initiate migration with a peer that has requested a zero-length connection ID" (RFC 9000 §9.5), and RFC 9308 §11 warns such a
connection is "effectively unable to survive NAT rebinding or migrate to a new path".

The server's only lever is `preferred_address`, offered once in the handshake, carrying an IPv4 and an IPv6 address plus a
connection ID with sequence number 1 and its stateless reset token (RFC 9000 §9.6.1, §18.2). The client validates and migrates
itself. "Migrating a connection to a new server address mid-connection is not supported by the version of QUIC specified in this
document. If a client receives packets from a new server address when the client has not initiated a migration to that address,
the client SHOULD discard these packets" (RFC 9000 §9.6).

### 5.5 Closing: immediate close, draining, stateless reset

Three terminations exist: idle timeout, immediate close, stateless reset (RFC 9000 §10).

CONNECTION_CLOSE ends everything at once: "A CONNECTION_CLOSE frame causes all streams to immediately become closed; open streams
can be assumed to be implicitly reset" (RFC 9000 §10.2). There are two frame types — 0x1c carries a transport error code, 0x1d an
application error code (RFC 9000 §19.19); RFC 9308 §6 describes them as "different types of CONNECTION_CLOSE frames... used to
signal transport and application errors". Only 0x1d may carry an application's reason.

The sender enters *closing*, the receiver *draining*; both SHOULD persist "for at least three times the current PTO interval" (RFC
9000 §10.2). A closing endpoint responds to incoming packets with CONNECTION_CLOSE at a rate-limited pace and may retain only its
connection ID and version (RFC 9000 §10.2.1). A draining endpoint "MUST NOT send any packets" beyond an optional single
CONNECTION_CLOSE (RFC 9000 §10.2.2). After these states end the endpoint discards state and MAY answer later packets with a
stateless reset (RFC 9000 §10.2).

Unacknowledged stream data at close is simply gone. Nothing in RFC 9000 §10 preserves it, and there is no linger or drain of
stream data; the graceful-shutdown gap is stated outright by RFC 9308 §10: "QUIC does not provide any mechanism for graceful
connection termination; applications using QUIC can define their own graceful termination process (see, for example, Section 5.2
of [QUIC-HTTP])." [inference] For a sender, the only way to know which of its writes the peer received before a close is an
application-level acknowledgement or, in HTTP/3's case, the GOAWAY identifier.

Stateless reset is the last resort for an endpoint with no connection state: a packet ending in a 16-byte token derived from the
connection ID, indistinguishable from a valid packet to anyone who does not know the token (RFC 9000 §10.3, §10.3.1, §10.3.2). RFC
9308 §10 notes it carries "no application-layer information", so an application learns only that the connection is unrecoverable.

## 6. HTTP/3 (RFC 9114), QPACK, Extended CONNECT (RFC 9220), WebTransport

### 6.1 Requests on streams

HTTP/3 (RFC 9114, June 2022) puts each request-response exchange on exactly one client-initiated bidirectional QUIC stream, and a
client MUST send only one request per stream (RFC 9114 §4.1, §6.1). The message is one HEADERS frame, optional DATA frames,
optional trailing HEADERS; an invalid sequence is a connection error H3_FRAME_UNEXPECTED (RFC 9114 §4.1). The first request is on
stream 0, then 4, 8, and so on; servers SHOULD permit at least 100 concurrent request streams (RFC 9114 §6.1). After sending the
request a client closes its send side, except for CONNECT (RFC 9114 §4.1, §4.4) — the exception WebTransport is built on. HTTP/3
does not use server-initiated bidirectional streams; receiving one is H3_STREAM_CREATION_ERROR unless an extension negotiated it
(RFC 9114 §6.1).

### 6.2 Control and QPACK unidirectional streams

Unidirectional streams begin with a variable-length integer stream type (RFC 9114 §6.2). Type 0x00 is the control stream: exactly
one per side, SETTINGS as its first frame or H3_MISSING_SETTINGS, a second one is H3_STREAM_CREATION_ERROR, and closing it is
H3_CLOSED_CRITICAL_STREAM (RFC 9114 §6.2.1). QPACK (RFC 9204, June 2022) adds encoder stream 0x02 and decoder stream 0x03 (RFC
9204 §4.2). Because of these, "the transport parameters sent by both clients and servers MUST allow the peer to create at least
three unidirectional streams" and SHOULD grant each at least 1024 bytes of credit (RFC 9114 §6.2). Unknown stream types must be
aborted or drained, never treated as a connection error (RFC 9114 §6.2). Unlike HTTP/2, all HTTP/3 frame headers and payloads are
subject to QUIC flow control, not only DATA (RFC 9114 §A.2.3), so a stalled reader stalls framing.

### 6.3 No priority in the base spec

"HTTP/3 does not provide a means of signaling priority" (RFC 9114 §A.2.1). RFC 9218 (June 2022) supplies the `Priority` header
field (§5) and the PRIORITY_UPDATE frame (§7) as a separate document, and it is advisory in both directions.

### 6.4 GOAWAY: the graceful-shutdown primitive QUIC lacks

GOAWAY (RFC 9114 §5.2, frame type 0x07) carries a direction-dependent identifier: a server sends a client-initiated bidirectional
stream ID; a client sends a push ID. "Requests or pushes with the indicated identifier or greater are rejected... by the sender of
the GOAWAY. This identifier MAY be zero if no requests or pushes were processed." Consequences, quoted because they are the exact
semantics an application needs for at-least-once behaviour across a restart:

- "Upon receipt of a GOAWAY frame, if the client has already sent requests with a stream ID greater than or equal to the
  identifier contained in the GOAWAY frame, those requests will not be processed. Clients can safely retry unprocessed requests on
  a different HTTP connection."
- "Requests on stream IDs less than the stream ID in a GOAWAY frame from the server might have been processed; their status cannot
  be known until a response is received, the stream is reset individually, another GOAWAY is received with a lower stream ID than
  that of the request in question, or the connection terminates."
- Successive GOAWAYs must be non-increasing; a larger identifier is H3_ID_ERROR.
- The two-phase drain: first a GOAWAY with the maximum value (2^62-4 for servers, 2^62-1 for clients) to stop new work, then a
  second GOAWAY with the real cutoff after in-flight requests arrive. "This ensures that a connection can be cleanly shut down
  without losing requests" (RFC 9114 §5.2).

Error codes relevant to reset handling (RFC 9114 §8.1): H3_NO_ERROR 0x0100, H3_EXCESSIVE_LOAD 0x0107, H3_ID_ERROR 0x0108,
H3_SETTINGS_ERROR 0x0109, H3_REQUEST_REJECTED 0x010b (not processed at all, hence safe to retry), H3_REQUEST_CANCELLED 0x010c,
H3_REQUEST_INCOMPLETE 0x010d, H3_MESSAGE_ERROR 0x010e, H3_VERSION_FALLBACK 0x0110.

### 6.5 Extended CONNECT (RFC 9220)

RFC 9220 (June 2022) ports RFC 8441 (September 2018) from HTTP/2 to HTTP/3: the same `:protocol` pseudo-header and the same
setting, registered separately for HTTP/3 as SETTINGS_ENABLE_CONNECT_PROTOCOL = 0x08, default 0 (RFC 9220 §3, §5). An unknown or
unsupported `:protocol` SHOULD get a 501. Stream FIN maps to an orderly TCP close and a stream reset maps to H3_REQUEST_CANCELLED
(RFC 9220 §3).

### 6.6 WebTransport over HTTP/3 (draft-ietf-webtrans-http3-16, 6 July 2026, WG Last Call)

The current revision differs substantially from earlier ones; names from drafts up to -13 are obsolete. In -16:

- Negotiation is SETTINGS_WT_ENABLED = 0x2c7cf000 (default 0; a value above 1 is H3_SETTINGS_ERROR). There is no
  SETTINGS_WEBTRANSPORT_MAX_SESSIONS in -16; that setting was introduced in draft-07 and later removed
  (draft-ietf-webtrans-http3-16 §3.1, §9.2; -13 change log).
- The upgrade token and `:protocol` value are `webtransport-h3` (§3.2, §9.1). Browser clients MUST send `Origin`. A server must
  additionally send SETTINGS_ENABLE_CONNECT_PROTOCOL=1, SETTINGS_H3_DATAGRAM=1, `max_datagram_frame_size` > 0 and an empty
  `reset_stream_at` transport parameter; failing that, the peer MAY close with WT_REQUIREMENTS_NOT_MET 0x212c0d48 (§3.1). The
  CONNECT request cannot be sent in 0-RTT (§3.2).
- The session ID is the stream ID of the CONNECT stream (§4). Unidirectional WebTransport streams use QUIC stream type 0x54
  followed by the session ID; bidirectional ones start with the value 0x41 (registered as WT_STREAM) followed by the session ID
  (§4.2, §4.3). Streams may be opened optimistically before the CONNECT response; a receiver buffers unassociated streams up to a
  cap and resets the excess with WT_BUFFERED_STREAM_REJECTED 0x3994bd84 (§4.6).
- Datagrams use RFC 9297 HTTP Datagrams: the WebTransport payload goes directly after the Quarter Stream ID identifying the
  CONNECT stream, with no additional context ID (§4.5; RFC 9297 §2.1).
- Session-level flow control exists in the current draft and is mandatory for pooled sessions: SETTINGS_WT_INITIAL_MAX_STREAMS_UNI
  (0x2b64), SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65), SETTINGS_WT_INITIAL_MAX_DATA (0x2b61), all default 0, with
  WT_MAX_STREAMS / WT_STREAMS_BLOCKED / WT_MAX_DATA / WT_DATA_BLOCKED capsules (§5.1, §5.5, §5.6). Per-stream limits come from
  QUIC itself, so WT_MAX_STREAM_DATA is prohibited over HTTP/3 (§5.4). Without negotiated flow control a client MUST NOT open more
  than one session (§5.1, §5.2).
- Termination: the WT_CLOSE_SESSION capsule (0x2843, renamed from CLOSE_WEBTRANSPORT_SESSION with the same codepoint) carries a
  32-bit application error code and a UTF-8 message of at most 1024 bytes; the sender must FIN immediately, and all associated
  streams are reset with WT_SESSION_GONE 0x170d7b68 (§6). WT_DRAIN_SESSION (0x78ae, formerly DRAIN_WEBTRANSPORT_SESSION) is
  advisory: after it, both sides MAY still open streams, and an HTTP/3 GOAWAY implies a drain of every session on the connection
  (§4.7).
- Application error codes are mapped into a reserved HTTP/3 error range (WT_APPLICATION_ERROR 0x52e4a40fa8db..0x52e5ac983162) by
  `first + n + floor(n/0x1e)` to skip greasing codepoints, and resets MUST use RESET_STREAM_AT with a Reliable Size covering the
  WebTransport header so the session ID always arrives (§4.4). This is why WebTransport depends on the partial-delivery reset
  extension of section 8.4.

### 6.7 WebTransport over HTTP/2 (draft-ietf-webtrans-http2-15, 6 July 2026)

The fallback runs the whole session inside HTTP/2 DATA frames on one CONNECT stream, so every QUIC mechanism becomes a capsule:
WT_STREAM 0x190B4D3B/0x190B4D3C (low bit = FIN) creates streams and carries their data, WT_RESET_STREAM 0x190B4D39 (with a
Reliable Size, modelled on RESET_STREAM_AT), WT_STOP_SENDING 0x190B4D3A, and per-stream flow control WT_MAX_STREAM_DATA
0x190B4D3E, which the HTTP/3 mapping forbids (§6.2-6.9). Datagrams become RFC 9297 DATAGRAM capsules and are therefore
retransmitted by TCP. The draft says so directly: "WebTransport over HTTP/2 does not support unreliable delivery" and "does not
support stream independence, as HTTP/2 inherently has head-of-line blocking" (§5.1). The same capsule protocol may also be run
over HTTP/3 under the token `webtransport` rather than `webtransport-h3`, with the same two losses (draft-ietf-webtrans-http3-16
§2.1.2).

### 6.8 What browsers actually expose

The W3C WebTransport API (Editor's Draft, 8 September 2026) exposes `createBidirectionalStream`, `createUnidirectionalStream`,
`incomingBidirectionalStreams`, `incomingUnidirectionalStreams`, `ready` / `closed` / `draining` promises, and `close(closeInfo)`
with a 32-bit `closeCode` and a reason truncated to 1024 UTF-8 bytes (§6.3, §6.4, §6.10). Stream options are `sendGroup`,
`sendOrder` and `waitUntilAvailable`, the last deciding whether a create call rejects or waits when no flow-control credit is
available (§6.11). Datagrams are a `WebTransportDatagramDuplexStream` with `readable`, `createWritable()`, `maxDatagramSize`,
`incomingMaxAge` / `outgoingMaxAge` and `incomingMaxBufferedDatagrams` / `outgoingMaxBufferedDatagrams` (§5); the
`incomingHighWaterMark` / `outgoingHighWaterMark` members that appeared in earlier versions of the spec are gone from the current
Editor's Draft and are marked deprecated in MDN browser-compat-data. Writes larger than `maxDatagramSize` resolve without being
sent (§4.3). `reliability` reports `"pending" | "reliable-only" | "supports-unreliable"`, and `congestionControl` is a hint
(`"default" | "throughput" | "low-latency"`) that is silently downgraded if unsupported (§6.3, §6.9). `serverCertificateHashes`
works only on dedicated (unpooled) connections, verifies a SHA-256 hash over the DER leaf certificate, and requires an X.509v3
certificate whose "total validity period MUST NOT exceed two weeks", with an allowed key set that must include ECDSA P-256 and
must not include RSA (§6.9).

Shipping status as of 2026-09-10, from MDN browser-compat-data (`api/WebTransport.json`, main branch) and the WebKit release blog:
Chrome 97, Edge and Opera mirrored, Firefox 114, Safari 26.4 (released 2026-03-24); MDN marks the feature Baseline "since March
2026". Gaps are substantial and asymmetric: `allowPooling`, `requireUnreliable`, `congestionControl`, `createSendGroup`,
`createWritable`, `sendOrder` and the `anticipatedConcurrent*` hints are unimplemented in Chrome, while Firefox has had
`congestionControl`, `allowPooling` and `requireUnreliable` since 114, `sendOrder` since 119 and
`createSendGroup`/`createWritable` since 155. `serverCertificateHashes`: Chrome 100, Firefox 125, Safari 26.4.
`incomingMaxBufferedDatagrams`: Chrome 151, Safari preview, not in Firefox. WebTransport over HTTP/2 is not shipped in any
browser: MDN browser-compat-data has no key for it and quic-go's documentation states neither Chrome nor Firefox implements the
fallback (undated third-party page, retrieved 2026-09-10). [inference] The practical consequence is that browser WebTransport is
HTTP/3-only, so the "fallback" exists on paper for non-browser clients.

## 7. MASQUE and proxying (RFC 9298, 9484)

### 7.1 CONNECT-UDP (RFC 9298, August 2022)

RFC 9298 defines the `connect-udp` upgrade token. A client is configured with an RFC 6570 URI Template carrying `target_host` and
`target_port`; the interoperability default is
`https://$PROXY_HOST:$PROXY_PORT/.well-known/masque/udp/{target_host}/{target_port}/` (§2, §3). On HTTP/2 and HTTP/3 the request
is extended CONNECT with `:protocol = connect-udp` (§3.4); on HTTP/1.1 it is a GET with `Upgrade: connect-udp` answered by 101
(§3.2, §3.3). The HTTP Datagram Payload is a Context ID followed by the UDP payload, with context ID 0 reserved for UDP payloads;
non-zero IDs are allocated per request, even by the client and odd by the proxy (§4, §5).

The guarantee is narrow. On a 2xx the proxy has a socket to the target and "commits to converting received HTTP Datagrams into UDP
packets, and vice versa, until the tunnel is closed" (§3), with inactivity timeouts not shorter than two minutes (§3.1). It does
not guarantee reachability — UDP is connectionless, so the 2xx does not mean the target answered — nor delivery, nor ordering, and
it will not repair size violations: "UDP proxies MUST NOT introduce fragmentation at the IP layer", oversized datagrams are
silently dropped (§3.1). Nested congestion control is called out explicitly: proxied congestion-controlled traffic runs under at
least two controllers, and the outer connection MUST NOT disable congestion control without out-of-band certainty (§6). An
intermediary must not convert a QUIC DATAGRAM into a DATAGRAM capsule, because that would silently make an unreliable tunnel
reliable and break the inner endpoint's path-MTU discovery (§6.1). RFC 9931 (March 2026) updates RFC 9298 §6.3 with requirements
against optimistic HTTP/1.1 protocol transitions.

### 7.2 CONNECT-IP (RFC 9484, October 2023)

`connect-ip` tunnels IP packets. Its URI Template variables `target` and `ipproto` are optional and may be wildcards (§3). Three
capsules configure the tunnel (§4.7): ADDRESS_ASSIGN (0x01) assigns addresses and prefixes, ADDRESS_REQUEST (0x02) requests them
with a Request ID that must be echoed, ROUTE_ADVERTISEMENT (0x03) announces forwarded ranges. Each capsule carries the complete
current list, so omission is withdrawal and an empty capsule withdraws everything. Context ID 0 carries a full IP packet from the
Version field onward (§5, §6). Overhead for a context-0 datagram over HTTP/3 is 51 bytes, which is why inner QUIC Initials pad to
1331 rather than 1200 (§10.1). Use cases: remote-access and site-to-site VPN, IP flow forwarding, proxied connection racing (§8).

### 7.3 QUIC-aware proxying (draft-ietf-masque-quic-proxy-09, 6 July 2026, WG Last Call)

Two additions to connect-udp. Connection-ID registration capsules (REGISTER_CLIENT_CID, REGISTER_TARGET_CID, the corresponding
ACK_/REJECT_/CLOSE_ capsules, MAX_CONNECTION_IDS) let a proxy multiplex many proxied QUIC connections over one UDP 4-tuple toward
a target (§3.1). Forwarded mode rewrites short-header packets between client and proxy using dedicated or virtual connection IDs
and a negotiated packet transform (`identity` or `scramble`), avoiding a second layer of encryption and encapsulation (§4, §4.3,
negotiated with `Proxy-QUIC-Forwarding` and `Proxy-QUIC-Port-Sharing`, §2.3). Long-header packets stay tunnelled and forwarded
mode is HTTP/3 only. The draft states the trade-off itself: "packets sent in Forwarded mode are not congestion controlled between
client and proxy" (§1).

Other MASQUE work in flight as of 2026-09: `draft-ietf-masque-connect-udp-listen-16` (24 August 2026, RFC Editor queue),
`draft-ietf-masque-connect-ethernet-14` (18 August 2026), `draft-ietf-masque-connect-ip-dns-06` (12 April 2026),
`draft-ietf-masque-connect-udp-ecn-dscp-02` (22 July 2026), `draft-ietf-masque-http-datagram-compression-01` (6 July 2026), and
the individual `draft-seemann-masque-connect-udp-rendezvous-00` (16 August 2026).

## 8. Multipath and other active drafts

### 8.1 Multipath QUIC (draft-ietf-quic-multipath-21, 17 March 2026, RFC Editor queue)

Approved and past IETF Last Call, awaiting an RFC number. It adds explicit path IDs — same value in both directions, starting at
0, monotonically increasing, never reused — negotiated by the `initial_max_path_id` transport parameter (§2.1). The design
decision is per-path packet number spaces, one per path ID, which "enables direct use of the loss detection and congestion control
mechanisms defined in [QUIC-RECOVERY] on a per-path basis" at the cost of requiring non-zero connection IDs and folding the path
ID into the AEAD nonce (§1, §2.4).

Frames in -21: PATH_ACK (0x3e/0x3f) with a Path Identifier, PATH_ABANDON (0x3e75) with error codes APPLICATION_ABANDON_PATH /
PATH_RESOURCE_LIMIT_REACHED / PATH_UNSTABLE_OR_POOR / NO_CID_AVAILABLE_FOR_PATH, PATH_STATUS_AVAILABLE (0x3e77) and
PATH_STATUS_BACKUP (0x3e76), PATH_NEW_CONNECTION_ID (0x3e78), PATH_RETIRE_CONNECTION_ID (0x3e79), MAX_PATH_ID (0x3e7a), and the
informational PATHS_BLOCKED (0x3e7b) / PATH_CIDS_BLOCKED (0x3e7c) (§4.1-4.7). All are 1-RTT only. Note that the frame names
PATH_AVAILABLE and PATH_STANDBY belong to earlier revisions.

What it does not give: a scheduler. Abstract: "This document does not specify address discovery or management, nor how
applications using QUIC schedule traffic over multiple paths." §1: "there are currently no IETF specifications that define
scheduling algorithms for simultaneously (i.e., concurrently) using multiple paths." §5.5 spells out the consequence for a
stream-based application: "If multiple paths are used to send data frames belonging to the same stream, data delivery will
experience the maximum delay of all used paths due to in-order delivery. The scheduling is a local decision, based on the
preferences of the application and the implementation." Retransmission strategy is equally open: "While this document does not
preclude a specific strategy, more detailed specification is out of scope" (§5.6).

### 8.2 QUIC-LB (draft-ietf-quic-load-balancers-21, 27 August 2025, expired 28 February 2026)

The server encodes a Server ID into the connection ID so a layer-4 load balancer can route by CID instead of 4-tuple, which is
what makes client migration and NAT rebinding survivable behind a load balancer. The first CID octet carries a three-bit Config
Rotation field (up to seven live configurations; codepoint 0b111 is always unroutable and falls back to 4-tuple routing) plus
optional length self-description (§3). Encoding is a single-pass encryption for one specific field length or a general four-pass
encryption (§5.4.1, §5.4.2, §5.5); undecodable CIDs are "unroutable" and handled by a fallback algorithm (§4.3). The load balancer
holds only the configuration, never TLS keys. Config rotation is the zero-downtime restart lever, and §9.5 covers the
stateless-reset oracle with one reset key per rotation codepoint. Retry-service handling was split out into
`draft-ietf-quic-retry-offload-00` (25 May 2022), which is expired and parked.

The document itself is expired with IESG state "Expired" while remaining a WG document. That is a real caveat:
connection-ID-routable server pools are widely implemented but rest on an unmaintained specification.

### 8.3 ACK frequency (draft-ietf-quic-ack-frequency-14, 5 February 2026, expired 9 August 2026)

This is not RFC 9538. RFC 9538 is "Content Delivery Network Interconnection (CDNI) Delegation Using the Automated Certificate
Management Environment", February 2024, and has nothing to do with QUIC. ACK frequency remains an Internet-Draft at -14, WG state
"WG Consensus: Waiting for Write-Up", IESG state "Expired".

Content: the `min_ack_delay` transport parameter (0xff04de1b) is a variable-length integer in microseconds — note that
`max_ack_delay` is in milliseconds — which must not exceed `max_ack_delay`, is a unilateral indication of support, and must not be
remembered across connections, so these frames cannot be used in 0-RTT (§3). The ACK_FREQUENCY frame (0xaf) carries a Sequence
Number, an Ack-Eliciting Threshold (0 means acknowledge every packet), a Requested Max Ack Delay in microseconds, and a Reordering
Threshold (§4). IMMEDIATE_ACK (0x1f) is a bare ack-eliciting frame that is congestion controlled and not retransmitted (§5). The
motivation is CPU and reverse-path capacity: "Sending UDP datagrams is very CPU intensive on some platforms... this reduction can
be critical for high packet rate connections", plus the symmetric sender-side cost of processing ACK-only packets and saturation
of asymmetric return paths (§2).

### 8.4 Partial-delivery stream reset (draft-ietf-quic-reliable-stream-reset-11, 6 September 2026)

Approved, awaiting the RFC Editor ("Approved-announcement to be sent::AD Followup"). The current title is "QUIC Stream Resets with
Partial Delivery". The transport parameter is `reset_stream_at` (0x1d) with an empty value; a non-empty value is
TRANSPORT_PARAMETER_ERROR, and the extension may be used in 0-RTT if both sides remember it (§3). RESET_STREAM_AT (frame type
0x24) is RESET_STREAM plus a trailing Reliable Size field (§4). The sender MUST deliver at least Reliable Size bytes,
retransmitting losses below that offset and SHOULD NOT retransmitting above it; Reliable Size greater than Final Size is
FRAME_ENCODING_ERROR; Final Size is still subject to flow control, so the frame may have to wait for credit (§4, §5). For the
receiving application the outcome is still an abrupt end: "after providing the stream data guaranteed by the Reliable Size, the
QUIC implementation signals a stream reset rather than a clean end of the stream" (§4). The draft calls itself "a form of
range-based partial reliability" and names WebTransport's session-ID prefix as the motivating case (§1).

### 8.5 Unreliable streams, partial reliability, message boundaries: nothing active

There is no active IETF draft or RFC providing unreliable streams, intra-stream partial reliability beyond a delivered prefix, or
message boundaries inside a QUIC stream. The attempts all expired without adoption: `draft-tiesel-quic-unreliable-streams-01`
(expired 3 May 2018), `draft-tiesel-quic-unreliable-http-00` (expired 9 March 2018), and
`draft-lubashev-quic-partial-reliability-03`, which proposed an EXPIRED_STREAM_DATA frame (expired 1 December 2018). What survived
is RFC 9221 DATAGRAM frames (whole-datagram unreliability, no streams) and the Reliable Size prefix of RESET_STREAM_AT. Message
framing inside a stream stays an application concern by design (RFC 9000 §2.2), and draft-ietf-quic-multipath-21 §5.6 repeats the
point for the multipath case, where a retransmission on a path with a smaller MTU forces resegmentation.

### 8.6 Other active QUIC working group drafts worth naming (state as of 2026-09)

- `draft-ietf-quic-qmux-02` (6 July 2026, WG document): "QMux version 1 provides, over bi-directional streams such as TLS, the
  same set of stream and datagram operations that applications rely upon in QUIC version 1" — a QUIC-shaped API over TCP+TLS for
  environments where UDP is blocked. Its §1.1 lists the costs: head-of-line blocking for everything, no unreliable datagram
  delivery (DATAGRAM frames become reliable and ordered), no migration, no multipath.
- `draft-ietf-quic-address-discovery-01` (15 August 2026): `address_discovery` transport parameter (0x9f81a176) and the
  OBSERVED_ADDRESS probing frame, an in-QUIC replacement for STUN that stays inside the encrypted envelope.
- `draft-ietf-quic-extended-key-update-03` (6 July 2026): fresh forward-secret keying beyond the RFC 9001 key-phase bit.
- `draft-ietf-quic-receive-ts-03` (20 July 2026): extended acknowledgements carrying per-packet receive timestamps, for
  delay-based congestion control and one-way delay measurement.
- qlog: `draft-ietf-quic-qlog-main-schema-14`, `draft-ietf-quic-qlog-quic-events-13`, `draft-ietf-quic-qlog-h3-events-13` (all 6
  July 2026) — the structured logging schema that makes QUIC debuggable at all.
- NAT traversal: there is no `draft-ietf-quic-nat-traversal`; the datatracker returns 404. The live work is individual:
  `draft-bruynooghe-n0-quic-nat-traversal-00` (6 July 2026) and `draft-seemann-masque-connect-udp-rendezvous-00` (16 August 2026).
  Also individual, not adopted: `draft-jholland-quic-multicast-09` (6 July 2026), `draft-zheng-quic-fec-extension-02` (16 March
  2026), `draft-gage-quic-pathmgmt-06` (31 May 2026).

## 9. Security: TLS 1.3 binding, client certificates, ALPN, retry and amplification limits

### 9.1 The TLS binding

QUIC does not run TLS over a stream; it carries TLS handshake messages in CRYPTO frames and takes keys from the TLS key schedule
for each encryption level (RFC 9001 §4, §5.1). Only TLS 1.3 is defined (RFC 9001 §4.2). Consequences that follow from the binding
rather than from TLS: transport parameters travel inside a TLS extension and are therefore authenticated, with no unauthenticated
exchange and no renegotiation (RFC 9001 §8.2); the TLS `EndOfEarlyData` message is unused and middlebox compatibility mode is
prohibited (§8.3, §8.4); everything except Version Negotiation packets is protected, but Initial keys derive from the client's
Destination Connection ID and so give no confidentiality against an on-path observer (RFC 9000 §21.1.2, RFC 9001 §5.2); Retry
packets carry an integrity tag rather than encryption (§5.8); and key updates are possible only after handshake confirmation,
driven by the Key Phase bit, with per-algorithm AEAD usage limits (§6, §6.6).

### 9.2 ALPN is mandatory

"QUIC requires that the cryptographic handshake provide authenticated protocol negotiation. TLS uses Application-Layer Protocol
Negotiation to select an application protocol. Unless another mechanism is used for agreeing on an application protocol, endpoints
MUST use ALPN for this purpose" (RFC 9001 §8.1). Failure to negotiate is a connection error 0x0178 `no_application_protocol`, and
RFC 9001 extends the requirement to clients, which RFC 7301 does not. An application protocol MAY restrict which QUIC versions it
runs over, and a mismatch is the same error (RFC 9001 §8.1).

### 9.3 Client authentication and its ceiling

"A server MAY request that the client authenticate during the handshake. A server MAY refuse a connection if the client is unable
to authenticate when requested" (RFC 9001 §4.4). Mutual TLS is therefore available, but only at handshake time. The prohibition is
absolute:

> "A server MUST NOT use post-handshake client authentication (as defined in Section 4.6.2 of [TLS13]) because the multiplexing
> offered by QUIC prevents clients from correlating the certificate request with the application-level event that triggered
> it... servers MUST NOT send post-handshake TLS CertificateRequest messages, and clients MUST treat receipt of such messages as
> a connection error of type PROTOCOL_VIOLATION." (RFC 9001 §4.4)

So a QUIC connection has exactly one client identity, fixed at the handshake, for its whole life. There is no way to elevate
privilege mid-connection at the transport layer, and no per-stream or per-message identity: any such notion belongs to the
application protocol. RFC 9001 §4.4 also warns that certificate chain size directly costs handshake performance because of the 3x
amplification limit, recommending ECDSA keys or certificate compression.

### 9.4 Address validation, the 3x limit, Retry and NEW_TOKEN

"Prior to validating the client address, servers MUST NOT send more than three times as many bytes as the number of bytes they
have received" (RFC 9000 §8.1), counting all payload bytes of datagrams attributed to the connection, including those whose
packets were discarded. Clients MUST pad datagrams containing Initial packets to at least 1200 bytes, and MUST send a packet on
each PTO to avoid the deadlock where the server is amplification-limited and the client has nothing to say (RFC 9000 §8.1).

Validation happens implicitly on receipt of a Handshake-protected packet, or explicitly:

- Retry (RFC 9000 §8.1.2): the server answers the client's Initial with a token, costing a round trip before any handshake work.
  The token must be constructed so the server can tell Retry tokens from NEW_TOKEN tokens (RFC 9000 §8.1.1) and must have at least
  128 bits of entropy (RFC 9000 §8.1.4).
- NEW_TOKEN (RFC 9000 §8.1.3): a token issued on one connection for use on a later one, letting a returning client skip Retry.
  Tokens are opaque to the client and MUST NOT carry application semantics (RFC 9001 §9.2).

RFC 9000 §21.3 notes the residual attack: an attacker can obtain a token, release the address, and have a later victim receive the
amplified flight. Path validation (§5.4 above) is the same mechanism applied to migration, and closing endpoints are bound by the
same 3x rule when answering packets from unvalidated addresses (RFC 9000 §10.2.1).

### 9.5 Denial-of-service surface that a server operator must bound

RFC 9000 §21 enumerates what a hostile peer can make the other side allocate. The ones a transport for many long-lived connections
has to answer:

- Stream commitment (§21.8): opening a high stream ID implicitly opens every lower one of that type; `initial_max_streams_*` and
  MAX_STREAMS are the only defence, and "setting the limit too low could affect performance when applications expect to open a
  large number of streams".
- Stream fragmentation and reassembly (§21.7): withheld prefixes force receiver buffering. Overcommitting flow control windows is
  faster when peers behave and is exactly what makes this attack work.
- Slowloris (§21.6): the recommended mitigations are limiting open streams and imposing minimum transfer-rate or timeout limits,
  both application policy.
- Optimistic ACK (§21.4): acknowledging unsent packets inflates the sender's rate; the suggested defence is skipping packet
  numbers.
- Peer denial of service (§21.9): frames with no observable state effect (PADDING, ACK-only packets, unknown frames) can be sent
  in volume; endpoints "SHOULD track the use of these frames and treat excessive use as a connection error".
- Stateless reset oracle (§21.11): if an attacker can make a server generate a stateless reset for a connection it shares an
  address with, it can tear down that connection; servers sharing a reset key across a pool must ensure the same connection cannot
  be routed to different servers.
- Request forgery (§21.5): the peer can influence where an endpoint sends UDP datagrams through Initial destinations, preferred
  addresses, spoofed migration and version negotiation.

## 10. Operational guidance (RFC 9308, RFC 9312, RFC 9368/9369)

### 10.1 Applicability (RFC 9308, September 2022)

RFC 9308 is the document written for people mapping an application protocol onto QUIC, and it is blunt about what QUIC leaves
undone. Points not already covered in sections 2, 3 and 5:

- Fallback is a design obligation, not an optional extra: §2 exists because "QUIC uses UDP as a substrate" and UDP is blocked or
  degraded on some paths, so an application "needs to be able to fall back" or accept the failures.
- Stream multiplexing is invisible to the network: "Streams are meaningful only to the application; since stream information is
  carried inside QUIC's encryption boundary, a given packet exposes no information about which stream(s) are carried within the
  packet. Therefore, stream multiplexing is not intended to be used for differentiating streams in terms of network treatment"
  (§4.1). Traffic needing different treatment belongs on separate connections.
- Design rules for stream use (§4): one stream gives ordering, several give concurrency, one message per stream gives message
  orientation and cancellation. Reaching the peer's stream limit does not automatically cause it to be raised; applications
  should query stream properties rather than infer them from the stream ID and must not assume which ID an unallocated stream
  will get; and a stream or a connection carries at most 2^62-1 bytes in each direction.
- QoS: "packets belonging to the same connection should use a single DSCP", and differential treatment requires separate
  connections, with the caveat that more connections mean competing congestion controllers (§12).
- Error handling: QUIC's error code space is separate from the application's, and application error codes may be reused between
  connection-level and stream-level errors (§6).
- Acknowledgement efficiency: the default every-other-packet strategy has real cost, and §7 points at the ack-frequency extension.

### 10.2 Manageability (RFC 9312, September 2022)

RFC 9312 describes the wire image for network operators, and what it says a network cannot see is the operative part for anyone
building a transport on QUIC:

- Only the Header Form bit, the version and connection IDs in long headers, and the Destination Connection ID in short headers are
  visible; packet numbers are always encrypted (§2.1, §2.7).
- "Observing a new connection ID does not necessarily indicate a new connection" (§2.6).
- "QUIC does not expose the end of a connection; the only indication to on-path devices that a flow has ended is that packets are
  no longer observed" (§3.6). Middlebox state therefore expires on a timer, which is the root of the keep-alive discussion in
  section 5.3.
- QUIC-LB-style CID encodings are the sanctioned way to share routing information with load balancers, and the encoding "should
  appear random to any other observers, which is most rigorously achieved with encryption" (§2.6).
- ACK-only packets are heuristically identifiable by size (§3.3, and RFC 9002 §8.2 notes the same as a traffic-analysis concern).

### 10.3 Version negotiation and compatible versions (RFC 9368, RFC 9369)

RFC 9000 §6 defines Version Negotiation packets but not the endpoint reaction; RFC 9368 (May 2023) supplies it. Incompatible
negotiation costs a round trip and works for any pair of versions; compatible negotiation costs nothing but requires that the
server can convert the client's first flight to the other version (RFC 9368 §2, §2.2, §2.3). Compatibility is directional: "It is
possible for version A to be compatible with version B and for version B not to be compatible with version A" (§2.2). Version
Information is exchanged in the handshake and exists to make the negotiation authenticated and downgrade-resistant (§3, §4).
Servers in a fleet must offer a consistent version set, and §5 discusses the deployment problem when they do not.

RFC 9369 (QUIC version 2, May 2023) exists mainly as an anti-ossification exercise: it changes the version number, the long-header
type codepoints, the Initial salt, the HKDF labels and the Retry integrity key/nonce, and nothing else (§3). "QUIC version 2
provides no change from QUIC version 1 for the capabilities available to applications" (§7). Version 1 and version 2 are
compatible in both directions for the purposes of RFC 9368 (§4.1), but session tickets and NEW_TOKEN tokens are version-specific
and must not be reused across versions (§5). An application protocol therefore gains nothing from version 2 except resistance to
middleboxes that hardcoded version 1 layouts.

## 11. Measured evidence from the literature

### 11.1 Head-of-line blocking: smaller than the claim

Marx et al., "Same Standards, Different Decisions" (ACM EPIQ '20, August 2020) surveyed 18 or more QUIC/HTTP-3 stacks with
qlog/qvis and found that ACK policy, packet coalescing, flow control and prioritisation diverge widely between implementations
that all claim the same standard. The companion analysis (Marx, Web Performance Calendar, 3 December 2020) spells out the
consequence: QUIC removes transport-level head-of-line blocking only when several streams are in flight simultaneously, and loss
on real paths is bursty (runs of roughly ten consecutive packets), so a sequential scheduler still stalls. The unresolved conflict
is named: interleaving maximises HOL-blocking removal, sequential sending minimises per-resource completion time. Caveat: analysis
plus traces, not a controlled loss-rate experiment.

Yu and Benson, "Dissecting Performance of Production QUIC" (ACM WWW '21, April 2021) is the strongest empirical rebuttal. Setup: a
1.3 GHz MacBook Air on home Wi-Fi, 10 Mbps shaping, 0/0.1/1% added loss, 50/100 ms added RTT, at least 40 runs per cell, against
production Google, Facebook and Cloudflare h3-29 endpoints with Chrome, Proxygen and ngtcp2. Result: "QUIC's removal of HOL
blocking has little impact on Page Load Time and Speed Index relative to congestion control for real-world web-pages". HTTP/3 beat
HTTP/2 consistently only for 100 KB objects, which is the handshake advantage; at 1 MB and 5 MB they were equivalent. Cloudflare's
HTTP/3 was much worse than its own HTTP/2 at 1% loss, root-caused to CUBIC-for-QUIC against BBR-for-TCP — a congestion controller
choice, not a protocol property. Caveat: one vantage point, uniformly random loss only.

Trevisan et al., "Measuring HTTP/3: Adoption and Performance" (arXiv 2102.12358, February 2021; Computer Communications 2022)
measured thousands of HTTP/3-capable sites: "HTTP/3 provides sizable benefits only in scenarios with high latency or very poor
bandwidth. Despite the adoption of QUIC, we do not find benefits in case of high packet loss." Caveat: third-party objects on
those pages still travel over HTTP/2 or HTTP/1.1, diluting the measured effect.

### 11.2 CPU cost of userspace QUIC: the sharpest disagreement

Zhang et al., "QUIC is not Quick Enough over Fast Internet" (arXiv 2310.09423, October 2023; ACM WWW '24, May 2024 — preprint
later peer-reviewed): UDP+QUIC+HTTP/3 loses up to 45.2% of data rate against TCP+TLS+HTTP/2 on fast links, and the gap grows with
bandwidth; video bitrate falls up to 9.8%. Reproduced across Chrome, Edge, Firefox and Opera, desktop and mobile, wired and
cellular. Profiling attributes it to receiver-side processing: "out of the 8.7 s consumed by QuicChromiumPacketReader, QUIC spends
3.0 s generating responses such as ACKs", against TCP's kernel-generated, delayed and offloaded ACKs. The load-bearing observation
is that "none of the QUIC implementations we examine uses UDP generic receive offload (GRO)".

Against it, Oku and Iyengar (Fastly, 30 April 2020 — vendor benchmark) measure the same bottlenecks and reach the opposite
conclusion once they are mitigated. Setup: quicly + picotls (AES128-GCM) on Ubuntu 19.10 / kernel 5.3, an Intel Core m3-6Y30
pinned to one core and clocked down to 400 MHz, USB gigabit Ethernet, no hardware offload. Baselines: raw TCP 708 Mbps, TLS 1.3
over TCP 466 Mbps, stock quicly 196 Mbps (40% of TLS/TCP). Then ACK every tenth packet → 240 Mbps; GSO coalescing 10 packets → 348
Mbps; 20 packets → 431 Mbps; packet size 1280 → 1460 bytes → 466 Mbps, parity. With production settings, 464 Mbps at 1460 bytes
(1% faster than TLS/TCP) and 425 Mbps at Chrome's 1350-byte default (8% slower). Fastly's own caveat: "a simple setting and
benchmark... we need to do more testing with more realistic and representative hardware".

Swett (Google, SIGCOMM EPIQ 2020 slides — vendor) is the third point: Google's early QUIC cost 3.5x the CPU of HTTPS/1.1, reduced
to 2x by January 2017; UDP send is 25% of CPU in their DASH workload and over 50% in some environments; `sendmsg` for UDP costs up
to 3.5x the cycles per byte of TCP on Linux. Quantified mitigations: UDP GSO is 7% faster than TCP GSO and hardware offload adds
another 2-3x; UDP GRO improves receive CPU by 35%; a single-STREAM-frame fast path 5%; profile-guided optimisation 15%; reducing
ACKs was "critical (25% reduction) to achieving parity with TCP in quicly benchmarks".

Jaeger et al., "QUIC on the Highway" (IFIP Networking 2023; arXiv 2309.16395) is the neutral referee on dedicated 10G hardware
using the QUIC Interop Runner: goodput spans 90 Mbit/s to 4900 Mbit/s purely by implementation choice, default OS socket buffers
are too small "by at least an order of magnitude", and "QUIC benefits less from NIC offloading and AES-NI while both features
improve the goodput of TCP to around 8000 Mbit/s". The follow-up "QUIC on the Fast Lane" (Computer Communications, May 2024) adds
100 Gbit/s NICs and reports that two of three implementations still did not profit from any segmentation offload as of December
2023. Huang and Zhao, "Accelerating QUIC with AF_XDP" (ICA3PP 2023 / Springer 2024) raise quic-go requests per second by 5-40% and
cut CPU by 5-50% with kernel bypass. LiteQUIC (ACM Multimedia 2024) combines ACK-frequency reduction, GSO and PicoTLS for 1.2x
average bitrate and 93.3% less rebuffering against an already GSO-optimised QUIC baseline.

Read together: the disagreement is not about mechanism — everyone agrees the cost is per-packet syscall and ACK processing — but
about whether it is inherent. Zhang measured deployed stacks that had not enabled GRO; Google, Fastly and the offload literature
measured what happens when they are enabled. The observed range is roughly 0.4x to 1.01x of TLS/TCP throughput per CPU, determined
by GSO/GRO, ACK frequency and packet size.

### 11.3 Stream multiplexing cost

Yu and Benson read the three production schedulers out of HTTP frame traces: Cloudflare sequential (no parallel multiplexing at
all, so HOL-blocking removal cannot apply), Facebook round-robin, Google batched round-robin; the measured effect of round-robin
on Speed Index under added loss was "negligible". Kakhki et al. (IMC '17) named "multiplexing large numbers of small objects" as a
QUIC weakness. No paper measures per-stream memory or CPU as a function of concurrent stream count; see section 11.9.

### 11.4 Datagrams against streams

RFC 9221 motivates DATAGRAM by handshake sharing and a single congestion controller and contains no measurements. Palmer et al.,
"The QUIC Fix for Optimal Video Streaming" (EPIQ '18, December 2018; arXiv 1809.10270 — preprint plus workshop paper, pre-standard
code) built a prototype mixing reliable and unreliable streams and reported it beating both TCP and stock QUIC for video. The
Media over QUIC community reached the opposite engineering conclusion: "Never* use Datagrams" (moq.dev, 17 February 2024) argues
"the fire-and-forget nature of datagrams only works when you need real-time latency; for everything else, there's QUIC streams",
and MoQ uses partial reliability through stream resets instead. Measurement-side: an evaluation of HTTP/3 and WebTransport in live
low-latency video (Springer, 2025) found latency "comparable to WebRTC" on a Unity remote-rendering testbed, and a WebTransport
game-streaming system (ACM ICCVCI 2025) measured datagram control messages adding 1-3 ms, average 1.6 ms. Both are single-testbed
studies.

### 11.5 0-RTT

Langley et al., "The QUIC Transport Protocol: Design and Internet-Scale Deployment" (SIGCOMM 2017) is still the only
Internet-scale number: Google Search latency -3.6% to -8%, YouTube rebuffers -15% to -18%, with QUIC at 35% of Google's egress.
Google's IETF 96 deployment slides (July 2016 — vendor) attribute over half of the latency improvement at median and 95th
percentile to 0-RTT. Kakhki et al. (IMC '17) confirm independently: "in the desktop environment, QUIC outperforms TCP+HTTPS in
nearly every scenario, primarily due to 0-RTT connection establishment and recovering from loss quickly" — while also finding QUIC
"significantly worse than TCP" under packet reordering, weaker gains on phones "due to its reliance on application-layer packet
processing and encryption", and unfairness, with QUIC consuming "approximately twice the bottleneck bandwidth of TCP". Muthuraj et
al., "Replication: Taking a long look at QUIC" (IMC '24, November 2024) re-ran the methodology on Emulab against gQUICv37 and IETF
QUICv1: the 0-RTT and multi-stream advantages replicate, and the reordering penalty largely disappears in QUICv1 because of BBR
and updated loss detection. Counter-datapoint, from the vendor survey table in Yu and Benson: Cloudflare's own 2020 measurement
had HTTP/3 1-4% worse than HTTP/2 across several PoPs, while Google reported +3% throughput and -2% search latency and Facebook
-6% request errors and -20% tail latency.

### 11.6 Migration in the field

Buchet and Pelsser, "An Analysis of QUIC Connection Migration in the Wild" (ACM SIGCOMM CCR, April 2025; arXiv 2410.06066) scanned
12,024,542 IPv4 targets, got 591,848 successful handshakes (4.9%) and only 11,854 successful migrations — 2% of targets, across
474 ASes (7.7% of the ASes with handshakes). "Support for connection migration is not yet present for all big QUIC providers."
Caveat: the probe changes connection ID and checks the response, not that the application still works, so it bounds advertised
support rather than useful migration. QUIC-HOA (IEEE, 2025) reports that "QUIC connection migration may fail with a probability as
high as 76% during a hard handover" between Wi-Fi networks, and that cross-layer handover initiation cuts average migration time
by up to 98%. mQUIC (IEEE Communications Magazine, 2023) reports significant gains over reconnecting, on a Wi-Fi plus commercial
5G testbed. Summary: migration works on soft handovers when both ends implement it, frequently fails on hard handovers, and
server-side deployment is sparse.

### 11.7 MQTT, AMQP and pub/sub over QUIC

Kumar and Dezfouli, "Implementation and analysis of QUIC for MQTT" (Computer Networks 150:28-45, 2019; arXiv 1810.07730) is the
canonical academic result. Testbed: Raspberry Pi 3B endpoints, lossy links. Numbers: message delivery latency -55.6% against MQTT
over TCP, 56.2% fewer packets during connection establishment, and, by eliminating half-open connections, up to -83.2% processor
and -50.3% memory. Caveat: gQUIC-era code, small payloads, Pi-class hardware, and the latency figure is dominated by handshake and
loss recovery rather than steady-state messaging.

Jeddou et al., "Delay and Energy Consumption of MQTT over QUIC" (Sensors 22(10):3694, May 2022): Raspberry Pi testbed, 100 Mbps
Ethernet publisher-to-broker, `tc`-emulated Wi-Fi and cellular. Error-free Wi-Fi baseline delay 25 ms with a 5th-95th percentile
spread of about 3 ms; higher MQTT QoS lowers delay as the error rate rises, most visibly at 5% loss. Conclusion: QUIC "does not
only yield a notable decrease in the delay and its variability... but it does not hinder the energy consumption".

EMQ/EMQX, "EMQ, Intel and SJTU explore MQTT over QUIC" (October 2023 — vendor benchmark): EMQX 5.0 with `emqtt_bench` on an AWS
c7g.xlarge, injected random loss at 0/25/50/75%. At 0% loss the two transports are similar; QUIC pulls ahead as loss rises; in a
NanoMQ bridging scenario MQTT over TCP fluctuated between 3 and 300 packets/s while MQTT over QUIC held 260-280 packets/s. EMQX's
documentation additionally claims lower CPU and memory during mass reconnect storms but higher bandwidth on reconnect. No
confidence intervals, no version matrix, artificially high loss rates.

AMQP over QUIC: Fernandez, Rafique et al. published "Use of QUIC for AMQP in IoT networks" (Computer Networks, February 2023) and
a companion in J. King Saud Univ. CIS (March 2023). Setup: AMQP 1.0 in Go, benchmarked in **ns-3 simulation**, not on a testbed,
over emulated Wi-Fi, 4G/LTE and satellite. Numbers: total communication time -22%, startup latency -62%, 7x throughput, -31%
energy; at 15% loss TCP degraded 20/16/36% against QUIC's 4/8/9%. The same papers report RTT being 71% higher for QUIC while
claiming the latency wins, which is reason for caution. No Kafka-over-QUIC evaluation exists in the peer-reviewed literature.

### 11.8 ACK overhead and datacentre QUIC

draft-ietf-quic-ack-frequency §2 states the mechanism: "Sending UDP datagrams is very CPU intensive on some platforms... this
reduction can be critical for high packet rate connections". Custura et al., "Reducing the acknowledgement frequency in IETF QUIC"
(Int. J. Satellite Communications and Networking 41(1), online October 2022) evaluated ACK policies across three IETF QUIC
implementations over cellular, terrestrial and satellite paths and found performance maintained with fewer ACKs, with lower
return-path volume and endpoint processing; an immediate ACK on detected reordering does not add significant overhead. The
counterweight is Zhang et al., who find that naive ACK reduction degrades throughput under poor conditions. For east-west traffic
the only dedicated design is DCQUIC (2021): against DCTCP with TLS 1.3, handshake completion about -73% and 10 KB object
completion 63-68% faster — a single simulation-and-testbed paper with no independent replication.

### 11.9 Where the literature is thin or absent

Every measurement above is a web, video, or IoT-telemetry workload. For a long-lived bidirectional messaging system there is no
published measurement, academic or vendor, on:

- Long-lived bidirectional sessions. All page-load and object-download studies measure connections lasting seconds. Nothing
  measures idle-timeout and keep-alive cost, connection-ID rotation overhead, congestion-controller behaviour after long idle
  periods, or memory growth over hours.
- Many-stream workloads. No study reports throughput, latency or per-stream memory as a function of concurrent stream count (10
  against 10^3 against 10^5). MAX_STREAMS and flow-control credit exhaustion behaviour is unmeasured.
- Server-initiated streams as a fan-out primitive. HTTP/2 Server Push measurements exist and were negative; QUIC server-initiated
  unidirectional streams are unmeasured. Media over QUIC is the closest work and is still design-stage.
- QUIC DATAGRAM in production messaging. RFC 9221 has no measurement section; the evidence is a 2018 pre-standard prototype, a
  game-streaming testbed, and MoQ's explicit rejection of datagrams.
- Pub/sub fan-out. Kumar/Dezfouli and EMQX both measure a single publisher-to-broker path. Nobody has measured broker CPU and
  memory for 10^5-10^6 subscriber connections against a TCP baseline.
- Kafka-style log replication over QUIC. No published work.

## 12. What QUIC does not give a messaging system

Each item is a property the standards state or decline to provide, not a criticism.

1. **No application-level acknowledgement.** A QUIC ACK certifies that packet protection was removed and the frames were processed
   by the transport (RFC 9000 §13.1). For datagrams it is spelled out: acknowledgement "does not guarantee that the application on
   the receiver successfully processed the data. Thus, this signal cannot replace application-layer signals that indicate
   successful processing" (RFC 9221 §5.2). For streams, the sending side's terminal state "Data Recvd" means all data was
   acknowledged, while the receiving side's "Data Read" — the state that means the application consumed it — is one "the sender
   cannot observe" (RFC 9000 §3.1, §3.2).
2. **No ordering across streams.** "QUIC does not provide any means of ensuring ordering between bytes on different streams" (RFC
   9000 §2); "There is no guarantee of transmission, reception, or delivery order across streams" (RFC 9308 §4).
3. **No stream priority on the wire.** "QUIC does not provide a mechanism for exchanging prioritization information" (RFC 9000
   §2.3); "Stream prioritization is not exposed to either the network or the receiver" (RFC 9308 §4.2). HTTP/3 has none either
   (RFC 9114 §A.2.1); RFC 9218 is a separate, HTTP-specific document.
4. **No message boundaries inside a stream.** "Streams are an ordered byte-stream abstraction with no other structure visible to
   QUIC. STREAM frame boundaries are not expected to be preserved" (RFC 9000 §2.2). No active draft adds them (section 8.5).
5. **No persistence, no durability, no redelivery.** Nothing in RFC 9000 stores data. A CONNECTION_CLOSE "causes all streams to
   immediately become closed; open streams can be assumed to be implicitly reset" (RFC 9000 §10.2), and unacknowledged stream data
   is simply lost.
6. **No resumption of stream state.** 0-RTT resumes the TLS session and remembered transport parameters (RFC 9001 §4.6, RFC 9000
   §7.4.1); "QUIC itself does not depend on any state being retained when resuming a connection unless 0-RTT is also used" (RFC
   9001 §4.5). Stream IDs, offsets and unacknowledged data do not survive.
7. **No graceful connection termination.** "QUIC does not provide any mechanism for graceful connection termination; applications
   using QUIC can define their own graceful termination process" (RFC 9308 §10). HTTP/3's GOAWAY is that mechanism for HTTP/3 only
   (RFC 9114 §5.2).
8. **No way to learn the peer's application read progress.** Section 2.4's list of stream operations has no such query, and RFC
   9000 §3.2 states the sender cannot observe it. Arriving MAX_STREAM_DATA credit is the only proxy, and it is only a proxy: the
   specification merely says a receiver "could determine the flow control offset to be advertised based on the current offset of
   data consumed on that stream" (RFC 9000 §4.1) — it is free to advertise on any other basis.
9. **No server-initiated migration.** "Clients are responsible for initiating all migrations" (RFC 9000 §9); "Migrating a
   connection to a new server address mid-connection is not supported by the version of QUIC specified in this document" (RFC 9000
   §9.6). The `preferred_address` parameter is a one-shot offer made during the handshake, acted on by the client.
10. **No datagram larger than one packet, and no datagram flow control.** "DATAGRAM frames cannot be fragmented" (RFC 9221 §5) and
    "do not provide any explicit flow control signaling and do not contribute to any per-flow or connection-wide data limit" (RFC
    9221 §5.3); a receiver under pressure simply drops them.
11. **No multicast or one-to-many delivery.** A QUIC connection is "shared state between a client and a server" (RFC 9000 §5). The
    only multicast work, `draft-jholland-quic-multicast-09`, is an individual submission, not a working-group document.
12. **No post-handshake authentication and no per-message identity.** "A server MUST NOT use post-handshake client authentication"
    (RFC 9001 §4.4). One identity per connection, fixed at the handshake.
13. **No per-stream network treatment.** Stream identity is inside the encryption boundary, so "stream multiplexing is not
    intended to be used for differentiating streams in terms of network treatment"; differentiated traffic needs separate
    connections and separate DSCPs (RFC 9308 §4.1, §12).
14. **No standardised keep-alive policy.** PING exists (RFC 9000 §19.2) but who sends it and how often is left to the application
    protocol (RFC 9000 §10.1.2, RFC 9308 §3.2).
15. **No replay protection for 0-RTT application data.** "Ultimately, the responsibility for managing the risks of replay attacks
    with 0-RTT lies with an application protocol" (RFC 9001 §9.2), and "once a server accepts 0-RTT data, there is no means of
    selectively discarding data that is received" (RFC 9308 §3.1).
16. **No visibility of connection end to the network.** "QUIC does not expose the end of a connection; the only indication to
    on-path devices that a flow has ended is that packets are no longer observed" (RFC 9312 §3.6), which is why middlebox state
    and keep-alives are an application concern.
17. **No standard API.** RFC 9000 §2.4 and §5.3 define operations, not an interface, so every behaviour left to "the
    implementation" in sections 2 through 5 is a portability hazard; see section 13.

## 13. Implementations and their divergences

All rows read from the project's own source or documentation on 2026-09-10 (default branch). "—" means not implemented;
"unverified" means the documentation is silent.

| Implementation (release) | DATAGRAM | Multipath | ack-frequency | QUIC-LB | 0-RTT | Migration | Congestion control (default) | Receive flow control |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| quinn 0.11.11 / quinn-proto 0.11.17 (2026-06-22 / 2026-08-17) | yes, on by default; recv buffer 1.25 MB, send buffer 1 MiB | — (issue #224 open) | yes both directions; peer control opt-in (`ack_frequency_config`, default `None`) | pluggable `ConnectionIdGenerator`, not the draft | client `enable_early_data`, server `max_early_data_size = u32::MAX`; app calls `into_0rtt()` | client rebinding; server `migration: true` by default; `preferred_address_v4/v6` off | NewReno, CUBIC, BBR (CUBIC) | fixed: stream 1.25 MB, connection `VarInt::MAX`, send 10 MB; no auto-tuning |
| cloudflare quiche 0.29.3 (2026-07-14) | opt-in `enable_dgram`, default off | — | — | app's job | opt-in `enable_early_data()`, off by default both roles | `migrate()`, `probe_path()`, `send_on_path` | Reno, CUBIC, `Bbr2Gcongestion` (CUBIC) | auto-tuning: doubles if consumed within 2xRTT, caps 16 MB stream / 24 MB connection |
| s2n-quic v1.88.0 (2026-08-21) | provider-based, default provider `Disabled` | — | — | — | reported unsupported in interop (2026-09-09) although crypto plumbing exists | client migration + path validation; server `preferred_address` unverified | CUBIC, BBR (CUBIC) | unverified |
| msquic v2.6.1 (2026-08-28) | yes; `DatagramReceiveEnabled` default FALSE; per-datagram send-state events | — (issue #3543 open) | yes (draft) | yes (`draft-ietf-quic-load-balancers`) | `ServerResumptionLevel` default `QUIC_SERVER_NO_RESUME`; opt-in both sides | `MigrationEnabled` TRUE, "requires a cooperative load-balancer" | CUBIC, BBR behind `QUIC_API_ENABLE_PREVIEW_FEATURES` (CUBIC) | conn 16 MiB, stream recv window 64 KiB, stream recv buffer 4 KiB |
| ngtcp2 v1.25.0 (2026-07-26) | yes; `writev_datagram` with `ack_datagram` / `lost_datagram` callbacks | — | — (own `ack_thresh`, default 2) | — | yes, ticket + transport-parameter file | `select_preferred_addr` callback, ignored if unset | Reno, CUBIC, BBR (CUBIC) | unverified |
| picoquic 1.1.52.0 (2026-09-10) | yes, per-path, with acked/lost/spurious callbacks | yes, draft-ietf-quic-multipath plus "simple multipath" | yes (draft-04) | yes (`picoquic_lb.c`) | yes | yes, `probe_new_path` / `abandon_path` | NewReno, CUBIC, dCUBIC, FastCC, BBR, BBRv1, Prague, C4 (NewReno; demo images pick BBR) | not documented |
| mvfst v2026.09.07.00 (2026-09-07) | yes, `DatagramConfig` | — in the open-source tree | yes, `minAckDelay` + `ackFrequencyConfig` | pluggable, Katran-integrated; not the draft | yes, with customisable 0-RTT path validation | yes | CUBIC, NewReno, BBR, BBR2, Copa (CUBIC) | `autotuneReceiveConnFlowControl` default false |
| neqo v0.31.1 (2026-09-01) | yes, bounded outgoing queue | — | legacy: ACK_FREQUENCY 0xaf and `MinAckDelay` 0xff02de1a from the older ack-delay draft; update issue #1267 open | — | yes (Firefox's production client) | `disable_migration` false by default | NewReno, CUBIC with Classic/HyStart/SEARCH slow start (CUBIC + Classic) | not documented; PMTUD off by default |
| Chromium / Google QUICHE (HEAD 2026-09-09) | yes, `QuicDatagramQueue` (MASQUE, WebTransport) | — | yes, `min_ack_delay` draft-10 codepoints behind connection options | yes, full `quiche/quic/load_balancer/` | yes | yes | CUBIC-bytes, Reno-bytes, BBR, BBRv2, BBRv3, PragueCubic; flag-driven default, otherwise CUBIC | initial 16 KB, auto-tuning, caps 16 MB stream / 24 MB session |

### 13.1 How stream termination reaches the application

This is where the APIs diverge semantically, not just cosmetically, and it is exactly the signal a messaging system wants.

- **quinn** folds two things into one future: `SendStream::stopped()` yields `Ok(Some(code))` on STOP_SENDING and `Ok(None)` once
  the local side called `finish()` and the peer acknowledged all stream data. The documentation warns that this is not
  application-level reception: "relying on `stopped` to know when the peer has read a stream to completion may introduce more
  latency than using an application-level response". Dropping a `SendStream` implicitly finishes it. 0-RTT adds
  `StoppedError::ZeroRttRejected`.
- **s2n-quic** splits them: `finish()` returns immediately, `close()` resolves only once outstanding data is acknowledged. A peer
  reset arrives as an error on the next operation (`StreamError::StreamReset`), not as an event.
- **cloudflare quiche** is poll-based: `stream_finished(id)`, `readable()`/`writable()` iterators, and `Error::StreamStopped` from
  `stream_send()`. There is no acknowledgement notification at stream granularity.
- **msquic** is event-driven and the most explicit: `PEER_SEND_ABORTED` (RESET_STREAM), `PEER_RECEIVE_ABORTED` (STOP_SENDING),
  `SEND_SHUTDOWN_COMPLETE`, `SHUTDOWN_COMPLETE`, plus `IDEAL_SEND_BUFFER_SIZE` — four distinct events where quinn has one future.
- **mvfst** is the finest-grained: `registerByteEventCallback(ByteEvent::Type::{ACK,TX}, streamId, offset, cb)` reports
  acknowledgement or transmission of an arbitrary byte offset.
- **ngtcp2** uses callbacks: `acked_stream_data_offset` (largest gap-free acknowledged offset), `stream_reset`,
  `stream_stop_sending`, `stream_close2` (carrying both directions' error codes since 1.25.0).
- **picoquic** delivers `stream_fin`, `stream_reset` and `stop_sending` through one callback enum; per-stream acknowledgement is
  not in that enum, although datagrams get `datagram_acked`.
- **neqo** emits `ConnectionEvent::{RecvStreamReset, SendStreamStopSending, SendStreamComplete}`, the last meaning the peer
  acknowledged everything sent on the stream.
- **Google QUICHE** uses virtual overrides on `QuicStream`: `OnStreamReset`, `OnStopSending`, `OnWriteSideInDataRecvdState()`,
  `IsWaitingForAcks()`.

Consequence: quinn, s2n-quic (`close()`), neqo (`SendStreamComplete`), msquic (`SEND_SHUTDOWN_COMPLETE`), ngtcp2
(`acked_stream_data_offset`) and mvfst (byte events) can tell an application that data was acknowledged; quiche and picoquic
cannot at stream granularity. None of them can tell it that the peer *read* the data, which matches RFC 9000 §3.2 — an
application-level acknowledgement is required in every case, and quinn's own documentation says so.

### 13.2 Interop

The QUIC Interop Runner builds each implementation as a container, drives it through an ns-3 based simulator and verifies both
file contents and the recorded pcap; a case an implementation cannot run exits 127 and is recorded as "unsupported" rather than
failed. Test cases include handshake, transfer, longrtt, chacha20, multiplexing, retry, resumption, zerortt, http3, blackhole,
keyupdate, ecn, amplificationlimit, handshakeloss, transferloss, handshakecorruption, transfercorruption, ipv6, v2, rebind-port,
rebind-addr and connectionmigration.

In the run of 2026-09-09 (16 servers, 15 clients, QUIC v1), gaps declared against all peers included: mvfst client — chacha20,
retry, resumption, keyupdate, ecn, v2; quiche server — ecn, v2, connectionmigration; quinn server — v2, connectionmigration;
s2n-quic — zerortt and v2 in both roles; msquic — http3 and ecn in both roles. Actual failures, as opposed to gaps, cluster on
rebind-addr, rebind-port, handshakecorruption, handshakeloss and connectionmigration: the quiche server failed rebind-port and
rebind-addr against 14 of 15 clients, and the ngtcp2 and s2n-quic servers failed connectionmigration against 14. Goodput on the 10
Mbps simulated link saturates for everyone (neqo 9528, msquic 9501, quinn 9469, picoquic 9413, mvfst 9375, ngtcp2 9282, s2n-quic
9149 kbps), so it separates nothing; the crosstraffic measurement spreads much wider (picoquic 7720 against neqo 3733 kbps) and
reflects congestion-controller aggressiveness. These are simulator numbers from one daily run, not a benchmark.

### 13.3 What this means for portability

[inference] Three defaults differ enough to change application behaviour on a port: datagram support (on in quinn and neqo, off in
quiche, s2n-quic and msquic), receive-window auto-tuning (quiche and QUICHE tune, quinn does not, mvfst has it off by default),
and the default congestion controller (CUBIC almost everywhere, NewReno in picoquic, flag-driven BBR variants in QUICHE). Section
3.2's obligations on the receiver are met by different strategies, so throughput on a high-BDP path is an implementation property,
not a QUIC property — which is exactly what Jaeger et al. measured (section 11.2).

### 13.4 Unverified

s2n-quic server `preferred_address` support and flow-control auto-tuning; ngtcp2 and picoquic flow-control auto-tuning; s2n-quic
application-level 0-RTT (code is present, the interop runner reports unsupported); the exact ack-frequency draft revision
implemented by quinn, msquic and mvfst — only picoquic (draft-04) and Google QUICHE (draft-10) name one.

## 14. Sources

Standards and specifications, all retrieved 2026-09-10.

1. RFC 8999, "Version-Independent Properties of QUIC", May 2021. https://www.rfc-editor.org/rfc/rfc8999 — invariants, connection ID opacity, version negotiation packet (sections 1, 10).
2. RFC 9000, "QUIC: A UDP-Based Multiplexed and Secure Transport", May 2021. https://www.rfc-editor.org/rfc/rfc9000 — streams, flow control, connections, migration, termination, acknowledgements, transport parameters, security considerations (sections 1-5, 9, 12).
3. RFC 9001, "Using TLS to Secure QUIC", May 2021. https://www.rfc-editor.org/rfc/rfc9001 — TLS binding, ALPN, peer authentication, 0-RTT and replay, key update (sections 1, 5, 9).
4. RFC 9002, "QUIC Loss Detection and Congestion Control", May 2021. https://www.rfc-editor.org/rfc/rfc9002 — RTT estimation, PTO, NewReno, persistent congestion, pacing, application-limited senders (section 3).
5. RFC 9114, "HTTP/3", June 2022. https://www.rfc-editor.org/rfc/rfc9114 — stream mapping, control streams, GOAWAY, error codes (sections 2.8, 6).
6. RFC 9204, "QPACK: Field Compression for HTTP/3", June 2022. https://www.rfc-editor.org/rfc/rfc9204 — encoder and decoder stream types (section 6.2).
7. RFC 9218, "Extensible Prioritization Scheme for HTTP", June 2022. https://www.rfc-editor.org/rfc/rfc9218 — priority as a separate, application-level scheme (sections 2.3, 6.3, 12).
8. RFC 9220, "Bootstrapping WebSockets with HTTP/3", June 2022. https://www.rfc-editor.org/rfc/rfc9220 — extended CONNECT, SETTINGS_ENABLE_CONNECT_PROTOCOL (section 6.5).
9. RFC 8441, "Bootstrapping WebSockets with HTTP/2", September 2018. https://www.rfc-editor.org/rfc/rfc8441 — the HTTP/2 original of extended CONNECT (section 6.5).
10. RFC 9221, "An Unreliable Datagram Extension to QUIC", March 2022. https://www.rfc-editor.org/rfc/rfc9221 — DATAGRAM frames, transport parameter, flow control, acknowledgement semantics (sections 4, 12).
11. RFC 9297, "HTTP Datagrams and the Capsule Protocol", August 2022. https://www.rfc-editor.org/rfc/rfc9297 — quarter stream IDs, SETTINGS_H3_DATAGRAM, capsules (sections 4.8, 6, 7).
12. RFC 9298, "Proxying UDP in HTTP", August 2022. https://www.rfc-editor.org/rfc/rfc9298 — CONNECT-UDP, context IDs, proxy guarantees (section 7.1).
13. RFC 9484, "Proxying IP in HTTP", October 2023. https://www.rfc-editor.org/rfc/rfc9484 — CONNECT-IP capsules and overhead (section 7.2).
14. RFC 9931, "Security Considerations for Optimistic Protocol Transitions in HTTP/1.1", March 2026. https://www.rfc-editor.org/rfc/rfc9931 — update to RFC 9298 §6.3 (section 7.1).
15. RFC 9308, "Applicability of the QUIC Transport Protocol", September 2022. https://www.rfc-editor.org/rfc/rfc9308 — stream design guidance, flow-control deadlocks, keep-alive, stream limits, graceful close, DSCP (sections 2, 3, 5, 10, 12).
16. RFC 9312, "Manageability of the QUIC Transport Protocol", September 2022. https://www.rfc-editor.org/rfc/rfc9312 — wire image, connection ID and rebinding, invisibility of connection end (sections 10.2, 12).
17. RFC 9368, "Compatible Version Negotiation for QUIC", May 2023. https://www.rfc-editor.org/rfc/rfc9368 — compatible and incompatible negotiation, version information (sections 1.4, 10.3).
18. RFC 9369, "QUIC Version 2", May 2023. https://www.rfc-editor.org/rfc/rfc9369 — what version 2 changes and does not change (sections 1.4, 10.3).
19. RFC 9538, "CDNI Delegation Using ACME", February 2024. https://www.rfc-editor.org/rfc/rfc9538 — checked to confirm it is not the ACK-frequency document (section 8.3).

Internet-Drafts, state as of 2026-09-10 from the IETF datatracker.

20. draft-ietf-webtrans-http3-16, "WebTransport over HTTP/3", 6 July 2026, WG Last Call. https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-16.txt — sessions, stream types, capsules, flow control (section 6.6).
21. draft-ietf-webtrans-http2-15, "WebTransport over HTTP/2", 6 July 2026, WG Last Call. https://www.ietf.org/archive/id/draft-ietf-webtrans-http2-15.txt — the fallback and what it loses (section 6.7).
22. draft-ietf-webtrans-http3-13 and -02 (change logs), for the removal of SETTINGS_WEBTRANSPORT_MAX_SESSIONS and the rename of CLOSE_WEBTRANSPORT_SESSION. https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/ (section 6.6).
23. draft-ietf-masque-quic-proxy-09, "QUIC-Aware Proxying Using HTTP", 6 July 2026, WG Last Call. https://datatracker.ietf.org/doc/draft-ietf-masque-quic-proxy/ (section 7.3).
24. draft-ietf-quic-multipath-21, "Managing multiple paths for a QUIC connection", 17 March 2026, RFC Editor queue. https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/ — path IDs, frames, scheduling out of scope (section 8.1).
25. draft-ietf-quic-load-balancers-21, "QUIC-LB", 27 August 2025, expired 28 February 2026. https://datatracker.ietf.org/doc/draft-ietf-quic-load-balancers/ (section 8.2).
26. draft-ietf-quic-retry-offload-00, 25 May 2022, expired and parked. https://datatracker.ietf.org/doc/draft-ietf-quic-retry-offload/ (section 8.2).
27. draft-ietf-quic-ack-frequency-14, "QUIC Acknowledgment Frequency", 5 February 2026, expired 9 August 2026, WG consensus awaiting write-up. https://datatracker.ietf.org/doc/draft-ietf-quic-ack-frequency/ (sections 3.4, 8.3, 11.8).
28. draft-ietf-quic-reliable-stream-reset-11, "QUIC Stream Resets with Partial Delivery", 6 September 2026, approved. https://datatracker.ietf.org/doc/draft-ietf-quic-reliable-stream-reset/ (section 8.4).
29. Expired partial-reliability work, none adopted: draft-tiesel-quic-unreliable-streams-01 (expired 3 May 2018), draft-tiesel-quic-unreliable-http-00 (expired 9 March 2018), draft-lubashev-quic-partial-reliability-03 (expired 1 December 2018), all at https://datatracker.ietf.org/ (section 8.5).
30. draft-ietf-quic-qmux-02, 6 July 2026. https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/ — QUIC semantics over TCP/TLS and its costs (section 8.6).
31. draft-ietf-quic-address-discovery-01, 15 August 2026. https://datatracker.ietf.org/doc/draft-ietf-quic-address-discovery/ (section 8.6).
32. draft-ietf-quic-extended-key-update-03, 6 July 2026, and draft-ietf-quic-receive-ts-03, 20 July 2026. https://datatracker.ietf.org/wg/quic/documents/ (section 8.6).
33. qlog drafts: draft-ietf-quic-qlog-main-schema-14, draft-ietf-quic-qlog-quic-events-13, draft-ietf-quic-qlog-h3-events-13, all 6 July 2026. https://datatracker.ietf.org/wg/quic/documents/ (section 8.6).
34. Individual submissions: draft-bruynooghe-n0-quic-nat-traversal-00 (6 July 2026), draft-seemann-masque-connect-udp-rendezvous-00 (16 August 2026), draft-jholland-quic-multicast-09 (6 July 2026), draft-zheng-quic-fec-extension-02 (16 March 2026), draft-gage-quic-pathmgmt-06 (31 May 2026). https://datatracker.ietf.org/ (sections 8.6, 12).
35. Other MASQUE drafts: draft-ietf-masque-connect-udp-listen-16 (24 August 2026), draft-ietf-masque-connect-ethernet-14 (18 August 2026), draft-ietf-masque-connect-ip-dns-06 (12 April 2026), draft-ietf-masque-connect-udp-ecn-dscp-02 (22 July 2026), draft-ietf-masque-http-datagram-compression-01 (6 July 2026). https://datatracker.ietf.org/wg/masque/documents/ (section 7.3).
36. IETF QUIC and MASQUE working group document lists. https://datatracker.ietf.org/wg/quic/documents/ and https://datatracker.ietf.org/wg/masque/documents/, retrieved 2026-09-10 (sections 7, 8).

Browser and web platform.

37. W3C WebTransport, Editor's Draft, 8 September 2026, revision 0e9cdc22903866480d6c74503b1571f58b8ed7a6. https://w3c.github.io/webtransport/ — the browser API surface (section 6.8).
38. MDN browser-compat-data, `api/WebTransport.json` and `api/WebTransportDatagramDuplexStream.json`, main branch, retrieved 2026-09-10. https://raw.githubusercontent.com/mdn/browser-compat-data/main/api/WebTransport.json (section 6.8).
39. MDN Web Docs, "WebTransport", Baseline since March 2026, retrieved 2026-09-10. https://developer.mozilla.org/en-US/docs/Web/API/WebTransport (section 6.8).
40. WebKit, "WebKit Features for Safari 26.4", 2026-03-24. https://webkit.org/blog/17862/webkit-features-for-safari-26-4/ — WebTransport shipping in Safari (section 6.8).
41. quic-go documentation, "WebTransport", undated third-party page, retrieved 2026-09-10. https://quic-go.net/docs/webtransport/ — claim that the HTTP/2 fallback is implemented by neither Chrome nor Firefox (section 6.8).

Literature. Vendor material and preprints are marked in the entry.

42. Marx, Herbots, Lamotte, Quax, "Same Standards, Different Decisions: A Study of QUIC and HTTP/3 Implementation Diversity", ACM EPIQ '20, 10 August 2020. https://dl.acm.org/doi/10.1145/3405796.3405828 (section 11.1).
43. Marx, "Head-of-Line Blocking in QUIC and HTTP/3: The Details", Web Performance Calendar, 3 December 2020 (non-peer-reviewed analysis). https://calendar.perfplanet.com/2020/head-of-line-blocking-in-quic-and-http-3-the-details/ (section 11.1).
44. Yu, Benson, "Dissecting Performance of Production QUIC", ACM WWW '21, April 2021. https://dl.acm.org/doi/10.1145/3442381.3450103 (sections 11.1, 11.3, 11.5).
45. Trevisan, Giordano, Drago, Safari Khatouni, "Measuring HTTP/3: Adoption and Performance", arXiv 2102.12358, 24 February 2021 (preprint; journal version Computer Communications 2022). https://arxiv.org/abs/2102.12358 (section 11.1).
46. Zhang, Jin, He, Hassan, Mao, Qian, Zhang, "QUIC is not Quick Enough over Fast Internet", arXiv 2310.09423, 13 October 2023; published ACM WWW '24, 13 May 2024. https://arxiv.org/abs/2310.09423 (sections 11.2, 11.8).
47. Oku, Iyengar (Fastly), "Measuring QUIC vs TCP computational efficiency", 30 April 2020 — vendor benchmark. https://www.fastly.com/blog/measuring-quic-vs-tcp-computational-efficiency (section 11.2).
48. Swett (Google), "QUIC CPU Performance", SIGCOMM EPIQ 2020 slides, August 2020 — vendor. https://conferences.sigcomm.org/sigcomm/2020/files/slides/epiq/0%20QUIC%20and%20HTTP_3%20CPU%20Performance.pdf (section 11.2).
49. Jaeger, Zirngibl, Kempf, Ploch, Carle, "QUIC on the Highway: Evaluating Performance on High-rate Links", IFIP Networking 2023; arXiv 2309.16395. https://arxiv.org/abs/2309.16395 (sections 11.2, 13.3).
50. Kempf, Jaeger et al., "QUIC on the Fast Lane: Extending Performance Evaluations on High-rate Links", Computer Communications, online 11 May 2024. https://www.sciencedirect.com/science/article/pii/S014036642400166X (section 11.2).
51. Huang, Zhao, "Accelerating QUIC with AF_XDP", ICA3PP 2023 / LNCS 14489, Springer 2024. https://link.springer.com/chapter/10.1007/978-981-97-0798-0_6 (section 11.2).
52. "LiteQUIC: Improving QoE of Video Streams by Reducing CPU Overhead of QUIC", ACM Multimedia 2024. https://dl.acm.org/doi/10.1145/3664647.3681670 (section 11.2).
53. Palmer, Krüger, Chandrasekaran, Feldmann, "The QUIC Fix for Optimal Video Streaming", ACM EPIQ '18, 4 December 2018; preprint arXiv:1809.10270. https://dl.acm.org/doi/10.1145/3284850.3284857 (section 11.4).
54. Media over QUIC, "Never* use Datagrams", moq.dev, 17 February 2024 (project blog). https://moq.dev/blog/never-use-datagrams/ (sections 11.4, 11.9).
55. "An Evaluation of HTTP/3 and WebTransport over QUIC in Live Low Latency Video Streaming", Springer LNCS, 2025. https://link.springer.com/chapter/10.1007/978-981-96-4288-5_23 (section 11.4).
56. "A WebTransport-based System for Real-Time Game Streaming", ACM ICCVCI 2025, 22 August 2025. https://dl.acm.org/doi/10.1145/3744725.3744726 (section 11.4).
57. Langley et al., "The QUIC Transport Protocol: Design and Internet-Scale Deployment", ACM SIGCOMM 2017. https://dl.acm.org/doi/10.1145/3098822.3098842 (section 11.5).
58. Swett (Google), "QUIC Deployment Experience @Google", IETF 96 slides, July 2016 — vendor. https://www.ietf.org/proceedings/96/slides/slides-96-quic-3.pdf (section 11.5).
59. Kakhki, Jero, Choffnes, Nita-Rotaru, Mislove, "Taking a Long Look at QUIC", ACM IMC '17, November 2017. https://dl.acm.org/doi/10.1145/3131365.3131368 (sections 11.3, 11.5).
60. Muthuraj, Eghbal, Lu, "Replication: Taking a long look at QUIC", ACM IMC '24, 4 November 2024. https://dl.acm.org/doi/10.1145/3646547.3688453 (section 11.5).
61. Buchet, Pelsser, "An Analysis of QUIC Connection Migration in the Wild", ACM SIGCOMM CCR, 10 April 2025; preprint arXiv:2410.06066. https://dl.acm.org/doi/10.1145/3727063.3727066 (section 11.6).
62. "QUIC-HOA: A Cross-layer, Handover-aware Design for QUIC Connection Migration", IEEE, 2025. https://ieeexplore.ieee.org/document/10900944/ (section 11.6).
63. "mQUIC: Use of QUIC for Handover Support with Connection Migration in Wireless/Mobile Networks", IEEE Communications Magazine, 2023. https://ieeexplore.ieee.org/document/10268842/ (section 11.6).
64. Kumar, Dezfouli, "Implementation and analysis of QUIC for MQTT", Computer Networks 150:28-45, 2019; preprint arXiv:1810.07730. https://www.sciencedirect.com/science/article/abs/pii/S1389128618310776 (section 11.7).
65. Jeddou, Fernández, Diez, Baina, Abdallah, Agüero, "Delay and Energy Consumption of MQTT over QUIC", Sensors 22(10):3694, 12 May 2022. https://www.mdpi.com/1424-8220/22/10/3694 (section 11.7).
66. EMQ, "EMQ, Intel and SJTU Explore MQTT over QUIC Together", October 2023 — vendor benchmark. https://github.com/emqx/blog/blob/main/en/202310/emq-intel-and-sjtu-explore-mqtt-over-quic-together.md, with https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/introduction.html (section 11.7).
67. Rafique, Fernández et al., "Use of QUIC for AMQP in IoT networks", Computer Networks, 17 February 2023, and the companion in J. King Saud Univ. CIS, 2 March 2023 — ns-3 simulation, not a testbed. https://www.sciencedirect.com/science/article/abs/pii/S1389128623000853 (section 11.7).
68. Custura, Secchi, Fairhurst et al., "Reducing the acknowledgement frequency in IETF QUIC", Int. J. Satellite Communications and Networking 41(1), online October 2022. https://onlinelibrary.wiley.com/doi/10.1002/sat.1466 (section 11.8).
69. "DCQUIC: Flexible and Reliable Software-defined Data Center Transport", 2021 — simulation and testbed, no independent replication. https://www.researchgate.net/publication/350277292 (section 11.8).

Implementations, all read from the default branch on 2026-09-10.

70. quinn: README, `quinn-proto/src/config/transport.rs`, `config/mod.rs`, `crypto/rustls.rs`, `connection/ack_frequency.rs`, `quinn/src/send_stream.rs`; releases quinn 0.11.11 (2026-06-22), quinn-proto 0.11.17 (2026-08-17); issue #224 (multipath, open). https://github.com/quinn-rs/quinn (section 13).
71. cloudflare quiche: README, `quiche/src/lib.rs`, `recovery/mod.rs`, `flowcontrol.rs`, repository tree; release 0.29.3 (2026-07-14). https://github.com/cloudflare/quiche (section 13).
72. s2n-quic: README, `provider/congestion_controller.rs`, `provider/datagram.rs`, `stream/send.rs`, `s2n-quic-core/src/stream/error.rs`; release v1.88.0 (2026-08-21). https://github.com/aws/s2n-quic (section 13).
73. msquic: README, `docs/Settings.md`, `src/inc/msquic.h`; release v2.6.1 (2026-08-28); issue #3543 (multipath, open). https://github.com/microsoft/msquic (section 13).
74. ngtcp2: `lib/includes/ngtcp2/ngtcp2.h`, README; release v1.25.0 (2026-07-26). https://github.com/ngtcp2/ngtcp2 (section 13).
75. picoquic: README, `picoquic/picoquic.h` (version 1.1.52.0), `register_all_cc_algorithms.c`, `picoquic_lb.h`. https://github.com/private-octopus/picoquic (section 13).
76. mvfst: README, `quic/state/TransportSettings.h`, `quic/api/QuicSocketLite.h`, `QuicAckFrequencyFunctions.h`; tag v2026.09.07.00. https://github.com/facebook/mvfst (section 13).
77. neqo: README, `neqo-transport/src/connection/params.rs`, `cc/mod.rs`, `frame.rs`, `events.rs`; release v0.31.1 (2026-09-01); issues #1656 (closed) and #1267 (open). https://github.com/mozilla/neqo (section 13).
78. Google QUICHE: `quiche/quic/core/quic_connection.cc`, `quic_sent_packet_manager.cc`, `quic_config.h`, `quic_constants.h`, `quic_stream.h`, `quiche/quic/load_balancer/`; HEAD of 2026-09-09. https://github.com/google/quiche (section 13).
79. QUIC Interop Runner: test-case definitions and methodology, https://raw.githubusercontent.com/quic-interop/quic-interop-runner/master/quic.md; results for the run started 2026-09-09T18:34, https://interop.seemann.io/logs/quic/2026-09-09T18:34/result.json (section 13.2).
