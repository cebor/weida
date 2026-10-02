# Connection statistics for one's own link (griasdi connection info)

## Context

griasdi is a chat and voice application in which every byte travels over
weida. It wants a per-device "connection info" view whose only purpose is to
let a user debug their own link: how long the path to the server takes, how
much of what they send and receive is lost, and -- the part that matters --
on which side of the server a problem sits.

The view is deliberately narrow. It shows no remote or local socket address,
and nothing about any other member: no per-user numbers, no names next to
numbers. It is a diagnosis of one device's connections, not a monitor of the
community.

griasdi already derives what it can from what weida delivers today:

- **Loss attribution for voice.** Every voice payload carries the sender's
  frame sequence number inside an end-to-end encrypted frame, and every
  datagram a server's RADIO forwards carries a per-topic segment number
  assigned at the server (`Received::Datagram.segment`). A gap in the
  sequence that also shows in the segment numbers was lost between the server
  and this device; a gap in the sequence without a gap in the segment numbers
  was already missing when it reached the server. The first is the user's
  download, the second is not the user's connection.
- **Arrival jitter** per RFC 3550 section 6.4.1, computed from arrival times
  and the 20 ms frame clock.
- **Playout counters** of its own jitter buffer: frames played, repaired from
  redundancy, concealed, late, dropped on overflow.
- **Upload loss and round-trip time** from `Flow::path_stats` on the voice
  uplink flow.

This document states what griasdi cannot get today, what weida provides, and
a proposal. It is a requirements document, not a decision; the decision
belongs in a note under `docs/decisions/`.

Everything under "What weida provides today" was read from the weida source at
commit `6dcb64b`; file and line references are given so a reader can check
rather than trust. Everything under "Proposal" is design, not fact.

## What griasdi needs

| Need | Shape |
| --- | --- |
| Round-trip time and its health outside a flow | a statistics call on the handle the application dialled, not on a flow |
| The numbers of the connection that carries a subscription or an RPC | one record per live connection of a handle |
| Bytes and datagrams in both directions, whole connection | counters that include all traffic, not one flow's payload |
| How long the current connection has been up, and how often it was redialled | a duration and a counter, so reconnects the application's own state machine never noticed become visible |
| Minimum RTT, lost bytes, current MTU | what quinn already tracks, in weida's own type |
| Download loss for traffic that is not voice | the sender's view of its own lost packets, reported to the receiver |
| No address in any statistics type | a hard constraint, not a preference |

## What weida provides today

Verified at `6dcb64b`.

- **Path and flow statistics exist as weida types.** `FlowStats` (sent,
  too_large, discarded, not_live, received, overflow) and `PathStats` (rtt,
  cwnd, congestion_events, lost_packets, sent_packets, max_datagram_size) are
  defined in `crates/weida/src/flow.rs:98-145`. `PathStats` has no
  `#[non_exhaustive]`, and its documentation notes that `quinn` 0.11 reports
  no RTT variation (`crates/weida/src/flow.rs:129`).
- **They are reachable only through a flow.** `Flow::path_stats`
  (`crates/weida/src/flow.rs:862-866`) and `IncomingFlow::path_stats`
  (`crates/weida/src/flow.rs:942-945`) return `None` on a local transport and
  the path of the flow's connection on QUIC. There is no way to ask a
  connection for its statistics without holding a flow on it.
- **They are filled from one quinn call.** `Link::path_stats`
  (`crates/weida/src/transport.rs:387-403`) reads `conn.stats().path` and
  copies five fields plus `max_datagram_size`. The UDP counters, `min_rtt`,
  `lost_bytes` and `current_mtu` that the same call returns are dropped.
- **A requester exposes almost nothing about its connections.** `Requester`
  has `peer_count` and `events` and no statistics
  (`crates/weida/src/endpoint.rs:203-211`). A subscription, an RPC or a file
  transfer rides a connection whose health the application cannot read.
- **A dish exposes its discard counters but not the path.** `Dish::stale` and
  `Dish::overflow` (`crates/weida/src/radio.rs:1145-1152`) count segments
  discarded on arrival; the datagram flows under a dish are internal and
  `Dish` has no `path_stats`.
- **Peer events carry no time and no count.** `PeerEvent`
  (`crates/weida/src/reconnect.rs:155-196`) reports `Connected`, `Lost`,
  `Retrying`, `GaveUp` and `Missed`, with attempt number and delay on
  `Retrying`. Nothing says how long a connection has lived or how many
  redials a handle has performed; an application timestamps events itself and
  loses everything that happened before it subscribed.
- **The segment number is what the loss attribution rests on.**
  `Received::Datagram { topic, segment, payload }`
  (`crates/weida/src/radio.rs:938-945`) gives the per-topic number the radio
  assigned. griasdi uses it and needs nothing more for voice downlink.
- **No address leaks through these types today.** None of `FlowStats`,
  `PathStats` or `PeerEvent` carries a socket address. That is a property
  worth keeping.

## Proposal

### Connection statistics without a flow

Every dialling handle -- `Requester`, `Subscriber`, `Pusher`, `Peer`, `Dish`
-- gets

```rust
pub fn connection_stats(&self) -> Vec<ConnectionStats>
```

one entry per live connection of the handle, each labelled by the dialled URL
exactly as the application gave it. A handle with no live connection returns
an empty vector. The call is synchronous and cheap: it reads quinn's counters
and takes no await.

This lets griasdi show ping and health outside voice, and the numbers of the
downlink connection, which voice code never holds a flow on.

### A weida-owned `ConnectionStats`

```rust
#[non_exhaustive]
pub struct ConnectionStats {
    pub url: Arc<str>,
    pub path: PathStats,
    pub tx: UdpCounts,
    pub rx: UdpCounts,
    pub age: Duration,
    pub redials: u64,
}

#[non_exhaustive]
pub struct UdpCounts {
    pub datagrams: u64,
    pub bytes: u64,
}
```

`PathStats` becomes `#[non_exhaustive]` and gains `min_rtt`, `lost_bytes` and
`current_mtu`. All of these exist in quinn-proto 0.11.18:
`ConnectionStats { udp_tx, udp_rx, path, .. }` with `UdpStats { datagrams,
bytes, .. }` (`quinn-proto-0.11.18/src/connection/stats.rs:10-19`),
`PathStats { rtt, min_rtt, lost_packets, lost_bytes, sent_packets,
current_mtu, .. }` (`stats.rs:133-160`) and `ConnectionStats`
(`stats.rs:162-176`). weida copies them into its own types so no `quinn` type
becomes public (`crates/weida/src/flow.rs:128`).

`age` counts from the handshake of the current connection, so it resets on a
redial. `redials` counts successful transparent redials of the handle
(decision 0031, `docs/decisions/0031-transparent-redial-and-the-sender-outbox.md`),
so an application sees reconnects that its own state machine never noticed.

### Loss as the remote side saw it

Only the sender's QUIC stack knows which of its packets were lost. For voice,
griasdi covers the downlink with segment gaps (see Context). For every other
kind of traffic a receiver has no way to learn how much of what was sent to
it never arrived.

Proposal: a capability-negotiated, periodic report in which each side sends
its `PathStats` for this connection to the other, at a low rate (for example
every 2 s while the other side has asked for it). The receiver then holds
both views of one connection: its own `connection_stats` and the sender's.
This is lower priority than the two items above.

### No address in any statistics type

Across all items: no remote or local socket address, resolved or observed,
appears in any statistics type or in the report. The only label is the URL
the application dialled, which it already holds and can leave off a screen.
A statistics screen built on these types cannot leak an address the
application did not give. This is a requirement on the types, not only on
griasdi's use of them.

## What weida would need, in priority order

1. **`connection_stats` on every dialling handle** (`Requester`,
   `Subscriber`, `Pusher`, `Peer`, `Dish`), one entry per live connection,
   labelled by the dialled URL.
2. **`ConnectionStats` and `UdpCounts`**, and `PathStats` extended with
   `min_rtt`, `lost_bytes` and `current_mtu` and marked `#[non_exhaustive]`;
   `age` and `redials` per connection.
3. **A remote-view report**: capability-negotiated, periodic exchange of each
   side's `PathStats` for the connection between them.
4. **The no-address rule** as an explicit invariant of every statistics type,
   covered by a test that fails if a field of address type is added.

## Rejected alternatives

- **Exposing `quinn::Connection` to the application.** It would answer
  everything above and break weida's rule that no `quinn` type is part of the
  public surface (`crates/weida/src/flow.rs:128`), and with it the freedom to
  change transport.
- **A griasdi-level ping RPC.** It measures the server's request handling
  rather than the path, adds an RPC per sample, and says nothing about loss.
- **Computing RTT variation in weida from `rtt()` samples.** The smoothed RTT
  already averages away the variation one wants to see; a variation computed
  from it is a smoothed number of a smoothed number.
- **Deriving upload loss from application acknowledgements.** Voice is
  deliberately unacknowledged; adding acknowledgements to read loss would
  change the traffic being measured.

## Open questions

1. **RTT variation.** `quinn` 0.11 keeps the variation internally and does not
   expose it (`crates/weida/src/flow.rs:129`). Either ask upstream for it or
   do without; griasdi measures voice jitter itself and can live without.
2. **Capability for the remote-view report.** Whether item 3 belongs in the
   HELLO capabilities (key `3`, `docs/PROTOCOL.md:488`; code `1` `datagram`
   is the only one defined, `docs/PROTOCOL.md:217-218`) as a second code, or
   on a reserved path.
3. **Server-side per-peer statistics.** A `Binding` could report statistics
   keyed by `Fingerprint`. griasdi does not need this today; it is recorded so
   the type in item 2 is not designed in a way that rules it out.
4. **Label for a connection shared by several handles.** If two handles ever
   share one connection, whether each reports it or only the first.
