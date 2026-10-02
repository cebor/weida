# 0036 — Connection statistics: one's own link, by dialled address, with no address in them

- **Status:** accepted
- **Date:** 2026-10-02
- **Items:** B-298 to B-303
- **Answers:** [requirements/griasdi-connection-info.md](../requirements/griasdi-connection-info.md)
  (all four proposals and all four open questions)
- **Amends:** [0034](0034-late-is-lost.md) §4.9 (statistics reachable through a flow only;
  `PathStats` without `min_rtt`, `lost_bytes`, `current_mtu`)
- **Related:** [0002](0002-control-and-bulk-separation.md) §6.2,
  [0007](0007-topic-namespace.md), [0021](0021-consensus-openraft.md),
  [0023](0023-completion-is-a-cursor.md), [0031](0031-transparent-redial-and-the-sender-outbox.md)
  §4.1, [0034](0034-late-is-lost.md) §4.2, §4.5, §4.9,
  [0035](0035-keys-proved-not-judged.md)

## 1. The question

griasdi wants a per-device "connection info" view whose only purpose is to let a user debug
their own link: how long the path to the server takes, how much of what they send and receive
is lost, and on which side of the server a problem sits [griasdi-connection-info, Context]. It
derives voice loss, jitter and playout counters itself; what it cannot get from weida is:

| Need | Shape |
| --- | --- |
| RTT and health outside a flow | a call on the handle the application dialled |
| The numbers of the connection under a subscription or an RPC | one record per live connection of a handle |
| Whole-connection bytes and datagrams in both directions | UDP counters, not one flow's payload |
| Connection age and redial count | so reconnects the application never noticed become visible |
| `min_rtt`, lost bytes, current MTU | what quinn tracks, in weida's own type |
| Download loss for traffic that is not voice | the sender's view of its own losses, reported to the receiver |
| No address in any statistics type | a hard constraint |

So the question is: **where does a connection's health become readable without a flow, what
does the record contain, how does a receiver learn the sender's losses, and how is "no address"
made a property of the types rather than of one application's discipline.**

## 2. The evidence, condensed

Verified at `6dcb64b`; every claim of the requirement's "What weida provides today" holds at the
lines it names. Two facts it did not state change the work:

**2.1 The lock file cannot provide `min_rtt`.** The requirement cites `quinn-proto` 0.11.18.
`Cargo.lock` held 0.11.17, whose `PathStats` (`quinn-proto-0.11.17/src/connection/stats.rs:
136-158`) has `lost_bytes` and `current_mtu` but **no `min_rtt`**; 0.11.18 added it
(`stats.rs:139-140`). The workspace requires `quinn = "0.11"`, so nothing prevents a resolver
from picking 0.11.17. `quinn` 0.11.12 is the first release whose manifest requires
`quinn-proto` 0.11.18 (`[dependencies.proto] version = "0.11.18"`); `cargo update -p quinn -p
quinn-proto` moves the lock to 0.11.12 / 0.11.19 under the workspace's Rust 1.88. 0.11.19 still
reports no RTT variation: `RttEstimator` keeps `var` (`connection/paths.rs:191`) and `PathStats`
does not carry it.

**2.2 A dialled address already is the unit the requirement asks for.** Every dialling handle
— `Requester`, `Pusher`, `Subscriber`, dialling `Paired`, `Surveyor`, `BusMember`, `Dish` —
holds one `Peer` (`crates/weida/src/endpoint.rs:172-174`, `281-283`, `567-574`, `805-815`,
`1003-1005`, `1277-1279`; `crates/weida/src/radio.rs:1092-1104`). A `Peer` holds one `Slot` per
dialled URL, carrying the URL **exactly as the application gave it** and the state `Live(ConnHandle)`,
`Down` or `Gone` (`crates/weida/src/stream.rs:53-71`), and `live_peers` already walks the live
ones under a `std` mutex without an await (`stream.rs:219-231`). The redial loop that replaces a
slot's connection is one function, `watch_slot`, and it sets the slot live again at exactly one
place (`stream.rs:726-731`).

**2.3 A connection has no birth time.** `ConnCtx` (`crates/weida/src/conn.rs:51-99`) records
the peer, the chain and the negotiated set, but no instant. `ConnCtx::spawn`
(`conn.rs:105-163`) runs once per connection, right after the QUIC handshake
(`crates/weida/src/pool.rs:281-289`) and once per local connection.

**2.4 One connection can be shared by several handles.** The pool key is `(authority, port,
ClientTls, address pin, path)` (`pool.rs:63-70`), so two handles dialling the same URL on the same
terms share one connection (`pool.rs:137-139`). Its counters are the connection's; a slot is the
handle's.

**2.5 One quinn call holds everything, and weida copies a sixth of it.** `Link::path_stats`
(`crates/weida/src/transport.rs:387-403`) reads `conn.stats().path` and copies five fields;
`ConnectionStats { udp_tx, udp_rx, path, .. }` from the same call carries the UDP counters, and
`quinn-proto` 0.11.19 `PathStats` carries `min_rtt`, `lost_bytes` and `current_mtu`
(`stats.rs:136-176`).

**2.6 No reserved path exists.** Paths are opaque and the application's
([0007](0007-topic-namespace.md)); [PROTOCOL.md](../PROTOCOL.md) defines no path weida claims for
itself, and capability code `1` is the only code (`PROTOCOL.md:495-506`), gating frame kind `7`
by "both HELLOs listed it". Kind `8` is the first free frame kind (`PROTOCOL.md:403-408`).

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — statistics on the connection handle the pool holds | a public `Connection` type | weida has no public connection type, and a pooled connection is shared (§2.4); exposing one makes the pool part of the API. Refused |
| **B — statistics per live slot of a `Peer`** | `connection_stats() -> Vec<ConnectionStats>` on `Peer` and every handle that holds one | a pooled connection shared by two handles appears under both (§4.6). Chosen |
| C — the proposal's record, with `path`, `tx`, `rx` mandatory | `ConnectionStats { url, path, tx, rx, age, redials }` | a local connection has no quinn statistics; either it is omitted, and an empty vector no longer means "no live connection", or it carries made-up zeros. Refused by the owner |
| D — three optional fields | `path: Option<PathStats>, tx: Option<UdpCounts>, rx: Option<UdpCounts>` | three fields that are always absent together, and a type that permits one without the others. Refused by the owner |
| **E — one optional transport record** | `transport: Option<TransportStats { path, tx, rx }>` | one `None` meaning "no path to measure", as `Flow::path_stats` already says it. Chosen by the owner |

For the remote view (§4.5):

| Option | Shape | Named loss |
| --- | --- | --- |
| F — a reserved path carrying DATA | `"$weida/report"` routed through the namespace | the first carve-out from the application's opaque path space (0007), and namespace dispatch is pattern-bound: a report would need a pattern. Refused |
| **G — capability code `2` and frame kind `8`** | gated exactly as code `1` gates kind `7` | a new frame kind and a new codec; a peer that does not list the code never sees it, so no v0 peer breaks. Chosen |
| H — a DATAGRAM per report | rides capability `1` | ties a report to flows being enabled, and a lost report is the one sample a user looking at loss wanted. Refused |

## 4. Decision

**Option B with E: `connection_stats` on every dialling handle, one record per live slot,
labelled by the URL as given; `PathStats` extended; the no-address rule enforced by the types'
test; and the remote view decided now as capability `2` with frame kind `8`, built after the
rest.**

### 4.1 The types

```rust
#[non_exhaustive]
pub struct ConnectionStats {        // Clone, Debug, PartialEq, Eq
    pub url: Arc<str>,              // the dialled URL, exactly as the application gave it
    pub age: Duration,              // since this connection was established; resets on a redial
    pub redials: u64,               // successful transparent redials of this slot
    pub transport: Option<TransportStats>,   // None on a local transport
}

#[non_exhaustive]
pub struct TransportStats {         // Clone, Copy, Debug, PartialEq, Eq
    pub path: PathStats,
    pub tx: UdpCounts,
    pub rx: UdpCounts,
}

#[non_exhaustive]
pub struct UdpCounts {              // Clone, Copy, Debug, Default, PartialEq, Eq
    pub datagrams: u64,
    pub bytes: u64,
}
```

`PathStats` becomes `#[non_exhaustive]` and gains `min_rtt: Duration`, `lost_bytes: u64` and
`current_mtu: u16`, beside its six fields. All statistics types live in one module,
`crates/weida/src/stats.rs`; `FlowStats` stays with the flow it counts. Everything is copied out
of one `quinn::Connection::stats()` call into weida's own types, so no `quinn` type becomes
public ([0034](0034-late-is-lost.md) §4.9's rule, unchanged). The workspace requires
`quinn = "0.11.12"`, the first release that guarantees `min_rtt` (§2.1).

`Flow::path_stats` and `IncomingFlow::path_stats` keep their signatures and return the extended
`PathStats`.

### 4.2 Where they are read

`Peer::connection_stats(&self) -> Vec<ConnectionStats>`, and the same method on `Requester`,
`Pusher`, `Subscriber`, `Paired` (empty on a bound pair, which dialled nothing), `Surveyor`,
`BusMember` and `Dish`, plus their `weida::blocking` twins. One entry per slot whose connection
is live — the set `peer_count` counts — in the order the addresses were dialled; a handle with no
live connection returns an empty vector. Synchronous and await-free: the slot list is collected
under its mutex and quinn's counters are read after it is released.

A bound side (`Replier`, `Puller`, `Publisher`, `Radio`, `Respondent`) gets nothing here; see
§4.7 item 3.

### 4.3 `age` and `redials`

`ConnCtx` records the instant `ConnCtx::spawn` runs — after the QUIC handshake, before HELLO —
and `age` is its elapsed time, so it resets with every new connection. `redials` lives on the
`Slot`, which outlives its connections ([0031](0031-transparent-redial-and-the-sender-outbox.md)
§4.1), and is incremented where `watch_slot` makes the slot live again after a successful
redial. A first `connect` is not a redial. The clock is `std::time::Instant`, so a paused Tokio
clock in a test does not stop it.

### 4.4 No address in any statistics type

The only label any statistics type carries is the URL the application dialled. No socket
address — remote or local, resolved or observed — appears in `PathStats`, `FlowStats`,
`UdpCounts`, `TransportStats`, `ConnectionStats` or the remote view of §4.5. This is an
[INVARIANTS.md](../INVARIANTS.md) entry and is enforced by `stats::tests::
statistics_types_carry_no_address`: it destructures every statistics type without `..` and passes
each field to a function bounded by a crate-private marker trait implemented for the field types
allowed (integers, `Duration`, `Arc<str>`, the statistics types themselves, `Option` of them).
A new field fails to compile until the test names it, and an address-typed field fails to compile
until someone writes `impl NoAddress for SocketAddr` — a line that cannot pass review by accident.

### 4.5 The remote view: capability `2`, frame kind `8` (decided now, built in B-301, B-302)

Only the sender's QUIC stack knows which of its packets were lost, so a receiver learns its
download loss from the sender.

- **Capability code `2` `path_report`.** A side lists it in HELLO key `3` exactly when its
  profile sets `Limits::path_report` (default `false`). Reports flow on a connection only when
  **both** HELLOs listed it — code `1`'s rule — so a server operator opts in per binding profile
  and a client per runtime, and neither is made to send by the other.
- **Frame kind `8` REPORT** on QUIC only. After negotiation each side opens **one** uni stream
  carrying a header-only head frame (an empty CBOR map; keys reserved) and then records
  `[varint length][CBOR map]` until FIN. A record is at most 256 bytes; its keys are `0` rtt µs,
  `1` min_rtt µs, `2` cwnd, `3` congestion_events, `4` lost_packets, `5` lost_bytes, `6`
  sent_packets, `7` current_mtu, `8` tx datagrams, `9` tx bytes, `10` rx datagrams, `11` rx
  bytes; unknown keys are skipped by [PROTOCOL.md](../PROTOCOL.md) §5's rule. The sender writes
  one record every **2 s**, a constant on its own clock: the peer cannot choose the rate.
- **Violations**: a REPORT stream from a peer when not both listed code `2`, a second REPORT
  stream on one connection, a record over 256 bytes, and a record truncated at FIN close the
  connection with `PROTOCOL_VIOLATION`.
- **The receiver keeps the latest record only** — one fixed-size slot per connection, no queue
  — and exposes it as `ConnectionStats::remote: Option<RemoteStats>` with the record's fields
  and how long ago it arrived. `None` until a first record, on a local transport, and when the
  code was not agreed.

Option F is refused because a reserved path would be the first carve-out from the opaque path
space and would need a pattern to dispatch through; H because it couples reports to flows.

### 4.6 Open question 4: a connection shared by several handles

Each handle reports it. The record is per **slot**: `url` and `redials` are the handle's,
`age` and `transport` are the shared connection's, so two handles on one pooled connection show
identical transport counters and may show different redial counts. The documentation of
`connection_stats` says so; no deduplication is attempted, because a handle cannot see which
other handles exist.

### 4.7 The other open questions

1. **RTT variation.** Not passed through and not computed: `quinn` 0.11.19 keeps it internally
   and exposes it nowhere (§2.1), and a variation of the smoothed RTT is the requirement's own
   rejected alternative. `min_rtt` beside `rtt` gives the floor; griasdi measures voice jitter
   itself. Revisit when `quinn` exposes it.
2. **Capability or reserved path** for the report: capability code `2` (§4.5).
3. **Server-side per-peer statistics.** Not built. `TransportStats` is the unit a bound side
   would report — it carries no URL and no dialler state — so a later `Binding` accessor keyed by
   `Fingerprint` reuses it beside `age`, without a type change here.

### 4.8 Python

`connection_stats` is reachable from Python — the bindings dial with the same handles — but no
statistics type has a Python class yet, and `Flow::path_stats` is absent with flows
([libraries/weida-py.md](../libraries/weida-py.md) §4.4a). It is recorded as absent with this
reason and filed as B-303.

## 5. Consequences and follow-ups

Documents (B-300): [ARCHITECTURE.md](../ARCHITECTURE.md) §1's flow row and the API sketch take
`connection_stats`; [PATTERNS.md](../PATTERNS.md) §1 (the dialling `Peer` paragraph) and §1.12
say where a connection's health is read and that `PathStats` is no longer flow-only;
[INVARIANTS.md](../INVARIANTS.md) takes §4.4; [IMPLEMENTATION.md](../IMPLEMENTATION.md) records
the phase entry and the quinn floor; [libraries/weida-py.md](../libraries/weida-py.md) records
the absent name; [0034](0034-late-is-lost.md) §4.9 points here; the requirement points at what
shipped. Backlog:

### B-298 — Connection statistics types and the quinn floor
kind: code | size: 45 | status: ready | needs: []
acceptance: [0036](0036-connection-statistics.md) §4.1: `crates/weida/src/stats.rs` holds `PathStats` (now `#[non_exhaustive]`, plus `min_rtt`, `lost_bytes`, `current_mtu`), `UdpCounts`, `TransportStats` and `ConnectionStats`, re-exported from the crate root; `Link::transport_stats` fills `TransportStats` from one `quinn` `stats()` call, `None` locally, and `Flow::path_stats`/`IncomingFlow::path_stats` read its `path`; the workspace requires `quinn = "0.11.12"`. §4.4's `statistics_types_carry_no_address` compiles only with every field of every statistics type named and bounded by `NoAddress`.

### B-299 — `connection_stats` on every dialling handle
kind: code | size: 60 | status: ready | needs: [B-298]
acceptance: [0036](0036-connection-statistics.md) §4.2, §4.3: `Peer::connection_stats` and the same method on `Requester`, `Pusher`, `Subscriber`, `Paired`, `Surveyor`, `BusMember`, `Dish` and their `weida::blocking` twins; one entry per live slot, labelled by the URL as given; `age` from `ConnCtx`'s birth instant, `redials` per slot. Tests in `crates/weida/tests/stats.rs`: empty before `connect` and after `disconnect`; over QUIC, `transport` is `Some` with `rtt > 0`, `current_mtu >= 1200`, tx and rx bytes growing across a round trip, and no resolved address in the record's `Debug` text for a dial by name; over a local transport `transport` is `None` and `age` grows; after a server restart `redials == 1` and `age` restarted.

### B-300 — Documents for 0036
kind: spec | size: 30 | status: ready | needs: [B-298, B-299]
acceptance: §5's edits; no passage outside `decisions/`, `research/`, BACKLOG and NIGHTLOG still says path statistics are reachable through a flow only or lists `PathStats` without its three new fields.

### B-301 — PROTOCOL: capability `2` and frame kind `8` REPORT
kind: spec | size: 45 | status: ready | needs: [B-300]
acceptance: [0036](0036-connection-statistics.md) §4.5 in [PROTOCOL.md](../PROTOCOL.md): code `2` `path_report` in §6.1's table with the both-listed rule, kind `8` in §4 and §4.1, a §6.10 REPORT with the head frame, the record keys, the 256-byte cap, the fixed 2 s interval and the four violations, `Limits::path_report` in §10, kind `9` named as the first free number; `weida-protocol` encodes and decodes the head frame and a record, with golden vectors and the `roundtrip` fuzz target extended.

### B-302 — The remote view on the connection
kind: code | size: 90 | status: ready | needs: [B-301]
acceptance: [0036](0036-connection-statistics.md) §4.5: with `Limits::path_report` on both sides each side sends one REPORT stream and a record every 2 s; the receiver keeps the latest record in one slot and `ConnectionStats::remote` returns it with its age; off on either side, nothing is sent and `remote` is `None`; each violation closes with `PROTOCOL_VIOLATION`, tested against a hand-written peer stream. INVARIANTS names the slot as fixed-size.

### B-303 — Python: connection statistics
kind: code | size: 45 | status: ready | needs: [B-299]
acceptance: [0036](0036-connection-statistics.md) §4.8: `connection_stats()` on every dialling class of both Python surfaces returns `weida.ConnectionStats` values (url, age seconds, redials, transport as a value with path, tx, rx, or `None`); a test over QUIC loopback asserts a positive RTT and `None` over `weida+inproc`; [libraries/weida-py.md](../libraries/weida-py.md) moves the row from absent to present.

## 6. What this note does not decide

- **Server-side per-peer statistics** (§4.7 item 3): a later requirement.
- **Toggling reports at run time.** Code `2` is agreed at HELLO for the life of the connection;
  an application that wants reports only while a view is open turns `Limits::path_report` on and
  ignores `remote` otherwise, at one record every 2 s.
- **RTT variation** (§4.7 item 1).

## 7. Sources

weida documents: [PROTOCOL.md](../PROTOCOL.md) §4, §5, §6.1; [INVARIANTS.md](../INVARIANTS.md);
decisions 0002, 0007, 0021, 0031, 0034, 0035;
[requirements/griasdi-connection-info.md](../requirements/griasdi-connection-info.md).

Code read for this note, at `6dcb64b`: `crates/weida/src/flow.rs:98-145, 862-866, 942-945`;
`crates/weida/src/transport.rs:387-403`; `crates/weida/src/stream.rs:53-107, 219-245, 330-586,
603-735`; `crates/weida/src/conn.rs:51-163`; `crates/weida/src/pool.rs:63-70, 102-313`;
`crates/weida/src/endpoint.rs:172-242, 281-311, 567-628, 805-917, 1003-1042, 1277-1377`;
`crates/weida/src/radio.rs:938-945, 1092-1152`; `crates/weida/src/reconnect.rs:155-196`;
`crates/weida/src/lib.rs:91`. `quinn-proto` 0.11.17 and 0.11.19 `src/connection/stats.rs`,
0.11.17 `src/connection/paths.rs:187-191, 307-335`; `quinn` 0.11.12 `Cargo.toml`
(`[dependencies.proto]`).
