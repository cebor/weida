# Prior art: messaging on and near QUIC

## 0. Scope

This sheet surveys systems that already run messaging, publish/subscribe, RPC or peer networking
over QUIC, or over a transport with the same primitives. It is a survey, so it does not use the
sheet template of `README.md`; instead every system is described along the same seven axes, as
sub-headings, so that the systems can be compared: **identity card** (project, language, license,
maturity, QUIC stack, date read), **message-to-transport mapping** (connection, stream, datagram,
message boundaries, large payloads), **flow control and backpressure** (unit of credit, grantor,
overload behaviour, application-level credit above QUIC), **delivery guarantees** (claim,
certificate, retries, duplicates, ordering scope), **identity, trust and authorization** (naming,
what the handshake proves, where policy is enforced), **connectivity** (reconnection, migration,
NAT traversal, relays, discovery) and **reported problems** (what the project itself lists).

Each system keeps its own vocabulary: Zenoh has links, priorities and congestion control; MOQT has
tracks, groups, subgroups and objects; iroh has endpoints, paths and ALPNs; EMQX has control and
data streams. No term is translated, and no sentence compares two systems unless a cited source
does.

Tier 1 is covered in depth (sections 1-5). Tier 2 (section 6) collects nine shorter cases,
including three that are not on QUIC and are here as design references: Mosh (roaming), Cap'n
Proto (capability handoff over a stream), WireGuard/Tailscale (key as identity). Negative
findings — NATS, Kafka, Redpanda, DDS — are stated with the searches that produced them.

All material was read on 2026-09-10 unless a source entry says otherwise. Section 9 numbers every
source; blog posts are marked `(blog)` and vendor-run measurements `(vendor benchmark)`. Author
inferences are marked `[inference]`.

## 1. Zenoh

### Identity card

Eclipse Zenoh, Rust, dual EPL-2.0 OR Apache-2.0, maintained by ZettaScale; manifest
self-description "Zenoh: The Zero Overhead Pub/Sub/Query Protocol" [2]. Release 1.10.1 published
2026-09-07 (1.10.0 2026-08-14, 1.9.0 2026-04-10), MSRV 1.75.0 [2][3]. QUIC is one of nine link
kinds, implemented on quinn 0.11.5 with rustls 0.23 [2]. There is no versioned transport
specification: `eclipse-zenoh/roadmap` holds RFCs for key expressions, selectors, ACLs,
liveliness and reliability, but the bit layout of `InitSyn`/`Frame`/`Fragment` exists only as doc
comments in `commons/zenoh-protocol`, so every wire statement below is sourced from 1.10.1 code
[5][6]. In passing: zenoh-pico (C, microcontrollers), the `zenoh-plugin-dds` and
`zenoh-plugin-ros2dds` bridges and the `storage_manager` plugin release in lockstep.

### Message-to-transport mapping

Three stacked layers: *transport* (sessions, links, frames, sequence numbers), *network* (routed
`Push`, `Request`, `Response`, `ResponseFinal`, `Interest`, `Declare`, `OAM`) and *zenoh*
(payloads `Put`, `Del`, `Query`, `Reply`, `Err`) [5][6]. Addressing is a `/`-separated key space;
key expressions use `*`, `$*` and `**` and must be canonical so that two expressions denoting the
same set are the same string [10]. Expressions are compressed by declaration
(`DeclareKeyExpr{id, wire_expr}`, later messages referencing the id plus a suffix) [6].

A `put` becomes `Push{wire_expr, ext_qos, ext_tstamp, ...}` carrying
`Put{timestamp, encoding, ext_attachment, payload: ZBuf}` [6]. Several complete network messages
that fit one batch travel in one `Frame{reliability, sn, payload: Vec<NetworkMessage>}` under a
single sequence number; anything larger is split into `Fragment{reliability, more, sn, ...}` [5].
The batch is the unit reaching the link: `transport.link.tx.batch_size` defaults to 65535, which
is also the maximum because the batch length prefix is a `u16` [1][5]. Receive side:
`link.rx.buffer_size` 65535 and `link.rx.max_message_size` (defragmentation ceiling)
1073741824 bytes, above which fragmented messages are dropped [1].

QUIC is a first-class link addressed `quic/host:port`, and its declared MTU is 65535 — set by
Zenoh's own 16-bit batch length, not by QUIC [9]. Since 1.9.0 the link maps priorities onto QUIC
streams, selected by ALPN: `hq-29` (Zenoh <= 1.8.0), `zenoh` (single stream), `zenoh-mr` (mixed
reliability), `zenoh-ms` (multi-stream), `zenoh-ms-mr` (both) [8]. Multi-stream means one
bidirectional stream for `Priority::Control` plus seven unidirectional streams opened in strict
order, so stream index + 1 identifies the priority, with `max_concurrent_uni_streams = 7` and
`set_priority(-(prio as i32))` because "QUIC stream priority semantics (P0 < P1 < P2) are the
opposite of Zenoh's (P0 > P1 > P2)" [8]. Mixed reliability splits one QUIC connection into a
reliable stream link plus a best-effort QUIC DATAGRAM link, returned as
`NewLink::MixedReliability{reliable, best_effort}`, the datagram link registering under the same
`quic` prefix with `IS_RELIABLE = false` [9]. Both are endpoint metadata knobs (`multistream`
default `auto`, `mixed_rel` default `0`) and the ALPN list is computed so an old peer falls back
to `hq-29` [8].

### Flow control and backpressure

No window of Zenoh's own: the link's, plus a per-priority queue in front of it with explicit
block-or-drop semantics. Eight priority classes exist (`Control`, `RealTime`, `InteractiveHigh`,
`InteractiveLow`, `DataHigh`, `Data` (default), `DataLow`, `Background`), each with its own queue
sized *in batches* — `queue.size.<class>` defaults to 2, range 1-16, so memory per queue is
`size * batch_size` [1][4].

Congestion control is per message via QoS flags: `D` = don't drop (Block), `F` = don't drop the
first message, no flag = Drop [7]. `CongestionControl::DEFAULT` is `Drop`, while the per-kind
defaults are `PUSH = Drop` and `REQUEST`/`DECLARE`/`INTEREST`/`OAM = Block` [4][13] — a plain
`put` is droppable, a query is not. What Drop drops is precise: a droppable message returns
immediately if the priority is already marked congested, otherwise waits at most
`drop.wait_before_drop` (1000 microseconds) for a free batch *in that priority's queue*, then
drops the whole message and marks the priority congested; a non-droppable message waits
`block.wait_before_close` (5 s) and then the session is closed [1][7]. Dropping is per priority
queue, never per link or per key expression: a saturated `Data` queue cannot drop `RealTime`.

Fragmentation gets its own deadline (`drop.max_wait_before_drop_fragments`, 50000 microseconds
shipped) and its own wire marker: if the deadline expires after the first fragment has left, the
sender emits a `Fragment` with the `Drop` extension so the receiver discards the partial
reassembly instead of stalling; if nothing has been sent, the sequence number is restored and the
message vanishes [1][5][7]. Batching is adaptive and on by default (`time_limit` 1 ms), triggered
by observed back-pressure, and `express` is the per-message opt-out that flushes immediately
[1][7]. Two extra shaping interceptors exist: `downsampling` (max frequency per key expression)
and `low_pass_filter` (drop above a serialized size) [1].

### Delivery guarantees

Reliability is per message (`BestEffort` or `Reliable`, default `Reliable`), carried as the `R`
flag of `Frame` and `Fragment`, so each priority has two independent `u32` sequence-number spaces;
width is negotiated (`sequence_number_resolution` default `32bit`, lower peer wins) [1][4][5].
"Reliable" is narrow and the project says so: the transport message set is only `Frame`,
`Fragment`, `KeepAlive`, `Close`, `OAM`, `Join` — no ack, nack or retransmit — so reliability is
hop-by-hop over reliable links [5]. The roadmap RFC states that with `Reliable` subscribers and
`Block` publishers no samples are dropped in a stable infrastructure and back-pressure reaches
the publishers, but router crashes or topology changes still lose samples; its proposed remedy
attaches `SourceInfo{source_id, source_sn}` to samples, keeps a bounded-history cache, and lets
subscribers detect gaps and re-query the range with a `_sn=<start>..<end>` selector, with no acks
or heartbeats while there is no loss [12].

Every value gets a Hybrid Logical Clock timestamp plus the generating node's UUID, giving unique
global ordering without consensus; `timestamping.enabled` defaults to true only on routers, and
future timestamps are re-timestamped rather than dropped [1][10]. Queries are correlated by
`RequestId` (u32), replies carry the responder's entity id in `ext_respid`, and `ResponseFinal`
terminates; further bounds are the `Budget` (max responses) and `Timeout` extensions, with
`queries_default_timeout` 10000 ms [1][6]. `QueryTarget` is `BestMatching`, `All` or
`AllComplete`; `ConsolidationMode` is `Auto`, `None`, `Monotonic` or `Latest` [6][13]. Discovery
is interest-driven and its failure mode is documented: `routing.interests.timeout` 10000 ms, whose
expiry "implies that the discovery protocol might be incomplete, leading to potential loss of
messages, queries or liveliness tokens", and `open.return_conditions.connect_scouted` defaults to
true because otherwise "first publications and queries after session open from peers may be lost"
[1].

### Identity, trust and authorization

Node identity is the ZID (1-16 bytes, random by default), and it is explicitly not a credential:
"ZID is not backed by an authentication mechanism, it can only be trusted for ACL if it is
dynamically added/removed by eventual dedicated Zenoh mechanisms when transports are opened/closed.
If managed manually in ACL config, can be useful for prototyping but should not be used in
production!" [1]. The handshake is `InitSyn`/`InitAck` (negotiating batch size, sequence-number
resolution and the `QoS`, `Shm`, `Auth`, `MultiLink`, `LowLatency`, `Compression`, `Patch`
extensions) then `OpenSyn{lease, initial_sn, cookie}`/`OpenAck` [5]. Application authentication is
`transport.auth` with `usrpwd` and `pubkey`, both null by default [1]. TLS options are shared by
the `tls` and `quic` links: `enable_mtls` (default false), `verify_name_on_connect` (default true),
`close_link_on_expiration` (default false; a listener can only disconnect an expired client if
mTLS is on) [1]; the QUIC link extracts the certificate common name into a `LinkAuthId` [9].
Authorization is a separate interceptor, disabled by default, deny-first, with rules over message
kinds and key expressions and subjects combining interfaces, certificate common names, usernames,
link protocols and ZIDs [1].

### Connectivity

A node is `router`, `peer` (default) or `client`; routers never auto-connect to each other, so
router topology is explicit [1][11]. Default listeners are `tcp/[::]:7447` (router) and
`tcp/[::]:0` (peer) [1]. Discovery is multicast scouting (group `224.0.0.224:7446`, `ttl` 1) plus
gossip scouting, whose `multihop` defaults to false because it "implies more scouting traffic and
a lower scalability"; clients do not gossip, and `autoconnect_strategy` `greater-zid` exists so
only one of two nodes initiates [1]. `scouting.timeout` 3000 ms, `scouting.delay` 500 ms; connect
retry 1000 ms doubling to 4000 ms [1]. Session limits: `open_timeout`/`accept_timeout` 10000 ms,
`accept_pending` 100, `max_sessions` 1000, `max_links` 1 incoming link per transport [1]. Link
liveness is a 10000 ms lease with four `KeepAlive` messages per period, citing ITU-T G.8013 (a
link fails when nothing arrives in 3.5x the interval) [1][5]. There is no migration concept: a
link is up or it is re-dialled.

### Reported problems

No Zenoh document says what QUIC buys over TCP; the configuration reference treats `quic` as one
prefix among nine, and the only stated rationale is issue #2016 "Reduce real-time latency through
QUIC link-layer QoS" plus the code comments about priority mapping [1][8][16]. The QUIC link's
defect history reads as catching up with TCP/TLS: mTLS ignored (#771, 2024), close/reopen panic
(#1019), missing DSCP (#2427) and missing MTU/PMTUD configuration (#2428), a feature-flag split
mis-advertising link support (#2506) [16]. Open on 2026-09-10: an admission-control DoS where one
unauthenticated connection blocks all new admissions because the accept loop awaits `accept_bi()`
inline (#2719, with the TLS analogue #2685), listener errors on connection-specific failures
(#2425), silent failure when a QUIC certificate is missing (#2018), inability to distrust the
WebPKI roots (#2764), a documented/actual contradiction about whether `root_ca_certificate`
replaces or adds to those roots (#2711), a hang closing a lowlatency QUIC session (#1267), and
"extension point for third-party link transports (avoid forking zenoh-link)" (#2707) — link kinds
are a closed set [16]. `transport.unicast.lowlatency` does not preserve QoS, is incompatible with
`qos.enabled` and cannot fragment [1].

Published measurements are old and off-target: the detailed comparison is Zenoh 0.7.0-rc over TCP
(2023-03-21), reporting up to ~4M msg/s and 67 Gbps on one machine and 10 us peer latency against
Cyclone DDS 8 us, MQTT 27 us and Kafka 73 us [15]. There is no public measurement of the QUIC link
at all, and none of multi-stream versus single-stream, so the benefit of `zenoh-ms` is
unquantified. The DSD 2023 paper claims "minimal wire overhead of 5 bytes"; the full text was not
retrievable, so that is an author claim [14].

## 2. iroh

### Identity card

iroh by number 0 (n0), Rust, MIT OR Apache-2.0, MSRV 1.91, release 1.2.0 (2026-09-09); crate
description "p2p quic connections dialed by public key" [19][20]. The QUIC stack is **not** quinn:
1.2.0 depends on `noq`, `noq-proto` and `noq-udp` 1.3.0, and the noq README says "Noq started out
as a fork of the excellent Quinn project. The main focus of development has been towards adding
support for more QUIC (draft) extensions: QUIC Multipath, QUIC Address Discovery (QAD), Using QUIC
to traverse Nat's (QNT)" [19][21]. The switch was announced in 0.97.0 (2026-03-16); before that
iroh shipped a patched fork as `iroh-quinn` [31]. Naming history matters when reading older
material: `iroh-net` folded into `iroh` in 0.29 (2024-12-05), and `NodeId`/`NodeAddr` became
`EndpointId`/`EndpointAddr` in 0.94.0 (2025-10-22) [34][35]. Version 1.0 (2026-06-15) "asserts
stability for both the wire protocol and language APIs", and "any change that affects the wire
stability of iroh will always coincide with a major release" [27].

### Message-to-transport mapping

The unit is the connection; there is no message layer. An `Endpoint` — one per application by
recommendation, so connections share paths — dials and accepts real QUIC connections with
bidirectional and unidirectional streams plus RFC 9221 datagrams [17][23]. Streams are "very cheap
to create", with one sharp edge: "to keep streams cheap, they are lazily created on the network:
only once a sender starts sending data on the stream will the receiver become aware of a stream"
[17].

Protocol multiplexing is ALPN and nothing else: "the connecting side proposes a list of ALPNs, and
the server side either chooses one of the ALPNs or refuses the handshake. This happens directly
during the QUIC handshake, so it has no overhead other than the raw bytes of the ALPN string"
[28]. The same post names both consequences: "the ClientHello containing the ALPN is unencrypted,
so on-path observers can see which protocol is used", and "two iroh endpoints talking multiple
protocols will have not one but *multiple* connections open (A QUIC connection is always for
*exactly one* ALPN). We share some internal state for these connections so that e.g. hole punching
doesn't have to start from zero" [28]. `Router` dispatches by ALPN; an empty ALPN is an error since
1.0.3 [17][20].

Relayed traffic is tunnelled UDP: endpoints "establish an entirely normal HTTPS connection to the
relay server and then upgrade it to a WebSocket connection", and the relay "forwards UDP datagrams
from one endpoint to another, tunneling them inside the HTTP connections... you send it a
destination NodeId together with a datagram", so it "is nothing more than another network path
along which UDP datagrams can travel between iroh nodes" [17][29]. Relay protocol v2 with version
negotiation landed in 0.98.0 [32].

Framing is each protocol's own. `iroh-blobs` 0.103.0 puts one request and its response on one
stream, where "a request describes data in terms of BLAKE3 hashes and byte ranges" and the answer
is "encoded as BLAKE3 verified streams, on the same QUIC stream"; stated goals include "be
paranoid about data integrity... Data will be validated both on the provider and getter side" and
no size limit "up to terabytes". Granularity is documented — "the minimum granularity is a chunk
group of 16KiB or 16 blake3 chunks" while "ranges are always given in terms of 1024 byte blake3
chunks" — so a range request costs about two chunk groups of overhead, and resuming an interrupted
download is just a request for the chunks not yet held [37].

### Flow control and backpressure

At the QUIC layer everything is per connection and exposed: `QuicTransportConfigBuilder` carries
`max_concurrent_bidi_streams`, `max_concurrent_uni_streams`, `stream_receive_window`,
`receive_window`, `send_window`, `send_fairness`, `max_idle_timeout`, `keep_alive_interval`,
datagram buffer sizes, congestion-controller factory, `ack_frequency_config`,
`max_concurrent_multipath_paths` and `max_remote_nat_traversal_addresses`, with runtime setters on
`Connection` and per-path `rtt()`/`congestion_state()` [18].

The relay path has a token bucket instead: "each client connection gets its own token-bucket
limiter on the side of the relay that reads data sent by that client (rx)", and "when a client's
bucket empties, the relay stops reading from that connection's socket until it refills. Data isn't
dropped. The client just experiences backpressure" [25]. Self-hosted relays default to no limit;
public limits are deliberately unpublished; since 1.1.0 the relay notifies clients on 1.0.4+ once
per connection that they are being throttled [20][25]. Commercial tiers put numbers on it: Pro
5 MB/s and 10,000 concurrent connections, Dedicated 60,000 [36].

`iroh-gossip` 0.101.0 has no send window: `GossipSender::broadcast` queues a command on the gossip
actor's mpsc channel [38]. [inference] Backpressure there is the actor mailbox only — the future
resolves when the command is queued, not when any peer has received anything. Payloads are bounded
by `DEFAULT_MAX_MESSAGE_SIZE = 4096` bytes (minimum 512, configurable) [38].

### Delivery guarantees

Per stream, QUIC's: reliable and ordered, no ordering between streams, plus unreliable datagrams;
noq adds 0-RTT, 0.5-RTT and unordered stream reads [17][21]. `iroh-blobs` certifies delivery
cryptographically rather than by acknowledgement — content-addressed by BLAKE3 and verified
incrementally on both sides — and its documented non-goals include range discovery: "the protocol
does not yet have a discovery mechanism for asking the provider what ranges are available for a
given blob" [37].

`iroh-gossip` is epidemic broadcast "based on epidemic broadcast trees... The implementation is
based on the papers HyParView and PlumTree": active and passive views with periodic shuffling, the
payload to the eager set and only a hash (`IHave`) to the lazy set, and a lazy peer that misses a
payload requesting it (`Graft`) and being promoted, which "self-optimizes the messaging graph by
latency. Note however that this optimization will work best if the messaging paths are stable...
If not, the relative message redundancy will grow and the ideal messaging graph might change
frequently" [38]. Defaults are in the source with their provenance: active view 5, passive view 30
and random walks 6/3/6 "from the paper (p9)", `shuffle_interval` 60 s and
`neighbor_request_timeout` 500 ms both "wild guess", graft timeouts 80/40 ms, dispatch timeout
5 ms, optimization threshold 7 rounds, `message_cache_retention` 30 s and `message_id_retention`
90 s, with the comment "current numbers are guesses that need validation" [38]. No delivery
guarantee is stated in words; what is stated is that cached messages expire — "if this is too low,
other nodes will not be able to retrieve messages once they need them" — and that ids are retained
"to not accidentally receive messages multiple times" [38]. [inference] Best-effort epidemic
broadcast with duplicate suppression and a bounded recovery window; nothing promises delivery to a
peer absent longer than the cache retention.

### Identity, trust and authorization

The identity *is* the key, and the docs state the deviation from ordinary QUIC: "Unlike standard
QUIC there is no client, server or server TLS key and certificate chain. Instead each iroh endpoint
has a unique SecretKey... Since the PublicKey is also used to identify the iroh endpoint it is also
known as the EndpointId... When accepting connections the peer's EndpointId is authenticated.
However it is up to the application to decide if a particular peer is allowed to connect or not"
[17]. Dialling needs the EndpointId, addressing information and the ALPN [17]. The TLS mechanism
moved from a libp2p-style self-signed certificate with a custom extension to TLS raw public keys
(RFC 7250); optional post-quantum key exchange ships in 1.0, while post-quantum *signatures* were
rejected because large keys would force endpoint ids to become hashes of keys [28]. Ed25519 is
load-bearing beyond the handshake: it signs the address records published for lookup and
authenticates the relay protocol [24].

Authorization is the application's, with two hooks: `EndpointHooks` (`before_connect`,
`after_handshake`) and the `Router` incoming filter, which rejects by address, endpoint id or ALPN
— with a measured reason to filter early: "benchmarks on the PR show ~30x throughput for
address-based rejection vs. accepting and closing" [32]. Public relays are unauthenticated; shared
and dedicated relays take endpoint-bound capability tokens derived locally from a project API key,
so the key never reaches the relay, and deleting a key withdraws access including open connections
[23][36]. A relay sees which endpoint ids exchange traffic, when and how much, but not content, and
only until the pair goes direct — and it is not told when that happens, so it "cannot reliably
infer how long two devices communicated" [23].

### Connectivity

The current answer is inside QUIC. Since 0.96.0 (2026-01-27) iroh uses QUIC multipath plus a QUIC
NAT-traversal extension: "this enables iroh to retain multiple network paths simultaneously within
a single QUIC connection", and "we have removed the `conn_type` method... Before, iroh was the one
controlling which path to send on. Now, with QUIC multipath, QUIC is able to handle multiple paths,
and chooses itself which path to send on" [30]. Applications see `Connection::paths()`,
`PathEvent{Opened, Closed, Selected, Lagged}` and per-path RTT and congestion state, each path
being a `TransportAddr::Relay` or `TransportAddr::Ip`; the selection policy is pluggable but
outside semantic versioning [18].

The move was made because the previous arrangement fought the transport: hole-punching packets
lived *outside* QUIC, distinguished by setting the QUIC bit to 0 (which also blocks RFC 9287
greasing), iroh "completely lies to the QUIC stack and tells it to send packets to some private
IPv6 range" and rewrites received packets, and it "somehow coerces the QUIC congestion controller
to restart whenever iroh chooses a new path" [29]. The earlier repair loop was application-level:
an interface and route monitor, a netcheck with STUN/ICMP probes, and a `CallMeMaybe` through the
relay with new candidates, with direct paths re-pinged every 5 seconds — "but the connection will
close if both nodes end up behind NATs" [33].

noq implements `draft-ietf-quic-multipath` (at draft-18 with final IANA numbers),
`draft-ietf-quic-address-discovery` and `draft-seemann-quic-nat-traversal`, with transport
parameters `address_discovery_role` and `max_remote_nat_traversal_addresses` and a default cap of 8
paths [21][22]. iroh's deviations are listed: no address pairings, multipath required, `REACH_OUT`
frames ("our renamed `PUNCH_ME_NOW`"), simplified rounds, and therefore "our transport parameter
has a completely different meaning"; "our approach requires different transport parameter numbers,
meanings, and error handling compared to the draft specification" [30]. Address lookup (renamed
from `discovery` in 0.96.0) resolves an EndpointId through pkarr-over-DNS, DNS, mDNS or the
Mainline DHT, and the advice is to store only the EndpointId and re-resolve, since relay URLs and
direct addresses "can change frequently in P2P networks" [23][30].

Reported success rates, kept apart from restatements: the FAQ says "roughly 9 out of 10 connections
go direct; the relay is only a stepping stone" and "hole-punching works roughly 9 out of 10 times",
with no measurement basis or period [24]. The 1.0 post says "it's normal to see 95% of data
transferred in a connection pass directly between devices" — a share-of-bytes claim — and reports
"more than 200 million endpoints created, in the last 30 days alone" on the public relays [27]. The
relay-pricing post uses 98% direct in an explicitly hypothetical cost model, labelled
"intentionally back-of-the-napkin math, not a universal benchmark", and shows one project's
dashboard at 93.10% [36]. The 2024 libp2p comparison attributes ~70% hole-punch success to libp2p,
citing a Protocol Labs campaign, and claims only a "higher success rate" for iroh, with no figure
[40].

### Reported problems

Browsers are relay-only: "all connections from browsers to somewhere else need to flow via a relay
server. This is because we can't port our hole-punching logic in iroh to browsers: They don't
support sending UDP packets to IP addresses from inside the browser sandbox"; the WASM build is
"the relay-only subset", and the named future options for a direct browser path are "WebTransport
with `serverCertificateHashes`, or WebRTC" [26].

The multipath migration regressed NAT traversal for two releases, and n0 said so: "if you've been
running iroh in environments that need NAT traversal, the last two releases probably felt worse
than 0.95... Connections that used to punch through would sometimes sit on the relay. Paths that
used to recover across a Wi-Fi to LTE switch would stall" — mostly fixed in 0.98 plus noq 0.18
(PTO capped post-handshake, path reset on network change, abandoned-path handling, retried probes,
holepunch frames no longer stuck behind stream data) together with an immediate RTT-based relay
health check instead of waiting up to 5 seconds [32]. 0.96 itself shipped with "a regression in
this release where holepunching is not re-triggered when your network conditions change in most
circumstances" [30].

Hole punching was known to be loss-sensitive before the move into QUIC. In issue #2317 (a
third-party report) a maintainer wrote: "the logs look fine: there are coordinated holepunching
attempts. But nothing makes it through... this issue made me realise (again probably) our
holepunching is rather vulnerable to packet loss", and separately declined to let arbitrary clients
relay because that "would not result in the desired reliability and uptime for our goals" [39].
Multipath and NAT traversal exist only in the fork: "as the diff grew this became increasingly
unlikely [to upstream]. So we decided to fork quinn" [28]. Other documented limits: public relays
have no SLA, support only the latest stable release and are rate-limited [26]; one ALPN per
connection means N protocols cost N connections [28]; dropping an `Endpoint` no longer closes
connections gracefully, so `Endpoint::close()` must be awaited [31]; on Android the default DNS
resolver relies on panic unwinding, so `panic = "abort"` makes the app panic [18]; and `iroh-blobs`
0.103.0 warns "this version of iroh-blobs is not yet considered production quality" [37].

## 3. EMQX MQTT over QUIC

### Identity card

EMQX, Erlang/OTP, Apache-2.0 broker; MQTT over QUIC introduced in EMQX 5.0 and declared
production-ready in EMQX Enterprise 5.1 (2023-06-29) [41][47]. Documentation read is the `latest`
tree, whose Docker example pins `emqx/emqx:5.8.8` [41][43]. The QUIC stack is Microsoft msquic via
the Erlang NIF `quicer` in `emqx/quic` (Apache-2.0, OTP 25+), whose README still says "Project
Status: Preview" [46]. Standardisation, in EMQX's words: "for now, MQTT over QUIC is not yet the
standard protocol for MQTT, but it has the capability to be deployed in production, and EMQ is
actively driving its standardization process within OASIS" [41]; the 2022 post adds that EMQ is
"preparing a draft proposal" [44]. No OASIS standard or IETF draft was found.

### Message-to-transport mapping

Deliberately minimal: "the current implementation of EMQX replaces the transport layer with a QUIC
stream, where the client initiates the connection and creates a bi-directional stream" [42]. MQTT
packets are unchanged. The listener is ordinary — `listeners.quic.default` with
`bind = "0.0.0.0:14567"`, `keyfile`, `certfile` — and disabled by default [43].

**Single-stream mode** "encapsulates the MQTT packets in a single bi-directional QUIC stream"
[42]. **Multi-streams mode** splits it: "the initial stream that is established from the client to
EMQX is referred to as the control stream. Its purpose is to handle the maintenance or update of
the MQTT connection. Following this, the client can initiate one or multiple data streams to
publish topics or subscribe to topics, per stream" [42]. Grouping is the client's choice — "use one
stream per topic", "one stream for QoS 1 and another for QoS 0", "one stream for publishing and
another for subscriptions" — while the broker binds packets to streams: "it sends PUBACK packets
over the stream where it receives the PUBLISH for QoS 1... It sends PUBLISH packets over the stream
where it gets the topic subscription and also expects PUBACK for QoS1 from the same stream" [42].
The ordering consequence is documented: "the order of data is maintained per stream, hence, if
there are two topics whose data is correlated and ordering is crucial, they should be mapped to the
same stream" [42].

There is no MQTT-level fragmentation: a large PUBLISH is a large payload on one stream, which is
the failure mode multi-stream exists to contain. Datagrams are unused; EMQX lists "broker-side
stream prioritization, broker-side flow control, and unreliable datagram" as features it "has not
utilized" [41].

### Flow control and backpressure

MQTT's own mechanisms sit unchanged on QUIC's per-stream and per-connection credit; what
multi-stream adds is separability: "flow control can be applied per data stream, allowing for
different flow control policies for different topics or QoS levels" [42]. Stream priority is a
client-side per-stream setting — the benchmark uses `"stream": 1, "stream_priority": 200` for a
small high-rate topic and `"stream": 2, "stream_priority": 8` for a large slow one, with stream 0
carrying `PINGREQ` at the highest priority [45].

The overload behaviour is documented as a measured failure. In the multi-stream post (2025-04-18,
vendor benchmark) a producer sends 1 KB messages ten times per second on `Topic1` and a 100 MB
message every 5 seconds on `Topic2` through an iptables limit of 29 packets per second, with a 60 s
MQTT keep-alive [45]:

- TCP/TLS, one connection: publishing stops after 6 s "that is when the 'big message' kicks in",
  the connection closes after 2 minutes, and the consumer receives 49 `Topic1` messages in total.
- QUIC, single stream: publishing also stops after 6 s, but "it is NOT due to the QUIC connection
  being closed; rather, it stems from the MQTT protocol layer's keep-alive timeout... There is
  nothing wrong with the QUIC connection itself; the real problem is the Head-of-Line (HOL)
  blocking that prevents the large message from being delivered in time to maintain the keep-alive
  deadline on the EMQX side." The consumer again receives 49 messages.
- QUIC, multi-stream (`Topic1` on stream 1 at priority 200, `Topic2` on stream 2 at priority 8):
  publishing continues for the whole run and the consumer is still receiving about 10 messages per
  second after 2m44s, roughly 1495 messages in total.

The same post names the trade-off — "while message ordering is guaranteed within a single stream,
it is not assured across different streams" — and the pathology single-stream congestion produces:
a large message that cannot finish inside the keep-alive interval gets the connection torn down,
after which "the client can reconnect and attempt to retransmit the large message. Unfortunately,
it is likely to time out again, resulting in the client getting stuck in a reconnect loop" [45].

### Delivery guarantees

MQTT semantics are retained in full — "while retaining compatibility with all MQTT protocol
features" — so QoS 0/1/2 and their acknowledgements are certified at the MQTT layer, not by QUIC,
while ordering scope shrinks from per-connection to per-stream [41][42]. Two limitations are
documented, and they are the interesting part:

> Preserving session state is currently not supported. This means that if a client needs to
> reconnect, it must resubscribe to the topics it previously subscribed to over a data stream.
>
> If the data stream is closed unexpectedly by either peer, the QoS 1 and QoS 2 message states are
> not preserved. [41]

So the QUIC listener trades MQTT session persistence for transport benefits, and a stream reset
loses in-flight QoS state that a TCP connection would have kept; the stated future work is exactly
that: "further investigation is also required on how to preserve the message states and resume the
subscription without reconnection" [41]. 0-RTT is used for reconnection with a documented default
and caveat: "EMQX sends NST [new session ticket] packets to the client by default, with a validity
of 2 hours", and "since 0 RTT early data is not protected against replay attacks, QUIC recommends
not carrying data on 0 RTT that would change the application state... EMQX does not support early
data by default" [44].

### Identity, trust and authorization

Unchanged from MQTT over TLS: the listener needs `keyfile` and `certfile`, and QUIC "inherently
includes security features equivalent to TLS. This means MQTT over QUIC will always be encrypted
and authenticated without requiring additional configuration" [42][43]. Client identity remains
MQTT credentials with EMQX's authentication and authorization chains; there is no key-as-identity
mechanism and no per-stream identity. Because a client may send SUBSCRIBE or PUBLISH before
CONNACK on a data stream, the authorization ordering rule is explicit: "EMQX will only begin
processing them after the client has established a connection and while the connection is allowed"
[42].

### Connectivity

Migration is transparent to MQTT: "without disconnecting and establishing a new QUIC connection,
the client is enabled to actively or passively migrate its local address to a new address due to
Network Address Translation (NAT) rebinding. The QUIC connection could be kept without major
disturbances, so to MQTT layer and above" [42]. The 2022 vendor benchmark measured it: with
TCP/TLS an address change requires the application to detect a disconnect, and "this process is
very slow due to various timers and involves many unnecessary retransmissions", whereas "QUIC's
processing is smoother, keeping connections alive when the address is switched without requiring
reconnections and leaving the application to no perception" [44].

Fallback is a client responsibility and treated as mandatory: "as QUIC is based on the UDP
protocol, many operators still have special routing strategies for UDP packets, often leading to
QUIC connection failures or packet losses. Therefore, MQTT over QUIC clients are designed with a
fallback feature... When QUIC is unavailable, it automatically switches to TCP/TLS 1.2" [43]. There
are no relays and no discovery: this is client-to-broker. Client support is the practical
constraint: NanoSDK (C, on msquic) with Python and Java bindings, `emqtt` (Erlang), `nanomq_cli
quic` as a test tool, and NanoMQ as an edge bridge converting MQTT/TCP into MQTT over QUIC "for
end-side IoT devices that are hard to integrate or lack a suitable MQTT over QUIC SDK" [43].

### Reported problems

The vendor benchmark reports where QUIC lost: "QUIC outperforms TLS in terms of CPU and memory
usage, but reconnection consumes more bandwidth than TLS" — CPU ~60% versus ~80% on first
connection and ~65% versus ~75% on reconnect, maximum memory 9 GB versus 12 GB, but bandwidth
peaking at 100 Mb against 30 Mb, attributed to "the large number of QUIC initial handshake packets
due to transport path MTU validation" [44]. Latency gains are RTT-conditional: "with 1ms roundtrip
time, QUIC and TLS do not show that many differences in latency performance. As the latency grows,
30ms roundtrip time, QUIC outperforms TLS a lot" (5000 clients, one node on an AWS m4.2xlarge, P95)
[44]. Beyond the two documented limitations, broker-side prioritization, broker-side flow control
and datagrams are unused [41], and the Erlang binding is still Preview [46]. The joint EMQ/Intel/
SJTU post (2023-10) reports the shape rather than a number: performance is comparable when the
network is ideal, and MQTT over QUIC "maintain[s] a remarkable level of stability in the face of
network fluctuations" as loss increases [48].

## 4. Media over QUIC (MoQ) and WebTransport-based messaging

### Identity card

Media over QUIC Transport (MOQT) is the IETF moq working group's publish/subscribe protocol;
the revision read is **draft-ietf-moq-transport-21, 2026-09-08**, 159 pages, with the milestone
"Dec 2026 Request publication of Publication and Subscription Protocol to IESG" [49][50]. Its
abstract sets the scope: MOQT "is a publish/subscribe protocol that runs over QUIC and
WebTransport. MOQT leverages the features of these transports, such as streams, datagrams,
priorities, and partial reliability. MOQT operates both point-to-point and through intermediate
relays... Despite its name, MOQT is media agnostic and can be used for a wide range of use
cases" [49]. Six further drafts are adopted (secure objects, two token schemes, streaming
formats) plus thirty individual ones [50]; two matter here:
**`draft-lcurley-moq-lite-05`** (2026-06-30, Informational, "not endorsed by the IETF"), a
deliberate simplification [52], and **`draft-jennings-moq-mocha-chat-00`** (2026-07-06),
literally "Messaging over MoQ Transport" [51]. WebTransport is the other half:
`draft-ietf-webtrans-http3-16` and `-overview-13` (both 2026-07-06, WG Last Call) with the W3C
API at Candidate Recommendation Snapshot 2026-07-30 [53][54][55]. Implementations: `moq-net`
0.2.19 (2026-09-09) and `@moq/net` 0.3.5, negotiating moq-lite by default and "moq-transport
drafts 14+ via version negotiation" [57]; `wtransport` 0.7.2 [60]; `webtransport-go` v0.13.0
[61]; Cloudflare operates public relays [58][59].

### Message-to-transport mapping

"MOQT has a hierarchical data model, comprised of tracks which contain groups, and groups that
contain objects. Inside of a group, the objects can be organized into subgroups" [49]. An
**object** is "an addressable unit whose payload is a sequence of bytes", identified by track
namespace, track name, group ID and object ID, and immutable — it "must be an identical sequence
of bytes regardless of how or where it is retrieved. An Object can become unavailable, but its
contents MUST NOT change over time" — with metadata always visible to relays and the payload
optionally end-to-end encrypted [49]. An object is known-not-to-exist (permanent, and "all
signals that an Object does not exist are authoritative"), known-to-exist, or unknown, and "a gap
in the observed Object IDs does not by itself convey any information about the skipped Objects"
[49].

A **subgroup** is "a sequence of one or more objects from the same group in ascending order by
Object ID" whose objects "have a dependency and priority relationship consistent with sharing a
stream and are sent on a single stream whenever possible", so "a Group is delivered using at
least as many streams as there are Subgroups" [49]. The rule binds both ways — "Objects from two
subgroups MUST NOT be sent on the same stream, and Objects from the same Subgroup MUST NOT be
sent on different streams, unless one of the streams was reset prematurely" — and the guidance is
explicitly a cost trade: "when assigning Objects to different Subgroups, the Original Publisher
makes a reasonable tradeoff between having an optimal mapping of Object relationships in a Group
and minimizing the number of streams used" [49]. A **group** "provides a join point for
subscriptions" [49].

Session stream usage is fixed: "MOQT uses a pair of unidirectional streams for creating the
session and exchanging control messages... rather than a single bidirectional stream [which]
allows either peer to send data as soon as it is able" [49]. Bidirectional streams carry
requests, each starting with `TRACK_STATUS`, `SUBSCRIBE`, `PUBLISH`, `FETCH`,
`PUBLISH_NAMESPACE`, `SUBSCRIBE_NAMESPACE` or `SUBSCRIBE_TRACKS`; objects travel on
unidirectional `SUBGROUP_HEADER` or `FETCH_HEADER` streams; request ids are client-even and
server-odd, and responses carry none because they share the request's stream [49]. The datagram
mapping is the lossy alternative — "a single object can be conveyed in a datagram", and "when the
total size is larger than the maximum datagram size for the session, the Object will be dropped
without any explicit notification" — a multi-hop hazard, because each session may have a
different limit and relays may add properties, "increasing the size of the Object and the chances
it will exceed the maximum datagram size of a downstream session and be dropped" [49].

MOCHA Chat shows the model with messages as payload: each device publishes on its own track in a
channel namespace (track `msg_v1_<HDevID>`) because "this per-device track design allows each
device to publish independently without coordinating Group IDs with other publishers"; groups are
wall-clock minutes (`group_id = floor(ntp_timestamp_seconds / 60)`), giving "natural time-based
partitioning of messages", a direct wall-clock-to-Group-ID mapping for history retrieval and a
caching unit; "each message occupies exactly one MOQT object", with compressed JSON payloads whose
algorithm is signalled by a track property subscribers MUST check [51]. moq-lite renames and
shrinks the same idea — Session > Broadcast > Track > Group > **Frame** — with one bidirectional
stream per request type, unidirectional group streams, and a datagram capped at 1200 bytes
carrying one single-frame group, never cached and deduplicated by group sequence [52].

WebTransport itself has no message primitive: the overview draft defines a Message only as "a
stream that is sufficiently small that it can be fully buffered" [54], and the W3C API states that
stream data is "an undifferentiated sequence of bytes", that writes may merge or split, and that
"applications that depend on having a message-based protocol that preserves message boundaries...
need to add a framing layer" [55].

### Flow control and backpressure

MOQT adds no credit scheme. An object "is not schedulable if it is known that no part of it can
be written due to underlying transport flow control limits", and resource limits are the
transport's: "MOQT uses stream limits and flow control to impose resource limits at the network
layer. Endpoints SHOULD set flow control limits based on the anticipated bitrate" [49]. It does
document the deadlock a layered scheme creates and the rule that avoids it: "to prevent
deadlocks, endpoints MUST allocate connection flow control to the control streams before
allocating it to any data streams. Otherwise, a receiver might wait for a control message
containing a Track Alias to release flow control, while the sender waits for flow control to send
the message" [49]. The one application credit is narrow: `MAX_REQUEST_UPDATES` bounds
unacknowledged `REQUEST_UPDATE`s per request stream, each response restoring one credit — and the
change log shows an earlier `MAX_REQUEST_ID`/`REQUESTS_BLOCKED` credit scheme being *removed*
first [49]. Slow subscribers have a named outcome instead of unbounded buffering: "if a subscriber
fails to consume Objects at a sufficient rate, causing the publisher to exceed its resource
limits, the publisher MAY terminate the subscription using PUBLISH_DONE with error
TOO_FAR_BEHIND"; starvation is likewise accepted, with "the publisher and subscriber MUST cancel
a stream, preferably the one with the lowest priority, after reaching a resource limit" [49].

WebTransport does have session-level credit, opt-in and mutual: enabled "only if BOTH endpoints
send a non-zero `SETTINGS_WT_INITIAL_MAX_STREAMS_UNI`, `..._BIDI` or
`SETTINGS_WT_INITIAL_MAX_DATA`", without which a client "MUST NOT open more than one session per
connection" [53]. Its capsules travel on the CONNECT stream and are therefore strictly ordered,
so "a non-increasing value is `WT_FLOW_CONTROL_ERROR`" — unlike QUIC's own frames — and
`WT_MAX_DATA` counts stream *body* bytes only so that associating a stream with its session can
never be blocked [53]. For relayed designs the key property is that the credit is hop-by-hop:
"intermediaries MUST consume the flow-control capsules and re-express their own limits, and are
responsible for storing data they granted credit for" [53]. In the browser API the incoming
datagram queue drops from the head when full rather than exerting backpressure, the outgoing one
exerts backpressure, and an oversized write "resolve[s] silently without sending" [55]. moq-lite
likewise "relies on QUIC/WebTransport stream limits and flow control for backpressure", resets
expired group streams "to avoid consuming flow control", and adds an explicit probe stream because
"not all QUIC implementations and browser WebTransport APIs expose RTT statistics directly" [52].

### Delivery guarantees

MOQT's guarantee is not "delivered" but "delivered while still useful", implemented with stream
resets. A priority number is 0-255 where "a lower priority number indicates higher priority";
Subscriber Priority is per request and updatable, Publisher Priority defaults per track and can be
overridden per subgroup or datagram, and Group Order is per subscription and "cannot be changed"
[49]. Scheduling is a four-level tie-break (subscriber priority, publisher priority, group order,
lowest subgroup or object ID, datagrams winning at equal priority) with two admissions: it "does
not provide a well-defined ordering for objects that belong to different subscriptions or FETCH
responses, but have the same subscriber and publisher priority", and "scheduled to be sent
first... is implementation dependent and is constrained by the prioritization interface of the
underlying transport". Control streams "SHOULD be prioritized highest, followed by the bidi
request streams and then all Objects" [49].

Partial reliability is two timeouts. `OBJECT_DELIVERY_TIMEOUT` starts at the last header byte and
is checked "before attempting to pass it to the underlying transport"; on expiry the
implementation "MUST reset the underlying transport stream with the reset stream code
DELIVERY_TIMEOUT and SHOULD NOT attempt to open a new stream to deliver additional Objects in
that Subgroup", while datagrams "MUST" be dropped [49]. `SUBGROUP_DELIVERY_TIMEOUT` starts when
the subgroup's FIN is known and runs until all data is committed, so that "MOQT can time out
subgroups where all of the data has been sent but not yet fully delivered due to packet loss"
[49]. Both are optional, settable by either side with the smaller non-zero value winning, and
implementations "SHOULD minimize the amount of data buffered at the underlying transport layer,
as any data buffered at this layer can no longer be timed out" [49]. With no timeout set, the
guarantee is complete delivery [49].

Duplicates are handled by identity: because objects are immutable and addressed by name plus
group and object ID, "processing the same Objects multiple times is idempotent, as the subscriber
or relay can identify and discard duplicates based on the Group ID and Object ID" [49]. Caching
relays key on that triple, may ignore later copies, and "an endpoint that receives a duplicate
Object with a different Forwarding Preference, Subgroup ID, Priority or Payload MUST treat the
track as Malformed"; with multiple publishers a relay "SHOULD attempt to deduplicate Objects
before forwarding" [49]. Ordering scope is the subgroup [49].

MOCHA Chat layers application ordering on top: each message carries a `prev` list forming "a DAG
across the per-device tracks in a channel", linearized by topological sort, then timestamp, then
message id; edits and deletes are new objects carrying `replaces`, and "relays forward edit and
delete objects like any other message object without interpreting their semantics" [51]. moq-lite
states its model in one sentence — tracks deliver groups out of order, groups deliver frames in
order, and "a subscriber MUST handle gaps, potentially caused by congestion" even with `ordered=1`
[52]. WebTransport's own controls are per stream: `RESET_STREAM_AT` with a reliable prefix so the
session id survives a reset, and a `sendOrder` that is explicitly local — "this is sender-side
data prioritization which does not guarantee reception order" [53][55].

### Identity, trust and authorization

MOQT is hop-by-hop: relays "are endpoints, which means they terminate Transport Sessions in order
to have visibility of MOQT Object metadata", so "the relays within the chain... will have access
to Track names, Track Properties, Object Properties, as well as the object's content unless it is
end-to-end encrypted" [49]. Two combinable mechanisms are defined: mutual TLS, where the mapping
from certificate to identity and any attribute-based policy is "out of scope", and authorization
tokens as message parameters, with Privacy Pass and Common Access Token schemes specified, a
negotiated token cache, and replay protection left to "the specific token scheme" [49][50]. The
traps are named: "relays that aggregate subscriptions from multiple downstream subscribers MUST
ensure each subscriber is independently authorized", and "a relay MUST ensure that a client cannot
publish to namespaces or tracks belonging to another identity... A relay that does not enforce
these checks allows any connected client to inject content into arbitrary namespaces" [49].
End-to-end protection is a layer above — publishers "can apply end-to-end object encryption, for
example using Secure Objects, so that relays retain access only to the metadata required for
forwarding" — and even then "object sizes, sizes of request messages, etc can make it possible for
a third party observer to identify media content, user patterns and media stream origin" [49].

On the WebTransport side identity is HTTP identity: `https` only, `Origin` required and verified
for browser clients, session addressed by (authority, path) [53][54]. The API authenticates only
the server: `serverCertificateHashes` permits a non-Web-PKI leaf restricted to ECDSA P-256 whose
"total validity period MUST NOT exceed two weeks", with no revocation and the explicit statement
that it "does not provide any means of authenticating the client. The application has to establish
the identity of the client in-band if necessary" [55]. libp2p's WebTransport specification is the
worked example: a Noise handshake on the first stream, with two overlapping 14-day certificates
whose hashes are advertised in the multiaddr [64]. moq-lite leaves authentication out of the
protocol, uses tokens in the implementation, and notes that hop ids leak relay topology while
"Exclude Hop is a loop-avoidance hint, not access control" [52][57].

### Connectivity

Long-lived state forces an explicit migration mechanism rather than reliance on transport
migration: "MOQT requires a long-lived and stateful session. However, a service provider needs
the ability to shutdown/restart a server without waiting for all sessions to drain naturally, as
that can take days for long-form media. MOQT enables proactively draining sessions via the GOAWAY
message" [49]. `GOAWAY` on the control stream migrates the session (optionally to a new URI) with
a timeout after which the sender closes with `GOAWAY_TIMEOUT`; on a request stream it migrates
only that request; and the recommended client behaviour is invisible migration — "ideally this is
transparent to the application using MOQT, which involves establishing a new session in the
background and migrating Established subscriptions and published namespaces" [49].

Establishment differs by transport: over WebTransport the client turns `moqt://` into `https://`
and sends extended CONNECT with MOQT identifiers in `WT-Available-Protocols`; over native QUIC it
connects directly and carries authority, path and query in Setup Options [49]. Because control,
request and object streams are independent, "a client can initiate a MOQT session, subscribe, and
start publishing Objects all in parallel", with early data buffered or reset [49].

0-RTT gets an explicit replay analysis whose conclusion follows from the object model: "MOQT
Messages and Objects as defined in this draft are safe to replay in most circumstances", since
`SUBSCRIBE` "requests Objects be delivered, but does not change the Objects being requested" and
replayed objects are idempotent [49]. The residual risk is load — a replayed `SUBSCRIBE_TRACKS`
"could cause the Relay to receive a number of new Subscriptions on the replaying client's behalf"
— so "relays MAY defer initiating upstream subscriptions until the handshake is complete or reject
0-RTT entirely" [49]. WebTransport cannot use 0-RTT for session creation at all, "because
initializing a WebTransport session uses CONNECT, which is not a safe method" [49][53].

Idle sessions are a documented problem with three options — QUIC PING keep-alives, periodic no-op
control messages, or "accept that idle connections can close and implement reconnection logic when
needed" — since the connection "can close due to idle timeout if no data is exchanged", including
"publisher sessions that have issued a PUBLISH_NAMESPACE and are waiting for subscribers" [49].
Discovery is namespace-based, with prefix filters and a section on relay resource protection in
large namespaces, and DNS/mDNS discovery in a separate individual draft [49][50]. Relay
pre-warming is a trade: forwarding upstream with no downstream subscriber "reduces latency but
consumes upstream and publisher resources for content no downstream subscriber is currently
receiving" [49]. moq-lite adds non-QUIC reachability itself — bare QUIC, WebTransport, and "Qmux"
over TCP/TLS or WebSocket "when UDP is unavailable" — and its relay added "a WebSocket fallback
for Safari/TCP support" [52][58].

### Reported problems

MOQT's change log is the evidence of churn: `MAX_REQUEST_ID`/`REQUESTS_BLOCKED` removed, a
`PUBLISH_BLOCKED` message added, `TOO_FAR_BEHIND` introduced, `MAX_REQUEST_UPDATES` added, group
and subgroup terminology clarified, multiple concurrent subscriptions per track allowed, and a new
variable-length integer encoding [49]. The security section still carries "TODO: Describe Cache
Poisoning attacks" [49]. Datagram objects can be dropped silently for size anywhere on the path,
priorities are only as good as the transport's scheduling interface, and starvation is accepted
with cancellation as the remedy [49].

The sharpest criticism is internal, in moq-lite's rationale: "MoqTransport has become too
complicated. There are too many messages, optional modes, and half-baked features"; the author
supports the working group's goals but says "the standardization process is hindering practical
experimentation", and calls moq-lite "the bare minimum needed for a real-time application aiming
to replace WebRTC... This draft is the current state, not the end state" [52]. The accompanying
post quantifies the friction as "650+ issues and 500+ PRs on moq-transport" [58]. Interop is
partial by construction: `moq-net` exposes "the intersection of features supported by both
protocols", uses sub-group 0 for everything and silently drops frames from other subgroups when
speaking moq-transport [57].

WebTransport's problems are deployment-shaped. The binding is still in WG Last Call and negotiates
draft versions by changing the `SETTINGS_WT_ENABLED` codepoint per revision; draft-16 newly
*requires* `RESET_STREAM_AT` from both endpoints [53]. `congestionControl` is "a hint to the user
agent", flagged a feature at risk "due to the lack of implementation in browsers of a congestion
control algorithm, at the time of writing, that optimizes for low latency" [55]. Pooling costs
observability, and `serverCertificateHashes` is incompatible with `allowPooling` [55]. Browser
support is uneven per option: the interface is Chrome 97 / Firefox 114 / Safari 26.4, but
`allowPooling`, `requireUnreliable` and `congestionControl` are unsupported in Chrome and
`options.protocols` landed only in Chrome 143 and Firefox 155 [56]. The capsule variant that lets
HTTP/2 intermediaries work loses the property that made WebTransport attractive: all WebTransport
streams share one HTTP/3 stream, so head-of-line blocking returns and datagrams become
retransmitted [53].

The most striking finding about messaging on WebTransport is how thin it is. The transport
libraries are mature — `wtransport` 0.7.2, whose README says the library "is not considered
completely production-ready", and `webtransport-go` v0.13.0, listing any-sync, Centrifugo,
go-libp2p, MediaMTX, SignalR-for-Go and a Go Socket.IO as users [60][61] — but both messaging
products that ship WebTransport reduce it to one stream. Centrifugo, still labelling it
experimental and "not recommended for production usage", says "we utilize a single bidirectional
stream of WebTransport to pass our protocol between client and server" [62]; Engine.IO makes it
normative: "a client MUST NOT open more than one WebTransport stream per session. Should it
happen, the server MUST close the WebTransport session" [63]. [inference] Both therefore gain
QUIC's connection properties and neither gains stream multiplexing or datagrams. RPC over
WebTransport is at the request stage: no gRPC-over-WebTransport specification exists, Connect's
web packages ship Connect and gRPC-Web only, and an open Effect-TS issue frames it as "picking
WebTransport means dropping out of RPC abstractions entirely and writing a bespoke transport";
game engines have no built-in support, with the Godot proposal open since 2022-01-31 [65].

## 5. HTTP/3-native RPC

### Identity card

gRPC's wire contract is `PROTOCOL-HTTP2.md` in `grpc/grpc` (undated, read from `master`) [66]; the
HTTP/3 mapping is gRFC **G2**, status `Implemented`, last updated 2021-08-25, whose only named
implementation is grpc-dotnet [67]. Connect is a separate protocol declaring
`connect-protocol-version: 1` with no document date [73]. RFC 9114 (June 2022) supplies the HTTP/3
facts [75].

### Message-to-transport mapping

One RPC per HTTP stream, and G2 changes nothing: the request/response shape, the `application/grpc`
content type and the message framing are unchanged for HTTP/3, with HTTP/3 stream ids taking the
same role [66][67]. A message is a `Length-Prefixed-Message` — one flag byte, a four-byte
big-endian length, then bytes — application framing unrelated to DATA-frame boundaries [66]. RFC
9114 §4.1 confirms HTTP/3 carries trailers as a trailing HEADERS field section and that each
request/response uses one client-initiated bidirectional stream [75]. Deadline expiry maps to
`RESET_STREAM` with `H3_REQUEST_CANCELLED` plus `STOP_SENDING`; G2's own caveat is that
out-of-order reception can let a reset sent after a complete response abort that response [67].
Connect avoids trailers entirely: unary is one bare body with no envelope and ordinary HTTP status
codes, while streaming uses enveloped messages, always starts with HTTP 200, and ends in an
`EndStreamResponse` envelope carrying the error and trailing metadata [73]. [inference] That puts
the terminal outcome in ordinary response bytes rather than in a trailing field section.

### Flow control and backpressure

Neither adds credit. gRPC suggests an 8 KiB default maximum for headers and trailers and maps
`ENHANCE_YOUR_CALM` to `RESOURCE_EXHAUSTED`, as G2 does for `H3_EXCESSIVE_LOAD` [66][67]. RFC 9114
notes HTTP/2's `FLOW_CONTROL_ERROR` "is not applicable" because QUIC handles flow control [75].
Concrete limits are the server's: Kestrel defaults to 100 inbound bidirectional and 10
unidirectional streams per connection, a 1 MiB read buffer and a 512-connection backlog [70].
Connect signals overload in-body or as HTTP 429/503 [73].

### Delivery guarantees

Ordering is stream-local [75]. gRPC claims no exactly-once execution: calls are not assumed
idempotent, calls not proven to have started are not retried, idempotent-marked calls may be sent
more than once, and there is no duplicate suppression [66]. The completion certificate is
`grpc-status` in trailers, required even for OK with `:status` 200 — precisely the behaviour an
HTTP/3 implementation must preserve [66]. Connect's terminal signal is the mandatory
`EndStreamResponse`, including an empty one on success, and its guidance is that no error code is
universally safe to retry [73].

### Identity, trust and authorization

Neither defines RPC-level identity. G2 requires TLS and states HTTP/3 "is never used by an insecure
channel"; `h3` selects HTTP/3 by ALPN [67][75]. Connect distinguishes `unauthenticated` (401) from
`permission_denied` (403) without prescribing a mechanism [73].

### Connectivity

G2 defines HTTP/3-only clients, which fail if the server lacks HTTP/3, and HTTP/2-or-greater
clients, which call over HTTP/2 first, accept an `alt-svc` advertisement, then replace the
connection [67]. [inference] A channel pinned to HTTP/3 has no TCP fallback within that attempt;
Alt-Svc discovery over HTTP/2 is the documented path. Microsoft documents the same behaviour for
.NET, while recommending `Http1AndHttp2AndHttp3` on the server "because routers, firewalls, and
proxies may not support HTTP/3" [70][71]; RFC 9114 makes the same recommendation for blocked UDP
[75]. One capability is documented as missing: .NET's own documentation states that `HttpClient` and
Kestrel did not support QUIC network transitions in .NET 7, "although HTTP/3/QUIC permits such
transitions", and advises disabling HTTP/3 if problems arise [72].

### Reported problems

grpc-go's HTTP/3 request (#5186) was closed on 2022-02-08 after a maintainer said HTTP/3 was not in
the team's foreseeable plans [68]. The cross-language issue grpc/grpc #19126 was opened 2019-05-23
and was still open when read; its maintainer comments record that the Cronet-based HTTP/3 available
to C++/ObjC/Java is client-only and normally needs "a load-balancing proxy speaking QUIC/HTTP/3
then HTTP/2 to the gRPC server", and that no in-tree general implementation plan had been noticed
[69]. grpc-dotnet is the exception: Kestrel HTTP/3 is documented as fully supported in .NET 7+,
with the .NET 6 client requiring a `SocketsHttpHandler.Http3Support` switch and libmsquic 1.9.x
[70][71][72]. Connect's stated cost is being "effectively two protocols", conceded as "less
conceptually pure"; bidirectional streaming still requires HTTP/2, and the FAQ says explicitly that
not all Connect implementations have HTTP/3 support [74]. No maintained gRPC-over-HTTP/3 stack for
Rust was found; the `h3` crate at 0.0.8 proves only that an HTTP/3 library exists [76].

## 6. Tier 2 systems

### 6.1 Quilkin

**Identity card.** Rust, Apache-2.0, `googleforgames/quilkin` (originally Embark Studios),
workspace version 0.10.1 (`quilkin-v0.10.0` 2026-01-27), README status "currently in *beta*
status ... being used in production systems, but the API may break"; described as "a
non-transparent UDP proxy specifically designed for use with large scale multiplayer dedicated
game server deployments" [77].

**Mapping.** No message abstraction: raw UDP datagrams, and "exactly one filter chain is
specified and used to process all packets that flow through Quilkin", traversed in reverse for
upstream-to-downstream packets, with per-packet dynamic metadata carrying state between filters
[78]. `Capture` extracts bytes by suffix, prefix, regex — or by "QUIC Destination Connection ID";
`TokenRouter` matches the captured token against endpoint tokens under `quilkin.dev/tokens` and
forwards only on match [78].

**Flow control and delivery.** None: excess is dropped. `LocalRateLimit` enforces `max_packets`
per `period` per source and is documented as inexact by up to N-1 packets with N threads; sessions
time out after 60 s and are capped at 5000 [78][79]. `Firewall` rules are ordered, first match
winning, and "if none of the configured rules match, then the request is denied" [78].

**Identity and connectivity.** Authorization is the routing token: a client proxy adds it with
`Concatenate`, the server proxy captures and routes it, and tokens arrive as Agones annotations or
xDS endpoint metadata; there is no cryptographic peer identity on the data path [78]. Topologies
are sidecar, client-proxy-to-sidecar, and client proxy to pools of proxies fed by xDS, with game
servers on private IPs; QCMP ("Quilkin Control Message Protocol", not QUIC) runs on port 7600 over
UDP and TCP with ping/reply timestamps feeding a latency map keyed by ICAO code [78].

**QUIC verdict, verified in three places.** The data path does not terminate QUIC: the only QUIC
awareness is a parsing rule deriving header type from bit 0x80 of the first byte per RFC 9000 and
reading the DCID at a fixed offset, never removing bytes [79]. The xDS control plane is gRPC over
HTTP/2 on TCP with no QUIC dependency [77]. QUIC *is* used for node-to-node state replication: the
corrosion crate depends on `quinn` 0.11 and `quinn-plaintext` 0.3 and maps three traffic classes
onto three primitives — SWIM gossip over datagrams, `Broadcast` over unidirectional streams,
`Sync` over bidirectional streams — with a 15-second keep-alive ("half the time of the idle
timeout set on the server") and one cached connection per socket address [80].

**Reported problems.** The FAQ refuses numbers: "we won't be publishing performance benchmarks,
as performance will always change depending on the underlying hardware, number of filters,
configurations and more. We highly recommend you run your own load tests" [78]. Console deployment
is unresolved and NDA-bound, and the documentation has drifted (four linked service pages do not
exist; one metadata key is documented as `quilkin.dev/captured` while code uses
`quilkin.dev/capture`) [78]. The Google Cloud announcement (2021-07-16) frames the goal as moving
work off the game loop — "remove non-game specific computation out of your game server's
processing loop" — plus hiding the server address and allowing redundant entry points; no source
read argues UDP against TCP or QUIC for the game data path itself [78][81].

### 6.2 libp2p QUIC transport

**Identity card.** `quic/README.md` revision r1 (2022-12-30), maturity "Recommendation" [82],
with `tls/tls.md` r0 (2019-03-23) [83], `peer-ids/peer-ids.md` r2 (2021-04-30) [84] and
`connections/README.md` r1 (2022-12-07) [85].

**Mapping.** "Since QUIC already provides an encrypted, stream-multiplexed connection, libp2p
directly uses QUIC streams, without any additional framing", and "libp2p only supports
bidirectional streams" [86]. Crucially, "there is no additional security handshake and stream
muxer needed as QUIC provides all of this by default. This also means that establishing a libp2p
connection between two nodes using QUIC only takes a single RTT" — the multistream-select round
trips for security and muxer selection disappear, leaving only per-stream application-protocol
negotiation [85][86]. ALPN is fixed to `libp2p`, and "QUIC enforces the use of ALPN, so the
handshake will fail if both peers can't agree" [82]. Multiaddrs encode the QUIC version
(`/quic-v1` versus `/quic` for draft-29), which before that code point were indistinguishable
[82].

**Flow control and delivery.** QUIC's, with implementation-pinned windows: go-libp2p uses 256
incoming streams, 5 incoming unidirectional streams, 10 MB stream and 15 MB connection receive
windows and a 15 s keep-alive, with datagrams enabled only because WebTransport needs them [87];
rust-libp2p uses 256 concurrent streams, 15 MB connection and 10 MB stream data ("ensure that one
stream is not consuming the whole connection") and disables unidirectional streams and datagrams
outright [88]. Above that, the connections specification recommends "an upper bound on the number
of open connections" with eviction of "expendable" ones, and go-libp2p added a resource manager in
v0.18.0 to "configure limits on connections, streams, and memory usage" [85][90]. Delivery is per
stream, streams
"must support backpressure", and the stated benefit is loss isolation: "if a packet that contains
stream data for one stream is lost, this only blocks progress on this one stream" [85][86].

**Identity.** The peer id is a multihash of the public key, with a rule that makes small keys
self-describing: "keys that serialize to more than 42 bytes must be hashed using sha256 multihash,
keys that serialize to at most 42 bytes must be hashed using the 'identity' multihash codec", so
an Ed25519 key is inlined verbatim [84]. The handshake proves host-key possession: a self-signed
certificate carries the libp2p Public Key Extension (OID `1.3.6.1.4.1.53594.1.1`) with
`SignedKey{publicKey, signature}` signed over `libp2p-tls-handshake:` plus the certificate's DER
`SubjectPublicKeyInfo` — "cryptographic proof that the peer was in possession of the private host
key at the time the certificate was signed" — and clients "MUST verify that the peer ID derived
from the certificate matches the peer ID they intended to connect to" [83]. Mutual authentication
is mandatory and TLS 1.3 the floor; one caveat is recorded, that the client finishes before the
server verifies, so "the client can already send application data" that is then discarded [83].

**Connectivity.** QUIC is recommended "due to its inherently faster handshake latency (a single
network-roundtrip)", with the drawback in the same paragraph: "however, UDP is blocked in a small
fraction of networks, therefore it is RECOMMENDED that libp2p nodes offer a TCP-based connection
option as a fallback" [82]. NAT traversal is a stack of named specifications — hole punching r1
(2022-06-13), AutoNAT v2 r2 (2023-04-15), Circuit Relay v2 r3 (2023-02-28), DCUtR r1 (2021-11-20),
the last calling relays "a reliable fallback ... albeit with a high-latency, low-bandwidth
connection" and "expensive to scale" [89]. Adoption is quantified: "QUIC accounts for 80-90% of
the connections made to PL-run bootstrappers participating in the public IPFS DHT" (2023-09-13)
[91].

**Reported problems.** rust-libp2p disables connection migration with the comment "long-term this
should be enabled, however we then need to handle address change on connections", and has no
datagram path [88]; go-libp2p rejects private networks on QUIC [87]; multistream-select is
acknowledged as inefficient, with a replacement under discussion [85]; and quic-go had to fork
Go's `crypto/tls` because it lacked a QUIC API, costing "extra effort every time a new Go version
was released" until Go 1.21 [91].

### 6.3 NATS over WebSocket and QUIC

**Status.** nats-server, Go, latest release read v2.14.6 (2026-08-27) [92]. **There is no QUIC
transport.** `go.mod` on `main` has no QUIC library at all [93]. (A code search for "quic" returns
26 files, but that is substring matching on "quick".) QUIC has been requested since 2017, appeared
on the roadmap for 2021-Q3 then 2021-Q4, and has been deferred repeatedly without being rejected:
issue #3140 is still open, created 2022-05-21 and last updated 2026-04-14 [92].

**What exists instead.** All transports are TCP-framed: client 4222, routes 6222, gateways 7222,
leaf nodes 7422, WebSocket on its own listener, MQTT 1883 [95]. WebSocket is the precedent the
maintainers cite — "our tentative plan is to support it for client connections and leafnodes,
similar to how we did websockets" (2022-05-21) — and it changes nothing above the transport
[92][95]. Leaf nodes are the answer to the connectivity problems QUIC requesters raise: an
outbound connection bridging subject interest, needing "no inbound firewall rule, no public
address" [95].

**Why not, in the maintainers' words.** First the delivery-model argument (2017-03-24): "it's not
clear what the benefits of QUIC would be versus TCP for a connection-oriented protocol like
NATS... A client application needs only a single TCP connection to service multiple streams of
data... the majority of users employ a small number of long-lived connections" [92]. Then the
conditional version (2024-02-14): "TCP with short RTT and no loss will beat QUIC. QUIC will have
an advantage over long RTT and lossy networks" [92].

**The protocol objection.** The only concrete mapping proposal (2024-12-10) suggested replacing
the gateway's single TCP connection with multiple unidirectional streams, hashing `RMSG` by
subject so that "messages for the same subject stay ordered". A maintainer's rebuttal the same day
identifies an interest-propagation race: "the non-RMSG would be ordered, but that does not
guarantee that an RMSG would not make it before a RS+", which makes the far side drop a reply; the
mitigation suggested is to "pin an account to a stream", which removes the parallelism that
motivated the change [92]. A performance objection (2025-06-12) argues that "high-throughput
UDP... is extremely CPU intensive because of a lot of syscalls (one per packet)", answered by the
observation that this "depends on the specific way that NATS is mapped to QUIC (stream per rpc?
stream per subscription? stream per message?)" [92]. The ADR that would have carried the design
was opened and closed the same day (2023-12-16) with "better to track the server issue here" [94].
Requester motivations are consistent: JetStream over satellite at 2 Mbps with 650 ms-2 s latency
and 5% loss, robot fleets on shared WiFi, and RTP over NATS where "wrapping them in TCP connection
strongly affects subscribers receive performance" [92].

### 6.4 Apache Kafka and Redpanda

**Status.** Kafka's protocol guide (4.3 docs, modified 2026-05-22) opens with "Kafka uses a binary
protocol over TCP" and defines nothing else [96]. The negative finding is evidenced: the KIP index
searched for `QUIC|HTTP/3|HTTP3|datagram|UDP` yields zero matching titles; an Apache JIRA query
`project=KAFKA AND text~"QUIC"` returns `{"total": 0}`; GitHub issue, PR and code searches return
nothing but `quick`/`QuickJS` matches [98]. Redpanda's broker properties (v26.2, 2026-09-03) list
`kafka_api` (9092), `admin` (9644), `rpc_server` (33145), pandaproxy and schema registry, all TLS
over TCP, with no UDP, QUIC or HTTP/3 listener [99].

**What a QUIC mapping would have to preserve.** Ordering is a per-connection guarantee implemented
by serialization: "the server guarantees that on a single TCP connection, requests will be
processed in the order they are sent and responses will return in that order as well. The broker's
request processing allows only a single in-flight request per connection in order to guarantee
this ordering", with pipelining happening in the OS socket buffer [96] — head-of-line blocking by
design. Version negotiation and authentication are per-connection state machines: `ApiVersions`
results "are only valid for the connection on which that information is obtained", and SASL
failure closes the connection [96]. Fetch sessions are per-connection soft state with their own
error codes [96]. The idempotent producer is bounded per connection because of broker state:
`max.in.flight.requests.per.connection` defaults to 5 and "enabling idempotence requires the value
of this configuration to be less than or equal to 5, because broker only retains at most 5 batches
for each producer" [97]. Batching across partitions is the throughput mechanism [96].
[inference] A stream-per-partition mapping breaks cross-partition batching; a stream-per-request
mapping breaks the same-order response guarantee and the per-connection state; spreading one
producer's batches over independent streams collides with the five-batch retention. Each is a
protocol change, not a transport swap. The nearest existing analysis is KIP-559 (Accepted, updated
2020-02-28), recording that JoinGroup/SyncGroup messages "could not be handled independently
without knowing the prior information exchanged between the members and the coordinator" —
motivated by L7 proxies, not QUIC [100].

**Nearest measurements.** None for Kafka. The neighbours are MQTT over QUIC (Kumar and Dezfouli,
*Computer Networks*, online 2018-12-21, connection overhead reduced "by up to 56%" in packets
exchanged) [101] and AMQP 1.0 over QUIC (*Journal of King Saud University CIS*, online 2023-03-02,
8.57% lower communication time over satellite and start-up latency improvements of 52%/38%/34% on
WiFi/4G-LTE/satellite) [102].

### 6.5 Cap'n Proto RPC and the three-party handoff

**Identity card.** An object-capability RPC protocol; the specification is the inline
documentation of `c++/src/capnp/rpc.capnp` (copyright header 2013-2014, read from `master`) plus
`capnproto.org/rpc.html` (undated) [103][104]. Vocabulary: *vat* (an object host), *capability*
(a reference that both designates and authorizes), the *four tables* per connection, *E-Order*,
*promise pipelining*. It is a design reference, not QUIC prior art: QUIC and datagram transports
are not mentioned anywhere, and the stated position is that "as it is a simple byte stream
protocol, it can easily be layered on top of SSL/TLS or other such protocols" [104].

**Mapping.** The transport requirement is "two-way, private, reliable, sequenced datagram
connections" whose `send` "returns successfully when the message (and all preceding messages) has
been acknowledged by the recipient" [103][108]. Ordering is a protocol obligation: messages "must
be delivered to the receiving application in the same order in which they were initiated by the
sending application. The goal is to support 'E-Order', which states that two calls made on the
same reference must be delivered in the order which they were made" [103]. There is no
per-message channel id; multiplexing is the four tables [103].

**Flow control.** Two credit mechanisms, both shaped by the single byte stream. The streaming
convention (`-> stream`, 0.8, 2020-04-23) is candid: "Cap'n Proto currently implements flow
control using a simple hack: it queries the send buffer size of the underlying network socket,
and sets that as the 'window size' for each stream... the TCP socket buffer size only approximates
the BDP of the first hop", and it needs no protocol change [106]. `setFlowLimit` works by not
reading, with an explicit warning: "when over the flow limit, all messages are blocked, including
returns. If the outstanding calls are themselves waiting on calls going in the opposite
direction, the flow limit may prevent those calls from completing, leading to deadlock" [108].
[inference] Both are exactly where a multi-stream transport's native per-stream credit would
replace ad-hoc logic.

**Delivery.** Reliability and ordering are preconditions, so there is no retransmission or
sequence numbering; disconnection is total — "everything on the four tables is lost. All questions
are canceled and throw exceptions. All imports become broken" — and the exception taxonomy
prescribes client behaviour (`overloaded`: retry much later; `disconnected`: start over)
[103][104][108].

**The ordering result worth transplanting.** Embargoes exist because path shortening reorders
messages: "if an application makes two calls foo() and bar() on the same capability reference, in
that order, the calls should be delivered in the order in which they were made. But if foo() is
called on a promise, and that promise happens to resolve before bar() is called, then the two
calls may travel different paths over the network, and thus could arrive in the wrong order"
[103]. The fix is an in-band barrier, `Disembargo`, rather than a local no-op-call embargo, which
is rejected as "pessimistic: in the three-party case, it requires an A -> B -> C -> B -> A round
trip before calls can start being delivered directly". A second rule bounds rather than solves:
once a promise resolves to a remote reference, further messages "will be forwarded strictly to R"
even if R is itself a promise, because "extending the embargo/disembargo protocol to be able to
shorted multiple hops at once seems difficult"; and the race "does not require each vat to be
*distinct*; as long as each resolution crosses a network boundary the race can occur" [103].
[inference] This is the most transferable result here for any design that upgrades a relayed path
to a direct one.

**Identity.** The capability *is* the authorization, and it is "impossible for others to access
the capability without consent of either the host or the receiver because the host only assigns
it an ID specific to the connection over which it was sent" [104]. Peer authentication is
delegated to the vat network, whose vat id is "typically some sort of public key", and a
`ThirdPartyCapId` is "the third party's public key fingerprint, hints on how to connect to the
third party (e.g. an IP address), and the nonce used in the corresponding `Provide` message's
`RecipientId`" [103][108]. Setup may pipeline through authentication: the transport "either queues
these messages until authenticated, or sends them encrypted such that only the authentic vat would
be able to decrypt them. The latter approach avoids a round trip for authentication" [103].
[inference] That describes TLS early data, written before QUIC shipped.

**Reported problems.** The levels never arrived: the C++ documentation still says "as of version
0.4... a Level 1 implementation. Persistent capabilities, three-way introductions, and distributed
equality are not yet implemented" [105], and the 1.0 announcement (2023-07-28) repeats it, adding
"the real world has seemingly proven that they aren't actually that important" [107]. Three-party
handoff was merged 2025-06-11 and reverted 2025-06-21, with `master` still carrying the original
design [103][108].

### 6.6 DDS and RTPS over QUIC

**Status.** DDSI-RTPS 2.5 (OMG formal/22-04-01, April 2022) is the interoperability wire protocol
[109], and no QUIC transport exists in any documented implementation. Fast DDS 3.6.2 lists UDPv4,
UDPv6, TCPv4, TCPv6 and SHM plus intra-process and data-sharing delivery, with matching transport
descriptors and TLS over TCP only [110]. RTI Connext lists builtin UDPv4, UDPv6, shared memory and
Real-Time WAN Transport, TCP as an extension library, and custom plugins as the escape hatch
[111]. Issue searches in Fast-DDS, OpenDDS and Cyclone DDS return no relevant hits, and literature
searches found no paper implementing or measuring RTPS over QUIC [115].

**Why the fit is awkward.** Both stacks are explicitly transport-agnostic — "the DDS layer itself
is transport independent, it defines a transport API and can run over any transport plugin that
implements this API" [110] — but RTPS carries its own reliability (HEARTBEAT/ACKNACK, per-writer
sequence numbers) precisely so it can run over unreliable transports [114]. [inference] A QUIC
mapping would have to choose per endpoint: reliable writers onto streams, duplicating
retransmission, or best-effort writers onto QUIC DATAGRAM; no vendor document describes either,
and no QUIC locator kind is defined. A second tension is security: DDS Security applies per-sample
AES-GCM/GMAC above a plaintext transport through the `DDS:Auth:PKI-DH`, `DDS:Access:Permissions`
and `DDS:Crypto:AES-GCM-GMAC` plugins [110], so QUIC's record protection would be a second
confidentiality layer.

**What the ecosystem did instead.** The 2026 optimization work stays on UDP: StreamRTPS (arXiv
2606.14214, 2026-06-12) replaces "the full RTPS header at runtime with a compact 2 B identifier",
aggregates samples per locator into single UDP packets and suppresses predictable heartbeats,
reporting "up to 27.9%" bandwidth reduction under best-effort and "a further 22.7%" from heartbeat
suppression under reliable transport [113]. The deployed answer to bad wireless links is a bridge:
`zenoh-plugin-ros2dds` "bridges all ROS 2 communications using DDS over Zenoh", motivated by the
fact that "a Zenoh bridge for DDS already exists and helped lot of robotic use cases to overcome
some wireless connectivity, bandwidth and integration issues", with DDS terminated locally and the
wide-area hop carried by Zenoh [112]. A 2026 survey states the split plainly: RTPS is UDP-primary
with TCP fallbacks, while "Zenoh employs a pluggable transport layer that supports multiple
protocols, including TCP, UDP, and QUIC" [114].

### 6.7 Mosh

**Identity card.** Mosh (mobile shell), C++; the design source is Winstein and Balakrishnan,
"Mosh: An Interactive Remote Shell for Mobile Clients", USENIX ATC 2012, with 1.4.0 released
2022-10-31 [116][117]. It is UDP, not QUIC, and is here because it solves roaming without
transport migration.

**Mapping.** Not a byte stream. The State Synchronization Protocol sends an *Instruction*: a
self-contained source state, target state and logical binary diff, where client-to-server objects
represent user input history and server-to-client objects represent terminal contents, the diff
being "the minimal message that transforms one frame to the current frame" — Mosh conveys the most
recent state rather than every octet generated [116].

**Flow control and delivery.** No credit, but rate adaptation: because the sender can diff
arbitrary states it skips obsolete frames, with a minimum interval of half the smoothed RTT (about
one Instruction in flight), a 50 Hz cap, 8 ms coalescing of server-side changes, and 100 ms
delayed acknowledgements that piggybacked on host data in over 99.9% of cases [116]. Delivery is
idempotent rather than acknowledged: every datagram is a diff between numbered states, so loss,
reordering and duplication are absorbed by applying it, and SSP needs no replay cache or message
history [116]. Under a 100 ms RTT with 29% loss per direction, median latency was 0.222 s against
SSH's 0.416 s, with means of 0.329 s and 16.8 s [116].

**Identity and roaming.** One shared key, AES-128 in OCB mode; key exchange and user
authentication are deliberately out of band — the client logs in over SSH, starts an unprivileged
`mosh-server`, takes the printed key and speaks UDP [116][117]. The roaming rule is the whole
trick: the server accepts an authentic datagram whose sequence number exceeds every prior one and
takes its source IP and port as the new target, so "the client need not time out or even know it
changed public IP address", with 3 s heartbeats keeping the mapping open [116].

**Reported problems.** The model is wrong for bulk output: exact scrollback for something like
`cat` of a large file is unavailable, and the paper recommends a pager or `screen`/`tmux`;
prediction is partial, with 70% of keystrokes echoed immediately and 0.9% producing an erroneous
prediction repaired within an RTT [116].

### 6.8 SSH3 / remote terminal over HTTP/3

**Identity card.** `francoismichel/ssh3`, Go, Apache-2.0, "an early proof of concept"; the paper
is "Towards SSH3: how HTTP/3 improves secure shells", arXiv:2312.08396v1 (2023-12-12), implemented
on quic-go; the successor specification is `draft-michel-remote-terminal-http3-00` (2024-07-31,
Experimental, expired 2025-02-01) [118][119][120].

**Mapping and flow control.** The SSH Connection protocol runs over HTTP/3 Extended CONNECT with
`:protocol = remote-terminal`, a 2xx establishing the session [119]. Then "after authentication,
each SSH channel uses a dedicated bidirectional QUIC stream", so ordering is preserved within a
channel while separate channels no longer share TCP's single stream, and each forwarded TCP
connection gets its own channel; UDP forwarding maps packets to QUIC datagrams and requires
`SETTINGS_H3_DATAGRAM = 1` [118][119]. Flow control is QUIC's per-stream credit, which the paper
notes avoids SSHv2's channel flow-control negotiation before data; nothing is added [118].

**Identity and connectivity.** Authentication is HTTP authentication: the CONNECT `Authorization`
header carries the material, the specification lists password, public key, OpenID Connect, SAML2
and WebAuthn, and a TLS-exported session identifier is included in public-key JWTs for replay
protection [118][119]. Migration is claimed at design level via QUIC, but the repository labels it
"soon" [118][120]. 0-RTT is discussed only for idempotent monitoring requests, since "HTTP/3 does
not provide replay protection" [118].

**Reported problems.** The README warns the project is experimental, needs "expert cryptographic
review over an extended timeframe", should not be deployed to production, and that the required
changes are "too distant from established SSH implementations' philosophy to expect integration"
[120]. Measured throughput was below OpenSSH on a local 25 Gb/s link: 1.91 Gb/s TCP forwarding
against 4.62 Gb/s, and 583 Mb/s UDP forwarding, with the prototype not throughput-optimized [118].
The often-quoted speedup should be stated as documented: SSHv2 needs 5-7 round trips to establish,
SSH3 needs 3 [120].

### 6.9 WireGuard and Tailscale

**Identity card.** WireGuard is a Layer-3 tunnel over UDP; the source is Donenfeld, "WireGuard:
Next Generation Kernel Network Tunnel", NDSS 2017, permanent draft revision dated 2020-06-01
[121]. Tailscale is a control plane plus a WireGuard data plane, documented as direct-UDP,
DERP-relayed and peer-relayed connections, all WireGuard-encrypted; neither uses QUIC on the data
path [122].

**Key as identity.** WireGuard's central concept is *cryptokey routing*: a peer is identified
strictly by a 32-byte Curve25519 public key mapped to `AllowedIPs`, which serve both as outbound
peer selection and as the inbound source-address authorization check after decryption; static keys
are exchanged out of band by an intentionally unspecified mechanism, and the Noise_IK-derived
one-round-trip handshake proves control of the private key belonging to a configured public key
[121].

**Roaming.** The same rule as Mosh, one layer down: on receiving a correctly authenticated packet,
WireGuard learns or replaces the peer's endpoint with that packet's outer source IP and port, so a
peer "may move between external IPs/mobile networks", and an on-path attacker forging that
unauthenticated outer address achieves only denial of service; the latest authenticated endpoint
"removes any need to keep a NAT session open for long", with `PersistentKeepalive` for cases that
need the mapping held [121].

**Control plane and authorization.** Tailscale separates a *machine key* (identifying the device
to the coordination server) from a *node key* (generated on user authentication, tied to machine
and user identity, used to configure WireGuard peers) [125][126]. The coordination server links
the node key to machine and user identity after the identity-provider flow, evaluates tailnet
policy and distributes the key only to permitted devices; removing a device revokes it immediately
[125]. Access control is deny-by-default with users, groups, tags and addresses as selectors, and
Tailnet Lock requires a trusted node's signature on a joining node's key, verified by peers before
connecting [127]. If the coordination server is down, established connections and cached policy
keep working but no new connections, key changes or policy updates are possible [125].

**NAT traversal.** DERP ("Designated Encrypted Relay for Packets") carries DISCO packets to
establish direct links and forwards encrypted WireGuard packets otherwise, with clients picking a
home relay by latency [123]. The 2020 post describes simultaneous UDP transmission after
exchanging `ip:port` through the coordination server, with a common UDP firewall timeout of 30
seconds [124]; the 2025 post says clients use STUN and ICE and race candidate paths, giving the
vendor's internal metric as direct traversal "well north of 90% in typical conditions" [128]. The
caveats are documented: two peers behind hard (symmetric) NAT cannot go direct, blocked UDP forces
relaying, relays have lower throughput, and a network change can push an established direct
connection back onto a relay [122][123]. The fraction of connections ultimately relayed is not
published.

**Reported problems (WireGuard).** Key distribution is out of scope by design; fixed
`Rekey-Timeout` retransmission (5 s, attempts for 90 s) instead of exponential backoff is named as
"critically important future work"; and two peers sharing a private key allow a replay to make an
initiator roam involuntarily [121].

## 7. Cross-system comparison table

Columns are the seven axes, condensed to a phrase each. Source numbers refer to section 9.

| System | Mapping | Flow control | Delivery | Identity/authz | Connectivity | Reported problems |
| --- | --- | --- | --- | --- | --- | --- |
| Zenoh 1.10.1 | `Push`/`Query` in batched `Frame`s; `Fragment` over the 65535-byte batch; 8 priorities mapped to 1 bidi + 7 uni QUIC streams via ALPN `zenoh-ms`; QUIC DATAGRAM for best-effort [5][8][9] | Per-priority queue of 2 batches; per-message `Block` (5 s then close) or `Drop` (1 ms, then drop and mark congested); adaptive batching, `express` opt-out [1][7] | Reliable = hop-by-hop over reliable links, no acks in the protocol; per-priority reliable/best-effort SN spaces; `ResponseFinal` ends a query; HLC timestamps order values [5][6][12] | ZID is not a credential; usrpwd/pubkey auth; TLS/mTLS per link; ACL interceptor off by default, deny-first [1] | router/peer/client; multicast + gossip scouting; lease 10 s with 4 keep-alives; no migration concept [1][11] | No document says what QUIC buys; QUIC link lagged TCP on mTLS, DSCP, PMTUD, accept-loop DoS; no QUIC measurements published [1][16] |
| iroh 1.2.0 | Connection is the unit; one ALPN per connection, so N protocols cost N connections; QUIC streams lazily created; relay tunnels UDP over HTTPS/WebSocket [17][28][29] | QUIC per-connection/per-stream config; relay applies a per-connection token bucket that stops reading rather than dropping; gossip bounded at 4096-byte messages [18][25][38] | Per-stream QUIC; `iroh-blobs` certifies by BLAKE3 verified streaming and resumes by chunk range; `iroh-gossip` is best-effort epidemic with 30 s cache [37][38] | Ed25519 public key *is* the EndpointId; TLS raw public keys; peer authenticated automatically, authorization left to `EndpointHooks`/router filter [17][28][32] | QUIC multipath + address discovery + NAT-traversal drafts inside a forked stack; relays for rendezvous and fallback; pkarr/DNS/mDNS/DHT address lookup [21][30] | Browsers relay-only; two releases regressed NAT traversal; hole punching loss-sensitive; multipath only in the fork [26][30][32][39] |
| EMQX MQTT over QUIC | MQTT packets on a bidi stream; multi-stream adds a control stream plus per-topic/per-QoS data streams with broker-side packet-to-stream binding [42] | MQTT receive maximum plus QUIC credit per data stream; broker-side prioritization and flow control not implemented [41][42] | Full MQTT QoS 0/1/2 unchanged; ordering per stream only; session state and in-flight QoS lost on reconnect or stream reset [41][42] | TLS listener with certificates; MQTT credentials; pre-CONNACK packets processed only after the connection is allowed [42][43] | Connection migration transparent to MQTT; client-side automatic fallback to TCP/TLS 1.2; no relays [42][43] | No session persistence; QoS state lost on stream close; reconnect uses more bandwidth than TLS; datagrams unused; client SDKs few [41][44] |
| MOQT (draft-21) | Track > Group > Subgroup > Object; subgroup = one stream; object = one datagram; two uni control streams, bidi request streams [49] | QUIC only, plus `MAX_REQUEST_UPDATES` per request stream; control streams get credit first to avoid deadlock; `TOO_FAR_BEHIND` terminates slow subscribers [49] | Objects immutable and idempotent by (track, group, object); partial reliability via two delivery timeouts that reset streams or drop datagrams; order within a subgroup [49] | Hop-by-hop; mTLS and/or authorization tokens (Common Access Token, Privacy Pass); relays must authorize each subscriber and prevent namespace impersonation; E2E via Secure Objects [49][50] | `GOAWAY` drains and migrates sessions; relays cache and fan out; namespace announcements for discovery; 0-RTT analysed as replay-safe [49] | Rapid churn between drafts; cache-poisoning section is a TODO; datagram objects silently dropped on size; starvation accepted [49] |
| moq-lite -05 | Broadcast > Track > Group > Frame; one uni stream per group; datagram = one single-frame group, <=1200 bytes [52] | None of its own; QUIC limits plus expiration, resetting expired group streams to free credit; explicit `PROBE` for RTT/bitrate [52] | Gaps expected by contract, even with `ordered=1`; two-sided expiration (timestamp age or arrival age, shorter wins) [52] | Out of scope in the protocol; JWT-style tokens in the implementation; hop IDs leak topology [52][57] | Bare QUIC, WebTransport, and Qmux over TCP/TLS or WebSocket when UDP is unavailable; `GOAWAY` with a new URI [52] | Exists because "MoqTransport has become too complicated"; interop only on the feature intersection; own wire churn across -01..-05 [52][57] |
| WebTransport + products | Session over extended CONNECT; uni/bidi streams and datagrams; no message primitive — framing is the application's [53][55] | Opt-in mutual session credit (`WT_MAX_STREAMS`, `WT_MAX_DATA`), hop-by-hop through intermediaries; incoming datagram queue drops from the head [53][55] | Per-stream QUIC with `RESET_STREAM_AT`; `sendOrder` is local only and "does not guarantee reception order" [53][55] | `https` + Origin verification; `serverCertificateHashes` (<=2 weeks, P-256) authenticates only the server; client identity must be established in-band [55][64] | HTTP/2 capsule fallback exists but collapses to one stream; browser support uneven per option [53][56] | Centrifugo and Engine.IO each use exactly one bidirectional stream; RPC-over-WebTransport is at request stage; no game engine support [62][63][65] |
| gRPC over HTTP/3 | One RPC per QUIC bidi stream; 5-byte length-prefixed messages; `grpc-status` in trailers (HTTP/3 does carry trailers) [66][67][75] | No credit of its own; 8 KiB header/trailer guidance; server stream limits (Kestrel: 100 bidi) [66][70] | No exactly-once; unstarted calls not retried; trailer status is the completion certificate [66] | TLS required, `h3` ALPN; no RPC-level identity model [67][75] | HTTP/3-only or HTTP/2-then-Alt-Svc; `Http1AndHttp2AndHttp3` recommended because middleboxes may not pass HTTP/3 [67][70] | grpc-go closed its request in 2022; C-core HTTP/3 is Cronet client-only; grpc-dotnet is the only documented production path; .NET lacked QUIC network transitions [68][69][70][72] |
| Connect v1 | Unary = one bare body, no envelope; streaming = enveloped messages ending in `EndStreamResponse` — no trailers at all [73] | None; overload as in-body error or HTTP 429/503 [73] | Terminal envelope required even on success; no universally retry-safe error code [73] | HTTP metadata; 401 vs 403 distinguished, mechanism unspecified [73] | HTTP-version independent, but bidi streaming needs HTTP/2 and "not all Connect implementations have HTTP/3 support" [73][74] | Two protocols in one, conceded as "less conceptually pure" [74] |
| Quilkin 0.10.1 | Raw UDP datagrams through one filter chain; `Capture` can read a QUIC DCID without terminating QUIC [78][79] | None; drop on excess; `LocalRateLimit` per source, inexact by N-1; sessions capped at 5000, 60 s timeout [78][79] | UDP only; firewall rules default-deny [78] | Routing token captured from the packet; no cryptographic peer identity on the data path [78] | Sidecar/client-proxy/proxy-pool with xDS over HTTP/2; QCMP for latency mapping; QUIC used only for corrosion state gossip (datagram/uni/bi per traffic class) [78][80] | No published benchmarks by policy; documentation drift; console story blocked by NDAs [78] |
| libp2p QUIC | QUIC streams used directly, no extra framing, bidi only; no security or muxer negotiation, so 1 RTT [82][86] | QUIC windows pinned per implementation (256 streams, 10 MB stream / 15 MB connection); resource manager above [87][88] | Per stream; loss isolated to the affected stream [86] | Peer id = multihash of the public key (<=42 bytes inlined); self-signed cert with OID `1.3.6.1.4.1.53594.1.1` proving host-key possession; mutual auth mandatory [83][84] | TCP fallback RECOMMENDED because UDP is blocked in some networks; hole punching, AutoNAT v2, Circuit Relay v2, DCUtR; 80-90% of DHT bootstrapper connections are QUIC [82][89][91] | Migration disabled in rust-libp2p; no datagrams there; no PSK on QUIC in go-libp2p; multistream-select acknowledged inefficient; forked `crypto/tls` cost [85][87][88][91] |
| NATS | No QUIC transport at all (`go.mod` has no QUIC library); TCP-framed client/route/gateway/leafnode/WebSocket/MQTT listeners [93][95] | Per-transport knobs only [95] | Core at-most-once over one connection; JetStream for persistence [92] | Per-connection TLS plus accounts/JWT credentials; WebSocket cookie credentials [95] | Leaf nodes are the outbound-only answer to firewalls; WebSocket for browsers [95] | Requested since 2017, deferred repeatedly, never rejected; interest/data ordering races block a multi-stream gateway mapping; userspace UDP CPU cost disputed [92][94] |
| Kafka / Redpanda | Size-delimited request/response over TCP, one in-flight request per connection by design [96] | `buffer.memory`, in-flight <= 5 for idempotence, broker request-size cap [96][97] | Per-partition order; idempotent producer keyed on broker-retained batches; duplicates detected by sequence errors [96][97] | Per-connection TLS + SASL handshake; per-resource authorization error codes [96] | Bootstrap plus metadata refresh; an address change is indistinguishable from failure [96] | No KIP, no JIRA, no code: verified negative. Per-connection ApiVersions/SASL/fetch-session state and the 5-batch rule make a stream mapping a protocol change [96][98][99] |
| Cap'n Proto RPC | Capabilities over a "reliable, sequenced" byte stream; multiplexing in four tables, not in streams [103][108] | Streaming window = socket send buffer; `setFlowLimit` stops reading, with a documented deadlock risk [106][108] | Ordering (E-Order) is a protocol obligation; disconnection destroys all four tables [103][104] | Capability = designation + permission; vat authentication delegated to the network; vat id "typically some sort of public key" [103][104] | Three-party handoff (`Provide`/`Accept`) forms a direct connection; `Disembargo` preserves order across the shortened path [103] | Levels 2-4 never shipped; three-party handoff merged 2025-06-11 and reverted 2025-06-21; flow control called "a simple hack" by its author [105][106][107][108] |
| DDS / RTPS | Pluggable transports: UDPv4/v6, TCPv4/v6, SHM; no QUIC transport in Fast DDS or Connext [110][111] | Writer-side flow controllers and publish modes, not transport credit [110] | RTPS carries its own reliability (HEARTBEAT/ACKNACK) so it can run over unreliable transports [114] | DDS Security plugins (PKI-DH, Permissions, AES-GCM-GMAC) above a plaintext transport [110] | SIMPLE/STATIC/Discovery Server; WAN via dedicated transports or a Zenoh bridge [110][112] | Verified negative for QUIC; 2026 optimization work stays on UDP; a QUIC mapping would duplicate both retransmission and encryption [113][114][115] |
| Mosh 1.4.0 | Not a byte stream: each datagram is an idempotent state diff (Instruction) with a sequence number [116] | Rate adaptation instead of credit: skip obsolete frames, >=RTT/2 spacing, 50 Hz cap, 8 ms coalescing, 100 ms delayed acks [116] | Delivery certified by resulting state, not by receipt; loss/reorder/duplication absorbed; no replay cache needed [116] | One AES-128-OCB key handed over by SSH; authentication deliberately out of band [116][117] | Roaming by rule: accept the newest authentic datagram's source address; 3 s heartbeats keep the mapping open [116] | Wrong model for bulk output (no exact scrollback); prediction succeeds for 70% of keystrokes [116] |
| SSH3 (remote terminal over HTTP/3) | SSH Connection over Extended CONNECT; one bidi QUIC stream per SSH channel; UDP forwarding over QUIC datagrams [118][119] | QUIC per-stream credit replaces SSHv2 channel flow control; nothing added [118] | Reliable per channel, unreliable for forwarded UDP [118][119] | HTTP authentication (password, public key, OIDC, SAML2, WebAuthn); TLS-exported session id in public-key JWTs [118][119] | Migration claimed via QUIC but labelled "soon" in the repository; 3 RTT establishment against SSHv2's 5-7 [118][120] | Explicit proof of concept, not for production; throughput below OpenSSH (1.91 vs 4.62 Gb/s); 0-RTT restricted to idempotent commands [118][120] |
| WireGuard / Tailscale | UDP-encapsulated encrypted IP packets; no streams; DERP relays forward encrypted packets and DISCO [121][123] | None; drop on unavailable endpoint; cookies rate-limit handshakes [121] | IP-level, no application delivery semantics; authenticated counters and TAI64N timestamps against replay [121] | Public key is the peer identity via cryptokey routing; Tailscale splits machine and node keys, distributes them by policy, deny-by-default ACLs, Tailnet Lock signatures [121][125][127] | Endpoint learned from the last authenticated packet (stateless roaming); coordination server for discovery, DERP fallback; direct traversal "well north of 90%" [121][122][128] | Key distribution out of scope; fixed rekey timeout named as future work; both-side hard NAT and blocked UDP force relaying [121][122] |

## 8. Recurring lessons the projects report

Only items that at least two independent projects state are listed.

1. **A stream per message is not the design; a stream per ordered group of messages is.** MOQT
   asks publishers to make "a reasonable tradeoff between having an optimal mapping of Object
   relationships in a Group and minimizing the number of streams used", pinning one subgroup to
   one stream [49]. Zenoh maps eight *priority classes* onto streams, not messages [8]. EMQX maps
   *topics and QoS levels* and lets the client choose the grouping [42]. Cap'n Proto multiplexes
   in its four tables rather than in transport channels [103].
2. **A single stream per connection reintroduces the blocking the transport was chosen to avoid,
   and the application sees it as a liveness failure, not as slowness.** EMQX measured a large
   PUBLISH on one QUIC stream causing an MQTT keep-alive timeout — "there is nothing wrong with
   the QUIC connection itself; the real problem is the Head-of-Line (HOL) blocking" — followed by
   a reconnect loop [45]. WebTransport's capsule variant, which exists so HTTP/2 intermediaries
   work, makes all streams share one QUIC stream and brings the blocking back [53]. Kafka has the
   property at protocol level and accepts it: the broker "allows only a single in-flight request
   per connection in order to guarantee this ordering" [96]. Centrifugo and Engine.IO both
   restrict WebTransport to one bidirectional stream and gain none of the multiplexing [62][63].
3. **Relays are part of the architecture, and roughly one path in ten needs them.** iroh: "roughly
   9 out of 10 connections go direct; the relay is only a stepping stone" [24]. Tailscale: direct
   traversal "well north of 90% in typical conditions", and no direct path at all when both peers
   are behind hard NAT [122][128]. libp2p ships Circuit Relay v2 and DCUtR and calls relays "a
   reliable fallback ... albeit with a high-latency, low-bandwidth connection" that is "expensive
   to scale" [89]. MOQT makes relays first-class endpoints that terminate sessions and cache [49].
4. **UDP is blocked often enough that a non-UDP path is mandatory.** libp2p: "UDP is blocked in a
   small fraction of networks, therefore it is RECOMMENDED that libp2p nodes offer a TCP-based
   connection option as a fallback" [82]. EMQX: "many operators still have special routing
   strategies for UDP packets, often leading to QUIC connection failures", so clients switch to
   TCP/TLS 1.2 automatically [43]. moq-lite defines Qmux over TCP/TLS and WebSocket "when UDP is
   unavailable" [52]. RFC 9114 and Kestrel's documentation both keep TCP-based HTTP in the
   recommended configuration [70][75].
5. **0-RTT is only worth the analysis if the application layer is already idempotent — and most
   projects disable it or scope it.** MOQT can argue for it because "processing the same Objects
   multiple times is idempotent", yet still advises relays to "defer initiating upstream
   subscriptions until the handshake is complete or reject 0-RTT entirely" [49]. EMQX measured
   0-RTT reconnection and then states "EMQX does not support early data by default" [44]. SSH3
   restricts it to whitelisted idempotent commands since "HTTP/3 does not provide replay
   protection" [118]. WebTransport cannot use it at all, because CONNECT "is not a safe method"
   [53].
6. **Partial reliability is implemented by cancelling streams and dropping datagrams, not by an
   unreliable message mode.** MOQT resets the stream with `DELIVERY_TIMEOUT` and drops datagrams
   past `OBJECT_DELIVERY_TIMEOUT` [49]. moq-lite resets expired group streams "to avoid consuming
   flow control" [52]. Zenoh drops droppable messages after a per-priority deadline and, once a
   fragment has left, sends a `Fragment` with the `Drop` extension so the receiver discards the
   partial reassembly instead of stalling [5][7]. WebTransport requires `RESET_STREAM_AT` so a
   reset keeps the prefix identifying the session [53].
7. **The transport's priority interface is the weak link, and control traffic must be prioritized
   explicitly.** MOQT: control streams "SHOULD be prioritized highest", while conceding that
   "scheduled to be sent first... is implementation dependent and is constrained by the
   prioritization interface of the underlying transport" [49]. Zenoh reserves class 0 for
   `Control` and inverts the sign when handing priorities to QUIC, "the opposite of Zenoh's"
   [4][8]. EMQX gives the control stream the highest priority so `PINGREQ` survives a saturated
   data stream [42][45]. moq-lite: "it may not be possible to get fine-grained control" [52].
8. **Application-level credit on top of QUIC is a deadlock generator, and the projects that have
   it document the ordering rule that saves them.** MOQT: "endpoints MUST allocate connection flow
   control to the control streams before allocating it to any data streams" [49]. Cap'n Proto's
   `setFlowLimit` warns that "the flow limit may prevent those calls from completing, leading to
   deadlock" [108]. WebTransport excludes stream headers and capsules from `WT_MAX_DATA` so
   associating a stream with its session can never block, and makes intermediaries re-express
   credit hop by hop [53].
9. **Key-as-identity removes the PKI problem and moves authorization into the application.** iroh:
   the public key "is also known as the EndpointId", the peer is authenticated automatically, but
   "it is up to the application to decide if a particular peer is allowed to connect or not" [17].
   libp2p derives the peer id from the key and proves host-key possession in the certificate
   [83][84]. WireGuard identifies a peer strictly by its Curve25519 key and puts key distribution
   out of scope [121]. Cap'n Proto expects the vat id to be "typically some sort of public key"
   [103]. Tailscale is the counter-example that shows the cost: an entire coordination service
   exists to distribute keys and compile policy [125].
10. **Session and subscription state, not the transport, is what breaks on reconnect.** EMQX's two
    documented limitations are both state: session state is not preserved, and "if the data stream
    is closed unexpectedly by either peer, the QoS 1 and QoS 2 message states are not preserved"
    [41]. MOQT needed a protocol message for it, because the session "can take days" to drain, so
    `GOAWAY` migrates subscriptions [49]. Kafka's per-connection `ApiVersions`, SASL and
    fetch-session state is why a transport swap is a protocol change [96]. NATS's blocking issue is
    the same shape: interest declarations and data on independent streams race [92].
11. **Running QUIC means owning a QUIC stack.** iroh forked quinn into `noq` for multipath,
    address discovery and NAT traversal, upstreaming having become "increasingly unlikely" as the
    diff grew [28]. quic-go forked Go's `crypto/tls` because it had no QUIC API, at the cost of
    "extra effort every time a new Go version was released" until Go 1.21 [91]. Zenoh's issue
    history shows the smaller version: mTLS, DSCP, PMTUD and accept-loop hardening each had to be
    re-implemented for the QUIC link after they worked for TCP/TLS [16]. NATS cites userspace UDP
    syscall cost as a reason not to start [92].
12. **Immutable, content-addressed units make duplicates, caching and resumption free.** MOQT
    objects "MUST NOT change over time" and are keyed by (full track name, group ID, object ID),
    which is what lets relays cache, subscribers deduplicate and 0-RTT replays be harmless [49].
    `iroh-blobs` addresses everything by BLAKE3 hash and verifies incrementally, so an interrupted
    transfer resumes by requesting missing chunk ranges rather than by protocol state [37]. Zenoh
    reaches for the same property with per-source sequence numbers plus a bounded history cache
    queried by range when a subscriber notices a gap [12].

## 9. Sources

1. Zenoh `DEFAULT_CONFIG.json5` (annotated reference configuration) — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/DEFAULT_CONFIG.json5 — `main`, workspace version 1.10.1, read 2026-09-10 — repository source — every default value quoted for Zenoh.
2. Zenoh workspace `Cargo.toml` — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/Cargo.toml — 1.10.1, read 2026-09-10 — repository source — license, MSRV, crate layout, quinn 0.11.5 / rustls 0.23.
3. Zenoh GitHub releases — https://github.com/eclipse-zenoh/zenoh/releases — queried 2026-09-10 (1.10.1 2026-09-07, 1.10.0 2026-08-14, 1.9.0 2026-04-10) — release metadata — version dates, QUIC feature PRs.
4. `zenoh-protocol` `core/mod.rs` — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/commons/zenoh-protocol/src/core/mod.rs — 1.10.1 — repository source — priority classes, `Reliability`, `CongestionControl` defaults, `ZenohIdProto`.
5. `zenoh-protocol` transport module (`mod.rs`, `frame.rs`, `fragment.rs`, `init.rs`, `open.rs`) — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/commons/zenoh-protocol/src/transport/ — 1.10.1 — repository source — handshake, `Frame`/`Fragment` layout, batch size, sequence numbers, fragment `First`/`Drop` extensions.
6. `zenoh-protocol` network and zenoh modules (`network/{mod,push,request,response,declare}.rs`, `zenoh/put.rs`) — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/commons/zenoh-protocol/src/ — 1.10.1 — repository source — `Push`/`Put`, request/response correlation, `ResponseFinal`, declarations.
7. `zenoh-transport` `common/pipeline.rs` — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/io/zenoh-transport/src/common/pipeline.rs — 1.10.1 — repository source — per-priority queues, drop/block deadlines, fragmentation loop, express bypass.
8. `zenoh-link-commons` `quic/{unicast,utils}.rs` — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/io/zenoh-link-commons/src/quic/ — 1.10.1 — repository source — ALPN set, priority-to-stream mapping, `set_priority` inversion, endpoint metadata knobs.
9. `zenoh-link-quic` and `zenoh-link-quic_datagram` — https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/io/zenoh-links/ — 1.10.1 — repository source — locator prefix, MTU rationale, mixed-reliability link pairing.
10. Zenoh manual: Abstractions — https://zenoh.io/docs/manual/abstractions/ — undated, read 2026-09-10 — vendor documentation — key expressions, selectors, HLC timestamps, entity definitions.
11. Zenoh getting started: Deployment — https://zenoh.io/docs/getting-started/deployment/ — undated, read 2026-09-10 — vendor documentation — peer/client/routed models, scouting behaviour.
12. Zenoh roadmap RFC: Non Blocking Fault Tolerant Reliability — https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Non%20Blocking%20Fault%20Tolerant%20Reliability.md — undated, read 2026-09-10 — design document — scope of hop-by-hop reliability, `SourceInfo`, range re-query.
13. docs.rs `zenoh` 1.10.1 (`CongestionControl`, `ConsolidationMode`) — https://docs.rs/zenoh/1.10.1/zenoh/ — 1.10.1 — API documentation — consolidation modes, congestion-control variants.
14. A. Corsaro et al., "Zenoh: Unifying Communication, Storage and Computation from the Cloud to the Microcontroller" — https://ieeexplore.ieee.org/document/10456820 — Euromicro DSD 2023, DOI 10.1109/DSD60849.2023.00065 — paper (abstract only; full text not retrievable) — positioning and the 5-byte overhead claim.
15. "Comparing the Performance of Zenoh, MQTT, Kafka, and DDS" — https://zenoh.io/blog/2023-03-21-zenoh-vs-mqtt-kafka-dds/ — 2023-03-21, Zenoh 0.7.0-rc — (blog) / (vendor benchmark, third-party authors) — throughput and latency figures with test setup.
16. `eclipse-zenoh/zenoh` issue tracker (#86, #771, #1019, #1267, #2016, #2018, #2425, #2427, #2428, #2506, #2675, #2685, #2707, #2711, #2719, #2764) — https://github.com/eclipse-zenoh/zenoh/issues — queried 2026-09-10 — issue tracker — QUIC link defects and open work.
17. docs.rs `iroh` crate root — https://docs.rs/iroh/latest/iroh/ — iroh 1.2.0, read 2026-09-10 — API documentation — connection model, EndpointId/encryption, relay description, stream semantics.
18. docs.rs `iroh::endpoint` module — https://docs.rs/iroh/latest/iroh/endpoint/index.html — iroh 1.2.0 — API documentation — paths and path events, transport config knobs, `PathSelector`, Android JNI note.
19. `iroh/Cargo.toml` — https://raw.githubusercontent.com/n0-computer/iroh/main/iroh/Cargo.toml — version 1.2.0, read 2026-09-10 — source manifest — license, MSRV 1.91, `noq` 1.3.0 dependencies, features.
20. iroh `CHANGELOG.md` — https://raw.githubusercontent.com/n0-computer/iroh/main/CHANGELOG.md — entries 1.2.0 (2026-09-09) through 1.0.0 (2026-06-15) — changelog — release dates, noq upgrades, rate-limit notification, empty-ALPN error.
21. `noq` README — https://raw.githubusercontent.com/n0-computer/noq/main/README.md — undated file, crate `noq` 1.3.0, read 2026-09-10 — repository README — quinn fork provenance and implemented drafts (multipath, QAD, QNT).
22. `noq` `CHANGELOG.md` and `noq-proto/src/transport_parameters.rs` — https://raw.githubusercontent.com/n0-computer/noq/main/ — 0.17.0 (2026-03-09) to 1.3.0 (2026-09-09), read 2026-09-10 — changelog and source — multipath draft-18, default 8 paths, NAT-traversal transport parameters.
23. iroh documentation concept pages: Endpoints, Relays, NAT Traversal, Protocols, Security & Privacy, Gossip Broadcast — https://docs.iroh.computer/concepts/ — read 2026-09-10 — vendor documentation — endpoint/relay/ALPN model, relay visibility, gossip usage.
24. iroh FAQ — https://docs.iroh.computer/about/faq — read 2026-09-10 — vendor documentation — "roughly 9 out of 10 connections go direct", ports, relay regions, Ed25519-only rationale.
25. iroh docs: Rate Limiting — https://docs.iroh.computer/relays/rate-limiting — read 2026-09-10 — vendor documentation — per-connection token bucket, stop-reading backpressure, unpublished public limits.
26. iroh docs: WebAssembly and Browsers, Compatibility, Public Relays — https://docs.iroh.computer/ — read 2026-09-10 — vendor documentation — browser relay-only limitation, platform matrix, public-relay policy.
27. Blog: "Iroh 1.0 — Dial Keys, not IPs" — https://www.iroh.computer/blog/v1 — 2026-06-15 — (blog) — wire/API stability promise, 95%-direct-data claim, 200M endpoints in 30 days.
28. Blog: "The road to iroh 1.0" — https://www.iroh.computer/blog/the-road-to-iroh-1-0 — 2026-07-09 — (blog) — ALPN decision and its consequences, quinn fork decision, Ed25519/PQ decisions, MoQ compatibility statement.
29. Blog: "iroh on QUIC Multipath" — https://www.iroh.computer/blog/iroh-on-QUIC-multipath — 2025-08-05 — (blog) — relay as HTTPS-upgraded WebSocket tunnel, magicsock QUIC-bit multiplexing, congestion-controller restart, rationale for multipath.
30. Blog: "iroh 0.96.0 — The QUIC Multipaths to 1.0" — https://www.iroh.computer/blog/iroh-0-96-0-the-quic-multipaths-to-1-0 — 2026-01-27 — (blog) — multipath and QNT adoption, draft deviations, removal of `conn_type`, holepunch regression.
31. Blog: "iroh 0.97.0 — Custom Transports & noq" — https://www.iroh.computer/blog/iroh-0-97-0-custom-transports-and-noq — 2026-03-16 — (blog) — switch to noq, custom transports, endpoint close semantics, relay reconnect behaviour.
32. Blog: "iroh 0.98.0 — Getting back to traversing NATs" — https://www.iroh.computer/blog/iroh-0-98-0-getting-back-to-traversing-nats — 2026-04-17 — (blog) — NAT-traversal regressions and fixes, faster relay health check, router incoming filter and its 30x figure.
33. Blog: "Healing Connections After Network Migration" — https://www.iroh.computer/blog/healing-connections — 2024-06-17 — (blog, pre-multipath) — netwatch/netcheck, `CallMeMaybe`, 5-second ping cadence, both-behind-NAT failure.
34. Blog: "iroh 0.29 — net is the new iroh" — https://www.iroh.computer/blog/iroh-0-29-net-is-the-new-iroh — 2024-12-05 — (blog) — the `iroh-net` to `iroh` restructuring.
35. Blog: "iroh 0.94.0 — The Endpoint Takeover" — https://www.iroh.computer/blog/iroh-0-94-0-the-endpoint-takeover — 2025-10-22 — (blog) — `NodeId`/`NodeAddr` to `EndpointId`/`EndpointAddr` rename, `TransportAddr`.
36. Blog: "Share relay servers to save money and heartache" — https://www.iroh.computer/blog/shared-relays — 2026-09-08 — (blog) / (vendor pricing and cost model) — rate-limit tiers, 98%-direct cost model with its caveat, 93.10% dashboard figure.
37. docs.rs `iroh-blobs` — https://docs.rs/iroh-blobs/latest/iroh_blobs/ — 0.103.0, read 2026-09-10 — API documentation — BLAKE3 verified streaming, chunk-range requests, resumption, non-goals, production-quality warning.
38. docs.rs `iroh-gossip` plus repository sources (`src/proto.rs`, `src/proto/hyparview.rs`, `src/proto/plumtree.rs`, `src/api.rs`) — https://docs.rs/iroh-gossip/latest/ and https://github.com/n0-computer/iroh-gossip — 0.101.0, read 2026-09-10 — API documentation and source — HyParView/PlumTree parameters, message-size constants, broadcast queueing.
39. GitHub issue `n0-computer/iroh#2317` "Enhancing iroh's Hole Punching Success Rate" — https://github.com/n0-computer/iroh/issues/2317 — opened 2024-05-22, closed 2024-10-10 — issue thread (third-party report with maintainer replies) — packet-loss vulnerability, stance against client-as-relay.
40. Blog: "Comparing Iroh & Libp2p" — https://www.iroh.computer/blog/comparing-iroh-and-libp2p — 2024-01-05 — (blog) — libp2p ~70% hole-punch figure attributed to a Protocol Labs campaign.
41. EMQX documentation: MQTT over QUIC — Introduction — https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/introduction.html — `latest` tree, read 2026-09-10 — vendor documentation — motivation, limitations, future work, standardisation statement.
42. EMQX documentation: MQTT over QUIC — Features and Benefits — https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/features-mqtt-over-quic.html — read 2026-09-10 — vendor documentation — single-stream and multi-stream modes, stream-packet binding, per-stream ordering rule.
43. EMQX documentation: Use MQTT over QUIC — https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/getting-started.html — read 2026-09-10 (example image `emqx/emqx:5.8.8`) — vendor documentation — listener configuration, client SDK inventory, TCP fallback.
44. EMQX blog: "MQTT over QUIC: Next-Generation IoT Standard Protocol" — https://github.com/emqx/blog/blob/main/en/202208/mqtt-over-quic.md (published 2022-08-24) — read 2026-09-10 — (blog) / (vendor benchmark) — CPU/memory/bandwidth table, 0-RTT and NST validity, migration and loss tests, OASIS intent.
45. EMQX blog: "How Multi-Stream of QUIC Could Mitigate the HOL Blocking Issue of MQTT Connection" — https://www.emqx.com/en/blog/multi-stream-of-mqtt-over-quic — 2025-04-18 — (blog) / (vendor benchmark) — three-way single-stream versus multi-stream test with per-second counters, stream priorities, ordering trade-off.
46. `emqx/quic` (quicer) README — https://raw.githubusercontent.com/emqx/quic/main/README.md — read 2026-09-10 — repository README — msquic NIF binding, "Project Status: Preview", OTP 25+, TLS backend selection.
47. EMQX Enterprise 5.1.0 release notes — https://www.emqx.com/en/blog/emqx-enterprise-5-1-0-release-notes — 2023-06-29 — (blog, retrieved via search 2026-09-10) — production-ready declaration for MQTT over QUIC.
48. EMQ, Intel and SJTU: "Explore MQTT over QUIC together" — https://github.com/emqx/blog/blob/main/en/202310/emq-intel-and-sjtu-explore-mqtt-over-quic-together.md — 2023-10 — (blog, retrieved via search 2026-09-10) — loss-rate-dependent stability finding.
49. `draft-ietf-moq-transport-21`, "Media over QUIC Transport" — https://www.ietf.org/archive/id/draft-ietf-moq-transport-21.txt — 2026-09-08, WG document — Internet-Draft — object model, stream usage, priorities, delivery timeouts, sessions and GOAWAY, relays, 0-RTT analysis, security and resource-exhaustion sections, change log.
50. IETF moq working group document list — https://datatracker.ietf.org/wg/moq/documents/ — read 2026-09-10 — working-group metadata — adopted and individual drafts (secure objects, C4M, Privacy Pass, MOCHA family, moq-lite, discovery).
51. `draft-jennings-moq-mocha-chat-00`, "MOCHA Chat: Messaging over MoQ Transport" — https://www.ietf.org/archive/id/draft-jennings-moq-mocha-chat-00.txt — 2026-07-06 — Internet-Draft (individual) — per-device tracks, minute groups, one object per message, causal DAG ordering, edit/delete rules.
52. `draft-lcurley-moq-lite-05`, "Media over QUIC — Lite" — https://datatracker.ietf.org/doc/draft-lcurley-moq-lite/ — 2026-06-30, Informational, "not endorsed by the IETF" — Internet-Draft (individual) — rationale, broadcast/track/group/frame model, expiration, prioritization, Qmux bindings.
53. `draft-ietf-webtrans-http3-16`, "WebTransport over HTTP/3" — https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/ — 2026-07-06, WG Last Call — Internet-Draft — session establishment, stream and datagram wire formats, flow-control capsules, close/drain capsules, `RESET_STREAM_AT` requirement, capsule variant.
54. `draft-ietf-webtrans-overview-13`, "The WebTransport Protocol Framework" — https://datatracker.ietf.org/doc/draft-ietf-webtrans-overview/ — 2026-07-06, WG Last Call — Internet-Draft — motivation against WebSocket and WebRTC, transport requirements, message definition, priority non-goal.
55. W3C WebTransport — https://www.w3.org/TR/webtransport/ (Candidate Recommendation Snapshot 2026-07-30) and https://w3c.github.io/webtransport/ (editor's draft 2026-09-08) — read 2026-09-10 — W3C specification — API surface, datagram queues, `sendOrder`/`sendGroup`, certificate hashes and two-week rule, statistics, security considerations.
56. MDN browser-compat-data `api/WebTransport.json` — https://raw.githubusercontent.com/mdn/browser-compat-data/main/api/WebTransport.json — retrieved 2026-09-10 — third-party compatibility data — per-browser and per-option support.
57. `moq-dev/moq` README and `moq-net` crate documentation — https://github.com/kixelated/moq and https://docs.rs/moq-net/latest/moq_net/ — `moq-net` 0.2.19 (2026-09-09), `@moq/net` 0.3.5 — repository README and crate docs — dual-protocol negotiation, feature-intersection interop, subgroup-0 degradation, crate inventory.
58. Blog: "The First MoQ CDN: Cloudflare" — https://moq.dev/blog/first-cdn/ — 2025-08-21 — (blog) — criticism of the standardisation process (650+ issues, 500+ PRs), Cloudflare preview limits, WebSocket fallback and JWT auth in moq-relay.
59. Cloudflare MoQ posts — https://blog.cloudflare.com/moq/ and https://blog.cloudflare.com/moq-relays/ — 2025-08-22 and 2026-07-31 — (vendor blog, retrieved via search 2026-09-10) — relay provisioning with separate publisher/subscriber credentials, MoQ layering on WebTransport.
60. `wtransport` — https://docs.rs/wtransport/latest/wtransport/ — 0.7.2, released 2026-08-11 — crate documentation — Rust WebTransport-over-HTTP/3 on quinn; "not considered completely production-ready".
61. `webtransport-go` — https://github.com/quic-go/webtransport-go — v0.13.0, 2026-08-30, README states draft-16 — repository README and module metadata — draft tracked and downstream user list.
62. Centrifugo: WebTransport transport — https://centrifugal.dev/docs/transports/webtransport — page undated, read 2026-09-10 (feature introduced in Centrifugo v4, 2022-07-19) — product documentation — experimental status, single bidirectional stream, HTTP/3 proxy requirements.
63. Socket.IO WebTransport guide and Engine.IO protocol v4.1 — https://socket.io/get-started/webtransport and https://socket.io/docs/v4/engine-io-protocol/ — support added in Socket.IO 4.7.0 (June 2023); pages undated, read 2026-09-10 — product documentation — one-stream-per-session rule, certificate constraints.
64. libp2p WebTransport specification — https://github.com/libp2p/specs/blob/master/webtransport/README.md — r0, 2022-10-12 — specification — certificate-hash addressing, in-band Noise handshake, 14-day dual-certificate rotation.
65. Search-only items on RPC and games over WebTransport: Effect-TS issue #6247 (opened 2026-05-24), Godot proposal #3899 (opened 2022-01-31), Connect "choosing a protocol" page, Kreya gRPC-web analysis (2026-03-23) — retrieved via search 2026-09-10, not read in full — issue trackers, documentation, third-party blog — evidence that RPC and game-engine support are at request/experiment stage.
66. gRPC over HTTP/2 (`PROTOCOL-HTTP2.md`) — https://raw.githubusercontent.com/grpc/grpc/master/doc/PROTOCOL-HTTP2.md — undated `master`, read 2026-09-10 — specification — one RPC per stream, message framing, `grpc-timeout`, `grpc-status` trailers, retry position.
67. gRFC G2: gRPC over HTTP/3 — https://raw.githubusercontent.com/grpc/proposal/master/G2-http3-protocol.md — status Implemented, last updated 2021-08-25 — specification — unchanged framing, error mapping, HTTP/3-only versus Alt-Svc negotiation.
68. GitHub issue `grpc/grpc-go#5186` — https://github.com/grpc/grpc-go/issues/5186 — opened and closed 2022-02-08, updated 2022-08-08 — issue — dedicated HTTP/3 request closed, redirected upstream.
69. GitHub issue `grpc/grpc#19126` — https://github.com/grpc/grpc/issues/19126 — opened 2019-05-23, open, updated 2026-07-14 — issue — cross-language status, Cronet client-only comments.
70. Microsoft: Use HTTP/3 with the ASP.NET Core Kestrel web server — https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel/http3 — 2026-04-14, .NET 10 view — vendor documentation — fully supported in .NET 7+, QUIC transport defaults, Alt-Svc, recommended protocol set.
71. Microsoft: Troubleshoot gRPC on .NET (HTTP/3 client configuration) — https://learn.microsoft.com/en-us/aspnet/core/grpc/troubleshoot — updated 2026-08-26 — vendor documentation — client support from .NET 6, Alt-Svc upgrade, forced version handler.
72. Microsoft: Use HTTP/3 with HttpClient — https://learn.microsoft.com/en-us/dotnet/core/extensions/httpclient-http3 — published 2023-05-19, updated 2026-03-30 — vendor documentation — `Http3Support` switch, libmsquic 1.9 constraint, no QUIC network transitions in .NET 7.
73. Connect Protocol Reference — https://connectrpc.com/docs/protocol/ — Connect protocol version 1, undated, read 2026-09-10 — specification — unary and streaming mapping, envelope format, `EndStreamResponse`, error codes.
74. Connect FAQs — https://connectrpc.com/docs/faq/ — undated, read 2026-09-10 — documentation — two-protocol trade-off, HTTP/2 requirement for bidi streaming, uneven HTTP/3 support.
75. RFC 9114, HTTP/3 — https://www.rfc-editor.org/rfc/rfc9114 — June 2022 — standard — one request per bidirectional stream, trailer field sections, `h3` ALPN, TCP fallback recommendation.
76. `h3` crate documentation — https://docs.rs/h3/latest/h3/ — 0.0.8, read 2026-09-10 — crate documentation — existence of a Rust HTTP/3 library only.
77. Quilkin workspace `Cargo.toml`, README and releases — https://github.com/googleforgames/quilkin — workspace version 0.10.1; `quilkin-v0.10.0` 2026-01-27, component crates 2026-05-26; read 2026-09-10 — repository sources and release metadata — identity, beta status, dependency set.
78. Quilkin documentation (`introduction.md`, `filters.md` and per-filter pages, `services/udp.md`, `services/qcmp.md`, `deployment/examples.md`, `deployment/configuration.md`, `faq.md`) — https://github.com/googleforgames/quilkin/tree/main/docs/src — `main`, read 2026-09-10 — project documentation — filter chain semantics, tokens and sessions, QCMP, topologies, benchmark policy, documentation drift.
79. Quilkin sources (`src/service.rs`, `src/net/sessions.rs`, `src/filters/capture/quic.rs`) — https://github.com/googleforgames/quilkin/tree/main/src — `main`, read 2026-09-10 — source code — session timeout and cap, service ports, QUIC DCID parsing without QUIC state.
80. `quilkin-corrosion` (`Cargo.toml`, `src/gossip/transport.rs`) — https://github.com/googleforgames/quilkin/tree/main/crates/corrosion — `main`, read 2026-09-10 — source code — quinn 0.11 plus quinn-plaintext, traffic classes mapped to datagram/uni/bi, keep-alive and error taxonomy.
81. Google Cloud blog: "Introducing Quilkin: open-source UDP proxies built for game server communication" — https://cloud.google.com/blog/products/gaming/introducing-quilkin — 2021-07-16 — (blog) — stated reasoning about game traffic and proxy benefits.
82. libp2p specification: QUIC in libp2p — https://github.com/libp2p/specs/blob/master/quic/README.md — r1, 2022-12-30, maturity Recommendation — specification — rationale, UDP-blocked drawback, multiaddr code points, ALPN `libp2p`.
83. libp2p specification: libp2p TLS Handshake — https://github.com/libp2p/specs/blob/master/tls/tls.md — r0, 2019-03-23 — specification — public-key extension OID, `SignedKey` and its signature string, certificate rules, mutual authentication.
84. libp2p specification: Peer Ids and Keys — https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md — r2, 2021-04-30 — specification — 42-byte identity-multihash rule, key types, deterministic encoding.
85. libp2p specification: Connection Establishment — https://github.com/libp2p/specs/blob/master/connections/README.md — r1, 2022-12-07 — specification — multistream-select, upgrade order, stream properties, connection limits, session-resumption future work.
86. libp2p documentation: QUIC — https://libp2p.io/docs/quic/ — undated, read 2026-09-10 — project documentation — native stream muxing, single-RTT handshake, head-of-line blocking argument, ossification argument.
87. go-libp2p sources (`p2p/transport/quicreuse/config.go`, `p2p/transport/quic/transport.go`) — https://github.com/libp2p/go-libp2p — `master`, read 2026-09-10 — source code — stream and window defaults, keep-alive, hole-punch timeout, no-PSK restriction.
88. rust-libp2p `transports/quic/src/config.rs` — https://github.com/libp2p/rust-libp2p — `master`, read 2026-09-10 — source code — timeouts and windows, disabled unidirectional streams and datagrams, migration disabled with rationale.
89. libp2p specifications: hole punching (r1, 2022-06-13), AutoNAT v2 (r2, 2023-04-15), Circuit Relay v2 (r3, 2023-02-28), DCUtR (r1, 2021-11-20) — https://github.com/libp2p/specs — read 2026-09-10 — specifications — NAT-traversal stack and relay cost statements.
90. Blog: "go-libp2p in 2022" — https://blog.libp2p.io/2023-02-13-go-libp2p-in-2022/ — 2023-02-13 — (blog) — QUIC version distinction and dial preference, optimized muxer selection, resource manager.
91. Blog: QUIC in libp2p (adoption and implementation cost) — https://blog.libp2p.io/ — 2023-09-13 — (blog) — 80-90% of PL bootstrapper connections over QUIC, `crypto/tls` fork cost, 0-RTT coordination.
92. GitHub issues `nats-io/nats-server#457` (created 2017-03-24, closed 2017-08-02, updated 2026-05-03) and `#3140` (created 2022-05-21, open, updated 2026-04-14) — https://github.com/nats-io/nats-server/issues/3140 — queried 2026-09-10 — issue threads — deferral chain, maintainer arguments, the multi-stream gateway proposal and its ordering rebuttal, performance dispute.
93. `nats-io/nats-server` `go.mod` — https://raw.githubusercontent.com/nats-io/nats-server/main/go.mod — `main`, go 1.26.0, read 2026-09-10 — source manifest — no QUIC dependency of any kind.
94. `nats-io/nats-architecture-and-design` issue #257 "QUIC as secure transport" and the ADR index — https://github.com/nats-io/nats-architecture-and-design — opened and closed 2023-12-16; index read 2026-09-10 — issue and index — no QUIC ADR exists.
95. NATS documentation: WebSocket, MQTT and leaf-node configuration, and default ports — https://docs.nats.io/ — read 2026-09-10 — project documentation — transport inventory, WebSocket as the precedent, leaf-node model, per-transport knobs.
96. Kafka protocol guide — https://kafka.apache.org/43/design/protocol/ — Kafka 4.3 docs, page modified 2026-05-22 — project documentation — "binary protocol over TCP", framing, single in-flight request ordering guarantee, correlation ids, ApiVersions and SASL sequences, fetch sessions, error codes.
97. Kafka producer configuration reference — https://kafka.apache.org/43/generated/producer_config.html — Kafka 4.3, read 2026-09-10 — project documentation — `max.in.flight.requests.per.connection` and the five-batch broker retention rule, idempotence constraints, buffer and timeout defaults.
98. Negative-search record for Kafka/Redpanda QUIC: KIP index (page version 3945), Apache JIRA query `project=KAFKA AND text~"QUIC"` (total 0), GitHub issue/PR/code searches — https://cwiki.apache.org/confluence/spaces/KAFKA/pages/50859233/Kafka+Improvement+Proposals — queried 2026-09-10 — search records — evidenced absence of QUIC work.
99. Redpanda broker configuration properties — https://docs.redpanda.com/streaming/current/reference/properties/broker-properties/ — page version 26.2, git-modified 2026-09-03 — vendor documentation — listener inventory (all TCP), TLS options, absence of any UDP/QUIC listener.
100. KIP-559: Make the Kafka Protocol Friendlier with L7 Proxies — https://cwiki.apache.org/confluence/spaces/KAFKA/pages/144511170/ — state Accepted, page updated 2020-02-28 — project documentation — non-self-contained JoinGroup/SyncGroup messages.
101. P. Kumar and B. Dezfouli, "Implementation and analysis of QUIC for MQTT" — https://www.sciencedirect.com/science/article/abs/pii/S1389128618310776 — *Computer Networks*, online 2018-12-21 — paper (third-party) — connection overhead reduced by up to 56% in packets exchanged.
102. "Performance evaluation of AMQP over QUIC in the internet-of-things networks" — https://www.sciencedirect.com/science/article/pii/S1319157823000551 — *Journal of King Saud University CIS*, online 2023-03-02 — paper (third-party) — satellite and mobile start-up latency improvements.
103. `capnproto/capnproto` `c++/src/capnp/rpc.capnp` — https://raw.githubusercontent.com/capnproto/capnproto/master/c%2B%2B/src/capnp/rpc.capnp — file undated (copyright 2013-2014), read from `master` 2026-09-10 — specification in source — vats, four tables, E-Order, message union, `Provide`/`Accept`, embargoes and the Tribble race, third-party identifiers.
104. Cap'n Proto: RPC Protocol — https://capnproto.org/rpc.html — undated, read 2026-09-10 — project documentation — promise pipelining, capability security, disconnection semantics, level definitions, byte-stream transport position.
105. Cap'n Proto: C++ RPC ("Current Status") — https://capnproto.org/cxxrpc.html — undated page, statement scoped to version 0.4, read 2026-09-10 — project documentation — Level 1 implementation status.
106. Blog: "Cap'n Proto 0.8: Streaming flow control, HTTP-over-RPC, fibers" — https://capnproto.org/news/2020-04-23-capnproto-0.8.html — 2020-04-23 — (blog, project-official) — `-> stream` convention, socket-buffer window "hack", http-over-capnp immaturity.
107. Blog: Cap'n Proto 1.0 — https://capnproto.org/news/2023-07-28-capnproto-1.0.html — 2023-07-28 — (blog, project-official) — three-party handoff and shared-memory RPC still unfinished.
108. `capnproto/capnproto` `c++/src/capnp/rpc.h` and three-party-handoff pull-request history (#2111 merged 2025-06-11, #2337 revert merged 2025-06-21) — https://github.com/capnproto/capnproto — read 2026-09-10 — source and repository metadata — `VatNetwork` requirement, `setFlowLimit` semantics and deadlock warning, `getPeerVatId`, revert of the 3PH implementation.
109. OMG DDS Interoperability Wire Protocol (DDSI-RTPS) 2.5 — https://www.omg.org/spec/DDSI-RTPS/2.5/ — OMG document formal/22-04-01, April 2022 — standard — version identity of the wire protocol (specification body not read; see gaps in the reply).
110. eProsima Fast DDS documentation, Transport Layer and Security — https://fast-dds.docs.eprosima.com/en/v3.6.2/ — 3.6.2, read 2026-09-10 — project documentation — transport inventory (UDPv4/v6, TCPv4/v6, SHM), TLS over TCP, security plugins, flow controllers.
111. RTI Connext User's Manual, Transport Plugins — https://community.rti.com/static/documentation/connext-dds/current/doc/manuals/connext_dds_professional/users_manual/ — current manual, read 2026-09-10 — vendor documentation — builtin UDPv4/UDPv6/shared memory/Real-Time WAN, TCP as extension, no QUIC.
112. `zenoh-plugin-ros2dds` / `zenoh-bridge-ros2dds` documentation — https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds — read 2026-09-10 — project documentation — bridge motivation, isolation requirements, Zenoh-side transport.
113. "StreamRTPS" — https://arxiv.org/abs/2606.14214 — 2026-06-12 — paper (third-party) — 2-byte stream identifiers, payload aggregation, heartbeat suppression, 27.9% and 22.7% reductions.
114. Survey of data-distribution middleware — https://arxiv.org/abs/2607.01304 — 2026-07-01 — paper (third-party) — RTPS as UDP-primary with TCP fallbacks; QUIC present only as a Zenoh link protocol.
115. Negative-search record for DDS/RTPS over QUIC: GitHub issue searches in `eProsima/Fast-DDS`, `OpenDDS/OpenDDS`, `eclipse-cyclonedds/cyclonedds`; arXiv/ACM/web literature searches — queried 2026-09-10 — search records — evidenced absence of a QUIC transport or measurement.
116. K. Winstein and H. Balakrishnan, "Mosh: An Interactive Remote Shell for Mobile Clients" — https://mosh.org/mosh-paper.pdf — USENIX ATC 2012 — paper — State Synchronization Protocol, Instructions and diffs, AES-OCB, roaming rule, rate control and measurements, stated limitations.
117. Mosh project site — https://mosh.org/ — site undated; release entry 1.4.0, 2022-10-31 — project documentation — SSH bootstrap and UDP operation, current release.
118. F. Michel and O. Bonaventure, "Towards SSH3: how HTTP/3 improves secure shells" — https://arxiv.org/abs/2312.08396 — arXiv:2312.08396v1, 2023-12-12 — paper — architecture on quic-go, one stream per channel, datagram forwarding, authentication, throughput figures, 0-RTT caveat.
119. `draft-michel-remote-terminal-http3-00`, "Remote terminal over HTTP/3 connections" — https://datatracker.ietf.org/doc/html/draft-michel-remote-terminal-http3-00 — 2024-07-31, Experimental, expired 2025-02-01 — Internet-Draft — Extended CONNECT fields, `SETTINGS_H3_DATAGRAM`, HTTP authentication schemes.
120. `francoismichel/ssh3` README — https://github.com/francoismichel/ssh3 — read 2026-09-10 — repository README — Apache-2.0, proof-of-concept warnings, 3 versus 5-7 RTT claim, migration listed as "soon".
121. J. A. Donenfeld, "WireGuard: Next Generation Kernel Network Tunnel" — https://www.wireguard.com/papers/wireguard.pdf — NDSS 2017; permanent draft revision dated 2020-06-01 — paper — cryptokey routing, `AllowedIPs`, Noise_IK handshake, endpoint learning and roaming, timers and stated future work.
122. Tailscale documentation: Connection types — https://tailscale.com/docs/reference/connection-types — last validated 2026-06-01 — vendor documentation — direct/DERP/peer-relay order, hard-NAT limitation, relay throughput caveat.
123. Tailscale documentation: DERP servers — https://tailscale.com/docs/reference/derp-servers — last validated 2026-01-21 — vendor documentation — DERP definition, DISCO and WireGuard packet roles, DERP map and home selection.
124. Blog: "How NAT traversal works" — https://tailscale.com/blog/how-nat-traversal-works — 2020-08-21 — (blog) — simultaneous UDP transmission, coordination server as side channel, 30-second firewall timeout example.
125. Tailscale documentation: Control and data planes; Node keys — https://tailscale.com/docs/concepts/control-data-planes and https://tailscale.com/docs/concepts/node-keys — last validated 2026-01-05 — vendor documentation — coordination-service duties, machine versus node keys, offline behaviour, revocation.
126. Blog: "Key management characteristics of the Tailscale Control Protocol" — https://tailscale.com/blog/tailscale-key-management — 2021-03-04 — (blog) — Curve25519 machine and node keys, control-connection ECDH, policy compilation.
127. Tailscale documentation: Access control (last validated 2025-05-29) and Tailnet Lock (last validated 2025-12-02) — https://tailscale.com/docs/features/access-control and https://tailscale.com/docs/features/tailnet-lock — vendor documentation — deny-by-default grants and ACLs, signing requirements for joining nodes.
128. Blog: "NAT traversal, and how we're improving it (pt. 1)" — https://tailscale.com/blog/nat-traversal-improvements-pt-1 — 2025-10-15 — (blog) / (vendor-reported metric) — direct traversal "well north of 90% in typical conditions", STUN/ICE path racing, IPv6 caveats.
