# 0013: Standalone competitor libraries beside weida, not backends inside it

Status: provisional
Date: 2026-09-11
Relates to: [0006](0006-guarantee-sets.md) §4.6-§4.9; [0008](0008-session-identity.md) §4.2,
§5; [0010](0010-local-transport.md) §4.4, §4.5, §4.8;
[0012](0012-local-connection-grouping.md) §4.1, §4.2; [LOOP.md](../LOOP.md) §9 Phase B;
[adapters/zmtp.md](../adapters/zmtp.md); [ARCHITECTURE.md](../ARCHITECTURE.md) §4, §5

## 1. The question

[LOOP.md](../LOOP.md) §9 Phase B says "Each foreign protocol is its own crate under
`crates/adapters/`" and gives six slices, of which three, four and five are a bridge inbound,
a bridge outbound and an interop bench. The ZeroMQ line built exactly that: a sans-I/O codec
(`crates/adapters/weida-zmtp`), a mapping document ([adapters/zmtp.md](../adapters/zmtp.md))
and a bridge in both directions (`crates/adapters/weida-zmtp-bridge`). The SP line repeated
it. Both work, both are tested against real foreign implementations, and both produce a shape
the project does not actually want.

**A bridge is a hop, and a hop is a process.** The mapping document says it in those words:
"The adapter is a **hop**, not a tunnel. It terminates ZMTP … and it terminates weida"
[zmtp §1]. That is the right description of what a bridge *is*, and it is also its ceiling. A
weida application that also has to talk to a ZeroMQ peer cannot do it by linking a library; it
has to stand a bridge up — a listener, an address, a lifecycle, a deployment — between itself
and a peer it could have spoken to directly. `Inbound::bind` takes a `SocketAddr` and a weida
endpoint and serves whoever connects (`crates/adapters/weida-zmtp-bridge/src/inbound.rs`);
there is no way to hold a `REQ` socket in your hand.

**The protocol logic is trapped.** Everything a ZeroMQ library needs already exists inside
that bridge, and is reachable by nobody. `wire.rs` drives the greeting, the NULL handshake and
the framed reader. `outbound.rs::Liveness` implements `ZMQ_HEARTBEAT_IVL` and gates `PING` on
the negotiated version. `inbound.rs::legacy_subscription` implements both subscription wire
forms. `subscriptions.rs` reference-counts subscriptions the way 37/ZMTP requires
("Subscribing to 'A' and 'A' counts as two subscriptions" [zeromq §2]). None of it is public,
none of it is a socket, and all of it was written to be thrown at one weida endpoint. The
repository contains most of a ZeroMQ implementation and ships zero ZeroMQ libraries.

**And the third option is worse.** The obvious fix — teach `Runtime::requester` to dial
`zmq://` — would put a second pattern semantics behind one API. It is rejected in §3, and the
rest of this note is what replaces it.

So: **what is the repository's product line, and where does a foreign protocol's
implementation live?**

## 2. The evidence, condensed

**"First-class" has to be a checkable list, and the ZeroMQ sheet already is one.** A complete
implementation is not a slogan but the union of five inventories, every one of them
enumerated:

- **Socket types.** Twenty rows in `zmq_socket(3)`'s table, each with compatible peers,
  direction, send/receive pattern, outgoing and incoming routing and the action in mute state
  [zeromq §4.1]: REQ, REP, DEALER, ROUTER, PUB, SUB, XPUB, XSUB, PUSH, PULL, PAIR stable, plus
  the draft thread-safe family CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER/CHANNEL and
  `ZMQ_STREAM` [zeromq §4.6]. Each carries semantics the table does not hold: REQ's lockstep
  with `EFSM` [zeromq §4.2], ROUTER's routing-id frame and silent drop [zeromq §4.2, §8], PUB's
  publisher-side prefix match [zeromq §4.3], PAIR's single peer and absent auto-reconnect
  [zeromq §4.5].
- **Transports.** `tcp`, `ipc`, `inproc` as the working set; `pgm`/`epgm`, `udp`, `vmci`,
  `tipc`, `vsock`, `ws`/`wss` as the rest, with `ws`/`wss` "disabled by default if DRAFT APIs
  are disabled" [zeromq §13].
- **Security.** NULL, PLAIN, CURVE, one mechanism per socket and assertive
  ("This prevents downgrade attacks"), plus ZAP as the authorization dialog over
  `inproc://zeromq.zap.01` with 200/300/400/500 and a user id, one handler per process
  [zeromq §10]. CURVE is fully specified down to the octet: four keys, HELLO 200 / WELCOME 168
  / INITIATE 257+ / READY 30+, a cookie the server "MUST discard… after a short interval", and
  an ABNF-versus-prose contradiction on the padding that the sheet resolves in favour of the
  grammar [zeromq §10].
- **Options and bounds.** Twenty-four rows of limits with their defaults [zeromq §11] and the
  whole `zmq_setsockopt` surface behind them — HWMs at 1000, `ZMQ_MAXMSGSIZE` at -1,
  `ZMQ_LINGER` at infinite, reconnect intervals, handshake interval, heartbeat triple, ZAP
  domain, CURVE keys, `ZMQ_ROUTER_MANDATORY`, `ZMQ_XPUB_VERBOSE`, `ZMQ_CONFLATE`,
  `ZMQ_REQ_CORRELATE`.
- **Monitoring and devices.** `zmq_socket_monitor`, a `ZMQ_PAIR` bound to an `inproc://`
  endpoint, "connection-oriented transports" only; `zmq_proxy(frontend, backend, capture)`
  [zeromq §2]. And above them the zguide's canonical recipes, each with a stated guarantee and
  a stated failure mode: Lazy Pirate, Simple and Paranoid Pirate, the load-balancing broker,
  Majordomo with MMI, Titanic, Binary Star, Freelance's three models, Clone/CHP, Espresso,
  the last-value cache [zeromq §9].

**What the one pure-Rust alternative does not have, from the same sheet.** zmq.rs is "A native
Rust implementation of ZeroMQ" with its own disclaimer, "This codebase does not implement all
of ZeroMQ's feature set"; transports TCP and IPC only; patterns REQ, REP, DEALER, ROUTER, PUB,
SUB, XPUB, XSUB, PUSH, PULL — so no PAIR and none of the thread-safe draft family
[zeromq §13]. Measured against 0.6.0, three more gaps are observable in one exchange: it
announces ZMTP **3.0**; its command decoder knows `READY` and answers `PING`, `PONG` and
`ERROR` with "Unknown command received" and a close; and it speaks ZMTP 2.0's subscription
form rather than the 3.x commands [zeromq §13]. Nothing in the sheet records NULL-only or
CURVE support for it either way, and its own documentation does not claim security mechanisms.
That is the gap a first-class implementation is measured into, and it is wide enough that
"another ZeroMQ in Rust" is not a redundant project.

**What weida already has that a ZeroMQ implementation needs.** Four things, all of them
general and none of them weida-protocol-shaped:

- **Runtime ownership.** `Runtime::new`, `with_handle` and `owned` differ "only in where the
  reactor comes from" [ARCHITECTURE §5], and the owned variant exists precisely so a caller
  needs no reactor of its own — `futures::executor::block_on` drives a full round trip against
  it (`crates/weida/tests/foreign_executor.rs`). A ZeroMQ library needs exactly that: libzmq's
  context owns an I/O thread pool and its users are overwhelmingly synchronous
  [zeromq §2, §13].
- **`Exec`: spawn, sleep, within, enter, resolve.** "the crate's **whole** surface onto the
  async runtime … Nothing outside that file calls `tokio::spawn`, `tokio::time` or
  `lookup_host`" [ARCHITECTURE §5], with a resolver that parses an IP literal in place, spawns
  a real lookup, returns *every* address and caps the count because a resolver answer is
  remote input (`crates/weida/src/runtime.rs`). ZeroMQ's own author lists synchronous DNS
  among libzmq's architectural mistakes — "when DNS was unavailable, the whole library,
  including the sockets that haven't used DNS, just hung" [zeromq §13]. The timers are the
  same story: `ZMQ_RECONNECT_IVL`, `ZMQ_HANDSHAKE_IVL`, `ZMQ_HEARTBEAT_IVL`,
  `ZMQ_CONNECT_TIMEOUT`, `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` and a finite `ZMQ_LINGER` are five
  clocks, and `Exec::sleep`/`Exec::within` already own the wheel.
- **Local-transport hygiene.** `AF_UNIX` bind with an explicit `0600` mode set after bind
  rather than inherited from `umask`, unlink-then-bind, a socket-type check before unlinking,
  a decoded path budget, and `SO_PEERCRED`/`LOCAL_PEERCRED` as the peer's proof
  ([0010](0010-local-transport.md) §4.5, `crates/weida/src/unix.rs`). ZeroMQ's `ipc://` has
  every one of those problems and answers none of them: the path limit is "113 characters
  including the 'ipc://' prefix", a local process can steal a bound endpoint, and the
  uid/gid/pid filters are deprecated in favour of ZAP [zeromq §10, §11]. The `inproc` registry
  is the same shape twice: weida's bus names are capped at 256 bytes because libzmq's `inproc`
  names are ([0010](0010-local-transport.md) §4.8, [zeromq §11]).
- **`Limits` as a discipline.** Not the type — the habit. "No remote input can cause unbounded
  memory allocation" [INVARIANTS], and the B-051 review pass extended it to the adapters after
  finding two unbounded structures in the ZMTP bridge, adding `max_pending_exchanges` and a
  subscription-table ceiling [INVARIANTS]. A ZeroMQ implementation inherits that rule against
  a protocol whose grammar permits a 2^63-1 octet frame and whose only defence is an option
  defaulting to "no limit" [zeromq §3, §11].

**What it must not share.** Three things, and the boundary is sharp:

- **weida's frames.** The DATA header, the preamble, the CBOR headers, `negotiate()` and the
  error codes are `weida-protocol`. ZMTP has its own framing (one flags octet, one- or
  eight-octet size, MORE and COMMAND [zeromq §3]) and its own handshake.
- **HELLO and the guarantee declarations.** A guarantee set is declared in HELLO and
  intersected ([0006](0006-guarantee-sets.md) §4.4). ZMTP's `READY` metadata is a property
  dictionary with `Socket-Type` and `Identity` and carries no guarantee vocabulary at all,
  because there is nothing to declare: "No frame, command or field acknowledges a message"
  [zeromq §6].
- **The `Link` transport enum.** `crates/weida/src/transport.rs` is "an enum rather than a
  trait object, deliberately" because "the set of transports is closed and small, it is
  decided in this crate". Its `Unix` variant *is*
  [0012](0012-local-connection-grouping.md): the `0x01`/`0x02`/`0x03` preamble, the 16-byte
  group token, the credential check, the parked reverse pool. A ZMTP connection over `ipc://`
  carries none of that — it is a plain `SOCK_STREAM` with a greeting on it, one connection
  carrying many messages, reconnecting by itself. Sharing `Link` would put weida's local
  preamble byte on a ZeroMQ socket, which is the one thing that would make the foreign
  implementation not foreign.

**And the project's own crate-splitting rule has just been satisfied.** ARCHITECTURE §4 keeps
runtime and transport in one crate on master doc §73's authority — "split crates where the
dependency/ownership boundary is real" — and states the trigger: "The split becomes worthwhile
when a second transport or adapter binding appears. Exactly one transport exists, so a
`runtime` crate would have exactly one consumer." A standalone `weida-zmq` is that second
consumer. What this note splits out is *not* §73's `transport-quic`, though: QUIC stays where
it is, because a ZeroMQ library has no use for it.

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — hop-only, today | one codec crate plus one bridge crate per protocol; the protocol logic is private to the bridge | a weida application embedding both cannot reach a ZeroMQ peer without a second process; the ZMTP session layer, the heartbeat, both subscription wire forms and the reference counting are written and unreachable; the repository competes with ZeroMQ while shipping no ZeroMQ |
| B — a zmq backend inside weida's primitives | `Requester` gains a ZeroMQ backend, `zmq://` resolves inside `weida`, one API over two networks | **rejected by the user, and on the merits.** Two pattern semantics behind one type: `Requester` promises concurrent exchanges ([ARCHITECTURE §6b]) and REQ "SHALL send and then receive exactly one message at a time" [zeromq §4.2]; weida's backpressure is `Block` with `Drop` for fan-out and ZeroMQ's is per socket type [zeromq §12/P4]; weida's guarantee chain would end somewhere invisible to the caller, which is exactly the silence [0006](0006-guarantee-sets.md) §4.7 forbids. It also blurs the product line: weida would *be* a ZeroMQ client rather than *compete with* ZeroMQ |
| C — a standalone library plus forwarding helpers | `weida-zmq` is a ZeroMQ implementation usable with no weida in the picture; `weida-zmq-bridge` marries the two networks and is the only place both appear | the workspace roughly doubles in surface; a `weida-runtime` crate has to exist and its public API becomes real; and the honest estimate below is ~36 hours of 90-minute items for ZeroMQ alone, against ~4.5 for the bridge slices it replaces |
| D — a separate repository per competitor | maximal product separation | the shared runtime becomes a published dependency with a version skew, and the cross-adapter test of Phase B slice 6 — a message in through one protocol and out through another — has no workspace to live in |
| E — library, but built on `weida`'s runtime as a dependency | `weida-zmq` depends on `weida` | a ZeroMQ user links quinn, rustls and the whole weida pattern layer to open a `tcp://` socket; and the dependency direction would let a ZeroMQ socket reach weida's types, which is the mistake the empty manifest of `weida-zmtp` exists to prevent [ARCHITECTURE §4] |

## 4. Decision

Option C. The repository holds **two kinds of product**, and says so everywhere:

- **weida** — a QUIC-based messaging protocol and library that competes with ZeroMQ,
  nanomsg/NNG and RabbitMQ and borrows from all of them.
- **Standalone implementations of those competitors in Rust** — `weida-zmq` first, `weida-nng`
  after it — which share weida's runtime and OS plumbing and share **nothing** of weida's
  protocol.

### 4.1 Crate layout: one directory per protocol family

```text
crates/
    core/                     weida-core        I/O-free model
    protocol/                 weida-protocol    weida wire codec
    runtime/                  weida-runtime     reactor, resolver, OS hygiene   (new)
    weida/                    weida             runtime + transports + patterns
    zmq/weida-zmtp/           weida-zmtp        ZMTP codec, no dependencies     (moved)
    zmq/weida-zmq/            weida-zmq         the ZeroMQ implementation       (new)
    zmq/weida-zmq-bridge/     weida-zmq-bridge  weida <-> ZeroMQ forwarder      (renamed)
    nng/weida-sp/             weida-sp          SP codec, no dependencies       (moved)
    nng/weida-nng/            weida-nng         the NNG implementation          (later)
    nng/weida-nng-bridge/     weida-nng-bridge  weida <-> SP forwarder          (renamed)
    interop/cross-tests/      cross-tests       one protocol in, another out    (moved)
```

**Not `crates/adapters/`, and the reason is not taste.** "Adapter" is a defined term in this
repository: LOOP §5 calls it "a codec, bridge or test-bench slice for a foreign protocol",
[adapters/README.md](../adapters/README.md) makes `docs/adapters/<proto>.md` the home of the
adapter-honesty invariant, and [0006](0006-guarantee-sets.md) §4.6 defines an adapter *edge* as
the place a guarantee chain ends. A standalone ZeroMQ library has no edge, terminates no weida
guarantee and answers to no mapping document. Filing it under `adapters/` would put the
product this note exists to create inside the category this note exists to escape, and a reader
scanning the crate list for "does this repository ship a ZeroMQ?" would read the directory name
as "no".

Directories are named for the **protocol family**, not the role, so that everything belonging
to one foreign protocol sits together regardless of whether it is a codec, a library or a
bridge — which is also what makes the `weida-nng` layout a copy rather than a new decision.
`cross-tests` moves out of `adapters/` because it belongs to no single family.

**The named loss is churn.** The move touches README, ARCHITECTURE §4, LOOP §9,
[adapters/zmtp.md](../adapters/zmtp.md) and [adapters/nng.md](../adapters/nng.md)'s path
references, and the workspace manifest, and buys no byte of behaviour. It is done once, in its
own item, and no **crate name** changes — only paths — so nothing published or depended upon
breaks.

### 4.2 What moves into `weida-runtime`, and what does not

New crate `crates/runtime`, depending on `weida-core` and `tokio` and nothing else. Contents,
each with the concrete reason a ZeroMQ implementation needs it:

| Moved | From | Why `weida-zmq` needs it |
| --- | --- | --- |
| `Exec` — `spawn`, `sleep`, `within`, `enter` | `weida/src/runtime.rs:31-90` | five ZeroMQ clocks (reconnect, handshake, heartbeat, connect timeout, send/recv timeout) and a linger budget; `enter` because `TcpListener::bind` needs a reactor context exactly as the quinn constructors do |
| `Exec::resolve` with its address cap and IP-literal fast path | `weida/src/runtime.rs:110-139` | `tcp://host:port` resolution that does not stall the library, against libzmq's named synchronous-DNS weakness [zeromq §13]; the cap because a resolver answer is remote input [INVARIANTS] |
| The three reactor-ownership constructors' machinery: `Exec::current`, `Exec::from_handle`, an owned reactor with the background-shutdown discipline of `OwnedRuntime` | `weida/src/runtime.rs:142-155, 276-322` | a `zmq::Context` must be constructible in a process with no ambient Tokio, which is most of libzmq's audience, and the blocking facade of §4.4 is built on it |
| `AF_UNIX` bind hygiene: unlink-then-bind, explicit mode, socket-type check, path budget; `peer_credentials` | `weida/src/unix.rs:84-137, 306-319` | `ipc://` has the same hazards and libzmq solves none of them [zeromq §11] |
| A generic named-endpoint registry with a byte budget | the `BUSES` map of `weida/src/inproc.rs:41-94`, made generic over what it hands the acceptor | `inproc://` is a context-scoped namespace with a 256-character budget [zeromq §11] |
| `shutdown_timeout` as a **bounded close budget** type | `weida/src/runtime.rs:407-510` | `ZMQ_LINGER` defaults to infinite and `zmq_ctx_term()` can therefore block forever [zeromq §12/P17]; a finite default is §4.4's choice and the budget type is where it lives |

**What stays in `weida`, and stays private:** `Link`/`SendHalf`/`RecvHalf`; the `LocalConn`
stream pair and the whole of [0012](0012-local-connection-grouping.md) — preamble bytes, group
token, credential binding, `ReversePool`, `Deficit`; `ClientPool` and its
`(host, port, ClientTls, fingerprint, path)` key; `ConnCtx` and the connection actor; the
drain receipts of [0009](0009-drain.md); `RuntimeConfig`, `Limits`, `ClientTls`/`ServerTls`;
every pattern endpoint. None of it is general, and all of it is weida's protocol wearing a
runtime's clothes.

**`weida`'s public API does not change.** `Runtime::new`, `Runtime::with_handle(handle,
config)` — still a `tokio::runtime::Handle` — `Runtime::owned`, `shutdown`, `drain`,
`Drained`, `suppressed_duplicates`, `config`, `listener`, `peer`, `requester`, `pusher`,
`subscriber` keep their signatures and their doc comments. `Exec` is `pub(crate)` today, so
publishing it in `weida-runtime` adds surface to a new crate and removes none from `weida`.
The only observable difference for a user of `Runtime` is one more node in `cargo tree`. The
extraction item's acceptance is exactly that: the existing suite compiles unchanged and no
`pub` item in `weida` moves.

**Dependency direction**, extending ARCHITECTURE §4's:

```text
core
 ↑        ↖
protocol   runtime
 ↑          ↑    ↖
weida ──────┘     weida-zmq        weida-zmq-bridge → weida, weida-zmq
```

`weida-runtime` MUST NOT depend on `weida-protocol` or `weida`. `weida-zmq` MUST NOT depend on
`weida` or `weida-protocol`. It *does* depend on `weida-core` transitively, for `Error` and
`LocalPrincipal`; that is deliberate — one OS-error vocabulary is better than two — and
`weida-zmq` maps it to its own errno-named enum at its boundary, which it would have to do
regardless, because libzmq's `EFSM`/`EAGAIN`/`ETERM`/`EHOSTUNREACH` vocabulary is not weida's
and never will be.

### 4.3 `weida-zmtp` stays what it is

Sans-I/O, and **`[dependencies]` stays empty**. That manifest is what makes the codec
checkable against 37/ZMTP instead of against our reading of it [ARCHITECTURE §4], and it
survives CURVE: the five CURVE commands are byte layouts with the crypto boxes as opaque
ranges, and Z85 is arithmetic. The `crypto_box` construction itself lives in `weida-zmq`,
where a dependency is allowed and `unsafe_code` stays forbidden. Only its directory moves.

### 4.4 The shape of `weida-zmq`

1. **Socket-oriented, like libzmq, and typed.** A `Context` (three constructors, mirroring
   `Runtime`'s) holding the `Exec`, the `inproc` namespace and the socket ceiling; then
   `ReqSocket`, `RepSocket`, `DealerSocket`, `RouterSocket`, `PubSocket`, `SubSocket`,
   `XPubSocket`, `XSubSocket`, `PushSocket`, `PullSocket`, `PairSocket` as distinct types, each
   with `bind`, `connect`, `unbind`, `disconnect` and a socket may hold many of each
   [zeromq §2]. Distinct types rather than one handle with a runtime tag, because the
   distinctions are compile-time facts: REQ's alternation is a state machine, PUB cannot
   receive, and rust-zmq already demonstrates the principle with a compile-fail test enforcing
   libzmq's thread-safety rule at the type level [zeromq §13]. libzmq's "MUST NOT use a not
   thread safe socket from multiple threads" becomes `Send + !Sync` and costs nothing; the
   draft thread-safe family, when it lands, is `Sync` and refuses multipart, which is what the
   specification says it is [zeromq §2].

2. **Async first.** `async fn send`/`recv`, plus `try_send`/`try_recv` for `ZMQ_DONTWAIT`'s
   `EAGAIN` and `send_timeout`/`recv_timeout` for `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`. Multipart is
   a first-class `Multipart` value sent and received atomically — "all frames or none"
   [zeromq §3] — which is the single largest thing weida does not have and a ZeroMQ library
   cannot omit.

3. **A blocking facade exists, behind a non-default `blocking` feature.** One wrapper per
   socket over a `Context::owned` reactor, and no second implementation of anything. It exists
   for two reasons and not for comfort: every canonical zguide recipe is written blocking, and
   a bench that had to rewrite them into async would no longer be testing "the zguide's
   patterns run unchanged in spirit"; and `Runtime::owned` was built precisely so a caller
   needs no reactor [ARCHITECTURE §5], so the capability is already paid for.

4. **Options are honoured or refused at configuration time — never silently ignored.**
   Honoured: `ZMQ_SNDHWM`/`RCVHWM`, `ZMQ_MAXMSGSIZE`, `ZMQ_LINGER`, `ZMQ_RECONNECT_IVL` and
   `_IVL_MAX`, `ZMQ_HANDSHAKE_IVL`, `ZMQ_CONNECT_TIMEOUT`, `ZMQ_HEARTBEAT_IVL`/`_TIMEOUT`/
   `_TTL`, `ZMQ_SNDTIMEO`/`RCVTIMEO`, `ZMQ_BACKLOG`, `ZMQ_IMMEDIATE`, `ZMQ_LAST_ENDPOINT`,
   `ZMQ_IPV6`, `ZMQ_TCP_KEEPALIVE*`, `ZMQ_ROUTING_ID`, `ZMQ_SUBSCRIBE`/`UNSUBSCRIBE`,
   `ZMQ_ROUTER_MANDATORY`, `ZMQ_ROUTER_HANDOVER`, `ZMQ_PROBE_ROUTER`, `ZMQ_XPUB_VERBOSE`/
   `_VERBOSER`/`_MANUAL`/`_WELCOME_MSG`, `ZMQ_CONFLATE`, `ZMQ_REQ_CORRELATE`/`_RELAXED`,
   `ZMQ_PLAIN_*`, `ZMQ_CURVE_*`, `ZMQ_ZAP_DOMAIN`, `ZMQ_ZAP_ENFORCE_DOMAIN`, `ZMQ_MAX_SOCKETS`.
   Refused with a message naming why: the multicast group `ZMQ_RATE`, `ZMQ_RECOVERY_IVL`,
   `ZMQ_MULTICAST_HOPS`, `ZMQ_MULTICAST_MAXTPDU` (no `pgm`/`epgm`); `ZMQ_WSS_*` (no
   `ws`/`wss`); `ZMQ_GSSAPI_*` (mechanism not implemented); `ZMQ_SOCKS_PROXY`;
   `ZMQ_TCP_ACCEPT_FILTER` and `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID`, which libzmq itself
   deprecates in favour of ZAP [zeromq §10]; `ZMQ_IO_THREADS` and `ZMQ_BLOCKY`, replaced by
   `Context::owned(worker_threads)` and by the finite close budget of §4.2. Refusal at
   configuration time is the same rule an adapter edge follows
   ([0006](0006-guarantee-sets.md) §4.7), and libzmq's own `EINVAL` for an unknown option
   licenses it.

5. **Two defaults deliberately differ from libzmq, and both are named in the parity table.**
   `ZMQ_LINGER` is finite (the close budget of §4.2) rather than -1, because an infinite
   linger is a hang with a rationale ([0009](0009-drain.md) §4.3) and libzmq's own users know
   it as a bug source [zeromq §12/P17]. `ZMQ_MAXMSGSIZE` has a real default rather than "no
   limit", because "no remote input can cause unbounded memory allocation" [INVARIANTS] and a
   ZMTP frame may declare 2^63-1 octets [zeromq §3]. Both are settable back to libzmq's value;
   neither is silent.

6. **Identity types stay apart, at the type level.** `weida_zmq::CurveKey` is a 32-byte
   Curve25519 public key with Z85 input and output; `weida_zmq::RoutingId` is 1-255
   self-asserted bytes whose first octet is nonzero; `weida_zmq::ZapUserId` is whatever a ZAP
   handler returned on a 200. `weida_core::Fingerprint` is the SHA-256 of a proved TLS public
   key and `weida_core::LocalPrincipal` is what the kernel said. **No `From`, `Into`,
   `AsRef` or `Deref` between the two groups exists anywhere in the workspace**, and none may
   be added: that is loss L6 of [adapters/zmtp.md](../adapters/zmtp.md) §8 and the adapter rule
   of [0008](0008-session-identity.md) §5 turned from prose into a compile error. Where the two
   must meet — a bridge — the meeting is an explicit configuration decision by a human, never a
   conversion. `LocalPrincipal` is shared, because `ipc://` peer credentials are the same
   kernel fact; it is not a ZAP identity and must not be presented as one.

### 4.5 The marriage helpers

`weida-zmq-bridge` (and later `weida-nng-bridge`) is what remains when the protocol leaves:
a **forwarder between a weida endpoint and a `weida-zmq` socket**, holding both on one `Exec`.

**What it does.** Terminates both sides, as it always did. Maps socket type to weida pattern
per [adapters/zmtp.md](../adapters/zmtp.md) §2. Translates subscriptions into weida filters
with the losses of §6 — a mid-segment prefix (L2), the reserved bytes `.`/`*`/`#` (L4) — and
refuses or opts into boundary-subscribe-plus-local-refilter. Refuses genuine multipart (L1).
Holds the reply deadline that turns a ROUTER's silent drop into `ERROR{NO_REPLY}` (L5). Bounds
what weida clients can make it hold (`max_pending_exchanges`, [INVARIANTS]). And it **states
its guarantee set**: `core` on the weida side — delivery `BestEffort`, acknowledgement
`TransportReceipt`, ordering `None`, deduplication `None`, backpressure `Block` with `Drop` for
fan-out ([0006](0006-guarantee-sets.md) §4.2) — and on the ZeroMQ side the losses of
[adapters/zmtp.md](../adapters/zmtp.md) §8 with no transfer point at all beyond `zmq_send`
returning [zeromq §6]. A configuration whose weida side asks for more than `core` is refused
when the bridge is configured, exactly as today ([0006](0006-guarantee-sets.md) §4.7).

**What it no longer does.** It does not implement ZMTP. The greeting, the version downgrade,
the NULL handshake, the framed reader, `PING`/`PONG` and its version gate, the two subscription
wire forms and ZeroMQ's non-idempotent reference counting all move into `weida-zmq`, where they
are a library's behaviour rather than a bridge's private code. The bridge becomes configuration
plus mapping plus refusal, which is all a hop ever was.

**And it does not become a tunnel or a durable hop.** Unchanged from
[0006](0006-guarantee-sets.md) §4.6 and §4.8.

### 4.6 No ZeroMQ inside weida's primitives

Stated so that a later reader does not re-derive it: `weida` gains no `zmq://` scheme, no
ZeroMQ backend behind `Requester`, `Pusher`, `Subscriber` or `Peer`, and no dependency on
`weida-zmq` in any direction. `Address`/`EndpointAddr` keep the three schemes
[0010](0010-local-transport.md) §4.8 defines. A process that wants both links both.

### 4.7 The definition of done for "first-class"

Six clauses; all six, or the parity table says which is missing.

1. **Every socket type of `zmq_socket(3)`'s table** [zeromq §4.1] implemented or listed as
   absent with a reason, with the manual's rows — compatible peers, direction, send/receive
   pattern, routing, mute-state action — asserted by a test each, not described.
2. **`tcp`, `ipc` and `inproc`**, with the rest named absent. `ws`/`wss`, the multicast family
   and `vmci`/`tipc`/`vsock` are out of scope until a user asks.
3. **NULL, PLAIN and CURVE, with ZAP.** CURVE against 26/CURVEZMQ octet counts, the cookie
   discipline and the three security models; ZAP over `inproc://zeromq.zap.01` with the four
   status codes, the user id, domains and `ZMQ_ZAP_ENFORCE_DOMAIN` [zeromq §10].
4. **Interop in both roles** against libzmq through the `zmq` crate (optional dev-dependency,
   `#[ignore]` with the install command where libzmq is absent, per [LOOP.md](../LOOP.md) §2)
   **and** against zmq.rs `zeromq` (pure Rust, always runs), for every pairing each supports —
   our socket as the connecting side and as the bound side.
5. **The zguide's canonical patterns run unchanged in spirit**, as examples that execute in
   `cargo test`: lazy pirate, simple and paranoid pirate, the load-balancing broker, Majordomo
   with MMI, Freelance models one and two, Clone/CHP, Binary Star, the last-value cache and
   Espresso — each asserting the guarantee the guide claims for it rather than merely running
   [zeromq §9].
6. **A feature-parity table against libzmq 4.3.5** in `docs/libraries/zmq.md`, row by row over
   socket types, transports, mechanisms, options, monitor events and devices, where every gap
   is named and no row says "partial" without saying what is missing.

## 5. Consequences and follow-ups

### 5.1 Proposed replacement for [LOOP.md](../LOOP.md) §9 Phase B

Not applied by this note; it is a proposal until the user accepts it.

> **Phase B — the competitor implementations, and the helpers that marry them to weida.**
> Each foreign protocol family is its own directory under `crates/<family>/` and produces two
> products: a **standalone library** that a user of that protocol can use with no weida in the
> picture, and a **forwarder** between a weida endpoint and that library's sockets
> ([decisions/0013](decisions/0013-competitor-libraries.md)). Six slices in this order:
> (1) sans-I/O codec with golden vectors and a fuzz target, with an empty `[dependencies]`;
> (2) `docs/adapters/<proto>.md` — stream, credit and guarantee mapping, transfer points and
> named losses, derived from the research sheet;
> (3) **the library**, in named sub-slices: 3a context, endpoints and error vocabulary on
> `weida-runtime`; 3b messages, per-peer queues and the high-water marks; 3c the connection
> engine with reconnect; 3d the pattern socket types, one sub-slice per family; 3e security
> and authorization; 3f options, monitoring and the devices;
> (4) **the marriage helpers** — the forwarder in both directions, rebuilt on the library,
> stating its guarantee set on the weida side and the foreign side's losses;
> (5) interop bench in both roles against the upstream implementations — pure Rust always,
> the C reference behind `#[ignore]` when absent — plus the numbers;
> (6) cross-adapter test: a message enters through one protocol and leaves through another,
> with the guarantees of both mapping documents asserted.
> Slice 3 is where the mass is: for ZeroMQ it is about eight times the three bridge slices it
> replaces. Slices 1, 2 and the bridges already exist for B1 and B2 and are not rebuilt from
> zero — §5.2 says which code moves where.
> B1 ZeroMQ/ZMTP 3.1: `weida-zmq`, complete to the definition of done of
>    [0013](decisions/0013-competitor-libraries.md) §4.7.
> B2 nanomsg/NNG SP: `weida-nng`, the same six slices in the same order.
> B3 MQTT 5, server side first: sessions, QoS 0/1/2, shared subscriptions, retained messages.
>    It stays an adapter rather than a library until Phase D, because an MQTT server is a
>    broker and the broker is Phase D.
> B4 AMQP 1.0 client (link credit onto the L2 credit of 0003), then NATS core client.
> After every Phase B slice, one research item: update that protocol's sheet with what the
> implementation taught, or open the next protocol's unknowns.

### 5.2 What happens to the existing bridge code

**`weida-zmtp-bridge` splits along the line between protocol and mapping.** Into the library:
`wire.rs` whole (`Session`, `Incoming`, the framed cancel-safe reader, `answer`,
`answer_command`, `sanitize`, `DropQueue`) — it is a ZMTP socket without the name;
`outbound.rs::Liveness` with its version gate, as `ZMQ_HEARTBEAT_IVL`; `SubscriptionForm`, as a
socket-level compatibility choice, because which wire form a peer reads is a property of that
peer's implementation and not of any bridge [zeromq §13]; `inbound.rs::legacy_subscription`
and the wire half of `apply_change`; the pattern-envelope consumption of `body_of` (REQ's empty
delimiter, ROUTER's identity frame), which is the socket's job in libzmq and ours; and the
reference counting in `subscriptions.rs`, which is 37/ZMTP's non-idempotence rule
[zeromq §2] and belongs to SUB and XPUB.

Staying in the forwarder: `Inbound`/`InboundConfig`/`Presenting` and
`Outbound`/`OutboundConfig`/`Dialling` as the configuration surface; `MidSegment` and the
prefix-to-weida-filter translation, which is L2/L4 and exists only because weida filters are
segmented ([0007](0007-topic-namespace.md) §4.2); the refusal of genuine multipart (L1); the
DEALER correlation envelope and `Pending`/`expire`, which exist because a weida `Replier` is
concurrent and a ROUTER drops silently (L5); the weida-side reconnect loop; `BridgeError`,
narrowed as protocol errors move out; and `max_pending_exchanges`. `max_message_bytes` becomes
`ZMQ_MAXMSGSIZE` on the socket, with the bridge keeping its own weida-side cap.

[adapters/zmtp.md](../adapters/zmtp.md) **stays and keeps its role** — it maps the forwarder,
not the library — and gains one paragraph saying that §3's framing rules and §6's two wire
forms are now `weida-zmq`'s behaviour, which the bridge inherits rather than implements.

**`weida-sp-bridge` follows the identical split** when B2 runs: `wire.rs` becomes
`weida-nng`'s pipe and session layer; the eleven protocol numbers, the 8-octet header, the
64-bit framing, the REQ/REP tag stacks and the PAIR v1 hop word become the library's;
`weida-sp` keeps its empty manifest and moves to `crates/nng/`. The forwarder keeps the tag
stack carried beside the exchange, the per-exchange deadline, the peer-close-ends-all rule and
loss L10's "a refusal is a close". The crate is renamed `weida-nng-bridge` in the same commit
that renames `weida-zmq-bridge`, so the two never disagree.

`crates/adapters/cross-tests` moves to `crates/interop/cross-tests` unchanged and keeps
asserting `BestEffort` end to end across a chain whose weakest link has no transfer point.

### 5.3 The ZeroMQ library as backlog items

Ids are `B-0xx` placeholders; the backlog owner assigns them, and `needs` names the
prerequisite item by title because the ids do not exist yet. In dependency order, every item
inside LOOP §5's 90-minute cap.

```
### B-0xx — Extract weida-runtime
kind: code | size: 90 | status: ready | needs: []
acceptance: `crates/runtime` holds `Exec` (spawn, sleep, within, enter, resolve with its address cap and IP-literal fast path), the three reactor-ownership constructors with the background-shutdown discipline, the `AF_UNIX` bind hygiene and `peer_credentials` of 0010 §4.5, a generic named-endpoint registry with a byte budget, and the bounded close budget; `crates/weida` uses it and **no `pub` item in `weida` changes** — the existing suite compiles untouched.

### B-0xx — weida-zmq: context, endpoints, error vocabulary
kind: code | size: 90 | status: ready | needs: [Extract weida-runtime]
acceptance: `crates/zmq/weida-zmq` with a `Context` created three ways on `weida-runtime`'s `Exec`, `ZMQ_MAX_SOCKETS` honoured, `tcp://`/`ipc://`/`inproc://` parsed under libzmq's own length rules (113 B ipc on Linux, 256 inproc), and an error enum carrying libzmq's errno names; no dependency on `weida` or `weida-protocol`.

### B-0xx — Message, per-peer queue and the high-water marks
kind: code | size: 90 | status: ready | needs: [weida-zmq: context, endpoints, error vocabulary]
acceptance: `Message`/`Multipart` sent and received atomically ("all frames or none"), `ZMQ_MAXMSGSIZE` checked from the declared length before any allocation, the per-peer double queue every pattern RFC specifies, `ZMQ_SNDHWM`/`ZMQ_RCVHWM` at 1000, and the mute-state action of `zmq_socket(3)`'s table — block, drop or `EAGAIN` — with one test per action.

### B-0xx — The connection engine: bind, connect, reconnect
kind: code | size: 90 | status: ready | needs: [Message, per-peer queue and the high-water marks]
acceptance: one socket binds and connects many endpoints; `ZMQ_RECONNECT_IVL`/`_IVL_MAX` backoff, `ZMQ_HANDSHAKE_IVL`, `ZMQ_CONNECT_TIMEOUT`, `ZMQ_IMMEDIATE`, `ZMQ_BACKLOG`, `ZMQ_LAST_ENDPOINT` after a wildcard bind, `unbind`/`disconnect`; a queue exists for a peer that never connected, per the RFCs' "whether or not the connection is established".

### B-0xx — The ZMTP session on the existing codec
kind: adapter | size: 90 | status: ready | needs: [The connection engine: bind, connect, reconnect]
acceptance: greeting with the 3.0 downgrade, NULL handshake, `READY` metadata with `Socket-Type` and `Identity`, MORE/COMMAND framing through `weida-zmtp` unchanged, `PING`/`PONG` gated on the negotiated version, `ERROR` sent and understood; zmtp.md §10.1's vectors still assert byte-for-byte and the codec's `[dependencies]` is still empty.

### B-0xx — REQ and REP
kind: code | size: 90 | status: ready | needs: [The ZMTP session on the existing codec]
acceptance: REQ's strict alternation with `EFSM` on any other order, the empty delimiter prepended and stripped, round-robin out and last-peer in, REP discarding a reply whose originator vanished, `ZMQ_REQ_CORRELATE` and `ZMQ_REQ_RELAXED`; close-and-reopen after `EFSM` works, which is what Lazy Pirate needs.

### B-0xx — DEALER and ROUTER
kind: code | size: 90 | status: ready | needs: [REQ and REP]
acceptance: unrestricted send and receive both ways; ROUTER's routing-id frame prepended and stripped with a peer-chosen `Identity` honoured; `ZMQ_ROUTER_MANDATORY` turning an unroutable message into `EHOSTUNREACH` instead of a silent drop; `ZMQ_ROUTER_HANDOVER` evicting an incumbent where the default rejects the collision; `ZMQ_PROBE_ROUTER`.

### B-0xx — PUSH, PULL and PAIR
kind: code | size: 90 | status: ready | needs: [The ZMTP session on the existing codec]
acceptance: PUSH round-robins only to peers whose queue is not full and blocks rather than discarding, PULL fair-queues its peers, and PAIR accepts at most one peer, does not auto-reconnect, and terminates further incoming connections while one is live.

### B-0xx — PUB and SUB
kind: code | size: 90 | status: ready | needs: [The ZMTP session on the existing codec]
acceptance: publisher-side binary prefix match against the start of the first frame; subscriptions additive and non-idempotent, so two SUBSCRIBEs need two CANCELs; an empty subscription matches everything and a fresh SUB nothing; drop-not-block at the HWM; both wire forms accepted — the 3.x commands and ZMTP 2.0's one-frame `%x01`/`%x00` — with the sent form configurable.

### B-0xx — XPUB and XSUB
kind: code | size: 90 | status: ready | needs: [PUB and SUB]
acceptance: XPUB delivers subscription messages to the application in the `1`/`0` form and synthesizes an unsubscribe when a subscriber disconnects; `ZMQ_XPUB_VERBOSE`, `_VERBOSER`, `_MANUAL`, `_WELCOME_MSG`; XSUB forwards subscriptions upstream and re-sends them on reconnect.

### B-0xx — The inproc transport
kind: code | size: 60 | status: ready | needs: [Message, per-peer queue and the high-water marks]
acceptance: a context-scoped namespace on `weida-runtime`'s registry with libzmq's 256-character budget, connect-before-bind working as libzmq 4.0 fixed it, and two contexts in one process never meeting.

### B-0xx — The ipc transport
kind: code | size: 90 | status: ready | needs: [The connection engine: bind, connect, reconnect]
acceptance: `AF_UNIX` with the hygiene of 0010 §4.5 — unlink-then-bind, explicit mode, socket-type check before unlinking, the 113-byte Linux path budget — peer credentials available to authorization, and the endpoint-stealing hazard documented rather than papered over.

### B-0xx — PLAIN and the ZAP dialog
kind: code | size: 90 | status: ready | needs: [The ZMTP session on the existing codec, The inproc transport]
acceptance: PLAIN's handshake with username and password; RFC 27's request and reply framing over `inproc://zeromq.zap.01` with status 200/300/400/500 and the user-id field, one handler per context, `ZMQ_ZAP_DOMAIN` and `ZMQ_ZAP_ENFORCE_DOMAIN`; a 400 refuses the connection before any message flows, and the user id never becomes a weida identity.

### B-0xx — CURVE command layouts in weida-zmtp
kind: adapter | size: 90 | status: ready | needs: [The ZMTP session on the existing codec]
acceptance: `HELLO` (200), `WELCOME` (168), `INITIATE` (257+), `READY` (30+) and `MESSAGE` encoded and decoded as byte layouts with the boxes as opaque ranges, the ABNF's 72-octet HELLO padding rather than the prose's 70, Z85 both ways, vectors published in zmtp.md and asserted both directions — and `weida-zmtp`'s `[dependencies]` still empty, because no crypto happens here.

### B-0xx — CURVE in weida-zmq
kind: code | size: 90 | status: ready | needs: [CURVE command layouts in weida-zmtp, PLAIN and the ZAP dialog]
acceptance: the four keys; the cookie discarded on a valid INITIATE or after a short interval; session keys destroyed on close; the three security models selectable; `ZMQ_CURVE_SERVER`/`_PUBLICKEY`/`_SECRETKEY`/`_SERVERKEY` taking 32 bytes or 40-character Z85; the peer's long-term key reaching the ZAP handler as the CURVE credential; one named RustCrypto dependency and `unsafe_code` still forbidden.

### B-0xx — The option table, honoured or refused
kind: code | size: 60 | status: ready | needs: [DEALER and ROUTER, XPUB and XSUB, The ipc transport]
acceptance: every `zmq_setsockopt` and `zmq_ctx_set` option is honoured or refused **at configuration time** with a message naming the reason — no transport, draft only, deprecated in favour of ZAP, or replaced by a `weida-runtime` construct — and never silently ignored; a test walks the whole table, and the two deliberate default changes (finite `ZMQ_LINGER`, bounded `ZMQ_MAXMSGSIZE`) are asserted as such.

### B-0xx — Monitor events and the devices
kind: code | size: 90 | status: ready | needs: [XPUB and XSUB, PUSH, PULL and PAIR]
acceptance: the `zmq_socket_monitor` event set delivered as a typed stream **and** over the `inproc://` PAIR form the Espresso recipe reads; `zmq_proxy(frontend, backend, capture)` and `zmq_proxy_steerable` with PAUSE/RESUME/TERMINATE/STATISTICS.

### B-0xx — The blocking facade
kind: code | size: 60 | status: ready | needs: [REQ and REP, PUSH, PULL and PAIR, PUB and SUB]
acceptance: a non-default `blocking` feature giving one wrapper per socket over a `Context::owned` reactor, with `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` and `ZMQ_DONTWAIT`; no second implementation of any protocol behaviour, and one zguide example that reads like its C original.

### B-0xx — Interop against zmq.rs, both roles
kind: adapter | size: 90 | status: ready | needs: [DEALER and ROUTER, XPUB and XSUB, PUSH, PULL and PAIR]
acceptance: every pairing both implement, with our socket as the connecting side **and** as the bound side, against `zeromq` 0.6 as a dev-dependency with no supervised process; the three skews the sheet records — a 3.0 greeting, `READY`-only command decoding, the legacy subscription form — exercised rather than assumed.

### B-0xx — Interop against libzmq, both roles
kind: adapter | size: 90 | status: ready | needs: [Interop against zmq.rs, both roles, CURVE in weida-zmq]
acceptance: the same matrix plus PAIR, PLAIN, CURVE and ZAP against libzmq through the `zmq` crate as an optional dev-dependency, `#[ignore]` with the install command in the doc comment where libzmq is absent (LOOP §2); every disagreement recorded in the sheet as measured, not inferred.

### B-0xx — zguide I: the pirates and the load-balancing broker
kind: adapter | size: 90 | status: ready | needs: [Interop against zmq.rs, both roles]
acceptance: lazy pirate, simple pirate over the chapter 3 load-balancing broker, and paranoid pirate with heartbeats, as examples that run in `cargo test`, each asserting the guide's own claim — an in-order reply or abandonment; a worker may crash and restart while the queue runs; the queue evicts a lost worker instead of discovering it through a failed request.

### B-0xx — zguide II: Majordomo and Freelance
kind: adapter | size: 90 | status: ready | needs: [zguide I: the pirates and the load-balancing broker]
acceptance: MDP/0.2 client, worker and broker with the six-byte header, per-service queues, heartbeats and `DISCONNECT`, plus `mmi.service` answering 200/404; Freelance models one and two with request numbering so a stale reply is ignored; each asserting the RFC's stated guarantee.

### B-0xx — zguide III: Clone, Binary Star and the pub-sub recipes
kind: adapter | size: 90 | status: ready | needs: [zguide II: Majordomo and Freelance, Monitor events and the devices]
acceptance: CHP's ROUTER/PUB/SUB port triple with `ICANHAZ?`/`KVSYNC`/`KTHXBAI`/`KVPUB`/`HUGZ`/`KVSET` and the strict-increment discard rule; the Binary Star state machine with its three events and the split-brain warning in the doc comment; the last-value cache and Espresso on the proxy capture socket.

### B-0xx — The parity table against libzmq 4.3.5
kind: spec | size: 60 | status: ready | needs: [The option table, honoured or refused, Interop against libzmq, both roles]
acceptance: `docs/libraries/zmq.md` with `docs/libraries/README.md` as its index in the shape `docs/adapters/README.md` set, stating row by row every socket type, transport, mechanism, option, monitor event and device as present, refused-with-reason or absent-with-reason; no row says "partial" without naming what is missing.

### B-0xx — The bridge rebuilt on the library
kind: adapter | size: 90 | status: ready | needs: [Interop against zmq.rs, both roles]
acceptance: `weida-zmq-bridge` keeps `Inbound`/`Outbound` and their configuration, drops `wire.rs`, the handshake driving, `Liveness` and the subscription reference counting in favour of `weida-zmq` sockets, still refuses zmtp.md §9's six configurations at configuration time, still states `core` on the weida side, and every existing bridge test and the interop bench pass with unchanged observable behaviour.

### B-0xx — The crate move and the two-product README
kind: spec | size: 45 | status: ready | needs: [weida-zmq: context, endpoints, error vocabulary]
acceptance: `crates/zmq/` holds `weida-zmtp`, `weida-zmq` and `weida-zmq-bridge`; `crates/nng/` holds `weida-sp` and `weida-nng-bridge`; `cross-tests` moves to `crates/interop/`; README's crate table gains the `kind` column of §5.5 and ARCHITECTURE §4's crate map and every stale doc path follow; no crate **name** changes, so nothing published breaks.
```

**Twenty-six items, 2175 minutes — about 36 hours of item time.** Stated plainly because the
number is the decision's real cost: the three ZMTP bridge slices that exist today (B-041,
B-042, B-043) were 270 minutes, so the library is **roughly eight times the bridge work it
replaces**, and against everything the ZeroMQ line has cost so far (B-018, B-030, B-041, B-042,
B-043, B-055 = 465 minutes) it is about **4.7 times**. The estimate excludes the research item
LOOP §9 requires after each slice, excludes the cross-adapter slice, and excludes `weida-nng`,
which repeats most of the shape at a size B2 must estimate for itself. Anyone approving this
note is approving that multiple.

### 5.4 Other follow-ups

- **[ARCHITECTURE.md](../ARCHITECTURE.md) §4** gains `weida-runtime` in the crate map and the
  dependency diagram of §4.2, and the "Why runtime and transport are one crate" subsection
  gains the sentence that its own trigger has fired — a second consumer appeared, and the cut
  taken was the reactor and the OS rather than master doc §73's `transport-quic`.
- **[ARCHITECTURE.md](../ARCHITECTURE.md) §5** keeps its `Exec` paragraph but says the grep
  that enforces it now runs over `crates/runtime` too.
- **[INVARIANTS.md](../INVARIANTS.md)** gains one line: the libraries are inside the
  unbounded-allocation invariant exactly as the adapters turned out to be (the B-051 lesson),
  and `weida-zmq`'s bounds are libzmq's own names — `ZMQ_SNDHWM`, `ZMQ_RCVHWM`,
  `ZMQ_MAXMSGSIZE`, `ZMQ_MAX_SOCKETS`, `ZMQ_BACKLOG`, `ZMQ_HANDSHAKE_IVL` — because a libzmq
  user must recognize them.
- **`docs/libraries/`** is created with the parity-table item: a `README.md` index in the shape
  `docs/adapters/README.md` set, and one document per library. A mapping document describes an
  edge; a library document describes a product, and the two must not be confused.
- **[0006](0006-guarantee-sets.md) §4.9 is unaffected and still binds**: the forwarder still
  has a mapping document, still names its losses and still refuses at configuration time. What
  changes is only that the code behind the edge is now a library anybody can use.
- **Open, deliberately.** (a) Whether `weida-zmq` should eventually be published from this
  workspace or split out once `weida-runtime` is stable — the answer depends on how often the
  runtime's API moves, which is unknown until the second consumer exists. (b) Whether the
  thread-safe draft family (CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, PEER/CHANNEL) is in
  scope at all; it is draft, absent from many distribution builds [zeromq §13], and the parity
  table can honestly say "absent, draft" for a long time. (c) Whether MQTT and AMQP get
  libraries too, or stay adapters: both presuppose a broker, and the broker is Phase D.

### 5.5 What the README's crate list says

One rule, so that the product line is legible from the table alone: **every crate row states
which of the three kinds it is**, in a `kind` column with exactly three values —

- `weida` — the protocol, its runtime and its shared plumbing (`weida-core`,
  `weida-protocol`, `weida-runtime`, `weida`);
- `library` — a standalone implementation of a foreign protocol, usable with no weida in the
  picture (`weida-zmtp`, `weida-zmq`, later `weida-sp`, `weida-nng`);
- `bridge` — a forwarder that terminates both networks and belongs to neither
  (`weida-zmq-bridge`, later `weida-nng-bridge`).

No row may describe a library as an adapter or a bridge as a library, and the paragraph above
the table says in one sentence that the repository holds two kinds of product. The intro
paragraph's "The ZeroMQ adapter has its first three slices" becomes a statement about the
ZeroMQ *library* and the bridge beside it.

## 6. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §4, §5, §6a-§6c;
[INVARIANTS.md](../INVARIANTS.md); [LOOP.md](../LOOP.md) §2, §5, §9;
[adapters/README.md](../adapters/README.md); [adapters/zmtp.md](../adapters/zmtp.md) §1, §2,
§3, §6, §7, §8, §9, §10; [adapters/nng.md](../adapters/nng.md) §8;
[0006](0006-guarantee-sets.md) §4.1, §4.2, §4.6-§4.9; [0007](0007-topic-namespace.md) §4.2,
§5; [0008](0008-session-identity.md) §4.2, §5; [0009](0009-drain.md) §4.3;
[0010](0010-local-transport.md) §4.4, §4.5, §4.8;
[0012](0012-local-connection-grouping.md) §4.1-§4.4.

Code read for §2 and §5.2: `crates/weida/src/runtime.rs`, `inproc.rs`, `unix.rs`, `drain.rs`,
`transport.rs`, `config.rs`; `crates/core/src/lib.rs`, `limits.rs`;
`crates/adapters/weida-zmtp/src/lib.rs`; `crates/adapters/weida-zmtp-bridge/src/lib.rs`,
`inbound.rs`, `outbound.rs`; `crates/adapters/weida-sp-bridge/src/lib.rs`.

Research sheets: [zeromq.md](../research/zeromq.md) §1, §2, §3, §4.1-§4.6, §5, §6, §8, §9,
§10, §11, §12/P4, §12/P17, §13; [nanomsg-nng.md](../research/nanomsg-nng.md) §6;
[ipc.md](../research/ipc.md) §1.2, §8.1.
