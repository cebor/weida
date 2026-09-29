# 0035 — Keys proved and not judged: any-key clients, the presented chain, RADIO admission

- **Status:** accepted
- **Date:** 2026-09-29
- **Items:** B-293 to B-297
- **Answers:** [requirements/griasdi-identity.md](../requirements/griasdi-identity.md) (all three
  proposals and both open questions)
- **Amends:** [0015](0015-peer-authorization.md) §2 and §4 ("a `Trust` pin list on a binding,
  connection-wide and all-or-nothing"), [ARCHITECTURE.md](../ARCHITECTURE.md) §5 (same sentence)
- **Related:** [0008](0008-session-identity.md) §4.2,
  [0017](0017-subscription-verdict.md) §4.1, §4.7,
  [0031](0031-transparent-redial-and-the-sender-outbox.md),
  [0032](0032-identity-sources-and-the-handoff.md), [0034](0034-late-is-lost.md) §4.6, §4.8

## 1. The question

griasdi is a chat and voice application whose every byte travels over weida. It has no accounts,
no registration and no login: a client presents a certificate that says "my identity is this
key", the TLS handshake proves possession, and to the server every client is a stranger with a
stable, unforgeable name — the model of an SSH user key or a WireGuard peer, minus the server
listing the key beforehand [griasdi-identity, Context]. Rights — membership in a community,
speaking in a room, hearing a room — are application logic on `(proved peer, path)`, which is
[0015](0015-peer-authorization.md)'s model unchanged.

Three needs follow [griasdi-identity, "What griasdi needs"]:

| Need | Shape |
| --- | --- |
| Every client has a proved identity | the binding requests a client certificate, verifies the handshake signature and accepts any key that passed it |
| The user behind a device | a device presents a chain — its own key, certified by the user's key — and the application reads it; the device fingerprint stays weida's peer |
| Rights on RADIO topics | a server decides which proved peer may join which topic, and withdraws a join when membership changes |

And two open questions: does webpki parse a leaf signed by a user key, and can the application
close every connection of one fingerprint without tracking connections itself.

So the question is: **how does a binding prove a key without judging it, how does what the peer
presented reach the application, and where does the application decide who may join a RADIO
topic** — without a new wire element and without making "trust everything" reachable where it
must not be.

## 2. The evidence, condensed

**2.1 Client identity is all-or-nothing against a fixed trust.** `ServerTls { identity,
client_trust: Option<TrustSource> }` (`crates/weida/src/config.rs:474-499`); `server_config`
maps `None` to `with_no_client_auth()`, an empty trust to `Error::Tls("client trust is empty:
…")`, and anything else to `ClientVerifier` (`crates/weida/src/tls.rs:557-590`). A `TrustSource`
holds pins and anchors and nothing else (`crates/weida/src/identity.rs`), and
`require_client(trust: impl Into<TrustSource>)` is its only entrance; `From<Trust> for
TrustSource` is at `identity.rs:652`.

**2.2 Judgement and proof are already separate.** rustls' `ClientCertVerifier` splits them by
construction: `verify_client_cert` decides whether a certificate is acceptable, and
`verify_tls12_signature` / `verify_tls13_signature` check that the peer holds its key.
`ClientVerifier` (`tls.rs:289-414`) judges the leaf through `Policy::judge` (`tls.rs:195-209`),
which returns `BadEncoding` when `spki_fingerprint` cannot parse it, and proves possession in the
signature methods with the provider's algorithms, independently of the judgement. The mode
griasdi asks for is the second without the first.

**2.3 A QUIC peer can already present a chain.** `Identity` holds "the certificate chain, leaf
first" and `ClientTls::with_identity` presents it. What arrives survives the handshake as
`peer_identity()`, a `Vec<CertificateDer<'static>>` that `peer_fingerprint` (`tls.rs:653-662`)
downcasts, hashes the leaf's SPKI of, and discards.

**2.4 The chain is peer-chosen, and weida names no bound for keeping it.** rustls bounds one
handshake message at 64 KiB, which bounds what is parsed; nothing in weida bounds what it would
keep once the connection is up, for the life of the connection and on every arrival.
[INVARIANTS.md](../INVARIANTS.md) requires that a bound is named before the allocation it caps
exists.

**2.5 The peer reaches the application per arrival.** `ConnCtx::peer` is set once from
`Link::peer()` in `ConnCtx::spawn` (`crates/weida/src/conn.rs:50-141`,
`crates/weida/src/transport.rs:75-84`) and copied into `IncomingMeta` by
`IncomingMeta::from_header(header, peer)` (`crates/weida/src/transfer.rs:143-209`) and into
`FlowInfo` (`crates/weida/src/flow.rs:1003-1012`).

**2.6 A RADIO join is admitted unconditionally and has no reply half.** `RadioHub::join`
(`crates/weida/src/radio.rs:166-205`) records any filter from any connection, bounded only by
`max_subscriptions`, whose slot `handle_subscription` reserves and releases around it
(`conn.rs:821-833`). By [0017](0017-subscription-verdict.md) §4.1 a join that produces no
connection close "has been **received and not refused**", and the cases behind that silence are
indistinguishable by design. §4.7 of the same note names the missing piece as "no local decision
point" — a local API question, not a wire question.

**2.7 The accept loop already counts connections per fingerprint.** `PeerCounts` (`listener.rs:
915-959`) admits and releases per proved fingerprint for `max_connections_per_peer`, but holds
only a count. An application close with `REJECTED` (`codes::REJECTED = 7`) reaches the dialler as
`ConnectionLost(PeerClosed)` (`conn.rs:268-281`), and the default `ReconnectPolicy` redials after
it (`crates/weida/src/reconnect.rs:83-87`).

## 3. Options considered

| Option | Shape | Named loss |
| --- | --- | --- |
| A — a `TrustSource` that trusts everything | `TrustSource::any()` beside pins and anchors | `TrustSource` is also what every dialling endpoint takes, so one misplaced value would make trust-everything reachable on the dialling side, where an empty trust is refused precisely because "the only alternative to failing here is trusting everything, which must never be reachable by omission" ([IMPLEMENTATION.md](../IMPLEMENTATION.md) §5). Refused |
| **B — a `ClientTrust` enum on `ServerTls`** | `ClientTrust::{Trusted(TrustSource), AnyKey}`, accepted only by `ServerTls::require_client` | `ServerTls::client_trust` changes type, a breaking pre-1.0 change; existing `require_client(Trust::…)` calls compile unchanged. Chosen |
| C — a `ClientPolicy` callback in the handshake | sees the chain and may refuse before a connection slot is spent | The requirement names it optional. Without it a refused key costs one handshake and one `max_connections` slot until the application closes it (§4.4). Deferred |
| D — the chain once per connection, through a peer-event accessor | `Binding::peer_chain(fingerprint)` or a connect event | Refused by the owner: the application would keep its own fingerprint-to-chain table, and a dish connection may carry no arrival before it joins, so the chain would not be at hand when admission needs it |

## 4. Decision

**Option B, with the chain on every arrival, an admission point on the radio, and a disconnect
on the binding. No frame, no key, no code.**

### 4.1 `ClientTrust::AnyKey`: proved, not judged

`ServerTls::require_client` takes `impl Into<ClientTrust>`:

```rust
pub enum ClientTrust {
    Trusted(TrustSource),   // today's meaning: pinned, or chaining to an anchor
    AnyKey,                 // any key the client proves it holds
}
```

`AnyKey` verifies three things and nothing else: the leaf parses (its SPKI fingerprint can be
computed), the presented chain fits the bound of §4.2, and the TLS 1.3 `CertificateVerify`
signature is valid for the leaf's key. It checks no validity dates and no issuer: the key *is*
the identity, as in SSH or WireGuard, and dates in a griasdi device certificate are griasdi's to
judge from the chain. An anonymous client is refused in the handshake, exactly as under
`Trusted`.

The peer is `PeerIdentity::Key(leaf SPKI fingerprint)`, the same value a pinned key yields, so
pools, `max_connections_per_peer`, redial and "the fingerprint is the peer"
([0008](0008-session-identity.md) §4.2) are unchanged.

The dialling side keeps `TrustSource` and gains no `AnyKey`: a server's address names its
fingerprint, and client-to-client connections, where both ends are strangers, are the later
need the requirement defers.

### 4.2 `PeerChain`: what the peer presented, bounded

`PeerChain` is the chain as it arrived — DER, leaf first — captured once per QUIC connection and
shared by `Arc` into every arrival: `IncomingMeta::peer_chain` and `FlowInfo::peer_chain`, beside
`peer`. weida parses nothing beyond the leaf's SPKI; the certificate profile is the
application's.

The chain is peer-chosen, so it is bounded before it is kept: at most
`MAX_PEER_CHAIN_CERTS = 8` certificates and `MAX_PEER_CHAIN_BYTES = 32768` bytes. Under
`AnyKey` a larger chain fails the handshake, because the chain is the point of the mode. Under
`Trusted` the peer was judged by pins or anchors and the chain is incidental: a larger one is
dropped, `peer_chain` is `None`, and `peer` is still set. `peer_chain` is `None` on every local
transport and for anonymous peers. It is present on both sides: a dialling endpoint sees the
server's chain.

### 4.3 RADIO admission and eviction

`Radio::with_admission(Fn(&Join<'_>) -> bool)`, where `Join` carries `peer`, `peer_chain` and
the filter exactly as the dish sent it, is consulted in `RadioHub::join` **before** a join is
recorded, and on every repeated join. This is the local decision point
[0017](0017-subscription-verdict.md) §4.7 named.

- **A refusal is silence.** It records nothing, reserves no `max_subscriptions` slot and closes
  nothing; by [0017](0017-subscription-verdict.md) §4.1 "received and not refused" is
  indistinguishable, by design, from "every copy declined", so nothing changes on the wire.
- **A refused repeat leaves the recorded filter alone.** Withdrawal is `evict`'s job, so an
  admission that changes its mind does not race a dish that happens to repeat a join.
- **Installing a policy re-screens the joins already recorded**, so there is no window between
  `listener.radio(path)` and `with_admission` in which a dish joins unscreened.
- **User code never runs under the hub lock.** The callback runs on the connection's task and
  must not block.

`Radio::evict(&PeerIdentity, filter) -> usize` withdraws a recorded filter, by exact string, from
every connection of that peer, releases its `max_subscriptions` slot, and returns how many joins
went. The dish is not told, per 0017. An anonymous dish has `peer: None`: it can be admitted or
refused, never evicted by name.

### 4.4 Open question 2: `Binding::disconnect`

`Binding::disconnect(Fingerprint) -> usize` closes every live connection of that fingerprint on
that binding with `REJECTED` and the reason `disconnected by the application`, and returns the
count. The accept loop's per-peer table becomes a table of connection handles rather than
counts, bounded by `max_connections` as before. The dialler sees `ConnectionLost(PeerClosed)` and
redials under its `ReconnectPolicy`: **this is not a ban**, because whether to stop on a peer's
close is the dialler's policy. A ban is refusing that peer's arrivals and joins (0015's
authorization surface and §4.3's admission), or the deferred `ClientPolicy`.

### 4.5 Open question 1: a leaf signed by a user key

Answered **yes** by `a_binding_that_requires_any_key_proves_it_and_judges_nothing` in
`crates/weida/tests/identity.rs` (B-293): a device leaf issued by `rcgen` 0.14.10 under a
separate, self-signed user key — no CA profile, no `BasicConstraints` on the issuer — presented
as the two-certificate chain `[device, user]`, completes the handshake against an `AnyKey`
binding over QUIC loopback, and the replier sees `PeerIdentity::Key` of the device key's
fingerprint. `webpki::EndEntityCert::try_from` parses the leaf; nothing checks its issuer.

### 4.6 What does not change

The wire, HELLO, the meaning of `Trusted` (a `TrustSource` of pins and anchors, rotated per
[0032](0032-identity-sources-and-the-handoff.md)), the refusal of an empty `Trusted` trust at
bind time, and the anonymous mode (`client_trust: None`).

### 4.7 Python

None of this is reachable from Python: its bindings bind QUIC only through `bind_quic(addr,
identity)` (`crates/py/weida-py/src/runtime.rs:98`, `sync.rs:133`) and accept anonymous clients
only. `peer_chain`, `with_admission`, `evict` and `disconnect` stay Rust-only and are recorded as
absent, with this reason, in [libraries/weida-py.md](../libraries/weida-py.md).

## 5. Consequences and follow-ups

Documents (B-297): [ARCHITECTURE.md](../ARCHITECTURE.md) §5 and the API sketch take
`ClientTrust`, `peer_chain` and `Binding::disconnect`, and lose "connection-wide and
all-or-nothing"; [IMPLEMENTATION.md](../IMPLEMENTATION.md) §5's client-identity row and §6's
authorization entry say the same; [PATTERNS.md](../PATTERNS.md) §1.9 gains `AnyKey`, the chain
and `disconnect`, and §6.4 admission and eviction; [GUARANTEES.md](../GUARANTEES.md)'s
peer-identity row mentions the chain; [INVARIANTS.md](../INVARIANTS.md) names the chain bound;
[libraries/weida-py.md](../libraries/weida-py.md) records the absent names; both griasdi
requirement documents and `README.md` point at what shipped. Backlog:

### B-293 — A binding that proves client keys and judges none
kind: code | size: 45 | status: ready | needs: []
acceptance: `ServerTls::require_client(ClientTrust::AnyKey)` ([0035](0035-keys-proved-not-judged.md) §4.1) requires a client certificate, verifies the TLS 1.3 handshake signature, and judges nothing else; the peer is `PeerIdentity::Key` of the leaf's SPKI fingerprint. `ClientTrust::Trusted(TrustSource)` keeps today's meaning and the empty-trust refusal, and existing `require_client(Trust::…)` calls compile unchanged. A chain above `MAX_PEER_CHAIN_CERTS` (8) or `MAX_PEER_CHAIN_BYTES` (32 KiB) fails an `AnyKey` handshake. Tests: `tls::tests::any_key_bounds_the_chain_it_keeps`, and `a_binding_that_requires_any_key_proves_it_and_judges_nothing` in `crates/weida/tests/identity.rs` — an anonymous client fails with `Error::Tls`, two generated keys are admitted as two distinct peers, and a device leaf signed by a user key is admitted as its own fingerprint (open question 1).

### B-294 — The presented chain on every arrival
kind: code | size: 45 | status: ready | needs: [B-293]
acceptance: [0035](0035-keys-proved-not-judged.md) §4.2: `PeerChain` (DER, leaf first, `Arc`-shared) is captured once per QUIC connection and carried on `IncomingMeta::peer_chain` and `FlowInfo::peer_chain` on both sides; `None` on the local transports, for anonymous peers, and when a `Trusted` binding's peer presented more than the bound. The B-293 test asserts the device requester's chain arrives byte-identical, two certificates, leaf first, and a generated identity's as one certificate; `an_anonymous_client_is_seen_as_nobody` asserts `None`.

### B-295 — RADIO admission and eviction
kind: code | size: 60 | status: ready | needs: [B-294]
acceptance: [0035](0035-keys-proved-not-judged.md) §4.3: `Radio::with_admission(Fn(&Join) -> bool)` is consulted before a join is recorded and on every repeat; a refusal records nothing, reserves no `max_subscriptions` slot and closes nothing; installing an admission re-screens recorded joins; `Radio::evict(&PeerIdentity, filter) -> usize` withdraws a filter from every connection of that peer and frees its slot; `weida::blocking::Radio` gains both. Tests in `crates/weida/tests/radio.rs`: `admission_refuses_a_join_silently_and_records_nothing`, `evict_withdraws_a_join_and_frees_its_subscription_slot`, `installing_an_admission_screens_joins_already_recorded`.

### B-296 — Disconnect a peer by fingerprint
kind: code | size: 30 | status: ready | needs: [B-293]
acceptance: [0035](0035-keys-proved-not-judged.md) §4.4: `Binding::disconnect(Fingerprint) -> usize` (and `weida::blocking::Binding::disconnect`) closes every live connection of that fingerprint on the binding with `REJECTED` and returns the count, from a per-peer table of connection handles still bounded by `max_connections`. Test `disconnect_closes_every_connection_of_one_peer_and_no_other` in `crates/weida/tests/identity.rs`: two connections of one key both see `PeerEvent::Lost { cause: PeerClosed }`, another key's requester still round-trips, and an unknown fingerprint closes 0 (open question 2).

### B-297 — Documents for 0035
kind: spec | size: 30 | status: ready | needs: [B-293, B-294, B-295, B-296]
acceptance: §5's edits to ARCHITECTURE, IMPLEMENTATION, PATTERNS, GUARANTEES, INVARIANTS, `libraries/weida-py.md` and `README.md`; both griasdi requirement documents point at what shipped; no passage outside `decisions/`, `research/`, BACKLOG and NIGHTLOG still says a binding's client trust is a pin list and nothing else.

## 6. What this note does not decide

- **A handshake `ClientPolicy`.** Option C; worth an item only if refused peers turn out to cost
  enough to matter.
- **`AnyKey` on the dialling side.** For client-to-client connections, where neither address
  names a fingerprint; a later requirement.
- **Certificate profiles.** What a device certificate must say about its user is griasdi's
  format, parsed by griasdi from `PeerChain`.

## 7. Sources

weida documents: [ARCHITECTURE.md](../ARCHITECTURE.md) §5; [IMPLEMENTATION.md](../IMPLEMENTATION.md)
§5, §6; [INVARIANTS.md](../INVARIANTS.md); decisions 0008, 0015, 0017, 0031, 0032, 0034;
[requirements/griasdi-identity.md](../requirements/griasdi-identity.md).

Code read for this note, at `b719afd`: `crates/weida/src/config.rs:258-260, 433-499`;
`crates/weida/src/identity.rs:564-652`; `crates/weida/src/tls.rs:129-133, 195-209, 289-414,
557-590, 653-662`; `crates/weida/src/conn.rs:50-141, 268-281, 572, 587, 821-833, 1005, 1239`;
`crates/weida/src/transport.rs:75-84`; `crates/weida/src/transfer.rs:143-209, 1099`;
`crates/weida/src/flow.rs:1003-1012`; `crates/weida/src/radio.rs:58-68, 138-272`;
`crates/weida/src/listener.rs:333-361, 627-644, 915-1061`; `crates/weida/src/reconnect.rs:83-87`;
`crates/protocol/src/codes.rs:24`; `crates/py/weida-py/src/runtime.rs:98`, `sync.rs:133`;
`rcgen` 0.14.10 (`CertificateParams::signed_by`, `Issuer::new`).
