# Proved client keys without prior trust (griasdi identity)

## Context

griasdi is a chat and voice application in which every byte travels over weida
([griasdi-voice.md](griasdi-voice.md)). It has no accounts, no registration and
no login. A client dials a server presenting its certificate, and all the
certificate says is "my identity is this key" -- which nobody can forge, because
the TLS handshake proves possession of the key. To the server every client is a
stranger with a stable, unforgeable name: the model of an SSH user key or a
WireGuard peer, minus the requirement that the server list the key beforehand.

Whether that identity gets any rights on a server -- membership in a community,
speaking in a room, hearing a room -- is application logic keyed on the identity
the transport already delivers, which is exactly
[0015](../decisions/0015-peer-authorization.md)'s model: authorization is the
acceptor's local decision on `(proved peer, path)`. There is no authentication
step beyond the handshake.

What weida lacks is the one thing this needs: a binding that **proves** a
client's key without **judging** it.

Everything under "What weida provides today" was read from the weida source at
commit `b719afd`. Everything under "Proposal" is design, not fact.

**Answered by [decisions/0035](../decisions/0035-keys-proved-not-judged.md) (accepted; shipped
in `b3ba683`).** Proposal 1 is `ServerTls::require_client(ClientTrust::AnyKey)`: the leaf must
parse, the chain must fit 8 certificates and 32 KiB, the TLS 1.3 signature is verified, and
nothing else is judged; the peer is `PeerIdentity::Key` of the leaf's fingerprint. Proposal 2
is `PeerChain` on every arrival, `IncomingMeta::peer_chain` and `FlowInfo::peer_chain`, DER
and leaf first. Proposal 3 is `Radio::with_admission`, which sees `Join { peer, peer_chain,
filter }` before a join is recorded and refuses by silence, and `Radio::evict(peer, filter)`.
Open question 1 is answered yes: a device leaf signed by a user key is admitted as its own
fingerprint (`a_binding_that_requires_any_key_proves_it_and_judges_nothing`). Open question 2
is `Binding::disconnect(fingerprint)`, which closes every connection of one key with
`REJECTED`; the dialler redials, so it is not a ban. Deferred, as this document allows: the
handshake `ClientPolicy` and `AnyKey` on the dialling side.

## What griasdi needs

| Need | Shape |
| --- | --- |
| Every client has a proved identity | the binding requests a client certificate, weida verifies the handshake signature, and accepts any key that passed it; `IncomingMeta::peer` / `FlowInfo::peer` carry its fingerprint |
| The user behind a device | a griasdi device presents a chain: its own key, certified by the user's key. The application reads the chain and keys rights on the user key; the device fingerprint stays weida's peer |
| Rights on RADIO topics | a server decides which proved peer may join which topic, and withdraws a join when membership changes |

## What weida provides today

- **Client identity is all-or-nothing against a fixed trust.**
  `ServerTls::client_trust` is either `None` -- peers are anonymous and
  `IncomingMeta::peer` is `None` -- or a `TrustSource` every client must satisfy
  (`crates/weida/src/config.rs:474-498`).
- **A `TrustSource` holds pins and anchors and nothing else**
  (`crates/weida/src/identity.rs:564-620`). An unknown key is admitted only if it
  is pinned beforehand or chains to a configured anchor, and an empty client trust
  is refused at binding time: "client trust is empty: a binding cannot require
  clients it would never accept" (`crates/weida/src/tls.rs:567-573`).
- **Proof and judgement are already separate in the code.** `ClientVerifier`
  judges the leaf against pins and anchors in `verify_client_cert`, and proves
  possession independently in `verify_tls13_signature`
  (`crates/weida/src/tls.rs:355-414`). The mode this document asks for is the
  second without the first.
- **The proved identity is the leaf's SPKI fingerprint**, and the rest of the
  presented chain is discarded after the handshake
  (`crates/weida/src/tls.rs:654-661`).
- **A client can present a chain.** `Identity` holds "the certificate chain, leaf
  first" (`crates/weida/src/config.rs:258-260`) and `ClientTls::with_identity`
  presents it (`crates/weida/src/config.rs:447-451`).
- **A RADIO join is admitted unconditionally.** `RadioHub::join` records any
  filter from any connection, bounded only by `max_subscriptions`
  (`crates/weida/src/radio.rs:166-205`); nothing lets the application refuse or
  withdraw one. By [0017](../decisions/0017-subscription-verdict.md) a join has no
  reply half, and silence is its defined answer.

## Proposal

### 1. A client-trust mode that proves and does not judge

`ServerTls::require_client(ClientTrust::AnyKey)` -- name to be chosen -- requests
a client certificate, requires one, verifies the TLS 1.3 `CertificateVerify`
signature as today, and accepts the key whatever signed the certificate. The
peer identity is the leaf's SPKI fingerprint, exactly as for a pinned key, so
pools, `max_connections_per_peer`, redial and 0008's "the fingerprint is the
peer" work unchanged.

It differs from anonymous mode (`client_trust: None`) in one respect only: the
client must present a key and prove it. Nothing about the key is trusted beyond
that it is the same key next time.

The same mode on the dialling side (`ClientTls`, for an address that names no
fingerprint) is what later client-to-client connections need, where both ends
are strangers until the application says otherwise. Not needed for the first
release: a server's address names its fingerprint.

### 2. The presented chain reaches the application

Beside the fingerprint, `IncomingMeta` and `FlowInfo` expose the chain the peer
presented, DER, leaf first -- or the connection does, once. griasdi verifies
there that the device key is certified by a user key and derives the user from
it. weida parses nothing beyond the leaf's SPKI, so the certificate profile is
griasdi's business. The chain is bounded in length and size before it is kept,
because the peer chooses it.

### 3. RADIO joins pass through the application

`Radio::with_admission(|peer: Option<&PeerIdentity>, filter: &str| bool)`,
consulted in `RadioHub::join` before the filter is recorded. A refused join is
silence, which 0017 already defines, so nothing changes on the wire.
`Radio::evict(peer, filter)` withdraws a recorded join, because room membership
changes while the dish stays connected.

Without it an SFU built as [0034](../decisions/0034-late-is-lost.md) §4.8 shows
sends every room's traffic to any connected client that joins `#`. The payload
is end-to-end encrypted, but who speaks when, and the server's upstream
bandwidth, are exposed to every client.

### Optional: a policy in the handshake

A `ClientPolicy` callback that sees the chain during the handshake and may
refuse it (a banned key never gets a connection slot or a HELLO). Not required:
griasdi can close a banned peer's connections after the handshake, and a refusal
there is ordinary application logic. Worth it only if refused peers turn out to
cost enough to matter.

## Rejected alternatives

- **Server-issued client certificates.** A registration step, and a client
  credential that depends on the server; griasdi's identity model has neither.
- **Anonymous clients.** Without a proved key the application has nothing to
  attach rights to, and after [0002](../decisions/0002-control-and-bulk-separation.md)
  every dialled path is its own connection: a client's `/voice`, `/events` and
  `/files` connections could not even be recognised as one peer (0015 §4.4).

## Open questions

1. **Does webpki parse a leaf with no CA-issued profile?** `spki_fingerprint`
   goes through `webpki::EndEntityCert::try_from`
   (`crates/weida/src/tls.rs:129-133`), which self-signed identities from
   `Identity::generate` already pass; a leaf signed by a user key should too, but
   that is to be confirmed by a test.
2. **Refusing a peer that is already connected.** Closing every connection of
   one fingerprint is a runtime operation the application cannot perform today
   without tracking connections itself.
