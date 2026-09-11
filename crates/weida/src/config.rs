//! Runtime and TLS configuration.
//!
//! Two questions, two types. [`Identity`] answers *who am I*: a certificate
//! chain and the private key behind it. [`Trust`] answers *whom do I accept*:
//! public-key fingerprints, certificate authorities, or the fingerprint the
//! dialled address itself names. [`ClientTls`] and [`ServerTls`] are the two
//! ways of combining them: a dialling endpoint always needs trust and may
//! carry an identity, a binding always needs an identity and may demand trust.

use std::path::PathBuf;
use std::time::Duration;

use weida_core::{Error, Fingerprint, Limits};
use weida_protocol::header::GuaranteeSet;

/// Configuration for one [`crate::Runtime`].
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Resource limits applied to every connection this runtime owns.
    pub limits: Limits,
    /// QUIC keep-alive interval for outgoing connections.
    pub keep_alive: Duration,
    /// QUIC idle timeout, applied in both directions.
    pub idle_timeout: Duration,
    /// Worker threads of the Tokio runtime [`crate::Runtime::owned`] creates.
    ///
    /// Ignored by [`crate::Runtime::new`] and [`crate::Runtime::with_handle`],
    /// which run on a reactor somebody else sized. One is the default because
    /// a messaging runtime is I/O bound and every extra worker is a thread a
    /// library takes from its host process without being asked; `0` is
    /// rejected rather than silently corrected.
    pub worker_threads: usize,
    /// How long [`crate::Runtime::shutdown`] waits for closed sockets to go
    /// idle before it returns anyway.
    ///
    /// The wait exists so peers see a clean `SHUTDOWN` rather than a timeout,
    /// and it is bounded because otherwise a peer's behaviour decides when
    /// this process may exit — the failure ZeroMQ's infinite `ZMQ_LINGER`
    /// default is known for
    /// (`docs/decisions/0009-drain.md` §4.4). QUIC's own closing and draining
    /// periods last about three times the path's probe timeout, so the default
    /// of one second is generous on any network where a clean close was
    /// possible at all, and it is not a deadline anything waits for twice:
    /// every endpoint is closed first, and only the idle wait is capped.
    pub shutdown_timeout: Duration,
    /// Guarantee set this runtime offers **and** requires of its peers
    /// (`docs/PROTOCOL.md` §6.1, §6.5).
    ///
    /// Offered and required are one setting in v0 on purpose: a set is a
    /// statement of what this side runs, and a peer that cannot match it
    /// fails the handshake rather than quietly giving less
    /// (`docs/decisions/0006-guarantee-sets.md` §4.4). The default is `core`,
    /// which is what every v0 peer declares by declaring nothing.
    pub guarantees: GuaranteeSet,
    /// How long a dial waits on one resolved address before trying the next.
    ///
    /// Only the addresses *before* the last one are bounded by it: a name
    /// that resolves to one address, and every IP literal, keeps the full
    /// handshake budget. The default of 250 ms is RFC 8305's Connection
    /// Attempt Delay, which exists for exactly this case — `localhost`
    /// resolving to `::1` before `127.0.0.1`, where the first address answers
    /// nothing at all and QUIC has no refusal to observe, so without a bound
    /// the second address is reached only after a handshake timeout.
    pub connect_attempt_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            limits: Limits::default(),
            keep_alive: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            worker_threads: 1,
            shutdown_timeout: Duration::from_secs(1),
            guarantees: GuaranteeSet::CORE,
            connect_attempt_timeout: Duration::from_millis(250),
        }
    }
}

/// Where PEM material comes from.
///
/// Certificates and keys are not always files. They arrive from a secret
/// manager, from an environment variable, from `include_str!`, or from a
/// Kubernetes secret already in memory. Requiring a path would force callers
/// to write private keys to disk just to hand them back — so both sources are
/// first class, and neither is a workaround for the other.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Pem {
    /// PEM bytes already in memory.
    Bytes(Vec<u8>),
    /// A PEM file, read when the TLS configuration is built.
    File(PathBuf),
}

impl Pem {
    /// Names the source for an error message.
    pub(crate) fn describe(&self) -> String {
        match self {
            Pem::Bytes(_) => "in-memory PEM".to_owned(),
            Pem::File(path) => path.display().to_string(),
        }
    }
}

/// Never prints the bytes: a `Pem` may hold a private key, and `Debug` output
/// ends up in logs.
impl std::fmt::Debug for Pem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pem::Bytes(b) => write!(f, "Pem::Bytes({} bytes)", b.len()),
            Pem::File(p) => f.debug_tuple("Pem::File").field(p).finish(),
        }
    }
}

impl From<PathBuf> for Pem {
    fn from(path: PathBuf) -> Pem {
        Pem::File(path)
    }
}

/// Who this endpoint is: a certificate chain and the private key behind it.
///
/// An identity is what a peer authenticates. Its [`Identity::fingerprint`] is
/// the value other endpoints pin, embed in an address, or list in a
/// [`Trust`] — the way an SSH host key or a WireGuard public key is the whole
/// of a peer's identity. The certificate around the key exists because TLS
/// requires one; when the fingerprint is what is trusted, nothing in the
/// certificate is consulted.
///
/// Generated identities ([`Identity::generate`]) are self-signed and made for
/// pinning. Identities issued by a certificate authority work the same way and
/// can *additionally* be trusted through that authority ([`Trust::anchor`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Identity {
    /// PEM source holding the certificate chain, leaf first.
    pub cert_chain: Pem,
    /// PEM source holding the private key.
    pub key: Pem,
}

impl Identity {
    /// A fresh self-signed identity with a new key, held only in memory.
    ///
    /// Made for pinning: the certificate carries no names, so it can only be
    /// trusted by fingerprint. Persist it with [`Identity::to_pem`] if the
    /// fingerprint must survive a restart.
    #[cfg(feature = "generate")]
    pub fn generate() -> Result<Identity, Error> {
        Identity::generate_for(Vec::<String>::new())
    }

    /// A fresh self-signed identity whose certificate also names `names`
    /// (DNS names or IP literals), so that a peer trusting the certificate
    /// itself as an anchor ([`Trust::anchor`]) can verify the host it dialled.
    #[cfg(feature = "generate")]
    pub fn generate_for(
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Identity, Error> {
        let names: Vec<String> = names.into_iter().map(Into::into).collect();
        let generated = rcgen::generate_simple_self_signed(names)
            .map_err(|e| Error::Tls(format!("generating an identity: {e}")))?;
        Ok(Identity {
            cert_chain: Pem::Bytes(generated.cert.pem().into_bytes()),
            key: Pem::Bytes(generated.signing_key.serialize_pem().into_bytes()),
        })
    }

    /// An identity from PEM buffers already in memory.
    ///
    /// Preferred when the key comes from a secret store: it never touches the
    /// filesystem, so there is no key file to create, protect and unlink. The
    /// two buffers may be the same bytes; sections of the wrong kind are
    /// skipped.
    pub fn from_pem(cert_chain: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Identity {
        Identity {
            cert_chain: Pem::Bytes(cert_chain.into()),
            key: Pem::Bytes(key.into()),
        }
    }

    /// An identity from a chain file and a key file.
    pub fn from_pem_files(cert_chain: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Identity {
        Identity {
            cert_chain: Pem::File(cert_chain.into()),
            key: Pem::File(key.into()),
        }
    }

    /// An identity from one PEM file holding both the chain and the key —
    /// the form [`Identity::to_pem`] writes.
    pub fn from_pem_file(path: impl Into<PathBuf>) -> Identity {
        let path = path.into();
        Identity {
            cert_chain: Pem::File(path.clone()),
            key: Pem::File(path),
        }
    }

    /// The fingerprint of this identity's public key: what peers pin.
    ///
    /// Reads the certificate source, so it can fail on a missing file or a
    /// malformed certificate.
    pub fn fingerprint(&self) -> Result<Fingerprint, Error> {
        let chain = crate::tls::certs_from(&self.cert_chain)?;
        crate::tls::spki_fingerprint(&chain[0])
    }

    /// The certificate chain as PEM: publishable, contains no key.
    pub fn certificate_pem(&self) -> Result<String, Error> {
        crate::tls::certificate_pem(&self.cert_chain)
    }

    /// Chain and key as one PEM document, for persisting a generated identity.
    ///
    /// Contains the private key. Write it with owner-only permissions and
    /// load it back with [`Identity::from_pem_file`].
    pub fn to_pem(&self) -> Result<String, Error> {
        let mut out = self.certificate_pem()?;
        out.push_str(&crate::tls::key_pem(&self.key)?);
        Ok(out)
    }
}

/// Whom a peer is accepted as.
///
/// A peer is accepted if its public-key fingerprint is one of `pins`, **or**
/// its certificate chains to one of `anchors` and names the host that was
/// dialled. An address that names a fingerprint
/// (`weida://sha256:…@host:port/path`) overrides both: only that identity is
/// accepted on that connection.
///
/// An empty `Trust` — [`Trust::by_address`] — accepts nothing but what the
/// address names. Dialling an address without a fingerprint under it fails
/// before any packet is sent. There is no platform root store and no
/// verification bypass anywhere in the shipped code.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Trust {
    /// PEM sources holding certificate-authority certificates.
    pub anchors: Vec<Pem>,
    /// Public-key fingerprints accepted outright.
    pub pins: Vec<Fingerprint>,
}

impl Trust {
    /// Trust only what each dialled address names.
    pub fn by_address() -> Trust {
        Trust::default()
    }

    /// Trust one public-key fingerprint.
    pub fn pin(fingerprint: Fingerprint) -> Trust {
        Trust::default().and_pin(fingerprint)
    }

    /// Trust the certificate authorities in one PEM buffer.
    pub fn anchor(pem: impl Into<Vec<u8>>) -> Trust {
        Trust::default().and_anchor(pem)
    }

    /// Trust the certificate authorities in one PEM file.
    pub fn anchor_file(path: impl Into<PathBuf>) -> Trust {
        Trust::default().and_anchor_file(path)
    }

    /// Adds a fingerprint.
    pub fn and_pin(mut self, fingerprint: Fingerprint) -> Trust {
        if !self.pins.contains(&fingerprint) {
            self.pins.push(fingerprint);
        }
        self
    }

    /// Adds the certificate authorities in a PEM buffer.
    pub fn and_anchor(mut self, pem: impl Into<Vec<u8>>) -> Trust {
        self.anchors.push(Pem::Bytes(pem.into()));
        self
    }

    /// Adds the certificate authorities in a PEM file.
    pub fn and_anchor_file(mut self, path: impl Into<PathBuf>) -> Trust {
        self.anchors.push(Pem::File(path.into()));
        self
    }

    /// True when nothing is trusted beyond what an address names.
    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty() && self.pins.is_empty()
    }
}

/// TLS configuration of a dialling endpoint: what it trusts and, optionally,
/// who it is.
///
/// Equality is by content, and the connection pool keys on it: two endpoints
/// dialling the same authority under different trust or identity must never
/// share a connection, or one would be using a peer authenticated on the
/// other's terms.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientTls {
    /// Whom to accept as the peer.
    pub trust: Trust,
    /// The identity to present. `None` dials anonymously, which a binding
    /// that requires client trust ([`ServerTls::require_client`]) refuses.
    pub identity: Option<Identity>,
}

impl ClientTls {
    /// Dials anonymously under `trust`.
    pub fn new(trust: Trust) -> ClientTls {
        ClientTls {
            trust,
            identity: None,
        }
    }

    /// Presents `identity` to every peer.
    pub fn with_identity(mut self, identity: Identity) -> ClientTls {
        self.identity = Some(identity);
        self
    }
}

impl From<Trust> for ClientTls {
    fn from(trust: Trust) -> ClientTls {
        ClientTls::new(trust)
    }
}

/// TLS configuration of a binding: who it is and, optionally, whom it lets in.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerTls {
    /// The identity presented to every dialling peer.
    pub identity: Identity,
    /// When set, every peer must present an identity this trusts; anonymous
    /// peers and untrusted identities fail the handshake. When `None`, peers
    /// are anonymous and [`crate::IncomingMeta::peer`] is `None`.
    pub client_trust: Option<Trust>,
}

impl ServerTls {
    /// Serves as `identity`, accepting anonymous peers.
    pub fn new(identity: Identity) -> ServerTls {
        ServerTls {
            identity,
            client_trust: None,
        }
    }

    /// Requires every peer to present an identity that `trust` accepts.
    pub fn require_client(mut self, trust: Trust) -> ServerTls {
        self.client_trust = Some(trust);
        self
    }
}

impl From<Identity> for ServerTls {
    fn from(identity: Identity) -> ServerTls {
        ServerTls::new(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_protocol_document() {
        let c = RuntimeConfig::default();
        assert_eq!(c.keep_alive, Duration::from_secs(10));
        assert_eq!(c.idle_timeout, Duration::from_secs(30));
        assert_eq!(c.limits, Limits::default());
    }

    #[test]
    fn a_pem_source_never_prints_its_bytes() {
        // Error messages and Debug output must not leak key material.
        assert_eq!(Pem::File("/tmp/ca.pem".into()).describe(), "/tmp/ca.pem");
        assert_eq!(Pem::Bytes(b"secret".to_vec()).describe(), "in-memory PEM");
        let debug = format!("{:?}", Identity::from_pem("-cert-", "-secret-key-"));
        assert!(!debug.contains("secret"), "{debug}");
    }

    #[test]
    fn trust_composes_and_deduplicates_pins() {
        let fp = Fingerprint::from_bytes([7; 32]);
        let t = Trust::pin(fp).and_pin(fp).and_anchor("-ca-");
        assert_eq!(t.pins, vec![fp]);
        assert_eq!(t.anchors, vec![Pem::Bytes(b"-ca-".to_vec())]);
        assert!(!t.is_empty());
        assert!(Trust::by_address().is_empty());
    }

    #[test]
    fn a_combined_pem_file_feeds_both_sources() {
        let id = Identity::from_pem_file("/tmp/id.pem");
        assert_eq!(id.cert_chain, Pem::File("/tmp/id.pem".into()));
        assert_eq!(id.key, Pem::File("/tmp/id.pem".into()));
    }

    #[test]
    fn different_trust_or_identity_is_a_different_client_tls() {
        // The pool keys on `ClientTls`; equality decides connection sharing.
        let fp = Fingerprint::from_bytes([1; 32]);
        let a = ClientTls::new(Trust::pin(fp));
        let b = ClientTls::new(Trust::pin(Fingerprint::from_bytes([2; 32])));
        let c = a.clone().with_identity(Identity::from_pem("-c-", "-k-"));
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, ClientTls::from(Trust::pin(fp)));
    }
}
