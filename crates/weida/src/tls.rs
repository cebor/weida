//! TLS and QUIC transport parameters.
//!
//! The ALPN token is set on both sides, so a peer speaking a different protocol
//! fails the TLS handshake rather than reaching our frame parser.
//!
//! The crypto provider is named explicitly instead of relying on the rustls
//! process default: a library must not depend on, or install, global state in
//! its host application.
//!
//! Peer verification is one policy applied in both directions
//! ([`Policy`]): a presented leaf certificate passes if its public-key
//! fingerprint is pinned, or — when anchors are configured — if it chains to
//! one of them under the usual webpki rules. A fingerprint named by the dialled
//! address is the only thing accepted on that connection. Whatever the trust
//! path, the handshake signature is verified with the provider's algorithms,
//! so a peer is only ever accepted for a key it proved it holds.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quinn::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use quinn::rustls::crypto::CryptoProvider;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use quinn::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use quinn::rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme,
};
use quinn::{TransportConfig, VarInt};
use weida_core::{Error, Fingerprint, Limits};

use crate::config::{ClientTls, Identity, Pem, ServerTls, Trust};

/// Reads a certificate chain from either PEM source.
pub(crate) fn certs_from(pem: &Pem) -> Result<Vec<CertificateDer<'static>>, Error> {
    let chain: Vec<CertificateDer<'static>> = match pem {
        Pem::Bytes(bytes) => CertificateDer::pem_slice_iter(bytes)
            .collect::<Result<_, _>>()
            .map_err(|e| tls_err("parsing certificates", e))?,
        Pem::File(path) => CertificateDer::pem_file_iter(path)
            .map_err(|e| tls_err("reading certificates", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| tls_err("parsing certificates", e))?,
    };
    if chain.is_empty() {
        return Err(Error::Tls(format!(
            "{} contains no certificates",
            pem.describe()
        )));
    }
    Ok(chain)
}

/// Reads a private key from either PEM source.
fn key_from(pem: &Pem) -> Result<PrivateKeyDer<'static>, Error> {
    match pem {
        Pem::Bytes(bytes) => PrivateKeyDer::from_pem_slice(bytes),
        Pem::File(path) => PrivateKeyDer::from_pem_file(path),
    }
    .map_err(|e| tls_err("reading the private key", e))
}

/// Re-encodes the certificate chain of a PEM source as PEM, and nothing else.
pub(crate) fn certificate_pem(pem: &Pem) -> Result<String, Error> {
    let mut out = String::new();
    for cert in certs_from(pem)? {
        pem_block(&mut out, "CERTIFICATE", &cert);
    }
    Ok(out)
}

/// Re-encodes the private key of a PEM source as a PKCS#8 PEM block.
pub(crate) fn key_pem(pem: &Pem) -> Result<String, Error> {
    let key = key_from(pem)?;
    let (label, der) = match &key {
        PrivateKeyDer::Pkcs8(k) => ("PRIVATE KEY", k.secret_pkcs8_der()),
        PrivateKeyDer::Pkcs1(k) => ("RSA PRIVATE KEY", k.secret_pkcs1_der()),
        PrivateKeyDer::Sec1(k) => ("EC PRIVATE KEY", k.secret_sec1_der()),
        _ => return Err(Error::Tls("unsupported private key encoding".into())),
    };
    let mut out = String::new();
    pem_block(&mut out, label, der);
    Ok(out)
}

fn pem_block(out: &mut String, label: &str, der: &[u8]) {
    use std::fmt::Write as _;
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let _ = writeln!(out, "-----BEGIN {label}-----");
    let mut line = 0;
    for chunk in der.chunks(3) {
        let n = chunk.len();
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let idx = [
            b[0] >> 2,
            ((b[0] & 0x03) << 4) | (b[1] >> 4),
            ((b[1] & 0x0f) << 2) | (b[2] >> 6),
            b[2] & 0x3f,
        ];
        for (i, ix) in idx.iter().enumerate() {
            out.push(if i <= n {
                ALPHABET[*ix as usize] as char
            } else {
                '='
            });
            line += 1;
            if line == 64 {
                out.push('\n');
                line = 0;
            }
        }
    }
    if line != 0 {
        out.push('\n');
    }
    let _ = writeln!(out, "-----END {label}-----");
}

/// SHA-256 of the leaf's DER `SubjectPublicKeyInfo`: the peer's fingerprint.
pub(crate) fn spki_fingerprint(cert: &CertificateDer<'_>) -> Result<Fingerprint, Error> {
    let parsed =
        webpki::EndEntityCert::try_from(cert).map_err(|e| tls_err("parsing the certificate", e))?;
    let spki = parsed.subject_public_key_info();
    let digest = ring::digest::digest(&ring::digest::SHA256, spki.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    Ok(Fingerprint::from_bytes(out))
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(quinn::rustls::crypto::ring::default_provider())
}

fn tls_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Tls(format!("{what}: {e}"))
}

/// Loads the anchors of a `Trust` into a root store; `None` when it has none.
fn root_store(trust: &Trust) -> Result<Option<Arc<RootCertStore>>, Error> {
    if trust.anchors.is_empty() {
        return Ok(None);
    }
    let mut roots = RootCertStore::empty();
    for source in &trust.anchors {
        for cert in certs_from(source)? {
            roots
                .add(cert)
                .map_err(|e| tls_err("adding a trust anchor", e))?;
        }
    }
    Ok(Some(Arc::new(roots)))
}

/// The fingerprint the peer presented and was refused for, if any.
///
/// Filled in by the verifier and read by the dialling side after a failed
/// handshake, so the failure can name what showed up instead of what was
/// expected. A refusal for a malformed certificate leaves it empty.
pub(crate) type Refused = Arc<Mutex<Option<Fingerprint>>>;

/// The trust decision shared by both verifiers.
struct Policy {
    /// The one identity an address named; overrides everything else.
    expected: Option<Fingerprint>,
    pins: HashSet<Fingerprint>,
    refused: Refused,
}

/// What the policy decided about a leaf without looking at its chain.
enum Verdict {
    Accept,
    /// Not pinned: only a chain to an anchor can still accept it.
    Chain(Fingerprint),
}

impl Policy {
    fn new(expected: Option<Fingerprint>, trust: &Trust) -> Policy {
        Policy {
            expected,
            pins: trust.pins.iter().copied().collect(),
            refused: Arc::new(Mutex::new(None)),
        }
    }

    fn judge(&self, end_entity: &CertificateDer<'_>) -> Result<Verdict, quinn::rustls::Error> {
        let presented = spki_fingerprint(end_entity)
            .map_err(|_| quinn::rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
        if let Some(expected) = self.expected {
            return if presented == expected {
                Ok(Verdict::Accept)
            } else {
                Err(self.refuse(presented))
            };
        }
        if self.pins.contains(&presented) {
            return Ok(Verdict::Accept);
        }
        Ok(Verdict::Chain(presented))
    }

    fn refuse(&self, presented: Fingerprint) -> quinn::rustls::Error {
        *self.refused.lock().expect("refusal record poisoned") = Some(presented);
        quinn::rustls::Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
    }
}

impl std::fmt::Debug for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Policy")
            .field("expected", &self.expected)
            .field("pins", &self.pins.len())
            .finish()
    }
}

/// Verifies the server a client dialled.
#[derive(Debug)]
struct PeerVerifier {
    policy: Policy,
    chain: Option<Arc<dyn ServerCertVerifier>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, quinn::rustls::Error> {
        match self.policy.judge(end_entity)? {
            Verdict::Accept => Ok(ServerCertVerified::assertion()),
            Verdict::Chain(presented) => match &self.chain {
                Some(chain) => chain
                    .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
                    .map_err(|_| self.policy.refuse(presented)),
                None => Err(self.policy.refuse(presented)),
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifies the client a binding accepted.
#[derive(Debug)]
struct ClientVerifier {
    policy: Policy,
    chain: Option<Arc<dyn ClientCertVerifier>>,
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for ClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // A pinned peer has no issuer to be hinted about, and webpki's hints
        // would make a pinned-but-unanchored client believe it cannot satisfy
        // the request. Sending none lets every client offer what it has.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, quinn::rustls::Error> {
        match self.policy.judge(end_entity)? {
            Verdict::Accept => Ok(ClientCertVerified::assertion()),
            Verdict::Chain(presented) => match &self.chain {
                Some(chain) => chain
                    .verify_client_cert(end_entity, intermediates, now)
                    .map_err(|_| self.policy.refuse(presented)),
                None => Err(self.policy.refuse(presented)),
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, quinn::rustls::Error> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Builds the QUIC transport parameters shared by both sides.
///
/// Every value here bounds memory a remote peer can cause us to hold; see
/// `docs/PROTOCOL.md` §10.
pub(crate) fn transport_config(
    limits: &Limits,
    idle_timeout: Duration,
    keep_alive: Option<Duration>,
) -> Result<TransportConfig, Error> {
    let mut tc = TransportConfig::default();
    tc.max_concurrent_uni_streams(VarInt::from_u32(limits.max_concurrent_uni_streams));
    // Bidirectional streams carry Req/Rep exchanges: one per live request, so
    // this bounds concurrent exchanges the peer can hold open on us.
    tc.max_concurrent_bidi_streams(VarInt::from_u32(limits.max_concurrent_bidi_streams));
    tc.stream_receive_window(
        VarInt::from_u64(limits.stream_receive_window)
            .map_err(|_| Error::Runtime("stream_receive_window exceeds 2^62-1".into()))?,
    );
    tc.receive_window(
        VarInt::from_u64(limits.connection_receive_window)
            .map_err(|_| Error::Runtime("connection_receive_window exceeds 2^62-1".into()))?,
    );
    let idle = quinn::IdleTimeout::try_from(idle_timeout)
        .map_err(|e| Error::Runtime(format!("idle_timeout out of range: {e}")))?;
    tc.max_idle_timeout(Some(idle));
    tc.keep_alive_interval(keep_alive);
    Ok(tc)
}

/// Loads an identity's chain and key.
fn load_identity(
    identity: &Identity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Error> {
    Ok((certs_from(&identity.cert_chain)?, key_from(&identity.key)?))
}

/// Builds a QUIC server configuration from the binding's identity and, when
/// it requires one, the trust it holds clients to.
pub(crate) fn server_config(
    tls: &ServerTls,
    limits: &Limits,
    idle_timeout: Duration,
) -> Result<quinn::ServerConfig, Error> {
    let (chain, key) = load_identity(&tls.identity)?;
    let provider = provider();

    let builder = quinn::rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?;
    let builder = match &tls.client_trust {
        None => builder.with_no_client_auth(),
        Some(trust) => {
            if trust.is_empty() {
                return Err(Error::Tls(
                    "client trust is empty: a binding cannot require clients it would never accept"
                        .into(),
                ));
            }
            let chain_verifier = match root_store(trust)? {
                Some(roots) => Some(
                    quinn::rustls::server::WebPkiClientVerifier::builder_with_provider(
                        roots,
                        Arc::clone(&provider),
                    )
                    .build()
                    .map(|v| v as Arc<dyn ClientCertVerifier>)
                    .map_err(|e| tls_err("building the client verifier", e))?,
                ),
                None => None,
            };
            builder.with_client_cert_verifier(Arc::new(ClientVerifier {
                policy: Policy::new(None, trust),
                chain: chain_verifier,
                provider: Arc::clone(&provider),
            }))
        }
    };
    let mut crypto = builder
        .with_single_cert(chain, key)
        .map_err(|e| tls_err("installing the server certificate", e))?;
    crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC server crypto", e))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(limits, idle_timeout, None)?));
    Ok(config)
}

/// Builds a QUIC client configuration for one dial.
///
/// `expected` is the fingerprint the address named, if any. The returned
/// [`Refused`] is where the verifier records a peer it turned away, so the
/// caller can report [`Error::Untrusted`] with the identity that showed up.
pub(crate) fn client_config(
    tls: &ClientTls,
    expected: Option<Fingerprint>,
    limits: &Limits,
    keep_alive: Duration,
    idle_timeout: Duration,
) -> Result<(quinn::ClientConfig, Refused), Error> {
    if expected.is_none() && tls.trust.is_empty() {
        return Err(Error::Tls(
            "nothing to trust: the address names no fingerprint and the trust set is empty".into(),
        ));
    }
    let provider = provider();
    let chain_verifier = match root_store(&tls.trust)? {
        Some(roots) => Some(
            quinn::rustls::client::WebPkiServerVerifier::builder_with_provider(
                roots,
                Arc::clone(&provider),
            )
            .build()
            .map(|v| v as Arc<dyn ServerCertVerifier>)
            .map_err(|e| tls_err("building the server verifier", e))?,
        ),
        None => None,
    };
    let policy = Policy::new(expected, &tls.trust);
    let refused = Arc::clone(&policy.refused);
    let verifier = Arc::new(PeerVerifier {
        policy,
        chain: chain_verifier,
        provider: Arc::clone(&provider),
    });

    let builder = quinn::rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut crypto = match &tls.identity {
        None => builder.with_no_client_auth(),
        Some(identity) => {
            let (chain, key) = load_identity(identity)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| tls_err("installing the client certificate", e))?
        }
    };
    crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC client crypto", e))?;
    let mut config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(
        limits,
        idle_timeout,
        Some(keep_alive),
    )?));
    Ok((config, refused))
}

/// The fingerprint of the identity a connected peer proved, if it presented
/// one. Computed once per connection, after the handshake.
pub(crate) fn peer_fingerprint(conn: &quinn::Connection) -> Option<Fingerprint> {
    let identity = conn.peer_identity()?;
    let chain = identity.downcast::<Vec<CertificateDer<'static>>>().ok()?;
    let leaf = chain.first()?;
    // The verifier already parsed this certificate; a failure here would mean
    // it accepted something it could not parse, which it does not.
    spki_fingerprint(leaf).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_config_accepts_the_defaults() {
        assert!(
            transport_config(
                &Limits::default(),
                Duration::from_secs(30),
                Some(Duration::from_secs(10))
            )
            .is_ok()
        );
    }

    #[test]
    fn an_absurd_idle_timeout_is_rejected_not_clamped() {
        let err =
            transport_config(&Limits::default(), Duration::from_secs(u64::MAX), None).unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[test]
    fn an_oversized_receive_window_is_rejected() {
        let limits = Limits {
            stream_receive_window: u64::MAX,
            ..Limits::default()
        };
        assert!(matches!(
            transport_config(&limits, Duration::from_secs(30), None).unwrap_err(),
            Error::Runtime(_)
        ));
    }

    #[test]
    fn missing_tls_files_are_reported_as_tls_errors() {
        let err = server_config(
            &ServerTls::new(Identity::from_pem_files(
                "/nonexistent/cert.pem",
                "/nonexistent/key.pem",
            )),
            &Limits::default(),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        let err = client_config(
            &ClientTls::new(Trust::anchor_file("/nonexistent/ca.pem")),
            None,
            &Limits::default(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[test]
    fn empty_trust_is_rejected_unless_the_address_names_a_peer() {
        let err = client_config(
            &ClientTls::new(Trust::by_address()),
            None,
            &Limits::default(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        assert!(
            client_config(
                &ClientTls::new(Trust::by_address()),
                Some(Fingerprint::from_bytes([0; 32])),
                &Limits::default(),
                Duration::from_secs(10),
                Duration::from_secs(30),
            )
            .is_ok()
        );
    }

    #[cfg(feature = "generate")]
    #[test]
    fn a_binding_cannot_require_clients_it_would_never_accept() {
        let identity = Identity::generate().expect("identity");
        let err = server_config(
            &ServerTls::new(identity).require_client(Trust::by_address()),
            &Limits::default(),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[cfg(feature = "generate")]
    #[test]
    fn a_generated_identity_roundtrips_through_pem_with_the_same_fingerprint() {
        let identity = Identity::generate().expect("identity");
        let fp = identity.fingerprint().expect("fingerprint");
        let pem = identity.to_pem().expect("pem");
        assert!(pem.contains("BEGIN CERTIFICATE"));
        assert!(pem.contains("BEGIN PRIVATE KEY"));
        let reloaded = Identity::from_pem(pem.clone(), pem);
        assert_eq!(reloaded.fingerprint().expect("fingerprint"), fp);
        assert!(
            !identity
                .certificate_pem()
                .expect("cert")
                .contains("PRIVATE"),
        );
    }

    #[cfg(feature = "generate")]
    #[test]
    fn the_fingerprint_is_over_the_key_not_the_certificate() {
        // Two certificates for one key must fingerprint identically: that is
        // what makes a pin survive a certificate renewal. A different key must
        // not.
        let key = rcgen::KeyPair::generate().expect("key");
        let a = rcgen::CertificateParams::new(vec!["a.example".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        let b = rcgen::CertificateParams::new(vec!["b.example".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        assert_ne!(a.der(), b.der());
        assert_eq!(
            spki_fingerprint(a.der()).unwrap(),
            spki_fingerprint(b.der()).unwrap()
        );
        let other = Identity::generate().unwrap().fingerprint().unwrap();
        assert_ne!(spki_fingerprint(a.der()).unwrap(), other);
    }

    #[test]
    fn pem_encoding_matches_the_reference_alphabet() {
        let mut out = String::new();
        pem_block(&mut out, "TEST", b"hello world");
        assert_eq!(
            out,
            "-----BEGIN TEST-----\naGVsbG8gd29ybGQ=\n-----END TEST-----\n"
        );
        let mut out = String::new();
        pem_block(&mut out, "TEST", b"ab");
        assert_eq!(out, "-----BEGIN TEST-----\nYWI=\n-----END TEST-----\n");
        let mut out = String::new();
        pem_block(&mut out, "TEST", &[0u8; 48]);
        let body: Vec<&str> = out.lines().collect();
        assert_eq!(body[1].len(), 64, "lines wrap at 64 columns");
    }
}
