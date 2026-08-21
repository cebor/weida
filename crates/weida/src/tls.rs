//! TLS and QUIC transport parameters.
//!
//! The ALPN token is set on both sides, so a peer speaking a different protocol
//! fails the TLS handshake rather than reaching our frame parser.
//!
//! The crypto provider is named explicitly instead of relying on the rustls
//! process default: a library must not depend on, or install, global state in
//! its host application.

use std::sync::Arc;
use std::time::Duration;

use quinn::rustls::RootCertStore;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use quinn::{TransportConfig, VarInt};
use weida_core::{Error, Limits};

use crate::config::{ClientTls, ServerTls};

fn provider() -> Arc<quinn::rustls::crypto::CryptoProvider> {
    Arc::new(quinn::rustls::crypto::ring::default_provider())
}

fn tls_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Tls(format!("{what}: {e}"))
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
    // Bidirectional streams are not part of the v0 protocol; refusing them
    // keeps a peer from allocating state we would never read.
    tc.max_concurrent_bidi_streams(VarInt::from_u32(0));
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

/// Builds a QUIC server configuration from PEM files on disk.
pub(crate) fn server_config(
    tls: &ServerTls,
    limits: &Limits,
    idle_timeout: Duration,
) -> Result<quinn::ServerConfig, Error> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&tls.cert_chain_pem)
        .map_err(|e| tls_err("reading certificate chain", e))?
        .collect::<Result<_, _>>()
        .map_err(|e| tls_err("parsing certificate chain", e))?;
    if chain.is_empty() {
        return Err(Error::Tls(format!(
            "certificate chain {} contains no certificates",
            tls.cert_chain_pem.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_file(&tls.key_pem)
        .map_err(|e| tls_err("reading private key", e))?;

    let mut crypto = quinn::rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| tls_err("installing the server certificate", e))?;
    crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC server crypto", e))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(limits, idle_timeout, None)?));
    Ok(config)
}

/// Builds a QUIC client configuration trusting exactly the configured PEM files.
pub(crate) fn client_config(
    tls: &ClientTls,
    limits: &Limits,
    keep_alive: Duration,
    idle_timeout: Duration,
) -> Result<quinn::ClientConfig, Error> {
    if tls.roots_pem.is_empty() {
        return Err(Error::Tls("client_tls.roots_pem is empty".into()));
    }
    let mut roots = RootCertStore::empty();
    for path in &tls.roots_pem {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
            .map_err(|e| tls_err("reading trust anchors", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| tls_err("parsing trust anchors", e))?;
        if certs.is_empty() {
            return Err(Error::Tls(format!(
                "trust anchor file {} contains no certificates",
                path.display()
            )));
        }
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| tls_err("adding a trust anchor", e))?;
        }
    }

    let mut crypto = quinn::rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|e| tls_err("selecting TLS 1.3", e))?
        .with_root_certificates(Arc::new(roots))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| tls_err("building the QUIC client crypto", e))?;
    let mut config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    config.transport_config(Arc::new(transport_config(
        limits,
        idle_timeout,
        Some(keep_alive),
    )?));
    Ok(config)
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
            &ServerTls::new("/nonexistent/cert.pem", "/nonexistent/key.pem"),
            &Limits::default(),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        let err = client_config(
            &ClientTls::from_pem_file("/nonexistent/ca.pem"),
            &Limits::default(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[test]
    fn an_empty_trust_store_is_rejected() {
        let err = client_config(
            &ClientTls { roots_pem: vec![] },
            &Limits::default(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }
}
