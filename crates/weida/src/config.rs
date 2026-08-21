//! Runtime and TLS configuration.

use std::path::PathBuf;
use std::time::Duration;

use weida_core::Limits;

/// Configuration for one [`crate::Runtime`].
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Resource limits applied to every connection this runtime owns.
    pub limits: Limits,
    /// Trust anchors for outgoing connections. Required before
    /// [`crate::Requester::connect`] can be used.
    pub client_tls: Option<ClientTls>,
    /// QUIC keep-alive interval for outgoing connections.
    pub keep_alive: Duration,
    /// QUIC idle timeout, applied in both directions.
    pub idle_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            limits: Limits::default(),
            client_tls: None,
            keep_alive: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

/// Trust anchors used when dialling.
///
/// v0 accepts explicit CA files only: no platform root store, and no
/// certificate-verification bypass anywhere in the shipped code.
#[derive(Clone, Debug)]
pub struct ClientTls {
    /// PEM files holding the certificates to trust.
    pub roots_pem: Vec<PathBuf>,
}

impl ClientTls {
    /// Trust exactly one PEM file.
    pub fn from_pem_file(path: impl Into<PathBuf>) -> ClientTls {
        ClientTls {
            roots_pem: vec![path.into()],
        }
    }
}

/// Server identity for a [`crate::Listener`].
#[derive(Clone, Debug)]
pub struct ServerTls {
    /// PEM file holding the certificate chain, leaf first.
    pub cert_chain_pem: PathBuf,
    /// PEM file holding the private key.
    pub key_pem: PathBuf,
}

impl ServerTls {
    /// Builds a server identity from a chain and a key file.
    pub fn new(cert_chain_pem: impl Into<PathBuf>, key_pem: impl Into<PathBuf>) -> ServerTls {
        ServerTls {
            cert_chain_pem: cert_chain_pem.into(),
            key_pem: key_pem.into(),
        }
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
        assert!(
            c.client_tls.is_none(),
            "trust must be opted into explicitly"
        );
        assert_eq!(c.limits, Limits::default());
    }

    #[test]
    fn tls_constructors_are_shorthand() {
        assert_eq!(ClientTls::from_pem_file("/tmp/ca.pem").roots_pem.len(), 1);
        let s = ServerTls::new("/tmp/c.pem", "/tmp/k.pem");
        assert_eq!(s.cert_chain_pem, PathBuf::from("/tmp/c.pem"));
        assert_eq!(s.key_pem, PathBuf::from("/tmp/k.pem"));
    }
}
