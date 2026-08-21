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

/// Where PEM material comes from.
///
/// Certificates and keys are not always files. They arrive from a secret
/// manager, from an environment variable, from `include_str!`, or from a
/// Kubernetes secret already in memory. Requiring a path would force callers
/// to write private keys to disk just to hand them back — so both sources are
/// first class, and neither is a workaround for the other.
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl From<PathBuf> for Pem {
    fn from(path: PathBuf) -> Pem {
        Pem::File(path)
    }
}

/// Trust anchors used when dialling.
///
/// v0 accepts explicit trust anchors only: no platform root store, and no
/// certificate-verification bypass anywhere in the shipped code.
#[derive(Clone, Debug)]
pub struct ClientTls {
    /// PEM sources holding the certificates to trust.
    pub roots_pem: Vec<Pem>,
}

impl ClientTls {
    /// Trust exactly one PEM file.
    pub fn from_pem_file(path: impl Into<PathBuf>) -> ClientTls {
        ClientTls {
            roots_pem: vec![Pem::File(path.into())],
        }
    }

    /// Trust the certificates in one PEM buffer already in memory.
    pub fn from_pem(pem: impl Into<Vec<u8>>) -> ClientTls {
        ClientTls {
            roots_pem: vec![Pem::Bytes(pem.into())],
        }
    }
}

/// Server identity for a [`crate::Listener`].
#[derive(Clone, Debug)]
pub struct ServerTls {
    /// PEM source holding the certificate chain, leaf first.
    pub cert_chain_pem: Pem,
    /// PEM source holding the private key.
    pub key_pem: Pem,
}

impl ServerTls {
    /// Builds a server identity from a chain file and a key file.
    pub fn new(cert_chain_pem: impl Into<PathBuf>, key_pem: impl Into<PathBuf>) -> ServerTls {
        ServerTls {
            cert_chain_pem: Pem::File(cert_chain_pem.into()),
            key_pem: Pem::File(key_pem.into()),
        }
    }

    /// Builds a server identity from PEM buffers already in memory.
    ///
    /// Preferred when the key comes from a secret store: it never touches the
    /// filesystem, so there is no key file to create, protect and unlink.
    pub fn from_pem(cert_chain: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> ServerTls {
        ServerTls {
            cert_chain_pem: Pem::Bytes(cert_chain.into()),
            key_pem: Pem::Bytes(key.into()),
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
        assert_eq!(s.cert_chain_pem, Pem::File(PathBuf::from("/tmp/c.pem")));
        assert_eq!(s.key_pem, Pem::File(PathBuf::from("/tmp/k.pem")));
    }

    #[test]
    fn tls_material_may_come_from_memory() {
        // The point of `Pem::Bytes`: a key from a secret store never has to be
        // written to disk to be usable.
        let s = ServerTls::from_pem("-cert-", "-key-");
        assert_eq!(s.cert_chain_pem, Pem::Bytes(b"-cert-".to_vec()));
        assert_eq!(s.key_pem, Pem::Bytes(b"-key-".to_vec()));

        let c = ClientTls::from_pem("-ca-");
        assert_eq!(c.roots_pem, vec![Pem::Bytes(b"-ca-".to_vec())]);
    }

    #[test]
    fn a_pem_source_names_itself_in_errors() {
        // Error messages must not print key bytes.
        assert_eq!(Pem::File("/tmp/ca.pem".into()).describe(), "/tmp/ca.pem");
        assert_eq!(Pem::Bytes(b"secret".to_vec()).describe(), "in-memory PEM");
    }
}
