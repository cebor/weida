//! Shared harness for the integration tests: a real QUIC server and client on
//! loopback, with a freshly generated self-signed certificate.
//!
//! No test hooks exist in the library. Everything below drives the public API
//! or, for the hostile-peer suite, speaks the wire protocol directly.

// Each test binary uses a different subset of this harness.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use weida::{Binding, ClientTls, Limits, Listener, Runtime, RuntimeConfig, ServerTls};

/// A generated certificate on disk, removed when dropped.
pub struct Certs {
    pub dir: PathBuf,
    pub cert_pem: PathBuf,
    pub key_pem: PathBuf,
}

impl Certs {
    /// Generates a certificate valid for `localhost`, `127.0.0.1` and `::1`.
    pub fn generate() -> Certs {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "weida-test-{}-{}-{id}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        // A killed test process cannot run `Drop`, so the directory may outlive
        // the run. Owner-only permissions keep the leftover key material
        // unreadable rather than merely short-lived.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .expect("restrict temp dir");
        }

        let generated = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "::1".to_owned(),
        ])
        .expect("generate certificate");

        let cert_pem = dir.join("cert.pem");
        let key_pem = dir.join("key.pem");
        std::fs::write(&cert_pem, generated.cert.pem()).expect("write cert");
        std::fs::write(&key_pem, generated.signing_key.serialize_pem()).expect("write key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_pem, std::fs::Permissions::from_mode(0o600))
                .expect("restrict key");
        }

        Certs {
            dir,
            cert_pem,
            key_pem,
        }
    }

    pub fn server_tls(&self) -> ServerTls {
        ServerTls::new(&self.cert_pem, &self.key_pem)
    }

    pub fn client_tls(&self) -> ClientTls {
        ClientTls::from_pem_file(&self.cert_pem)
    }
}

impl Drop for Certs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A running server: runtime, listener and one loopback binding.
pub struct Server {
    pub runtime: Runtime,
    pub listener: Listener,
    pub binding: Binding,
    pub addr: SocketAddr,
    pub certs: Certs,
}

impl Server {
    /// Starts a server on an ephemeral loopback port with default limits.
    pub async fn start() -> Server {
        Server::start_with(Limits::default()).await
    }

    /// Starts a server with explicit limits.
    pub async fn start_with(limits: Limits) -> Server {
        let certs = Certs::generate();
        let config = RuntimeConfig {
            limits,
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).expect("runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let addr = binding.local_addr();
        Server {
            runtime,
            listener,
            binding,
            addr,
            certs,
        }
    }

    /// A `weida://` URL for `path` on this server.
    pub fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }

    /// A client runtime trusting this server's certificate.
    pub fn client_runtime(&self) -> Runtime {
        self.client_runtime_with(Limits::default())
    }

    /// A client runtime with explicit limits.
    pub fn client_runtime_with(&self, limits: Limits) -> Runtime {
        Runtime::new(RuntimeConfig {
            limits,
            client_tls: Some(self.certs.client_tls()),
            ..RuntimeConfig::default()
        })
        .expect("client runtime")
    }
}

/// Raw wire-protocol peers.
///
/// These speak `weida-protocol` over bare `quinn` so the hostile-peer suite can
/// send byte sequences the library would never produce. The library itself has
/// no test hooks.
pub mod raw {
    use std::sync::Arc;

    use quinn::rustls::pki_types::pem::PemObject;
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use weida_protocol::{FrameKind, Hello, MAX_PREAMBLE_LEN, Preamble, encode_frame};

    use super::Certs;

    fn provider() -> Arc<quinn::rustls::crypto::CryptoProvider> {
        Arc::new(quinn::rustls::crypto::ring::default_provider())
    }

    /// A client endpoint trusting `certs` and offering the weida ALPN.
    pub fn client_endpoint(certs: &Certs) -> quinn::Endpoint {
        let mut roots = quinn::rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&certs.cert_pem).expect("read cert") {
            roots.add(cert.expect("parse cert")).expect("add root");
        }
        let mut crypto = quinn::rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&quinn::rustls::version::TLS13])
            .expect("tls 1.3")
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth();
        crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];
        let config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("quic crypto"),
        ));

        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().expect("loopback"))
            .expect("client endpoint");
        endpoint.set_default_client_config(config);
        endpoint
    }

    /// A server endpoint on an ephemeral loopback port.
    pub fn server_endpoint(certs: &Certs) -> (quinn::Endpoint, std::net::SocketAddr) {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&certs.cert_pem)
            .expect("read cert")
            .collect::<Result<_, _>>()
            .expect("parse cert");
        let key = PrivateKeyDer::from_pem_file(&certs.key_pem).expect("read key");
        let mut crypto = quinn::rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&quinn::rustls::version::TLS13])
            .expect("tls 1.3")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server cert");
        crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];
        let config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(crypto).expect("quic crypto"),
        ));

        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().expect("loopback"))
            .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local addr");
        (endpoint, addr)
    }

    /// Sends a well-formed HELLO so the peer's negotiation completes.
    pub async fn send_hello(conn: &quinn::Connection) {
        send_frame(conn, FrameKind::Hello, &Hello::v0(16 * 1024, 1024).encode()).await;
    }

    /// Sends one header-only frame on its own stream.
    pub async fn send_frame(conn: &quinn::Connection, kind: FrameKind, header: &[u8]) {
        let mut stream = conn.open_uni().await.expect("open uni");
        stream
            .write_all(&encode_frame(kind, header))
            .await
            .expect("write frame");
        stream.finish().expect("finish");
    }

    /// Sends arbitrary bytes on a fresh unidirectional stream.
    pub async fn send_raw(conn: &quinn::Connection, bytes: &[u8]) {
        let mut stream = conn.open_uni().await.expect("open uni");
        stream.write_all(bytes).await.expect("write raw");
        stream.finish().expect("finish");
    }

    /// Reads one frame's preamble and header from `stream`.
    pub async fn read_frame(stream: &mut quinn::RecvStream) -> (Preamble, Vec<u8>) {
        let mut scratch = [0u8; MAX_PREAMBLE_LEN];
        let mut have = 0usize;
        let preamble = loop {
            match weida_protocol::parse_preamble(&scratch[..have], 16 * 1024) {
                Ok((preamble, _)) => break preamble,
                Err(weida_protocol::PreambleError::Incomplete) => {
                    let n = stream
                        .read(&mut scratch[have..have + 1])
                        .await
                        .expect("read preamble")
                        .expect("stream ended inside the preamble");
                    have += n;
                }
                Err(e) => panic!("bad preamble: {e}"),
            }
        };
        let mut header = vec![0u8; preamble.header_len as usize];
        stream.read_exact(&mut header).await.expect("read header");
        (preamble, header)
    }

    /// Accepts inbound streams until one carries a frame of `kind`, returning
    /// the stream and its header bytes.
    ///
    /// Necessary because the peer's own HELLO arrives on an unrelated stream:
    /// unidirectional streams are unordered, so "the next stream" is never a
    /// safe assumption.
    pub async fn accept_frame(
        conn: &quinn::Connection,
        kind: FrameKind,
    ) -> (quinn::RecvStream, Vec<u8>) {
        loop {
            let mut stream = conn.accept_uni().await.expect("accept uni");
            let (preamble, header) = read_frame(&mut stream).await;
            if preamble.kind == kind {
                return (stream, header);
            }
        }
    }

    /// Accepts streams until a DATA frame arrives, returning its header.
    pub async fn accept_data(
        conn: &quinn::Connection,
    ) -> (quinn::RecvStream, weida_protocol::DataHeader) {
        let (stream, header) = accept_frame(conn, FrameKind::Data).await;
        (
            stream,
            weida_protocol::DataHeader::decode(&header).expect("decode DATA header"),
        )
    }

    /// Waits for the peer to close the connection and returns the application
    /// error code it used.
    pub async fn closed_code(conn: &quinn::Connection) -> u64 {
        match conn.closed().await {
            quinn::ConnectionError::ApplicationClosed(frame) => frame.error_code.into_inner(),
            other => panic!("expected an application close, got {other}"),
        }
    }
}

/// Deterministic payload generator: no dependency, reproducible failures.
pub struct Xorshift(u64);

impl Xorshift {
    pub fn new(seed: u64) -> Xorshift {
        Xorshift(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Fills `buf` with pseudorandom bytes.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
    }
}

/// FNV-1a, for cheap end-to-end payload verification.
pub struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    pub fn update(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x1000_0000_01b3);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}
