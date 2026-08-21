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
        let listener = runtime
            .listener(certs.server_tls())
            .await
            .expect("listener");
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().expect("loopback address"))
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
