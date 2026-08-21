//! Minimal Push/Pull: fire-and-forget transfers, no replies.
//!
//! ```text
//! cargo run -p weida --example push_pull
//! ```
//!
//! Runs both halves in one process on loopback, so there is nothing to set up
//! and nothing to clean up afterwards. Pull **binds** (like a replier), Push
//! **connects** (like a requester).
//!
//! The one thing worth noticing: `ack_mode` is orthogonal to the pattern. The
//! same `send` is best effort or acknowledged depending only on the metadata,
//! and the outcome says which — `SentBestEffort` settles at FIN without waiting
//! for anyone, `Acked(Accepted)` means the receiving application actually read
//! the payload.
//!
//! Expect the jobs to arrive out of order. Each transfer is its own QUIC
//! stream and streams are unordered relative to each other, so ordering is
//! `None` for Push/Pull (docs/GUARANTEES.md §6). That is the protocol working
//! as specified, not a race in this example.

use std::net::SocketAddr;
use std::path::PathBuf;

use weida::{AckMode, ClientTls, Runtime, RuntimeConfig, ServerTls, TransferMeta};

const JOBS: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let certs = Certs::generate()?;

    // --- the pulling side: binds and receives -----------------------------
    let server = Runtime::new(RuntimeConfig::default())?;
    let listener = server.listener(certs.server_tls()).await?;
    certs.forget_key(); // loaded now; no key material left on disk
    let binding = listener
        .bind_quic("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());

    let puller = listener.puller("/jobs")?;
    let worker = tokio::spawn(async move {
        for _ in 0..JOBS {
            let transfer = puller.recv().await?;
            // `recv`, not `accept`: a pulled transfer owes no reply.
            let ack = transfer.meta().ack_mode;
            let body = transfer.collect(64 * 1024).await?;
            println!(
                "  pulled {:>16}  (ack_mode={ack})",
                String::from_utf8_lossy(&body)
            );
        }
        Ok::<(), weida::Error>(())
    });

    // --- the pushing side: connects and sends -----------------------------
    let client = Runtime::new(RuntimeConfig {
        client_tls: Some(certs.client_tls()),
        ..RuntimeConfig::default()
    })?;
    let pusher = client.pusher();
    pusher.connect(&url).await?;

    println!("pushing {JOBS} jobs to {url}");
    for i in 0..JOBS {
        // The last one asks to be acknowledged; the rest are fire and forget.
        let meta = if i == JOBS - 1 {
            TransferMeta::default().with_ack(AckMode::Accepted)
        } else {
            TransferMeta::default()
        };
        let outcome = pusher
            .send_with(meta, format!("job-{i}").as_bytes())
            .await?;
        println!("  sent    job-{i}          -> {outcome}");
    }

    worker.await??;

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// A self-signed certificate in a temporary directory, removed on drop.
///
/// Examples are not a place to leave private keys lying around: the key is
/// unlinked as soon as the listener has loaded it (`forget_key`), and the
/// directory goes away with this value.
struct Certs {
    dir: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}

impl Certs {
    fn generate() -> Result<Certs, Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join(format!("weida-push-pull-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        std::fs::write(&cert, generated.cert.pem())?;
        std::fs::write(&key, generated.signing_key.serialize_pem())?;
        Ok(Certs { dir, cert, key })
    }

    fn server_tls(&self) -> ServerTls {
        ServerTls::new(&self.cert, &self.key)
    }

    fn client_tls(&self) -> ClientTls {
        ClientTls::from_pem_file(&self.cert)
    }

    /// Unlinks the private key. `Runtime::listener` loads it eagerly, so after
    /// that call the file is no longer needed.
    fn forget_key(&self) {
        let _ = std::fs::remove_file(&self.key);
    }
}

impl Drop for Certs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
