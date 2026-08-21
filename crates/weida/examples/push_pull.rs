//! Minimal Push/Pull: fire-and-forget transfers, no replies.
//!
//! ```text
//! cargo run -p weida --example push_pull
//! ```
//!
//! Runs both halves in one process on loopback. Pull **binds** (like a
//! replier), Push **connects** (like a requester).
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

use weida::{AckMode, ClientTls, Runtime, RuntimeConfig, ServerTls, TransferMeta};

const JOBS: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A throwaway certificate for loopback. It stays in memory: `ServerTls`
    // and `ClientTls` take PEM buffers, so there is no file to write, protect
    // or clean up.
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let cert_pem = cert.cert.pem();

    // --- the pulling side: binds and receives -----------------------------
    let server = Runtime::new(RuntimeConfig::default())?;
    let listener = server
        .listener(ServerTls::from_pem(
            cert_pem.clone(),
            cert.signing_key.serialize_pem(),
        ))
        .await?;
    let binding = listener
        .bind_quic("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());

    let puller = listener.puller("/jobs")?;
    let worker = tokio::spawn(async move {
        for _ in 0..JOBS {
            // `recv`, not `accept`: a pulled transfer owes no reply.
            let transfer = puller.recv().await?;
            let ack = transfer.meta().ack_mode;
            let body = transfer.collect(64 * 1024).await?;
            println!(
                "  pulled {:>7}  (ack_mode={ack})",
                String::from_utf8_lossy(&body)
            );
        }
        Ok::<(), weida::Error>(())
    });

    // --- the pushing side: connects and sends -----------------------------
    let client = Runtime::new(RuntimeConfig {
        client_tls: Some(ClientTls::from_pem(cert_pem)),
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
        println!("  sent   job-{i}    -> {outcome}");
    }

    worker.await??;

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}
