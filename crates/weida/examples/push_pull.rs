//! Minimal Push/Pull: fire-and-forget transfers, no replies.
//!
//! ```text
//! cargo run -p weida --example push_pull
//! ```
//!
//! Runs both halves in one process on loopback. Pull **binds** (like a
//! replier), Push **connects** (like a requester).
//!
//! The one thing worth noticing: the receipt is orthogonal to the pattern.
//! `send` is fire-and-forget pipeline semantics — it returns at FIN and costs
//! no round trip. `open()`/`finish()`/`delivered()` is the same transfer with
//! QUIC's own fin-acknowledgement awaited: it means *the peer's transport
//! holds every byte*, not that the peer's application read them. There is no
//! application-level acknowledgement in the v0 core, by design.
//!
//! Expect the jobs to arrive out of order. Each transfer is its own QUIC
//! stream and streams are unordered relative to each other, so ordering is
//! `None` for Push/Pull (docs/GUARANTEES.md §6). That is the protocol working
//! as specified, not a race in this example.

use std::net::SocketAddr;

use weida::{ClientTls, Runtime, RuntimeConfig, ServerTls, TransferMeta};

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
    let listener = server.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse::<SocketAddr>()?,
            ServerTls::from_pem(cert_pem.clone(), cert.signing_key.serialize_pem()),
        )
        .await?;
    let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());

    let puller = listener.puller("/jobs")?;
    let worker = tokio::spawn(async move {
        for _ in 0..JOBS {
            // `recv`, not `accept`: a pulled transfer owes no reply.
            let transfer = puller.recv().await?;
            let body = transfer.collect(64 * 1024).await?;
            println!("  pulled {:>7}", String::from_utf8_lossy(&body));
        }
        Ok::<(), weida::Error>(())
    });

    // --- the pushing side: connects and sends -----------------------------
    let client = Runtime::new(RuntimeConfig::default())?;
    let pusher = client.pusher(ClientTls::from_pem(cert_pem));
    pusher.connect(&url).await?;

    println!("pushing {JOBS} jobs to {url}");
    for i in 0..JOBS - 1 {
        pusher.send(format!("job-{i}").as_bytes()).await?;
        println!("  sent   job-{i}    -> queued");
    }

    // The last one keeps the receipt. `delivered()` resolves on QUIC's
    // transport acknowledgement — the bytes are in the peer's stack, which is
    // strictly weaker than "the worker processed the job".
    let last = JOBS - 1;
    let mut transfer = pusher.open(TransferMeta::default()).await?;
    transfer.write_all(format!("job-{last}").as_bytes()).await?;
    transfer.finish()?.delivered().await?;
    println!("  sent   job-{last}    -> delivered (transport receipt)");

    worker.await??;

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}
