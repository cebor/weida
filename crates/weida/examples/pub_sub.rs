//! Minimal Pub/Sub: one publisher, two subscribers, prefix-filtered topics.
//!
//! ```text
//! cargo run -p weida --example pub_sub
//! ```
//!
//! Runs everything in one process on loopback. Pub **binds**, Sub
//! **connects** — the opposite of Push/Pull.
//!
//! Two things this shows that are easy to get wrong:
//!
//! * A filter is a **byte prefix**, not a pattern. `"px."` matches `px.eur`;
//!   nothing is a wildcard. The empty filter matches everything.
//! * `publish` is synchronous and returns how many subscribers it reached. It
//!   never waits for a subscriber, and `0` is not an error — a publisher with
//!   no audience is the normal startup state.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use weida::{ClientTls, Publisher, Runtime, RuntimeConfig, ServerTls, Subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let certs = Certs::generate()?;

    // --- the publishing side: binds ---------------------------------------
    let server = Runtime::new(RuntimeConfig::default())?;
    let listener = server.listener(certs.server_tls()).await?;
    certs.forget_key(); // loaded now; no key material left on disk
    let binding = listener
        .bind_quic("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let url = format!("weida://127.0.0.1:{}/md", binding.local_addr().port());

    let publisher = listener.publisher("/md")?;
    println!("publishing on {url}");
    // Nobody is listening yet, and that is fine.
    println!(
        "  before anyone subscribed: publish reached {} subscribers",
        publisher.publish("px.eur", &b"ignored"[..])?
    );

    // --- two subscribing sides: connect -----------------------------------
    // One runtime each: a subscriber claims its path in its connection's
    // namespace, so two subscribers cannot share one pooled connection.
    let prices_rt = client_runtime(&certs)?;
    let everything_rt = client_runtime(&certs)?;

    let prices = prices_rt.subscriber();
    prices.connect(&url).await?;
    prices.subscribe("px.").await?;

    let everything = everything_rt.subscriber();
    everything.connect(&url).await?;
    everything.subscribe("").await?;

    // Subscriptions register in the connection's frame-processing task, so
    // they become visible to the publisher a moment after `subscribe` returns.
    await_filters(&publisher, 2).await;
    println!(
        "  {} subscribers, {} filters\n",
        publisher.subscriber_count(),
        publisher.filter_count()
    );

    // --- publish ----------------------------------------------------------
    for (topic, payload) in [
        ("px.eur", "1.0847"),
        ("fx.usd", "0.9219"),
        ("px.gbp", "0.8461"),
    ] {
        let reached = publisher.publish(topic, payload.as_bytes())?;
        println!("published {topic:<8} {payload:<8} -> {reached} subscriber(s)");
    }

    // `px.` matches two of the three topics; the empty filter matches all
    // three. Reading exactly that many is safe because that many were
    // enqueued — *not* because they arrive in publication order. Every message
    // is its own QUIC stream and streams are unordered relative to each other,
    // so ordering is `None` (docs/GUARANTEES.md §6). Run this a few times and
    // the topics below will swap around.
    println!();
    show(&prices, "prices    (filter \"px.\")", 2).await?;
    show(&everything, "everything (filter \"\")  ", 3).await?;

    drop(prices);
    drop(everything);
    prices_rt.shutdown().await;
    everything_rt.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// Reads `count` messages and prints their topics.
async fn show(sub: &Subscriber, label: &str, count: usize) -> Result<(), weida::Error> {
    print!("{label} received:");
    for _ in 0..count {
        let transfer = sub.recv().await?;
        // The topic rides on the transfer's metadata, next to the trace context.
        let topic = transfer.meta().topic.clone().unwrap_or_default();
        let body = transfer.collect(64 * 1024).await?;
        print!(" {topic}={}", String::from_utf8_lossy(&body));
    }
    println!();
    Ok(())
}

/// Waits until the publisher can see `count` filters.
async fn await_filters(publisher: &Publisher, count: usize) {
    for _ in 0..5_000 {
        if publisher.filter_count() == count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("subscriptions never registered");
}

fn client_runtime(certs: &Certs) -> Result<Runtime, Box<dyn std::error::Error>> {
    Ok(Runtime::new(RuntimeConfig {
        client_tls: Some(certs.client_tls()),
        ..RuntimeConfig::default()
    })?)
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
        let dir = std::env::temp_dir().join(format!("weida-pub-sub-{}", std::process::id()));
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
