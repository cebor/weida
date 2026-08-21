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
use std::time::Duration;

use weida::{ClientTls, Publisher, Runtime, RuntimeConfig, ServerTls, Subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A throwaway certificate for loopback, kept in memory; see push_pull.rs.
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
    let cert_pem = cert.cert.pem();

    // --- the publishing side: binds ---------------------------------------
    let server = Runtime::new(RuntimeConfig::default())?;
    let listener = server.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse::<SocketAddr>()?,
            ServerTls::from_pem(cert_pem.clone(), cert.signing_key.serialize_pem()),
        )
        .await?;
    let url = format!("weida://127.0.0.1:{}/md", binding.local_addr().port());

    let publisher = listener.publisher("/md")?;
    println!("publishing on {url}");
    println!(
        "  before anyone subscribed: publish reached {} subscribers",
        publisher.publish("px.eur", &b"ignored"[..])?
    );

    // --- two subscribing sides: connect -----------------------------------
    // One runtime each: a subscriber claims its path in its connection's
    // namespace, so two subscribers cannot share one pooled connection.
    let prices_rt = client_runtime(&cert_pem)?;
    let everything_rt = client_runtime(&cert_pem)?;

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
    show(&prices, "prices     (filter \"px.\")", 2).await?;
    show(&everything, "everything (filter \"\")   ", 3).await?;

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

fn client_runtime(cert_pem: &str) -> Result<Runtime, Box<dyn std::error::Error>> {
    Ok(Runtime::new(RuntimeConfig {
        client_tls: Some(ClientTls::from_pem(cert_pem)),
        ..RuntimeConfig::default()
    })?)
}
