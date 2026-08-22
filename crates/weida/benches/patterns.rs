//! Push/Pull and Pub/Sub benchmarks over loopback QUIC.
//!
//! Two profiles matching what the patterns are for: a one-way send where the
//! acknowledgement mode is the only variable, and a fan-out where the cost
//! scales with subscriber count. As in `echo.rs`, the server is built once per
//! group so the measured work is the transfer, not TLS handshakes.

use std::net::SocketAddr;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{
    AckMode, ClientTls, Listener, Publisher, Puller, Runtime, RuntimeConfig, ServerTls,
    TransferMeta,
};

const PAYLOAD: usize = 1024;

struct Harness {
    runtime: Runtime,
    /// The certificate as PEM text. Nothing is written to disk: `criterion`
    /// exits the process when the run ends, so `Drop` would not fire and a
    /// key file would survive the benchmark.
    cert_pem: String,
    addr: SocketAddr,
    listener: Listener,
    _binding: weida::Binding,
}

impl Harness {
    /// A client runtime and an endpoint that trusts this server.
    fn client(&self) -> Runtime {
        Runtime::new(RuntimeConfig::default()).expect("client runtime")
    }

    fn trust(&self) -> ClientTls {
        ClientTls::from_pem(self.cert_pem.clone())
    }

    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }
}

async fn harness() -> Harness {
    let generated =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).expect("certificate");
    let cert_pem = generated.cert.pem();

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::from_pem(cert_pem.clone(), generated.signing_key.serialize_pem()),
        )
        .await
        .expect("bind");
    let addr = binding.local_addr();

    Harness {
        runtime,
        cert_pem,
        addr,
        listener,
        _binding: binding,
    }
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Drains a puller forever, so the sender never stalls on a full queue.
fn drain(puller: Puller) {
    tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(64 * 1024).await;
        }
    });
}

fn bench_push(c: &mut Criterion) {
    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (best_effort, acked) = rt.block_on(async {
        drain(harness.listener.puller("/best").expect("puller best"));
        drain(harness.listener.puller("/acked").expect("puller acked"));

        let client = harness.client();
        let best_effort = client.pusher(harness.trust());
        best_effort
            .connect(&harness.url("/best"))
            .await
            .expect("connect best");
        let acked = client.pusher(harness.trust());
        acked
            .connect(&harness.url("/acked"))
            .await
            .expect("connect acked");
        (best_effort, acked)
    });

    let payload = vec![0x61u8; PAYLOAD];
    let mut group = c.benchmark_group("push");
    group.throughput(Throughput::Bytes(PAYLOAD as u64));

    // Best effort settles at FIN: no round trip, so this measures the send
    // path alone.
    group.bench_function("push_1kib_best_effort", |b| {
        b.to_async(&rt).iter(|| async {
            let outcome = best_effort.send(black_box(&payload)).await.expect("send");
            black_box(outcome)
        })
    });

    // `Accepted` waits for the receiver's ACK, which is emitted when the
    // application reaches EOF: one round trip plus the handler's read.
    group.bench_function("push_1kib_acked", |b| {
        b.to_async(&rt).iter(|| async {
            let outcome = acked
                .send_with(
                    TransferMeta::default().with_ack(AckMode::Accepted),
                    black_box(&payload),
                )
                .await
                .expect("send");
            black_box(outcome)
        })
    });
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

fn bench_fanout(c: &mut Criterion) {
    const SUBSCRIBERS: usize = 8;

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (publisher, mut subscribers) = rt.block_on(async {
        let publisher: Publisher = harness.listener.publisher("/md").expect("publisher");
        let mut subscribers = Vec::with_capacity(SUBSCRIBERS);
        for _ in 0..SUBSCRIBERS {
            // One runtime per subscriber: a subscriber claims its path in its
            // connection's namespace, so they cannot share a pooled connection.
            let client = harness.client();
            let sub = client.subscriber(harness.trust());
            sub.connect(&harness.url("/md")).await.expect("connect");
            sub.subscribe("").await.expect("subscribe");
            subscribers.push((client, sub));
        }
        // Wait until the publisher can see all of them, or the first iteration
        // would measure a partial fan-out.
        while publisher.filter_count() != SUBSCRIBERS {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        (publisher, subscribers)
    });

    let payload = vec![0x7au8; PAYLOAD];
    let mut group = c.benchmark_group("fanout");
    group.throughput(Throughput::Bytes((SUBSCRIBERS * PAYLOAD) as u64));
    group.sample_size(10);

    group.bench_function("pub_1kib_8_subscribers", |b| {
        b.to_async(&rt).iter(|| async {
            let sent = publisher
                .publish("px.eur", black_box(payload.clone()))
                .expect("publish");
            assert_eq!(sent, SUBSCRIBERS, "a subscriber dropped the message");
            // Draining every subscriber is part of the measured work: the
            // publish itself only enqueues.
            for (_, sub) in &subscribers {
                let transfer = sub.recv().await.expect("recv");
                let body = transfer.collect(64 * 1024).await.expect("collect");
                debug_assert_eq!(body.len(), PAYLOAD);
            }
            black_box(sent)
        })
    });
    group.finish();

    subscribers.clear();
    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

criterion_group!(benches, bench_push, bench_fanout);
criterion_main!(benches);
