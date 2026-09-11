//! Push/Pull and Pub/Sub benchmarks over loopback QUIC.
//!
//! Two profiles matching what the patterns are for: a one-way send where the
//! acknowledgement mode is the only variable, and a fan-out where the cost
//! scales with subscriber count. As in `echo.rs`, the server is built once per
//! group so the measured work is the transfer, not TLS handshakes.

use std::net::SocketAddr;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{Identity, Listener, Publisher, Puller, Runtime, RuntimeConfig, TransferMeta, Trust};
use weida_protocol::{DataHeader, FrameKind, encode_frame};

const PAYLOAD: usize = 1024;

struct Harness {
    runtime: Runtime,
    /// The server's fingerprint. Nothing is written to disk: `criterion`
    /// exits the process when the run ends, so `Drop` would not fire and a
    /// key file would survive the benchmark.
    trust: Trust,
    addr: SocketAddr,
    listener: Listener,
    _binding: weida::Binding,
}

impl Harness {
    /// A client runtime and an endpoint that trusts this server.
    fn client(&self) -> Runtime {
        Runtime::new(RuntimeConfig::default()).expect("client runtime")
    }

    fn trust(&self) -> Trust {
        self.trust.clone()
    }

    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }
}

async fn harness() -> Harness {
    let identity = Identity::generate().expect("identity");
    let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
        .await
        .expect("bind");
    let addr = binding.local_addr();

    Harness {
        runtime,
        trust,
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

    let (best_effort, delivered) = rt.block_on(async {
        drain(harness.listener.puller("/best").expect("puller best"));
        drain(
            harness
                .listener
                .puller("/delivered")
                .expect("puller delivered"),
        );

        let client = harness.client();
        let best_effort = client.pusher(harness.trust());
        best_effort
            .connect(&harness.url("/best"))
            .await
            .expect("connect best");
        let delivered = client.pusher(harness.trust());
        delivered
            .connect(&harness.url("/delivered"))
            .await
            .expect("connect delivered");
        (best_effort, delivered)
    });

    let payload = vec![0x61u8; PAYLOAD];
    let mut group = c.benchmark_group("push");
    group.throughput(Throughput::Bytes(PAYLOAD as u64));

    // Best effort settles at FIN: no round trip, so this measures the send
    // path alone.
    group.bench_function("push_1kib_best_effort", |b| {
        b.to_async(&rt).iter(|| async {
            best_effort.send(black_box(&payload)).await.expect("send");
        })
    });

    // Awaiting the receipt waits for the peer's transport acknowledgement:
    // one network round trip, and no application involvement at all.
    group.bench_function("push_1kib_delivered", |b| {
        b.to_async(&rt).iter(|| async {
            let mut transfer = delivered.open(TransferMeta::default()).await.expect("open");
            transfer
                .write_all(black_box(&payload))
                .await
                .expect("write");
            transfer
                .finish()
                .expect("finish")
                .delivered()
                .await
                .expect("delivered");
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

/// What the two DATA keys of decision 0001 will cost at a high message rate.
///
/// Neither key exists on the wire yet, so each is simulated by the key with its
/// exact wire shape: `content_len` is a `uint` key with a `uint` value, the
/// shape of the sequence number, and a fingerprint-shaped `content_type` is a
/// `uint` key with a 71-byte `tstr`, the shape of the producer identity of
/// decision 0008. The two endpoint paths are the same length, so the only
/// difference between the two measurements is the two keys.
fn bench_header_cost(c: &mut Criterion) {
    /// Small enough that the header is a visible fraction of the message.
    const SMALL: usize = 64;
    /// A five-byte CBOR `uint`: what a long-lived producer's counter reaches.
    const SEQUENCE: u64 = 1 << 20;
    /// `sha256:` plus 64 hex digits — the producer name of decision 0008.
    const PRODUCER: &str =
        "sha256:22ed30a800000000000000000000000000000000000000000000000000009f25";
    assert_eq!(PRODUCER.len(), 71, "the producer key is a 71-byte tstr");

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());

    let (lean_pusher, rich_pusher) = rt.block_on(async {
        drain(harness.listener.puller("/keys0").expect("puller keys0"));
        drain(harness.listener.puller("/keys2").expect("puller keys2"));

        let client = harness.client();
        let lean = client.pusher(harness.trust());
        lean.connect(&harness.url("/keys0"))
            .await
            .expect("connect keys0");
        let rich = client.pusher(harness.trust());
        rich.connect(&harness.url("/keys2"))
            .await
            .expect("connect keys2");
        (lean, rich)
    });

    let payload = vec![0x61u8; SMALL];
    let lean_meta = TransferMeta::default();
    let rich_meta = TransferMeta::default()
        .with_content_len(SEQUENCE)
        .with_content_type(PRODUCER);

    // Criterion reports messages per second; the bytes per message are
    // deterministic, so they are computed rather than measured.
    let lean_bytes = wire_bytes("/keys0", &lean_meta, SMALL);
    let rich_bytes = wire_bytes("/keys2", &rich_meta, SMALL);
    eprintln!(
        "header cost: {lean_bytes} B/message minimal, {rich_bytes} B/message with two extra keys \
         (+{} B, +{:.1} % over a {SMALL}-byte payload)",
        rich_bytes - lean_bytes,
        (rich_bytes - lean_bytes) as f64 * 100.0 / lean_bytes as f64,
    );

    let mut group = c.benchmark_group("header");
    group.throughput(Throughput::Elements(1));

    for (name, meta, pusher) in [
        ("push_64b_keys0", &lean_meta, &lean_pusher),
        ("push_64b_keys2", &rich_meta, &rich_pusher),
    ] {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                let mut transfer = pusher.open(meta.clone()).await.expect("open");
                transfer
                    .write_all(black_box(&payload))
                    .await
                    .expect("write");
                // Best effort: the receipt is dropped, so this is the send path
                // and nothing else.
                transfer.finish().expect("finish");
            })
        });
    }
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

/// Bytes one DATA frame puts on the wire: preamble, CBOR header, payload.
///
/// Rebuilds the header the runtime writes for `meta` (`crates/weida/src/transfer.rs`,
/// `data_header`): the endpoint, whatever `meta` carries, and a generated
/// `traceparent`.
fn wire_bytes(endpoint: &str, meta: &TransferMeta, payload: usize) -> usize {
    let header = DataHeader {
        endpoint: Some(endpoint.to_owned()),
        content_len: meta.content_len,
        content_type: meta.content_type.clone(),
        // Any W3C traceparent: the field is fixed-width, 55 bytes.
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned()),
        tracestate: None,
        topic: None,
    };
    encode_frame(FrameKind::Data, &header.encode()).len() + payload
}

criterion_group!(benches, bench_push, bench_fanout, bench_header_cost);
criterion_main!(benches);
