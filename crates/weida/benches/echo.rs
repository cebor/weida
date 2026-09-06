//! End-to-end Req/Rep benchmarks over loopback QUIC.
//!
//! Two profiles, matching master doc §69: a small round trip where latency
//! dominates, and a large streaming transfer where throughput does. The server
//! is built once per group so the measured work is the transfer, not TLS
//! handshakes.

use std::net::SocketAddr;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use tokio::io::AsyncReadExt;
use weida::{Identity, Limits, Listener, Runtime, RuntimeConfig, TransferMeta, Trust};

const CHUNK: usize = 64 * 1024;

struct Harness {
    runtime: Runtime,
    client: Runtime,
    trust: Trust,
    addr: SocketAddr,
    _listener: Listener,
    _binding: weida::Binding,
}

/// Starts an echo server and a client runtime that pins it.
///
/// The identity stays in memory: `criterion` exits the process when the run
/// ends, so a `Drop` that removed a temporary directory would not fire and the
/// private key would outlive the benchmark.
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

    let replier = listener.replier("/echo").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            tokio::spawn(async move {
                let mut body = request.take_body();
                let mut out = match request.reply(TransferMeta::default()).await {
                    Ok(out) => out,
                    Err(_) => return,
                };
                let mut chunk = vec![0u8; CHUNK];
                loop {
                    let n = match body.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if out.write_all(&chunk[..n]).await.is_err() {
                        return;
                    }
                }
                let _ = out.finish();
            });
        }
    });

    let client = Runtime::new(RuntimeConfig {
        limits: Limits::default(),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");

    Harness {
        runtime,
        client,
        trust,
        addr,
        _listener: listener,
        _binding: binding,
    }
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn bench_small_rtt(c: &mut Criterion) {
    let rt = tokio_runtime();
    let harness = rt.block_on(harness());
    let url = format!("weida://127.0.0.1:{}/echo", harness.addr.port());

    let requester = rt.block_on(async {
        let requester = harness.client.requester(harness.trust.clone());
        requester.connect(&url).await.expect("connect");
        requester
    });

    let payload = vec![0x61u8; 1024];
    let mut group = c.benchmark_group("echo");
    group.throughput(Throughput::Bytes(payload.len() as u64));

    group.bench_function("echo_1kib_rtt", |b| {
        b.to_async(&rt).iter(|| async {
            let reply = requester
                .request(black_box(&payload))
                .await
                .expect("request");
            let body = reply.collect(4096).await.expect("collect");
            debug_assert_eq!(body.len(), 1024);
            black_box(body.len())
        })
    });

    // The same echo through the raw handles, so the cost of `request`'s
    // convenience layer is visible against it.
    group.bench_function("echo_1kib_rtt_explicit", |b| {
        b.to_async(&rt).iter(|| async {
            let (mut transfer, reply) =
                requester.open(TransferMeta::default()).await.expect("open");
            transfer
                .write_all(black_box(&payload))
                .await
                .expect("write");
            transfer.finish().expect("finish");
            let body = reply
                .recv()
                .await
                .expect("recv")
                .collect(4096)
                .await
                .expect("collect");
            black_box(body.len())
        })
    });
    group.finish();

    drop(requester);
    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

fn bench_stream_throughput(c: &mut Criterion) {
    const TOTAL: usize = 64 * 1024 * 1024;

    let rt = tokio_runtime();
    let harness = rt.block_on(harness());
    let url = format!("weida://127.0.0.1:{}/echo", harness.addr.port());
    let requester = rt.block_on(async {
        let requester = harness.client.requester(harness.trust.clone());
        requester.connect(&url).await.expect("connect");
        requester
    });

    let payload = vec![0x7au8; CHUNK];
    let mut group = c.benchmark_group("stream");
    // Both directions cross the wire, so a full echo moves twice the payload.
    group.throughput(Throughput::Bytes(2 * TOTAL as u64));
    group.sample_size(10);

    group.bench_function("stream_throughput_64mib", |b| {
        b.to_async(&rt).iter(|| async {
            let (mut transfer, reply) = requester
                .open(TransferMeta::default().with_content_len(TOTAL as u64))
                .await
                .expect("open");

            // The echo must be drained concurrently, or both sides stall on
            // flow control.
            let reader = tokio::spawn(async move {
                let mut reply = reply.recv().await.expect("recv");
                let mut sink = vec![0u8; CHUNK];
                let mut received = 0usize;
                loop {
                    let n = reply.read(&mut sink).await.expect("read");
                    if n == 0 {
                        break;
                    }
                    received += n;
                }
                received
            });

            let mut sent = 0usize;
            while sent < TOTAL {
                transfer.write_all(&payload).await.expect("write");
                sent += payload.len();
            }
            transfer.finish().expect("finish");
            let received = reader.await.expect("reader");
            debug_assert_eq!(received, TOTAL);
            black_box(received)
        })
    });
    group.finish();

    drop(requester);
    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

criterion_group!(benches, bench_small_rtt, bench_stream_throughput);
criterion_main!(benches);
