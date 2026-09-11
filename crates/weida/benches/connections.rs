//! What a connection to a peer costs: handshake latency and memory.
//!
//! Decision 0002 gives each peer pair a control connection and one bulk
//! connection per dialled path, so the load-bearing question is what the
//! *second* connection to a peer that is already connected costs. Two figures
//! answer it: the latency of a cold handshake against a pooled dial that does
//! none, and the resident-memory delta per established connection.
//!
//! The pool keys on `(host, port, ClientTls, address fingerprint)`, so a second
//! connection to the same server on the same terms cannot be dialled from one
//! runtime today — which is exactly why 0002 needs a second pool tier. The
//! memory figures therefore use one client runtime per connection, all against
//! one server, and both ends live in this process: the delta covers the client
//! and server state of each connection together, which is what a two-tier pool
//! would pay for on a loopback deployment.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use weida::{Identity, Listener, Puller, Runtime, RuntimeConfig, Trust};

/// Connection counts the report covers: one, the second one 0002 adds, and a
/// per-path fan of 64.
const COUNTS: [usize; 3] = [1, 2, 64];

struct Harness {
    runtime: Runtime,
    trust: Trust,
    addr: SocketAddr,
    /// Held only so the endpoint namespace outlives the measurements.
    _listener: Listener,
    _binding: weida::Binding,
}

impl Harness {
    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }
}

/// A server with a puller that drains, so a connected client is never stalled.
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
    drain(listener.puller("/sink").expect("puller"));

    Harness {
        runtime,
        trust,
        addr,
        _listener: listener,
        _binding: binding,
    }
}

fn drain(puller: Puller) {
    tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(64 * 1024).await;
        }
    });
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Resident set size of this process in bytes, on Linux.
///
/// `VmRSS` rather than `VmHWM`: the interesting quantity is what a set of live
/// connections holds *now*, which a high-water mark cannot express once
/// something has been freed.
fn current_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

fn bench_connect(c: &mut Criterion) {
    let rt = tokio_runtime();
    let harness = rt.block_on(harness());
    report_connection_cost(&rt, &harness);

    let url = harness.url("/sink");
    let mut group = c.benchmark_group("connect");
    // A handshake is milliseconds of certificate work, not microseconds of
    // codec: ten samples keep the run honest without keeping it long.
    group.sample_size(20);

    // A cold connection: a fresh runtime per iteration, with only `connect`
    // timed. This is what the second connection of 0002 would cost if it is a
    // real handshake.
    group.bench_function("cold_handshake", |b| {
        let trust = harness.trust.clone();
        let url = url.clone();
        b.to_async(&rt).iter_custom(move |iters| {
            let trust = trust.clone();
            let url = url.clone();
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
                    let pusher = client.pusher(trust.clone());
                    let start = Instant::now();
                    pusher.connect(&url).await.expect("connect");
                    total += start.elapsed();
                    client.shutdown().await;
                }
                total
            }
        })
    });

    // A pooled dial on the same terms: the pool answers and no handshake
    // happens. This is today's cost of "a second connection to the same peer",
    // and it is not a connection at all.
    group.bench_function("pooled_dial", |b| {
        let trust = harness.trust.clone();
        let url = url.clone();
        b.to_async(&rt).iter_custom(move |iters| {
            let trust = trust.clone();
            let url = url.clone();
            async move {
                let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
                let warm = client.pusher(trust.clone());
                warm.connect(&url).await.expect("first connect");

                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    warm.connect(&url).await.expect("pooled connect");
                    total += start.elapsed();
                }
                client.shutdown().await;
                total
            }
        })
    });
    group.finish();

    rt.block_on(async { harness.runtime.clone().shutdown().await });
}

/// Prints handshake time and resident memory for 1, 2 and 64 connections.
///
/// Criterion measures one operation repeatedly; this measures a *set* of live
/// connections, which is a different question and has no criterion shape.
///
/// The runtimes are built first and measured before anything is dialled,
/// because one client runtime per connection is the only way to hold two
/// connections to one peer today (§ module docs) and a runtime carries its own
/// quinn endpoint and UDP socket. Reporting the two deltas separately keeps
/// that overhead out of the per-connection figure 0002 actually needs.
fn report_connection_cost(rt: &tokio::runtime::Runtime, harness: &Harness) {
    let url = harness.url("/sink");

    for n in COUNTS {
        let baseline = current_rss();
        let report = rt.block_on(async {
            let mut clients = Vec::with_capacity(n);
            for _ in 0..n {
                let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
                let pusher = client.pusher(harness.trust.clone());
                clients.push((client, pusher));
            }
            let idle = current_rss();

            let start = Instant::now();
            let mut first = Duration::ZERO;
            let mut last = Duration::ZERO;
            for (index, (_, pusher)) in clients.iter().enumerate() {
                let one = Instant::now();
                pusher.connect(&url).await.expect("connect");
                let one = one.elapsed();
                if index == 0 {
                    first = one;
                }
                last = one;
            }
            let elapsed = start.elapsed();
            // Read while every connection is still live and held.
            let connected = current_rss();

            for (client, pusher) in clients {
                drop(pusher);
                client.shutdown().await;
            }
            (elapsed, first, last, idle, connected)
        });
        let (elapsed, first, last, idle, connected) = report;

        let memory = match (baseline, idle, connected) {
            (Some(before), Some(idle), Some(after)) => {
                let runtimes = idle.saturating_sub(before);
                let connections = after.saturating_sub(idle);
                format!(
                    "RSS {:.2} MiB -> {:.2} MiB; {:.1} KiB per idle runtime, \
                     {:.1} KiB per connection",
                    before as f64 / (1024.0 * 1024.0),
                    after as f64 / (1024.0 * 1024.0),
                    runtimes as f64 / 1024.0 / n as f64,
                    connections as f64 / 1024.0 / n as f64,
                )
            }
            _ => "RSS unavailable on this platform".to_owned(),
        };
        eprintln!(
            "connections n={n}: {elapsed:.2?} for {n} handshakes, first {first:.2?}, \
             last {last:.2?}, {memory}"
        );
    }
}

criterion_group!(benches, bench_connect);
criterion_main!(benches);
