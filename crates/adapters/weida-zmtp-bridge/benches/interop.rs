//! What the ZMTP bridge costs, against the same work with no bridge in it.
//!
//! `docs/adapters/zmtp.md` §10 item 6: round-trip latency and messages per
//! second for REQ/REP and PUSH/PULL through the adapter, against a direct
//! `zeromq`-to-`zeromq` pair on loopback. The direct pair is the control, and
//! it is the whole point of the comparison — a number for the bridge alone
//! says nothing, because it includes TCP on one side, QUIC on the other and a
//! process boundary neither library has.
//!
//! What each row measures:
//!
//! - `direct` — zmq.rs REQ to zmq.rs REP, one process, loopback TCP. No weida.
//! - `bridged` — zmq.rs REQ to the inbound bridge to a weida `Replier`. One
//!   TCP hop, one QUIC hop, two protocol terminations.
//!
//! Both are one process on one machine, so the difference is the bridge and the
//! second transport rather than the network. The `max_message_bytes` question
//! of §11 is answered with the payload sweep: a run at 1 KiB and one at 1 MiB
//! show whether the cap's default is anywhere near the working range.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use weida::{
    ClientTls, Identity, Listener, Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust,
};
use weida_zmtp_bridge::{Inbound, InboundConfig, Presenting};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

/// The payload sizes the sweep uses: one small enough to be pure overhead, one
/// large enough for the copies to matter.
const SIZES: [usize; 2] = [1024, 1024 * 1024];

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// A loopback address with a port the OS picks.
async fn free_port() -> (SocketAddr, String) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);
    (addr, format!("tcp://{addr}"))
}

/// A weida server with a `Replier` that echoes, and a `Puller` that drains.
struct WeidaSide {
    runtime: Runtime,
    _listener: Listener,
    _binding: weida::Binding,
    url_base: String,
}

async fn weida_side() -> WeidaSide {
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity),
        )
        .await
        .expect("bind");
    let url_base = format!(
        "weida://{}@127.0.0.1:{}",
        fingerprint,
        binding.local_addr().port()
    );

    // The echo responder: `take_body` so the reply can be written while the
    // request is still being read, which is what a real replier does.
    let replier = listener.replier("/echo").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = match request.take_body().collect(8 * 1024 * 1024).await {
                Ok(body) => body,
                Err(_) => continue,
            };
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                continue;
            };
            if reply.write_all(&body).await.is_ok() {
                let _ = reply.finish();
            }
        }
    });

    let puller = listener.puller("/sink").expect("puller");
    tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(8 * 1024 * 1024).await;
        }
    });

    WeidaSide {
        runtime,
        _listener: listener,
        _binding: binding,
        url_base,
    }
}

/// Starts an inbound bridge serving in the background.
async fn bridge(weida_url: String, presenting: Presenting) -> SocketAddr {
    let bridge = Inbound::bind(
        InboundConfig::new(
            "127.0.0.1:0".parse().expect("loopback"),
            weida_url,
            presenting,
        ),
        ClientTls::new(Trust::by_address()),
    )
    .await
    .expect("bind the bridge");
    let addr = bridge.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = bridge.serve().await;
    });
    addr
}

/// A zmq.rs REP socket that echoes whatever it is sent, on its own task.
async fn zmq_echo(endpoint: &str) {
    let mut rep = zeromq::RepSocket::new();
    rep.bind(endpoint).await.expect("bind");
    tokio::spawn(async move {
        while let Ok(message) = rep.recv().await {
            if rep.send(message).await.is_err() {
                break;
            }
        }
    });
}

/// A zmq.rs PULL socket that drains forever, on its own task.
async fn zmq_drain(endpoint: &str) {
    let mut pull = zeromq::PullSocket::new();
    pull.bind(endpoint).await.expect("bind");
    tokio::spawn(async move { while pull.recv().await.is_ok() {} });
}

/// REQ/REP round-trip latency: a real ZeroMQ requester, with and without the
/// bridge behind it.
fn bench_req_rep(c: &mut Criterion) {
    let rt = tokio_runtime();

    let (weida, direct_req, bridged_req) = rt.block_on(async {
        let weida = weida_side().await;

        let (_, endpoint) = free_port().await;
        zmq_echo(&endpoint).await;
        let mut direct_req = zeromq::ReqSocket::new();
        direct_req.connect(&endpoint).await.expect("connect direct");

        let bridge_addr = bridge(format!("{}/echo", weida.url_base), Presenting::Rep).await;
        let mut bridged_req = zeromq::ReqSocket::new();
        bridged_req
            .connect(&format!("tcp://{bridge_addr}"))
            .await
            .expect("connect bridged");

        // One exchange each before measuring: the bridge dials weida lazily on
        // its first message, and a TLS handshake in the first sample would be
        // the only thing this bench could see.
        for req in [&mut direct_req, &mut bridged_req] {
            req.send(ZmqMessage::from("warm")).await.expect("warm send");
            req.recv().await.expect("warm recv");
        }
        (
            weida,
            Arc::new(Mutex::new(direct_req)),
            Arc::new(Mutex::new(bridged_req)),
        )
    });

    let mut group = c.benchmark_group("interop_req_rep");
    // Latency, not throughput: one round trip per iteration.
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for size in SIZES {
        let payload = ZmqMessage::from(vec![0x61u8; size]);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(format!("direct_{size}"), |b| {
            b.to_async(&rt).iter(|| {
                let socket = Arc::clone(&direct_req);
                let payload = payload.clone();
                async move {
                    let mut socket = socket.lock().await;
                    socket.send(black_box(payload)).await.expect("send");
                    socket.recv().await.expect("recv");
                }
            })
        });

        group.bench_function(format!("bridged_{size}"), |b| {
            b.to_async(&rt).iter(|| {
                let socket = Arc::clone(&bridged_req);
                let payload = payload.clone();
                async move {
                    let mut socket = socket.lock().await;
                    socket.send(black_box(payload)).await.expect("send");
                    socket.recv().await.expect("recv");
                }
            })
        });
    }
    group.finish();

    rt.block_on(async { weida.runtime.clone().shutdown().await });
}

/// PUSH/PULL messages per second: one-way, so this is the send path and the
/// bridge's forwarding loop rather than a round trip.
fn bench_push_pull(c: &mut Criterion) {
    let rt = tokio_runtime();

    let (weida, direct_push, bridged_push) = rt.block_on(async {
        let weida = weida_side().await;

        let (_, endpoint) = free_port().await;
        zmq_drain(&endpoint).await;
        let mut direct_push = zeromq::PushSocket::new();
        direct_push
            .connect(&endpoint)
            .await
            .expect("connect direct");

        let bridge_addr = bridge(format!("{}/sink", weida.url_base), Presenting::Pull).await;
        let mut bridged_push = zeromq::PushSocket::new();
        bridged_push
            .connect(&format!("tcp://{bridge_addr}"))
            .await
            .expect("connect bridged");

        for push in [&mut direct_push, &mut bridged_push] {
            push.send(ZmqMessage::from("warm")).await.expect("warm");
        }
        (
            weida,
            Arc::new(Mutex::new(direct_push)),
            Arc::new(Mutex::new(bridged_push)),
        )
    });

    let mut group = c.benchmark_group("interop_push_pull");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for size in SIZES {
        let payload = ZmqMessage::from(vec![0x61u8; size]);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(format!("direct_{size}"), |b| {
            b.to_async(&rt).iter(|| {
                let socket = Arc::clone(&direct_push);
                let payload = payload.clone();
                async move {
                    socket
                        .lock()
                        .await
                        .send(black_box(payload))
                        .await
                        .expect("send");
                }
            })
        });

        group.bench_function(format!("bridged_{size}"), |b| {
            b.to_async(&rt).iter(|| {
                let socket = Arc::clone(&bridged_push);
                let payload = payload.clone();
                async move {
                    socket
                        .lock()
                        .await
                        .send(black_box(payload))
                        .await
                        .expect("send");
                }
            })
        });
    }
    group.finish();

    rt.block_on(async { weida.runtime.clone().shutdown().await });
}

criterion_group!(benches, bench_req_rep, bench_push_pull);
criterion_main!(benches);
