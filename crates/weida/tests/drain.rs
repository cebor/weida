//! `Runtime::drain`: the bounded counterpart of the abortive shutdown.
//!
//! The difference only shows where the transport cannot acknowledge a finished
//! transfer straight away, so every test here makes the receive window smaller
//! than the payload: the bytes then wait on the peer's *application* reading,
//! which is exactly the window in which `shutdown` would cut the transfer
//! short and `drain` gives it its chance
//! (`docs/decisions/0009-drain.md` §4.1, §4.2).

mod common;

use std::time::Duration;

use common::Server;
use weida::{Identity, Limits, Runtime, RuntimeConfig, TransferMeta, Trust};

const DEADLINE: Duration = Duration::from_secs(15);
/// Bigger than both windows below, so the last bytes cannot be acknowledged
/// until the receiving application reads.
const PAYLOAD: usize = 512 * 1024;

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A client whose transport cannot get a large payload acknowledged on its
/// own: small windows, so the peer's reads are what unblock it.
fn narrow() -> RuntimeConfig {
    RuntimeConfig {
        limits: Limits {
            stream_receive_window: 64 * 1024,
            connection_receive_window: 128 * 1024,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    }
}

#[tokio::test]
async fn a_finished_transfer_that_shutdown_cuts_short_arrives_under_drain() {
    let server = Server::start_with_config(narrow()).await;
    let puller = server.listener.puller("/slow").expect("puller");

    // The reader starts late on purpose: the payload is still unacknowledged
    // when the client asks to stop.
    let reader = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(PAYLOAD).await
    });

    let client = server.client_runtime_with_config(narrow());
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/slow")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(&vec![7u8; PAYLOAD]))
        .await
        .expect("write");
    // Fire and forget: the receipt is dropped, which is what leaves the drain
    // as the only thing that can wait for it.
    drop(transfer.finish().expect("finish"));
    drop(pusher);

    let drained = within(client.drain(Duration::from_secs(5))).await;
    assert_eq!(
        drained.outstanding, 0,
        "the drain must have waited for the transfer: {drained:?}"
    );
    assert_eq!(drained.delivered, 1);

    let body = within(reader).await.expect("reader task").expect("collect");
    assert_eq!(body.len(), PAYLOAD, "the whole payload must have arrived");
    assert!(body.iter().all(|byte| *byte == 7));
}

#[tokio::test]
async fn the_same_transfer_is_cut_short_by_shutdown() {
    let server = Server::start_with_config(narrow()).await;
    let puller = server.listener.puller("/slow").expect("puller");

    let reader = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(PAYLOAD).await
    });

    let client = server.client_runtime_with_config(narrow());
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/slow")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(&vec![7u8; PAYLOAD]))
        .await
        .expect("write");
    drop(transfer.finish().expect("finish"));
    drop(pusher);

    // The abortive close: the unacknowledged tail is reset, so the reader
    // never sees the whole payload. This is the behaviour `drain` exists to
    // offer an alternative to, asserted so that the pair cannot drift.
    within(client.shutdown()).await;

    let body = within(reader).await.expect("reader task");
    if let Ok(bytes) = body {
        assert!(
            bytes.len() < PAYLOAD,
            "shutdown must not deliver the whole payload, got {} bytes",
            bytes.len()
        );
    }
}

/// Claim: a drain that cannot finish returns at its deadline with a non-zero
/// outstanding count, and that is not an error.
///
/// The peer here is frozen rather than merely slow, and the distinction is
/// the interesting part: QUIC acknowledges bytes into the receive window
/// whether or not the application reads them, so a slow *reader* still
/// settles a finished transfer. What leaves one outstanding is an
/// acknowledgement that never comes. The peer is therefore given its own
/// single-threaded reactor, and that thread is blocked outright: the socket
/// stays open, the connection stays up, and nothing is read or acknowledged.
#[tokio::test]
async fn a_drain_against_a_peer_that_reads_nothing_expires_with_a_count() {
    /// Long enough to cover the drain below with room to spare.
    const FREEZE: Duration = Duration::from_secs(2);

    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (freeze_tx, freeze_rx) = std::sync::mpsc::channel::<()>();

    let peer = std::thread::spawn(move || {
        let reactor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("peer reactor");
        reactor.block_on(async move {
            let runtime = Runtime::new(narrow()).expect("peer runtime");
            let listener = runtime.listener();
            let binding = listener
                .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
                .await
                .expect("bind");
            let _puller = listener.puller("/frozen").expect("puller");
            addr_tx
                .send(binding.local_addr())
                .expect("hand over the port");
            // Waiting off-reactor, so the peer still serves until the signal.
            let _ = tokio::task::spawn_blocking(move || freeze_rx.recv()).await;
            // And then it serves nothing at all.
            std::thread::sleep(FREEZE);
        });
    });

    let addr = addr_rx.recv().expect("peer port");
    let url = format!("weida://127.0.0.1:{}/frozen", addr.port());
    let client = Runtime::new(narrow()).expect("client runtime");
    let pusher = client.pusher(Trust::pin(fingerprint));
    within(pusher.connect(&url)).await.expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"a payload nobody will confirm"))
        .await
        .expect("write");

    freeze_tx.send(()).expect("freeze the peer");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The FIN goes into the silence, and nobody holds its receipt.
    drop(transfer.finish().expect("finish"));
    drop(pusher);

    // The smallest deadline that demonstrates expiry: long enough not to be
    // a scheduling race, short enough to cost a fraction of a second.
    let started = std::time::Instant::now();
    let drained = within(client.drain(Duration::from_millis(200))).await;
    assert_eq!(
        drained.outstanding, 1,
        "the unacknowledged transfer must be counted: {drained:?}"
    );
    assert_eq!(drained.delivered, 0);
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the drain must have waited for its deadline"
    );
    peer.join().expect("peer thread");
}
