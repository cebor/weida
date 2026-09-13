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

// --- admission stops first (0009 §4.5) -----------------------------------

/// A runtime that is a server *and* a stalled sender: it holds a binding with
/// two registered pullers, and one finished transfer to a frozen peer whose
/// receipt can never settle.
///
/// The stalled receipt is the point. A drain with nothing outstanding returns
/// at once, so there would be no window in which to observe a refusal; this
/// one runs for its whole deadline, and that deadline is the window.
struct Draining {
    runtime: Runtime,
    /// `weida://<fingerprint>@127.0.0.1:<port>`, without a path.
    url: String,
    peer: std::thread::JoinHandle<()>,
    _pullers: Vec<weida::Puller>,
    _binding: weida::Binding,
}

impl Draining {
    async fn start() -> Draining {
        let frozen_identity = Identity::generate().expect("frozen identity");
        let frozen_fp = frozen_identity.fingerprint().expect("fingerprint");
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (freeze_tx, freeze_rx) = std::sync::mpsc::channel::<()>();

        // The peer that will never acknowledge anything again: its own
        // single-threaded reactor, blocked outright, so the socket stays open
        // and no packet is answered.
        let peer = std::thread::spawn(move || {
            let reactor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("peer reactor");
            reactor.block_on(async move {
                let runtime = Runtime::new(RuntimeConfig::default()).expect("peer runtime");
                let listener = runtime.listener();
                let binding = listener
                    .bind_quic("127.0.0.1:0".parse().expect("loopback"), frozen_identity)
                    .await
                    .expect("bind");
                let _puller = listener.puller("/frozen").expect("puller");
                addr_tx
                    .send(binding.local_addr())
                    .expect("hand over the port");
                let _ = tokio::task::spawn_blocking(move || freeze_rx.recv()).await;
                std::thread::sleep(Duration::from_secs(5));
            });
        });

        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
            .await
            .expect("bind");
        // Held for the test: dropping a puller unregisters its path.
        let pullers = vec![
            listener.puller("/early").expect("puller"),
            listener.puller("/late").expect("puller"),
        ];
        let url = format!(
            "weida://{}@127.0.0.1:{}",
            fingerprint,
            binding.local_addr().port()
        );

        // One transfer to the frozen peer, finished, its receipt dropped: the
        // drain is the only thing left that can wait for it, and it will wait
        // in vain.
        let frozen_addr = addr_rx.recv().expect("peer port");
        let pusher = runtime.pusher(Trust::pin(frozen_fp));
        let frozen_url = format!("weida://127.0.0.1:{}/frozen", frozen_addr.port());
        within(pusher.connect(&frozen_url)).await.expect("connect");
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(b"nobody will confirm this"))
            .await
            .expect("write");
        freeze_tx.send(()).expect("freeze the peer");
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(transfer.finish().expect("finish"));
        drop(pusher);

        Draining {
            runtime,
            url,
            peer,
            _pullers: pullers,
            _binding: binding,
        }
    }

    /// Starts the drain and returns once its admission flag is certainly set.
    ///
    /// `Runtime::drain` stops admission before its first await, so polling
    /// the future **once** here is the ordering: after it, a dial or a stream
    /// is late by construction, and no timer has to be outrun. The rest of
    /// the drain runs on a task, as a caller's would.
    async fn begin(&self, deadline: Duration) -> tokio::task::JoinHandle<weida::Drained> {
        let runtime = self.runtime.clone();
        let mut drain = Box::pin(runtime.drain(deadline));
        let first =
            std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "a drain with a stalled receipt outstanding cannot finish on its first poll"
        );
        tokio::spawn(drain)
    }
}

/// Claim: a draining binding refuses a **new connection** rather than
/// accepting it and then closing it ([0009](../../../docs/decisions/0009-drain.md)
/// §4.5). Admission stops first, and it stops for connections and not only
/// for streams.
///
/// The control is in the same test: the same client dials one path before the
/// drain and another during it. One connection per dialled path means the
/// second dial is a real handshake against the binding rather than a reuse of
/// the first.
#[tokio::test]
async fn a_draining_binding_refuses_a_new_connection() {
    let server = Draining::start().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher = client.pusher(Trust::by_address());

    within(pusher.connect(&format!("{}/early", server.url)))
        .await
        .expect("a dial before the drain must be accepted");

    let drain = server.begin(Duration::from_secs(2)).await;

    let err = within(pusher.connect(&format!("{}/late", server.url)))
        .await
        .expect_err("a dial during the drain must be refused");
    assert!(
        !drain.is_finished(),
        "the refusal must have been observed while the drain was still running"
    );
    // What the refusal is *not*: a completed handshake that is then closed.
    // quinn refuses the attempt outright, so the dialling side never has a
    // connection at all.
    assert_eq!(
        pusher.peer_count(),
        1,
        "only the early path connected: {err:?}"
    );

    let drained = within(drain).await.expect("drain task");
    assert_eq!(drained.outstanding, 1, "{drained:?}");
    client.shutdown().await;
    server.peer.join().expect("peer thread");
}

/// Claim: while a drain waits, a **new stream on a connection that already
/// exists** is refused with `SHUTDOWN`, and the sender learns it as a refusal
/// rather than as an unexplained stop code
/// ([0009](../../../docs/decisions/0009-drain.md) §4.5).
///
/// The receipt is what carries the answer: `send` does not await one, so the
/// transfer is opened, written and finished by hand and the refusal is read
/// off `Delivery::delivered`.
#[tokio::test]
async fn a_draining_runtime_refuses_a_late_stream_on_an_open_connection() {
    let server = Draining::start().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher = client.pusher(Trust::by_address());
    within(pusher.connect(&format!("{}/early", server.url)))
        .await
        .expect("connect");

    // A transfer before the drain is served, so the refusal below is the
    // drain's answer and not a broken setup.
    let mut accepted = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(accepted.write_all(b"in time")).await.expect("write");
    within(accepted.finish().expect("finish").delivered())
        .await
        .expect("a transfer before the drain is acknowledged");

    let drain = server.begin(Duration::from_secs(2)).await;

    let mut late = within(pusher.open(TransferMeta::default()))
        .await
        .expect("the stream budget is local, so opening still succeeds");
    let _ = late.write_all(b"too late").await;
    let err = match late.finish() {
        Ok(receipt) => within(receipt.delivered())
            .await
            .expect_err("a late transfer must be refused"),
        Err(e) => e,
    };
    assert!(
        matches!(err, weida::Error::Rejected),
        "STOP_SENDING(SHUTDOWN) must reach the sender as a refusal, got {err:?}"
    );
    assert!(!drain.is_finished(), "the drain was still running");

    let drained = within(drain).await.expect("drain task");
    assert_eq!(drained.outstanding, 1, "{drained:?}");
    client.shutdown().await;
    server.peer.join().expect("peer thread");
}
