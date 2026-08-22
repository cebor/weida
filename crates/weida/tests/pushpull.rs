//! Push/Pull over real QUIC on loopback.
//!
//! Push is the one-way transfer primitive with no reply slot: the same
//! `OutgoingTransfer` machinery as Req, minus correlation. These tests assert
//! that the outcome vocabulary, the acknowledgement modes and the round-robin
//! peer policy all carry over unchanged, and that mixing patterns on one path
//! is refused rather than reinterpreted.

mod common;

use std::time::Duration;

use common::Server;
use weida::{AckMode, AckState, Error, Outcome, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

#[tokio::test]
async fn push_pull_roundtrip_with_ack() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let payload = vec![0x5au8; 1024];
    let sender = {
        let payload = payload.clone();
        tokio::spawn(async move {
            pusher
                .send_with(
                    TransferMeta::default().with_ack(AckMode::Accepted),
                    &payload,
                )
                .await
        })
    };

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(transfer.meta().endpoint.as_deref(), Some("/jobs"));
    assert_eq!(transfer.meta().correlation_id, None);
    let body = within(transfer.collect(64 * 1024)).await.expect("collect");
    assert_eq!(body, payload);

    // The ACK is emitted when the application reaches EOF, so it can only
    // resolve after `collect` above.
    let outcome = within(sender).await.expect("task").expect("send");
    assert_eq!(outcome, Outcome::Acked(AckState::Accepted));

    client.shutdown().await;
}

#[tokio::test]
async fn push_best_effort() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // Best effort settles at FIN without waiting for the peer.
    let outcome = within(pusher.send(b"fire and forget")).await.expect("send");
    assert_eq!(outcome, Outcome::SentBestEffort);

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(transfer.meta().ack_mode, AckMode::None);
    let body = within(transfer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"fire and forget");

    client.shutdown().await;
}

#[tokio::test]
async fn push_round_robins_two_peers() {
    // One pusher, two peers, two distinct destination paths on one server.
    // A peer carries the path that was dialled on it, so which puller receives
    // a message *is* the observable output of the selection policy — the same
    // `PeerSet::pick` a requester uses (pattern primitive P3).
    let server = Server::start().await;
    let left = server.listener.puller("/left").expect("left");
    let right = server.listener.puller("/right").expect("right");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/left")))
        .await
        .expect("dial left");
    within(pusher.connect(&server.url("/right")))
        .await
        .expect("dial right");
    assert_eq!(pusher.peer_count(), 2);

    const TOTAL: usize = 8;
    for i in 0..TOTAL {
        within(pusher.send(format!("m{i}").as_bytes()))
            .await
            .expect("send");
    }

    // Round-robin over two peers splits an even count exactly in half. If
    // selection had collapsed onto one peer, the second `recv` below would
    // block until the deadline.
    let mut seen = Vec::new();
    for _ in 0..TOTAL / 2 {
        let t = within(left.recv()).await.expect("recv left");
        assert_eq!(t.meta().endpoint.as_deref(), Some("/left"));
        seen.push(within(t.collect(64)).await.expect("collect left"));
    }
    for _ in 0..TOTAL / 2 {
        let t = within(right.recv()).await.expect("recv right");
        assert_eq!(t.meta().endpoint.as_deref(), Some("/right"));
        seen.push(within(t.collect(64)).await.expect("collect right"));
    }

    // Every message arrived exactly once, and nowhere else.
    seen.sort();
    let mut expected: Vec<Vec<u8>> = (0..TOTAL).map(|i| format!("m{i}").into_bytes()).collect();
    expected.sort();
    assert_eq!(seen, expected);

    client.shutdown().await;
}

#[tokio::test]
async fn push_cancel_mid_transfer() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"partial")).await.expect("write");

    let mut inbound = within(puller.recv()).await.expect("recv");
    transfer.cancel();

    // The reset surfaces as `Canceled` on the reading side, not as EOF: a
    // canceled push must never look like a complete one.
    let err = within(inbound.read_capped(64 * 1024))
        .await
        .expect_err("a canceled transfer must not read as complete");
    assert!(matches!(err, Error::Canceled), "{err:?}");

    // The connection survives: the next push is served normally.
    let outcome = within(pusher.send(b"next")).await.expect("send");
    assert_eq!(outcome, Outcome::SentBestEffort);
    let next = within(puller.recv()).await.expect("recv next");
    assert_eq!(within(next.collect(64)).await.expect("collect"), b"next");

    client.shutdown().await;
}

#[tokio::test]
async fn request_to_pull_path_is_unsupported() {
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // The path exists but serves oneshots. Refusing beats guessing.
    let err = within(requester.request(b"hello"))
        .await
        .expect_err("a request to a pull path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    client.shutdown().await;
}

#[tokio::test]
async fn push_to_rep_path_is_unsupported() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/rpc").expect("replier");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/rpc")))
        .await
        .expect("connect");

    let err = within(pusher.send_with(
        TransferMeta::default().with_ack(AckMode::Accepted),
        b"hello",
    ))
    .await
    .expect_err("a push to a replier path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    client.shutdown().await;
}

#[tokio::test]
async fn push_to_an_unknown_path_is_reported() {
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/nope")))
        .await
        .expect("connect");

    let err = within(pusher.send_with(
        TransferMeta::default().with_ack(AckMode::Accepted),
        b"hello",
    ))
    .await
    .expect_err("an unknown path must be reported");
    assert!(matches!(err, Error::UnknownEndpoint), "{err:?}");

    client.shutdown().await;
}
