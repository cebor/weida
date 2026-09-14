//! PAIR: one connection, one peer, both directions.
//!
//! The pattern [ARCHITECTURE.md](../../../docs/ARCHITECTURE.md) §6b maps and
//! nobody had built. It is the smallest of the three because it is
//! architecturally identical to what already exists — which is exactly what
//! makes it worth a test file: if PAIR needed one byte of new wire
//! vocabulary, the claim that the pattern layer is API and nothing else would
//! be false. `a_pair_talks_to_a_bare_peer_and_acceptor_on_the_same_path` is
//! that claim, tested.

mod common;

use std::time::Duration;

use common::Server;
use weida::{Error, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

#[tokio::test]
async fn both_directions_carry_transfers_concurrently() {
    let server = Server::start().await;
    let bound = server.listener.pair("/link").expect("bound pair");

    let client = server.client_runtime();
    let dialling = client.pair(server.trust());
    within(dialling.connect(&server.url("/link")))
        .await
        .expect("connect");
    assert_eq!(dialling.peer_count(), 1);

    // Dialling side first: a bound pair learns which connection its peer is
    // on when that peer speaks, so this is the order PAIR imposes.
    within(dialling.send(b"from the dialler"))
        .await
        .expect("send");
    let inbound = within(bound.recv()).await.expect("recv");
    assert_eq!(inbound.meta().endpoint.as_deref(), Some("/link"));
    assert_eq!(
        within(inbound.collect(1024)).await.expect("collect"),
        b"from the dialler"
    );

    // And back, on the same connection, with no reply half involved.
    within(bound.send(b"from the bound side"))
        .await
        .expect("send back");
    let answer = within(dialling.recv()).await.expect("recv");
    assert_eq!(
        within(answer.collect(1024)).await.expect("collect"),
        b"from the bound side"
    );

    // Concurrently in both directions: two transfers in flight at once, which
    // is the property that distinguishes PAIR from Req/Rep.
    let up = within(dialling.open(TransferMeta::default()))
        .await
        .expect("open up");
    let down = within(bound.open(TransferMeta::default()))
        .await
        .expect("open down");
    let (mut up, mut down) = (up, down);
    within(up.write_all(b"up")).await.expect("write up");
    within(down.write_all(b"down")).await.expect("write down");
    up.finish().expect("finish up");
    down.finish().expect("finish down");
    assert_eq!(
        within(within(bound.recv()).await.expect("recv up").collect(64))
            .await
            .expect("collect"),
        b"up"
    );
    assert_eq!(
        within(
            within(dialling.recv())
                .await
                .expect("recv down")
                .collect(64)
        )
        .await
        .expect("collect"),
        b"down"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn a_second_connection_is_refused_and_the_first_keeps_working() {
    let server = Server::start().await;
    let bound = server.listener.pair("/link").expect("bound pair");

    let first_client = server.client_runtime();
    let first = first_client.pair(server.trust());
    within(first.connect(&server.url("/link")))
        .await
        .expect("connect");
    within(first.send(b"mine")).await.expect("send");
    assert_eq!(
        within(within(bound.recv()).await.expect("recv").collect(64))
            .await
            .expect("collect"),
        b"mine"
    );

    // A second peer. Its transfer is refused with `LIMIT_EXCEEDED` — a
    // capacity decision said out loud — and the connection survives the
    // refusal, so the second client learns it rather than hanging.
    let second_client = server.client_runtime();
    let second = second_client.pair(server.trust());
    within(second.connect(&server.url("/link")))
        .await
        .expect("connect: the refusal is per stream, not per connection");
    let mut transfer = within(second.open(TransferMeta::default()))
        .await
        .expect("open");
    let _ = transfer.write_all(b"me too").await;
    let refused = match transfer.finish() {
        Ok(delivery) => within(delivery.delivered())
            .await
            .expect_err("the newcomer is refused"),
        Err(e) => e,
    };
    assert!(
        matches!(refused, Error::LimitExceeded),
        "expected LIMIT_EXCEEDED, got {refused:?}"
    );

    // The first peer is **kept**, which is the whole rule: ZeroMQ's PAIR
    // would have dropped it for the newcomer.
    within(first.send(b"still mine")).await.expect("send");
    assert_eq!(
        within(within(bound.recv()).await.expect("recv").collect(64))
            .await
            .expect("collect"),
        b"still mine"
    );
    within(bound.send(b"and back")).await.expect("send back");
    assert_eq!(
        within(within(first.recv()).await.expect("recv").collect(64))
            .await
            .expect("collect"),
        b"and back"
    );

    first_client.shutdown().await;
    second_client.shutdown().await;
}

#[tokio::test]
async fn a_pair_talks_to_a_bare_peer_and_acceptor_on_the_same_path() {
    // PAIR adds **no wire vocabulary**: the frames are the ones a raw L0
    // `Peer` and `Acceptor` exchange, so the two are interchangeable on the
    // same path. If this needed a translation step, the pattern layer would
    // be protocol rather than API.
    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/link").expect("acceptor");

    let client = server.client_runtime();
    let dialling = client.pair(server.trust());
    within(dialling.connect(&server.url("/link")))
        .await
        .expect("connect");
    within(dialling.send(b"pair to acceptor"))
        .await
        .expect("send");

    match within(acceptor.accept()).await.expect("accept") {
        weida::Incoming::Stream(transfer) => {
            assert_eq!(transfer.meta().endpoint.as_deref(), Some("/link"));
            assert_eq!(
                within(transfer.collect(1024)).await.expect("collect"),
                b"pair to acceptor"
            );
        }
        other => panic!("a paired send is a one-way transfer, got {other:?}"),
    }

    // And the other way round: a bound pair takes what a bare `Peer` sends.
    let bound_server = Server::start().await;
    let bound = bound_server.listener.pair("/link").expect("bound pair");
    let bare_client = bound_server.client_runtime();
    let peer = bare_client.peer(bound_server.trust());
    within(peer.connect(&bound_server.url("/link")))
        .await
        .expect("connect");
    let mut raw = within(peer.open(TransferMeta::default()))
        .await
        .expect("open");
    within(raw.write_all(b"peer to pair")).await.expect("write");
    raw.finish().expect("finish");
    assert_eq!(
        within(within(bound.recv()).await.expect("recv").collect(64))
            .await
            .expect("collect"),
        b"peer to pair"
    );

    client.shutdown().await;
    bare_client.shutdown().await;
}

#[tokio::test]
async fn a_canceled_transfer_is_never_seen_as_complete() {
    // §1.5 through the new type: a reader must never mistake an interrupted
    // stream for a finished one, whatever pattern carried it.
    let server = Server::start().await;
    let bound = server.listener.pair("/link").expect("bound pair");

    let client = server.client_runtime();
    let dialling = client.pair(server.trust());
    within(dialling.connect(&server.url("/link")))
        .await
        .expect("connect");

    let mut transfer = within(dialling.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"half a message"))
        .await
        .expect("write");
    // The receiver takes the transfer first: a reset that overtakes the
    // header would discard the whole stream, and what is under test is what a
    // reader sees *after* it has one.
    let mut inbound = within(bound.recv()).await.expect("recv");
    // Abandoned rather than finished: the stream is reset, so the reader sees
    // a failure and not a short payload.
    transfer.cancel();

    let err = within(inbound.read_capped(1024))
        .await
        .expect_err("an interrupted transfer is never complete");
    assert!(
        matches!(err, Error::Canceled),
        "expected Canceled, got {err:?}"
    );

    // The pair survives it: a canceled transfer is one message, not the
    // connection.
    within(dialling.send(b"the next one")).await.expect("send");
    assert_eq!(
        within(within(bound.recv()).await.expect("recv").collect(64))
            .await
            .expect("collect"),
        b"the next one"
    );

    client.shutdown().await;
}
