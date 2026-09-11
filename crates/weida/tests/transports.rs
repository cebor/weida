//! The same pattern tests over both transports.
//!
//! Each body below is written once and run twice: over native QUIC and over
//! the in-process transport of
//! [decision 0010](../../docs/decisions/0010-local-transport.md). That is
//! what the transport boundary introduced in B-037 is for — above it, the
//! frames, the HELLO exchange, the negotiation and the patterns are the same
//! (`docs/PROTOCOL.md` §2.1) — and running the *same* body is the only way to
//! check that claim rather than restate it.

mod common;

use std::time::Duration;

use common::{Harness, Transport};
use weida::{Deduplication, Error, GuaranteeSet, OrderingMode, RuntimeConfig, TransferMeta};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Req/Rep: one exchange, uppercased and returned.
async fn req_rep_echo(h: &Harness) {
    let replier = h.listener.replier("/transform").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        let payload = body.read_capped(64 * 1024).await.expect("read");
        reply
            .write_all(&payload.to_ascii_uppercase())
            .await
            .expect("write");
        reply.finish().expect("finish");
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/transform")))
        .await
        .expect("connect");
    let (mut request, reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(request.write_all(b"hello weida"))
        .await
        .expect("write");
    request.finish().expect("finish");
    let body = within(
        within(reply.recv())
            .await
            .expect("reply")
            .collect(64 * 1024),
    )
    .await
    .expect("collect");
    assert_eq!(body, b"HELLO WEIDA".to_vec());
    within(handler).await.expect("handler");
    client.shutdown().await;
}

/// Push/Pull: a one-way transfer and its receipt.
async fn push_pull_delivery(h: &Harness) {
    let puller = h.listener.puller("/jobs").expect("puller");

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"work item"))
        .await
        .expect("write");
    let delivery = transfer.finish().expect("finish");

    let received = within(puller.recv()).await.expect("recv");
    // Nobody is proved in process, and an anonymous QUIC client presents no
    // key either, so both transports report the same thing here [0010 §4.4].
    assert_eq!(received.meta().peer, None);
    let body = within(received.collect(64 * 1024)).await.expect("collect");
    assert_eq!(body, b"work item".to_vec());
    within(delivery.delivered()).await.expect("delivered");
    client.shutdown().await;
}

/// Pub/Sub: a filtered fan-out to two subscribers.
async fn pub_sub_fan_out(h: &Harness) {
    let publisher = h.listener.publisher("/md").expect("publisher");

    // Two client runtimes, because two subscribers on one runtime share the
    // pooled connection and collide on the path — which is what
    // `a_second_subscriber_on_one_connection_collides` pins.
    let first_client = h.client();
    let second_client = h.client();
    let first = first_client.subscriber(h.trust());
    let second = second_client.subscriber(h.trust());
    within(first.connect(&h.url("/md"))).await.expect("connect");
    within(second.connect(&h.url("/md")))
        .await
        .expect("connect");
    within(first.subscribe("px.#")).await.expect("subscribe");
    within(second.subscribe("fx.#")).await.expect("subscribe");

    // The publisher learns of a subscription asynchronously; publish until
    // both filters are registered rather than sleeping for them.
    within(async {
        while publisher.filter_count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    publisher.publish("px.eur", &b"price"[..]).expect("publish");
    publisher.publish("fx.usd", &b"rate"[..]).expect("publish");

    let to_first = within(first.recv()).await.expect("recv");
    assert_eq!(to_first.meta().topic.as_deref(), Some("px.eur"));
    assert_eq!(
        within(to_first.collect(1024)).await.expect("collect"),
        b"price".to_vec()
    );
    let to_second = within(second.recv()).await.expect("recv");
    assert_eq!(to_second.meta().topic.as_deref(), Some("fx.usd"));
    assert_eq!(
        within(to_second.collect(1024)).await.expect("collect"),
        b"rate".to_vec()
    );
    first_client.shutdown().await;
    second_client.shutdown().await;
}

#[tokio::test]
async fn req_rep_over_quic() {
    req_rep_echo(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn req_rep_over_inproc() {
    req_rep_echo(&Harness::start(Transport::Inproc).await).await;
}

#[tokio::test]
async fn push_pull_over_quic() {
    push_pull_delivery(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn push_pull_over_inproc() {
    push_pull_delivery(&Harness::start(Transport::Inproc).await).await;
}

#[tokio::test]
async fn pub_sub_over_quic() {
    pub_sub_fan_out(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn pub_sub_over_inproc() {
    pub_sub_fan_out(&Harness::start(Transport::Inproc).await).await;
}

/// Claim: a bus nobody bound is unreachable, and the address rules of
/// [0010 §4.8] are enforced before anything is dialled.
#[tokio::test]
async fn a_local_address_is_checked_before_it_is_dialled() {
    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::by_address()));

    // Nothing bound: the same outcome as dialling a closed port.
    let err = within(pusher.connect("weida+inproc://nobody-bound/jobs"))
        .await
        .expect_err("no such bus");
    assert!(matches!(err, Error::ConnectionLost(_)), "{err:?}");

    // A fingerprint would claim an authentication a local address cannot do.
    let err = within(pusher.connect(
        "weida+inproc://sha256:0000000000000000000000000000000000000000000000000000000000000000@bus/jobs",
    ))
    .await
    .expect_err("a local address carries no fingerprint");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");

    // 257 bytes of bus name: one past the budget libzmq uses for the same
    // thing.
    let long = "b".repeat(257);
    let err = within(pusher.connect(&format!("weida+inproc://{long}/jobs")))
        .await
        .expect_err("bus name too long");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    client.shutdown().await;
}

/// Claim: a local peer that goes away is reported as a loss, not as a hang.
///
/// A local connection has no socket to close, so the connection itself is
/// the only handle there is: shutting the server runtime down closes the
/// links it registered, and the dialling side sees the same
/// `ConnectionLost` a QUIC peer's close produces
/// ([decisions/0010](../../docs/decisions/0010-local-transport.md) §4.2,
/// B-028's `LossCause`).
#[tokio::test]
async fn a_local_peer_that_goes_away_is_reported_as_connection_loss() {
    let h = Harness::start(Transport::Inproc).await;
    let _puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");

    // The server goes away: its bindings close and its connections with them.
    h.shutdown().await;

    let err = within(async {
        loop {
            match pusher.open(TransferMeta::default()).await {
                Ok(mut transfer) => {
                    // The connection may not have noticed yet; a write to a
                    // gone peer is what settles it.
                    if let Err(e) = transfer.write_all(b"x").await {
                        break e;
                    }
                }
                Err(e) => break e,
            }
        }
    })
    .await;
    assert!(
        matches!(err, Error::ConnectionLost(_) | Error::NotConnected),
        "{err:?}"
    );
    client.shutdown().await;
}

/// Claim: the guarantees and the drain are above the transport, so they work
/// over the in-process one unchanged.
///
/// Ordering, deduplication and `Runtime::drain` are all defined on headers
/// and receipts rather than on sockets, and the transport boundary is what
/// keeps that true. Nothing about them is excluded locally
/// (`docs/decisions/0010-local-transport.md` §4.2).
#[tokio::test]
async fn guarantees_and_the_drain_work_over_inproc() {
    let guarantees = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        deduplication: Deduplication::Bounded,
        dedup_window_ms: Some(500),
        ..GuaranteeSet::CORE
    };
    let config = RuntimeConfig {
        guarantees,
        ..RuntimeConfig::default()
    };
    let h = Harness::start_with(Transport::Inproc, config.clone()).await;
    let puller = h.listener.puller("/jobs").expect("puller");

    let client = h.client_with(config);
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");

    for body in [&b"first"[..], &b"second"[..]] {
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(body)).await.expect("write");
        // Fire and forget: the receipt is dropped, so only a drain can wait
        // for it.
        drop(transfer.finish().expect("finish"));
    }

    for expected in [0u64, 1] {
        let transfer = within(puller.recv()).await.expect("recv");
        assert_eq!(
            transfer.meta().sequence,
            Some(expected),
            "the negotiated ordering numbers local transfers too"
        );
        assert_eq!(transfer.meta().gap, None);
        within(transfer.collect(1024)).await.expect("collect");
    }

    let drained = within(client.drain(Duration::from_secs(2))).await;
    assert_eq!(
        drained.outstanding, 0,
        "a local drain waits on the same receipts: {drained:?}"
    );
    assert!(drained.delivered <= 2, "{drained:?}");
    h.shutdown().await;
}
