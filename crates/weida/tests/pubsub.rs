//! Pub/Sub over real QUIC on loopback.
//!
//! Fan-out is the one genuinely new selection policy in Phase 3; everything
//! else reuses the one-way transfer path. These tests pin the parts that are
//! easy to get subtly wrong: prefix filtering, per-subscriber FIFO delivery,
//! subscribe/unsubscribe bookkeeping, and the drop policy that keeps a slow
//! subscriber from stalling the publisher.

mod common;

use std::time::Duration;

use common::Server;
use weida::{Error, Limits, Publisher, Subscriber};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Waits until the publisher observes `count` filters.
///
/// Registry updates happen in the frame-processing task, so a subscribe is
/// visible a moment after `subscribe()` returns. Polling the publisher's own
/// counter is exact; a sleep would only be a guess.
async fn await_filters(publisher: &Publisher, count: usize) {
    within(async {
        while publisher.filter_count() != count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}

/// Receives one message and returns `(topic, payload)`.
async fn recv_one(sub: &Subscriber) -> (String, Vec<u8>) {
    let transfer = within(sub.recv()).await.expect("recv");
    let topic = transfer
        .meta()
        .topic
        .clone()
        .expect("a published message carries its topic");
    let body = within(transfer.collect(1024 * 1024))
        .await
        .expect("collect");
    (topic, body)
}

#[tokio::test]
async fn subscribe_prefix_filters_topics() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.")).await.expect("subscribe px.");
    within(sub.subscribe("ctl.")).await.expect("subscribe ctl.");
    await_filters(&publisher, 2).await;

    assert_eq!(
        publisher.publish("px.eur", &b"one"[..]).expect("publish"),
        1
    );
    // No filter matches `fx.`, so this reaches nobody.
    assert_eq!(
        publisher.publish("fx.usd", &b"two"[..]).expect("publish"),
        0
    );
    assert_eq!(
        publisher
            .publish("ctl.end", &b"three"[..])
            .expect("publish"),
        1
    );

    // The per-subscriber writer is FIFO, so receiving the `ctl.end` sentinel
    // proves `fx.usd` was filtered out rather than merely late.
    assert_eq!(recv_one(&sub).await, ("px.eur".to_owned(), b"one".to_vec()));
    assert_eq!(
        recv_one(&sub).await,
        ("ctl.end".to_owned(), b"three".to_vec())
    );

    client.shutdown().await;
}

#[tokio::test]
async fn empty_filter_receives_all() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe all");
    await_filters(&publisher, 1).await;

    for topic in ["px.eur", "fx.usd", "anything"] {
        assert_eq!(publisher.publish(topic, &b"x"[..]).expect("publish"), 1);
    }
    for topic in ["px.eur", "fx.usd", "anything"] {
        assert_eq!(recv_one(&sub).await, (topic.to_owned(), b"x".to_vec()));
    }

    client.shutdown().await;
}

#[tokio::test]
async fn two_subscribers_both_receive() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // Two runtimes, hence two connections: one subscriber per connection is
    // the supported shape, since a subscriber claims its path in the
    // connection's namespace.
    let client_a = server.client_runtime();
    let client_b = server.client_runtime();
    let a = client_a.subscriber(server.trust());
    let b = client_b.subscriber(server.trust());
    within(a.connect(&server.url("/md")))
        .await
        .expect("connect a");
    within(b.connect(&server.url("/md")))
        .await
        .expect("connect b");
    within(a.subscribe("px.")).await.expect("subscribe a");
    within(b.subscribe("px.")).await.expect("subscribe b");
    await_filters(&publisher, 2).await;
    assert_eq!(publisher.subscriber_count(), 2);

    assert_eq!(
        publisher.publish("px.eur", &b"tick"[..]).expect("publish"),
        2
    );
    assert_eq!(recv_one(&a).await, ("px.eur".to_owned(), b"tick".to_vec()));
    assert_eq!(recv_one(&b).await, ("px.eur".to_owned(), b"tick".to_vec()));

    client_a.shutdown().await;
    client_b.shutdown().await;
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.")).await.expect("subscribe px.");
    within(sub.subscribe("ctl.")).await.expect("subscribe ctl.");
    await_filters(&publisher, 2).await;

    within(sub.unsubscribe("px.")).await.expect("unsubscribe");
    await_filters(&publisher, 1).await;

    assert_eq!(publisher.publish("px.x", &b"gone"[..]).expect("publish"), 0);
    assert_eq!(
        publisher.publish("ctl.end", &b"kept"[..]).expect("publish"),
        1
    );

    // Only the sentinel arrives; FIFO ordering makes this conclusive.
    assert_eq!(
        recv_one(&sub).await,
        ("ctl.end".to_owned(), b"kept".to_vec())
    );

    client.shutdown().await;
}

#[tokio::test]
async fn late_publisher_receives_early_subscription() {
    let server = Server::start().await;
    // No publisher yet: the path is not registered at all.
    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.")).await.expect("subscribe");

    // The publisher appears afterwards and still finds the subscription.
    let publisher = server.listener.publisher("/md").expect("publisher");
    await_filters(&publisher, 1).await;
    assert_eq!(
        publisher.publish("px.eur", &b"late"[..]).expect("publish"),
        1
    );
    assert_eq!(
        recv_one(&sub).await,
        ("px.eur".to_owned(), b"late".to_vec())
    );

    client.shutdown().await;
}

#[tokio::test]
async fn slow_subscriber_drops_not_blocks() {
    const MSG: usize = 32 * 1024;
    const COUNT: usize = 100;

    // The byte budget is a *publisher-side* limit, so it belongs to the server
    // runtime that owns the registry. 64 KiB holds two messages.
    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: 64 * 1024,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // A subscriber that never reads only exerts backpressure once its QUIC
    // receive window fills, so the window has to be small enough to reach.
    // With a 128 KiB connection window, four unread messages stall the
    // publisher's writer for this subscriber; its budget is then never
    // returned and further publishes are dropped for it.
    let slow_rt = server.client_runtime_with(Limits {
        connection_receive_window: 128 * 1024,
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    });
    let fast_rt = server.client_runtime();
    let slow = slow_rt.subscriber(server.trust());
    let fast = fast_rt.subscriber(server.trust());
    within(slow.connect(&server.url("/md")))
        .await
        .expect("connect slow");
    within(fast.connect(&server.url("/md")))
        .await
        .expect("connect fast");
    within(slow.subscribe("")).await.expect("subscribe slow");
    within(fast.subscribe("")).await.expect("subscribe fast");
    await_filters(&publisher, 2).await;

    // Publish and drain the fast subscriber in lockstep: its budget is always
    // free before the next publish, so it can never be the one dropping.
    // The slow subscriber never reads.
    let payload = vec![0xa5u8; MSG];
    let mut fast_received = 0usize;
    // The whole loop sits inside the deadline: were `publish` to wait on the
    // slow subscriber, this would time out instead of dropping messages.
    within(async {
        for _ in 0..COUNT {
            publisher
                .publish("px.eur", payload.clone())
                .expect("publish");
            let (topic, body) = recv_one(&fast).await;
            assert_eq!(topic, "px.eur");
            assert_eq!(body.len(), MSG);
            fast_received += 1;
        }
    })
    .await;

    assert_eq!(fast_received, COUNT, "the fast subscriber lost messages");
    assert!(
        publisher.dropped() > 0,
        "the slow subscriber should have lost messages"
    );

    drop(fast);
    drop(slow);
    slow_rt.shutdown().await;
    fast_rt.shutdown().await;
}

#[tokio::test]
async fn sub_meta_carries_topic_and_trace() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    publisher.publish("px.eur", &b"body"[..]).expect("publish");
    let transfer = within(sub.recv()).await.expect("recv");
    let meta = transfer.meta().clone();
    assert_eq!(meta.topic.as_deref(), Some("px.eur"));
    assert_eq!(meta.endpoint.as_deref(), Some("/md"));
    assert_eq!(meta.correlation_id, None);
    assert_eq!(meta.content_len, Some(4));
    assert!(
        meta.trace.is_some(),
        "fan-out must propagate a trace context"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn a_payload_larger_than_the_budget_is_refused() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    // Nothing could ever enqueue this, so it is an error rather than a silent
    // drop for every subscriber.
    let oversized = vec![0u8; Limits::default().subscriber_buffer_bytes + 1];
    let err = publisher.publish("px.eur", oversized).unwrap_err();
    assert!(matches!(err, Error::LimitExceeded), "{err:?}");
}

#[tokio::test]
async fn publishing_to_nobody_is_not_an_error() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    assert_eq!(publisher.subscriber_count(), 0);
    assert_eq!(publisher.publish("px.eur", &b"x"[..]).expect("publish"), 0);
}

#[tokio::test]
async fn a_publisher_path_refuses_inbound_transfers() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    // A publisher path accepts no inbound transfers of any role.
    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/md")))
        .await
        .expect("connect");
    let err = within(pusher.send_with(
        weida::TransferMeta::default().with_ack(weida::AckMode::Accepted),
        b"nope",
    ))
    .await
    .expect_err("a push to a publisher path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    client.shutdown().await;
}

#[tokio::test]
async fn a_second_subscriber_on_one_connection_collides() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    // Both subscribers share the runtime's pooled connection and claim the
    // same path in its namespace. Refusing beats silently multiplexing two
    // subscribers onto one queue.
    let client = server.client_runtime();
    let first = client.subscriber(server.trust());
    within(first.connect(&server.url("/md")))
        .await
        .expect("connect first");
    let second = client.subscriber(server.trust());
    let err = within(second.connect(&server.url("/md")))
        .await
        .expect_err("the path is already claimed on this connection");
    assert!(matches!(err, Error::AlreadyRegistered), "{err:?}");

    client.shutdown().await;
}
