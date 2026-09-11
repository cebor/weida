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

/// The segmented grammar over a real connection: one-segment `*`, trailing
/// `#`, and the boundary a byte prefix could not see.
#[tokio::test]
async fn subscribe_filters_topics_by_segment() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.*")).await.expect("subscribe px.*");
    within(sub.subscribe("ctl.#"))
        .await
        .expect("subscribe ctl.#");
    within(sub.subscribe("sensors.temp"))
        .await
        .expect("subscribe sensors.temp");
    await_filters(&publisher, 3).await;

    // `px.*` takes exactly one segment.
    assert_eq!(
        publisher.publish("px.eur", &b"one"[..]).expect("publish"),
        1
    );
    assert_eq!(
        publisher
            .publish("px.eur.spot", &b"too deep"[..])
            .expect("publish"),
        0
    );
    assert_eq!(
        publisher.publish("px", &b"too short"[..]).expect("publish"),
        0
    );
    // No filter matches `fx` at all.
    assert_eq!(
        publisher
            .publish("fx.usd", &b"nobody"[..])
            .expect("publish"),
        0
    );
    // The boundary a byte prefix over-matched: `sensors.temp` must not select
    // `sensors.temperature`. This assertion fails on the old matcher.
    assert_eq!(
        publisher
            .publish("sensors.temperature", &b"not mine"[..])
            .expect("publish"),
        0
    );
    assert_eq!(
        publisher
            .publish("sensors.temp", &b"mine"[..])
            .expect("publish"),
        1
    );
    // `ctl.#` takes the parent and everything under it.
    assert_eq!(
        publisher.publish("ctl", &b"parent"[..]).expect("publish"),
        1
    );
    assert_eq!(
        publisher
            .publish("ctl.end.now", &b"deep"[..])
            .expect("publish"),
        1
    );

    // The per-subscriber writer is FIFO, so the order below also proves the
    // unmatched topics were filtered out rather than merely late.
    assert_eq!(recv_one(&sub).await, ("px.eur".to_owned(), b"one".to_vec()));
    assert_eq!(
        recv_one(&sub).await,
        ("sensors.temp".to_owned(), b"mine".to_vec())
    );
    assert_eq!(recv_one(&sub).await, ("ctl".to_owned(), b"parent".to_vec()));
    assert_eq!(
        recv_one(&sub).await,
        ("ctl.end.now".to_owned(), b"deep".to_vec())
    );

    client.shutdown().await;
}

/// A published topic is data, not a pattern: its `*` is an ordinary byte.
#[tokio::test]
async fn a_topic_containing_a_wildcard_byte_is_literal() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("px.*")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    // `px.*` selects it as one segment, like any other one-segment topic.
    assert_eq!(publisher.publish("px.*", &b"star"[..]).expect("publish"), 1);
    assert_eq!(recv_one(&sub).await, ("px.*".to_owned(), b"star".to_vec()));

    client.shutdown().await;
}

/// A filter the grammar forbids fails locally instead of travelling to the
/// publisher, which would answer it by closing the connection.
#[tokio::test]
async fn an_illegal_filter_is_refused_before_it_reaches_the_wire() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let client = server.client_runtime();
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    for bad in ["px*", "px.#.eur", "p*x"] {
        let err = within(sub.subscribe(bad))
            .await
            .expect_err("the grammar must refuse it");
        assert!(matches!(err, Error::Protocol(_)), "{bad}: {err:?}");
    }
    assert_eq!(sub.filter_count(), 0);

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
    within(a.subscribe("px.#")).await.expect("subscribe a");
    within(b.subscribe("px.#")).await.expect("subscribe b");
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
    within(sub.subscribe("px.#")).await.expect("subscribe px.#");
    within(sub.subscribe("ctl.#"))
        .await
        .expect("subscribe ctl.#");
    await_filters(&publisher, 2).await;

    within(sub.unsubscribe("px.#")).await.expect("unsubscribe");
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
    within(sub.subscribe("px.#")).await.expect("subscribe");

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

    // A publisher path accepts no inbound stream of either kind.
    //
    // The payload is larger than the stream receive window on purpose. A
    // transfer that fits in flight can be acknowledged by the peer's
    // *transport* before the peer's *application* refuses it, and then the
    // receipt truthfully says "delivered" — a transport receipt says nothing
    // about the application, including that it said no. Past the window the
    // write cannot complete until the peer reads or refuses, so the refusal
    // is the only way out.
    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/md")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    let payload = vec![0u8; 2 * 1024 * 1024];
    let refused = async {
        transfer.write_all(&payload).await?;
        transfer.finish()?.delivered().await
    };
    let err = within(refused)
        .await
        .expect_err("a push to a publisher path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/md")))
        .await
        .expect("connect");
    let err = within(requester.request(b"nope"))
        .await
        .expect_err("an exchange with a publisher path must be refused");
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
