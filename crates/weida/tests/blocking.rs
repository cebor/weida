//! The blocking facade, from threads with no executor (B-194).
//!
//! These are plain `#[test]` functions: no `#[tokio::test]` anywhere, which
//! is the point — a synchronous caller has no reactor, and everything below
//! runs on the reactor `weida::blocking::Runtime` owns and never shows.
//!
//! One round trip per pattern, because what needs proving is that the facade
//! drives the asynchronous surface correctly, not that the patterns work: they
//! have their own suites. The fourth test is the one that earns its place
//! twice over — calling a blocking method from inside a Tokio runtime is the
//! one mistake that deadlocks instead of failing, and the facade refuses it.
#![cfg(all(feature = "blocking", feature = "generate"))]

use std::time::Duration;

use weida::blocking::Runtime;
use weida::{Error, Identity, RuntimeConfig, Trust};

/// A payload ceiling for every receive: the facade has no default, on purpose.
const CAP: usize = 1024 * 1024;

/// Binds a server on loopback and returns the runtime, the binding and the
/// address a client dials.
fn served(path: &str) -> (Runtime, weida::blocking::Binding, String) {
    let runtime = Runtime::new(RuntimeConfig::default()).expect("an owned reactor");
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let binding = runtime
        .bind_quic("127.0.0.1:0".parse().expect("addr"), identity)
        .expect("bind");
    let url = format!("weida://{fingerprint}@{}{path}", binding.local_addr());
    (runtime, binding, url)
}

#[test]
fn a_request_and_its_reply_cross_between_two_threads() {
    let (server, binding, url) = served("/echo");
    let replier = binding.replier("/echo").expect("replier");

    // The replier runs on its own thread, blocking on `accept`, which is how
    // a synchronous server is written.
    let answering = std::thread::spawn(move || {
        let request = replier.accept(CAP).expect("accept");
        assert_eq!(request.message().payload, b"ping");
        // The metadata is the asynchronous surface's, passed through: this
        // client presented no identity, so the peer is `None` — which is the
        // documented answer and not a facade artefact.
        assert!(request.message().meta.peer.is_none());
        assert_eq!(request.message().meta.endpoint.as_deref(), Some("/echo"));
        request.reply(b"pong").expect("reply");
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let requester = client.requester(Trust::by_address());
    requester.connect(&url).expect("connect");
    assert_eq!(requester.request(b"ping", CAP).expect("request"), b"pong");

    answering.join().expect("the replier thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

#[test]
fn a_push_reaches_a_puller_and_the_receipt_comes_back() {
    let (server, binding, url) = served("/ingest");
    let puller = binding.puller("/ingest").expect("puller");

    let draining = std::thread::spawn(move || {
        let first = puller.recv(CAP).expect("recv");
        assert_eq!(first.payload, b"one");
        let second = puller.recv(CAP).expect("recv");
        assert_eq!(second.payload, b"two");
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&url).expect("connect");
    // `send` returns when the peer's transport holds the bytes, which is what
    // a synchronous caller has to be able to check.
    pusher.send(b"one").expect("send");
    pusher.send(b"two").expect("send");

    draining.join().expect("the puller thread");
    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

#[test]
fn a_published_message_reaches_a_blocking_subscriber() {
    let (server, binding, url) = served("/md");
    let publisher = binding.publisher("/md").expect("publisher");

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let subscriber = client.subscriber(Trust::by_address());
    subscriber.connect(&url).expect("connect");
    subscriber.subscribe("px.#").expect("subscribe");

    // Publishing never waits for a subscriber, so the publisher may run
    // before the subscription has arrived: retry until it has, exactly as the
    // asynchronous Pub/Sub tests do, because a sleep would only be a guess.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the subscription never reached the publisher"
        );
        if publisher
            .publish("px.eur", &b"1.0812"[..])
            .expect("publish")
            > 0
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let message = subscriber.recv(CAP).expect("recv");
    assert_eq!(message.payload, b"1.0812");
    assert_eq!(message.meta.topic.as_deref(), Some("px.eur"));
    // A filter that does not match gets nothing, which is the one claim a
    // facade could break by dropping the filter on the way down.
    assert_eq!(publisher.publish("fx.chf", &b"x"[..]).expect("publish"), 0);

    client.shutdown().expect("client shutdown");
    server.shutdown().expect("server shutdown");
}

/// The mistake that would otherwise be a hang: blocking a reactor worker.
#[test]
fn the_facade_refuses_to_block_a_reactor_thread() {
    let reactor = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime to be inside of");
    let refused = reactor.block_on(async { Runtime::new(RuntimeConfig::default()) });
    match refused {
        Err(Error::Runtime(message)) => {
            assert!(
                message.contains("deadlock"),
                "the refusal must say why: {message}"
            );
        }
        Ok(_) => panic!("a blocking runtime built inside a reactor would deadlock on first use"),
        Err(other) => panic!("{other:?}"),
    }

    // And the same for a call on an endpoint built outside: it is the *call*
    // that blocks, so the check belongs on every entry point rather than only
    // on the constructor.
    let outside = Runtime::new(RuntimeConfig::default()).expect("outside a reactor");
    let requester = outside.requester(Trust::by_address());
    let refused = reactor.block_on(async { requester.connect("weida://127.0.0.1:1/x") });
    assert!(
        matches!(refused, Err(Error::Runtime(_))),
        "{refused:?} must be refused rather than deadlock"
    );
    outside.shutdown().expect("shutdown");
}
