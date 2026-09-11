//! Identity and trust: who a peer is, and on what terms it is accepted.
//!
//! Every test dials a real loopback server. The claims defended here are the
//! ones an operator relies on when a fingerprint is the whole of a peer's
//! identity: a pin accepts exactly one key, an address that names a key
//! overrides everything else, a binding can demand a client identity and
//! learn which one it got, and a connection authenticated on one set of terms
//! is never handed to an endpoint dialling on another.

mod common;

use std::future::Future;
use std::time::Duration;

use common::{Certs, Server};
use weida::{ClientTls, Error, Identity, PeerIdentity, Runtime, RuntimeConfig, ServerTls, Trust};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Spawns an echo replier and returns nothing; the handler runs until the
/// listener goes away.
fn spawn_echo(replier: weida::Replier) {
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request.body().read_capped(1024).await.expect("body");
            let mut reply = request
                .reply(weida::TransferMeta::default())
                .await
                .expect("reply");
            reply.write_all(&body).await.expect("write");
            let _ = reply.finish();
        }
    });
}

async fn roundtrip(requester: &weida::Requester) -> Vec<u8> {
    let reply = within(requester.request(b"ping")).await.expect("request");
    within(reply.collect(64)).await.expect("collect")
}

#[tokio::test]
async fn an_address_that_names_the_peer_needs_no_other_trust() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));

    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect by address");
    assert_eq!(roundtrip(&requester).await, b"ping");

    client.shutdown().await;
}

#[tokio::test]
async fn a_wrong_pin_in_the_address_is_refused_with_what_answered() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/echo").expect("replier");

    let stranger = Certs::generate();
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    let err = within(requester.connect(&url)).await.unwrap_err();
    match err {
        Error::Untrusted(presented) => assert_eq!(presented, server.certs.fingerprint()),
        other => panic!("expected Untrusted, got {other:?}"),
    }
    assert!(err_is_definite(&Error::Untrusted(
        server.certs.fingerprint()
    )));
    assert_eq!(requester.peer_count(), 0, "a refused dial adds no peer");

    client.shutdown().await;
}

fn err_is_definite(e: &Error) -> bool {
    e.is_definite_failure()
}

#[tokio::test]
async fn an_address_pin_overrides_an_anchor_that_would_have_accepted() {
    // The server's own certificate is a valid anchor for it. Naming a
    // different key in the address must still refuse it: the address is the
    // most specific statement of intent and wins.
    let server = Server::start().await;
    let _replier = server.listener.replier("/echo").expect("replier");

    let stranger = Certs::generate();
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    assert!(matches!(
        within(requester.connect(&url)).await.unwrap_err(),
        Error::Untrusted(_)
    ));

    client.shutdown().await;
}

#[tokio::test]
async fn a_pinned_trust_accepts_the_key_and_only_the_key() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));
    let stranger = Certs::generate();

    let client = server.client_runtime();

    let pinned = client.requester(Trust::pin(server.certs.fingerprint()));
    within(pinned.connect(&server.url("/echo")))
        .await
        .expect("pinned connect");
    assert_eq!(roundtrip(&pinned).await, b"ping");

    let wrong = client.requester(Trust::pin(stranger.fingerprint()));
    assert!(matches!(
        within(wrong.connect(&server.url("/echo")))
            .await
            .unwrap_err(),
        Error::Untrusted(_)
    ));

    client.shutdown().await;
}

#[tokio::test]
async fn pins_and_anchors_compose() {
    // One endpoint, two servers: one reachable through its anchor, the other
    // through its pin. Neither term alone would cover both.
    let anchored = Server::start().await;
    let pinned = Server::start().await;
    spawn_echo(anchored.listener.replier("/echo").expect("replier"));
    spawn_echo(pinned.listener.replier("/echo").expect("replier"));

    let client = anchored.client_runtime();
    let requester = client.requester(
        Trust::anchor_file(&anchored.certs.cert_pem).and_pin(pinned.certs.fingerprint()),
    );
    within(requester.connect(&anchored.url("/echo")))
        .await
        .expect("anchored");
    within(requester.connect(&pinned.url("/echo")))
        .await
        .expect("pinned");
    assert_eq!(requester.peer_count(), 2);
    for _ in 0..2 {
        assert_eq!(roundtrip(&requester).await, b"ping");
    }

    client.shutdown().await;
}

#[tokio::test]
async fn an_anchor_checks_the_name_and_a_pin_does_not() {
    // A certificate trusted as an anchor is verified the way a CA-issued one
    // is: the host dialled must be among its names. This identity names only
    // `localhost`, and the dial goes to `127.0.0.1`, so the anchor path
    // refuses it — while a pin on the same key accepts it, because a pin
    // says nothing about names.
    let identity = Identity::generate_for(["localhost"]).expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let cert_pem = identity.certificate_pem().expect("cert");

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
        .await
        .expect("bind");
    spawn_echo(listener.replier("/echo").expect("replier"));
    let url = format!("weida://127.0.0.1:{}/echo", binding.local_addr().port());

    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let anchored = client.requester(Trust::anchor(cert_pem.clone()));
    match within(anchored.connect(&url)).await.unwrap_err() {
        Error::Untrusted(presented) => assert_eq!(presented, fingerprint),
        other => panic!("expected Untrusted, got {other:?}"),
    }

    let pinned = client.requester(Trust::pin(fingerprint));
    within(pinned.connect(&url)).await.expect("pinned connect");
    assert_eq!(roundtrip(&pinned).await, b"ping");

    // Both terms together: the pin carries it, the anchor's name check is
    // never reached.
    let both = client.requester(Trust::anchor(cert_pem).and_pin(fingerprint));
    within(both.connect(&url)).await.expect("connect");

    client.shutdown().await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_binding_that_requires_clients_learns_who_they_are() {
    let identity = Identity::generate().expect("identity");
    let client_identity = Identity::generate().expect("client identity");
    let client_fp = client_identity.fingerprint().expect("fingerprint");

    let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity.clone()).require_client(Trust::pin(client_fp)),
        )
        .await
        .expect("bind");
    let url = format!(
        "weida://{}@127.0.0.1:{}/who",
        identity.fingerprint().expect("fingerprint"),
        binding.local_addr().port()
    );

    let replier = listener.replier("/who").expect("replier");
    let seen = tokio::spawn(async move {
        let request = replier.accept().await.expect("accept");
        let peer = request.meta().peer;
        let mut reply = request
            .reply(weida::TransferMeta::default())
            .await
            .expect("reply");
        reply.write_all(b"ok").await.expect("write");
        let _ = reply.finish();
        peer
    });

    // Anonymous: the handshake fails, and the failure is a TLS outcome.
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let anonymous = client.requester(Trust::by_address());
    let err = within(anonymous.connect(&url)).await.unwrap_err();
    assert!(matches!(err, Error::Tls(_)), "{err:?}");

    // An identity the binding does not trust: refused just the same.
    let stranger = client.requester(
        ClientTls::new(Trust::by_address()).with_identity(Identity::generate().expect("id")),
    );
    let err = within(stranger.connect(&url)).await.unwrap_err();
    assert!(matches!(err, Error::Tls(_)), "{err:?}");

    // The pinned identity: accepted, and the handler sees exactly its key.
    let trusted =
        client.requester(ClientTls::new(Trust::by_address()).with_identity(client_identity));
    within(trusted.connect(&url)).await.expect("connect");
    let reply = within(trusted.request(b"?")).await.expect("request");
    assert_eq!(within(reply.collect(16)).await.expect("collect"), b"ok");
    assert_eq!(
        within(seen).await.expect("handler"),
        Some(PeerIdentity::Key(client_fp))
    );

    // The reply carries the server's identity the same way.
    let reply = within(trusted.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    drop(reply);

    client.shutdown().await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn an_anonymous_client_is_seen_as_nobody() {
    let server = Server::start().await;
    let replier = server.listener.replier("/who").expect("replier");
    let seen = tokio::spawn(async move {
        let request = replier.accept().await.expect("accept");
        request.meta().peer
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/who")))
        .await
        .expect("connect");
    let (mut transfer, reply) = within(requester.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"?")).await.expect("write");
    let _ = transfer.finish();
    assert_eq!(within(seen).await.expect("handler"), None);
    drop(reply);

    client.shutdown().await;
}

#[tokio::test]
async fn the_reply_names_the_server_the_requester_dialled() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));

    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect");
    let reply = within(requester.request(b"x")).await.expect("request");
    assert_eq!(
        reply.meta().peer,
        Some(PeerIdentity::Key(server.certs.fingerprint()))
    );

    client.shutdown().await;
}

#[tokio::test]
async fn connections_are_not_shared_across_different_terms() {
    // Same runtime, same authority. The first endpoint pins the right key
    // and connects. The second names a different key in its address; if the
    // pool handed it the first connection it would succeed without ever
    // checking. It must not.
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));
    let stranger = Certs::generate();

    let client = server.client_runtime();
    let right = client.requester(Trust::by_address());
    within(right.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect");
    assert_eq!(roundtrip(&right).await, b"ping");

    let wrong = client.requester(Trust::by_address());
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    assert!(matches!(
        within(wrong.connect(&url)).await.unwrap_err(),
        Error::Untrusted(_)
    ));
    // And the first endpoint is unaffected by its neighbour's refusal.
    assert_eq!(roundtrip(&right).await, b"ping");

    client.shutdown().await;
}

/// Claim: the connections of one peer are bound together by the proved
/// fingerprint, and by nothing else
/// ([0008](../../../docs/decisions/0008-session-identity.md) §4.2). One
/// connection per dialled path means a second path is a second connection to
/// the *same* peer, so a second server answering on that authority is refused,
/// carrying the fingerprint that answered rather than a bare failure.
///
/// The setup is the load-balancer case, and it needs a raw server because two
/// `quinn` endpoints cannot share a port: one socket, whose identity is
/// swapped between the two handshakes. The client trusts both keys, so nothing
/// but the binding rule can refuse the second dial.
#[tokio::test]
async fn a_second_path_that_answers_with_another_identity_is_refused() {
    let first = Certs::generate();
    let second = Certs::generate();
    let first_fp = first.fingerprint();
    let second_fp = second.fingerprint();

    let (endpoint, addr) = common::raw::server_endpoint(&first);
    let swap = common::raw::server_config(&second);
    tokio::spawn(async move {
        // First handshake: identity one. The identity is swapped *before* the
        // HELLO that lets the client's `connect` return, so the second dial
        // cannot race ahead of the swap.
        let incoming = endpoint.accept().await.expect("first connection");
        let held = incoming.await.expect("first handshake");
        endpoint.set_server_config(Some(swap));
        common::raw::send_hello(&held).await;

        // Second handshake: identity two. It never needs a HELLO — the pool
        // refuses it on identity, before negotiation begins.
        if let Some(incoming) = endpoint.accept().await {
            let _ = incoming.await;
        }
        // Hold the first connection open: the binding rule compares against
        // this peer's *live* connections.
        held.closed().await;
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher = client.pusher(ClientTls::new(Trust::pin(first_fp).and_pin(second_fp)));

    let base = format!("weida://127.0.0.1:{}", addr.port());
    within(pusher.connect(&format!("{base}/a")))
        .await
        .expect("the first path connects");

    let err = within(pusher.connect(&format!("{base}/b")))
        .await
        .expect_err("a different peer on the same authority must be refused");
    match err {
        Error::Untrusted(fp) => assert_eq!(
            fp, second_fp,
            "the refusal names who answered, so an operator can pin it"
        ),
        other => panic!("expected Untrusted, got {other:?}"),
    }
    // The path that was already connected is untouched by its neighbour's
    // refusal: one connection per path means one failure per path.
    assert_eq!(pusher.peer_count(), 1);

    client.shutdown().await;
}

/// Claim: `max_connections_per_peer` bounds what one peer can hold, counted by
/// the identity it proved, and one connection per dialled path is exactly why
/// it is needed — the dialling side chooses the path count
/// ([0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §7).
///
/// The binding requires a client identity, because a bound it cannot attribute
/// is a bound it cannot enforce: two anonymous connections may not be treated
/// as one peer.
#[tokio::test]
async fn a_peer_cannot_hold_more_connections_than_its_ceiling() {
    const CEILING: usize = 3;

    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let client_identity = Identity::generate().expect("client identity");
    let client_fp = client_identity.fingerprint().expect("client fingerprint");

    let runtime = Runtime::new(RuntimeConfig {
        max_connections_per_peer: CEILING,
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity).require_client(Trust::pin(client_fp)),
        )
        .await
        .expect("bind");
    let base = format!(
        "weida://{}@127.0.0.1:{}",
        fingerprint,
        binding.local_addr().port()
    );
    // Held for the duration: dropping a puller unregisters its path.
    let _pullers: Vec<_> = (0..=CEILING)
        .map(|i| listener.puller(&format!("/p{i}")).expect("puller"))
        .collect();

    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher =
        client.pusher(ClientTls::new(Trust::pin(fingerprint)).with_identity(client_identity));

    // Up to the ceiling: one connection per path, all accepted.
    for i in 0..CEILING {
        within(pusher.connect(&format!("{base}/p{i}")))
            .await
            .unwrap_or_else(|e| panic!("path {i} must connect: {e:?}"));
    }
    assert_eq!(pusher.peer_count(), CEILING);

    // One more path is one more connection, and the binding refuses it by
    // saying which limit was hit.
    let err = within(pusher.connect(&format!("{base}/p{CEILING}")))
        .await
        .expect_err("the ceiling must bite");
    assert!(matches!(err, Error::LimitExceeded), "{err:?}");

    client.shutdown().await;
    runtime.shutdown().await;
}
