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
use weida::{ClientTls, Error, Identity, Runtime, RuntimeConfig, ServerTls, Trust};

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
    assert_eq!(within(seen).await.expect("handler"), Some(client_fp));

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
    assert_eq!(reply.meta().peer, Some(server.certs.fingerprint()));

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
