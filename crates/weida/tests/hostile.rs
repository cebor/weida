//! Hostile-peer tests: raw `quinn` endpoints speaking hand-encoded frames.
//!
//! Master doc §81 rule 18: all network input is hostile. These tests send byte
//! sequences the library would never produce, and assert on the QUIC
//! application error codes and on the outcomes the local API reports. The
//! library has no test hooks; everything below is written against the wire.

mod common;

use std::time::Duration;

use common::{Certs, Server, raw};
use weida::TransferId;
use weida::{AckMode, Error, Runtime, RuntimeConfig, TransferMeta, codes};
use weida_protocol::{
    AckHeader, DataHeader, FrameKind, Hello, MAGIC, SubscriptionHeader, encode_preamble,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

// --- hostile client against our server ----------------------------------

#[tokio::test]
async fn garbage_first_bytes_close_the_connection() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Not our magic byte: the stream cannot be interpreted at all.
    raw::send_raw(&conn, &[0xff, 0xff, 0xff, 0xff]).await;

    let code = within(raw::closed_code(&conn)).await;
    assert_eq!(
        code,
        codes::PROTOCOL_VIOLATION,
        "expected a violation close"
    );

    // The binding survives: one hostile connection must not take out the
    // listener. A fresh well-behaved client still gets served.
    let replier = server.listener.replier("/ok").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let body = request.body().read_capped(64).await.expect("body");
        let reply = request.reply(TransferMeta::default()).await.expect("reply");
        assert_eq!(body, b"ping");
        reply.finish().await.expect("finish");
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/ok")))
        .await
        .expect("connect");
    let reply = within(requester.request(b"ping")).await.expect("request");
    assert!(within(reply.collect(64)).await.expect("collect").is_empty());
    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn an_unknown_frame_kind_closes_the_connection() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Correct magic, undefined kind 9.
    raw::send_raw(&conn, &[MAGIC, 9, 0x00]).await;
    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn hello_with_an_unsupported_version_fails_negotiation() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let hello = Hello {
        versions: vec![99],
        max_header_bytes: 16 * 1024,
        max_transfers: 8,
        capabilities: vec![],
        required_capabilities: vec![],
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::NEGOTIATION_FAILED
    );
}

#[tokio::test]
async fn a_required_capability_we_lack_fails_negotiation() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    let hello = Hello {
        versions: vec![0],
        max_header_bytes: 16 * 1024,
        max_transfers: 8,
        capabilities: vec![7],
        required_capabilities: vec![7],
    };
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::NEGOTIATION_FAILED
    );
}

#[tokio::test]
async fn an_oversized_header_length_closes_the_connection_before_allocating() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // Claim a 1 MiB header against a 16 KiB cap, and send none of it.
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, 1024 * 1024, &mut bytes);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_malformed_cbor_header_closes_the_connection() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // A DATA preamble announcing three bytes of header, followed by bytes that
    // are not a CBOR map.
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, 3, &mut bytes);
    bytes.extend_from_slice(&[0xff, 0xff, 0xff]);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_zero_transfer_id_closes_the_connection() {
    let server = Server::start().await;
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Hand-encode `{0: "/t", 1: 0, 2: 1}`: transfer id 0 is reserved.
    let header = [0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x00, 0x02, 0x01];
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    raw::send_raw(&conn, &bytes).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_reserved_ack_mode_is_refused_with_unsupported() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // ack_mode = 2 (`stored`) is reserved in v0. The server must answer
    // UNSUPPORTED and refuse the payload, never silently downgrade it.
    let mut header = DataHeader::request("/t", TransferId::FIRST, AckMode::None);
    header.ack_mode = 2;
    let encoded = header.encode();
    let mut stream = conn.open_uni().await.expect("open uni");
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, encoded.len() as u64, &mut bytes);
    bytes.extend_from_slice(&encoded);
    stream.write_all(&bytes).await.expect("write header");

    // The ERROR frame arrives on its own stream, which is not necessarily the
    // next one: the server's HELLO is in flight too.
    let (_error_stream, error_header) = within(raw::accept_frame(&conn, FrameKind::Error)).await;
    let error = weida_protocol::ErrorHeader::decode(&error_header).expect("decode ERROR");
    assert_eq!(error.re.get(), 1);
    assert_eq!(error.code, weida::ErrorCode::Unsupported.to_wire());

    // And the payload is refused with STOP_SENDING(REJECTED).
    let stopped = within(async {
        let payload = vec![0u8; 1024 * 1024];
        loop {
            if let Err(e) = stream.write_all(&payload).await {
                return e;
            }
        }
    })
    .await;
    match stopped {
        quinn::WriteError::Stopped(code) => assert_eq!(code.into_inner(), codes::REJECTED),
        other => panic!("expected STOP_SENDING(REJECTED), got {other}"),
    }

    // The connection itself stays healthy: a refused transfer is not a
    // protocol violation.
    assert!(conn.close_reason().is_none());
}

#[tokio::test]
async fn an_ack_for_an_unknown_transfer_is_ignored() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/t").expect("replier");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Nothing is outstanding: this races legitimately with cancellation, so it
    // must be ignored rather than treated as an error.
    raw::send_frame(
        &conn,
        FrameKind::Ack,
        &AckHeader::accepted(TransferId::new(4242).expect("non-zero")).encode(),
    )
    .await;

    // Give the peer a chance to misbehave, then confirm it did not.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(conn.close_reason().is_none());
}

// --- hostile server against our client ----------------------------------

/// Spawns a raw server that runs `behaviour` for its first connection.
fn raw_server<F, Fut>(certs: &Certs, behaviour: F) -> std::net::SocketAddr
where
    F: FnOnce(quinn::Connection) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (endpoint, addr) = raw::server_endpoint(certs);
    tokio::spawn(async move {
        let incoming = endpoint.accept().await.expect("a connection");
        let conn = incoming.await.expect("handshake");
        behaviour(conn).await;
        // Keep the endpoint alive for the duration of the behaviour.
        drop(endpoint);
    });
    addr
}

fn client_for() -> Runtime {
    Runtime::new(RuntimeConfig::default()).expect("client runtime")
}

#[tokio::test]
async fn a_server_that_never_answers_yields_indeterminate() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        raw::send_hello(&conn).await;
        // Read the request to FIN, then vanish without an ACK or a reply.
        let (mut stream, header) = raw::accept_data(&conn).await;
        assert_eq!(header.endpoint.as_deref(), Some("/t"));
        let mut sink = vec![0u8; 64 * 1024];
        while let Ok(Some(n)) = stream.read(&mut sink).await {
            if n == 0 {
                break;
            }
        }
        conn.close(quinn::VarInt::from_u32(0), b"bye");
    });

    let client = client_for();
    let requester = client.requester(certs.client_tls());
    within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect("connect");

    let (mut transfer, pending) =
        within(requester.open(TransferMeta::default().with_ack(AckMode::Accepted)))
            .await
            .expect("open");
    within(transfer.write_all(b"payload")).await.expect("write");

    // The payload reached the peer, the answer did not: the outcome is
    // genuinely unknown (master doc §22).
    let err = within(transfer.finish())
        .await
        .expect_err("must not succeed");
    assert!(
        matches!(err, Error::Indeterminate),
        "expected Indeterminate, got {err:?}"
    );
    let err = within(pending.recv()).await.expect_err("must not succeed");
    assert!(
        matches!(err, Error::Indeterminate),
        "expected Indeterminate, got {err:?}"
    );
}

#[tokio::test]
async fn a_server_that_disappears_mid_stream_yields_connection_lost() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        raw::send_hello(&conn).await;
        let (mut stream, _) = raw::accept_data(&conn).await;
        // Read only a little, then drop the connection before the FIN.
        let mut sink = vec![0u8; 4096];
        let _ = stream.read(&mut sink).await;
        conn.close(quinn::VarInt::from_u32(0), b"gone");
    });

    let client = client_for();
    let requester = client.requester(certs.client_tls());
    within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect("connect");

    let (mut transfer, _pending) =
        within(requester.open(TransferMeta::default().with_ack(AckMode::Accepted)))
            .await
            .expect("open");

    // The connection dies before our FIN, so the transfer definitely did not
    // arrive: this is a definite failure, not an unknown outcome.
    let payload = vec![0x5au8; 4 * 1024 * 1024];
    let err = within(async {
        loop {
            if let Err(e) = transfer.write_all(&payload).await {
                return e;
            }
        }
    })
    .await;
    assert!(
        matches!(err, Error::ConnectionLost),
        "expected ConnectionLost, got {err:?}"
    );
    assert!(err.is_definite_failure());
}

#[tokio::test]
async fn a_server_that_never_sends_hello_is_dropped_after_the_timeout() {
    let certs = Certs::generate();
    let addr = raw_server(&certs, |conn| async move {
        // Stay silent: no HELLO, ever.
        conn.closed().await;
    });

    let limits = weida::Limits {
        hello_timeout_ms: 500,
        ..weida::Limits::default()
    };
    let client = Runtime::new(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })
    .expect("client runtime");

    let requester = client.requester(certs.client_tls());
    let err = within(requester.connect(&format!("weida://127.0.0.1:{}/t", addr.port())))
        .await
        .expect_err("connect must not succeed without negotiation");
    assert!(
        matches!(err, Error::Negotiation(_) | Error::ConnectionLost),
        "expected a negotiation failure, got {err:?}"
    );
}

// --- subscriptions and the oneshot role ---------------------------------

#[tokio::test]
async fn subscribe_with_an_oversized_filter_closes_the_connection() {
    let server = Server::start().await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // One byte over the 256-byte filter cap. The header is well-formed CBOR,
    // so only the decoder's per-field cap can catch it.
    let filter = "a".repeat(weida_protocol::header_limits::MAX_FILTER_BYTES + 1);
    let header = SubscriptionHeader::new("/md", filter).encode();
    raw::send_frame(&conn, FrameKind::Subscribe, &header).await;

    assert_eq!(
        within(raw::closed_code(&conn)).await,
        codes::PROTOCOL_VIOLATION
    );
}

#[tokio::test]
async fn a_subscribe_flood_closes_the_connection_with_limit_exceeded() {
    let limits = weida::Limits {
        max_subscriptions: 8,
        ..weida::Limits::default()
    };
    let server = Server::start_with(limits).await;
    let _publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Distinct filters, so none is deduplicated: one past the cap.
    for i in 0..=limits.max_subscriptions {
        let header = SubscriptionHeader::new("/md", format!("f{i}.")).encode();
        // The connection dies partway through, so a failed send is expected.
        let Ok(mut stream) = conn.open_uni().await else {
            break;
        };
        if stream
            .write_all(&weida_protocol::encode_frame(FrameKind::Subscribe, &header))
            .await
            .is_err()
        {
            break;
        }
        let _ = stream.finish();
    }

    // SUBSCRIBE carries no transfer id, so there is nothing to answer with an
    // ERROR frame: the connection is the only granularity available.
    assert_eq!(within(raw::closed_code(&conn)).await, codes::LIMIT_EXCEEDED);
}

#[tokio::test]
async fn unsubscribing_an_unknown_filter_is_ignored() {
    let server = Server::start().await;
    let publisher = server.listener.publisher("/md").expect("publisher");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // Neither the filter nor the connection is known to the registry.
    let header = SubscriptionHeader::new("/md", "never.").encode();
    raw::send_frame(&conn, FrameKind::Unsubscribe, &header).await;
    // An unknown path is equally harmless.
    let header = SubscriptionHeader::new("/nope", "x").encode();
    raw::send_frame(&conn, FrameKind::Unsubscribe, &header).await;

    // The connection survives and still accepts a real subscription.
    let header = SubscriptionHeader::new("/md", "px.").encode();
    raw::send_frame(&conn, FrameKind::Subscribe, &header).await;
    within(async {
        while publisher.filter_count() != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(conn.close_reason().is_none(), "the connection must survive");
}

#[tokio::test]
async fn oneshot_data_before_hello_is_parked_not_rejected() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");

    // DATA first, HELLO second. Unidirectional streams are unordered, so a
    // transfer that overtakes our HELLO must be parked until negotiation
    // completes, not refused. The new oneshot role is no exception.
    let header = DataHeader::oneshot("/jobs", TransferId::FIRST, AckMode::None).encode();
    let mut stream = conn.open_uni().await.expect("open uni");
    let mut bytes = Vec::new();
    encode_preamble(FrameKind::Data, header.len() as u64, &mut bytes);
    bytes.extend_from_slice(&header);
    stream.write_all(&bytes).await.expect("write header");

    // Give the server a chance to (wrongly) reject before our HELLO arrives.
    tokio::time::sleep(Duration::from_millis(50)).await;
    raw::send_hello(&conn).await;

    stream
        .write_all(b"parked payload")
        .await
        .expect("write body");
    stream.finish().expect("finish");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(transfer.meta().endpoint.as_deref(), Some("/jobs"));
    let body = within(transfer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"parked payload");
    assert!(conn.close_reason().is_none(), "the connection must survive");
}
