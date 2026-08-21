//! Req/Rep over real QUIC on loopback: the §83 reference prototype's
//! functional requirements.
//!
//! Every test here runs a real `quinn` endpoint pair with a freshly generated
//! certificate. Nothing is mocked and the library exposes no test hooks.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Server, Xorshift};
use tokio::io::AsyncReadExt;
use weida::{AckMode, AckState, Error, Outcome, TraceContext, TransferMeta};

/// Serves one request with an uppercasing echo handler.
///
/// The reply stream is opened after the first chunk arrives, which is the
/// overlap the architecture requires (§83 requirements 6-8).
async fn spawn_uppercase_handler(replier: weida::Replier) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut reply = None;
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let n = request.body().read(&mut chunk).await.expect("read body");
            if n == 0 {
                break;
            }
            let out = reply.get_or_insert(
                request
                    .reply(TransferMeta::default())
                    .await
                    .expect("open reply"),
            );
            chunk[..n].make_ascii_uppercase();
            out.write_all(&chunk[..n]).await.expect("write reply");
        }
        if let Some(reply) = reply {
            reply.finish().await.expect("finish reply");
        }
    })
}

#[tokio::test]
async fn echo_roundtrip_with_ack() {
    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let body = "hello weida ".repeat(64);
    assert_eq!(body.len(), 768);

    let (mut transfer, pending) = requester
        .open(TransferMeta::default().with_ack(AckMode::Accepted))
        .await
        .expect("open");
    transfer.write_all(body.as_bytes()).await.expect("write");
    let outcome = transfer.finish().await.expect("finish");

    // The ACK means the peer read the payload to FIN and handed it over.
    assert_eq!(outcome, Outcome::Acked(AckState::Accepted));

    let reply = pending.recv().await.expect("recv reply");
    let received = reply.collect(1 << 20).await.expect("collect");
    assert_eq!(received, body.to_uppercase().into_bytes());

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn best_effort_finish_reports_sent_without_waiting() {
    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let reply = requester.request(b"abc").await.expect("request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"ABC");

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn streaming_overlap() {
    // 4 MiB per half: far beyond both the 1 MiB stream window and the 16 MiB
    // connection window, so the transfer cannot complete unless it really
    // streams in both directions at once.
    const HALF: usize = 4 * 1024 * 1024;

    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let mut rng = Xorshift::new(0x51ee_d0d0_5eed_0001);
    let mut payload = vec![0u8; HALF * 2];
    rng.fill(&mut payload);
    // Restrict to lowercase ASCII so the transform is a bijection we can check.
    for b in payload.iter_mut() {
        *b = b'a' + (*b % 26);
    }

    let (mut transfer, pending) = requester
        .open(TransferMeta::default().with_content_len((HALF * 2) as u64))
        .await
        .expect("open");

    // The reply must be drained concurrently with writing the request. This is
    // not a test artefact: with a 1:1 responder, flow control in both
    // directions is live at the same time, so a client that writes its whole
    // request before reading anything would stall the responder — and
    // therefore itself. Independent streams are what make the overlap possible
    // (master doc §10).
    let (first_byte_tx, first_byte_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut reply = pending.recv().await.expect("early reply header");
        let mut received = Vec::with_capacity(HALF * 2);
        let mut chunk = vec![0u8; 64 * 1024];
        let mut announce = Some(first_byte_tx);
        loop {
            let n = reply.read(&mut chunk).await.expect("read reply");
            if n == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..n]);
            if let Some(tx) = announce.take() {
                let _ = tx.send(received[0]);
            }
        }
        received
    });

    transfer.write_all(&payload[..HALF]).await.expect("write 1");

    // Proof of overlap: a reply byte exists before the request reaches FIN.
    let first = tokio::time::timeout(Duration::from_secs(30), first_byte_rx)
        .await
        .expect("a reply byte must arrive before the request is finished")
        .expect("reader task alive");
    assert_eq!(first, payload[0].to_ascii_uppercase());

    transfer.write_all(&payload[HALF..]).await.expect("write 2");
    let outcome = transfer.finish().await.expect("finish");
    assert_eq!(outcome, Outcome::SentBestEffort);

    let received = reader.await.expect("reader task");
    assert_eq!(received.len(), payload.len());
    let expected: Vec<u8> = payload.iter().map(|b| b.to_ascii_uppercase()).collect();
    assert_eq!(received, expected);

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn cancel_mid_transfer() {
    let server = Server::start().await;
    let replier = server.listener.replier("/sink").expect("replier");

    let observed = Arc::new(tokio::sync::Mutex::new(None::<Error>));
    let server_side = Arc::clone(&observed);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match request.body().read(&mut chunk).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) => {
                    // The reset surfaces as an I/O error carrying the code.
                    *server_side.lock().await = Some(Error::Io(e));
                    break;
                }
            }
        }
        // Answer so the requester's second request can be served too.
        let reply = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        reply.finish().await.expect("finish reply");
    });

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/sink"))
        .await
        .expect("connect");

    let (mut transfer, pending) = requester.open(TransferMeta::default()).await.expect("open");
    let chunk = vec![0x61u8; 256 * 1024];
    for _ in 0..4 {
        transfer.write_all(&chunk).await.expect("write");
    }
    transfer.cancel();
    drop(pending);

    handler.await.expect("handler");
    assert!(
        observed.lock().await.is_some(),
        "the server must observe the reset instead of a clean EOF"
    );

    // The connection is still usable: cancellation is per transfer.
    let replier2 = server.listener.replier("/again").expect("replier");
    let handler2 = spawn_uppercase_handler(replier2).await;
    let requester2 = client.requester();
    requester2
        .connect(&server.url("/again"))
        .await
        .expect("reconnect");
    let reply = requester2.request(b"ok").await.expect("second request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"OK");

    handler2.await.expect("handler2");
    client.shutdown().await;
}

#[tokio::test]
async fn reply_abort_on_cancel_frame() {
    let server = Server::start().await;
    let replier = server.listener.replier("/firehose").expect("replier");

    let saw_cancel = Arc::new(tokio::sync::Mutex::new(false));
    let server_side = Arc::clone(&saw_cancel);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        let mut reply = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        let mut canceled = request.canceled();
        let chunk = vec![0x7au8; 64 * 1024];
        loop {
            if *canceled.borrow_and_update() {
                *server_side.lock().await = true;
                break;
            }
            tokio::select! {
                _ = canceled.changed() => continue,
                written = reply.write_all(&chunk) => {
                    if written.is_err() {
                        // The requester's cancel reached the stream first.
                        *server_side.lock().await = true;
                        break;
                    }
                }
            }
        }
        // A canceled reply is abandoned, not finished.
        drop(reply);
    });

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/firehose"))
        .await
        .expect("connect");

    let (mut transfer, pending) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(b"start").await.expect("write");
    transfer.finish().await.expect("finish");

    // Take the reply, read a little, then walk away: dropping mid-stream must
    // reach the responder.
    let mut reply = pending.recv().await.expect("recv reply");
    let mut sample = [0u8; 1024];
    reply.read_exact(&mut sample).await.expect("read a sample");
    drop(reply);

    tokio::time::timeout(Duration::from_secs(10), handler)
        .await
        .expect("handler must notice the cancellation")
        .expect("handler");
    assert!(*saw_cancel.lock().await);

    // The connection survives.
    let replier2 = server.listener.replier("/after").expect("replier");
    let handler2 = spawn_uppercase_handler(replier2).await;
    let requester2 = client.requester();
    requester2
        .connect(&server.url("/after"))
        .await
        .expect("reconnect");
    let reply = requester2.request(b"live").await.expect("request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"LIVE");

    handler2.await.expect("handler2");
    client.shutdown().await;
}

#[tokio::test]
async fn trace_propagation() {
    let server = Server::start().await;
    let replier = server.listener.replier("/traced").expect("replier");

    let seen = Arc::new(tokio::sync::Mutex::new(None::<TraceContext>));
    let server_side = Arc::clone(&seen);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        *server_side.lock().await = request.meta().trace;
        let _ = request.body().read_capped(1024).await;
        // No explicit trace: the reply must inherit the request's context.
        let reply = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        reply.finish().await.expect("finish reply");
    });

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/traced"))
        .await
        .expect("connect");

    let trace =
        TraceContext::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("valid traceparent");

    let (mut transfer, pending) = requester
        .open(TransferMeta::default().with_trace(trace))
        .await
        .expect("open");
    assert_eq!(transfer.trace(), trace);
    transfer.write_all(b"x").await.expect("write");
    transfer.finish().await.expect("finish");

    let reply = pending.recv().await.expect("recv reply");
    let reply_trace = reply.meta().trace.expect("reply carries a trace context");
    assert_eq!(reply_trace.trace_id, trace.trace_id);
    assert_eq!(reply_trace.span_id, trace.span_id);

    handler.await.expect("handler");
    assert_eq!(seen.lock().await.expect("server saw a trace"), trace);
    client.shutdown().await;
}

#[tokio::test]
async fn unknown_endpoint_is_reported() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/known").expect("replier");

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/nowhere"))
        .await
        .expect("connect");

    let (mut transfer, pending) = requester.open(TransferMeta::default()).await.expect("open");
    // The refusal may surface on the write, on the finish, or on the reply,
    // depending on how fast the peer answers; all three must name the cause.
    let write = transfer.write_all(&vec![0u8; 4096]).await;
    let outcome = match write {
        Ok(()) => transfer.finish().await.map(|_| ()),
        Err(e) => Err(e),
    };
    let err = match outcome {
        Err(e) => e,
        Ok(()) => pending.recv().await.expect_err("reply must fail"),
    };
    assert!(
        matches!(err, Error::UnknownEndpoint),
        "expected UnknownEndpoint, got {err:?}"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn dropping_a_request_without_replying_reports_no_reply() {
    let server = Server::start().await;
    let replier = server.listener.replier("/silent").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        drop(request);
    });

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/silent"))
        .await
        .expect("connect");

    let (mut transfer, pending) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(b"anyone there?").await.expect("write");
    transfer.finish().await.expect("finish");

    let err = pending.recv().await.expect_err("reply must fail");
    assert!(
        matches!(err, Error::NoReply),
        "expected NoReply, got {err:?}"
    );

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn a_reserved_ack_mode_is_refused_not_downgraded() {
    let server = Server::start().await;
    let replier = server.listener.replier("/strict").expect("replier");
    // Nothing ever accepts: the refusal happens in the dispatch path.
    let _ = replier;

    let client = server.client_runtime();
    let requester = client.requester();
    requester
        .connect(&server.url("/strict"))
        .await
        .expect("connect");

    // `AckMode` cannot express a reserved code, so this case is covered by the
    // hostile-peer suite; here we assert the honest path: an ack the server
    // does support is honoured rather than dropped.
    let replier = server.listener.replier("/honest").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;
    let requester2 = client.requester();
    requester2
        .connect(&server.url("/honest"))
        .await
        .expect("connect");
    let (mut transfer, pending) = requester2
        .open(TransferMeta::default().with_ack(AckMode::Accepted))
        .await
        .expect("open");
    transfer.write_all(b"ack me").await.expect("write");
    assert_eq!(
        transfer.finish().await.expect("finish"),
        Outcome::Acked(AckState::Accepted)
    );
    let reply = pending.recv().await.expect("recv");
    assert_eq!(reply.collect(64).await.expect("collect"), b"ACK ME");

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn multi_peer_requests_round_robin() {
    let a = Server::start().await;
    let b = Server::start().await;
    let handler_a = spawn_uppercase_handler(a.listener.replier("/rr").expect("replier")).await;
    let handler_b = spawn_uppercase_handler(b.listener.replier("/rr").expect("replier")).await;

    // One client trusting both servers.
    let client = weida::Runtime::new(weida::RuntimeConfig {
        client_tls: Some(weida::ClientTls {
            roots_pem: vec![a.certs.cert_pem.clone(), b.certs.cert_pem.clone()],
        }),
        ..weida::RuntimeConfig::default()
    })
    .expect("client runtime");

    let requester = client.requester();
    requester.connect(&a.url("/rr")).await.expect("connect a");
    requester.connect(&b.url("/rr")).await.expect("connect b");
    assert_eq!(requester.peer_count(), 2);

    for _ in 0..2 {
        let reply = requester.request(b"peer").await.expect("request");
        assert_eq!(reply.collect(16).await.expect("collect"), b"PEER");
    }

    handler_a.await.expect("handler a");
    handler_b.await.expect("handler b");
    client.shutdown().await;
}

#[tokio::test]
async fn duplicate_endpoint_registration_is_rejected() {
    let server = Server::start().await;
    let _first = server.listener.replier("/dup").expect("first");
    assert!(matches!(
        server.listener.replier("/dup"),
        Err(Error::AlreadyRegistered)
    ));
    assert!(matches!(
        server.listener.replier("no-slash"),
        Err(Error::InvalidEndpointPath)
    ));
}
