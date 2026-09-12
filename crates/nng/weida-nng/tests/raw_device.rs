//! Raw sockets and the device, over real connections.

use std::time::Duration;

use weida_nng::{
    Context, ContextConfig, Error, Message, RawSocket, RepSocket, ReqSocket, SocketOptions, raw,
};
use weida_sp::{Backtrace, EndpointType, backtrace};

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        resend_time: Duration::from_secs(30),
        ..SocketOptions::default()
    }
}

/// Claim: a raw socket refuses a context, and the refusal names the reason
/// the state a context holds is absent (§4).
#[tokio::test]
async fn a_raw_socket_refuses_a_context() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    for protocol in [
        EndpointType::Req,
        EndpointType::Rep,
        EndpointType::Surveyor,
        EndpointType::Respondent,
        EndpointType::Bus,
    ] {
        let socket = RawSocket::open(&ctx, protocol).expect("raw socket");
        let err = socket.context().unwrap_err();
        assert!(matches!(err, Error::ENOTSUP(_)), "{protocol:?}: {err:?}");
        assert!(err.cause().contains("holds no protocol state"));
    }
}

/// Claim: a raw socket preserves the wire header. A raw REP receives the
/// tag stack rather than a stripped payload, and a raw send writes back
/// exactly what it is given — which is what makes a forwarder possible at
/// all (§4).
#[tokio::test]
async fn a_raw_socket_hands_over_the_wire_header_untouched() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let raw_rep = RawSocket::with_options(&ctx, EndpointType::Rep, options()).expect("raw rep");
    let url = raw_rep
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    client.dial(&url).await.expect("dial");
    client.send(b"ping".to_vec()).await.expect("send");

    let (pipe, request) = raw_rep.recv().await.expect("the raw request");
    assert!(
        request.header().is_empty(),
        "raw mode claims nothing: the whole payload is the body"
    );
    let (stack, payload) = backtrace::decode(request.body(), 8).expect("the stack is still there");
    assert_eq!(payload, b"ping");
    assert_eq!(stack.peers, Vec::<u32>::new());

    // Answer by hand: the application owns the header in raw mode.
    let reply = Message::from_body(stack.encode_message(b"pong"));
    raw_rep.send_to(pipe, reply).await.expect("reply");
    assert_eq!(client.recv().await.expect("reply").body(), b"pong");
}

/// Claim: a device forwards a request through and a reply back, and the
/// forwarder's **own peer id was pushed on the way in and popped on the
/// way out** — the mechanism that routes a reply with no global address
/// anywhere (§4).
#[tokio::test]
async fn a_request_through_a_device_is_pushed_and_popped() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // The device: a raw REP facing the clients, a raw REQ facing the
    // servers, which is the load-balancing intermediary of §9.
    let front = RawSocket::with_options(&ctx, EndpointType::Rep, options()).expect("raw rep");
    let back = RawSocket::with_options(&ctx, EndpointType::Req, options()).expect("raw req");
    let front_url = front
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let back_url = back
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let forwarding = {
        let front = front.clone();
        let back = back.clone();
        tokio::spawn(async move { raw::device(&front, &back).await })
    };

    let server = RepSocket::with_options(&ctx, options()).expect("rep");
    server.dial(&back_url).await.expect("dial the device");
    let client = ReqSocket::with_options(&ctx, options()).expect("req");
    client.dial(&front_url).await.expect("dial the device");
    while front.pipe_count() < 1 || back.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    client.send(b"work".to_vec()).await.expect("send");

    let request = server.recv().await.expect("the request arrived");
    assert_eq!(request.body(), b"work");
    let (stack, _) = backtrace::decode(request.header(), 8).expect("the stack");
    assert_eq!(
        stack.peers.len(),
        1,
        "the forwarder pushed exactly its own peer id: {stack:?}"
    );
    let pushed = stack.peers[0];

    server.send(b"done".to_vec()).await.expect("reply");
    let reply = server_reply(&client).await;
    let (reply_stack, _) = backtrace::decode(reply.header(), 8).expect("the stack");
    assert!(
        reply_stack.peers.is_empty(),
        "the forwarder popped its own id again; the originator sees only its request ID: \
         {reply_stack:?}"
    );
    assert_eq!(reply_stack.id, stack.id, "and the same request ID");
    assert_ne!(pushed, 0, "the pushed id was a real pipe id");
    assert_eq!(reply.body(), b"done");

    forwarding.abort();
}

async fn server_reply(client: &ReqSocket) -> Message {
    tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("the reply came back through the device")
        .expect("a reply")
}

/// Claim: a device between sockets that cannot pair is refused, because it
/// would forward messages nobody can interpret.
#[tokio::test]
async fn a_device_between_incompatible_sockets_is_refused() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RawSocket::open(&ctx, EndpointType::Rep).expect("raw rep");
    let pull = RawSocket::open(&ctx, EndpointType::Pull).expect("raw pull");
    let err = raw::device(&rep, &pull).await.unwrap_err();
    assert!(matches!(err, Error::ENOTSUP(_)), "{err:?}");
    assert!(err.cause().contains("pair"));
}

/// Claim: a device increments the PAIR v1 hop count and drops a message
/// past its own `NNG_OPT_MAXTTL`, with nothing sent back — the loop
/// control PAIR v1 exists for (§4, §8).
#[tokio::test]
async fn a_device_bounds_the_pair_v1_hop_count() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let ttl = SocketOptions {
        max_ttl: 3,
        ..options()
    };
    let left = RawSocket::with_options(&ctx, EndpointType::PairV1, ttl.clone()).expect("raw pair1");
    let right = RawSocket::with_options(&ctx, EndpointType::PairV1, ttl).expect("raw pair1");
    let left_url = left
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let right_url = right
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let forwarding = {
        let left = left.clone();
        let right = right.clone();
        tokio::spawn(async move { raw::device(&left, &right).await })
    };

    let sender = RawSocket::with_options(&ctx, EndpointType::PairV1, options()).expect("raw pair1");
    let receiver =
        RawSocket::with_options(&ctx, EndpointType::PairV1, options()).expect("raw pair1");
    sender.dial(&left_url).await.expect("dial");
    receiver.dial(&right_url).await.expect("dial");
    while left.pipe_count() < 1 || right.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A message that has travelled once already: the device makes it two.
    sender
        .send(Message::from_body(
            [1u32.to_be_bytes().as_slice(), b"onward"].concat(),
        ))
        .await
        .expect("send");
    let (_, forwarded) = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("forwarded")
        .expect("a message");
    assert_eq!(
        &forwarded.body()[..4],
        2u32.to_be_bytes(),
        "the device incremented the hop count"
    );
    assert_eq!(&forwarded.body()[4..], b"onward");

    // And one that is already at the ceiling is dropped silently.
    sender
        .send(Message::from_body(
            [3u32.to_be_bytes().as_slice(), b"too far"].concat(),
        ))
        .await
        .expect("send");
    let nothing = tokio::time::timeout(Duration::from_millis(400), receiver.recv()).await;
    assert!(
        nothing.is_err(),
        "a message past the device's MAXTTL must be dropped and not forwarded"
    );
    assert_eq!(receiver.pipe_count(), 1, "and the pipe survives the drop");

    forwarding.abort();
}

/// Claim: a raw send writes the header it is given and nothing else, so a
/// tag stack an application built by hand arrives byte for byte.
#[tokio::test]
async fn a_raw_send_writes_exactly_what_it_is_given() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let url = rep
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let raw_req = RawSocket::with_options(&ctx, EndpointType::Req, options()).expect("raw req");
    raw_req.dial(&url).await.expect("dial");

    // A stack with a forwarder id in it, invented by hand — raw mode means
    // nobody checks.
    let mut stack = Backtrace::direct(0x1234);
    stack.push_peer(0x0BAD);
    raw_req
        .send(Message::from_body(stack.encode_message(b"by hand")))
        .await
        .expect("send");

    let request = rep.recv().await.expect("the cooked replier");
    assert_eq!(request.body(), b"by hand");
    let (seen, _) = backtrace::decode(request.header(), 8).expect("the stack");
    assert_eq!(seen.peers, vec![0x0BAD]);
    assert_eq!(seen.id, 0x1234);
}
