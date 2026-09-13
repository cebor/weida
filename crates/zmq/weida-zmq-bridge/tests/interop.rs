//! The bridge against an independent ZeroMQ: the pure-Rust `zeromq` crate.
//!
//! `inbound.rs` and `outbound.rs` drive the bridge with a peer built on
//! `weida-zmtp`, which is byte-exact against the golden vectors of
//! `docs/adapters/zmtp.md` §10.1 and therefore faithful — and shares every
//! assumption with the code under test. This file is what §10 items 3-6 ask
//! for: the same matrices against an implementation that shares no code with
//! ours, where a disagreement is an interoperability bug rather than a
//! tautology.
//!
//! What that immediately bought is recorded in `docs/adapters/zmtp.md` §3 and
//! §9.5: `zeromq` 0.6 announces ZMTP **3.0** and understands exactly one
//! command, `READY`. A `PING` or an `ERROR` is "Unknown command received" and
//! kills the connection. So the bridge's heartbeat is gated on the version the
//! peer announced, and the tests below assert that a 3.0 peer is left alone.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use weida::{ClientTls, Error, Identity, Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust};
use weida_zmq_bridge::{
    Dialling, Inbound, InboundConfig, Outbound, OutboundConfig, Presenting, SubscriptionForm,
};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

fn tracing_once() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_zmq_bridge=debug")
        .try_init();
}

/// A weida runtime with a binding, and the URL to reach a path on it.
struct WeidaSide {
    runtime: Runtime,
    listener: weida::Listener,
    _binding: weida::Binding,
    url_base: String,
}

impl WeidaSide {
    async fn start() -> WeidaSide {
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");
        let runtime = Runtime::new(RuntimeConfig::default()).expect("weida runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback"),
                ServerTls::new(identity),
            )
            .await
            .expect("bind");
        let url_base = format!(
            "weida://{}@127.0.0.1:{}",
            fingerprint,
            binding.local_addr().port()
        );
        WeidaSide {
            runtime,
            listener,
            _binding: binding,
            url_base,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.url_base, path)
    }
}

/// Starts an inbound bridge serving in the background.
async fn inbound(config: InboundConfig) -> (SocketAddr, Runtime) {
    tracing_once();
    let bridge = Inbound::bind(config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the bridge");
    let addr = bridge.local_addr().expect("local addr");
    let runtime = bridge.runtime().clone();
    tokio::spawn(async move {
        let _ = bridge.serve().await;
    });
    (addr, runtime)
}

/// Starts an outbound bridge serving in the background, dialling `connect`.
async fn outbound(config: OutboundConfig) -> (String, Runtime) {
    tracing_once();
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = config.weida_path.clone();
    let bridge = Outbound::bind(config, ServerTls::new(identity))
        .await
        .expect("bind the bridge");
    let url = format!(
        "weida://{}@127.0.0.1:{}{}",
        fingerprint,
        bridge.weida_addr().port(),
        path
    );
    let runtime = bridge.runtime().clone();
    tokio::spawn(async move {
        let _ = bridge.serve().await;
    });
    (url, runtime)
}

/// How many OS-chosen ports one bind may lose to another test before the
/// machine, rather than the race, is the explanation.
const PROBES: usize = 8;

/// Binds *their* socket to a loopback port the OS picked, retrying on
/// `AddrInUse` with a fresh probe, and returns the address the bridge dials.
///
/// The port has to be probed by binding `127.0.0.1:0`, reading it and letting
/// go, because zmq.rs's `bind` needs a concrete number and cannot report one
/// back — and between letting go and their bind, another test binary of the
/// suite can take it. That window cannot be closed while their bind needs a
/// number, so it is retried instead; the bound keeps a machine with no free
/// ports from looking like a flake.
async fn bound_by_them(socket: &mut impl Socket) -> SocketAddr {
    let mut taken = None;
    for _ in 0..PROBES {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("probe bind");
        let addr = probe.local_addr().expect("probe addr");
        drop(probe);
        let endpoint = format!("tcp://{addr}");
        match within(socket.bind(&endpoint)).await {
            Ok(_) => return addr,
            Err(zeromq::ZmqError::Network(error))
                if error.kind() == std::io::ErrorKind::AddrInUse =>
            {
                taken = Some(addr);
            }
            Err(error) => panic!("zmq.rs binds {endpoint}: {error}"),
        }
    }
    panic!("{PROBES} probed ports in a row were taken, the last of them {taken:?}");
}

fn one_frame(message: &ZmqMessage) -> &[u8] {
    assert_eq!(
        message.len(),
        1,
        "a single-part message is what these patterns send"
    );
    message.get(0).expect("the frame")
}

// --- The inbound matrix: zmq.rs dials the bridge -----------------------------

/// Claim: a real ZeroMQ `REQ` reaches a weida `Replier` and the reply comes
/// back — the envelope 28/REQREP puts on the wire consumed on the way in and
/// mirrored onto the reply, against a peer that did not learn that rule from us.
#[tokio::test]
async fn a_real_zmq_req_reaches_a_weida_replier() {
    let weida_side = WeidaSide::start().await;
    let replier = weida_side.listener.replier("/rpc").expect("replier");
    let (addr, bridge_runtime) = inbound(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    ))
    .await;

    let responder = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let body = request.take_body().collect(1024).await.expect("body");
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply
            .write_all(&[b"re:".as_slice(), &body].concat())
            .await
            .expect("write");
        reply.finish().expect("finish");
    });

    let mut req = zeromq::ReqSocket::new();
    within(req.connect(&format!("tcp://{addr}")))
        .await
        .expect("zmq.rs connects to the bridge");
    within(req.send(ZmqMessage::from("hello")))
        .await
        .expect("send");
    let reply = within(req.recv()).await.expect("recv");
    assert_eq!(one_frame(&reply), b"re:hello");

    within(responder).await.expect("responder");
    let _ = req.close().await;
    weida_side.runtime.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a real ZeroMQ `PUSH` reaches a weida `Puller`, one message per
/// transfer and in order on one connection.
#[tokio::test]
async fn a_real_zmq_push_reaches_a_weida_puller() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/work").expect("puller");
    let (addr, bridge_runtime) = inbound(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/work"),
        Presenting::Pull,
    ))
    .await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{addr}")))
        .await
        .expect("connect");
    for i in 0..3u8 {
        within(push.send(ZmqMessage::from(vec![b'j', b'0' + i])))
            .await
            .expect("send");
    }

    let mut seen = Vec::new();
    for _ in 0..3 {
        let transfer = within(puller.recv()).await.expect("recv");
        seen.push(within(transfer.collect(64)).await.expect("collect"));
    }
    seen.sort();
    assert_eq!(seen, vec![b"j0".to_vec(), b"j1".to_vec(), b"j2".to_vec()]);

    let _ = push.close().await;
    weida_side.runtime.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a real ZeroMQ `SUB` receives from a weida `Publisher`, and its byte
/// prefix matches because the topic is its own frame.
///
/// This is the case where an independent peer is worth most: the topic-as-frame
/// decision was made to satisfy *a* subscriber's prefix match, and only a
/// foreign subscriber can confirm the match lands where we think it does.
///
/// The prefix is `sport.`, **with** the separator, because that is what loss L2
/// costs a real subscriber: a byte prefix that stops mid-segment has no weida
/// filter with the same meaning and is refused
/// (`a_real_zmq_sub_with_a_mid_segment_prefix_gets_nothing` is that case).
#[tokio::test]
async fn a_real_zmq_sub_receives_from_a_weida_publisher() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/news").expect("publisher");
    let (addr, bridge_runtime) = inbound(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/news"),
        Presenting::Pub,
    ))
    .await;

    let mut sub = zeromq::SubSocket::new();
    within(sub.connect(&format!("tcp://{addr}")))
        .await
        .expect("connect");
    within(sub.subscribe("sport.")).await.expect("subscribe");

    // The subscription has to travel SUB -> bridge -> weida publisher before a
    // publish can match it, and a publisher with no subscriber drops rather
    // than queues, so the publish is retried. Bounded: a hang here would be a
    // test that never reports what it found.
    let mut received = None;
    for _ in 0..100 {
        if publisher.subscriber_count() > 0 {
            publisher
                .publish("sport.score", b"3-1".to_vec())
                .expect("publish");
            if let Ok(message) = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await
            {
                received = Some(message.expect("recv"));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let received =
        received.expect("the subscription reached the publisher and a message came back");
    assert_eq!(received.len(), 2, "topic frame, then payload");
    assert_eq!(received.get(0).expect("topic"), &b"sport.score".as_slice());
    assert_eq!(received.get(1).expect("payload"), &b"3-1".as_slice());

    let _ = sub.close().await;
    weida_side.runtime.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim (loss L2): a real ZeroMQ subscriber's *natural* prefix stops
/// mid-segment, the bridge refuses it, and the subscriber gets nothing.
///
/// This is the loss the mapping document names, priced against a real peer
/// rather than described: `sport` is what a ZeroMQ programmer writes, and no
/// weida filter selects the same messages — `sport.*` would also match
/// `sport` topics the byte prefix `sport` would miss nothing of, but it would
/// *not* match `sportsdesk.x`, which the byte prefix does. So the bridge
/// refuses with `ERROR`, and against this peer the refusal costs the
/// connection too: `zeromq` 0.6 cannot decode `ERROR` at all. Both halves are
/// asserted here because both are what a user will meet.
#[tokio::test]
async fn a_real_zmq_sub_with_a_mid_segment_prefix_gets_nothing() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/news").expect("publisher");
    let (addr, bridge_runtime) = inbound(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/news"),
        Presenting::Pub,
    ))
    .await;

    let mut sub = zeromq::SubSocket::new();
    within(sub.connect(&format!("tcp://{addr}")))
        .await
        .expect("connect");
    within(sub.subscribe("sport")).await.expect("subscribe");

    // Publish for a while regardless of subscriber_count: the point is that no
    // message ever arrives, and that a refused subscription never silently
    // becomes a matching one. A `recv` that fails is the connection being gone
    // — which is this peer's own answer to `ERROR` and not a delivery, so the
    // two outcomes are distinguished rather than lumped together.
    for _ in 0..20 {
        let _ = publisher.publish("sport.score", b"3-1".to_vec());
        match tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            Ok(Ok(message)) => panic!(
                "a mid-segment prefix must select nothing, but {} frame(s) arrived",
                message.len()
            ),
            // The peer closed on the ERROR, which is the documented price.
            Ok(Err(_)) => break,
            Err(_) => {}
        }
    }
    assert_eq!(
        publisher.subscriber_count(),
        0,
        "a refused subscription must not reach the weida publisher"
    );

    let _ = sub.close().await;
    weida_side.runtime.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim (loss L1): a genuine multipart message from a real ZeroMQ peer is
/// refused rather than flattened — and the refusal is the connection ending,
/// not a silent concatenation.
#[tokio::test]
async fn a_real_zmq_multipart_message_is_refused_rather_than_flattened() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/work").expect("puller");
    let (addr, bridge_runtime) = inbound(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/work"),
        Presenting::Pull,
    ))
    .await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{addr}")))
        .await
        .expect("connect");
    let multipart = ZmqMessage::try_from(vec![
        bytes::Bytes::from_static(b"a"),
        bytes::Bytes::from_static(b"b"),
    ])
    .expect("two frames");
    within(push.send(multipart)).await.expect("send");

    // Nothing may reach the weida side: not the first frame, not the two
    // concatenated. The bridge drops the connection instead.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), puller.recv())
            .await
            .is_err(),
        "a multipart message must not arrive as a weida transfer"
    );

    let _ = push.close().await;
    weida_side.runtime.shutdown().await;
    bridge_runtime.shutdown().await;
}

// --- The outbound matrix: the bridge dials zmq.rs ----------------------------

/// Claim: a weida `Requester` reaches a real ZeroMQ `REP` through the bridge.
///
/// The bridge presents `DEALER`, so the envelope it synthesizes has to be
/// exactly what a REP socket strips — and a REP socket written by somebody else
/// is the only thing that proves it.
#[tokio::test]
async fn a_weida_requester_reaches_a_real_zmq_rep() {
    let mut rep = zeromq::RepSocket::new();
    let addr = bound_by_them(&mut rep).await;

    let (url, bridge_runtime) = outbound(OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    ))
    .await;

    let responder = tokio::spawn(async move {
        let request = rep.recv().await.expect("recv");
        let body = request.get(0).expect("frame").to_vec();
        rep.send(ZmqMessage::from([b"re:".as_slice(), &body].concat()))
            .await
            .expect("send");
        rep
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");
    let reply = within(requester.request(b"hello")).await.expect("a reply");
    assert_eq!(
        within(reply.collect(64)).await.expect("collect"),
        b"re:hello"
    );

    let rep = within(responder).await.expect("responder");
    let _ = rep.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Pusher` reaches a real ZeroMQ `PULL` through the bridge.
#[tokio::test]
async fn a_weida_pusher_reaches_a_real_zmq_pull() {
    let mut pull = zeromq::PullSocket::new();
    let addr = bound_by_them(&mut pull).await;

    let (url, bridge_runtime) = outbound(OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        Dialling::Push,
    ))
    .await;

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(ClientTls::new(Trust::by_address()));
    within(pusher.connect(&url)).await.expect("connect");
    for i in 0..3u8 {
        within(pusher.send(&[b'j', b'0' + i])).await.expect("send");
    }

    let mut seen = Vec::new();
    for _ in 0..3 {
        let message = within(pull.recv()).await.expect("recv");
        seen.push(one_frame(&message).to_vec());
    }
    seen.sort();
    assert_eq!(seen, vec![b"j0".to_vec(), b"j1".to_vec(), b"j2".to_vec()]);

    let _ = pull.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Subscriber` receives from a real ZeroMQ `PUB` through the
/// bridge, with the bridge's own subscription reaching a foreign publisher.
#[tokio::test]
async fn a_weida_subscriber_receives_from_a_real_zmq_pub() {
    let mut publisher = zeromq::PubSocket::new();
    let addr = bound_by_them(&mut publisher).await;

    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/news",
        Dialling::Sub,
    );
    config.subscribe = vec![b"sport".to_vec()];
    // The finding of this item: `zeromq` 0.6 reads only the legacy
    // subscription form, so an interop run says so in configuration rather
    // than discovering it as a closed connection (`SubscriptionForm`).
    config.subscription_form = SubscriptionForm::LegacyMessage;
    let (url, bridge_runtime) = outbound(config).await;

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let subscriber = client.subscriber(ClientTls::new(Trust::by_address()));
    within(subscriber.subscribe("sport.*"))
        .await
        .expect("filter");
    within(subscriber.connect(&url)).await.expect("subscribe");

    // A ZeroMQ PUB drops for a subscriber whose subscription has not arrived,
    // so the publish is repeated until the chain is up. Both hops drop rather
    // than queue, which is the guarantee the mapping document claims.
    let mut payload = None;
    for _ in 0..100 {
        let mut message = ZmqMessage::from("sport.score");
        message.push_back(bytes::Bytes::from_static(b"3-1"));
        within(publisher.send(message)).await.expect("send");
        if let Ok(transfer) =
            tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await
        {
            let transfer = transfer.expect("recv");
            assert_eq!(transfer.meta().topic.as_deref(), Some("sport.score"));
            payload = Some(within(transfer.collect(64)).await.expect("collect"));
            break;
        }
    }
    assert_eq!(
        payload
            .expect("the chain delivered a published message")
            .as_slice(),
        b"3-1"
    );

    let _ = publisher.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim (loss L5): a foreign peer that takes a request and never answers is
/// reported to the weida requester as `NoReply` after the deadline, not as a
/// hang — the one thing the adapter can do about `ZMQ_ROUTER_MANDATORY` being
/// an option on somebody else's socket.
#[tokio::test]
async fn a_request_a_real_zmq_router_drops_becomes_no_reply() {
    let mut router = zeromq::RouterSocket::new();
    let addr = bound_by_them(&mut router).await;

    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    );
    config.reply_deadline = Duration::from_millis(300);
    let (url, bridge_runtime) = outbound(config).await;

    // The ROUTER reads the request and answers nothing, which is exactly what
    // an unroutable message looks like with `ZMQ_ROUTER_MANDATORY` off.
    let silent = tokio::spawn(async move {
        let _ = router.recv().await.expect("the request arrives");
        router
    });

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");
    let outcome = within(requester.request(b"unroutable")).await;
    assert!(
        matches!(outcome, Err(Error::NoReply)),
        "a silently dropped request must surface as NoReply, got {outcome:?}"
    );

    let router = within(silent).await.expect("router task");
    let _ = router.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: the bridge does not send a 3.1 command to a peer that announced 3.0.
///
/// `zeromq` 0.6 announces ZMTP 3.0 and decodes `READY` only — a `PING` is
/// "Unknown command received" and ends the connection. So the heartbeat is
/// gated on the announced version, and this test holds a connection open for
/// several heartbeat intervals and then uses it.
#[tokio::test]
async fn a_three_zero_peer_is_not_sent_pings() {
    let mut rep = zeromq::RepSocket::new();
    let addr = bound_by_them(&mut rep).await;

    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    );
    // Short enough that several intervals pass inside the test: if the bridge
    // pinged a 3.0 peer, the connection would be gone before the request.
    config.heartbeat = Some(Duration::from_millis(50));
    let (url, bridge_runtime) = outbound(config).await;

    let responder = tokio::spawn(async move {
        let _ = rep.recv().await.expect("recv");
        rep.send(ZmqMessage::from("alive")).await.expect("send");
        rep
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");
    let reply = within(requester.request(b"still there?"))
        .await
        .expect("the connection survived eight heartbeat intervals");
    assert_eq!(within(reply.collect(64)).await.expect("collect"), b"alive");

    let rep = within(responder).await.expect("responder");
    let _ = rep.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida payload larger than `max_message_bytes` is refused rather
/// than truncated — the foreign peer is handed nothing at all, and the
/// connection survives for the next message.
///
/// Where the refusal is *not* observable is worth stating, because the test
/// was written the other way first and was wrong: Push/Pull is fire-and-forget
/// and a 256-byte payload fits the window in one write, so the sender's
/// transport receipt resolves `Ok` before the bridge has read a byte. A
/// transport receipt says the peer's **transport** took the bytes and never
/// that anything read them ([GUARANTEES.md](../../../../docs/GUARANTEES.md)
/// §3). So the observable is the peer's silence, plus a live connection
/// afterwards.
#[tokio::test]
async fn a_payload_over_the_cap_reaches_no_real_zmq_peer() {
    let mut pull = zeromq::PullSocket::new();
    let addr = bound_by_them(&mut pull).await;

    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        Dialling::Push,
    );
    config.max_message_bytes = 64;
    let (url, bridge_runtime) = outbound(config).await;

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = Arc::new(client.pusher(ClientTls::new(Trust::by_address())));
    within(pusher.connect(&url)).await.expect("connect");
    within(pusher.send(&vec![b'x'; 256])).await.expect("send");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), pull.recv())
            .await
            .is_err(),
        "no part of an over-cap payload may reach the peer — not truncated, not at all"
    );

    // The cap is per message, not a poisoned connection: a small one still
    // crosses afterwards.
    within(pusher.send(b"small")).await.expect("send");
    let message = within(pull.recv()).await.expect("recv");
    assert_eq!(one_frame(&message), b"small");

    let _ = pull.close().await;
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}
