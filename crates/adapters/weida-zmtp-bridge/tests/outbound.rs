//! weida endpoints against a foreign ZeroMQ peer, through the outbound bridge.
//!
//! The mirror of `inbound.rs`, and the peer is the mirror too: here the ZMTP
//! side **accepts** and the bridge dials it. Same caveat as there — the peer is
//! built on this repository's codec, so it is faithful on the wire and not an
//! independent implementation; that is what slice 5's `zeromq` run is for.

use std::net::SocketAddr;
use std::time::Duration;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use weida::{ClientTls, Error, Identity, Runtime, RuntimeConfig, ServerTls, Trust};
use weida_zmtp::{Command, FrameKind, Greeting, Mechanism, Metadata, SocketType, frame, greeting};
use weida_zmtp_bridge::{Dialling, Outbound, OutboundConfig};

const DEADLINE: Duration = Duration::from_secs(10);
const CAP: u64 = 1024 * 1024;

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A ZeroMQ peer that accepts one connection and speaks `ours`.
struct Peer {
    io: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    /// Binds, and returns the address plus a future that completes the
    /// handshake once the bridge dials.
    async fn listen() -> (SocketAddr, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the ZeroMQ side");
        let addr = listener.local_addr().expect("local addr");
        (addr, listener)
    }

    async fn accept(listener: &TcpListener, ours: SocketType) -> Peer {
        let (io, _) = listener.accept().await.expect("accept the bridge");
        let mut peer = Peer {
            io,
            buf: Vec::new(),
        };
        peer.io
            .write_all(&Greeting::null().encode())
            .await
            .expect("greeting");
        let mut theirs = [0u8; greeting::GREETING_LEN];
        peer.read_exactly(&mut theirs)
            .await
            .expect("their greeting");
        Greeting::decode(&theirs)
            .expect("a greeting")
            .accept(Mechanism::NULL)
            .expect("NULL both ways");
        peer.send_command(&Command::Ready(Metadata::new().with_socket_type(ours)))
            .await;
        let body = peer.read_command().await.expect("the bridge's READY");
        match Command::decode(&body).expect("decode") {
            Command::Ready(metadata) => assert!(metadata.socket_type().is_some()),
            other => panic!("expected READY, got {}", other.name()),
        }
        peer
    }

    async fn send_command(&mut self, command: &Command<'_>) {
        let bytes = command.encode().expect("encode");
        self.io.write_all(&bytes).await.expect("write command");
    }

    async fn send_message(&mut self, parts: &[&[u8]]) {
        let (last, leading) = parts.split_last().expect("at least one frame");
        let mut out = Vec::new();
        for part in leading {
            out.extend_from_slice(&frame::encode(FrameKind::Message { more: true }, part));
        }
        out.extend_from_slice(&frame::encode(FrameKind::Message { more: false }, last));
        self.io.write_all(&out).await.expect("write message");
    }

    async fn read_frame(&mut self) -> std::io::Result<(FrameKind, Vec<u8>)> {
        loop {
            match frame::decode(&self.buf, CAP) {
                Ok((header, body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok((header.kind, body));
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
        }
    }

    /// Reads one whole message, answering any command that arrives first —
    /// the bridge heartbeats, so a `PING` may land between messages.
    async fn read_message(&mut self) -> std::io::Result<Vec<Vec<u8>>> {
        let mut parts = Vec::new();
        loop {
            let (kind, body) = self.read_frame().await?;
            match kind {
                FrameKind::Command => {
                    if let Ok(Command::Ping { context, .. }) = Command::decode(&body) {
                        self.send_command(&Command::Pong { context }).await;
                    }
                }
                FrameKind::Message { more } => {
                    parts.push(body);
                    if !more {
                        return Ok(parts);
                    }
                }
            }
        }
    }

    async fn read_command(&mut self) -> std::io::Result<Vec<u8>> {
        loop {
            let (kind, body) = self.read_frame().await?;
            if kind == FrameKind::Command {
                return Ok(body);
            }
        }
    }

    async fn read_exactly(&mut self, out: &mut [u8]) -> std::io::Result<()> {
        while self.buf.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.buf[..out.len()]);
        self.buf.drain(..out.len());
        Ok(())
    }

    async fn fill(&mut self) -> std::io::Result<()> {
        let at = self.buf.len();
        let mut chunk = vec![0u8; 16 * 1024];
        let n = self.io.read(&mut chunk).await?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        self.buf.truncate(at);
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }
}

/// Starts an outbound bridge and returns the weida URL to dial and its runtime.
async fn bridge(config: OutboundConfig) -> (String, Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_zmtp_bridge=debug")
        .try_init();
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = config.weida_path.clone();
    let outbound = Outbound::bind(config, ServerTls::new(identity))
        .await
        .expect("bind the bridge");
    let url = format!(
        "weida://{}@127.0.0.1:{}{}",
        fingerprint,
        outbound.weida_addr().port(),
        path
    );
    let runtime = outbound.runtime().clone();
    tokio::spawn(async move {
        let _ = outbound.serve().await;
    });
    (url, runtime)
}

fn client() -> Runtime {
    Runtime::new(RuntimeConfig::default()).expect("client runtime")
}

/// Claim: a weida `Requester` reaches a foreign REP, and the reply comes back —
/// with the correlation envelope 28/REQREP returns, so **concurrent** weida
/// exchanges are matched by id rather than by arrival order.
///
/// Four exchanges rather than two, because two cannot distinguish a correct
/// implementation from a coin flip: an id-blind bridge that answered whichever
/// exchange its table yielded first would pair two of them correctly half the
/// time. With four, answered in reverse, it would have to guess right four
/// times running.
#[tokio::test]
async fn a_weida_requester_reaches_a_foreign_rep_and_exchanges_are_correlated() {
    const EXCHANGES: usize = 4;

    let (addr, zmq) = Peer::listen().await;
    let (url, bridge_runtime) = bridge(OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    ))
    .await;
    let mut peer = Peer::accept(&zmq, SocketType::Rep).await;

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    // All four in flight at once, each with its own body so a mispairing is
    // visible in the payload and not only in the timing.
    let mut tasks = Vec::new();
    let mut envelopes = Vec::new();
    for i in 0..EXCHANGES {
        let requester = Arc::clone(&requester);
        let body = format!("req-{i}");
        tasks.push(tokio::spawn(async move {
            requester.request(body.as_bytes()).await
        }));
        let parts = within(peer.read_message()).await.expect("a request");
        assert_eq!(parts.len(), 3, "[id, delimiter, body]");
        assert!(parts[1].is_empty(), "the delimiter ends the envelope");
        assert_eq!(parts[2], format!("req-{i}").as_bytes());
        envelopes.push(parts[0].clone());
    }
    for (i, id) in envelopes.iter().enumerate() {
        assert!(
            envelopes[..i].iter().all(|seen| seen != id),
            "each exchange must carry its own id"
        );
    }

    // Answer the last one first, and nothing else yet: an implementation that
    // paired by arrival order, or by whichever entry came to hand, would
    // resolve one of the others here. The payload check alone would not have
    // caught that — the one still waiting is what does.
    let last = EXCHANGES - 1;
    peer.send_message(&[&envelopes[last], &[], b"re-3"]).await;
    let reply = within(&mut tasks[last])
        .await
        .expect("task")
        .expect("a reply");
    assert_eq!(
        within(reply.collect(64)).await.expect("collect"),
        b"re-3",
        "the reply goes to the exchange whose id it carries"
    );
    for (i, task) in tasks.iter().enumerate().take(last) {
        assert!(
            !task.is_finished(),
            "exchange {i} was not answered and must still be waiting"
        );
    }

    // Then the rest, still in reverse: each must receive its own echo.
    for i in (0..last).rev() {
        peer.send_message(&[&envelopes[i], &[], format!("re-{i}").as_bytes()])
            .await;
        let reply = within(&mut tasks[i]).await.expect("task").expect("a reply");
        assert_eq!(
            within(reply.collect(64)).await.expect("collect"),
            format!("re-{i}").as_bytes(),
            "exchange {i} received somebody else's reply"
        );
    }

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Pusher` reaches a foreign PULL, one ZMTP message per
/// transfer.
#[tokio::test]
async fn a_weida_pusher_reaches_a_foreign_pull() {
    let (addr, zmq) = Peer::listen().await;
    let (url, bridge_runtime) = bridge(OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/jobs",
        Dialling::Push,
    ))
    .await;
    let mut peer = Peer::accept(&zmq, SocketType::Pull).await;

    let client = client();
    let pusher = client.pusher(ClientTls::new(Trust::by_address()));
    within(pusher.connect(&url)).await.expect("connect");
    within(pusher.send(b"one")).await.expect("send one");
    within(pusher.send(b"two")).await.expect("send two");

    assert_eq!(
        within(peer.read_message()).await.expect("first"),
        vec![b"one".to_vec()],
        "one frame per transfer, no envelope invented"
    );
    assert_eq!(
        within(peer.read_message()).await.expect("second"),
        vec![b"two".to_vec()]
    );

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Subscriber` receives from a foreign PUB — the bridge
/// subscribes with the configured prefixes and republishes what arrives, with
/// the message's first frame becoming the weida topic (§6).
#[tokio::test]
async fn a_weida_subscriber_receives_from_a_foreign_pub() {
    let (addr, zmq) = Peer::listen().await;
    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/feed",
        Dialling::Sub,
    );
    config.subscribe = vec![b"px.".to_vec()];
    let (url, bridge_runtime) = bridge(config).await;
    let mut peer = Peer::accept(&zmq, SocketType::Pub).await;

    // The bridge subscribes with what it was configured with: a publisher has
    // no way to learn a weida subscriber's filters (loss L7).
    let body = within(peer.read_command()).await.expect("a SUBSCRIBE");
    match Command::decode(&body).expect("decode") {
        Command::Subscribe(prefix) => assert_eq!(prefix, b"px."),
        other => panic!("expected SUBSCRIBE, got {}", other.name()),
    }

    let client = client();
    let subscriber = client.subscriber(ClientTls::new(Trust::by_address()));
    within(subscriber.connect(&url)).await.expect("connect");
    within(subscriber.subscribe("px.#"))
        .await
        .expect("subscribe");

    // Publish until the weida subscription has landed, then assert the one
    // that must arrive.
    let received = loop {
        peer.send_message(&[b"px.eurusd", b"1.0921"]).await;
        match tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await {
            Ok(transfer) => break transfer.expect("recv"),
            Err(_) => continue,
        }
    };
    assert_eq!(received.meta().topic.as_deref(), Some("px.eurusd"));
    assert_eq!(
        within(received.collect(64)).await.expect("collect"),
        b"1.0921"
    );

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a reply that never comes ends the weida exchange with a typed error
/// rather than parking it forever — loss L5, where a ROUTER drops an
/// unroutable request silently and the absence is the only observation.
#[tokio::test]
async fn a_request_the_foreign_peer_drops_is_refused_with_no_reply() {
    let (addr, zmq) = Peer::listen().await;
    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    );
    // Short, so the test is fast; the default is ten seconds.
    config.reply_deadline = Duration::from_millis(300);
    let (url, bridge_runtime) = bridge(config).await;
    let mut peer = Peer::accept(&zmq, SocketType::Router).await;

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    let pending = tokio::spawn({
        let requester = Arc::clone(&requester);
        async move { requester.request(b"unroutable").await }
    });
    // The peer reads it and says nothing at all, which is exactly what
    // `ZMQ_ROUTER_MANDATORY` being off looks like from here.
    let request = within(peer.read_message()).await.expect("the request");
    assert_eq!(request[2], b"unroutable");

    let err = within(pending)
        .await
        .expect("task")
        .expect_err("a dropped request must not wait forever");
    assert!(
        matches!(err, Error::NoReply),
        "the requester must be told there will be no reply, got {err:?}"
    );

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: the bridge heartbeats its own socket rather than trusting TCP (§3),
/// and any inbound traffic counts as a sign of life.
#[tokio::test]
async fn the_bridge_pings_its_foreign_peer() {
    let (addr, zmq) = Peer::listen().await;
    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/jobs",
        Dialling::Push,
    );
    config.heartbeat = Some(Duration::from_millis(50));
    let (_url, bridge_runtime) = bridge(config).await;
    let mut peer = Peer::accept(&zmq, SocketType::Pull).await;

    let body = within(peer.read_command()).await.expect("a PING");
    match Command::decode(&body).expect("decode") {
        Command::Ping { ttl, context } => {
            assert!(ttl > 0, "the TTL tells the peer how long to wait");
            // Answering keeps the connection alive; the bridge treats any
            // traffic as liveness.
            peer.send_command(&Command::Pong { context }).await;
        }
        other => panic!("expected PING, got {}", other.name()),
    }
    // And it keeps beating rather than stopping after one.
    let body = within(peer.read_command()).await.expect("a second PING");
    assert!(matches!(
        Command::decode(&body).expect("decode"),
        Command::Ping { .. }
    ));

    bridge_runtime.shutdown().await;
}

/// Claim: the configuration refusals of §9 happen at bind time, before
/// anything is served.
#[tokio::test]
async fn the_configuration_refusals_happen_before_serving() {
    let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let peer: SocketAddr = "127.0.0.1:1".parse().expect("addr");

    // SUB with nothing to subscribe to: a publisher would be asked for
    // nothing, and an empty prefix must be said out loud.
    let config = OutboundConfig::new(peer, loopback, "/feed", Dialling::Sub);
    assert!(
        Outbound::bind(config, ServerTls::new(Identity::generate().expect("id")))
            .await
            .is_err()
    );

    // A zero reply deadline: the one thing that ends a silently dropped
    // exchange.
    let mut config = OutboundConfig::new(peer, loopback, "/rpc", Dialling::Dealer);
    config.reply_deadline = Duration::ZERO;
    assert!(
        Outbound::bind(config, ServerTls::new(Identity::generate().expect("id")))
            .await
            .is_err()
    );

    // A guarantee set above `core`.
    let mut config = OutboundConfig::new(peer, loopback, "/rpc", Dialling::Dealer);
    config.runtime.guarantees = weida::GuaranteeSet {
        deduplication: weida::Deduplication::Bounded,
        dedup_window_ms: Some(1000),
        ..weida::GuaranteeSet::CORE
    };
    assert!(
        Outbound::bind(config, ServerTls::new(Identity::generate().expect("id")))
            .await
            .is_err()
    );

    // And the one that is fine, so the test is not asserting that everything
    // fails.
    let config = OutboundConfig::new(peer, loopback, "/rpc", Dialling::Dealer);
    let ok = Outbound::bind(config, ServerTls::new(Identity::generate().expect("id")))
        .await
        .expect("a sound configuration binds");
    ok.runtime().clone().shutdown().await;
}

/// Claim: the bridge holds a bounded number of exchanges waiting for a reply,
/// and one past the ceiling is refused **immediately** rather than parked until
/// the deadline.
///
/// The bound the B-051 review pass found missing: each waiting exchange holds a
/// request and its body, and how many there are is whatever weida clients
/// choose to open. The peer here answers nothing until the end, so the ceiling
/// is the only thing that can decide the excess — and the last assertion is
/// what makes the test about a *ceiling* rather than about a refusal: the
/// exchanges inside it are still there, and still get their replies.
#[tokio::test]
async fn an_exchange_past_the_pending_ceiling_is_refused_at_once() {
    const CEILING: usize = 2;

    let (addr, zmq) = Peer::listen().await;
    let mut config = OutboundConfig::new(
        addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Dealer,
    );
    config.max_pending_exchanges = CEILING;
    // Long enough that a parked exchange would outlive this test: if the
    // ceiling did not refuse, the third request would hang here rather than
    // return an error.
    config.reply_deadline = Duration::from_secs(60);
    let (url, bridge_runtime) = bridge(config).await;
    let mut peer = Peer::accept(&zmq, SocketType::Rep).await;

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    // Fill the ceiling: the peer reads each request and answers none.
    let mut inside = Vec::new();
    let mut envelopes = Vec::new();
    for i in 0..CEILING {
        let requester = Arc::clone(&requester);
        let body = format!("req-{i}");
        inside.push(tokio::spawn(async move {
            requester.request(body.as_bytes()).await
        }));
        let parts = within(peer.read_message()).await.expect("a request");
        envelopes.push(parts[0].clone());
    }

    // One more. Nothing of it may reach the peer, and the requester must be
    // told now.
    let refused = within(requester.request(b"over-the-ceiling")).await;
    assert!(
        matches!(refused, Err(Error::Rejected)),
        "the exchange past the ceiling must be refused, got {refused:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), peer.read_message())
            .await
            .is_err(),
        "a refused exchange must not be forwarded to the ZeroMQ peer"
    );

    // The ceiling is a ceiling, not a wall: what was admitted still completes.
    for (i, (task, envelope)) in inside.into_iter().zip(envelopes).enumerate() {
        peer.send_message(&[&envelope, &[], format!("re-{i}").as_bytes()])
            .await;
        let reply = within(task).await.expect("task").expect("a reply");
        assert_eq!(
            within(reply.collect(64)).await.expect("collect"),
            format!("re-{i}").as_bytes()
        );
    }

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}
