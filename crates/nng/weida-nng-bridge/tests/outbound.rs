//! weida endpoints against a foreign SP peer, through the outbound bridge.
//!
//! The peer here is a plain `TcpListener` speaking the SP TCP mapping with
//! `weida-sp` - byte-exact against the golden vectors of
//! `docs/adapters/nng.md` §10.1, and therefore a faithful peer and not an
//! independent one, which is why §10's `nng` run is still owed. No C library
//! is built and nothing is `#[ignore]`d.
//!
//! What these tests defend is the direction's own decisions: one request on
//! the wire per exchange and no resend timer, a deadline and a closed
//! connection both reaching the requester as `ERROR{NO_REPLY}`, the pending
//! ceiling refusing before a body is read, the receiver-side prefix match,
//! and the topic split.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use weida::{ClientTls, GuaranteeSet, Identity, Runtime, RuntimeConfig, ServerTls, Trust};
use weida_nng_bridge::{BridgeError, Dialling, Outbound, OutboundConfig, TopicSplit};
use weida_sp::header::{EndpointType, HEADER_LEN};
use weida_sp::{ProtocolHeader, backtrace, message};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A foreign SP peer: a listener, and one accepted connection driven by hand.
struct PeerListener {
    listener: TcpListener,
    ours: EndpointType,
}

impl PeerListener {
    async fn start(ours: EndpointType) -> PeerListener {
        PeerListener {
            listener: TcpListener::bind("127.0.0.1:0").await.expect("bind"),
            ours,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("addr")
    }

    /// Accepts the bridge's connection and exchanges protocol headers.
    async fn accept(&self) -> Peer {
        let (io, _) = self.listener.accept().await.expect("accept");
        let mut peer = Peer {
            io,
            buf: Vec::new(),
        };
        peer.io
            .write_all(&ProtocolHeader::new(self.ours).encode())
            .await
            .expect("our header");
        let theirs = peer.read_header().await.expect("their header");
        assert_eq!(
            theirs.endpoint,
            self.ours.peer(),
            "the bridge dials as the legal opposite"
        );
        peer
    }

    /// Accepts and answers with a header the bridge must refuse.
    async fn accept_as(&self, lying: EndpointType) -> Peer {
        let (io, _) = self.listener.accept().await.expect("accept");
        let mut peer = Peer {
            io,
            buf: Vec::new(),
        };
        peer.io
            .write_all(&ProtocolHeader::new(lying).encode())
            .await
            .expect("our header");
        peer
    }
}

struct Peer {
    io: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    async fn read_header(&mut self) -> std::io::Result<ProtocolHeader> {
        let mut bytes = [0u8; HEADER_LEN];
        self.read_exactly(&mut bytes).await?;
        Ok(ProtocolHeader::decode(&bytes).expect("a protocol header"))
    }

    async fn send(&mut self, parts: &[&[u8]]) {
        let body: Vec<u8> = parts.concat();
        self.io
            .write_all(&message::encode(&body))
            .await
            .expect("write");
    }

    async fn recv(&mut self) -> std::io::Result<Vec<u8>> {
        loop {
            match message::decode(&self.buf, 8 * 1024 * 1024) {
                Ok((body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok(body);
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => return Err(std::io::Error::other(format!("{e}"))),
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
        let mut chunk = [0u8; 4096];
        let n = self.io.read(&mut chunk).await?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }
}

/// Starts a bridge, serving in the background.
async fn bridge(config: OutboundConfig) -> (String, Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_nng_bridge=debug")
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

/// Claim: a weida `Requester` reaches a foreign REP, the request carries a
/// tag this side allocated, and the reply comes back matched by that tag
/// (`docs/adapters/nng.md` §3, [rfc-reqrep §5]).
///
/// Claim (L3, observable): **exactly one** request reaches the wire. A cooked
/// REQ would retransmit on its resend timer; this bridge owns no such timer,
/// because retransmitting would invent at-least-once for a requester that
/// asked for one attempt.
#[tokio::test]
async fn a_weida_requester_reaches_a_foreign_rep_exactly_once() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let (url, bridge_runtime) = bridge(OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    ))
    .await;
    let mut peer = peer.accept().await;

    let client = client();
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");

    let answering = tokio::spawn(async move {
        let body = peer.recv().await.expect("a request");
        let (stack, payload) = backtrace::decode(&body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
        assert_eq!(payload, b"ping");
        assert!(
            stack.peers.is_empty(),
            "this side is the requester, so there are no forwarder ids"
        );
        peer.send(&[&stack.encode(), b"pong"]).await;
        // Nothing else may arrive: a retransmission would show up here.
        let idle = tokio::time::timeout(Duration::from_millis(300), peer.recv()).await;
        assert!(idle.is_err(), "the bridge must not retransmit");
        stack.id
    });

    let reply = within(requester.request(b"ping")).await.expect("a reply");
    assert_eq!(within(reply.collect(64)).await.expect("collect"), b"pong");
    let id = within(answering).await.expect("the peer task");
    assert!(id <= weida_sp::backtrace::MAX_ID, "a 31-bit request id");

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: several weida exchanges are in flight at once and each is matched
/// by its tag, not by order - the peer answers them in reverse.
#[tokio::test]
async fn concurrent_exchanges_are_matched_by_tag() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let (url, bridge_runtime) = bridge(OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    ))
    .await;
    let mut peer = peer.accept().await;

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    tokio::spawn(async move {
        let mut held = Vec::new();
        while held.len() < 4 {
            let body = peer.recv().await.expect("a request");
            let (stack, payload) =
                backtrace::decode(&body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
            held.push((stack, payload.to_vec()));
        }
        for (stack, payload) in held.into_iter().rev() {
            let mut answer = b"re:".to_vec();
            answer.extend_from_slice(&payload);
            peer.send(&[&stack.encode(), &answer]).await;
        }
    });

    let mut waiting = Vec::new();
    for n in 0..4u8 {
        let requester = Arc::clone(&requester);
        waiting.push(tokio::spawn(async move {
            let reply = requester.request(&[b'q', b'0' + n]).await.expect("reply");
            reply.collect(64).await.expect("collect")
        }));
    }
    for (n, handle) in waiting.into_iter().enumerate() {
        let body = within(handle).await.expect("task");
        assert_eq!(body, format!("re:q{n}").into_bytes());
    }

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim (L10 in reverse, observable): a peer that never answers turns into
/// `ERROR{NO_REPLY}` at the deadline, not a hang.
///
/// SP has no way to decline and no error frame, so silence is all a REP peer
/// can say (`docs/research/nanomsg-nng.md` §4, §6). The deadline is what
/// turns it into something typed - B-042's answer for ZMTP, taken here for a
/// different reason: there the peer *dropped* the request (ROUTER_MANDATORY
/// off), here it may simply be declining.
#[tokio::test]
async fn a_peer_that_never_answers_becomes_no_reply_at_the_deadline() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    );
    config.reply_deadline = Duration::from_millis(300);
    let (url, bridge_runtime) = bridge(config).await;
    let mut silent = peer.accept().await;

    let client = client();
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");

    let err = within(requester.request(b"anyone there"))
        .await
        .expect_err("silence becomes an error");
    assert!(
        matches!(err, weida::Error::NoReply),
        "the requester learns the shape of the failure: {err:?}"
    );
    // The request did reach the peer - the deadline is about the answer, not
    // about delivery.
    let body = within(silent.recv()).await.expect("the request arrived");
    assert!(body.len() > 4);

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: an exchange whose reply never comes expires **under traffic**, not
/// only on an otherwise silent loop.
///
/// The companion of the test above, and the one that pins the mechanism. The
/// reaper's timer used to be built inside the `select!`, so every accepted
/// request, every message from the socket and every pipe event restarted it:
/// with a sweep at a quarter of the deadline, anything above a few events a
/// second postponed expiry indefinitely and the `ERROR{NO_REPLY}` the test
/// above asserts simply never happened. The ZMTP bridge is fixed the same way
/// and has the same test.
#[tokio::test]
async fn a_pending_exchange_expires_while_traffic_keeps_arriving() {
    /// Short enough for a fast test, and divisible by eight below.
    const REPLY_DEADLINE: Duration = Duration::from_millis(400);

    let peer = PeerListener::start(EndpointType::Rep).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    );
    config.reply_deadline = REPLY_DEADLINE;
    let (url, bridge_runtime) = bridge(config).await;
    let mut silent = peer.accept().await;

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    // The exchange under test. The peer reads it and never answers it.
    let watched = tokio::spawn({
        let requester = Arc::clone(&requester);
        async move { requester.request(b"watched").await }
    });
    let request = within(silent.recv()).await.expect("the request arrived");
    assert!(request.ends_with(b"watched"), "behind its backtrace");
    let waiting_since = std::time::Instant::now();

    // Then steady traffic through the same loop: a request every eighth of the
    // deadline, so a quarter-deadline sweep never has a gap to itself.
    let noise = tokio::spawn({
        let requester = Arc::clone(&requester);
        async move {
            loop {
                let asking = Arc::clone(&requester);
                tokio::spawn(async move { asking.request(b"noise").await });
                tokio::time::sleep(REPLY_DEADLINE / 8).await;
            }
        }
    });
    // The peer keeps reading, so the traffic reaches the bridge's loop rather
    // than filling a socket buffer, and keeps saying nothing.
    let draining = tokio::spawn(async move { while silent.recv().await.is_ok() {} });

    let error = within(watched)
        .await
        .expect("task")
        .expect_err("nobody ever answered this exchange");
    assert!(
        matches!(error, weida::Error::NoReply),
        "an expired exchange is refused with NoReply, got {error:?}"
    );
    // Bounded rather than eventual: the sweep is a quarter of the deadline, so
    // three deadlines is generous and a starved reaper cannot come in under it.
    let took = waiting_since.elapsed();
    assert!(
        took < REPLY_DEADLINE * 3,
        "expiry under load took {took:?}, which is not bounded by the deadline"
    );

    noise.abort();
    draining.abort();
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a peer that closes mid-exchange does not make the requester wait
/// out the deadline - the close *is* the answer SP cannot give, so every
/// pending exchange is refused at once.
///
/// This is the one place this slice answers the question differently from
/// B-042, which only had the deadline: the deadline still exists for a peer
/// that stays connected and says nothing, and the close short-circuits it.
#[tokio::test]
async fn a_peer_that_closes_ends_every_pending_exchange_immediately() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    );
    // Far longer than the test may take: if the close were not handled, this
    // test would time out rather than fail.
    config.reply_deadline = Duration::from_secs(600);
    let (url, bridge_runtime) = bridge(config).await;
    let mut peer = peer.accept().await;

    let client = client();
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");

    let waiting = tokio::spawn(async move { requester.request(b"ping").await.map(|_| ()) });
    // Wait until the request is on the wire, then vanish.
    let _ = within(peer.recv()).await.expect("the request");
    drop(peer);

    let err = within(waiting)
        .await
        .expect("task")
        .expect_err("the exchange cannot succeed");
    assert!(
        matches!(err, weida::Error::NoReply),
        "a closed peer is reported, not waited out: {err:?}"
    );

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: past `max_pending_exchanges` an exchange is refused immediately,
/// and refused **before its body is read** - the cheaper refusal, and the
/// bound the ZMTP bridge needed a review pass to grow (B-053).
#[tokio::test]
async fn the_pending_ceiling_refuses_before_a_body_is_read() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    );
    config.max_pending_exchanges = 1;
    config.reply_deadline = Duration::from_secs(600);
    let (url, bridge_runtime) = bridge(config).await;
    let peer = Arc::new(Mutex::new(peer.accept().await));

    let client = client();
    let requester = Arc::new(client.requester(ClientTls::new(Trust::by_address())));
    within(requester.connect(&url)).await.expect("connect");

    // One exchange parked at the peer, which never answers.
    let first = {
        let requester = Arc::clone(&requester);
        tokio::spawn(async move { requester.request(b"first").await.map(|_| ()) })
    };
    {
        let mut peer = peer.lock().await;
        let _ = within(peer.recv()).await.expect("the first request");
    }

    // The second is refused, and nothing new reaches the peer.
    let err = within(requester.request(b"second"))
        .await
        .expect_err("past the ceiling");
    assert!(
        matches!(err, weida::Error::Rejected),
        "the requester is told immediately rather than parked: {err:?}"
    );
    {
        let mut peer = peer.lock().await;
        let idle = tokio::time::timeout(Duration::from_millis(300), peer.recv()).await;
        assert!(idle.is_err(), "the refused exchange never reached the wire");
    }

    first.abort();
    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Pusher` reaches a foreign PULL, one SP message per
/// transfer, with `Block` on both sides and nothing converted into a drop.
#[tokio::test]
async fn a_weida_pusher_reaches_a_foreign_pull() {
    let peer = PeerListener::start(EndpointType::Pull).await;
    let (url, bridge_runtime) = bridge(OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/jobs",
        Dialling::Push,
    ))
    .await;
    let mut peer = peer.accept().await;

    let client = client();
    let pusher = client.pusher(ClientTls::new(Trust::by_address()));
    within(pusher.connect(&url)).await.expect("connect");
    within(pusher.send(b"one")).await.expect("send");
    within(pusher.send(b"two")).await.expect("send");

    assert_eq!(within(peer.recv()).await.expect("first"), b"one");
    assert_eq!(within(peer.recv()).await.expect("second"), b"two");

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a weida `Subscriber` receives from a foreign PUB, with the topic
/// split off the leading body bytes (§6) and the prefix match applied **by
/// the bridge**, because SP filters at the subscriber and nothing is ever
/// sent to the publisher.
///
/// Claim (L1, from this end): the discarded publication still crossed the
/// link. The test observes it by counting what the peer wrote against what
/// the weida subscriber received.
#[tokio::test]
async fn a_weida_subscriber_receives_what_the_bridge_kept() {
    let peer = PeerListener::start(EndpointType::Pub).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/feed",
        Dialling::Sub,
    );
    config.subscribe = vec![b"px.".to_vec()];
    config.topic_split = TopicSplit::Delimiter(0);
    let (url, bridge_runtime) = bridge(config).await;
    let mut peer = peer.accept().await;

    let client = client();
    let subscriber = client.subscriber(ClientTls::new(Trust::by_address()));
    within(subscriber.connect(&url)).await.expect("connect");
    within(subscriber.subscribe("px.#")).await.expect("filter");

    let written = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&written);
    tokio::spawn(async move {
        // Three publications, one of which this bridge keeps nothing of.
        for body in [
            &b"px.eurusd\x001.0921"[..],
            &b"fx.gbpusd\x001.2711"[..],
            &b"px.gbpusd\x001.2712"[..],
        ] {
            peer.send(&[body]).await;
            counter.fetch_add(1, Ordering::Relaxed);
        }
        // Hold the connection open so the bridge keeps reading.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let first = within(subscriber.recv()).await.expect("a publication");
    assert_eq!(first.meta().topic.as_deref(), Some("px.eurusd"));
    assert_eq!(within(first.collect(64)).await.expect("collect"), b"1.0921");
    let second = within(subscriber.recv()).await.expect("the next one");
    assert_eq!(
        second.meta().topic.as_deref(),
        Some("px.gbpusd"),
        "the unsubscribed topic never became a weida publication"
    );
    assert_eq!(
        written.load(Ordering::Relaxed),
        3,
        "all three crossed the SP link; the filter ran here, not at the publisher"
    );

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: a peer whose endpoint type may not talk to the one this bridge
/// dialled is closed on - SP has nothing to answer with (L10) - and the weida
/// side is told `Unsupported` rather than left to discover a missing
/// endpoint.
#[tokio::test]
async fn a_wrong_peer_type_closes_the_wire_and_tells_the_weida_side() {
    let peer = PeerListener::start(EndpointType::Rep).await;
    let mut config = OutboundConfig::new(
        peer.addr(),
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        Dialling::Req,
    );
    config.refusal_grace = Duration::from_secs(5);
    let (url, bridge_runtime) = bridge(config).await;
    // The peer announces PULL, which no REQ may talk to.
    let mut lying = peer.accept_as(EndpointType::Pull).await;

    let client = client();
    let requester = client.requester(ClientTls::new(Trust::by_address()));
    within(requester.connect(&url)).await.expect("connect");
    let err = within(requester.request(b"hello"))
        .await
        .expect_err("the far end is unusable");
    assert!(
        matches!(err, weida::Error::Unsupported),
        "the weida side learns why: {err:?}"
    );

    // The bridge's own protocol header arrives first - the mapping requires
    // it to be written before the peer's is read - and then nothing.
    let header = within(lying.read_header()).await.expect("their header");
    assert_eq!(header.endpoint, EndpointType::Req);
    let mut chunk = [0u8; 32];
    let read = within(lying.io.read(&mut chunk)).await.expect("read");
    assert_eq!(read, 0, "the bridge closed rather than explaining");

    client.shutdown().await;
    bridge_runtime.shutdown().await;
}

/// Claim: the configurations `docs/adapters/nng.md` §9 refuses are refused at
/// bind time, with the reason.
#[tokio::test]
async fn refused_configurations_fail_at_bind() {
    let listen: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let connect: SocketAddr = "127.0.0.1:1".parse().expect("addr");

    async fn refused(config: OutboundConfig, what: &str) -> String {
        let identity = Identity::generate().expect("identity");
        match Outbound::bind(config, ServerTls::new(identity)).await {
            Err(BridgeError::Configuration(reason)) => reason,
            Err(other) => panic!("{what}: expected a configuration refusal, got {other}"),
            Ok(_) => panic!("{what}: must be refused"),
        }
    }

    let mut above_core = OutboundConfig::new(connect, listen, "/rpc", Dialling::Req);
    above_core.runtime.guarantees = GuaranteeSet {
        ordering: weida::OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let reason = refused(above_core, "a guarantee set above core").await;
    assert!(reason.contains("core"), "{reason}");

    let mut no_deadline = OutboundConfig::new(connect, listen, "/rpc", Dialling::Req);
    no_deadline.reply_deadline = Duration::ZERO;
    let reason = refused(no_deadline, "a zero reply deadline").await;
    assert!(
        reason.contains("retransmit"),
        "the refusal says why the deadline is the only end: {reason}"
    );

    let mut no_pending = OutboundConfig::new(connect, listen, "/rpc", Dialling::Req);
    no_pending.max_pending_exchanges = 0;
    refused(no_pending, "a zero pending ceiling").await;

    let mut no_cap = OutboundConfig::new(connect, listen, "/rpc", Dialling::Req);
    no_cap.max_message_bytes = 0;
    refused(no_cap, "an unlimited message cap").await;

    // A SUB that subscribes to nothing would receive every publication and
    // keep none; the empty prefix has to be written out.
    let silent_sub = OutboundConfig::new(connect, listen, "/feed", Dialling::Sub);
    let reason = refused(silent_sub, "a SUB with no subscription").await;
    assert!(reason.contains("empty prefix"), "{reason}");
}
