//! A foreign SP peer against the bridge, and weida on the far side.
//!
//! The peer here speaks the SP TCP mapping over a plain `TcpStream` with
//! `weida-sp`, which is byte-exact against the golden vectors of
//! `docs/adapters/nng.md` §10.1 - so it is a faithful SP peer for these
//! tests, and its being *our* codec is exactly why the interop bench against
//! a real NNG peer is still owed (slice 5, §10 items 3-6). No C library is
//! built and nothing is `#[ignore]`d here.
//!
//! What these tests defend is the bridge: the three patterns it maps, the
//! four things it refuses, and the named losses of §8 that are observable
//! from this side.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use weida::{
    ClientTls, GuaranteeSet, Identity, Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust,
};
use weida_nng_bridge::{BridgeError, Inbound, InboundConfig, Presenting};
use weida_sp::header::{EndpointType, HEADER_LEN};
use weida_sp::{Backtrace, ProtocolHeader, backtrace, message};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// The weida side: a runtime, a binding and the URL to reach an endpoint on
/// it.
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

/// Starts a bridge, serving in the background, and returns where to reach it.
async fn bridge(config: InboundConfig) -> (SocketAddr, Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_nng_bridge=debug")
        .try_init();
    let inbound = Inbound::bind(config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the bridge");
    let addr = inbound.local_addr().expect("local addr");
    let runtime = inbound.runtime().clone();
    tokio::spawn(async move {
        let _ = inbound.serve().await;
    });
    (addr, runtime)
}

/// An SP peer: a TCP socket and the codec, nothing else.
struct Peer {
    io: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    /// Connects and exchanges protocol headers as `ours`.
    async fn connect(addr: SocketAddr, ours: EndpointType) -> Peer {
        let mut peer = Peer::raw(addr).await;
        peer.io
            .write_all(&ProtocolHeader::new(ours).encode())
            .await
            .expect("our header");
        let theirs = peer.read_header().await.expect("their header");
        assert_eq!(
            theirs.endpoint,
            ours.peer(),
            "the bridge presents the legal opposite"
        );
        peer
    }

    /// Connects without saying anything, for the refusal tests.
    async fn raw(addr: SocketAddr) -> Peer {
        Peer {
            io: TcpStream::connect(addr).await.expect("connect"),
            buf: Vec::new(),
        }
    }

    async fn read_header(&mut self) -> std::io::Result<ProtocolHeader> {
        let mut bytes = [0u8; HEADER_LEN];
        self.read_exactly(&mut bytes).await?;
        Ok(ProtocolHeader::decode(&bytes).expect("a protocol header"))
    }

    /// Sends one message: a 64-bit length and the concatenated parts.
    async fn send(&mut self, parts: &[&[u8]]) {
        let body: Vec<u8> = parts.concat();
        self.io
            .write_all(&message::encode(&body))
            .await
            .expect("write");
    }

    /// Sends raw bytes, for the shapes a codec would never produce.
    async fn send_raw(&mut self, bytes: &[u8]) {
        self.io.write_all(bytes).await.expect("write");
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
                Err(e) => {
                    return Err(std::io::Error::other(format!("{e}")));
                }
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

    /// Waits for the peer to close, returning whatever arrived first.
    async fn expect_close(&mut self) {
        let mut chunk = [0u8; 64];
        loop {
            match within(self.io.read(&mut chunk)).await {
                Ok(0) => return,
                Ok(_) => continue,
                Err(_) => return,
            }
        }
    }
}

/// Spawns a weida replier that echoes with a prefix, counting how many
/// requests it saw.
fn echo_replier(
    listener: &weida::Listener,
    path: &'static str,
) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let seen = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&seen);
    let replier = listener.replier(path).expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request.body().read_capped(4096).await.expect("body");
            counter.fetch_add(1, Ordering::Relaxed);
            let mut reply = request
                .reply(TransferMeta::default())
                .await
                .expect("reply half");
            let mut echoed = b"re:".to_vec();
            echoed.extend_from_slice(&body);
            reply.write_all(&echoed).await.expect("write");
            reply.finish().expect("finish");
        }
    });
    seen
}

/// Claim: a foreign REQ reaches a weida `Replier`, and the reply comes back
/// carrying the request's tag stack **unchanged** - including a forwarder's
/// peer id, which is what routes the answer back through a device
/// (`docs/adapters/nng.md` §3, [rfc-reqrep §5]).
#[tokio::test]
async fn a_foreign_req_reaches_a_weida_replier_and_the_tags_come_back() {
    let weida_side = WeidaSide::start().await;
    echo_replier(&weida_side.listener, "/rpc");

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    ))
    .await;

    let mut peer = Peer::connect(addr, EndpointType::Req).await;
    let mut stack = Backtrace::direct(1);
    stack.push_peer(7);
    peer.send(&[&stack.encode(), b"ping"]).await;

    let body = within(peer.recv()).await.expect("a reply");
    let (tags, payload) = backtrace::decode(&body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(tags, stack, "the reply carries the request's stack");
    assert_eq!(payload, b"re:ping");
    // Byte-exact, because the routing depends on it: peer id then request id
    // with the terminal bit.
    assert_eq!(&body[..8], &[0, 0, 0, 7, 0x80, 0, 0, 1]);

    // A second exchange on the same connection, with its own request id.
    let second = Backtrace::direct(2);
    peer.send(&[&second.encode(), b"again"]).await;
    let body = within(peer.recv()).await.expect("a second reply");
    let (tags, payload) = backtrace::decode(&body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
    assert_eq!(tags.id, 2);
    assert_eq!(payload, b"re:again");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: requests are answered concurrently, and each reply is paired by its
/// tag rather than by arrival order.
///
/// A cooked REQ holds one outstanding request per **context** and a socket
/// may own many (`docs/research/nanomsg-nng.md` §2), which is the difference
/// from ZMTP's lockstep REQ that made this bridge's REP loop concurrent.
#[tokio::test]
async fn several_requests_in_flight_are_answered_by_tag_not_by_order() {
    let weida_side = WeidaSide::start().await;
    let replier = weida_side.listener.replier("/rpc").expect("replier");
    // Answer in reverse: the first request is held until the last arrives, so
    // a bridge that replied in arrival order could not pass this.
    tokio::spawn(async move {
        let mut held = Vec::new();
        while held.len() < 4 {
            let mut request = replier.accept().await.expect("accept");
            let body = request.body().read_capped(4096).await.expect("body");
            held.push((request, body));
        }
        for (request, body) in held.into_iter().rev() {
            let mut reply = request
                .reply(TransferMeta::default())
                .await
                .expect("reply half");
            reply.write_all(&body).await.expect("write");
            reply.finish().expect("finish");
        }
    });

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    ))
    .await;

    let mut peer = Peer::connect(addr, EndpointType::Req).await;
    for id in 1..=4u32 {
        peer.send(&[&Backtrace::direct(id).encode(), format!("q{id}").as_bytes()])
            .await;
    }

    let mut answered = Vec::new();
    for _ in 0..4 {
        let body = within(peer.recv()).await.expect("a reply");
        let (tags, payload) = backtrace::decode(&body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
        assert_eq!(
            payload,
            format!("q{}", tags.id).as_bytes(),
            "each reply carries the payload of the request whose tag it has"
        );
        answered.push(tags.id);
    }
    answered.sort_unstable();
    assert_eq!(answered, vec![1, 2, 3, 4]);

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim (loss L4, observable): a retransmitted request reaches the weida
/// replier **twice**.
///
/// A cooked REQ resends on its resend timer, on disconnect, or when a peer
/// becomes available (`docs/research/nanomsg-nng.md` §4), and the resend
/// carries the same 32-bit request tag [rfc-reqrep §5]. That tag is what
/// makes the duplicate recognizable in a capture and it is *not* a
/// deduplication key: `docs/adapters/nng.md` §7 decided the duplicate is
/// forwarded, because suppressing it needs an identity §9.3 refuses to
/// invent. Here the resend is sent by hand, since the bridge is not the one
/// that would retransmit.
#[tokio::test]
async fn a_retransmitted_request_reaches_the_replier_twice() {
    use std::sync::atomic::Ordering;

    let weida_side = WeidaSide::start().await;
    let seen = echo_replier(&weida_side.listener, "/rpc");

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    ))
    .await;

    let mut peer = Peer::connect(addr, EndpointType::Req).await;
    let stack = Backtrace::direct(11);
    peer.send(&[&stack.encode(), b"charge"]).await;
    let first = within(peer.recv()).await.expect("the first reply");

    // The same request id again: a resend, indistinguishable on the wire from
    // the original.
    peer.send(&[&stack.encode(), b"charge"]).await;
    let second = within(peer.recv()).await.expect("the reply to the resend");

    assert_eq!(first, second, "both answers carry the same tag and payload");
    assert_eq!(
        seen.load(Ordering::Relaxed),
        2,
        "the duplicate reached the weida application; it was not suppressed"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a foreign PUSH reaches a weida `Puller`, one SP message per weida
/// transfer (§3).
#[tokio::test]
async fn a_foreign_push_reaches_a_weida_puller() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/jobs").expect("puller");

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    ))
    .await;

    let mut peer = Peer::connect(addr, EndpointType::Push).await;
    peer.send(&[b"one"]).await;
    peer.send(&[b"two"]).await;

    let first = within(puller.recv()).await.expect("first");
    assert_eq!(within(first.collect(64)).await.expect("collect"), b"one");
    let second = within(puller.recv()).await.expect("second");
    assert_eq!(within(second.collect(64)).await.expect("collect"), b"two");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a foreign SUB receives from a weida `Publisher`, with the topic as
/// the **leading bytes of the body** and no topic field anywhere (§3, §6).
///
/// Claim (loss L1, observable): the bridge subscribes to everything and sends
/// every copy, because SP filters at the subscriber and a SUB socket cannot
/// tell the bridge what it wants (`docs/research/nanomsg-nng.md` §4). A weida
/// filter therefore cannot reduce what crosses this link - the test observes
/// that by receiving a topic a ZMTP bridge would have filtered out at the
/// publisher.
#[tokio::test]
async fn a_foreign_sub_receives_every_copy_and_filters_it_itself() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/feed").expect("publisher");

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/feed"),
        Presenting::Pub,
    ))
    .await;

    // A SUB peer says nothing at all: it cannot send, and its subscriptions
    // are local to it.
    let mut peer = Peer::connect(addr, EndpointType::Sub).await;
    while publisher.subscriber_count() == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    publisher
        .publish("px.eurusd", b"1.0921".to_vec())
        .expect("publish");
    publisher
        .publish("fx.gbpusd", b"1.2711".to_vec())
        .expect("publish");

    let first = within(peer.recv()).await.expect("a published message");
    assert_eq!(
        first, b"px.eurusd1.0921",
        "the topic is the leading bytes of the body, with no separator"
    );
    let second = within(peer.recv()).await.expect("the second message");
    assert_eq!(
        second, b"fx.gbpusd1.2711",
        "a topic this peer would filter out still crosses the link: L1"
    );
    // Which is what a subscriber-side filter looks like from here: the peer
    // decides by prefix, on bytes it has already received.
    assert!(first.starts_with(b"px."));
    assert!(!second.starts_with(b"px."));

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim (loss L10, observable): an endpoint type that may not talk here is
/// refused **before any traffic**, and the refusal is a close - SP has no
/// error frame to carry a reason ([rfc-tcp §2], `docs/adapters/nng.md` §8).
#[tokio::test]
async fn a_wrong_endpoint_type_is_closed_on_after_the_header() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/jobs").expect("puller");
    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    ))
    .await;

    let mut peer = Peer::raw(addr).await;
    peer.send_raw(&ProtocolHeader::new(EndpointType::Sub).encode())
        .await;
    // The bridge's own header arrives - it is sent before the peer's is read,
    // as the mapping requires - and then the connection ends with nothing
    // else on it.
    let header = within(peer.read_header()).await.expect("their header");
    assert_eq!(header.endpoint, EndpointType::Pull);
    within(peer.expect_close()).await;

    // And nothing was forwarded: the puller never saw a message.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), puller.recv())
            .await
            .is_err(),
        "a refused peer forwards nothing"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a protocol header with the wrong magic or an unsupported version is
/// closed on, which is the only remedy the mapping defines [rfc-tcp §2].
#[tokio::test]
async fn a_malformed_protocol_header_is_closed_on() {
    for bad in [
        [0x00, 0x53, 0x51, 0x00, 0x00, 0x50, 0, 0], // wrong magic
        [0x00, 0x53, 0x50, 0x01, 0x00, 0x50, 0, 0], // version 1
        [0x00, 0x53, 0x50, 0x00, 0x00, 0x50, 0, 1], // reserved not zero
    ] {
        let weida_side = WeidaSide::start().await;
        let _puller = weida_side.listener.puller("/jobs").expect("puller");
        let (addr, bridge_runtime) = bridge(InboundConfig::new(
            "127.0.0.1:0".parse().expect("loopback"),
            weida_side.url("/jobs"),
            Presenting::Pull,
        ))
        .await;

        let mut peer = Peer::raw(addr).await;
        peer.send_raw(&bad).await;
        within(peer.expect_close()).await;

        bridge_runtime.shutdown().await;
        weida_side.runtime.shutdown().await;
    }
}

/// Claim (loss L2/L9, observable): a message larger than `max_message_bytes`
/// is refused from its declared size alone - before a payload byte is read -
/// and the connection ends, because SP gives a reader no way to skip a body
/// it declined (`weida_sp::error::MessageError::is_violation`).
#[tokio::test]
async fn an_oversized_declaration_is_refused_before_its_body() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/jobs").expect("puller");
    let mut config = InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    );
    config.max_message_bytes = 64;
    let (addr, bridge_runtime) = bridge(config).await;

    let mut peer = Peer::connect(addr, EndpointType::Push).await;
    // Declare 2^64-1 octets and send none of them.
    peer.send_raw(&[0xFFu8; 8]).await;
    within(peer.expect_close()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), puller.recv())
            .await
            .is_err(),
        "nothing was forwarded, and nothing was allocated for the declaration"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a tag stack deeper than the configured `MAXTTL` is refused.
///
/// The ceiling is a local choice with two published values - 1-255 on the
/// specification side, 15 in NNG's source (`docs/adapters/nng.md` §11) - so
/// what matters is that the configured one is enforced and that nothing
/// beyond it is read.
#[tokio::test]
async fn a_tag_stack_deeper_than_the_hop_ceiling_is_refused() {
    let weida_side = WeidaSide::start().await;
    let seen = echo_replier(&weida_side.listener, "/rpc");
    let mut config = InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    );
    config.max_hops = 2;
    let (addr, bridge_runtime) = bridge(config).await;

    let mut peer = Peer::connect(addr, EndpointType::Req).await;
    let mut stack = Backtrace::direct(5);
    for peer_id in 1..=3u32 {
        stack.push_peer(peer_id);
    }
    peer.send(&[&stack.encode(), b"deep"]).await;
    within(peer.expect_close()).await;
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "nothing reached the weida side"
    );

    // Two hops is inside the ceiling, on a fresh connection.
    let mut peer = Peer::connect(addr, EndpointType::Req).await;
    let mut shallow = Backtrace::direct(6);
    shallow.push_peer(1);
    shallow.push_peer(2);
    peer.send(&[&shallow.encode(), b"ok"]).await;
    let body = within(peer.recv()).await.expect("a reply");
    let (tags, payload) = backtrace::decode(&body, 2).expect("tags");
    assert_eq!(tags, shallow);
    assert_eq!(payload, b"re:ok");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: the configurations `docs/adapters/nng.md` §9 refuses are refused at
/// bind time, with the reason, rather than at the first message.
#[tokio::test]
async fn refused_configurations_fail_at_bind() {
    let weida_side = WeidaSide::start().await;
    let url = weida_side.url("/jobs");
    let listen: SocketAddr = "127.0.0.1:0".parse().expect("loopback");

    /// Binds and expects a configuration refusal naming its reason.
    async fn refused(config: InboundConfig, what: &str) -> String {
        match Inbound::bind(config, ClientTls::new(Trust::by_address())).await {
            Err(BridgeError::Configuration(reason)) => reason,
            Err(other) => panic!("{what}: expected a configuration refusal, got {other}"),
            Ok(_) => panic!("{what}: must be refused"),
        }
    }

    let mut above_core = InboundConfig::new(listen, &url, Presenting::Pull);
    above_core.runtime.guarantees = GuaranteeSet {
        ordering: weida::OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let reason = refused(above_core, "a guarantee set above core").await;
    assert!(
        reason.contains("core"),
        "the refusal names the rule: {reason}"
    );

    let mut no_cap = InboundConfig::new(listen, &url, Presenting::Pull);
    no_cap.max_message_bytes = 0;
    let reason = refused(no_cap, "an unlimited cap").await;
    assert!(
        reason.contains("RECVMAXSZ"),
        "the refusal names what SP would have allowed: {reason}"
    );

    let mut no_hops = InboundConfig::new(listen, &url, Presenting::Pull);
    no_hops.max_hops = 0;
    refused(no_hops, "a zero hop ceiling").await;

    let mut no_connections = InboundConfig::new(listen, &url, Presenting::Pull);
    no_connections.max_connections = 0;
    refused(no_connections, "a zero connection ceiling").await;

    weida_side.runtime.shutdown().await;
}
