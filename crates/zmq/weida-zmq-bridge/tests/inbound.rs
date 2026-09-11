//! A foreign ZeroMQ peer against the bridge, and weida on the far side.
//!
//! The peer here speaks ZMTP over a plain `TcpStream` with `weida-zmtp`, which
//! is byte-exact against the golden vectors of `docs/adapters/zmtp.md` §10.1 —
//! so it is a faithful ZMTP peer for these tests, and its being *our* codec is
//! exactly why the interop bench against the pure-Rust `zeromq` crate is still
//! owed (slice 5, §10 items 3-6). What these tests defend is the bridge: the
//! handshake it drives, the patterns it maps, and the four things it refuses.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use weida::{
    ClientTls, GuaranteeSet, Identity, Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust,
};
use weida_zmq_bridge::{BridgeError, Inbound, InboundConfig, MidSegment, Presenting};
use weida_zmtp::{Command, FrameKind, Greeting, Mechanism, Metadata, SocketType, frame, greeting};

const DEADLINE: Duration = Duration::from_secs(10);
const CAP: u64 = 1024 * 1024;

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// The weida side: a runtime, a binding and the URL to reach an endpoint on it.
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
///
/// A connection the bridge gives up on says why through `tracing`, so the
/// subscriber is installed here: run a failing test with `--nocapture` and the
/// reason is in the output rather than inferred from a closed socket.
async fn bridge(config: InboundConfig) -> (SocketAddr, Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_zmq_bridge=debug")
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

/// A ZeroMQ peer: a TCP socket and the codec, nothing else.
struct Peer {
    io: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    /// Connects and completes the greeting and the NULL handshake as `ours`.
    async fn connect(addr: SocketAddr, ours: SocketType) -> Peer {
        let io = TcpStream::connect(addr).await.expect("connect");
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
        // The bridge's own READY, which a real peer must also read: the
        // handshake is READY both ways, and leaving it in the stream would
        // make every later read see it.
        let body = peer.read_command().await.expect("the bridge's READY");
        match Command::decode(&body).expect("decode") {
            Command::Ready(metadata) => {
                assert!(
                    metadata.socket_type().is_some(),
                    "the bridge must announce its socket type"
                );
            }
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

    /// Reads one frame, whatever it is.
    async fn read_frame(&mut self) -> std::io::Result<(FrameKind, Vec<u8>)> {
        loop {
            match frame::decode(&self.buf, CAP) {
                Ok((header, body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok((header.kind, body));
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => {
                    return Err(std::io::Error::other(e.to_string()));
                }
            }
        }
    }

    /// Reads one whole message, refusing to treat a command as one.
    async fn read_message(&mut self) -> std::io::Result<Vec<Vec<u8>>> {
        let mut parts = Vec::new();
        loop {
            let (kind, body) = self.read_frame().await?;
            match kind {
                FrameKind::Command => {
                    return Err(std::io::Error::other(format!(
                        "expected a message, got a command of {} octets",
                        body.len()
                    )));
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

    /// Reads until a command arrives, which is how the peer observes a refusal.
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
        self.buf.resize(at + 16 * 1024, 0);
        let n = self.io.read(&mut self.buf[at..]).await?;
        self.buf.truncate(at + n);
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        Ok(())
    }
}

/// Claim: a foreign REQ socket reaches a weida `Replier`, and the reply comes
/// back — including the empty delimiter frame REQ expects, which the bridge
/// consumed on the way in (`docs/adapters/zmtp.md` §2, §3).
#[tokio::test]
async fn a_foreign_req_reaches_a_weida_replier_and_the_reply_returns() {
    let weida_side = WeidaSide::start().await;
    let replier = weida_side.listener.replier("/rpc").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request.body().read_capped(4096).await.expect("body");
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

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/rpc"),
        Presenting::Rep,
    ))
    .await;

    let mut peer = Peer::connect(addr, SocketType::Req).await;
    // A REQ socket puts the empty delimiter on the wire ahead of the body.
    peer.send_message(&[&[], b"ping"]).await;
    let reply = within(peer.read_message()).await.expect("a reply");
    assert_eq!(
        reply,
        vec![Vec::new(), b"re:ping".to_vec()],
        "the delimiter comes back because a REQ socket expects one"
    );

    // Lockstep: a second exchange on the same connection works.
    peer.send_message(&[&[], b"again"]).await;
    let reply = within(peer.read_message()).await.expect("a second reply");
    assert_eq!(reply[1], b"re:again");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a foreign PUSH socket reaches a weida `Puller`, one ZMTP message per
/// weida transfer (§3).
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

    let mut peer = Peer::connect(addr, SocketType::Push).await;
    peer.send_message(&[b"one"]).await;
    peer.send_message(&[b"two"]).await;

    let first = within(puller.recv()).await.expect("first");
    assert_eq!(within(first.collect(64)).await.expect("collect"), b"one");
    let second = within(puller.recv()).await.expect("second");
    assert_eq!(within(second.collect(64)).await.expect("collect"), b"two");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a foreign SUB socket receives from a weida `Publisher`, with its
/// byte-prefix subscription translated into a segmented filter, and the topic
/// arriving as its own frame so the peer's own prefix match lands on the topic
/// (§6).
#[tokio::test]
async fn a_foreign_sub_receives_from_a_weida_publisher() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/feed").expect("publisher");

    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/feed"),
        Presenting::Pub,
    ))
    .await;

    let mut peer = Peer::connect(addr, SocketType::Sub).await;
    // A prefix ending at a segment boundary: `px.` becomes the filter `px.#`.
    peer.send_command(&Command::Subscribe(b"px.")).await;

    // The subscription travels peer → bridge → weida, so publish until the
    // publisher can see a subscriber rather than sleeping on it.
    while publisher.subscriber_count() == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    publisher
        .publish("px.eurusd", b"1.0921".to_vec())
        .expect("publish");

    let message = within(peer.read_message())
        .await
        .expect("a published message");
    assert_eq!(
        message,
        vec![b"px.eurusd".to_vec(), b"1.0921".to_vec()],
        "the topic is its own frame, ahead of the payload"
    );

    // A topic the filter does not select never arrives: publishing it and then
    // a selected one proves the order without a sleep.
    publisher
        .publish("fx.eurusd", b"nope".to_vec())
        .expect("publish");
    publisher
        .publish("px.gbpusd", b"1.2711".to_vec())
        .expect("publish");
    let message = within(peer.read_message()).await.expect("the next message");
    assert_eq!(message[0], b"px.gbpusd", "the unselected topic was skipped");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a socket type that may not talk to the bridge is refused with
/// `ERROR` before the close, which is what the specification asks for and what
/// lets the peer's operator see why (§2).
#[tokio::test]
async fn a_socket_type_that_may_not_talk_here_gets_an_error_frame() {
    let weida_side = WeidaSide::start().await;
    let _puller = weida_side.listener.puller("/jobs").expect("puller");
    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    ))
    .await;

    // PULL accepts PUSH and nothing else; a SUB peer is a configuration
    // mistake, not a protocol error.
    let mut peer = Peer::connect(addr, SocketType::Sub).await;
    let body = within(peer.read_command()).await.expect("an ERROR command");
    match Command::decode(&body).expect("decode") {
        Command::Error(reason) => {
            assert!(
                reason.contains("SUB") && reason.contains("PULL"),
                "the reason must name both types: {reason}"
            );
        }
        other => panic!("expected ERROR, got {}", other.name()),
    }

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a genuine multipart message is refused rather than flattened — loss
/// L1, and the default of §9.2. The connection ends, because a message that
/// cannot be carried is not a message this bridge can skip past: ZMTP delivers
/// multipart atomically, so dropping part of one would be worse than refusing.
#[tokio::test]
async fn a_multipart_message_is_refused_rather_than_flattened() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/jobs").expect("puller");
    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    ))
    .await;

    let mut peer = Peer::connect(addr, SocketType::Push).await;
    peer.send_message(&[b"envelope", b"payload"]).await;

    // The bridge closes rather than forwarding either frame or their
    // concatenation.
    let ended = within(peer.read_frame()).await;
    assert!(
        ended.is_err(),
        "the bridge must close on a multipart message, got {ended:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), puller.recv())
            .await
            .is_err(),
        "nothing may reach the weida puller"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a subscription no filter can express is refused with `ERROR` and
/// costs the peer that subscription only — the connection stays up, because a
/// SUB peer with one bad prefix and three good ones must keep the three
/// (L2, L4, §9.3).
#[tokio::test]
async fn a_subscription_that_no_filter_expresses_is_refused_and_the_peer_stays() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/feed").expect("publisher");
    let (addr, bridge_runtime) = bridge(InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/feed"),
        Presenting::Pub,
    ))
    .await;

    let mut peer = Peer::connect(addr, SocketType::Sub).await;
    // Mid-segment: `px.eur` also matches the topic `px.eurusd` for the peer,
    // and no weida filter selects that set.
    peer.send_command(&Command::Subscribe(b"px.eur")).await;
    let body = within(peer.read_command()).await.expect("an ERROR command");
    match Command::decode(&body).expect("decode") {
        Command::Error(reason) => assert!(
            reason.contains("segment boundary"),
            "the reason must say what is wrong: {reason}"
        ),
        other => panic!("expected ERROR, got {}", other.name()),
    }

    // And the connection is still usable: a prefix that does translate works.
    peer.send_command(&Command::Subscribe(b"px.")).await;
    while publisher.subscriber_count() == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    publisher
        .publish("px.eurusd", b"1.0921".to_vec())
        .expect("publish");
    let message = within(peer.read_message())
        .await
        .expect("a published message");
    assert_eq!(message[0], b"px.eurusd");

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: the opt-in of §9.3 subscribes at the enclosing boundary and
/// re-applies the byte prefix locally, so the peer gets exactly what it asked
/// for and the widening never reaches it.
#[tokio::test]
async fn the_mid_segment_opt_in_refilters_locally() {
    let weida_side = WeidaSide::start().await;
    let publisher = weida_side.listener.publisher("/feed").expect("publisher");
    let mut config = InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/feed"),
        Presenting::Pub,
    );
    config.mid_segment = MidSegment::BoundaryAndRefilter;
    let (addr, bridge_runtime) = bridge(config).await;

    let mut peer = Peer::connect(addr, SocketType::Sub).await;
    peer.send_command(&Command::Subscribe(b"px.eur")).await;
    while publisher.subscriber_count() == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // `px.#` brings both to the bridge; only the one the peer's prefix matches
    // is written to it.
    publisher
        .publish("px.gbpusd", b"1.2711".to_vec())
        .expect("publish");
    publisher
        .publish("px.eurusd", b"1.0921".to_vec())
        .expect("publish");
    let message = within(peer.read_message())
        .await
        .expect("a published message");
    assert_eq!(
        message,
        vec![b"px.eurusd".to_vec(), b"1.0921".to_vec()],
        "the widened filter's extra topic was dropped at the bridge"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}

/// Claim: a weida-side guarantee set above `core` is refused at configuration
/// time, because the ZeroMQ side has no mechanism to carry it (§9.4). This is
/// the rule at an adapter edge: refuse rather than silently fail to provide.
#[tokio::test]
async fn a_guarantee_set_above_core_is_refused_at_configuration_time() {
    let mut config = InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        "weida://127.0.0.1:7443/feed",
        Presenting::Pull,
    );
    config.runtime.guarantees = GuaranteeSet {
        ordering: weida::OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let err = match Inbound::bind(config, ClientTls::new(Trust::by_address())).await {
        Err(e) => e,
        Ok(_) => panic!("a set above core must be refused"),
    };
    match err {
        BridgeError::Configuration(reason) => assert!(
            reason.contains("core"),
            "the refusal must name the rule: {reason}"
        ),
        other => panic!("expected a configuration refusal, got {other:?}"),
    }
}

/// Claim: a message beyond `max_message_bytes` is refused from its frame
/// header, before the body is read (§3). The bound exists because ZMTP grants
/// no credit and allows 2^63-1 octets per frame.
#[tokio::test]
async fn a_message_beyond_the_cap_is_refused_before_it_is_buffered() {
    let weida_side = WeidaSide::start().await;
    let puller = weida_side.listener.puller("/jobs").expect("puller");
    let mut config = InboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_side.url("/jobs"),
        Presenting::Pull,
    );
    config.max_message_bytes = 1024;
    let (addr, bridge_runtime) = bridge(config).await;

    let mut peer = Peer::connect(addr, SocketType::Push).await;
    // A header that declares far more than the cap, and no body at all: if the
    // bridge waited for the body it would hang here instead of closing.
    let mut header = vec![0x02u8];
    header.extend_from_slice(&(64u64 * 1024).to_be_bytes());
    peer.io.write_all(&header).await.expect("write the header");

    let ended = within(peer.read_frame()).await;
    assert!(
        ended.is_err(),
        "the bridge must close on an over-large declaration, got {ended:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), puller.recv())
            .await
            .is_err(),
        "nothing may reach the weida puller"
    );

    bridge_runtime.shutdown().await;
    weida_side.runtime.shutdown().await;
}
