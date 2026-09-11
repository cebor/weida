//! One message, two foreign protocols, weida in the middle.
//!
//! Phase B slice 6 (`docs/LOOP.md` §9), the item both mapping documents have
//! been promising: `docs/adapters/zmtp.md` §10 item 7 and
//! `docs/adapters/nng.md` §10. A message enters through one adapter and
//! leaves through the other, and what is asserted is what **both** documents
//! promise - not what either promises alone.
//!
//! # The weakest link, stated once and asserted below
//!
//! ZeroMQ's transfer point is `zmq_send` returning and nothing further
//! [zmtp §7]; SP has **no transfer point at all** - no application
//! acknowledgement, no broker receipt, no persistence signal [nng §7],
//! [nanomsg-nng §6]. weida's own `Delivery::delivered()` proves one hop's
//! transport and says nothing about the application, let alone about the
//! *next* protocol's application ([GUARANTEES.md](../../../docs/GUARANTEES.md)
//! §1). Composed, the chain's honest claim is therefore `BestEffort` end to
//! end, and `the_chain_is_best_effort_end_to_end` asserts it rather than
//! leaving it to a comment: a send that succeeded at the ZeroMQ edge is
//! satisfied even when nothing ever reaches the SP end.
//!
//! # Both foreign ends are the real implementations
//!
//! `zeromq` (pure Rust) and `nng` (the C library through its Rust binding).
//! A peer built on this repository's own codec is faithful on the wire and
//! shares every assumption with the code under test; the point of this file
//! is that neither end does. The one exception is
//! `an_sp_hop_count_ceiling_at_the_second_hop_reaches_the_first_peer_as_silence`,
//! which needs a reply carrying a multi-hop tag stack - something no single
//! REP socket produces without an `nng_device` topology [nanomsg-nng §4] - so
//! that one peer is raw TCP on `weida-sp`, and says so.

use std::net::SocketAddr;
use std::time::Duration;

use nng::options::Options;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida::{ClientTls, GuaranteeSet, Identity, Runtime, ServerTls, Trust};
use weida_sp::header::EndpointType;
use weida_sp::{Backtrace, ProtocolHeader, backtrace, message};
use weida_sp_bridge::{
    Dialling as SpDialling, Inbound as SpInbound, InboundConfig as SpInboundConfig,
    Outbound as SpOutbound, OutboundConfig as SpOutboundConfig, Presenting as SpPresenting,
    TopicSplit,
};
use weida_zmtp_bridge::{
    Dialling as ZmqDialling, Inbound as ZmqInbound, InboundConfig as ZmqInboundConfig,
    Outbound as ZmqOutbound, OutboundConfig as ZmqOutboundConfig, Presenting as ZmqPresenting,
    SubscriptionForm,
};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

const DEADLINE: Duration = Duration::from_secs(10);
/// The topic delimiter both SP-side configurations use. NUL is the one octet
/// a weida topic cannot contain (`docs/adapters/nng.md` §6).
const NUL: u8 = 0;

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

fn tracing_once() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("weida_zmtp_bridge=debug,weida_sp_bridge=debug")
        .try_init();
}

/// Lets a chain finish standing up.
///
/// Each bridge registers its weida endpoint inside `serve`, so a message sent
/// the instant after `spawn` can arrive before the far half exists. Every
/// assertion below still has its own deadline; this only keeps the common
/// case off the retry paths.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}

async fn free_port() -> (SocketAddr, String) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);
    (addr, format!("tcp://{addr}"))
}

/// Starts a ZMTP inbound bridge: foreign ZeroMQ peers in, weida onward.
async fn zmtp_in(config: ZmqInboundConfig) -> (SocketAddr, Runtime) {
    tracing_once();
    assert_eq!(
        config.runtime.guarantees,
        GuaranteeSet::CORE,
        "every edge of this chain runs `core`; anything else is refused at bind \
         (docs/adapters/zmtp.md §9.4)"
    );
    let bridge = ZmqInbound::bind(config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the ZMTP inbound bridge");
    let addr = bridge.local_addr().expect("local addr");
    let runtime = bridge.runtime().clone();
    tokio::spawn(async move {
        let _ = bridge.serve().await;
    });
    (addr, runtime)
}

/// Starts a ZMTP outbound bridge: weida in, a foreign ZeroMQ peer onward.
async fn zmtp_out(config: ZmqOutboundConfig) -> (String, Runtime) {
    tracing_once();
    assert_eq!(config.runtime.guarantees, GuaranteeSet::CORE);
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = config.weida_path.clone();
    let bridge = ZmqOutbound::bind(config, ServerTls::new(identity))
        .await
        .expect("bind the ZMTP outbound bridge");
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

/// Starts an SP inbound bridge: foreign SP peers in, weida onward.
async fn sp_in(config: SpInboundConfig) -> (SocketAddr, Runtime) {
    tracing_once();
    assert_eq!(
        config.runtime.guarantees,
        GuaranteeSet::CORE,
        "SP has no transfer point, so `core` is the only set an SP edge can carry \
         (docs/adapters/nng.md §7)"
    );
    let bridge = SpInbound::bind(config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the SP inbound bridge");
    let addr = bridge.local_addr().expect("local addr");
    let runtime = bridge.runtime().clone();
    tokio::spawn(async move {
        let _ = bridge.serve().await;
    });
    (addr, runtime)
}

/// Starts an SP outbound bridge: weida in, a foreign SP peer onward.
async fn sp_out(config: SpOutboundConfig) -> (String, Runtime) {
    tracing_once();
    assert_eq!(config.runtime.guarantees, GuaranteeSet::CORE);
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = config.weida_path.clone();
    let bridge = SpOutbound::bind(config, ServerTls::new(identity))
        .await
        .expect("bind the SP outbound bridge");
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

/// An `nng` socket with finite timeouts, so a broken chain fails a test
/// instead of hanging one.
fn nng_socket(protocol: nng::Protocol) -> nng::Socket {
    let socket = nng::Socket::new(protocol).expect("nng socket");
    socket
        .set_opt::<nng::options::RecvTimeout>(Some(Duration::from_secs(5)))
        .expect("recv timeout");
    socket
        .set_opt::<nng::options::SendTimeout>(Some(Duration::from_secs(5)))
        .expect("send timeout");
    socket
}

fn one_frame(message: &ZmqMessage) -> &[u8] {
    assert_eq!(message.len(), 1, "these patterns send single-part messages");
    message.get(0).expect("the frame")
}

// --- ZMTP in, SP out ---------------------------------------------------------

/// Claim: a real ZeroMQ `REQ` reaches a real NNG `REP` through two bridges and
/// weida in between, and the reply comes back - with each protocol's envelope
/// restored on its own side.
///
/// Neither envelope crosses: the 28/REQREP delimiter is consumed by the ZMTP
/// bridge [zmtp §2] and the 32-bit tag stack is written by the SP bridge
/// [nng §3], [rfc-reqrep §5]. If either had been forwarded as payload, the
/// far peer would have answered the wrong bytes or not at all - which is why
/// asserting the payload is enough to assert both.
#[tokio::test]
async fn a_zmq_req_reaches_an_nng_rep_and_the_reply_returns() {
    let (nng_addr, nng_url) = free_port().await;
    let rep = nng_socket(nng::Protocol::Rep0);
    rep.listen(&nng_url).expect("nng listens");
    let responder = std::thread::spawn(move || {
        let request = rep.recv().expect("a request");
        let mut answer = b"re:".to_vec();
        answer.extend_from_slice(request.as_slice());
        rep.send(nng::Message::from(answer.as_slice()))
            .expect("send the reply");
    });

    let (weida_url, sp_runtime) = sp_out(SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        SpDialling::Req,
    ))
    .await;
    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Rep,
    ))
    .await;
    settle().await;

    let mut req = zeromq::ReqSocket::new();
    within(req.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("zmq.rs connects");
    within(req.send(ZmqMessage::from("hello")))
        .await
        .expect("send");
    let reply = within(req.recv()).await.expect("a reply across both hops");
    assert_eq!(one_frame(&reply), b"re:hello");

    responder.join().expect("the nng responder");
    let _ = req.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim: a real ZeroMQ `PUSH` reaches a real NNG `PULL` through the chain,
/// one message per transfer, in order on one connection.
///
/// Order is a per-hop property on both sides - ZeroMQ's pipe is ordered
/// [zmtp §7], weida's ordering is `None` across streams
/// ([GUARANTEES.md](../../../docs/GUARANTEES.md) §6) - so what this asserts is
/// what one connection through one bridge pair actually delivers, and nothing
/// stronger.
#[tokio::test]
async fn a_zmq_push_reaches_an_nng_pull() {
    let (nng_addr, nng_url) = free_port().await;
    let pull = nng_socket(nng::Protocol::Pull0);
    pull.listen(&nng_url).expect("nng listens");

    let (weida_url, sp_runtime) = sp_out(SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        SpDialling::Push,
    ))
    .await;
    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pull,
    ))
    .await;
    settle().await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("connect");
    for i in 0..3u8 {
        within(push.send(ZmqMessage::from(vec![b'j', b'0' + i])))
            .await
            .expect("send");
    }

    let received = tokio::task::spawn_blocking(move || {
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(pull.recv().expect("an nng message").as_slice().to_vec());
        }
        seen
    });
    let seen = within(received).await.expect("the nng puller");
    assert_eq!(
        seen,
        vec![b"j0".to_vec(), b"j1".to_vec(), b"j2".to_vec()],
        "one connection through one bridge pair keeps its order"
    );

    let _ = push.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim: a real ZeroMQ `PUB` reaches a real NNG `SUB`, with **both** topic
/// conventions applied in turn - and they are different conventions, which is
/// the point.
///
/// ZMTP carries the topic as its own frame, because a prefix match "won't
/// cross a frame boundary" [zmtp §6]; SP has no topic field at all and a
/// subscription is a byte prefix of the body [nng §6], [nanomsg-nng §3]. The
/// chain therefore translates frame → weida topic → leading bytes, and the
/// NUL delimiter is what lets the last step be undone by whoever reads it.
#[tokio::test]
async fn a_zmq_pub_reaches_an_nng_sub_through_both_topic_conventions() {
    let (zmq_addr, zmq_url) = free_port().await;
    let mut publisher = zeromq::PubSocket::new();
    within(publisher.bind(&zmq_url))
        .await
        .expect("zmq.rs binds");

    let mut zmq_config = ZmqOutboundConfig::new(
        zmq_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/news",
        ZmqDialling::Sub,
    );
    // zmq.rs announces ZMTP 3.0 and reads only the legacy subscription form
    // (`docs/adapters/zmtp.md` §10, B-043).
    zmq_config.subscription_form = SubscriptionForm::LegacyMessage;
    zmq_config.subscribe = vec![b"sport.".to_vec()];
    let (weida_url, zmq_runtime) = zmtp_out(zmq_config).await;

    let mut sp_config = SpInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        SpPresenting::Pub,
    );
    sp_config.topic_delimiter = Some(NUL);
    let (sp_addr, sp_runtime) = sp_in(sp_config).await;

    // `nng_dial` is synchronous and completes only once the SP handshake has
    // been answered, so it must not run on the thread the bridge's accept
    // loop needs.
    let sub = tokio::task::spawn_blocking({
        let url = format!("tcp://{sp_addr}");
        move || {
            let sub = nng_socket(nng::Protocol::Sub0);
            sub.dial(&url).expect("nng dials");
            sub.set_opt::<nng::options::protocol::pubsub::Subscribe>(b"sport.".to_vec())
                .expect("subscribe");
            sub
        }
    })
    .await
    .expect("the nng subscriber");
    settle().await;

    // A publisher drops what has no subscriber yet, on both protocols, so the
    // message is repeated until the chain is up rather than slept on.
    let received = tokio::task::spawn_blocking(move || sub.recv().map(|m| m.as_slice().to_vec()));
    let mut published = 0;
    let body = loop {
        within(
            publisher.send(
                ZmqMessage::try_from(vec![
                    bytes::Bytes::from_static(b"sport.football"),
                    bytes::Bytes::from_static(b"goal"),
                ])
                .expect("a two-frame message"),
            ),
        )
        .await
        .expect("publish");
        published += 1;
        if received.is_finished() || published > 40 {
            break received.await.expect("the nng subscriber");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let body = body.expect("an nng publication");
    assert_eq!(
        body, b"sport.football\0goal",
        "the ZMTP topic frame became a weida topic and then SP's leading bytes"
    );

    let _ = publisher.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

// --- SP in, ZMTP out ---------------------------------------------------------

/// Claim: the reverse chain carries Push/Pull too - a real NNG `PUSH` reaches
/// a real ZeroMQ `PULL`.
#[tokio::test]
async fn an_nng_push_reaches_a_zmq_pull() {
    let (zmq_addr, zmq_url) = free_port().await;
    let mut pull = zeromq::PullSocket::new();
    within(pull.bind(&zmq_url)).await.expect("zmq.rs binds");

    let (weida_url, zmq_runtime) = zmtp_out(ZmqOutboundConfig::new(
        zmq_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        ZmqDialling::Push,
    ))
    .await;
    let (sp_addr, sp_runtime) = sp_in(SpInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        SpPresenting::Pull,
    ))
    .await;
    settle().await;

    // Dial and send on a blocking thread: `nng_dial` waits for the SP
    // handshake, which the bridge's accept loop has to run to answer.
    let url = format!("tcp://{sp_addr}");
    tokio::task::spawn_blocking(move || {
        let push = nng_socket(nng::Protocol::Push0);
        push.dial(&url).expect("nng dials");
        for i in 0..3u8 {
            push.send(nng::Message::from([b'n', b'0' + i].as_slice()))
                .expect("nng send");
        }
    })
    .await
    .expect("the nng pusher");

    let mut seen = Vec::new();
    for _ in 0..3 {
        let message = within(pull.recv()).await.expect("recv");
        seen.push(one_frame(&message).to_vec());
    }
    assert_eq!(seen, vec![b"n0".to_vec(), b"n1".to_vec(), b"n2".to_vec()]);

    let _ = pull.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim: the reverse chain carries Pub/Sub, with the topic conventions
/// applied in the other order - SP's leading bytes become a weida topic and
/// then a ZMTP frame of its own.
#[tokio::test]
async fn an_nng_pub_reaches_a_zmq_sub_through_both_topic_conventions() {
    let (nng_addr, nng_url) = free_port().await;
    let publisher = nng_socket(nng::Protocol::Pub0);
    publisher.listen(&nng_url).expect("nng listens");

    let mut sp_config = SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/feed",
        SpDialling::Sub,
    );
    sp_config.subscribe = vec![b"px.".to_vec()];
    sp_config.topic_split = TopicSplit::Delimiter(NUL);
    let (weida_url, sp_runtime) = sp_out(sp_config).await;

    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pub,
    ))
    .await;

    let mut sub = zeromq::SubSocket::new();
    within(sub.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("zmq.rs connects");
    within(sub.subscribe("px.")).await.expect("subscribe");
    settle().await;

    let publishing = std::thread::spawn(move || {
        for _ in 0..40 {
            if publisher
                .send(nng::Message::from(b"px.eurusd\x001.0921".as_slice()))
                .is_err()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    let message = within(sub.recv()).await.expect("a publication");
    assert_eq!(
        message.len(),
        2,
        "ZMTP carries the topic as its own frame [zmtp §6]"
    );
    assert_eq!(message.get(0).expect("topic"), &b"px.eurusd"[..]);
    assert_eq!(message.get(1).expect("payload"), &b"1.0921"[..]);

    let _ = sub.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
    let _ = publishing.join();
}

// --- What the chain does not promise ----------------------------------------

/// Claim: the chain's end-to-end guarantee is `BestEffort`, and that is
/// asserted rather than described.
///
/// A ZeroMQ `PUSH` that returns has handed its message to a socket [zmtp §7];
/// SP has no transfer point to confirm anything [nng §7]. So a send can
/// succeed at the first edge while nothing arrives at the far end, and here
/// nothing can: the NNG puller is closed before the message is sent. The
/// assertion is the pair - the send succeeded, the message is gone - which is
/// exactly what "BestEffort end to end" means and what neither document may
/// be read as promising more than.
#[tokio::test]
async fn the_chain_is_best_effort_end_to_end() {
    let (nng_addr, nng_url) = free_port().await;
    let pull = nng_socket(nng::Protocol::Pull0);
    pull.listen(&nng_url).expect("nng listens");

    let (weida_url, sp_runtime) = sp_out(SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        SpDialling::Push,
    ))
    .await;
    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pull,
    ))
    .await;
    settle().await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("connect");

    // The far end goes away, and nothing anywhere in the chain tells the
    // sender.
    pull.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    within(push.send(ZmqMessage::from("into the void")))
        .await
        .expect("the ZeroMQ send succeeds: it means the socket took it, nothing more");

    // And there is no way to observe delivery, because there is nothing to
    // observe it with: the only honest check is that the far end never got
    // it, which a fresh puller on the same address proves by receiving
    // nothing.
    let late = nng_socket(nng::Protocol::Pull0);
    let missed = tokio::task::spawn_blocking(move || {
        let _ = late.listen(&nng_url);
        late.recv().is_err()
    });
    assert!(
        within(missed).await.expect("the late puller"),
        "a message accepted at the ZeroMQ edge is not a message delivered at the SP edge"
    );

    let _ = push.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim (composed loss): a ZMTP multipart message is refused at the **first**
/// hop and nothing reaches the second.
///
/// Multipart has no weida counterpart - "no message-part concept anywhere in
/// v0" [zmtp §3] - and concatenating would invent an application protocol, so
/// the ZMTP bridge refuses (loss L1 of that document). The composition worth
/// asserting is the negative: the SP end, which has its own framing and could
/// perfectly well have carried the concatenation, never sees it. A loss at
/// the first hop is not repaired by the second protocol's capabilities.
#[tokio::test]
async fn a_zmtp_multipart_is_refused_at_the_first_hop_and_never_reaches_the_second() {
    let (nng_addr, nng_url) = free_port().await;
    let pull = nng_socket(nng::Protocol::Pull0);
    pull.listen(&nng_url).expect("nng listens");

    let (weida_url, sp_runtime) = sp_out(SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        SpDialling::Push,
    ))
    .await;
    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pull,
    ))
    .await;
    settle().await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("connect");
    let multipart = ZmqMessage::try_from(vec![
        bytes::Bytes::from_static(b"part one"),
        bytes::Bytes::from_static(b"part two"),
    ])
    .expect("a two-frame message");
    within(push.send(multipart))
        .await
        .expect("the socket takes it");

    let starved = tokio::task::spawn_blocking(move || pull.recv().is_err());
    assert!(
        within(starved).await.expect("the nng puller"),
        "the refusal happened at the ZMTP edge; the SP edge saw nothing to carry"
    );

    let _ = push.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim (composed bound): a payload over the **smaller** of the two caps is
/// refused at the edge that owns that cap, and nothing is buffered at the
/// other.
///
/// Both bridges bound what they hold, for the same reason and with the same
/// arithmetic [zmtp §3], [nng §3]; composed, the chain's effective cap is the
/// smaller one, and it applies at the first hop that sees the payload. The
/// test makes the ZMTP edge the smaller one (4 KiB against the SP edge's
/// 1 MiB default) and asserts that an 8 KiB message dies there.
#[tokio::test]
async fn the_smaller_cap_decides_and_the_far_edge_buffers_nothing() {
    let (nng_addr, nng_url) = free_port().await;
    let pull = nng_socket(nng::Protocol::Pull0);
    pull.listen(&nng_url).expect("nng listens");

    let sp_config = SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        SpDialling::Push,
    );
    assert_eq!(
        sp_config.max_message_bytes,
        1024 * 1024,
        "the SP edge keeps its default; the ZMTP edge is the smaller one"
    );
    let (weida_url, sp_runtime) = sp_out(sp_config).await;

    let mut zmq_config = ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pull,
    );
    zmq_config.max_message_bytes = 4096;
    let (zmq_addr, zmq_runtime) = zmtp_in(zmq_config).await;
    settle().await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("connect");
    let _ = push.send(ZmqMessage::from(vec![0xAB; 8192])).await;

    let starved = tokio::task::spawn_blocking(move || pull.recv().is_err());
    assert!(
        within(starved).await.expect("the nng puller"),
        "the oversized payload was refused before the SP edge could hold any of it"
    );

    // A small message on a fresh connection still works: the cap refused a
    // message, not the chain.
    let mut again = zeromq::PushSocket::new();
    within(again.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("reconnect");
    within(again.send(ZmqMessage::from("small")))
        .await
        .expect("send");

    let _ = push.close().await;
    let _ = again.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}

/// Claim (composed loss): an SP hop-count ceiling at the **second** hop
/// reaches the first peer only as silence.
///
/// The SP bridge bounds the tag stack it will decode by the local `MAXTTL`
/// [nng §3], [nanomsg-nng §11]; a reply past it is refused, and SP has no
/// error frame to say so with (loss L10). Upstream, the ZMTP bridge has an
/// `ERROR` command it *could* have used and no reply to attach it to, so what
/// the ZeroMQ requester observes is a closed connection and no answer. That
/// conversion - a refusal at the far hop becoming silence at the near one -
/// is the thing this chain adds to either document alone.
///
/// The SP peer here is raw TCP on `weida-sp` rather than `nng`: a reply with
/// three forwarder ids in its stack is what a chain of `nng_device` hops
/// produces [nanomsg-nng §4], and standing one up is the interop bench's
/// business, not this test's.
#[tokio::test]
async fn an_sp_hop_count_ceiling_at_the_second_hop_reaches_the_first_peer_as_silence() {
    let sp_peer = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the raw SP peer");
    let sp_addr = sp_peer.local_addr().expect("addr");

    let mut sp_config = SpOutboundConfig::new(
        sp_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/rpc",
        SpDialling::Req,
    );
    // One hop: a reply that has been through three devices is past it.
    sp_config.max_hops = 1;
    sp_config.reply_deadline = Duration::from_secs(600);
    let (weida_url, sp_runtime) = sp_out(sp_config).await;

    // The peer answers with a stack deeper than the bridge will decode.
    let deep = tokio::spawn(async move {
        let (mut io, _) = sp_peer.accept().await.expect("accept");
        let mut header = [0u8; 8];
        io.read_exact(&mut header).await.expect("their header");
        assert_eq!(
            ProtocolHeader::decode(&header).expect("header").endpoint,
            EndpointType::Req
        );
        io.write_all(&ProtocolHeader::new(EndpointType::Rep).encode())
            .await
            .expect("our header");

        // Read one message, whatever its size, then answer it.
        let mut buf = Vec::new();
        let body = loop {
            match message::decode(&buf, 1024 * 1024) {
                Ok((body, _)) => break body.to_vec(),
                Err(e) if !e.is_violation() => {
                    let mut chunk = [0u8; 1024];
                    let n = io.read(&mut chunk).await.expect("read");
                    assert!(n > 0, "the bridge closed before sending a request");
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => panic!("the bridge sent something unreadable: {e}"),
            }
        };
        let (stack, _) = backtrace::decode(&body, 8).expect("the bridge's own stack");
        let mut deep = Backtrace::direct(stack.id);
        for peer in 1..=3u32 {
            deep.push_peer(peer);
        }
        io.write_all(&message::encode(&deep.encode_message(b"pong")))
            .await
            .expect("write the reply");
        // Hold the connection so the failure is the ceiling, not a close.
        tokio::time::sleep(Duration::from_secs(3)).await;
    });

    let (zmq_addr, zmq_runtime) = zmtp_in(ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Rep,
    ))
    .await;
    settle().await;

    let mut req = zeromq::ReqSocket::new();
    within(req.connect(&format!("tcp://{zmq_addr}")))
        .await
        .expect("connect");
    within(req.send(ZmqMessage::from("hello")))
        .await
        .expect("send");

    let answered = tokio::time::timeout(Duration::from_secs(2), req.recv()).await;
    match answered {
        Err(_) => {}
        Ok(Err(_)) => {}
        Ok(Ok(message)) => panic!(
            "the refused reply must not reach the ZeroMQ peer, got {:?}",
            one_frame(&message)
        ),
    }

    let _ = deep.await;
    let _ = req.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
}
