//! The guide's chapter 4: **depth, and what a chain may claim.**
//!
//! ```text
//! cargo run -p weida-cross-tests --example guide_depth
//! ```
//!
//! Five programs, one per claim `docs/GUIDE.md` §4 makes, in the shape
//! chapters 1 and 2 established: each returns its outcome as a **value**
//! rather than printing it, and `tests/guide_depth.rs` includes this file as a
//! module and asserts the claims.
//!
//! This chapter lives here rather than in `crates/weida/examples/` for one
//! reason: it needs **two foreign protocols**, and the two adapter crates know
//! nothing of each other on purpose (`docs/ARCHITECTURE.md` §4). A chain that
//! runs through both belongs to neither, which is why this crate exists at
//! all. Both foreign ends are the real implementations — `zeromq` in pure Rust
//! and `nng`'s C library — so a disagreement here is an interoperability bug
//! rather than a tautology.
//!
//! The chain is always the same shape:
//!
//! ```text
//! ZeroMQ peer --ZMTP--> inbound bridge --weida--> outbound bridge --SP--> nng peer
//! ```
//!
//! Two hops, three protocols, and every question of the chapter is about what
//! may be said at the far end of that picture.

use std::net::SocketAddr;
use std::time::Duration;

use nng::options::Options;
// `Delivery` is the transfer receipt in this library's public surface, so the
// guarantee dimension is re-exported as `DeliveryLevel`.
use weida::{
    Acknowledgement, ClientTls, DeliveryLevel, GuaranteeSet, Identity, Runtime, ServerTls, Trust,
};
use weida_nng_bridge::{
    Dialling as SpDialling, Inbound as SpInbound, InboundConfig as SpInboundConfig,
    Outbound as SpOutbound, OutboundConfig as SpOutboundConfig, Presenting as SpPresenting,
    TopicSplit,
};
use weida_zmq_bridge::{
    Dialling as ZmqDialling, Inbound as ZmqInbound, InboundConfig as ZmqInboundConfig,
    Outbound as ZmqOutbound, OutboundConfig as ZmqOutboundConfig, Presenting as ZmqPresenting,
    SubscriptionForm,
};
use zeromq::{Socket, SocketSend, ZmqMessage};

/// Every wait in this file is bounded: a broken chain must fail rather than
/// hang, and a chapter a reader runs must end.
const DEADLINE: Duration = Duration::from_secs(10);

/// The topic delimiter the SP side uses. NUL is the one octet a weida topic
/// cannot contain (`docs/adapters/nng.md` §6).
const NUL: u8 = 0;

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Lets a chain finish standing up: each bridge registers its weida endpoint
/// inside `serve`, so a message sent the instant after the spawn can arrive
/// before the far half exists.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}

/// An `nng` socket with finite timeouts.
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

/// Listens on an ephemeral port and asks `nng` which one it got.
///
/// `Url` and not `LocalAddr`: on a listener bound to port `0` the two options
/// disagree, and `LocalAddr` answers with a port nothing is listening on
/// (B-252).
fn nng_listen(socket: &nng::Socket) -> SocketAddr {
    let listener = nng::Listener::new(socket, "tcp://127.0.0.1:0").expect("nng listens");
    let url = listener
        .get_opt::<nng::options::Url>()
        .expect("the url nng resolved");
    url.trim_start_matches("tcp://")
        .parse()
        .unwrap_or_else(|e| panic!("{url}: {e}"))
}

/// A whole chain: a ZeroMQ address to send to, an `nng` socket to receive on,
/// and the two runtimes that carry the middle.
pub struct Chain {
    /// Where a foreign ZeroMQ peer connects.
    pub zmq_addr: SocketAddr,
    /// The weida runtime of the ZMTP edge.
    pub zmq_runtime: Runtime,
    /// The weida runtime of the SP edge.
    pub sp_runtime: Runtime,
}

impl Chain {
    async fn shutdown(self) {
        self.zmq_runtime.shutdown().await;
        self.sp_runtime.shutdown().await;
    }
}

/// Builds the chain in the picture above, with `nng_addr` as the far end and
/// `zmq_cap` as the ZMTP edge's payload ceiling.
///
/// Both edges assert `GuaranteeSet::CORE`, which is not decoration: an edge
/// configured above core is **refused at bind**, because neither foreign
/// protocol has a mechanism to carry more (`docs/adapters/zmtp.md` §9.4,
/// `docs/adapters/nng.md` §7). That refusal is the first half of this
/// chapter's claim, enforced by the code rather than by a document.
async fn chain(nng_addr: SocketAddr, zmq_cap: Option<usize>, split: TopicSplit) -> Chain {
    let mut sp_config = SpOutboundConfig::new(
        nng_addr,
        "127.0.0.1:0".parse().expect("loopback"),
        "/work",
        SpDialling::Push,
    );
    sp_config.topic_split = split;
    assert_eq!(sp_config.runtime.guarantees, GuaranteeSet::CORE);
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = sp_config.weida_path.clone();
    let sp = SpOutbound::bind(sp_config, ServerTls::new(identity))
        .await
        .expect("bind the SP outbound bridge");
    let weida_url = format!(
        "weida://{}@127.0.0.1:{}{}",
        fingerprint,
        sp.weida_addr().port(),
        path
    );
    let sp_runtime = sp.runtime().clone();
    tokio::spawn(async move {
        let _ = sp.serve().await;
    });

    let mut zmq_config = ZmqInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        ZmqPresenting::Pull,
    );
    if let Some(cap) = zmq_cap {
        zmq_config.max_message_bytes = cap as u64;
    }
    assert_eq!(zmq_config.runtime.guarantees, GuaranteeSet::CORE);
    let zmq = ZmqInbound::bind(zmq_config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the ZMTP inbound bridge");
    let zmq_addr = zmq.local_addr().expect("local addr");
    let zmq_runtime = zmq.runtime().clone();
    tokio::spawn(async move {
        let _ = zmq.serve().await;
    });

    settle().await;
    Chain {
        zmq_addr,
        zmq_runtime,
        sp_runtime,
    }
}

// --- §4.1 What crosses two protocols ---------------------------------------

/// What arrived at the far end of the chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Crossed {
    /// The topic the ZeroMQ publisher sent as its own frame.
    pub zmtp_topic_frame: Vec<u8>,
    /// The payload frame.
    pub zmtp_payload_frame: Vec<u8>,
    /// What the `nng` subscriber received, verbatim: one flat body.
    pub sp_body: Vec<u8>,
}

/// Claim §4.1: **a hop is a translation, and what crosses is what both
/// protocols can express.**
///
/// The topic is what makes the translation visible, because the two protocols
/// express it differently and neither is weida's way. ZMTP carries a topic as
/// its **own frame**, because a prefix match "won't cross a frame boundary"
/// ([zmtp](../../../docs/adapters/zmtp.md) §6); weida carries it as a
/// segmented field in the DATA header; SP has no topic field at all and a
/// subscription is a byte prefix of the body
/// ([nng](../../../docs/adapters/nng.md) §6). So this chain translates
/// **frame → weida topic → leading bytes**, and the NUL delimiter is what lets
/// the last step be undone by whoever reads it — NUL because it is the one
/// octet a weida topic cannot contain.
///
/// Note what the far end receives: `sport.football\0goal`, one flat body. The
/// frame boundary the sender used **does not exist** on the other side, and
/// the delimiter is a convention the reader has to know. That is what a
/// translating hop costs, and it is the cheapest example of it in this
/// repository.
///
/// # Errors
///
/// Never: every failure here is a broken chain and panics with the reason.
pub async fn crossing() -> Crossed {
    let mut publisher = zeromq::PubSocket::new();
    let zmq_addr = within(publisher.bind("tcp://127.0.0.1:0"))
        .await
        .expect("zmq.rs binds");
    let zmq_addr = match zmq_addr {
        zeromq::Endpoint::Tcp(host, port) => format!("{host}:{port}"),
        other => panic!("a tcp bind answered with {other}"),
    };

    // weida dials the ZeroMQ publisher and presents SP's Pub to the far end:
    // the chain is ZMTP-out at the near edge and SP-in at the far one, which
    // is the direction a subscription has to travel.
    let mut zmq_config = ZmqOutboundConfig::new(
        zmq_addr.parse().expect("the address zeromq bound"),
        "127.0.0.1:0".parse().expect("loopback"),
        "/news",
        ZmqDialling::Sub,
    );
    // zmq.rs announces ZMTP 3.0 and reads only the legacy subscription form
    // (`docs/adapters/zmtp.md` §10, B-043).
    zmq_config.subscription_form = SubscriptionForm::LegacyMessage;
    zmq_config.subscribe = vec![b"sport.".to_vec()];
    assert_eq!(zmq_config.runtime.guarantees, GuaranteeSet::CORE);
    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let path = zmq_config.weida_path.clone();
    let zmq = ZmqOutbound::bind(zmq_config, ServerTls::new(identity))
        .await
        .expect("bind the ZMTP outbound bridge");
    let weida_url = format!(
        "weida://{}@127.0.0.1:{}{}",
        fingerprint,
        zmq.weida_addr().port(),
        path
    );
    let zmq_runtime = zmq.runtime().clone();
    tokio::spawn(async move {
        let _ = zmq.serve().await;
    });

    let mut sp_config = SpInboundConfig::new(
        "127.0.0.1:0".parse().expect("loopback"),
        weida_url,
        SpPresenting::Pub,
    );
    sp_config.topic_delimiter = Some(NUL);
    assert_eq!(sp_config.runtime.guarantees, GuaranteeSet::CORE);
    let sp = SpInbound::bind(sp_config, ClientTls::new(Trust::by_address()))
        .await
        .expect("bind the SP inbound bridge");
    let sp_addr = sp.local_addr().expect("local addr");
    let sp_runtime = sp.runtime().clone();
    tokio::spawn(async move {
        let _ = sp.serve().await;
    });

    // `nng_dial` is synchronous and completes only once the SP handshake has
    // been answered, so it must not run on the thread the bridge's accept loop
    // needs.
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

    let zmtp_topic_frame = b"sport.football".to_vec();
    let zmtp_payload_frame = b"goal".to_vec();
    // A publisher drops what has no subscriber yet, on both protocols, so the
    // message is repeated until the chain is up rather than slept on.
    let received = tokio::task::spawn_blocking(move || sub.recv().map(|m| m.as_slice().to_vec()));
    let mut published = 0;
    let sp_body = loop {
        within(
            publisher.send(
                ZmqMessage::try_from(vec![
                    bytes::Bytes::from(zmtp_topic_frame.clone()),
                    bytes::Bytes::from(zmtp_payload_frame.clone()),
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
    let sp_body = sp_body.expect("an nng publication");

    let _ = publisher.close().await;
    zmq_runtime.shutdown().await;
    sp_runtime.shutdown().await;
    Crossed {
        zmtp_topic_frame,
        zmtp_payload_frame,
        sp_body,
    }
}

// --- §4.2 What the chain may claim ------------------------------------------

/// What the two ends of the chain each observed about one message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainClaim {
    /// Whether the ZeroMQ send returned successfully.
    pub near_edge_accepted: bool,
    /// Whether anything ever arrived at the SP end.
    pub far_edge_received: bool,
}

/// Claim §4.2: **the chain's claim is the weakest hop's, and nothing composes
/// upward.**
///
/// ZeroMQ's transfer point is `zmq_send` returning and nothing further
/// ([zmtp](../../../docs/adapters/zmtp.md) §7); SP has **no transfer point at
/// all** — no application acknowledgement, no broker receipt, no persistence
/// signal ([nng](../../../docs/adapters/nng.md) §7). weida's own
/// `Delivery::delivered()` proves one hop's transport and says nothing about
/// the application, let alone about the next protocol's
/// ([GUARANTEES.md](../../../docs/GUARANTEES.md) §1). Composed, the honest
/// end-to-end claim is `BestEffort`.
///
/// The program makes that a pair of observations rather than a sentence: the
/// far end is **closed before the send**, the send succeeds anyway, and a
/// fresh receiver on the same address proves nothing arrived.
///
/// # Errors
///
/// Never: a broken chain panics with the reason.
pub async fn chain_claim() -> ChainClaim {
    let pull = nng_socket(nng::Protocol::Pull0);
    let nng_addr = nng_listen(&pull);
    let chain = chain(nng_addr, None, TopicSplit::Delimiter(NUL)).await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{}", chain.zmq_addr)))
        .await
        .expect("connect");

    // The far end goes away, and nothing anywhere in the chain tells the
    // sender.
    pull.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let near_edge_accepted = within(push.send(ZmqMessage::from("into the void")))
        .await
        .is_ok();

    // There is nothing to observe delivery *with*, so the only honest check is
    // that the far end never got it: a fresh puller on the address the first
    // one released receives nothing. The listen is retried and asserted
    // rather than ignored, because a puller that never bound would also
    // receive nothing and would prove the claim for the wrong reason.
    let late = nng_socket(nng::Protocol::Pull0);
    let url = format!("tcp://{nng_addr}");
    let far_edge_received = tokio::task::spawn_blocking(move || {
        let mut bound = late.listen(&url);
        for _ in 0..50 {
            if bound.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            bound = late.listen(&url);
        }
        bound.expect("the late puller takes the address the first one released");
        late.recv().is_ok()
    });
    let far_edge_received = within(far_edge_received).await.expect("the late puller");

    let _ = push.close().await;
    chain.shutdown().await;
    ChainClaim {
        near_edge_accepted,
        far_edge_received,
    }
}

// --- §4.3 Whose limit decides -----------------------------------------------

/// What a payload above one edge's ceiling did to the chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapVerdict {
    /// The payload offered, in bytes.
    pub offered: usize,
    /// The ZMTP edge's ceiling.
    pub near_cap: usize,
    /// Whether the far end received anything at all.
    pub far_edge_received: bool,
}

/// Claim §4.3: **the smaller ceiling decides, and it decides at the first hop
/// that sees it.**
///
/// The ZMTP edge is configured with a 4 KiB payload ceiling and the SP edge
/// keeps its 1 MiB default. An 8 KiB message is refused at the first hop, and
/// the far edge **buffers nothing** — which is the part worth the program: a
/// chain does not carry a payload as far as it can and then discard it, so the
/// memory a refused message costs the second hop is zero.
///
/// # Errors
///
/// Never: a broken chain panics with the reason.
pub async fn smaller_cap_decides() -> CapVerdict {
    const NEAR_CAP: usize = 4 * 1024;
    const OFFERED: usize = 8 * 1024;

    let pull = nng_socket(nng::Protocol::Pull0);
    let nng_addr = nng_listen(&pull);
    let chain = chain(nng_addr, Some(NEAR_CAP), TopicSplit::Delimiter(NUL)).await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{}", chain.zmq_addr)))
        .await
        .expect("connect");
    // The send itself succeeds: ZeroMQ handed the message to a socket, which
    // is all its transfer point ever meant (§4.2).
    let _ = within(push.send(ZmqMessage::from(vec![0x7au8; OFFERED]))).await;

    let far_edge_received = tokio::task::spawn_blocking(move || pull.recv().is_ok());
    let far_edge_received = within(far_edge_received)
        .await
        .expect("the nng puller reported");

    let _ = push.close().await;
    chain.shutdown().await;
    CapVerdict {
        offered: OFFERED,
        near_cap: NEAR_CAP,
        far_edge_received,
    }
}

// --- §4.4 A refusal that cannot travel -------------------------------------

/// What a message with no weida counterpart did to the chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefusedAtTheEdge {
    /// Frames the ZeroMQ peer sent in one message.
    pub frames: usize,
    /// Whether the ZeroMQ send returned successfully anyway.
    pub near_edge_accepted: bool,
    /// Whether the far end received anything.
    pub far_edge_received: bool,
}

/// Claim §4.4: **a loss at the first hop is invisible to the sender and total
/// for the chain.**
///
/// A two-frame ZeroMQ message has no weida counterpart — there is "no
/// message-part concept anywhere in v0"
/// ([zmtp](../../../docs/adapters/zmtp.md) §3) — and concatenating the frames
/// would invent an application protocol, so the bridge refuses it. That is
/// loss **L1** of the adapter's named losses, and the composition is the
/// lesson: the SP side could have carried those bytes perfectly well, and it
/// never sees them. A chain's capability is an intersection, not a union.
///
/// The sender's side of it is the uncomfortable half, and it follows from
/// §4.2: `zmq_send` returned, so by ZeroMQ's own contract the send *succeeded*
/// — of a message that will never exist anywhere else.
///
/// # Errors
///
/// Never: a broken chain panics with the reason.
pub async fn refused_at_the_first_hop() -> RefusedAtTheEdge {
    let pull = nng_socket(nng::Protocol::Pull0);
    let nng_addr = nng_listen(&pull);
    let chain = chain(nng_addr, None, TopicSplit::Delimiter(NUL)).await;

    let mut push = zeromq::PushSocket::new();
    within(push.connect(&format!("tcp://{}", chain.zmq_addr)))
        .await
        .expect("connect");

    let mut multipart = ZmqMessage::from(bytes::Bytes::from_static(b"part one"));
    multipart.push_back(bytes::Bytes::from_static(b"part two"));
    let frames = multipart.len();
    let near_edge_accepted = within(push.send(multipart)).await.is_ok();

    let far_edge_received = tokio::task::spawn_blocking(move || pull.recv().is_ok());
    let far_edge_received = within(far_edge_received)
        .await
        .expect("the nng puller reported");

    let _ = push.close().await;
    chain.shutdown().await;
    RefusedAtTheEdge {
        frames,
        near_edge_accepted,
        far_edge_received,
    }
}

// --- §4.5 The intersection, as arithmetic -----------------------------------

/// What intersecting two guarantee sets produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Intersection {
    /// The set a weida hop offers by default.
    pub core: GuaranteeSet,
    /// What two core hops agree on.
    pub agreed: GuaranteeSet,
    /// The set a hop that could promise more would offer.
    pub stronger: GuaranteeSet,
    /// What a core hop and that stronger hop agree on — the number that shows
    /// the direction of the arithmetic, which `core ∩ core` cannot.
    pub agreed_with_stronger: GuaranteeSet,
    /// Whether a caller can ask a live connection what it negotiated.
    pub observable_on_a_connection: bool,
}

/// Claim §4.5: **the intersection is arithmetic you can do yourself, and the
/// runtime's own answer is not observable.**
///
/// `GuaranteeSet::intersect` is the whole mechanism: per dimension the weaker
/// of the two levels, an exact match required for the dimensions that are not
/// ordered, and a connection **refused** with `NEGOTIATION_FAILED` when the
/// result does not reach what a peer requires — there is no downgrade path
/// ([GUARANTEES.md](../../../docs/GUARANTEES.md) §4,
/// [0006](../../../docs/decisions/0006-guarantee-sets.md) §4.4). Both sides
/// run it on their own and the peer's HELLO, so both reach the same verdict
/// without a round trip.
///
/// The honest second half is what this program cannot show: **no public
/// accessor reports the negotiated set of a live connection.** The value is
/// computed, stored and enforced inside the runtime, and an application that
/// wants to know what its connection agreed to has to read the adapter's
/// mapping document instead of asking. That is a gap this chapter names rather
/// than papers over, and the field below reports it as `false` so a future
/// accessor breaks this test.
///
/// # Errors
///
/// Never.
pub fn intersection() -> Intersection {
    let core = GuaranteeSet::CORE;
    let agreed = core
        .intersect(&core)
        .expect("core is comparable with itself");

    // A hop that could promise more. `core ∩ core` is symmetric, so it cannot
    // show *which* direction the intersection goes — which is the whole
    // claim. This pair can: a peer offering `AtLeastOnce` delivery and a
    // broker's `Accepted` completion still agrees on core with a hop that
    // offers neither.
    let stronger = GuaranteeSet {
        delivery: DeliveryLevel::AtLeastOnce,
        acknowledgement: Acknowledgement::Accepted,
        ..GuaranteeSet::CORE
    };
    let agreed_with_stronger = core
        .intersect(&stronger)
        .expect("the two sets differ only in ordered dimensions");

    Intersection {
        core,
        agreed,
        stronger,
        agreed_with_stronger,
        // There is no `Connection::guarantees()`, no `Endpoint::negotiated()`
        // and nothing on `Runtime`: `Agreed` is `pub(crate)`. Checked by hand
        // against the public surface of `weida` at this commit.
        observable_on_a_connection: false,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("§4.1 what crosses two protocols");
    let crossed = crossing().await;
    println!(
        "     ZMTP sent two frames, {:?} and {:?}",
        String::from_utf8_lossy(&crossed.zmtp_topic_frame),
        String::from_utf8_lossy(&crossed.zmtp_payload_frame)
    );
    println!(
        "     SP received one body, {:?} — the frame boundary became a NUL",
        String::from_utf8_lossy(&crossed.sp_body)
    );

    println!("§4.2 what the chain may claim");
    let claim = chain_claim().await;
    println!(
        "     the ZeroMQ send succeeded: {}; anything arrived at the SP end: {}",
        claim.near_edge_accepted, claim.far_edge_received
    );

    println!("§4.3 whose limit decides");
    let cap = smaller_cap_decides().await;
    println!(
        "     {} B offered through a {} B near cap and a 1 MiB far cap: far end received {}",
        cap.offered, cap.near_cap, cap.far_edge_received
    );

    println!("§4.4 a refusal that cannot travel");
    let refused = refused_at_the_first_hop().await;
    println!(
        "     {} ZMTP frames in one message: the send succeeded ({}), the far end received {}",
        refused.frames, refused.near_edge_accepted, refused.far_edge_received
    );

    println!("§4.5 the intersection, as arithmetic");
    let intersected = intersection();
    println!(
        "     core ∩ core = {:?} delivery, {:?} acknowledgement",
        intersected.agreed.delivery, intersected.agreed.acknowledgement
    );
    println!(
        "     core ∩ ({:?}, {:?}) = {:?} delivery, {:?} acknowledgement  <- the weaker, per dimension",
        intersected.stronger.delivery,
        intersected.stronger.acknowledgement,
        intersected.agreed_with_stronger.delivery,
        intersected.agreed_with_stronger.acknowledgement
    );
    println!(
        "     can an application ask a live connection what it negotiated? {}",
        intersected.observable_on_a_connection
    );
    Ok(())
}
