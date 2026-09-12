//! The inbound direction: foreign ZeroMQ peers, weida onward.
//!
//! One `weida-zmq` socket bound on the ZeroMQ side, one weida endpoint, and
//! one loop per socket type. **The protocol is not here any more**
//! ([0013](https://git.doodleshnookie.net/hannes/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2): the greeting, the socket-type check, the framing, the envelope a
//! pattern defines, `PING`/`PONG`, the subscription wire forms and their
//! reference counting are `weida-zmq`'s, and what remains is the mapping.

use std::net::SocketAddr;

use weida::{ClientTls, GuaranteeSet, Runtime, RuntimeConfig};
use weida_zmq::{
    Context, ContextConfig, Endpoint, Message, Multipart, PullSocket, RepSocket, SocketOptions,
    TcpHost, XPubSocket, subscriptions::read_message_form,
};

use crate::error::BridgeError;
use crate::subscriptions::{Filters, MidSegment, translate};

/// Which ZeroMQ socket type the bridge presents to whoever connects.
///
/// Three, because these are the three weida patterns exist for and the three
/// whose ZeroMQ counterparts bind rather than connect
/// (`docs/ARCHITECTURE.md` §6c.4). The peer's own type is checked against this
/// one by the library, with the specification's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presenting {
    /// `REP`: a `REQ` or `DEALER` peer's messages become weida Req/Rep
    /// exchanges, and the reply travels back.
    Rep,
    /// `PULL`: a `PUSH` peer's messages become weida one-way transfers.
    Pull,
    /// `PUB`: a `SUB` peer's subscriptions become weida filters and the
    /// published messages travel to it.
    ///
    /// The socket underneath is an **XPUB**, because the subscriptions have to
    /// reach this application to be translated — which is what XPUB is for,
    /// and what libzmq's own last-value-cache recipe does with it. A ZeroMQ
    /// `SUB` peer cannot tell the difference: XPUB is PUB on the wire.
    Pub,
}

/// How to bridge one ZeroMQ address onto one weida endpoint.
#[derive(Clone, Debug)]
pub struct InboundConfig {
    /// TCP address to accept ZeroMQ peers on.
    pub listen: SocketAddr,
    /// The weida endpoint to speak to: `weida://[fingerprint@]host:port/path`.
    pub weida_url: String,
    /// Which socket type to present to the ZeroMQ side.
    pub presenting: Presenting,
    /// Largest whole ZMTP message the bridge will hold, in octets, summed over
    /// a message's frames.
    ///
    /// This is the bound `docs/adapters/zmtp.md` §3 requires, and it is now
    /// `ZMQ_MAXMSGSIZE` on the socket (`SocketOptions::max_message_size`):
    /// ZMTP grants no credit and its grammar allows 2^63-1 octets per frame,
    /// so a declared length past the cap is refused from the frame header,
    /// before the body is read. It also bounds the other direction: a weida
    /// payload beyond it is refused rather than truncated.
    ///
    /// **1 MiB, from the interop bench** (B-043). The cost through the bridge
    /// is linear in message size with no cliff anywhere — a megabyte costs
    /// 3.37 ms round-trip against 439 µs for a direct ZeroMQ pair — so the
    /// number is not a latency choice but a memory one, and the memory is
    /// `max_message_bytes` per direction per connection against the socket's
    /// own `max_peers`. 1 MiB is `stream_receive_window`, the weida per-stream
    /// budget the bridge's own reads already live inside [PROTOCOL §10].
    pub max_message_bytes: u64,
    /// What to do with a subscription whose byte prefix does not end at a
    /// segment boundary (loss L2).
    pub mid_segment: MidSegment,
    /// Bytes the bridge may hold for one `SUB` peer that is not reading.
    ///
    /// Dropping at the bound is ZeroMQ's own rule for PUB and weida's for
    /// fan-out, so both sides agree that a slow subscriber loses messages
    /// rather than stalling anyone.
    ///
    /// **Bytes here, messages on the socket.** `ZMQ_SNDHWM` counts messages,
    /// which is libzmq's unit and the one the library keeps, so this byte
    /// budget becomes a per-peer high-water mark of
    /// `queue_bytes / max_message_bytes` messages, at least one. The product
    /// is what a slow subscriber can really pin, and it is the product this
    /// number names: 8 MiB, which is `subscriber_buffer_bytes`, the weida-side
    /// ceiling on exactly the same thing [PROTOCOL §10].
    pub queue_bytes: usize,
    /// The weida runtime configuration.
    ///
    /// Its guarantee set must be `core`: the ZeroMQ side has no mechanism to
    /// carry ordering, deduplication or any completion level beyond a
    /// transport receipt, so anything above `core` is refused at configuration
    /// time rather than silently unmet (`docs/adapters/zmtp.md` §9.4).
    pub runtime: RuntimeConfig,
}

impl InboundConfig {
    /// A configuration for one address, endpoint and socket type, with the
    /// defaults the mapping document argues for.
    pub fn new(listen: SocketAddr, weida_url: impl Into<String>, presenting: Presenting) -> Self {
        InboundConfig {
            listen,
            weida_url: weida_url.into(),
            presenting,
            max_message_bytes: 1024 * 1024,
            mid_segment: MidSegment::Refuse,
            queue_bytes: 8 * 1024 * 1024,
            runtime: RuntimeConfig::default(),
        }
    }

    /// The ZeroMQ socket this configuration asks for, with the bounds it
    /// names.
    ///
    /// The per-peer subscription ceilings come from here too: 256 prefixes of
    /// at most 256 octets, which is what weida's own `max_subscriptions` and
    /// the 256 B filter cap of `docs/PROTOCOL.md` §10 allow one connection —
    /// the bridge is the side a stranger talks to, so it sets them rather
    /// than taking the library's more generous defaults.
    fn socket_options(&self) -> SocketOptions {
        let mut options = SocketOptions {
            max_message_size: self.max_message_bytes + FRAME_HEADER_ALLOWANCE,
            max_subscriptions: 256,
            max_subscription_bytes: 256,
            ..SocketOptions::default()
        };
        let depth = usize::try_from(self.max_message_bytes)
            .map(|cap| self.queue_bytes / cap.max(1))
            .unwrap_or(1);
        options.pipe.outgoing.hwm = depth.max(1);
        options
    }
}

/// What `ZMQ_MAXMSGSIZE` gets above [`InboundConfig::max_message_bytes`].
///
/// The two numbers count different things, and the difference belongs to the
/// bridge rather than to either side. `max_message_bytes` is a **payload**
/// budget — "the largest whole ZMTP message, summed over a message's frames" —
/// while `ZMQ_MAXMSGSIZE` in this library counts the frame headers too, which
/// is the honest thing for a bound whose purpose is to cap what a peer can
/// make this side hold. A message this bridge maps has at most three frames,
/// so 32 octets is more header than one can carry: a payload of exactly
/// `max_message_bytes` still fits, which is what a caller who set that number
/// meant.
const FRAME_HEADER_ALLOWANCE: u64 = 32;

/// The bound ZeroMQ socket, whichever type the configuration asked for.
enum Presented {
    Rep(RepSocket),
    Pull(PullSocket),
    Pub(XPubSocket),
}

/// A running bridge: one ZeroMQ socket and the weida runtime behind it.
pub struct Inbound {
    presented: Presented,
    bound: Endpoint,
    runtime: Runtime,
    config: InboundConfig,
    tls: ClientTls,
    /// The ZeroMQ context the socket lives in, kept alive beside it.
    _context: Context,
}

impl Inbound {
    /// Binds the ZeroMQ-side socket and prepares the weida side.
    ///
    /// Refuses, before anything is served:
    ///
    /// * a weida-side guarantee set above `core` (§9.4);
    /// * a `max_message_bytes` of zero, which would refuse every message;
    /// * a weida URL that does not parse, which is weida's own check.
    pub async fn bind(config: InboundConfig, tls: ClientTls) -> Result<Inbound, BridgeError> {
        if config.runtime.guarantees != GuaranteeSet::CORE {
            return Err(BridgeError::Configuration(
                "the weida side of a ZMTP bridge must run the `core` guarantee set: ZeroMQ has no \
                 mechanism to carry ordering, deduplication or any completion beyond a transport \
                 receipt (docs/adapters/zmtp.md §9.4)"
                    .into(),
            ));
        }
        if config.max_message_bytes == 0 {
            return Err(BridgeError::Configuration(
                "max_message_bytes is zero, so every message would be refused".into(),
            ));
        }
        // Parsed here so a typo fails at bind time rather than on the first
        // message.
        let _ = weida::EndpointAddr::parse(&config.weida_url)?;

        let runtime = Runtime::new(config.runtime.clone())?;
        let context = Context::new(ContextConfig::default())?;
        let options = config.socket_options();
        let endpoint = format!("tcp://{}", config.listen);
        let (presented, bound) = match config.presenting {
            Presenting::Rep => {
                let socket = RepSocket::with_options(&context, options)?;
                let bound = socket.bind(&endpoint).await?;
                (Presented::Rep(socket), bound)
            }
            Presenting::Pull => {
                let socket = PullSocket::with_options(&context, options)?;
                let bound = socket.bind(&endpoint).await?;
                (Presented::Pull(socket), bound)
            }
            Presenting::Pub => {
                let socket = XPubSocket::with_options(&context, options)?;
                let bound = socket.bind(&endpoint).await?;
                (Presented::Pub(socket), bound)
            }
        };
        Ok(Inbound {
            presented,
            bound,
            runtime,
            config,
            tls,
            _context: context,
        })
    }

    /// The address the ZeroMQ side is listening on, with the port the OS chose
    /// when the configuration asked for zero.
    ///
    /// This is `ZMQ_LAST_ENDPOINT` read back, which is the only way to learn a
    /// wildcard port.
    pub fn local_addr(&self) -> Result<SocketAddr, BridgeError> {
        match &self.bound {
            Endpoint::Tcp {
                host: TcpHost::Ip(ip),
                port,
            } => Ok(SocketAddr::new(*ip, *port)),
            Endpoint::Tcp { host, port } => Err(BridgeError::Configuration(format!(
                "the bound endpoint names no address to report: {host}:{port}"
            ))),
            other => Err(BridgeError::Configuration(format!(
                "a ZMTP bridge binds tcp, and this is {}",
                other.transport()
            ))),
        }
    }

    /// The weida runtime this bridge dials with, for shutting it down.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Serves ZeroMQ peers until the socket or the weida side ends.
    ///
    /// **One socket for every peer**, where this used to be one task per
    /// connection: accepting, the handshake and the fair queue across peers
    /// are the socket's job now. A peer that ends its connection costs
    /// nothing, and a message this bridge cannot carry ends the *socket*,
    /// because a ZeroMQ socket has no API to drop one peer and libzmq has none
    /// either — the difference is written down in `docs/adapters/zmtp.md` §9.2
    /// rather than absorbed.
    pub async fn serve(self) -> Result<(), BridgeError> {
        let Inbound {
            presented,
            runtime,
            config,
            tls,
            ..
        } = self;
        match presented {
            Presented::Rep(socket) => serve_rep(socket, &runtime, &config, &tls).await,
            Presented::Pull(socket) => serve_pull(socket, &runtime, &config, &tls).await,
            Presented::Pub(socket) => serve_pub(socket, &runtime, &config, &tls).await,
        }
    }
}

/// The body of one inbound message.
///
/// The pattern's own envelope is already gone: a REP socket "removes and
/// stores the address envelope, including the delimiter" and puts it back on
/// the reply, which is 28/REQREP's rule and now the library's code rather than
/// this bridge's (§2). What is left must be a single frame — anything else is
/// a genuine multipart message, refused because concatenating it would invent
/// an application protocol weida does not have (loss L1).
fn body_of(message: Multipart) -> Result<Vec<u8>, BridgeError> {
    let mut frames = message.into_frames();
    match frames.len() {
        1 => Ok(frames.remove(0).as_slice().to_vec()),
        n => Err(BridgeError::Protocol(format!(
            "a multipart message of {n} frames has no weida representation, and concatenating it \
             would invent an application protocol (loss L1); configure the ZeroMQ side to send \
             single-frame messages"
        ))),
    }
}

/// `REP`: one weida Req/Rep exchange per inbound message.
///
/// The lockstep is the socket's: a REP socket "SHALL receive and then send
/// exactly one message at a time", and this loop is that alternation with a
/// weida exchange in the middle.
async fn serve_rep(
    mut socket: RepSocket,
    runtime: &Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let requester = runtime.requester(tls.clone());
    requester.connect(&config.weida_url).await?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        let body = body_of(socket.recv().await?)?;
        let reply = requester.request(&body).await?;
        let payload = reply.collect(cap).await?;
        // What a vanished originator costs is the socket's rule — "SHALL
        // silently discard the reply, or return an error, if the originating
        // peer is no longer connected" — and it is reported rather than
        // hidden.
        socket.send(Multipart::single(payload)).await?;
    }
}

/// `PULL`: one weida one-way transfer per inbound message.
///
/// Backpressure is end to end by construction. PUSH blocks at its high-water
/// mark and "SHALL NOT discard"; weida's Push/Pull backpressure is `Block`. The
/// loop awaits the weida send before receiving again, so a stalled weida
/// puller stops the socket reading, which closes the TCP window, which blocks
/// the PUSH socket — one policy, three layers.
async fn serve_pull(
    mut socket: PullSocket,
    runtime: &Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let pusher = runtime.pusher(tls.clone());
    pusher.connect(&config.weida_url).await?;

    loop {
        let body = body_of(socket.recv().await?)?;
        pusher.send(&body).await?;
    }
}

/// `PUB`: the peer's subscriptions become weida filters, and what the weida
/// publisher fans out travels to the peer.
///
/// Two directions on one `select!`: the XPUB's subscription stream and the
/// weida subscriber. Both halves are cancel-safe — a socket that is not read
/// leaves its message queued, and a `Subscriber::recv` that is dropped leaves
/// the message in its queue.
///
/// The topic travels as its **own frame**, ahead of the payload. That is the
/// zguide's envelope convention, and it is what makes the peer's own prefix
/// match land on the topic rather than on the payload — "the match won't cross
/// a frame boundary" (`docs/adapters/zmtp.md` §6). It is also what does the
/// local re-filter of [`MidSegment::BoundaryAndRefilter`]: the socket holds
/// the prefix the peer sent and matches it against that frame, so a topic the
/// widened weida filter brought in and the peer never asked for is dropped by
/// 29/PUBSUB's ordinary publisher-side filtering.
async fn serve_pub(
    mut socket: XPubSocket,
    runtime: &Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let subscriber = runtime.subscriber(tls.clone());
    subscriber.connect(&config.weida_url).await?;
    let mut filters = Filters::default();
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        tokio::select! {
            control = socket.recv() => {
                let message = control?;
                let frames = message.frames();
                // An XPUB's application reads subscriptions, and only
                // subscriptions: a SUB or XSUB peer may not send anything
                // else, so a message that is not one is a peer speaking a
                // protocol this bridge does not.
                let Some((subscribe, prefix)) = frames
                    .first()
                    .filter(|_| frames.len() == 1)
                    .and_then(|frame| read_message_form(frame.as_slice()))
                else {
                    return Err(BridgeError::Protocol(
                        "a SUB peer sent a message that is not a subscription, which its socket \
                         type cannot do"
                            .into(),
                    ));
                };
                let prefix = prefix.to_vec();
                if let Err(e) = apply_change(
                    &mut socket,
                    &mut filters,
                    &subscriber,
                    subscribe,
                    &prefix,
                    config,
                )
                .await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    // A refused subscription costs the peer that
                    // subscription; whether it costs the connection is the
                    // peer's choice, since an incoming ERROR is fatal by
                    // specification. This side keeps serving either way.
                    tracing::info!(error = %e, "subscription refused");
                }
            }
            published = subscriber.recv() => match published {
                Ok(transfer) => {
                    let topic = transfer.meta().topic.clone().unwrap_or_default();
                    let payload = transfer.collect(cap).await?;
                    // Which subscribers want it, and what happens to the ones
                    // that are not keeping up, are the socket's: it matches
                    // the topic frame against each peer's prefixes and drops
                    // at the high-water mark, which is 29/PUBSUB's rule and
                    // weida's fan-out policy at once.
                    let report = socket.publish(Multipart::new(vec![
                        Message::from(topic.into_bytes()),
                        Message::from(payload),
                    ])?);
                    if report.dropped > 0 {
                        tracing::warn!(
                            dropped = report.dropped,
                            budget = config.queue_bytes,
                            "a ZeroMQ subscriber is not keeping up; its oldest queued messages \
                             were dropped to make room"
                        );
                    }
                }
                // The weida publisher went away. Reconnecting re-sends the
                // filters, because that is what `Subscriber::connect` does
                // [PATTERNS §1.8] — and it re-sends them rather than resuming
                // anything, since reconnection is not re-registration
                // [0008 §4.5].
                Err(e) if reconnectable(&e) => {
                    reconnect(&subscriber, config).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// Is this the weida publisher going away rather than a real failure?
fn reconnectable(e: &weida::Error) -> bool {
    matches!(
        e,
        weida::Error::ConnectionLost(_) | weida::Error::NotConnected | weida::Error::Indeterminate
    )
}

/// Re-dials the weida side, which re-sends the filters this peer's
/// subscriptions translated to.
///
/// Bounded on purpose: a bridge that retried forever would hide a weida
/// outage from the ZeroMQ side, and the ZeroMQ side has its own reconnect loop
/// that is better at it — a ZMTP peer reconnects automatically, and on a fresh
/// connection the bridge rebuilds everything from that peer's own
/// subscriptions. So this tries a few times and then lets the ZeroMQ socket
/// fail, which is the honest signal.
async fn reconnect(
    subscriber: &weida::Subscriber,
    config: &InboundConfig,
) -> Result<(), BridgeError> {
    /// Attempts, and the wait between them. Short, because a weida binding
    /// that is coming back usually does so within a round trip or two.
    const ATTEMPTS: usize = 3;
    const WAIT: std::time::Duration = std::time::Duration::from_millis(250);

    let mut last = None;
    for attempt in 1..=ATTEMPTS {
        tokio::time::sleep(WAIT).await;
        match subscriber.connect(&config.weida_url).await {
            Ok(()) => {
                tracing::info!(
                    attempt,
                    "reconnected to the weida publisher; filters re-sent"
                );
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(attempt, error = %e, "reconnect failed");
                last = Some(e);
            }
        }
    }
    Err(last
        .map(BridgeError::Weida)
        .unwrap_or(BridgeError::PeerClosed))
}

/// Subscribes or cancels one prefix on the weida side.
///
/// The socket has already applied it to its own table — the count, the
/// deduplication and both wire forms are 37/ZMTP's rules and the library's
/// code. What is left is the translation and its refusal:
///
/// * a prefix that translates is held in the ledger, and the weida side is
///   told the first time that filter is needed;
/// * a prefix that does not is answered with an `ERROR` naming the reason and
///   taken back out of the socket's table, so the socket matches nothing for
///   it. The peer is told because a silently ignored subscription is a
///   subscriber that waits forever for messages nobody will send, and ZMTP has
///   no per-subscription error channel — it may well treat the `ERROR` as
///   fatal and close, which is the protocol's own price (§9.3).
async fn apply_change(
    socket: &mut XPubSocket,
    filters: &mut Filters,
    subscriber: &weida::Subscriber,
    subscribe: bool,
    prefix: &[u8],
    config: &InboundConfig,
) -> Result<(), BridgeError> {
    let translated = match translate(prefix, config.mid_segment) {
        Ok(translated) => translated,
        Err(e) => {
            if subscribe {
                socket.refuse(&e.to_string())?;
                // Refused on the weida side, so refused on this one too:
                // otherwise the socket would keep matching a prefix no filter
                // ever brings messages for, and a later subscription that
                // widens the weida side would start delivering it.
                let _ = socket.unsubscribe(prefix);
            }
            return Err(e);
        }
    };
    if subscribe {
        if filters.hold(&translated.filter) {
            subscriber.subscribe(&translated.filter).await?;
        }
    } else if filters.release(&translated.filter) {
        subscriber.unsubscribe(&translated.filter).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_zmtp::SocketType;

    #[test]
    fn the_socket_types_are_the_ones_the_mapping_table_names() {
        // Each presented type must accept exactly the peers
        // `docs/adapters/zmtp.md` §2 maps onto it, which is the
        // specification's own table in `weida-zmtp` — and the check is made
        // at the handshake by `weida-zmq`, so this pins the table the library
        // uses rather than a rule of the bridge's own.
        assert!(SocketType::Rep.accepts(SocketType::Req));
        assert!(SocketType::Rep.accepts(SocketType::Dealer));
        assert!(SocketType::Pull.accepts(SocketType::Push));
        assert!(SocketType::Pub.accepts(SocketType::Sub));
        assert!(SocketType::Pub.accepts(SocketType::XSub));
        // The presented PUB is an XPUB socket, which must accept the same
        // peers or a SUB peer would be turned away by the substitution.
        assert!(SocketType::XPub.accepts(SocketType::Sub));
        assert!(SocketType::XPub.accepts(SocketType::XSub));

        // And the crossings §9.1 refuses are refused by the table itself, so
        // the bridge needs no rule of its own for them.
        assert!(!SocketType::Pull.accepts(SocketType::Sub));
        assert!(!SocketType::XPub.accepts(SocketType::Push));
        assert!(!SocketType::Rep.accepts(SocketType::Sub));
    }

    #[test]
    fn a_real_multipart_message_is_refused() {
        // One frame is a body. The pattern's envelope never reaches here any
        // more: a REP socket strips the delimiter and restores it on the
        // reply, which is why this function no longer takes an `envelope`
        // argument — that rule is `weida-zmq`'s, tested there.
        let body = body_of(Multipart::single(b"request".to_vec())).expect("one frame");
        assert_eq!(body, b"request");

        // Two frames are a genuine multipart message, whatever they hold.
        for parts in [
            vec![b"a".to_vec(), b"b".to_vec()],
            vec![Vec::new(), b"b".to_vec()],
        ] {
            let message =
                Multipart::new(parts.into_iter().map(Message::from).collect()).expect("a message");
            let err = body_of(message).expect_err("loss L1");
            assert!(matches!(err, BridgeError::Protocol(_)), "{err:?}");
        }
    }

    #[test]
    fn the_queue_budget_becomes_a_message_high_water_mark() {
        // The socket counts messages, the configuration bounds bytes: the
        // product is what a slow subscriber pins, so the depth is the budget
        // divided by the cap.
        let mut config = InboundConfig::new(
            "127.0.0.1:0".parse().expect("loopback"),
            "weida://127.0.0.1:7443/feed",
            Presenting::Pub,
        );
        assert_eq!(config.socket_options().pipe.outgoing.hwm, 8);

        // And a budget smaller than one message still holds one, because a
        // high-water mark of zero means *no limit* in libzmq.
        config.queue_bytes = 1;
        assert_eq!(config.socket_options().pipe.outgoing.hwm, 1);
    }
}
