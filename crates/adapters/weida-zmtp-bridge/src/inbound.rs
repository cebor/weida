//! The inbound direction: foreign ZeroMQ peers, weida onward.
//!
//! One listener, one socket type presented, one weida endpoint. Each accepted
//! connection is a task that drives the ZMTP handshake and then one of three
//! loops, chosen by what the bridge presents.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use weida::{ClientTls, GuaranteeSet, Runtime, RuntimeConfig};
use weida_zmtp::{Command, SocketType};

use crate::error::BridgeError;
use crate::subscriptions::{Change, MidSegment, Subscriptions};
use crate::wire::{DropQueue, Incoming, Session, answer, answer_command, sanitize};

/// Which ZeroMQ socket type the bridge presents to whoever connects.
///
/// Three, because these are the three weida patterns exist for and the three
/// whose ZeroMQ counterparts bind rather than connect
/// (`docs/ARCHITECTURE.md` §6c.4). The peer's own type is checked against this
/// one with the specification's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presenting {
    /// `REP`: a `REQ` or `DEALER` peer's messages become weida Req/Rep
    /// exchanges, and the reply travels back.
    Rep,
    /// `PULL`: a `PUSH` peer's messages become weida one-way transfers.
    Pull,
    /// `PUB`: a `SUB` peer's subscriptions become weida filters and the
    /// published messages travel to it.
    Pub,
}

impl Presenting {
    fn socket_type(self) -> SocketType {
        match self {
            Presenting::Rep => SocketType::Rep,
            Presenting::Pull => SocketType::Pull,
            Presenting::Pub => SocketType::Pub,
        }
    }
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
    /// This is the bound `docs/adapters/zmtp.md` §3 requires: ZMTP grants no
    /// credit and its grammar allows 2^63-1 octets per frame, and a ZeroMQ
    /// peer cannot be handed a body before it is complete, so the bridge
    /// buffers whole messages and this is what keeps a remote peer from
    /// choosing the allocation. It also bounds the other direction: a weida
    /// payload beyond it is refused rather than truncated.
    ///
    /// The default, 8 MiB, is `subscriber_buffer_bytes` — the weida-side
    /// neighbour, since `ZMQ_MAXMSGSIZE` has no default at all. §11 of the
    /// mapping document leaves the measured number to the interop bench.
    pub max_message_bytes: u64,
    /// What to do with a subscription whose byte prefix does not end at a
    /// segment boundary (loss L2).
    pub mid_segment: MidSegment,
    /// Messages the bridge may hold for one `SUB` peer that is not reading.
    ///
    /// Dropping at the bound is ZeroMQ's own rule for PUB and weida's for
    /// fan-out, so both sides agree that a slow subscriber loses messages
    /// rather than stalling anyone.
    pub queue_depth: usize,
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
            max_message_bytes: 8 * 1024 * 1024,
            mid_segment: MidSegment::Refuse,
            queue_depth: 1024,
            runtime: RuntimeConfig::default(),
        }
    }
}

/// A running bridge: a TCP listener and the weida runtime behind it.
pub struct Inbound {
    listener: TcpListener,
    runtime: Runtime,
    config: Arc<InboundConfig>,
    tls: Arc<ClientTls>,
}

impl Inbound {
    /// Binds the ZeroMQ-side listener and prepares the weida side.
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
        let listener = TcpListener::bind(config.listen).await?;
        Ok(Inbound {
            listener,
            runtime,
            config: Arc::new(config),
            tls: Arc::new(tls),
        })
    }

    /// The address the ZeroMQ side is listening on, with the port the OS chose
    /// when the configuration asked for zero.
    pub fn local_addr(&self) -> Result<SocketAddr, BridgeError> {
        Ok(self.listener.local_addr()?)
    }

    /// The weida runtime this bridge dials with, for shutting it down.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Accepts and serves ZeroMQ peers until the listener fails.
    ///
    /// One task per connection, and a connection that fails takes nothing else
    /// with it: a ZMTP peer is expected to reconnect, and the bridge holds no
    /// state on its behalf (`docs/adapters/zmtp.md` §3, and reconnection is
    /// not re-registration — `docs/decisions/0008-session-identity.md` §4.5).
    pub async fn serve(self) -> Result<(), BridgeError> {
        loop {
            let (socket, from) = self.listener.accept().await?;
            let config = Arc::clone(&self.config);
            let tls = Arc::clone(&self.tls);
            let runtime = self.runtime.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_one(socket, runtime, config, tls).await {
                    match e {
                        BridgeError::PeerClosed => {
                            tracing::debug!(%from, "ZeroMQ peer closed the connection");
                        }
                        e => tracing::warn!(%from, error = %e, "bridged connection ended"),
                    }
                }
            });
        }
    }
}

/// Drives one accepted ZeroMQ connection.
async fn serve_one(
    socket: TcpStream,
    runtime: Runtime,
    config: Arc<InboundConfig>,
    tls: Arc<ClientTls>,
) -> Result<(), BridgeError> {
    // Nagle off: a bridge writes whole messages and a delayed small frame is a
    // delayed message, which is what the ZeroMQ side notices.
    socket.set_nodelay(true)?;
    let mut session = Session::new(socket, config.max_message_bytes);
    let ours = config.presenting.socket_type();
    let theirs = session.handshake(ours).await?;
    tracing::debug!(
        presenting = ours.as_str(),
        peer = theirs.as_str(),
        "ZMTP handshake complete"
    );

    match config.presenting {
        Presenting::Rep => serve_rep(session, runtime, &config, &tls).await,
        Presenting::Pull => serve_pull(session, runtime, &config, &tls).await,
        Presenting::Pub => serve_pub(session, runtime, &config, &tls).await,
    }
}

/// The body of one inbound message, with the pattern's own envelope consumed.
///
/// REQ prepends an empty delimiter frame and REP strips it, so `[empty, body]`
/// is the shape a REQ socket puts on the wire and the delimiter is envelope
/// rather than payload (`docs/adapters/zmtp.md` §2). A DEALER peer leaves the
/// envelope to its application, so a bare `[body]` is accepted too. Anything
/// else is a genuine multipart message: refused, because concatenating it
/// would invent an application protocol weida does not have — loss L1.
fn body_of(parts: Vec<Vec<u8>>, envelope: bool) -> Result<(Vec<u8>, bool), BridgeError> {
    let mut parts = parts;
    match parts.len() {
        1 => Ok((parts.remove(0), false)),
        2 if envelope && parts[0].is_empty() => Ok((parts.remove(1), true)),
        n => Err(BridgeError::Protocol(format!(
            "a multipart message of {n} frames has no weida representation, and concatenating it \
             would invent an application protocol (loss L1); configure the ZeroMQ side to send \
             single-frame messages"
        ))),
    }
}

/// `REP`: one weida Req/Rep exchange per inbound message.
///
/// REQ is lockstep — "SHALL send and then receive exactly one message at a
/// time" — so this loop is too, and deliberately: weida could run the
/// exchanges concurrently, but the peer cannot produce a second request, so
/// concurrency here would buy nothing and hide the REQ state machine.
async fn serve_rep(
    mut session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let requester = runtime.requester(tls.clone());
    requester.connect(&config.weida_url).await?;

    loop {
        let parts = match session.read_next().await? {
            Incoming::Message(message) => message.0,
            // PING is answered; anything else a REP peer sends is ignored, as
            // a command it has no use for.
            Incoming::Command(body) => {
                answer_command(&mut session, &body).await?;
                continue;
            }
        };
        let (body, had_envelope) = body_of(parts, true)?;
        let reply = requester.request(&body).await?;
        let payload = reply
            .collect(usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX))
            .await?;
        if had_envelope {
            session.write_message(&[&[], &payload]).await?;
        } else {
            session.write_message(&[&payload]).await?;
        }
    }
}

/// `PULL`: one weida one-way transfer per inbound message.
///
/// Backpressure is end to end by construction. PUSH blocks at its high-water
/// mark and "SHALL NOT discard"; weida's Push/Pull backpressure is `Block`. The
/// loop awaits the weida send before reading again, so a stalled weida puller
/// stops the bridge reading, which closes the TCP window, which blocks the
/// PUSH socket — one policy, three layers.
async fn serve_pull(
    mut session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let pusher = runtime.pusher(tls.clone());
    pusher.connect(&config.weida_url).await?;

    loop {
        let parts = match session.read_next().await? {
            Incoming::Message(message) => message.0,
            Incoming::Command(body) => {
                answer_command(&mut session, &body).await?;
                continue;
            }
        };
        let (body, _) = body_of(parts, false)?;
        pusher.send(&body).await?;
    }
}

/// `PUB`: the peer's subscriptions become weida filters, and what the weida
/// publisher fans out travels to the peer.
///
/// Two directions on one connection, so one `select!` over the socket and the
/// weida subscriber. Both halves are cancel-safe: the session buffers its own
/// bytes, and a `Subscriber::recv` that is dropped leaves the message in its
/// queue.
///
/// The topic travels as its **own frame**, ahead of the payload. That is the
/// zguide's envelope convention, and it is what makes the peer's own prefix
/// match land on the topic rather than on the payload — "the match won't cross
/// a frame boundary" (`docs/adapters/zmtp.md` §6).
async fn serve_pub(
    mut session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let subscriber = runtime.subscriber(tls.clone());
    subscriber.connect(&config.weida_url).await?;
    let mut subs = Subscriptions::default();
    let mut queue = DropQueue::new(config.queue_depth);
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        tokio::select! {
            inbound = session.read_next() => match inbound? {
                Incoming::Command(body) => {
                    if let Err(e) = apply_subscription(&mut session, &mut subs, &subscriber, &body, config).await {
                        if e.is_fatal() {
                            return Err(e);
                        }
                        // A refused subscription costs the peer that
                        // subscription and nothing else: it is told with
                        // ERROR and the connection stays up.
                        tracing::info!(error = %e, "subscription refused");
                    }
                }
                Incoming::Message(_) => {
                    return Err(BridgeError::Protocol(
                        "a SUB peer sent a message, which its socket type cannot do".into(),
                    ));
                }
            },
            published = subscriber.recv() => match published {
                Ok(transfer) => {
                    let topic = transfer.meta().topic.clone().unwrap_or_default();
                    let payload = transfer.collect(cap).await?;
                    if subs.wants(&topic) {
                        if queue.push(payload) {
                            tracing::warn!(
                                dropped = queue.dropped(),
                                depth = config.queue_depth,
                                "the ZeroMQ subscriber is not keeping up; dropped its oldest \
                                 queued message"
                            );
                        }
                        while let Some(message) = queue.pop() {
                            session.write_message(&[topic.as_bytes(), &message]).await?;
                        }
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
/// subscriptions. So this tries a few times and then lets the ZMTP connection
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

/// Applies one `SUBSCRIBE` or `CANCEL`, or answers another command.
async fn apply_subscription(
    session: &mut Session<TcpStream>,
    subs: &mut Subscriptions,
    subscriber: &weida::Subscriber,
    body: &[u8],
    config: &InboundConfig,
) -> Result<(), BridgeError> {
    let command = Command::decode(body)?;
    let change = match command {
        Command::Subscribe(prefix) => match subs.subscribe(prefix, config.mid_segment) {
            Ok(change) => change,
            Err(e) => {
                // The peer is told, because a silently ignored subscription is
                // a subscriber that waits forever for messages nobody will
                // send.
                session
                    .write_command(&Command::Error(&sanitize(&e.to_string())))
                    .await?;
                return Err(e);
            }
        },
        Command::Cancel(prefix) => subs.cancel(prefix, config.mid_segment),
        other => return answer(session, other).await,
    };
    match change {
        Change::Subscribe(filter) => subscriber.subscribe(&filter).await?,
        Change::Unsubscribe(filter) => subscriber.unsubscribe(&filter).await?,
        Change::None => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_socket_types_are_the_ones_the_mapping_table_names() {
        // Each presented type must accept exactly the peers
        // `docs/adapters/zmtp.md` §2 maps onto it, which is the specification's
        // own table in `weida-zmtp`.
        assert!(SocketType::Rep.accepts(SocketType::Req));
        assert!(SocketType::Rep.accepts(SocketType::Dealer));
        assert!(SocketType::Pull.accepts(SocketType::Push));
        assert!(SocketType::Pub.accepts(SocketType::Sub));
        assert!(SocketType::Pub.accepts(SocketType::XSub));

        // And the crossings §9.1 refuses are refused by the table itself, so
        // the bridge needs no rule of its own for them.
        assert!(!SocketType::Pull.accepts(SocketType::Sub));
        assert!(!SocketType::Pub.accepts(SocketType::Push));
        assert!(!SocketType::Rep.accepts(SocketType::Sub));
    }

    #[test]
    fn a_pattern_envelope_is_consumed_and_a_real_multipart_is_refused() {
        // REQ's empty delimiter: envelope, consumed, and remembered so the
        // reply carries one back.
        let (body, envelope) =
            body_of(vec![Vec::new(), b"request".to_vec()], true).expect("REQ shape");
        assert_eq!(body, b"request");
        assert!(envelope);

        // A DEALER peer that sends no delimiter.
        let (body, envelope) = body_of(vec![b"request".to_vec()], true).expect("bare shape");
        assert_eq!(body, b"request");
        assert!(!envelope);

        // Two non-empty frames are a genuine multipart message.
        let err = body_of(vec![b"a".to_vec(), b"b".to_vec()], true).expect_err("loss L1");
        assert!(matches!(err, BridgeError::Protocol(_)), "{err:?}");

        // And on a pattern with no envelope, even the delimiter shape is
        // multipart.
        let err = body_of(vec![Vec::new(), b"b".to_vec()], false).expect_err("loss L1");
        assert!(matches!(err, BridgeError::Protocol(_)), "{err:?}");
    }

    #[test]
    fn an_error_reason_survives_being_put_on_the_wire() {
        // `ERROR` carries printable ASCII only, and the reasons here are
        // written for a human: the ones that quote a filter contain quotes and
        // may contain anything the peer sent.
        let reason = sanitize("the prefix \"px.\u{1}\" is not printable");
        assert!(reason.is_ascii());
        assert!(
            reason.bytes().all(|b| (0x20..=0x7E).contains(&b)),
            "{reason}"
        );
        assert!(Command::Error(&reason).encode().is_ok());

        let long = sanitize(&"x".repeat(400));
        assert_eq!(long.len(), 255, "an ERROR reason is at most 255 octets");
        assert!(Command::Error(&long).encode().is_ok());
    }
}
