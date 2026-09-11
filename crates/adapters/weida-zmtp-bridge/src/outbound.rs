//! The outbound direction: weida endpoints, a foreign ZeroMQ peer onward.
//!
//! The mirror of [`crate::Inbound`], and the mirror is not symmetric. Inbound,
//! the bridge binds on the ZeroMQ side and dials on the weida side; here it
//! **binds on the weida side** — Rep, Pull and Pub bind, per
//! `docs/ARCHITECTURE.md` §6c.4 — and dials the foreign peer.
//!
//! Two things that only exist in this direction:
//!
//! * **Correlation.** A weida `Replier` accepts concurrent exchanges and a
//!   ZeroMQ `REP` answers in order, so the bridge presents `DEALER` and puts a
//!   request id in the envelope. 28/REQREP makes that work: a REP socket keeps
//!   every frame up to the delimiter and prepends it to the reply, so the id
//!   comes back with it. A ROUTER peer does the same. Presenting `REQ` instead
//!   would be lockstep — "SHALL send and then receive exactly one message at a
//!   time" — which is correct and throws weida's concurrency away.
//! * **A reply that never comes.** `ZMQ_ROUTER_MANDATORY` is off by default,
//!   so a ROUTER that cannot route a request **drops it silently** — loss L5.
//!   Nothing arrives, so nothing can be observed except the absence, and a
//!   weida exchange would wait forever. Every outbound exchange therefore has
//!   a deadline, and on expiry the weida requester is told with a typed
//!   `ERROR{NoReply}` rather than left hanging.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use weida::{
    ErrorCode, GuaranteeSet, IncomingRequest, Listener, Runtime, RuntimeConfig, ServerTls,
    TransferMeta,
};
use weida_zmtp::{Command, SocketType};

use crate::error::BridgeError;
use crate::wire::{Incoming, Session, answer_command};

/// Which ZeroMQ socket type the bridge presents when it dials out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialling {
    /// `DEALER`, for a foreign `REP` or `ROUTER`: a weida `Replier` binds here
    /// and each exchange is forwarded with a correlation envelope.
    Dealer,
    /// `PUSH`, for a foreign `PULL`: a weida `Puller` binds here and each
    /// transfer becomes one ZMTP message.
    Push,
    /// `SUB`, for a foreign `PUB` or `XPUB`: a weida `Publisher` binds here and
    /// what the foreign publisher sends is published onward.
    Sub,
}

impl Dialling {
    fn socket_type(self) -> SocketType {
        match self {
            Dialling::Dealer => SocketType::Dealer,
            Dialling::Push => SocketType::Push,
            Dialling::Sub => SocketType::Sub,
        }
    }
}

/// How to bridge one weida endpoint onto one foreign ZeroMQ peer.
#[derive(Clone, Debug)]
pub struct OutboundConfig {
    /// The foreign ZeroMQ peer to dial.
    pub connect: SocketAddr,
    /// Where the weida side listens for QUIC.
    pub weida_listen: SocketAddr,
    /// The endpoint path to register on the weida side.
    pub weida_path: String,
    /// Which socket type to present to the ZeroMQ side.
    pub dialling: Dialling,
    /// Largest whole ZMTP message the bridge will hold, in octets.
    pub max_message_bytes: u64,
    /// How long an exchange may wait for its ZMTP reply before the weida
    /// requester is told there will not be one.
    ///
    /// Mandatory and finite, for the reason the module documents: a ROUTER
    /// drops an unroutable request **silently** by default (loss L5), so the
    /// absence of a reply is the only observation available and a bridge
    /// without a deadline would park the exchange forever. Ten seconds by
    /// default, which is generous for a local ZeroMQ peer and short enough
    /// that a caller notices.
    pub reply_deadline: Duration,
    /// Prefixes to subscribe with when presenting `SUB`.
    ///
    /// Configuration rather than translation: the weida side here is a
    /// `Publisher`, and weida gives a publisher no way to learn its
    /// subscribers' filters — that is `XPUB`'s subscription stream, which has
    /// no weida counterpart (loss L7). So the bridge subscribes to what it was
    /// told to.
    pub subscribe: Vec<Vec<u8>>,
    /// `PING` interval toward the foreign peer, or `None` for no heartbeat.
    ///
    /// On by default here, unlike libzmq, and deliberately: TCP's own timeout
    /// "can be roughly 30 minutes", which is not a liveness signal for a
    /// bridge. It is **not** derived from weida's `idle_timeout` and does not
    /// derive it: the two bound different hops (`docs/adapters/zmtp.md` §3).
    pub heartbeat: Option<Duration>,
    /// How many heartbeat intervals of silence declare the peer dead. The
    /// specification's own guidance is "some multiple of that interval
    /// (usually 3-5)".
    pub liveness_multiple: u32,
    /// The weida runtime configuration; its guarantee set must be `core`
    /// (`docs/adapters/zmtp.md` §9.4).
    pub runtime: RuntimeConfig,
}

impl OutboundConfig {
    /// A configuration with the defaults the mapping document argues for.
    pub fn new(
        connect: SocketAddr,
        weida_listen: SocketAddr,
        weida_path: impl Into<String>,
        dialling: Dialling,
    ) -> Self {
        OutboundConfig {
            connect,
            weida_listen,
            weida_path: weida_path.into(),
            dialling,
            max_message_bytes: 8 * 1024 * 1024,
            reply_deadline: Duration::from_secs(10),
            subscribe: Vec::new(),
            heartbeat: Some(Duration::from_secs(5)),
            liveness_multiple: 3,
            runtime: RuntimeConfig::default(),
        }
    }
}

/// A bridge from one weida endpoint to one foreign ZeroMQ peer.
pub struct Outbound {
    runtime: Runtime,
    listener: Listener,
    binding: weida::Binding,
    config: OutboundConfig,
}

impl Outbound {
    /// Binds the weida side and validates the configuration.
    ///
    /// Refuses, before anything is served: a guarantee set above `core`
    /// (§9.4), a `max_message_bytes` of zero, a zero `reply_deadline` or
    /// `liveness_multiple`, and — for `SUB` — an empty subscription list,
    /// which would connect to a publisher and ask for nothing.
    pub async fn bind(config: OutboundConfig, tls: ServerTls) -> Result<Outbound, BridgeError> {
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
        if config.reply_deadline.is_zero() {
            return Err(BridgeError::Configuration(
                "reply_deadline is zero: a ROUTER peer drops an unroutable request silently, so \
                 the deadline is the only thing that ends such an exchange (loss L5)"
                    .into(),
            ));
        }
        if config.liveness_multiple == 0 {
            return Err(BridgeError::Configuration(
                "liveness_multiple is zero, which would declare the peer dead immediately".into(),
            ));
        }
        if config.dialling == Dialling::Sub && config.subscribe.is_empty() {
            return Err(BridgeError::Configuration(
                "presenting SUB with no subscription would connect to a publisher and ask it for \
                 nothing; an empty prefix subscribes to everything and must be said explicitly"
                    .into(),
            ));
        }

        let runtime = Runtime::new(config.runtime.clone())?;
        let listener = runtime.listener();
        let binding = listener.bind_quic(config.weida_listen, tls).await?;
        Ok(Outbound {
            runtime,
            listener,
            binding,
            config,
        })
    }

    /// The address the weida side is listening on.
    pub fn weida_addr(&self) -> SocketAddr {
        self.binding.local_addr()
    }

    /// The weida runtime this bridge serves with.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Dials the foreign peer and serves until either side ends.
    ///
    /// One connection, because that is what the configuration names. A ZeroMQ
    /// socket reconnects on its own and this bridge does not: a dialled peer
    /// that goes away ends the run and its supervisor decides, which is the
    /// same choice the inbound direction makes about its weida side.
    pub async fn serve(self) -> Result<(), BridgeError> {
        let socket = TcpStream::connect(self.config.connect).await?;
        socket.set_nodelay(true)?;
        let mut session = Session::new(socket, self.config.max_message_bytes);
        let ours = self.config.dialling.socket_type();
        let theirs = session.handshake(ours).await?;
        tracing::debug!(
            presenting = ours.as_str(),
            peer = theirs.as_str(),
            "ZMTP handshake complete"
        );

        match self.config.dialling {
            Dialling::Dealer => serve_dealer(session, &self.listener, &self.config).await,
            Dialling::Push => serve_push(session, &self.listener, &self.config).await,
            Dialling::Sub => serve_sub(session, &self.listener, &self.config).await,
        }
    }
}

/// A weida exchange waiting for its ZMTP reply.
struct Pending {
    request: IncomingRequest,
    since: Instant,
}

/// `DEALER` toward a foreign `REP` or `ROUTER`: weida exchanges, correlated.
async fn serve_dealer(
    mut session: Session<TcpStream>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let replier = listener.replier(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    let mut next_id: u64 = 0;
    let mut liveness = Liveness::new(config);

    loop {
        tokio::select! {
            accepted = replier.accept() => {
                let mut request = accepted?;
                let body = request.body().read_capped(cap).await?;
                next_id += 1;
                let id = next_id;
                // The envelope 28/REQREP defines: every frame up to the
                // delimiter comes back with the reply, so the id is the
                // correlation and nothing is invented.
                session
                    .write_message(&[&id.to_be_bytes(), &[], &body])
                    .await?;
                pending.insert(id, Pending { request, since: Instant::now() });
            }
            inbound = session.read_next() => {
                liveness.saw_traffic();
                match inbound? {
                    Incoming::Command(body) => answer_command(&mut session, &body).await?,
                    Incoming::Message(message) => {
                        let (id, reply) = split_reply(message.0)?;
                        match pending.remove(&id) {
                            Some(waiting) => {
                                let mut out = waiting.request.reply(TransferMeta::default()).await?;
                                out.write_all(&reply).await?;
                                out.finish()?;
                            }
                            // A reply for an exchange that timed out, or an id
                            // we never sent. Neither is worth closing on: the
                            // requester has already been told.
                            None => tracing::debug!(id, "a reply arrived for no pending exchange"),
                        }
                    }
                }
            }
            () = liveness.tick() => {
                liveness.beat(&mut session).await?;
            }
            () = tokio::time::sleep(config.reply_deadline / 4) => {
                expire(&mut pending, config.reply_deadline).await;
            }
        }
    }
}

/// Splits a reply into its correlation id and its payload.
///
/// The shape a REP or ROUTER peer returns: the envelope this side sent
/// (`[id, empty]`), then the reply frames. More than one reply frame is a
/// multipart message and is refused (loss L1).
fn split_reply(parts: Vec<Vec<u8>>) -> Result<(u64, Vec<u8>), BridgeError> {
    let mut parts = parts;
    if parts.len() != 3 || !parts[1].is_empty() {
        return Err(BridgeError::Protocol(format!(
            "expected a reply of [id, delimiter, body], got {} frames",
            parts.len()
        )));
    }
    let body = parts.remove(2);
    let id = parts.remove(0);
    let id: [u8; 8] = id.try_into().map_err(|_| {
        BridgeError::Protocol("the reply's envelope is not one this bridge sent".into())
    })?;
    Ok((u64::from_be_bytes(id), body))
}

/// Tells every exchange past its deadline that no reply is coming.
async fn expire(pending: &mut HashMap<u64, Pending>, deadline: Duration) {
    let due: Vec<u64> = pending
        .iter()
        .filter(|(_, waiting)| waiting.since.elapsed() >= deadline)
        .map(|(id, _)| *id)
        .collect();
    for id in due {
        let Some(waiting) = pending.remove(&id) else {
            continue;
        };
        tracing::warn!(
            id,
            "no ZMTP reply within the deadline; refusing the weida exchange with NoReply"
        );
        // `NoReply` rather than `Rejected`: the request was taken and nobody
        // declined it — a ROUTER simply dropped it, which is loss L5 and the
        // one thing this side *can* report.
        waiting.request.refuse(ErrorCode::NoReply).await;
    }
}

/// `PUSH` toward a foreign `PULL`: one ZMTP message per weida transfer.
async fn serve_push(
    mut session: Session<TcpStream>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let puller = listener.puller(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);
    let mut liveness = Liveness::new(config);

    loop {
        tokio::select! {
            arrived = puller.recv() => {
                let transfer = arrived?;
                let body = transfer.collect(cap).await?;
                // Writing blocks when the peer's window is full, which stops
                // this loop reading, which is weida's `Block` backpressure
                // reaching a ZeroMQ high-water mark. Both sides block rather
                // than drop for this pattern, so nothing is converted.
                session.write_message(&[&body]).await?;
            }
            inbound = session.read_next() => {
                liveness.saw_traffic();
                match inbound? {
                    Incoming::Command(body) => answer_command(&mut session, &body).await?,
                    // A PULL peer cannot send us a message.
                    Incoming::Message(_) => {
                        return Err(BridgeError::Protocol(
                            "a PULL peer sent a message, which its socket type cannot do".into(),
                        ));
                    }
                }
            }
            () = liveness.tick() => liveness.beat(&mut session).await?,
        }
    }
}

/// `SUB` toward a foreign `PUB`: what the publisher sends is published onward.
///
/// The topic is the message's first frame, which is the envelope convention
/// the zguide recommends and §6 requires here: weida's `publish` takes a topic
/// and a payload as separate things, and a single-frame ZeroMQ message says
/// nothing about where its topic ends.
async fn serve_sub(
    mut session: Session<TcpStream>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let publisher = listener.publisher(&config.weida_path)?;
    let mut liveness = Liveness::new(config);

    for prefix in &config.subscribe {
        session.write_command(&Command::Subscribe(prefix)).await?;
    }

    loop {
        tokio::select! {
            inbound = session.read_next() => {
                liveness.saw_traffic();
                match inbound? {
                    Incoming::Command(body) => answer_command(&mut session, &body).await?,
                    Incoming::Message(message) => {
                        let (topic, payload) = split_published(message.0)?;
                        match publisher.publish(&topic, payload) {
                            Ok(_) => {}
                            // A payload larger than what a subscriber may be
                            // held: refused locally rather than truncated, and
                            // the connection survives, because the next
                            // message may be perfectly deliverable.
                            Err(weida::Error::LimitExceeded) => tracing::warn!(
                                topic,
                                "published payload is larger than subscriber_buffer_bytes; dropped"
                            ),
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }
            () = liveness.tick() => liveness.beat(&mut session).await?,
        }
    }
}

/// Splits a published ZeroMQ message into a weida topic and payload.
fn split_published(parts: Vec<Vec<u8>>) -> Result<(String, Vec<u8>), BridgeError> {
    let mut parts = parts;
    if parts.len() != 2 {
        return Err(BridgeError::Protocol(format!(
            "a published message must be [topic, payload] — the envelope convention this bridge \
             requires and states in its configuration (docs/adapters/zmtp.md §6) — got {} frames",
            parts.len()
        )));
    }
    let payload = parts.remove(1);
    let topic = String::from_utf8(parts.remove(0)).map_err(|_| {
        BridgeError::Protocol("a weida topic is text and this one is not valid UTF-8".into())
    })?;
    Ok((topic, payload))
}

/// The heartbeat of `docs/adapters/zmtp.md` §3: `PING` on a timer, and silence
/// declared fatal after a multiple of it.
struct Liveness {
    interval: Option<Duration>,
    multiple: u32,
    last: Instant,
}

impl Liveness {
    fn new(config: &OutboundConfig) -> Liveness {
        Liveness {
            interval: config.heartbeat,
            multiple: config.liveness_multiple,
            last: Instant::now(),
        }
    }

    /// "A peer SHOULD treat any incoming traffic (not just a PONG reply) as a
    /// sign of life."
    fn saw_traffic(&mut self) {
        self.last = Instant::now();
    }

    /// Sleeps until the next beat is due, or forever when the heartbeat is
    /// off. Cancel-safe, because it holds no state: the deadline is recomputed
    /// from `last` each time.
    async fn tick(&self) {
        match self.interval {
            Some(interval) => tokio::time::sleep(interval).await,
            None => std::future::pending().await,
        }
    }

    /// Sends one `PING`, and reports the peer dead when it has been silent for
    /// `multiple` intervals.
    async fn beat(&mut self, session: &mut Session<TcpStream>) -> Result<(), BridgeError> {
        let Some(interval) = self.interval else {
            return Ok(());
        };
        if self.last.elapsed() >= interval * self.multiple {
            return Err(BridgeError::PeerClosed);
        }
        // The TTL tells the peer how long to wait before giving up on us, in
        // tenths of a second, and it is the same silence budget we apply.
        let tenths = (interval * self.multiple).as_millis() / 100;
        let ttl = u16::try_from(tenths).unwrap_or(u16::MAX);
        session
            .write_command(&Command::Ping {
                ttl,
                context: b"weida",
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reply_must_carry_the_envelope_it_was_sent_with() {
        let (id, body) = split_reply(vec![
            7u64.to_be_bytes().to_vec(),
            Vec::new(),
            b"ok".to_vec(),
        ])
        .expect("the REP shape");
        assert_eq!(id, 7);
        assert_eq!(body, b"ok");

        // No delimiter, a missing frame, or a multipart reply: all refused
        // rather than guessed at.
        for parts in [
            vec![7u64.to_be_bytes().to_vec(), b"x".to_vec(), b"ok".to_vec()],
            vec![7u64.to_be_bytes().to_vec(), b"ok".to_vec()],
            vec![
                7u64.to_be_bytes().to_vec(),
                Vec::new(),
                b"a".to_vec(),
                b"b".to_vec(),
            ],
        ] {
            assert!(matches!(split_reply(parts), Err(BridgeError::Protocol(_))));
        }

        // An envelope that is not eight octets was not sent by this bridge.
        assert!(matches!(
            split_reply(vec![b"nope".to_vec(), Vec::new(), b"ok".to_vec()]),
            Err(BridgeError::Protocol(_))
        ));
    }

    #[test]
    fn a_published_message_must_carry_its_topic_as_a_frame() {
        let (topic, payload) =
            split_published(vec![b"px.eurusd".to_vec(), b"1.09".to_vec()]).expect("the shape");
        assert_eq!(topic, "px.eurusd");
        assert_eq!(payload, b"1.09");

        // One frame says nothing about where the topic ends, and three is a
        // multipart message.
        for parts in [
            vec![b"px.eurusd1.09".to_vec()],
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        ] {
            assert!(matches!(
                split_published(parts),
                Err(BridgeError::Protocol(_))
            ));
        }

        // A topic must be text, because weida's is.
        assert!(matches!(
            split_published(vec![vec![0xff, 0xfe], b"x".to_vec()]),
            Err(BridgeError::Protocol(_))
        ));
    }

    #[test]
    fn silence_is_fatal_only_after_the_multiple() {
        let mut liveness = Liveness {
            interval: Some(Duration::from_millis(100)),
            multiple: 3,
            last: Instant::now() - Duration::from_millis(250),
        };
        // 250 ms of silence against a 300 ms budget: still alive.
        assert!(liveness.last.elapsed() < Duration::from_millis(300));
        liveness.last = Instant::now() - Duration::from_millis(400);
        assert!(liveness.last.elapsed() >= Duration::from_millis(300));

        // And a bridge with the heartbeat off never declares anything.
        let off = Liveness {
            interval: None,
            multiple: 3,
            last: Instant::now() - Duration::from_secs(3600),
        };
        assert!(off.interval.is_none());
    }
}
