//! The outbound direction: weida endpoints, a foreign ZeroMQ peer onward.
//!
//! The mirror of [`crate::Inbound`], and the mirror is not symmetric. Inbound,
//! the bridge binds on the ZeroMQ side and dials on the weida side; here it
//! **binds on the weida side** — Rep, Pull and Pub bind, per
//! `docs/ARCHITECTURE.md` §6c.4 — and dials the foreign peer with a
//! `weida-zmq` socket.
//!
//! Two things that only exist in this direction:
//!
//! * **Correlation.** A weida `Replier` accepts concurrent exchanges and a
//!   ZeroMQ `REP` answers in order, so the bridge dials as `DEALER` and puts a
//!   request id in the envelope. 28/REQREP makes that work: a REP socket keeps
//!   every frame up to the delimiter and prepends it to the reply, so the id
//!   comes back with it. A ROUTER peer does the same. Dialling `REQ` instead
//!   would be lockstep — "SHALL send and then receive exactly one message at a
//!   time" — which is correct and throws weida's concurrency away.
//! * **A reply that never comes.** `ZMQ_ROUTER_MANDATORY` is off by default,
//!   so a ROUTER that cannot route a request **drops it silently** — loss L5.
//!   Nothing arrives, so nothing can be observed except the absence, and a
//!   weida exchange would wait forever. Every outbound exchange therefore has
//!   a deadline, and on expiry the weida requester is told with a typed
//!   `ERROR{NoReply}` rather than left hanging.
//!
//! **What is not here any more**
//! ([0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2): the greeting and the handshake, the framing, `PING`/`PONG` with its
//! version gate — `Liveness` is `ZMQ_HEARTBEAT_IVL`/`_TIMEOUT`/`_TTL` on the
//! socket now — and the choice of subscription wire form, which is
//! `SocketOptions::subscription_form` because which form a peer reads is a
//! property of that peer's implementation and not of any bridge.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use weida::{
    ErrorCode, GuaranteeSet, IncomingRequest, Listener, Runtime, RuntimeConfig, ServerTls,
    TransferMeta,
};
use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, PushSocket, SocketOptions, SubSocket,
};

use crate::error::BridgeError;

/// How a subscription is put on the wire toward a foreign publisher: the
/// library's own choice, re-exported so a bridge caller configures it in one
/// place.
///
/// Two forms exist because the implementations disagree, and neither this
/// bridge nor its user should have to find that out from a dead connection.
/// 3.x has the `SUBSCRIBE`/`CANCEL` **commands** and that is the default;
/// ZMTP 2.0's form is a one-frame **message** whose first octet is `1` or `0`,
/// which is also how libzmq presents subscriptions to an XPUB application. The
/// pure-Rust `zeromq` crate announces ZMTP 3.0 and reads only the legacy form,
/// so an interop run against it needs
/// [`SubscriptionForm::LegacyMessage`](weida_zmq::SubscriptionForm::LegacyMessage).
pub use weida_zmq::SubscriptionForm;

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
    ///
    /// `ZMQ_MAXMSGSIZE` on the socket, and the same number and reasoning as
    /// the inbound direction's: the interop bench (B-043) found the cost
    /// linear in message size with no cliff, so the number bounds memory
    /// rather than latency.
    pub max_message_bytes: u64,
    /// How long an exchange may wait for its ZMTP reply before the weida
    /// requester is told there will not be one.
    ///
    /// Mandatory and finite, for the reason the module documents: a ROUTER
    /// drops an unroutable request **silently** by default (loss L5), so the
    /// absence of a reply is the only observation available and a bridge
    /// without a deadline would park the exchange forever.
    ///
    /// **Ten seconds stays, and the interop bench is why** (B-043). A full
    /// round trip through this bridge measured 81 µs at 1 KiB and 3.37 ms at
    /// 1 MiB, so the default is some 3000 times the slowest exchange the
    /// adapter's own cap allows: a deadline this far above the working range
    /// cannot misfire on a merely slow peer, which is the only failure mode
    /// that would matter here — a lost request costs one exchange, a deadline
    /// that fires early costs correct ones.
    pub reply_deadline: Duration,
    /// Exchanges that may wait for a ZMTP reply at once.
    ///
    /// Each waiting exchange holds a weida request **and** its body, already
    /// read, so the exposure is `max_pending_exchanges × max_message_bytes` —
    /// 64 MiB at the defaults, against a 1 MiB cap — and the count is whatever
    /// weida clients choose to open, which is remote input. QUIC bounds
    /// concurrent streams *per connection* and the reply deadline bounds how
    /// long an entry lives; neither bounds the sum across clients, which is
    /// what this does. Past it the exchange is refused immediately with
    /// `ERROR{REJECTED}`, which is the honest answer.
    pub max_pending_exchanges: usize,
    /// Prefixes to subscribe with when dialling `SUB`.
    ///
    /// Configuration rather than translation: the weida side here is a
    /// `Publisher`, and weida gives a publisher no way to learn its
    /// subscribers' filters — that is `XPUB`'s subscription stream, which has
    /// no weida counterpart (loss L7). So the bridge subscribes to what it was
    /// told to.
    pub subscribe: Vec<Vec<u8>>,
    /// Which wire form those prefixes take: see [`SubscriptionForm`].
    pub subscription_form: SubscriptionForm,
    /// `PING` interval toward the foreign peer, or `None` for no heartbeat.
    ///
    /// On by default here, unlike libzmq, and deliberately: TCP's own timeout
    /// "can be roughly 30 minutes", which is not a liveness signal for a
    /// bridge. It is **not** derived from weida's `idle_timeout` and does not
    /// derive it: the two bound different hops (`docs/adapters/zmtp.md` §3).
    ///
    /// The heartbeat itself is the socket's — `ZMQ_HEARTBEAT_IVL` — including
    /// the rule that a peer which negotiated ZMTP 3.0 gets no `PING` at all,
    /// because `PING`/`PONG` are 3.1 commands and sending one to a 3.0 peer is
    /// a protocol violation.
    pub heartbeat: Option<Duration>,
    /// How many heartbeat intervals of silence declare the peer dead, which
    /// becomes `ZMQ_HEARTBEAT_TIMEOUT` and the `ZMQ_HEARTBEAT_TTL` carried in
    /// the `PING`. The specification's own guidance is "some multiple of that
    /// interval (usually 3-5)".
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
            max_message_bytes: 1024 * 1024,
            reply_deadline: Duration::from_secs(10),
            max_pending_exchanges: 64,
            subscribe: Vec::new(),
            subscription_form: SubscriptionForm::default(),
            heartbeat: Some(Duration::from_secs(5)),
            liveness_multiple: 3,
            runtime: RuntimeConfig::default(),
        }
    }

    /// The ZeroMQ socket this configuration asks for.
    ///
    /// The heartbeat is three options rather than one loop: the interval is
    /// `ZMQ_HEARTBEAT_IVL`, the silence budget is `ZMQ_HEARTBEAT_TIMEOUT`, and
    /// the same budget is the `ZMQ_HEARTBEAT_TTL` hint the `PING` carries so
    /// the peer knows how long to wait for us.
    fn socket_options(&self) -> SocketOptions {
        let budget = self.heartbeat.map(|ivl| ivl * self.liveness_multiple);
        SocketOptions {
            // The same allowance as the inbound direction's, and for the same
            // reason: this number is a payload budget and `ZMQ_MAXMSGSIZE`
            // counts frame headers.
            max_message_size: self.max_message_bytes + 32,
            heartbeat_ivl: self.heartbeat,
            heartbeat_timeout: budget,
            heartbeat_ttl: budget,
            subscription_form: self.subscription_form,
            ..SocketOptions::default()
        }
    }

    /// The endpoint string for the foreign peer.
    fn endpoint(&self) -> String {
        format!("tcp://{}", self.connect)
    }
}

/// A bridge from one weida endpoint to one foreign ZeroMQ peer.
pub struct Outbound {
    runtime: Runtime,
    listener: Listener,
    binding: weida::Binding,
    config: OutboundConfig,
    context: Context,
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
        if config.max_pending_exchanges == 0 {
            return Err(BridgeError::Configuration(
                "max_pending_exchanges is zero, so every exchange would be refused".into(),
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
        let context = Context::new(ContextConfig::default())?;
        Ok(Outbound {
            runtime,
            listener,
            binding,
            config,
            context,
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

    /// Dials the foreign peer and serves until the weida side ends.
    ///
    /// One endpoint, because that is what the configuration names — and
    /// dialling is now the socket's, which means the **reconnect** is too:
    /// where this used to end the run when the peer went away, the socket
    /// re-dials with `ZMQ_RECONNECT_IVL` backoff and the queued messages wait
    /// for it, which is what every ZeroMQ application already expects.
    pub async fn serve(self) -> Result<(), BridgeError> {
        let options = self.config.socket_options();
        let endpoint = self.config.endpoint();
        match self.config.dialling {
            Dialling::Dealer => {
                let socket = DealerSocket::with_options(&self.context, options)?;
                socket.connect(&endpoint)?;
                serve_dealer(socket, &self.listener, &self.config).await
            }
            Dialling::Push => {
                let socket = PushSocket::with_options(&self.context, options)?;
                socket.connect(&endpoint)?;
                serve_push(socket, &self.listener, &self.config).await
            }
            Dialling::Sub => {
                let mut socket = SubSocket::with_options(&self.context, options)?;
                for prefix in &self.config.subscribe {
                    socket.subscribe(prefix)?;
                }
                socket.connect(&endpoint)?;
                serve_sub(socket, &self.listener, &self.config).await
            }
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
    mut socket: DealerSocket,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let replier = listener.replier(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    let mut next_id: u64 = 0;

    loop {
        tokio::select! {
            accepted = replier.accept() => {
                let mut request = accepted?;
                // The ceiling first, because it is the cheaper refusal: a
                // request refused before its body is read costs this side
                // nothing, where reading first would buffer up to
                // `max_message_bytes` for an exchange that is about to be
                // turned away. Refused now rather than parked and refused at
                // the deadline, so the requester learns immediately.
                if pending.len() >= config.max_pending_exchanges {
                    tracing::warn!(
                        ceiling = config.max_pending_exchanges,
                        "refused a weida exchange: the ZMTP peer already has that many \
                         replies outstanding"
                    );
                    request.refuse(ErrorCode::Rejected).await;
                    continue;
                }
                // Same rule as the push loop: over the cap the payload is
                // already refused with `STOP_SENDING(REJECTED)` and none of it
                // buffered, and a refusal is per-stream. Here the requester
                // gets the ERROR frame as well, because an exchange that will
                // never be answered must say so rather than wait for the
                // deadline.
                let body = match request.body().read_capped(cap).await {
                    Ok(body) => body,
                    Err(weida::Error::LimitExceeded) => {
                        tracing::warn!(
                            cap,
                            "refused a weida request past max_message_bytes; the ZeroMQ peer \
                             cannot be handed a message it must receive atomically"
                        );
                        request.refuse(ErrorCode::Rejected).await;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                next_id += 1;
                let id = next_id;
                // The envelope 28/REQREP defines: every frame up to the
                // delimiter comes back with the reply, so the id is the
                // correlation and nothing is invented.
                socket
                    .send(Multipart::new(vec![
                        Message::from(id.to_be_bytes().to_vec()),
                        Message::empty(),
                        Message::from(body),
                    ])?)
                    .await?;
                pending.insert(id, Pending { request, since: Instant::now() });
            }
            inbound = socket.recv() => {
                let (id, reply) = split_reply(frames_of(inbound?))?;
                match pending.remove(&id) {
                    Some(waiting) => {
                        let mut out = waiting.request.reply(TransferMeta::default()).await?;
                        out.write_all(&reply).await?;
                        out.finish()?;
                    }
                    // A reply for an exchange that timed out, or an id we
                    // never sent. Neither is worth closing on: the requester
                    // has already been told.
                    None => tracing::debug!(id, "a reply arrived for no pending exchange"),
                }
            }
            () = tokio::time::sleep(config.reply_deadline / 4) => {
                expire(&mut pending, config.reply_deadline).await;
            }
        }
    }
}

/// One message's frames as plain octets, which is what the shape checks below
/// read.
fn frames_of(message: Multipart) -> Vec<Vec<u8>> {
    message
        .into_frames()
        .into_iter()
        .map(|frame| frame.as_slice().to_vec())
        .collect()
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
    mut socket: PushSocket,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let puller = listener.puller(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        let transfer = puller.recv().await?;
        // Over the cap, `collect` has already refused the rest of the payload
        // with `STOP_SENDING(REJECTED)` and buffered none of it. That is a
        // refusal of **this transfer**, and weida keeps refusals per stream —
        // "a refusal is per-stream, a violation ends the connection"
        // (`docs/PROTOCOL.md` §3) — so the loop continues rather than taking
        // the ZeroMQ peer down for one oversized message it never saw.
        let body = match transfer.collect(cap).await {
            Ok(body) => body,
            Err(weida::Error::LimitExceeded) => {
                tracing::warn!(
                    cap,
                    "refused a weida payload past max_message_bytes; the ZeroMQ peer cannot be \
                     handed a message it must receive atomically"
                );
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        // A PUSH socket "SHALL NOT discard messages that it cannot send" and
        // blocks instead, which stops this loop reading: weida's `Block`
        // backpressure reaching a ZeroMQ high-water mark, with nothing
        // converted on the way.
        socket.send(Multipart::single(body)).await?;
    }
}

/// `SUB` toward a foreign `PUB`: what the publisher sends is published onward.
///
/// The topic is the message's first frame, which is the envelope convention
/// the zguide recommends and §6 requires here: weida's `publish` takes a topic
/// and a payload as separate things, and a single-frame ZeroMQ message says
/// nothing about where its topic ends.
async fn serve_sub(
    mut socket: SubSocket,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let publisher = listener.publisher(&config.weida_path)?;

    loop {
        let (topic, payload) = split_published(frames_of(socket.recv().await?))?;
        match publisher.publish(&topic, payload) {
            Ok(_) => {}
            // A payload larger than what a subscriber may be held: refused
            // locally rather than truncated, and the connection survives,
            // because the next message may be perfectly deliverable.
            Err(weida::Error::LimitExceeded) => tracing::warn!(
                topic,
                "published payload is larger than subscriber_buffer_bytes; dropped"
            ),
            Err(e) => return Err(e.into()),
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

    /// Claim: the heartbeat the configuration asks for becomes the three
    /// socket options that implement it, so the `PING` carries a TTL the peer
    /// can act on rather than a zero.
    ///
    /// The loop that used to do this is gone: `Liveness`, its version gate and
    /// its "any incoming traffic is a sign of life" rule are `weida-zmq`'s,
    /// tested there (0013 §5.2).
    #[test]
    fn the_heartbeat_becomes_socket_options() {
        let mut config = OutboundConfig::new(
            "127.0.0.1:1".parse().expect("addr"),
            "127.0.0.1:0".parse().expect("loopback"),
            "/jobs",
            Dialling::Push,
        );
        config.heartbeat = Some(Duration::from_millis(100));
        config.liveness_multiple = 3;
        let options = config.socket_options();
        assert_eq!(options.heartbeat_ivl, Some(Duration::from_millis(100)));
        assert_eq!(options.heartbeat_timeout, Some(Duration::from_millis(300)));
        assert_eq!(
            options.heartbeat_ttl,
            Some(Duration::from_millis(300)),
            "the TTL tells the peer the same silence budget this side applies"
        );

        // No heartbeat means no option at all, not a zero interval: liveness
        // is then the transport's business.
        config.heartbeat = None;
        let options = config.socket_options();
        assert_eq!(options.heartbeat_ivl, None);
        assert_eq!(options.heartbeat_timeout, None);
        assert_eq!(options.heartbeat_ttl, None);
    }
}
