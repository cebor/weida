//! The outbound direction: weida endpoints, a foreign SP peer onward.
//!
//! The mirror of [`crate::Inbound`], and the mirror is not symmetric.
//! Inbound, the bridge binds on the SP side and dials on the weida side;
//! here it **binds on the weida side** — Rep, Pull and Pub bind, per
//! `docs/ARCHITECTURE.md` §6c.4 — and dials the foreign peer.
//!
//! **The bridge no longer speaks SP.** The protocol header, the pairing
//! check, the framing, the tag stack and the local prefix match are
//! `weida-nng`'s sockets
//! ([0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2). Three things remain this direction's own, and all three are the
//! reason it is a bridge rather than a socket.
//!
//! **It uses a raw REQ socket, deliberately.** A cooked REQ owns a resend
//! timer: it retransmits on the timer, on peer disconnect, or when a peer
//! becomes available (`docs/research/nanomsg-nng.md` §4). A bridge that did
//! the same would be inventing at-least-once on behalf of a weida requester
//! that asked for one attempt — the duplicate-producing behaviour
//! `docs/adapters/nng.md` §8 names as L3 and L4, arriving from the wrong
//! side. [`weida_nng::RawSocket`] is exactly the socket with the wire and
//! without the state machine: the bridge allocates a 31-bit request id per
//! exchange, writes it with the terminal bit [rfc-reqrep §5], matches the
//! reply by that id, and owns no timer that would send anything twice.
//! Exactly one request per weida exchange reaches the wire, which is the
//! observation L3 asks for.
//!
//! **A reply that never comes is a deadline.** A REP peer may simply not
//! answer — a respondent "may decline by not replying" and SP has no way to
//! say "no" (`docs/research/nanomsg-nng.md` §4, §6) — so the absence is the
//! only observation available, and a weida exchange would otherwise wait
//! forever. Every exchange therefore has a deadline, and on expiry the
//! weida requester is told with a typed `ERROR{NoReply}` rather than left
//! hanging.
//!
//! **Subscriptions never reach the wire.** SP filters at the subscriber and
//! a SUB socket cannot send (§4), so a subscription is not a wire message at
//! all. The prefixes are now held by the [`weida_nng::SubSocket`] the bridge
//! dials with — the library does the matching, and counts what it
//! discarded — but what may *not* happen is deriving those prefixes from
//! weida filters; see [`OutboundConfig::subscribe`] and loss L11.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::Notify;
use weida::{
    ErrorCode, GuaranteeSet, IncomingRequest, Listener, Runtime, RuntimeConfig, ServerTls,
    TransferMeta,
};
use weida_nng::{
    Admission, Context, ContextConfig, Message, PipeEvent, PipeInfo, PushSocket, RawSocket,
    SocketOptions, SubSocket,
};
use weida_sp::backtrace::{self, Backtrace, MAX_ID};
use weida_sp::header::EndpointType;

use crate::error::BridgeError;

/// Which SP protocol the bridge presents when it dials out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialling {
    /// `REQ` toward a foreign `REP`: weida exchanges, one request each, no
    /// resend timer.
    Req,
    /// `PUSH` toward a foreign `PULL`: one SP message per weida transfer.
    Push,
    /// `SUB` toward a foreign `PUB`: what the publisher sends is published
    /// onward, after this side's own prefix match.
    Sub,
}

impl Dialling {
    fn endpoint(self) -> EndpointType {
        match self {
            Dialling::Req => EndpointType::Req,
            Dialling::Push => EndpointType::Push,
            Dialling::Sub => EndpointType::Sub,
        }
    }
}

/// How a published SP body is split into a weida topic and a payload.
///
/// SP has no topic field: "PUB/SUB uses the initial bytes of the body as a
/// topic; they are neither a separate wire field nor typed metadata"
/// (`docs/research/nanomsg-nng.md` §3). weida's `publish` takes the two
/// separately (`docs/PROTOCOL.md` §6.4), so *something* has to say where the
/// topic ends, and nothing on the wire does. That something is this
/// configuration, which is what `docs/adapters/nng.md` §6 means by "the
/// split is adapter configuration, not SP".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicSplit {
    /// The topic is everything before the first occurrence of this octet,
    /// and the payload is everything after it.
    ///
    /// The default is `0x00`: a weida topic is text (`docs/PROTOCOL.md`
    /// §6.4), so NUL is the one octet that can never occur inside one, which
    /// makes it the only delimiter that is never ambiguous. It is also what
    /// [`crate::InboundConfig::topic_delimiter`] writes when the two
    /// directions are paired.
    Delimiter(u8),
    /// The first `n` octets are the topic and the rest is payload, for a
    /// fixed-width topic convention. A body shorter than `n` is refused.
    Fixed(usize),
    /// The whole body is the payload and every message is published under
    /// this topic. For a publisher whose bodies carry no topic at all.
    Constant(String),
}

impl Default for TopicSplit {
    fn default() -> Self {
        TopicSplit::Delimiter(0)
    }
}

impl TopicSplit {
    /// Splits one published body.
    fn apply<'a>(&'a self, body: &'a [u8]) -> Result<(&'a str, &'a [u8]), BridgeError> {
        let (topic, payload) = match self {
            TopicSplit::Delimiter(byte) => match body.iter().position(|b| b == byte) {
                Some(at) => (&body[..at], &body[at + 1..]),
                None => {
                    return Err(BridgeError::Protocol(format!(
                        "a published body carries no {byte:#04x} delimiter, so this bridge \
                         cannot tell where its topic ends (docs/adapters/nng.md §6)"
                    )));
                }
            },
            TopicSplit::Fixed(n) => {
                if body.len() < *n {
                    return Err(BridgeError::Protocol(format!(
                        "a published body of {} octets is shorter than the {n}-octet topic this \
                         bridge is configured for",
                        body.len()
                    )));
                }
                body.split_at(*n)
            }
            TopicSplit::Constant(topic) => return Ok((topic.as_str(), body)),
        };
        let topic = std::str::from_utf8(topic).map_err(|_| {
            BridgeError::Protocol("a weida topic is text and this one is not valid UTF-8".into())
        })?;
        Ok((topic, payload))
    }
}

/// How to bridge one weida endpoint onto one foreign SP peer.
#[derive(Clone, Debug)]
pub struct OutboundConfig {
    /// The foreign SP peer to dial.
    pub connect: SocketAddr,
    /// Where the weida side listens for QUIC.
    pub weida_listen: SocketAddr,
    /// The endpoint path to register on the weida side.
    pub weida_path: String,
    /// Which SP protocol to present.
    pub dialling: Dialling,
    /// Largest message body the bridge will hold, in octets.
    ///
    /// `NNG_OPT_RECVMAXSZ` on the SP socket and this crate's own weida-side
    /// cap, which is the split 0013 §5.2 asks for. 1 MiB, the same number
    /// and the same reasoning as the inbound direction's: it bounds memory
    /// rather than latency, and the product it is a factor of is stated on
    /// [`max_pending_exchanges`](OutboundConfig::max_pending_exchanges).
    pub max_message_bytes: u64,
    /// Largest tag stack the bridge will accept on a reply, in hops:
    /// `NNG_OPT_MAXTTL` on the socket.
    ///
    /// The bridge writes a one-tag stack, but a reply may come back through
    /// devices that pushed their own peer ids [rfc-reqrep §5], so the
    /// ceiling applies to what is read. `MAXTTL` is 1-255 on the
    /// specification side and 15 in NNG's source (`docs/adapters/nng.md`
    /// §11).
    pub max_hops: usize,
    /// How long an exchange may wait for its SP reply before the weida
    /// requester is told there will not be one.
    ///
    /// Mandatory and finite, for the reason the module documents: a REP peer
    /// that does not answer says nothing, and this bridge does not
    /// retransmit, so the deadline is the only thing that ends such an
    /// exchange.
    pub reply_deadline: Duration,
    /// Exchanges that may wait for an SP reply at once.
    ///
    /// Each waiting exchange holds a weida request **and** its body, already
    /// read, so the worst case one bridge holds is
    ///
    /// ```text
    /// max_pending_exchanges × max_message_bytes
    ///          64           ×      1 MiB        = 64 MiB
    /// ```
    ///
    /// and the count is whatever weida clients choose to open, which is
    /// remote input: QUIC bounds concurrent streams per connection and the
    /// deadline bounds how long an entry lives, but neither bounds the sum
    /// across clients. Past the ceiling an exchange is refused immediately
    /// with `ERROR{REJECTED}` — before its body is read, which is the
    /// cheaper refusal — rather than parked until the deadline.
    pub max_pending_exchanges: usize,
    /// Byte prefixes this side accepts when presenting `SUB`, applied
    /// **locally**.
    ///
    /// SP subscriptions never reach the wire: matching happens at the
    /// subscriber (`docs/research/nanomsg-nng.md` §4), so these are what the
    /// bridge's own SUB socket keeps, not something the peer is told. An
    /// empty prefix accepts everything, which is SP's own empty
    /// subscription, and it must be written explicitly — a `SUB` with no
    /// subscription at all receives nothing.
    ///
    /// They are configuration and **not** derived from weida filters. Two
    /// reasons, one per direction: the weida side here is a `Publisher`, and
    /// weida gives a publisher no way to learn its subscribers' filters; and
    /// a segmented weida filter does not reduce to a byte prefix unless it
    /// is a literal prefix ending at a separator followed by `#`
    /// ([0007](https://github.com/tuco86/weida/blob/main/docs/decisions/0007-topic-namespace.md)
    /// §4.5). That is loss L11 of `docs/adapters/nng.md` §8.
    pub subscribe: Vec<Vec<u8>>,
    /// How a published body is split into topic and payload: see
    /// [`TopicSplit`].
    pub topic_split: TopicSplit,
    /// How long the bridge keeps answering the weida side after the SP peer
    /// turned out to be the wrong endpoint type.
    ///
    /// A mismatch closes the SP connection — SP has no error frame, so there
    /// is nothing to answer the peer with (loss L10) — but the weida side is
    /// bound and its clients deserve better than a vanished endpoint. For
    /// this long, every exchange is refused with `ERROR{UNSUPPORTED}`, which
    /// reaches a requester as [`weida::Error::Unsupported`]; then the bridge
    /// gives up and its supervisor decides.
    pub refusal_grace: Duration,
    /// The weida runtime configuration; its guarantee set must be `core`
    /// (`docs/adapters/nng.md` §7, §9.2, §9.3).
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
            max_hops: backtrace::DEFAULT_MAX_HOPS,
            reply_deadline: Duration::from_secs(10),
            max_pending_exchanges: 64,
            subscribe: Vec::new(),
            topic_split: TopicSplit::default(),
            refusal_grace: Duration::from_secs(5),
            runtime: RuntimeConfig::default(),
        }
    }

    /// The SP socket's options: this configuration's bounds under NNG's own
    /// names.
    fn socket_options(&self) -> SocketOptions {
        SocketOptions {
            recv_max_size: self.max_message_bytes,
            max_ttl: self.max_hops,
            // One peer, because that is what the configuration names: this
            // bridge dials one address.
            max_pipes: 1,
            ..SocketOptions::default()
        }
    }

    /// The URL for the SP peer this bridge dials.
    fn url(&self) -> String {
        format!("tcp://{}", self.connect)
    }
}

/// A bridge from one weida endpoint to one foreign SP peer.
pub struct Outbound {
    runtime: Runtime,
    listener: Listener,
    binding: weida::Binding,
    config: OutboundConfig,
}

impl Outbound {
    /// Binds the weida side and validates the configuration.
    ///
    /// Refuses, before anything is served: a guarantee set above `core`, a
    /// `max_message_bytes` or `max_pending_exchanges` of zero, a zero
    /// `max_hops`, a zero `reply_deadline`, and — for `SUB` — an empty
    /// subscription list, which would dial a publisher and keep nothing.
    pub async fn bind(config: OutboundConfig, tls: ServerTls) -> Result<Outbound, BridgeError> {
        if config.runtime.guarantees != GuaranteeSet::CORE {
            return Err(BridgeError::Configuration(
                "the weida side of an SP bridge must run the `core` guarantee set: SP has no \
                 transfer point, no producer sequence and no deduplication key \
                 (docs/adapters/nng.md §7, §9.2, §9.3)"
                    .into(),
            ));
        }
        if config.max_message_bytes == 0 {
            return Err(BridgeError::Configuration(
                "max_message_bytes is zero, so every message would be refused; SP's unlimited \
                 RECVMAXSZ cannot be honoured here (docs/adapters/nng.md §9.6)"
                    .into(),
            ));
        }
        if config.max_pending_exchanges == 0 {
            return Err(BridgeError::Configuration(
                "max_pending_exchanges is zero, so every exchange would be refused".into(),
            ));
        }
        if config.max_hops == 0 {
            return Err(BridgeError::Configuration(
                "max_hops is zero, so every reply's tag stack would be refused".into(),
            ));
        }
        if config.reply_deadline.is_zero() {
            return Err(BridgeError::Configuration(
                "reply_deadline is zero: an SP peer that does not answer says nothing and this \
                 bridge does not retransmit, so the deadline is the only thing that ends such \
                 an exchange"
                    .into(),
            ));
        }
        if config.dialling == Dialling::Sub && config.subscribe.is_empty() {
            return Err(BridgeError::Configuration(
                "presenting SUB with no subscription would receive every publication and keep \
                 none; an empty prefix accepts everything and must be said explicitly \
                 (docs/research/nanomsg-nng.md §4)"
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
    /// One connection, because that is what the configuration names — the
    /// socket's pipe ceiling is one. The dial is the synchronous form, so it
    /// returns only once the peer's protocol header has arrived and the
    /// pairing has been checked (`docs/research/nanomsg-nng.md` §1); a
    /// refused pairing therefore fails here, before any traffic, and the
    /// weida side is told for `refusal_grace`.
    pub async fn serve(self) -> Result<(), BridgeError> {
        let sp = Context::new(ContextConfig::default())?;
        let options = self.config.socket_options();
        let ours = self.config.dialling.endpoint();
        let url = self.config.url();

        match self.config.dialling {
            Dialling::Req => {
                // Raw, so that nothing retransmits: see the module note.
                let socket = RawSocket::with_options(&sp, ours, options)?;
                let gone = watch_for_the_peer(&socket);
                dial_or_refuse(socket.dial(&url).await, ours, &self.listener, &self.config).await?;
                serve_req(socket, gone, &self.listener, &self.config).await
            }
            Dialling::Push => {
                let socket = PushSocket::with_options(&sp, options)?;
                let gone = watch_for_the_peer(&socket);
                dial_or_refuse(socket.dial(&url).await, ours, &self.listener, &self.config).await?;
                serve_push(socket, gone, &self.listener, &self.config).await
            }
            Dialling::Sub => {
                let socket = SubSocket::with_options(&sp, options)?;
                // The prefixes are the library's now: it receives every
                // publication and discards what matches nothing, which is
                // what "SP filters at the subscriber" means when the
                // subscriber is a bridge.
                for prefix in &self.config.subscribe {
                    socket.subscribe(prefix.clone());
                }
                let gone = watch_for_the_peer(&socket);
                dial_or_refuse(socket.dial(&url).await, ours, &self.listener, &self.config).await?;
                serve_sub(socket, gone, &self.listener, &self.config).await
            }
        }
    }
}

/// Turns a refused dial into the two things it means: a closed SP
/// connection, and a weida side that says so for a while.
///
/// `NNG_EPROTO` is the pairing refusal — the library checks the peer's
/// endpoint type before any traffic and closes with nothing sent back, which
/// is all SP has (loss L10). Everything else is a connection that did not
/// happen and is reported as itself.
async fn dial_or_refuse(
    dialled: Result<weida_nng::Dialer, weida_nng::Error>,
    ours: EndpointType,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    match dialled {
        Ok(_dialer) => Ok(()),
        Err(weida_nng::Error::EPROTO(reason)) => {
            let reason = reason.to_string();
            tracing::warn!(
                ?ours,
                %reason,
                "the dialled SP peer is the wrong endpoint type; refusing the weida side"
            );
            refuse_weida_side(listener, config).await;
            Err(BridgeError::EndpointType { ours, reason })
        }
        Err(other) => Err(other.into()),
    }
}

/// Signals once the peer's pipe is removed.
///
/// The library reconnects a dialer whose pipe closes
/// (`docs/research/nanomsg-nng.md` §1); this bridge does not want that —
/// "a peer that goes away ends the run and its supervisor decides" — so the
/// pipe event is what ends the loops. The callback is handed a
/// [`PipeInfo`] and no socket handle, which is why it can be this small.
///
/// `notify_one` rather than `notify_waiters`: it stores a permit, so a loop
/// that reaches its wait after the pipe has already gone still sees it.
fn watch_for_the_peer<S: HasNotify>(socket: &S) -> Arc<Notify> {
    let gone = Arc::new(Notify::new());
    let signal = Arc::clone(&gone);
    socket.install(Arc::new(move |event, info: &PipeInfo| {
        tracing::debug!(?event, id = info.id.get(), "SP pipe event");
        if event == PipeEvent::RemPost {
            signal.notify_one();
        }
        Admission::Accept
    }));
    gone
}

/// The one thing the three socket types have in common here: a pipe-event
/// callback. Written as a trait rather than a macro so the three loops take
/// an ordinary argument.
trait HasNotify {
    fn install(&self, callback: weida_nng::PipeCallback);
}

macro_rules! has_notify {
    ($socket:ty) => {
        impl HasNotify for $socket {
            fn install(&self, callback: weida_nng::PipeCallback) {
                self.notify(callback);
            }
        }
    };
}

has_notify!(RawSocket);
has_notify!(PushSocket);
has_notify!(SubSocket);

/// Tells the weida side that the far end is unusable, for a bounded while.
///
/// `Unsupported` rather than `Rejected`: nothing declined the work, the
/// endpoint behind this bridge cannot serve it at all
/// (`docs/PROTOCOL.md` §6.4). A `Puller` has no coded refusal to send, so a
/// transfer is dropped, which resets its stream — the per-stream refusal
/// weida already defines.
async fn refuse_weida_side(listener: &Listener, config: &OutboundConfig) {
    let until = Instant::now() + config.refusal_grace;
    match config.dialling {
        Dialling::Req => {
            let Ok(replier) = listener.replier(&config.weida_path) else {
                return;
            };
            while Instant::now() < until {
                let remaining = until.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, replier.accept()).await {
                    Ok(Ok(request)) => request.refuse(ErrorCode::Unsupported).await,
                    Ok(Err(_)) | Err(_) => return,
                }
            }
        }
        Dialling::Push => {
            let Ok(puller) = listener.puller(&config.weida_path) else {
                return;
            };
            while Instant::now() < until {
                let remaining = until.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, puller.recv()).await {
                    Ok(Ok(transfer)) => drop(transfer),
                    Ok(Err(_)) | Err(_) => return,
                }
            }
        }
        // A publisher has nobody to refuse: a weida subscriber that
        // connects simply receives nothing, because there is no publication
        // to forward.
        Dialling::Sub => {}
    }
}

/// A weida exchange waiting for its SP reply.
struct Pending {
    request: IncomingRequest,
    since: Instant,
}

/// Request ids, per [rfc-reqrep §5]: 31 bits, the first picked at random and
/// each next one incremented.
///
/// "At random" matters for a bridge that may be restarted while a peer still
/// holds state from the previous run, so the seed is the clock rather than a
/// constant; this crate wants no dependency for sixteen bits of entropy.
struct RequestIds(u32);

impl RequestIds {
    fn new() -> RequestIds {
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since| since.subsec_nanos())
            .unwrap_or(1);
        RequestIds(seed & MAX_ID)
    }

    fn next(&mut self) -> u32 {
        self.0 = self.0.wrapping_add(1) & MAX_ID;
        self.0
    }
}

/// `REQ` toward a foreign `REP`: weida exchanges, one request each.
async fn serve_req(
    socket: RawSocket,
    gone: Arc<Notify>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let replier = listener.replier(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);
    let mut pending: HashMap<u32, Pending> = HashMap::new();
    let mut ids = RequestIds::new();

    // Two ways an exchange ends without an answer, and both have to reach
    // the requester as something typed. The deadline is one; the peer's
    // close is the other, and it is the faster of the two — SP says nothing
    // when it gives up, so the close *is* the message (L10 seen from this
    // side).
    let outcome = loop {
        tokio::select! {
            // Biased, and the order is the rule: a reply already in hand
            // beats the peer's close. A REP peer that answers and hangs up
            // makes both branches ready at once, and an unbiased select
            // would refuse a request that was in fact answered.
            biased;
            inbound = socket.recv() => {
                let (_, message) = match inbound {
                    Ok(received) => received,
                    Err(e) => break Err(e.into()),
                };
                let (stack, payload) = match backtrace::decode(message.body(), config.max_hops) {
                    Ok((stack, payload)) => (stack, payload.to_vec()),
                    // A reply this side cannot parse is a reply nobody can
                    // be given; the connection is finished, as a real REQ
                    // socket's would be.
                    Err(e) => break Err(BridgeError::Protocol(e.to_string())),
                };
                match pending.remove(&stack.id) {
                    Some(waiting) => {
                        let mut out = waiting.request.reply(TransferMeta::default()).await?;
                        out.write_all(&payload).await?;
                        out.finish()?;
                    }
                    // A reply for an exchange that timed out, or an id this
                    // side never sent — which is also what a
                    // *retransmitted* answer looks like. Neither is worth
                    // closing on: the requester has already been told.
                    None => tracing::debug!(
                        id = stack.id,
                        "a reply arrived for no pending exchange"
                    ),
                }
            }
            accepted = replier.accept() => {
                let mut request = match accepted {
                    Ok(request) => request,
                    Err(e) => break Err(BridgeError::from(e)),
                };
                // The ceiling first, because it is the cheaper refusal: a
                // request refused before its body is read costs this side
                // nothing, where reading first would buffer up to
                // `max_message_bytes` for an exchange about to be turned
                // away.
                if pending.len() >= config.max_pending_exchanges {
                    tracing::warn!(
                        ceiling = config.max_pending_exchanges,
                        "refused a weida exchange: the SP peer already has that many replies \
                         outstanding"
                    );
                    request.refuse(ErrorCode::Rejected).await;
                    continue;
                }
                let body = match request.body().read_capped(cap).await {
                    Ok(body) => body,
                    Err(weida::Error::LimitExceeded) => {
                        tracing::warn!(
                            cap,
                            "refused a weida request past max_message_bytes; NNG delivers a \
                             message wholly or not at all, so it cannot be handed a prefix"
                        );
                        request.refuse(ErrorCode::Rejected).await;
                        continue;
                    }
                    Err(e) => break Err(e.into()),
                };
                let id = ids.next();
                // One request, once. The tag is the correlation and there is
                // no timer that would write it a second time.
                let message = Message::from_body(Backtrace::direct(id).encode_message(&body));
                if let Err(e) = socket.send(message).await {
                    request.refuse(ErrorCode::NoReply).await;
                    break Err(e.into());
                }
                pending.insert(id, Pending { request, since: Instant::now() });
            }
            () = gone.notified() => {
                break Err(BridgeError::PeerClosed);
            }
            () = tokio::time::sleep(config.reply_deadline / 4) => {
                expire(&mut pending, config.reply_deadline).await;
            }
        }
    };

    // A run that ends says why: the two sides each see half of it, and
    // without this the half nobody is looking at is lost.
    if let Err(error) = &outcome {
        tracing::warn!(%error, "the outbound REQ bridge is ending");
    }

    // Whatever ended the loop, nobody waiting may be left waiting: without
    // this they would sit out the whole deadline for an answer that has
    // already become impossible.
    for (id, waiting) in pending.drain() {
        tracing::warn!(id, "the SP connection ended with this exchange unanswered");
        waiting.request.refuse(ErrorCode::NoReply).await;
    }
    outcome
}

/// Tells every exchange past its deadline that no reply is coming.
async fn expire(pending: &mut HashMap<u32, Pending>, deadline: Duration) {
    let due: Vec<u32> = pending
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
            "no SP reply within the deadline; refusing the weida exchange with NoReply"
        );
        // `NoReply` rather than `Rejected`: the request was taken and nobody
        // declined it — SP has no way to decline — so the absence is all
        // this side can report.
        waiting.request.refuse(ErrorCode::NoReply).await;
    }
}

/// `PUSH` toward a foreign `PULL`: one SP message per weida transfer.
///
/// Both sides block: a PUSH socket waits for a puller that can accept
/// (`docs/research/nanomsg-nng.md` §4, §5) and weida's Push/Pull
/// backpressure is `Block` (`docs/GUARANTEES.md` §6), so sending when the
/// peer cannot take the message stops this loop, which stops the weida
/// puller. Nothing is converted into a drop, which is the pairing rule of
/// `docs/adapters/nng.md` §4.
async fn serve_push(
    socket: PushSocket,
    gone: Arc<Notify>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let puller = listener.puller(&config.weida_path)?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        let transfer = tokio::select! {
            transfer = puller.recv() => transfer?,
            () = gone.notified() => return Err(BridgeError::PeerClosed),
        };
        // Over the cap, `collect` has already refused the rest of the
        // payload and buffered none of it. That is a refusal of **this
        // transfer**, and weida keeps refusals per stream, so the loop
        // continues rather than taking the SP peer down for one oversized
        // message it never saw.
        let body = match transfer.collect(cap).await {
            Ok(body) => body,
            Err(weida::Error::LimitExceeded) => {
                tracing::warn!(
                    cap,
                    "refused a weida payload past max_message_bytes; NNG delivers a message \
                     wholly or not at all"
                );
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        socket.send(body).await?;
    }
}

/// `SUB` toward a foreign `PUB`: what the publisher sends is published
/// onward.
///
/// Nothing is sent to the peer, ever — a SUB socket has no send operation
/// (`docs/research/nanomsg-nng.md` §4) — and the prefix match happens in the
/// library's own SUB socket, against the body as it arrived and before the
/// topic is split off. That is what "SP filters at the subscriber" means
/// when the subscriber is a bridge, and
/// [`SubSocket::discarded`](weida_nng::SubSocket::discarded) counts what it
/// cost: the bandwidth was spent either way, which is L1 seen from this end.
async fn serve_sub(
    socket: SubSocket,
    gone: Arc<Notify>,
    listener: &Listener,
    config: &OutboundConfig,
) -> Result<(), BridgeError> {
    let publisher = listener.publisher(&config.weida_path)?;

    loop {
        let message = tokio::select! {
            received = socket.recv() => received?,
            () = gone.notified() => return Err(BridgeError::PeerClosed),
        };
        let (topic, payload) = match config.topic_split.apply(message.body()) {
            Ok(split) => split,
            // A body this bridge cannot split is a configuration mismatch,
            // not a protocol violation by the peer — but it is also not
            // something to guess about, so the run ends and says why.
            Err(e) => return Err(e),
        };
        match publisher.publish(topic, payload.to_vec()) {
            Ok(_) => {}
            Err(weida::Error::LimitExceeded) => tracing::warn!(
                topic,
                "published payload is larger than subscriber_buffer_bytes; dropped"
            ),
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialling_maps_onto_the_endpoint_types_of_the_document() {
        assert_eq!(Dialling::Req.endpoint(), EndpointType::Req);
        assert_eq!(Dialling::Push.endpoint(), EndpointType::Push);
        assert_eq!(Dialling::Sub.endpoint(), EndpointType::Sub);
    }

    #[test]
    fn a_delimited_body_splits_where_the_octet_is() {
        let split = TopicSplit::Delimiter(0);
        let (topic, payload) = split.apply(b"orders/new\0{}").expect("split");
        assert_eq!(topic, "orders/new");
        assert_eq!(payload, b"{}");
        assert!(split.apply(b"no delimiter here").is_err());
    }

    #[test]
    fn a_fixed_topic_takes_the_first_octets_and_refuses_a_short_body() {
        let split = TopicSplit::Fixed(4);
        let (topic, payload) = split.apply(b"abcdrest").expect("split");
        assert_eq!(topic, "abcd");
        assert_eq!(payload, b"rest");
        assert!(split.apply(b"ab").is_err());
    }

    #[test]
    fn a_constant_topic_keeps_the_whole_body() {
        let split = TopicSplit::Constant("feed".into());
        let (topic, payload) = split.apply(b"whatever").expect("split");
        assert_eq!(topic, "feed");
        assert_eq!(payload, b"whatever");
    }

    /// Claim: request ids stay inside the 31 bits the tag stack leaves,
    /// because the terminal bit is the encoding's and not the id's
    /// [rfc-reqrep §5].
    #[test]
    fn request_ids_stay_inside_thirty_one_bits() {
        let mut ids = RequestIds(MAX_ID - 1);
        assert_eq!(ids.next(), MAX_ID);
        assert_eq!(ids.next(), 0);
        assert!(RequestIds::new().next() <= MAX_ID);
    }

    /// Claim: the bridge's bounds become the SP socket's options under
    /// NNG's own names, and the pipe ceiling is one because the
    /// configuration names one peer.
    #[test]
    fn the_bridges_bounds_are_the_sockets_options() {
        let mut config = OutboundConfig::new(
            "127.0.0.1:1".parse().expect("addr"),
            "127.0.0.1:0".parse().expect("addr"),
            "/rpc",
            Dialling::Req,
        );
        config.max_message_bytes = 2048;
        config.max_hops = 5;
        let options = config.socket_options();
        assert_eq!(options.recv_max_size, 2048);
        assert_eq!(options.max_ttl, 5);
        assert_eq!(options.max_pipes, 1);
        assert_eq!(config.url(), "tcp://127.0.0.1:1");
    }
}
