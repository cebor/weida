//! The inbound direction: foreign SP peers, weida onward.
//!
//! One SP socket of `weida-nng` listening, one endpoint type presented, one
//! weida endpoint. **The bridge no longer speaks SP**: the protocol header,
//! the pairing check, the 64-bit framing and the REQ tag stack are the
//! library's, and what is left here is configuration, mapping and refusal —
//! which is all a hop ever was
//! ([0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §4.5, §5.2).

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Semaphore;
use weida::{ClientTls, GuaranteeSet, Runtime, RuntimeConfig};
use weida_nng::{Context, ContextConfig, PubSocket, PullSocket, RepSocket, SocketOptions};
use weida_sp::backtrace;
use weida_sp::header::EndpointType;

use crate::error::BridgeError;

/// Which SP protocol the bridge presents to whoever connects.
///
/// Three, because these are the three weida patterns that exist and the
/// three whose SP counterparts the bridge can serve by binding
/// (`docs/ARCHITECTURE.md` §6c.4). SP itself lets either role listen or
/// dial (`docs/research/nanomsg-nng.md` §1), so which side binds is the
/// adapter's configuration and not a translation — loss L8 of
/// `docs/adapters/nng.md` §8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presenting {
    /// `REP`: a `REQ` peer's messages become weida Req/Rep exchanges, and
    /// the reply travels back carrying the request's tag stack unchanged.
    Rep,
    /// `PULL`: a `PUSH` peer's messages become weida one-way transfers.
    Pull,
    /// `PUB`: what a weida `Publisher` fans out travels to a `SUB` peer,
    /// which filters it itself.
    Pub,
}

impl Presenting {
    fn endpoint(self) -> EndpointType {
        match self {
            Presenting::Rep => EndpointType::Rep,
            Presenting::Pull => EndpointType::Pull,
            Presenting::Pub => EndpointType::Pub,
        }
    }
}

/// How to bridge one SP address onto one weida endpoint.
#[derive(Clone, Debug)]
pub struct InboundConfig {
    /// TCP address to accept SP peers on.
    pub listen: SocketAddr,
    /// The weida endpoint to speak to: `weida://[fingerprint@]host:port/path`.
    pub weida_url: String,
    /// Which SP protocol to present.
    pub presenting: Presenting,
    /// Largest message body the bridge will hold, in octets.
    ///
    /// This is `NNG_OPT_RECVMAXSZ` on the SP socket **and** the bridge's own
    /// weida-side cap, which is the split
//! [0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
    /// §5.2 asks for: the SP side's limit is the library's option, judged
    /// from the declared 64-bit length before anything is allocated, and the
    /// weida side's is this crate's, because a weida payload has to be
    /// buffered whole before its length can be written — NNG "delivers a
    /// message wholly or not at all" (§2), so one beyond this is refused
    /// rather than truncated (loss L2).
    ///
    /// Zero is refused at configuration time, because it would refuse every
    /// message; there is no "unlimited" setting, which is loss L9.
    ///
    /// **The default, 1 MiB, is a memory choice and the product is stated.**
    /// The worst case one bridge holds is
    ///
    /// ```text
    /// max_message_bytes × 2 × max_in_flight × max_connections
    ///          1 MiB    × 2 ×      4        ×      64          = 512 MiB
    /// ```
    ///
    /// Twice `max_in_flight` because a request payload is held while its
    /// exchange runs and its reply is held while it waits for the writer.
    /// Raising any factor raises the product, which is the whole reason all
    /// three are configuration and not constants.
    pub max_message_bytes: u64,
    /// Largest tag stack the bridge will accept on a REQ body, in hops:
    /// `NNG_OPT_MAXTTL` on the socket.
    ///
    /// The specification side documents 1-255 and NNG's source caps it at 15
    /// (`docs/research/nanomsg-nng.md` §11), so this is a choice and the
    /// default is SP's own common one, 8.
    pub max_hops: usize,
    /// Requests the bridge will have in flight at once.
    ///
    /// A cooked REQ socket holds one outstanding request per context and a
    /// socket may own many (`docs/research/nanomsg-nng.md` §2), so a peer can
    /// legitimately pipeline. Unbounded concurrency would let it choose how
    /// many tasks and payloads this process holds; at the bound the bridge
    /// stops taking requests, which stops it reading, which is TCP
    /// backpressure and not a drop.
    pub max_in_flight: usize,
    /// SP connections this bridge serves at once: the socket's pipe ceiling.
    ///
    /// SP bounds nothing a stranger opens (`docs/INVARIANTS.md`), so the
    /// library's `max_pipes` is this number and a connection past it is
    /// closed as soon as it is accepted.
    pub max_connections: usize,
    /// An octet written between the topic and the payload when presenting
    /// `PUB`, or `None` for the two concatenated.
    ///
    /// SP has no topic field: the topic is the leading bytes of the body and
    /// nothing on the wire says where it ends
    /// (`docs/research/nanomsg-nng.md` §3), so a peer matching a byte prefix
    /// needs no delimiter and gets none by default. One is needed the moment
    /// the *other* side has to recover the topic — an SP consumer that
    /// re-publishes, or this crate's own [`crate::Outbound`] with a matching
    /// [`crate::TopicSplit::Delimiter`]. `0x00` is the octet to use: a weida
    /// topic is text, so NUL can never occur inside one.
    pub topic_delimiter: Option<u8>,
    /// The weida runtime configuration.
    ///
    /// Its guarantee set must be `core`: SP has no transfer point at all, no
    /// producer sequence and no deduplication key, so anything above `core`
    /// is refused at configuration time rather than silently unmet
    /// (`docs/adapters/nng.md` §7, §9.2, §9.3).
    pub runtime: RuntimeConfig,
}

impl InboundConfig {
    /// A configuration for one address, endpoint and SP protocol, with the
    /// defaults the mapping document argues for.
    pub fn new(listen: SocketAddr, weida_url: impl Into<String>, presenting: Presenting) -> Self {
        InboundConfig {
            listen,
            weida_url: weida_url.into(),
            presenting,
            max_message_bytes: 1024 * 1024,
            max_hops: backtrace::DEFAULT_MAX_HOPS,
            max_in_flight: 4,
            max_connections: 64,
            topic_delimiter: None,
            runtime: RuntimeConfig::default(),
        }
    }

    /// The SP socket's options: this configuration's bounds under NNG's own
    /// names.
    fn socket_options(&self) -> SocketOptions {
        SocketOptions {
            recv_max_size: self.max_message_bytes,
            max_ttl: self.max_hops,
            max_pipes: self.max_connections,
            ..SocketOptions::default()
        }
    }
}

/// The SP socket one bridge presents, one variant per [`Presenting`].
enum Presented {
    Rep(RepSocket),
    Pull(PullSocket),
    Pub(PubSocket),
}

/// A running bridge: an SP socket and the weida runtime behind it.
pub struct Inbound {
    presented: Presented,
    local: SocketAddr,
    runtime: Runtime,
    config: Arc<InboundConfig>,
    tls: Arc<ClientTls>,
    /// Held so the SP sockets' reactor container outlives them.
    _sp: Context,
}

impl Inbound {
    /// Binds the SP-side listener and prepares the weida side.
    ///
    /// Refuses, before anything is served:
    ///
    /// * a weida-side guarantee set above `core` (§9.2, §9.3);
    /// * a `max_message_bytes` of zero, which would refuse every message and
    ///   is the only way to ask for `RECVMAXSZ = 0` here (§9.6, loss L9);
    /// * a `max_hops` of zero, which would refuse every REQ body;
    /// * a zero in either factor of the memory product;
    /// * a weida URL that does not parse, which is weida's own check.
    pub async fn bind(config: InboundConfig, tls: ClientTls) -> Result<Inbound, BridgeError> {
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
        if config.max_hops == 0 {
            return Err(BridgeError::Configuration(
                "max_hops is zero, so every request's tag stack would be refused".into(),
            ));
        }
        if config.max_in_flight == 0 || config.max_connections == 0 {
            return Err(BridgeError::Configuration(
                "max_in_flight and max_connections are the factors of the memory product this \
                 bridge is bounded by; zero in either would serve nothing"
                    .into(),
            ));
        }
        // Parsed here so a typo fails at bind time rather than on the first
        // message.
        let _ = weida::EndpointAddr::parse(&config.weida_url)?;

        let runtime = Runtime::new(config.runtime.clone())?;
        let sp = Context::new(ContextConfig::default())?;
        let options = config.socket_options();
        let url = format!("tcp://{}", config.listen);
        let (presented, local) = match config.presenting {
            Presenting::Rep => {
                let socket = RepSocket::with_options(&sp, options)?;
                let bound = socket.listen(&url).await?;
                let local = bound_addr(&bound)?;
                (Presented::Rep(socket), local)
            }
            Presenting::Pull => {
                let socket = PullSocket::with_options(&sp, options)?;
                let bound = socket.listen(&url).await?;
                let local = bound_addr(&bound)?;
                (Presented::Pull(socket), local)
            }
            Presenting::Pub => {
                let socket = PubSocket::with_options(&sp, options)?;
                let bound = socket.listen(&url).await?;
                let local = bound_addr(&bound)?;
                (Presented::Pub(socket), local)
            }
        };
        tracing::debug!(presenting = ?config.presenting.endpoint(), %local, "SP side bound");
        Ok(Inbound {
            presented,
            local,
            runtime,
            config: Arc::new(config),
            tls: Arc::new(tls),
            _sp: sp,
        })
    }

    /// The address the SP side is listening on, with the port the OS chose
    /// when the configuration asked for zero.
    pub fn local_addr(&self) -> Result<SocketAddr, BridgeError> {
        Ok(self.local)
    }

    /// The weida runtime this bridge dials with, for shutting it down.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Serves SP peers until the socket or the weida side ends.
    ///
    /// A peer that goes away takes nothing else with it: a dialling SP peer
    /// reconnects on its own with its configured backoff
    /// (`docs/research/nanomsg-nng.md` §1) and the bridge holds no state on
    /// its behalf — no session, no subscription, nothing resumed (§12/P6).
    pub async fn serve(self) -> Result<(), BridgeError> {
        match self.presented {
            Presented::Rep(socket) => {
                serve_rep(socket, self.runtime, &self.config, &self.tls).await
            }
            Presented::Pull(socket) => {
                serve_pull(socket, self.runtime, &self.config, &self.tls).await
            }
            Presented::Pub(socket) => {
                serve_pub(socket, self.runtime, &self.config, &self.tls).await
            }
        }
    }
}

/// The address a listener ended up on, which for a wildcard port is the only
/// way to learn the port.
fn bound_addr(listener: &weida_nng::Listener) -> Result<SocketAddr, BridgeError> {
    let url = listener.url().to_string();
    let addr = url.trim_start_matches("tcp://");
    addr.parse().map_err(|_| {
        BridgeError::Configuration(format!("the SP side bound {url}, which is not an address"))
    })
}

/// `REP`: one weida Req/Rep exchange per inbound message, concurrently.
///
/// **The tag stack is the library's now.** A `RepCtx` remembers the stack
/// and the pipe its request arrived on and writes the stack back on the
/// reply — "the processing node attaches the backtrace stack from the
/// request to the reply" [rfc-reqrep §5] — so the bridge holds one context
/// per in-flight exchange and nothing else. weida needs none of it, because
/// the reply half of the exchange *is* the correlation
/// (`docs/PATTERNS.md` §1.2).
///
/// **A retransmitted request becomes a second exchange.** A cooked REQ
/// resends on its resend timer, on peer disconnect, or when a peer becomes
/// available (`docs/research/nanomsg-nng.md` §4), and the bridge cannot tell
/// a resend from a new request: the request ID is 31 bits, per-context and
/// randomly seeded [rfc-reqrep §5]. Forwarding it is what
/// `docs/adapters/nng.md` §7 decided — the duplicate reaches the weida
/// application, which is loss L4 — and suppressing it would need a
/// deduplication identity nobody has agreed on (§9.3).
async fn serve_rep(
    socket: RepSocket,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let requester = runtime.requester(tls.clone());
    requester.connect(&config.weida_url).await?;
    let requester = Arc::new(requester);

    // One permit per in-flight exchange: at the bound this loop stops taking
    // requests, which stops the socket's queues draining, which closes the
    // TCP window and is the peer's backpressure.
    let permits = Arc::new(Semaphore::new(config.max_in_flight));
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        let permit = match Arc::clone(&permits).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return Ok(()),
        };
        // One context per exchange, which is exactly what an `nng_ctx` is
        // for: "several requests to be processed in parallel over one
        // socket" (§4).
        let exchange_ctx = socket.context();
        let request = exchange_ctx.recv().await?;
        let requester = Arc::clone(&requester);
        tokio::spawn(async move {
            let _permit = permit;
            match exchange(&requester, request.body(), cap).await {
                Ok(reply) => {
                    if let Err(e) = exchange_ctx.send(reply).await {
                        tracing::debug!(error = %e, "the SP peer went before its reply");
                    }
                }
                Err(e) => {
                    // SP has no way to say "this request failed": there is
                    // no error frame and a REQ peer's only recovery is its
                    // own resend timer (`docs/adapters/nng.md` §8 L10). Not
                    // answering is therefore the honest behaviour, and it is
                    // what a REP socket that drops a request also produces.
                    tracing::warn!(error = %e, "exchange failed; the SP peer will see no reply");
                }
            }
        });
    }
}

/// One weida exchange, collected under the bridge's own cap.
async fn exchange(
    requester: &weida::Requester,
    payload: &[u8],
    cap: usize,
) -> Result<Vec<u8>, BridgeError> {
    let reply = requester.request(payload).await?;
    Ok(reply.collect(cap).await?)
}

/// `PULL`: one weida one-way transfer per inbound message.
///
/// Backpressure is end to end by construction. A PUSH socket selects only
/// pullers that can accept a message and waits or times out when none can
/// (`docs/research/nanomsg-nng.md` §4, §5); weida's Push/Pull backpressure
/// is `Block` (`docs/GUARANTEES.md` §6). The loop awaits the weida send
/// before receiving again, so a stalled weida puller stops the bridge
/// draining the SP socket, which closes the TCP window, which stops the PUSH
/// socket — one policy, three layers, which is the pairing rule of
/// `docs/adapters/nng.md` §4.
async fn serve_pull(
    socket: PullSocket,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let pusher = runtime.pusher(tls.clone());
    pusher.connect(&config.weida_url).await?;

    loop {
        let message = socket.recv().await?;
        pusher.send(message.body()).await?;
    }
}

/// `PUB`: what a weida `Publisher` fans out travels to the SP peers, which
/// filter it themselves.
///
/// **The bridge subscribes to everything on the weida side.** SP filters at
/// the subscriber: "PUB broadcasts every message to every connected SUB;
/// each SUB filters locally by prefix subscription, so subscriptions do not
/// reduce link bandwidth" (`docs/research/nanomsg-nng.md` §4), and a SUB
/// socket cannot send, so it never tells the bridge what it wants. There is
/// nothing to translate into a weida filter, so the bridge uses the empty
/// filter, which `docs/adapters/nng.md` §6 maps exactly onto SP's empty
/// subscription: every publication on the path arrives and the peer decides.
/// That is loss L1 in this direction.
///
/// **The topic is the leading bytes of the body**, because SP has no topic
/// field at all (§3) and a subscription is a byte prefix of the body (§4).
/// The bridge writes the weida `topic` and then the payload, with no
/// separator unless one is configured: that concatenation *is* the
/// convention, and it is configuration rather than framing.
///
/// **A peer that cannot keep up loses its copy**, in the library's queues
/// and counted there: `PubSocket::send` is best-effort and never blocks (§4,
/// §5), which is also why this loop cannot be stalled by one slow
/// subscriber — the drop happens on the SP side rather than backing up into
/// the weida publisher.
async fn serve_pub(
    socket: PubSocket,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let subscriber = runtime.subscriber(tls.clone());
    subscriber.connect(&config.weida_url).await?;
    subscriber.subscribe("").await?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        let transfer = subscriber.recv().await?;
        let topic = transfer.meta().topic.clone().unwrap_or_default();
        let payload = transfer.collect(cap).await?;
        let mut body = Vec::with_capacity(topic.len() + payload.len() + 1);
        body.extend_from_slice(topic.as_bytes());
        if let Some(byte) = config.topic_delimiter {
            body.push(byte);
        }
        body.extend_from_slice(&payload);
        let broadcast = socket.send(body)?;
        if broadcast.dropped > 0 {
            tracing::debug!(
                dropped = broadcast.dropped,
                queued = broadcast.queued,
                "a SUB peer could not take its copy"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presenting_maps_onto_the_endpoint_types_of_the_document() {
        assert_eq!(Presenting::Rep.endpoint(), EndpointType::Rep);
        assert_eq!(Presenting::Pull.endpoint(), EndpointType::Pull);
        assert_eq!(Presenting::Pub.endpoint(), EndpointType::Pub);
        // And each accepts exactly the peer the mapping table names.
        assert_eq!(EndpointType::Rep.peer(), EndpointType::Req);
        assert_eq!(EndpointType::Pull.peer(), EndpointType::Push);
        assert_eq!(EndpointType::Pub.peer(), EndpointType::Sub);
    }

    /// Claim: the bridge's bounds become the SP socket's options under NNG's
    /// own names, so there is one number per bound rather than two that can
    /// drift.
    #[test]
    fn the_bridges_bounds_are_the_sockets_options() {
        let mut config = InboundConfig::new(
            "127.0.0.1:0".parse().expect("loopback"),
            "weida://127.0.0.1:1/jobs",
            Presenting::Pull,
        );
        config.max_message_bytes = 4096;
        config.max_hops = 3;
        config.max_connections = 7;
        let options = config.socket_options();
        assert_eq!(options.recv_max_size, 4096);
        assert_eq!(options.max_ttl, 3);
        assert_eq!(options.max_pipes, 7);
    }
}
