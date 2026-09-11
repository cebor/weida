//! The inbound direction: foreign SP peers, weida onward.
//!
//! One listener, one endpoint type presented, one weida endpoint. Each
//! accepted connection is a task that exchanges protocol headers and then
//! runs one of three loops, chosen by what the bridge presents.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use weida::{ClientTls, GuaranteeSet, Runtime, RuntimeConfig};
use weida_sp::backtrace;
use weida_sp::header::EndpointType;

use crate::error::BridgeError;
use crate::wire::{Session, frame};

/// Which SP protocol the bridge presents to whoever connects.
///
/// Three, because these are the three weida patterns that exist and the three
/// whose SP counterparts the bridge can serve by binding
/// (`docs/ARCHITECTURE.md` §6c.4). SP itself lets either role listen or dial
/// (`docs/research/nanomsg-nng.md` §1), so which side binds is the adapter's
/// configuration and not a translation - loss L8 of
/// `docs/adapters/nng.md` §8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presenting {
    /// `REP`: a `REQ` peer's messages become weida Req/Rep exchanges, and the
    /// reply travels back carrying the request's tag stack unchanged.
    Rep,
    /// `PULL`: a `PUSH` peer's messages become weida one-way transfers.
    Pull,
    /// `PUB`: what a weida `Publisher` fans out travels to a `SUB` peer, which
    /// filters it itself.
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
    /// This is the bound `docs/adapters/nng.md` §3 requires. An SP message
    /// may declare 2^64-1 octets [rfc-tcp §3], there is no credit on the wire
    /// and `RECVMAXSZ` is unlimited by default
    /// (`docs/research/nanomsg-nng.md` §5), so without this the peer chooses
    /// the allocation. It bounds both directions: NNG "delivers a message
    /// wholly or not at all" (§2), so a weida payload has to be buffered
    /// before its length can be written, and one beyond this is refused
    /// rather than truncated - loss L2.
    ///
    /// Zero is refused at configuration time, because it would refuse every
    /// message; there is no "unlimited" setting, which is loss L9.
    ///
    /// **The default, 1 MiB, is a memory choice and the product is stated.**
    /// It is `stream_receive_window`, the per-stream budget the bridge's own
    /// weida reads already live inside (`docs/PROTOCOL.md` §10), and it is
    /// the number B-043 settled on for the ZMTP bridge for the same reason:
    /// a cap borrowed from `subscriber_buffer_bytes` multiplied into a total
    /// nobody had chosen. Here the worst case one bridge holds is
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
    /// Largest tag stack the bridge will accept on a REQ body, in hops.
    ///
    /// The local `MAXTTL`. The specification side documents 1-255 and NNG's
    /// source caps it at 15 (`docs/research/nanomsg-nng.md` §11,
    /// `weida_sp::backtrace`), so this is a choice and the default is SP's
    /// own common one, 8. It also bounds an allocation: a tag stack is a
    /// `Vec<u32>` a peer fills, so the ceiling is `max_hops × 4` octets per
    /// in-flight request and nothing beyond it is ever read.
    pub max_hops: usize,
    /// Requests the bridge will have in flight at once on one connection.
    ///
    /// A cooked REQ socket holds one outstanding request per context and a
    /// socket may own many (`docs/research/nanomsg-nng.md` §2), so a peer can
    /// legitimately pipeline. Unbounded concurrency would let it choose how
    /// many tasks and payloads this process holds; at the bound the bridge
    /// stops reading, which is TCP backpressure and not a drop.
    pub max_in_flight: usize,
    /// SP connections this bridge serves at once.
    ///
    /// The listener would otherwise accept without a ceiling, and every
    /// accepted connection costs a weida connection and up to
    /// `2 × max_in_flight` buffered messages. At the bound the bridge stops
    /// accepting, so a new peer waits in the kernel's backlog rather than
    /// being served badly.
    pub max_connections: usize,
    /// The weida runtime configuration.
    ///
    /// Its guarantee set must be `core`: SP has no transfer point at all, no
    /// producer sequence and no deduplication key, so anything above `core` is
    /// refused at configuration time rather than silently unmet
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
    /// Binds the SP-side listener and prepares the weida side.
    ///
    /// Refuses, before anything is served:
    ///
    /// * a weida-side guarantee set above `core` (§9.2, §9.3);
    /// * a `max_message_bytes` of zero, which would refuse every message and
    ///   is the only way to ask for `RECVMAXSZ = 0` here (§9.6, loss L9);
    /// * a `max_hops` of zero, which would refuse every REQ body;
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
        let listener = TcpListener::bind(config.listen).await?;
        Ok(Inbound {
            listener,
            runtime,
            config: Arc::new(config),
            tls: Arc::new(tls),
        })
    }

    /// The address the SP side is listening on, with the port the OS chose
    /// when the configuration asked for zero.
    pub fn local_addr(&self) -> Result<SocketAddr, BridgeError> {
        Ok(self.listener.local_addr()?)
    }

    /// The weida runtime this bridge dials with, for shutting it down.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Accepts and serves SP peers until the listener fails.
    ///
    /// One task per connection, and a connection that fails takes nothing
    /// else with it: a dialing SP peer reconnects on its own with its
    /// configured backoff (`docs/research/nanomsg-nng.md` §1) and the bridge
    /// holds no state on its behalf - no session, no subscription, nothing
    /// resumed (§12/P6).
    pub async fn serve(self) -> Result<(), BridgeError> {
        // The ceiling on accepted connections, held for as long as the
        // connection is served: without it the listener would accept without
        // a bound and the memory product of `max_message_bytes` would have no
        // last factor.
        let slots = Arc::new(Semaphore::new(self.config.max_connections));
        loop {
            let slot = Arc::clone(&slots)
                .acquire_owned()
                .await
                .map_err(|_| BridgeError::Configuration("connection ceiling closed".into()))?;
            let (socket, from) = self.listener.accept().await?;
            let config = Arc::clone(&self.config);
            let tls = Arc::clone(&self.tls);
            let runtime = self.runtime.clone();
            tokio::spawn(async move {
                let _slot = slot;
                if let Err(e) = serve_one(socket, runtime, config, tls).await {
                    match e {
                        BridgeError::PeerClosed => {
                            tracing::debug!(%from, "SP peer closed the connection");
                        }
                        e => tracing::warn!(%from, error = %e, "bridged connection ended"),
                    }
                }
            });
        }
    }
}

/// Drives one accepted SP connection.
async fn serve_one(
    socket: TcpStream,
    runtime: Runtime,
    config: Arc<InboundConfig>,
    tls: Arc<ClientTls>,
) -> Result<(), BridgeError> {
    // Nagle off: a bridge writes whole messages and a delayed small message is
    // a delayed message, which is what the SP side notices.
    socket.set_nodelay(true)?;
    let mut session = Session::new(socket, config.max_message_bytes);
    let ours = config.presenting.endpoint();
    let theirs = session.handshake(ours).await?;
    tracing::debug!(
        presenting = ?ours,
        peer = ?theirs,
        "SP protocol headers exchanged"
    );

    match config.presenting {
        Presenting::Rep => serve_rep(session, runtime, &config, &tls).await,
        Presenting::Pull => serve_pull(session, runtime, &config, &tls).await,
        Presenting::Pub => serve_pub(session, runtime, &config, &tls).await,
    }
}

/// `REP`: one weida Req/Rep exchange per inbound message, concurrently.
///
/// The tag stack is consumed on the way in and written back unchanged on the
/// reply - "the processing node attaches the backtrace stack from the request
/// to the reply" [rfc-reqrep §5] - which is what lets a device between the
/// peer and the bridge route the answer home. weida needs none of it, because
/// the reply half of the exchange *is* the correlation
/// (`docs/PATTERNS.md` §1.2), so the stack is carried beside the exchange
/// rather than inside it.
///
/// **A retransmitted request becomes a second exchange.** A cooked REQ resends
/// on its resend timer, on peer disconnect, or when a peer becomes available
/// (`docs/research/nanomsg-nng.md` §4), and the bridge cannot tell a resend
/// from a new request: the request ID is 31 bits, per-context and randomly
/// seeded [rfc-reqrep §5]. Forwarding it is what `docs/adapters/nng.md` §7
/// decided - the duplicate reaches the weida application, which is loss L4 -
/// and suppressing it would need a deduplication identity nobody has agreed
/// on (§9.3).
async fn serve_rep(
    session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let requester = runtime.requester(tls.clone());
    requester.connect(&config.weida_url).await?;
    let requester = Arc::new(requester);

    let (mut reader, mut writer) = session.split();
    let (replies_tx, mut replies_rx) = mpsc::channel::<Vec<u8>>(config.max_in_flight);
    let writing = tokio::spawn(async move {
        while let Some(bytes) = replies_rx.recv().await {
            if writer.write_all(&bytes).await.is_err() || writer.flush().await.is_err() {
                break;
            }
        }
    });

    // One permit per in-flight exchange: at the bound this loop stops reading,
    // which closes the TCP window and is the peer's backpressure.
    let permits = Arc::new(Semaphore::new(config.max_in_flight));
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    let outcome = loop {
        let permit = match Arc::clone(&permits).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break Ok(()),
        };
        let body = match reader.read_message().await {
            Ok(body) => body,
            Err(e) => break Err(e),
        };
        let (stack, payload) = match backtrace::decode(&body, config.max_hops) {
            Ok(split) => (split.0, split.1.to_vec()),
            Err(e) => break Err(e.into()),
        };

        let requester = Arc::clone(&requester);
        let replies = replies_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match exchange(&requester, &payload, cap).await {
                Ok(reply) => {
                    let bytes = frame(&[&stack.encode(), &reply]);
                    let _ = replies.send(bytes).await;
                }
                Err(e) => {
                    // SP has no way to say "this request failed": there is no
                    // error frame and a REQ peer's only recovery is its own
                    // resend timer (`docs/adapters/nng.md` §8 L10). Not
                    // answering is therefore the honest behaviour, and it is
                    // what a REP socket that drops a request also produces.
                    tracing::warn!(error = %e, "exchange failed; the SP peer will see no reply");
                }
            }
        });
    };

    drop(replies_tx);
    let _ = writing.await;
    outcome
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
/// (`docs/research/nanomsg-nng.md` §4, §5); weida's Push/Pull backpressure is
/// `Block` (`docs/GUARANTEES.md` §6). The loop awaits the weida send before
/// reading again, so a stalled weida puller stops the bridge reading, which
/// closes the TCP window, which stops the PUSH socket - one policy, three
/// layers, which is the pairing rule of `docs/adapters/nng.md` §4.
async fn serve_pull(
    mut session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let pusher = runtime.pusher(tls.clone());
    pusher.connect(&config.weida_url).await?;

    loop {
        let body = session.read_message().await?;
        pusher.send(&body).await?;
    }
}

/// `PUB`: what a weida `Publisher` fans out travels to the SP peer, which
/// filters it itself.
///
/// **The bridge subscribes to everything on the weida side.** SP filters at
/// the subscriber: "PUB broadcasts every message to every connected SUB; each
/// SUB filters locally by prefix subscription, so subscriptions do not reduce
/// link bandwidth" (`docs/research/nanomsg-nng.md` §4), and a SUB socket
/// cannot send, so it never tells the bridge what it wants. There is nothing
/// to translate into a weida filter, so the bridge uses the empty filter,
/// which `docs/adapters/nng.md` §6 maps exactly onto SP's empty subscription:
/// every publication on the path arrives and the peer decides. That is loss
/// L1 in this direction - a weida filter cannot reduce what crosses the SP
/// link, because the bridge does not know what the peer would keep.
///
/// **The topic is the leading bytes of the body**, because SP has no topic
/// field at all (§3) and a subscription is a byte prefix of the body (§4).
/// The bridge therefore writes the weida `topic` and then the payload, with
/// no separator: that concatenation *is* the convention, and it is
/// configuration rather than framing (`docs/adapters/nng.md` §6).
async fn serve_pub(
    mut session: Session<TcpStream>,
    runtime: Runtime,
    config: &InboundConfig,
    tls: &ClientTls,
) -> Result<(), BridgeError> {
    let subscriber = runtime.subscriber(tls.clone());
    subscriber.connect(&config.weida_url).await?;
    subscriber.subscribe("").await?;
    let cap = usize::try_from(config.max_message_bytes).unwrap_or(usize::MAX);

    loop {
        tokio::select! {
            // A SUB socket has no send operation [nanomsg-nng §4], so
            // anything arriving here is the peer closing - or a peer that is
            // not what it said it was.
            inbound = session.read_message() => {
                let body = inbound?;
                return Err(BridgeError::Protocol(format!(
                    "a SUB peer sent {} octets; a SUB socket has no send operation",
                    body.len()
                )));
            }
            published = subscriber.recv() => {
                let transfer = published?;
                let topic = transfer.meta().topic.clone().unwrap_or_default();
                let payload = transfer.collect(cap).await?;
                // Written inline rather than queued: a peer that stops
                // reading stalls this loop, and the drop then happens at the
                // weida publisher's per-subscriber budget, where it is
                // counted (`docs/GUARANTEES.md` §6). NNG drops in the
                // subscriber's own queue instead
                // (`docs/research/nanomsg-nng.md` §4); both sides drop and
                // neither stalls the publisher, which is the pairing
                // `docs/adapters/nng.md` §4 requires.
                session.write_message(&[topic.as_bytes(), &payload]).await?;
            }
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
}
