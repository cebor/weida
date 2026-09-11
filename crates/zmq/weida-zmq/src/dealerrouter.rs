//! DEALER and ROUTER: request-reply with the state machine taken out.
//!
//! 28/REQREP, through `docs/research/zeromq.md` §4.2:
//!
//! - **DEALER** "works as an asynchronous replacement for REQ". Unrestricted
//!   in both directions: round-robin over the peers "available only when
//!   [they have] an outgoing queue that is not full", block or error when
//!   none is, never discard, fair-queue inbound. It adds **no envelope at
//!   all** — talking to a REP means the *application* writes the empty
//!   delimiter, which is what "must emulate REQ's envelope exactly" means.
//! - **ROUTER** "works as an asynchronous replacement for REP". It
//!   identifies each peer's double queue by a routing id and, inbound,
//!   prefixes the message with it; outbound it removes the first frame as
//!   that id and routes by it, and "SHALL either silently drop the message,
//!   or return an error, depending on configuration, if the queue does not
//!   exist, or is full". It "SHALL NOT block on sending".
//!
//! **The routing id is the whole of ROUTER.** "An identity (also called an
//! address) is a binary string whose only meaning is 'this is a unique handle
//! to the connection'". A peer may choose its own through the `Identity`
//! property of its `READY` — `ZMQ_ROUTING_ID` on its side — and a peer that
//! chooses none is given one, distinguishable by its leading zero
//! ([`RoutingKey`]). ROUTER learns a peer only when it has one, which is why
//! `ZMQ_PROBE_ROUTER` exists: it makes a new connection announce itself with
//! an empty message so that "an application can really reply" *and* speak
//! first.
//!
//! **Three ROUTER options change what happens at the edges**, and each is
//! libzmq's own: `ZMQ_ROUTER_MANDATORY` turns the silent drop into
//! `EHOSTUNREACH` (or `EAGAIN` at the high-water mark),
//! `ZMQ_ROUTER_HANDOVER` lets a newcomer take an identity from an incumbent
//! rather than being rejected, and `ZMQ_PROBE_ROUTER` is set on the *peer*
//! rather than on the ROUTER.

use std::collections::HashMap;
use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::engine::PeerId;
use crate::error::{Error, Result};
use crate::identity::RoutingKey;
use crate::message::{Message, Multipart};
use crate::options::SocketOptions;
use crate::pipe::{MuteAction, Sent};
use crate::socket::{SocketCore, socket_endpoints};

/// A DEALER socket: REQ without the lockstep.
#[derive(Debug)]
pub struct DealerSocket {
    core: SocketCore,
}

impl DealerSocket {
    /// A DEALER socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<DealerSocket> {
        DealerSocket::with_options(context, SocketOptions::default())
    }

    /// A DEALER socket with `options`.
    ///
    /// The mute action is the table's row for DEALER — block — and not a
    /// setting: "SHALL not accept further messages when it has no available
    /// peers" and never discard.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<DealerSocket> {
        options.pipe.outgoing.mute = MuteAction::Block;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(DealerSocket {
            core: SocketCore::new(context, SocketType::Dealer, options)?,
        })
    }

    /// Sends a message, round-robin over the peers with room.
    ///
    /// Unrestricted: no alternation, no envelope, no state. Blocks when every
    /// peer's queue is full and when there is no peer at all, bounded by
    /// `ZMQ_SNDTIMEO`.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        let limit = self.core.options().send_timeout;
        let exec = self.core.exec().clone();
        let message = message.into();
        let delivered = match limit {
            None => self.core.send_round_robin(message).await?,
            Some(limit) => match exec
                .within(limit, self.core.send_round_robin(message))
                .await
            {
                Some(result) => result?,
                None => {
                    return Err(Error::EAGAIN(
                        format!("no peer took the message within {limit:?} (ZMQ_SNDTIMEO)").into(),
                    ));
                }
            },
        };
        if delivered.peer.is_none() {
            return Err(Error::EAGAIN(
                "no peer took the message; a DEALER never discards one".into(),
            ));
        }
        Ok(())
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` rather than a wait.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// Sends under an explicit wall-clock bound.
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        let exec = self.core.exec().clone();
        match exec
            .within(limit, self.core.send_round_robin(message.into()))
            .await
        {
            Some(result) => result.map(|_| ()),
            None => Err(Error::EAGAIN(
                format!("no peer took the message within {limit:?}").into(),
            )),
        }
    }

    /// Receives the next message from any peer, fair-queued, bounded by
    /// `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        match limit {
            None => self.core.recv_fair().await.map(|(_, message)| message),
            Some(limit) => match exec.within(limit, self.core.recv_fair()).await {
                Some(result) => result.map(|(_, message)| message),
                None => Err(Error::EAGAIN(
                    format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.core.recv_fair()).await {
            Some(result) => result.map(|(_, message)| message),
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?}").into(),
            )),
        }
    }
}

socket_endpoints!(DealerSocket);

/// A ROUTER socket: REP without the lockstep, addressing peers by routing id.
///
/// **The routing table is bounded, and by a number that is already named.**
/// It holds exactly one entry per live peer — [`RouterSocket::recv`],
/// [`RouterSocket::send`] and [`RouterSocket::peers`] all reconcile it
/// against the engine's peer set first, inserting for a peer that has
/// arrived and dropping the entry of one that has gone — so its ceiling is
/// `max_peers` entries, each at most
/// [`MAX_ROUTING_ID_BYTES`](crate::MAX_ROUTING_ID_BYTES) of peer-chosen
/// bytes: 1024 × 255 B at the defaults, twice over for the two directions of
/// the map. A stranger chooses the *contents* of a key but not how many
/// there are, and a departed peer's key does not outlive it. That is stated
/// here rather than assumed from `max_peers`, and a test asserts the
/// shrinking half.
#[derive(Debug)]
pub struct RouterSocket {
    core: SocketCore,
    /// Routing id to peer, and back. Two maps because both directions are
    /// hot: inbound needs the id of a peer, outbound the peer of an id.
    by_key: HashMap<RoutingKey, PeerId>,
    by_peer: HashMap<PeerId, RoutingKey>,
    /// The counter behind a generated routing id; see [`RoutingKey`].
    next_generated: u32,
}

impl RouterSocket {
    /// A ROUTER socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<RouterSocket> {
        RouterSocket::with_options(context, SocketOptions::default())
    }

    /// A ROUTER socket with `options`.
    ///
    /// The mute action is the table's row for ROUTER — drop — and not a
    /// setting: a ROUTER that blocked on one slow peer would stall every
    /// other, which is why its default is "brutal". `ZMQ_ROUTER_MANDATORY`
    /// changes what the *caller* is told, not what the queue does.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<RouterSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(RouterSocket {
            core: SocketCore::new(context, SocketType::Router, options)?,
            by_key: HashMap::new(),
            by_peer: HashMap::new(),
            next_generated: 1,
        })
    }

    /// Receives the next message, prefixed with the sender's routing id.
    ///
    /// `[routing-id][…the peer's frames]`, fair-queued across peers and
    /// bounded by `ZMQ_RCVTIMEO`. The id is the peer's announced `Identity`
    /// where it announced one and a generated key otherwise.
    pub async fn recv(&mut self) -> Result<Multipart> {
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        match limit {
            None => self.await_message().await,
            Some(limit) => match exec.within(limit, self.await_message()).await {
                Some(result) => result,
                None => Err(Error::EAGAIN(
                    format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.learn_peers();
        let (peer, message) = self.core.try_recv_fair()?;
        Ok(self.with_routing_id(peer, message))
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.await_message()).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?}").into(),
            )),
        }
    }

    /// Sends `message`, whose **first frame is the routing id** of the peer
    /// it is for.
    ///
    /// Never blocks unless `ZMQ_ROUTER_MANDATORY` is set. Without it, an
    /// unroutable message and a full queue are both [`Sent::Dropped`] — the
    /// silent drop, made visible in the return value. With it, an unroutable
    /// message is `EHOSTUNREACH` and a full queue is waited on, bounded by
    /// `ZMQ_SNDTIMEO`, which is libzmq's rule exactly.
    ///
    /// Fails with `EINVAL` when the message carries nothing but its routing
    /// id: a ROUTER strips that frame, and what is left would be no message.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        self.learn_peers();
        let (key, body) = split_routing_id(message.into())?;
        let mandatory = self.core.options().router_mandatory;

        let Some(peer) = self.by_key.get(&key).copied() else {
            return self.unroutable(&key, mandatory);
        };
        let Some(pipe) = self.core.pipe_of(peer) else {
            return self.unroutable(&key, mandatory);
        };
        let queue = pipe.outgoing();
        if queue.has_room() {
            queue.try_send(body)?;
            return Ok(Sent::Queued);
        }
        if !mandatory {
            // "0 discards silently when it cannot be routed **or the peer's
            // SNDHWM is reached**."
            tracing::debug!(
                key = ?key,
                "dropped a message: the peer's queue is at its high-water mark"
            );
            return Ok(Sent::Dropped);
        }
        // ZMQ_ROUTER_MANDATORY without ZMQ_DONTWAIT: wait for room, bounded
        // by ZMQ_SNDTIMEO.
        let limit = self.core.options().send_timeout;
        let queued = self
            .core
            .within(limit, async {
                loop {
                    queue.wait_for_room().await;
                    if queue.is_closed() {
                        return Err(Error::EHOSTUNREACH(
                            "the peer went away while its queue was full".into(),
                        ));
                    }
                    if queue.has_room() {
                        return Ok(());
                    }
                }
            })
            .await;
        match queued {
            Ok(()) => {
                queue.try_send(body)?;
                Ok(Sent::Queued)
            }
            Err(e) => Err(e),
        }
    }

    /// The `ZMQ_DONTWAIT` form.
    ///
    /// Without `ZMQ_ROUTER_MANDATORY` this is the same silent drop; with it,
    /// a full queue is `EAGAIN` and an unknown id `EHOSTUNREACH`, which is
    /// the pair libzmq documents for the option under `ZMQ_DONTWAIT`.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        self.learn_peers();
        let (key, body) = split_routing_id(message.into())?;
        let mandatory = self.core.options().router_mandatory;

        let Some(peer) = self.by_key.get(&key).copied() else {
            return self.unroutable(&key, mandatory);
        };
        let Some(pipe) = self.core.pipe_of(peer) else {
            return self.unroutable(&key, mandatory);
        };
        let queue = pipe.outgoing();
        if queue.has_room() {
            queue.try_send(body)?;
            return Ok(Sent::Queued);
        }
        if mandatory {
            Err(Error::EAGAIN(
                "this peer's queue is at its high-water mark (ZMQ_ROUTER_MANDATORY)".into(),
            ))
        } else {
            Ok(Sent::Dropped)
        }
    }

    /// The routing ids this ROUTER can address right now.
    ///
    /// A ROUTER knows a peer only once it has a routing id for it, which is
    /// after the peer's handshake — and, without `ZMQ_PROBE_ROUTER` on the
    /// peer, that is all it knows until the peer speaks.
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        self.learn_peers();
        let mut keys: Vec<RoutingKey> = self.by_key.keys().cloned().collect();
        keys.sort();
        keys
    }

    async fn await_message(&mut self) -> Result<Multipart> {
        self.learn_peers();
        let (peer, message) = self.core.recv_fair().await?;
        Ok(self.with_routing_id(peer, message))
    }

    fn with_routing_id(&mut self, peer: PeerId, message: Multipart) -> Multipart {
        let key = self.key_for(peer);
        let mut frames = Vec::with_capacity(message.len() + 1);
        frames.push(Message::from(key.as_bytes()));
        frames.extend(message.into_frames());
        Multipart::new(frames).expect("a routing id plus at least one frame")
    }

    /// Gives every current peer a routing id, and forgets the ones that left.
    ///
    /// This is also where a duplicate identity is resolved:
    /// `ZMQ_ROUTER_HANDOVER` decides whether the newcomer takes the name and
    /// the incumbent is disconnected, or the newcomer is rejected — libzmq's
    /// default being to reject it.
    fn learn_peers(&mut self) {
        let live = self.core.engine().peers();
        self.by_peer.retain(|peer, key| {
            let alive = live.iter().any(|current| current.id == *peer);
            if !alive {
                self.by_key.remove(key);
            }
            alive
        });
        for peer in live {
            if self.by_peer.contains_key(&peer.id) {
                continue;
            }
            if !peer.announced {
                // Its READY has not been read yet, so its own identity is
                // not known: keying it now would key it by a generated id
                // for the rest of its life.
                continue;
            }
            let key = match &peer.identity {
                Some(identity) => RoutingKey::announced(identity),
                None => {
                    let key = RoutingKey::generated(self.next_generated);
                    self.next_generated = self.next_generated.wrapping_add(1).max(1);
                    key
                }
            };
            if let Some(incumbent) = self.by_key.get(&key).copied() {
                if self.core.options().router_handover {
                    tracing::debug!(
                        key = ?key,
                        %incumbent,
                        "handing a routing id over to a newcomer (ZMQ_ROUTER_HANDOVER)"
                    );
                    self.core.engine().evict(incumbent);
                    self.by_peer.remove(&incumbent);
                } else {
                    tracing::warn!(
                        key = ?key,
                        %incumbent,
                        "rejected a peer claiming a routing id already in use"
                    );
                    self.core.engine().evict(peer.id);
                    continue;
                }
            }
            self.by_key.insert(key.clone(), peer.id);
            self.by_peer.insert(peer.id, key);
        }
    }

    /// This peer's routing id, learning it if the handshake has completed
    /// since the last reconciliation — which it usually has, because a
    /// message from a peer is proof that its `READY` was read.
    fn key_for(&mut self, peer: PeerId) -> RoutingKey {
        if let Some(key) = self.by_peer.get(&peer) {
            return key.clone();
        }
        self.learn_peers();
        if let Some(key) = self.by_peer.get(&peer) {
            return key.clone();
        }
        self.assign(peer)
    }

    fn assign(&mut self, peer: PeerId) -> RoutingKey {
        let key = RoutingKey::generated(self.next_generated);
        self.next_generated = self.next_generated.wrapping_add(1).max(1);
        self.by_key.insert(key.clone(), peer);
        self.by_peer.insert(peer, key.clone());
        key
    }

    fn unroutable(&self, key: &RoutingKey, mandatory: bool) -> Result<Sent> {
        if mandatory {
            Err(Error::EHOSTUNREACH(
                format!("no peer holds the routing id {key:?} (ZMQ_ROUTER_MANDATORY)").into(),
            ))
        } else {
            // "ROUTER sockets do have a somewhat brutal way of dealing with
            // messages they can't send anywhere: they drop them silently."
            tracing::debug!(key = ?key, "dropped a message for a routing id nobody holds");
            Ok(Sent::Dropped)
        }
    }
}

socket_endpoints!(RouterSocket);

/// Takes the routing id off the front of an outbound ROUTER message.
fn split_routing_id(message: Multipart) -> Result<(RoutingKey, Multipart)> {
    let mut frames = message.into_frames();
    if frames.len() < 2 {
        return Err(Error::EINVAL(
            "a ROUTER message is a routing id followed by the message; this one has only the \
             routing id"
                .into(),
        ));
    }
    let key = RoutingKey::from_wire(frames.remove(0).as_slice());
    Ok((
        key,
        Multipart::new(frames).expect("at least one frame left"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::identity::RoutingId;
    use crate::pipe::{PipeConfig, QueueConfig};

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn body(message: &Multipart) -> Vec<Vec<u8>> {
        message
            .frames()
            .iter()
            .map(|frame| frame.as_slice().to_vec())
            .collect()
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn dealer_options(routing_id: &[u8]) -> SocketOptions {
        SocketOptions {
            routing_id: Some(RoutingId::new(routing_id).expect("id")),
            reconnect_ivl: Some(Duration::from_millis(20)),
            ..SocketOptions::default()
        }
    }

    /// Claim: both directions are unrestricted — a DEALER sends two messages
    /// without waiting for anything, and the ROUTER answers them in the
    /// order it likes, addressing each by the routing id it was handed.
    #[tokio::test]
    async fn dealer_and_router_are_unrestricted_in_both_directions() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut dealer = DealerSocket::new(&ctx).expect("dealer");
        dealer.connect(&endpoint.to_string()).expect("connect");

        // Two requests, no reply awaited in between: this is what REQ
        // refuses with EFSM and DEALER exists to allow.
        dealer.send("one").await.expect("first");
        dealer.send("two").await.expect("second");

        let first = router.recv().await.expect("first request");
        let second = router.recv().await.expect("second request");
        assert_eq!(first.len(), 2, "routing id plus body");
        assert_eq!(body(&first)[1], b"one".to_vec());
        assert_eq!(body(&second)[1], b"two".to_vec());
        assert_eq!(
            body(&first)[0],
            body(&second)[0],
            "one peer, one routing id"
        );

        // Replies, newest first, because nothing makes a ROUTER answer in
        // order either.
        let key = body(&first)[0].clone();
        for reply in ["answer two", "answer one"] {
            let framed = Multipart::new(vec![Message::from(key.clone()), Message::from(reply)])
                .expect("frames");
            assert_eq!(router.send(framed).await.expect("reply"), Sent::Queued);
        }
        assert_eq!(
            body(&dealer.recv().await.expect("reply")),
            vec![b"answer two".to_vec()]
        );
        assert_eq!(
            body(&dealer.recv().await.expect("reply")),
            vec![b"answer one".to_vec()]
        );
    }

    /// Claim: a peer that names itself is addressed by that name, and one
    /// that does not is addressed by a generated id — distinguishable by its
    /// leading zero, which is libzmq's own convention.
    #[tokio::test]
    async fn a_peer_chosen_identity_is_honoured() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut named =
            DealerSocket::with_options(&ctx, dealer_options(b"worker-1")).expect("named dealer");
        named.connect(&endpoint.to_string()).expect("connect");
        named.send("from the named one").await.expect("send");

        let request = router.recv().await.expect("request");
        assert_eq!(body(&request)[0], b"worker-1".to_vec());

        let mut anonymous = DealerSocket::new(&ctx).expect("anonymous dealer");
        anonymous.connect(&endpoint.to_string()).expect("connect");
        anonymous
            .send("from the anonymous one")
            .await
            .expect("send");

        let request = router.recv().await.expect("request");
        let key = body(&request)[0].clone();
        assert_eq!(key[0], 0, "a generated routing id starts with a zero octet");
        assert_eq!(
            key.len(),
            5,
            "zero plus a 32-bit number, as libzmq shapes it"
        );

        // And the ROUTER can address either of them by what it was handed.
        let framed =
            Multipart::new(vec![Message::from(key), Message::from("hello")]).expect("frames");
        assert_eq!(router.send(framed).await.expect("reply"), Sent::Queued);
        assert_eq!(
            body(&anonymous.recv().await.expect("reply")),
            vec![b"hello".to_vec()]
        );
    }

    /// Claim: `ZMQ_ROUTER_MANDATORY` is the difference between a message
    /// disappearing and a caller being told. Unroutable is `EHOSTUNREACH`
    /// with it and a reported drop without it; a full queue is `EAGAIN` with
    /// it under `ZMQ_DONTWAIT`.
    #[tokio::test]
    async fn router_mandatory_replaces_the_silent_drop() {
        let ctx = context();
        let mut lenient = RouterSocket::new(&ctx).expect("router");
        let stranger = Multipart::new(vec![
            Message::from("nobody-here"),
            Message::from("into the void"),
        ])
        .expect("frames");
        assert_eq!(
            lenient.send(stranger.clone()).await.expect("dropped"),
            Sent::Dropped,
            "the default is the brutal one"
        );

        let mut strict = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                ..SocketOptions::default()
            },
        )
        .expect("router");
        let err = strict.send(stranger).await.unwrap_err();
        assert_eq!(err.errno(), "EHOSTUNREACH", "{err}");

        // At the high-water mark, under ZMQ_DONTWAIT, the same option says
        // EAGAIN instead.
        let mut full = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                pipe: PipeConfig {
                    outgoing: QueueConfig {
                        hwm: 1,
                        mute: MuteAction::Drop,
                    },
                    ..PipeConfig::default()
                },
                ..SocketOptions::default()
            },
        )
        .expect("router");
        let endpoint = full.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut dealer = DealerSocket::with_options(&ctx, dealer_options(b"slow")).expect("dealer");
        dealer.connect(&endpoint.to_string()).expect("connect");
        dealer.send("hello").await.expect("send");
        full.recv()
            .await
            .expect("the request, so the peer is known");

        // The queue holds one; the second try_send has nowhere to put its
        // message and says so rather than dropping it.
        let mut refused = 0;
        for _ in 0..8 {
            let framed =
                Multipart::new(vec![Message::from("slow"), Message::from("x")]).expect("frames");
            if let Err(e) = full.try_send(framed) {
                assert_eq!(e.errno(), "EAGAIN", "{e}");
                refused += 1;
            }
        }
        assert!(
            refused > 0,
            "a full queue under mandatory must report EAGAIN"
        );
    }

    /// Claim: two peers claiming one identity are resolved by
    /// `ZMQ_ROUTER_HANDOVER` — rejected by default, and with the option the
    /// newcomer takes the name and the incumbent is disconnected.
    #[tokio::test]
    async fn router_handover_decides_a_duplicate_identity() {
        let ctx = context();

        // Default: the newcomer is rejected, so the incumbent keeps the name.
        let mut strict = RouterSocket::new(&ctx).expect("router");
        let endpoint = strict.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut first = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: None,
                ..dealer_options(b"twin")
            },
        )
        .expect("first");
        first.connect(&endpoint.to_string()).expect("connect");
        first.send("i am the incumbent").await.expect("send");
        let request = strict.recv().await.expect("request");
        assert_eq!(body(&request)[0], b"twin".to_vec());

        let second = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: None,
                ..dealer_options(b"twin")
            },
        )
        .expect("second");
        second.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| strict.peers().len() == 1).await;
        // One name, one peer: the newcomer was evicted rather than admitted.
        assert_eq!(strict.peers(), vec![RoutingKey::from_wire(b"twin")]);
        let framed = Multipart::new(vec![Message::from("twin"), Message::from("still you")])
            .expect("frames");
        assert_eq!(strict.send(framed).await.expect("reply"), Sent::Queued);
        assert_eq!(
            body(&first.recv().await.expect("reply")),
            vec![b"still you".to_vec()],
            "the incumbent kept the name"
        );

        // With handover the answer is the other one.
        let mut handing_over = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_handover: true,
                ..SocketOptions::default()
            },
        )
        .expect("router");
        let endpoint = handing_over.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut incumbent = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: None,
                ..dealer_options(b"twin")
            },
        )
        .expect("incumbent");
        incumbent.connect(&endpoint.to_string()).expect("connect");
        incumbent.send("first here").await.expect("send");
        handing_over.recv().await.expect("request");

        let mut newcomer = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: None,
                ..dealer_options(b"twin")
            },
        )
        .expect("newcomer");
        newcomer.connect(&endpoint.to_string()).expect("connect");
        newcomer.send("mine now").await.expect("send");
        let request = handing_over.recv().await.expect("the newcomer's request");
        assert_eq!(body(&request)[1], b"mine now".to_vec());

        let framed =
            Multipart::new(vec![Message::from("twin"), Message::from("for you")]).expect("frames");
        assert_eq!(
            handing_over.send(framed).await.expect("reply"),
            Sent::Queued
        );
        assert_eq!(
            body(&newcomer.recv().await.expect("reply")),
            vec![b"for you".to_vec()],
            "the newcomer took the name"
        );
    }

    /// Claim: `ZMQ_PROBE_ROUTER` makes a new connection announce itself, so
    /// the ROUTER can address a peer that has said nothing of its own — the
    /// gap the option exists to close.
    #[tokio::test]
    async fn probe_router_announces_a_peer_before_it_speaks() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut dealer = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                probe_router: true,
                ..dealer_options(b"prober")
            },
        )
        .expect("dealer");
        dealer.connect(&endpoint.to_string()).expect("connect");

        // The application sent nothing. The probe is what arrives, and the
        // application is expected to filter it — an empty body.
        let probe = router.recv().await.expect("the probe");
        assert_eq!(body(&probe)[0], b"prober".to_vec());
        assert_eq!(probe.len(), 2);
        assert!(probe.frames()[1].is_empty(), "the probe carries nothing");

        // And the ROUTER can now speak first, which is the point.
        let framed = Multipart::new(vec![
            Message::from("prober"),
            Message::from("you are known"),
        ])
        .expect("frames");
        assert_eq!(router.send(framed).await.expect("send"), Sent::Queued);
        assert_eq!(
            body(&dealer.recv().await.expect("message")),
            vec![b"you are known".to_vec()]
        );
    }

    /// Claim: an option a socket type cannot honour is refused where it is
    /// set — `ZMQ_PROBE_ROUTER` on a socket that is not REQ, DEALER or
    /// ROUTER, and the ROUTER options on anything else.
    #[tokio::test]
    async fn an_option_the_socket_type_cannot_honour_is_refused() {
        let ctx = context();
        let err = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        let err = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                req_correlate: true,
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        // And the options each type *does* own are accepted.
        RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                router_handover: true,
                probe_router: true,
                ..SocketOptions::default()
            },
        )
        .expect("a ROUTER may have all three");
    }

    /// Claim: the routing table follows the peers rather than growing with
    /// whatever has ever connected — one entry per live peer, and a
    /// departure takes its entry with it. That is what bounds the table at
    /// `max_peers`, and it is the half a stranger could otherwise exploit by
    /// reconnecting under a new identity each time.
    #[tokio::test]
    async fn the_routing_table_follows_the_live_peers() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

        for name in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            let mut dealer = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    reconnect_ivl: None,
                    ..dealer_options(name)
                },
            )
            .expect("dealer");
            dealer.connect(&endpoint.to_string()).expect("connect");
            dealer.send("hello").await.expect("send");
            router.recv().await.expect("request");
            // Each peer is dropped immediately: three identities have been
            // seen, and none of them may still be in the table.
            drop(dealer);
        }

        wait_for(|| router.peers().is_empty()).await;
        assert!(
            router.by_peer.is_empty(),
            "both directions of the map must shrink with the peers"
        );
    }

    /// Claim: a ROUTER message that is only a routing id is refused rather
    /// than sent as an empty message — the frame is stripped, so what is
    /// left has to be a message.
    #[tokio::test]
    async fn a_router_message_needs_a_body() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let err = router
            .send(Multipart::single("just-an-id"))
            .await
            .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }
}
