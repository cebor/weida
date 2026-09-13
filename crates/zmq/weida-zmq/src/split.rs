//! The two halves of a socket whose directions are independent.
//!
//! Five socket types have a send side and a receive side that the
//! specification does not couple — DEALER, ROUTER, PAIR, XPUB and XSUB — so
//! a task parked in `recv` should not make a concurrent `send` on the same
//! socket wait behind it. Each of those types has a `split(self)` that hands
//! back a sending half and a receiving half, usable **at the same time from
//! two tasks**. REQ and REP are deliberately not here: 28/REQREP makes them
//! alternate, and a split would promise an independence the protocol
//! forbids.
//!
//! The halves are the same `SocketCore` twice
//! ([`SocketCore`] is a handle), so nothing is
//! implemented a second time: the socket types and their halves call the
//! same functions in this module, over the `Sync` inside of the core. The
//! engine — the connections, the queues, the peers — stays alive while
//! either half does and closes when the last one goes, exactly as a whole
//! socket closes when it is dropped.
//!
//! What a half is *not*: a way to share a socket between threads. Each half
//! is `Send + !Sync` like the socket it came from, so the thread rule holds
//! per half, and the two halves may live on two tasks and two threads
//! because the state they touch together is the engine's, which is `Sync`,
//! plus the routing table of a ROUTER, which is behind a lock that is never
//! held across an await.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::engine::{Engine, PeerId};
use crate::error::{Error, Result};
use crate::identity::RoutingKey;
use crate::message::{Message, Multipart};
use crate::pipe::Sent;
use crate::pubsub::Published;
use crate::socket::{Shared, SocketCore};
use crate::subscriptions::{self, Subscriptions};

// --- the bodies the socket types and their halves share ---------------------

/// A round-robin send that never discards: DEALER's and PAIR's.
pub(crate) async fn send_never_dropping(core: &Shared, message: Multipart) -> Result<()> {
    let limit = core.options().send_timeout;
    let exec = core.exec().clone();
    let delivered = match limit {
        None => core.send_round_robin(message).await?,
        Some(limit) => match exec.within(limit, core.send_round_robin(message)).await {
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
            "no peer took the message; this socket type never discards one".into(),
        ));
    }
    Ok(())
}

/// A round-robin send under an explicit bound.
pub(crate) async fn send_within(core: &Shared, message: Multipart, limit: Duration) -> Result<()> {
    let exec = core.exec().clone();
    match exec.within(limit, core.send_round_robin(message)).await {
        Some(result) => result.map(|_| ()),
        None => Err(Error::EAGAIN(
            format!("no peer took the message within {limit:?}").into(),
        )),
    }
}

/// A fair-queued receive bounded by `ZMQ_RCVTIMEO`.
pub(crate) async fn recv_fair(core: &Shared) -> Result<Multipart> {
    let limit = core.options().recv_timeout;
    let exec = core.exec().clone();
    match limit {
        None => core.recv_fair().await.map(|(_, message)| message),
        Some(limit) => match exec.within(limit, core.recv_fair()).await {
            Some(result) => result.map(|(_, message)| message),
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
            )),
        },
    }
}

/// A fair-queued receive under an explicit bound.
pub(crate) async fn recv_fair_within(core: &Shared, limit: Duration) -> Result<Multipart> {
    let exec = core.exec().clone();
    match exec.within(limit, core.recv_fair()).await {
        Some(result) => result.map(|(_, message)| message),
        None => Err(Error::EAGAIN(
            format!("nothing arrived within {limit:?}").into(),
        )),
    }
}

/// A fan-out to every peer with room, dropping for the rest: XPUB's
/// publish and XSUB's upstream send, with an optional match on the first
/// frame.
pub(crate) fn fan_out(core: &Shared, message: Multipart, matching: bool) -> Published {
    let topic = message.frames()[0].as_slice().to_vec();
    let mut report = Published {
        delivered: 0,
        dropped: 0,
        unmatched: 0,
    };
    for peer in core.peers() {
        if matching && !peer.subscriptions.matches(&topic) {
            report.unmatched += 1;
            continue;
        }
        let queue = peer.pipe.outgoing();
        if queue.has_room() && queue.try_send(message.clone()).is_ok() {
            report.delivered += 1;
        } else {
            report.dropped += 1;
        }
    }
    report
}

// --- DEALER -------------------------------------------------------------------

/// The sending half of a [`DealerSocket`][crate::DealerSocket].
#[derive(Debug)]
pub struct DealerSend {
    pub(crate) core: SocketCore,
}

/// The receiving half of a [`DealerSocket`][crate::DealerSocket].
#[derive(Debug)]
pub struct DealerRecv {
    pub(crate) core: SocketCore,
}

impl DealerSend {
    /// As [`DealerSocket::send`][crate::DealerSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        send_never_dropping(&self.core, message.into()).await
    }

    /// As [`DealerSocket::try_send`][crate::DealerSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// As [`DealerSocket::send_timeout`][crate::DealerSocket::send_timeout].
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        send_within(&self.core, message.into(), limit).await
    }
}

impl DealerRecv {
    /// As [`DealerSocket::recv`][crate::DealerSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`DealerSocket::try_recv`][crate::DealerSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`DealerSocket::recv_timeout`][crate::DealerSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

// --- PAIR ---------------------------------------------------------------------

/// The sending half of a [`PairSocket`][crate::PairSocket].
#[derive(Debug)]
pub struct PairSend {
    pub(crate) core: SocketCore,
}

/// The receiving half of a [`PairSocket`][crate::PairSocket].
#[derive(Debug)]
pub struct PairRecv {
    pub(crate) core: SocketCore,
}

impl PairSend {
    /// As [`PairSocket::send`][crate::PairSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        send_never_dropping(&self.core, message.into()).await
    }

    /// As [`PairSocket::try_send`][crate::PairSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// As [`PairSocket::send_timeout`][crate::PairSocket::send_timeout].
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        send_within(&self.core, message.into(), limit).await
    }
}

impl PairRecv {
    /// As [`PairSocket::recv`][crate::PairSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`PairSocket::try_recv`][crate::PairSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`PairSocket::recv_timeout`][crate::PairSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

// --- XSUB ---------------------------------------------------------------------

/// The sending half of an [`XSubSocket`][crate::XSubSocket]: subscriptions
/// and upstream messages.
#[derive(Debug)]
pub struct XSubSend {
    pub(crate) core: SocketCore,
    pub(crate) mine: Arc<Subscriptions>,
}

/// The receiving half of an [`XSubSocket`][crate::XSubSocket].
#[derive(Debug)]
pub struct XSubRecv {
    pub(crate) core: SocketCore,
}

impl XSubSend {
    /// As [`XSubSocket::subscribe`][crate::XSubSocket::subscribe].
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        xsub_subscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// As [`XSubSocket::unsubscribe`][crate::XSubSocket::unsubscribe].
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        xsub_unsubscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// As [`XSubSocket::subscriptions`][crate::XSubSocket::subscriptions].
    pub fn subscriptions(&self) -> Vec<Vec<u8>> {
        self.mine.prefixes()
    }

    /// As [`XSubSocket::send`][crate::XSubSocket::send].
    pub fn send(&mut self, message: impl Into<Multipart>) -> Published {
        fan_out(&self.core, message.into(), false)
    }
}

impl XSubRecv {
    /// As [`XSubSocket::recv`][crate::XSubSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`XSubSocket::try_recv`][crate::XSubSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`XSubSocket::recv_timeout`][crate::XSubSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

pub(crate) fn xsub_subscribe(core: &Shared, mine: &Subscriptions, prefix: &[u8]) -> Result<()> {
    if mine.subscribe(prefix).is_none() {
        return Err(Error::EINVAL(
            format!(
                "this socket already holds its ceiling of {} distinct subscriptions",
                mine.len()
            )
            .into(),
        ));
    }
    xsub_forward(core, subscriptions::write_message_form(true, prefix));
    Ok(())
}

pub(crate) fn xsub_unsubscribe(core: &Shared, mine: &Subscriptions, prefix: &[u8]) -> Result<()> {
    match mine.cancel(prefix) {
        None => Err(Error::EINVAL(
            "this socket is not subscribed to that prefix".into(),
        )),
        Some(_) => {
            xsub_forward(core, subscriptions::write_message_form(false, prefix));
            Ok(())
        }
    }
}

fn xsub_forward(core: &Shared, frame: Vec<u8>) {
    let message = Multipart::single(Message::from(frame));
    for peer in core.peers() {
        let _ = peer.pipe.outgoing().try_send(message.clone());
    }
}

// --- XPUB ---------------------------------------------------------------------

/// The publishing half of an [`XPubSocket`][crate::XPubSocket].
#[derive(Debug)]
pub struct XPubPublish {
    pub(crate) core: SocketCore,
}

/// The receiving half of an [`XPubSocket`][crate::XPubSocket]: the
/// subscriptions and messages subscribers send, and the three operations on
/// the subscriber that spoke last — `subscribe`, `unsubscribe`, `refuse` —
/// which belong here because "last" is what this half knows.
#[derive(Debug)]
pub struct XPubRecv {
    pub(crate) core: SocketCore,
    pub(crate) events: XPubEvents,
}

impl XPubPublish {
    /// As [`XPubSocket::publish`][crate::XPubSocket::publish].
    pub fn publish(&mut self, message: impl Into<Multipart>) -> Published {
        fan_out(&self.core, message.into(), true)
    }

    /// As [`XPubSocket::subscriber_count`][crate::XPubSocket::subscriber_count].
    pub fn subscriber_count(&self) -> usize {
        self.core.peers().len()
    }

    /// As [`XPubSocket::anybody_wants`][crate::XPubSocket::anybody_wants].
    pub fn anybody_wants(&self, topic: &[u8]) -> bool {
        self.core
            .peers()
            .iter()
            .any(|peer| peer.subscriptions.matches(topic))
    }
}

impl XPubRecv {
    /// As [`XPubSocket::recv`][crate::XPubSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        self.events.recv(&self.core).await
    }

    /// As [`XPubSocket::try_recv`][crate::XPubSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.events.try_recv(&self.core)
    }

    /// As [`XPubSocket::recv_timeout`][crate::XPubSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        self.events.recv_within(&self.core, limit).await
    }

    /// As [`XPubSocket::subscribe`][crate::XPubSocket::subscribe].
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.subscribe(&self.core, prefix.as_ref())
    }

    /// As [`XPubSocket::unsubscribe`][crate::XPubSocket::unsubscribe].
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.unsubscribe(&self.core, prefix.as_ref())
    }

    /// As [`XPubSocket::refuse`][crate::XPubSocket::refuse].
    pub fn refuse(&self, reason: &str) -> Result<()> {
        self.events.refuse(&self.core, reason)
    }
}

/// The receive-side memory of an XPUB: what each peer held, the
/// unsubscribes synthesized for peers that left, and who spoke last.
#[derive(Debug, Default)]
pub(crate) struct XPubEvents {
    /// What each peer was last seen holding, so that a departure can be
    /// turned into unsubscribes for the application.
    remembered: HashMap<PeerId, Vec<Vec<u8>>>,
    /// Unsubscribes synthesized for peers that have gone, waiting to be
    /// handed over.
    synthesized: VecDeque<Multipart>,
    /// The peer whose subscription was delivered most recently, which is what
    /// `ZMQ_XPUB_MANUAL`'s `subscribe` applies to — libzmq applies it to the
    /// last pipe too.
    last_subscriber: Option<PeerId>,
}

impl XPubEvents {
    pub(crate) async fn recv(&mut self, core: &Shared) -> Result<Multipart> {
        let limit = core.options().recv_timeout;
        let exec = core.exec().clone();
        match limit {
            None => self.next_event(core).await,
            Some(limit) => match exec.within(limit, self.next_event(core)).await {
                Some(result) => result,
                None => Err(Error::EAGAIN(
                    format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    pub(crate) fn try_recv(&mut self, core: &Shared) -> Result<Multipart> {
        self.reconcile(core);
        if let Some(synthesized) = self.synthesized.pop_front() {
            return Ok(synthesized);
        }
        let (peer, message) = core.try_recv_fair()?;
        self.note(core, peer, &message);
        Ok(message)
    }

    pub(crate) async fn recv_within(
        &mut self,
        core: &Shared,
        limit: Duration,
    ) -> Result<Multipart> {
        let exec = core.exec().clone();
        match exec.within(limit, self.next_event(core)).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?}").into(),
            )),
        }
    }

    pub(crate) fn subscribe(&self, core: &Shared, prefix: &[u8]) -> Result<()> {
        let peer = self.subscriber(core)?;
        if peer.subscriptions.subscribe(prefix).is_none() {
            return Err(Error::EINVAL(
                "that subscriber is at its subscription ceiling".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn unsubscribe(&self, core: &Shared, prefix: &[u8]) -> Result<()> {
        let peer = self.subscriber(core)?;
        match peer.subscriptions.cancel(prefix) {
            Some(_) => Ok(()),
            None => Err(Error::EINVAL(
                "that subscriber does not hold that subscription".into(),
            )),
        }
    }

    pub(crate) fn refuse(&self, core: &Shared, reason: &str) -> Result<()> {
        self.subscriber(core)?.pipe.refuse(reason);
        Ok(())
    }

    async fn next_event(&mut self, core: &Shared) -> Result<Multipart> {
        loop {
            self.reconcile(core);
            if let Some(synthesized) = self.synthesized.pop_front() {
                return Ok(synthesized);
            }
            if let Ok((peer, message)) = core.try_recv_fair() {
                self.note(core, peer, &message);
                return Ok(message);
            }
            // Nothing queued and nothing synthesized. An XPUB must wake on a
            // peer **going away** as well as on a message, because a
            // departure is an event it owes its application — so this waits
            // for either and loops rather than parking on a receive.
            core.wait_for_activity().await;
        }
    }

    fn subscriber(&self, core: &Shared) -> Result<crate::engine::Peer> {
        let peers = core.peers();
        let wanted = self.last_subscriber;
        peers
            .into_iter()
            .find(|peer| Some(peer.id) == wanted)
            .ok_or_else(|| {
                Error::EINVAL(
                    "no subscription has been delivered yet, so there is no subscriber to apply \
                     this to"
                        .into(),
                )
            })
    }

    /// Remembers what a peer holds, and which peer spoke last.
    fn note(&mut self, core: &Shared, peer: PeerId, message: &Multipart) {
        if message.len() == 1
            && subscriptions::read_message_form(message.frames()[0].as_slice()).is_some()
        {
            self.last_subscriber = Some(peer);
        }
        if let Some(current) = core.peers().into_iter().find(|current| current.id == peer) {
            self.remembered
                .insert(peer, current.subscriptions.prefixes());
        }
    }

    /// Turns departures into unsubscribes, and keeps the memory in step with
    /// the live peers so that it cannot grow past `max_peers` entries.
    fn reconcile(&mut self, core: &Shared) {
        let live = core.engine().peers();
        for peer in &live {
            if let Some(known) = self.remembered.get_mut(&peer.id) {
                let current = peer.subscriptions.prefixes();
                if !current.is_empty() {
                    *known = current;
                }
            } else {
                self.remembered
                    .insert(peer.id, peer.subscriptions.prefixes());
            }
        }
        let gone: Vec<PeerId> = self
            .remembered
            .keys()
            .copied()
            .filter(|id| !live.iter().any(|peer| peer.id == *id))
            .collect();
        for id in gone {
            let prefixes = self.remembered.remove(&id).unwrap_or_default();
            for prefix in prefixes {
                // "SHALL, if the subscriber peer disconnects prematurely,
                // generate a suitable unsubscribe request for the calling
                // application."
                self.synthesized.push_back(Multipart::single(Message::from(
                    subscriptions::write_message_form(false, &prefix),
                )));
            }
            if self.last_subscriber == Some(id) {
                self.last_subscriber = None;
            }
        }
    }
}

// --- ROUTER -------------------------------------------------------------------

/// The sending half of a [`RouterSocket`][crate::RouterSocket].
#[derive(Debug)]
pub struct RouterSend {
    pub(crate) core: SocketCore,
    pub(crate) routing: Arc<Mutex<Routing>>,
}

/// The receiving half of a [`RouterSocket`][crate::RouterSocket].
#[derive(Debug)]
pub struct RouterRecv {
    pub(crate) core: SocketCore,
    pub(crate) routing: Arc<Mutex<Routing>>,
}

impl RouterSend {
    /// As [`RouterSocket::send`][crate::RouterSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        router_send(&self.core, &self.routing, message.into()).await
    }

    /// As [`RouterSocket::try_send`][crate::RouterSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        router_try_send(&self.core, &self.routing, message.into())
    }

    /// As [`RouterSocket::peers`][crate::RouterSocket::peers].
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        lock(&self.routing).keys(&self.core)
    }
}

impl RouterRecv {
    /// As [`RouterSocket::recv`][crate::RouterSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        router_recv(&self.core, &self.routing).await
    }

    /// As [`RouterSocket::try_recv`][crate::RouterSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        router_try_recv(&self.core, &self.routing)
    }

    /// As [`RouterSocket::recv_timeout`][crate::RouterSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        router_recv_within(&self.core, &self.routing, limit).await
    }

    /// As [`RouterSocket::peers`][crate::RouterSocket::peers].
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        lock(&self.routing).keys(&self.core)
    }
}

/// A ROUTER's routing table: routing id to peer, and back.
///
/// Behind a `Mutex` only so that the two halves of a split ROUTER may hold
/// it together; the lock is taken for a reconciliation or a lookup and
/// never across an await. Bounded as the socket's doc says: one entry per
/// live peer, reconciled against the engine before every use.
#[derive(Debug)]
pub(crate) struct Routing {
    /// Two maps because both directions are hot: inbound needs the id of a
    /// peer, outbound the peer of an id.
    by_key: HashMap<RoutingKey, PeerId>,
    by_peer: HashMap<PeerId, RoutingKey>,
    /// The counter behind a generated routing id; see [`RoutingKey`].
    next_generated: u32,
}

impl Routing {
    pub(crate) fn new() -> Routing {
        Routing {
            by_key: HashMap::new(),
            by_peer: HashMap::new(),
            next_generated: 1,
        }
    }

    /// Entries in the table, both maps kept in step, for the test that pins
    /// its shrinking half.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        debug_assert_eq!(self.by_key.len(), self.by_peer.len());
        self.by_key.len()
    }

    pub(crate) fn keys(&mut self, core: &Shared) -> Vec<RoutingKey> {
        self.learn_peers(core);
        let mut keys: Vec<RoutingKey> = self.by_key.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub(crate) fn peer_of(&self, key: &RoutingKey) -> Option<PeerId> {
        self.by_key.get(key).copied()
    }

    /// Gives every current peer a routing id, and forgets the ones that left.
    ///
    /// This is also where a duplicate identity is resolved:
    /// `ZMQ_ROUTER_HANDOVER` decides whether the newcomer takes the name and
    /// the incumbent is disconnected, or the newcomer is rejected — libzmq's
    /// default being to reject it.
    pub(crate) fn learn_peers(&mut self, core: &Shared) {
        let engine: &Engine = core.engine();
        let live = engine.peers();
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
                None => self.next_key(),
            };
            if let Some(incumbent) = self.by_key.get(&key).copied() {
                if core.options().router_handover {
                    tracing::debug!(
                        key = ?key,
                        %incumbent,
                        "handing a routing id over to a newcomer (ZMQ_ROUTER_HANDOVER)"
                    );
                    engine.evict(incumbent);
                    self.by_peer.remove(&incumbent);
                } else {
                    tracing::warn!(
                        key = ?key,
                        %incumbent,
                        "rejected a peer claiming a routing id already in use"
                    );
                    engine.evict(peer.id);
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
    pub(crate) fn key_for(&mut self, core: &Shared, peer: PeerId) -> RoutingKey {
        if let Some(key) = self.by_peer.get(&peer) {
            return key.clone();
        }
        self.learn_peers(core);
        if let Some(key) = self.by_peer.get(&peer) {
            return key.clone();
        }
        let key = self.next_key();
        self.by_key.insert(key.clone(), peer);
        self.by_peer.insert(peer, key.clone());
        key
    }

    fn next_key(&mut self) -> RoutingKey {
        let key = RoutingKey::generated(self.next_generated);
        self.next_generated = self.next_generated.wrapping_add(1).max(1);
        key
    }
}

pub(crate) fn lock(routing: &Mutex<Routing>) -> std::sync::MutexGuard<'_, Routing> {
    routing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) async fn router_recv(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    let limit = core.options().recv_timeout;
    let exec = core.exec().clone();
    match limit {
        None => router_await(core, routing).await,
        Some(limit) => match exec.within(limit, router_await(core, routing)).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
            )),
        },
    }
}

pub(crate) async fn router_recv_within(
    core: &Shared,
    routing: &Mutex<Routing>,
    limit: Duration,
) -> Result<Multipart> {
    let exec = core.exec().clone();
    match exec.within(limit, router_await(core, routing)).await {
        Some(result) => result,
        None => Err(Error::EAGAIN(
            format!("nothing arrived within {limit:?}").into(),
        )),
    }
}

pub(crate) fn router_try_recv(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    lock(routing).learn_peers(core);
    let (peer, message) = core.try_recv_fair()?;
    Ok(with_routing_id(core, routing, peer, message))
}

async fn router_await(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    lock(routing).learn_peers(core);
    let (peer, message) = core.recv_fair().await?;
    Ok(with_routing_id(core, routing, peer, message))
}

fn with_routing_id(
    core: &Shared,
    routing: &Mutex<Routing>,
    peer: PeerId,
    message: Multipart,
) -> Multipart {
    let key = lock(routing).key_for(core, peer);
    let mut frames = Vec::with_capacity(message.len() + 1);
    frames.push(Message::from(key.as_bytes()));
    frames.extend(message.into_frames());
    Multipart::new(frames).expect("a routing id plus at least one frame")
}

pub(crate) async fn router_send(
    core: &Shared,
    routing: &Mutex<Routing>,
    message: Multipart,
) -> Result<Sent> {
    let (key, body) = split_routing_id(message)?;
    let mandatory = core.options().router_mandatory;
    let peer = {
        let mut table = lock(routing);
        table.learn_peers(core);
        table.peer_of(&key)
    };
    let Some(peer) = peer else {
        return unroutable(&key, mandatory);
    };
    let Some(pipe) = core.pipe_of(peer) else {
        return unroutable(&key, mandatory);
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
    let limit = core.options().send_timeout;
    let queued = crate::socket::within(core.exec(), limit, async {
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

pub(crate) fn router_try_send(
    core: &Shared,
    routing: &Mutex<Routing>,
    message: Multipart,
) -> Result<Sent> {
    let (key, body) = split_routing_id(message)?;
    let mandatory = core.options().router_mandatory;
    let peer = {
        let mut table = lock(routing);
        table.learn_peers(core);
        table.peer_of(&key)
    };
    let Some(peer) = peer else {
        return unroutable(&key, mandatory);
    };
    let Some(pipe) = core.pipe_of(peer) else {
        return unroutable(&key, mandatory);
    };
    let queue = pipe.outgoing();
    if queue.has_room() {
        queue.try_send(body)?;
        return Ok(Sent::Queued);
    }
    if mandatory {
        Err(Error::EAGAIN(
            "the peer's queue is at its high-water mark (ZMQ_ROUTER_MANDATORY)".into(),
        ))
    } else {
        Ok(Sent::Dropped)
    }
}

fn unroutable(key: &RoutingKey, mandatory: bool) -> Result<Sent> {
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

/// Takes the routing id off the front of an outbound message.
///
/// `EINVAL` when nothing would be left: a ROUTER strips that frame, and a
/// message that is only its address is no message.
pub(crate) fn split_routing_id(message: Multipart) -> Result<(RoutingKey, Multipart)> {
    let mut frames = message.into_frames();
    if frames.len() < 2 {
        return Err(Error::EINVAL(
            "a ROUTER message is a routing id followed by the message; this one has only the \
             routing id"
                .into(),
        ));
    }
    let key = RoutingKey::from_wire(frames.remove(0).as_slice());
    let body = Multipart::new(frames).expect("at least one frame remains");
    Ok((key, body))
}
