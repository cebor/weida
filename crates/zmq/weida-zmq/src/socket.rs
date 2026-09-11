//! What every socket type shares: the engine, the routing decisions and the
//! two thread rules.
//!
//! A socket type is a **pattern over the engine**. The engine binds,
//! connects, reconnects and holds one [`Pipe`] per peer; what a socket type
//! adds is the two decisions the pattern RFCs specify per type — *which pipe*
//! a message goes to (round-robin, fan-out, by routing id, the last peer) and
//! *what envelope* it carries. Both live in the socket type, and every bound
//! lives in the queue (`docs/research/zeromq.md` §4.1, §5).
//!
//! **`Send + !Sync`, and the type system says so.** libzmq: "Applications
//! MUST NOT use a *not* thread safe socket from multiple threads under any
//! circumstances. Doing so results in undefined behaviour" (§2). Every socket
//! type here holds a [`SocketCore`], which holds a `PhantomData<Cell<()>>`:
//! it may be moved between threads and it may not be shared between them, and
//! that is a compile error rather than a paragraph. The draft thread-safe
//! family, when it lands, is the family that *is* `Sync` and refuses
//! multipart — which is what the specification says it is.
//!
//! **No socket without a session.** [`SocketCore::new`] builds the
//! [`ZmtpSession`] for the socket type it is given, so a socket that speaks
//! nothing cannot be constructed
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4).

use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use weida_runtime::Exec;
use weida_zmtp::SocketType;

use crate::context::Context;
use crate::endpoint::Endpoint;
use crate::engine::{Discarded, Engine, Peer, PeerId, Session};
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::{MuteAction, Pipe, Queue, Sent};
use crate::session::ZmtpSession;

/// What a round-robin send did, and to whom.
///
/// The peer matters to a pattern with a memory: REQ "SHALL accept an
/// incoming message only from the last peer that it sent a request to", and
/// that is the peer this reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivered {
    /// Which peer took it, or `None` when the socket type dropped it.
    pub peer: Option<PeerId>,
    /// Queued, or discarded at the bound.
    pub sent: Sent,
}

/// Every public socket type, once, in the order the patterns were built.
///
/// `$apply` is a macro invoked with the whole list, which is how a caller
/// says something about all of them without naming them again — the
/// compile-fail harness of `tests/thread_rule.rs` makes its two assertions
/// per socket type this way.
///
/// **The list is load-bearing, not documentation.** Every socket type takes
/// its endpoint surface from the crate's `socket_endpoints!`, and that macro
/// refuses to expand for a type this list does not name, so a new socket
/// type cannot arrive without the thread rule being asserted for it.
#[macro_export]
macro_rules! for_each_socket_type {
    ($apply:ident) => {
        $apply! {
            ReqSocket, RepSocket, DealerSocket, RouterSocket, PubSocket, SubSocket, XPubSocket,
            XSubSocket, PushSocket, PullSocket, PairSocket
        }
    };
}

/// Implemented for exactly the types [`for_each_socket_type`] names.
///
/// Nothing is ever called through it: it exists so that the list and the
/// socket types cannot drift apart silently.
pub trait Enumerated {}

macro_rules! mark_enumerated {
    ($($socket:ident),+ $(,)?) => {
        $(impl $crate::socket::Enumerated for $crate::$socket {})+
    };
}

crate::for_each_socket_type!(mark_enumerated);

/// Gives a socket type the endpoint surface every socket type has.
///
/// `bind`, `connect`, `unbind`, `disconnect`, `ZMQ_LAST_ENDPOINT` and
/// `zmq_close` are identical for every pattern — "a socket may connect to
/// many endpoints and bind many at once" is a property of sockets, not of
/// REQ. Written once here so that five socket-type modules do not write it
/// five times, and so that adding one cannot forget a method.
///
/// It also refuses to expand for a socket type [`for_each_socket_type`] does
/// not name: a socket without endpoints is not a socket anybody can use, so
/// this is where a new one is made to join the list the thread-rule harness
/// reads.
macro_rules! socket_endpoints {
    ($socket:ty) => {
        socket_endpoints!($socket, no_connect);

        impl $socket {
            /// Connects an endpoint. Returns as soon as the peer exists, like
            /// `zmq_connect`: the queue is there and the dialling happens
            /// behind it.
            pub fn connect(&self, endpoint: &str) -> $crate::error::Result<()> {
                self.core.connect(endpoint)
            }
        }
    };
    // For the one socket type whose `connect` is its own — PAIR takes at
    // most one peer, so its second call is `EINVAL` — while everything else
    // about its endpoints is every socket's.
    ($socket:ty, no_connect) => {
        const _: () = {
            const fn enumerated<T: $crate::socket::Enumerated>() {}
            enumerated::<$socket>()
        };

        impl $socket {
            /// Binds an endpoint and returns the one actually bound, which is
            /// what `ZMQ_LAST_ENDPOINT` reports and the only way to learn a
            /// wildcard port.
            pub async fn bind(
                &self,
                endpoint: &str,
            ) -> $crate::error::Result<$crate::endpoint::Endpoint> {
                self.core.bind(endpoint).await
            }

            /// Stops accepting on an endpoint. Peers already accepted there
            /// keep their connections.
            pub fn unbind(&self, endpoint: &str) -> $crate::error::Result<()> {
                self.core.unbind(endpoint)
            }

            /// Disconnects an endpoint, destroying its queue and reporting
            /// what it held.
            pub fn disconnect(
                &self,
                endpoint: &str,
            ) -> $crate::error::Result<$crate::engine::Discarded> {
                self.core.disconnect(endpoint)
            }

            /// `ZMQ_LAST_ENDPOINT`: the last endpoint bound or connected.
            pub fn last_endpoint(&self) -> Option<$crate::endpoint::Endpoint> {
                self.core.last_endpoint()
            }

            /// Peers this socket may send to right now, honouring
            /// `ZMQ_IMMEDIATE`.
            pub fn peer_count(&self) -> usize {
                self.core.peers().len()
            }

            /// Every connection this socket has, connected or not, as a
            /// snapshot.
            ///
            /// Named for what it is rather than `peers`, which on a ROUTER is
            /// the routing table. A snapshot and not a handle: "application
            /// code cannot manipulate individual underlying connections"
            /// (`docs/research/zeromq.md` §2) — but it can read what the
            /// library knows about one, which for a local peer includes the
            /// credentials the kernel attributed to it
            /// ([`Peer::credentials`][$crate::engine::Peer::credentials]).
            pub fn connections(&self) -> Vec<$crate::engine::Peer> {
                self.core.engine().peers()
            }

            /// The socket type this socket announces in its `READY`.
            pub fn socket_type(&self) -> ::weida_zmtp::SocketType {
                self.core.socket_type()
            }

            /// `zmq_socket_monitor`: this socket's connection lifecycle as a
            /// typed stream, replacing any monitor installed before.
            ///
            /// [`crate::monitor::serve_pair`] renders the same stream onto a
            /// PAIR socket in libzmq's two-frame `inproc://` form, which is
            /// what the zguide's Espresso recipe reads.
            pub fn monitor(
                &self,
                events: $crate::monitor::MonitorEvents,
            ) -> $crate::monitor::Monitor {
                self.core.engine().monitor(events)
            }

            /// `zmq_close`: stops accepting and dialling, and destroys every
            /// queue.
            pub fn close(&self) {
                self.core.close();
            }
        }
    };
}

pub(crate) use socket_endpoints;

/// The half of a socket that is the same for every pattern.
pub struct SocketCore {
    engine: Engine,
    exec: Exec,
    socket_type: SocketType,
    /// Where the round-robin stands. A socket is `!Sync`, so this needs no
    /// synchronisation — which is half the point of the rule.
    cursor: usize,
    /// libzmq's thread rule, as a type: `Cell` is `Send` and not `Sync`, so
    /// this core — and every socket holding one — may cross threads and may
    /// not be shared across them.
    not_sync: PhantomData<Cell<()>>,
}

impl SocketCore {
    /// Creates the engine for a socket of `socket_type`, with its ZMTP
    /// session.
    pub fn new(
        context: &Context,
        socket_type: SocketType,
        options: SocketOptions,
    ) -> Result<SocketCore> {
        // Type-blind validation happens in `Engine::new`; this is the half
        // only a socket type can judge — an option that means nothing for
        // this pattern is refused rather than ignored (0013 §4.4 item 4).
        options.validate_for(socket_type)?;
        let session: Arc<dyn Session> = Arc::new(ZmtpSession::new(socket_type));
        let engine = Engine::new(context, options, session)?;
        Ok(SocketCore {
            exec: context.exec().clone(),
            engine,
            socket_type,
            cursor: 0,
            not_sync: PhantomData,
        })
    }

    /// Creates the engine for a socket of `socket_type` with a session the
    /// socket type built itself.
    ///
    /// A subscribing socket needs this: its session carries the
    /// subscription set, so that a reconnect re-sends it without the socket
    /// being told. The session is still a [`ZmtpSession`] — no socket can be
    /// built without one.
    pub fn with_session(
        context: &Context,
        socket_type: SocketType,
        options: SocketOptions,
        session: Arc<ZmtpSession>,
    ) -> Result<SocketCore> {
        options.validate_for(socket_type)?;
        let engine = Engine::new(context, options, session)?;
        Ok(SocketCore {
            exec: context.exec().clone(),
            engine,
            socket_type,
            cursor: 0,
            not_sync: PhantomData,
        })
    }

    /// The socket type this socket announces in its `READY`.
    pub const fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    /// The engine underneath, for a pattern that needs more than the helpers
    /// here.
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The reactor, for `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` and any clock a pattern
    /// keeps.
    pub const fn exec(&self) -> &Exec {
        &self.exec
    }

    /// The options this socket runs under.
    pub fn options(&self) -> &SocketOptions {
        self.engine.options()
    }

    /// Binds an endpoint and returns the one actually bound
    /// (`ZMQ_LAST_ENDPOINT`).
    pub async fn bind(&self, endpoint: &str) -> Result<Endpoint> {
        self.engine.bind(&Endpoint::parse(endpoint)?).await
    }

    /// Connects an endpoint, asynchronously, like `zmq_connect`.
    pub fn connect(&self, endpoint: &str) -> Result<()> {
        self.engine.connect(&Endpoint::parse(endpoint)?).map(|_| ())
    }

    /// Stops accepting on an endpoint.
    pub fn unbind(&self, endpoint: &str) -> Result<()> {
        self.engine.unbind(&Endpoint::parse(endpoint)?)
    }

    /// Disconnects an endpoint, destroying its pipe and reporting what it
    /// held.
    pub fn disconnect(&self, endpoint: &str) -> Result<Discarded> {
        self.engine.disconnect(&Endpoint::parse(endpoint)?)
    }

    /// The last endpoint bound or connected.
    pub fn last_endpoint(&self) -> Option<Endpoint> {
        self.engine.last_endpoint()
    }

    /// Peers this socket may send to right now, honouring `ZMQ_IMMEDIATE`.
    pub fn peers(&self) -> Vec<Peer> {
        self.engine.outgoing_peers()
    }

    /// Closes the socket: stops accepting and dialling, destroys every pipe.
    pub fn close(&self) {
        self.engine.close();
    }

    /// Sends to the next peer with room, round-robin, honouring the socket
    /// type's mute action when none has.
    ///
    /// "A peer is available only when it has an outgoing queue that is not
    /// full", and the rotation is over the available ones (28/REQREP's DEALER
    /// rule, which REQ and PUSH share). With [`MuteAction::Block`] this waits
    /// for room on *whichever* peer frees first, and for a peer at all when
    /// there is none — "SHALL block on sending… when it has no connected
    /// peers".
    pub async fn send_round_robin(&mut self, message: Multipart) -> Result<Delivered> {
        let mute = self.options().pipe.outgoing.mute;
        loop {
            let peers = self.peers();
            if let Some((index, queue)) = self.next_with_room(&peers) {
                self.cursor = index.wrapping_add(1);
                queue.try_send(message)?;
                return Ok(Delivered {
                    peer: Some(peers[index].id),
                    sent: Sent::Queued,
                });
            }
            match mute {
                MuteAction::Drop => {
                    return Ok(Delivered {
                        peer: None,
                        sent: Sent::Dropped,
                    });
                }
                MuteAction::Fail => return Err(mute_error(peers.is_empty())),
                MuteAction::Block => wait_for_room(&self.engine, &peers).await,
            }
        }
    }

    /// The `ZMQ_DONTWAIT` form: never waits, and reports `EAGAIN` when no
    /// peer has room — whatever the mute action, because a caller who asked
    /// not to block asked for the refusal.
    pub fn try_send_round_robin(&mut self, message: Multipart) -> Result<PeerId> {
        let peers = self.peers();
        let Some((index, queue)) = self.next_with_room(&peers) else {
            return Err(mute_error(peers.is_empty()));
        };
        self.cursor = index.wrapping_add(1);
        queue.try_send(message)?;
        Ok(peers[index].id)
    }

    /// Sends to one named peer: REP's originator, ROUTER's routing id.
    ///
    /// Reports `EHOSTUNREACH` when that peer is gone, which is what a socket
    /// type with `ZMQ_ROUTER_MANDATORY` surfaces and what one without turns
    /// into a drop.
    pub async fn send_to(&mut self, peer: PeerId, message: Multipart) -> Result<Sent> {
        let Some(pipe) = self.pipe_of(peer) else {
            return Err(Error::EHOSTUNREACH(
                format!("{peer} is gone; its queue was destroyed with it").into(),
            ));
        };
        pipe.outgoing().send(message).await
    }

    /// This peer's pipe, if it still exists.
    pub fn pipe_of(&self, peer: PeerId) -> Option<Pipe> {
        self.engine.peer(peer).map(|peer| peer.pipe)
    }

    /// Takes the next message from any peer, fair-queued.
    ///
    /// "Fair-queued" is the rotation the manual names for every receiving
    /// socket type: the cursor advances past whoever was served, so one busy
    /// peer cannot starve the others (§4.1).
    pub async fn recv_fair(&mut self) -> Result<(PeerId, Multipart)> {
        loop {
            if let Some(taken) = self.take_fair() {
                return Ok(taken);
            }
            let peers = self.engine.peers();
            wait_for_message(&self.engine, &peers).await;
        }
    }

    /// The `ZMQ_DONTWAIT` form of [`SocketCore::recv_fair`].
    pub fn try_recv_fair(&mut self) -> Result<(PeerId, Multipart)> {
        match self.take_fair() {
            Some(taken) => Ok(taken),
            None => Err(Error::EAGAIN("nothing is queued from any peer".into())),
        }
    }

    /// Takes the next message from `peer` only, **discarding** whatever
    /// arrives from anybody else.
    ///
    /// REQ's rule, in the specification's words: "SHALL accept an incoming
    /// message only from the last peer that it sent a request to. SHALL
    /// discard silently any messages received from other peers" (§4.2).
    pub async fn recv_from(&mut self, peer: PeerId) -> Result<Multipart> {
        loop {
            let peers = self.engine.peers();
            for other in &peers {
                if other.id == peer {
                    continue;
                }
                while let Ok(stray) = other.pipe.incoming().try_recv() {
                    tracing::debug!(
                        peer = %other.id,
                        frames = stray.len(),
                        "discarded a message from a peer this socket did not ask"
                    );
                }
            }
            let Some(pipe) = self.pipe_of(peer) else {
                return Err(Error::EHOSTUNREACH(
                    format!("{peer} is gone before it answered").into(),
                ));
            };
            if let Ok(message) = pipe.incoming().try_recv() {
                return Ok(message);
            }
            wait_for_message(&self.engine, &peers).await;
        }
    }

    /// Waits until a message could be waiting, or the peer set changed.
    ///
    /// For a socket type whose `recv` has more to report than messages — an
    /// XPUB turns a departure into an unsubscribe — so it must wake on both
    /// and decide for itself.
    pub async fn wait_for_activity(&mut self) {
        let peers = self.engine.peers();
        wait_for_message(&self.engine, &peers).await;
    }

    /// The next peer with room, starting at the cursor, and its index.
    fn next_with_room(&self, peers: &[Peer]) -> Option<(usize, Arc<Queue>)> {
        if peers.is_empty() {
            return None;
        }
        (0..peers.len()).find_map(|offset| {
            let index = (self.cursor.wrapping_add(offset)) % peers.len();
            let queue = peers[index].pipe.outgoing();
            queue.has_room().then_some((index, queue))
        })
    }

    /// One fair-queued take, advancing the cursor past whoever was served.
    fn take_fair(&mut self) -> Option<(PeerId, Multipart)> {
        let peers = self.engine.peers();
        if peers.is_empty() {
            return None;
        }
        for offset in 0..peers.len() {
            let index = (self.cursor.wrapping_add(offset)) % peers.len();
            if let Ok(message) = peers[index].pipe.incoming().try_recv() {
                self.cursor = index.wrapping_add(1);
                return Some((peers[index].id, message));
            }
        }
        None
    }
}

/// Waits for room on any of `peers`, or for the peer set to change.
///
/// A free function taking `&Engine` rather than a method taking `&self`, and
/// that is load-bearing: a socket is `!Sync`, so a future holding
/// `&SocketCore` is not `Send` and could not be moved into a task. Holding
/// `&Engine` — which *is* `Sync` — keeps every socket future `Send`, so
/// libzmq's rule comes out exactly right: a socket may be moved to another
/// thread and used there, and may not be shared between two.
async fn wait_for_room(engine: &Engine, peers: &[Peer]) {
    let queues: Vec<Arc<Queue>> = peers.iter().map(|peer| peer.pipe.outgoing()).collect();
    let waits: Vec<_> = queues.iter().map(|queue| queue.wait_for_room()).collect();
    first_of(waits, engine.wait_for_peer_change()).await;
}

/// Waits for a message from any of `peers`, or for the peer set to change.
async fn wait_for_message(engine: &Engine, peers: &[Peer]) {
    let queues: Vec<Arc<Queue>> = peers.iter().map(|peer| peer.pipe.incoming()).collect();
    let waits: Vec<_> = queues
        .iter()
        .map(|queue| queue.wait_for_message())
        .collect();
    first_of(waits, engine.wait_for_peer_change()).await;
}

/// Runs `future` under a wall-clock bound — `ZMQ_SNDTIMEO` and
/// `ZMQ_RCVTIMEO` — reporting `EAGAIN` when it expires, which is the errno
/// libzmq uses for both.
pub async fn within<T>(
    exec: &Exec,
    limit: Option<Duration>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match limit {
        None => future.await,
        Some(limit) => match exec.within(limit, future).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("the operation did not complete within {limit:?}").into(),
            )),
        },
    }
}

impl std::fmt::Debug for SocketCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketCore")
            .field("socket_type", &self.socket_type.as_str())
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

/// Why a send found nowhere to go.
fn mute_error(no_peers: bool) -> Error {
    if no_peers {
        Error::EAGAIN("this socket has no peer to send to".into())
    } else {
        Error::EAGAIN("every peer's queue is at its high-water mark".into())
    }
}

/// Resolves as soon as the first of `many` — or `one` — does.
///
/// `tokio::select!` cannot take a set whose size is a peer count, and pulling
/// in a combinator crate for twenty lines is worse than the twenty lines.
/// Every future here is one `Notify` wait, so polling them all on every wake
/// costs a peer-count pass and no allocation beyond the vector.
async fn first_of<A: Future<Output = ()>, B: Future<Output = ()>>(many: Vec<A>, one: B) {
    FirstOf {
        many: many.into_iter().map(Box::pin).collect(),
        one: Box::pin(one),
    }
    .await
}

struct FirstOf<A, B> {
    many: Vec<Pin<Box<A>>>,
    one: Pin<Box<B>>,
}

impl<A: Future<Output = ()>, B: Future<Output = ()>> Future for FirstOf<A, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        // `Self` is `Unpin` — every field is a `Pin<Box<_>>` — so the
        // projection needs no `unsafe`, which this crate forbids anyway.
        let this = self.get_mut();
        if this.one.as_mut().poll(cx).is_ready() {
            return Poll::Ready(());
        }
        for future in &mut this.many {
            if future.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;

    /// Claim: a socket may be **moved** between threads, which is what
    /// `Send` means and what a caller handing a socket to a task needs.
    ///
    /// The other half of libzmq's rule — a socket may not be *shared* — is
    /// `!Sync`, carried by `PhantomData<Cell<()>>` in [`SocketCore`]. There
    /// is no negative bound to write here, so it is asserted where a
    /// negative can be: `tests/thread_rule.rs` compiles a file that asks for
    /// `Sync` on every socket type and requires it to fail, with the
    /// expected error committed beside it.
    #[tokio::test]
    async fn a_socket_core_can_be_moved_between_threads() {
        fn assert_send<T: Send>(_: &T) {}
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let core = SocketCore::new(&ctx, SocketType::Req, SocketOptions::default()).expect("core");
        assert_send(&core);
        assert_eq!(core.socket_type(), SocketType::Req);

        // Moved, not shared: a task takes ownership and uses it there.
        let moved = tokio::spawn(async move { core.socket_type() })
            .await
            .expect("the task");
        assert_eq!(moved, SocketType::Req);
    }

    /// Claim: `within` turns an expired wait into `EAGAIN`, which is the
    /// errno `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO` report.
    #[tokio::test]
    async fn an_expired_timeout_is_eagain() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let core = SocketCore::new(&ctx, SocketType::Rep, SocketOptions::default()).expect("core");
        let err = within(
            core.exec(),
            Some(Duration::from_millis(5)),
            std::future::pending::<Result<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");

        // And a bound that does not expire is transparent.
        within(core.exec(), Some(Duration::from_secs(30)), async { Ok(()) })
            .await
            .expect("inside the bound");
    }
}
