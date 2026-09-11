//! The per-peer queue — ZeroMQ's "pipe" — and what happens when it is full.
//!
//! **Every pattern RFC specifies the same object.** One queue per connected
//! peer, or a double queue, one per direction; created when an outgoing
//! connection is *initiated* and maintained "whether or not the connection is
//! established"; created when a peer connects; destroyed on that peer's
//! disconnect, "discarding any messages it contains"; with sizes constrained
//! "to a runtime-configurable limit" (`docs/research/zeromq.md` §2). That is
//! [`Pipe`], and the limit is `ZMQ_SNDHWM`/`ZMQ_RCVHWM`.
//!
//! **Credit is local and nobody grants it.** ZMTP 3.1 has no flow-control
//! signalling at all — credits are listed under "Topics for Discussion" — so
//! a high-water mark is purely a local bound on a local queue, applied per
//! peer, counted in *messages* rather than bytes (§5). It is also inexact in
//! libzmq: kernel socket buffers sit underneath it, "the actual limit may be
//! as much as 90% lower depending on the flow of messages", and for `inproc`
//! "the real HWM is the sum of both sides' configured HWMs" (§5, §11). What
//! is exact is the *behaviour at the bound*, which is what this module
//! implements and what its tests pin.
//!
//! **What happens at the bound is a property of the socket type**, from
//! `zmq_socket(3)`'s "Action in mute state" column: block, drop, or return
//! `EAGAIN` (§4.1). [`MuteAction::sending`] is that column, written once, so
//! that the socket types of the later slices carry it rather than each
//! re-deciding it at every send site.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use weida_zmtp::SocketType;

use crate::error::{Error, Result};
use crate::message::Multipart;

/// Default `ZMQ_SNDHWM`: outstanding messages queued for one peer.
///
/// libzmq's default, and libzmq's unit — messages, not bytes
/// (`docs/research/zeromq.md` §5, §11). Zero means no limit, which is what
/// ZeroMQ 2.x had by default and "was easy but also typically fatal for
/// high-volume publishers" (§11); it is available and it is not the default.
pub const DEFAULT_SNDHWM: usize = 1000;

/// Default `ZMQ_RCVHWM`: the same bound on the receiving direction.
pub const DEFAULT_RCVHWM: usize = 1000;

/// What a socket does when a peer's queue is full, or when it has no peer to
/// send to at all: `zmq_socket(3)`'s "Action in mute state".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MuteAction {
    /// Block until there is room. REQ, DEALER, PUSH, PULL, PAIR, CLIENT,
    /// CHANNEL and SCATTER, and the reason PUSH "SHALL NOT discard messages
    /// that it cannot send".
    Block,
    /// Discard the message silently and count it. PUB, XPUB, XSUB, ROUTER and
    /// RADIO: a publisher that blocked would let one slow subscriber stall
    /// every other one.
    Drop,
    /// Report `EAGAIN` and let the caller decide. SERVER, PEER and STREAM —
    /// the thread-safe family, which has no blocking send.
    Fail,
}

impl MuteAction {
    /// What `zmq_socket(3)`'s table says for `socket`, or `None` where the
    /// manual omits the row.
    ///
    /// The manual gives no mute-state action for REP, SUB, DISH or GATHER
    /// while giving one for PULL (`docs/research/zeromq.md` §4.1). That gap
    /// is reported rather than filled in: a socket type whose row is missing
    /// must state its own choice where it is implemented, and see
    /// [`MuteAction::receiving`] for the one receive-side rule the RFCs do
    /// state.
    pub const fn sending(socket: SocketType) -> Option<MuteAction> {
        match socket {
            SocketType::Req
            | SocketType::Dealer
            | SocketType::Push
            | SocketType::Pull
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Channel
            | SocketType::Scatter => Some(MuteAction::Block),
            SocketType::Pub
            | SocketType::XPub
            | SocketType::XSub
            | SocketType::Router
            | SocketType::Radio => Some(MuteAction::Drop),
            SocketType::Server | SocketType::Peer => Some(MuteAction::Fail),
            SocketType::Rep | SocketType::Sub | SocketType::Dish | SocketType::Gather => None,
        }
    }

    /// What a *receiving* socket does when one publisher's queue is full.
    ///
    /// Only the pub/sub RFCs state a receive-side rule: a SUB or XSUB "SHALL
    /// silently discard messages if the queue for a publisher is full"
    /// (29/PUBSUB, `docs/research/zeromq.md` §5). Every other socket type
    /// gets `None`, which means the RFCs are silent and the receiving side is
    /// backpressure — the queue simply stops being drained, which is how a
    /// PULL slows a PUSH down.
    pub const fn receiving(socket: SocketType) -> Option<MuteAction> {
        match socket {
            SocketType::Sub | SocketType::XSub => Some(MuteAction::Drop),
            _ => None,
        }
    }
}

/// How one direction of a peer's pipe is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// `ZMQ_SNDHWM`/`ZMQ_RCVHWM` for this direction, in messages. `0` means
    /// no limit, as in libzmq.
    pub hwm: usize,
    /// What to do at the bound.
    pub mute: MuteAction,
}

impl QueueConfig {
    /// The outgoing direction of a socket type, at libzmq's default HWM.
    ///
    /// Fails with `EINVAL` for a socket type whose mute-state row the manual
    /// omits: that choice belongs to the socket type, not to this
    /// constructor.
    pub const fn sending(socket: SocketType) -> Result<QueueConfig> {
        match MuteAction::sending(socket) {
            Some(mute) => Ok(QueueConfig {
                hwm: DEFAULT_SNDHWM,
                mute,
            }),
            None => Err(Error::EINVAL(std::borrow::Cow::Borrowed(
                "zmq_socket(3) states no mute-state action for this socket type; \
                 the socket must choose one explicitly",
            ))),
        }
    }
}

/// What a send did.
///
/// A drop is a return value rather than a silent `Ok`: `MuteAction::Drop` is
/// a documented outcome of `zmq_send` on a PUB socket, and a caller that
/// wants to count or log it must be able to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sent {
    /// The message is in the queue.
    Queued,
    /// The queue was full and this socket type drops rather than blocks. The
    /// message is gone; nothing will retry it.
    Dropped,
}

/// One direction of one peer's pipe: a bounded queue of whole messages.
///
/// Messages, not bytes, because that is libzmq's unit of credit (§5). Whole
/// messages, because a message is delivered "all frames or none" (§3), so a
/// queue slot always holds a complete one.
pub struct Queue {
    config: QueueConfig,
    state: Mutex<QueueState>,
    /// Woken when a message is taken out, for a blocked sender.
    room: Notify,
    /// Woken when a message goes in, for a waiting receiver.
    ready: Notify,
}

struct QueueState {
    messages: VecDeque<Multipart>,
    dropped: u64,
    closed: bool,
}

impl Queue {
    /// An empty queue under `config`.
    pub fn new(config: QueueConfig) -> Queue {
        Queue {
            config,
            state: Mutex::new(QueueState {
                messages: VecDeque::new(),
                dropped: 0,
                closed: false,
            }),
            room: Notify::new(),
            ready: Notify::new(),
        }
    }

    /// This queue's bound and its action at the bound.
    pub const fn config(&self) -> QueueConfig {
        self.config
    }

    /// Messages queued right now.
    pub fn len(&self) -> usize {
        self.lock().messages.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.lock().messages.is_empty()
    }

    /// Messages this queue has discarded at its bound, for a socket type
    /// that drops.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// Whether the peer's pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Whether one more message fits right now.
    ///
    /// For a socket type that round-robins, this is how "a peer is available
    /// only when it has an outgoing queue that is not full" is checked
    /// (28/REQREP's DEALER rule, `docs/research/zeromq.md` §4.2). A socket
    /// owns the producing end of its outgoing queues and is `!Sync`, so a
    /// `true` here cannot become false under it before it sends.
    pub fn has_room(&self) -> bool {
        let state = self.lock();
        !state.closed && !self.at_bound(&state)
    }

    /// Waits until [`Queue::has_room`] would be true, or the pipe is
    /// destroyed.
    ///
    /// For a sender choosing between peers: it waits on *this* queue, and a
    /// socket races one of these per peer so that whichever frees first wins.
    pub async fn wait_for_room(&self) {
        loop {
            let mut room = std::pin::pin!(self.room.notified());
            // `notified()` registers the waiter when it is **polled**, so
            // registering is `enable()` and not construction: without it a
            // `notify_waiters()` between the check below and the await wakes
            // nobody, and this wait never ends. The same at every wait in
            // this file, and the reason a reply could sit in a queue while
            // its session slept.
            room.as_mut().enable();
            if self.has_room() || self.is_closed() {
                return;
            }
            room.await;
        }
    }

    /// Waits until a message is queued, or the pipe is destroyed.
    ///
    /// The fair-queueing counterpart of [`Queue::wait_for_room`]: a receiver
    /// races one per peer and then takes from whichever answered.
    pub async fn wait_for_message(&self) {
        loop {
            let mut ready = std::pin::pin!(self.ready.notified());
            ready.as_mut().enable();
            {
                let state = self.lock();
                if !state.messages.is_empty() || state.closed {
                    return;
                }
            }
            ready.await;
        }
    }

    /// Queues `message`, applying this queue's mute action at the bound.
    ///
    /// - [`MuteAction::Block`] waits for room — the backpressure a PUSH or a
    ///   DEALER applies, and the reason neither discards.
    /// - [`MuteAction::Drop`] discards the message being sent, keeping what
    ///   is already queued, and counts it in [`Queue::dropped`].
    /// - [`MuteAction::Fail`] reports `EAGAIN`.
    ///
    /// Fails with `EHOSTUNREACH` once the pipe is destroyed: the peer is
    /// gone, and the RFCs destroy its queue with it.
    pub async fn send(&self, message: Multipart) -> Result<Sent> {
        loop {
            // Register before looking, so that room freed between the two
            // wakes this wait instead of being missed by it.
            let mut room = std::pin::pin!(self.room.notified());
            room.as_mut().enable();
            {
                let mut state = self.lock();
                if state.closed {
                    return Err(gone());
                }
                if !self.at_bound(&state) {
                    state.messages.push_back(message);
                    drop(state);
                    self.ready.notify_waiters();
                    return Ok(Sent::Queued);
                }
                match self.config.mute {
                    MuteAction::Drop => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    MuteAction::Fail => {
                        return Err(Error::EAGAIN(
                            format!(
                                "this peer's queue holds its high-water mark of {} messages",
                                self.config.hwm
                            )
                            .into(),
                        ));
                    }
                    MuteAction::Block => {}
                }
            }
            room.await;
        }
    }

    /// Queues `message` without ever waiting — `ZMQ_DONTWAIT`.
    ///
    /// Reports `EAGAIN` at the bound **whatever** the mute action is: a
    /// caller that asked not to block is asking for the refusal, not for a
    /// silent drop. A socket type that drops still drops on the blocking
    /// path, where the drop is its documented behaviour rather than the
    /// caller's choice.
    pub fn try_send(&self, message: Multipart) -> Result<()> {
        let mut state = self.lock();
        if state.closed {
            return Err(gone());
        }
        if self.at_bound(&state) {
            return Err(Error::EAGAIN(
                format!(
                    "this peer's queue holds its high-water mark of {} messages",
                    self.config.hwm
                )
                .into(),
            ));
        }
        state.messages.push_back(message);
        drop(state);
        self.ready.notify_waiters();
        Ok(())
    }

    /// Takes the next message, waiting for one.
    ///
    /// Fails with `EHOSTUNREACH` when the pipe is destroyed and drained: the
    /// peer is gone and nothing further will arrive on it.
    pub async fn recv(&self) -> Result<Multipart> {
        loop {
            let mut ready = std::pin::pin!(self.ready.notified());
            ready.as_mut().enable();
            {
                let mut state = self.lock();
                if let Some(message) = state.messages.pop_front() {
                    drop(state);
                    self.room.notify_waiters();
                    return Ok(message);
                }
                if state.closed {
                    return Err(gone());
                }
            }
            ready.await;
        }
    }

    /// Takes the next message if one is queued — `ZMQ_DONTWAIT`.
    pub fn try_recv(&self) -> Result<Multipart> {
        let mut state = self.lock();
        match state.messages.pop_front() {
            Some(message) => {
                drop(state);
                self.room.notify_waiters();
                Ok(message)
            }
            None if state.closed => Err(gone()),
            None => Err(Error::EAGAIN("nothing is queued from this peer".into())),
        }
    }

    /// Destroys the queue: discards what it holds and wakes everyone waiting
    /// on it.
    ///
    /// This is the pattern RFCs' rule for a disconnect — the double queue is
    /// destroyed, "discarding any messages it contains" (§2) — and it is why
    /// a ZeroMQ send that returned is not a delivery: the queue it went into
    /// may be thrown away with its peer.
    ///
    /// Returns how many messages were discarded.
    pub fn close(&self) -> usize {
        let discarded = {
            let mut state = self.lock();
            state.closed = true;
            let discarded = state.messages.len();
            state.messages.clear();
            discarded
        };
        self.room.notify_waiters();
        self.ready.notify_waiters();
        discarded
    }

    fn at_bound(&self, state: &QueueState) -> bool {
        self.config.hwm != 0 && state.messages.len() >= self.config.hwm
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state.lock().expect("queue poisoned")
    }
}

fn gone() -> Error {
    Error::EHOSTUNREACH(
        "this peer's pipe was destroyed, discarding what it held; the peer is gone".into(),
    )
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Queue")
            .field("hwm", &self.config.hwm)
            .field("mute", &self.config.mute)
            .field("queued", &state.messages.len())
            .field("dropped", &state.dropped)
            .field("closed", &state.closed)
            .finish()
    }
}

/// How both directions of one peer's pipe are bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipeConfig {
    /// The outgoing direction: `ZMQ_SNDHWM` and the socket's mute action.
    pub outgoing: QueueConfig,
    /// The incoming direction: `ZMQ_RCVHWM` and what a full receive queue
    /// does.
    pub incoming: QueueConfig,
}

impl Default for PipeConfig {
    /// libzmq's defaults with the blocking action, which is what every
    /// bidirectional pattern in `zmq_socket(3)`'s table except ROUTER does.
    fn default() -> PipeConfig {
        PipeConfig {
            outgoing: QueueConfig {
                hwm: DEFAULT_SNDHWM,
                mute: MuteAction::Block,
            },
            incoming: QueueConfig {
                hwm: DEFAULT_RCVHWM,
                mute: MuteAction::Block,
            },
        }
    }
}

/// One peer's double queue: outgoing and incoming, each bounded on its own.
///
/// Created when a connection is *initiated* rather than when it completes,
/// and maintained "whether or not the connection is established" (§2) — which
/// is why `ZMQ_IMMEDIATE` exists as an option to suppress exactly that, and
/// why a message can be queued for a peer that never answers. Cloning shares
/// the queues: the socket holds one clone and the connection engine the
/// other, which is the only way a queue can be a rendezvous between them.
#[derive(Clone, Debug)]
pub struct Pipe {
    outgoing: Arc<Queue>,
    incoming: Arc<Queue>,
}

impl Pipe {
    /// A fresh double queue under `config`.
    pub fn new(config: PipeConfig) -> Pipe {
        Pipe {
            outgoing: Arc::new(Queue::new(config.outgoing)),
            incoming: Arc::new(Queue::new(config.incoming)),
        }
    }

    /// The queue of messages headed for this peer.
    pub fn outgoing(&self) -> Arc<Queue> {
        Arc::clone(&self.outgoing)
    }

    /// The queue of messages received from this peer.
    pub fn incoming(&self) -> Arc<Queue> {
        Arc::clone(&self.incoming)
    }

    /// Destroys both queues, discarding what they hold: the peer
    /// disconnected. Returns the counts discarded, outgoing first.
    pub fn close(&self) -> (usize, usize) {
        (self.outgoing.close(), self.incoming.close())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn queue(hwm: usize, mute: MuteAction) -> Arc<Queue> {
        Arc::new(Queue::new(QueueConfig { hwm, mute }))
    }

    fn message(body: &str) -> Multipart {
        Multipart::single(body)
    }

    /// Claim: the mute-state column of `zmq_socket(3)`'s table, row for row,
    /// including the four rows the manual omits.
    #[test]
    fn the_mute_action_is_the_manuals_table() {
        for socket in [
            SocketType::Req,
            SocketType::Dealer,
            SocketType::Push,
            SocketType::Pull,
            SocketType::Pair,
            SocketType::Client,
            SocketType::Channel,
            SocketType::Scatter,
        ] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Block),
                "{socket:?}"
            );
        }
        for socket in [
            SocketType::Pub,
            SocketType::XPub,
            SocketType::XSub,
            SocketType::Router,
            SocketType::Radio,
        ] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Drop),
                "{socket:?}"
            );
        }
        for socket in [SocketType::Server, SocketType::Peer] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Fail),
                "{socket:?}"
            );
        }
        // The manual prints no action for these four, and the gap is reported
        // rather than guessed.
        for socket in [
            SocketType::Rep,
            SocketType::Sub,
            SocketType::Dish,
            SocketType::Gather,
        ] {
            assert_eq!(MuteAction::sending(socket), None, "{socket:?}");
            let err = QueueConfig::sending(socket).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{socket:?}: {err}");
        }

        // The one receive-side rule the RFCs do state.
        assert_eq!(
            MuteAction::receiving(SocketType::Sub),
            Some(MuteAction::Drop)
        );
        assert_eq!(
            MuteAction::receiving(SocketType::XSub),
            Some(MuteAction::Drop)
        );
        assert_eq!(MuteAction::receiving(SocketType::Pull), None);
    }

    /// Claim: at the bound, a blocking queue blocks — and the send completes
    /// as soon as a receive makes room. This is the backpressure PUSH,
    /// DEALER and PAIR apply, and the reason they never discard.
    #[tokio::test]
    async fn at_the_bound_a_blocking_queue_waits_for_room() {
        let q = queue(1, MuteAction::Block);
        assert_eq!(q.send(message("first")).await.expect("first"), Sent::Queued);

        let sender = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.send(message("second")).await })
        };
        // Margin: 50 ms for a send that must *not* complete. The
        // phenomenon is the absence of an event, so the interval only has to
        // exceed the time the send would take if the bound did not hold,
        // which is a queue push on the same thread — microseconds.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!sender.is_finished(), "the bound must hold the sender");
        assert_eq!(q.dropped(), 0, "a blocking queue discards nothing");

        // Draining one message is what lets it through.
        assert_eq!(q.recv().await.expect("first out"), message("first"));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), sender)
                .await
                .expect("the sender completed")
                .expect("the task")
                .expect("the send"),
            Sent::Queued
        );
        assert_eq!(q.recv().await.expect("second out"), message("second"));
    }

    /// Claim: at the bound, a dropping queue discards the message being sent,
    /// keeps what it already holds and counts the loss. PUB must not let one
    /// slow subscriber stall the others.
    #[tokio::test]
    async fn at_the_bound_a_dropping_queue_discards_the_new_message() {
        let q = queue(1, MuteAction::Drop);
        assert_eq!(q.send(message("kept")).await.expect("kept"), Sent::Queued);
        assert_eq!(
            q.send(message("lost")).await.expect("lost"),
            Sent::Dropped,
            "a drop is reported, not a silent Ok"
        );
        assert_eq!(q.dropped(), 1);
        assert_eq!(q.len(), 1);
        assert_eq!(
            q.recv().await.expect("out"),
            message("kept"),
            "the queued message must survive the drop"
        );
    }

    /// Claim: at the bound, a failing queue reports `EAGAIN` — the
    /// thread-safe family's answer, which has no blocking send.
    #[tokio::test]
    async fn at_the_bound_a_failing_queue_reports_eagain() {
        let q = queue(1, MuteAction::Fail);
        q.send(message("first")).await.expect("first");
        let err = q.send(message("second")).await.unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");
        assert_eq!(q.len(), 1);
        assert_eq!(q.dropped(), 0, "a refusal is not a drop");
    }

    /// Claim: `ZMQ_DONTWAIT` reports `EAGAIN` at the bound whatever the mute
    /// action is — including on a queue that would otherwise drop, because a
    /// caller who asked not to block asked for the refusal.
    #[tokio::test]
    async fn dontwait_reports_eagain_whatever_the_mute_action() {
        for mute in [MuteAction::Block, MuteAction::Drop, MuteAction::Fail] {
            let q = queue(1, mute);
            q.try_send(message("first")).expect("first");
            let err = q.try_send(message("second")).unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{mute:?}: {err}");
            assert_eq!(q.dropped(), 0, "{mute:?}: try_send never drops");
        }
    }

    /// Claim: a receive that finds nothing is `EAGAIN` without waiting, and
    /// the blocking one returns as soon as a message arrives.
    #[tokio::test]
    async fn a_receive_waits_and_dontwait_does_not() {
        let q = queue(DEFAULT_RCVHWM, MuteAction::Block);
        let err = q.try_recv().unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");

        let receiver = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.recv().await })
        };
        // Margin: 20 ms for a receive that must not complete yet; a
        // receive that wrongly returned would have done so in microseconds.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!receiver.is_finished());
        q.send(message("late")).await.expect("send");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receiver)
                .await
                .expect("the receiver completed")
                .expect("the task")
                .expect("the recv"),
            message("late")
        );
    }

    /// Claim: a high-water mark of zero is libzmq's "no limit" — the 2.x
    /// default, available and deliberately not ours.
    #[tokio::test]
    async fn a_zero_high_water_mark_is_no_limit() {
        let q = queue(0, MuteAction::Fail);
        for n in 0..2_000 {
            assert_eq!(
                q.send(message(&format!("{n}"))).await.expect("no limit"),
                Sent::Queued
            );
        }
        assert_eq!(q.len(), 2_000);
        assert_eq!(q.dropped(), 0);
    }

    /// Claim: destroying a peer's pipe discards what it held and unblocks
    /// everyone on it — the pattern RFCs' disconnect rule, and the reason a
    /// send that returned is not a delivery.
    #[tokio::test]
    async fn a_destroyed_pipe_discards_and_unblocks() {
        let q = queue(2, MuteAction::Block);
        q.send(message("one")).await.expect("one");
        q.send(message("two")).await.expect("two");

        let blocked = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.send(message("three")).await })
        };
        // Margin: as above — 20 ms against a would-be completion measured
        // in microseconds.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());

        assert_eq!(q.close(), 2, "both queued messages are discarded");
        let err = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("the blocked sender was woken")
            .expect("the task")
            .unwrap_err();
        assert_eq!(err.errno(), "EHOSTUNREACH", "{err}");

        assert!(q.is_closed());
        assert_eq!(q.len(), 0);
        assert_eq!(q.try_recv().unwrap_err().errno(), "EHOSTUNREACH");
        assert_eq!(q.recv().await.unwrap_err().errno(), "EHOSTUNREACH");
        assert_eq!(
            q.send(message("after")).await.unwrap_err().errno(),
            "EHOSTUNREACH"
        );
    }

    /// Claim: a pipe is two independent queues, so a full outgoing direction
    /// does not stop messages arriving from that peer — which is what
    /// "double queue, one per direction" buys.
    #[tokio::test]
    async fn the_two_directions_of_a_pipe_are_independent() {
        let pipe = Pipe::new(PipeConfig {
            outgoing: QueueConfig {
                hwm: 1,
                mute: MuteAction::Fail,
            },
            incoming: QueueConfig {
                hwm: 4,
                mute: MuteAction::Block,
            },
        });
        let out = pipe.outgoing();
        let inn = pipe.incoming();

        out.send(message("to the peer")).await.expect("outgoing");
        assert_eq!(
            out.send(message("over the bound"))
                .await
                .unwrap_err()
                .errno(),
            "EAGAIN"
        );

        inn.send(message("from the peer")).await.expect("incoming");
        assert_eq!(
            inn.recv().await.expect("delivered"),
            message("from the peer")
        );

        // Cloning a pipe shares the queues rather than copying them: that is
        // how the socket and the connection engine meet.
        let same = pipe.clone();
        assert_eq!(same.outgoing().len(), 1);

        assert_eq!(pipe.close(), (1, 0));
        assert!(same.incoming().is_closed());
    }

    /// Claim: a queue holds whole messages, so multipart survives the trip
    /// frame for frame and cannot be interleaved with another message.
    #[tokio::test]
    async fn a_queue_slot_holds_a_whole_message() {
        let q = queue(2, MuteAction::Block);
        let envelope = Multipart::new(vec![
            crate::message::Message::empty(),
            crate::message::Message::from("body"),
        ])
        .expect("frames");
        q.send(envelope.clone()).await.expect("send");
        q.send(Multipart::single("other")).await.expect("send");

        let first = q.recv().await.expect("first");
        assert_eq!(first, envelope);
        assert_eq!(first.len(), 2);
        assert_eq!(q.recv().await.expect("second"), Multipart::single("other"));
    }
}
