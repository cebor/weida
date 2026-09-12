//! The per-pipe queues — `NNG_OPT_SENDBUF` and `NNG_OPT_RECVBUF` — and what
//! each protocol does when one is full.
//!
//! "NNG socket `SENDBUF` and `RECVBUF` are depths in **messages**, each
//! configurable from 0 through 8192, not byte credit"
//! (`docs/research/nanomsg-nng.md` §5). So a queue here holds whole
//! [`Message`]s and counts them, and the byte bound is a different
//! mechanism entirely: `NNG_OPT_RECVMAXSZ` bounds one message
//! ([`crate::message`]) and has nothing to do with how many fit.
//!
//! **Zero is not "unlimited" here; it is zero.** libzmq's high-water mark of
//! `0` means no limit, and NNG's buffer depth of `0` means no buffer: "A
//! full or zero-depth send path blocks until it can queue/hand off" (§5),
//! and PUSH ships with `SENDBUF=0` precisely so that a send waits for a
//! puller that can take it rather than piling up behind one that cannot
//! (§5, §4). A [`Queue`] of depth zero is therefore a rendezvous: a message
//! may be deposited only when a taker is already waiting for it. Getting
//! this backwards would turn PUSH's documented default into "no bound at
//! all", which is the opposite of what it says and a violation of
//! `docs/INVARIANTS.md` besides.
//!
//! **What happens at the bound is a property of the protocol**, not of the
//! call site: "PUSH/PAIR block, BUS drops, and SUB drops old or rejects new
//! locally" (§12/P4). [`FullAction`] is that sentence written once, and
//! [`FullAction::sending`]/[`FullAction::receiving`] are the per-protocol
//! table, so no socket type re-decides it at each send.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::Notify;
use weida_sp::EndpointType;

use crate::error::{Error, Result};
use crate::message::Message;

/// Deepest queue NNG admits, in messages: "each configurable from 0 through
/// 8192" (§5).
pub const MAX_QUEUE_DEPTH: usize = 8192;

/// Default `NNG_OPT_SENDBUF`, in messages.
///
/// Zero, which is the value NNG documents for PUSH (§5) and the value that
/// makes a send mean what SP says it means: the message is handed to a peer
/// that can take it now, or the sender waits. A positive depth is an
/// explicit decision to let messages pile up for a peer that is not keeping
/// up, and it is available up to [`MAX_QUEUE_DEPTH`].
pub const DEFAULT_SEND_DEPTH: usize = 0;

/// Default `NNG_OPT_RECVBUF`, in messages.
///
/// **NNG publishes no default for this one**, so the number is ours and is
/// named here rather than hidden. Zero would mean a message only moves off
/// the connection while the application is already parked in a receive,
/// which turns every gap between two `recv` calls into transport
/// backpressure and, for the protocols that drop rather than block —
/// PUB/SUB, BUS, SURVEY — into loss the sheet does not describe. 128 is deep
/// enough that an application doing work between receives does not stall its
/// peer, and shallow enough that a slow reader is bounded within one socket
/// rather than within one machine (`docs/INVARIANTS.md`).
pub const DEFAULT_RECV_DEPTH: usize = 128;

/// What a protocol does when the queue it is putting a message into is full.
///
/// The four values are the four behaviours the sheet names, and no socket
/// type invents a fifth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FullAction {
    /// Wait for room, and report `NNG_ETIMEDOUT` if a send timeout expires
    /// first. PUSH and PAIR: "PUSH/PAIR block" (§12/P4), "with no eligible
    /// peer, the send waits or times out" (§4).
    Block,
    /// Discard the message being sent, keep what is queued, and let the send
    /// succeed. BUS: "send is best-effort, nonblocking, and discards when a
    /// peer cannot receive" (§4); PUB and SURVEYOR broadcast the same way
    /// (§4, §5).
    Drop,
    /// Discard the **oldest** queued message to make room for the new one.
    /// SUB's default, `SUB_PREFNEW=true`: "the default policy removes its
    /// oldest queued message to make room" (§4).
    DropOldest,
    /// Refuse the **new** message, keeping what is queued.
    /// `SUB_PREFNEW=false`: "preserves old queued messages by rejecting the
    /// new message" (§4).
    RejectNewest,
}

impl FullAction {
    /// What `protocol` does at a full outgoing queue.
    ///
    /// Blocking is the rule and dropping is the named exception, which is
    /// the shape the sheet states it in: the general contract is "a full or
    /// zero-depth send path blocks until it can queue/hand off, unless the
    /// socket send timeout yields `NNG_ETIMEDOUT`" (§5), and BUS, PUB and
    /// SURVEYOR are the three that broadcast best-effort instead (§4).
    pub const fn sending(protocol: EndpointType) -> FullAction {
        match protocol {
            EndpointType::Bus | EndpointType::Pub | EndpointType::Surveyor => FullAction::Drop,
            _ => FullAction::Block,
        }
    }

    /// What `protocol` does at a full incoming queue.
    ///
    /// SUB is the only protocol with a documented receive-side policy, and
    /// it has two — [`FullAction::DropOldest`] by default and
    /// [`FullAction::RejectNewest`] under `SUB_PREFNEW=false` (§4). For
    /// every other protocol a full receive queue stops the pipe from
    /// reading, which is [`FullAction::Block`] and is how the backpressure
    /// reaches the peer at all: SP has no credit to withhold (§12/P12), so
    /// the only signal is not reading.
    pub const fn receiving(protocol: EndpointType) -> FullAction {
        match protocol {
            EndpointType::Sub => FullAction::DropOldest,
            _ => FullAction::Block,
        }
    }
}

/// One pipe's identity within one socket, stable for that pipe's lifetime.
///
/// NNG's `nng_pipe` is "an opaque, value-passed handle for one connection,
/// associated with exactly one creating dialer or listener and therefore one
/// socket" (§2). The number is also what a raw BUS puts in its header and
/// what a device pushes onto a tag stack, so it is 31 bits wide: the tag
/// stack's top bit is its terminator and belongs to the encoding
/// [rfc-reqrep §5].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipeId(u32);

impl PipeId {
    /// The largest pipe ID, which is what the tag stack's 31 bits allow.
    pub const MAX: u32 = 0x7fff_ffff;

    /// A pipe ID from a counter, masked into the 31 bits a tag stack has
    /// room for.
    pub const fn new(id: u32) -> PipeId {
        PipeId(id & PipeId::MAX)
    }

    /// The number, for a header, a log line or a routing table.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for PipeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipe {}", self.0)
    }
}

/// How one direction of one pipe is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// `NNG_OPT_SENDBUF` or `NNG_OPT_RECVBUF` for this direction, in
    /// messages, `0..=`[`MAX_QUEUE_DEPTH`]. Zero is a rendezvous, not
    /// "unlimited".
    pub depth: usize,
    /// What to do at the bound.
    pub full: FullAction,
}

impl QueueConfig {
    /// Refuses a depth NNG would refuse, where it is configured.
    pub fn validate(&self, option: &str) -> Result<()> {
        if self.depth > MAX_QUEUE_DEPTH {
            return Err(Error::EINVAL(
                format!(
                    "{option} is 0..={MAX_QUEUE_DEPTH} messages; {} is out of range",
                    self.depth
                )
                .into(),
            ));
        }
        Ok(())
    }
}

/// What a send did.
///
/// A drop is a return value rather than a silent `Ok`, because for BUS, PUB
/// and SURVEYOR it is the documented outcome and a caller that wants to
/// count or log it must be able to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sent {
    /// The message is queued for the peer.
    Queued,
    /// The queue was full and this protocol discards rather than waits. The
    /// message is gone and nothing will retry it.
    Dropped,
}

/// One direction of one pipe: a bounded queue of whole messages.
pub struct Queue {
    config: QueueConfig,
    state: Mutex<QueueState>,
    /// Woken when a message is taken out, or when a taker parks — both are
    /// "there may be room now" for a depth-zero rendezvous.
    room: Notify,
    /// Woken when a message goes in.
    ready: Notify,
}

struct QueueState {
    messages: VecDeque<Message>,
    /// Takers currently parked in [`Queue::recv`]. Only a depth-zero queue
    /// consults it, and for that queue it *is* the room: a rendezvous
    /// accepts a message exactly when somebody is already waiting for one.
    waiting: usize,
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
                waiting: 0,
                dropped: 0,
                closed: false,
            }),
            room: Notify::new(),
            ready: Notify::new(),
        }
    }

    /// This queue's depth and its action at the bound.
    pub const fn config(&self) -> QueueConfig {
        self.config
    }

    /// Messages queued right now.
    pub fn len(&self) -> usize {
        self.lock().messages.len()
    }

    /// Whether the queue holds nothing.
    pub fn is_empty(&self) -> bool {
        self.lock().messages.is_empty()
    }

    /// Messages this queue discarded at its bound.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// Whether the pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Whether one more message fits **right now**.
    ///
    /// This is the eligibility test PUSH round-robins over: "PUSH selects
    /// only a puller capable of accepting a message, so its rotation is over
    /// the ready subset rather than a static worker list" (§4). For a
    /// depth-zero queue it is true only while a taker is parked, which is
    /// exactly what "capable of accepting" means with no buffer.
    pub fn has_room(&self) -> bool {
        let state = self.lock();
        !state.closed && self.accepts(&state)
    }

    /// Waits until [`Queue::has_room`] would be true, or the pipe is
    /// destroyed.
    pub async fn wait_for_room(&self) {
        loop {
            let mut room = std::pin::pin!(self.room.notified());
            // `notified()` registers the waiter when it is **polled**, so
            // registering is `enable()` and not construction: without it,
            // room freed between the check below and the await wakes nobody
            // and this wait never ends.
            room.as_mut().enable();
            if self.has_room() || self.is_closed() {
                return;
            }
            room.await;
        }
    }

    /// Waits until a message is queued, or the pipe is destroyed.
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

    /// Queues `message`, applying this queue's [`FullAction`] at the bound.
    ///
    /// Fails with `NNG_ECLOSED` once the pipe is destroyed: the peer is gone
    /// and its queue went with it.
    pub async fn send(&self, message: Message) -> Result<Sent> {
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
                if self.accepts(&state) {
                    state.messages.push_back(message);
                    drop(state);
                    self.ready.notify_waiters();
                    return Ok(Sent::Queued);
                }
                match self.config.full {
                    FullAction::Drop => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    FullAction::DropOldest => {
                        // A depth-zero queue has no oldest to drop, and
                        // dropping the new one is the only way to make the
                        // policy mean anything there.
                        if state.messages.pop_front().is_some() {
                            state.dropped += 1;
                            state.messages.push_back(message);
                            drop(state);
                            self.ready.notify_waiters();
                            self.room.notify_waiters();
                            return Ok(Sent::Queued);
                        }
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    FullAction::RejectNewest => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    // Blocking is the one action that keeps the message:
                    // the loop waits for room and offers it again.
                    FullAction::Block => {}
                }
            }
            room.await;
        }
    }

    /// Queues `message` without ever waiting.
    ///
    /// Reports `NNG_EAGAIN` at the bound **whatever** the action is: a
    /// caller that asked not to block is asking for the refusal, not for a
    /// silent drop. A protocol that drops still drops on the waiting path,
    /// where the drop is its documented behaviour rather than the caller's
    /// choice.
    pub fn try_send(&self, message: Message) -> Result<()> {
        let closed = self.is_closed();
        match self.offer(message) {
            Ok(()) => Ok(()),
            Err(_) if closed => Err(gone()),
            Err(_) => Err(Error::EAGAIN(self.full_cause())),
        }
    }

    /// Queues `message` if there is room **right now**, and hands it back
    /// if there is not.
    ///
    /// The primitive a round-robin needs: a sender that has just seen
    /// [`Queue::has_room`] may still lose the last slot to another thread —
    /// an NNG socket is usable from several at once (§2) — and a send that
    /// swallowed the message on that race would lose it. A closed queue
    /// hands it back too, so a caller rotating over pipes skips a dead one
    /// and keeps its message.
    pub fn offer(&self, message: Message) -> std::result::Result<(), Message> {
        let mut state = self.lock();
        if state.closed || !self.accepts(&state) {
            return Err(message);
        }
        state.messages.push_back(message);
        drop(state);
        self.ready.notify_waiters();
        Ok(())
    }

    /// Takes the next message, waiting for one.
    ///
    /// Parking here is also what makes a depth-zero queue accept anything at
    /// all, so the waiter count is published to senders before the wait.
    pub async fn recv(&self) -> Result<Message> {
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
                state.waiting += 1;
            }
            self.room.notify_waiters();
            let parked = Parked(self);
            ready.await;
            drop(parked);
        }
    }

    /// Takes the next message if one is queued.
    pub fn try_recv(&self) -> Result<Message> {
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
    /// "A pipe is removed when its peer, owning dialer/listener, or
    /// `nng_pipe_close()` closes it" and "communication over that pipe is
    /// then impossible" (§1). Returns how many messages were discarded,
    /// because SP reports nothing to anybody and a count in a log is the
    /// only trace there will be.
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

    fn accepts(&self, state: &QueueState) -> bool {
        if self.config.depth == 0 {
            // A rendezvous: room exists only while somebody is waiting for a
            // message, and only for as many messages as there are waiters.
            state.waiting > state.messages.len()
        } else {
            state.messages.len() < self.config.depth
        }
    }

    fn full_cause(&self) -> crate::error::Cause {
        if self.config.depth == 0 {
            "no peer is ready to take a message and this queue has no buffer (depth 0)".into()
        } else {
            format!(
                "this pipe's queue holds its depth of {} messages",
                self.config.depth
            )
            .into()
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().expect("queue poisoned")
    }
}

/// How both directions of one pipe are bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipeConfig {
    /// `NNG_OPT_SENDBUF` and the protocol's action at its bound.
    pub outgoing: QueueConfig,
    /// `NNG_OPT_RECVBUF` and the protocol's action at its bound.
    pub incoming: QueueConfig,
}

impl PipeConfig {
    /// The defaults of `protocol`: NNG's depths and NNG's behaviour at the
    /// bound, so that a socket type that says nothing gets the protocol's
    /// own answer rather than a house one.
    pub const fn of(protocol: EndpointType) -> PipeConfig {
        PipeConfig {
            outgoing: QueueConfig {
                depth: DEFAULT_SEND_DEPTH,
                full: FullAction::sending(protocol),
            },
            incoming: QueueConfig {
                depth: DEFAULT_RECV_DEPTH,
                full: FullAction::receiving(protocol),
            },
        }
    }
}

/// One connection's double queue, outgoing and incoming, each bounded on
/// its own.
///
/// A pipe exists **only while its connection does**: "endpoints create
/// pipes, which are message-oriented connected streams and commonly map 1:1
/// to TCP or IPC connections" (§1), and a pipe "is removed when its peer,
/// owning dialer/listener, or `nng_pipe_close()` closes it" (§1). That is
/// the opposite of ZMTP's rule, where a queue is created when a connection
/// is *initiated* and survives reconnects, and it is why nothing here can
/// be queued for a peer that has not arrived.
///
/// Cloning shares the queues: the socket holds one clone and the session
/// driving the connection holds the other, which is the only way a queue
/// can be a rendezvous between them.
#[derive(Clone, Debug)]
pub struct Pipe {
    id: PipeId,
    outgoing: std::sync::Arc<Queue>,
    incoming: std::sync::Arc<Queue>,
}

impl Pipe {
    /// A pipe with both queues empty.
    pub fn new(id: PipeId, config: PipeConfig) -> Pipe {
        Pipe {
            id,
            outgoing: std::sync::Arc::new(Queue::new(config.outgoing)),
            incoming: std::sync::Arc::new(Queue::new(config.incoming)),
        }
    }

    /// This pipe's id within its socket.
    pub const fn id(&self) -> PipeId {
        self.id
    }

    /// Messages on their way to the peer.
    pub fn outgoing(&self) -> &Queue {
        &self.outgoing
    }

    /// Messages that arrived from the peer.
    pub fn incoming(&self) -> &Queue {
        &self.incoming
    }

    /// Destroys both queues and reports what they held, which is the only
    /// trace a discarded message leaves: SP tells nobody anything (§6).
    pub fn close(&self) -> Discarded {
        Discarded {
            outgoing: self.outgoing.close(),
            incoming: self.incoming.close(),
        }
    }

    /// Whether this pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.outgoing.is_closed()
    }
}

/// What destroying a pipe discarded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Discarded {
    /// Messages queued for the peer that will never be sent.
    pub outgoing: usize,
    /// Messages received from the peer that will never be delivered.
    pub incoming: usize,
}

/// Decrements the parked-taker count however [`Queue::recv`]'s await ends,
/// cancellation included: a cancelled receive that left its count behind
/// would make a depth-zero queue accept a message nobody is waiting for.
struct Parked<'a>(&'a Queue);

impl Drop for Parked<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.waiting = state.waiting.saturating_sub(1);
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Queue")
            .field("depth", &self.config.depth)
            .field("full", &self.config.full)
            .field("queued", &state.messages.len())
            .field("dropped", &state.dropped)
            .field("closed", &state.closed)
            .finish()
    }
}

fn gone() -> Error {
    Error::ECLOSED("this pipe is closed; its queue went with it".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(depth: usize, full: FullAction) -> QueueConfig {
        QueueConfig { depth, full }
    }

    fn msg(n: u8) -> Message {
        Message::from_body(vec![n])
    }

    /// Claim: the per-protocol table is the sheet's sentence — PUSH and PAIR
    /// block, BUS drops, SUB drops the oldest — and every protocol has an
    /// answer rather than a default nobody chose.
    #[test]
    fn the_full_queue_action_is_the_protocols_own() {
        assert_eq!(FullAction::sending(EndpointType::Push), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::PairV0), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::PairV1), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::Req), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::Bus), FullAction::Drop);
        assert_eq!(FullAction::sending(EndpointType::Pub), FullAction::Drop);
        assert_eq!(
            FullAction::sending(EndpointType::Surveyor),
            FullAction::Drop
        );
        assert_eq!(
            FullAction::receiving(EndpointType::Sub),
            FullAction::DropOldest
        );
        assert_eq!(FullAction::receiving(EndpointType::Pull), FullAction::Block);
    }

    /// Claim: a depth-zero queue is a rendezvous and not an unbounded one.
    /// This is PUSH's documented default, and getting it backwards would
    /// turn "no buffer" into "no bound".
    #[test]
    fn depth_zero_is_a_rendezvous() {
        let queue = Queue::new(config(0, FullAction::Block));
        assert!(!queue.has_room(), "nobody is waiting, so nothing fits");
        assert!(matches!(queue.try_send(msg(1)), Err(Error::EAGAIN(_))));
        assert_eq!(queue.len(), 0);

        futures::executor::block_on(async {
            let taking = queue.recv();
            let mut taking = std::pin::pin!(taking);
            // Poll once so the taker is parked and its room is published.
            assert!(
                futures::poll!(taking.as_mut()).is_pending(),
                "nothing is queued yet"
            );
            assert!(queue.has_room(), "a parked taker is the room");
            queue.try_send(msg(7)).expect("the rendezvous accepts one");
            assert!(
                queue.try_send(msg(8)).is_err(),
                "one waiter is room for one message"
            );
            assert_eq!(taking.await.expect("delivered").body(), [7]);
        });
    }

    /// Claim: a blocking queue makes the sender wait rather than discard,
    /// and the wait ends when a taker frees a slot. This is PUSH and PAIR.
    #[test]
    fn a_blocking_queue_waits_for_room() {
        let queue = Queue::new(config(1, FullAction::Block));
        futures::executor::block_on(async {
            assert_eq!(queue.send(msg(1)).await.expect("first"), Sent::Queued);

            let second = queue.send(msg(2));
            let mut second = std::pin::pin!(second);
            assert!(
                futures::poll!(second.as_mut()).is_pending(),
                "the queue is full"
            );
            assert_eq!(queue.dropped(), 0, "a blocking queue discards nothing");

            assert_eq!(queue.recv().await.expect("first out").body(), [1]);
            assert_eq!(second.await.expect("second in"), Sent::Queued);
            assert_eq!(queue.recv().await.expect("second out").body(), [2]);
        });
    }

    /// Claim: a dropping queue keeps what it has, discards the new message,
    /// counts it, and still reports success — BUS "discards when a peer
    /// cannot receive" while "the originating send succeeds without
    /// blocking" (§4).
    #[test]
    fn a_dropping_queue_discards_the_new_message_and_succeeds() {
        let queue = Queue::new(config(1, FullAction::Drop));
        futures::executor::block_on(async {
            assert_eq!(queue.send(msg(1)).await.expect("first"), Sent::Queued);
            assert_eq!(queue.send(msg(2)).await.expect("second"), Sent::Dropped);
            assert_eq!(queue.dropped(), 1);
            assert_eq!(queue.len(), 1);
            assert_eq!(
                queue.recv().await.expect("what survived").body(),
                [1],
                "the queued message is the one that was kept"
            );
        });
    }

    /// Claim: `SUB_PREFNEW=true` drops the oldest to make room, which is the
    /// opposite choice from the same full queue, and it is observable in
    /// what comes out.
    #[test]
    fn dropping_the_oldest_keeps_the_newest() {
        let queue = Queue::new(config(2, FullAction::DropOldest));
        futures::executor::block_on(async {
            for n in 1..=4 {
                assert_eq!(queue.send(msg(n)).await.expect("sent"), Sent::Queued);
            }
            assert_eq!(queue.dropped(), 2);
            assert_eq!(queue.recv().await.expect("oldest survivor").body(), [3]);
            assert_eq!(queue.recv().await.expect("newest").body(), [4]);
        });
    }

    /// Claim: `SUB_PREFNEW=false` rejects the new message instead, so the
    /// two policies differ in what a reader sees and not merely in a
    /// counter.
    #[test]
    fn rejecting_the_newest_keeps_the_oldest() {
        let queue = Queue::new(config(2, FullAction::RejectNewest));
        futures::executor::block_on(async {
            for n in 1..=4 {
                let sent = queue.send(msg(n)).await.expect("sent");
                assert_eq!(sent, if n <= 2 { Sent::Queued } else { Sent::Dropped });
            }
            assert_eq!(queue.dropped(), 2);
            assert_eq!(queue.recv().await.expect("first").body(), [1]);
            assert_eq!(queue.recv().await.expect("second").body(), [2]);
        });
    }

    /// Claim: a non-blocking send refuses at the bound whatever the
    /// protocol's action is — the caller asked for the refusal — and a
    /// non-blocking receive refuses an empty queue.
    #[test]
    fn the_non_blocking_forms_refuse_rather_than_drop() {
        for action in [
            FullAction::Block,
            FullAction::Drop,
            FullAction::DropOldest,
            FullAction::RejectNewest,
        ] {
            let queue = Queue::new(config(1, action));
            queue.try_send(msg(1)).expect("room for one");
            let err = queue.try_send(msg(2)).unwrap_err();
            assert!(matches!(err, Error::EAGAIN(_)), "{action:?}: {err:?}");
            assert_eq!(queue.dropped(), 0, "{action:?} discarded on the try path");
            assert_eq!(queue.try_recv().expect("the queued one").body(), [1]);
            assert!(matches!(queue.try_recv(), Err(Error::EAGAIN(_))));
        }
    }

    /// Claim: closing a pipe discards what its queue held, reports how much,
    /// and makes every further operation `NNG_ECLOSED` — including one that
    /// was already waiting, which is how a parked receive is unblocked when
    /// a peer goes away.
    #[test]
    fn closing_a_queue_discards_and_unblocks() {
        let queue = Queue::new(config(4, FullAction::Block));
        futures::executor::block_on(async {
            queue.send(msg(1)).await.expect("first");
            queue.send(msg(2)).await.expect("second");

            let waiting = Queue::new(config(1, FullAction::Block));
            let parked = waiting.recv();
            let mut parked = std::pin::pin!(parked);
            assert!(futures::poll!(parked.as_mut()).is_pending());
            assert_eq!(waiting.close(), 0);
            assert!(matches!(parked.await, Err(Error::ECLOSED(_))));

            assert_eq!(queue.close(), 2);
            assert!(queue.is_closed());
            assert!(matches!(queue.try_send(msg(3)), Err(Error::ECLOSED(_))));
            assert!(matches!(queue.send(msg(3)).await, Err(Error::ECLOSED(_))));
            assert!(matches!(queue.try_recv(), Err(Error::ECLOSED(_))));
        });
    }

    /// Claim: a depth NNG refuses is refused here, at configuration time,
    /// with the option named — never silently clamped.
    #[test]
    fn a_depth_out_of_nngs_range_is_refused_where_it_is_configured() {
        assert!(
            config(MAX_QUEUE_DEPTH, FullAction::Block)
                .validate("NNG_OPT_SENDBUF")
                .is_ok()
        );
        assert!(
            config(0, FullAction::Block)
                .validate("NNG_OPT_SENDBUF")
                .is_ok()
        );
        let err = config(MAX_QUEUE_DEPTH + 1, FullAction::Block)
            .validate("NNG_OPT_RECVBUF")
            .unwrap_err();
        assert!(matches!(err, Error::EINVAL(_)), "{err:?}");
        assert!(err.cause().contains("NNG_OPT_RECVBUF"));
        assert!(err.cause().contains("8192"));
    }

    /// Claim: a pipe ID fits the 31 bits a tag stack leaves for it, because
    /// the top bit is the stack's terminator and a device pushes pipe IDs
    /// onto that stack.
    #[test]
    fn a_pipe_id_fits_the_tag_stack() {
        assert_eq!(PipeId::new(7).get(), 7);
        assert_eq!(PipeId::new(u32::MAX).get(), PipeId::MAX);
        assert_eq!(PipeId::MAX, weida_sp::backtrace::MAX_ID);
        assert_eq!(PipeId::new(9).to_string(), "pipe 9");
    }
}
