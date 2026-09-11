//! The zguide's **Binary Star**, chapter 4: `bstar.c`'s finite state machine
//! and the PUB-SUB state exchange around it.
//!
//! ```text
//! cargo run -p weida-zmq --example binary_star
//! ```
//!
//! The pattern: "two servers, active and passive; the passive does no work
//! and monitors the active, taking over only after the active has been absent
//! for a configured time **and** clients ask it to connect". Its guarantee is
//! *at most one active server*, and its three-way rule is the whole of the
//! design: "server will not become active until it receives application
//! connection requests and cannot see peer".
//!
//! The state machine runs on three events - "Peer Active", "Client Request"
//! and "Client Vote", a client request arriving while the peer has been
//! silent for two heartbeats. State is exchanged over PUB-SUB **only**, and
//! the guide says why the obvious alternatives fail: "PUSH/DEALER block if
//! peer is not ready; PAIR does not reconnect after peer disappearance and
//! return; ROUTER needs peer address before send".
//!
//! # The split-brain warning, verbatim
//!
//! > "We must not split a Binary Star architecture into two islands, each
//! > with a set of applications. While this may be a common type of network
//! > architecture, you should use federation, not high-availability failover,
//! > in such cases."
//!
//! The dangerous topology is a pair split across two buildings with
//! applications in both and one link between them: lose the link and there
//! are two client groups and two active servers, each correct by its own
//! evidence. Nothing in this file - or in libzmq, or in `bstar.c` - can
//! detect that from the inside; [`BinaryStar::event`] reports the case it
//! *can* see, a peer that announces itself active while this server is
//! active, as a fatal `EFSM` exactly as the C aborts with "fatal error - dual
//! actives". The mitigation is physical: "a dedicated peer link on the same
//! switch or a crossover cable, better still two private interconnects on
//! separate NICs".
//!
//! Two further non-goals, stated so nobody reads more into the pattern than
//! it offers: it does **not** replicate state (clients "must recreate server
//! state and retransmit anything lost"), and recovery is manual by design,
//! because automatic recovery "creates a second outage and ambiguity".
//!
//! # Which surface, and why
//!
//! **Async.** The C form is a `zloop` reactor - `bstar_new`, voter
//! registration, active/passive handlers, `bstar_start` - polling the state
//! socket and a heartbeat timer. The state machine itself is pure, and is
//! written here as a pure function so a test can assert transitions without
//! any sockets at all.
//!
//! # The three differences from the C
//!
//! * **The clock is outside the state machine.** `s_execute_fsm` consults
//!   `self->peer_expiry` inside the `CLIENT_REQUEST` branch; here
//!   [`BinaryStar::classify`] turns a client request into `ClientRequest` or
//!   `ClientVote` by the clock, and [`BinaryStar::event`] is a pure
//!   transition. That is the same rule with the two halves separated, which
//!   is what makes "cannot see peer" assertable on its own.
//! * **A dual active is an error value, not an `abort()`.** The C calls
//!   `zsys_error` and returns -1, which stops `bstar_start`; here it is
//!   `EFSM` and a counter, because a library may not kill its caller's
//!   process - and the honest part, that neither can *detect* a split brain
//!   from the inside, is above.
//! * **`bstar_new`'s two-endpoint constructor is two calls.** The C takes
//!   both endpoints up front; [`bind_state_pub`] binds this server's
//!   publisher and reports where it landed, and [`start_star`] connects to
//!   the peer's - which is what a test needs when the OS picks the ports.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, Error, Multipart, PubSocket, Result, SubSocket};

/// `#define HEARTBEAT 1000 // In msecs`
pub const HEARTBEAT: Duration = Duration::from_secs(1);

/// The four states of `bstar.c`, which are also the four state messages on
/// the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// `STATE_PRIMARY`: the primary server, waiting for its peer.
    Primary,
    /// `STATE_BACKUP`: the backup server, waiting for its peer.
    Backup,
    /// `STATE_ACTIVE`: this server is doing the work.
    Active,
    /// `STATE_PASSIVE`: this server is watching the other one.
    Passive,
}

impl State {
    /// The single octet this state travels as.
    pub fn as_wire(self) -> &'static [u8] {
        match self {
            State::Primary => b"\x01",
            State::Backup => b"\x02",
            State::Active => b"\x03",
            State::Passive => b"\x04",
        }
    }

    /// The state a peer announced, or `None` for anything else on the wire.
    pub fn from_wire(octet: &[u8]) -> Option<State> {
        match octet {
            b"\x01" => Some(State::Primary),
            b"\x02" => Some(State::Backup),
            b"\x03" => Some(State::Active),
            b"\x04" => Some(State::Passive),
            _ => None,
        }
    }
}

/// The three events the guide names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// "Peer Active", and its siblings: what the peer says it is. The C
    /// spells this out as `PEER_PRIMARY`/`PEER_BACKUP`/`PEER_ACTIVE`/
    /// `PEER_PASSIVE`; one event carrying the state is the same four cases.
    Peer(State),
    /// "Client Request": an application asked this server to work, while the
    /// peer is visible.
    ClientRequest,
    /// "Client Vote": a client request arriving after the peer has been
    /// silent for two heartbeats. The vote, not the silence, is what
    /// promotes a passive server - "only when needed".
    ClientVote,
}

/// What a client's request came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// This server is active: serve the request.
    Serve,
    /// This server is not active and has no mandate to become it: the client
    /// must try the other address.
    Reject,
    /// A peer event, which no client is waiting on.
    Noted,
}

/// `bstar.c`'s finite state machine, with the clock kept outside it.
#[derive(Clone, Copy, Debug)]
pub struct BinaryStar {
    state: State,
    peer_last_seen: Option<Instant>,
    silence: Duration,
}

impl BinaryStar {
    /// A server in its start-up state, `Primary` or `Backup`.
    ///
    /// `heartbeat` is the C's `HEARTBEAT`; the silence that turns a request
    /// into a vote is two of them, which is `bstar.c`'s
    /// `peer_expiry = zclock_time () + 2 * HEARTBEAT`.
    pub fn new(state: State, heartbeat: Duration) -> BinaryStar {
        BinaryStar {
            state,
            peer_last_seen: None,
            silence: heartbeat * 2,
        }
    }

    /// The state this server believes it is in.
    pub fn state(&self) -> State {
        self.state
    }

    /// Records that the peer was heard from, which is what resets the vote
    /// window.
    pub fn peer_seen(&mut self, now: Instant) {
        self.peer_last_seen = Some(now);
    }

    /// Whether a client's request is an ordinary request or a vote: a vote
    /// needs the peer to have been silent for two heartbeats, and a peer
    /// never heard from at all is **not** silence - a server that has not yet
    /// met its peer has no evidence either way, which is why `bstar.c`
    /// asserts `peer_expiry > 0` here.
    pub fn classify(&self, now: Instant) -> Event {
        match self.peer_last_seen {
            Some(seen) if now.duration_since(seen) >= self.silence => Event::ClientVote,
            _ => Event::ClientRequest,
        }
    }

    /// One transition.
    ///
    /// # Errors
    ///
    /// `EFSM` for the one case the machine can see and must not survive: a
    /// peer announcing itself active while this server is active. "Two
    /// actives would mean split-brain."
    pub fn event(&mut self, event: Event) -> Result<Verdict> {
        match (self.state, event) {
            //  Primary server is waiting for peer to connect.
            //  Accepts CLIENT_REQUEST events in this state.
            (State::Primary, Event::Peer(State::Backup | State::Passive)) => {
                self.state = State::Active;
                Ok(Verdict::Noted)
            }
            (State::Primary, Event::Peer(State::Active)) => {
                self.state = State::Passive;
                Ok(Verdict::Noted)
            }
            //  Backup server is waiting for peer to connect.
            //  Rejects CLIENT_REQUEST events in this state.
            (State::Backup, Event::Peer(State::Active)) => {
                self.state = State::Passive;
                Ok(Verdict::Noted)
            }
            (State::Backup, Event::ClientRequest | Event::ClientVote) => Ok(Verdict::Reject),
            //  Server is active.
            (State::Active, Event::Peer(State::Active)) => Err(Error::EFSM(
                "fatal error - dual actives, which would mean split-brain".into(),
            )),
            (State::Active, Event::ClientRequest | Event::ClientVote) => Ok(Verdict::Serve),
            //  Server is passive. A restarting peer hands the work over; a
            //  vote takes it.
            (State::Passive, Event::Peer(State::Primary | State::Backup)) => {
                self.state = State::Active;
                Ok(Verdict::Noted)
            }
            //  A client request while the peer is visible is refused: the
            //  peer is doing the work.
            (State::Primary | State::Passive, Event::ClientRequest) => Ok(Verdict::Reject),
            (State::Primary | State::Passive, Event::ClientVote) => {
                self.state = State::Active;
                Ok(Verdict::Serve)
            }
            //  Everything else changes nothing: a passive server told its
            //  peer is active, a primary told its peer is primary.
            (_, Event::Peer(_)) => Ok(Verdict::Noted),
        }
    }
}

/// A running half of a Binary Star pair.
pub struct StarPeer {
    machine: Arc<Mutex<BinaryStar>>,
    /// Client requests refused - the client's cue to try the other address.
    pub rejected: Arc<AtomicUsize>,
    /// Dual actives seen, which is as much of a split brain as a peer can
    /// detect.
    pub split_brain: Arc<AtomicUsize>,
    /// The state exchange, so a test can make this server disappear.
    pub task: tokio::task::JoinHandle<()>,
}

impl StarPeer {
    /// The state this server believes it is in.
    pub fn state(&self) -> State {
        self.machine.lock().expect("the state machine").state()
    }

    /// A client request, classified by the clock and then run through the
    /// machine - the three-way rule in one call.
    ///
    /// # Errors
    ///
    /// `EFSM` on a dual active.
    pub fn client_request(&self) -> Result<Verdict> {
        let mut machine = self.machine.lock().expect("the state machine");
        let event = machine.classify(Instant::now());
        let verdict = machine.event(event)?;
        if verdict == Verdict::Reject {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        Ok(verdict)
    }
}

/// Binds this server's state publisher and reports where it landed, so the
/// peer can be told.
///
/// # Errors
///
/// What binding a socket reports.
pub async fn bind_state_pub(context: &Context) -> Result<(PubSocket, String)> {
    let publisher = PubSocket::new(context)?;
    let endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    Ok((publisher, endpoint))
}

/// `bstar_new` plus `bstar_start`: publish this server's state every
/// heartbeat, and drive the machine from the peer's.
///
/// # Errors
///
/// What connecting or subscribing a socket reports.
pub fn start_star(
    context: &Context,
    role: State,
    mut publisher: PubSocket,
    peer: &str,
    heartbeat: Duration,
) -> Result<StarPeer> {
    let mut subscriber = SubSocket::new(context)?;
    subscriber.subscribe("")?;
    subscriber.connect(peer)?;
    let machine = Arc::new(Mutex::new(BinaryStar::new(role, heartbeat)));
    let rejected = Arc::new(AtomicUsize::new(0));
    let split_brain = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn({
        let machine = Arc::clone(&machine);
        let split_brain = Arc::clone(&split_brain);
        async move {
            loop {
                tokio::select! {
                    arrived = subscriber.recv() => {
                        let Ok(message) = arrived else { return };
                        let Some(peer_state) = State::from_wire(message.frames()[0].as_slice())
                        else {
                            continue;
                        };
                        let mut machine = machine.lock().expect("the state machine");
                        machine.peer_seen(Instant::now());
                        if machine.event(Event::Peer(peer_state)).is_err() {
                            //  Dual actives. The C aborts here; a library
                            //  counts it and keeps publishing, so the peer
                            //  and the operator can both see it.
                            split_brain.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    () = tokio::time::sleep(heartbeat) => {
                        let state = machine.lock().expect("the state machine").state();
                        publisher.publish(Multipart::single(state.as_wire().to_vec()));
                    }
                }
            }
        }
    });
    Ok(StarPeer {
        machine,
        rejected,
        split_brain,
        task,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let heartbeat = Duration::from_millis(200);
    let (primary_pub, primary_endpoint) = bind_state_pub(&context).await?;
    let (backup_pub, backup_endpoint) = bind_state_pub(&context).await?;
    let primary = start_star(
        &context,
        State::Primary,
        primary_pub,
        &backup_endpoint,
        heartbeat,
    )?;
    let backup = start_star(
        &context,
        State::Backup,
        backup_pub,
        &primary_endpoint,
        heartbeat,
    )?;

    tokio::time::sleep(heartbeat * 4).await;
    println!(
        "I: primary is {:?}, backup is {:?}",
        primary.state(),
        backup.state()
    );
    println!("I: the active one serves: {:?}", primary.client_request()?);
    println!("I: the passive one refuses: {:?}", backup.client_request()?);

    //  The primary disappears. Silence alone changes nothing.
    primary.task.abort();
    tokio::time::sleep(heartbeat * 4).await;
    println!(
        "I: after the primary vanished, backup is {:?}",
        backup.state()
    );
    //  A client asks, and now the vote promotes it.
    println!("I: the vote: {:?}", backup.client_request()?);
    println!("I: backup is now {:?}", backup.state());
    Ok(())
}
