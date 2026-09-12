//! The endpoint engine: dialers, listeners, pipes and reconnect.
//!
//! This is everything an SP socket does *below* its protocol. "A socket owns
//! zero or more listener and dialer endpoints; endpoints create pipes, which
//! are message-oriented connected streams and commonly map 1:1 to TCP or IPC
//! connections", and "either role may listen, dial, or do both; endpoint
//! direction does not prescribe request/reply or other application role"
//! (`docs/research/nanomsg-nng.md` §1). So an [`Engine`] owns the
//! [`Dialer`]s, the [`Listener`]s and one [`Pipe`] per live connection, and
//! the socket types above choose which pipe a message goes to.
//!
//! **A pipe is a connection, not an endpoint.** Unlike ZMTP, where a queue
//! is created when a connection is *initiated* and survives reconnects, an
//! SP pipe exists only while its connection does and "communication over
//! that pipe is then impossible" once it is removed (§1). Nothing can be
//! queued here for a peer that has not arrived.
//!
//! **A synchronous dial waits for the peer's protocol header.** "The
//! synchronous `nng_dial()` returns only after the peer's 8-octet protocol
//! header has arrived, not when the TCP connection is established", and a
//! program that calls it from the thread that must answer a greeting
//! "deadlocks until the socket timeout fires" (§1). [`Engine::dial`] keeps
//! the first half and improves the second: it is `async`, so waiting for the
//! header occupies no thread and the reactor keeps serving every other
//! socket in the process — the deadlock the sheet describes cannot be
//! constructed. It is also bounded, by
//! [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout),
//! where NNG waits for its socket timeout.
//!
//! **Every timer goes through `weida-runtime`'s `Exec`**: the reconnect
//! backoff and the handshake deadline. Nothing here calls `tokio::time`.
//!
//! What this module does **not** do is speak SP. A connection is handed to a
//! [`Session`], and the 8-octet header, the pairing check and the 64-bit
//! framing are that session's
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §5.2).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use weida_runtime::Exec;
use weida_sp::EndpointType;

use crate::context::{Context, SocketSlot};
use crate::endpoint::{Endpoint, TcpHost};
use crate::error::{Cause, Error, Result};
use crate::options::SocketOptions;
use crate::pipe::{Discarded, Pipe, PipeId};
use crate::transport::Stream;

/// What a [`Session`] returns: a future that ends when the connection does.
pub type SessionFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Which side of a connection this socket is.
///
/// SP itself does not care — "either role may listen, dial, or do both;
/// endpoint direction does not prescribe request/reply or other application
/// role" (§1) — but reconnect does: only a dialer redials, because only a
/// dialer has an address to redial.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// This side dialled the peer.
    Dial,
    /// This side accepted the peer.
    Listen,
}

/// How a session tells the engine that the peer's protocol header arrived.
///
/// The handshake is the only thing between a connection and a pipe, and only
/// the session can see it happen. So the engine hands this over with the
/// connection, races it against
/// [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout),
/// and closes the connection if the timer wins — which is what keeps a peer
/// that connects and says nothing from holding a pipe slot for as long as it
/// likes.
#[derive(Debug)]
pub struct HandshakeGate {
    done: Option<oneshot::Sender<EndpointType>>,
}

impl HandshakeGate {
    /// Reports that the peer's header arrived and named `peer`.
    ///
    /// Idempotent: a second call does nothing, because there is only one
    /// handshake per connection and a session that called twice would
    /// otherwise be a panic rather than a bug.
    pub fn complete(&mut self, peer: EndpointType) {
        if let Some(done) = self.done.take() {
            let _ = done.send(peer);
        }
    }
}

/// One established connection, handed to a [`Session`].
#[derive(Debug)]
pub struct Connection {
    /// The byte stream.
    pub stream: Stream,
    /// The queues this connection carries, already bounded.
    pub pipe: Pipe,
    /// Which side dialled.
    pub role: Role,
    /// The endpoint this connection belongs to, for a log line.
    pub endpoint: Endpoint,
    /// What this socket speaks, which is what goes in its protocol header.
    pub protocol: EndpointType,
    /// `NNG_OPT_RECVMAXSZ` for this connection.
    pub recv_max_size: u64,
    /// Signalled once the peer's protocol header has been read and accepted.
    pub handshake: HandshakeGate,
}

/// What drives one connection once its bytes flow: in this library, SP.
///
/// One implementation for every protocol, because the 8-octet header and the
/// 64-bit framing do not depend on the pattern — the protocol is a *field*
/// in the header, not a different wire format (§3).
pub trait Session: Send + Sync + 'static {
    /// Runs `connection` until it ends.
    fn start(&self, connection: Connection) -> SessionFuture;
}

/// A pipe lifecycle event, as `nng_pipe_notify(3)` defines them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PipeEvent {
    /// `NNG_PIPE_EV_ADD_PRE`: the connection handshook and the pipe is about
    /// to enter the socket. This is the only event whose answer matters: a
    /// callback may refuse the pipe here, "for example after local
    /// authorization logic", and that is "application policy, not SP
    /// authorization" (§10).
    AddPre,
    /// `NNG_PIPE_EV_ADD_POST`: the pipe is in the socket and traffic may
    /// flow.
    AddPost,
    /// `NNG_PIPE_EV_REM_POST`: the pipe is gone and "communication over that
    /// pipe is then impossible" (§1).
    RemPost,
}

/// What a callback knows about a pipe.
///
/// **A value, not a handle, and that is the rule NNG can only write down.**
/// NNG's callback "runs under the socket lock and must not access the
/// socket" (§2) — a sentence a C program obeys or deadlocks. Here the
/// callback is handed this and nothing else, so touching the socket is not
/// something it can do wrongly; it is something it cannot express.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipeInfo {
    /// The pipe's id within its socket.
    pub id: PipeId,
    /// Which side dialled.
    pub role: Role,
    /// The endpoint that created this pipe.
    pub endpoint: Endpoint,
    /// What the peer said it was, once its header has been read.
    pub peer: Option<EndpointType>,
    /// `NNG_OPT_LOCADDR`: this end's address, where the transport has one.
    pub local_addr: Option<String>,
    /// `NNG_OPT_REMADDR`: the peer's address, where the transport has one.
    pub remote_addr: Option<String>,
}

/// A callback's answer to [`PipeEvent::AddPre`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Let the pipe into the socket.
    Accept,
    /// Refuse it. The connection is closed and nothing is sent to the peer,
    /// because SP has no way to say more than that (§6). The cause is for
    /// this side's log.
    Reject(Cause),
}

/// What the engine calls on a pipe event.
pub type PipeCallback = Arc<dyn Fn(PipeEvent, &PipeInfo) -> Admission + Send + Sync>;

/// A dialer's identity within its socket, as `nng_dialer_id()` reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DialerId(u32);

impl DialerId {
    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A listener's identity within its socket, as `nng_listener_id()` reports
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListenerId(u32);

impl ListenerId {
    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One named dialling endpoint of a socket.
///
/// "A dialer initiates a connection to a URL … Both are associated with one
/// socket and create pipes" (§2). It is a value the application holds, so
/// that one of a socket's several dialers can be closed without closing the
/// others.
#[derive(Clone, Debug)]
pub struct Dialer {
    id: DialerId,
    url: Endpoint,
    engine: Engine,
}

impl Dialer {
    /// This dialer's id within its socket.
    pub const fn id(&self) -> DialerId {
        self.id
    }

    /// `NNG_OPT_URL`: the endpoint this dialer dials.
    pub const fn url(&self) -> &Endpoint {
        &self.url
    }

    /// `nng_dialer_close()`: stop dialling and redialling. A pipe this
    /// dialer already created is closed with it, because a pipe belongs to
    /// the endpoint that made it (§2).
    pub fn close(&self) {
        self.engine.close_dialer(self.id);
    }
}

/// One named listening endpoint of a socket.
#[derive(Clone, Debug)]
pub struct Listener {
    id: ListenerId,
    url: Endpoint,
    engine: Engine,
}

impl Listener {
    /// This listener's id within its socket.
    pub const fn id(&self) -> ListenerId {
        self.id
    }

    /// `NNG_OPT_URL`: the endpoint actually listened on, which is the only
    /// way to learn the port behind a wildcard.
    pub const fn url(&self) -> &Endpoint {
        &self.url
    }

    /// `nng_listener_close()`: stop accepting, and close the pipes this
    /// listener created.
    pub fn close(&self) {
        self.engine.close_listener(self.id);
    }
}

struct PipeEntry {
    pipe: Pipe,
    info: PipeInfo,
    /// False while the connection is handshaking. Such a pipe is invisible
    /// to the socket and still counts against the ceiling, because a
    /// connection nobody has authenticated is exactly the one an attacker
    /// opens.
    admitted: bool,
    dialer: Option<DialerId>,
    listener: Option<ListenerId>,
}

/// One live dialer or listener: the task that runs it, which is what
/// closing one aborts. The URL lives on the [`Dialer`] or [`Listener`] the
/// application holds, so it is not kept twice.
struct EndpointEntry {
    task: JoinHandle<()>,
}

struct EngineState {
    pipes: HashMap<PipeId, PipeEntry>,
    dialers: HashMap<DialerId, EndpointEntry>,
    listeners: HashMap<ListenerId, EndpointEntry>,
    next_pipe: u32,
    next_endpoint: u32,
    closed: bool,
}

struct EngineInner {
    exec: Exec,
    protocol: EndpointType,
    options: SocketOptions,
    session: Arc<dyn Session>,
    notify: Mutex<Option<PipeCallback>>,
    state: Mutex<EngineState>,
    /// Woken whenever the admitted pipe set changes, so a socket waiting for
    /// a peer does not poll.
    changed: Notify,
    /// Holds this socket's slot under its context's ceiling for as long as
    /// the engine lives.
    _slot: SocketSlot,
}

/// The transport-facing half of a socket: what dials, listens, reconnects
/// and holds the pipes.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

/// What a task needs to run a connection without keeping the socket alive.
#[derive(Clone)]
struct TaskCtx {
    engine: Engine,
}

impl Engine {
    /// Creates the engine of a socket speaking `protocol`.
    ///
    /// Fails with `NNG_EINVAL` or `NNG_ENOTSUP` for a configuration this
    /// protocol cannot have, and with `NNG_ENOFILES` when the context is
    /// already at its socket ceiling.
    pub fn new(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        session: Arc<dyn Session>,
    ) -> Result<Engine> {
        options.validate_for(protocol)?;
        let slot = context.open_socket()?;
        Ok(Engine {
            inner: Arc::new(EngineInner {
                exec: context.exec().clone(),
                protocol,
                options,
                session,
                notify: Mutex::new(None),
                state: Mutex::new(EngineState {
                    pipes: HashMap::new(),
                    dialers: HashMap::new(),
                    listeners: HashMap::new(),
                    next_pipe: 1,
                    next_endpoint: 1,
                    closed: false,
                }),
                changed: Notify::new(),
                _slot: slot,
            }),
        })
    }

    /// What this socket speaks.
    pub fn protocol(&self) -> EndpointType {
        self.inner.protocol
    }

    /// The options this socket runs under.
    pub fn options(&self) -> &SocketOptions {
        &self.inner.options
    }

    /// The reactor this socket's tasks and timers run on.
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// `nng_pipe_notify()`: install the callback that sees every pipe
    /// event, replacing any installed before.
    ///
    /// One callback for all three events rather than NNG's three
    /// registrations, because the three are one lifecycle and a program that
    /// registers for two of them has a gap it did not decide to have.
    pub fn notify(&self, callback: PipeCallback) {
        *self.inner.notify.lock().expect("notify poisoned") = Some(callback);
    }

    /// Every pipe the socket may use right now.
    pub fn pipes(&self) -> Vec<Pipe> {
        self.lock()
            .pipes
            .values()
            .filter(|entry| entry.admitted)
            .map(|entry| entry.pipe.clone())
            .collect()
    }

    /// What the socket knows about each of its pipes, in id order.
    pub fn pipe_infos(&self) -> Vec<PipeInfo> {
        let mut infos: Vec<PipeInfo> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.admitted)
            .map(|entry| entry.info.clone())
            .collect();
        infos.sort_by_key(|info| info.id);
        infos
    }

    /// Waits until the admitted pipe set changes.
    ///
    /// A socket with nothing to send to waits on this rather than polling:
    /// "with no eligible peer, the send waits or times out" (§4), and this
    /// is the wait.
    pub async fn changed(&self) {
        let notified = self.inner.changed.notified();
        notified.await;
    }

    /// Whether the socket has been closed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Dials `endpoint`, returning only once the peer's protocol header has
    /// arrived — `nng_dial()` without `NNG_FLAG_NONBLOCK` (§1).
    ///
    /// A refused first connection is reported and **no retry is started**,
    /// which is NNG's rule for this form; once a pipe has existed, the
    /// dialer redials it whenever it closes, which is NNG's rule for every
    /// form. A handshake that fails on that first connection is reported the
    /// same way as a refused connect, because the pipe never existed.
    ///
    /// Unlike NNG's, this wait costs no thread and is bounded by
    /// [`SocketOptions::handshake_timeout`](crate::SocketOptions::handshake_timeout).
    pub async fn dial(&self, endpoint: &Endpoint) -> Result<Dialer> {
        let (id, rx) = self.start_dialer(endpoint, false)?;
        match rx.await {
            Ok(Ok(())) => Ok(self.dialer_handle(id, endpoint.clone())),
            Ok(Err(error)) => {
                self.close_dialer(id);
                Err(error)
            }
            Err(_) => {
                self.close_dialer(id);
                Err(Error::ECLOSED(
                    "the socket was closed while the dial was in progress".into(),
                ))
            }
        }
    }

    /// Dials `endpoint` without waiting — `NNG_FLAG_NONBLOCK` (§1).
    ///
    /// "Makes that first attempt asynchronous, after which failures retry
    /// periodically", with the backoff of `NNG_OPT_RECONNMINT` and
    /// `NNG_OPT_RECONNMAXT`. The dialer exists immediately and the pipe
    /// appears whenever the peer does.
    pub fn dial_nonblocking(&self, endpoint: &Endpoint) -> Result<Dialer> {
        let (id, _rx) = self.start_dialer(endpoint, true)?;
        Ok(self.dialer_handle(id, endpoint.clone()))
    }

    /// Listens on `endpoint` and accepts automatically.
    ///
    /// Returns the endpoint actually bound, which for a wildcard port is the
    /// only way to learn the port.
    pub async fn listen(&self, endpoint: &Endpoint) -> Result<Listener> {
        // Before the OS, not after: a closed socket refuses without taking
        // an address, so a close followed by a listen reports the close
        // rather than whatever the address happens to be doing.
        if self.is_closed() {
            return Err(closed());
        }
        let listener = bind(endpoint).await?;
        let bound = bound_endpoint(endpoint, &listener)?;
        let id = {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            ListenerId(next(&mut state.next_endpoint))
        };
        let ctx = TaskCtx {
            engine: self.clone(),
        };
        let url = bound.clone();
        let task = self
            .inner
            .exec
            .spawn(accept_loop(ctx, listener, id, url.clone()));
        let mut state = self.lock();
        if state.closed {
            task.abort();
            return Err(closed());
        }
        state.listeners.insert(id, EndpointEntry { task });
        drop(state);
        Ok(Listener {
            id,
            url,
            engine: self.clone(),
        })
    }

    /// `nng_dialer_close()`.
    pub fn close_dialer(&self, id: DialerId) {
        let entry = self.lock().dialers.remove(&id);
        if let Some(entry) = entry {
            entry.task.abort();
        }
        let owned: Vec<PipeId> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.dialer == Some(id))
            .map(|entry| entry.info.id)
            .collect();
        // Retired here rather than left to the task that was driving them:
        // aborting a dialer stops the very loop that would have reported
        // the removal, and a pipe nobody retires is a pipe that stays in
        // the socket's table for ever.
        for pipe in owned {
            self.retire(pipe);
        }
    }

    /// `nng_listener_close()`.
    pub fn close_listener(&self, id: ListenerId) {
        let entry = self.lock().listeners.remove(&id);
        if let Some(entry) = entry {
            entry.task.abort();
        }
        let owned: Vec<PipeId> = self
            .lock()
            .pipes
            .values()
            .filter(|entry| entry.listener == Some(id))
            .map(|entry| entry.info.id)
            .collect();
        for pipe in owned {
            self.retire(pipe);
        }
    }

    /// `nng_pipe_close()`: destroy one pipe and everything in its queues.
    ///
    /// The session sees its queues close and ends, which removes the entry
    /// and fires [`PipeEvent::RemPost`].
    pub fn close_pipe(&self, id: PipeId) -> Discarded {
        let pipe = self.lock().pipes.get(&id).map(|entry| entry.pipe.clone());
        match pipe {
            Some(pipe) => pipe.close(),
            None => Discarded::default(),
        }
    }

    /// `nng_close()`: stop dialling and accepting, and destroy every pipe.
    pub fn close(&self) {
        let (dialers, listeners, pipes) = {
            let mut state = self.lock();
            state.closed = true;
            (
                state.dialers.drain().collect::<Vec<_>>(),
                state.listeners.drain().collect::<Vec<_>>(),
                state.pipes.keys().copied().collect::<Vec<PipeId>>(),
            )
        };
        for (_, entry) in dialers {
            entry.task.abort();
        }
        for (_, entry) in listeners {
            entry.task.abort();
        }
        for pipe in pipes {
            self.retire(pipe);
        }
        self.inner.changed.notify_waiters();
    }

    fn dialer_handle(&self, id: DialerId, url: Endpoint) -> Dialer {
        Dialer {
            id,
            url,
            engine: self.clone(),
        }
    }

    fn start_dialer(
        &self,
        endpoint: &Endpoint,
        retry_first: bool,
    ) -> Result<(DialerId, oneshot::Receiver<Result<()>>)> {
        let id = {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            DialerId(next(&mut state.next_endpoint))
        };
        let (tx, rx) = oneshot::channel();
        let ctx = TaskCtx {
            engine: self.clone(),
        };
        let task = self.inner.exec.spawn(dialer_loop(
            ctx,
            id,
            endpoint.clone(),
            Some(tx),
            retry_first,
        ));
        let mut state = self.lock();
        if state.closed {
            task.abort();
            return Err(closed());
        }
        state.dialers.insert(id, EndpointEntry { task });
        Ok((id, rx))
    }

    /// Reserves a pipe slot against the socket's ceiling.
    fn reserve(
        &self,
        role: Role,
        endpoint: &Endpoint,
        dialer: Option<DialerId>,
        listener: Option<ListenerId>,
        stream: &Stream,
    ) -> Result<(PipeId, Pipe)> {
        let mut state = self.lock();
        if state.closed {
            return Err(closed());
        }
        let ceiling = self.inner.options.max_pipes;
        if state.pipes.len() >= ceiling {
            return Err(Error::ENOFILES(
                format!("this socket already holds its ceiling of {ceiling} pipes").into(),
            ));
        }
        let id = PipeId::new(next(&mut state.next_pipe));
        let pipe = Pipe::new(id, self.inner.options.pipe_config(self.inner.protocol));
        state.pipes.insert(
            id,
            PipeEntry {
                pipe: pipe.clone(),
                info: PipeInfo {
                    id,
                    role,
                    endpoint: endpoint.clone(),
                    peer: None,
                    local_addr: stream.local_addr(),
                    remote_addr: stream.remote_addr(),
                },
                admitted: false,
                dialer,
                listener,
            },
        );
        Ok((id, pipe))
    }

    /// Runs the callback, if one is installed. Never under the state lock:
    /// the callback is the application's code and the socket's lock is not
    /// the application's to hold.
    fn fire(&self, event: PipeEvent, info: &PipeInfo) -> Admission {
        let callback = self.inner.notify.lock().expect("notify poisoned").clone();
        match callback {
            Some(callback) => callback(event, info),
            None => Admission::Accept,
        }
    }

    /// Moves a handshaken pipe into the socket, or refuses it.
    fn admit(&self, id: PipeId, peer: EndpointType) -> Result<()> {
        let info = {
            let mut state = self.lock();
            let Some(entry) = state.pipes.get_mut(&id) else {
                return Err(closed());
            };
            entry.info.peer = Some(peer);
            entry.info.clone()
        };
        if let Admission::Reject(why) = self.fire(PipeEvent::AddPre, &info) {
            return Err(Error::EPEERAUTH(why));
        }
        {
            let mut state = self.lock();
            if state.closed {
                return Err(closed());
            }
            match state.pipes.get_mut(&id) {
                Some(entry) => entry.admitted = true,
                None => return Err(closed()),
            }
        }
        self.fire(PipeEvent::AddPost, &info);
        self.inner.changed.notify_waiters();
        Ok(())
    }

    /// Removes a pipe and fires `REM_POST`, whether it was ever admitted or
    /// not.
    fn retire(&self, id: PipeId) {
        let entry = self.lock().pipes.remove(&id);
        if let Some(entry) = entry {
            entry.pipe.close();
            if entry.admitted {
                self.fire(PipeEvent::RemPost, &entry.info);
                self.inner.changed.notify_waiters();
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, EngineState> {
        self.inner.state.lock().expect("engine state poisoned")
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Engine")
            .field("protocol", &self.inner.protocol)
            .field("dialers", &state.dialers.len())
            .field("listeners", &state.listeners.len())
            .field("pipes", &state.pipes.len())
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}

fn next(counter: &mut u32) -> u32 {
    let id = *counter;
    *counter = counter.wrapping_add(1).max(1);
    id
}

fn closed() -> Error {
    Error::ECLOSED("this socket is closed".into())
}

/// Dials one endpoint, and keeps redialling it after each pipe closes.
async fn dialer_loop(
    ctx: TaskCtx,
    id: DialerId,
    endpoint: Endpoint,
    mut first: Option<oneshot::Sender<Result<()>>>,
    retry_first: bool,
) {
    let mut attempt = 0u32;
    loop {
        if ctx.engine.is_closed() {
            return;
        }
        let outcome = match connect(ctx.engine.exec(), &endpoint, &ctx.engine.inner.options).await {
            Ok(stream) => {
                run_pipe(
                    &ctx,
                    stream,
                    Role::Dial,
                    &endpoint,
                    Some(id),
                    None,
                    &mut first,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => {
                // The pipe lived and then closed: "after a dialer pipe
                // closes, its dialer attempts reconnection" (§1), from the
                // shortest delay again.
                attempt = 0;
            }
            Err(error) => {
                if let Some(tx) = first.take() {
                    let _ = tx.send(Err(error));
                    if !retry_first {
                        // "A refused first connection is returned
                        // immediately and no retry is started" (§1).
                        return;
                    }
                } else {
                    tracing::debug!(%endpoint, %error, "SP dial failed; retrying");
                }
            }
        }
        let delay = ctx.engine.inner.options.reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        ctx.engine.exec().sleep(delay).await;
    }
}

/// Accepts connections on one listening endpoint. There is no accept call in
/// SP's API: a listener accepts automatically (§1).
async fn accept_loop(ctx: TaskCtx, listener: TcpListener, id: ListenerId, endpoint: Endpoint) {
    loop {
        let accepted = match listener.accept().await {
            Ok((stream, _)) => Stream::tcp(stream),
            Err(error) => {
                tracing::debug!(%endpoint, %error, "SP listener could not accept");
                continue;
            }
        };
        let ctx = ctx.clone();
        let endpoint = endpoint.clone();
        ctx.engine.exec().clone().spawn(async move {
            let mut first = None;
            if let Err(error) = run_pipe(
                &ctx,
                accepted,
                Role::Listen,
                &endpoint,
                None,
                Some(id),
                &mut first,
            )
            .await
            {
                tracing::debug!(%endpoint, %error, "SP connection ended before it was a pipe");
            }
        });
    }
}

/// Runs one connection from its first octet to its last.
///
/// Returns `Ok(())` once a pipe that was admitted has closed, and an error
/// when the connection never became a pipe — which is what a synchronous
/// dial reports and what a listener logs.
async fn run_pipe(
    ctx: &TaskCtx,
    stream: Stream,
    role: Role,
    endpoint: &Endpoint,
    dialer: Option<DialerId>,
    listener: Option<ListenerId>,
    first: &mut Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    let (id, pipe) = ctx
        .engine
        .reserve(role, endpoint, dialer, listener, &stream)?;
    let (done, signal) = oneshot::channel();
    let connection = Connection {
        stream,
        pipe,
        role,
        endpoint: endpoint.clone(),
        protocol: ctx.engine.protocol(),
        recv_max_size: ctx.engine.inner.options.recv_max_size,
        handshake: HandshakeGate { done: Some(done) },
    };
    let mut session = ctx.engine.inner.session.start(connection);

    let deadline = ctx
        .engine
        .exec()
        .sleep(ctx.engine.inner.options.handshake_timeout);
    let peer = {
        tokio::pin!(deadline);
        tokio::select! {
            ended = &mut session => {
                ctx.engine.retire(id);
                return match ended {
                    Ok(()) => Err(Error::ECONNRESET(
                        "the peer closed before its protocol header arrived".into(),
                    )),
                    Err(error) => Err(error),
                };
            }
            handshook = signal => match handshook {
                Ok(peer) => peer,
                Err(_) => {
                    ctx.engine.retire(id);
                    return Err(Error::EPROTO(
                        "the session ended the handshake without naming the peer".into(),
                    ));
                }
            },
            () = &mut deadline => {
                ctx.engine.retire(id);
                return Err(Error::ETIMEDOUT(
                    "the peer did not send its protocol header in time".into(),
                ));
            }
        }
    };

    if let Err(error) = ctx.engine.admit(id, peer) {
        ctx.engine.retire(id);
        return Err(error);
    }
    if let Some(tx) = first.take() {
        let _ = tx.send(Ok(()));
    }

    let outcome = session.await;
    ctx.engine.retire(id);
    match outcome {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::debug!(%endpoint, %error, "SP pipe ended");
            Ok(())
        }
    }
}

/// One connect attempt against one endpoint.
async fn connect(exec: &Exec, endpoint: &Endpoint, options: &SocketOptions) -> Result<Stream> {
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let mut last = Error::EADDRINVAL("no address to dial".into());
            for addr in addresses(exec, host, *port, options.max_addresses).await? {
                match TcpStream::connect(addr).await {
                    Ok(stream) => return Ok(Stream::tcp(stream)),
                    Err(error) => last = Error::from(error),
                }
            }
            Err(last)
        }
        other => Err(unsupported(other)),
    }
}

/// Binds one endpoint.
async fn bind(endpoint: &Endpoint) -> Result<TcpListener> {
    match endpoint {
        Endpoint::Tcp { host, port } => {
            let addr = match host {
                // A wildcard listens on every IPv4 interface, which is what
                // `tcp://*:port` means to NNG. An IPv6 wildcard is written
                // `tcp://[::]:port`.
                TcpHost::Any => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), *port),
                TcpHost::Ip(ip) => SocketAddr::new(*ip, *port),
                TcpHost::Name(name) => {
                    // A listener's host is an interface, and resolving one
                    // is the resolver's job only for a literal; anything
                    // else would bind an address this machine may not have.
                    let ip: IpAddr = name.parse().map_err(|_| {
                        Error::EADDRINVAL(
                            format!(
                                "cannot listen on {name:?}: a listening address must be an IP \
                                 literal or *"
                            )
                            .into(),
                        )
                    })?;
                    SocketAddr::new(ip, *port)
                }
            };
            TcpListener::bind(addr).await.map_err(Error::from)
        }
        other => Err(unsupported(other)),
    }
}

fn bound_endpoint(requested: &Endpoint, listener: &TcpListener) -> Result<Endpoint> {
    let local = listener.local_addr().map_err(Error::from)?;
    match requested {
        Endpoint::Tcp { host, .. } => Ok(Endpoint::Tcp {
            host: host.clone(),
            port: local.port(),
        }),
        other => Ok(other.clone()),
    }
}

async fn addresses(
    exec: &Exec,
    host: &TcpHost,
    port: u16,
    max_addresses: usize,
) -> Result<Vec<SocketAddr>> {
    Ok(match host {
        // Dialling the wildcard is dialling this host, which is what NNG's
        // own URL parser makes of it.
        TcpHost::Any => vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)],
        TcpHost::Ip(ip) => vec![SocketAddr::new(*ip, port)],
        TcpHost::Name(name) => exec.resolve(name, port, max_addresses).await?,
    })
}

fn unsupported(endpoint: &Endpoint) -> Error {
    Error::ENOTSUP(
        format!(
            "the {} transport is not carried by this engine",
            endpoint.transport()
        )
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::context::{Context, ContextConfig};

    /// A session that does the least a session can do: send the 8-octet
    /// protocol header, read the peer's, report it, then sit on the
    /// connection until its queues are closed.
    ///
    /// Enough to exercise every engine rule — the handshake gate, the
    /// ceiling, the pipe events, reconnect — and no more, because the SP
    /// session proper is its own slice and this file must not anticipate it.
    struct HeaderOnly {
        handshakes: Arc<AtomicUsize>,
        answer: bool,
    }

    impl Session for HeaderOnly {
        fn start(&self, connection: Connection) -> SessionFuture {
            let handshakes = Arc::clone(&self.handshakes);
            let answer = self.answer;
            Box::pin(async move {
                let Connection {
                    mut stream,
                    pipe,
                    protocol,
                    mut handshake,
                    ..
                } = connection;
                stream
                    .write_all(&weida_sp::ProtocolHeader::new(protocol).encode())
                    .await?;
                stream.flush().await?;
                if !answer {
                    // A peer that connects and never speaks: the engine's
                    // handshake deadline is the only thing that ends it.
                    std::future::pending::<()>().await;
                }
                let mut theirs = [0u8; weida_sp::HEADER_LEN];
                stream.read_exact(&mut theirs).await?;
                let peer = weida_sp::ProtocolHeader::decode(&theirs)?;
                handshakes.fetch_add(1, Ordering::SeqCst);
                handshake.complete(peer.endpoint);
                // Stay until the pipe is destroyed under us.
                let _ = pipe.incoming().recv().await;
                Ok(())
            })
        }
    }

    fn engine(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        handshakes: &Arc<AtomicUsize>,
    ) -> Engine {
        Engine::new(
            context,
            protocol,
            options,
            Arc::new(HeaderOnly {
                handshakes: Arc::clone(handshakes),
                answer: true,
            }),
        )
        .expect("engine")
    }

    fn counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    /// Claim: a listener accepts automatically, a dial creates one pipe per
    /// connection on both sides, and the endpoint objects carry the URL
    /// actually used — including the port behind a wildcard.
    #[tokio::test]
    async fn a_dial_and_a_listen_make_one_pipe_each() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);

        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        assert!(!listener.url().has_wildcard_port(), "the port is knowable");

        let dialer = client.dial(listener.url()).await.expect("dial");
        assert_eq!(dialer.url(), listener.url());

        assert_eq!(client.pipes().len(), 1);
        let infos = client.pipe_infos();
        assert_eq!(infos[0].role, Role::Dial);
        assert_eq!(infos[0].peer, Some(EndpointType::Rep));
        assert!(infos[0].remote_addr.is_some());

        // The server side is admitted too, once its own handshake finishes.
        for _ in 0..100 {
            if server.pipes().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(server.pipes().len(), 1);
        assert_eq!(server.pipe_infos()[0].role, Role::Listen);
    }

    /// Claim: the synchronous dial returns only once the peer's protocol
    /// header has arrived — not when the TCP connection is established.
    /// A listener that accepts and says nothing is the difference.
    #[tokio::test]
    async fn a_synchronous_dial_waits_for_the_peers_header() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a raw listener");
        let port = silent.local_addr().unwrap().port();
        let held = tokio::spawn(async move {
            // Accept and never answer, which is what a peer stuck in its own
            // dial looks like.
            let (stream, _) = silent.accept().await.expect("accept");
            std::future::pending::<()>().await;
            drop(stream);
        });

        let seen = counter();
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                handshake_timeout: Duration::from_millis(200),
                ..SocketOptions::default()
            },
            &seen,
        );
        let url = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap();
        let error = client.dial(&url).await.unwrap_err();
        assert!(matches!(error, Error::ETIMEDOUT(_)), "{error:?}");
        assert_eq!(seen.load(Ordering::SeqCst), 0, "no handshake completed");
        assert!(client.pipes().is_empty(), "no pipe without a handshake");
        held.abort();
    }

    /// Claim: a refused first synchronous dial is reported and starts no
    /// retry — NNG's own rule — while the non-blocking form returns at once
    /// and keeps trying until the peer appears.
    #[tokio::test]
    async fn the_two_dial_forms_differ_in_exactly_one_thing() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                reconnect_min: Duration::from_millis(5),
                ..SocketOptions::default()
            },
            &seen,
        );

        // A port nobody is on. Bound and dropped, so it is free and refusing.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let port = dead.local_addr().unwrap().port();
        drop(dead);
        let url = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap();

        let error = client.dial(&url).await.unwrap_err();
        assert!(
            matches!(error, Error::ECONNREFUSED(_) | Error::EUNREACHABLE(_)),
            "{error:?}"
        );
        assert!(
            client.lock().dialers.is_empty(),
            "a refused synchronous dial leaves no dialer behind"
        );

        // The non-blocking form returns before anything has connected, and
        // the pipe appears once somebody listens on that address.
        let dialer = client.dial_nonblocking(&url).expect("nonblocking dial");
        assert!(client.pipes().is_empty());

        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        server
            .listen(&Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).unwrap())
            .await
            .expect("listen");
        for _ in 0..200 {
            if client.pipes().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(client.pipes().len(), 1, "the retry found the peer");
        dialer.close();
    }

    /// Claim: a dialer redials after its pipe closes, which is the rule that
    /// applies to both dial forms once a pipe has existed.
    #[tokio::test]
    async fn a_dialer_redials_after_its_pipe_closes() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                reconnect_min: Duration::from_millis(5),
                ..SocketOptions::default()
            },
            &seen,
        );
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        client.dial(listener.url()).await.expect("dial");
        let first = client.pipe_infos()[0].id;

        client.close_pipe(first);
        for _ in 0..200 {
            let pipes = client.pipe_infos();
            if pipes.len() == 1 && pipes[0].id != first {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the dialer did not redial");
    }

    /// Claim: all three pipe events reach the callback in order, and the
    /// callback's refusal at `ADD_PRE` keeps the pipe out of the socket —
    /// which is the hook local authorization runs in (§10).
    #[tokio::test]
    async fn pipe_events_reach_the_callback_and_add_pre_can_refuse() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);

        let events: Arc<Mutex<Vec<PipeEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        client.notify(Arc::new(move |event, info: &PipeInfo| {
            recorded.lock().unwrap().push(event);
            assert_eq!(info.role, Role::Dial);
            Admission::Accept
        }));

        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");
        client.dial(listener.url()).await.expect("dial");
        assert_eq!(
            *events.lock().unwrap(),
            vec![PipeEvent::AddPre, PipeEvent::AddPost]
        );

        let id = client.pipe_infos()[0].id;
        client.close_pipe(id);
        for _ in 0..200 {
            if events.lock().unwrap().contains(&PipeEvent::RemPost) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            events.lock().unwrap().first(),
            Some(&PipeEvent::AddPre),
            "the order is add-pre, add-post, remove-post"
        );
        assert!(events.lock().unwrap().contains(&PipeEvent::RemPost));

        // A refusal keeps the pipe out, and the dial reports it.
        let refusing = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);
        refusing.notify(Arc::new(|event, _: &PipeInfo| match event {
            PipeEvent::AddPre => Admission::Reject("not on the allow-list".into()),
            _ => Admission::Accept,
        }));
        let error = refusing.dial(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::EPEERAUTH(_)), "{error:?}");
        assert!(refusing.pipes().is_empty());
    }

    /// Claim: the pipe ceiling is real and is reported as `NNG_ENOFILES`,
    /// and it counts a connection that has not handshaken — which is the
    /// only kind an attacker opens.
    #[tokio::test]
    async fn the_pipe_ceiling_bounds_what_a_stranger_can_open() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");

        let client = engine(
            &ctx,
            EndpointType::Req,
            SocketOptions {
                max_pipes: 1,
                ..SocketOptions::default()
            },
            &seen,
        );
        client.dial(listener.url()).await.expect("the first pipe");
        let error = client.dial(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::ENOFILES(_)), "{error:?}");
        assert!(error.cause().contains('1'));
    }

    /// Claim: closing a dialer closes the pipe it made, and closing the
    /// socket closes everything — a dialer that kept redialling after
    /// `nng_close()` would be a socket that cannot be shut down.
    #[tokio::test]
    async fn closing_an_endpoint_closes_what_it_created() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let server = engine(&ctx, EndpointType::Rep, SocketOptions::default(), &seen);
        let client = engine(&ctx, EndpointType::Req, SocketOptions::default(), &seen);
        let listener = server
            .listen(&Endpoint::parse("tcp://127.0.0.1:0").unwrap())
            .await
            .expect("listen");

        let dialer = client.dial(listener.url()).await.expect("dial");
        dialer.close();
        for _ in 0..200 {
            if client.pipes().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            client.pipes().is_empty(),
            "the dialer took its pipe with it"
        );

        server.close();
        assert!(server.is_closed());
        assert!(server.pipes().is_empty());
        let error = server.listen(listener.url()).await.unwrap_err();
        assert!(matches!(error, Error::ECLOSED(_)), "{error:?}");
    }

    /// Claim: a transport this engine does not carry is refused by name
    /// rather than accepted and quietly dropped, because SP's only way to
    /// refuse anything is a close and a silent one is a hang (§6).
    #[tokio::test]
    async fn an_uncarried_transport_is_refused_by_name() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let seen = counter();
        let socket = engine(&ctx, EndpointType::Bus, SocketOptions::default(), &seen);
        let error = socket
            .listen(&Endpoint::parse("inproc://orders").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ENOTSUP(_)), "{error:?}");
        assert!(error.cause().contains("inproc"));
    }
}
