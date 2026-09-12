//! REQ and REP: request-reply with contexts, request IDs and the resend
//! timer.
//!
//! "A cooked REQ sends one outstanding request per socket context and
//! normally spreads requests among peer REP sockets; the selected REP
//! receives then replies. REQ automatically resends until reply or timeout"
//! (`docs/research/nanomsg-nng.md` §4). Both halves are here: the spreading
//! is the round-robin over the pipes that can accept now, and the resending
//! is the socket's own clock.
//!
//! **A context is the unit, not the socket.** "`nng_ctx` shares its socket,
//! endpoints, and pipes but owns stateful protocol state such as a request
//! ID and retry timer, enabling independent concurrent transactions" (§2).
//! So [`ReqCtx`] holds the request ID, the outstanding body and the
//! deadline, and a socket is the one context it opened for itself — which
//! is exactly how `nng_send()` on a REQ socket relates to `nng_ctx_send()`.
//!
//! **Retry is not free, and a test says so.** "REQ retransmission creates
//! duplicate requests, including when a reply is lost; the protocol has no
//! deduplication key exposed to the REP beyond its routing header" (§7).
//!
//! **All three triggers.** "An outstanding request is resent after its
//! resend timer elapses, when the original peer disconnects, or when a peer
//! becomes available while it is waiting" (§4). One task per socket watches
//! all three, so a context holding an unanswered request is retried whether
//! or not anybody is inside `recv` at that moment.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use weida_sp::{Backtrace, EndpointType, backtrace};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::replier::{CtxId, Replier, ReplierCtx, framed};
use crate::socket::{SocketCore, socket_endpoints, within};

/// How often the socket's resend clock looks at its contexts.
///
/// NNG's own granularity: "the resend clock's default check granularity is
/// one second and is shared by the socket's contexts" (§4). The clock also
/// wakes when a deadline is armed or the pipe set changes, so this is a
/// ceiling on lateness rather than the mechanism.
pub const RESEND_GRANULARITY: Duration = Duration::from_secs(1);

/// Request IDs are 31 bits, "seeded at random and incremented per requester
/// context" [rfc-reqrep §5].
///
/// One stream for the whole process rather than one per context: it gives
/// the same uniqueness with no chance of two contexts of one socket
/// colliding, and the seed — taken once from the clock — is what keeps a
/// restarted process from reusing IDs its previous incarnation left in
/// flight, which is why the RFC asks for randomness at all.
static REQUEST_IDS: LazyLock<AtomicU32> = LazyLock::new(|| {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos() ^ (since.as_secs() as u32))
        .unwrap_or(1);
    AtomicU32::new(seed & backtrace::MAX_ID)
});

fn next_request_id() -> u32 {
    REQUEST_IDS.fetch_add(1, Ordering::Relaxed) & backtrace::MAX_ID
}

// --------------------------------------------------------------------------
// REQ
// --------------------------------------------------------------------------

/// One request a context is waiting for a reply to.
#[derive(Clone, Debug)]
struct Outstanding {
    id: u32,
    payload: Vec<u8>,
    /// The pipe it went to last, so that the pipe's disappearance is a
    /// resend trigger (§4).
    pipe: Option<PipeId>,
    /// When the resend clock should send it again.
    deadline: Instant,
}

#[derive(Debug, Default)]
struct ReqCtxState {
    outstanding: Option<Outstanding>,
    /// A reply another context's receive pumped in on our behalf.
    reply: Option<Message>,
    /// "Only one pending receive per context" (§4).
    receiving: bool,
}

struct ReqShared {
    core: Arc<SocketCore>,
    resend_time: Duration,
    contexts: Mutex<HashMap<CtxId, ReqCtxState>>,
    next_ctx: AtomicU64,
    /// Woken when a reply lands in somebody's slot.
    delivered: Notify,
    /// Woken when a deadline is armed, so the clock re-reads them.
    rearmed: Notify,
    resend: Mutex<Option<JoinHandle<()>>>,
}

impl ReqShared {
    fn lock(&self) -> MutexGuard<'_, HashMap<CtxId, ReqCtxState>> {
        self.contexts.lock().expect("req contexts poisoned")
    }
}

impl std::fmt::Debug for ReqShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqShared")
            .field("resend_time", &self.resend_time)
            .field("contexts", &self.lock().len())
            .finish_non_exhaustive()
    }
}

impl Drop for ReqShared {
    fn drop(&mut self) {
        if let Some(task) = self.resend.lock().expect("resend poisoned").take() {
            task.abort();
        }
    }
}

/// A REQ socket: one outstanding request per context, resent until
/// answered.
///
/// Cloning shares everything, because `nng_socket` is a value an
/// application copies (§2).
#[derive(Clone, Debug)]
pub struct ReqSocket {
    core: Arc<SocketCore>,
    shared: Arc<ReqShared>,
    /// The context a bare `send`/`recv` uses, which is what NNG gives every
    /// socket implicitly.
    implicit: ReqCtx,
}

impl ReqSocket {
    /// A REQ socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<ReqSocket> {
        ReqSocket::with_options(context, SocketOptions::default())
    }

    /// A REQ socket with `options`.
    ///
    /// `NNG_OPT_SENDBUF` and `NNG_OPT_RECVBUF` are refused here by name:
    /// REQ "permits only one outstanding transaction per context", so there
    /// is nothing for a depth to bound (§5).
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<ReqSocket> {
        let resend_time = options.resend_time;
        let core = Arc::new(SocketCore::new(context, EndpointType::Req, options)?);
        let shared = Arc::new(ReqShared {
            core: Arc::clone(&core),
            resend_time,
            contexts: Mutex::new(HashMap::new()),
            next_ctx: AtomicU64::new(1),
            delivered: Notify::new(),
            rearmed: Notify::new(),
            resend: Mutex::new(None),
        });
        let task = core.exec().spawn(resend_loop(Arc::clone(&shared)));
        *shared.resend.lock().expect("resend poisoned") = Some(task);
        let implicit = ReqCtx::open(&shared);
        Ok(ReqSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: a context with its own request ID, its own resend
    /// deadline and its own outstanding request, over this socket's pipes.
    pub fn context(&self) -> ReqCtx {
        ReqCtx::open(&self.shared)
    }

    /// The context a bare [`ReqSocket::send`] and [`ReqSocket::recv`] use.
    pub fn implicit_context(&self) -> ReqCtx {
        self.implicit.clone()
    }

    /// Sends a request on this socket's own context.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }

    /// Receives the reply to this socket's own outstanding request.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// How long a context waits for its reply before sending the request
    /// again: `NNG_OPT_REQ_RESENDTIME`.
    pub fn resend_time(&self) -> Duration {
        self.shared.resend_time
    }
}

socket_endpoints!(ReqSocket);

/// One REQ transaction: an `nng_ctx` on a REQ socket.
///
/// Cloning is what `nng_ctx` being a value means; the context is retired
/// when the last clone goes, and an outstanding request is forgotten with
/// it — its reply, if it ever arrives, is discarded, the same thing a newer
/// request does to an older one (§4).
#[derive(Clone, Debug)]
pub struct ReqCtx {
    handle: Arc<ReqHandle>,
}

#[derive(Debug)]
struct ReqHandle {
    id: CtxId,
    shared: Arc<ReqShared>,
}

impl Drop for ReqHandle {
    fn drop(&mut self) {
        self.shared.lock().remove(&self.id);
    }
}

impl ReqCtx {
    fn open(shared: &Arc<ReqShared>) -> ReqCtx {
        let id = CtxId::new(shared.next_ctx.fetch_add(1, Ordering::Relaxed));
        shared.lock().insert(id, ReqCtxState::default());
        ReqCtx {
            handle: Arc::new(ReqHandle {
                id,
                shared: Arc::clone(shared),
            }),
        }
    }

    /// This context's id, as `nng_ctx_id()` reports it.
    pub fn id(&self) -> CtxId {
        self.handle.id
    }

    fn shared(&self) -> &Arc<ReqShared> {
        &self.handle.shared
    }

    /// Sends `body` as a request, to one of the available repliers.
    ///
    /// The 32-bit request ID goes out with its terminal bit set, which is
    /// what tells a replier where the tag stack ends [rfc-reqrep §5] and
    /// what the reply is matched by.
    ///
    /// A second send cancels interest in the first reply: "sending a newer
    /// request cancels the requester's interest in its earlier reply and
    /// causes a late old reply to be discarded. It does not withdraw the
    /// earlier request from a replier" (§4).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let payload = body.into();
        let id = next_request_id();
        let pipe = self
            .shared()
            .core
            .send_round_robin(framed(&Backtrace::direct(id), &payload))
            .await?;
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            state.outstanding = Some(Outstanding {
                id,
                payload,
                pipe: Some(pipe),
                deadline: Instant::now() + self.shared().resend_time,
            });
            state.reply = None;
        }
        self.shared().rearmed.notify_waiters();
        Ok(())
    }

    /// Receives the reply to this context's outstanding request.
    ///
    /// `NNG_ESTATE` with no outstanding request, and `NNG_ESTATE` for a
    /// second concurrent receive on one context: "REQ receive without an
    /// active request, or a second concurrent receive, returns
    /// `NNG_ESTATE`" (§4).
    pub async fn recv(&self) -> Result<Message> {
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            if state.outstanding.is_none() {
                return Err(Error::ESTATE(
                    "this context has no outstanding request to receive a reply to".into(),
                ));
            }
            if state.receiving {
                return Err(Error::ESTATE(
                    "this context already has a receive in progress; one per context".into(),
                ));
            }
            state.receiving = true;
        }
        let _guard = Receiving {
            shared: self.shared(),
            id: self.id(),
        };
        let limit = self.shared().core.options().recv_timeout;
        within(self.shared().core.exec(), limit, self.wait_for_reply()).await
    }

    async fn wait_for_reply(&self) -> Result<Message> {
        loop {
            {
                let mut contexts = self.shared().lock();
                let state = contexts.entry(self.id()).or_default();
                if let Some(reply) = state.reply.take() {
                    state.outstanding = None;
                    return Ok(reply);
                }
            }
            // Nobody pumps on our behalf unless we pump ourselves: whichever
            // context is inside a receive drains the pipes and files each
            // reply under the request ID it carries.
            let delivered = self.shared().delivered.notified();
            if let Some((_, message)) = self.shared().core.try_take_any() {
                route(self.shared(), message);
                continue;
            }
            tokio::select! {
                () = delivered => {}
                () = self.shared().core.wait_for_message() => {}
            }
        }
    }
}

/// Releases the one-receive-per-context flag however the receive ends,
/// including a cancelled future and an expired `NNG_OPT_RECVTIMEO`.
struct Receiving<'a> {
    shared: &'a ReqShared,
    id: CtxId,
}

impl Drop for Receiving<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.shared.lock().get_mut(&self.id) {
            state.receiving = false;
        }
    }
}

/// Files an arrived reply under the context whose request ID it carries,
/// and discards it when no context is waiting for that ID — which is what a
/// cancelled request's late reply is (§4).
fn route(shared: &Arc<ReqShared>, message: Message) {
    let max_hops = shared.core.options().max_ttl;
    let Ok((stack, payload)) = backtrace::decode(message.body(), max_hops) else {
        // "If the reply is shorter than 32 bits, it is malformed and the
        // endpoint MUST ignore it" [rfc-reqrep §5].
        return;
    };
    let reply = framed(&stack, payload);
    let mut contexts = shared.lock();
    for state in contexts.values_mut() {
        if state
            .outstanding
            .as_ref()
            .is_some_and(|outstanding| outstanding.id == stack.id)
        {
            state.reply = Some(reply);
            drop(contexts);
            shared.delivered.notify_waiters();
            return;
        }
    }
}

/// The socket's resend clock: the three triggers, in one place.
///
/// It sleeps until the nearest deadline, or [`RESEND_GRANULARITY`] —
/// NNG's own one second — whichever is sooner, and is woken early by a
/// context arming a deadline or by the pipe set changing. A resend time
/// shorter than the granularity is therefore honoured rather than rounded
/// up to it.
async fn resend_loop(shared: Arc<ReqShared>) {
    loop {
        let rearmed = shared.rearmed.notified();
        let changed = shared.core.engine().changed();
        let tick = shared.core.exec().sleep(next_wake(&shared));
        tokio::select! {
            () = rearmed => {}
            () = changed => {}
            () = tick => {}
        }
        if shared.core.engine().is_closed() {
            return;
        }
        resend_due(&shared);
    }
}

/// How long the clock may sleep: until the nearest armed deadline, capped
/// at NNG's granularity so that a request with no deadline in sight still
/// has its peer-gone trigger looked at once a second.
fn next_wake(shared: &Arc<ReqShared>) -> Duration {
    let now = Instant::now();
    shared
        .lock()
        .values()
        .filter_map(|state| state.outstanding.as_ref())
        .map(|outstanding| outstanding.deadline.saturating_duration_since(now))
        .min()
        .unwrap_or(RESEND_GRANULARITY)
        .min(RESEND_GRANULARITY)
}

fn resend_due(shared: &Arc<ReqShared>) {
    let now = Instant::now();
    let live: Vec<PipeId> = shared.core.pipes().iter().map(|pipe| pipe.id()).collect();
    let due: Vec<(CtxId, u32, Vec<u8>)> = {
        let contexts = shared.lock();
        contexts
            .iter()
            .filter_map(|(id, state)| {
                let outstanding = state.outstanding.as_ref()?;
                // One: the timer elapsed. Two: the peer it went to is gone.
                // Three: it has no peer at all and one is available now
                // (§4).
                let timer = outstanding.deadline <= now;
                let peer_gone = outstanding.pipe.is_none_or(|pipe| !live.contains(&pipe));
                (timer || (peer_gone && !live.is_empty()))
                    .then(|| (*id, outstanding.id, outstanding.payload.clone()))
            })
            .collect()
    };
    for (ctx, id, payload) in due {
        // Round-robin again, so a resend after a disconnect reaches a
        // different replier — which is what makes the third trigger useful.
        let sent = shared
            .core
            .offer_round_robin(framed(&Backtrace::direct(id), &payload));
        let mut contexts = shared.lock();
        let Some(state) = contexts.get_mut(&ctx) else {
            continue;
        };
        let Some(outstanding) = state.outstanding.as_mut() else {
            continue;
        };
        match sent {
            Ok(pipe) => {
                outstanding.pipe = Some(pipe);
                outstanding.deadline = Instant::now() + shared.resend_time;
            }
            // Nowhere to send it right now. The request stays outstanding
            // with no pipe, which is trigger three the next time one
            // arrives; the deadline moves on so the clock waits rather
            // than spinning on a deadline it cannot satisfy.
            Err(_) => {
                outstanding.pipe = None;
                outstanding.deadline = Instant::now() + shared.resend_time;
            }
        }
    }
}

// --------------------------------------------------------------------------
// REP
// --------------------------------------------------------------------------

/// A REP socket: receive then reply, in that order, once per context.
///
/// The machine underneath is [`crate::replier`]'s, which RESPONDENT uses
/// too: the two protocols differ in the id in their header and in what the
/// terminal tag is called, and in nothing a replier does.
#[derive(Clone, Debug)]
pub struct RepSocket {
    core: Arc<SocketCore>,
    shared: Arc<Replier>,
    implicit: RepCtx,
}

/// One REP transaction: an `nng_ctx` on a REP socket.
pub type RepCtx = ReplierCtx;

impl RepSocket {
    /// A REP socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<RepSocket> {
        RepSocket::with_options(context, SocketOptions::default())
    }

    /// A REP socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<RepSocket> {
        let shared = Replier::new(
            context,
            EndpointType::Rep,
            options,
            "a REP context may send only the reply to a request it received",
        )?;
        let core = Arc::clone(shared.core());
        let implicit = ReplierCtx::open(&shared);
        Ok(RepSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: a context that processes one request
    /// independently of the others, which is how "several requests to be
    /// processed in parallel over one socket" works (§4).
    pub fn context(&self) -> RepCtx {
        ReplierCtx::open(&self.shared)
    }

    /// The context a bare [`RepSocket::recv`] and [`RepSocket::send`] use.
    pub fn implicit_context(&self) -> RepCtx {
        self.implicit.clone()
    }

    /// Receives a request on this socket's own context.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// Replies to the request this socket's own context received.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }
}

socket_endpoints!(RepSocket);
