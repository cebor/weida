//! The replying half of a tag-stack protocol, written once.
//!
//! REP and RESPONDENT are the same machine. Both receive a message whose
//! body begins with the 32-bit tag stack, both remember the stack and the
//! pipe it arrived on, both may send only the answer to what they
//! received, and both return the reply toward the originator of the most
//! recently received message (`docs/research/nanomsg-nng.md` §4). The only
//! differences are the protocol id in the header and what the terminal tag
//! is called — a request ID for REP, a survey ID for RESPONDENT — and
//! neither of those is behaviour.
//!
//! So the machine lives here and [`crate::reqrep::RepSocket`] and
//! [`crate::survey::RespondentSocket`] are two sockets over it. Writing it
//! twice would mean two places for the `NNG_ESTATE` rule to drift apart.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use weida_sp::{Backtrace, EndpointType, backtrace};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::socket::{SocketCore, within};

/// A context's identity within its socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CtxId(u64);

impl CtxId {
    /// A context id from a counter.
    pub(crate) const fn new(id: u64) -> CtxId {
        CtxId(id)
    }

    /// The number, as `nng_ctx_id()` reports it.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for CtxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "context {}", self.0)
    }
}

/// The wire form of a request, a survey or their answer: the tag stack,
/// then the payload.
pub(crate) fn framed(stack: &Backtrace, payload: &[u8]) -> Message {
    Message::from_parts(stack.encode(), payload.to_vec())
}

#[derive(Clone, Debug)]
struct Answering {
    stack: Backtrace,
    pipe: PipeId,
}

#[derive(Debug, Default)]
struct ReplierCtxState {
    /// The stack and the pipe of the message this context is answering.
    answering: Option<Answering>,
    receiving: bool,
}

/// What every context of one replying socket shares.
pub struct Replier {
    core: Arc<SocketCore>,
    contexts: Mutex<HashMap<CtxId, ReplierCtxState>>,
    next_ctx: AtomicU64,
    /// What a send without a receive is called, in this protocol's words.
    refusal: &'static str,
}

impl Replier {
    /// A replying socket speaking `protocol`.
    pub(crate) fn new(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        refusal: &'static str,
    ) -> Result<Arc<Replier>> {
        let core = Arc::new(SocketCore::new(context, protocol, options)?);
        Ok(Arc::new(Replier {
            core,
            contexts: Mutex::new(HashMap::new()),
            next_ctx: AtomicU64::new(1),
            refusal,
        }))
    }

    /// The socket underneath, for the endpoint surface.
    pub(crate) fn core(&self) -> &Arc<SocketCore> {
        &self.core
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<CtxId, ReplierCtxState>> {
        self.contexts.lock().expect("replier contexts poisoned")
    }
}

impl std::fmt::Debug for Replier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replier")
            .field("contexts", &self.lock().len())
            .finish_non_exhaustive()
    }
}

/// One replying transaction: an `nng_ctx` on a REP or RESPONDENT socket.
///
/// Cloning is what `nng_ctx` being a value means; the context is retired
/// when the last clone goes.
#[derive(Clone, Debug)]
pub struct ReplierCtx {
    handle: Arc<ReplierHandle>,
}

#[derive(Debug)]
struct ReplierHandle {
    id: CtxId,
    shared: Arc<Replier>,
}

impl Drop for ReplierHandle {
    fn drop(&mut self) {
        self.shared.lock().remove(&self.id);
    }
}

impl ReplierCtx {
    pub(crate) fn open(shared: &Arc<Replier>) -> ReplierCtx {
        let id = CtxId(shared.next_ctx.fetch_add(1, Ordering::Relaxed));
        shared.lock().insert(id, ReplierCtxState::default());
        ReplierCtx {
            handle: Arc::new(ReplierHandle {
                id,
                shared: Arc::clone(shared),
            }),
        }
    }

    /// This context's id.
    pub fn id(&self) -> CtxId {
        self.handle.id
    }

    fn shared(&self) -> &Arc<Replier> {
        &self.handle.shared
    }

    /// Receives one message, remembering how to answer it.
    ///
    /// A second concurrent receive on one context is `NNG_ESTATE`: "a
    /// second simultaneous receive is likewise rejected" (§4).
    pub async fn recv(&self) -> Result<Message> {
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
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
        within(self.shared().core.exec(), limit, self.take()).await
    }

    async fn take(&self) -> Result<Message> {
        let max_hops = self.shared().core.options().max_ttl;
        loop {
            let Some((pipe, message)) = self.shared().core.try_take_any() else {
                self.shared().core.wait_for_message().await;
                continue;
            };
            let Ok((stack, payload)) = backtrace::decode(message.body(), max_hops) else {
                // A stack with no terminator inside the local `MAXTTL`, or
                // one that ends inside a tag, is malformed rather than
                // merely deep: "if the reply is shorter than 32 bits, it is
                // malformed and the endpoint MUST ignore it"
                // [rfc-reqrep §5], and NNG closes the pipe on a truncated
                // tag rather than reading the next message out of a stream
                // it can no longer trust (§8). So the pipe goes and the
                // loop looks at the others.
                self.shared().core.engine().close_pipe(pipe);
                continue;
            };
            let received = framed(&stack, payload);
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            state.answering = Some(Answering { stack, pipe });
            return Ok(received);
        }
    }

    /// Answers what this context received.
    ///
    /// `NNG_ESTATE` when nothing was received: "a cooked REP may send only
    /// after receiving its corresponding request … violations return
    /// `NNG_ESTATE`" (§4), and a respondent needs a received survey for the
    /// same reason.
    ///
    /// An answer whose originator has gone is discarded rather than
    /// reported: SP has no way to tell anybody, and the originator's own
    /// timer — a resend for REQ, a survey deadline for SURVEYOR — is what
    /// recovers (§4, §6).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let Some(answering) = self
            .shared()
            .lock()
            .get_mut(&self.id())
            .and_then(|state| state.answering.take())
        else {
            return Err(Error::ESTATE(self.shared().refusal.into()));
        };
        let reply = framed(&answering.stack, &body.into());
        match self.shared().core.send_to(answering.pipe, reply).await {
            Ok(_) => Ok(()),
            Err(Error::ECLOSED(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }
}

struct Receiving<'a> {
    shared: &'a Replier,
    id: CtxId,
}

impl Drop for Receiving<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.shared.lock().get_mut(&self.id) {
            state.receiving = false;
        }
    }
}
