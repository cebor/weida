//! PUSH and PULL: the pipeline.
//!
//! "A PUSH selects one connected puller able to receive, round-robin among
//! available peers; unavailable peers are excluded by flow control. With no
//! eligible peer, the send waits or times out. PULL receives as messages
//! arrive. If two peers have messages ready, their order is undefined; PULL
//! cannot send and PUSH cannot receive" (`docs/research/nanomsg-nng.md` §4).
//!
//! **Availability is now, not turn order.** "PUSH selects only a puller
//! capable of accepting a message, so its rotation is over the ready subset
//! rather than a static worker list. This is load distribution by immediate
//! acceptance, not an advertised capacity or service-rate protocol" (§4).
//! A puller whose queue is full is skipped on this pass and reconsidered on
//! the next one; nothing is held for it.
//!
//! **The directions are types, not conventions.** A `PushSocket` has no
//! `recv` and a `PullSocket` has no `send`, so the manual's "PUSH has no
//! receive operation; PULL has no send operation" (§4) is a compile error
//! rather than a runtime one. `tests/directions.rs` asserts exactly that,
//! because an absent method cannot be tested by calling it.
//!
//! **And nothing here promises delivery.** "The pipeline manual says flow
//! control attempts to avoid drops but gives no delivery guarantee and no
//! acknowledgement. A process that needs confirmation of completed work
//! must add it above the pattern" (§4). A send that returned means a peer's
//! queue took the message, which is not the same as a worker having done
//! anything with it.

use std::sync::Arc;

use weida_sp::EndpointType;

use crate::context::Context;
use crate::error::Result;
use crate::message::Message;
use crate::options::SocketOptions;
use crate::socket::{SocketCore, socket_endpoints};

/// A PUSH socket: send only, round-robin over the pullers that can accept
/// a message now.
#[derive(Clone, Debug)]
pub struct PushSocket {
    core: Arc<SocketCore>,
}

impl PushSocket {
    /// A PUSH socket on `context`, with NNG's defaults — including
    /// `NNG_OPT_SENDBUF` of zero, which is the depth PUSH documents (§5)
    /// and the reason a send waits for a puller rather than piling up
    /// behind one.
    pub fn new(context: &Context) -> Result<PushSocket> {
        PushSocket::with_options(context, SocketOptions::default())
    }

    /// A PUSH socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<PushSocket> {
        Ok(PushSocket {
            core: Arc::new(SocketCore::new(context, EndpointType::Push, options)?),
        })
    }

    /// Sends `body` to the next puller that can accept it.
    ///
    /// Waits when none can — including when there is no puller at all —
    /// and reports `NNG_ETIMEDOUT` once `NNG_OPT_SENDTIMEO` expires, which
    /// is "the send waits or times out" (§4). Nothing is discarded: PUSH's
    /// action at a full queue is to block (§12/P4).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.core
            .send_round_robin(Message::from_body(body.into()))
            .await
            .map(|_| ())
    }

    /// The non-blocking form: `NNG_ETIMEDOUT` at once when no puller can
    /// accept a message, rather than a wait.
    pub fn try_send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.core
            .try_send_round_robin(Message::from_body(body.into()))
            .map(|_| ())
    }
}

socket_endpoints!(PushSocket);

/// A PULL socket: receive only, fair-queued over its peers.
#[derive(Clone, Debug)]
pub struct PullSocket {
    core: Arc<SocketCore>,
}

impl PullSocket {
    /// A PULL socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<PullSocket> {
        PullSocket::with_options(context, SocketOptions::default())
    }

    /// A PULL socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<PullSocket> {
        Ok(PullSocket {
            core: Arc::new(SocketCore::new(context, EndpointType::Pull, options)?),
        })
    }

    /// Receives the next message from any pusher.
    ///
    /// "If two peers have messages ready, their order is undefined" (§4):
    /// this rotates over the peers so that one busy pusher cannot starve
    /// the others, and promises nothing beyond that. Bounded by
    /// `NNG_OPT_RECVTIMEO`.
    pub async fn recv(&self) -> Result<Message> {
        self.core.recv_any().await.map(|(_, message)| message)
    }

    /// The non-blocking form: `NNG_EAGAIN` when nothing has arrived.
    pub fn try_recv(&self) -> Result<Message> {
        self.core.try_recv_any().map(|(_, message)| message)
    }
}

socket_endpoints!(PullSocket);
