//! Raw sockets and the device: the wire, with the protocol taken out.
//!
//! "Cooked sockets enforce the pattern state machines and headers; raw
//! constructors bypass them, leaving send/receive and header semantics to
//! the application. `nng_device()` requires raw sockets but just forwards
//! messages" (`docs/research/nanomsg-nng.md` §4).
//!
//! **What a raw socket keeps and what it gives up.** It keeps the
//! handshake, the pairing rule, the framing, the pipes and every bound —
//! those are the transport, not the pattern. It gives up the state machine
//! (`NNG_ESTATE` has nothing to be in state *for*), the retry, the
//! matching, the local filtering and the header manipulation: a raw
//! receive hands over the payload exactly as it arrived, tag stack and hop
//! count included, and a raw send writes exactly what it is given.
//!
//! **Contexts are refused, and the reason is the definition.** "Raw
//! sockets cannot use contexts: raw mode makes the application responsible
//! for the omitted protocol state" (§4). An `nng_ctx` *is* per-transaction
//! protocol state, so on a socket that deliberately holds none there is
//! nothing for one to hold — which is why [`RawSocket::context`] returns
//! [`Infallible`]: the type says there is no
//! context to be had, and the error says why.
//!
//! **The device is two raw sockets and a routing rule.** "Protocol header
//! designs such as REQ/REP backtraces and PAIR v1 TTL supply the routing
//! or loop information that a forwarding topology needs" (§4). [`device`]
//! is that rule and nothing else: it adds no processing, no queueing
//! discipline and no retry, and what it does with a header depends only on
//! which protocol the socket it arrived on speaks.

use std::convert::Infallible;
use std::sync::Arc;

use weida_sp::{Backtrace, EndpointType, backtrace, pair};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::socket::{Broadcast, SocketCore, socket_endpoints};

/// A raw socket of any SP protocol: the wire, with the pattern's state
/// machine absent.
#[derive(Clone, Debug)]
pub struct RawSocket {
    core: Arc<SocketCore>,
}

impl RawSocket {
    /// A raw socket speaking `protocol`, with NNG's defaults.
    ///
    /// This is `nng_<protocol>_open_raw()`, and every SP protocol has one:
    /// raw mode is a property of the socket rather than of the protocol
    /// (§4).
    pub fn open(context: &Context, protocol: EndpointType) -> Result<RawSocket> {
        RawSocket::with_options(context, protocol, SocketOptions::default())
    }

    /// A raw socket with `options`.
    ///
    /// The buffer options are refused for the same protocols a cooked
    /// socket refuses them on, because that refusal is about the protocol's
    /// transaction model and not about the state machine raw mode drops.
    pub fn with_options(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
    ) -> Result<RawSocket> {
        Ok(RawSocket {
            core: Arc::new(SocketCore::new(context, protocol, options)?),
        })
    }

    /// Refused: a raw socket has no per-transaction state for a context to
    /// hold (§4).
    ///
    /// The return type is [`Infallible`], so the
    /// absence is in the signature rather than only in the error message.
    pub fn context(&self) -> Result<Infallible> {
        Err(Error::ENOTSUP(
            format!(
                "a raw {} socket holds no protocol state, so a context would hold nothing; \
                 raw mode makes the application responsible for what a context would have kept",
                crate::protocol::protocol(self.core.protocol()).name
            )
            .into(),
        ))
    }

    /// Sends `message` exactly as given — header then body, with nothing
    /// added and nothing checked — to the next pipe that can accept it.
    pub async fn send(&self, message: Message) -> Result<PipeId> {
        self.core.send_round_robin(message).await
    }

    /// Sends `message` to one named pipe, which is how a reverse route is
    /// followed.
    pub async fn send_to(&self, pipe: PipeId, message: Message) -> Result<()> {
        self.core.send_to(pipe, message).await.map(|_| ())
    }

    /// Offers `message` to every pipe, which is what a raw PUB, BUS or
    /// SURVEYOR send is.
    pub fn send_to_all(&self, message: &Message) -> Broadcast {
        self.core.send_to_all(message)
    }

    /// Offers `message` to every pipe except `pipe` — a raw BUS
    /// re-broadcast, which "excludes the incoming pipe ID … preventing
    /// immediate reflection in a one-socket device" (§4).
    pub fn send_to_all_excluding(&self, pipe: PipeId, message: &Message) -> Broadcast {
        let mut broadcast = Broadcast::default();
        for target in self.core.pipes() {
            if target.id() == pipe {
                continue;
            }
            match target.outgoing().offer(message.clone()) {
                Ok(()) => broadcast.queued += 1,
                Err(_) => broadcast.dropped += 1,
            }
        }
        broadcast
    }

    /// Receives the next message from any pipe, with its wire payload
    /// untouched.
    ///
    /// The returned message's body is exactly the octets that followed the
    /// length field — tag stack, hop count, pipe id and all — because
    /// deciding what any of them mean is what raw mode hands back to the
    /// application. The pipe it arrived on comes beside it, which is what
    /// a raw BUS would otherwise read out of its header.
    pub async fn recv(&self) -> Result<(PipeId, Message)> {
        self.core.recv_any().await
    }

    /// The non-blocking form.
    pub fn try_recv(&self) -> Result<(PipeId, Message)> {
        self.core.try_recv_any()
    }

    /// The protocol this socket speaks.
    pub fn raw_protocol(&self) -> EndpointType {
        self.core.protocol()
    }
}

socket_endpoints!(RawSocket);

/// What a forwarder does with a message, decided by the protocol of the
/// socket it arrived on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Routing {
    /// Push the ingress pipe id onto the front of the tag stack and send
    /// the message onward. "A device prepends its local peer ID on request
    /// reception" (§4).
    Push,
    /// Pop the front of the tag stack and use it to select the pipe the
    /// message goes back out on. "Each forwarder pops its ID to select the
    /// reverse pipe" (§4).
    Pop,
    /// Increment the PAIR v1 hop count and drop the message past
    /// `NNG_OPT_MAXTTL`. "A forwarder's local `MAXTTL` policy bounds
    /// forwarding" (§4).
    Hop,
    /// Forward the payload unchanged: the protocol carries no routing
    /// information at all.
    Plain,
}

impl Routing {
    const fn of(protocol: EndpointType) -> Routing {
        match protocol {
            // The sides that receive a request or a survey are the sides
            // that push: the stack has to grow on the way in so that it can
            // shrink on the way back.
            EndpointType::Rep | EndpointType::Respondent => Routing::Push,
            // The sides that receive an answer pop what they pushed.
            EndpointType::Req | EndpointType::Surveyor => Routing::Pop,
            EndpointType::PairV1 => Routing::Hop,
            _ => Routing::Plain,
        }
    }
}

/// Whether a destination protocol delivers to every peer or to one.
const fn broadcasts(protocol: EndpointType) -> bool {
    matches!(
        protocol,
        EndpointType::Pub | EndpointType::Bus | EndpointType::Surveyor
    )
}

/// What one forwarded message did, for a caller that wants to count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Forwarded {
    /// Messages handed to the other socket.
    pub forwarded: u64,
    /// Messages dropped: a hop count past `NNG_OPT_MAXTTL`, a malformed
    /// tag stack, a reverse route whose pipe has gone, or a peer that
    /// could not take the copy. Every one of them is silent on the wire
    /// (§6, §8).
    pub dropped: u64,
}

/// `nng_device()`: forwards messages between two raw sockets, in both
/// directions, until either is closed.
///
/// The two sockets must speak compatible protocols — the pairing rule of
/// the SP registry — which is the same rule a pipe is admitted under. A
/// device between a raw REP socket (which REQ clients dial) and a raw REQ
/// socket (which dials REP servers) is the load-balancing intermediary the
/// sheet describes (§9).
///
/// **It adds nothing.** No retry, no matching, no state machine and no
/// application processing: "`nng_device()` operates on raw sockets and
/// forwards messages without adding application processing" (§4). What it
/// does do is the header bookkeeping the topology needs: push and pop the
/// tag stack for request-reply and survey, increment and bound the PAIR v1
/// hop count, and exclude the ingress pipe when a raw BUS re-broadcasts
/// through one socket.
///
/// Fails with `NNG_ENOTSUP` when the two protocols cannot pair, because a
/// device between incompatible sockets would forward messages nobody can
/// interpret.
pub async fn device(first: &RawSocket, second: &RawSocket) -> Result<Forwarded> {
    let left = first.raw_protocol();
    let right = second.raw_protocol();
    if left.peer() != right {
        return Err(Error::ENOTSUP(
            format!(
                "a device needs two raw sockets that can pair: {left:?} pairs with {:?}, \
                 not with {right:?}",
                left.peer()
            )
            .into(),
        ));
    }

    let mut tally = Forwarded::default();
    loop {
        if first.core.engine().is_closed() || second.core.engine().is_closed() {
            return Ok(tally);
        }
        let taken = match first.core.try_take_any() {
            Some(taken) => Some((true, taken)),
            None => second.core.try_take_any().map(|taken| (false, taken)),
        };
        let Some((from_first, (pipe, message))) = taken else {
            tokio::select! {
                () = first.core.wait_for_message() => {}
                () = second.core.wait_for_message() => {}
            }
            continue;
        };
        let (from, to) = if from_first {
            (first, second)
        } else {
            (second, first)
        };
        match forward(from, to, pipe, message).await {
            Ok(()) => tally.forwarded += 1,
            Err(_) => tally.dropped += 1,
        }
    }
}

/// One message across the device.
async fn forward(
    from: &RawSocket,
    to: &RawSocket,
    ingress: PipeId,
    message: Message,
) -> Result<()> {
    let max_hops = from.core.options().max_ttl;
    let payload = message.into_body();
    let onward = match Routing::of(from.raw_protocol()) {
        Routing::Push => {
            let (mut stack, body) = backtrace::decode(&payload, max_hops)
                .map_err(|why| Error::EPROTO(why.to_string().into()))?;
            // The forwarder's own peer id goes in front of whatever is
            // already there, so the outermost hop is popped first (§4).
            let mut peers = vec![ingress.get()];
            peers.append(&mut stack.peers);
            let pushed = Backtrace {
                peers,
                id: stack.id,
            };
            Some(Message::from_parts(pushed.encode(), body.to_vec()))
        }
        Routing::Pop => {
            let (mut stack, body) = backtrace::decode(&payload, max_hops)
                .map_err(|why| Error::EPROTO(why.to_string().into()))?;
            if stack.peers.is_empty() {
                // Nothing left to pop: this answer was not routed through
                // here, and there is nowhere to send it.
                return Err(Error::EPROTO(
                    "a reply reached this device with an empty peer stack".into(),
                ));
            }
            let reverse = PipeId::new(stack.peers.remove(0));
            let popped = Message::from_parts(stack.encode(), body.to_vec());
            to.send_to(reverse, popped).await?;
            None
        }
        Routing::Hop => {
            let (hops, body) = pair::decode(&payload, max_hops as u32)
                .map_err(|why| Error::EPROTO(why.to_string().into()))?;
            let next = pair::next_hop(hops);
            if next as usize > max_hops {
                // "Reply past the local hop ceiling: dropped on receipt,
                // the pipe kept, and nothing is sent back" (§8).
                return Err(Error::EPROTO(
                    format!("hop count {next} is past this device's MAXTTL of {max_hops}").into(),
                ));
            }
            Some(Message::from_parts(
                next.to_be_bytes().to_vec(),
                body.to_vec(),
            ))
        }
        Routing::Plain => Some(Message::from_body(payload)),
    };

    let Some(onward) = onward else {
        return Ok(());
    };
    if broadcasts(to.raw_protocol()) {
        // A single-socket device re-broadcasting a BUS message must not
        // reflect it to the pipe it came from (§4).
        let broadcast = if std::ptr::eq(from, to) {
            to.send_to_all_excluding(ingress, &onward)
        } else {
            to.send_to_all(&onward)
        };
        if broadcast.queued == 0 && broadcast.dropped > 0 {
            return Err(Error::ETIMEDOUT("no peer took the forwarded copy".into()));
        }
        return Ok(());
    }
    to.send(onward).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the routing rule is read off the protocol, so a device does
    /// not have to be told which of its two sockets faces the requesters.
    #[test]
    fn the_routing_rule_comes_from_the_protocol() {
        assert_eq!(Routing::of(EndpointType::Rep), Routing::Push);
        assert_eq!(Routing::of(EndpointType::Respondent), Routing::Push);
        assert_eq!(Routing::of(EndpointType::Req), Routing::Pop);
        assert_eq!(Routing::of(EndpointType::Surveyor), Routing::Pop);
        assert_eq!(Routing::of(EndpointType::PairV1), Routing::Hop);
        assert_eq!(Routing::of(EndpointType::PairV0), Routing::Plain);
        assert_eq!(Routing::of(EndpointType::Push), Routing::Plain);
        assert!(broadcasts(EndpointType::Pub));
        assert!(broadcasts(EndpointType::Bus));
        assert!(!broadcasts(EndpointType::Push));
    }
}
