//! Socket options the connection engine reads, with libzmq's names, numbers
//! and sentinels.
//!
//! Two rules from
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item 4
//! govern this module. An option is **honoured or refused**, never silently
//! ignored, so an unusable value fails where it is set rather than at the
//! first message. And every default here is libzmq's own
//! (`docs/research/zeromq.md` §11), except where 0013 §4.4 item 5 names the
//! deviation.
//!
//! **libzmq's sentinels become `Option`.** `-1` means "no reconnect" for
//! `ZMQ_RECONNECT_IVL` and "no limit" for `ZMQ_MAXMSGSIZE`; `0` means "no
//! backoff" for `ZMQ_RECONNECT_IVL_MAX`, "no limit" for `ZMQ_HANDSHAKE_IVL`
//! and "the OS default" for `ZMQ_CONNECT_TIMEOUT`. Three different meanings
//! for two numbers is a footgun in C and does not need reproducing: each one
//! is a `None` whose documentation says which libzmq value it is.

use std::time::Duration;

use weida_zmtp::SocketType;

use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::message::{DEFAULT_MAX_MESSAGE_FRAMES, DEFAULT_MAX_MESSAGE_SIZE, MessageLimits};
use crate::pipe::PipeConfig;

/// `ZMQ_RECONNECT_IVL` default: 100 ms (`docs/research/zeromq.md` §11).
pub const DEFAULT_RECONNECT_IVL: Duration = Duration::from_millis(100);

/// `ZMQ_HANDSHAKE_IVL` default: 30 s (`docs/research/zeromq.md` §11).
pub const DEFAULT_HANDSHAKE_IVL: Duration = Duration::from_secs(30);

/// `ZMQ_BACKLOG` default: 100 pending connections
/// (`docs/research/zeromq.md` §11).
pub const DEFAULT_BACKLOG: u32 = 100;

/// Addresses one hostname may resolve to before the rest are ignored.
///
/// **Not a libzmq option.** It exists because a resolver answer is remote
/// input and "no remote input can cause unbounded memory allocation"
/// (`docs/INVARIANTS.md`), and because `weida-runtime`'s resolver takes the
/// cap as an argument rather than owning one. More than one address is
/// necessary because the first is not necessarily reachable — `localhost`
/// commonly resolves to both `::1` and `127.0.0.1`.
pub const DEFAULT_MAX_RESOLVED_ADDRESSES: usize = 8;

/// Peers one socket admits from accepted connections, by default.
///
/// libzmq has no such option, so the number is ours to choose and to state.
/// 1024 is `ZMQ_MAX_SOCKETS`'s own order of magnitude and one above
/// `ZMQ_BACKLOG`'s 100, so a listener that fills its accept queue several
/// times over is still admitted; what it bounds is the product underneath —
/// at the default high-water marks and message size, 1024 peers is the
/// ceiling on `1024 × 2 × ZMQ_RCVHWM × ZMQ_MAXMSGSIZE` rather than on
/// nothing at all. A deployment that expects more peers than this raises it
/// deliberately, having seen that arithmetic.
pub const DEFAULT_MAX_PEERS: usize = 1024;

/// The largest `ZMQ_HEARTBEAT_TTL` the wire can carry: the field is
/// deciseconds in a `u16`, so 6553.5 s (`docs/research/zeromq.md` §11).
pub const MAX_HEARTBEAT_TTL: Duration = Duration::from_millis(6_553_500);

/// What a socket's connection engine is configured with.
///
/// The socket types of the later slices own the rest of the option surface —
/// subscriptions, routing ids, the XPUB flags — and keep these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketOptions {
    /// `ZMQ_RECONNECT_IVL`: how long to wait before reconnecting a dropped
    /// or refused outgoing connection. `None` is libzmq's `-1`: do not
    /// reconnect at all.
    pub reconnect_ivl: Option<Duration>,
    /// `ZMQ_RECONNECT_IVL_MAX`: the ceiling the interval doubles towards.
    /// `None` is libzmq's `0`: no backoff, retry at `reconnect_ivl` forever.
    pub reconnect_ivl_max: Option<Duration>,
    /// `ZMQ_HANDSHAKE_IVL`: how long a connection has to complete its
    /// handshake before it is closed. `None` is libzmq's `0`: no limit —
    /// which is also how an unauthenticated peer holds a connection open, so
    /// the default is finite.
    pub handshake_ivl: Option<Duration>,
    /// `ZMQ_CONNECT_TIMEOUT`: how long one connect attempt may take.
    /// `None` is libzmq's `0`: whatever the OS does, which on Linux is tens
    /// of seconds of SYN retries.
    pub connect_timeout: Option<Duration>,
    /// `ZMQ_IMMEDIATE`: queue only to completed connections.
    ///
    /// `false` — libzmq's default — lets a sender fill the queue of an
    /// endpoint that has never connected, which is what the pattern RFCs
    /// require ("SHALL maintain the double queue whether or not the
    /// connection is established") and what loses messages on a round-robin
    /// socket whose peer never arrives. `true` hides such a peer from the
    /// sender instead (`docs/research/zeromq.md` §5).
    pub immediate: bool,
    /// `ZMQ_BACKLOG`: pending connections the OS may hold on a bound
    /// endpoint.
    pub backlog: u32,
    /// `ZMQ_MAXMSGSIZE` for inbound messages. Finite by default, which is
    /// 0013 §4.4 item 5's second deliberate deviation — see
    /// [`DEFAULT_MAX_MESSAGE_SIZE`].
    pub max_message_size: u64,
    /// `ZMQ_HEARTBEAT_IVL`: how often to send a `PING`. `None` is libzmq's
    /// `0`: no heartbeat, and liveness is the transport's business.
    ///
    /// `PING`/`PONG` are ZMTP 3.1 commands, so a connection that negotiated
    /// 3.0 gets no heartbeat whatever this says — see
    /// [`crate::session::ZmtpSession`].
    pub heartbeat_ivl: Option<Duration>,
    /// `ZMQ_HEARTBEAT_TIMEOUT`: how long a peer may be silent before it is
    /// declared dead. `None` is libzmq's `0`, which means
    /// `ZMQ_HEARTBEAT_IVL` (`docs/research/zeromq.md` §11).
    pub heartbeat_timeout: Option<Duration>,
    /// `ZMQ_HEARTBEAT_TTL`: the hint carried in our `PING` telling the peer
    /// how long to wait for us before giving up. `None` is libzmq's `0`: no
    /// hint. The wire field is deciseconds in a `u16`, so the maximum is
    /// 6553.5 s and finer granularity is rounded down.
    pub heartbeat_ttl: Option<Duration>,
    /// `ZMQ_SNDTIMEO`: how long a blocking send may wait before reporting
    /// `EAGAIN`. `None` is libzmq's `-1`: wait forever.
    pub send_timeout: Option<Duration>,
    /// `ZMQ_RCVTIMEO`: the same bound on a blocking receive.
    pub recv_timeout: Option<Duration>,
    /// `ZMQ_REQ_CORRELATE`: prefix every REQ request with a request-id frame
    /// and discard replies that do not start with it
    /// (`docs/research/zeromq.md` §3).
    pub req_correlate: bool,
    /// `ZMQ_REQ_RELAXED`: let a REQ socket send a new request without having
    /// received the previous reply, abandoning that exchange rather than
    /// reporting `EFSM`.
    ///
    /// **Requires `req_correlate`, and this library refuses the pair rather
    /// than warning about it.** libzmq's own note: without correlation "a
    /// late reply to an aborted request can be reported as the reply to the
    /// superseding request", which is a wrong answer rather than a slow one.
    /// Refusing at configuration time is 0013 §4.4 item 4's rule; libzmq
    /// documents the hazard and allows it, and that difference is named here.
    pub req_relaxed: bool,
    /// `ZMQ_ROUTER_MANDATORY`: report an unroutable message instead of
    /// dropping it silently.
    ///
    /// libzmq: `0` "discards silently when it cannot be routed or the peer's
    /// SNDHWM is reached"; `1` reports `EHOSTUNREACH` when unroutable and
    /// `EAGAIN` at the high-water mark under `ZMQ_DONTWAIT`, blocking
    /// otherwise (`docs/research/zeromq.md` §4.2). ROUTER's own default is
    /// the brutal one, and it is kept: a ROUTER that blocked on one slow
    /// peer would stall every other.
    pub router_mandatory: bool,
    /// `ZMQ_ROUTER_HANDOVER`: let a newcomer claim an identity an existing
    /// peer already holds, disconnecting the incumbent.
    ///
    /// `false` — libzmq's default — rejects the newcomer instead, which is
    /// the safe answer when an identity is a name two peers may both believe
    /// they own.
    pub router_handover: bool,
    /// `ZMQ_PROBE_ROUTER`: send an empty message on every new connection, so
    /// that a ROUTER peer learns of this socket before it has anything to
    /// say.
    ///
    /// Legal on REQ, DEALER and ROUTER only — "the option must not be set
    /// against other socket types" — and refused at construction elsewhere.
    /// The receiving application must filter the empty message out.
    pub probe_router: bool,
    /// `ZMQ_ROUTING_ID`: the identity this socket announces in its `READY`,
    /// so that a ROUTER peer can address it by a name it chose rather than
    /// by a generated one (`docs/research/zeromq.md` §4.2).
    pub routing_id: Option<RoutingId>,
    /// Frames one inbound message may have — not a libzmq option, because
    /// 37/ZMTP has no such limit and an unbounded frame count is unbounded
    /// memory. See [`DEFAULT_MAX_MESSAGE_FRAMES`].
    pub max_message_frames: usize,
    /// Peers this socket will admit from **accepted** connections.
    ///
    /// **Not a libzmq option**, and the parity table says so in those terms:
    /// `ZMQ_MAX_SOCKETS` bounds sockets per context, `ZMQ_BACKLOG` bounds the
    /// kernel's accept queue, and neither bounds how many established
    /// connections one socket holds — libzmq has no option that does. Every
    /// admitted peer carries a queue of `ZMQ_RCVHWM` messages inbound and
    /// `ZMQ_SNDHWM` outbound, so without this the exposure is that product
    /// times a number a stranger chooses. See
    /// [`DEFAULT_MAX_PEERS`] for the default and its arithmetic. Connections
    /// this socket dialled are not counted: their number is how many times
    /// the application called `connect`.
    pub max_peers: usize,
    /// `ZMQ_SNDHWM`/`ZMQ_RCVHWM` and the mute action, per peer.
    pub pipe: PipeConfig,
    /// See [`DEFAULT_MAX_RESOLVED_ADDRESSES`].
    pub max_resolved_addresses: usize,
}

impl Default for SocketOptions {
    fn default() -> SocketOptions {
        SocketOptions {
            reconnect_ivl: Some(DEFAULT_RECONNECT_IVL),
            reconnect_ivl_max: None,
            handshake_ivl: Some(DEFAULT_HANDSHAKE_IVL),
            connect_timeout: None,
            immediate: false,
            backlog: DEFAULT_BACKLOG,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_message_frames: DEFAULT_MAX_MESSAGE_FRAMES,
            max_peers: DEFAULT_MAX_PEERS,
            heartbeat_ivl: None,
            heartbeat_timeout: None,
            heartbeat_ttl: None,
            send_timeout: None,
            recv_timeout: None,
            req_correlate: false,
            req_relaxed: false,
            router_mandatory: false,
            router_handover: false,
            probe_router: false,
            routing_id: None,
            pipe: PipeConfig::default(),
            max_resolved_addresses: DEFAULT_MAX_RESOLVED_ADDRESSES,
        }
    }
}

impl SocketOptions {
    /// Refuses a configuration that cannot be delivered, with `EINVAL` and a
    /// message naming the value.
    pub fn validate(&self) -> Result<()> {
        if self.max_message_size == 0 {
            return Err(Error::EINVAL(
                "ZMQ_MAXMSGSIZE is zero, so every message would be refused".into(),
            ));
        }
        if self.req_relaxed && !self.req_correlate {
            return Err(Error::EINVAL(
                "ZMQ_REQ_RELAXED without ZMQ_REQ_CORRELATE lets a late reply to an abandoned \
                 request be reported as the reply to the one that superseded it; libzmq \
                 documents that hazard and this library refuses it"
                    .into(),
            ));
        }
        if self.max_peers == 0 {
            return Err(Error::EINVAL(
                "max_peers is zero, so this socket could accept no connection at all".into(),
            ));
        }
        if self.max_message_frames == 0 {
            return Err(Error::EINVAL(
                "max_message_frames is zero, so every message would be refused".into(),
            ));
        }
        if self.max_resolved_addresses == 0 {
            return Err(Error::EINVAL(
                "max_resolved_addresses is zero, so no hostname could be dialled".into(),
            ));
        }
        if let (Some(ivl), Some(max)) = (self.reconnect_ivl, self.reconnect_ivl_max)
            && max < ivl
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_RECONNECT_IVL_MAX ({max:?}) is below ZMQ_RECONNECT_IVL ({ivl:?}), \
                     so the backoff would count downwards"
                )
                .into(),
            ));
        }
        if self.reconnect_ivl.is_none() && self.reconnect_ivl_max.is_some() {
            return Err(Error::EINVAL(
                "ZMQ_RECONNECT_IVL_MAX is set while reconnection is disabled \
                 (ZMQ_RECONNECT_IVL = -1)"
                    .into(),
            ));
        }
        if let Some(ttl) = self.heartbeat_ttl
            && ttl > MAX_HEARTBEAT_TTL
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_HEARTBEAT_TTL travels as deciseconds in a u16, so its maximum is \
                     {MAX_HEARTBEAT_TTL:?}; this one is {ttl:?}"
                )
                .into(),
            ));
        }
        if self.heartbeat_timeout.is_some() && self.heartbeat_ivl.is_none() {
            return Err(Error::EINVAL(
                "ZMQ_HEARTBEAT_TIMEOUT is set while the heartbeat is off \
                 (ZMQ_HEARTBEAT_IVL = 0), so nothing would ever measure the silence"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Refuses an option this socket *type* cannot honour, which no
    /// type-blind [`SocketOptions::validate`] can see.
    ///
    /// 0013 §4.4 item 4's rule is "honoured or refused, never silently
    /// ignored", and an option that means nothing for a socket type is the
    /// case libzmq itself names: `ZMQ_PROBE_ROUTER` "must not be set against
    /// other socket types" (`docs/research/zeromq.md` §4.2).
    pub fn validate_for(&self, socket_type: SocketType) -> Result<()> {
        if self.probe_router
            && !matches!(
                socket_type,
                SocketType::Req | SocketType::Dealer | SocketType::Router
            )
        {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_PROBE_ROUTER is a REQ, DEALER and ROUTER option; a {} socket \
                     cannot honour it",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        if (self.router_mandatory || self.router_handover) && socket_type != SocketType::Router {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_ROUTER_MANDATORY and ZMQ_ROUTER_HANDOVER are ROUTER options; a {} \
                     socket cannot honour them",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        if (self.req_correlate || self.req_relaxed) && socket_type != SocketType::Req {
            return Err(Error::EINVAL(
                format!(
                    "ZMQ_REQ_CORRELATE and ZMQ_REQ_RELAXED are REQ options; a {} socket \
                     cannot honour them",
                    socket_type.as_str()
                )
                .into(),
            ));
        }
        Ok(())
    }

    /// What bounds one inbound message: `ZMQ_MAXMSGSIZE` and the frame
    /// ceiling, which the session hands to every connection it drives.
    pub const fn message_limits(&self) -> MessageLimits {
        MessageLimits::new(self.max_message_size, self.max_message_frames)
    }

    /// The next reconnect interval after `previous`, doubling towards
    /// `ZMQ_RECONNECT_IVL_MAX`.
    ///
    /// libzmq: with `_IVL_MAX` above `_IVL` the interval grows exponentially
    /// up to that ceiling, and with `_IVL_MAX` at its default the interval
    /// never changes (`docs/research/zeromq.md` §11). No jitter is added
    /// here; libzmq randomizes and this does not, which matters only to a
    /// thundering herd of reconnecting peers and is a named difference rather
    /// than an oversight.
    ///
    /// Returns `None` when reconnection is disabled.
    pub fn next_reconnect_ivl(&self, previous: Option<Duration>) -> Option<Duration> {
        let base = self.reconnect_ivl?;
        let Some(previous) = previous else {
            return Some(base);
        };
        match self.reconnect_ivl_max {
            None => Some(base),
            Some(max) => Some(previous.saturating_mul(2).min(max)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: with no `_IVL_MAX` the interval never changes, and with one it
    /// doubles up to that ceiling and stops there.
    #[test]
    fn the_backoff_doubles_towards_the_ceiling() {
        let flat = SocketOptions::default();
        assert_eq!(
            flat.next_reconnect_ivl(None),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            flat.next_reconnect_ivl(Some(Duration::from_millis(100))),
            Some(Duration::from_millis(100)),
            "no ceiling means no backoff"
        );

        let backing_off = SocketOptions {
            reconnect_ivl: Some(Duration::from_millis(100)),
            reconnect_ivl_max: Some(Duration::from_millis(350)),
            ..SocketOptions::default()
        };
        let mut ivl = backing_off.next_reconnect_ivl(None);
        assert_eq!(ivl, Some(Duration::from_millis(100)));
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(200)));
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(350)), "capped at _IVL_MAX");
        ivl = backing_off.next_reconnect_ivl(ivl);
        assert_eq!(ivl, Some(Duration::from_millis(350)), "and it stays there");
    }

    /// Claim: `ZMQ_RECONNECT_IVL = -1` disables reconnection, and nothing
    /// resurrects it.
    #[test]
    fn reconnection_can_be_disabled() {
        let once = SocketOptions {
            reconnect_ivl: None,
            reconnect_ivl_max: None,
            ..SocketOptions::default()
        };
        assert_eq!(once.next_reconnect_ivl(None), None);
        assert_eq!(once.next_reconnect_ivl(Some(Duration::from_secs(1))), None);
        once.validate().expect("disabling reconnection is legal");
    }

    /// Claim: a configuration that cannot be delivered is refused where it is
    /// set, with the value named.
    #[test]
    fn unusable_options_are_refused() {
        // The defaults are a configuration this library can deliver, which
        // is the only claim about them worth a test: the numbers themselves
        // are libzmq's and are stated where they are defined.
        SocketOptions::default()
            .validate()
            .expect("the defaults are usable");

        for broken in [
            SocketOptions {
                max_message_size: 0,
                ..SocketOptions::default()
            },
            SocketOptions {
                max_resolved_addresses: 0,
                ..SocketOptions::default()
            },
            SocketOptions {
                reconnect_ivl: Some(Duration::from_millis(500)),
                reconnect_ivl_max: Some(Duration::from_millis(100)),
                ..SocketOptions::default()
            },
            SocketOptions {
                reconnect_ivl: None,
                reconnect_ivl_max: Some(Duration::from_millis(100)),
                ..SocketOptions::default()
            },
        ] {
            let err = broken.validate().unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(!err.cause().is_empty());
        }
    }
}
