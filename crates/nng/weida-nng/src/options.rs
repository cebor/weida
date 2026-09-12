//! What a socket is configured with, honoured or refused where it is set.
//!
//! Every field is an NNG option under NNG's own name, except the three this
//! library adds because SP bounds nothing and something must
//! (`docs/INVARIANTS.md`): [`SocketOptions::max_pipes`],
//! [`SocketOptions::handshake_timeout`] and
//! [`SocketOptions::max_addresses`]. Each of those says in its own
//! documentation that it is ours and why NNG has nothing to compare it to.
//!
//! The rule this module exists to keep is
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item 4:
//! a value this library cannot deliver fails **where it is configured**,
//! with the NNG code that names the reason, and is never silently corrected
//! or ignored. [`SocketOptions::validate_for`] is that check for the half
//! only the protocol can judge — `NNG_OPT_SENDBUF` on a REQ socket is the
//! sheet's own example (§5).

use std::time::Duration;

use weida_sp::EndpointType;

use crate::error::Result;
use crate::message::DEFAULT_RECV_MAX_SIZE;
use crate::pipe::{DEFAULT_RECV_DEPTH, DEFAULT_SEND_DEPTH, FullAction, PipeConfig, QueueConfig};
use crate::protocol::protocol;

/// Default `NNG_OPT_RECONNMINT`: the first retry delay after a pipe closes.
///
/// NNG's own default of 100 milliseconds (§1, §11).
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(100);

/// Default `NNG_OPT_RECONNMAXT`: the ceiling the retry delay grows to.
///
/// Zero, which is NNG's default and NNG's way of saying "no exponential
/// backoff": the delay stays at `RECONNMINT` (§1). Set it above the minimum
/// and the delay doubles from the minimum up to this value.
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::ZERO;

/// Pipes one socket admits at once, by default.
///
/// **NNG has no such option**, because SP bounds nothing a stranger opens:
/// a listener accepts, a pipe is created, and the only thing that ever
/// removes one is a close (§1, §11). A socket with an open listener on a
/// public address therefore has an unbounded structure in it, which
/// `docs/INVARIANTS.md` forbids, so the ceiling exists and is named. At it,
/// an accepted connection is closed immediately — which is the only refusal
/// SP has (§6) — and a dial reports `NNG_ENOFILES`.
pub const DEFAULT_MAX_PIPES: usize = 1024;

/// How long a connection has to complete the SP handshake, by default.
///
/// **NNG has no such option either.** The SP mapping says both sides send
/// the 8-octet protocol header immediately and both wait for the peer's
/// (§3), and says nothing about what to do with a peer that connects and
/// then sends nothing. Without a deadline that peer holds a pipe slot for
/// as long as it likes, which makes [`DEFAULT_MAX_PIPES`] trivial to
/// exhaust. Ten seconds is long enough for any real handshake — there is no
/// round trip in it — and short enough that the slot comes back.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Addresses one hostname may resolve to before the rest are ignored.
///
/// A resolver answer is remote input, so the count is capped
/// (`docs/INVARIANTS.md`). NNG resolves and dials without publishing a
/// number; this one is ours and is settable.
pub const DEFAULT_MAX_ADDRESSES: usize = 8;

/// Default `NNG_OPT_REQ_RESENDTIME`: NNG's own one minute.
pub const DEFAULT_RESEND_TIME: Duration = Duration::from_secs(60);

/// Default `NNG_OPT_MAXTTL`: the 8 that "supported forwarding protocols
/// commonly default to" (§11).
pub const DEFAULT_MAX_TTL: usize = 8;

/// The largest `NNG_OPT_MAXTTL` the *manual* documents (§11).
pub const SPEC_MAX_TTL: usize = 255;

/// The largest `NNG_OPT_MAXTTL` NNG's own source accepts, which is what a
/// real peer enforces: `NNI_MAX_MAX_TTL` is 15 (§11). A value above this
/// is legal here and is not portable, and that is said where it is set
/// rather than discovered when a node drops the message.
pub const NNG_MAX_TTL: usize = 15;

/// One socket's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketOptions {
    /// `NNG_OPT_RECVMAXSZ`: the largest message this socket accepts from a
    /// peer, judged from the declared length before anything is allocated.
    /// [`crate::RECV_MAX_SIZE_UNLIMITED`] is NNG's "no limit".
    pub recv_max_size: u64,
    /// `NNG_OPT_SENDBUF`: outgoing queue depth per pipe, in messages,
    /// `0..=`[`crate::MAX_QUEUE_DEPTH`]. Zero is a rendezvous, not "no
    /// limit".
    ///
    /// `None` is the protocol's own default
    /// ([`PipeConfig::default_send_depth`](crate::PipeConfig::default_send_depth)):
    /// zero for the protocols that wait at a full queue, which is PUSH's
    /// documented value (§5), and a real depth for the ones that discard,
    /// because discarding at depth zero would discard nearly everything.
    pub send_depth: Option<usize>,
    /// `NNG_OPT_RECVBUF`: incoming queue depth per pipe, in messages.
    /// `None` is [`crate::DEFAULT_RECV_DEPTH`].
    pub recv_depth: Option<usize>,
    /// `NNG_OPT_SENDTIMEO`: how long a send waits before `NNG_ETIMEDOUT`.
    /// `None` is NNG's `NNG_DURATION_INFINITE`, which is its default.
    pub send_timeout: Option<Duration>,
    /// `NNG_OPT_RECVTIMEO`: how long a receive waits before
    /// `NNG_ETIMEDOUT`. `None` is NNG's default of forever.
    pub recv_timeout: Option<Duration>,
    /// `NNG_OPT_RECONNMINT`: the first retry delay after a dialer's pipe
    /// closes.
    pub reconnect_min: Duration,
    /// `NNG_OPT_RECONNMAXT`: the ceiling the retry delay doubles towards.
    /// [`Duration::ZERO`] is NNG's "no exponential backoff".
    pub reconnect_max: Duration,
    /// Pipes this socket admits at once. See [`DEFAULT_MAX_PIPES`]: this
    /// one is ours, because SP has no such bound and needs one.
    pub max_pipes: usize,
    /// How long a connection has to complete the SP handshake. See
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`]: also ours, for the same reason.
    pub handshake_timeout: Duration,
    /// Addresses one hostname may resolve to. Also ours
    /// ([`DEFAULT_MAX_ADDRESSES`]).
    pub max_addresses: usize,
    /// `NNG_OPT_SUB_PREFNEW`: what a SUB socket does when its queue of
    /// admitted publications is full.
    ///
    /// `true` — NNG's default — "removes its oldest queued message to make
    /// room"; `false` "preserves old queued messages by rejecting the new
    /// message" (§4). It is a SUB-only option and says nothing on any
    /// other protocol.
    pub sub_prefer_new: bool,
    /// `NNG_OPT_REQ_RESENDTIME`: how long a REQ context waits for its
    /// reply before sending the request again. NNG's default is a minute.
    ///
    /// The resend is protocol recovery and not flow control: "it can
    /// duplicate a request after a missing reply" (§5), which is why a
    /// service behind REQ has to be repeat-safe (§9).
    pub resend_time: Duration,
    /// `NNG_OPT_MAXTTL`: how many forwarder hops a message may carry, and
    /// therefore how deep a tag stack may be.
    ///
    /// "`MAXTTL` is 1-255; supported forwarding protocols commonly default
    /// to 8" (§11). NNG's own source caps it at 15, so a value above that
    /// is accepted here and is **not portable** — a real NNG node refuses
    /// a stack of 16 (§11). Both numbers are published rather than one
    /// chosen silently.
    pub max_ttl: usize,
}

impl Default for SocketOptions {
    fn default() -> SocketOptions {
        SocketOptions {
            recv_max_size: DEFAULT_RECV_MAX_SIZE,
            send_depth: None,
            recv_depth: None,
            send_timeout: None,
            recv_timeout: None,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            max_pipes: DEFAULT_MAX_PIPES,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            max_addresses: DEFAULT_MAX_ADDRESSES,
            sub_prefer_new: true,
            resend_time: DEFAULT_RESEND_TIME,
            max_ttl: DEFAULT_MAX_TTL,
        }
    }
}

impl SocketOptions {
    /// Checks everything that can be judged without knowing the protocol.
    pub fn validate(&self) -> Result<()> {
        QueueConfig {
            depth: self.send_depth.unwrap_or(DEFAULT_SEND_DEPTH),
            full: FullAction::Block,
        }
        .validate("NNG_OPT_SENDBUF")?;
        QueueConfig {
            depth: self.recv_depth.unwrap_or(DEFAULT_RECV_DEPTH),
            full: FullAction::Block,
        }
        .validate("NNG_OPT_RECVBUF")?;
        if self.max_pipes == 0 {
            return Err(crate::Error::EINVAL(
                "SocketOptions::max_pipes must be at least 1; a socket that admits no pipe \
                 cannot communicate"
                    .into(),
            ));
        }
        if self.max_addresses == 0 {
            return Err(crate::Error::EINVAL(
                "SocketOptions::max_addresses must be at least 1".into(),
            ));
        }
        if self.handshake_timeout.is_zero() {
            return Err(crate::Error::EINVAL(
                "SocketOptions::handshake_timeout must be nonzero; zero would refuse every \
                 connection"
                    .into(),
            ));
        }
        if !self.reconnect_max.is_zero() && self.reconnect_max < self.reconnect_min {
            return Err(crate::Error::EINVAL(
                format!(
                    "NNG_OPT_RECONNMAXT ({:?}) is below NNG_OPT_RECONNMINT ({:?}); a ceiling \
                     under its floor is not a backoff",
                    self.reconnect_max, self.reconnect_min
                )
                .into(),
            ));
        }
        if self.max_ttl < 1 || self.max_ttl > SPEC_MAX_TTL {
            return Err(crate::Error::EINVAL(
                format!(
                    "NNG_OPT_MAXTTL is 1..={SPEC_MAX_TTL}; {} is out of range",
                    self.max_ttl
                )
                .into(),
            ));
        }
        if self.resend_time.is_zero() {
            return Err(crate::Error::EINVAL(
                "NNG_OPT_REQ_RESENDTIME must be nonzero; zero would resend a request \
                 continuously"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Checks the half only the protocol can judge, and refuses an option
    /// that means nothing for it by name — `NNG_OPT_SENDBUF` on REQ, whose
    /// one outstanding transaction per context leaves nothing to buffer
    /// (§5).
    pub fn validate_for(&self, endpoint: EndpointType) -> Result<()> {
        self.validate()?;
        let row = protocol(endpoint);
        if self.send_depth.is_some() {
            row.require_buffers("NNG_OPT_SENDBUF")?;
        }
        if self.recv_depth.is_some() {
            row.require_buffers("NNG_OPT_RECVBUF")?;
        }
        Ok(())
    }

    /// The queue bounds one pipe of `endpoint`'s protocol runs under: these
    /// depths, and that protocol's behaviour at the bound.
    pub fn pipe_config(&self, endpoint: EndpointType) -> PipeConfig {
        let mut config = PipeConfig::of(endpoint);
        if let Some(depth) = self.send_depth {
            config.outgoing.depth = depth;
        }
        if let Some(depth) = self.recv_depth {
            config.incoming.depth = depth;
        }
        config
    }

    /// The delay before retry number `attempt` (counting from zero), under
    /// `RECONNMINT` and `RECONNMAXT`.
    ///
    /// "Retry delay begins at `NNG_OPT_RECONNMINT` and grows exponentially
    /// to `NNG_OPT_RECONNMAXT` when the latter is nonzero" (§1). When it is
    /// zero there is no growth and every delay is the minimum, which is
    /// NNG's own default behaviour.
    pub fn reconnect_delay(&self, attempt: u32) -> Duration {
        if self.reconnect_max.is_zero() || self.reconnect_max <= self.reconnect_min {
            return self.reconnect_min;
        }
        let factor = 1u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
        self.reconnect_min
            .checked_mul(factor)
            .unwrap_or(self.reconnect_max)
            .min(self.reconnect_max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the defaults are NNG's where NNG has one — 100 ms and no
    /// backoff growth, infinite timeouts — so a program ported from NNG
    /// behaves the same without setting anything.
    #[test]
    fn the_defaults_are_nngs_where_nng_has_one() {
        let options = SocketOptions::default();
        assert_eq!(options.reconnect_min, Duration::from_millis(100));
        assert_eq!(options.reconnect_max, Duration::ZERO);
        assert_eq!(options.send_timeout, None);
        assert_eq!(options.recv_timeout, None);
        assert!(options.validate().is_ok());
    }

    /// Claim: with `RECONNMAXT` at zero the delay never grows, and with it
    /// set the delay doubles from the minimum and stops at the ceiling —
    /// which is the whole of what the sheet says the backoff is.
    #[test]
    fn the_backoff_grows_from_the_minimum_to_the_maximum() {
        let flat = SocketOptions::default();
        for attempt in 0..10 {
            assert_eq!(flat.reconnect_delay(attempt), Duration::from_millis(100));
        }

        let growing = SocketOptions {
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_millis(800),
            ..SocketOptions::default()
        };
        assert_eq!(growing.reconnect_delay(0), Duration::from_millis(100));
        assert_eq!(growing.reconnect_delay(1), Duration::from_millis(200));
        assert_eq!(growing.reconnect_delay(2), Duration::from_millis(400));
        assert_eq!(growing.reconnect_delay(3), Duration::from_millis(800));
        assert_eq!(growing.reconnect_delay(4), Duration::from_millis(800));
        // And no overflow at the end of the world.
        assert_eq!(
            growing.reconnect_delay(u32::MAX),
            Duration::from_millis(800)
        );
    }

    /// Claim: a configuration this library cannot deliver is refused where
    /// it is set, with `NNG_EINVAL` and the option named — never clamped
    /// and never ignored.
    #[test]
    fn an_impossible_configuration_is_refused_at_configuration_time() {
        let cases: [(SocketOptions, &str); 5] = [
            (
                SocketOptions {
                    send_depth: Some(crate::MAX_QUEUE_DEPTH + 1),
                    ..SocketOptions::default()
                },
                "NNG_OPT_SENDBUF",
            ),
            (
                SocketOptions {
                    recv_depth: Some(crate::MAX_QUEUE_DEPTH + 1),
                    ..SocketOptions::default()
                },
                "NNG_OPT_RECVBUF",
            ),
            (
                SocketOptions {
                    max_pipes: 0,
                    ..SocketOptions::default()
                },
                "max_pipes",
            ),
            (
                SocketOptions {
                    handshake_timeout: Duration::ZERO,
                    ..SocketOptions::default()
                },
                "handshake_timeout",
            ),
            (
                SocketOptions {
                    reconnect_min: Duration::from_secs(2),
                    reconnect_max: Duration::from_secs(1),
                    ..SocketOptions::default()
                },
                "NNG_OPT_RECONNMAXT",
            ),
        ];
        for (options, named) in cases {
            let err = options.validate().unwrap_err();
            assert!(matches!(err, crate::Error::EINVAL(_)), "{err:?}");
            assert!(err.cause().contains(named), "{err} does not name {named}");
        }
    }

    /// Claim: a buffer depth on REQ is refused by protocol, which is the
    /// sheet's own example of an option a protocol does not support (§5),
    /// while leaving the default alone is not a configuration at all.
    #[test]
    fn a_buffer_depth_on_req_is_refused_by_protocol() {
        let defaults = SocketOptions::default();
        assert!(defaults.validate_for(EndpointType::Req).is_ok());

        let buffered = SocketOptions {
            send_depth: Some(4),
            ..SocketOptions::default()
        };
        let err = buffered.validate_for(EndpointType::Req).unwrap_err();
        assert!(matches!(err, crate::Error::ENOTSUP(_)), "{err:?}");
        assert!(err.cause().contains("NNG_OPT_SENDBUF"));
        assert!(buffered.validate_for(EndpointType::Push).is_ok());
    }

    /// Claim: the per-pipe bounds a socket runs under are its depths with
    /// its protocol's behaviour at the bound — the two halves come from
    /// different places and meet here.
    #[test]
    fn pipe_bounds_combine_the_depths_with_the_protocols_action() {
        let options = SocketOptions {
            send_depth: Some(4),
            recv_depth: Some(5),
            ..SocketOptions::default()
        };
        let bus = options.pipe_config(EndpointType::Bus);
        assert_eq!(bus.outgoing.depth, 4);
        assert_eq!(bus.outgoing.full, FullAction::Drop);
        assert_eq!(bus.incoming.depth, 5);
        assert_eq!(bus.incoming.full, FullAction::Block);

        // And a protocol that says nothing keeps its own default depth:
        // zero where a full queue makes the sender wait, a real depth
        // where it makes the sender discard.
        let defaults = SocketOptions::default();
        assert_eq!(defaults.pipe_config(EndpointType::Push).outgoing.depth, 0);
        assert_eq!(
            defaults.pipe_config(EndpointType::Pub).outgoing.depth,
            crate::pipe::DEFAULT_BROADCAST_SEND_DEPTH
        );
    }
}
