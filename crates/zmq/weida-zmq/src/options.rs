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

use crate::error::{Error, Result};
use crate::message::DEFAULT_MAX_MESSAGE_SIZE;
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
        Ok(())
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

    /// Claim: every default is the number libzmq's manual publishes.
    #[test]
    fn the_defaults_are_libzmqs() {
        let options = SocketOptions::default();
        assert_eq!(options.reconnect_ivl, Some(Duration::from_millis(100)));
        assert_eq!(options.reconnect_ivl_max, None, "libzmq's 0: no backoff");
        assert_eq!(options.handshake_ivl, Some(Duration::from_secs(30)));
        assert_eq!(options.connect_timeout, None, "libzmq's 0: the OS default");
        assert!(!options.immediate, "libzmq's 0");
        assert_eq!(options.backlog, 100);
        options.validate().expect("the defaults are usable");
    }

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
