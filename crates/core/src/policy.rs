//! Per-transfer policy and outcome vocabulary.
//!
//! Master doc §18-§22: an acknowledgement names a precisely defined next-hop
//! state, guarantees are hop-local, and "I do not know" is a first-class
//! outcome. v0 implements the weakest two acknowledgement levels; the stronger
//! ones exist on the wire as reserved codes so that adding them later does not
//! change the DATA frame layout.

use std::fmt;

/// Wire code for `ack_mode = none`.
pub const ACK_MODE_NONE: u64 = 0;
/// Wire code for `ack_mode = accepted`.
pub const ACK_MODE_ACCEPTED: u64 = 1;
/// Reserved: peer has persisted the transfer. Not implemented in v0.
pub const ACK_MODE_STORED: u64 = 2;
/// Reserved: peer has replicated the transfer. Not implemented in v0.
pub const ACK_MODE_REPLICATED: u64 = 3;
/// Reserved: peer's application reported successful processing. Not implemented
/// in v0.
pub const ACK_MODE_PROCESSED: u64 = 4;

/// Acknowledgement level requested by the sender of a transfer.
///
/// A requested level is never silently weakened (master doc §21): a peer that
/// cannot honour a level answers with `UNSUPPORTED`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AckMode {
    /// No acknowledgement. Nothing is registered locally and no control stream
    /// is created: a disabled guarantee stays off the hot path (master doc §70).
    #[default]
    None,
    /// The peer must confirm that it read the payload to FIN and handed it to
    /// its application.
    Accepted,
}

impl AckMode {
    /// The wire code for this mode.
    pub const fn to_wire(self) -> u64 {
        match self {
            AckMode::None => ACK_MODE_NONE,
            AckMode::Accepted => ACK_MODE_ACCEPTED,
        }
    }

    /// Interprets a wire code, returning `None` for reserved or unknown values.
    ///
    /// Callers must answer reserved values with `UNSUPPORTED` rather than
    /// downgrading them.
    pub const fn from_wire(code: u64) -> Option<AckMode> {
        match code {
            ACK_MODE_NONE => Some(AckMode::None),
            ACK_MODE_ACCEPTED => Some(AckMode::Accepted),
            _ => None,
        }
    }

    /// True if the sender must wait for an acknowledgement after its FIN.
    pub const fn wants_ack(self) -> bool {
        matches!(self, AckMode::Accepted)
    }
}

impl fmt::Display for AckMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AckMode::None => f.write_str("none"),
            AckMode::Accepted => f.write_str("accepted"),
        }
    }
}

/// Wire code for `state = accepted` in an ACK frame.
pub const ACK_STATE_ACCEPTED: u64 = 1;

/// The next-hop state an acknowledgement reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AckState {
    /// The peer read the payload to FIN and handed it to its application. This
    /// says nothing about any hop beyond the peer (master doc §19).
    Accepted,
}

impl AckState {
    /// The wire code for this state.
    pub const fn to_wire(self) -> u64 {
        match self {
            AckState::Accepted => ACK_STATE_ACCEPTED,
        }
    }

    /// Interprets a wire code, returning `None` for unknown values.
    pub const fn from_wire(code: u64) -> Option<AckState> {
        match code {
            ACK_STATE_ACCEPTED => Some(AckState::Accepted),
            _ => None,
        }
    }
}

impl fmt::Display for AckState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AckState::Accepted => f.write_str("Accepted"),
        }
    }
}

/// The successful outcome of an outgoing transfer.
///
/// Failures are [`crate::Error`] values; in particular
/// [`crate::Error::Indeterminate`] is a failure the application must not treat
/// as "not delivered".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The payload was written and finished with `ack_mode = none`. The peer's
    /// state is unknown by construction, which is what was requested.
    SentBestEffort,
    /// The peer reported the named state.
    Acked(AckState),
    /// The transfer was canceled locally before completing.
    Canceled,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::SentBestEffort => f.write_str("SentBestEffort"),
            Outcome::Acked(s) => write!(f, "Acked({s})"),
            Outcome::Canceled => f.write_str("Canceled"),
        }
    }
}

/// Role of a DATA transfer on the wire.
///
/// `oneshot` (`0`) is reserved for the fire-and-forget patterns of Phase 3 and
/// is not accepted in v0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// A request; carries an endpoint path and expects a correlated reply.
    Request,
    /// A reply; carries the correlation id of the request it answers.
    Reply,
}

/// Wire code for `role = oneshot` (reserved).
pub const ROLE_ONESHOT: u64 = 0;
/// Wire code for `role = request`.
pub const ROLE_REQUEST: u64 = 1;
/// Wire code for `role = reply`.
pub const ROLE_REPLY: u64 = 2;

impl Role {
    /// The wire code for this role.
    pub const fn to_wire(self) -> u64 {
        match self {
            Role::Request => ROLE_REQUEST,
            Role::Reply => ROLE_REPLY,
        }
    }

    /// Interprets a wire code, returning `None` for reserved or unknown values.
    pub const fn from_wire(code: u64) -> Option<Role> {
        match code {
            ROLE_REQUEST => Some(Role::Request),
            ROLE_REPLY => Some(Role::Reply),
            _ => None,
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Request => f.write_str("request"),
            Role::Reply => f.write_str("reply"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_mode_default_is_none() {
        assert_eq!(AckMode::default(), AckMode::None);
        assert!(!AckMode::None.wants_ack());
        assert!(AckMode::Accepted.wants_ack());
    }

    #[test]
    fn ack_mode_wire_roundtrip() {
        for m in [AckMode::None, AckMode::Accepted] {
            assert_eq!(AckMode::from_wire(m.to_wire()), Some(m));
        }
    }

    #[test]
    fn reserved_ack_modes_are_not_downgraded() {
        for code in [
            ACK_MODE_STORED,
            ACK_MODE_REPLICATED,
            ACK_MODE_PROCESSED,
            5,
            u64::MAX,
        ] {
            assert!(
                AckMode::from_wire(code).is_none(),
                "code {code} must not map to a v0 mode"
            );
        }
    }

    #[test]
    fn ack_state_wire_roundtrip() {
        assert_eq!(AckState::from_wire(1), Some(AckState::Accepted));
        assert_eq!(AckState::from_wire(0), None);
        assert_eq!(AckState::from_wire(2), None);
    }

    #[test]
    fn role_wire_roundtrip_and_reserved() {
        assert_eq!(Role::from_wire(1), Some(Role::Request));
        assert_eq!(Role::from_wire(2), Some(Role::Reply));
        assert_eq!(Role::from_wire(ROLE_ONESHOT), None);
        assert_eq!(Role::from_wire(3), None);
    }

    #[test]
    fn outcome_display_is_stable() {
        assert_eq!(Outcome::SentBestEffort.to_string(), "SentBestEffort");
        assert_eq!(
            Outcome::Acked(AckState::Accepted).to_string(),
            "Acked(Accepted)"
        );
        assert_eq!(Outcome::Canceled.to_string(), "Canceled");
    }
}
