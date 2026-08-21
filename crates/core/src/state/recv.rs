//! Receiver-side machine for one inbound transfer.
//!
//! Decides when an ACK is owed, when the payload must be refused and which
//! ERROR frame the peer is owed, per `docs/FAILURE_MODEL.md` and
//! `docs/PROTOCOL.md` §9.

use crate::error::{ErrorCode, StopReason};
use crate::policy::{AckMode, AckState};

/// Lifecycle of one inbound transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecvState {
    /// The payload is still arriving.
    Reading,
    /// The application consumed the payload to FIN.
    Delivered,
    /// The transfer ended without delivery.
    Aborted,
}

/// Things that can happen to an inbound transfer.
#[derive(Debug)]
pub enum RecvEvent {
    /// The application read the payload to FIN.
    PayloadEnd,
    /// The sender reset the stream before FIN.
    PeerReset,
    /// The application dropped the body before FIN.
    AppAbandonedBody,
    /// No endpoint is registered for the requested path.
    UnknownEndpoint,
    /// The header requested a reserved role or ack mode.
    UnsupportedPolicy,
    /// The application dropped an accepted request without opening a reply.
    RequestDroppedWithoutReply,
    /// The receiving side failed internally.
    InternalFailure,
}

/// What the transport must do after an event.
#[derive(Debug, PartialEq, Eq)]
pub enum RecvAction {
    /// Nothing to emit.
    None,
    /// Send an ACK frame naming this transfer.
    SendAck(AckState),
    /// Drop partial state silently: the sender already knows (it reset the
    /// stream), so neither an ACK nor an ERROR is owed.
    Discard,
    /// Refuse further payload with `STOP_SENDING(reason)`, and additionally
    /// tell the peer why with an ERROR frame when `error` is set.
    Refuse {
        /// QUIC application code for `STOP_SENDING`.
        reason: StopReason,
        /// Optional ERROR frame code.
        error: Option<ErrorCode>,
    },
    /// Send an ERROR frame only; the payload was already fully received.
    SendError(ErrorCode),
}

/// The receiver-side machine for one inbound transfer.
#[derive(Debug)]
pub struct RecvMachine {
    state: RecvState,
    ack_mode: AckMode,
}

impl RecvMachine {
    /// Creates a machine for a transfer whose header requested `ack_mode`.
    pub fn new(ack_mode: AckMode) -> RecvMachine {
        RecvMachine {
            state: RecvState::Reading,
            ack_mode,
        }
    }

    /// Current state.
    pub fn state(&self) -> RecvState {
        self.state
    }

    /// Applies one event.
    pub fn on(&mut self, event: RecvEvent) -> RecvAction {
        match (self.state, event) {
            (RecvState::Reading, RecvEvent::PayloadEnd) => {
                self.state = RecvState::Delivered;
                if self.ack_mode.wants_ack() {
                    RecvAction::SendAck(AckState::Accepted)
                } else {
                    RecvAction::None
                }
            }
            (RecvState::Reading, RecvEvent::PeerReset) => {
                self.state = RecvState::Aborted;
                RecvAction::Discard
            }
            (RecvState::Reading, RecvEvent::AppAbandonedBody) => {
                self.state = RecvState::Aborted;
                RecvAction::Refuse {
                    reason: StopReason::Rejected,
                    error: None,
                }
            }
            (RecvState::Reading, RecvEvent::UnknownEndpoint) => {
                self.state = RecvState::Aborted;
                RecvAction::Refuse {
                    reason: StopReason::UnknownEndpoint,
                    error: Some(ErrorCode::UnknownEndpoint),
                }
            }
            (RecvState::Reading, RecvEvent::UnsupportedPolicy) => {
                self.state = RecvState::Aborted;
                RecvAction::Refuse {
                    reason: StopReason::Rejected,
                    error: Some(ErrorCode::Unsupported),
                }
            }
            // Dropped before FIN: refuse the rest of the payload and tell the
            // requester no reply is coming, so it does not wait forever.
            (RecvState::Reading, RecvEvent::RequestDroppedWithoutReply) => {
                self.state = RecvState::Aborted;
                RecvAction::Refuse {
                    reason: StopReason::Rejected,
                    error: Some(ErrorCode::NoReply),
                }
            }
            (RecvState::Reading, RecvEvent::InternalFailure) => {
                self.state = RecvState::Aborted;
                RecvAction::Refuse {
                    reason: StopReason::Rejected,
                    error: Some(ErrorCode::Internal),
                }
            }
            (RecvState::Delivered, RecvEvent::RequestDroppedWithoutReply) => {
                self.state = RecvState::Aborted;
                RecvAction::SendError(ErrorCode::NoReply)
            }
            (RecvState::Delivered, RecvEvent::InternalFailure) => {
                self.state = RecvState::Aborted;
                RecvAction::SendError(ErrorCode::Internal)
            }
            // A reset or a second FIN after delivery changes nothing, and an
            // aborted transfer emits at most one refusal.
            (RecvState::Delivered | RecvState::Aborted, _) => RecvAction::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_end_acks_only_when_requested() {
        let mut m = RecvMachine::new(AckMode::Accepted);
        assert_eq!(
            m.on(RecvEvent::PayloadEnd),
            RecvAction::SendAck(AckState::Accepted)
        );
        assert_eq!(m.state(), RecvState::Delivered);

        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(m.on(RecvEvent::PayloadEnd), RecvAction::None);
        assert_eq!(m.state(), RecvState::Delivered);
    }

    #[test]
    fn peer_reset_discards_without_answering() {
        let mut m = RecvMachine::new(AckMode::Accepted);
        assert_eq!(m.on(RecvEvent::PeerReset), RecvAction::Discard);
        assert_eq!(m.state(), RecvState::Aborted);
        // No ACK is ever owed for a reset transfer.
        assert_eq!(m.on(RecvEvent::PayloadEnd), RecvAction::None);
    }

    #[test]
    fn abandoned_body_refuses_without_an_error_frame() {
        let mut m = RecvMachine::new(AckMode::Accepted);
        assert_eq!(
            m.on(RecvEvent::AppAbandonedBody),
            RecvAction::Refuse {
                reason: StopReason::Rejected,
                error: None
            }
        );
    }

    #[test]
    fn unknown_endpoint_refuses_and_explains() {
        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(
            m.on(RecvEvent::UnknownEndpoint),
            RecvAction::Refuse {
                reason: StopReason::UnknownEndpoint,
                error: Some(ErrorCode::UnknownEndpoint)
            }
        );
    }

    #[test]
    fn reserved_policy_is_answered_unsupported_not_downgraded() {
        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(
            m.on(RecvEvent::UnsupportedPolicy),
            RecvAction::Refuse {
                reason: StopReason::Rejected,
                error: Some(ErrorCode::Unsupported)
            }
        );
    }

    #[test]
    fn dropping_a_request_without_a_reply_reports_no_reply() {
        // After the body was fully read: only the ERROR frame is needed.
        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(m.on(RecvEvent::PayloadEnd), RecvAction::None);
        assert_eq!(
            m.on(RecvEvent::RequestDroppedWithoutReply),
            RecvAction::SendError(ErrorCode::NoReply)
        );

        // Dropped mid-body: refuse the remaining payload as well.
        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(
            m.on(RecvEvent::RequestDroppedWithoutReply),
            RecvAction::Refuse {
                reason: StopReason::Rejected,
                error: Some(ErrorCode::NoReply)
            }
        );
    }

    #[test]
    fn internal_failure_is_reported_in_both_phases() {
        let mut m = RecvMachine::new(AckMode::None);
        assert_eq!(
            m.on(RecvEvent::InternalFailure),
            RecvAction::Refuse {
                reason: StopReason::Rejected,
                error: Some(ErrorCode::Internal)
            }
        );

        let mut m = RecvMachine::new(AckMode::None);
        m.on(RecvEvent::PayloadEnd);
        assert_eq!(
            m.on(RecvEvent::InternalFailure),
            RecvAction::SendError(ErrorCode::Internal)
        );
    }

    #[test]
    fn an_aborted_transfer_emits_nothing_further() {
        let mut m = RecvMachine::new(AckMode::Accepted);
        m.on(RecvEvent::UnknownEndpoint);
        for ev in [
            RecvEvent::PayloadEnd,
            RecvEvent::PeerReset,
            RecvEvent::AppAbandonedBody,
            RecvEvent::UnknownEndpoint,
            RecvEvent::UnsupportedPolicy,
            RecvEvent::RequestDroppedWithoutReply,
            RecvEvent::InternalFailure,
        ] {
            assert_eq!(m.on(ev), RecvAction::None);
        }
    }

    #[test]
    fn a_delivered_transfer_never_acks_twice() {
        let mut m = RecvMachine::new(AckMode::Accepted);
        assert_eq!(
            m.on(RecvEvent::PayloadEnd),
            RecvAction::SendAck(AckState::Accepted)
        );
        assert_eq!(m.on(RecvEvent::PayloadEnd), RecvAction::None);
    }
}
