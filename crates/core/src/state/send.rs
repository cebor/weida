//! Sender-side outcome machine.
//!
//! Implements the sender-outcome rules of `docs/FAILURE_MODEL.md` exactly:
//!
//! | event | state | outcome |
//! | --- | --- | --- |
//! | connection lost | before local FIN | `Err(ConnectionLost)` — definitely not delivered |
//! | connection lost | after local FIN, awaiting ACK/reply | `Err(Indeterminate)` |
//! | ERROR frame | any | `Err(code)` |
//! | STOP_SENDING | any | `Err(Rejected \| UnknownEndpoint \| Canceled)` |
//! | ACK | any | `Ok(Acked(state))` |
//! | local FIN, `ack_mode = none` | streaming | `Ok(SentBestEffort)` |
//! | local cancel | before settle | `Ok(Canceled)` |

use crate::error::{Error, ErrorCode, StopReason};
use crate::policy::{AckMode, AckState, Outcome};

/// Lifecycle of one outgoing transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendState {
    /// The local FIN has not been written yet.
    Streaming,
    /// The local FIN is written and an acknowledgement is expected.
    AwaitingAck,
    /// A terminal outcome has been produced.
    Settled,
}

/// Things that can happen to an outgoing transfer.
#[derive(Debug)]
pub enum SendEvent {
    /// The local side finished writing the payload (FIN flushed).
    LocalFin,
    /// The application canceled the transfer; the stream is reset with
    /// `CANCELED`.
    LocalCancel,
    /// The connection was lost.
    ConnectionLost,
    /// The peer acknowledged the transfer.
    Ack(AckState),
    /// The peer sent an ERROR frame referring to this transfer.
    ErrorFrame(ErrorCode),
    /// The peer refused further payload with `STOP_SENDING`.
    StopSending(StopReason),
}

/// What the transport must do after an event.
#[derive(Debug)]
pub enum SendAction {
    /// Keep waiting; the transfer has no outcome yet.
    Continue,
    /// Resolve the caller's future with this outcome.
    Resolve(Result<Outcome, Error>),
    /// The transfer is already settled; drop the event (it raced with the
    /// outcome and is not an error, see `docs/PROTOCOL.md` §6.3).
    Ignore,
}

/// The sender-side machine for one outgoing transfer.
#[derive(Debug)]
pub struct SendMachine {
    state: SendState,
    ack_mode: AckMode,
    /// A terminal result observed before the local FIN was reported.
    ///
    /// This is not a hypothetical race: `quinn`'s `finish()` only marks the FIN,
    /// so the peer can read to FIN and answer before the local handle reports
    /// `LocalFin` to the connection actor.
    pending: Option<Result<Outcome, Error>>,
}

impl SendMachine {
    /// Creates a machine for a transfer opened with `ack_mode`.
    pub fn new(ack_mode: AckMode) -> SendMachine {
        SendMachine {
            state: SendState::Streaming,
            ack_mode,
            pending: None,
        }
    }

    /// Current state.
    pub fn state(&self) -> SendState {
        self.state
    }

    /// The acknowledgement level this transfer requested.
    pub fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    /// Applies one event.
    pub fn on(&mut self, event: SendEvent) -> SendAction {
        if self.state == SendState::Settled {
            return SendAction::Ignore;
        }
        match event {
            SendEvent::LocalFin => match self.pending.take() {
                Some(result) => self.settle(result),
                None if self.ack_mode.wants_ack() => {
                    self.state = SendState::AwaitingAck;
                    SendAction::Continue
                }
                None => self.settle(Ok(Outcome::SentBestEffort)),
            },
            SendEvent::LocalCancel => self.settle(Ok(Outcome::Canceled)),
            SendEvent::ConnectionLost => match self.pending.take() {
                Some(result) => self.settle(result),
                None if self.state == SendState::Streaming => {
                    self.settle(Err(Error::ConnectionLost))
                }
                // FIN was written and the answer never arrived: the peer may or
                // may not have accepted the transfer (master doc §22).
                None => self.settle(Err(Error::Indeterminate)),
            },
            SendEvent::Ack(state) => self.terminal(Ok(Outcome::Acked(state))),
            SendEvent::ErrorFrame(code) => self.terminal(Err(code.into())),
            SendEvent::StopSending(reason) => self.terminal(Err(reason.into())),
        }
    }

    /// Records a peer-supplied terminal result.
    ///
    /// After the local FIN the result resolves the caller immediately. Before
    /// it, the result is held until `LocalFin` (or connection loss) so that the
    /// outcome does not race the application's own `finish()` call. When two
    /// peer results are held, failure wins over success: an ERROR frame
    /// observed before an ACK is delivered takes precedence.
    fn terminal(&mut self, result: Result<Outcome, Error>) -> SendAction {
        if self.state == SendState::AwaitingAck {
            return self.settle(result);
        }
        match &self.pending {
            Some(Ok(_)) if result.is_err() => self.pending = Some(result),
            None => self.pending = Some(result),
            Some(_) => {}
        }
        SendAction::Continue
    }

    fn settle(&mut self, result: Result<Outcome, Error>) -> SendAction {
        self.state = SendState::Settled;
        self.pending = None;
        SendAction::Resolve(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(action: SendAction) -> Result<Outcome, Error> {
        match action {
            SendAction::Resolve(r) => r,
            other => panic!("expected a resolution, got {other:?}"),
        }
    }

    fn assert_continues(action: SendAction) {
        match action {
            SendAction::Continue => {}
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    fn assert_ignored(action: SendAction) {
        match action {
            SendAction::Ignore => {}
            other => panic!("expected Ignore, got {other:?}"),
        }
    }

    #[test]
    fn best_effort_settles_at_fin() {
        let mut m = SendMachine::new(AckMode::None);
        assert_eq!(m.state(), SendState::Streaming);
        assert!(matches!(
            resolved(m.on(SendEvent::LocalFin)),
            Ok(Outcome::SentBestEffort)
        ));
        assert_eq!(m.state(), SendState::Settled);
    }

    #[test]
    fn accepted_waits_for_the_ack() {
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::LocalFin));
        assert_eq!(m.state(), SendState::AwaitingAck);
        assert!(matches!(
            resolved(m.on(SendEvent::Ack(AckState::Accepted))),
            Ok(Outcome::Acked(AckState::Accepted))
        ));
    }

    #[test]
    fn connection_loss_before_fin_is_a_definite_failure() {
        let mut m = SendMachine::new(AckMode::Accepted);
        let err = resolved(m.on(SendEvent::ConnectionLost)).unwrap_err();
        assert!(matches!(err, Error::ConnectionLost));
        assert!(err.is_definite_failure());
    }

    #[test]
    fn connection_loss_after_fin_is_indeterminate() {
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::LocalFin));
        let err = resolved(m.on(SendEvent::ConnectionLost)).unwrap_err();
        assert!(matches!(err, Error::Indeterminate));
        assert!(!err.is_definite_failure());
    }

    #[test]
    fn error_frame_maps_to_its_error() {
        let cases = [
            (ErrorCode::UnknownEndpoint, "UnknownEndpoint"),
            (ErrorCode::Rejected, "Rejected"),
            (ErrorCode::Unsupported, "Unsupported"),
            (ErrorCode::Internal, "Transport"),
            (ErrorCode::NoReply, "NoReply"),
        ];
        for (code, want) in cases {
            let mut m = SendMachine::new(AckMode::Accepted);
            assert_continues(m.on(SendEvent::LocalFin));
            let err = resolved(m.on(SendEvent::ErrorFrame(code))).unwrap_err();
            let got = match err {
                Error::UnknownEndpoint => "UnknownEndpoint",
                Error::Rejected => "Rejected",
                Error::Unsupported => "Unsupported",
                Error::NoReply => "NoReply",
                Error::Transport(_) => "Transport",
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(got, want, "code {code:?}");
        }
    }

    #[test]
    fn stop_sending_maps_by_reason() {
        let cases = [
            (StopReason::Rejected, "Rejected"),
            (StopReason::UnknownEndpoint, "UnknownEndpoint"),
            (StopReason::Canceled, "Canceled"),
        ];
        for (reason, want) in cases {
            let mut m = SendMachine::new(AckMode::Accepted);
            assert_continues(m.on(SendEvent::LocalFin));
            let err = resolved(m.on(SendEvent::StopSending(reason))).unwrap_err();
            let got = match err {
                Error::Rejected => "Rejected",
                Error::UnknownEndpoint => "UnknownEndpoint",
                Error::Canceled => "Canceled",
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(got, want, "reason {reason:?}");
        }
    }

    #[test]
    fn stop_sending_mid_stream_settles_immediately_at_fin() {
        // The peer refuses while we are still writing: the write fails, and the
        // outcome is reported when the handle finishes.
        let mut m = SendMachine::new(AckMode::None);
        assert_continues(m.on(SendEvent::StopSending(StopReason::Rejected)));
        assert_eq!(m.state(), SendState::Streaming);
        assert!(matches!(
            resolved(m.on(SendEvent::LocalFin)).unwrap_err(),
            Error::Rejected
        ));
    }

    #[test]
    fn ack_racing_ahead_of_the_local_fin_report_is_not_lost() {
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::Ack(AckState::Accepted)));
        assert!(matches!(
            resolved(m.on(SendEvent::LocalFin)),
            Ok(Outcome::Acked(AckState::Accepted))
        ));
    }

    #[test]
    fn error_observed_before_the_ack_is_delivered_wins() {
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::ErrorFrame(ErrorCode::Rejected)));
        assert_continues(m.on(SendEvent::Ack(AckState::Accepted)));
        assert!(matches!(
            resolved(m.on(SendEvent::LocalFin)).unwrap_err(),
            Error::Rejected
        ));
    }

    #[test]
    fn an_ack_already_delivered_is_not_overridden_by_a_late_error() {
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::LocalFin));
        assert!(matches!(
            resolved(m.on(SendEvent::Ack(AckState::Accepted))),
            Ok(Outcome::Acked(AckState::Accepted))
        ));
        assert_ignored(m.on(SendEvent::ErrorFrame(ErrorCode::Internal)));
    }

    #[test]
    fn a_held_ack_survives_connection_loss() {
        // The peer acknowledged, then the connection dropped before our FIN was
        // reported: the transfer demonstrably arrived, so it is not lost.
        let mut m = SendMachine::new(AckMode::Accepted);
        assert_continues(m.on(SendEvent::Ack(AckState::Accepted)));
        assert!(matches!(
            resolved(m.on(SendEvent::ConnectionLost)),
            Ok(Outcome::Acked(AckState::Accepted))
        ));
    }

    #[test]
    fn local_cancel_settles_as_canceled() {
        for mode in [AckMode::None, AckMode::Accepted] {
            let mut m = SendMachine::new(mode);
            assert!(matches!(
                resolved(m.on(SendEvent::LocalCancel)),
                Ok(Outcome::Canceled)
            ));
            assert_ignored(m.on(SendEvent::ConnectionLost));
        }
    }

    #[test]
    fn events_after_settling_are_ignored() {
        let mut m = SendMachine::new(AckMode::None);
        let _ = m.on(SendEvent::LocalFin);
        assert_ignored(m.on(SendEvent::Ack(AckState::Accepted)));
        assert_ignored(m.on(SendEvent::ConnectionLost));
        assert_ignored(m.on(SendEvent::LocalFin));
        assert_ignored(m.on(SendEvent::ErrorFrame(ErrorCode::Internal)));
        assert_ignored(m.on(SendEvent::StopSending(StopReason::Canceled)));
        assert_ignored(m.on(SendEvent::LocalCancel));
    }

    #[test]
    fn ack_mode_is_reported() {
        assert_eq!(
            SendMachine::new(AckMode::Accepted).ack_mode(),
            AckMode::Accepted
        );
    }
}
