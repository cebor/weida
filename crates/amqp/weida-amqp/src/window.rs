//! The session's six flow-control variables, and the arithmetic over them.
//!
//! AMQP has two independent, simultaneously active credit schemes. This is the
//! *session* one: counted in `transfer` **frames**, not messages, and — since
//! frames are size-capped — byte control in disguise. Part 2 §2.1.2 says so
//! outright: "Since frames have a maximum size for a given connection, this
//! provides flow control based on the number of bytes transmitted". The other
//! scheme, link credit, is counted in messages and arrives with the links.
//!
//! Both must permit a transfer. A receiver granting generous link credit but a
//! tight incoming window forces the sender to dribble multi-frame messages; a
//! receiver granting a large window but zero link credit gets nothing. The
//! specification ties the two together nowhere, which is why they are two
//! types here rather than one.
//!
//! # The six variables
//!
//! Part 2 §2.5.6, per session endpoint:
//!
//! | Variable | Whose | What it is |
//! | --- | --- | --- |
//! | `next-incoming-id` | ours | the transfer-id we expect next from the peer |
//! | `incoming-window` | ours | how many transfer frames we can receive now |
//! | `next-outgoing-id` | ours | the transfer-id of our next outgoing frame |
//! | `outgoing-window` | ours | how many we are willing to send |
//! | `remote-incoming-window` | derived | how many we may send without exceeding the peer's incoming window |
//! | `remote-outgoing-window` | derived | how many may arrive without exceeding the peer's outgoing window |
//!
//! The two derived ones are the load-bearing pair. `remote-incoming-window`
//! is what makes "the peer's incoming-window is never exceeded" checkable:
//! it is decremented on every send and recomputed from every `flow`, and at
//! zero the sender **stalls**. Nothing is dropped and nothing disconnects —
//! exceeding it is `amqp:session:window-violation` and ends the session, so
//! the check has to happen before the frame is written, not after.
//!
//! # Serial arithmetic, not counters
//!
//! `next-outgoing-id` and `next-incoming-id` are RFC 1982 serial numbers in
//! 32 bits: they wrap, and they are compared by serial arithmetic rather than
//! by magnitude. A session that transferred four billion frames and then
//! compared ids with `<` would conclude it had gone backwards. Every
//! comparison here goes through [`serial_gt`] or [`serial_distance`].

/// The default window this client begins a session with, in `transfer`
/// frames.
///
/// **Ours, and a real bound.** `begin.incoming-window` is mandatory and has
/// no default (Part 2 §2.7.2), which makes it one of only three limits in the
/// protocol that are safe by construction — the field cannot be left out. 400
/// is the number RabbitMQ 4.x uses (`rabbit.max_incoming_window`), and with a
/// 128 KiB frame it bounds the session's in-flight bytes at about 50 MiB.
pub const DEFAULT_WINDOW: u32 = 400;

/// Whether `a` is after `b` in RFC 1982 serial arithmetic over 32 bits.
///
/// The comparison a transfer-id needs: half the space is "after" and half is
/// "before", so wrapping is not a special case but the definition.
#[must_use]
pub const fn serial_gt(a: u32, b: u32) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000_0000
}

/// How far `a` is after `b`, in serial arithmetic.
///
/// Saturates at zero where `a` is not after `b`, because every caller here
/// wants "how many frames have I got room for" and a negative answer is the
/// same as none.
#[must_use]
pub const fn serial_distance(a: u32, b: u32) -> u32 {
    if serial_gt(a, b) || a == b {
        a.wrapping_sub(b)
    } else {
        0
    }
}

/// What a `flow` told us about the peer's session state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteFlow {
    /// The peer's `next-incoming-id`. `None` before it has seen our `begin`,
    /// which the specification explicitly permits.
    pub next_incoming_id: Option<u32>,
    /// The peer's `incoming-window`.
    pub incoming_window: u32,
    /// The peer's `next-outgoing-id`.
    pub next_outgoing_id: u32,
    /// The peer's `outgoing-window`.
    pub outgoing_window: u32,
}

/// One session endpoint's flow-control state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    /// The transfer-id we expect next from the peer.
    pub next_incoming_id: u32,
    /// How many transfer frames we can receive now.
    pub incoming_window: u32,
    /// The transfer-id of our next outgoing transfer frame.
    pub next_outgoing_id: u32,
    /// How many we are willing to send.
    pub outgoing_window: u32,
    /// How many we may send without exceeding the peer's incoming window.
    pub remote_incoming_window: u32,
    /// How many may arrive without exceeding the peer's outgoing window.
    pub remote_outgoing_window: u32,
    /// The `next-outgoing-id` we announced in `begin`, kept because the
    /// specification's recomputation needs it in the one case where the peer
    /// has not seen our `begin` yet.
    initial_outgoing_id: u32,
    /// Whether the peer's `begin` has been seen, which decides whether
    /// `next-incoming-id` may go on the wire at all.
    seen_remote_begin: bool,
}

impl Windows {
    /// A fresh endpoint that will announce `next_outgoing_id` and the two
    /// windows in its `begin`.
    #[must_use]
    pub const fn new(next_outgoing_id: u32, incoming_window: u32, outgoing_window: u32) -> Self {
        Self {
            // Meaningless until the peer's `begin` arrives, and MUST NOT be
            // put on the wire before then.
            next_incoming_id: 0,
            incoming_window,
            next_outgoing_id,
            outgoing_window,
            // "A freshly begun session has no permission to send anything
            // until the partner's begin or flow says so": zero here is not a
            // stall to be worked around, it is the correct starting point.
            remote_incoming_window: 0,
            remote_outgoing_window: 0,
            initial_outgoing_id: next_outgoing_id,
            seen_remote_begin: false,
        }
    }

    /// Whether the peer's `begin` has been seen.
    #[must_use]
    pub const fn seen_remote_begin(&self) -> bool {
        self.seen_remote_begin
    }

    /// What goes in `flow.next-incoming-id`.
    ///
    /// `None` until the peer's `begin` has been seen: Part 2 §2.7.4 says the
    /// field MUST NOT be set before then, and a peer receiving it anyway
    /// would recompute its own window from a number that means nothing.
    #[must_use]
    pub const fn wire_next_incoming_id(&self) -> Option<u32> {
        if self.seen_remote_begin {
            Some(self.next_incoming_id)
        } else {
            None
        }
    }

    /// Applies the peer's `begin`, which is also its first flow state.
    pub fn apply_begin(&mut self, remote: RemoteFlow) {
        self.seen_remote_begin = true;
        self.next_incoming_id = remote.next_outgoing_id;
        self.apply_flow(remote);
    }

    /// Recomputes the two derived windows from a `flow`.
    ///
    /// The formula is Part 2 §2.5.6's, including the branch for a peer that
    /// has not seen our `begin`:
    ///
    /// ```text
    /// remote-incoming-window = next-incoming-id(flow) + incoming-window(flow)
    ///                          - next-outgoing-id(us)
    /// ```
    ///
    /// and, where the flow's `next-incoming-id` is unset,
    /// `initial-outgoing-id(us)` stands in for it. Getting that branch wrong
    /// is the difference between a session that can send and one that stalls
    /// forever on its first frame.
    pub fn apply_flow(&mut self, remote: RemoteFlow) {
        self.next_incoming_id = remote.next_outgoing_id;
        self.remote_outgoing_window = remote.outgoing_window;
        let base = remote.next_incoming_id.unwrap_or(self.initial_outgoing_id);
        let limit = base.wrapping_add(remote.incoming_window);
        self.remote_incoming_window = serial_distance(limit, self.next_outgoing_id);
    }

    /// Whether one more `transfer` frame may be sent.
    ///
    /// The whole point of the scheme: at zero the sender MUST stall.
    /// Exceeding the peer's incoming window is
    /// `amqp:session:window-violation` and ends the session, so this is asked
    /// *before* a frame is encoded rather than after it is written.
    #[must_use]
    pub const fn may_send(&self) -> bool {
        self.remote_incoming_window > 0
    }

    /// Accounts for one `transfer` frame we are sending.
    ///
    /// Returns `false` and changes nothing where there was no room, so a
    /// caller that ignored [`Windows::may_send`] still cannot overrun.
    pub fn record_sent(&mut self) -> bool {
        if !self.may_send() {
            return false;
        }
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        self.remote_incoming_window -= 1;
        // Part 2 §2.5.6: the endpoint MAY also decrement its own
        // `outgoing-window` by policy. This client does, so that the number
        // it advertises stays a statement about what it will still send
        // rather than a constant.
        self.outgoing_window = self.outgoing_window.saturating_sub(1);
        true
    }

    /// Accounts for one `transfer` frame that arrived.
    ///
    /// `Err` is a window violation: the peer sent more than we said we could
    /// receive, and Part 2 §2.8.17 answers that with `end` carrying
    /// `amqp:session:window-violation`.
    pub fn record_received(&mut self, transfer_id: u32) -> Result<(), WindowViolation> {
        if self.incoming_window == 0 {
            return Err(WindowViolation {
                transfer_id,
                next_incoming_id: self.next_incoming_id,
                incoming_window: self.incoming_window,
            });
        }
        // The current maximum incoming transfer-id is
        // `incoming-window + next-incoming-id - 1`, so anything above it is
        // outside the window we advertised.
        let highest = self
            .next_incoming_id
            .wrapping_add(self.incoming_window)
            .wrapping_sub(1);
        if serial_gt(transfer_id, highest) {
            return Err(WindowViolation {
                transfer_id,
                next_incoming_id: self.next_incoming_id,
                incoming_window: self.incoming_window,
            });
        }
        self.next_incoming_id = transfer_id.wrapping_add(1);
        self.incoming_window -= 1;
        self.remote_outgoing_window = self.remote_outgoing_window.saturating_sub(1);
        Ok(())
    }

    /// Restores the incoming window to `window` after the application has
    /// taken delivery.
    ///
    /// The window is how a receiver applies backpressure: it shrinks as
    /// frames arrive and only grows again when the receiver says so, in a
    /// `flow`. This is the "says so".
    pub fn replenish_incoming(&mut self, window: u32) {
        self.incoming_window = window;
    }
}

/// The peer sent a transfer frame outside the window we advertised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowViolation {
    /// The transfer-id that arrived.
    pub transfer_id: u32,
    /// What we were expecting next.
    pub next_incoming_id: u32,
    /// How much room we had said there was.
    pub incoming_window: u32,
}

impl core::fmt::Display for WindowViolation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "transfer-id {} is outside the incoming window of {} from {}",
            self.transfer_id, self.incoming_window, self.next_incoming_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote(next_incoming_id: Option<u32>, incoming_window: u32) -> RemoteFlow {
        RemoteFlow {
            next_incoming_id,
            incoming_window,
            next_outgoing_id: 0,
            outgoing_window: 400,
        }
    }

    #[test]
    fn a_fresh_session_may_not_send_anything() {
        // Not a stall to be worked around: until the partner's begin or flow
        // arrives there is no permission to send, and that is the correct
        // starting point.
        let windows = Windows::new(0, 400, 400);
        assert!(!windows.may_send());
        assert_eq!(windows.remote_incoming_window, 0);
        assert_eq!(
            windows.wire_next_incoming_id(),
            None,
            "next-incoming-id MUST NOT be set before the peer's begin"
        );
    }

    #[test]
    fn the_peers_begin_opens_the_window_and_unlocks_the_field() {
        let mut windows = Windows::new(0, 400, 400);
        windows.apply_begin(RemoteFlow {
            next_incoming_id: Some(0),
            incoming_window: 8,
            next_outgoing_id: 100,
            outgoing_window: 400,
        });
        assert!(windows.seen_remote_begin());
        assert_eq!(windows.remote_incoming_window, 8);
        assert_eq!(
            windows.next_incoming_id, 100,
            "the two directions are numbered independently"
        );
        assert_eq!(windows.wire_next_incoming_id(), Some(100));
    }

    #[test]
    fn a_flow_from_a_peer_that_has_not_seen_our_begin_uses_our_initial_id() {
        // The branch that decides whether a session can ever send its first
        // frame. Our initial-outgoing-id is 50, so a peer offering a window
        // of 3 without knowing where we started still grants three frames.
        let mut windows = Windows::new(50, 400, 400);
        windows.apply_flow(remote(None, 3));
        assert_eq!(windows.remote_incoming_window, 3);

        // And with the field set, the same three frames are measured from
        // the peer's own number.
        let mut windows = Windows::new(50, 400, 400);
        windows.apply_flow(remote(Some(50), 3));
        assert_eq!(windows.remote_incoming_window, 3);
    }

    #[test]
    fn the_window_drives_to_zero_and_the_sender_stalls_rather_than_overruns() {
        let mut windows = Windows::new(0, 400, 400);
        windows.apply_flow(remote(Some(0), 3));
        for expected in [2, 1, 0] {
            assert!(windows.may_send());
            assert!(windows.record_sent());
            assert_eq!(windows.remote_incoming_window, expected);
        }
        // At zero the sender MUST NOT send more. Nothing is dropped and
        // nothing disconnects; it waits.
        assert!(!windows.may_send());
        assert!(
            !windows.record_sent(),
            "a caller that ignored may_send still cannot overrun"
        );
        assert_eq!(
            windows.next_outgoing_id, 3,
            "the refused frame did not advance the sequence"
        );

        // A `flow` granting more room is what releases it.
        windows.apply_flow(RemoteFlow {
            next_incoming_id: Some(3),
            incoming_window: 2,
            next_outgoing_id: 0,
            outgoing_window: 400,
        });
        assert_eq!(windows.remote_incoming_window, 2);
        assert!(windows.may_send());
    }

    #[test]
    fn a_transfer_beyond_the_advertised_window_is_a_violation() {
        let mut windows = Windows::new(0, 2, 400);
        windows.apply_begin(RemoteFlow {
            next_incoming_id: Some(0),
            incoming_window: 400,
            next_outgoing_id: 10,
            outgoing_window: 400,
        });
        // The window is 2 from id 10, so 10 and 11 are inside it and 12 is
        // not.
        windows.record_received(10).expect("inside the window");
        assert_eq!(windows.incoming_window, 1);
        assert_eq!(windows.next_incoming_id, 11);
        let violation = windows
            .record_received(12)
            .expect_err("12 is past the highest id the window admits");
        assert_eq!(violation.transfer_id, 12);
        assert_eq!(violation.next_incoming_id, 11);

        windows.record_received(11).expect("inside the window");
        assert_eq!(windows.incoming_window, 0);
        // At zero nothing further may arrive at all.
        assert!(windows.record_received(12).is_err());
        // Until the receiver says so.
        windows.replenish_incoming(4);
        windows.record_received(12).expect("the window reopened");
    }

    #[test]
    fn transfer_ids_are_serial_numbers_and_wrap() {
        // A session that transferred four billion frames and then compared
        // ids by magnitude would conclude it had gone backwards.
        assert!(serial_gt(1, 0));
        assert!(serial_gt(0, u32::MAX), "0 is after 2^32-1");
        assert!(!serial_gt(u32::MAX, 0));
        assert!(!serial_gt(5, 5), "a number is not after itself");
        assert_eq!(serial_distance(2, u32::MAX), 3);
        assert_eq!(
            serial_distance(0, 5),
            0,
            "saturates rather than wrapping far"
        );

        let mut windows = Windows::new(u32::MAX - 1, 400, 400);
        windows.apply_flow(remote(Some(u32::MAX - 1), 4));
        assert_eq!(windows.remote_incoming_window, 4);
        for _ in 0..4 {
            assert!(windows.record_sent());
        }
        assert_eq!(
            windows.next_outgoing_id, 2,
            "the sequence wrapped through zero"
        );
    }

    #[test]
    fn a_window_of_zero_from_the_peer_is_an_immediate_stall() {
        // RabbitMQ sends `incoming_window = 0` on every session under a
        // memory or disk alarm, which is exactly this.
        let mut windows = Windows::new(0, 400, 400);
        windows.apply_flow(remote(Some(0), 400));
        assert!(windows.may_send());
        windows.apply_flow(remote(Some(0), 0));
        assert!(!windows.may_send());
    }

    #[test]
    fn the_outgoing_window_we_advertise_shrinks_as_we_send() {
        let mut windows = Windows::new(0, 400, 2);
        windows.apply_flow(remote(Some(0), 400));
        assert!(windows.record_sent());
        assert_eq!(windows.outgoing_window, 1);
        assert!(windows.record_sent());
        assert_eq!(windows.outgoing_window, 0);
        // It is a policy number, not a permission: the peer's incoming
        // window is what stops us, and it still has room.
        assert!(windows.may_send());
    }
}
