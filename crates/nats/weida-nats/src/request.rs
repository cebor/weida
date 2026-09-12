//! Request-reply: the inbox, the mandatory window, and the two ways a
//! request can end without an answer.
//!
//! # There is no correlation field
//!
//! > A requester subscribes to a unique inbox and publishes a request whose
//! > reply-to field is that inbox. A responder receives the reply subject
//! > alongside the request and publishes its response to that subject. The
//! > reply is therefore routed by ordinary subject interest, not by a hidden
//! > correlation field. (`docs/research/nats.md` §4)
//!
//! So there is no correlation identifier anywhere in this crate — not on the
//! wire, not in a header, not in the message type. The reply subject *is* the
//! correlation: it is unique per request, the responder can only have learned
//! it from that request, and the server routes the answer by subject interest
//! like any other publication. A client that added an id would be adding a
//! second, weaker mechanism beside the one the protocol has.
//!
//! # One inbox subscription, not one per request
//!
//! The connection subscribes once to `<prefix><token>.>` and gives each
//! request a reply subject `<prefix><token>.<n>`. The alternative — a `SUB`
//! and an `UNSUB` around every request — costs two control lines per round
//! trip and makes the server do two table edits for each one.
//!
//! What that buys is one client-local map from reply subject to the waiter,
//! which is still subject routing: the wildcard subscription delivers
//! everything under the connection's inbox and the exact subject picks the
//! request. The map is remote-influenced in size — one entry per outstanding
//! request — so it has a named bound of ours,
//! [`ConnectionOptions::max_pending_requests`](crate::options::ConnectionOptions::max_pending_requests),
//! because the protocol bounds it nowhere.
//!
//! # The inbox token is unique, not secret
//!
//! `_INBOX.` plus "something unique" is all the protocol asks for. This crate
//! builds that token from three things it already has: the process id, a
//! nanosecond clock reading, and a process-local counter. **It is not
//! cryptographically random and is not meant to be.**
//!
//! That is acceptable here because an inbox name is a routing token rather
//! than a secret. Authorization is the server's: "connections authenticate
//! with configured credentials ... then are authorized by account and subject
//! permissions" (`docs/research/nats.md` §12 P14), and a `_INBOX.>`
//! permission is how deployments scope inboxes. Guessing this client's inbox
//! subject gains an attacker exactly what a subject permission already allows
//! it — nothing more — while what correctness actually needs is that two
//! requests never share a reply subject. The counter gives that inside one
//! process, the process id across processes on one host, and the clock
//! reading across restarts that reuse a process id.
//!
//! Where an application needs an unguessable inbox, it sets
//! [`ConnectionOptions::inbox_prefix`](crate::options::ConnectionOptions::inbox_prefix)
//! to a prefix it generated with a source of randomness it trusts — the same
//! division of labour as the TLS trust anchors and the NKey signature.
//!
//! # The window is the caller's and is mandatory
//!
//! Every request takes a `Duration` as a plain argument, not an
//! `Option<Duration>` with a default. "Client request APIs create and manage
//! an `_INBOX` reply subscription and impose a caller-selected timeout"
//! (§4) — and a request API that *can* hang is the failure this module
//! exists to prevent. Core NATS has no delivery receipt of any kind, so a
//! request with no deadline is a task that may never be scheduled again.
//!
//! # Two outcomes, never confused
//!
//! * `NATS/1.0 503` on the inbox — [`Error::NoResponders`] — means the
//!   server found no interest in the request's subject *at the moment it was
//!   published*. It arrives in one round trip, and only where `headers` and
//!   `no_responders` were both negotiated in `CONNECT`.
//! * The window elapsing — [`Error::RequestTimeout`] — means somebody may
//!   well have been listening and did not answer in time.
//!
//! Reporting the first as the second turns a missing service into a slow one
//! and costs the caller the whole window to find out.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::message::Message;

/// The inbox of one connection: a unique stem and a counter under it.
#[derive(Debug)]
pub(crate) struct Inbox {
    /// `<prefix><token>`, without a trailing dot.
    stem: String,
    /// The next reply subject's last token.
    next: AtomicU64,
}

impl Inbox {
    /// Builds the inbox stem for a new connection.
    ///
    /// `prefix` ends in a dot, which
    /// [`ConnectionOptions::validate`](crate::options::ConnectionOptions::validate)
    /// has already checked, so the stem is `prefix` plus a token of hex
    /// digits — no dot, no space, nothing a control line would split on.
    pub(crate) fn new(prefix: &str) -> Self {
        /// Distinguishes two connections made in the same nanosecond by the
        /// same process, which is the case a clock reading alone misses.
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);

        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let pid = std::process::id();
        Self {
            stem: format!("{prefix}{pid:x}{nanos:x}{sequence:x}"),
            next: AtomicU64::new(0),
        }
    }

    /// The subject the inbox subscription asks for: everything under the
    /// stem.
    ///
    /// `>` rather than `*` so that a nested reply subject — which JetStream's
    /// own conventions use — still arrives.
    pub(crate) fn pattern(&self) -> String {
        format!("{}.>", self.stem)
    }

    /// A reply subject no other request on this connection will be given.
    pub(crate) fn next_reply_subject(&self) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{}.{n}", self.stem)
    }
}

/// The requests waiting for a reply, keyed by their reply subject.
///
/// Owned by the driver, like the subscription table, and bounded by a number
/// of ours.
#[derive(Debug)]
pub(crate) struct Pending {
    entries: HashMap<Vec<u8>, mpsc::Sender<Message>>,
    max: usize,
}

impl Pending {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max,
        }
    }

    /// The bound, for the error that reports it.
    pub(crate) const fn max(&self) -> usize {
        self.max
    }

    /// Registers a waiter. `false` where the map is full.
    pub(crate) fn insert(&mut self, reply_subject: Vec<u8>, tx: mpsc::Sender<Message>) -> bool {
        if self.entries.len() >= self.max {
            return false;
        }
        self.entries.insert(reply_subject, tx);
        true
    }

    pub(crate) fn remove(&mut self, reply_subject: &[u8]) {
        self.entries.remove(reply_subject);
    }

    /// Hands a reply to the request whose reply subject it arrived on.
    ///
    /// The subject is the only key. A reply that arrives for a request that
    /// has already given up is dropped, which is the honest answer: the
    /// caller's window is over and Core NATS keeps nothing for replay.
    pub(crate) fn dispatch(&mut self, message: Message) -> bool {
        let Some(tx) = self.entries.get(&message.subject) else {
            return false;
        };
        // Bounded and non-blocking: the waiter's channel is sized for the
        // responses it asked for, and a request that got its answers does not
        // get to stall the driver with the ones that came late.
        tx.try_send(message).is_ok()
    }

    /// Ends every outstanding request, which is what the end of the
    /// connection does to them.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Turns the first message on an inbox into the outcome it represents.
///
/// The 503 check is here rather than at the call sites so that the single and
/// the scatter-gather form cannot disagree about it.
pub(crate) fn outcome(message: Message) -> Result<Message> {
    if message.is_no_responders() {
        return Err(Error::NoResponders);
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{NO_RESPONDERS, OwnedHeaders};

    /// Two connections in one process must not share an inbox stem, or one
    /// connection's replies would arrive on the other's wildcard
    /// subscription.
    #[test]
    fn two_inboxes_in_one_process_differ() {
        let first = Inbox::new("_INBOX.");
        let second = Inbox::new("_INBOX.");
        assert_ne!(first.stem, second.stem);
        assert!(first.stem.starts_with("_INBOX."));
        assert!(
            first.pattern().ends_with(".>"),
            "the inbox subscription takes everything under the stem"
        );
    }

    /// The token must be a single subject token: a dot or a space in it would
    /// silently change the shape of every reply subject and of the wildcard
    /// that collects them.
    #[test]
    fn the_inbox_token_is_one_subject_token() {
        let inbox = Inbox::new("_INBOX.");
        let token = inbox.stem.strip_prefix("_INBOX.").expect("the prefix");
        assert!(
            token.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{token:?}"
        );
        crate::subject::check_subscribe_subject(inbox.pattern().as_bytes())
            .expect("the inbox pattern is a subject this client would send");
        crate::subject::check_publish_subject(inbox.next_reply_subject().as_bytes())
            .expect("a reply subject is a literal subject");
    }

    /// Each request gets its own reply subject, because that subject is the
    /// whole of the correlation.
    #[test]
    fn every_request_gets_its_own_reply_subject() {
        let inbox = Inbox::new("_INBOX.");
        let first = inbox.next_reply_subject();
        let second = inbox.next_reply_subject();
        assert_ne!(first, second);
        assert!(first.starts_with(&inbox.stem));
        assert!(crate::subject::matches(
            inbox.pattern().as_bytes(),
            second.as_bytes()
        ));
    }

    /// A reply is routed by its subject and nothing else, and one that has no
    /// waiter is dropped rather than delivered somewhere.
    #[test]
    fn a_reply_goes_to_the_subject_that_asked() {
        let mut pending = Pending::new(4);
        let (tx, mut rx) = mpsc::channel(1);
        assert!(pending.insert(b"_INBOX.a.1".to_vec(), tx));

        let mine = Message {
            subject: b"_INBOX.a.1".to_vec(),
            payload: b"answer".to_vec(),
            ..Message::default()
        };
        assert!(pending.dispatch(mine));
        assert_eq!(rx.try_recv().expect("delivered").payload, b"answer");

        let stray = Message {
            subject: b"_INBOX.a.2".to_vec(),
            ..Message::default()
        };
        assert!(!pending.dispatch(stray), "no waiter, no delivery");

        pending.remove(b"_INBOX.a.1");
        let late = Message {
            subject: b"_INBOX.a.1".to_vec(),
            ..Message::default()
        };
        assert!(
            !pending.dispatch(late),
            "a reply that arrives after the caller gave up has nowhere to go"
        );
    }

    /// The map is bounded, because every entry is a request that may never be
    /// answered.
    #[test]
    fn the_pending_map_is_bounded() {
        let mut pending = Pending::new(1);
        let (first, _rx1) = mpsc::channel(1);
        let (second, _rx2) = mpsc::channel(1);
        assert!(pending.insert(b"_INBOX.a.1".to_vec(), first));
        assert!(!pending.insert(b"_INBOX.a.2".to_vec(), second));
        assert_eq!(pending.max(), 1);
    }

    /// A 503 is not an answer, and the conversion says so in one place so the
    /// two request forms cannot disagree.
    #[test]
    fn a_503_is_the_no_responder_error_and_not_a_reply() {
        let mut headers = OwnedHeaders::new();
        headers.status = Some(NO_RESPONDERS);
        let message = Message {
            headers: Some(headers),
            ..Message::default()
        };
        assert!(matches!(outcome(message), Err(Error::NoResponders)));

        let ordinary = Message {
            payload: b"answer".to_vec(),
            ..Message::default()
        };
        assert_eq!(
            outcome(ordinary).expect("a reply").payload,
            b"answer",
            "an ordinary reply passes through untouched"
        );
    }
}
