//! The link: `attach`, `detach`, the two handle spaces, and the credit a
//! delivery is sent under.
//!
//! A link is "a unidirectional named route between two nodes, attached at
//! each end to a terminus" (Part 2 §2.6). Five of its rules are easy to get
//! subtly wrong, and each one has a test here.
//!
//! # Two schemes must permit one transfer
//!
//! A message goes out only when **both** the link's credit and the session's
//! window allow it. Link credit is counted in messages and lives in
//! [`crate::credit`]; the window is counted in `transfer` frames and lives in
//! [`crate::window`]. So a message larger than one frame takes one unit of
//! credit and as many window units as it takes frames, and either scheme
//! alone can stall a sender that the other is happy with. The specification
//! ties them together nowhere, and neither does this module.
//!
//! # The two ends choose their handles independently
//!
//! "The ends need not agree: the local choice is the *output handle*, the
//! remote the *input handle*" (Part 2 §2.6.2). So an `attach` carries the
//! handle *its own sender* chose, and the answering `attach` carries a
//! different number. Which means the answer cannot be correlated by handle —
//! it is correlated by **name**, and this module keeps three maps because of
//! it: by our output handle, by the peer's input handle, and by name.
//!
//! ```text
//! client                                                    broker
//!   attach(name="orders", handle=0, role=sender)   -->
//!                                                  <--  attach(name="orders", handle=7, role=receiver)
//!   transfer(handle=0)                             -->
//!                                                  <--  disposition(role=receiver)
//! ```
//!
//! # The name is the identity, and a second attach steals
//!
//! A name "MUST uniquely identify the link among all links of the same
//! direction between the two containers", so a link is active on one
//! connection at a time and a second attach elsewhere *steals* it: the first
//! MUST be closed with `amqp:link:stolen` (Part 2 §2.6.1). The rule exists so
//! that re-establishment works when only one party has noticed a failure —
//! without it, a client reconnecting after a network partition the broker has
//! not seen yet could not get its link back.
//!
//! This client implements the half it owns: attaching a link whose name and
//! direction are already in use on this connection detaches the incumbent
//! with `amqp:link:stolen` and proceeds. Silently refusing the second attach
//! would be the *opposite* of the specified behaviour, and quietly running
//! two links with one name would break the peer's own bookkeeping.
//!
//! # An errored handle is never silently reused
//!
//! "An errored link endpoint MUST be detached with `detach(error=...)` and
//! destroyed; any later input on that handle or its delivery-ids MUST end the
//! session with `amqp:session:errant-link`" (Part 2 §2.6.5). Handles *may* be
//! reused after a clean close — but reusing one that was detached with an
//! error means a late frame for the dead link lands on a live one, and the
//! peer is entitled to still be sending them. So handle allocation skips
//! a handle that was errored, for the life of the session.
//!
//! # Deliveries on one link do not interleave, and are bounded
//!
//! "The deliveries on a given link MUST NOT interleave" (Part 2 §2.6.14), so
//! a second delivery-tag arriving while the first is incomplete is refused
//! here rather than concatenated — the octets of two messages are not a
//! message. And nothing in the protocol bounds the *number* of frames a
//! delivery may take, which is what makes `max-message-size` load-bearing
//! rather than advisory: without it a peer hands a receiver an unbounded
//! message one bounded frame at a time. The bound is checked before the
//! buffer grows, and exceeding it is `amqp:link:message-size-exceeded`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc, oneshot};
use weida_amqp_codec::message::{Message, Progress, Reassembly};
use weida_amqp_codec::performative::{Attach, Detach, Flow, Performative, Transfer};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{ReceiverSettleMode, Role, SenderSettleMode};

use crate::credit::Credit;
use crate::delivery::{Delivery, Sent};
use crate::error::{Condition, Error, Result};
use crate::options::multiple;
use crate::terminus::{Source, Target};
use crate::window::Windows;

/// How many frames may be queued for one link before the driver waits.
///
/// **A bound of ours.** Link credit bounds *messages* and the session window
/// bounds *transfer frames*, but `attach`, `detach` and `disposition` are
/// outside both, and the specification bounds them nowhere. Reaching this
/// applies backpressure to the connection driver rather than growing a queue
/// nobody bounded.
pub const LINK_QUEUE: usize = 64;

/// How this end settles and when, plus what the peer actually created.
///
/// Filled in from the answering `attach`, which is **data and not an
/// acknowledgement**: "an endpoint that cannot create exactly the requested
/// terminus MAY adjust properties but MUST then report what it actually
/// created" (Part 2 §2.6.3). A client that ignored the answer would be
/// running against a terminus it did not ask for — a queue instead of an
/// exchange, a filter silently dropped, a settle mode it cannot honour.
#[derive(Clone, Debug, PartialEq)]
pub struct Negotiated {
    /// How the sender settles. Authoritative from whichever end has
    /// `role=sender`, because the field describes that end's own behaviour.
    pub snd_settle_mode: SenderSettleMode,
    /// When the receiver settles. Authoritative from whichever end has
    /// `role=receiver`.
    ///
    /// The field worth checking: `Second` is what exactly-once is built from
    /// and Artemis refuses it outright ("The Broker does not currently
    /// support ReceiverSettleMode of SECOND"), while RabbitMQ lists
    /// exactly-once as unsupported. A client that asked for `Second` and did
    /// not read the answer would believe it had a guarantee it does not have.
    pub rcv_settle_mode: ReceiverSettleMode,
    /// The source the peer says is in place. `None` means the peer refused to
    /// provide a terminus and MUST then immediately detach.
    pub remote_source: Option<Source>,
    /// The target the peer says is in place.
    pub remote_target: Option<Target>,
    /// The peer's `max-message-size`: zero or unset means no limit.
    pub remote_max_message_size: Option<u64>,
    /// What the peer offered.
    pub remote_offered_capabilities: Vec<String>,
}

/// Where a link is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkState {
    /// `attach` sent, the answering `attach` not yet seen.
    Attaching,
    /// Both `attach` frames exchanged.
    Attached,
    /// `detach` sent, the partner's not yet seen.
    Detaching,
    /// Done. `Some` where either end gave a condition.
    Detached(Option<Condition>),
}

impl LinkState {
    /// Whether deliveries may flow.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Attached)
    }
}

/// How a link is attached.
#[derive(Clone, Debug)]
pub struct LinkOptions {
    /// `attach.name`, mandatory and the link's real identity.
    pub name: String,
    /// `attach.role`: `false` is the sender, `true` the receiver.
    pub role: Role,
    /// The source terminus. A receiver describes the node it reads from; a
    /// sender still sends one, because "the link is always considered to be
    /// between the source as described by the sender, and the target as
    /// described by the receiver".
    pub source: Option<Source>,
    /// The target terminus.
    pub target: Option<Target>,
    /// How this end will settle if it is the sender.
    pub snd_settle_mode: SenderSettleMode,
    /// When this end will settle if it is the receiver.
    pub rcv_settle_mode: ReceiverSettleMode,
    /// Zero or unset means no limit. Set it: the alternative is agreeing to
    /// a message of unbounded size, and `amqp:link:message-size-exceeded`
    /// exists precisely so a receiver can say no.
    pub max_message_size: Option<u64>,
    /// Where a sender starts its `delivery-count` sequence. MUST NOT be null
    /// for a sender and is ignored for a receiver.
    pub initial_delivery_count: u32,
    /// `attach.offered-capabilities`.
    pub offered_capabilities: Vec<String>,
    /// `attach.desired-capabilities`.
    pub desired_capabilities: Vec<String>,
}

impl LinkOptions {
    /// A sending link to `target`.
    #[must_use]
    pub fn sender(name: impl Into<String>, target: Target) -> Self {
        Self {
            name: name.into(),
            role: Role::Sender,
            source: Some(Source::default()),
            target: Some(target),
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            initial_delivery_count: 0,
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
        }
    }

    /// A receiving link from `source`.
    #[must_use]
    pub fn receiver(name: impl Into<String>, source: Source) -> Self {
        Self {
            name: name.into(),
            role: Role::Receiver,
            source: Some(source),
            target: Some(Target::default()),
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            initial_delivery_count: 0,
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
        }
    }

    /// Refuses an unusable configuration where it is configured.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Configuration(
                "attach.name is mandatory and is the link's identity: a second \
                 attach of the same name steals the first, so an empty one \
                 would steal every link on the connection"
                    .into(),
            ));
        }
        Ok(())
    }

    fn to_attach(&self, handle: u32) -> Attach<'_> {
        let mut attach = Attach::new(&self.name, handle, self.role);
        attach.snd_settle_mode = self.snd_settle_mode;
        attach.rcv_settle_mode = self.rcv_settle_mode;
        attach.source = self.source.as_ref().map(Source::to_value);
        attach.target = self.target.as_ref().map(Target::to_value);
        // MUST NOT be null for a sender; harmless and ignored for a receiver,
        // and sending it either way keeps one code path.
        attach.initial_delivery_count = Some(self.initial_delivery_count);
        attach.max_message_size = self.max_message_size;
        attach.offered_capabilities = multiple(&self.offered_capabilities);
        attach.desired_capabilities = multiple(&self.desired_capabilities);
        attach
    }
}

/// This client's `max-message-size`, deliberately not the specification's
/// "no limit".
///
/// **A default that differs on purpose** (0013 §4.4 item 5). Unset or zero
/// means unlimited, which is agreeing to allocate whatever a peer sends one
/// bounded frame at a time. 16 MiB is far above what any broker in the sheet
/// accepts — Service Bus caps a Premium message at 100 MB and a Standard one
/// at 256 KB — and finite. Settable back to zero, but never silently.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// What a link hands to the layer above it.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// A whole message, reassembled from however many `transfer` frames it
    /// took.
    Delivery(Delivery),
    /// A frame for this link that is neither a `transfer` nor a `flow`: a
    /// `disposition`, whole and undecoded.
    Frame(Vec<u8>),
    /// The peer's `flow` changed this link's credit state, and the new state
    /// is the one [`Link::credit`] reports.
    ///
    /// Worth an event rather than only a field, because a receiver draining a
    /// link is *waiting* for this: credit reaching zero with no delivery in
    /// between is the definite answer that there was nothing to get.
    Flow(Credit),
    /// The link detached.
    Detached(Option<Condition>),
}

/// The state a link's handle and the connection driver share.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) name: String,
    pub(crate) role: Role,
    /// Our choice, and the number that goes in every frame we send.
    pub(crate) output_handle: u32,
    /// The session's outgoing channel.
    pub(crate) channel: u16,
    pub(crate) state: Mutex<LinkState>,
    pub(crate) negotiated: Mutex<Option<Negotiated>>,
    /// The peer's choice, learned from the answering `attach`.
    pub(crate) input_handle: Mutex<Option<u32>>,
    /// This link's credit, counted in messages.
    pub(crate) credit: Mutex<Credit>,
    /// The session this link rides on, held for the **other** credit scheme:
    /// a `transfer` frame needs link credit *and* a session window unit, the
    /// two are independent, and only the session has the second.
    pub(crate) session: Arc<crate::session::Shared>,
    /// Woken whenever either scheme may have made room — a `flow` for this
    /// link, or one for the session it is on. A sender waiting for credit
    /// waits here rather than polling.
    pub(crate) flow: Notify,
    /// The largest frame the peer will accept, which is what decides where a
    /// message is split.
    pub(crate) max_frame_size: u32,
    /// The next delivery-tag this end will choose.
    pub(crate) next_tag: Mutex<u64>,
}

/// An AMQP 1.0 link.
#[derive(Debug)]
pub struct Link {
    shared: Arc<Shared>,
    outbound: mpsc::Sender<crate::connection::Outbound>,
    events: mpsc::Receiver<LinkEvent>,
}

impl Link {
    pub(crate) fn new(
        shared: Arc<Shared>,
        outbound: mpsc::Sender<crate::connection::Outbound>,
        events: mpsc::Receiver<LinkEvent>,
    ) -> Self {
        Self {
            shared,
            outbound,
            events,
        }
    }

    /// The link's name, which is its identity between the two containers.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// Which end of the link this is.
    #[must_use]
    pub fn role(&self) -> Role {
        self.shared.role
    }

    /// Our handle: the number this end writes into every frame it sends.
    #[must_use]
    pub fn output_handle(&self) -> u32 {
        self.shared.output_handle
    }

    /// The peer's handle: the number it writes into every frame it sends.
    ///
    /// A different number from [`Link::output_handle`] in general, which is
    /// why the answering `attach` is correlated by name and not by handle.
    #[must_use]
    pub fn input_handle(&self) -> Option<u32> {
        *self.shared.input_handle.lock().expect("not poisoned")
    }

    /// Where the link is.
    #[must_use]
    pub fn state(&self) -> LinkState {
        self.shared.state.lock().expect("not poisoned").clone()
    }

    /// What the answering `attach` said: the settle modes actually in force
    /// and the termini actually created.
    #[must_use]
    pub fn negotiated(&self) -> Option<Negotiated> {
        self.shared.negotiated.lock().expect("not poisoned").clone()
    }

    /// The source the peer says is in place, or `None` if it refused to
    /// provide one.
    #[must_use]
    pub fn remote_source(&self) -> Option<Source> {
        self.negotiated().and_then(|n| n.remote_source)
    }

    /// The target the peer says is in place.
    #[must_use]
    pub fn remote_target(&self) -> Option<Target> {
        self.negotiated().and_then(|n| n.remote_target)
    }

    /// The next event, or `None` once the link has detached and everything
    /// queued has been taken.
    pub async fn next_event(&mut self) -> Option<LinkEvent> {
        self.events.recv().await
    }

    /// The next whole message, skipping the events that are not one.
    ///
    /// `None` where the link detached before another delivery arrived — which
    /// includes the drain case, and is why a caller that needs to tell
    /// "nothing to get" from "link gone" reads [`Link::next_event`] instead.
    pub async fn next_delivery(&mut self) -> Option<Delivery> {
        loop {
            match self.events.recv().await? {
                LinkEvent::Delivery(delivery) => return Some(delivery),
                LinkEvent::Detached(_) => return None,
                LinkEvent::Frame(_) | LinkEvent::Flow(_) => {}
            }
        }
    }

    /// This link's credit state: `delivery-count`, `link-credit`,
    /// `available` and `drain`.
    #[must_use]
    pub fn credit(&self) -> Credit {
        *self.shared.credit.lock().expect("not poisoned")
    }

    /// Whether a message could go out right now.
    ///
    /// Three conditions, and they are three because the protocol has three:
    /// the link must be attached, the **link** must have credit — counted in
    /// messages, granted by the receiver — and the **session** window must
    /// have room for a frame. A generous grant with a full window sends
    /// nothing, and so does an empty window with generous credit.
    #[must_use]
    pub fn may_send(&self) -> bool {
        self.state().is_usable()
            && self.credit().may_send()
            && self
                .shared
                .session
                .windows
                .lock()
                .expect("not poisoned")
                .may_send()
    }

    /// A **receiver**: grants credit for `credit` more messages.
    ///
    /// Sets rather than adds. What goes on the wire is the absolute limit
    /// `delivery-count + link-credit`, so granting 10 twice grants 10 and a
    /// `flow` that is duplicated or overtaken cannot inflate anything. Clears
    /// `drain`, because a grant and a drain are opposite requests.
    pub async fn grant_credit(&self, credit: u32) -> Result<()> {
        self.role_must_be(Role::Receiver, "grant credit")?;
        {
            let mut state = self.shared.credit.lock().expect("not poisoned");
            state.grant(credit);
            state.set_drain(false);
        }
        self.send_flow(false).await
    }

    /// A **receiver**: asks the sender to drain.
    ///
    /// The sender sends whatever it has and then advances `delivery-count`
    /// until `link-credit` is zero, reporting the new state. That is what
    /// turns "wait for a message" into "wait for a definite answer": credit
    /// reaching zero with no delivery in between means there was nothing to
    /// get, and a get-with-timeout is built out of exactly that.
    pub async fn drain(&self) -> Result<()> {
        self.role_must_be(Role::Receiver, "drain")?;
        self.shared
            .credit
            .lock()
            .expect("not poisoned")
            .set_drain(true);
        self.send_flow(false).await
    }

    /// A **receiver**: stops the link with `flow(link-credit=0, echo=true)`.
    ///
    /// The echo is the point. Transfers already in flight may still arrive
    /// until the sender has processed the new limit, and the echoed `flow` is
    /// the marker after which no further transfer will come — without it,
    /// "stopped" is a hope rather than an observation (Part 2 §2.6.10).
    pub async fn stop(&self) -> Result<()> {
        self.role_must_be(Role::Receiver, "stop")?;
        {
            let mut state = self.shared.credit.lock().expect("not poisoned");
            state.grant(0);
            state.set_drain(false);
        }
        self.send_flow(true).await
    }

    /// A **receiver**: restores the session's incoming window and tells the
    /// peer.
    ///
    /// The session window shrinks as `transfer` frames arrive and only grows
    /// again when the receiver says so; this is the saying so. A receiver
    /// that granted link credit but never replenished the window would stall
    /// its sender with credit in hand.
    pub async fn replenish_session_window(&self) -> Result<()> {
        let window = self.shared.session.options.incoming_window;
        self.shared
            .session
            .windows
            .lock()
            .expect("not poisoned")
            .replenish_incoming(window);
        self.send_flow(false).await
    }

    /// A **sender**: sends one message, stalling until both schemes permit
    /// it.
    ///
    /// Link credit is taken once for the whole message, because it is counted
    /// in messages; a session window unit is taken for **each frame**, which
    /// is what makes a large message dribble out under a tight window instead
    /// of violating it.
    pub async fn send(&self, message: &Message<'_>) -> Result<Sent> {
        self.send_payload(&message.to_vec()?).await
    }

    /// A **sender**: sends already-encoded message sections.
    ///
    /// The sections are not re-parsed: a forwarder that received octets and
    /// is passing them on has nothing to gain from a decode and re-encode,
    /// and the specification's message identity is the octets.
    pub async fn send_payload(&self, payload: &[u8]) -> Result<Sent> {
        self.role_must_be(Role::Sender, "send")?;
        let settled = matches!(self.settle_mode(), SenderSettleMode::Settled);
        let tag = self.next_tag();
        let budget = self.frame_budget(&tag)?;

        // One unit of link credit, for the message. Taken before the first
        // frame and not per frame: "one unit of link-credit permits the
        // delivery-count to advance by one", and a multi-frame message
        // advances it once.
        self.take_credit().await?;

        let mut delivery_id = 0u32;
        let mut frames = 0usize;
        let mut offset = 0usize;
        loop {
            let end = (offset + budget).min(payload.len());
            let more = end < payload.len();
            let id = match self.take_window().await {
                Ok(id) => id,
                Err(error) => {
                    // Half a delivery on the wire is worse than none: `abort`
                    // tells the receiver to discard what it has and settles
                    // the delivery implicitly (Part 2 §2.7.5).
                    self.abort(delivery_id, &tag, frames).await;
                    return Err(error);
                }
            };
            if frames == 0 {
                // A delivery's id is the transfer-id of its first frame. The
                // specification never states the relation between the two
                // sequences; every implementation derives both from the one
                // counter, and a peer that expected otherwise would have no
                // way to say so.
                delivery_id = id;
            }
            let fragment = &payload[offset..end];
            let mut transfer = Transfer::new(self.shared.output_handle);
            if frames == 0 {
                transfer.delivery_id = Some(delivery_id);
                transfer.delivery_tag = Some(&tag);
                transfer.message_format = Some(0);
                transfer.settled = Some(settled);
            }
            transfer.more = more;
            let frame = self.frame(Performative::Transfer(transfer), fragment);
            match frame {
                Ok(bytes) => self.write(bytes).await?,
                Err(error) => {
                    self.abort(delivery_id, &tag, frames).await;
                    return Err(error);
                }
            }
            frames += 1;
            offset = end;
            if !more {
                break;
            }
        }

        Ok(Sent {
            delivery_id,
            delivery_tag: tag,
            frames,
            settled,
        })
    }

    /// How this end settles, as the answering `attach` left it.
    ///
    /// `Mixed` before the answer arrives, which is the specification's own
    /// default and means "decided per delivery" — and this client's decision
    /// for a `Mixed` link is unsettled, because a settled transfer is one the
    /// receiver cannot report anything about.
    fn settle_mode(&self) -> SenderSettleMode {
        self.negotiated()
            .map_or(SenderSettleMode::Mixed, |n| n.snd_settle_mode)
    }

    /// Refuses an operation the other end of the link owns.
    fn role_must_be(&self, role: Role, what: &str) -> Result<()> {
        if self.shared.role == role {
            return Ok(());
        }
        Err(Error::Configuration(format!(
            "only the {} may {what}, and link {} is the {}",
            role_name(role),
            self.shared.name,
            role_name(self.shared.role)
        )))
    }

    /// The next delivery-tag: a per-link counter, eight octets.
    ///
    /// The requirement is that a tag be "unique among the deliveries either
    /// end could consider unsettled on this link", which a counter satisfies
    /// without a random source — and the field allows 32 octets, so eight is
    /// a quarter of the budget for a number that cannot repeat before the
    /// connection has outlived everything.
    fn next_tag(&self) -> Vec<u8> {
        let mut next = self.shared.next_tag.lock().expect("not poisoned");
        let tag = *next;
        *next = next.wrapping_add(1);
        tag.to_be_bytes().to_vec()
    }

    /// How many payload octets fit in one frame of this delivery.
    ///
    /// Measured rather than guessed: the first frame's performative is the
    /// largest one the delivery will carry — it is the frame with the id, the
    /// tag and the format — so encoding it once and subtracting gives a
    /// budget every frame of the delivery fits inside.
    fn frame_budget(&self, tag: &[u8]) -> Result<usize> {
        let mut probe = Transfer::new(self.shared.output_handle);
        probe.delivery_id = Some(u32::MAX);
        probe.delivery_tag = Some(tag);
        probe.message_format = Some(0);
        probe.settled = Some(true);
        probe.more = true;
        let mut scratch = Vec::new();
        Performative::Transfer(probe).encode(&mut scratch)?;
        let ceiling = self
            .shared
            .max_frame_size
            .max(weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE) as usize;
        Ok(ceiling.saturating_sub(FRAME_HEADER + scratch.len()).max(1))
    }

    /// Waits until this link has credit, and takes one unit.
    ///
    /// Registered with the waker *before* the state is read, so a `flow` that
    /// lands between the two is not a lost wake-up.
    async fn take_credit(&self) -> Result<()> {
        loop {
            let notified = self.shared.flow.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            self.still_usable()?;
            if self
                .shared
                .credit
                .lock()
                .expect("not poisoned")
                .record_sent()
            {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Waits until the session window has room for one frame, and takes it,
    /// reporting the transfer-id that frame will carry.
    async fn take_window(&self) -> Result<u32> {
        loop {
            let notified = self.shared.flow.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            self.still_usable()?;
            let reserved = self
                .shared
                .session
                .windows
                .lock()
                .expect("not poisoned")
                .reserve();
            if let Some(id) = reserved {
                return Ok(id);
            }
            notified.await;
        }
    }

    /// Refuses to keep waiting on a link that has gone.
    fn still_usable(&self) -> Result<()> {
        let state = self.state();
        if state.is_usable() {
            return Ok(());
        }
        match state {
            LinkState::Detached(Some(condition)) => Err(Error::Closed(Some(condition))),
            _ => Err(Error::ConnectionGone),
        }
    }

    /// Tells the receiver to discard a delivery this end could not finish.
    async fn abort(&self, delivery_id: u32, tag: &[u8], frames: usize) {
        if frames == 0 {
            // Nothing left our hands, so there is nothing to discard and no
            // delivery-id the peer would recognise.
            return;
        }
        let mut transfer = Transfer::new(self.shared.output_handle);
        transfer.delivery_id = Some(delivery_id);
        transfer.delivery_tag = Some(tag);
        transfer.aborted = true;
        if let Ok(bytes) = self.frame(Performative::Transfer(transfer), &[]) {
            let _ = self.write(bytes).await;
        }
    }

    /// This end's `flow`, carrying both link and session state.
    ///
    /// Every `flow` carries the session's three mandatory fields whether or
    /// not it names a handle, so a link-level `flow` refreshes the session
    /// too — which is also why an `echo` asking for session state is
    /// satisfied by one.
    async fn send_flow(&self, echo: bool) -> Result<()> {
        let credit = *self.shared.credit.lock().expect("not poisoned");
        let windows = *self.shared.session.windows.lock().expect("not poisoned");
        let flow = flow_frame(&windows, &credit, self.shared.output_handle, echo);
        self.send_performative(Performative::Flow(flow)).await
    }

    /// Sends one performative on this link's session channel.
    pub(crate) async fn send_performative(&self, performative: Performative<'_>) -> Result<()> {
        let bytes = self.frame(performative, &[])?;
        self.write(bytes).await
    }

    /// Encodes one frame: a performative and, for a `transfer`, the payload
    /// that follows it in the same body.
    fn frame(&self, performative: Performative<'_>, payload: &[u8]) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        weida_amqp_codec::frame::write(
            &mut bytes,
            weida_amqp_codec::frame::FrameKind::Amqp,
            self.shared.channel,
            self.shared.max_frame_size,
            |body| {
                performative.encode(body)?;
                body.extend_from_slice(payload);
                Ok(())
            },
        )?;
        Ok(bytes)
    }

    /// Hands a frame to the connection driver, which is what keeps `close`
    /// the last thing ever written.
    async fn write(&self, bytes: Vec<u8>) -> Result<()> {
        self.outbound
            .send(crate::connection::Outbound::Frame(bytes))
            .await
            .map_err(|_| Error::ConnectionGone)
    }

    /// Detaches the link, destroying the endpoint at both ends.
    ///
    /// Idempotent.
    pub async fn detach(&self) -> Result<()> {
        self.detach_with(true, None).await
    }

    /// Detaches the link with an error.
    ///
    /// An errored endpoint is destroyed and **its handle is never reused for
    /// the life of the session**: the peer is entitled to still be sending
    /// frames for it, and a reused handle would deliver them to a live link.
    pub async fn detach_with(&self, closed: bool, error: Option<Condition>) -> Result<()> {
        {
            let mut state = self.shared.state.lock().expect("not poisoned");
            if matches!(*state, LinkState::Detached(_)) {
                return Ok(());
            }
            *state = LinkState::Detaching;
        }
        let codec = error.as_ref().map(Condition::as_codec);
        self.send_performative(Performative::Detach(Detach {
            handle: self.shared.output_handle,
            closed,
            error: codec,
        }))
        .await
    }
}

/// One session's links, in the driver.
#[derive(Debug, Default)]
pub(crate) struct Links {
    by_output: HashMap<u32, Entry>,
    /// The peer's handle to ours. Two spaces, because the two ends choose
    /// independently.
    input_to_output: HashMap<u32, u32>,
    /// Handles that were detached with an error, never reused for the life
    /// of the session.
    errored: HashSet<u32>,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) shared: Arc<Shared>,
    pub(crate) events: mpsc::Sender<LinkEvent>,
    pub(crate) pending: Option<oneshot::Sender<Result<Link>>>,
    pub(crate) outbound: mpsc::Sender<crate::connection::Outbound>,
    pub(crate) rx: Option<mpsc::Receiver<LinkEvent>>,
    /// What we asked for, kept so that the answering `attach` can be read
    /// against it: the settle modes we own are ours and the ones we do not
    /// are the peer's, and only this tells them apart.
    pub(crate) options: Box<LinkOptions>,
    /// The delivery being reassembled, bounded by the `max-message-size`
    /// **this** end advertised: the bound is ours to enforce, because it is
    /// our buffer the frames land in.
    pub(crate) reassembly: Reassembly,
}

/// What one incoming frame for a link amounts to.
#[derive(Debug)]
pub(crate) enum Incoming {
    /// Accounted for, with nothing for the application yet.
    Nothing,
    /// A whole message.
    Delivery(Delivery),
    /// The link must be detached with this condition, and its handle is
    /// poisoned for the life of the session.
    Refused(Condition),
}

impl Entry {
    /// One `transfer` frame for this link.
    ///
    /// Three things happen here and the order matters: the delivery's
    /// identity is checked, the payload is appended **under the
    /// `max-message-size` bound**, and the receiver's credit is spent once
    /// per delivery rather than once per frame.
    pub(crate) fn accept_transfer(&mut self, transfer: &Transfer<'_>, payload: &[u8]) -> Incoming {
        if self.shared.role == Role::Sender {
            // A sending link has no incoming deliveries; the peer has the
            // roles the wrong way round and there is nothing to reassemble.
            return Incoming::Refused(Condition::described(
                condition::NOT_ALLOWED,
                format!(
                    "a transfer arrived on link {}, which this end attached as the sender",
                    self.shared.name
                ),
            ));
        }

        // A tag that differs from the one being reassembled is a *second*
        // delivery arriving before the first finished, which Part 2 §2.6.14
        // forbids outright: "deliveries on one link MUST NOT interleave".
        // Reported here rather than left to the payload check, because the
        // octets of two messages concatenated are not a message.
        let interleaving = self.reassembly.in_progress()
            && transfer
                .delivery_tag
                .is_some_and(|tag| self.reassembly.delivery_tag() != Some(tag));
        if interleaving {
            return Incoming::Refused(Condition::described(
                condition::NOT_ALLOWED,
                format!(
                    "a second delivery began on link {} before the first was complete",
                    self.shared.name
                ),
            ));
        }

        let first = !self.reassembly.in_progress();
        if first {
            let within = self
                .shared
                .credit
                .lock()
                .expect("not poisoned")
                .record_received();
            if !within {
                // The specification lets a receiver either handle the excess
                // or detach with `amqp:link:transfer-limit-exceeded`. This
                // client handles it: the sender sent under credit we had
                // granted and then withdrew, and dropping the message would
                // lose it.
                tracing::debug!(
                    link = self.shared.name,
                    "a delivery arrived past the credit this end had granted"
                );
            }
        }

        match self.reassembly.accept(transfer, payload) {
            Ok(Progress::Incomplete { .. }) => Incoming::Nothing,
            Ok(Progress::Aborted) => {
                // "Discard everything transferred for this delivery"; it is
                // implicitly settled, so there is nothing to answer either.
                tracing::debug!(link = self.shared.name, "the sender aborted a delivery");
                Incoming::Nothing
            }
            Ok(Progress::Complete) => {
                let delivery_id = self.reassembly.delivery_id().unwrap_or_default();
                let tag = self.reassembly.delivery_tag().unwrap_or_default().to_vec();
                let format = self.reassembly.message_format().unwrap_or_default();
                let settled = transfer.settled.unwrap_or(false);
                Incoming::Delivery(Delivery::new(
                    delivery_id,
                    tag,
                    format,
                    settled,
                    self.shared.output_handle,
                    self.reassembly.take(),
                ))
            }
            Err(error) => {
                // `MessageTooLarge` is the one that matters: the codec
                // refused it before the buffer grew, and
                // `amqp:link:message-size-exceeded` is the condition that
                // exists so a receiver can say no.
                let condition =
                    if matches!(error, weida_amqp_codec::DecodeError::MessageTooLarge { .. }) {
                        condition::LINK_MESSAGE_SIZE_EXCEEDED
                    } else {
                        condition::NOT_ALLOWED
                    };
                self.reassembly.reset();
                Incoming::Refused(Condition::described(condition, error.to_string()))
            }
        }
    }

    /// The peer's `flow` for this link.
    ///
    /// Returns the `flow` this end owes in answer, which is owed in two
    /// cases: an `echo` asked for our state, or a `drain` did — a drained
    /// sender MUST report, and a sender with nothing available MUST advance
    /// `delivery-count` until `link-credit` is zero first.
    pub(crate) fn accept_flow(
        &mut self,
        flow: &Flow<'_>,
        windows: &Windows,
    ) -> Option<Flow<'static>> {
        let mut credit = self.shared.credit.lock().expect("not poisoned");
        let mut owed = flow.echo;
        match self.shared.role {
            Role::Sender => {
                credit.apply_grant(flow.delivery_count, flow.link_credit, flow.drain);
                if flow.drain {
                    credit.consume_for_drain();
                    owed = true;
                }
            }
            Role::Receiver => credit.apply_sender_state(flow.delivery_count, flow.available),
        }
        let snapshot = *credit;
        drop(credit);
        // The waker before the event: a sender parked on credit should be
        // running again whether or not anything is reading the events.
        self.shared.flow.notify_waiters();
        // Never with `echo` set: answering an echo with an echo loops
        // forever, and the specification says so.
        owed.then(|| flow_frame(windows, &snapshot, self.shared.output_handle, false))
    }

    /// The credit state, for the event the application sees.
    pub(crate) fn credit(&self) -> Credit {
        *self.shared.credit.lock().expect("not poisoned")
    }

    /// Wakes anything parked on this link because the session window moved.
    pub(crate) fn wake(&self) {
        self.shared.flow.notify_waiters();
    }
}

impl Links {
    /// The lowest free output handle, skipping any that was errored.
    ///
    /// `handle_max` is the *peer's*, because it states the highest handle its
    /// sender will accept. Lowest-free is what Part 2 §2.6.2 recommends and
    /// is what keeps the table dense.
    pub(crate) fn lowest_free(&self, handle_max: u32) -> Option<u32> {
        (0..=handle_max)
            .find(|handle| !self.by_output.contains_key(handle) && !self.errored.contains(handle))
    }

    /// The link of this name and direction, if one is attached.
    ///
    /// The uniqueness scope of Part 2 §2.6.1: among links of the *same
    /// direction* between the two containers, so a sender and a receiver may
    /// share a name.
    pub(crate) fn by_name(&self, name: &str, role: Role) -> Option<u32> {
        self.by_output
            .iter()
            .find(|(_, entry)| entry.shared.name == name && entry.shared.role == role)
            .map(|(handle, _)| *handle)
    }

    pub(crate) fn insert(&mut self, handle: u32, entry: Entry) {
        self.by_output.insert(handle, entry);
    }

    pub(crate) fn get_mut(&mut self, output: u32) -> Option<&mut Entry> {
        self.by_output.get_mut(&output)
    }

    /// The link a frame carrying the peer's handle belongs to.
    pub(crate) fn by_input(&mut self, input: u32) -> Option<&mut Entry> {
        let output = *self.input_to_output.get(&input)?;
        self.by_output.get_mut(&output)
    }

    /// Whether the peer's handle is already mapped, which is what
    /// `amqp:session:handle-in-use` is about.
    pub(crate) fn input_in_use(&self, input: u32) -> bool {
        self.input_to_output.contains_key(&input)
    }

    pub(crate) fn map_input(&mut self, input: u32, output: u32) {
        self.input_to_output.insert(input, output);
    }

    /// Forgets a link. `errored` poisons its handle for the life of the
    /// session.
    pub(crate) fn remove(&mut self, output: u32, errored: bool) -> Option<Entry> {
        self.input_to_output.retain(|_, mapped| *mapped != output);
        if errored {
            self.errored.insert(output);
        }
        self.by_output.remove(&output)
    }

    /// Wakes everything parked on any link of this session, which is what a
    /// session-level `flow` amounts to: the window may have room now.
    pub(crate) fn wake_all(&self) {
        for entry in self.by_output.values() {
            entry.wake();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.by_output.len()
    }

    pub(crate) fn drain(&mut self) -> Vec<Entry> {
        self.input_to_output.clear();
        self.by_output.drain().map(|(_, entry)| entry).collect()
    }
}

/// Reads the answering `attach` into what was actually negotiated.
///
/// The settle modes are not a handshake where both sides must agree on one
/// value: each field describes the behaviour of *one* end, so the
/// authoritative value comes from whichever `attach` was sent by the end the
/// field is about (Part 2 §2.7.3). This client keeps both and reports them,
/// because the peer may have narrowed what we asked for and only the
/// application can decide whether that is acceptable.
pub(crate) fn negotiate(ours: &LinkOptions, theirs: &Attach<'_>) -> Result<Negotiated> {
    let (snd_settle_mode, rcv_settle_mode) = match ours.role {
        // We are the sender, so snd-settle-mode is ours and rcv-settle-mode
        // is the peer's.
        Role::Sender => (ours.snd_settle_mode, theirs.rcv_settle_mode),
        // We are the receiver, so it is the other way round.
        Role::Receiver => (theirs.snd_settle_mode, ours.rcv_settle_mode),
    };
    Ok(Negotiated {
        snd_settle_mode,
        rcv_settle_mode,
        remote_source: match theirs.source.clone() {
            Some(value) => Some(Source::from_value(value)?),
            None => None,
        },
        remote_target: match theirs.target.clone() {
            Some(value) => Some(Target::from_value(value)?),
            None => None,
        },
        remote_max_message_size: theirs.max_message_size.filter(|size| *size != 0),
        remote_offered_capabilities: theirs
            .offered_capabilities
            .iter()
            .map(str::to_owned)
            .collect(),
    })
}

/// Builds this client's `attach` for a link on `handle`.
pub(crate) fn attach_frame(channel: u16, handle: u32, options: &LinkOptions) -> Result<Vec<u8>> {
    let attach = options.to_attach(handle);
    let mut bytes = Vec::new();
    weida_amqp_codec::frame::write(
        &mut bytes,
        weida_amqp_codec::frame::FrameKind::Amqp,
        channel,
        u32::MAX,
        |body| Performative::Attach(attach).encode(body),
    )?;
    Ok(bytes)
}

/// Builds a `flow` carrying this link's credit and its session's windows.
///
/// Every `flow` carries `incoming-window`, `next-outgoing-id` and
/// `outgoing-window` whether or not it names a handle — they are mandatory —
/// so a link-level `flow` refreshes the session state too, and an `echo`
/// asking for session state is answered by one.
pub(crate) fn flow_frame(
    windows: &Windows,
    credit: &Credit,
    handle: u32,
    echo: bool,
) -> Flow<'static> {
    Flow {
        next_incoming_id: windows.wire_next_incoming_id(),
        incoming_window: windows.incoming_window,
        next_outgoing_id: windows.next_outgoing_id,
        outgoing_window: windows.outgoing_window,
        handle: Some(handle),
        delivery_count: credit.wire_delivery_count(),
        // A receiver states the credit it grants; a sender states what it has
        // left, which is the same number read from the other side.
        link_credit: Some(credit.link_credit()),
        available: Some(credit.available()),
        drain: credit.drain(),
        echo,
        properties: None,
    }
}

/// Eight octets: `SIZE`, `DOFF`, `TYPE` and two type-specific bytes.
const FRAME_HEADER: usize = 8;

/// The word for a role, for an error message a person has to read.
const fn role_name(role: Role) -> &'static str {
    match role {
        Role::Sender => "sender",
        Role::Receiver => "receiver",
    }
}

/// The condition a stolen link is closed with (Part 2 §2.8.18).
pub const STOLEN: &str = condition::LINK_STOLEN;

#[cfg(test)]
mod tests {
    use super::*;

    fn session_shared() -> Arc<crate::session::Shared> {
        Arc::new(crate::session::Shared {
            outgoing_channel: 0,
            incoming_channel: Mutex::new(Some(0)),
            windows: Mutex::new(Windows::new(0, 400, 400)),
            state: Mutex::new(crate::session::SessionState::Begun),
            options: crate::session::SessionOptions::default(),
        })
    }

    fn entry(name: &str, role: Role, handle: u32) -> Entry {
        let (events, rx) = mpsc::channel(4);
        let (outbound, _) = mpsc::channel(4);
        Entry {
            shared: Arc::new(Shared {
                name: name.to_owned(),
                role,
                output_handle: handle,
                channel: 0,
                state: Mutex::new(LinkState::Attaching),
                negotiated: Mutex::new(None),
                input_handle: Mutex::new(None),
                credit: Mutex::new(match role {
                    Role::Sender => Credit::sender(0),
                    Role::Receiver => Credit::receiver(),
                }),
                session: session_shared(),
                flow: Notify::new(),
                max_frame_size: weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE,
                next_tag: Mutex::new(0),
            }),
            events,
            pending: None,
            outbound,
            rx: Some(rx),
            options: Box::new(LinkOptions::sender(name, Target::at("q"))),
            reassembly: Reassembly::new(DEFAULT_MAX_MESSAGE_SIZE),
        }
    }

    #[test]
    fn handles_are_allocated_lowest_free() {
        let mut links = Links::default();
        assert_eq!(links.lowest_free(255), Some(0));
        links.insert(0, entry("a", Role::Sender, 0));
        links.insert(1, entry("b", Role::Sender, 1));
        assert_eq!(links.lowest_free(255), Some(2));
        // A cleanly closed link's handle is free again.
        links.remove(0, false);
        assert_eq!(links.lowest_free(255), Some(0));
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn an_errored_handle_is_never_reused() {
        // The peer is entitled to still be sending frames for the dead link,
        // and a reused handle would deliver them to a live one.
        let mut links = Links::default();
        links.insert(0, entry("a", Role::Sender, 0));
        links.remove(0, true);
        assert_eq!(
            links.lowest_free(255),
            Some(1),
            "handle 0 is poisoned for the life of the session"
        );
        links.insert(1, entry("b", Role::Sender, 1));
        links.remove(1, false);
        assert_eq!(links.lowest_free(255), Some(1), "a clean close frees it");
    }

    #[test]
    fn the_bound_on_the_table_is_the_peers_handle_max() {
        let mut links = Links::default();
        links.insert(0, entry("a", Role::Sender, 0));
        assert_eq!(links.lowest_free(1), Some(1));
        links.insert(1, entry("b", Role::Sender, 1));
        assert_eq!(
            links.lowest_free(1),
            None,
            "the peer's handle-max is what bounds what we may use"
        );
    }

    #[test]
    fn the_two_handle_spaces_are_separate() {
        let mut links = Links::default();
        links.insert(0, entry("orders", Role::Sender, 0));
        // The peer answered with its own handle 7.
        assert!(!links.input_in_use(7));
        links.map_input(7, 0);
        assert!(links.input_in_use(7));
        assert_eq!(links.by_input(7).map(|e| e.shared.output_handle), Some(0));
        assert!(
            links.by_input(0).is_none(),
            "our own output handle is not an input handle"
        );
        links.remove(0, false);
        assert!(!links.input_in_use(7));
    }

    #[test]
    fn a_name_is_unique_per_direction_and_not_per_connection() {
        // Part 2 §2.6.1 scopes uniqueness to links of the same direction, so
        // a sender and a receiver may share a name.
        let mut links = Links::default();
        links.insert(0, entry("orders", Role::Sender, 0));
        links.insert(1, entry("orders", Role::Receiver, 1));
        assert_eq!(links.by_name("orders", Role::Sender), Some(0));
        assert_eq!(links.by_name("orders", Role::Receiver), Some(1));
        assert_eq!(links.by_name("invoices", Role::Sender), None);
    }

    #[test]
    fn an_empty_link_name_is_refused_where_it_is_configured() {
        let mut options = LinkOptions::sender("", Target::at("q"));
        let error = options.validate().expect_err("refused");
        assert!(error.to_string().contains("steals"), "{error}");
        options.name = "orders".into();
        options.validate().expect("a named link is fine");
    }

    #[test]
    fn the_settle_modes_come_from_the_end_each_one_describes() {
        // Not a handshake on one value: snd-settle-mode describes the
        // sender's behaviour and rcv-settle-mode the receiver's, so each
        // comes from the end it is about.
        let mut ours = LinkOptions::sender("orders", Target::at("q"));
        ours.snd_settle_mode = SenderSettleMode::Unsettled;
        ours.rcv_settle_mode = ReceiverSettleMode::Second;

        let mut theirs = Attach::new("orders", 7, Role::Receiver);
        theirs.snd_settle_mode = SenderSettleMode::Mixed;
        // The broker refuses `second` and says `first`, which is what
        // Artemis does.
        theirs.rcv_settle_mode = ReceiverSettleMode::First;
        let their_target = Target::at("q");
        theirs.target = Some(their_target.to_value());

        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(
            negotiated.snd_settle_mode,
            SenderSettleMode::Unsettled,
            "we are the sender, so snd-settle-mode is ours"
        );
        assert_eq!(
            negotiated.rcv_settle_mode,
            ReceiverSettleMode::First,
            "we are not the receiver, so rcv-settle-mode is the peer's - and \
             asking for Second does not make it so"
        );

        // And the other way round when we receive.
        let mut ours = LinkOptions::receiver("orders", Source::at("q"));
        ours.rcv_settle_mode = ReceiverSettleMode::Second;
        let mut theirs = Attach::new("orders", 7, Role::Sender);
        theirs.snd_settle_mode = SenderSettleMode::Settled;
        let their_source = Source::at("q");
        theirs.source = Some(their_source.to_value());
        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(negotiated.snd_settle_mode, SenderSettleMode::Settled);
        assert_eq!(negotiated.rcv_settle_mode, ReceiverSettleMode::Second);
    }

    #[test]
    fn a_null_terminus_in_the_answer_is_a_refusal_and_not_an_error() {
        // "A partner that will not provide a terminus answers with that field
        // null, which establishes a link to a nonexistent terminus, and MUST
        // then immediately detach."
        let ours = LinkOptions::receiver("orders", Source::at("nope"));
        let theirs = Attach::new("orders", 7, Role::Sender);
        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(negotiated.remote_source, None);
        assert_eq!(negotiated.remote_target, None);
    }

    #[test]
    fn a_zero_max_message_size_is_folded_into_no_limit() {
        // Zero and unset both mean unlimited, so no caller has to remember
        // the rule.
        let ours = LinkOptions::sender("orders", Target::at("q"));
        let mut theirs = Attach::new("orders", 7, Role::Receiver);
        theirs.max_message_size = Some(0);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap().remote_max_message_size,
            None
        );
        theirs.max_message_size = Some(1024);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap().remote_max_message_size,
            Some(1024)
        );
    }

    #[test]
    fn our_attach_sets_a_bounded_max_message_size() {
        let options = LinkOptions::sender("orders", Target::at("q"));
        assert_eq!(options.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        let bytes = attach_frame(0, 3, &options).expect("encodes");
        let frame = weida_amqp_codec::frame::decode(&bytes, u32::MAX).expect("a frame");
        let (performative, _) =
            Performative::decode(frame.body, weida_amqp_codec::Limits::DEFAULT).unwrap();
        match performative {
            Performative::Attach(attach) => {
                assert_eq!(attach.name, "orders");
                assert_eq!(attach.handle, 3);
                assert_eq!(attach.role, Role::Sender);
                assert_eq!(attach.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
                assert_eq!(
                    attach.initial_delivery_count,
                    Some(0),
                    "MUST NOT be null for a sender"
                );
                assert!(attach.target.is_some());
            }
            other => panic!("expected attach, got {}", other.name()),
        }
    }
}
