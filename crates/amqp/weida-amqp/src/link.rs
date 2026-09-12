//! The link: `attach`, `detach`, and the two handle spaces.
//!
//! A link is "a unidirectional named route between two nodes, attached at
//! each end to a terminus" (Part 2 §2.6). Three of its rules are easy to get
//! subtly wrong, and each one has a test here.
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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};
use weida_amqp_codec::performative::{Attach, Detach, Performative};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{ReceiverSettleMode, Role, SenderSettleMode};

use crate::error::{Condition, Error, Result};
use crate::options::multiple;
use crate::terminus::{Source, Target};

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
    /// A frame for this link, whole and undecoded — the transfers and
    /// dispositions of B-160 and B-161.
    Frame(Vec<u8>),
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
        self.send(Performative::Detach(Detach {
            handle: self.shared.output_handle,
            closed,
            error: codec,
        }))
        .await
    }

    /// Sends one performative on this link's session channel.
    pub(crate) async fn send(&self, performative: Performative<'_>) -> Result<()> {
        let mut bytes = Vec::new();
        weida_amqp_codec::frame::write(
            &mut bytes,
            weida_amqp_codec::frame::FrameKind::Amqp,
            self.shared.channel,
            u32::MAX,
            |body| performative.encode(body),
        )?;
        self.outbound
            .send(crate::connection::Outbound::Frame(bytes))
            .await
            .map_err(|_| Error::ConnectionGone)
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

/// The condition a stolen link is closed with (Part 2 §2.8.18).
pub const STOLEN: &str = condition::LINK_STOLEN;

#[cfg(test)]
mod tests {
    use super::*;

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
            }),
            events,
            pending: None,
            outbound,
            rx: Some(rx),
            options: Box::new(LinkOptions::sender(name, Target::at("q"))),
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
