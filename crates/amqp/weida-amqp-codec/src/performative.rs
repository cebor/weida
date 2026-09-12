//! The nine performatives, `open` `0x10` through `close` `0x18`.
//!
//! An AMQP frame body is "one performative encoded as a described type,
//! optionally followed by an opaque payload" (Part 2 §2.3.2), and the nine
//! are the whole vocabulary of the transport: there is no tenth and no
//! extension point. Each one is a composite type — a described `list` read by
//! position — so each struct here is that list with the specification's field
//! names and the specification's defaults.
//!
//! ```text
//! open  0x10   begin 0x11   attach      0x12   flow   0x13   transfer 0x14
//! disposition  0x15   detach 0x16   end  0x17   close 0x18
//! ```
//!
//! # What is *not* typed here, on purpose
//!
//! `attach.source`, `attach.target`, `transfer.state` and
//! `disposition.state` keep their values as [`Value`]. That is not a gap: the
//! specification types them `*` — "any type which provides source", "any type
//! which provides delivery-state" — and puts the definitions in Part 3, which
//! the transport deliberately does not depend on ("the transport layer
//! assumes as little as possible about messages", Part 2 §2.6.11). Part 3's
//! own types convert to and from a `Value`, so the layering is the
//! specification's rather than an unfinished edge.
//!
//! # Defaults are omitted, not written
//!
//! A field equal to the default the specification gives it encodes as `null`
//! and is then trimmed with the other trailing nulls, so an `open` carrying
//! only a `container-id` is a ten-octet body. The two are semantically
//! identical — Part 1 §1.4 — and the shorter one is what every
//! implementation sends.

use crate::decode;
use crate::encode;
use crate::error::{DecodeError, EncodeError};
use crate::fields::Fields;
use crate::frame::DEFAULT_MAX_FRAME_SIZE;
use crate::limits::Limits;
use crate::types::{
    AmqpError, MAX_DELIVERY_TAG_BYTES, Multiple, ReceiverSettleMode, Role, SenderSettleMode,
    check_tag,
};
use crate::value::{Descriptor, Value};

/// `open.channel-max`'s default: 65535, so 65536 simultaneous sessions
/// (Part 2 §2.7.1).
pub const DEFAULT_CHANNEL_MAX: u16 = 65535;

/// `begin.handle-max`'s default: the highest usable handle, so 2^32 links per
/// session (Part 2 §2.7.2).
pub const DEFAULT_HANDLE_MAX: u32 = 4_294_967_295;

/// `transfer.message-format`'s only defined value: Part 3 defines exactly one
/// message format and numbers it zero (Part 3 §3.2.16).
pub const MESSAGE_FORMAT: u32 = 0;

/// The nine performatives, numeric descriptor and symbolic descriptor
/// together, in the order Part 2 §2.7 defines them.
pub const DESCRIPTORS: [(u64, &str); 9] = [
    (Open::DESCRIPTOR, Open::SYMBOLIC),
    (Begin::DESCRIPTOR, Begin::SYMBOLIC),
    (Attach::DESCRIPTOR, Attach::SYMBOLIC),
    (Flow::DESCRIPTOR, Flow::SYMBOLIC),
    (Transfer::DESCRIPTOR, Transfer::SYMBOLIC),
    (Disposition::DESCRIPTOR, Disposition::SYMBOLIC),
    (Detach::DESCRIPTOR, Detach::SYMBOLIC),
    (End::DESCRIPTOR, End::SYMBOLIC),
    (Close::DESCRIPTOR, Close::SYMBOLIC),
];

/// `open`: the first frame either peer sends, on channel 0
/// (Part 2 §2.7.1).
#[derive(Clone, Debug, PartialEq)]
pub struct Open<'a> {
    /// Mandatory. The container's name, which MUST be stable across
    /// reconnects because link recovery is keyed on it.
    pub container_id: &'a str,
    /// The virtual host the sender wants. RabbitMQ selects a non-default
    /// virtual host through this field and not through the address.
    pub hostname: Option<&'a str>,
    /// The largest frame the *sender of this open* will accept. The default
    /// is no limit at all.
    pub max_frame_size: u32,
    /// The highest channel number the sender will accept, so one less than
    /// the number of simultaneous sessions.
    pub channel_max: u16,
    /// Milliseconds: the longest the sender wants to go without a frame from
    /// its partner. Zero equals unset, and unset means no timeout — though
    /// the specification warns an implementation MAY apply an internal
    /// default anyway.
    pub idle_time_out: Option<u32>,
    /// Locales the sender offers for its own outgoing text.
    pub outgoing_locales: Multiple<'a>,
    /// Locales the sender will accept.
    pub incoming_locales: Multiple<'a>,
    /// Extension capabilities the sender supports.
    pub offered_capabilities: Multiple<'a>,
    /// Extension capabilities the sender wants. A peer MUST NOT use one it
    /// did not list here.
    pub desired_capabilities: Multiple<'a>,
    /// A `fields` map of connection properties.
    pub properties: Option<Value<'a>>,
}

impl<'a> Open<'a> {
    /// `0x10`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0010;
    /// `amqp:open:list`.
    pub const SYMBOLIC: &'static str = "amqp:open:list";

    /// An `open` with only its mandatory field set and every other field at
    /// the specification's default.
    #[must_use]
    pub const fn new(container_id: &'a str) -> Self {
        Self {
            container_id,
            hostname: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_time_out: None,
            outgoing_locales: Multiple::None,
            incoming_locales: Multiple::None,
            offered_capabilities: Multiple::None,
            desired_capabilities: Multiple::None,
            properties: None,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            container_id: fields.required_string("container-id")?,
            hostname: fields.string("hostname")?,
            max_frame_size: fields.uint_or("max-frame-size", DEFAULT_MAX_FRAME_SIZE)?,
            channel_max: fields.ushort_or("channel-max", DEFAULT_CHANNEL_MAX)?,
            idle_time_out: fields.uint("idle-time-out")?,
            outgoing_locales: fields.multiple("outgoing-locales")?,
            incoming_locales: fields.multiple("incoming-locales")?,
            offered_capabilities: fields.multiple("offered-capabilities")?,
            desired_capabilities: fields.multiple("desired-capabilities")?,
            properties: fields.map("properties")?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                Value::String(self.container_id),
                opt_string(self.hostname),
                uint_unless(self.max_frame_size, DEFAULT_MAX_FRAME_SIZE),
                ushort_unless(self.channel_max, DEFAULT_CHANNEL_MAX),
                opt_uint(self.idle_time_out),
                self.outgoing_locales.to_value(),
                self.incoming_locales.to_value(),
                self.offered_capabilities.to_value(),
                self.desired_capabilities.to_value(),
                opt_value(&self.properties),
            ],
            out,
        )
    }
}

/// `begin`: a session on a channel (Part 2 §2.7.2).
#[derive(Clone, Debug, PartialEq)]
pub struct Begin<'a> {
    /// Unset on the `begin` that starts a session; on the answer, the
    /// *incoming* channel the responder saw. The two directions are numbered
    /// independently, so the two halves usually carry different numbers.
    pub remote_channel: Option<u16>,
    /// Mandatory. The transfer-id of the first frame this endpoint will send.
    pub next_outgoing_id: u32,
    /// Mandatory. How many `transfer` frames the sender can currently
    /// receive. One of the three bounds in the protocol that is safe by
    /// construction, because the field has no default.
    pub incoming_window: u32,
    /// Mandatory. How many `transfer` frames the sender is willing to send.
    pub outgoing_window: u32,
    /// The highest link handle the sender will accept.
    pub handle_max: u32,
    /// Extension capabilities the sender supports.
    pub offered_capabilities: Multiple<'a>,
    /// Extension capabilities the sender wants.
    pub desired_capabilities: Multiple<'a>,
    /// A `fields` map of session properties.
    pub properties: Option<Value<'a>>,
}

impl<'a> Begin<'a> {
    /// `0x11`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0011;
    /// `amqp:begin:list`.
    pub const SYMBOLIC: &'static str = "amqp:begin:list";

    /// A `begin` with the three mandatory windows set and everything else at
    /// its default.
    #[must_use]
    pub const fn new(next_outgoing_id: u32, incoming_window: u32, outgoing_window: u32) -> Self {
        Self {
            remote_channel: None,
            next_outgoing_id,
            incoming_window,
            outgoing_window,
            handle_max: DEFAULT_HANDLE_MAX,
            offered_capabilities: Multiple::None,
            desired_capabilities: Multiple::None,
            properties: None,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            remote_channel: fields.ushort("remote-channel")?,
            next_outgoing_id: fields.required_uint("next-outgoing-id")?,
            incoming_window: fields.required_uint("incoming-window")?,
            outgoing_window: fields.required_uint("outgoing-window")?,
            handle_max: fields.uint_or("handle-max", DEFAULT_HANDLE_MAX)?,
            offered_capabilities: fields.multiple("offered-capabilities")?,
            desired_capabilities: fields.multiple("desired-capabilities")?,
            properties: fields.map("properties")?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                opt_ushort(self.remote_channel),
                Value::Uint(self.next_outgoing_id),
                Value::Uint(self.incoming_window),
                Value::Uint(self.outgoing_window),
                uint_unless(self.handle_max, DEFAULT_HANDLE_MAX),
                self.offered_capabilities.to_value(),
                self.desired_capabilities.to_value(),
                opt_value(&self.properties),
            ],
            out,
        )
    }
}

/// `attach`: a link between two termini (Part 2 §2.7.3).
#[derive(Clone, Debug, PartialEq)]
pub struct Attach<'a> {
    /// Mandatory. Unique among the links of the same direction between the
    /// two containers — which is what makes a second attach elsewhere a
    /// *steal* rather than a second link.
    pub name: &'a str,
    /// Mandatory. The sender's alias for this link endpoint.
    pub handle: u32,
    /// Mandatory. `false` is the sender, `true` the receiver.
    pub role: Role,
    /// How the sender will settle.
    pub snd_settle_mode: SenderSettleMode,
    /// When the receiver will settle.
    pub rcv_settle_mode: ReceiverSettleMode,
    /// The source terminus, as a value providing `source` (Part 3 §3.5.3). A
    /// partner that will not provide one answers with this field null and
    /// MUST then immediately detach.
    pub source: Option<Value<'a>>,
    /// The target terminus, as a value providing `target` (Part 3 §3.5.4).
    pub target: Option<Value<'a>>,
    /// A map from delivery-tag to delivery state for everything this endpoint
    /// still considers unsettled. Non-null is what distinguishes a *resuming*
    /// attach from a re-attach.
    pub unsettled: Option<Value<'a>>,
    /// True when `unsettled` did not fit in one frame. While it is true,
    /// absence of a tag is not evidence of settlement.
    pub incomplete_unsettled: bool,
    /// MUST NOT be null for a sender and is ignored for a receiver: the point
    /// on the `delivery-count` sequence the sender starts from.
    pub initial_delivery_count: Option<u32>,
    /// Zero or unset means no limit.
    pub max_message_size: Option<u64>,
    /// Extension capabilities the sender supports.
    pub offered_capabilities: Multiple<'a>,
    /// Extension capabilities the sender wants.
    pub desired_capabilities: Multiple<'a>,
    /// A `fields` map of link properties.
    pub properties: Option<Value<'a>>,
}

impl<'a> Attach<'a> {
    /// `0x12`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0012;
    /// `amqp:attach:list`.
    pub const SYMBOLIC: &'static str = "amqp:attach:list";

    /// An `attach` with its three mandatory fields set and everything else at
    /// its default.
    #[must_use]
    pub const fn new(name: &'a str, handle: u32, role: Role) -> Self {
        Self {
            name,
            handle,
            role,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: Multiple::None,
            desired_capabilities: Multiple::None,
            properties: None,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        let name = fields.required_string("name")?;
        let handle = fields.required_uint("handle")?;
        let role = Role::from_bool(fields.boolean("role")?.ok_or(
            DecodeError::MissingMandatoryField {
                composite: "attach",
                field: "role",
            },
        )?);
        let snd_settle_mode = match fields.ubyte("snd-settle-mode")? {
            Some(octet) => SenderSettleMode::from_octet(octet)?,
            None => SenderSettleMode::Mixed,
        };
        let rcv_settle_mode = match fields.ubyte("rcv-settle-mode")? {
            Some(octet) => ReceiverSettleMode::from_octet(octet)?,
            None => ReceiverSettleMode::First,
        };
        Ok(Self {
            name,
            handle,
            role,
            snd_settle_mode,
            rcv_settle_mode,
            source: fields.any()?,
            target: fields.any()?,
            unsettled: fields.map("unsettled")?,
            incomplete_unsettled: fields.boolean_or("incomplete-unsettled", false)?,
            initial_delivery_count: fields.uint("initial-delivery-count")?,
            max_message_size: fields.ulong("max-message-size")?,
            offered_capabilities: fields.multiple("offered-capabilities")?,
            desired_capabilities: fields.multiple("desired-capabilities")?,
            properties: fields.map("properties")?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                Value::String(self.name),
                Value::Uint(self.handle),
                Value::Boolean(self.role.is_receiver()),
                ubyte_unless(
                    self.snd_settle_mode.octet(),
                    SenderSettleMode::Mixed.octet(),
                ),
                ubyte_unless(
                    self.rcv_settle_mode.octet(),
                    ReceiverSettleMode::First.octet(),
                ),
                opt_value(&self.source),
                opt_value(&self.target),
                opt_value(&self.unsettled),
                flag(self.incomplete_unsettled),
                opt_uint(self.initial_delivery_count),
                opt_ulong(self.max_message_size),
                self.offered_capabilities.to_value(),
                self.desired_capabilities.to_value(),
                opt_value(&self.properties),
            ],
            out,
        )
    }
}

/// `flow`: session window and link credit, in one frame (Part 2 §2.7.4).
///
/// Every `flow` carries session state — the three mandatory window fields —
/// and *may* carry link state on top. That is why a link-level `flow` also
/// refreshes the session window, and why `flow(echo=true)` with no handle is
/// answerable by any `flow` at all.
#[derive(Clone, Debug, PartialEq)]
pub struct Flow<'a> {
    /// The transfer-id the sender expects next. MUST NOT be set before the
    /// partner's `begin` has been seen.
    pub next_incoming_id: Option<u32>,
    /// Mandatory. How many `transfer` frames the sender can now receive.
    pub incoming_window: u32,
    /// Mandatory. The transfer-id of the sender's next outgoing frame.
    pub next_outgoing_id: u32,
    /// Mandatory. How many the sender is willing to send.
    pub outgoing_window: u32,
    /// Set to carry link state as well as session state.
    pub handle: Option<u32>,
    /// From a sender, its current `delivery-count`. From a receiver, the last
    /// value it knows of the sender's — and MUST NOT be set at all if the
    /// receiver has not yet seen the sender's `attach`.
    pub delivery_count: Option<u32>,
    /// The credit the receiver grants. Communicated as part of an absolute
    /// delivery-limit (`delivery-count + link-credit`) rather than as an
    /// increment, which is what makes a `flow` idempotent.
    pub link_credit: Option<u32>,
    /// The sender's backlog: how much credit it could use.
    pub available: Option<u32>,
    /// Set by the receiver: consume all remaining credit and report, so that
    /// "wait for a message" becomes "wait for a definite answer".
    pub drain: bool,
    /// Ask the partner for its state at the earliest convenient opportunity.
    /// Answering an echo with an echo loops forever and SHOULD be avoided.
    pub echo: bool,
    /// A `fields` map. Part 4 puts a `txn-id` here for transactional
    /// acquisition; RabbitMQ reports `rabbitmq:active` here for a quorum
    /// queue's single active consumer.
    pub properties: Option<Value<'a>>,
}

impl<'a> Flow<'a> {
    /// `0x13`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0013;
    /// `amqp:flow:list`.
    pub const SYMBOLIC: &'static str = "amqp:flow:list";

    /// A `flow` carrying session state only.
    #[must_use]
    pub const fn session(
        incoming_window: u32,
        next_outgoing_id: u32,
        outgoing_window: u32,
    ) -> Self {
        Self {
            next_incoming_id: None,
            incoming_window,
            next_outgoing_id,
            outgoing_window,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            next_incoming_id: fields.uint("next-incoming-id")?,
            incoming_window: fields.required_uint("incoming-window")?,
            next_outgoing_id: fields.required_uint("next-outgoing-id")?,
            outgoing_window: fields.required_uint("outgoing-window")?,
            handle: fields.uint("handle")?,
            delivery_count: fields.uint("delivery-count")?,
            link_credit: fields.uint("link-credit")?,
            available: fields.uint("available")?,
            drain: fields.boolean_or("drain", false)?,
            echo: fields.boolean_or("echo", false)?,
            properties: fields.map("properties")?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                opt_uint(self.next_incoming_id),
                Value::Uint(self.incoming_window),
                Value::Uint(self.next_outgoing_id),
                Value::Uint(self.outgoing_window),
                opt_uint(self.handle),
                opt_uint(self.delivery_count),
                opt_uint(self.link_credit),
                opt_uint(self.available),
                flag(self.drain),
                flag(self.echo),
                opt_value(&self.properties),
            ],
            out,
        )
    }
}

/// `transfer`: one frame of one delivery (Part 2 §2.7.5).
#[derive(Clone, Debug, PartialEq)]
pub struct Transfer<'a> {
    /// Mandatory. Which link.
    pub handle: u32,
    /// The session-scoped alias for the delivery. MAY be omitted on a
    /// continuation frame but MUST NOT differ if present.
    pub delivery_id: Option<u32>,
    /// Up to 32 octets chosen by the sending application, unique among the
    /// deliveries either end could consider unsettled on this link.
    pub delivery_tag: Option<&'a [u8]>,
    /// Part 3 defines exactly one format and numbers it zero.
    pub message_format: Option<u32>,
    /// Tri-state on purpose: unset means "whatever the link negotiated", and
    /// that is not the same as `false`.
    pub settled: Option<bool>,
    /// More frames follow for this delivery.
    pub more: bool,
    /// May only narrow what the link negotiated, never widen it.
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    /// The delivery's state, as a value providing `delivery-state`
    /// (Part 3 §3.4).
    pub state: Option<Value<'a>>,
    /// This transfer resumes a delivery from a previous link endpoint.
    pub resume: bool,
    /// Discard everything transferred for this delivery. Takes precedence
    /// over `more`, and implicitly settles the delivery.
    pub aborted: bool,
    /// A hint that the peer need not urgently communicate updated state. Not
    /// part of the transfer state and not retained across resumption.
    pub batchable: bool,
}

impl<'a> Transfer<'a> {
    /// `0x14`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0014;
    /// `amqp:transfer:list`.
    pub const SYMBOLIC: &'static str = "amqp:transfer:list";

    /// A `transfer` on `handle` with everything else at its default.
    #[must_use]
    pub const fn new(handle: u32) -> Self {
        Self {
            handle,
            delivery_id: None,
            delivery_tag: None,
            message_format: None,
            settled: None,
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        let handle = fields.required_uint("handle")?;
        let delivery_id = fields.uint("delivery-id")?;
        let delivery_tag = fields.binary("delivery-tag")?;
        if let Some(tag) = delivery_tag
            && tag.len() > MAX_DELIVERY_TAG_BYTES
        {
            return Err(DecodeError::RestrictionViolated {
                restriction: "delivery-tag",
                value: tag.len() as u64,
                limit: MAX_DELIVERY_TAG_BYTES as u64,
            });
        }
        let message_format = fields.uint("message-format")?;
        let settled = fields.boolean("settled")?;
        let more = fields.boolean_or("more", false)?;
        let rcv_settle_mode = match fields.ubyte("rcv-settle-mode")? {
            Some(octet) => Some(ReceiverSettleMode::from_octet(octet)?),
            None => None,
        };
        Ok(Self {
            handle,
            delivery_id,
            delivery_tag,
            message_format,
            settled,
            more,
            rcv_settle_mode,
            state: fields.any()?,
            resume: fields.boolean_or("resume", false)?,
            aborted: fields.boolean_or("aborted", false)?,
            batchable: fields.boolean_or("batchable", false)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        if let Some(tag) = self.delivery_tag {
            check_tag(tag, "delivery-tag", MAX_DELIVERY_TAG_BYTES)?;
        }
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                Value::Uint(self.handle),
                opt_uint(self.delivery_id),
                self.delivery_tag.map_or(Value::Null, Value::Binary),
                opt_uint(self.message_format),
                self.settled.map_or(Value::Null, Value::Boolean),
                flag(self.more),
                self.rcv_settle_mode
                    .map_or(Value::Null, |mode| Value::Ubyte(mode.octet())),
                opt_value(&self.state),
                flag(self.resume),
                flag(self.aborted),
                flag(self.batchable),
            ],
            out,
        )
    }
}

/// `disposition`: a delivery state over a range of delivery-ids
/// (Part 2 §2.7.6).
#[derive(Clone, Debug, PartialEq)]
pub struct Disposition<'a> {
    /// Mandatory. Which end of the link is speaking; one `disposition` may
    /// cover deliveries from many links of the session provided they all have
    /// this role.
    pub role: Role,
    /// Mandatory. The lowest delivery-id covered.
    pub first: u32,
    /// The highest delivery-id covered; unset means `first` alone.
    pub last: Option<u32>,
    /// Whether the sender of this frame has settled the deliveries.
    /// Settlement is idempotent and one-way.
    pub settled: bool,
    /// The state to apply, as a value providing `delivery-state`.
    pub state: Option<Value<'a>>,
    /// A batching hint, not part of the transfer state.
    pub batchable: bool,
}

impl<'a> Disposition<'a> {
    /// `0x15`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0015;
    /// `amqp:disposition:list`.
    pub const SYMBOLIC: &'static str = "amqp:disposition:list";

    /// A `disposition` covering one delivery-id.
    #[must_use]
    pub const fn new(role: Role, first: u32) -> Self {
        Self {
            role,
            first,
            last: None,
            settled: false,
            state: None,
            batchable: false,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        let role = Role::from_bool(fields.boolean("role")?.ok_or(
            DecodeError::MissingMandatoryField {
                composite: "disposition",
                field: "role",
            },
        )?);
        Ok(Self {
            role,
            first: fields.required_uint("first")?,
            last: fields.uint("last")?,
            settled: fields.boolean_or("settled", false)?,
            state: fields.any()?,
            batchable: fields.boolean_or("batchable", false)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                Value::Boolean(self.role.is_receiver()),
                Value::Uint(self.first),
                opt_uint(self.last),
                flag(self.settled),
                opt_value(&self.state),
                flag(self.batchable),
            ],
            out,
        )
    }
}

/// `detach`: unmap a handle, and optionally destroy the link endpoint
/// (Part 2 §2.7.7).
#[derive(Clone, Debug, PartialEq)]
pub struct Detach<'a> {
    /// Mandatory. Which handle to unmap.
    pub handle: u32,
    /// `true` destroys the link endpoint at both ends. `false` leaves the
    /// deliveries live, which is why a `disposition` may still refer to them.
    pub closed: bool,
    /// An errored link endpoint MUST be detached with its error, and any
    /// later input on that handle MUST then end the session with
    /// `amqp:session:errant-link`.
    pub error: Option<AmqpError<'a>>,
}

impl<'a> Detach<'a> {
    /// `0x16`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0016;
    /// `amqp:detach:list`.
    pub const SYMBOLIC: &'static str = "amqp:detach:list";

    /// A `detach` that unmaps the handle without destroying the endpoint.
    #[must_use]
    pub const fn new(handle: u32) -> Self {
        Self {
            handle,
            closed: false,
            error: None,
        }
    }

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            handle: fields.required_uint("handle")?,
            closed: fields.boolean_or("closed", false)?,
            error: read_error(&mut fields)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[
                Value::Uint(self.handle),
                flag(self.closed),
                write_error(self.error.as_ref()),
            ],
            out,
        )
    }
}

/// `end`: a session ends (Part 2 §2.7.8).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct End<'a> {
    /// With an error, the sender MUST then silently discard all incoming
    /// frames until the partner's `end` — the `DISCARDING` state.
    pub error: Option<AmqpError<'a>>,
}

impl<'a> End<'a> {
    /// `0x17`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0017;
    /// `amqp:end:list`.
    pub const SYMBOLIC: &'static str = "amqp:end:list";

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            error: read_error(&mut fields)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[write_error(self.error.as_ref())],
            out,
        )
    }
}

/// `close`: the connection ends, and this MUST be the last thing ever written
/// (Part 2 §2.7.9).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Close<'a> {
    /// Why, if there is a reason.
    pub error: Option<AmqpError<'a>>,
}

impl<'a> Close<'a> {
    /// `0x18`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0018;
    /// `amqp:close:list`.
    pub const SYMBOLIC: &'static str = "amqp:close:list";

    fn read(mut fields: Fields<'a>) -> Result<Self, DecodeError> {
        Ok(Self {
            error: read_error(&mut fields)?,
        })
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        encode::composite(
            &Descriptor::Code(Self::DESCRIPTOR),
            &[write_error(self.error.as_ref())],
            out,
        )
    }
}

/// One of the nine.
#[derive(Clone, Debug, PartialEq)]
pub enum Performative<'a> {
    /// `open`, `0x10`.
    Open(Open<'a>),
    /// `begin`, `0x11`.
    Begin(Begin<'a>),
    /// `attach`, `0x12`.
    Attach(Attach<'a>),
    /// `flow`, `0x13`.
    Flow(Flow<'a>),
    /// `transfer`, `0x14`.
    Transfer(Transfer<'a>),
    /// `disposition`, `0x15`.
    Disposition(Disposition<'a>),
    /// `detach`, `0x16`.
    Detach(Detach<'a>),
    /// `end`, `0x17`.
    End(End<'a>),
    /// `close`, `0x18`.
    Close(Close<'a>),
}

impl<'a> Performative<'a> {
    /// Decodes one performative from the front of an AMQP frame body.
    ///
    /// Returns the performative and how many octets it occupied. What follows
    /// in the frame body is the opaque payload of a `transfer`, which this
    /// function deliberately does not touch.
    ///
    /// ```
    /// use weida_amqp_codec::{Limits, performative::{Open, Performative}};
    ///
    /// let mut body = Vec::new();
    /// Performative::Open(Open::new("client-1")).encode(&mut body).expect("encodes");
    /// assert_eq!(body, [0x00, 0x53, 0x10, 0xc0, 0x0b, 0x01, 0xa1, 0x08,
    ///                   b'c', b'l', b'i', b'e', b'n', b't', b'-', b'1']);
    ///
    /// let (back, used) = Performative::decode(&body, Limits::DEFAULT).expect("decodes");
    /// assert_eq!(used, body.len());
    /// assert_eq!(back.name(), "open");
    /// ```
    pub fn decode(input: &'a [u8], limits: Limits) -> Result<(Self, usize), DecodeError> {
        let composite = decode::composite(input, limits)?;
        let used = composite.used;
        let code = resolve(&composite.descriptor).ok_or(DecodeError::UnknownComposite {
            kind: "performative",
            descriptor: composite.descriptor.code(),
        })?;
        let performative = match code {
            Open::DESCRIPTOR => Self::Open(Open::read(Fields::new(composite.fields, "open"))?),
            Begin::DESCRIPTOR => Self::Begin(Begin::read(Fields::new(composite.fields, "begin"))?),
            Attach::DESCRIPTOR => {
                Self::Attach(Attach::read(Fields::new(composite.fields, "attach"))?)
            }
            Flow::DESCRIPTOR => Self::Flow(Flow::read(Fields::new(composite.fields, "flow"))?),
            Transfer::DESCRIPTOR => {
                Self::Transfer(Transfer::read(Fields::new(composite.fields, "transfer"))?)
            }
            Disposition::DESCRIPTOR => Self::Disposition(Disposition::read(Fields::new(
                composite.fields,
                "disposition",
            ))?),
            Detach::DESCRIPTOR => {
                Self::Detach(Detach::read(Fields::new(composite.fields, "detach"))?)
            }
            End::DESCRIPTOR => Self::End(End::read(Fields::new(composite.fields, "end"))?),
            Close::DESCRIPTOR => Self::Close(Close::read(Fields::new(composite.fields, "close"))?),
            other => {
                return Err(DecodeError::UnknownComposite {
                    kind: "performative",
                    descriptor: Some(other),
                });
            }
        };
        Ok((performative, used))
    }

    /// Appends this performative's canonical encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::Open(body) => body.write(out),
            Self::Begin(body) => body.write(out),
            Self::Attach(body) => body.write(out),
            Self::Flow(body) => body.write(out),
            Self::Transfer(body) => body.write(out),
            Self::Disposition(body) => body.write(out),
            Self::Detach(body) => body.write(out),
            Self::End(body) => body.write(out),
            Self::Close(body) => body.write(out),
        }
    }

    /// The numeric descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> u64 {
        match self {
            Self::Open(_) => Open::DESCRIPTOR,
            Self::Begin(_) => Begin::DESCRIPTOR,
            Self::Attach(_) => Attach::DESCRIPTOR,
            Self::Flow(_) => Flow::DESCRIPTOR,
            Self::Transfer(_) => Transfer::DESCRIPTOR,
            Self::Disposition(_) => Disposition::DESCRIPTOR,
            Self::Detach(_) => Detach::DESCRIPTOR,
            Self::End(_) => End::DESCRIPTOR,
            Self::Close(_) => Close::DESCRIPTOR,
        }
    }

    /// The performative's name, for a log line.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Open(_) => "open",
            Self::Begin(_) => "begin",
            Self::Attach(_) => "attach",
            Self::Flow(_) => "flow",
            Self::Transfer(_) => "transfer",
            Self::Disposition(_) => "disposition",
            Self::Detach(_) => "detach",
            Self::End(_) => "end",
            Self::Close(_) => "close",
        }
    }
}

/// The numeric descriptor a performative descriptor names, in either form.
fn resolve(descriptor: &Descriptor<'_>) -> Option<u64> {
    match descriptor {
        Descriptor::Code(code) => Some(*code),
        Descriptor::Symbol(name) => DESCRIPTORS
            .iter()
            .find(|(_, symbolic)| symbolic == name)
            .map(|(code, _)| *code),
    }
}

fn read_error<'a>(fields: &mut Fields<'a>) -> Result<Option<AmqpError<'a>>, DecodeError> {
    match fields.any()? {
        Some(value) => Ok(Some(AmqpError::from_value(value)?)),
        None => Ok(None),
    }
}

fn write_error<'a>(error: Option<&AmqpError<'a>>) -> Value<'a> {
    error.map_or(Value::Null, AmqpError::to_value)
}

/// A `boolean` field whose default is `false`: `true` is written, `false` is
/// omitted.
const fn flag(value: bool) -> Value<'static> {
    if value {
        Value::Boolean(true)
    } else {
        Value::Null
    }
}

const fn opt_uint(value: Option<u32>) -> Value<'static> {
    match value {
        Some(value) => Value::Uint(value),
        None => Value::Null,
    }
}

const fn opt_ushort(value: Option<u16>) -> Value<'static> {
    match value {
        Some(value) => Value::Ushort(value),
        None => Value::Null,
    }
}

const fn opt_ulong(value: Option<u64>) -> Value<'static> {
    match value {
        Some(value) => Value::Ulong(value),
        None => Value::Null,
    }
}

const fn opt_string(value: Option<&str>) -> Value<'_> {
    match value {
        Some(value) => Value::String(value),
        None => Value::Null,
    }
}

const fn uint_unless(value: u32, default: u32) -> Value<'static> {
    if value == default {
        Value::Null
    } else {
        Value::Uint(value)
    }
}

const fn ushort_unless(value: u16, default: u16) -> Value<'static> {
    if value == default {
        Value::Null
    } else {
        Value::Ushort(value)
    }
}

const fn ubyte_unless(value: u8, default: u8) -> Value<'static> {
    if value == default {
        Value::Null
    } else {
        Value::Ubyte(value)
    }
}

/// Clones a `*`-typed field's value for the encoder.
///
/// The only clone in this module. A `source`, a `target`, an `unsettled` map
/// or a `properties` map is a decoded `Value` the caller owns, and the
/// encoder takes `&self` because a performative is often sent more than once
/// — an `attach` and its answering `attach` carry the same termini.
fn opt_value<'a>(value: &Option<Value<'a>>) -> Value<'a> {
    value.clone().unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::condition;

    fn round_trip(performative: &Performative<'_>) -> Vec<u8> {
        let mut bytes = Vec::new();
        performative.encode(&mut bytes).expect("encodes");
        let (back, used) = Performative::decode(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len(), "{}", performative.name());
        assert_eq!(&back, performative, "{}", performative.name());
        bytes
    }

    #[test]
    fn every_performative_round_trips_at_its_defaults() {
        for performative in [
            Performative::Open(Open::new("c1")),
            Performative::Begin(Begin::new(0, 8, 8)),
            Performative::Attach(Attach::new("link-1", 0, Role::Sender)),
            Performative::Flow(Flow::session(8, 0, 8)),
            Performative::Transfer(Transfer::new(0)),
            Performative::Disposition(Disposition::new(Role::Receiver, 0)),
            Performative::Detach(Detach::new(0)),
            Performative::End(End::default()),
            Performative::Close(Close::default()),
        ] {
            round_trip(&performative);
        }
    }

    #[test]
    fn every_performative_round_trips_with_every_field_set() {
        let props = Value::Map(vec![(Value::Symbol("product"), Value::String("weida"))]);
        let state = Value::Described(Box::new(crate::Described {
            descriptor: Descriptor::Code(0x24),
            value: Value::List(Vec::new()),
        }));
        let error = AmqpError {
            condition: condition::LINK_STOLEN,
            description: Some("attached elsewhere"),
            info: Some(Value::Map(vec![(
                Value::Symbol("queue"),
                Value::String("q1"),
            )])),
        };

        round_trip(&Performative::Open(Open {
            container_id: "c1",
            hostname: Some("vhost:tenant-1"),
            max_frame_size: 131_072,
            channel_max: 511,
            idle_time_out: Some(30_000),
            outgoing_locales: Multiple::One("en-US"),
            incoming_locales: Multiple::from_slice(&["en-US", "de-DE"]),
            offered_capabilities: Multiple::One("ANONYMOUS-RELAY"),
            desired_capabilities: Multiple::One("sole-connection-for-container"),
            properties: Some(props.clone()),
        }));

        round_trip(&Performative::Begin(Begin {
            remote_channel: Some(7),
            next_outgoing_id: 42,
            incoming_window: 400,
            outgoing_window: 400,
            handle_max: 255,
            offered_capabilities: Multiple::One("a"),
            desired_capabilities: Multiple::One("b"),
            properties: Some(props.clone()),
        }));

        round_trip(&Performative::Attach(Attach {
            name: "link-1",
            handle: 3,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(state.clone()),
            target: Some(state.clone()),
            unsettled: Some(Value::Map(vec![(Value::Binary(b"tag-1"), state.clone())])),
            incomplete_unsettled: true,
            initial_delivery_count: Some(0),
            max_message_size: Some(1_048_576),
            offered_capabilities: Multiple::One("a"),
            desired_capabilities: Multiple::from_slice(&["b", "c"]),
            properties: Some(props.clone()),
        }));

        round_trip(&Performative::Flow(Flow {
            next_incoming_id: Some(1),
            incoming_window: 400,
            next_outgoing_id: 2,
            outgoing_window: 400,
            handle: Some(3),
            delivery_count: Some(9),
            link_credit: Some(170),
            available: Some(1000),
            drain: true,
            echo: true,
            properties: Some(props.clone()),
        }));

        round_trip(&Performative::Transfer(Transfer {
            handle: 3,
            delivery_id: Some(11),
            delivery_tag: Some(b"tag-1"),
            message_format: Some(MESSAGE_FORMAT),
            settled: Some(false),
            more: true,
            rcv_settle_mode: Some(ReceiverSettleMode::First),
            state: Some(state.clone()),
            resume: true,
            aborted: true,
            batchable: true,
        }));

        round_trip(&Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 11,
            last: Some(14),
            settled: true,
            state: Some(state),
            batchable: true,
        }));

        round_trip(&Performative::Detach(Detach {
            handle: 3,
            closed: true,
            error: Some(error.clone()),
        }));
        round_trip(&Performative::End(End {
            error: Some(error.clone()),
        }));
        round_trip(&Performative::Close(Close { error: Some(error) }));
    }

    #[test]
    fn a_default_field_is_omitted_rather_than_written() {
        // `open` with only a container-id: three octets of descriptor, a
        // list8 header, and the string. Nine trailing nulls do not appear.
        let mut bytes = Vec::new();
        Performative::Open(Open::new("c1"))
            .encode(&mut bytes)
            .expect("encodes");
        assert_eq!(
            bytes,
            [0x00, 0x53, 0x10, 0xc0, 0x05, 0x01, 0xa1, 0x02, b'c', b'1']
        );

        // `max-frame-size` set to its own default is still omitted.
        let mut same = Open::new("c1");
        same.max_frame_size = DEFAULT_MAX_FRAME_SIZE;
        same.channel_max = DEFAULT_CHANNEL_MAX;
        let mut written = Vec::new();
        Performative::Open(same)
            .encode(&mut written)
            .expect("encodes");
        assert_eq!(written, bytes);
    }

    #[test]
    fn an_absent_default_decodes_to_the_default() {
        let mut bytes = Vec::new();
        Performative::Open(Open::new("c1"))
            .encode(&mut bytes)
            .expect("encodes");
        let (Performative::Open(open), _) = Performative::decode(&bytes, Limits::DEFAULT).unwrap()
        else {
            panic!("expected an open");
        };
        assert_eq!(open.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        assert_eq!(open.channel_max, DEFAULT_CHANNEL_MAX);
        assert_eq!(open.idle_time_out, None);

        let mut bytes = Vec::new();
        Performative::Begin(Begin::new(0, 8, 8))
            .encode(&mut bytes)
            .expect("encodes");
        let (Performative::Begin(begin), _) =
            Performative::decode(&bytes, Limits::DEFAULT).unwrap()
        else {
            panic!("expected a begin");
        };
        assert_eq!(begin.handle_max, DEFAULT_HANDLE_MAX);
    }

    #[test]
    fn a_mandatory_field_left_out_is_refused() {
        // `begin` with only a remote-channel: the three windows are
        // mandatory and have no defaults, which is what makes them the
        // safe-by-construction bounds.
        let mut bytes = Vec::new();
        encode::composite(
            &Descriptor::Code(Begin::DESCRIPTOR),
            &[Value::Ushort(1)],
            &mut bytes,
        )
        .expect("encodes");
        assert_eq!(
            Performative::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::MissingMandatoryField {
                composite: "begin",
                field: "next-outgoing-id"
            })
        );
    }

    #[test]
    fn role_survives_the_boolean_it_travels_as() {
        for role in [Role::Sender, Role::Receiver] {
            let mut bytes = Vec::new();
            Performative::Attach(Attach::new("l", 0, role))
                .encode(&mut bytes)
                .expect("encodes");
            let (Performative::Attach(attach), _) =
                Performative::decode(&bytes, Limits::DEFAULT).unwrap()
            else {
                panic!("expected an attach");
            };
            assert_eq!(attach.role, role);
        }
        // And the octet is the one the specification assigns.
        let mut bytes = Vec::new();
        Performative::Attach(Attach::new("l", 0, Role::Sender))
            .encode(&mut bytes)
            .expect("encodes");
        assert!(
            bytes.contains(&crate::codes::FALSE),
            "sender is false on the wire"
        );
    }

    #[test]
    fn a_symbolic_descriptor_names_the_same_performative() {
        // Every implementation writes the numeric form, but Part 1 §1.5
        // assigns both and either is legal.
        let mut bytes = Vec::new();
        encode::composite(
            &Descriptor::Symbol(Close::SYMBOLIC),
            &[Value::Null],
            &mut bytes,
        )
        .expect("encodes");
        let (performative, used) = Performative::decode(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len());
        assert_eq!(performative, Performative::Close(Close::default()));
    }

    #[test]
    fn a_tenth_performative_is_refused_rather_than_ignored() {
        let mut bytes = Vec::new();
        encode::composite(&Descriptor::Code(0x19), &[], &mut bytes).expect("encodes");
        assert_eq!(
            Performative::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::UnknownComposite {
                kind: "performative",
                descriptor: Some(0x19)
            })
        );

        let mut bytes = Vec::new();
        encode::composite(&Descriptor::Symbol("amqp:invented:list"), &[], &mut bytes)
            .expect("encodes");
        assert_eq!(
            Performative::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::UnknownComposite {
                kind: "performative",
                descriptor: None
            })
        );
    }

    #[test]
    fn a_delivery_tag_over_thirty_two_octets_is_refused_both_ways() {
        let long = [0u8; 33];
        let mut transfer = Transfer::new(0);
        transfer.delivery_tag = Some(&long);
        let mut bytes = Vec::new();
        assert_eq!(
            Performative::Transfer(transfer).encode(&mut bytes),
            Err(EncodeError::RestrictionViolated {
                restriction: "delivery-tag",
                value: 33,
                limit: 32
            })
        );

        // And a peer that sends one anyway.
        let mut bytes = Vec::new();
        encode::composite(
            &Descriptor::Code(Transfer::DESCRIPTOR),
            &[Value::Uint(0), Value::Uint(1), Value::Binary(&long)],
            &mut bytes,
        )
        .expect("encodes");
        assert_eq!(
            Performative::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::RestrictionViolated {
                restriction: "delivery-tag",
                value: 33,
                limit: 32
            })
        );
    }

    #[test]
    fn transfer_settled_keeps_its_three_states_apart() {
        // Unset means "whatever the link negotiated"; false means "not
        // settled". A codec that folded them would lose the distinction the
        // specification builds at-most-once out of.
        for settled in [None, Some(false), Some(true)] {
            let mut transfer = Transfer::new(0);
            transfer.settled = settled;
            let mut bytes = Vec::new();
            Performative::Transfer(transfer)
                .encode(&mut bytes)
                .expect("encodes");
            let (Performative::Transfer(back), _) =
                Performative::decode(&bytes, Limits::DEFAULT).unwrap()
            else {
                panic!("expected a transfer");
            };
            assert_eq!(back.settled, settled);
        }
    }

    #[test]
    fn the_descriptor_table_is_the_nine_in_order() {
        let codes: Vec<u64> = DESCRIPTORS.iter().map(|(code, _)| *code).collect();
        assert_eq!(codes, (0x10..=0x18).collect::<Vec<u64>>());
        for (code, symbolic) in DESCRIPTORS {
            assert!(symbolic.starts_with("amqp:"), "{symbolic}");
            assert!(symbolic.ends_with(":list"), "{symbolic}");
            assert_eq!(resolve(&Descriptor::Symbol(symbolic)), Some(code));
        }
    }

    #[test]
    fn an_error_round_trips_through_its_own_descriptor() {
        let error = AmqpError::new(condition::CONNECTION_FRAMING_ERROR)
            .described("a valid frame header cannot be formed");
        let value = error.to_value();
        assert_eq!(AmqpError::from_value(value).unwrap(), error);
    }

    #[test]
    fn an_error_without_a_condition_is_refused() {
        let value = crate::fields::described_value(AmqpError::DESCRIPTOR, vec![Value::Null]);
        assert_eq!(
            AmqpError::from_value(value),
            Err(DecodeError::MissingMandatoryField {
                composite: "error",
                field: "condition"
            })
        );
    }
}
