//! `source` and `target`: the stateful ends of a link.
//!
//! A terminus is "the stateful end of a link inside a node, whose primary
//! responsibility is to maintain a record of the status of each active
//! delivery attempt until such a time as it is safe to forget" — the
//! unsettled map (Part 2 §2.6). Sources track outgoing messages, targets
//! incoming, and a link is always "between the source as described by the
//! sender, and the target as described by the receiver" (Part 2 §2.6.3).
//!
//! That sentence is the load-bearing one and it is why both a [`Source`] and
//! a [`Target`] go in every `attach`, whichever role this end has: the
//! initiator describes both ends and the partner answers with what it
//! actually created, which may differ. "An endpoint that cannot create
//! exactly the requested terminus MAY adjust properties but MUST then report
//! what it actually created", so the answering `attach` is data and not an
//! acknowledgement — a client that ignored it would be running against a
//! terminus it did not ask for. [`crate::Link::remote_source`] and
//! [`crate::Link::remote_target`] are how a caller sees what it got.
//!
//! A partner that will not provide a terminus at all answers with the field
//! **null** and MUST then immediately detach. That is not an error code, it
//! is an absence, and it is how "no such address" arrives when the peer does
//! not send `amqp:not-found`.
//!
//! # Addresses are not defined by the core standard
//!
//! `address` is an archetype the core specification leaves undefined, with
//! `address-string` as its only concrete provider (Part 3 §3.2.15). Every
//! broker invented its own syntax and they do not agree — RabbitMQ 4.x wants
//! `/queues/:queue` or `/exchanges/:exchange/:routing-key`, Artemis wants a
//! plain name or `address::queue`, Service Bus wants the entity path, ActiveMQ
//! Classic wants `queue://name`. So this type carries the string and
//! interprets nothing, and `docs/libraries/amqp.md` records which syntax each
//! peer demanded rather than this crate guessing.

use weida_amqp_codec::fields::{Fields, described_value};
use weida_amqp_codec::{DecodeError, Value};

use crate::options::multiple;
use crate::owned::OwnedValue;

/// `terminus-durability`: what survives (Part 3 §3.5.5).
///
/// The knob that is *not* message durability. `header.durable` is per message
/// and a hard contract; this is per terminus and decides what state the node
/// keeps. [`TerminusDurability::UnsettledState`] is what makes exactly-once
/// survive a broker restart, because it is the unsettled map that resumption
/// compares — and RabbitMQ explicitly does not support the machinery that
/// would use it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TerminusDurability {
    /// `0`, the default: no terminus state is retained.
    #[default]
    None,
    /// `1`: the existence and configuration of the terminus are retained.
    Configuration,
    /// `2`: those, plus the unsettled state for durable messages.
    UnsettledState,
}

impl TerminusDurability {
    /// The `uint` on the wire.
    #[must_use]
    pub const fn code(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Configuration => 1,
            Self::UnsettledState => 2,
        }
    }

    /// The durability a `uint` names.
    pub const fn from_code(code: u32) -> Result<Self, DecodeError> {
        match code {
            0 => Ok(Self::None),
            1 => Ok(Self::Configuration),
            2 => Ok(Self::UnsettledState),
            other => Err(DecodeError::RestrictionViolated {
                restriction: "terminus-durability",
                value: other as u64,
                limit: 2,
            }),
        }
    }
}

/// `terminus-expiry-policy`: when the `timeout` countdown starts
/// (Part 3 §3.5.6).
///
/// Not "how long the terminus lives" but *when the clock starts*.
/// Re-attaching before expiry aborts the countdown, and a recurrence restarts
/// the timer from the full configured value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TerminusExpiryPolicy {
    /// The expiry timer starts when the link is detached.
    LinkDetach,
    /// `session-end`, the default: the timer starts when the session ends.
    #[default]
    SessionEnd,
    /// The timer starts when the connection closes.
    ConnectionClose,
    /// The terminus never expires.
    Never,
}

impl TerminusExpiryPolicy {
    /// The `symbol` on the wire.
    #[must_use]
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::LinkDetach => "link-detach",
            Self::SessionEnd => "session-end",
            Self::ConnectionClose => "connection-close",
            Self::Never => "never",
        }
    }

    /// The policy a `symbol` names.
    pub fn from_symbol(symbol: &str) -> Result<Self, DecodeError> {
        match symbol {
            "link-detach" => Ok(Self::LinkDetach),
            "session-end" => Ok(Self::SessionEnd),
            "connection-close" => Ok(Self::ConnectionClose),
            "never" => Ok(Self::Never),
            _ => Err(DecodeError::WrongType {
                field: "expiry-policy",
                code: weida_amqp_codec::codes::SYM8,
            }),
        }
    }
}

/// `source.distribution-mode`: anycast or multicast, in the protocol's own
/// words (Part 3 §3.5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistributionMode {
    /// `move`: the message goes to ACQUIRED before transfer and to ARCHIVED
    /// once settled successfully, so it is gone for other links. This is what
    /// competing consumers are built from.
    Move,
    /// `copy`: node state is unchanged, so other links can still get the same
    /// message. The source must then retain enough state not to resend a
    /// message already accepted, which `move` gets for free.
    Copy,
}

impl DistributionMode {
    /// The `symbol` on the wire.
    #[must_use]
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Move => "move",
            Self::Copy => "copy",
        }
    }

    /// The mode a `symbol` names.
    pub fn from_symbol(symbol: &str) -> Result<Self, DecodeError> {
        match symbol {
            "move" => Ok(Self::Move),
            "copy" => Ok(Self::Copy),
            _ => Err(DecodeError::WrongType {
                field: "distribution-mode",
                code: weida_amqp_codec::codes::SYM8,
            }),
        }
    }
}

/// `source`, descriptor `0x28` (Part 3 §3.5.3).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Source {
    /// The node's address, in whatever syntax the peer demands. `None` on a
    /// request with `dynamic` set, and `None` in an answer means the peer
    /// refused to provide a terminus at all.
    pub address: Option<String>,
    /// What state the node retains.
    pub durable: TerminusDurability,
    /// When the `timeout` countdown starts.
    pub expiry_policy: TerminusExpiryPolicy,
    /// Seconds. Default 0.
    pub timeout: u32,
    /// Ask the *partner* to create the node. The requester MUST NOT set
    /// `address`, and the creator MUST return the generated address — which
    /// is how a reply node is made on demand.
    pub dynamic: bool,
    /// Desired, then actual, properties of a dynamic node, including its
    /// `lifetime-policy`.
    pub dynamic_node_properties: Option<OwnedValue>,
    /// `move` or `copy`.
    pub distribution_mode: Option<DistributionMode>,
    /// A `filter-set`: a symbol-keyed map whose values are described types.
    /// The receiver sets what it wants, the sender answers with what is
    /// actually in place, and **the receiver MUST check the answer and detach
    /// if it does not meet its needs** — an invalid filter is commonly just
    /// omitted from the answering `attach` rather than reported.
    pub filter: Option<OwnedValue>,
    /// The outcome applied to a transfer that has not reached a terminal
    /// state at the receiver when it is settled, including when the source is
    /// destroyed.
    pub default_outcome: Option<OwnedValue>,
    /// The symbolic descriptors of the outcomes this link may choose from.
    /// May be empty, meaning `default-outcome` is assumed for everything; if
    /// neither is set the source MUST support `accepted`.
    pub outcomes: Vec<String>,
    /// Extension capabilities of the node.
    pub capabilities: Vec<String>,
}

impl Source {
    /// `0x28`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0028;
    /// `amqp:source:list`.
    pub const SYMBOLIC: &'static str = "amqp:source:list";

    /// A source naming one address and nothing else.
    #[must_use]
    pub fn at(address: impl Into<String>) -> Self {
        Self {
            address: Some(address.into()),
            ..Self::default()
        }
    }

    /// A source asking the partner to create the node and report its
    /// address, which is how a request-reply inbox is made.
    #[must_use]
    pub fn dynamic() -> Self {
        Self {
            dynamic: true,
            ..Self::default()
        }
    }

    /// The described value this source encodes to.
    #[must_use]
    pub fn to_value(&self) -> Value<'_> {
        described_value(
            Self::DESCRIPTOR,
            vec![
                self.address.as_deref().map_or(Value::Null, Value::String),
                unless_default(self.durable.code()),
                unless_symbol(self.expiry_policy, TerminusExpiryPolicy::SessionEnd),
                unless_default(self.timeout),
                flag(self.dynamic),
                borrowed(&self.dynamic_node_properties),
                self.distribution_mode
                    .map_or(Value::Null, |mode| Value::Symbol(mode.symbol())),
                borrowed(&self.filter),
                borrowed(&self.default_outcome),
                symbols(&self.outcomes),
                symbols(&self.capabilities),
            ],
        )
    }

    /// Reads a source from a decoded described value.
    pub fn from_value(value: Value<'_>) -> Result<Self, DecodeError> {
        let mut fields = Fields::from_described(value, Self::DESCRIPTOR, Self::SYMBOLIC, "source")?;
        Ok(Self {
            address: fields.string("address")?.map(str::to_owned),
            durable: TerminusDurability::from_code(fields.uint_or("durable", 0)?)?,
            expiry_policy: match fields.symbol("expiry-policy")? {
                Some(symbol) => TerminusExpiryPolicy::from_symbol(symbol)?,
                None => TerminusExpiryPolicy::SessionEnd,
            },
            timeout: fields.uint_or("timeout", 0)?,
            dynamic: fields.boolean_or("dynamic", false)?,
            dynamic_node_properties: keep(fields.map("dynamic-node-properties")?)?,
            distribution_mode: match fields.symbol("distribution-mode")? {
                Some(symbol) => Some(DistributionMode::from_symbol(symbol)?),
                None => None,
            },
            filter: keep(fields.map("filter")?)?,
            default_outcome: keep(fields.any()?)?,
            outcomes: fields
                .multiple("outcomes")?
                .iter()
                .map(str::to_owned)
                .collect(),
            capabilities: fields
                .multiple("capabilities")?
                .iter()
                .map(str::to_owned)
                .collect(),
        })
    }
}

/// `target`, descriptor `0x29` (Part 3 §3.5.4).
///
/// The same fields as a [`Source`] minus the four that only make sense where
/// messages come *from*: `distribution-mode`, `filter`, `default-outcome` and
/// `outcomes`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Target {
    /// The node's address.
    pub address: Option<String>,
    /// What state the node retains.
    pub durable: TerminusDurability,
    /// When the `timeout` countdown starts.
    pub expiry_policy: TerminusExpiryPolicy,
    /// Seconds. Default 0.
    pub timeout: u32,
    /// Ask the partner to create the node.
    pub dynamic: bool,
    /// Desired, then actual, properties of a dynamic node.
    pub dynamic_node_properties: Option<OwnedValue>,
    /// Extension capabilities of the node.
    pub capabilities: Vec<String>,
}

impl Target {
    /// `0x29`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_0029;
    /// `amqp:target:list`.
    pub const SYMBOLIC: &'static str = "amqp:target:list";

    /// A target naming one address and nothing else.
    #[must_use]
    pub fn at(address: impl Into<String>) -> Self {
        Self {
            address: Some(address.into()),
            ..Self::default()
        }
    }

    /// The *anonymous* terminus: a null address, where every transfer must
    /// carry `properties.to` instead.
    ///
    /// A distinct thing from "no target", which is what an answering `attach`
    /// with a null target means. RabbitMQ settles an unroutable transfer on
    /// an anonymous terminus `released`.
    #[must_use]
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// The described value this target encodes to.
    #[must_use]
    pub fn to_value(&self) -> Value<'_> {
        described_value(
            Self::DESCRIPTOR,
            vec![
                self.address.as_deref().map_or(Value::Null, Value::String),
                unless_default(self.durable.code()),
                unless_symbol(self.expiry_policy, TerminusExpiryPolicy::SessionEnd),
                unless_default(self.timeout),
                flag(self.dynamic),
                borrowed(&self.dynamic_node_properties),
                symbols(&self.capabilities),
            ],
        )
    }

    /// Reads a target from a decoded described value.
    pub fn from_value(value: Value<'_>) -> Result<Self, DecodeError> {
        let mut fields = Fields::from_described(value, Self::DESCRIPTOR, Self::SYMBOLIC, "target")?;
        Ok(Self {
            address: fields.string("address")?.map(str::to_owned),
            durable: TerminusDurability::from_code(fields.uint_or("durable", 0)?)?,
            expiry_policy: match fields.symbol("expiry-policy")? {
                Some(symbol) => TerminusExpiryPolicy::from_symbol(symbol)?,
                None => TerminusExpiryPolicy::SessionEnd,
            },
            timeout: fields.uint_or("timeout", 0)?,
            dynamic: fields.boolean_or("dynamic", false)?,
            dynamic_node_properties: keep(fields.map("dynamic-node-properties")?)?,
            capabilities: fields
                .multiple("capabilities")?
                .iter()
                .map(str::to_owned)
                .collect(),
        })
    }
}

/// One field of a terminus, borrowed back out of its [`OwnedValue`].
///
/// The lifetime works because [`Source::to_value`] and [`Target::to_value`]
/// take `&self`: the value they build borrows from the terminus, and the
/// terminus owns the octets.
fn borrowed(value: &Option<OwnedValue>) -> Value<'_> {
    match value {
        Some(owned) => owned.value(),
        None => Value::Null,
    }
}

/// Keeps a decoded field so it can outlive the frame it arrived in.
///
/// The encode cannot fail for anything a decode produced — the decoder
/// accepts only what the encoder can write back — but the error is reported
/// rather than asserted, because "cannot fail" is a claim about the codec and
/// this is the only place that would find out otherwise.
fn keep(value: Option<Value<'_>>) -> Result<Option<OwnedValue>, DecodeError> {
    match value {
        Some(value) => Ok(Some(OwnedValue::new(&value).map_err(|_| {
            DecodeError::WrongType {
                field: "terminus field",
                code: value.canonical_code(),
            }
        })?)),
        None => Ok(None),
    }
}

const fn flag(value: bool) -> Value<'static> {
    if value {
        Value::Boolean(true)
    } else {
        Value::Null
    }
}

const fn unless_default(value: u32) -> Value<'static> {
    if value == 0 {
        Value::Null
    } else {
        Value::Uint(value)
    }
}

fn unless_symbol(policy: TerminusExpiryPolicy, default: TerminusExpiryPolicy) -> Value<'static> {
    if policy == default {
        Value::Null
    } else {
        Value::Symbol(policy.symbol())
    }
}

fn symbols(items: &[String]) -> Value<'_> {
    multiple(items).to_value()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_source(source: &Source) {
        let value = source.to_value();
        let bytes = weida_amqp_codec::encode::to_vec(&value).expect("encodes");
        let (decoded, used) =
            weida_amqp_codec::decode::value(&bytes, weida_amqp_codec::Limits::BODY)
                .expect("decodes");
        assert_eq!(used, bytes.len());
        assert_eq!(&Source::from_value(decoded).expect("reads"), source);
    }

    fn round_trip_target(target: &Target) {
        let value = target.to_value();
        let bytes = weida_amqp_codec::encode::to_vec(&value).expect("encodes");
        let (decoded, _) = weida_amqp_codec::decode::value(&bytes, weida_amqp_codec::Limits::BODY)
            .expect("decodes");
        assert_eq!(&Target::from_value(decoded).expect("reads"), target);
    }

    #[test]
    fn the_five_fields_the_item_names_round_trip() {
        round_trip_source(&Source {
            address: Some("/queues/q1".into()),
            durable: TerminusDurability::UnsettledState,
            expiry_policy: TerminusExpiryPolicy::Never,
            timeout: 300,
            dynamic: false,
            ..Source::default()
        });
        round_trip_target(&Target {
            address: Some("/exchanges/amq.direct/rk".into()),
            durable: TerminusDurability::Configuration,
            expiry_policy: TerminusExpiryPolicy::LinkDetach,
            timeout: 60,
            dynamic: false,
            ..Target::default()
        });
    }

    #[test]
    fn a_minimal_terminus_is_legal_and_encodes_to_almost_nothing() {
        // "A minimal source or target with every field unset is legal for
        // containers that only produce or only consume" (Part 3 §3.5.3-4).
        let source = Source::default();
        let bytes = weida_amqp_codec::encode::to_vec(&source.to_value()).expect("encodes");
        assert_eq!(bytes, [0x00, 0x53, 0x28, 0x45], "descriptor and list0");
        round_trip_source(&source);

        let target = Target::anonymous();
        let bytes = weida_amqp_codec::encode::to_vec(&target.to_value()).expect("encodes");
        assert_eq!(bytes, [0x00, 0x53, 0x29, 0x45]);
        assert_eq!(
            target.address, None,
            "the anonymous terminus is a null address, not a missing target"
        );
    }

    #[test]
    fn the_defaults_are_the_specifications_and_are_omitted() {
        assert_eq!(TerminusDurability::default(), TerminusDurability::None);
        assert_eq!(TerminusDurability::default().code(), 0);
        assert_eq!(
            TerminusExpiryPolicy::default(),
            TerminusExpiryPolicy::SessionEnd
        );
        assert_eq!(TerminusExpiryPolicy::default().symbol(), "session-end");
        // A field set to its own default does not appear on the wire.
        let source = Source {
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            ..Source::at("q")
        };
        let bytes = weida_amqp_codec::encode::to_vec(&source.to_value()).expect("encodes");
        assert_eq!(
            bytes,
            [0x00, 0x53, 0x28, 0xc0, 0x04, 0x01, 0xa1, 0x01, b'q']
        );
    }

    #[test]
    fn a_dynamic_source_carries_no_address_and_a_created_one_does() {
        let asked = Source::dynamic();
        assert!(asked.dynamic);
        assert_eq!(
            asked.address, None,
            "the requester MUST NOT set address when asking for a dynamic node"
        );
        round_trip_source(&asked);

        // The creator "MUST return the generated address", so the answer has
        // both.
        let created = Source {
            address: Some("_INBOX.link-1.client-1".into()),
            dynamic: true,
            ..Source::default()
        };
        round_trip_source(&created);
    }

    #[test]
    fn a_filter_set_and_a_default_outcome_survive_the_frame_they_arrived_in() {
        // Both are consulted for the life of the link, so both have to
        // outlive the read that produced them.
        let source = Source {
            address: Some("s".into()),
            filter: Some(
                OwnedValue::new(&Value::Map(vec![(
                    Value::Symbol("apache.org:selector-filter:string"),
                    Value::String("amqp.correlation_id = 'abc'"),
                )]))
                .unwrap(),
            ),
            default_outcome: Some(
                OwnedValue::new(&weida_amqp_codec::state::DeliveryState::Released.to_value())
                    .unwrap(),
            ),
            outcomes: vec!["amqp:accepted:list".into(), "amqp:rejected:list".into()],
            capabilities: vec!["topic".into()],
            ..Source::default()
        };
        round_trip_source(&source);
    }

    #[test]
    fn a_distribution_mode_is_move_or_copy_and_nothing_else() {
        assert_eq!(DistributionMode::Move.symbol(), "move");
        assert_eq!(DistributionMode::Copy.symbol(), "copy");
        assert_eq!(
            DistributionMode::from_symbol("move").unwrap(),
            DistributionMode::Move
        );
        assert!(DistributionMode::from_symbol("balanced").is_err());
        round_trip_source(&Source {
            address: Some("s".into()),
            distribution_mode: Some(DistributionMode::Copy),
            ..Source::default()
        });
    }

    #[test]
    fn a_durability_or_policy_outside_its_restriction_is_refused() {
        assert!(TerminusDurability::from_code(3).is_err());
        assert!(TerminusExpiryPolicy::from_symbol("link-close").is_err());
    }

    #[test]
    fn a_source_where_a_target_belongs_is_refused() {
        // The two differ only in their descriptor, and confusing them is the
        // easiest way to attach a link the wrong way round.
        let source = Source::at("q");
        assert!(Target::from_value(source.to_value()).is_err());
        let target = Target::at("q");
        assert!(Source::from_value(target.to_value()).is_err());
    }
}
