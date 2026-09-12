//! The restricted types the performatives are built from.
//!
//! Part 2 and Part 3 define most of their fields not as primitives but as
//! *restricted* types: `role` is a `boolean` with two named values, `handle`
//! is a `uint` that means something, `sender-settle-mode` is a `ubyte` with
//! three. Every one of them is a Rust type here, for the reason the
//! specification gives them names at all: `attach(role: false)` and
//! `attach(role: Role::Sender)` read differently, and only the second cannot
//! be written the wrong way round.
//!
//! [`Multiple`] is the odd one out. It is not a type in the specification but
//! an *attribute* of a field, and Part 1 §1.4 fixes its encoding: "a single
//! element of the type specified in the field description is always
//! permitted. Multiple values are represented by the use of an array", and
//! "a null value and a zero-length array ... MUST be treated as semantically
//! identical". Three wire forms, one meaning, so one type.

use crate::codes;
use crate::error::{DecodeError, EncodeError};
use crate::value::{Array, ElementKind, Value};

/// The largest `delivery-tag` the type permits: "up to 32 octets"
/// (Part 2 §2.8.7).
pub const MAX_DELIVERY_TAG_BYTES: usize = 32;

/// The largest `transaction-id` the type permits, the same 32 octets
/// (Part 4 §4.5.4).
pub const MAX_TRANSACTION_ID_BYTES: usize = 32;

/// `role`: which end of the link a peer is (Part 2 §2.8.1).
///
/// `false` is the sender and `true` the receiver, which is worth writing down
/// because the mapping is not the one a reader guesses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// `false`. Sends `transfer`; holds `delivery-count` and may not grant
    /// credit.
    Sender,
    /// `true`. Sends `flow` with `link-credit`; is the only end that may
    /// grant it.
    Receiver,
}

impl Role {
    /// The `boolean` on the wire.
    #[must_use]
    pub const fn is_receiver(self) -> bool {
        matches!(self, Self::Receiver)
    }

    /// The role a `boolean` names.
    #[must_use]
    pub const fn from_bool(receiver: bool) -> Self {
        if receiver {
            Self::Receiver
        } else {
            Self::Sender
        }
    }

    /// The other end.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Sender => Self::Receiver,
            Self::Receiver => Self::Sender,
        }
    }
}

/// `sender-settle-mode`: how the sender settles, negotiated on `attach`
/// (Part 2 §2.8.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum SenderSettleMode {
    /// `0`: every delivery is sent unsettled, so `transfer.settled` MUST be
    /// false or unset on every frame unless the delivery is aborted. The
    /// at-least-once and exactly-once constructions both start here.
    Unsettled,
    /// `1`: every delivery is settled at the sender by the time it is fully
    /// transferred — at-most-once, and the sender has announced up front that
    /// it has already forgotten the tag.
    Settled,
    /// `2`, the default: the sender may mix the two per delivery.
    #[default]
    Mixed,
}

impl SenderSettleMode {
    /// The `ubyte` on the wire.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Unsettled => 0,
            Self::Settled => 1,
            Self::Mixed => 2,
        }
    }

    /// The mode an octet names.
    pub const fn from_octet(octet: u8) -> Result<Self, DecodeError> {
        match octet {
            0 => Ok(Self::Unsettled),
            1 => Ok(Self::Settled),
            2 => Ok(Self::Mixed),
            other => Err(DecodeError::RestrictionViolated {
                restriction: "sender-settle-mode",
                value: other as u64,
                limit: 2,
            }),
        }
    }
}

/// `receiver-settle-mode`: when the receiver settles, negotiated on `attach`
/// (Part 2 §2.8.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ReceiverSettleMode {
    /// `0`, the default: the receiver settles as soon as it has processed the
    /// delivery, without waiting for the sender. A lost `disposition` then
    /// means a duplicate on resumption, which is what makes this
    /// at-least-once.
    #[default]
    First,
    /// `1`: the receiver MUST NOT settle until it has sent its `disposition`
    /// *and* received a settled `disposition` back. The only mechanism in the
    /// protocol that suppresses duplicates rather than flagging them — and
    /// the one Artemis refuses and RabbitMQ does not implement.
    Second,
}

impl ReceiverSettleMode {
    /// The `ubyte` on the wire.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }

    /// The mode an octet names.
    pub const fn from_octet(octet: u8) -> Result<Self, DecodeError> {
        match octet {
            0 => Ok(Self::First),
            1 => Ok(Self::Second),
            other => Err(DecodeError::RestrictionViolated {
                restriction: "receiver-settle-mode",
                value: other as u64,
                limit: 1,
            }),
        }
    }
}

/// A field the specification marks `multiple`: absent, one value, or an array
/// of them.
///
/// Only symbol-valued fields are `multiple` in the specification —
/// capabilities, locales, outcomes, SASL mechanisms — so this type is
/// symbol-valued rather than generic. Part 1 §1.4's rule that a null and an
/// empty array are semantically identical is why [`Multiple::None`] is the
/// only representation of absence: the decoder folds both into it, and the
/// encoder writes `null`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Multiple<'a> {
    /// No value: `null` on the wire, or an array with no elements.
    #[default]
    None,
    /// One value, written as a bare `symbol`.
    One(&'a str),
    /// Several, written as an `array` of `symbol`.
    Many(Vec<&'a str>),
}

impl<'a> Multiple<'a> {
    /// A `Multiple` holding whatever `items` holds, choosing the wire form
    /// the specification's rule implies.
    #[must_use]
    pub fn from_slice(items: &[&'a str]) -> Self {
        match items {
            [] => Self::None,
            [one] => Self::One(one),
            many => Self::Many(many.to_vec()),
        }
    }

    /// Whether there is nothing here.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::One(_) => false,
            Self::Many(items) => items.is_empty(),
        }
    }

    /// How many values.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::One(_) => 1,
            Self::Many(items) => items.len(),
        }
    }

    /// The values, in wire order.
    pub fn iter(&self) -> impl Iterator<Item = &'a str> + '_ {
        let (one, many) = match self {
            Self::None => (None, [].as_slice()),
            Self::One(one) => (Some(*one), [].as_slice()),
            Self::Many(items) => (None, items.as_slice()),
        };
        one.into_iter().chain(many.iter().copied())
    }

    /// Whether `wanted` is among the values.
    ///
    /// The question every capability check asks: a peer MUST NOT use a
    /// capability it did not list in `desired-capabilities`, and a peer that
    /// requires an offered capability that is absent MUST close the
    /// connection (Part 2 §2.7.1).
    #[must_use]
    pub fn contains(&self, wanted: &str) -> bool {
        self.iter().any(|item| item == wanted)
    }

    /// Reads a `multiple` field from a decoded value.
    pub fn from_value(value: &Value<'a>, field: &'static str) -> Result<Self, DecodeError> {
        match value {
            Value::Null => Ok(Self::None),
            Value::Symbol(one) => Ok(Self::One(one)),
            Value::Array(array) => {
                let mut items = Vec::with_capacity(array.items().len());
                for item in array.items() {
                    match item {
                        Value::Symbol(text) => items.push(*text),
                        other => {
                            return Err(DecodeError::WrongType {
                                field,
                                code: other.canonical_code(),
                            });
                        }
                    }
                }
                // "A null value and a zero-length array ... MUST be treated
                // as semantically identical", so an empty array is absence.
                Ok(match items.len() {
                    0 => Self::None,
                    1 => Self::One(items[0]),
                    _ => Self::Many(items),
                })
            }
            other => Err(DecodeError::WrongType {
                field,
                code: other.canonical_code(),
            }),
        }
    }

    /// The value this `multiple` encodes to.
    ///
    /// One value is a bare `symbol` and not a one-element array, because that
    /// is the form the specification says is "always permitted" and it is two
    /// octets shorter.
    #[must_use]
    pub fn to_value(&self) -> Value<'a> {
        match self {
            Self::None => Value::Null,
            Self::One(one) => Value::Symbol(one),
            Self::Many(items) => match items.as_slice() {
                [] => Value::Null,
                [one] => Value::Symbol(one),
                many => {
                    // One constructor for every element, so it has to be wide
                    // enough for the longest of them.
                    let code = if many.iter().any(|item| item.len() > usize::from(u8::MAX)) {
                        codes::SYM32
                    } else {
                        codes::SYM8
                    };
                    Value::Array(Array::new(
                        ElementKind::Primitive(code),
                        many.iter().map(|item| Value::Symbol(item)).collect(),
                    ))
                }
            },
        }
    }
}

/// `error`, descriptor `0x1d`, `amqp:error:list` (Part 2 §2.8.14).
///
/// The one structure that travels on `close`, `end`, `detach` and inside a
/// `rejected` outcome, which is why it lives here rather than with any of
/// them.
#[derive(Clone, Debug, PartialEq)]
pub struct AmqpError<'a> {
    /// A `symbol` naming what went wrong. The specification's own values are
    /// in [`condition`]; a domain may define its own.
    pub condition: &'a str,
    /// Text for a log, never for a decision.
    pub description: Option<&'a str>,
    /// A map of whatever the sender thought would help. RabbitMQ puts `queue`
    /// and `reason` here for a rejection.
    pub info: Option<Value<'a>>,
}

impl<'a> AmqpError<'a> {
    /// The descriptor of `amqp:error:list`.
    pub const DESCRIPTOR: u64 = 0x0000_0000_0000_001d;

    /// The symbolic descriptor Part 1 §1.5 assigns alongside the numeric one.
    pub const SYMBOLIC: &'static str = "amqp:error:list";

    /// An error with a condition and nothing else.
    #[must_use]
    pub const fn new(condition: &'a str) -> Self {
        Self {
            condition,
            description: None,
            info: None,
        }
    }

    /// The same error with a description attached.
    #[must_use]
    pub const fn described(mut self, description: &'a str) -> Self {
        self.description = Some(description);
        self
    }

    /// The described value this error encodes to.
    ///
    /// `error` is a field of `close`, `end`, `detach` and `rejected` rather
    /// than a frame body of its own, so it travels as a value and not
    /// through [`crate::encode::composite`].
    #[must_use]
    pub fn to_value(&self) -> Value<'a> {
        crate::fields::described_value(
            Self::DESCRIPTOR,
            vec![
                Value::Symbol(self.condition),
                self.description.map_or(Value::Null, Value::String),
                self.info.clone().unwrap_or(Value::Null),
            ],
        )
    }

    /// Reads an error from a decoded described value.
    pub fn from_value(value: Value<'a>) -> Result<Self, DecodeError> {
        let mut fields = crate::fields::Fields::from_described(
            value,
            Self::DESCRIPTOR,
            Self::SYMBOLIC,
            "error",
        )?;
        Ok(Self {
            condition: fields.required_symbol("condition")?,
            description: fields.string("description")?,
            info: fields.map("info")?,
        })
    }
}

/// The error conditions the specification names, as the symbols they are on
/// the wire.
///
/// Constants rather than an enum, for the reason Part 2 §2.8.14 gives: the
/// `condition` field is a `symbol` and a domain may define its own values, so
/// an exhaustive Rust enum would have to carry an `Other(&str)` arm and would
/// then buy nothing over the `&str` itself. What the constants buy is that a
/// typo is a compile error at the places that matter.
pub mod condition {
    /// An internal error occurred; the operation may be retried
    /// (Part 2 §2.8.15).
    pub const INTERNAL_ERROR: &str = "amqp:internal-error";
    /// A peer attempted to work with a remote entity that does not exist.
    pub const NOT_FOUND: &str = "amqp:not-found";
    /// No access due to security settings.
    pub const UNAUTHORIZED_ACCESS: &str = "amqp:unauthorized-access";
    /// Data could not be decoded.
    pub const DECODE_ERROR: &str = "amqp:decode-error";
    /// A peer exceeded its resource allocation.
    pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";
    /// The peer tried to use a capability or operation that is not allowed in
    /// the current state.
    pub const NOT_ALLOWED: &str = "amqp:not-allowed";
    /// An invalid field was passed in a body, and the operation could not
    /// proceed.
    pub const INVALID_FIELD: &str = "amqp:invalid-field";
    /// The peer tried to use functionality that is not implemented.
    pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";
    /// The client attempted to work with a server entity to which it has no
    /// access because another client is working with it.
    pub const RESOURCE_LOCKED: &str = "amqp:resource-locked";
    /// The client made a request that was not allowed because some
    /// precondition failed.
    pub const PRECONDITION_FAILED: &str = "amqp:precondition-failed";
    /// A server entity the client is working with has been deleted.
    pub const RESOURCE_DELETED: &str = "amqp:resource-deleted";
    /// The peer sent a frame that is not permitted in the current state.
    pub const ILLEGAL_STATE: &str = "amqp:illegal-state";
    /// The peer cannot send a frame because the smallest encoding of the
    /// performative with the currently valid values would be too large to fit
    /// within a frame of the agreed maximum frame size.
    pub const FRAME_SIZE_TOO_SMALL: &str = "amqp:frame-size-too-small";

    /// An operator intervened to close the connection for some reason
    /// (Part 2 §2.8.16).
    pub const CONNECTION_FORCED: &str = "amqp:connection:forced";
    /// A valid frame header cannot be formed from the incoming byte stream.
    pub const CONNECTION_FRAMING_ERROR: &str = "amqp:connection:framing-error";
    /// The container is no longer available on the current connection; the
    /// `info` map carries `hostname`, `network-host` and `port`.
    pub const CONNECTION_REDIRECT: &str = "amqp:connection:redirect";

    /// The peer violated incoming window for the session (Part 2 §2.8.17).
    pub const SESSION_WINDOW_VIOLATION: &str = "amqp:session:window-violation";
    /// Input was received for a link that was detached with an error.
    pub const SESSION_ERRANT_LINK: &str = "amqp:session:errant-link";
    /// An attach was received using a handle that is already in use for an
    /// attached link.
    pub const SESSION_HANDLE_IN_USE: &str = "amqp:session:handle-in-use";
    /// A frame arrived carrying a handle that is not currently in use.
    pub const SESSION_UNATTACHED_HANDLE: &str = "amqp:session:unattached-handle";

    /// An operator intervened to detach for some reason (Part 2 §2.8.18).
    pub const LINK_DETACH_FORCED: &str = "amqp:link:detach-forced";
    /// The peer sent more message transfers than currently allowed on the
    /// link.
    pub const LINK_TRANSFER_LIMIT_EXCEEDED: &str = "amqp:link:transfer-limit-exceeded";
    /// The peer sent a larger message than is currently allowed on the link.
    pub const LINK_MESSAGE_SIZE_EXCEEDED: &str = "amqp:link:message-size-exceeded";
    /// The address provided cannot be resolved to a terminus at the current
    /// container; the `info` map adds `address` to the redirect fields.
    pub const LINK_REDIRECT: &str = "amqp:link:redirect";
    /// The link has been attached elsewhere, causing the existing attachment
    /// to be forcibly closed.
    pub const LINK_STOLEN: &str = "amqp:link:stolen";

    /// The specified `txn-id` does not exist (Part 4 §4.5.8).
    pub const TRANSACTION_UNKNOWN_ID: &str = "amqp:transaction:unknown-id";
    /// The transaction was rolled back for an unspecified reason.
    pub const TRANSACTION_ROLLBACK: &str = "amqp:transaction:rollback";
    /// The work represented by this transaction took too long.
    pub const TRANSACTION_TIMEOUT: &str = "amqp:transaction:timeout";
}

/// Checks a `delivery-tag` or `transaction-id` against the 32-octet
/// restriction its type carries, on the way out.
pub(crate) fn check_tag(
    tag: &[u8],
    restriction: &'static str,
    limit: usize,
) -> Result<(), EncodeError> {
    if tag.len() > limit {
        return Err(EncodeError::RestrictionViolated {
            restriction,
            value: tag.len() as u64,
            limit: limit as u64,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_maps_false_to_sender() {
        // The direction a reader gets wrong: `false` is the sender.
        assert_eq!(Role::from_bool(false), Role::Sender);
        assert_eq!(Role::from_bool(true), Role::Receiver);
        assert!(!Role::Sender.is_receiver());
        assert_eq!(Role::Sender.opposite(), Role::Receiver);
    }

    #[test]
    fn the_settle_mode_defaults_are_the_specifications() {
        assert_eq!(SenderSettleMode::default(), SenderSettleMode::Mixed);
        assert_eq!(SenderSettleMode::default().octet(), 2);
        assert_eq!(ReceiverSettleMode::default(), ReceiverSettleMode::First);
        assert_eq!(ReceiverSettleMode::default().octet(), 0);
    }

    #[test]
    fn a_settle_mode_outside_its_restriction_is_refused() {
        assert_eq!(
            SenderSettleMode::from_octet(3),
            Err(DecodeError::RestrictionViolated {
                restriction: "sender-settle-mode",
                value: 3,
                limit: 2
            })
        );
        assert_eq!(
            ReceiverSettleMode::from_octet(2),
            Err(DecodeError::RestrictionViolated {
                restriction: "receiver-settle-mode",
                value: 2,
                limit: 1
            })
        );
    }

    #[test]
    fn multiple_folds_the_three_wire_forms_into_one_meaning() {
        assert_eq!(Multiple::from_slice(&[]), Multiple::None);
        assert_eq!(Multiple::from_slice(&["a"]), Multiple::One("a"));
        assert_eq!(
            Multiple::from_slice(&["a", "b"]),
            Multiple::Many(vec!["a", "b"])
        );

        // An empty array is absence, which is the rule that makes the two
        // encodings semantically identical.
        let empty_array = Value::Array(Array::new(ElementKind::Primitive(codes::SYM8), Vec::new()));
        assert_eq!(
            Multiple::from_value(&empty_array, "offered-capabilities").unwrap(),
            Multiple::None
        );
        assert_eq!(
            Multiple::from_value(&Value::Null, "offered-capabilities").unwrap(),
            Multiple::None
        );
    }

    #[test]
    fn one_value_is_a_bare_symbol_not_a_one_element_array() {
        assert_eq!(Multiple::One("x").to_value(), Value::Symbol("x"));
        assert_eq!(
            Multiple::Many(vec!["x"]).to_value(),
            Value::Symbol("x"),
            "a one-element Many still writes the shorter form"
        );
    }

    #[test]
    fn multiple_iterates_and_answers_contains() {
        let caps = Multiple::from_slice(&["sole-connection-for-container", "ANONYMOUS-RELAY"]);
        assert_eq!(caps.len(), 2);
        assert!(!caps.is_empty());
        assert!(caps.contains("ANONYMOUS-RELAY"));
        assert!(!caps.contains("DELAYED_DELIVERY"));
        assert_eq!(
            caps.iter().collect::<Vec<_>>(),
            ["sole-connection-for-container", "ANONYMOUS-RELAY"]
        );
        assert_eq!(Multiple::None.iter().count(), 0);
        assert_eq!(Multiple::One("only").iter().collect::<Vec<_>>(), ["only"]);
    }

    #[test]
    fn a_multiple_of_non_symbols_is_the_wrong_type() {
        assert_eq!(
            Multiple::from_value(&Value::String("not a symbol"), "outcomes"),
            Err(DecodeError::WrongType {
                field: "outcomes",
                code: codes::STR8
            })
        );
    }

    #[test]
    fn a_long_symbol_widens_the_arrays_constructor() {
        let long = "x".repeat(300);
        let items = ["short", long.as_str()];
        let value = Multiple::from_slice(&items).to_value();
        let Value::Array(array) = &value else {
            panic!("expected an array");
        };
        assert_eq!(
            array.element(),
            &ElementKind::Primitive(codes::SYM32),
            "one constructor has to be wide enough for the longest element"
        );
        let bytes = crate::encode::to_vec(&value).expect("encodes");
        let (back, _) = crate::decode::value(&bytes, crate::Limits::DEFAULT).expect("decodes");
        assert_eq!(
            Multiple::from_value(&back, "offered-capabilities").unwrap(),
            Multiple::from_slice(&items)
        );
    }

    #[test]
    fn a_delivery_tag_is_bounded_at_thirty_two_octets() {
        assert!(check_tag(&[0u8; 32], "delivery-tag", MAX_DELIVERY_TAG_BYTES).is_ok());
        assert_eq!(
            check_tag(&[0u8; 33], "delivery-tag", MAX_DELIVERY_TAG_BYTES),
            Err(EncodeError::RestrictionViolated {
                restriction: "delivery-tag",
                value: 33,
                limit: 32
            })
        );
    }
}
