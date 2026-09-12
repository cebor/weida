//! Delivery states: one non-terminal, four terminal.
//!
//! Part 3 §3.4 defines five states, and the distinction between them is the
//! whole of AMQP's acknowledgement model. Four are *outcomes* — terminal, and
//! once a sender has published one no later frame from the sender can change
//! it (Part 2 §2.7.5). The fifth, `received`, exists only to describe how far
//! a partially transferred body got.
//!
//! ```text
//! received 0x23   accepted 0x24   rejected 0x25   released 0x26   modified 0x27
//! ```
//!
//! # What each one tells the application
//!
//! Not "a frame arrived" but what the peer has committed to:
//!
//! * `accepted` — the receiver processed the message and expects the sender
//!   to retire it at the source. It does **not** mean "on disk": durability
//!   is asserted by `header.durable` on the way in and enforced by the
//!   target's refusal to accept otherwise. It does not increment
//!   `header.delivery-count`.
//! * `rejected` — invalid and unprocessable, and the message will not be
//!   redelivered by this node. It *does* increment `delivery-count`, which is
//!   what a redelivery limit counts.
//! * `released` — not acted upon, available again, unchanged.
//!   `delivery-count` MUST NOT be incremented, so a released message is
//!   indistinguishable from one never delivered.
//! * `modified` — available again *with a note*. `delivery-failed=true`
//!   increments `delivery-count`; `undeliverable-here=true` means this link
//!   endpoint MUST NOT see it again; the annotations are merged into the
//!   message's own, values in the outcome replacing existing keys.
//!
//! The three booleans of `modified` have no defaults in the specification, so
//! they are `Option<bool>` here: "unset" and "false" are different statements
//! and only the first leaves the node's own policy in charge.

use crate::error::{DecodeError, EncodeError};
use crate::fields::{Fields, described_value};
use crate::types::AmqpError;
use crate::value::{Descriptor, Value};

/// The five delivery states, numeric descriptor and symbolic descriptor
/// together, in the order Part 3 §3.4 defines them.
pub const DESCRIPTORS: [(u64, &str); 5] = [
    (0x0000_0000_0000_0023, "amqp:received:list"),
    (0x0000_0000_0000_0024, "amqp:accepted:list"),
    (0x0000_0000_0000_0025, "amqp:rejected:list"),
    (0x0000_0000_0000_0026, "amqp:released:list"),
    (0x0000_0000_0000_0027, "amqp:modified:list"),
];

/// A delivery state, as it appears in `transfer.state`, `disposition.state`
/// and an `attach.unsettled` map.
#[derive(Clone, Debug, PartialEq)]
pub enum DeliveryState<'a> {
    /// `received`, `0x23`. Non-terminal: how much of the body is in hand.
    ///
    /// `Received { section_number: 0, section_offset: 0 }` means no message
    /// data at all was transferred, and `Received(X, N)` for a fully received
    /// section X of N octets is equivalent to `Received(X + 1, 0)` — the
    /// specification says so explicitly, which is why this type does not try
    /// to normalize between them.
    ///
    /// The sender MUST NOT send it except on the first `transfer` of a
    /// resumed delivery.
    Received {
        /// The first section for which data can be resent, or for which not
        /// all data has been received. Section 0 is the first.
        section_number: u32,
        /// The first octet within that section, counting from 0.
        section_offset: u64,
    },
    /// `accepted`, `0x24`. Successfully processed.
    Accepted,
    /// `rejected`, `0x25`. Invalid and unprocessable, with optional
    /// diagnostics.
    Rejected {
        /// Why. Optional in the specification, and a broker that omits it
        /// leaves the application with nothing to log.
        error: Option<AmqpError<'a>>,
    },
    /// `released`, `0x26`. Not acted upon, available again, unchanged.
    Released,
    /// `modified`, `0x27`. Available again, with recorded changes.
    Modified {
        /// `true` increments the message's `delivery-count`.
        delivery_failed: Option<bool>,
        /// `true` means the message MUST NOT be redelivered to the modifying
        /// link endpoint.
        undeliverable_here: Option<bool>,
        /// A `fields` map merged into the message's `message-annotations`,
        /// values here replacing existing keys.
        message_annotations: Option<Value<'a>>,
    },
}

impl<'a> DeliveryState<'a> {
    /// Whether this is an *outcome*: a terminal state, which no later frame
    /// from the sender may change.
    #[must_use]
    pub const fn is_outcome(&self) -> bool {
        !matches!(self, Self::Received { .. })
    }

    /// Whether applying this outcome increments the message's
    /// `header.delivery-count` (Part 3 §3.4.2-3.4.5).
    ///
    /// `rejected` always; `modified` when `delivery-failed` is true;
    /// `accepted` and `released` never. This is the rule a redelivery limit
    /// is built on, and getting it backwards makes a poison message loop
    /// forever.
    #[must_use]
    pub const fn increments_delivery_count(&self) -> bool {
        matches!(
            self,
            Self::Rejected { .. }
                | Self::Modified {
                    delivery_failed: Some(true),
                    ..
                }
        )
    }

    /// The numeric descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> u64 {
        match self {
            Self::Received { .. } => 0x23,
            Self::Accepted => 0x24,
            Self::Rejected { .. } => 0x25,
            Self::Released => 0x26,
            Self::Modified { .. } => 0x27,
        }
    }

    /// The state's name, for a log line.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Received { .. } => "received",
            Self::Accepted => "accepted",
            Self::Rejected { .. } => "rejected",
            Self::Released => "released",
            Self::Modified { .. } => "modified",
        }
    }

    /// The described value this state encodes to.
    ///
    /// A delivery state is a *field* — `transfer.state`,
    /// `disposition.state`, a value in `attach.unsettled` — so it travels as
    /// a value and not as a frame body of its own.
    #[must_use]
    pub fn to_value(&self) -> Value<'a> {
        match self {
            Self::Received {
                section_number,
                section_offset,
            } => described_value(
                0x23,
                vec![Value::Uint(*section_number), Value::Ulong(*section_offset)],
            ),
            Self::Accepted => described_value(0x24, Vec::new()),
            Self::Rejected { error } => described_value(
                0x25,
                vec![error.as_ref().map_or(Value::Null, AmqpError::to_value)],
            ),
            Self::Released => described_value(0x26, Vec::new()),
            Self::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => described_value(
                0x27,
                vec![
                    delivery_failed.map_or(Value::Null, Value::Boolean),
                    undeliverable_here.map_or(Value::Null, Value::Boolean),
                    message_annotations.clone().unwrap_or(Value::Null),
                ],
            ),
        }
    }

    /// Reads a delivery state from a decoded described value.
    ///
    /// ```
    /// use weida_amqp_codec::state::DeliveryState;
    ///
    /// let accepted = DeliveryState::Accepted;
    /// let value = accepted.to_value();
    /// assert_eq!(DeliveryState::from_value(value).unwrap(), accepted);
    /// ```
    pub fn from_value(value: Value<'a>) -> Result<Self, DecodeError> {
        let descriptor = match &value {
            Value::Described(described) => described.descriptor,
            other => {
                return Err(DecodeError::WrongType {
                    field: "delivery-state",
                    code: other.canonical_code(),
                });
            }
        };
        let code = resolve(&descriptor).ok_or(DecodeError::UnknownComposite {
            kind: "delivery state",
            descriptor: descriptor.code(),
        })?;
        let symbolic = DESCRIPTORS
            .iter()
            .find(|(found, _)| *found == code)
            .map_or("", |(_, symbolic)| *symbolic);
        let mut fields = Fields::from_described(value, code, symbolic, name_of(code))?;
        Ok(match code {
            0x23 => Self::Received {
                section_number: fields.required_uint("section-number")?,
                section_offset: fields.ulong("section-offset")?.ok_or(
                    DecodeError::MissingMandatoryField {
                        composite: "received",
                        field: "section-offset",
                    },
                )?,
            },
            0x24 => Self::Accepted,
            0x25 => Self::Rejected {
                error: match fields.any()? {
                    Some(value) => Some(AmqpError::from_value(value)?),
                    None => None,
                },
            },
            0x26 => Self::Released,
            0x27 => Self::Modified {
                delivery_failed: fields.boolean("delivery-failed")?,
                undeliverable_here: fields.boolean("undeliverable-here")?,
                message_annotations: fields.map("message-annotations")?,
            },
            other => {
                return Err(DecodeError::UnknownComposite {
                    kind: "delivery state",
                    descriptor: Some(other),
                });
            }
        })
    }

    /// Encodes this state straight into `out`, for a caller building a
    /// performative field by hand.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        crate::encode::value(&self.to_value(), out)
    }
}

fn name_of(code: u64) -> &'static str {
    match code {
        0x23 => "received",
        0x24 => "accepted",
        0x25 => "rejected",
        0x26 => "released",
        0x27 => "modified",
        _ => "delivery-state",
    }
}

fn resolve(descriptor: &Descriptor<'_>) -> Option<u64> {
    match descriptor {
        Descriptor::Code(code) => Some(*code),
        Descriptor::Symbol(name) => DESCRIPTORS
            .iter()
            .find(|(_, symbolic)| symbolic == name)
            .map(|(code, _)| *code),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::condition;

    fn round_trip(state: DeliveryState<'_>) {
        let value = state.to_value();
        assert_eq!(
            DeliveryState::from_value(value).expect("decodes"),
            state,
            "{}",
            state.name()
        );
    }

    #[test]
    fn every_state_round_trips() {
        round_trip(DeliveryState::Received {
            section_number: 0,
            section_offset: 0,
        });
        round_trip(DeliveryState::Received {
            section_number: 3,
            section_offset: 4096,
        });
        round_trip(DeliveryState::Accepted);
        round_trip(DeliveryState::Rejected { error: None });
        round_trip(DeliveryState::Rejected {
            error: Some(
                AmqpError::new(condition::PRECONDITION_FAILED)
                    .described("the target cannot honour durable=true"),
            ),
        });
        round_trip(DeliveryState::Released);
        round_trip(DeliveryState::Modified {
            delivery_failed: None,
            undeliverable_here: None,
            message_annotations: None,
        });
        round_trip(DeliveryState::Modified {
            delivery_failed: Some(true),
            undeliverable_here: Some(false),
            message_annotations: Some(Value::Map(vec![(
                Value::Symbol("x-opt-reason"),
                Value::String("poison"),
            )])),
        });
    }

    #[test]
    fn four_of_the_five_are_outcomes() {
        assert!(
            !DeliveryState::Received {
                section_number: 0,
                section_offset: 0
            }
            .is_outcome()
        );
        for state in [
            DeliveryState::Accepted,
            DeliveryState::Rejected { error: None },
            DeliveryState::Released,
            DeliveryState::Modified {
                delivery_failed: None,
                undeliverable_here: None,
                message_annotations: None,
            },
        ] {
            assert!(state.is_outcome(), "{}", state.name());
        }
    }

    #[test]
    fn only_rejected_and_a_failed_modified_increment_the_delivery_count() {
        // The rule a redelivery limit counts on, and the one a reader is
        // most likely to get backwards: `released` does not increment, so a
        // released message is indistinguishable from one never delivered.
        assert!(DeliveryState::Rejected { error: None }.increments_delivery_count());
        assert!(
            DeliveryState::Modified {
                delivery_failed: Some(true),
                undeliverable_here: None,
                message_annotations: None,
            }
            .increments_delivery_count()
        );
        assert!(
            !DeliveryState::Modified {
                delivery_failed: Some(false),
                undeliverable_here: Some(true),
                message_annotations: None,
            }
            .increments_delivery_count(),
            "undeliverable-here alone does not count as an attempt"
        );
        assert!(
            !DeliveryState::Modified {
                delivery_failed: None,
                undeliverable_here: None,
                message_annotations: None,
            }
            .increments_delivery_count(),
            "unset is not true"
        );
        assert!(!DeliveryState::Accepted.increments_delivery_count());
        assert!(!DeliveryState::Released.increments_delivery_count());
    }

    #[test]
    fn modified_keeps_unset_and_false_apart() {
        // Unset leaves the node's policy in charge; false is a statement.
        // The two encode differently and must decode differently.
        let unset = DeliveryState::Modified {
            delivery_failed: None,
            undeliverable_here: None,
            message_annotations: None,
        };
        let explicit = DeliveryState::Modified {
            delivery_failed: Some(false),
            undeliverable_here: Some(false),
            message_annotations: None,
        };
        let unset_bytes = crate::encode::to_vec(&unset.to_value()).expect("encodes");
        let explicit_bytes = crate::encode::to_vec(&explicit.to_value()).expect("encodes");
        assert_ne!(unset_bytes, explicit_bytes);
        assert_eq!(unset_bytes, [0x00, 0x53, 0x27, 0x45], "every field omitted");
        assert_ne!(unset, explicit);
    }

    #[test]
    fn accepted_is_four_octets() {
        // The most common frame field in AMQP, and the shortest it can be:
        // described, smallulong, 0x24, list0.
        assert_eq!(
            crate::encode::to_vec(&DeliveryState::Accepted.to_value()).expect("encodes"),
            [0x00, 0x53, 0x24, 0x45]
        );
    }

    #[test]
    fn received_requires_both_of_its_fields() {
        let partial = described_value(0x23, vec![Value::Uint(1)]);
        assert_eq!(
            DeliveryState::from_value(partial),
            Err(DecodeError::MissingMandatoryField {
                composite: "received",
                field: "section-offset"
            })
        );
    }

    #[test]
    fn a_sixth_delivery_state_is_refused() {
        // Part 4 adds `declared` 0x33 and `transactional-state` 0x34; this
        // crate implements Part 3's five and refuses the rest rather than
        // guessing, which is what makes "transactions are absent" checkable.
        let declared = described_value(0x33, vec![Value::Binary(b"txn")]);
        assert_eq!(
            DeliveryState::from_value(declared),
            Err(DecodeError::UnknownComposite {
                kind: "delivery state",
                descriptor: Some(0x33)
            })
        );
    }

    #[test]
    fn a_symbolic_descriptor_names_the_same_state() {
        let value = Value::Described(Box::new(crate::Described {
            descriptor: Descriptor::Symbol("amqp:released:list"),
            value: Value::List(Vec::new()),
        }));
        assert_eq!(
            DeliveryState::from_value(value).unwrap(),
            DeliveryState::Released
        );
    }

    #[test]
    fn the_descriptor_table_is_the_five_in_order() {
        let codes: Vec<u64> = DESCRIPTORS.iter().map(|(code, _)| *code).collect();
        assert_eq!(codes, (0x23..=0x27).collect::<Vec<u64>>());
    }
}
