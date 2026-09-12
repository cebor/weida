//! Reading a composite's fields by position, with the types the
//! specification gives them.
//!
//! A composite type is a described `list` and its fields are identified by
//! *position*, not by name: `open`'s third field is `max-frame-size` because
//! it is third. Two rules follow, and both are Part 1 §1.4's:
//!
//! * a trailing null field may be omitted, so a short list and a list of
//!   nulls are the same value — [`Fields`] therefore answers `null` past the
//!   declared count rather than reporting an end;
//! * a field of the wrong type is a violation, not a value to coerce. A
//!   `max-frame-size` that arrived as a `ulong` is [`DecodeError::WrongType`]
//!   naming the field, because a codec checked against a foreign
//!   specification is the wrong place to be generous.
//!
//! Mandatory fields get their own accessors. `open.container-id` is mandatory
//! and a peer that omits it has sent something this codec cannot make into an
//! `Open`, so the accessor returns the value rather than an `Option` and
//! reports [`DecodeError::MissingMandatoryField`] naming both the composite
//! and the field.
//!
//! # Two sources, one reader
//!
//! A performative is read straight from the frame body, where the fields are
//! octets and [`crate::decode::ListCursor`] walks them without collecting
//! anything. But a composite *nested inside* a field — `close.error`,
//! `rejected.error`, `attach.source` — has already been decoded into a
//! [`Value`] by the time the outer reader reaches it. Both cases land in
//! [`Fields`]: [`Fields::new`] takes the cursor, [`Fields::from_described`]
//! takes the value, and the field accessors above them are the same code.
//! The second path moves each field out of the decoded list rather than
//! cloning it, so a nested map is not copied to be read.

use crate::decode::ListCursor;
use crate::error::DecodeError;
use crate::types::Multiple;
use crate::value::{Descriptor, Value};

/// Where a composite's fields come from.
#[derive(Clone, Debug)]
enum Source<'a> {
    /// Octets, walked one field at a time.
    Cursor(ListCursor<'a>),
    /// An already-decoded field list, consumed by moving each field out.
    Decoded { items: Vec<Value<'a>>, next: usize },
}

/// A cursor over one composite's fields.
#[derive(Clone, Debug)]
pub struct Fields<'a> {
    source: Source<'a>,
    composite: &'static str,
}

impl<'a> Fields<'a> {
    /// Reads the fields of `composite` from a list cursor over frame octets.
    ///
    /// The name is for the error messages: "attach requires name" is worth
    /// more to whoever reads the log than "field 0 missing".
    #[must_use]
    pub const fn new(cursor: ListCursor<'a>, composite: &'static str) -> Self {
        Self {
            source: Source::Cursor(cursor),
            composite,
        }
    }

    /// Reads the fields of `composite` from a decoded described value,
    /// checking the descriptor first.
    ///
    /// `code` and `symbolic` are the numeric and symbolic descriptors the
    /// specification assigns together; either is accepted, because either is
    /// legal on the wire.
    pub fn from_described(
        value: Value<'a>,
        code: u64,
        symbolic: &str,
        composite: &'static str,
    ) -> Result<Self, DecodeError> {
        let canonical = value.canonical_code();
        let Value::Described(described) = value else {
            return Err(DecodeError::WrongType {
                field: composite,
                code: canonical,
            });
        };
        let described = *described;
        if !described.descriptor.matches(code, symbolic) {
            return Err(DecodeError::UnexpectedDescriptor {
                expected: code,
                found: described.descriptor.code(),
            });
        }
        let inner = described.value.canonical_code();
        let Value::List(items) = described.value else {
            return Err(DecodeError::WrongType {
                field: composite,
                code: inner,
            });
        };
        Ok(Self {
            source: Source::Decoded { items, next: 0 },
            composite,
        })
    }

    /// The composite these fields belong to, as the specification names it.
    #[must_use]
    pub const fn composite(&self) -> &'static str {
        self.composite
    }

    /// How many fields the list declared and the reader has not reached yet.
    #[must_use]
    pub fn remaining(&self) -> u32 {
        match &self.source {
            Source::Cursor(cursor) => cursor.remaining(),
            Source::Decoded { items, next } => (items.len() - *next) as u32,
        }
    }

    /// The next field as a raw value, or `null` past the declared count.
    pub fn next_value(&mut self) -> Result<Value<'a>, DecodeError> {
        match &mut self.source {
            Source::Cursor(cursor) => cursor.next_value(),
            Source::Decoded { items, next } => {
                let Some(slot) = items.get_mut(*next) else {
                    return Ok(Value::Null);
                };
                *next += 1;
                // Moved out rather than cloned: a nested map is read, not
                // copied.
                Ok(core::mem::replace(slot, Value::Null))
            }
        }
    }

    /// The next field, whatever type it is: for `*`-typed fields such as
    /// `transfer.state`, `attach.source` and `disposition.state`, which the
    /// specification itself leaves as "any type that provides X".
    pub fn any(&mut self) -> Result<Option<Value<'a>>, DecodeError> {
        let value = self.next_value()?;
        Ok(if value.is_null() { None } else { Some(value) })
    }

    fn optional<T>(
        &mut self,
        field: &'static str,
        extract: impl FnOnce(&Value<'a>) -> Option<T>,
    ) -> Result<Option<T>, DecodeError> {
        let value = self.next_value()?;
        if value.is_null() {
            return Ok(None);
        }
        match extract(&value) {
            Some(found) => Ok(Some(found)),
            None => Err(DecodeError::WrongType {
                field,
                code: value.canonical_code(),
            }),
        }
    }

    fn required<T>(
        &mut self,
        field: &'static str,
        extract: impl FnOnce(&Value<'a>) -> Option<T>,
    ) -> Result<T, DecodeError> {
        let composite = self.composite;
        self.optional(field, extract)?
            .ok_or(DecodeError::MissingMandatoryField { composite, field })
    }

    /// An optional `symbol`.
    pub fn symbol(&mut self, field: &'static str) -> Result<Option<&'a str>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Symbol(text) => Some(*text),
            _ => None,
        })
    }

    /// A mandatory `symbol`.
    pub fn required_symbol(&mut self, field: &'static str) -> Result<&'a str, DecodeError> {
        self.required(field, |value| match value {
            Value::Symbol(text) => Some(*text),
            _ => None,
        })
    }

    /// An optional `string`.
    pub fn string(&mut self, field: &'static str) -> Result<Option<&'a str>, DecodeError> {
        self.optional(field, |value| match value {
            Value::String(text) => Some(*text),
            _ => None,
        })
    }

    /// A mandatory `string`.
    pub fn required_string(&mut self, field: &'static str) -> Result<&'a str, DecodeError> {
        self.required(field, |value| match value {
            Value::String(text) => Some(*text),
            _ => None,
        })
    }

    /// An optional `binary`.
    pub fn binary(&mut self, field: &'static str) -> Result<Option<&'a [u8]>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Binary(data) => Some(*data),
            _ => None,
        })
    }

    /// A mandatory `binary`.
    pub fn required_binary(&mut self, field: &'static str) -> Result<&'a [u8], DecodeError> {
        self.required(field, |value| match value {
            Value::Binary(data) => Some(*data),
            _ => None,
        })
    }

    /// An optional `boolean`, where absence is distinguishable from `false`.
    pub fn boolean(&mut self, field: &'static str) -> Result<Option<bool>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Boolean(flag) => Some(*flag),
            _ => None,
        })
    }

    /// A `boolean` with the default the specification gives it.
    pub fn boolean_or(&mut self, field: &'static str, default: bool) -> Result<bool, DecodeError> {
        Ok(self.boolean(field)?.unwrap_or(default))
    }

    /// An optional `ubyte`.
    pub fn ubyte(&mut self, field: &'static str) -> Result<Option<u8>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Ubyte(v) => Some(*v),
            _ => None,
        })
    }

    /// A mandatory `ubyte`.
    pub fn required_ubyte(&mut self, field: &'static str) -> Result<u8, DecodeError> {
        self.required(field, |value| match value {
            Value::Ubyte(v) => Some(*v),
            _ => None,
        })
    }

    /// An optional `ushort`.
    pub fn ushort(&mut self, field: &'static str) -> Result<Option<u16>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Ushort(v) => Some(*v),
            _ => None,
        })
    }

    /// A `ushort` with the default the specification gives it.
    pub fn ushort_or(&mut self, field: &'static str, default: u16) -> Result<u16, DecodeError> {
        Ok(self.ushort(field)?.unwrap_or(default))
    }

    /// An optional `uint` — `handle`, `transfer-number`, `sequence-no`,
    /// `delivery-number`, `seconds` and `milliseconds` are all this type.
    pub fn uint(&mut self, field: &'static str) -> Result<Option<u32>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Uint(v) => Some(*v),
            _ => None,
        })
    }

    /// A mandatory `uint`.
    pub fn required_uint(&mut self, field: &'static str) -> Result<u32, DecodeError> {
        self.required(field, |value| match value {
            Value::Uint(v) => Some(*v),
            _ => None,
        })
    }

    /// A `uint` with the default the specification gives it.
    pub fn uint_or(&mut self, field: &'static str, default: u32) -> Result<u32, DecodeError> {
        Ok(self.uint(field)?.unwrap_or(default))
    }

    /// An optional `ulong`.
    pub fn ulong(&mut self, field: &'static str) -> Result<Option<u64>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Ulong(v) => Some(*v),
            _ => None,
        })
    }

    /// An optional `timestamp`.
    pub fn timestamp(&mut self, field: &'static str) -> Result<Option<i64>, DecodeError> {
        self.optional(field, |value| match value {
            Value::Timestamp(v) => Some(*v),
            _ => None,
        })
    }

    /// A field marked `multiple`, in any of its three wire forms.
    pub fn multiple(&mut self, field: &'static str) -> Result<Multiple<'a>, DecodeError> {
        let value = self.next_value()?;
        Multiple::from_value(&value, field)
    }

    /// A field marked `multiple` and `mandatory`, which "MUST contain at
    /// least one value (i.e. for such a field both null and an array with no
    /// entries are invalid)" — Part 1 §1.4.
    pub fn required_multiple(&mut self, field: &'static str) -> Result<Multiple<'a>, DecodeError> {
        let composite = self.composite;
        let found = self.multiple(field)?;
        if found.is_empty() {
            return Err(DecodeError::MissingMandatoryField { composite, field });
        }
        Ok(found)
    }

    /// A `map` field: `fields`, `filter-set`, `node-properties`,
    /// `annotations`. Kept as a value because this codec does not interpret
    /// the keys, and Part 3 reserves every key it has not defined.
    pub fn map(&mut self, field: &'static str) -> Result<Option<Value<'a>>, DecodeError> {
        let value = self.next_value()?;
        match value {
            Value::Null => Ok(None),
            Value::Map(_) => Ok(Some(value)),
            other => Err(DecodeError::WrongType {
                field,
                code: other.canonical_code(),
            }),
        }
    }
}

/// Builds the value of a composite nested inside a field, with its trailing
/// null fields omitted.
///
/// The mirror of [`crate::encode::composite`] for the case where the
/// composite is a *field* of another one rather than a frame body:
/// `close.error` and `rejected.error` are the same `amqp:error:list`, and
/// only the first is written straight into a frame.
#[must_use]
pub fn described_value<'a>(code: u64, mut fields: Vec<Value<'a>>) -> Value<'a> {
    let keep = fields
        .iter()
        .rposition(|field| !field.is_null())
        .map_or(0, |i| i + 1);
    fields.truncate(keep);
    Value::Described(Box::new(crate::value::Described {
        descriptor: Descriptor::Code(code),
        value: Value::List(fields),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes;
    use crate::limits::Limits;
    use crate::{Described, decode, encode};

    fn fields_of(values: &[Value<'_>]) -> Vec<u8> {
        let mut out = Vec::new();
        encode::composite(&Descriptor::Code(0x10), values, &mut out).expect("encodes");
        out
    }

    fn reader(bytes: &[u8]) -> Fields<'_> {
        let composite = decode::composite(bytes, Limits::DEFAULT).expect("a composite");
        Fields::new(composite.fields, "open")
    }

    #[test]
    fn a_trimmed_trailing_field_reads_as_absent() {
        let bytes = fields_of(&[Value::String("c1")]);
        let mut fields = reader(&bytes);
        assert_eq!(fields.required_string("container-id").unwrap(), "c1");
        assert_eq!(fields.string("hostname").unwrap(), None);
        assert_eq!(fields.uint("max-frame-size").unwrap(), None);
        assert_eq!(
            fields.multiple("offered-capabilities").unwrap(),
            Multiple::None
        );
    }

    #[test]
    fn a_defaulted_field_takes_its_default_when_absent() {
        let bytes = fields_of(&[Value::String("c1")]);
        let mut fields = reader(&bytes);
        let _ = fields.next_value();
        let _ = fields.next_value();
        assert_eq!(
            fields.uint_or("max-frame-size", 4_294_967_295).unwrap(),
            4_294_967_295
        );
        assert_eq!(fields.ushort_or("channel-max", 65535).unwrap(), 65535);
        assert!(!fields.boolean_or("drain", false).unwrap());
    }

    #[test]
    fn a_mandatory_field_that_is_null_is_missing() {
        let bytes = fields_of(&[Value::Null, Value::String("h")]);
        let mut fields = reader(&bytes);
        assert_eq!(
            fields.required_string("container-id"),
            Err(DecodeError::MissingMandatoryField {
                composite: "open",
                field: "container-id"
            })
        );
    }

    #[test]
    fn a_field_of_the_wrong_type_is_refused_rather_than_coerced() {
        // `max-frame-size` is a uint; a peer sending a ulong is not offering
        // a number to be widened, it is disagreeing about the field.
        let bytes = fields_of(&[Value::String("c1"), Value::Null, Value::Ulong(4096)]);
        let mut fields = reader(&bytes);
        let _ = fields.next_value();
        let _ = fields.next_value();
        assert_eq!(
            fields.uint("max-frame-size"),
            Err(DecodeError::WrongType {
                field: "max-frame-size",
                code: codes::ULONG
            })
        );
    }

    #[test]
    fn a_mandatory_multiple_refuses_an_empty_array() {
        let bytes = fields_of(&[Multiple::None.to_value()]);
        let mut fields = Fields::new(
            decode::composite(&bytes, Limits::DEFAULT).unwrap().fields,
            "sasl-mechanisms",
        );
        assert_eq!(
            fields.required_multiple("sasl-server-mechanisms"),
            Err(DecodeError::MissingMandatoryField {
                composite: "sasl-mechanisms",
                field: "sasl-server-mechanisms"
            })
        );
    }

    #[test]
    fn a_star_typed_field_comes_back_whole() {
        let state = Value::Described(Box::new(Described {
            descriptor: Descriptor::Code(0x24),
            value: Value::List(Vec::new()),
        }));
        let bytes = fields_of(core::slice::from_ref(&state));
        let mut fields = reader(&bytes);
        assert_eq!(fields.any().unwrap(), Some(state));
        assert_eq!(fields.any().unwrap(), None);
    }

    #[test]
    fn a_nested_composite_reads_through_the_same_accessors() {
        let nested = described_value(
            0x1d,
            vec![
                Value::Symbol("amqp:decode-error"),
                Value::String("bad list header"),
                Value::Null,
            ],
        );
        let mut fields = Fields::from_described(nested, 0x1d, "amqp:error:list", "error")
            .expect("a described error");
        assert_eq!(fields.remaining(), 2, "the trailing null was omitted");
        assert_eq!(
            fields.required_symbol("condition").unwrap(),
            "amqp:decode-error"
        );
        assert_eq!(
            fields.string("description").unwrap(),
            Some("bad list header")
        );
        assert_eq!(fields.map("info").unwrap(), None);
    }

    #[test]
    fn a_nested_composite_accepts_the_symbolic_descriptor() {
        let nested = Value::Described(Box::new(Described {
            descriptor: Descriptor::Symbol("amqp:error:list"),
            value: Value::List(vec![Value::Symbol("amqp:internal-error")]),
        }));
        let mut fields = Fields::from_described(nested, 0x1d, "amqp:error:list", "error")
            .expect("a described error");
        assert_eq!(
            fields.required_symbol("condition").unwrap(),
            "amqp:internal-error"
        );
    }

    #[test]
    fn a_nested_composite_with_the_wrong_descriptor_is_refused() {
        let nested = described_value(0x24, vec![]);
        assert_eq!(
            Fields::from_described(nested, 0x1d, "amqp:error:list", "error").expect_err("refused"),
            DecodeError::UnexpectedDescriptor {
                expected: 0x1d,
                found: Some(0x24)
            }
        );
        // And something that is not described at all.
        assert_eq!(
            Fields::from_described(Value::Uint(1), 0x1d, "amqp:error:list", "error")
                .expect_err("refused"),
            DecodeError::WrongType {
                field: "error",
                code: codes::SMALLUINT
            }
        );
    }

    #[test]
    fn described_value_omits_trailing_nulls() {
        assert_eq!(
            described_value(0x24, vec![Value::Null, Value::Null]),
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Code(0x24),
                value: Value::List(Vec::new())
            }))
        );
    }
}
