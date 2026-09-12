//! A decoded AMQP value that outlives the frame it arrived in.
//!
//! The codec borrows: a `Value` points into the buffer it was decoded from,
//! which is exactly right for a decoder and exactly wrong for a client. A
//! `source.filter` is consulted for the life of the link, a
//! `source.default-outcome` decides the fate of every delivery whose receiver
//! state is unknown, and an `attach.unsettled` map has to survive until the
//! deliveries in it are settled — none of which fits inside one read.
//!
//! The honest options are an owned mirror of the whole Part 1 type system, or
//! keeping the **canonical encoding** and decoding on demand. This is the
//! second, for three reasons:
//!
//! 1. the bytes are what has to go back on the wire anyway — an answering
//!    `attach` repeats the terminus it was given — so storing them costs
//!    nothing a re-encode would not;
//! 2. the canonical encoding is a normal form, so comparing two
//!    [`OwnedValue`]s by their octets *is* comparing them by value, which a
//!    mirror would have had to prove;
//! 3. an owned mirror of twenty-four primitive types, three compound types
//!    and the described type is several hundred lines that exist only to
//!    avoid one allocation per `attach`.
//!
//! The cost is a decode per read. These fields are read when a link attaches
//! and when a delivery resumes, not per message, so the trade is on the right
//! side.

use std::sync::Arc;

use weida_amqp_codec::{EncodeError, Limits, Value, decode, encode};

/// A value held as its canonical encoding, decodable on demand.
#[derive(Clone, Debug)]
pub struct OwnedValue {
    bytes: Arc<[u8]>,
}

impl OwnedValue {
    /// Keeps `value` by encoding it canonically.
    pub fn new(value: &Value<'_>) -> Result<Self, EncodeError> {
        Ok(Self {
            bytes: encode::to_vec(value)?.into(),
        })
    }

    /// Keeps octets that are already a canonical encoding, validating them.
    ///
    /// For the case where the encoding is what arrived and re-encoding it
    /// would be a round trip for nothing. Validated on the way in so that
    /// [`OwnedValue::value`] can be infallible: an `OwnedValue` that exists
    /// holds a value that decodes, and that is the type's whole invariant.
    pub fn try_from_bytes(
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<Self, weida_amqp_codec::DecodeError> {
        let bytes = bytes.into();
        decode::value(&bytes, Limits::BODY)?;
        Ok(Self { bytes })
    }

    /// The value, borrowed from this `OwnedValue`.
    ///
    /// Infallible by construction: both constructors decode or encode their
    /// input before the value exists, so the octets here always decode.
    #[must_use]
    pub fn value(&self) -> Value<'_> {
        decode::value(&self.bytes, Limits::BODY)
            .expect("an OwnedValue holds octets that decoded once already")
            .0
    }

    /// The canonical encoding, which is what goes back on the wire.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl PartialEq for OwnedValue {
    /// Octet equality, which for a canonical encoding is value equality.
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for OwnedValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_survives_the_buffer_it_was_decoded_from() {
        let owned = {
            let bytes = encode::to_vec(&Value::Map(vec![(
                Value::Symbol("apache.org:selector-filter:string"),
                Value::String("amqp.correlation_id = 'abc'"),
            )]))
            .expect("encodes");
            let (value, _) = decode::value(&bytes, Limits::BODY).expect("decodes");
            OwnedValue::new(&value).expect("keeps")
            // `bytes` goes out of scope here; a borrowed `Value` could not.
        };
        let Value::Map(entries) = owned.value() else {
            panic!("expected a map");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, Value::String("amqp.correlation_id = 'abc'"));
    }

    #[test]
    fn equality_is_by_value_because_the_encoding_is_canonical() {
        // The same value written two different ways on the wire becomes one
        // `OwnedValue`, which is what makes octet comparison sound.
        let narrow = OwnedValue::new(&Value::Uint(7)).unwrap();
        let from_wide = {
            let wide = [0x70u8, 0x00, 0x00, 0x00, 0x07];
            let (value, _) = decode::value(&wide, Limits::BODY).expect("decodes");
            OwnedValue::new(&value).unwrap()
        };
        assert_eq!(narrow, from_wide);
        assert_eq!(narrow.as_bytes(), [0x52, 0x07]);
        assert_ne!(narrow, OwnedValue::new(&Value::Uint(8)).unwrap());
    }

    #[test]
    fn cloning_shares_the_octets() {
        let owned = OwnedValue::new(&Value::Binary(&[0xab; 64])).unwrap();
        let clone = owned.clone();
        assert_eq!(owned, clone);
        assert!(
            std::ptr::eq(owned.as_bytes(), clone.as_bytes()),
            "an Arc clone shares rather than copies"
        );
    }
}
