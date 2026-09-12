//! Payload lists: the four packets whose payload is a repetition of one shape.
//!
//! SUBSCRIBE carries "a list of Topic Filters and Subscription Options"
//! (3.8.3), UNSUBSCRIBE "a list of the Topic Filters" (3.10.3), and SUBACK and
//! UNSUBACK "a list of Reason Codes" one per filter, in the order the request
//! listed them (3.9.3, 3.11.3) [mqtt5 §3]. All four are a repetition until the
//! packet ends: there is no count field, so the Remaining Length *is* the
//! count.
//!
//! [`PayloadList`] holds one of these two ways, exactly as
//! [`crate::property::Properties`] holds its repeatable properties: the wire
//! bytes when it was decoded, re-walked on demand and allocating nothing
//! however many entries a peer sent, or a slice the caller owns when it was
//! built. A decoded SUBSCRIBE with a thousand filters therefore costs the same
//! as one with none.
//!
//! Validation happens once, in [`PayloadList::decode`], which walks the whole
//! payload and reports the first entry that does not parse. [`PayloadList::iter`]
//! afterwards walks bytes already proved good, so it is infallible and simply
//! stops — which is what keeps it an `Iterator` rather than a fallible stream.

use crate::data::Reader;
use crate::error::{DecodeError, EncodeError};
use crate::types::PacketType;

/// One entry of a packet's payload list.
pub trait PayloadItem<'a>: Copy + Sized {
    /// Reads one entry from the front of `reader`.
    ///
    /// # Errors
    ///
    /// Whatever the entry's own grammar reports. Inside a payload list a
    /// short read is the packet contradicting its Remaining Length, so
    /// callers map [`DecodeError::Incomplete`] before it escapes.
    fn read(reader: &mut Reader<'a>) -> Result<Self, DecodeError>;

    /// Bytes this entry occupies.
    ///
    /// # Errors
    ///
    /// [`EncodeError::FieldTooLong`] where the entry holds a string above
    /// 65,535 bytes.
    fn item_len(&self) -> Result<u32, EncodeError>;

    /// Appends this entry.
    ///
    /// # Errors
    ///
    /// As [`PayloadItem::item_len`].
    fn put(&self, out: &mut Vec<u8>) -> Result<(), EncodeError>;
}

/// A packet payload that is a list of `T`.
#[derive(Clone, Copy, Debug)]
pub struct PayloadList<'a, T> {
    /// The payload bytes a decode borrowed. Empty for a list a caller built.
    wire: &'a [u8],
    /// Entries a caller supplied, in order.
    items: &'a [T],
}

impl<T> Default for PayloadList<'_, T> {
    fn default() -> Self {
        PayloadList {
            wire: &[],
            items: &[],
        }
    }
}

impl<'a, T: PayloadItem<'a>> PayloadList<'a, T> {
    /// A list of `items`, in the order they will be encoded.
    ///
    /// Order is not cosmetic here: SUBACK's and UNSUBACK's codes are matched
    /// to filters by position (3.9.3, 3.11.3) [mqtt5 §3].
    #[must_use]
    pub const fn new(items: &'a [T]) -> PayloadList<'a, T> {
        PayloadList { wire: &[], items }
    }

    /// Whether the list has no entries. A Protocol Error for all four packets
    /// that use one ([MQTT-3.8.3-2], [MQTT-3.10.3-2]) [mqtt5 §3].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.wire.is_empty() && self.items.is_empty()
    }

    /// The entries, in wire order.
    #[must_use]
    pub fn iter(&self) -> PayloadIter<'a, T> {
        if self.wire.is_empty() {
            PayloadIter(PayloadIterInner::Slice(self.items.iter()))
        } else {
            PayloadIter(PayloadIterInner::Wire(Reader::new(self.wire)))
        }
    }

    /// Decodes the rest of `reader` as a list, validating every entry.
    ///
    /// The packet's Remaining Length is the only terminator, so this consumes
    /// everything that is left.
    ///
    /// # Errors
    ///
    /// [`DecodeError::EmptyPayload`] when there are no entries,
    /// [`DecodeError::PacketLengthMismatch`] when an entry runs past the
    /// packet's declared end, and whatever the entry grammar reports.
    pub fn decode(
        reader: &mut Reader<'a>,
        packet_type: PacketType,
    ) -> Result<PayloadList<'a, T>, DecodeError> {
        let wire = reader.rest();
        if wire.is_empty() {
            return Err(DecodeError::EmptyPayload { packet_type });
        }

        let mut inner = Reader::new(wire);
        while !inner.is_empty() {
            T::read(&mut inner).map_err(|error| match error {
                DecodeError::Incomplete => DecodeError::PacketLengthMismatch,
                other => other,
            })?;
        }

        Ok(PayloadList { wire, items: &[] })
    }

    /// Bytes the list occupies.
    ///
    /// # Errors
    ///
    /// As [`PayloadItem::item_len`].
    pub fn body_len(&self) -> Result<u32, EncodeError> {
        let mut len = 0u32;
        for item in self.iter() {
            len += item.item_len()?;
        }
        Ok(len)
    }

    /// Appends the list.
    ///
    /// # Errors
    ///
    /// As [`PayloadItem::item_len`].
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        for item in self.iter() {
            item.put(out)?;
        }
        Ok(())
    }
}

impl<'a, T: PayloadItem<'a> + PartialEq> PartialEq for PayloadList<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a, T: PayloadItem<'a> + Eq> Eq for PayloadList<'a, T> {}

enum PayloadIterInner<'a, T> {
    Wire(Reader<'a>),
    Slice(core::slice::Iter<'a, T>),
}

/// The entries of a [`PayloadList`], in wire order.
pub struct PayloadIter<'a, T>(PayloadIterInner<'a, T>);

impl<'a, T: PayloadItem<'a>> Iterator for PayloadIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match &mut self.0 {
            PayloadIterInner::Slice(iter) => iter.next().copied(),
            // The bytes were validated by `decode`, so a failure here can only
            // be the end of the list: stop rather than panic, which keeps the
            // iterator total.
            PayloadIterInner::Wire(reader) => {
                if reader.is_empty() {
                    None
                } else {
                    T::read(reader).ok()
                }
            }
        }
    }
}
