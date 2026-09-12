//! A message as an application sees it, and the owned form of the
//! `NATS/1.0` header block.
//!
//! `weida-nats-codec`'s `Op` and `Headers` borrow from the buffer they were
//! decoded from, which is exactly right for a codec and exactly wrong for a
//! message that travels down a channel to another task. So one copy is made,
//! once, at the point the operation leaves the read buffer — and it is the
//! only copy on the receive path.
//!
//! # Octets, not text
//!
//! `subject`, `reply_to` and `payload` are `Vec<u8>`. The payload is opaque
//! by definition: "the protocol assigns no payload schema or content type"
//! (`docs/research/nats.md` §3). The subject and the reply subject are remote
//! input, and a client that refused a non-UTF-8 subject would refuse a
//! message a conforming server delivered — the same reason the codec keeps
//! them as octets. [`Message::subject_str`] and [`Message::reply_to_str`] are
//! there for the ordinary case where they are text.
//!
//! Header names and values are `String`, because ADR-4's grammar is
//! HTTP-like text and a client compares them.

use weida_nats_codec::Headers;

/// The `NATS/1.0` status a server sends when a request found no responder.
///
/// "A request with no responders can receive a fast no-responder status when
/// the client enables `no_responders` and headers"
/// (`docs/research/nats.md` §4).
pub const NO_RESPONDERS: u16 = 503;

/// One delivered message.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// "Subject name this message was received on" — the literal subject the
    /// publisher used, not the pattern that matched it.
    pub subject: Vec<u8>,
    /// The `sid` of the subscription this copy was delivered to.
    ///
    /// Kept because it is the one field that says *which* subscription
    /// received it, and two subscriptions on this connection can match the
    /// same publication and each get their own copy.
    pub sid: u64,
    /// "The subject on which the publisher is listening for responses", where
    /// there was one. This is what a responder publishes its answer to, and
    /// it is the whole of request-reply's correlation.
    pub reply_to: Option<Vec<u8>>,
    /// The header block, where the message arrived as `HMSG`.
    pub headers: Option<OwnedHeaders>,
    /// The payload, exactly as long as the control line declared. May be
    /// empty, and may hold `CRLF`.
    pub payload: Vec<u8>,
}

impl Message {
    /// The subject as text, where it is UTF-8.
    #[must_use]
    pub fn subject_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.subject).ok()
    }

    /// The reply subject as text, where there is one and it is UTF-8.
    #[must_use]
    pub fn reply_to_str(&self) -> Option<&str> {
        self.reply_to
            .as_deref()
            .and_then(|r| std::str::from_utf8(r).ok())
    }

    /// The `NATS/1.0` status, where the block carried one.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        self.headers.as_ref().and_then(|headers| headers.status)
    }

    /// Whether this is the no-responder answer to a request.
    ///
    /// A `503` with no payload, which is what the server sends the instant it
    /// finds no interest in a request's subject. Distinguished from a
    /// responder that happened to answer with a 503-shaped block by nothing
    /// at all — the protocol offers no distinction, and none is invented
    /// here.
    #[must_use]
    pub fn is_no_responders(&self) -> bool {
        self.status() == Some(NO_RESPONDERS)
    }
}

/// A `NATS/1.0` header block that owns its names and values.
///
/// A vector of pairs rather than a map, for the two reasons the codec gives:
/// names may repeat with different values ("including supporting multi-value
/// headers") and case is preserved between publisher and receiver. A map
/// would have to fold one of the two away.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OwnedHeaders {
    /// The three-digit status after `NATS/1.0`, where the version line
    /// carried one.
    pub status: Option<u16>,
    /// The text after the status, where there was one.
    pub description: Option<String>,
    entries: Vec<(String, String)>,
}

impl OwnedHeaders {
    /// An empty block.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            status: None,
            description: None,
            entries: Vec::new(),
        }
    }

    /// Append an entry, keeping its case and its position.
    pub fn push(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.entries.push((name.into(), value.into()));
    }

    /// Every entry, in order, duplicates kept.
    #[must_use]
    pub fn entries(&self) -> &[(String, String)] {
        &self.entries
    }

    /// Whether the block holds no entries. A block with a status and no
    /// entries is the ordinary shape of a no-responder message.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many entries the block holds, duplicates counted separately.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The first value stored under exactly this name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(stored, _)| stored == name)
            .map(|(_, value)| value.as_str())
    }

    /// The first value whose name matches ignoring ASCII case.
    ///
    /// The protocol promises case is *preserved*, not that it is significant,
    /// so a client reading a block another language's client wrote may well
    /// have to ignore it. What must not happen is the storage folding it.
    #[must_use]
    pub fn get_ignore_ascii_case(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(stored, _)| stored.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Copies a decoded block out of the read buffer it borrows from.
    #[must_use]
    pub fn from_borrowed(headers: &Headers<'_>) -> Self {
        Self {
            status: headers.status,
            description: headers.description.map(str::to_owned),
            entries: headers
                .entries()
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    /// The borrowed form, for putting this block back on the wire.
    #[must_use]
    pub fn as_borrowed(&self) -> Headers<'_> {
        let mut headers = Headers::new();
        headers.status = self.status;
        headers.description = self.description.as_deref();
        for (name, value) in &self.entries {
            headers.push(name.as_str(), value.as_str());
        }
        headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_nats_codec::Limits;

    /// The round trip is what makes `as_borrowed` trustworthy: a block copied
    /// out of a decode and written back must produce the octets it came from,
    /// repeated names and case included.
    #[test]
    fn a_block_survives_the_copy_out_and_back() {
        let wire = b"NATS/1.0\r\nFoodGroup: vegetable\r\nfoodgroup: fruit\r\n\r\n";
        let decoded = Headers::decode(wire, Limits::DEFAULT).expect("a block");
        let owned = OwnedHeaders::from_borrowed(&decoded);
        assert_eq!(owned.len(), 2, "a repeated name is two entries");
        assert_eq!(owned.get("FoodGroup"), Some("vegetable"));
        assert_eq!(
            owned.get("foodgroup"),
            Some("fruit"),
            "case is significant to `get`, because it is preserved on the wire"
        );
        assert_eq!(owned.get_ignore_ascii_case("FOODGROUP"), Some("vegetable"));

        let mut out = Vec::new();
        owned.as_borrowed().encode(&mut out).expect("writable");
        assert_eq!(out, wire);
    }

    /// The no-responder answer is a status and nothing else, and that is what
    /// `is_no_responders` keys on.
    #[test]
    fn the_no_responder_answer_is_a_status_with_no_entries() {
        let decoded = Headers::decode(b"NATS/1.0 503\r\n\r\n", Limits::DEFAULT).expect("a block");
        let message = Message {
            headers: Some(OwnedHeaders::from_borrowed(&decoded)),
            ..Message::default()
        };
        assert_eq!(message.status(), Some(NO_RESPONDERS));
        assert!(message.is_no_responders());
        assert!(message.headers.as_ref().expect("headers").is_empty());
        assert!(!Message::default().is_no_responders());
    }
}
