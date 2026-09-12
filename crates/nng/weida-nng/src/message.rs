//! The message: a protocol header and an application body, in separate
//! storage, delivered whole or not at all.
//!
//! "An `nng_msg` has separate application body and protocol header storage;
//! application data is opaque to most protocols" (`docs/research/nanomsg-nng.md`
//! §2), and "NNG delivers a message wholly or not at all; it does not expose
//! partial-message delivery or streaming bodies" (§3). Both sentences are
//! this type: two `Vec<u8>`, and no API anywhere that hands out half of one.
//!
//! **Why two buffers rather than one with an offset.** Every protocol that
//! has a header builds it on one side and consumes it on the other: a REQ
//! pushes a request ID, a device pushes its own peer ID in front of that, a
//! PAIR v1 increments a hop count, a raw BUS reads the pipe a message came
//! in on. All of those edit the header while the body stays untouched and
//! unexamined — "application data is opaque to most protocols" — so the
//! split is where the protocols already put it, and a body handed to an
//! application never has to be re-sliced past a header it does not know
//! about.
//!
//! **On the wire there is no split.** A message is a big-endian 64-bit octet
//! count followed by exactly that many octets: header then body, with no
//! type tag, no flags and no continuation bit (§3). [`Message::encode`]
//! writes that, and [`decode`] reads it — refusing an over-large declaration
//! from the size field alone, before a single body octet is allocated, which
//! is the only place `NNG_OPT_RECVMAXSZ` can be enforced at all.

use weida_sp::message;

use crate::error::{Error, Result};

/// Largest message this library accepts from a peer by default, in bytes.
///
/// **This default deliberately differs from NNG's**
/// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item
/// 5). `NNG_OPT_RECVMAXSZ` is "unlimited at zero" (§11) and SP's grammar
/// permits a declared body of 2^64-1 octets (§3), so the two together are an
/// invitation to allocate whatever a stranger asks for, and
/// `docs/INVARIANTS.md` forbids exactly that. One mebibyte is the number
/// NNG itself shipped as its default before it was relaxed, so a program
/// ported from NNG meets a familiar number rather than a novel one.
///
/// It is settable, including to [`RECV_MAX_SIZE_UNLIMITED`], and it is never
/// silent: an over-large message is `NNG_EMSGSIZE` with both numbers in the
/// message.
pub const DEFAULT_RECV_MAX_SIZE: u64 = 1024 * 1024;

/// The value NNG gives `NNG_OPT_RECVMAXSZ` to mean "no limit" (§11).
///
/// Available, because a program ported from NNG may depend on it and a
/// library that refused it would be lying about its parity. Setting it opts
/// out of the only inbound size defence SP has: from then on a peer chooses
/// the allocation.
pub const RECV_MAX_SIZE_UNLIMITED: u64 = 0;

/// One SP message: the protocol header the protocols write and read, and the
/// application body they do not look at.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    header: Vec<u8>,
    body: Vec<u8>,
}

impl Message {
    /// An empty message: no header, no body.
    ///
    /// An empty message is a real message in SP — "an empty message is the
    /// eight zero octets and nothing after them" (§3) — and REQ/REP use one
    /// routinely.
    pub const fn new() -> Message {
        Message {
            header: Vec::new(),
            body: Vec::new(),
        }
    }

    /// A message carrying `body` and no protocol header.
    pub fn from_body(body: impl Into<Vec<u8>>) -> Message {
        Message {
            header: Vec::new(),
            body: body.into(),
        }
    }

    /// A message with both halves given.
    pub fn from_parts(header: Vec<u8>, body: Vec<u8>) -> Message {
        Message { header, body }
    }

    /// The protocol header: the tag stack, the hop count, the pipe ID —
    /// whatever this protocol puts in front of the body, or nothing.
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// The application body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The application body, for a caller building one in place.
    pub fn body_mut(&mut self) -> &mut Vec<u8> {
        &mut self.body
    }

    /// The protocol header, for a protocol writing its own.
    pub fn header_mut(&mut self) -> &mut Vec<u8> {
        &mut self.header
    }

    /// Body octets. `nng_msg_len()`, which counts the body and not the
    /// header.
    pub fn len(&self) -> usize {
        self.body.len()
    }

    /// Whether the body is empty. The header may not be.
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    /// Octets this message occupies on the wire, size field excluded.
    pub fn wire_len(&self) -> usize {
        self.header.len() + self.body.len()
    }

    /// The body alone, consuming the message.
    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    /// Both halves, consuming the message.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.header, self.body)
    }

    /// Appends a 32-bit word to the end of the header.
    /// `nng_msg_header_append_u32()`.
    pub fn append_header_u32(&mut self, word: u32) {
        self.header.extend_from_slice(&word.to_be_bytes());
    }

    /// Inserts a 32-bit word at the front of the header.
    /// `nng_msg_header_insert_u32()` — what a device does with its own peer
    /// ID on the way in (§4).
    pub fn insert_header_u32(&mut self, word: u32) {
        self.header.splice(0..0, word.to_be_bytes());
    }

    /// Removes a 32-bit word from the front of the header.
    /// `nng_msg_header_trim_u32()` — what a device does when routing a reply
    /// back (§4). `None` when fewer than four octets remain.
    pub fn trim_header_u32(&mut self) -> Option<u32> {
        if self.header.len() < 4 {
            return None;
        }
        let word = u32::from_be_bytes([
            self.header[0],
            self.header[1],
            self.header[2],
            self.header[3],
        ]);
        self.header.drain(..4);
        Some(word)
    }

    /// Removes a 32-bit word from the end of the header.
    /// `nng_msg_header_chop_u32()`.
    pub fn chop_header_u32(&mut self) -> Option<u32> {
        let at = self.header.len().checked_sub(4)?;
        let word = u32::from_be_bytes([
            self.header[at],
            self.header[at + 1],
            self.header[at + 2],
            self.header[at + 3],
        ]);
        self.header.truncate(at);
        Some(word)
    }

    /// Moves the first `len` octets of the body into the header.
    ///
    /// This is what a cooked socket does with an arriving message: the wire
    /// carries one run of octets and only the protocol knows how many of
    /// them in front are its own — four per tag for a REQ stack, four for a
    /// PAIR v1 hop count, none for PUB/SUB (§3). Doing it here rather than
    /// in each protocol means the header is always the protocol's and the
    /// body is always the application's, whichever socket type is holding
    /// the message.
    ///
    /// Fails with `NNG_EPROTO` when the body is shorter than `len`: a
    /// message whose declared protocol header runs off its own end is
    /// malformed, which is the case the RFC says MUST be ignored.
    pub fn split_header_off_body(&mut self, len: usize) -> Result<()> {
        if self.body.len() < len {
            return Err(Error::EPROTO(
                format!(
                    "the protocol header claims {len} octets and the whole message is {}",
                    self.body.len()
                )
                .into(),
            ));
        }
        self.header.extend_from_slice(&self.body[..len]);
        self.body.drain(..len);
        Ok(())
    }

    /// Appends the whole wire form — the 64-bit size field, the header, the
    /// body — to `out`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.reserve(message::SIZE_LEN + self.wire_len());
        message::encode_size(self.wire_len() as u64, out);
        out.extend_from_slice(&self.header);
        out.extend_from_slice(&self.body);
    }

    /// The whole wire form as its own buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(message::SIZE_LEN + self.wire_len());
        self.write_to(&mut out);
        out
    }
}

/// Decodes one whole message from the front of `input`.
///
/// Three outcomes, and the middle one is the reason this returns an
/// `Option` rather than erroring on a short read:
///
/// * `Ok(Some((message, used)))` — a complete message, and how many octets
///   it took. The header is empty: the wire carries one run of octets, and
///   which of them belong to the protocol is a question only the protocol
///   can answer ([`Message::split_header_off_body`]).
/// * `Ok(None)` — not yet. Read more and call again; nothing was consumed
///   and nothing was allocated. This is "wholly or not at all" (§3) as
///   control flow: there is no way to obtain the first half of a message.
/// * `Err(NNG_EMSGSIZE)` — the peer declared more than `max_body_bytes`.
///   Judged **from the size field alone**, before the body is looked at, let
///   alone reserved: SP grants no credit, a declaration may be 2^64-1 octets
///   (§3), and this is the only point at which the allocation is still ours
///   to refuse.
///
/// `max_body_bytes` of [`RECV_MAX_SIZE_UNLIMITED`] is NNG's "no limit".
pub fn decode(input: &[u8], max_body_bytes: u64) -> Result<Option<(Message, usize)>> {
    let cap = if max_body_bytes == RECV_MAX_SIZE_UNLIMITED {
        u64::MAX
    } else {
        max_body_bytes
    };
    match message::decode(input, cap) {
        Ok((body, used)) => Ok(Some((Message::from_body(body.to_vec()), used))),
        Err(weida_sp::MessageError::Incomplete) => Ok(None),
        Err(err @ weida_sp::MessageError::BodyTooLarge { .. }) => {
            Err(Error::EMSGSIZE(format!("{err} (NNG_OPT_RECVMAXSZ)").into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the header and the body are separate storage — editing one
    /// never touches the other — and the body is what `nng_msg_len()`
    /// counts.
    #[test]
    fn the_header_and_the_body_are_separate() {
        let mut msg = Message::from_body(b"payload".to_vec());
        assert!(msg.header().is_empty());
        assert_eq!(msg.len(), 7);

        msg.append_header_u32(0x8000_0001);
        assert_eq!(msg.header(), [0x80, 0x00, 0x00, 0x01]);
        assert_eq!(msg.body(), b"payload");
        assert_eq!(msg.len(), 7, "a header is not body length");
        assert_eq!(msg.wire_len(), 11);
    }

    /// Claim: the four header edits are NNG's four, and front and back are
    /// distinct — a device pushes its peer ID at the front of a stack whose
    /// last word is the request ID, and pops it from the front again.
    #[test]
    fn header_words_go_on_and_come_off_at_both_ends() {
        let mut msg = Message::new();
        msg.append_header_u32(0x8000_0009); // the request ID, terminal
        msg.insert_header_u32(7); // a forwarder's peer ID, in front
        assert_eq!(msg.header().len(), 8);

        assert_eq!(msg.trim_header_u32(), Some(7));
        assert_eq!(msg.chop_header_u32(), Some(0x8000_0009));
        assert_eq!(msg.trim_header_u32(), None);
        assert_eq!(msg.chop_header_u32(), None);

        // A header with fewer than four octets left yields nothing rather
        // than a truncated word.
        let mut short = Message::new();
        short.header_mut().extend_from_slice(&[1, 2, 3]);
        assert_eq!(short.trim_header_u32(), None);
        assert_eq!(short.chop_header_u32(), None);
        assert_eq!(short.header(), [1, 2, 3]);
    }

    /// Claim: claiming header octets off the body moves them, and a claim
    /// longer than the message is `NNG_EPROTO` rather than a panic or a
    /// silent short header.
    #[test]
    fn claiming_header_octets_off_the_body_moves_them() {
        let mut msg = Message::from_body(b"\x00\x00\x00\x01hello".to_vec());
        msg.split_header_off_body(4).expect("four octets");
        assert_eq!(msg.header(), [0, 0, 0, 1]);
        assert_eq!(msg.body(), b"hello");

        let mut short = Message::from_body(b"ab".to_vec());
        let err = short.split_header_off_body(4).unwrap_err();
        assert!(matches!(err, Error::EPROTO(_)), "{err:?}");
        assert_eq!(short.body(), b"ab", "a refused split changes nothing");
    }

    /// Claim: the wire form is the 64-bit size followed by header then body,
    /// and it round-trips through the decoder with the same octets.
    #[test]
    fn the_wire_form_is_a_size_and_one_run_of_octets() {
        let msg = Message::from_parts(b"\x80\x00\x00\x05".to_vec(), b"ping".to_vec());
        let wire = msg.encode();
        assert_eq!(&wire[..8], &[0, 0, 0, 0, 0, 0, 0, 8]);
        assert_eq!(&wire[8..], b"\x80\x00\x00\x05ping");

        let (decoded, used) = decode(&wire, DEFAULT_RECV_MAX_SIZE)
            .expect("a legal message")
            .expect("a complete one");
        assert_eq!(used, wire.len());
        // The decoder does not know where the protocol header ends; the
        // protocol says so afterwards.
        assert_eq!(decoded.body(), b"\x80\x00\x00\x05ping");
        let mut decoded = decoded;
        decoded.split_header_off_body(4).expect("the tag");
        assert_eq!(decoded, msg);

        // An empty message is eight zero octets and nothing after them.
        assert_eq!(Message::new().encode(), [0u8; 8]);
    }

    /// Claim: a message is delivered wholly or not at all. Every truncation
    /// of a complete frame yields "not yet" rather than a partial message,
    /// and only the complete frame produces one.
    #[test]
    fn a_partial_message_is_never_half_delivered() {
        let wire = Message::from_body(b"hello".to_vec()).encode();
        for cut in 0..wire.len() {
            assert_eq!(
                decode(&wire[..cut], DEFAULT_RECV_MAX_SIZE).expect("not an error"),
                None,
                "{cut} octets produced a message"
            );
        }
        assert!(
            decode(&wire, DEFAULT_RECV_MAX_SIZE)
                .expect("legal")
                .is_some()
        );
    }

    /// Claim: `RECVMAXSZ` is judged from the declared 64-bit length alone.
    /// Eight octets of `0xFF` declare 2^64-1 and nothing follows them; a
    /// decoder that reserved before checking would die on this input, and
    /// this one answers `NNG_EMSGSIZE` with both numbers.
    #[test]
    fn an_oversized_declaration_is_refused_before_anything_is_allocated() {
        let hostile = [0xFFu8; 8];
        let err = decode(&hostile, DEFAULT_RECV_MAX_SIZE).unwrap_err();
        assert!(matches!(err, Error::EMSGSIZE(_)), "{err:?}");
        assert!(err.cause().contains(&DEFAULT_RECV_MAX_SIZE.to_string()));
        assert!(err.cause().contains("NNG_OPT_RECVMAXSZ"));

        // Exactly at the cap is accepted; one over is not.
        let body = vec![7u8; 64];
        let wire = Message::from_body(body).encode();
        assert!(decode(&wire, 64).expect("legal").is_some());
        assert!(matches!(decode(&wire, 63), Err(Error::EMSGSIZE(_))));
    }

    /// Claim: NNG's own "no limit" value is available and means what it says
    /// there — the declaration is believed — which is the parity price of
    /// letting a ported program keep its configuration.
    #[test]
    fn nngs_unlimited_is_available_and_means_unlimited() {
        let wire = Message::from_body(vec![0u8; 4096]).encode();
        assert!(
            decode(&wire, RECV_MAX_SIZE_UNLIMITED)
                .expect("legal")
                .is_some()
        );
        // And it really is the declaration being believed: a hostile
        // declaration is not refused, it is merely incomplete.
        assert_eq!(
            decode(&[0xFFu8; 8], RECV_MAX_SIZE_UNLIMITED).expect("legal"),
            None
        );
    }
}
