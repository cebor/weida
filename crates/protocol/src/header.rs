//! CBOR frame headers.
//!
//! The codecs are written by hand rather than derived. Three reasons:
//!
//! * the strictness rules (duplicate-key rejection, non-uint key rejection,
//!   per-field string caps, depth-limited skipping) are protocol requirements,
//!   not serialization defaults;
//! * the exact byte layout is normative — see the golden vectors in
//!   `docs/PROTOCOL.md` §8 — and a derive macro's field ordering is not a
//!   contract we control;
//! * every decoder here is a hostile-input boundary, so its allocation
//!   behaviour has to be readable.
//!
//! All decode failures are protocol violations that close the connection.
//!
//! **No field of a DATA header is required by the decoder.** A header no longer
//! carries the stream's role, so the decoder cannot know which fields the
//! context demands; `endpoint`-on-initiating-streams is enforced by the
//! transport's dispatch, which does know (`docs/PROTOCOL.md` §6.2).

use std::convert::Infallible;

use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use weida_core::Error;

/// Decoder limits. The string caps are normative
/// (`docs/PROTOCOL.md` §6); the list and depth caps are defensive
/// implementation limits documented in the same section.
pub mod limits {
    /// Cap for the DATA `endpoint` field.
    pub const MAX_ENDPOINT_BYTES: usize = 512;
    /// Cap for the DATA `content_type` field.
    pub const MAX_CONTENT_TYPE_BYTES: usize = 256;
    /// Cap for the DATA `traceparent` field.
    pub const MAX_TRACEPARENT_BYTES: usize = 128;
    /// Cap for the DATA `tracestate` field.
    pub const MAX_TRACESTATE_BYTES: usize = 512;
    /// Cap for the DATA `topic` field.
    pub const MAX_TOPIC_BYTES: usize = 256;
    /// Length of the DATA `producer` field: a raw 32-byte digest.
    ///
    /// Both a cap and an exact length. `docs/PROTOCOL.md` §6.2 defines the
    /// value as "the raw 32-byte digest", so a longer one is a framing
    /// violation and a shorter one names nothing this specification defines.
    pub const PRODUCER_BYTES: usize = 32;
    /// Cap for the SUBSCRIBE/UNSUBSCRIBE `filter` field.
    pub const MAX_FILTER_BYTES: usize = 256;
    /// Cap for the ERROR `message` field.
    pub const MAX_MESSAGE_BYTES: usize = 1024;
    /// Cap on the number of items in a HELLO list field.
    ///
    /// Without it, a hostile peer could pin `max_concurrent_uni_streams`
    /// worth of large `Vec<u64>`s by opening many HELLO streams.
    pub const MAX_LIST_ITEMS: usize = 64;
    /// Nesting depth allowed when skipping an unknown field.
    pub const MAX_SKIP_DEPTH: usize = 8;
}

/// HELLO keys.
mod hello_key {
    pub const VERSIONS: u64 = 0;
    pub const MAX_HEADER_BYTES: u64 = 1;
    pub const MAX_TRANSFERS: u64 = 2;
    pub const CAPABILITIES: u64 = 3;
    pub const REQUIRED_CAPABILITIES: u64 = 4;
}

/// DATA keys.
mod data_key {
    pub const ENDPOINT: u64 = 0;
    pub const CONTENT_LEN: u64 = 1;
    pub const CONTENT_TYPE: u64 = 2;
    pub const TRACEPARENT: u64 = 3;
    pub const TRACESTATE: u64 = 4;
    pub const TOPIC: u64 = 5;
    pub const SEQUENCE: u64 = 6;
    pub const PRODUCER: u64 = 7;
}

/// ERROR keys.
mod error_key {
    pub const CODE: u64 = 0;
    pub const MESSAGE: u64 = 1;
}

/// SUBSCRIBE and UNSUBSCRIBE keys.
mod subscription_key {
    pub const ENDPOINT: u64 = 0;
    pub const FILTER: u64 = 1;
}

/// The topic filter grammar of `docs/PROTOCOL.md` §6.4.
///
/// A topic and a filter are byte strings split on [`filter::SEPARATOR`] into
/// segments. The two wildcards are whole-segment tokens, and everything else
/// is literal: there is no escape character, no normalization and no case
/// folding ([decisions/0007](../../../docs/decisions/0007-topic-namespace.md)
/// §4.2). The matcher lives with the fan-out it serves, in
/// `weida::pubsub`; what belongs here is the rule that says which filters may
/// exist at all, so an illegal one is refused at the codec boundary instead of
/// reaching a matcher that would have to cope with it.
pub mod filter {
    use super::HeaderError;

    /// Segment separator: `.`, one byte.
    pub const SEPARATOR: char = '.';
    /// Matches exactly one whole segment.
    pub const ONE_SEGMENT: &str = "*";
    /// Matches zero or more trailing segments; legal only as the last segment.
    pub const REST: &str = "#";

    /// Checks `filter` against the grammar.
    ///
    /// The empty filter is legal and matches every topic. One pass, no
    /// allocation.
    pub fn validate(filter: &str) -> Result<(), HeaderError> {
        let mut segments = filter.split(SEPARATOR).peekable();
        while let Some(segment) = segments.next() {
            let is_last = segments.peek().is_none();
            if segment.contains(ONE_SEGMENT) && segment != ONE_SEGMENT {
                return Err(HeaderError::InvalidFilter(
                    "`*` must occupy a whole segment",
                ));
            }
            if segment.contains(REST) {
                if segment != REST {
                    return Err(HeaderError::InvalidFilter(
                        "`#` must occupy a whole segment",
                    ));
                }
                if !is_last {
                    return Err(HeaderError::InvalidFilter("`#` must be the final segment"));
                }
            }
        }
        Ok(())
    }
}

/// Why a header was rejected. Every variant is a protocol violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// The bytes are not well-formed CBOR, or a value had the wrong type.
    Malformed(&'static str),
    /// An indefinite-length item was used where the protocol forbids it.
    Indefinite,
    /// A map key appeared twice.
    DuplicateKey(u64),
    /// A map key was not greater than the preceding one; keys must ascend.
    UnorderedKey(u64),
    /// A map key was not an unsigned integer.
    NonUintKey,
    /// A required key was absent.
    MissingKey(u64),
    /// A text field exceeded its cap.
    StringTooLong {
        /// Key of the offending field.
        key: u64,
        /// Length found.
        len: usize,
        /// Cap for this field.
        max: usize,
    },
    /// A list field declared more items than the decoder accepts.
    ListTooLong {
        /// Key of the offending field.
        key: u64,
        /// Declared item count.
        len: u64,
        /// Cap for list fields.
        max: usize,
    },
    /// An unknown field nested deeper than [`limits::MAX_SKIP_DEPTH`].
    DepthExceeded,
    /// Bytes remained after the header map.
    TrailingBytes,
    /// A topic filter violated the grammar of `docs/PROTOCOL.md` §6.4.
    InvalidFilter(&'static str),
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderError::Malformed(what) => write!(f, "malformed header: {what}"),
            HeaderError::Indefinite => f.write_str("indefinite-length items are not allowed"),
            HeaderError::DuplicateKey(k) => write!(f, "duplicate header key {k}"),
            HeaderError::UnorderedKey(k) => {
                write!(f, "header key {k} is out of ascending order")
            }
            HeaderError::NonUintKey => f.write_str("header key is not an unsigned integer"),
            HeaderError::MissingKey(k) => write!(f, "required header key {k} is missing"),
            HeaderError::StringTooLong { key, len, max } => {
                write!(
                    f,
                    "key {key}: text of {len} bytes exceeds the {max} byte cap"
                )
            }
            HeaderError::ListTooLong { key, len, max } => {
                write!(
                    f,
                    "key {key}: list of {len} items exceeds the {max} item cap"
                )
            }
            HeaderError::DepthExceeded => f.write_str("unknown field nested too deeply"),
            HeaderError::TrailingBytes => f.write_str("trailing bytes after the header"),
            HeaderError::InvalidFilter(why) => write!(f, "invalid topic filter: {why}"),
        }
    }
}

impl std::error::Error for HeaderError {}

impl From<HeaderError> for Error {
    fn from(e: HeaderError) -> Error {
        Error::Protocol(e.to_string())
    }
}

/// Encoding into a `Vec` cannot fail, so the encoders return bytes directly.
fn encode_with(
    f: impl FnOnce(&mut Encoder<Vec<u8>>) -> Result<(), minicbor::encode::Error<Infallible>>,
) -> Vec<u8> {
    let mut e = Encoder::new(Vec::new());
    f(&mut e).expect("encoding into a Vec is infallible");
    e.into_writer()
}

/// Skips one CBOR value iteratively, refusing to recurse and refusing to nest
/// deeper than `max_depth`.
///
/// `minicbor` has its own `skip`, but the protocol requires a specific,
/// auditable bound on hostile nesting; a value that nests deeper is rejected
/// rather than tolerated.
fn skip_value(d: &mut Decoder<'_>, max_depth: usize) -> Result<(), HeaderError> {
    // `stack` holds the outstanding item counts of enclosing containers; its
    // length is the current nesting depth and is bounded by `max_depth`.
    let mut stack: Vec<u64> = Vec::new();
    let mut remaining: u64 = 1;

    loop {
        if remaining == 0 {
            match stack.pop() {
                Some(outer) => {
                    remaining = outer;
                    continue;
                }
                None => return Ok(()),
            }
        }
        remaining -= 1;

        let ty = d
            .datatype()
            .map_err(|_| HeaderError::Malformed("truncated value"))?;
        let nested = match ty {
            Type::Bool => {
                d.bool().map_err(|_| HeaderError::Malformed("bool"))?;
                None
            }
            Type::Null => {
                d.null().map_err(|_| HeaderError::Malformed("null"))?;
                None
            }
            Type::Undefined => {
                d.undefined()
                    .map_err(|_| HeaderError::Malformed("undefined"))?;
                None
            }
            Type::U8
            | Type::U16
            | Type::U32
            | Type::U64
            | Type::I8
            | Type::I16
            | Type::I32
            | Type::I64
            | Type::Int => {
                d.int().map_err(|_| HeaderError::Malformed("integer"))?;
                None
            }
            Type::F32 | Type::F64 => {
                d.f64().map_err(|_| HeaderError::Malformed("float"))?;
                None
            }
            Type::Bytes => {
                d.bytes()
                    .map_err(|_| HeaderError::Malformed("byte string"))?;
                None
            }
            Type::String => {
                d.str().map_err(|_| HeaderError::Malformed("text string"))?;
                None
            }
            Type::Array => Some(
                d.array()
                    .map_err(|_| HeaderError::Malformed("array"))?
                    .ok_or(HeaderError::Indefinite)?,
            ),
            Type::Map => {
                let pairs = d
                    .map()
                    .map_err(|_| HeaderError::Malformed("map"))?
                    .ok_or(HeaderError::Indefinite)?;
                Some(
                    pairs
                        .checked_mul(2)
                        .ok_or(HeaderError::Malformed("map length overflow"))?,
                )
            }
            Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef => {
                return Err(HeaderError::Indefinite);
            }
            Type::Break => return Err(HeaderError::Malformed("unexpected break")),
            // Tags, half-floats and other simple values carry no meaning in
            // weida headers; extensions must use plain data items.
            Type::Tag => return Err(HeaderError::Malformed("tags are not allowed")),
            Type::F16 => return Err(HeaderError::Malformed("half floats are not allowed")),
            Type::Simple => return Err(HeaderError::Malformed("simple values are not allowed")),
            Type::Unknown(_) => return Err(HeaderError::Malformed("unknown major type")),
        };

        if let Some(count) = nested
            && count > 0
        {
            if stack.len() >= max_depth {
                return Err(HeaderError::DepthExceeded);
            }
            stack.push(remaining);
            remaining = count;
        }
    }
}

/// Reader for one strict header map.
struct MapReader<'a, 'b> {
    d: &'a mut Decoder<'b>,
    remaining: u64,
    /// Bitmask of seen keys `0..=63`, for the required-key checks. The
    /// specification reserves that range, so presence needs no allocation.
    seen: u64,
    /// Previously read key.
    ///
    /// Keys are required to ascend strictly, which makes duplicate detection
    /// complete for *every* key — including extension keys the decoder skips —
    /// in constant space. A set of seen extension keys would be exactly the
    /// remote-controlled allocation the invariants forbid.
    last: Option<u64>,
}

impl<'a, 'b> MapReader<'a, 'b> {
    fn new(d: &'a mut Decoder<'b>) -> Result<MapReader<'a, 'b>, HeaderError> {
        let len = d
            .map()
            .map_err(|_| HeaderError::Malformed("header is not a map"))?
            .ok_or(HeaderError::Indefinite)?;
        Ok(MapReader {
            d,
            remaining: len,
            seen: 0,
            last: None,
        })
    }

    fn next_key(&mut self) -> Result<Option<u64>, HeaderError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        match self.d.datatype() {
            Ok(Type::U8 | Type::U16 | Type::U32 | Type::U64) => {}
            Ok(_) => return Err(HeaderError::NonUintKey),
            Err(_) => return Err(HeaderError::Malformed("truncated key")),
        }
        let key = self.d.u64().map_err(|_| HeaderError::NonUintKey)?;
        if let Some(prev) = self.last {
            if key == prev {
                return Err(HeaderError::DuplicateKey(key));
            }
            if key < prev {
                return Err(HeaderError::UnorderedKey(key));
            }
        }
        self.last = Some(key);
        if key < 64 {
            self.seen |= 1u64 << key;
        }
        Ok(Some(key))
    }

    fn saw(&self, key: u64) -> bool {
        key < 64 && self.seen & (1u64 << key) != 0
    }

    fn require(&self, key: u64) -> Result<(), HeaderError> {
        if self.saw(key) {
            Ok(())
        } else {
            Err(HeaderError::MissingKey(key))
        }
    }

    fn u64(&mut self) -> Result<u64, HeaderError> {
        self.d
            .u64()
            .map_err(|_| HeaderError::Malformed("expected an unsigned integer"))
    }

    fn text(&mut self, key: u64, max: usize) -> Result<String, HeaderError> {
        let s = self
            .d
            .str()
            .map_err(|_| HeaderError::Malformed("expected a text string"))?;
        if s.len() > max {
            return Err(HeaderError::StringTooLong {
                key,
                len: s.len(),
                max,
            });
        }
        Ok(s.to_owned())
    }

    /// Reads a byte string of exactly `N` bytes.
    ///
    /// The cap is checked before the length is trusted for anything, and a
    /// shorter value is rejected rather than padded: `docs/PROTOCOL.md` §6.2
    /// defines the one field that uses this as a raw 32-byte digest, and half
    /// a digest identifies nobody.
    fn byte_array<const N: usize>(&mut self, key: u64) -> Result<[u8; N], HeaderError> {
        let bytes = self
            .d
            .bytes()
            .map_err(|_| HeaderError::Malformed("expected a byte string"))?;
        if bytes.len() > N {
            return Err(HeaderError::StringTooLong {
                key,
                len: bytes.len(),
                max: N,
            });
        }
        bytes
            .try_into()
            .map_err(|_| HeaderError::Malformed("byte string has the wrong length"))
    }

    fn uint_list(&mut self, key: u64) -> Result<Vec<u64>, HeaderError> {
        let len = self
            .d
            .array()
            .map_err(|_| HeaderError::Malformed("expected an array"))?
            .ok_or(HeaderError::Indefinite)?;
        if len > limits::MAX_LIST_ITEMS as u64 {
            return Err(HeaderError::ListTooLong {
                key,
                len,
                max: limits::MAX_LIST_ITEMS,
            });
        }
        // `len` is now bounded by MAX_LIST_ITEMS, so reserving is safe.
        let mut out = Vec::with_capacity(len as usize);
        for _ in 0..len {
            out.push(self.u64()?);
        }
        Ok(out)
    }

    fn skip(&mut self) -> Result<(), HeaderError> {
        skip_value(self.d, limits::MAX_SKIP_DEPTH)
    }
}

/// Rejects trailing bytes after a header map.
fn finish(d: &Decoder<'_>) -> Result<(), HeaderError> {
    if d.position() == d.input().len() {
        Ok(())
    } else {
        Err(HeaderError::TrailingBytes)
    }
}

/// HELLO header: connection negotiation input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    /// Wire protocol versions the sender supports.
    pub versions: Vec<u64>,
    /// Largest header the sender is willing to receive.
    pub max_header_bytes: u64,
    /// Advisory concurrent inbound transfer count.
    pub max_transfers: u64,
    /// Capability codes the sender supports.
    pub capabilities: Vec<u64>,
    /// Capability codes the sender requires the peer to support.
    pub required_capabilities: Vec<u64>,
}

impl Hello {
    /// The HELLO a v0 implementation sends.
    pub fn v0(max_header_bytes: u64, max_transfers: u64) -> Hello {
        Hello {
            versions: vec![crate::VERSION],
            max_header_bytes,
            max_transfers,
            capabilities: Vec::new(),
            required_capabilities: Vec::new(),
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        encode_with(|e| {
            e.map(5)?;
            e.u64(hello_key::VERSIONS)?
                .array(self.versions.len() as u64)?;
            for v in &self.versions {
                e.u64(*v)?;
            }
            e.u64(hello_key::MAX_HEADER_BYTES)?
                .u64(self.max_header_bytes)?;
            e.u64(hello_key::MAX_TRANSFERS)?.u64(self.max_transfers)?;
            e.u64(hello_key::CAPABILITIES)?
                .array(self.capabilities.len() as u64)?;
            for c in &self.capabilities {
                e.u64(*c)?;
            }
            e.u64(hello_key::REQUIRED_CAPABILITIES)?
                .array(self.required_capabilities.len() as u64)?;
            for c in &self.required_capabilities {
                e.u64(*c)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<Hello, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut versions = Vec::new();
        let mut max_header_bytes = 0;
        let mut max_transfers = 0;
        let mut capabilities = Vec::new();
        let mut required_capabilities = Vec::new();
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    hello_key::VERSIONS => versions = m.uint_list(key)?,
                    hello_key::MAX_HEADER_BYTES => max_header_bytes = m.u64()?,
                    hello_key::MAX_TRANSFERS => max_transfers = m.u64()?,
                    hello_key::CAPABILITIES => capabilities = m.uint_list(key)?,
                    hello_key::REQUIRED_CAPABILITIES => required_capabilities = m.uint_list(key)?,
                    _ => m.skip()?,
                }
            }
            for key in [
                hello_key::VERSIONS,
                hello_key::MAX_HEADER_BYTES,
                hello_key::MAX_TRANSFERS,
                hello_key::CAPABILITIES,
                hello_key::REQUIRED_CAPABILITIES,
            ] {
                m.require(key)?;
            }
        }
        finish(&d)?;
        Ok(Hello {
            versions,
            max_header_bytes,
            max_transfers,
            capabilities,
            required_capabilities,
        })
    }
}

/// DATA header: one transfer.
///
/// Every field is optional at the decoder. Which of them the *context*
/// requires is a dispatch question: an initiating stream must name an endpoint
/// and the reply half of an exchange must not, but the decoder sees bytes, not
/// streams (`docs/PROTOCOL.md` §6.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataHeader {
    /// Endpoint path. Required on an initiating stream, ignored on a reply.
    pub endpoint: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// W3C `traceparent`.
    pub traceparent: Option<String>,
    /// W3C `tracestate`, opaque passthrough.
    pub tracestate: Option<String>,
    /// Pub/Sub topic; opaque bytes, selected by the filter grammar of
    /// `docs/PROTOCOL.md` §6.4. Only meaningful on transfers fanned out by a
    /// publisher.
    pub topic: Option<String>,
    /// Per-producer sequence number, for ordering and gap detection
    /// (`docs/PROTOCOL.md` §6.2, key `6`).
    ///
    /// **Specified ahead of code**: the codec carries it, and no v0 sender
    /// sets it. It is not a transfer identifier and correlates nothing — an
    /// exchange is correlated by its stream.
    pub sequence: Option<u64>,
    /// Producer identity: the raw 32-byte digest (`docs/PROTOCOL.md` §6.2,
    /// key `7`).
    ///
    /// **Specified ahead of code**, and absent in the default case by design:
    /// the receiver already knows the sending peer's proved fingerprint from
    /// the handshake, so this names a producer only where it is *not* the
    /// connection peer — a relay, or a name an L2 subscription supplies
    /// ([decisions/0008](../../../docs/decisions/0008-session-identity.md)
    /// §4.4). The `sha256:<64 hex>` spelling is presentation only and never
    /// goes on the wire.
    pub producer: Option<[u8; limits::PRODUCER_BYTES]>,
}

impl DataHeader {
    /// A header addressing `endpoint`, for the initiating half of a stream.
    pub fn addressed(endpoint: impl Into<String>) -> DataHeader {
        DataHeader {
            endpoint: Some(endpoint.into()),
            ..DataHeader::default()
        }
    }

    /// A header for the reply half of an exchange: no endpoint, no topic.
    ///
    /// The stream is the correlation, so a reply carries no identifier of the
    /// request it answers.
    pub fn reply() -> DataHeader {
        DataHeader::default()
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let count = u64::from(self.endpoint.is_some())
            + u64::from(self.content_len.is_some())
            + u64::from(self.content_type.is_some())
            + u64::from(self.traceparent.is_some())
            + u64::from(self.tracestate.is_some())
            + u64::from(self.topic.is_some())
            + u64::from(self.sequence.is_some())
            + u64::from(self.producer.is_some());
        encode_with(|e| {
            e.map(count)?;
            if let Some(endpoint) = &self.endpoint {
                e.u64(data_key::ENDPOINT)?.str(endpoint)?;
            }
            if let Some(len) = self.content_len {
                e.u64(data_key::CONTENT_LEN)?.u64(len)?;
            }
            if let Some(ct) = &self.content_type {
                e.u64(data_key::CONTENT_TYPE)?.str(ct)?;
            }
            if let Some(tp) = &self.traceparent {
                e.u64(data_key::TRACEPARENT)?.str(tp)?;
            }
            if let Some(ts) = &self.tracestate {
                e.u64(data_key::TRACESTATE)?.str(ts)?;
            }
            if let Some(topic) = &self.topic {
                e.u64(data_key::TOPIC)?.str(topic)?;
            }
            // Keys 6 and 7 are written only when set, which for every v0
            // sender means never: nothing in `weida` populates them yet.
            if let Some(sequence) = self.sequence {
                e.u64(data_key::SEQUENCE)?.u64(sequence)?;
            }
            if let Some(producer) = &self.producer {
                e.u64(data_key::PRODUCER)?.bytes(producer)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<DataHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut header = DataHeader::default();
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    data_key::ENDPOINT => {
                        header.endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?)
                    }
                    data_key::CONTENT_LEN => header.content_len = Some(m.u64()?),
                    data_key::CONTENT_TYPE => {
                        header.content_type = Some(m.text(key, limits::MAX_CONTENT_TYPE_BYTES)?)
                    }
                    data_key::TRACEPARENT => {
                        header.traceparent = Some(m.text(key, limits::MAX_TRACEPARENT_BYTES)?)
                    }
                    data_key::TRACESTATE => {
                        header.tracestate = Some(m.text(key, limits::MAX_TRACESTATE_BYTES)?)
                    }
                    data_key::TOPIC => header.topic = Some(m.text(key, limits::MAX_TOPIC_BYTES)?),
                    data_key::SEQUENCE => header.sequence = Some(m.u64()?),
                    data_key::PRODUCER => header.producer = Some(m.byte_array(key)?),
                    _ => m.skip()?,
                }
            }
        }
        finish(&d)?;
        Ok(header)
    }
}

/// ERROR header.
///
/// Legal only on the reply half of a bidirectional stream: an ERROR is the
/// alternative to a reply, so it needs no reference to what it answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorHeader {
    /// Raw error code.
    pub code: u64,
    /// Human-readable detail; never machine-interpreted.
    pub message: Option<String>,
}

impl ErrorHeader {
    /// Builds a header for a known error code.
    pub fn new(code: weida_core::ErrorCode) -> ErrorHeader {
        ErrorHeader {
            code: code.to_wire(),
            message: None,
        }
    }

    /// The error code, or `None` for an unknown one.
    pub fn error_code(&self) -> Option<weida_core::ErrorCode> {
        weida_core::ErrorCode::from_wire(self.code)
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        let count = 1 + u64::from(self.message.is_some());
        encode_with(|e| {
            e.map(count)?;
            e.u64(error_key::CODE)?.u64(self.code)?;
            if let Some(msg) = &self.message {
                e.u64(error_key::MESSAGE)?.str(msg)?;
            }
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<ErrorHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut code = 0;
        let mut message = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    error_key::CODE => code = m.u64()?,
                    error_key::MESSAGE => message = Some(m.text(key, limits::MAX_MESSAGE_BYTES)?),
                    _ => m.skip()?,
                }
            }
            m.require(error_key::CODE)?;
        }
        finish(&d)?;
        Ok(ErrorHeader { code, message })
    }
}

/// SUBSCRIBE and UNSUBSCRIBE header.
///
/// Both frames carry the same two keys: the publisher path to (un)subscribe on
/// and the topic filter. The filter is a segmented pattern, not a byte prefix
/// ([`filter`]): the empty filter matches every topic, `*` matches one whole
/// segment and a trailing `#` matches zero or more.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionHeader {
    /// Publisher endpoint path.
    pub endpoint: String,
    /// Topic filter; the empty string matches everything. A decoded header's
    /// filter has passed [`filter::validate`].
    pub filter: String,
}

impl SubscriptionHeader {
    /// A header for `endpoint` and `filter`.
    pub fn new(endpoint: impl Into<String>, filter: impl Into<String>) -> SubscriptionHeader {
        SubscriptionHeader {
            endpoint: endpoint.into(),
            filter: filter.into(),
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        // Both keys are required, so neither is elided: an absent filter and an
        // empty filter would otherwise be indistinguishable on the wire, and
        // the empty filter is the "everything" subscription.
        encode_with(|e| {
            e.map(2)?;
            e.u64(subscription_key::ENDPOINT)?.str(&self.endpoint)?;
            e.u64(subscription_key::FILTER)?.str(&self.filter)?;
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<SubscriptionHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut endpoint = None;
        let mut filter = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    subscription_key::ENDPOINT => {
                        endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?)
                    }
                    subscription_key::FILTER => {
                        filter = Some(m.text(key, limits::MAX_FILTER_BYTES)?)
                    }
                    _ => m.skip()?,
                }
            }
            m.require(subscription_key::ENDPOINT)?;
            m.require(subscription_key::FILTER)?;
        }
        // The grammar is checked here, at the codec boundary, so no matcher
        // ever sees a filter it would have to interpret twice; an illegal one
        // closes the connection with `PROTOCOL_VIOLATION`
        // (`docs/PROTOCOL.md` §6.4).
        let filter = filter.expect("presence checked above");
        filter::validate(&filter)?;
        finish(&d)?;
        Ok(SubscriptionHeader {
            endpoint: endpoint.expect("presence checked above"),
            filter,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_core::ErrorCode;

    // --- golden vectors, docs/PROTOCOL.md §8 ------------------------------

    #[test]
    fn golden_data_request_header() {
        let h = DataHeader::addressed("/t");
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA1, 0x00, 0x62, 0x2F, 0x74]);
        assert_eq!(bytes.len(), 0x05);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_data_reply_header() {
        // The stream is the correlation, so a reply header is an empty map.
        let h = DataHeader::reply();
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA0]);
        assert_eq!(bytes.len(), 0x01);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_hello_header() {
        let h = Hello::v0(16384, 1024);
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA5, 0x00, 0x81, 0x00, 0x01, 0x19, 0x40, 0x00, 0x02, 0x19, 0x04, 0x00, 0x03, 0x80,
                0x04, 0x80
            ]
        );
        assert_eq!(bytes.len(), 0x10);
        assert_eq!(Hello::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_error_header() {
        let h = ErrorHeader::new(ErrorCode::NoReply);
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA1, 0x00, 0x05]);
        assert_eq!(bytes.len(), 0x03);
        assert_eq!(ErrorHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_pub_copy_data_header() {
        let mut h = DataHeader::addressed("/md");
        h.topic = Some("px.eur".into());
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x05, 0x66, 0x70, 0x78, 0x2E, 0x65, 0x75, 0x72
            ]
        );
        assert_eq!(bytes.len(), 0x0E);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    /// The digest of the §8 vectors: SHA-256 of `"test"`, the value the
    /// address examples in `docs/PROTOCOL.md` already use.
    const VECTOR_PRODUCER: [u8; limits::PRODUCER_BYTES] = [
        0x9F, 0x86, 0xD0, 0x81, 0x88, 0x4C, 0x7D, 0x65, 0x9A, 0x2F, 0xEA, 0xA0, 0xC5, 0x5A, 0xD0,
        0x15, 0xA3, 0xBF, 0x4F, 0x1B, 0x2B, 0x0B, 0x82, 0x2C, 0xD1, 0x5D, 0x6C, 0x15, 0xB0, 0xF0,
        0x0A, 0x08,
    ];

    #[test]
    fn golden_sequenced_data_header() {
        let mut h = DataHeader::addressed("/t");
        h.sequence = Some(1);
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA2, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01]);
        assert_eq!(bytes.len(), 0x07);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_relayed_data_header() {
        let mut h = DataHeader::addressed("/t");
        h.sequence = Some(1);
        h.producer = Some(VECTOR_PRODUCER);
        let bytes = h.encode();
        let mut expected = vec![0xA3, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01, 0x07, 0x58, 0x20];
        expected.extend_from_slice(&VECTOR_PRODUCER);
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len(), 0x2A);
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn a_producer_longer_than_the_cap_is_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?
                .bytes(&[0u8; limits::PRODUCER_BYTES + 1])?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes),
            Err(HeaderError::StringTooLong {
                key: data_key::PRODUCER,
                len: limits::PRODUCER_BYTES + 1,
                max: limits::PRODUCER_BYTES,
            })
        );
    }

    #[test]
    fn a_producer_shorter_than_a_digest_is_rejected() {
        // Half a digest identifies nobody, so it is a framing violation
        // rather than a value to carry (`docs/PROTOCOL.md` §6.2).
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?.bytes(&[0u8; 16])?;
            Ok(())
        });
        assert!(matches!(
            DataHeader::decode(&bytes),
            Err(HeaderError::Malformed(_))
        ));
    }

    #[test]
    fn the_new_keys_reject_the_wrong_cbor_type() {
        let sequence_as_text = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::SEQUENCE)?.str("7")?;
            Ok(())
        });
        assert!(DataHeader::decode(&sequence_as_text).is_err());

        let producer_as_text = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::PRODUCER)?.str("sha256:…")?;
            Ok(())
        });
        assert!(DataHeader::decode(&producer_as_text).is_err());
    }

    #[test]
    fn a_v0_header_carries_neither_new_key() {
        // What the runtime actually writes: keys 6 and 7 are specified ahead
        // of code, and no v0 sender sets them.
        let mut h = DataHeader::addressed("/t");
        h.traceparent = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into());
        let bytes = h.encode();
        let mut d = Decoder::new(&bytes);
        let pairs = d.map().unwrap().unwrap();
        let keys: Vec<u64> = (0..pairs)
            .map(|_| {
                let key = d.u64().unwrap();
                d.skip().unwrap();
                key
            })
            .collect();
        assert_eq!(keys, vec![data_key::ENDPOINT, data_key::TRACEPARENT]);
    }

    #[test]
    fn golden_subscription_headers() {
        let h = SubscriptionHeader::new("/md", "px.");
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E
            ]
        );
        assert_eq!(bytes.len(), 0x0B);
        // One header layout serves both kinds; only the kind byte differs, and
        // that byte belongs to the preamble (see `tests/golden_vectors.rs`).
        assert_eq!(SubscriptionHeader::decode(&bytes).unwrap(), h);
    }

    // --- roundtrips -------------------------------------------------------

    #[test]
    fn data_header_roundtrip_with_every_field() {
        let h = DataHeader {
            endpoint: Some("/transform".into()),
            content_len: Some(1 << 40),
            content_type: Some("application/octet-stream".into()),
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
            topic: Some("px.eur".into()),
            sequence: Some(u64::MAX),
            producer: Some([0x5A; limits::PRODUCER_BYTES]),
        };
        assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn error_header_roundtrip_with_and_without_message() {
        let bare = ErrorHeader::new(ErrorCode::UnknownEndpoint);
        assert_eq!(ErrorHeader::decode(&bare.encode()).unwrap(), bare);
        assert_eq!(bare.error_code(), Some(ErrorCode::UnknownEndpoint));

        let with_msg = ErrorHeader {
            code: 4,
            message: Some("handler panicked".into()),
        };
        assert_eq!(ErrorHeader::decode(&with_msg.encode()).unwrap(), with_msg);
    }

    #[test]
    fn keys_are_emitted_in_ascending_order() {
        let h = DataHeader {
            endpoint: Some("/x".into()),
            content_len: Some(1),
            content_type: Some("t".into()),
            traceparent: Some("p".into()),
            tracestate: Some("s".into()),
            topic: Some("k".into()),
            sequence: Some(9),
            producer: Some([0u8; limits::PRODUCER_BYTES]),
        };
        let bytes = h.encode();
        let mut d = Decoder::new(&bytes);
        let n = d.map().unwrap().unwrap();
        let mut last = None;
        for _ in 0..n {
            let key = d.u64().unwrap();
            if let Some(prev) = last {
                assert!(key > prev, "keys must ascend: {prev} then {key}");
            }
            last = Some(key);
            d.skip().unwrap();
        }
    }

    // --- optional fields --------------------------------------------------

    #[test]
    fn every_data_field_is_optional_at_the_decoder() {
        // The endpoint requirement lives in dispatch, not here: a reply half
        // legitimately carries none, and the decoder cannot tell the halves
        // apart.
        assert_eq!(DataHeader::decode(&[0xA0]).unwrap(), DataHeader::default());

        let only_topic = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::TOPIC)?.str("px.eur")?;
            Ok(())
        });
        let h = DataHeader::decode(&only_topic).unwrap();
        assert_eq!(h.topic.as_deref(), Some("px.eur"));
        assert_eq!(h.endpoint, None);
    }

    #[test]
    fn absent_fields_are_omitted_by_the_encoder() {
        let h = DataHeader::addressed("/t");
        assert_eq!(h.encode(), vec![0xA1, 0x00, 0x62, 0x2F, 0x74]);
    }

    // --- forward compatibility -------------------------------------------

    #[test]
    fn unknown_keys_are_skipped() {
        // Re-encode the golden DATA header with an extra key 63 holding a
        // nested structure, and check it still decodes to the same value.
        let h = DataHeader::addressed("/t");
        let extended = encode_with(|e| {
            e.map(2)?;
            e.u64(0)?.str("/t")?;
            e.u64(63)?.array(2)?.u64(7)?.map(1)?.u64(1)?.bool(true)?;
            Ok(())
        });
        assert_eq!(DataHeader::decode(&extended).unwrap(), h);
    }

    #[test]
    fn unknown_keys_above_the_reserved_range_are_skipped() {
        let extended = encode_with(|e| {
            e.map(2)?;
            e.u64(1)?.u64(5)?;
            e.u64(1000)?.str("future")?;
            Ok(())
        });
        let h = DataHeader::decode(&extended).unwrap();
        assert_eq!(h.content_len, Some(5));
    }

    #[test]
    fn skipping_tolerates_nesting_up_to_the_depth_limit() {
        for depth in [1usize, limits::MAX_SKIP_DEPTH] {
            let bytes = encode_with(|e| {
                e.map(2)?;
                e.u64(data_key::CONTENT_LEN)?.u64(1)?;
                e.u64(50)?;
                for _ in 0..depth {
                    e.array(1)?;
                }
                e.u64(1)?;
                Ok(())
            });
            let h = DataHeader::decode(&bytes).unwrap_or_else(|e| panic!("depth {depth}: {e}"));
            assert_eq!(h.content_len, Some(1), "depth {depth}");
        }
    }

    #[test]
    fn skipping_rejects_nesting_beyond_the_depth_limit() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(50)?;
            for _ in 0..(limits::MAX_SKIP_DEPTH + 1) {
                e.array(1)?;
            }
            e.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DepthExceeded
        );
    }

    #[test]
    fn skipping_a_wide_shallow_structure_is_fine() {
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(data_key::CONTENT_LEN)?.u64(1)?;
            e.u64(40)?.array(64)?;
            for i in 0..64u64 {
                e.u64(i)?;
            }
            Ok(())
        });
        assert_eq!(DataHeader::decode(&bytes).unwrap().content_len, Some(1));
    }

    // --- strictness -------------------------------------------------------

    #[test]
    fn duplicate_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(1)?.u64(1)?;
            e.u64(1)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DuplicateKey(1)
        );
    }

    #[test]
    fn non_uint_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.str("endpoint")?.str("/t")?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::NonUintKey
        );

        let negative = encode_with(|e| {
            e.map(1)?;
            e.i64(-1)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&negative).unwrap_err(),
            HeaderError::NonUintKey
        );
    }

    #[test]
    fn indefinite_maps_are_rejected() {
        let bytes = encode_with(|e| {
            e.begin_map()?;
            e.u64(1)?.u64(1)?;
            e.end()?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Indefinite
        );
    }

    #[test]
    fn indefinite_arrays_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(5)?;
            e.u64(0)?.begin_array()?.u64(0)?.end()?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            e.u64(3)?.array(0)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(Hello::decode(&bytes).unwrap_err(), HeaderError::Indefinite);
    }

    #[test]
    fn value_type_mismatches_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(data_key::CONTENT_LEN)?.str("not a number")?;
            Ok(())
        });
        assert!(matches!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed(_)
        ));
    }

    #[test]
    fn missing_required_keys_are_rejected() {
        // ERROR without a code.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(error_key::MESSAGE)?.str("why")?;
            Ok(())
        });
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(error_key::CODE)
        );

        // HELLO missing capabilities.
        let bytes = encode_with(|e| {
            e.map(4)?;
            e.u64(0)?.array(1)?.u64(0)?;
            e.u64(1)?.u64(16384)?;
            e.u64(2)?.u64(16)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(hello_key::CAPABILITIES)
        );
    }

    // --- subscription headers ---------------------------------------------

    #[test]
    fn an_empty_filter_is_legal_and_survives_the_roundtrip() {
        let h = SubscriptionHeader::new("/md", "");
        let bytes = h.encode();
        assert_eq!(SubscriptionHeader::decode(&bytes).unwrap(), h);
        // The key is written even though the value is empty: absent and empty
        // must stay distinguishable, and empty means "every topic".
        assert!(
            bytes.contains(&0x60),
            "the empty filter is encoded: {bytes:?}"
        );
    }

    #[test]
    fn the_filter_grammar_accepts_what_docs_protocol_6_4_permits() {
        for ok in [
            "",
            "#",
            "px",
            "px.eur",
            "px.*",
            "*.eur",
            "sensors.*.temp",
            "px.#",
            "px.",
            "a..b",
        ] {
            assert_eq!(filter::validate(ok), Ok(()), "{ok:?} must be legal");
        }
    }

    #[test]
    fn the_filter_grammar_rejects_partial_and_misplaced_wildcards() {
        for bad in [
            "px*", "*px", "p*x.eur", "px.e*ur", "#.px", "px.#.eur", "px#",
        ] {
            assert!(
                matches!(filter::validate(bad), Err(HeaderError::InvalidFilter(_))),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn an_illegal_filter_is_rejected_at_the_codec_boundary() {
        // Encoding does not validate — a test may build any bytes — but
        // decoding does, which is what makes the grammar enforceable against
        // a peer (`docs/PROTOCOL.md` §6.4).
        let bytes = SubscriptionHeader::new("/md", "px.#.eur").encode();
        assert!(matches!(
            SubscriptionHeader::decode(&bytes),
            Err(HeaderError::InvalidFilter(_))
        ));
        let e: Error = HeaderError::InvalidFilter("`#` must be the final segment").into();
        assert!(e.to_string().contains("invalid topic filter"));
    }

    #[test]
    fn subscription_strings_are_capped() {
        for (key, max) in [
            (subscription_key::ENDPOINT, limits::MAX_ENDPOINT_BYTES),
            (subscription_key::FILTER, limits::MAX_FILTER_BYTES),
        ] {
            let build = |len: usize| {
                let text = "a".repeat(len);
                let mut h = SubscriptionHeader::new("/md", "px.");
                if key == subscription_key::ENDPOINT {
                    h.endpoint = text;
                } else {
                    h.filter = text;
                }
                h.encode()
            };
            assert_eq!(
                SubscriptionHeader::decode(&build(max + 1)).unwrap_err(),
                HeaderError::StringTooLong {
                    key,
                    len: max + 1,
                    max
                },
                "key {key}"
            );
            assert!(
                SubscriptionHeader::decode(&build(max)).is_ok(),
                "key {key} at cap"
            );
        }
    }

    #[test]
    fn subscription_headers_require_both_keys() {
        let only = |key: u64| {
            encode_with(|e| {
                e.map(1)?;
                e.u64(key)?.str("/md")?;
                Ok(())
            })
        };
        assert_eq!(
            SubscriptionHeader::decode(&only(subscription_key::ENDPOINT)).unwrap_err(),
            HeaderError::MissingKey(subscription_key::FILTER)
        );
        assert_eq!(
            SubscriptionHeader::decode(&only(subscription_key::FILTER)).unwrap_err(),
            HeaderError::MissingKey(subscription_key::ENDPOINT)
        );
    }

    #[test]
    fn subscription_headers_reject_malformed_input() {
        assert!(SubscriptionHeader::decode(&[]).is_err());
        // Trailing bytes.
        let mut bytes = SubscriptionHeader::new("/md", "px.").encode();
        bytes.push(0xff);
        assert_eq!(
            SubscriptionHeader::decode(&bytes).unwrap_err(),
            HeaderError::TrailingBytes
        );
        // Unknown keys are skipped, like every other header.
        let extended = encode_with(|e| {
            e.map(3)?;
            e.u64(0)?.str("/md")?;
            e.u64(1)?.str("px.")?;
            e.u64(40)?.array(2)?.u64(1)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            SubscriptionHeader::decode(&extended).unwrap(),
            SubscriptionHeader::new("/md", "px.")
        );
    }

    #[test]
    fn oversized_strings_are_rejected_per_field() {
        // Built through the encoder, which emits keys in ascending order.
        let with_text = |key: u64, text: String| -> Vec<u8> {
            let mut h = DataHeader::reply();
            match key {
                data_key::ENDPOINT => h.endpoint = Some(text),
                data_key::CONTENT_TYPE => h.content_type = Some(text),
                data_key::TRACEPARENT => h.traceparent = Some(text),
                data_key::TRACESTATE => h.tracestate = Some(text),
                data_key::TOPIC => h.topic = Some(text),
                other => panic!("key {other} is not a text field"),
            }
            h.encode()
        };
        let cases: [(u64, usize); 5] = [
            (data_key::ENDPOINT, limits::MAX_ENDPOINT_BYTES),
            (data_key::CONTENT_TYPE, limits::MAX_CONTENT_TYPE_BYTES),
            (data_key::TRACEPARENT, limits::MAX_TRACEPARENT_BYTES),
            (data_key::TRACESTATE, limits::MAX_TRACESTATE_BYTES),
            (data_key::TOPIC, limits::MAX_TOPIC_BYTES),
        ];
        for (key, max) in cases {
            assert_eq!(
                DataHeader::decode(&with_text(key, "a".repeat(max + 1))).unwrap_err(),
                HeaderError::StringTooLong {
                    key,
                    len: max + 1,
                    max
                },
                "key {key}"
            );
            assert!(
                DataHeader::decode(&with_text(key, "a".repeat(max))).is_ok(),
                "key {key} at cap"
            );
        }
    }

    #[test]
    fn unordered_keys_are_rejected() {
        // Descending keys break the ascending-order rule, which is what makes
        // duplicate detection complete for extension keys.
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(2)?.str("t")?;
            e.u64(1)?.u64(1)?;
            e.u64(3)?.str("p")?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::UnorderedKey(1)
        );
    }

    #[test]
    fn duplicate_extension_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(1)?.u64(1)?;
            e.u64(1000)?.u64(1)?;
            e.u64(1000)?.u64(2)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::DuplicateKey(1000)
        );
    }

    #[test]
    fn oversized_error_messages_are_rejected() {
        let big = "m".repeat(limits::MAX_MESSAGE_BYTES + 1);
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(error_key::CODE)?.u64(2)?;
            e.u64(error_key::MESSAGE)?.str(&big)?;
            Ok(())
        });
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::StringTooLong {
                key: error_key::MESSAGE,
                len: limits::MAX_MESSAGE_BYTES + 1,
                max: limits::MAX_MESSAGE_BYTES
            }
        );
    }

    #[test]
    fn oversized_lists_are_rejected_without_allocating() {
        // A one-byte array header claiming 2^32 items must not reserve memory.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(0)?.array(u64::from(u32::MAX))?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap_err(),
            HeaderError::ListTooLong {
                key: hello_key::VERSIONS,
                len: u64::from(u32::MAX),
                max: limits::MAX_LIST_ITEMS
            }
        );
    }

    #[test]
    fn lists_exactly_at_the_cap_are_accepted() {
        let bytes = encode_with(|e| {
            e.map(5)?;
            e.u64(0)?.array(limits::MAX_LIST_ITEMS as u64)?;
            for i in 0..limits::MAX_LIST_ITEMS as u64 {
                e.u64(i)?;
            }
            e.u64(1)?.u64(16384)?;
            e.u64(2)?.u64(16)?;
            e.u64(3)?.array(0)?;
            e.u64(4)?.array(0)?;
            Ok(())
        });
        assert_eq!(
            Hello::decode(&bytes).unwrap().versions.len(),
            limits::MAX_LIST_ITEMS
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = ErrorHeader::new(ErrorCode::Rejected).encode();
        bytes.push(0xff);
        assert_eq!(
            ErrorHeader::decode(&bytes).unwrap_err(),
            HeaderError::TrailingBytes
        );
    }

    #[test]
    fn truncated_headers_are_rejected() {
        let full = DataHeader::addressed("/t").encode();
        for cut in 0..full.len() {
            assert!(
                DataHeader::decode(&full[..cut]).is_err(),
                "prefix of {cut} bytes must not decode"
            );
        }
    }

    #[test]
    fn empty_input_is_rejected_for_every_header() {
        assert!(Hello::decode(&[]).is_err());
        assert!(DataHeader::decode(&[]).is_err());
        assert!(ErrorHeader::decode(&[]).is_err());
        assert!(SubscriptionHeader::decode(&[]).is_err());
    }

    #[test]
    fn tags_are_rejected() {
        // Key 50 is unknown, so the value goes through `skip_value`, which is
        // where the tag rule lives.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(50)?.tag(minicbor::data::IanaTag::Cbor)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed("tags are not allowed")
        );
    }

    // --- reserved value passthrough ---------------------------------------

    #[test]
    fn unknown_error_codes_survive_decoding() {
        let err = ErrorHeader {
            code: 99,
            message: None,
        };
        let decoded = ErrorHeader::decode(&err.encode()).unwrap();
        assert_eq!(decoded.code, 99);
        assert_eq!(decoded.error_code(), None);
    }

    #[test]
    fn header_errors_become_protocol_errors() {
        let e: Error = HeaderError::DuplicateKey(3).into();
        assert!(matches!(e, Error::Protocol(_)));
        assert!(e.to_string().contains("duplicate header key 3"));
    }
}
