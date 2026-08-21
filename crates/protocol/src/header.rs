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

use std::convert::Infallible;

use minicbor::data::Type;
use minicbor::{Decoder, Encoder};
use weida_core::policy::{AckMode, Role};
use weida_core::{Error, TransferId};

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
    pub const TRANSFER_ID: u64 = 1;
    pub const ROLE: u64 = 2;
    pub const CORRELATION_ID: u64 = 3;
    pub const ACK_MODE: u64 = 4;
    pub const CONTENT_LEN: u64 = 5;
    pub const CONTENT_TYPE: u64 = 6;
    pub const TRACEPARENT: u64 = 7;
    pub const TRACESTATE: u64 = 8;
}

/// ACK keys.
mod ack_key {
    pub const RE: u64 = 0;
    pub const STATE: u64 = 1;
}

/// ERROR keys.
mod error_key {
    pub const RE: u64 = 0;
    pub const CODE: u64 = 1;
    pub const MESSAGE: u64 = 2;
}

/// CANCEL keys.
mod cancel_key {
    pub const ID: u64 = 0;
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
    /// A transfer id was the reserved `0`.
    ZeroTransferId(u64),
    /// Bytes remained after the header map.
    TrailingBytes,
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
            HeaderError::ZeroTransferId(k) => write!(f, "key {k}: transfer id 0 is reserved"),
            HeaderError::TrailingBytes => f.write_str("trailing bytes after the header"),
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

    fn transfer_id(&mut self, key: u64) -> Result<TransferId, HeaderError> {
        let raw = self.u64()?;
        TransferId::new(raw).ok_or(HeaderError::ZeroTransferId(key))
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
/// `role` and `ack_mode` are kept as raw wire values so that reserved codes
/// survive decoding and can be answered with `UNSUPPORTED` instead of being
/// silently reinterpreted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataHeader {
    /// Endpoint path; present iff `role` is `request`.
    pub endpoint: Option<String>,
    /// Sender's transfer id.
    pub transfer_id: TransferId,
    /// Raw role code.
    pub role: u64,
    /// Request id this reply answers; present iff `role` is `reply`.
    pub correlation_id: Option<TransferId>,
    /// Raw ack mode code.
    pub ack_mode: u64,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// W3C `traceparent`.
    pub traceparent: Option<String>,
    /// W3C `tracestate`, opaque passthrough.
    pub tracestate: Option<String>,
}

impl DataHeader {
    /// A request header with no optional metadata.
    pub fn request(
        endpoint: impl Into<String>,
        transfer_id: TransferId,
        ack_mode: AckMode,
    ) -> DataHeader {
        DataHeader {
            endpoint: Some(endpoint.into()),
            transfer_id,
            role: Role::Request.to_wire(),
            correlation_id: None,
            ack_mode: ack_mode.to_wire(),
            content_len: None,
            content_type: None,
            traceparent: None,
            tracestate: None,
        }
    }

    /// A reply header with no optional metadata.
    pub fn reply(
        transfer_id: TransferId,
        correlation_id: TransferId,
        ack_mode: AckMode,
    ) -> DataHeader {
        DataHeader {
            endpoint: None,
            transfer_id,
            role: Role::Reply.to_wire(),
            correlation_id: Some(correlation_id),
            ack_mode: ack_mode.to_wire(),
            content_len: None,
            content_type: None,
            traceparent: None,
            tracestate: None,
        }
    }

    /// The role, or `None` for a reserved or unknown code.
    pub fn role(&self) -> Option<Role> {
        Role::from_wire(self.role)
    }

    /// The ack mode, or `None` for a reserved or unknown code.
    pub fn ack_mode(&self) -> Option<AckMode> {
        AckMode::from_wire(self.ack_mode)
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        // `ack_mode` defaults to 0 when absent, so the zero value is omitted
        // rather than written out (`docs/PROTOCOL.md` §5). `role` is always
        // written: it selects request, reply or a reserved behaviour.
        let count = 2
            + u64::from(self.endpoint.is_some())
            + u64::from(self.correlation_id.is_some())
            + u64::from(self.ack_mode != weida_core::policy::ACK_MODE_NONE)
            + u64::from(self.content_len.is_some())
            + u64::from(self.content_type.is_some())
            + u64::from(self.traceparent.is_some())
            + u64::from(self.tracestate.is_some());
        encode_with(|e| {
            e.map(count)?;
            if let Some(endpoint) = &self.endpoint {
                e.u64(data_key::ENDPOINT)?.str(endpoint)?;
            }
            e.u64(data_key::TRANSFER_ID)?.u64(self.transfer_id.get())?;
            e.u64(data_key::ROLE)?.u64(self.role)?;
            if let Some(id) = self.correlation_id {
                e.u64(data_key::CORRELATION_ID)?.u64(id.get())?;
            }
            if self.ack_mode != weida_core::policy::ACK_MODE_NONE {
                e.u64(data_key::ACK_MODE)?.u64(self.ack_mode)?;
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
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<DataHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut endpoint = None;
        let mut transfer_id = None;
        // Absent `ack_mode` means `0 = none`; `role` has no default and is
        // required.
        let mut role = 0;
        let mut correlation_id = None;
        let mut ack_mode = weida_core::policy::ACK_MODE_NONE;
        let mut content_len = None;
        let mut content_type = None;
        let mut traceparent = None;
        let mut tracestate = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    data_key::ENDPOINT => endpoint = Some(m.text(key, limits::MAX_ENDPOINT_BYTES)?),
                    data_key::TRANSFER_ID => transfer_id = Some(m.transfer_id(key)?),
                    data_key::ROLE => role = m.u64()?,
                    data_key::CORRELATION_ID => correlation_id = Some(m.transfer_id(key)?),
                    data_key::ACK_MODE => ack_mode = m.u64()?,
                    data_key::CONTENT_LEN => content_len = Some(m.u64()?),
                    data_key::CONTENT_TYPE => {
                        content_type = Some(m.text(key, limits::MAX_CONTENT_TYPE_BYTES)?)
                    }
                    data_key::TRACEPARENT => {
                        traceparent = Some(m.text(key, limits::MAX_TRACEPARENT_BYTES)?)
                    }
                    data_key::TRACESTATE => {
                        tracestate = Some(m.text(key, limits::MAX_TRACESTATE_BYTES)?)
                    }
                    _ => m.skip()?,
                }
            }
            m.require(data_key::TRANSFER_ID)?;
            m.require(data_key::ROLE)?;
            // Conditional requirements. A reserved role requires neither, and
            // the receiver answers it with UNSUPPORTED.
            if role == Role::Request.to_wire() {
                m.require(data_key::ENDPOINT)?;
            }
            if role == Role::Reply.to_wire() {
                m.require(data_key::CORRELATION_ID)?;
            }
        }
        finish(&d)?;
        Ok(DataHeader {
            endpoint,
            transfer_id: transfer_id.expect("presence checked above"),
            role,
            correlation_id,
            ack_mode,
            content_len,
            content_type,
            traceparent,
            tracestate,
        })
    }
}

/// ACK header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckHeader {
    /// The recipient's outgoing transfer id being acknowledged.
    pub re: TransferId,
    /// Raw ack state code.
    pub state: u64,
}

impl AckHeader {
    /// An `accepted` acknowledgement.
    pub fn accepted(re: TransferId) -> AckHeader {
        AckHeader {
            re,
            state: weida_core::policy::ACK_STATE_ACCEPTED,
        }
    }

    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        encode_with(|e| {
            e.map(2)?;
            e.u64(ack_key::RE)?.u64(self.re.get())?;
            e.u64(ack_key::STATE)?.u64(self.state)?;
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<AckHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut re = None;
        let mut state = 0;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    ack_key::RE => re = Some(m.transfer_id(key)?),
                    ack_key::STATE => state = m.u64()?,
                    _ => m.skip()?,
                }
            }
            m.require(ack_key::RE)?;
            m.require(ack_key::STATE)?;
        }
        finish(&d)?;
        Ok(AckHeader {
            re: re.expect("presence checked above"),
            state,
        })
    }
}

/// ERROR header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorHeader {
    /// The recipient's outgoing transfer id this error refers to.
    pub re: TransferId,
    /// Raw error code.
    pub code: u64,
    /// Human-readable detail; never machine-interpreted.
    pub message: Option<String>,
}

impl ErrorHeader {
    /// Builds a header for a known error code.
    pub fn new(re: TransferId, code: weida_core::ErrorCode) -> ErrorHeader {
        ErrorHeader {
            re,
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
        let count = 2 + u64::from(self.message.is_some());
        encode_with(|e| {
            e.map(count)?;
            e.u64(error_key::RE)?.u64(self.re.get())?;
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
        let mut re = None;
        let mut code = 0;
        let mut message = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    error_key::RE => re = Some(m.transfer_id(key)?),
                    error_key::CODE => code = m.u64()?,
                    error_key::MESSAGE => message = Some(m.text(key, limits::MAX_MESSAGE_BYTES)?),
                    _ => m.skip()?,
                }
            }
            m.require(error_key::RE)?;
            m.require(error_key::CODE)?;
        }
        finish(&d)?;
        Ok(ErrorHeader {
            re: re.expect("presence checked above"),
            code,
            message,
        })
    }
}

/// CANCEL header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelHeader {
    /// The sender's own request transfer id whose replies are no longer wanted.
    pub id: TransferId,
}

impl CancelHeader {
    /// Encodes the header.
    pub fn encode(&self) -> Vec<u8> {
        encode_with(|e| {
            e.map(1)?;
            e.u64(cancel_key::ID)?.u64(self.id.get())?;
            Ok(())
        })
    }

    /// Decodes the header.
    pub fn decode(bytes: &[u8]) -> Result<CancelHeader, HeaderError> {
        let mut d = Decoder::new(bytes);
        let mut id = None;
        {
            let mut m = MapReader::new(&mut d)?;
            while let Some(key) = m.next_key()? {
                match key {
                    cancel_key::ID => id = Some(m.transfer_id(key)?),
                    _ => m.skip()?,
                }
            }
            m.require(cancel_key::ID)?;
        }
        finish(&d)?;
        Ok(CancelHeader {
            id: id.expect("presence checked above"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_core::ErrorCode;

    fn tid(v: u64) -> TransferId {
        TransferId::new(v).unwrap()
    }

    // --- golden vectors, docs/PROTOCOL.md §8 ------------------------------

    #[test]
    fn golden_data_header() {
        let h = DataHeader::request("/t", tid(1), AckMode::Accepted);
        let bytes = h.encode();
        assert_eq!(
            bytes,
            vec![
                0xA4, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x01, 0x02, 0x01, 0x04, 0x01
            ]
        );
        assert_eq!(bytes.len(), 0x0B);
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
    fn golden_ack_header() {
        let h = AckHeader::accepted(tid(1));
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA2, 0x00, 0x01, 0x01, 0x01]);
        assert_eq!(bytes.len(), 0x05);
        assert_eq!(AckHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn golden_cancel_header() {
        let h = CancelHeader { id: tid(1) };
        let bytes = h.encode();
        assert_eq!(bytes, vec![0xA1, 0x00, 0x01]);
        assert_eq!(bytes.len(), 0x03);
        assert_eq!(CancelHeader::decode(&bytes).unwrap(), h);
    }

    // --- roundtrips -------------------------------------------------------

    #[test]
    fn data_header_roundtrip_with_every_field() {
        let h = DataHeader {
            endpoint: Some("/transform".into()),
            transfer_id: tid(9),
            role: 1,
            correlation_id: None,
            ack_mode: 1,
            content_len: Some(1 << 40),
            content_type: Some("application/octet-stream".into()),
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
        };
        assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn reply_header_roundtrip() {
        let h = DataHeader::reply(tid(4), tid(7), AckMode::None);
        let bytes = h.encode();
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
        assert_eq!(h.role(), Some(Role::Reply));
        assert_eq!(h.ack_mode(), Some(AckMode::None));
    }

    #[test]
    fn error_header_roundtrip_with_and_without_message() {
        let bare = ErrorHeader::new(tid(3), ErrorCode::UnknownEndpoint);
        assert_eq!(ErrorHeader::decode(&bare.encode()).unwrap(), bare);
        assert_eq!(bare.error_code(), Some(ErrorCode::UnknownEndpoint));

        let with_msg = ErrorHeader {
            re: tid(3),
            code: 4,
            message: Some("handler panicked".into()),
        };
        assert_eq!(ErrorHeader::decode(&with_msg.encode()).unwrap(), with_msg);
    }

    #[test]
    fn keys_are_emitted_in_ascending_order() {
        let h = DataHeader {
            endpoint: Some("/x".into()),
            transfer_id: tid(1),
            role: 1,
            correlation_id: Some(tid(2)),
            ack_mode: 1,
            content_len: Some(1),
            content_type: Some("t".into()),
            traceparent: Some("p".into()),
            tracestate: Some("s".into()),
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

    // --- defaults ---------------------------------------------------------

    #[test]
    fn absent_ack_mode_decodes_as_none() {
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(0)?.str("/t")?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            Ok(())
        });
        let h = DataHeader::decode(&bytes).unwrap();
        assert_eq!(h.ack_mode, 0);
        assert_eq!(h.ack_mode(), Some(AckMode::None));
    }

    #[test]
    fn absent_role_is_rejected() {
        // `role` has no default: request, reply and the reserved codes are
        // materially different behaviours, so it must be stated explicitly.
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(1)?.u64(4)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(data_key::ROLE)
        );
    }

    #[test]
    fn default_values_are_omitted_by_the_encoder() {
        let h = DataHeader::request("/t", tid(1), AckMode::None);
        let bytes = h.encode();
        // Only endpoint, transfer_id and role are written.
        assert_eq!(
            bytes,
            vec![0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x01, 0x02, 0x01]
        );
        assert_eq!(DataHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn an_explicit_default_value_is_still_accepted() {
        let bytes = encode_with(|e| {
            e.map(4)?;
            e.u64(0)?.str("/t")?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(0)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap(),
            DataHeader::request("/t", tid(1), AckMode::None)
        );
    }

    // --- forward compatibility -------------------------------------------

    #[test]
    fn unknown_keys_are_skipped() {
        // Re-encode the golden DATA header with an extra key 63 holding a
        // nested structure, and check it still decodes to the same value.
        let h = DataHeader::request("/t", tid(1), AckMode::Accepted);
        let extended = encode_with(|e| {
            e.map(5)?;
            e.u64(0)?.str("/t")?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(1)?;
            e.u64(63)?.array(2)?.u64(7)?.map(1)?.u64(1)?.bool(true)?;
            Ok(())
        });
        assert_eq!(DataHeader::decode(&extended).unwrap(), h);
    }

    #[test]
    fn unknown_keys_above_the_reserved_range_are_skipped() {
        let extended = encode_with(|e| {
            e.map(5)?;
            e.u64(1)?.u64(5)?;
            e.u64(2)?.u64(2)?;
            e.u64(3)?.u64(9)?;
            e.u64(4)?.u64(0)?;
            e.u64(1000)?.str("future")?;
            Ok(())
        });
        let h = DataHeader::decode(&extended).unwrap();
        assert_eq!(h.transfer_id, tid(5));
        assert_eq!(h.correlation_id, Some(tid(9)));
    }

    #[test]
    fn skipping_tolerates_nesting_up_to_the_depth_limit() {
        for depth in [1usize, limits::MAX_SKIP_DEPTH] {
            let bytes = encode_with(|e| {
                e.map(4)?;
                e.u64(1)?.u64(1)?;
                e.u64(2)?.u64(1)?;
                e.u64(4)?.u64(0)?;
                e.u64(50)?;
                for _ in 0..depth {
                    e.array(1)?;
                }
                e.u64(1)?;
                Ok(())
            });
            // role=request without an endpoint is rejected, so use role=reply
            // free headers: this header has role=1 and no endpoint, hence the
            // expected MissingKey. Depth handling is what matters here.
            let err = DataHeader::decode(&bytes).unwrap_err();
            assert_eq!(
                err,
                HeaderError::MissingKey(data_key::ENDPOINT),
                "depth {depth}"
            );
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
            e.map(5)?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(2)?;
            e.u64(3)?.u64(1)?;
            e.u64(4)?.u64(1)?;
            e.u64(9)?.array(64)?;
            for i in 0..64u64 {
                e.u64(i)?;
            }
            Ok(())
        });
        assert_eq!(DataHeader::decode(&bytes).unwrap().transfer_id, tid(1));
    }

    // --- strictness -------------------------------------------------------

    #[test]
    fn duplicate_keys_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(4)?;
            e.u64(1)?.u64(1)?;
            e.u64(1)?.u64(2)?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(0)?;
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
            e.str("transfer_id")?.u64(1)?;
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
            e.map(3)?;
            e.u64(1)?.str("not a number")?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(0)?;
            Ok(())
        });
        assert!(matches!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed(_)
        ));
    }

    #[test]
    fn missing_required_keys_are_rejected() {
        // No transfer_id.
        let bytes = encode_with(|e| {
            e.map(2)?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(0)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(data_key::TRANSFER_ID)
        );

        // Request without an endpoint.
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(1)?;
            e.u64(4)?.u64(0)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(data_key::ENDPOINT)
        );

        // Reply without a correlation id.
        let bytes = encode_with(|e| {
            e.map(3)?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(2)?;
            e.u64(4)?.u64(0)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::MissingKey(data_key::CORRELATION_ID)
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

    #[test]
    fn zero_transfer_ids_are_rejected() {
        for key in [data_key::TRANSFER_ID, data_key::CORRELATION_ID] {
            let bytes = encode_with(|e| {
                e.map(3)?;
                e.u64(key)?.u64(0)?;
                e.u64(2)?.u64(2)?;
                e.u64(4)?.u64(0)?;
                Ok(())
            });
            assert_eq!(
                DataHeader::decode(&bytes).unwrap_err(),
                HeaderError::ZeroTransferId(key)
            );
        }
        let ack = encode_with(|e| {
            e.map(2)?;
            e.u64(0)?.u64(0)?;
            e.u64(1)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            AckHeader::decode(&ack).unwrap_err(),
            HeaderError::ZeroTransferId(0)
        );
        let cancel = encode_with(|e| {
            e.map(1)?;
            e.u64(0)?.u64(0)?;
            Ok(())
        });
        assert_eq!(
            CancelHeader::decode(&cancel).unwrap_err(),
            HeaderError::ZeroTransferId(0)
        );
    }

    #[test]
    fn oversized_strings_are_rejected_per_field() {
        // Built through the encoder, which emits keys in ascending order.
        let with_text = |key: u64, text: String| -> Vec<u8> {
            let mut h = DataHeader::reply(tid(1), tid(2), AckMode::None);
            match key {
                data_key::ENDPOINT => h.endpoint = Some(text),
                data_key::CONTENT_TYPE => h.content_type = Some(text),
                data_key::TRACEPARENT => h.traceparent = Some(text),
                data_key::TRACESTATE => h.tracestate = Some(text),
                other => panic!("key {other} is not a text field"),
            }
            h.encode()
        };
        let cases: [(u64, usize); 4] = [
            (data_key::ENDPOINT, limits::MAX_ENDPOINT_BYTES),
            (data_key::CONTENT_TYPE, limits::MAX_CONTENT_TYPE_BYTES),
            (data_key::TRACEPARENT, limits::MAX_TRACEPARENT_BYTES),
            (data_key::TRACESTATE, limits::MAX_TRACESTATE_BYTES),
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
            e.u64(2)?.u64(2)?;
            e.u64(1)?.u64(1)?;
            e.u64(3)?.u64(1)?;
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
            e.map(5)?;
            e.u64(1)?.u64(1)?;
            e.u64(2)?.u64(2)?;
            e.u64(3)?.u64(1)?;
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
            e.map(3)?;
            e.u64(0)?.u64(1)?;
            e.u64(1)?.u64(2)?;
            e.u64(2)?.str(&big)?;
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
        let mut bytes = CancelHeader { id: tid(1) }.encode();
        bytes.push(0xff);
        assert_eq!(
            CancelHeader::decode(&bytes).unwrap_err(),
            HeaderError::TrailingBytes
        );
    }

    #[test]
    fn truncated_headers_are_rejected() {
        let full = DataHeader::request("/t", tid(1), AckMode::Accepted).encode();
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
        assert!(AckHeader::decode(&[]).is_err());
        assert!(ErrorHeader::decode(&[]).is_err());
        assert!(CancelHeader::decode(&[]).is_err());
    }

    #[test]
    fn tags_are_rejected() {
        let bytes = encode_with(|e| {
            e.map(1)?;
            e.u64(9)?.tag(minicbor::data::IanaTag::Cbor)?.u64(1)?;
            Ok(())
        });
        assert_eq!(
            DataHeader::decode(&bytes).unwrap_err(),
            HeaderError::Malformed("tags are not allowed")
        );
    }

    // --- reserved value passthrough ---------------------------------------

    #[test]
    fn reserved_roles_and_ack_modes_survive_decoding() {
        for role in [0u64, 3, 99] {
            for ack in [2u64, 3, 4, 77] {
                let bytes = encode_with(|e| {
                    e.map(3)?;
                    e.u64(1)?.u64(1)?;
                    e.u64(2)?.u64(role)?;
                    e.u64(4)?.u64(ack)?;
                    Ok(())
                });
                let h = DataHeader::decode(&bytes).unwrap();
                assert_eq!(h.role, role);
                assert_eq!(h.ack_mode, ack);
                assert_eq!(h.role(), None, "role {role} must not be interpreted");
                assert_eq!(h.ack_mode(), None, "ack mode {ack} must not be interpreted");
            }
        }
    }

    #[test]
    fn unknown_ack_states_and_error_codes_survive_decoding() {
        let ack = AckHeader {
            re: tid(1),
            state: 42,
        };
        assert_eq!(AckHeader::decode(&ack.encode()).unwrap().state, 42);

        let err = ErrorHeader {
            re: tid(1),
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
