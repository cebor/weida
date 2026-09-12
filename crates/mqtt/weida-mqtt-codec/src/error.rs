//! Why a control packet was rejected, in MQTT's own vocabulary.
//!
//! MQTT names exactly two classes of wire fault and gives each a reason code:
//! a **Malformed Packet** (0x81) is "a Control Packet that cannot be parsed
//! according to this specification", and a **Protocol Error** (0x82) is one
//! that parses but breaks a rule (2.4, 4.13) [mqtt5 §1]. A receiver that
//! detects either MUST close the connection if it is a server and SHOULD if it
//! is a client ([MQTT-4.13.1-1]) [mqtt5 §1]. So a decode failure here is not a
//! diagnostic, it is a verdict about the connection, and
//! [`DecodeError::reason_code`] is the byte a caller puts in the DISCONNECT it
//! sends before closing.
//!
//! One variant is deliberately **not** a verdict. [`DecodeError::Incomplete`]
//! means "read more bytes": MQTT runs on a byte stream, a packet may straddle
//! any number of reads, and over WebSocket a frame may even hold a partial
//! control packet, with receivers forbidden to assume packet/frame alignment
//! ([MQTT-6.0.0-2]) [mqtt5 §3]. [`DecodeError::is_violation`] is the one
//! question a reader loop asks.
//!
//! Encoding fails for different reasons, and all of them are the local
//! caller's fault rather than a peer's: a string that does not fit its
//! two-byte length prefix, a packet above the peer's declared `Maximum Packet
//! Size`, or a property that the packet type does not permit. Those are
//! [`EncodeError`], and none of them has a reason code, because nothing is put
//! on the wire.

use std::fmt;

use crate::property::PropertyId;
use crate::types::PacketType;

/// The Malformed Packet reason code (2.4) [mqtt5 §1].
pub const MALFORMED_PACKET: u8 = 0x81;
/// The Protocol Error reason code (2.4) [mqtt5 §1].
pub const PROTOCOL_ERROR: u8 = 0x82;
/// The Packet Too Large reason code (2.4) [mqtt5 §1].
pub const PACKET_TOO_LARGE: u8 = 0x95;
/// The Unsupported Protocol Version reason code (2.4) [mqtt5 §1].
pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
/// The Implementation Specific Error reason code (2.4) [mqtt5 §1].
pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;

/// Why a control packet could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The input does not hold the whole packet yet. Not a violation: read
    /// more bytes and try again.
    Incomplete,

    /// A Variable Byte Integer used more bytes than the value needs. "The
    /// encoded value MUST use the minimum number of bytes necessary to
    /// represent the value" ([MQTT-1.5.5-1]) [mqtt5 §3].
    VarintNotMinimal,
    /// A Variable Byte Integer ran past its fourth byte, so it is not one: the
    /// encoding is 1 to 4 bytes and stops at 268,435,455 (1.5.5) [mqtt5 §3].
    VarintTooLong,

    /// A UTF-8 Encoded String was not well-formed UTF-8 ([MQTT-1.5.4-1])
    /// [mqtt5 §3].
    MalformedUtf8,
    /// A UTF-8 Encoded String contained U+0000, which "MUST NOT" appear
    /// ([MQTT-1.5.4-2]) [mqtt5 §3].
    NullCharacter,

    /// Packet type 0 is Reserved and Forbidden (2.1.2) [mqtt5 §3].
    ReservedPacketType,
    /// The four low bits of the fixed header are not what this packet type
    /// requires. "Where a flag bit is marked as 'Reserved', it is reserved for
    /// future use and MUST be set to the value listed" ([MQTT-2.1.3-1])
    /// [mqtt5 §3].
    InvalidFlags {
        /// The packet type whose flags were wrong.
        packet_type: PacketType,
        /// The four low bits that were found.
        flags: u8,
    },
    /// The declared packet size exceeds the maximum the caller supplied. The
    /// answer on the wire is DISCONNECT 0x95 (3.1.2.11.4) [mqtt5 §5].
    PacketTooLarge {
        /// Total packet bytes the fixed header declared.
        size: u32,
        /// The caller's ceiling.
        max: u32,
    },
    /// The packet's body held bytes the grammar does not account for: the
    /// Remaining Length was larger than the fields needed.
    TrailingBytes {
        /// How many bytes were left over.
        len: usize,
    },
    /// A field ran past the packet's declared Remaining Length. All of the
    /// declared bytes had arrived, so this is the packet contradicting its own
    /// header and not a request for more — reporting
    /// [`DecodeError::Incomplete`] here would park a reader on bytes the peer
    /// already finished sending.
    PacketLengthMismatch,
    /// A packet type this build of the codec does not decode. Answered with
    /// 0x83 (Implementation specific error), which is the code 2.4 provides
    /// for exactly this: "the packet is valid but is not accepted by this
    /// receiver" [mqtt5 §1].
    UnimplementedPacketType {
        /// The type that arrived.
        packet_type: PacketType,
    },

    /// A property identifier is not one of the 27 the specification defines
    /// (2.2.2.2) [mqtt5 §3].
    UnknownProperty {
        /// The identifier that was read.
        id: u32,
    },
    /// A defined property identifier appeared in a packet type that does not
    /// carry it (2.2.2.2) [mqtt5 §3].
    PropertyNotAllowed {
        /// The property.
        id: PropertyId,
        /// The packet type it appeared in. `None` means the CONNECT payload's
        /// Will Properties, which is a property set without a packet type of
        /// its own (3.1.3.2) [mqtt5 §3].
        packet_type: Option<PacketType>,
    },
    /// A property appeared twice. "Repetition is a Protocol Error for every
    /// property except `User Property` and `Subscription Identifier`"
    /// [mqtt5 §3].
    DuplicateProperty {
        /// The property that repeated.
        id: PropertyId,
    },
    /// A property's value is outside the range the specification gives it —
    /// `Receive Maximum` 0, `Maximum Packet Size` 0, a boolean-valued property
    /// that is neither 0 nor 1, `Topic Alias` 0, `Subscription Identifier` 0,
    /// or `Maximum QoS` above 1 [mqtt5 §11].
    InvalidPropertyValue {
        /// The property.
        id: PropertyId,
        /// The value as a 32-bit number, whatever its wire width was.
        value: u32,
    },
    /// The property block's declared length does not match the bytes it
    /// contains (2.2.2.1) [mqtt5 §3].
    PropertyLengthMismatch,

    /// The CONNECT variable header did not begin with the four-byte string
    /// `MQTT`, which sits at a fixed offset and length and "will not be
    /// changed by future versions" (3.1.2.1) [mqtt5 §0].
    ProtocolNameInvalid,
    /// The Protocol Version byte was not 5. A server answers CONNACK 0x84
    /// (3.1.2.2) [mqtt5 §1].
    UnsupportedProtocolVersion {
        /// The version byte that was found.
        version: u8,
    },
    /// The reserved bit 0 of the CONNECT flags byte was set. "The Server MUST
    /// validate that the reserved flag ... is set to 0" ([MQTT-3.1.2-3])
    /// (3.1.2.3) [mqtt5 §1].
    ReservedConnectFlag,
    /// A QoS field held 3, which no packet permits (3.1.2.6, 3.3.1.2)
    /// [mqtt5 §6].
    InvalidQos {
        /// The two bits that were found.
        qos: u8,
    },
    /// Will Flag was 0 but Will QoS or Will Retain was not. "If the Will Flag
    /// is set to 0, then the Will QoS MUST be set to 0" ([MQTT-3.1.2-11]) and
    /// "the Will Retain Flag MUST be set to 0" ([MQTT-3.1.2-13]) (3.1.2.6,
    /// 3.1.2.7) [mqtt5 §1].
    WillFlagsWithoutWill,
    /// `Authentication Data` appeared without `Authentication Method`. "It is
    /// a Protocol Error to include Authentication Data if there is no
    /// Authentication Method" ([MQTT-3.1.2-33]) (3.1.2.11.10) [mqtt5 §1].
    AuthenticationDataWithoutMethod,
    /// A CONNACK carried a non-zero Reason Code together with a set Session
    /// Present flag. "If a Server sends a CONNACK packet containing a non-zero
    /// Reason Code it MUST set Session Present to 0" ([MQTT-3.2.2-6])
    /// (3.2.2.1.1) [mqtt5 §1].
    SessionPresentWithError {
        /// The reason code that was carried.
        reason_code: u8,
    },
    /// The acknowledge-flags byte of a CONNACK had a bit other than bit 0 set.
    /// "The remaining bits ... are reserved for future use. The Server MUST
    /// set all of them to 0" ([MQTT-3.2.2-1]) (3.2.2.1) [mqtt5 §1].
    ReservedConnackFlag {
        /// The byte that was found.
        flags: u8,
    },
    /// A reason code is not one the specification lists for this packet type
    /// (2.4) [mqtt5 §1].
    InvalidReasonCode {
        /// The packet type the code appeared in.
        packet_type: PacketType,
        /// The byte that was found.
        code: u8,
    },
}

impl DecodeError {
    /// Whether the connection is finished.
    ///
    /// False only for [`DecodeError::Incomplete`], which asks for more bytes.
    /// Every other variant is a Malformed Packet or a Protocol Error, and both
    /// oblige a server to close and advise a client to ([MQTT-4.13.1-1])
    /// (4.13.1) [mqtt5 §1].
    #[must_use]
    pub const fn is_violation(&self) -> bool {
        !matches!(self, DecodeError::Incomplete)
    }

    /// The reason code to put in a DISCONNECT, or `None` where there is
    /// nothing to report.
    ///
    /// The split between 0x81 and 0x82 is the specification's own: a packet
    /// that cannot be parsed is malformed, one that parses and breaks a rule
    /// is a Protocol Error (2.4) [mqtt5 §1]. Two variants map elsewhere
    /// because the specification gives them their own codes: an oversized
    /// packet is 0x95 and a bad Protocol Version is 0x84, which a server puts
    /// on a CONNACK rather than a DISCONNECT (3.2.2.2) [mqtt5 §1].
    #[must_use]
    pub const fn reason_code(&self) -> Option<u8> {
        match self {
            DecodeError::Incomplete => None,

            DecodeError::PacketTooLarge { .. } => Some(PACKET_TOO_LARGE),
            DecodeError::UnsupportedProtocolVersion { .. } => Some(UNSUPPORTED_PROTOCOL_VERSION),
            DecodeError::UnimplementedPacketType { .. } => Some(IMPLEMENTATION_SPECIFIC_ERROR),

            // Parses, then breaks a rule.
            DecodeError::DuplicateProperty { .. }
            | DecodeError::InvalidPropertyValue { .. }
            | DecodeError::ReservedConnectFlag
            | DecodeError::WillFlagsWithoutWill
            | DecodeError::AuthenticationDataWithoutMethod
            | DecodeError::SessionPresentWithError { .. }
            | DecodeError::ReservedConnackFlag { .. } => Some(PROTOCOL_ERROR),

            // Cannot be parsed as this specification describes.
            _ => Some(MALFORMED_PACKET),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Incomplete => f.write_str("incomplete packet: more bytes needed"),
            DecodeError::VarintNotMinimal => {
                f.write_str("variable byte integer is not minimally encoded")
            }
            DecodeError::VarintTooLong => {
                f.write_str("variable byte integer is longer than four bytes")
            }
            DecodeError::MalformedUtf8 => f.write_str("string is not well-formed UTF-8"),
            DecodeError::NullCharacter => f.write_str("string contains U+0000"),
            DecodeError::ReservedPacketType => f.write_str("packet type 0 is reserved"),
            DecodeError::InvalidFlags { packet_type, flags } => {
                write!(
                    f,
                    "{packet_type} has invalid fixed-header flags 0x{flags:X}"
                )
            }
            DecodeError::PacketTooLarge { size, max } => {
                write!(f, "packet of {size} bytes exceeds the maximum of {max}")
            }
            DecodeError::TrailingBytes { len } => {
                write!(f, "{len} trailing bytes after the packet body")
            }
            DecodeError::PacketLengthMismatch => {
                f.write_str("a field runs past the declared remaining length")
            }
            DecodeError::UnimplementedPacketType { packet_type } => {
                write!(f, "{packet_type} is not decoded by this codec")
            }
            DecodeError::UnknownProperty { id } => write!(f, "unknown property identifier {id}"),
            DecodeError::PropertyNotAllowed { id, packet_type } => match packet_type {
                Some(packet_type) => write!(f, "property {id} is not carried by {packet_type}"),
                None => write!(f, "property {id} is not a will property"),
            },
            DecodeError::DuplicateProperty { id } => write!(f, "property {id} appears twice"),
            DecodeError::InvalidPropertyValue { id, value } => {
                write!(f, "property {id} has invalid value {value}")
            }
            DecodeError::PropertyLengthMismatch => {
                f.write_str("property length does not match the properties")
            }
            DecodeError::ProtocolNameInvalid => f.write_str("protocol name is not \"MQTT\""),
            DecodeError::UnsupportedProtocolVersion { version } => {
                write!(f, "protocol version {version} is not 5")
            }
            DecodeError::ReservedConnectFlag => f.write_str("reserved connect flag is set"),
            DecodeError::InvalidQos { qos } => write!(f, "QoS {qos} is not 0, 1 or 2"),
            DecodeError::WillFlagsWithoutWill => {
                f.write_str("will QoS or will retain set without a will")
            }
            DecodeError::AuthenticationDataWithoutMethod => {
                f.write_str("authentication data without an authentication method")
            }
            DecodeError::SessionPresentWithError { reason_code } => write!(
                f,
                "session present with reason code 0x{reason_code:02X}, which must be 0x00"
            ),
            DecodeError::ReservedConnackFlag { flags } => {
                write!(f, "reserved connack flag bits set in 0x{flags:02X}")
            }
            DecodeError::InvalidReasonCode { packet_type, code } => {
                write!(f, "0x{code:02X} is not a {packet_type} reason code")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Why a control packet could not be encoded.
///
/// Every variant is the local caller's fault. Nothing is written to `out`
/// before the whole packet has been found encodable, so a failed encode leaves
/// the output buffer exactly as it was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// A UTF-8 Encoded String or Binary Data field does not fit its two-byte
    /// length prefix, which caps it at 65,535 bytes (1.5.4, 1.5.6)
    /// [mqtt5 §3].
    FieldTooLong {
        /// The length that was offered.
        len: usize,
    },
    /// The packet does not fit the Remaining Length encoding, whose ceiling is
    /// 268,435,455 bytes (2.1.4) [mqtt5 §3].
    PacketTooLong {
        /// The body length that was computed.
        len: u64,
    },
    /// The packet exceeds the peer's declared `Maximum Packet Size`. "The
    /// Client MUST NOT send packets exceeding Maximum Packet Size to the
    /// Server" ([MQTT-3.2.2-15]) (3.2.2.3.6) [mqtt5 §5], so this is refused
    /// here rather than discovered from the peer's DISCONNECT 0x95.
    PacketTooLarge {
        /// Total packet bytes.
        size: u32,
        /// The peer's ceiling.
        max: u32,
    },
    /// A property was set that this packet type does not carry (2.2.2.2)
    /// [mqtt5 §3].
    PropertyNotAllowed {
        /// The property.
        id: PropertyId,
    },
    /// A property value is outside the range the specification gives it. The
    /// mirror of [`DecodeError::InvalidPropertyValue`], checked before the
    /// value reaches the wire.
    InvalidPropertyValue {
        /// The property.
        id: PropertyId,
        /// The value as a 32-bit number.
        value: u32,
    },
    /// A Subscription Identifier outside 1..=268,435,455 (3.8.2.1.2)
    /// [mqtt5 §11].
    InvalidSubscriptionIdentifier {
        /// The value that was offered.
        value: u32,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::FieldTooLong { len } => {
                write!(f, "field of {len} bytes exceeds the 65,535-byte bound")
            }
            EncodeError::PacketTooLong { len } => {
                write!(
                    f,
                    "body of {len} bytes exceeds the 268,435,455-byte ceiling"
                )
            }
            EncodeError::PacketTooLarge { size, max } => {
                write!(
                    f,
                    "packet of {size} bytes exceeds the peer's maximum of {max}"
                )
            }
            EncodeError::PropertyNotAllowed { id } => {
                write!(f, "property {id} is not carried by this packet")
            }
            EncodeError::InvalidPropertyValue { id, value } => {
                write!(f, "property {id} has invalid value {value}")
            }
            EncodeError::InvalidSubscriptionIdentifier { value } => {
                write!(f, "subscription identifier {value} is not in 1..=268435455")
            }
        }
    }
}

impl std::error::Error for EncodeError {}
