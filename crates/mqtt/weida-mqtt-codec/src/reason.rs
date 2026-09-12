//! Reason codes: a one-byte result on every acknowledgement, new in 5.0.
//!
//! 3.1.1 had a six-value CONNACK *return code* and nothing at all on PUBACK,
//! PUBREC, PUBREL, PUBCOMP or UNSUBACK, so a 3.1.1 client learned why a server
//! objected only by watching it close the socket [mqtt5 §1.9]. 5.0 puts a
//! reason code on every acknowledgement plus an optional human `Reason
//! String`.
//!
//! **The 0x80 line is the whole semantics.** "A Reason Code value of 0x80 or
//! greater indicates a failure"; below it is a success, which is why 0x10 (No
//! matching subscribers) is a success code and not an error (2.4)
//! [mqtt5 §4.1]. [`ConnectReasonCode::is_error`] is that comparison and
//! nothing more.
//!
//! Each packet type has its own permitted subset, and the codes are not a
//! single flat enumeration the whole protocol shares — so each packet's codes
//! are their own type here, and a code outside the subset is
//! [`crate::DecodeError::InvalidReasonCode`] rather than an opaque byte a
//! caller has to interpret. This module holds the CONNACK subset; the
//! acknowledgement subsets arrive with the packets that carry them.

use crate::error::DecodeError;
use crate::types::PacketType;

/// The CONNACK reason codes (3.2.2.2) [mqtt5 §1].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectReasonCode {
    /// 0x00 — the connection is accepted.
    Success = 0x00,
    /// 0x80 — the server does not wish to reveal the reason, or none applies.
    UnspecifiedError = 0x80,
    /// 0x81 — data in the CONNECT could not be parsed correctly.
    MalformedPacket = 0x81,
    /// 0x82 — data in the CONNECT does not conform to this specification.
    ProtocolError = 0x82,
    /// 0x83 — the CONNECT is valid but is not accepted by this server.
    ImplementationSpecificError = 0x83,
    /// 0x84 — the server does not support the client's protocol version.
    UnsupportedProtocolVersion = 0x84,
    /// 0x85 — the Client Identifier is correctly formed but not accepted.
    ClientIdentifierNotValid = 0x85,
    /// 0x86 — the server does not accept the User Name or Password.
    BadUserNameOrPassword = 0x86,
    /// 0x87 — the client is not authorized to connect.
    NotAuthorized = 0x87,
    /// 0x88 — the MQTT service is not available.
    ServerUnavailable = 0x88,
    /// 0x89 — the server is busy; try again later.
    ServerBusy = 0x89,
    /// 0x8A — this client has been banned by administrative action.
    Banned = 0x8A,
    /// 0x8C — the authentication method is not supported, or does not match
    /// the method currently in use.
    BadAuthenticationMethod = 0x8C,
    /// 0x90 — the Will Topic Name is not malformed but is not accepted.
    TopicNameInvalid = 0x90,
    /// 0x95 — the CONNECT exceeded the maximum permissible size.
    PacketTooLarge = 0x95,
    /// 0x97 — an implementation or administrative imposed limit was exceeded.
    QuotaExceeded = 0x97,
    /// 0x99 — the Will payload does not match the Payload Format Indicator.
    PayloadFormatInvalid = 0x99,
    /// 0x9A — the server does not support retained messages, and the Will
    /// Retain flag was set.
    RetainNotSupported = 0x9A,
    /// 0x9B — the server does not support the Will QoS.
    QosNotSupported = 0x9B,
    /// 0x9C — the client should temporarily use another server.
    UseAnotherServer = 0x9C,
    /// 0x9D — the client should permanently use another server.
    ServerMoved = 0x9D,
    /// 0x9F — the connection rate limit has been exceeded.
    ConnectionRateExceeded = 0x9F,
}

impl ConnectReasonCode {
    /// The byte this code occupies.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// The code for `byte`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::InvalidReasonCode`] for a byte 3.2.2.2 does not list.
    /// The alternative — carrying the byte through as an unknown — is refused
    /// deliberately: a client that cannot name the code cannot act on it, and
    /// the specification's list is closed.
    pub const fn from_byte(byte: u8) -> Result<ConnectReasonCode, DecodeError> {
        Ok(match byte {
            0x00 => ConnectReasonCode::Success,
            0x80 => ConnectReasonCode::UnspecifiedError,
            0x81 => ConnectReasonCode::MalformedPacket,
            0x82 => ConnectReasonCode::ProtocolError,
            0x83 => ConnectReasonCode::ImplementationSpecificError,
            0x84 => ConnectReasonCode::UnsupportedProtocolVersion,
            0x85 => ConnectReasonCode::ClientIdentifierNotValid,
            0x86 => ConnectReasonCode::BadUserNameOrPassword,
            0x87 => ConnectReasonCode::NotAuthorized,
            0x88 => ConnectReasonCode::ServerUnavailable,
            0x89 => ConnectReasonCode::ServerBusy,
            0x8A => ConnectReasonCode::Banned,
            0x8C => ConnectReasonCode::BadAuthenticationMethod,
            0x90 => ConnectReasonCode::TopicNameInvalid,
            0x95 => ConnectReasonCode::PacketTooLarge,
            0x97 => ConnectReasonCode::QuotaExceeded,
            0x99 => ConnectReasonCode::PayloadFormatInvalid,
            0x9A => ConnectReasonCode::RetainNotSupported,
            0x9B => ConnectReasonCode::QosNotSupported,
            0x9C => ConnectReasonCode::UseAnotherServer,
            0x9D => ConnectReasonCode::ServerMoved,
            0x9F => ConnectReasonCode::ConnectionRateExceeded,
            code => {
                return Err(DecodeError::InvalidReasonCode {
                    packet_type: PacketType::Connack,
                    code,
                });
            }
        })
    }

    /// Whether the code is a failure: "a Reason Code value of 0x80 or greater
    /// indicates a failure" (2.4) [mqtt5 §1].
    #[must_use]
    pub const fn is_error(self) -> bool {
        self.as_byte() >= 0x80
    }
}

/// Every CONNACK reason code, for exhaustive tests and for the parity table.
pub const CONNECT_REASON_CODES: [ConnectReasonCode; 22] = [
    ConnectReasonCode::Success,
    ConnectReasonCode::UnspecifiedError,
    ConnectReasonCode::MalformedPacket,
    ConnectReasonCode::ProtocolError,
    ConnectReasonCode::ImplementationSpecificError,
    ConnectReasonCode::UnsupportedProtocolVersion,
    ConnectReasonCode::ClientIdentifierNotValid,
    ConnectReasonCode::BadUserNameOrPassword,
    ConnectReasonCode::NotAuthorized,
    ConnectReasonCode::ServerUnavailable,
    ConnectReasonCode::ServerBusy,
    ConnectReasonCode::Banned,
    ConnectReasonCode::BadAuthenticationMethod,
    ConnectReasonCode::TopicNameInvalid,
    ConnectReasonCode::PacketTooLarge,
    ConnectReasonCode::QuotaExceeded,
    ConnectReasonCode::PayloadFormatInvalid,
    ConnectReasonCode::RetainNotSupported,
    ConnectReasonCode::QosNotSupported,
    ConnectReasonCode::UseAnotherServer,
    ConnectReasonCode::ServerMoved,
    ConnectReasonCode::ConnectionRateExceeded,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_listed_code_round_trips_and_no_other_byte_decodes() {
        let mut listed = [false; 256];
        for code in CONNECT_REASON_CODES {
            assert_eq!(
                ConnectReasonCode::from_byte(code.as_byte()),
                Ok(code),
                "0x{:02X}",
                code.as_byte()
            );
            listed[usize::from(code.as_byte())] = true;
        }

        for byte in 0u8..=255 {
            if listed[usize::from(byte)] {
                continue;
            }
            assert_eq!(
                ConnectReasonCode::from_byte(byte),
                Err(DecodeError::InvalidReasonCode {
                    packet_type: PacketType::Connack,
                    code: byte
                }),
                "0x{byte:02X} is not a CONNACK code"
            );
        }
    }

    /// The 0x80 line, and the one code below it that a reader might expect to
    /// be an error: 3.1.1's six return codes started at 0x01, so 0x01..0x7F
    /// being *successes* in 5.0 is the change worth a test.
    #[test]
    fn the_error_line_is_at_0x80() {
        assert!(!ConnectReasonCode::Success.is_error());
        assert!(ConnectReasonCode::UnspecifiedError.is_error());
        for code in CONNECT_REASON_CODES {
            assert_eq!(code.is_error(), code.as_byte() >= 0x80, "{code:?}");
        }
        // 3.1.1's "unacceptable protocol version" was 0x01; in 5.0 that byte
        // is not a reason code at all, and 0x84 is.
        assert!(ConnectReasonCode::from_byte(0x01).is_err());
        assert_eq!(
            ConnectReasonCode::from_byte(0x84),
            Ok(ConnectReasonCode::UnsupportedProtocolVersion)
        );
    }
}
