//! Reason codes: a one-byte result on every acknowledgement, new in 5.0.
//!
//! 3.1.1 had a six-value CONNACK *return code* and nothing at all on PUBACK,
//! PUBREC, PUBREL, PUBCOMP or UNSUBACK, so a 3.1.1 client learned why a server
//! objected only by watching it close the socket [mqtt5 §1.9]. 5.0 puts a
//! reason code on every acknowledgement plus an optional human `Reason
//! String`.
//!
//! **The 0x80 line is the whole semantics.** "A Reason Code value of 0x80 or
//! greater indicates a failure"; below it is a success (2.4) [mqtt5 §1]. That
//! line is why three codes a reader might take for errors are not:
//! `0x10 No matching subscribers` on PUBACK and PUBREC — the only in-protocol
//! signal that a message reached nobody, and the server "MAY use this Reason
//! Code instead of 0x00" [mqtt5 §12/P16] — `0x11 No subscription existed` on
//! UNSUBACK, and SUBACK's `0x01`/`0x02`, which are granted QoS levels rather
//! than complaints. `is_error` is that comparison and nothing more.
//!
//! **Each packet type has its own closed subset, so each gets its own type.**
//! The codes are not one flat enumeration the protocol shares: 0x91 means
//! "Packet identifier in use" on PUBACK and "in use" again on SUBACK but
//! 0x92 "not found" exists only on PUBREL and PUBCOMP, and 0xA2 exists only on
//! SUBACK and DISCONNECT. A byte outside a packet's subset is
//! [`DecodeError::InvalidReasonCode`] rather than an opaque value a caller has
//! to interpret, because a client that cannot name a code cannot act on it.
//!
//! Two subsets share a type, exactly as the specification shares the list:
//! PUBREC's codes are PUBACK's (3.5.2.1) and PUBCOMP's are PUBREL's
//! (3.7.2.1), so [`PubrecReasonCode`] and [`PubcompReasonCode`] are aliases
//! rather than copies.

use crate::error::DecodeError;
use crate::types::PacketType;

/// Defines one packet's reason-code subset: the enum, its byte mapping, its
/// inventory, and the exhaustive round-trip test that proves no byte outside
/// the subset decodes.
macro_rules! reason_codes {
    (
        $(#[$meta:meta])*
        $name:ident for $packet:ident, all = $all:ident;
        $( $(#[$vmeta:meta])* $variant:ident = $value:literal, )*
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $( $(#[$vmeta])* $variant = $value, )*
        }

        impl $name {
            /// The byte this code occupies.
            #[must_use]
            pub const fn as_byte(self) -> u8 {
                self as u8
            }

            /// The code for `byte`.
            ///
            /// # Errors
            ///
            /// [`DecodeError::InvalidReasonCode`] for a byte the
            /// specification does not list for this packet type.
            pub const fn from_byte(byte: u8) -> Result<$name, DecodeError> {
                Ok(match byte {
                    $( $value => $name::$variant, )*
                    code => {
                        return Err(DecodeError::InvalidReasonCode {
                            packet_type: PacketType::$packet,
                            code,
                        });
                    }
                })
            }

            /// Whether the code is a failure: "a Reason Code value of 0x80 or
            /// greater indicates a failure" (2.4) [mqtt5 §1].
            #[must_use]
            pub const fn is_error(self) -> bool {
                self.as_byte() >= 0x80
            }
        }

        /// Every code the specification lists for this packet type, for
        /// exhaustive tests and for the parity table.
        pub const $all: &[$name] = &[ $( $name::$variant, )* ];
    };
}

reason_codes! {
    /// The CONNACK reason codes (3.2.2.2) [mqtt5 §1].
    ConnectReasonCode for Connack, all = CONNECT_REASON_CODES;
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

reason_codes! {
    /// The PUBACK reason codes (3.4.2.1) [mqtt5 §6], shared verbatim by PUBREC
    /// (3.5.2.1).
    ///
    /// A code of 0x80 or above means the PUBLISH counts as acknowledged and
    /// MUST NOT be retransmitted ([MQTT-4.4.0-2]) [mqtt5 §6] — the message is
    /// dead with no protocol recourse, which is why this list is worth having
    /// as a type rather than as a byte.
    PubackReasonCode for Puback, all = PUBACK_REASON_CODES;
    /// 0x00 — the message is accepted.
    Success = 0x00,
    /// 0x10 — no matching subscribers. A **success**, server only, and the
    /// only in-protocol "delivered to nobody" signal [mqtt5 §4.1].
    NoMatchingSubscribers = 0x10,
    /// 0x80 — the receiver does not accept the publish, reason unspecified.
    UnspecifiedError = 0x80,
    /// 0x83 — the PUBLISH is valid but the receiver is not willing to accept
    /// it.
    ImplementationSpecificError = 0x83,
    /// 0x87 — the publish is not authorized.
    NotAuthorized = 0x87,
    /// 0x90 — the Topic Name is not malformed but is not accepted.
    TopicNameInvalid = 0x90,
    /// 0x91 — the Packet Identifier is already in use, which "might indicate
    /// a mismatch in the Session State between the Client and Server".
    PacketIdentifierInUse = 0x91,
    /// 0x97 — an implementation or administrative imposed limit was exceeded.
    QuotaExceeded = 0x97,
    /// 0x99 — the payload does not match the Payload Format Indicator.
    PayloadFormatInvalid = 0x99,
}

reason_codes! {
    /// The PUBREL reason codes (3.6.2.1) [mqtt5 §6], shared verbatim by
    /// PUBCOMP (3.7.2.1). Two values, and the second is the interesting one.
    PubrelReasonCode for Pubrel, all = PUBREL_REASON_CODES;
    /// 0x00 — the message is released, or publication is complete.
    Success = 0x00,
    /// 0x92 — the Packet Identifier is not known. The specification's own
    /// gloss is deliberately ambivalent: it "is not an error during recovery,
    /// but at other times indicates a mismatch between the Session State on
    /// the Client and Server" (3.6.2.1) [mqtt5 §6], and the codec cannot tell
    /// the two apart because only a session can.
    PacketIdentifierNotFound = 0x92,
}

reason_codes! {
    /// The SUBACK reason codes (3.9.3) [mqtt5 §6]: one per Topic Filter, in
    /// the order the SUBSCRIBE listed them.
    ///
    /// The first three are not successes in the ordinary sense but *answers*:
    /// the server reports the maximum QoS it granted, which may be less than
    /// the client asked for, and "the QoS of Application Messages sent in
    /// response to a Subscription MUST be the minimum of the QoS of the
    /// originally published message and the Maximum QoS granted"
    /// ([MQTT-3.8.4-8]) [mqtt5 §6].
    SubackReasonCode for Suback, all = SUBACK_REASON_CODES;
    /// 0x00 — granted QoS 0.
    GrantedQos0 = 0x00,
    /// 0x01 — granted QoS 1.
    GrantedQos1 = 0x01,
    /// 0x02 — granted QoS 2.
    GrantedQos2 = 0x02,
    /// 0x80 — the subscription is not accepted, reason unspecified.
    UnspecifiedError = 0x80,
    /// 0x83 — the SUBSCRIBE is valid but the server is not willing to accept
    /// it.
    ImplementationSpecificError = 0x83,
    /// 0x87 — the client is not authorized to make this subscription.
    NotAuthorized = 0x87,
    /// 0x8F — the Topic Filter is correctly formed but is not allowed.
    TopicFilterInvalid = 0x8F,
    /// 0x91 — the Packet Identifier is already in use.
    PacketIdentifierInUse = 0x91,
    /// 0x97 — an implementation or administrative imposed limit was exceeded.
    QuotaExceeded = 0x97,
    /// 0x9E — the server does not support Shared Subscriptions.
    SharedSubscriptionsNotSupported = 0x9E,
    /// 0xA1 — the server does not support Subscription Identifiers.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// 0xA2 — the server does not support Wildcard Subscriptions.
    WildcardSubscriptionsNotSupported = 0xA2,
}

reason_codes! {
    /// The UNSUBACK reason codes (3.11.3) [mqtt5 §6]: one per Topic Filter, in
    /// the order the UNSUBSCRIBE listed them. New in 5.0 — 3.1.1's UNSUBACK
    /// carried no status at all [mqtt5 §1.9].
    UnsubackReasonCode for Unsuback, all = UNSUBACK_REASON_CODES;
    /// 0x00 — the subscription is deleted.
    Success = 0x00,
    /// 0x11 — no subscription existed. A **success**: the end state the client
    /// asked for holds either way.
    NoSubscriptionExisted = 0x11,
    /// 0x80 — the unsubscribe could not be completed, reason unspecified.
    UnspecifiedError = 0x80,
    /// 0x83 — the UNSUBSCRIBE is valid but the server is not willing to accept
    /// it.
    ImplementationSpecificError = 0x83,
    /// 0x87 — the client is not authorized to unsubscribe this filter.
    NotAuthorized = 0x87,
    /// 0x8F — the Topic Filter is correctly formed but is not allowed.
    TopicFilterInvalid = 0x8F,
    /// 0x91 — the Packet Identifier is already in use.
    PacketIdentifierInUse = 0x91,
}

reason_codes! {
    /// The DISCONNECT reason codes (3.14.2.1) [mqtt5 §1]: twenty-nine values,
    /// and the whole reason 5.0's DISCONNECT flows server-to-client at all.
    ///
    /// In 3.1.1 DISCONNECT was client-to-server only and a server reported
    /// errors by closing the socket, leaving the client to guess
    /// [mqtt5 §1.9]. Two codes are the client's own: 0x00, which discards the
    /// Will ([MQTT-3.14.4-3]), and 0x04, which asks for it anyway.
    DisconnectReasonCode for Disconnect, all = DISCONNECT_REASON_CODES;
    /// 0x00 — normal disconnection; the server MUST NOT publish the Will.
    NormalDisconnection = 0x00,
    /// 0x04 — disconnect with Will Message: close, and publish the Will.
    /// Client to server only.
    DisconnectWithWillMessage = 0x04,
    /// 0x80 — unspecified error.
    UnspecifiedError = 0x80,
    /// 0x81 — the received packet does not conform to this specification.
    MalformedPacket = 0x81,
    /// 0x82 — an unexpected or out-of-order packet was received.
    ProtocolError = 0x82,
    /// 0x83 — the packet is valid but is not accepted by this receiver.
    ImplementationSpecificError = 0x83,
    /// 0x87 — the request is not authorized. Server to client only.
    NotAuthorized = 0x87,
    /// 0x89 — the server is busy and cannot continue. Server to client only.
    ServerBusy = 0x89,
    /// 0x8B — the server is shutting down. Server to client only.
    ServerShuttingDown = 0x8B,
    /// 0x8D — no packet has been received for 1.5 times the Keep Alive
    /// ([MQTT-3.1.2-22]). Server to client only, and the close happens with
    /// or without this packet, so the Will fires.
    KeepAliveTimeout = 0x8D,
    /// 0x8E — another connection using the same Client Identifier has
    /// connected ([MQTT-3.1.4-3]). Server to client only.
    SessionTakenOver = 0x8E,
    /// 0x8F — the Topic Filter is correctly formed but is not accepted.
    /// Server to client only.
    TopicFilterInvalid = 0x8F,
    /// 0x90 — the Topic Name is correctly formed but is not accepted.
    TopicNameInvalid = 0x90,
    /// 0x93 — more than Receive Maximum publications were in flight (4.9).
    ReceiveMaximumExceeded = 0x93,
    /// 0x94 — an alias of 0, or above the peer's Topic Alias Maximum.
    TopicAliasInvalid = 0x94,
    /// 0x95 — the packet exceeded the receiver's Maximum Packet Size.
    PacketTooLarge = 0x95,
    /// 0x96 — the received data rate is too high.
    MessageRateTooHigh = 0x96,
    /// 0x97 — an implementation or administrative imposed limit was exceeded.
    QuotaExceeded = 0x97,
    /// 0x98 — the connection is closed by administrative action.
    AdministrativeAction = 0x98,
    /// 0x99 — the payload does not match the Payload Format Indicator.
    PayloadFormatInvalid = 0x99,
    /// 0x9A — the server does not support retained messages. Server to client
    /// only.
    RetainNotSupported = 0x9A,
    /// 0x9B — the server does not support the QoS that was used. Server to
    /// client only.
    QosNotSupported = 0x9B,
    /// 0x9C — use another server temporarily. Server to client only.
    UseAnotherServer = 0x9C,
    /// 0x9D — the server has moved permanently. Server to client only.
    ServerMoved = 0x9D,
    /// 0x9E — the server does not support Shared Subscriptions. Server to
    /// client only.
    SharedSubscriptionsNotSupported = 0x9E,
    /// 0x9F — the connection rate limit has been exceeded. Server to client
    /// only.
    ConnectionRateExceeded = 0x9F,
    /// 0xA0 — the maximum connection time authorized has elapsed. Server to
    /// client only.
    MaximumConnectTime = 0xA0,
    /// 0xA1 — the server does not support Subscription Identifiers. Server to
    /// client only.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// 0xA2 — the server does not support Wildcard Subscriptions. Server to
    /// client only.
    WildcardSubscriptionsNotSupported = 0xA2,
}

reason_codes! {
    /// The AUTH reason codes (3.15.2.1) [mqtt5 §10]. Three values, and the
    /// packet type itself is Reserved and Forbidden in 3.1.1 [mqtt5 §1.9].
    AuthReasonCode for Auth, all = AUTH_REASON_CODES;
    /// 0x00 — authentication is successful. Server to client only; a
    /// successful CONNACK is what actually ends the exchange.
    Success = 0x00,
    /// 0x18 — continue authentication: the challenge/response step, sent by
    /// either side (4.12).
    ContinueAuthentication = 0x18,
    /// 0x19 — re-authenticate, sent by a client at any time after CONNACK
    /// using the same method ([MQTT-4.12.1-1]) (4.12.1).
    ReAuthenticate = 0x19,
}

/// PUBREC's reason codes are PUBACK's, verbatim (3.5.2.1) [mqtt5 §6].
pub type PubrecReasonCode = PubackReasonCode;

/// PUBCOMP's reason codes are PUBREL's, verbatim (3.7.2.1) [mqtt5 §6].
pub type PubcompReasonCode = PubrelReasonCode;

#[cfg(test)]
mod tests {
    use super::*;

    /// For each subset: every listed code round-trips, and **no other byte
    /// decodes**. The second half is what makes the list closed rather than a
    /// suggestion, and it is asserted over all 256 bytes rather than over a
    /// sample.
    macro_rules! assert_closed {
        ($name:ident, $all:ident, $packet:ident) => {{
            let mut listed = [false; 256];
            for &code in $all {
                assert_eq!(
                    $name::from_byte(code.as_byte()),
                    Ok(code),
                    "{}: 0x{:02X}",
                    stringify!($name),
                    code.as_byte()
                );
                assert!(
                    !listed[usize::from(code.as_byte())],
                    "{}: 0x{:02X} listed twice",
                    stringify!($name),
                    code.as_byte()
                );
                listed[usize::from(code.as_byte())] = true;
                assert_eq!(
                    code.is_error(),
                    code.as_byte() >= 0x80,
                    "{}: the 0x80 line",
                    stringify!($name)
                );
            }
            for byte in 0u8..=255 {
                if listed[usize::from(byte)] {
                    continue;
                }
                assert_eq!(
                    $name::from_byte(byte),
                    Err(DecodeError::InvalidReasonCode {
                        packet_type: PacketType::$packet,
                        code: byte
                    }),
                    "{}: 0x{byte:02X} is not in the subset",
                    stringify!($name)
                );
            }
            $all.len()
        }};
    }

    #[test]
    fn every_subset_is_closed_and_has_the_documented_size() {
        assert_eq!(
            assert_closed!(ConnectReasonCode, CONNECT_REASON_CODES, Connack),
            22
        );
        assert_eq!(
            assert_closed!(PubackReasonCode, PUBACK_REASON_CODES, Puback),
            9
        );
        assert_eq!(
            assert_closed!(PubrelReasonCode, PUBREL_REASON_CODES, Pubrel),
            2
        );
        assert_eq!(
            assert_closed!(SubackReasonCode, SUBACK_REASON_CODES, Suback),
            12
        );
        assert_eq!(
            assert_closed!(UnsubackReasonCode, UNSUBACK_REASON_CODES, Unsuback),
            7
        );
        // The specification's own count: "DISCONNECT also flows
        // server-to-client with one of 29 reason codes" [mqtt5 §1].
        assert_eq!(
            assert_closed!(DisconnectReasonCode, DISCONNECT_REASON_CODES, Disconnect),
            29
        );
        assert_eq!(assert_closed!(AuthReasonCode, AUTH_REASON_CODES, Auth), 3);
    }

    /// The three codes below 0x80 that a reader might take for errors, and the
    /// one 3.1.1 habit that does not survive: its CONNACK return code 0x01
    /// (unacceptable protocol version) is not a 5.0 reason code at all.
    #[test]
    fn the_successes_that_look_like_failures() {
        assert!(!PubackReasonCode::NoMatchingSubscribers.is_error());
        assert!(!UnsubackReasonCode::NoSubscriptionExisted.is_error());
        assert!(!SubackReasonCode::GrantedQos2.is_error());
        assert!(!DisconnectReasonCode::DisconnectWithWillMessage.is_error());

        assert!(ConnectReasonCode::from_byte(0x01).is_err());
        assert_eq!(
            ConnectReasonCode::from_byte(0x84),
            Ok(ConnectReasonCode::UnsupportedProtocolVersion)
        );
    }

    /// The two shared lists are shared, not copied: a code added to one must
    /// appear in the other, and the aliases make that a type identity rather
    /// than a convention.
    #[test]
    fn pubrec_shares_pubacks_list_and_pubcomp_shares_pubrels() {
        let code: PubrecReasonCode = PubackReasonCode::NoMatchingSubscribers;
        assert_eq!(code.as_byte(), 0x10);
        let code: PubcompReasonCode = PubrelReasonCode::PacketIdentifierNotFound;
        assert_eq!(code.as_byte(), 0x92);
    }

    /// 0x92 exists only on PUBREL and PUBCOMP, and 0x91 only where an
    /// identifier can already be in use — the disjointness that makes one flat
    /// enumeration wrong.
    #[test]
    fn the_subsets_genuinely_differ() {
        assert!(PubrelReasonCode::from_byte(0x92).is_ok());
        assert!(PubackReasonCode::from_byte(0x92).is_err());
        assert!(PubackReasonCode::from_byte(0x91).is_ok());
        assert!(PubrelReasonCode::from_byte(0x91).is_err());
        assert!(SubackReasonCode::from_byte(0xA2).is_ok());
        assert!(DisconnectReasonCode::from_byte(0xA2).is_ok());
        assert!(UnsubackReasonCode::from_byte(0xA2).is_err());
        assert!(AuthReasonCode::from_byte(0x80).is_err());
    }
}
