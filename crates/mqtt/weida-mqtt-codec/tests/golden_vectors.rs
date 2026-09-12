//! Conformance: the golden test vectors of `docs/adapters/mqtt5.md` §10.1.
//!
//! §10.1 publishes these octets, so an implementation — this one, or a future
//! reimplementation, or a reader checking a broker against the document — MUST
//! encode exactly them and MUST decode them back to exactly these values. The
//! vectors are asserted here through the crate's public surface only: the unit
//! tests beside each module cover the halves, and this file is what makes the
//! published bytes binding.
//!
//! Every accept vector is asserted **in both directions**. Encoding alone
//! would not catch a decoder that is wrong in the same way, which is the
//! failure mode that matters when the other end of the wire is Mosquitto and
//! not us. The rejection vectors are asserted once, on the decoder, because
//! there is nothing to encode: they are bytes a peer may send and this codec
//! must refuse.
//!
//! Vectors 5 to 19 cover the thirteen packet types B-140 adds, and are
//! asserted there. What is in this file is vectors 1 to 4 and 20 to 23, plus
//! the two CONNECT payload-order cases §10.1's row 2 exists for.

use weida_mqtt_codec::error::{MALFORMED_PACKET, PROTOCOL_ERROR};
use weida_mqtt_codec::{
    Connack, Connect, ConnectReasonCode, DecodeError, Packet, PayloadFormat, Properties, QoS, Will,
    varint,
};

/// The ceiling every vector is decoded under. The protocol's own is
/// 268,435,455 bytes of Remaining Length plus five of fixed header (2.1.4),
/// and no vector is anywhere near it.
const CAP: u32 = varint::MAX;

/// Asserts a packet encodes to exactly `expected` and that `expected` decodes
/// back to exactly that packet.
#[track_caller]
fn assert_vector(packet: &Packet<'_>, expected: &[u8]) {
    let mut out = Vec::new();
    packet.encode(&mut out).expect("encodes");
    assert_eq!(out, expected, "{:?}: encoded bytes", packet.packet_type());

    let (decoded, used) = Packet::decode(expected, CAP).expect("decodes");
    assert_eq!(used, expected.len(), "consumed the whole packet");
    assert_eq!(&decoded, packet, "decoded value");

    // The published length is the one `Maximum Packet Size` is measured
    // against, so it is part of the vector rather than a derived detail.
    assert_eq!(packet.encoded_len(), Ok(expected.len() as u32));
}

/// Vector 1: the minimal CONNECT — clean start, keep alive 60, client
/// identifier `a`, no properties.
#[test]
fn vector_1_minimal_connect() {
    assert_vector(
        &Packet::Connect(Connect {
            client_id: "a",
            clean_start: true,
            keep_alive: 60,
            ..Connect::default()
        }),
        &[
            0x10, 0x0E, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            0x01, 0x61,
        ],
    );
}

/// Vector 2: CONNECT with a Will and credentials, in the payload order of the
/// normative body (3.1.3) rather than Appendix B's — Client Identifier, Will
/// Properties, Will Topic, Will Payload, User Name, Password.
#[test]
fn vector_2_connect_with_will_and_credentials() {
    assert_vector(
        &Packet::Connect(Connect {
            client_id: "c",
            clean_start: true,
            keep_alive: 60,
            will: Some(Box::new(Will {
                topic: "d",
                payload: &[0x01],
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties {
                    will_delay_interval: Some(10),
                    ..Properties::new()
                },
            })),
            user_name: Some("u"),
            password: Some(&[0x70]),
            ..Connect::default()
        }),
        &[
            0x10, 0x20, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0xCE, 0x00, 0x3C, 0x00, 0x00,
            0x01, 0x63, 0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x64, 0x00, 0x01, 0x01,
            0x00, 0x01, 0x75, 0x00, 0x01, 0x70,
        ],
    );
}

/// The half of vector 2 that is a claim about ordering rather than about
/// bytes: the Will fields sit between the Client Identifier and the User Name,
/// so moving the User Name in front of them — Appendix B's shape — is refused
/// rather than silently mis-decoded.
#[test]
fn vector_2_the_payload_order_is_load_bearing() {
    let mut wire = vec![
        0x10, 0x20, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0xCE, 0x00, 0x3C, 0x00, 0x00, 0x01,
        0x63, 0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x64, 0x00, 0x01, 0x01, 0x00, 0x01,
        0x75, 0x00, 0x01, 0x70,
    ];
    // Swap the Will Properties block for the User Name: the Will Properties
    // length byte 0x05 is then read as a five-byte property block starting
    // with 0x00, which is not a property identifier.
    wire[16] = 0x00;
    wire[17] = 0x01;
    wire[18] = 0x75;
    let error = Packet::decode(&wire, CAP).expect_err("the order is not free");
    assert!(error.is_violation(), "{error}");
}

/// Vector 3: the minimal CONNACK — no session, Success, no properties.
#[test]
fn vector_3_minimal_connack() {
    assert_vector(
        &Packet::Connack(Connack::default()),
        &[0x20, 0x03, 0x00, 0x00, 0x00],
    );
}

/// Vector 4: CONNACK with Session Present 1 and a Receive Maximum of 10.
#[test]
fn vector_4_connack_with_session_present_and_receive_maximum() {
    assert_vector(
        &Packet::Connack(Connack {
            session_present: true,
            reason_code: ConnectReasonCode::Success,
            properties: Properties {
                receive_maximum: Some(10),
                ..Properties::new()
            },
        }),
        &[0x20, 0x06, 0x01, 0x00, 0x03, 0x21, 0x00, 0x0A],
    );
}

/// Vector 20: a non-minimal Variable Byte Integer. Both `81 00` (the value 1
/// in two bytes) and `80 00` (0 in two) decode arithmetically and both spend a
/// byte they do not need, which [MQTT-1.5.5-1] forbids. `80 01` is
/// deliberately included as the counter-example: it is the *minimal* encoding
/// of 128 and must be accepted, which is what stops the check from being "the
/// high bit was set".
#[test]
fn vector_20_a_non_minimal_remaining_length_is_refused() {
    for bytes in [&[0x10u8, 0x81, 0x00][..], &[0x10, 0x80, 0x00][..]] {
        let error = Packet::decode(bytes, CAP).expect_err("not minimal");
        assert_eq!(error, DecodeError::VarintNotMinimal, "{bytes:02X?}");
        assert_eq!(error.reason_code(), Some(MALFORMED_PACKET));
    }

    // `80 01` is minimal: 128 has no shorter encoding. It is not refused for
    // its encoding, only for the 128 bytes of body that are not there.
    assert_eq!(
        Packet::decode(&[0x10, 0x80, 0x01], CAP),
        Err(DecodeError::Incomplete)
    );

    assert_eq!(
        varint::decode(&[0x81, 0x00]),
        Err(DecodeError::VarintNotMinimal)
    );
    assert_eq!(varint::decode(&[0x80, 0x01]), Ok((128, 2)));
}

/// Vector 21: a fifth Remaining Length byte. The encoding is 1 to 4 bytes
/// (1.5.5), so this is not a Variable Byte Integer at all.
#[test]
fn vector_21_a_five_byte_remaining_length_is_refused() {
    let error =
        Packet::decode(&[0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], CAP).expect_err("not an integer");
    assert_eq!(error, DecodeError::VarintTooLong);
    assert_eq!(error.reason_code(), Some(MALFORMED_PACKET));
}

/// Vector 22: a repeated Session Expiry Interval. Repetition is a Protocol
/// Error for every property except User Property and Subscription Identifier,
/// and the reason code is 0x82 rather than 0x81 — the packet parses, then
/// breaks a rule.
#[test]
fn vector_22_a_repeated_property_is_a_protocol_error() {
    let wire = [
        0x10, 0x18, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3C, //
        0x0A, 0x11, 0x00, 0x00, 0x00, 0x1E, 0x11, 0x00, 0x00, 0x00, 0x1F, //
        0x00, 0x01, 0x61,
    ];
    let error = Packet::decode(&wire, CAP).expect_err("refused");
    assert!(
        matches!(error, DecodeError::DuplicateProperty { .. }),
        "{error}"
    );
    assert_eq!(error.reason_code(), Some(PROTOCOL_ERROR));

    // One occurrence of the same property is of course fine: the same packet
    // with a five-byte property block instead of a ten-byte one.
    let once = [
        0x10, 0x13, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3C, //
        0x05, 0x11, 0x00, 0x00, 0x00, 0x1E, //
        0x00, 0x01, 0x61,
    ];
    let (packet, used) = Packet::decode(&once, CAP).expect("one occurrence");
    assert_eq!(used, once.len());
    let Packet::Connect(connect) = packet else {
        panic!("a connect")
    };
    assert_eq!(connect.properties.session_expiry_interval, Some(30));
}

/// Vector 23: an unknown property identifier on CONNECT, answered with 0x81.
/// Two cases, and the second is the one a byte-oriented reader gets wrong:
/// `0x7F` is simply not in table 2-4, while `0x80 0x01` is the identifier 128,
/// which only a Variable Byte Integer reader sees as one value.
#[test]
fn vector_23_an_unknown_property_identifier_is_malformed() {
    for (block, id) in [
        (&[0x02u8, 0x7F, 0x00][..], 0x7F_u32),
        (&[0x03, 0x80, 0x01, 0x00][..], 128),
    ] {
        let mut wire = vec![
            0x10, 0x00, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3C,
        ];
        wire.extend_from_slice(block);
        wire.extend_from_slice(&[0x00, 0x01, 0x61]);
        wire[1] = (wire.len() - 2) as u8;

        let error = Packet::decode(&wire, CAP).expect_err("refused");
        assert_eq!(error, DecodeError::UnknownProperty { id }, "{block:02X?}");
        assert_eq!(error.reason_code(), Some(MALFORMED_PACKET));
    }
}

/// Not a numbered vector but the claim §10.1's header makes: a property the
/// packet type does not carry is refused. `Will Delay Interval` is a Will
/// property, so the same five bytes are legal inside the Will Properties block
/// and illegal in CONNECT's own.
#[test]
fn a_property_in_the_wrong_packet_is_refused_and_right_where_it_belongs() {
    let mut wire = vec![
        0x10, 0x13, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3C, //
        0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, //
        0x00, 0x01, 0x61,
    ];
    let error = Packet::decode(&wire, CAP).expect_err("refused in CONNECT");
    assert!(
        matches!(error, DecodeError::PropertyNotAllowed { .. }),
        "{error}"
    );
    assert_eq!(error.reason_code(), Some(MALFORMED_PACKET));

    // The same property inside the Will Properties, where it belongs: set the
    // Will Flag, empty CONNECT properties, and move the block into the
    // payload after the Client Identifier.
    wire = vec![
        0x10, 0x19, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x06, 0x00, 0x3C, 0x00, //
        0x00, 0x01, 0x61, //
        0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, //
        0x00, 0x01, 0x64, 0x00, 0x00,
    ];
    let (packet, used) = Packet::decode(&wire, CAP).expect("legal as a will property");
    assert_eq!(used, wire.len());
    let Packet::Connect(connect) = packet else {
        panic!("a connect")
    };
    let will = connect.will.expect("a will");
    assert_eq!(will.properties.will_delay_interval, Some(10));
    assert_eq!(will.topic, "d");
    assert_eq!(will.payload, b"");
}

/// The whole of 3.1.2.11 and 3.2.2.3 through the public surface, in one packet
/// each. B-139's acceptance names "every property of 3.1.2.11 and 3.2.2.3
/// encoded and decoded", and this is that claim as a test rather than as a
/// count of match arms.
#[test]
fn every_connect_and_connack_property_survives_the_wire() {
    let pairs = [("trace", "1"), ("trace", "2")];

    let connect = Connect {
        client_id: "every-property",
        clean_start: false,
        keep_alive: 65_535,
        properties: Properties {
            session_expiry_interval: Some(0xFFFF_FFFF),
            receive_maximum: Some(1),
            maximum_packet_size: Some(varint::MAX),
            topic_alias_maximum: Some(65_535),
            request_response_information: Some(true),
            request_problem_information: Some(false),
            authentication_method: Some("SCRAM-SHA-1"),
            authentication_data: Some(&[0x00, 0xFF]),
            ..Properties::new()
        }
        .with_user_properties(&pairs),
        will: Some(Box::new(Will {
            topic: "status/every-property",
            payload: b"offline",
            qos: QoS::ExactlyOnce,
            retain: true,
            properties: Properties {
                will_delay_interval: Some(0xFFFF_FFFF),
                payload_format_indicator: Some(PayloadFormat::Utf8),
                message_expiry_interval: Some(3600),
                content_type: Some("text/plain; charset=utf-8"),
                response_topic: Some("reply/every-property"),
                correlation_data: Some(&[0xDE, 0xAD, 0xBE, 0xEF]),
                ..Properties::new()
            }
            .with_user_properties(&pairs),
        })),
        user_name: Some("operator"),
        password: Some(&[0x73, 0x33, 0x63]),
    };
    let mut out = Vec::new();
    let packet = Packet::Connect(connect);
    packet.encode(&mut out).expect("encodes");
    let (decoded, used) = Packet::decode(&out, CAP).expect("decodes");
    assert_eq!(used, out.len());
    assert_eq!(decoded, packet);

    let connack = Connack {
        session_present: true,
        reason_code: ConnectReasonCode::Success,
        properties: Properties {
            session_expiry_interval: Some(7200),
            receive_maximum: Some(10),
            maximum_qos: Some(QoS::AtLeastOnce),
            retain_available: Some(false),
            maximum_packet_size: Some(2_000_000),
            assigned_client_identifier: Some("auto-9f86d081"),
            topic_alias_maximum: Some(10),
            reason_string: Some("connected"),
            wildcard_subscription_available: Some(true),
            subscription_identifier_available: Some(false),
            shared_subscription_available: Some(true),
            server_keep_alive: Some(30),
            response_information: Some("reply/auto-9f86d081"),
            server_reference: Some("[fe80::9610:3eff:fe1c]:1883 myserver.xyz.org:8883"),
            authentication_method: Some("SCRAM-SHA-1"),
            authentication_data: Some(&[0x01]),
            ..Properties::new()
        }
        .with_user_properties(&pairs),
    };
    let mut out = Vec::new();
    let packet = Packet::Connack(connack);
    packet.encode(&mut out).expect("encodes");
    let (decoded, used) = Packet::decode(&out, CAP).expect("decodes");
    assert_eq!(used, out.len());
    assert_eq!(decoded, packet);

    // Both user properties survive, in order and with the repeated key: that
    // is the one property whose repetition is legal and whose order
    // [MQTT-3.3.2-17] pins.
    let Packet::Connack(connack) = decoded else {
        panic!("a connack")
    };
    assert_eq!(
        connack.properties.user_properties().collect::<Vec<_>>(),
        [("trace", "1"), ("trace", "2")]
    );
}

/// The bound, through the public surface: a declaration above the caller's
/// ceiling is refused from the fixed header alone, having consumed nothing.
#[test]
fn the_maximum_packet_size_is_applied_before_the_body() {
    // A CONNECT that declares 16,384 bytes of body in a three-byte Remaining
    // Length, against a 1 KiB ceiling.
    let wire = [0x10, 0x80, 0x80, 0x01];
    assert_eq!(
        Packet::decode(&wire, 1024),
        Err(DecodeError::PacketTooLarge {
            size: 16_388,
            max: 1024
        })
    );
    // Raising the ceiling turns the same four bytes into a request for the
    // body, which proves the refusal came from the header and not from the
    // missing bytes.
    assert_eq!(Packet::decode(&wire, 16_388), Err(DecodeError::Incomplete));
}
