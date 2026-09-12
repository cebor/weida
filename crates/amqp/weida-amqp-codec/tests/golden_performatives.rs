//! Golden vectors for the frame header, the protocol headers, the nine
//! performatives and the five SASL bodies.
//!
//! Part 2 and Part 5 print field tables, not hex dumps: unlike Part 1's
//! Figure 1.19 there is no published encoding of an `open` to transcribe. So
//! these vectors are *derived* — each one is the field order of Part 2 §2.7
//! or Part 5 §5.3.3 encoded under Part 1's rules, written out octet by octet
//! with the field each octet belongs to named beside it — and what they pin
//! is that the derivation never changes silently. A refactor that reordered
//! `flow`'s fields or stopped omitting a default would pass every round-trip
//! test in the crate and fail here.
//!
//! Every vector is asserted in both directions: the octets decode to the
//! performative, and the performative encodes to exactly those octets. The
//! second half is what makes a default-omission rule checkable, because the
//! only difference between "omitted" and "written as null" is a byte count.

use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE, SASL_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{
    Attach, Begin, Close, DESCRIPTORS, Detach, Disposition, End, Flow, MESSAGE_FORMAT, Open,
    Performative, Transfer,
};
use weida_amqp_codec::protocol_header::{self, ProtocolHeader, ProtocolId};
use weida_amqp_codec::sasl::{self, SaslCode, SaslFrame, SaslInit, SaslMechanisms, SaslOutcome};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{AmqpError, Limits, Multiple, ReceiverSettleMode, Role, SenderSettleMode};

/// Concatenates literal octets and text, so a vector can name its strings
/// instead of spelling them in hex.
fn octets(parts: &[Part<'_>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        match part {
            Part::Hex(raw) => bytes.extend_from_slice(raw),
            Part::Text(text) => bytes.extend_from_slice(text.as_bytes()),
        }
    }
    bytes
}

enum Part<'a> {
    Hex(&'a [u8]),
    Text(&'a str),
}

use Part::{Hex, Text};

fn check(label: &str, expected: Vec<u8>, performative: Performative<'_>) {
    let (decoded, used) = Performative::decode(&expected, Limits::DEFAULT)
        .unwrap_or_else(|e| panic!("{label} should decode: {e}"));
    assert_eq!(used, expected.len(), "{label} consumes exactly its octets");
    assert_eq!(decoded, performative, "{label} decodes to its value");

    let mut written = Vec::new();
    performative
        .encode(&mut written)
        .unwrap_or_else(|e| panic!("{label} should encode: {e}"));
    assert_eq!(
        written, expected,
        "{label} encodes to exactly the golden octets"
    );
}

#[test]
fn open_with_only_its_mandatory_field() {
    // 00 53 10          described, smallulong, 0x10 = amqp:open:list
    // c0 0b 01          list8, size 11, one field
    // a1 08 "client-1"  container-id
    // The other nine fields are at their defaults and do not appear.
    check(
        "open (minimal)",
        octets(&[
            Hex(&[0x00, 0x53, 0x10]),
            Hex(&[0xc0, 0x0b, 0x01]),
            Hex(&[0xa1, 0x08]),
            Text("client-1"),
        ]),
        Performative::Open(Open::new("client-1")),
    );
}

#[test]
fn open_with_the_four_fields_a_client_actually_sets() {
    // a1 08 "client-1"     container-id
    // a1 09 "localhost"    hostname
    // 70 00 00 10 00       max-frame-size 4096
    // 60 00 07             channel-max 7
    // 70 00 00 75 30       idle-time-out 30000 ms
    let mut open = Open::new("client-1");
    open.hostname = Some("localhost");
    open.max_frame_size = 4096;
    open.channel_max = 7;
    open.idle_time_out = Some(30_000);
    check(
        "open (negotiating)",
        octets(&[
            Hex(&[0x00, 0x53, 0x10]),
            Hex(&[0xc0, 0x23, 0x05]),
            Hex(&[0xa1, 0x08]),
            Text("client-1"),
            Hex(&[0xa1, 0x09]),
            Text("localhost"),
            Hex(&[0x70, 0x00, 0x00, 0x10, 0x00]),
            Hex(&[0x60, 0x00, 0x07]),
            Hex(&[0x70, 0x00, 0x00, 0x75, 0x30]),
        ]),
        Performative::Open(open),
    );
}

#[test]
fn begin_with_its_three_mandatory_windows() {
    // 60 00 00   remote-channel 0 - present and zero, which is not the same
    //            as absent: the answering begin must carry it.
    // 43         next-outgoing-id 0, in the zero-octet uint form
    // 52 08      incoming-window 8
    // 52 08      outgoing-window 8
    let mut begin = Begin::new(0, 8, 8);
    begin.remote_channel = Some(0);
    check(
        "begin",
        octets(&[
            Hex(&[0x00, 0x53, 0x11]),
            Hex(&[0xc0, 0x09, 0x04]),
            Hex(&[0x60, 0x00, 0x00]),
            Hex(&[0x43]),
            Hex(&[0x52, 0x08]),
            Hex(&[0x52, 0x08]),
        ]),
        Performative::Begin(begin),
    );
}

#[test]
fn attach_as_a_sender_is_a_false_role_octet() {
    // a1 06 "link-1"  name
    // 43              handle 0
    // 42              role = false = sender
    check(
        "attach",
        octets(&[
            Hex(&[0x00, 0x53, 0x12]),
            Hex(&[0xc0, 0x0b, 0x03]),
            Hex(&[0xa1, 0x06]),
            Text("link-1"),
            Hex(&[0x43]),
            Hex(&[0x42]),
        ]),
        Performative::Attach(Attach::new("link-1", 0, Role::Sender)),
    );
}

#[test]
fn attach_with_both_settle_modes_set_away_from_their_defaults() {
    // 50 00   snd-settle-mode = unsettled (default is mixed = 2)
    // 50 01   rcv-settle-mode = second (default is first = 0)
    let mut attach = Attach::new("l", 0, Role::Receiver);
    attach.snd_settle_mode = SenderSettleMode::Unsettled;
    attach.rcv_settle_mode = ReceiverSettleMode::Second;
    check(
        "attach (settle modes)",
        octets(&[
            Hex(&[0x00, 0x53, 0x12]),
            Hex(&[0xc0, 0x0a, 0x05]),
            Hex(&[0xa1, 0x01]),
            Text("l"),
            Hex(&[0x43]),
            Hex(&[0x41]),
            Hex(&[0x50, 0x00]),
            Hex(&[0x50, 0x01]),
        ]),
        Performative::Attach(attach),
    );
}

#[test]
fn flow_carries_session_state_with_no_link_state() {
    // 40      next-incoming-id absent: the partner's begin has not been seen
    // 52 08   incoming-window 8
    // 43      next-outgoing-id 0
    // 52 08   outgoing-window 8
    check(
        "flow (session only)",
        octets(&[
            Hex(&[0x00, 0x53, 0x13]),
            Hex(&[0xc0, 0x07, 0x04]),
            Hex(&[0x40]),
            Hex(&[0x52, 0x08]),
            Hex(&[0x43]),
            Hex(&[0x52, 0x08]),
        ]),
        Performative::Flow(Flow::session(8, 0, 8)),
    );
}

#[test]
fn flow_granting_credit_carries_link_state_on_top() {
    // 43      next-incoming-id 0
    // 52 08   incoming-window 8
    // 43      next-outgoing-id 0
    // 52 08   outgoing-window 8
    // 52 03   handle 3
    // 43      delivery-count 0
    // 52 aa   link-credit 170 - RabbitMQ's initial grant
    let mut flow = Flow::session(8, 0, 8);
    flow.next_incoming_id = Some(0);
    flow.handle = Some(3);
    flow.delivery_count = Some(0);
    flow.link_credit = Some(170);
    check(
        "flow (credit)",
        octets(&[
            Hex(&[0x00, 0x53, 0x13]),
            Hex(&[0xc0, 0x0c, 0x07]),
            Hex(&[0x43]),
            Hex(&[0x52, 0x08]),
            Hex(&[0x43]),
            Hex(&[0x52, 0x08]),
            Hex(&[0x52, 0x03]),
            Hex(&[0x43]),
            Hex(&[0x52, 0xaa]),
        ]),
        Performative::Flow(flow),
    );
}

#[test]
fn transfer_with_a_tag_and_an_explicit_unsettled_flag() {
    // 43         handle 0
    // 43         delivery-id 0
    // a0 01 00   delivery-tag, one octet
    // 40         message-format absent
    // 42         settled = false, written because unset means something else
    let mut transfer = Transfer::new(0);
    transfer.delivery_id = Some(0);
    transfer.delivery_tag = Some(&[0x00]);
    transfer.settled = Some(false);
    check(
        "transfer",
        octets(&[
            Hex(&[0x00, 0x53, 0x14]),
            Hex(&[0xc0, 0x08, 0x05]),
            Hex(&[0x43]),
            Hex(&[0x43]),
            Hex(&[0xa0, 0x01, 0x00]),
            Hex(&[0x40]),
            Hex(&[0x42]),
        ]),
        Performative::Transfer(transfer),
    );
}

#[test]
fn transfer_with_more_set_is_a_continuation() {
    // 52 07   handle 7
    // 40      delivery-id omitted on a continuation frame
    // 40      delivery-tag omitted likewise
    // 43      message-format 0, the only format Part 3 defines
    // 40      settled unset
    // 41      more = true
    let mut transfer = Transfer::new(7);
    transfer.message_format = Some(MESSAGE_FORMAT);
    transfer.more = true;
    check(
        "transfer (continuation)",
        octets(&[
            Hex(&[0x00, 0x53, 0x14]),
            Hex(&[0xc0, 0x08, 0x06]),
            Hex(&[0x52, 0x07]),
            Hex(&[0x40]),
            Hex(&[0x40]),
            Hex(&[0x43]),
            Hex(&[0x40]),
            Hex(&[0x41]),
        ]),
        Performative::Transfer(transfer),
    );
}

#[test]
fn disposition_over_a_range_of_delivery_ids() {
    // 41      role = true = receiver
    // 43      first 0
    // 52 03   last 3
    // 41      settled = true
    let mut disposition = Disposition::new(Role::Receiver, 0);
    disposition.last = Some(3);
    disposition.settled = true;
    check(
        "disposition",
        octets(&[
            Hex(&[0x00, 0x53, 0x15]),
            Hex(&[0xc0, 0x06, 0x04]),
            Hex(&[0x41]),
            Hex(&[0x43]),
            Hex(&[0x52, 0x03]),
            Hex(&[0x41]),
        ]),
        Performative::Disposition(disposition),
    );
}

#[test]
fn detach_that_closes_the_link() {
    // 43   handle 0
    // 41   closed = true
    let mut detach = Detach::new(0);
    detach.closed = true;
    check(
        "detach",
        octets(&[
            Hex(&[0x00, 0x53, 0x16]),
            Hex(&[0xc0, 0x03, 0x02]),
            Hex(&[0x43]),
            Hex(&[0x41]),
        ]),
        Performative::Detach(detach),
    );
}

#[test]
fn end_carrying_an_error_nests_one_composite_in_another() {
    // 00 53 17                   amqp:end:list
    // c0 1b 01                   one field
    //   00 53 1d                 amqp:error:list
    //   c0 16 01                 one field
    //     a3 13 "amqp:internal-error"
    check(
        "end (with error)",
        octets(&[
            Hex(&[0x00, 0x53, 0x17]),
            Hex(&[0xc0, 0x1c, 0x01]),
            Hex(&[0x00, 0x53, 0x1d]),
            Hex(&[0xc0, 0x16, 0x01]),
            Hex(&[0xa3, 0x13]),
            Text(condition::INTERNAL_ERROR),
        ]),
        Performative::End(End {
            error: Some(AmqpError::new(condition::INTERNAL_ERROR)),
        }),
    );
}

#[test]
fn close_with_no_error_is_four_octets() {
    // Every field null, so the field list is `list0` and the whole
    // performative is the shortest a performative can be.
    check(
        "close",
        octets(&[Hex(&[0x00, 0x53, 0x18, 0x45])]),
        Performative::Close(Close::default()),
    );
}

#[test]
fn the_nine_descriptors_are_the_nine_the_specification_assigns() {
    let expected: [(u64, &str); 9] = [
        (0x10, "amqp:open:list"),
        (0x11, "amqp:begin:list"),
        (0x12, "amqp:attach:list"),
        (0x13, "amqp:flow:list"),
        (0x14, "amqp:transfer:list"),
        (0x15, "amqp:disposition:list"),
        (0x16, "amqp:detach:list"),
        (0x17, "amqp:end:list"),
        (0x18, "amqp:close:list"),
    ];
    assert_eq!(DESCRIPTORS, expected);
}

fn check_sasl(label: &str, expected: Vec<u8>, body: SaslFrame<'_>) {
    let (decoded, used) = SaslFrame::decode(&expected, Limits::DEFAULT)
        .unwrap_or_else(|e| panic!("{label} should decode: {e}"));
    assert_eq!(used, expected.len(), "{label} consumes exactly its octets");
    assert_eq!(decoded, body, "{label} decodes to its value");

    let mut written = Vec::new();
    body.encode(&mut written)
        .unwrap_or_else(|e| panic!("{label} should encode: {e}"));
    assert_eq!(written, expected, "{label} encodes to the golden octets");
}

#[test]
fn sasl_mechanisms_is_an_array_of_symbols_in_preference_order() {
    // 00 53 40              amqp:sasl-mechanisms:list
    // c0 15 01              one field
    //   e0 12 02 a3         array8, size 18, two elements, all sym8
    //     05 "PLAIN"
    //     09 "ANONYMOUS"
    check_sasl(
        "sasl-mechanisms",
        octets(&[
            Hex(&[0x00, 0x53, 0x40]),
            Hex(&[0xc0, 0x15, 0x01]),
            Hex(&[0xe0, 0x12, 0x02, 0xa3]),
            Hex(&[0x05]),
            Text(sasl::PLAIN),
            Hex(&[0x09]),
            Text(sasl::ANONYMOUS),
        ]),
        SaslFrame::Mechanisms(SaslMechanisms {
            server_mechanisms: Multiple::from_slice(&[sasl::PLAIN, sasl::ANONYMOUS]),
        }),
    );
}

#[test]
fn sasl_mechanisms_with_one_mechanism_is_a_bare_symbol() {
    // A server not requiring authentication SHOULD advertise exactly
    // ANONYMOUS, and one value is written as a symbol rather than a
    // one-element array.
    check_sasl(
        "sasl-mechanisms (one)",
        octets(&[
            Hex(&[0x00, 0x53, 0x40]),
            Hex(&[0xc0, 0x0c, 0x01]),
            Hex(&[0xa3, 0x09]),
            Text(sasl::ANONYMOUS),
        ]),
        SaslFrame::Mechanisms(SaslMechanisms {
            server_mechanisms: Multiple::One(sasl::ANONYMOUS),
        }),
    );
}

#[test]
fn sasl_init_carries_plains_nul_separated_response() {
    // 00 53 41            amqp:sasl-init:list
    // c0 16 02            two fields
    //   a3 05 "PLAIN"     mechanism
    //   a0 0c ...         initial-response: NUL guest NUL guest
    check_sasl(
        "sasl-init",
        octets(&[
            Hex(&[0x00, 0x53, 0x41]),
            Hex(&[0xc0, 0x16, 0x02]),
            Hex(&[0xa3, 0x05]),
            Text(sasl::PLAIN),
            Hex(&[0xa0, 0x0c, 0x00]),
            Text("guest"),
            Hex(&[0x00]),
            Text("guest"),
        ]),
        SaslFrame::Init(SaslInit {
            mechanism: sasl::PLAIN,
            initial_response: Some(b"\0guest\0guest"),
            hostname: None,
        }),
    );
}

#[test]
fn sasl_challenge_and_response_are_one_binary_field_each() {
    check_sasl(
        "sasl-challenge",
        octets(&[
            Hex(&[0x00, 0x53, 0x42]),
            Hex(&[0xc0, 0x08, 0x01]),
            Hex(&[0xa0, 0x05]),
            Text("nonce"),
        ]),
        SaslFrame::Challenge(b"nonce"),
    );
    check_sasl(
        "sasl-response",
        octets(&[
            Hex(&[0x00, 0x53, 0x43]),
            Hex(&[0xc0, 0x08, 0x01]),
            Hex(&[0xa0, 0x05]),
            Text("reply"),
        ]),
        SaslFrame::Response(b"reply"),
    );
}

#[test]
fn all_five_sasl_outcome_codes_have_a_vector() {
    for (octet, code) in [
        (0x00u8, SaslCode::Ok),
        (0x01, SaslCode::Auth),
        (0x02, SaslCode::Sys),
        (0x03, SaslCode::SysPerm),
        (0x04, SaslCode::SysTemp),
    ] {
        check_sasl(
            "sasl-outcome",
            octets(&[
                Hex(&[0x00, 0x53, 0x44]),
                Hex(&[0xc0, 0x03, 0x01]),
                Hex(&[0x50, octet]),
            ]),
            SaslFrame::Outcome(SaslOutcome {
                code,
                additional_data: None,
            }),
        );
    }
}

#[test]
fn the_three_protocol_headers_are_eight_octets_each() {
    for (header, expected, id) in [
        (
            ProtocolHeader::AMQP,
            b"AMQP\x00\x01\x00\x00",
            ProtocolId::Amqp,
        ),
        (
            ProtocolHeader::TLS,
            b"AMQP\x02\x01\x00\x00",
            ProtocolId::Tls,
        ),
        (
            ProtocolHeader::SASL,
            b"AMQP\x03\x01\x00\x00",
            ProtocolId::Sasl,
        ),
    ] {
        assert_eq!(&header.encode(), expected);
        assert_eq!(header.encode().len(), protocol_header::LEN);
        let decoded = protocol_header::decode(expected).expect("a header");
        assert_eq!(decoded.id, id);
        assert!(decoded.version.is_1_0_0());
    }
}

#[test]
fn an_open_inside_a_frame_is_the_first_thing_a_client_writes() {
    // The whole first write of a connection that needs no security layer:
    // eight octets of protocol header, then an AMQP frame on channel 0
    // carrying `open`.
    let mut wire = ProtocolHeader::AMQP.encode().to_vec();
    frame::write(&mut wire, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
        Performative::Open(Open::new("client-1")).encode(body)
    })
    .expect("fits the pre-negotiation frame size");

    assert_eq!(
        wire,
        octets(&[
            Hex(b"AMQP\x00\x01\x00\x00"),
            // SIZE 24, DOFF 2, TYPE 0x00 (AMQP), channel 0
            Hex(&[0x00, 0x00, 0x00, 0x18, 0x02, 0x00, 0x00, 0x00]),
            Hex(&[0x00, 0x53, 0x10]),
            Hex(&[0xc0, 0x0b, 0x01]),
            Hex(&[0xa1, 0x08]),
            Text("client-1"),
        ])
    );

    let header = protocol_header::decode(&wire).expect("a protocol header");
    assert_eq!(header, ProtocolHeader::AMQP);
    let read = frame::decode(&wire[protocol_header::LEN..], MIN_MAX_FRAME_SIZE).expect("a frame");
    assert_eq!(read.header.channel, 0);
    assert_eq!(read.used(), 24);
    let (performative, used) =
        Performative::decode(read.body, Limits::DEFAULT).expect("a performative");
    assert_eq!(used, read.body.len(), "no payload after an open");
    assert_eq!(performative, Performative::Open(Open::new("client-1")));
}

#[test]
fn a_sasl_init_inside_a_sasl_frame_is_typed_one() {
    let mut wire = ProtocolHeader::SASL.encode().to_vec();
    frame::write(&mut wire, FrameKind::Sasl, 0, SASL_MAX_FRAME_SIZE, |body| {
        SaslFrame::Init(SaslInit {
            mechanism: sasl::ANONYMOUS,
            initial_response: None,
            hostname: None,
        })
        .encode(body)
    })
    .expect("fits");

    assert_eq!(
        wire,
        octets(&[
            Hex(b"AMQP\x03\x01\x00\x00"),
            // SIZE 25, DOFF 2, TYPE 0x01 (SASL), the two type-specific
            // octets zero because a SASL frame has no channel.
            Hex(&[0x00, 0x00, 0x00, 0x19, 0x02, 0x01, 0x00, 0x00]),
            Hex(&[0x00, 0x53, 0x41]),
            Hex(&[0xc0, 0x0c, 0x01]),
            Hex(&[0xa3, 0x09]),
            Text(sasl::ANONYMOUS),
        ])
    );
    let read = frame::decode(&wire[protocol_header::LEN..], SASL_MAX_FRAME_SIZE).expect("a frame");
    assert_eq!(read.header.kind, FrameKind::Sasl);
}

#[test]
fn a_transfer_frame_is_a_performative_followed_by_an_opaque_payload() {
    // The one frame shape where the body is not just a performative: what
    // follows the transfer is the message, and the frame layer does not look
    // at it.
    let mut transfer = Transfer::new(0);
    transfer.delivery_id = Some(0);
    transfer.delivery_tag = Some(b"t");
    let payload = b"\x00\x53\x75\xa0\x05hello";

    let mut wire = Vec::new();
    frame::write(&mut wire, FrameKind::Amqp, 0, 4096, |body| {
        Performative::Transfer(transfer.clone()).encode(body)?;
        body.extend_from_slice(payload);
        Ok(())
    })
    .expect("fits");

    let read = frame::decode(&wire, 4096).expect("a frame");
    let (performative, used) =
        Performative::decode(read.body, Limits::DEFAULT).expect("a performative");
    assert_eq!(performative, Performative::Transfer(transfer));
    assert_eq!(
        &read.body[used..],
        payload,
        "everything after the performative is the payload, byte for byte"
    );
}
