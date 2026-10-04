//! Conformance: the golden test vectors of `docs/PROTOCOL.md` §8.
//!
//! §8 states that implementations MUST encode exactly these bytes and MUST
//! decode these bytes back to these field sets. The vectors are specified as
//! **complete frames** — preamble and header together — so they are asserted
//! that way here, as whole `encode_frame` output built from real headers.
//!
//! This lives in an integration test rather than beside the codecs on purpose:
//! §8 is a statement about the crate's public surface, so the check consumes
//! exactly that surface, with no access to private helpers. The unit tests in
//! `frame.rs` and `header.rs` cover the two halves independently; this file is
//! what makes the documented frames byte-exact as published.

use weida_core::ErrorCode;
use weida_protocol::header::{
    Acknowledgement, CursorLevel, GuaranteeSet, HeaderError, OrderingMode, ReportMode, limits,
};
use weida_protocol::{
    CAPABILITY_DATAGRAM, CreditHeader, CursorHeader, DataHeader, ErrorHeader, FlowHeader,
    FrameKind, Hello, SubscriptionHeader, decode_cursor_record, encode_cursor_record, encode_frame,
    flow_prefix, split_flow_datagram,
};

/// Asserts one documented frame, and that its header half decodes back.
#[track_caller]
fn assert_frame(name: &str, kind: FrameKind, header: Vec<u8>, expected: &[u8]) {
    let frame = encode_frame(kind, &header);
    assert_eq!(frame, expected, "{name}: frame bytes");
    // The preamble's advertised length must agree with the header actually
    // written; a hardcoded length in a test cannot catch that drift.
    let (preamble, used) = weida_protocol::parse_preamble(&frame, 16 * 1024)
        .unwrap_or_else(|e| panic!("{name}: preamble does not parse: {e}"));
    assert_eq!(preamble.kind, kind, "{name}: kind");
    assert_eq!(
        preamble.header_len as usize,
        header.len(),
        "{name}: advertised header length"
    );
    assert_eq!(&frame[used..], &header[..], "{name}: header bytes verbatim");
}

#[test]
fn golden_data_request_frame() {
    let h = DataHeader::addressed("/t");
    assert_frame(
        "DATA request",
        FrameKind::Data,
        h.encode(),
        &[0x57, 0x01, 0x05, 0xA1, 0x00, 0x62, 0x2F, 0x74],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_data_reply_frame() {
    // The bidirectional stream is the correlation, so the reply half names
    // neither an endpoint nor the request it answers: the header is empty.
    let h = DataHeader::reply();
    assert_frame(
        "DATA reply",
        FrameKind::Data,
        h.encode(),
        &[0x57, 0x01, 0x01, 0xA0],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_data_confirm_frame() {
    // The L2 publisher confirm: a reply half carrying the achieved level and
    // nothing else. Three header bytes, and no frame kind of its own
    // (`docs/decisions/0018-minimal-broker.md` §4.6).
    let h = DataHeader {
        achieved: Some(Acknowledgement::Accepted),
        ..DataHeader::reply()
    };
    assert_frame(
        "DATA confirm",
        FrameKind::Data,
        h.encode(),
        &[0x57, 0x01, 0x03, 0xA1, 0x08, 0x02],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn an_undefined_achieved_level_is_refused_rather_than_read_as_the_weakest() {
    // `A1 08 06`: key 8, value 6 — one past `Processed`. Skipping it or
    // reading it as `None` would invent a claim about a producer's message,
    // so the decoder refuses the header (`docs/PROTOCOL.md` §6.2).
    let err = DataHeader::decode(&[0xA1, 0x08, 0x06]).expect_err("undefined level");
    assert!(
        matches!(
            err,
            HeaderError::UnknownLevel {
                dimension: "achieved",
                value: 6
            }
        ),
        "{err:?}"
    );
}

#[test]
fn golden_hello_frame() {
    let h = Hello::v0(16384, 1024);
    assert_frame(
        "HELLO",
        FrameKind::Hello,
        h.encode(),
        &[
            0x57, 0x00, 0x10, 0xA5, 0x00, 0x81, 0x00, 0x01, 0x19, 0x40, 0x00, 0x02, 0x19, 0x04,
            0x00, 0x03, 0x80, 0x04, 0x80,
        ],
    );
    assert_eq!(Hello::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_error_frame() {
    let h = ErrorHeader::new(ErrorCode::NoReply);
    assert_frame(
        "ERROR",
        FrameKind::Error,
        h.encode(),
        &[0x57, 0x02, 0x03, 0xA1, 0x00, 0x05],
    );
    assert_eq!(ErrorHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_fanout_data_frame() {
    // The shape of one copy a publisher writes to one subscriber.
    let mut h = DataHeader::addressed("/md");
    h.topic = Some("px.eur".into());
    assert_frame(
        "DATA fan-out copy",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x0E, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x05, 0x66, 0x70, 0x78, 0x2E,
            0x65, 0x75, 0x72,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

/// The digest the §8 vectors use: SHA-256 of `"test"`, the same value the
/// address examples in `docs/PROTOCOL.md` carry.
const VECTOR_PRODUCER: [u8; 32] = [
    0x9F, 0x86, 0xD0, 0x81, 0x88, 0x4C, 0x7D, 0x65, 0x9A, 0x2F, 0xEA, 0xA0, 0xC5, 0x5A, 0xD0, 0x15,
    0xA3, 0xBF, 0x4F, 0x1B, 0x2B, 0x0B, 0x82, 0x2C, 0xD1, 0x5D, 0x6C, 0x15, 0xB0, 0xF0, 0x0A, 0x08,
];

#[test]
fn golden_sequenced_data_frame() {
    // DATA key 6. Specified ahead of code: the codec writes it when set, and
    // no v0 sender sets it.
    let mut h = DataHeader::addressed("/t");
    h.sequence = Some(1);
    assert_frame(
        "DATA with a sequence",
        FrameKind::Data,
        h.encode(),
        &[0x57, 0x01, 0x07, 0xA2, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_relayed_data_frame() {
    // DATA keys 6 and 7 together: the shape a relay or an L2 hop writes when
    // the producer is not the connection peer. The producer is the raw
    // 32-byte digest as a CBOR `bstr` (0x58 0x20 …), never the hex spelling.
    let mut h = DataHeader::addressed("/t");
    h.sequence = Some(1);
    h.producer = Some(VECTOR_PRODUCER);
    let mut expected = vec![
        0x57, 0x01, 0x2A, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x06, 0x01, 0x07, 0x58, 0x20,
    ];
    expected.extend_from_slice(&VECTOR_PRODUCER);
    assert_frame(
        "DATA with a sequence and a producer",
        FrameKind::Data,
        h.encode(),
        &expected,
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_subscribe_and_unsubscribe_frames() {
    let h = SubscriptionHeader::new("/md", "px.");
    let header = h.encode();
    assert_frame(
        "SUBSCRIBE",
        FrameKind::Subscribe,
        header.clone(),
        &[
            0x57, 0x03, 0x0B, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E,
        ],
    );
    assert_frame(
        "UNSUBSCRIBE",
        FrameKind::Unsubscribe,
        header.clone(),
        &[
            0x57, 0x04, 0x0B, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E,
        ],
    );
    // One header layout serves both kinds: the two frames differ in exactly
    // one byte, and it is the kind byte.
    let sub = encode_frame(FrameKind::Subscribe, &header);
    let unsub = encode_frame(FrameKind::Unsubscribe, &header);
    let differing: Vec<usize> = (0..sub.len()).filter(|&i| sub[i] != unsub[i]).collect();
    assert_eq!(differing, vec![1], "only the kind byte may differ");
    assert_eq!(SubscriptionHeader::decode(&header).unwrap(), h);
}

/// The filter grammar of §6.4 on the wire: one vector per construct.
///
/// These fix the *encoding* of a filter string; what each selects is the
/// matcher's business (`weida::pubsub`), and the comments name it so the two
/// cannot drift apart silently.
#[test]
fn golden_filter_grammar_frames() {
    // A literal filter: whole segments, nothing special.
    assert_frame(
        "SUBSCRIBE literal",
        FrameKind::Subscribe,
        SubscriptionHeader::new("/md", "px.eur").encode(),
        &[
            0x57, 0x03, 0x0E, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x66, 0x70, 0x78, 0x2E,
            0x65, 0x75, 0x72,
        ],
    );
    // `*` in the middle: exactly one segment there, literal on both sides.
    assert_frame(
        "SUBSCRIBE one-segment wildcard",
        FrameKind::Subscribe,
        SubscriptionHeader::new("/md", "sensors.*.temp").encode(),
        &[
            0x57, 0x03, 0x16, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x6E, 0x73, 0x65, 0x6E,
            0x73, 0x6F, 0x72, 0x73, 0x2E, 0x2A, 0x2E, 0x74, 0x65, 0x6D, 0x70,
        ],
    );
    // Trailing `#`: the parent and everything under it.
    assert_frame(
        "SUBSCRIBE rest wildcard",
        FrameKind::Subscribe,
        SubscriptionHeader::new("/md", "ctl.#").encode(),
        &[
            0x57, 0x03, 0x0D, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x65, 0x63, 0x74, 0x6C,
            0x2E, 0x23,
        ],
    );
    // The empty filter: every topic. The key is present and the value is the
    // empty text string `0x60`, because absent and empty must stay distinct.
    assert_frame(
        "SUBSCRIBE empty filter",
        FrameKind::Subscribe,
        SubscriptionHeader::new("/md", "").encode(),
        &[
            0x57, 0x03, 0x08, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x60,
        ],
    );

    for filter in ["px.eur", "sensors.*.temp", "ctl.#", ""] {
        let h = SubscriptionHeader::new("/md", filter);
        assert_eq!(SubscriptionHeader::decode(&h.encode()).unwrap(), h);
    }
}

/// A published topic is data, not a pattern: `*` in a topic is a byte like
/// any other, and the encoder treats it as such.
#[test]
fn golden_literal_wildcard_topic_frame() {
    let mut h = DataHeader::addressed("/md");
    h.topic = Some("px.*".into());
    assert_frame(
        "DATA topic containing a literal `*`",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x0C, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x05, 0x64, 0x70, 0x78, 0x2E,
            0x2A,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

/// The extended HELLO: the last vector §8 listed as outstanding.
///
/// Both declarations are written because both are above `core`; a `core`
/// declaration is never written, which is what keeps a v0 HELLO byte-exact
/// (`golden_hello_frame` above pins that).
#[test]
fn golden_hello_with_guarantees_frame() {
    let detect = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        ..GuaranteeSet::CORE
    };
    let h = Hello {
        guarantees_offered: Some(detect),
        guarantees_required: Some(detect),
        ..Hello::v0(16384, 1024)
    };
    assert_frame(
        "HELLO with guarantee declarations",
        FrameKind::Hello,
        h.encode(),
        &[
            0x57, 0x00, 0x18, 0xA7, 0x00, 0x81, 0x00, 0x01, 0x19, 0x40, 0x00, 0x02, 0x19, 0x04,
            0x00, 0x03, 0x80, 0x04, 0x80, 0x05, 0xA1, 0x04, 0x01, 0x06, 0xA1, 0x04, 0x01,
        ],
    );
    assert_eq!(Hello::decode(&h.encode()).unwrap(), h);

    // A `core` declaration is not written at all: the same HELLO with
    // explicit `core` sets is byte-identical to the v0 one.
    let explicit_core = Hello {
        guarantees_offered: Some(GuaranteeSet::CORE),
        guarantees_required: Some(GuaranteeSet::CORE),
        ..Hello::v0(16384, 1024)
    };
    assert_eq!(explicit_core.encode(), Hello::v0(16384, 1024).encode());
}

#[test]
fn golden_credit_frames() {
    // The L2 credit frame: which subscription, and how many messages it will
    // take in total (`docs/decisions/0003-credit-unit.md` §4.2).
    let granting = CreditHeader::new("/jobs", "px.eur", 5);
    assert_frame(
        "CREDIT",
        FrameKind::Credit,
        granting.encode(),
        &[
            0x57, 0x05, 0x12, 0xA3, 0x00, 0x65, 0x2F, 0x6A, 0x6F, 0x62, 0x73, 0x01, 0x66, 0x70,
            0x78, 0x2E, 0x65, 0x75, 0x72, 0x02, 0x05,
        ],
    );
    assert_eq!(CreditHeader::decode(&granting.encode()).unwrap(), granting);

    // The pause, and the value every subscription starts at: the empty filter
    // is the whole-queue subscription, and `0` is a consumer that will take
    // nothing until it says otherwise.
    let paused = CreditHeader::new("/jobs", "", 0);
    assert_frame(
        "CREDIT zero",
        FrameKind::Credit,
        paused.encode(),
        &[
            0x57, 0x05, 0x0C, 0xA3, 0x00, 0x65, 0x2F, 0x6A, 0x6F, 0x62, 0x73, 0x01, 0x60, 0x02,
            0x00,
        ],
    );
    assert_eq!(CreditHeader::decode(&paused.encode()).unwrap(), paused);
}

#[test]
fn a_credit_header_without_a_limit_is_refused() {
    // `A2 00 62 2F 71 01 60`: endpoint and filter, no limit. An absent limit
    // and a limit of zero would otherwise be the same frame, and zero is the
    // pause — so the key is required rather than defaulted.
    let err = CreditHeader::decode(&[0xA2, 0x00, 0x62, 0x2F, 0x71, 0x01, 0x60])
        .expect_err("no limit key");
    assert!(matches!(err, HeaderError::MissingKey(2)), "{err:?}");
}

#[test]
fn golden_cursor_head_frame() {
    // Kind 6, one required key: which report this stream carries. No length,
    // no endpoint, no correlation beyond the id the DATA header allocated
    // (`docs/decisions/0024-three-families-one-back-channel.md` §4.4).
    let h = CursorHeader { report_id: 1 };
    assert_frame(
        "CURSOR head",
        FrameKind::Cursor,
        h.encode(),
        &[0x57, 0x06, 0x03, 0xA1, 0x00, 0x01],
    );
    assert_eq!(CursorHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_cursor_records() {
    // Records are QUIC varint pairs, not CBOR: `Accepted` at byte 64 is three
    // bytes, and the 2-byte offset form is the shortest that carries 64.
    let mut out = Vec::new();
    encode_cursor_record(CursorLevel::Known(Acknowledgement::Accepted), 64, &mut out).unwrap();
    assert_eq!(out, [0x02, 0x40, 0x40]);
    assert_eq!(
        decode_cursor_record(&out).unwrap(),
        Some((CursorLevel::Known(Acknowledgement::Accepted), 64, 3))
    );

    let mut out = Vec::new();
    encode_cursor_record(
        CursorLevel::Known(Acknowledgement::Stored),
        1_000_000,
        &mut out,
    )
    .unwrap();
    assert_eq!(out, [0x03, 0x80, 0x0F, 0x42, 0x40]);
    assert_eq!(
        decode_cursor_record(&out).unwrap(),
        Some((CursorLevel::Known(Acknowledgement::Stored), 1_000_000, 5))
    );
}

#[test]
fn golden_data_orders_a_report() {
    // Keys 9 and 10: the id of the CURSOR stream to expect, and the single
    // level ordered on it. The mode stays absent because `Progress` is the
    // default, exactly as a `core` guarantee declaration is never written.
    let h = DataHeader {
        report_id: Some(1),
        report: vec![CursorLevel::Known(Acknowledgement::Accepted)],
        ..DataHeader::addressed("/t")
    };
    assert_frame(
        "DATA orders a report",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x0A, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x09, 0x01, 0x0A, 0x81, 0x02,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_data_orders_final_only() {
    // Two levels — one weida's, one the application's — and the non-default
    // mode. The array is strictly ascending, which is what makes the wire
    // form canonical.
    let h = DataHeader {
        report_id: Some(1),
        report: vec![
            CursorLevel::Known(Acknowledgement::Stored),
            CursorLevel::Application(17),
        ],
        report_mode: ReportMode::FinalOnly,
        ..DataHeader::addressed("/t")
    };
    assert_frame(
        "DATA orders final-only",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x0D, 0xA4, 0x00, 0x62, 0x2F, 0x74, 0x09, 0x01, 0x0A, 0x82, 0x03, 0x11,
            0x0B, 0x01,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

/// One rejection per decoder rule of §6.2 keys `9`-`11`.
///
/// Each of these is a header a hostile or buggy peer can send, and each names
/// a different error: a report nobody can serve, a non-canonical order, an
/// unbounded one, a level from the range a later version of this
/// specification will fill, and a mode this one does not define.
#[test]
fn a_malformed_report_order_is_refused_rule_by_rule() {
    // `A1 0A 81 02`: key 10 with no key 9 — an order with no stream.
    let err = DataHeader::decode(&[0xA1, 0x0A, 0x81, 0x02]).expect_err("report without id");
    assert_eq!(err, HeaderError::InvalidReport("report without report_id"));

    // `A1 09 01`: key 9 with no key 10 — a stream with nothing to report.
    let err = DataHeader::decode(&[0xA1, 0x09, 0x01]).expect_err("id without report");
    assert_eq!(err, HeaderError::InvalidReport("report_id without report"));

    // `A2 09 01 0A 82 03 02`: levels 3 then 2 — descending.
    let err = DataHeader::decode(&[0xA2, 0x09, 0x01, 0x0A, 0x82, 0x03, 0x02])
        .expect_err("descending levels");
    assert_eq!(err, HeaderError::InvalidReport("report levels must ascend"));

    // The same level twice is the same violation: ascent is strict, which is
    // what makes duplicate detection free.
    let err = DataHeader::decode(&[0xA2, 0x09, 0x01, 0x0A, 0x82, 0x02, 0x02])
        .expect_err("repeated level");
    assert_eq!(err, HeaderError::InvalidReport("report levels must ascend"));

    // 17 ascending application levels: one past the cap.
    let over_cap = DataHeader {
        report_id: Some(1),
        report: (0..=limits::MAX_REPORT_LEVELS as u64)
            .map(|i| CursorLevel::Application(CursorLevel::APPLICATION_FLOOR + i))
            .collect(),
        ..DataHeader::reply()
    };
    let err = DataHeader::decode(&over_cap.encode()).expect_err("too many levels");
    assert_eq!(err, HeaderError::InvalidReport("too many report levels"));

    // `A2 09 01 0A 81 0F`: level 15 — reserved, and not an application's to
    // name, so it is refused rather than carried as an opaque stage.
    let err =
        DataHeader::decode(&[0xA2, 0x09, 0x01, 0x0A, 0x81, 0x0F]).expect_err("reserved level 15");
    assert_eq!(
        err,
        HeaderError::UnknownLevel {
            dimension: "report",
            value: 15
        }
    );

    // `A3 09 01 0A 81 02 0B 02`: report mode 2.
    let err = DataHeader::decode(&[0xA3, 0x09, 0x01, 0x0A, 0x81, 0x02, 0x0B, 0x02])
        .expect_err("unknown report mode");
    assert_eq!(
        err,
        HeaderError::UnknownLevel {
            dimension: "report_mode",
            value: 2
        }
    );
}

#[test]
fn a_cursor_head_frame_without_a_report_id_is_refused() {
    // `A0`: the empty map. The id is the only thing a CURSOR stream names, so
    // an absent one names no report at all.
    let err = CursorHeader::decode(&[0xA0]).expect_err("no report id");
    assert_eq!(err, HeaderError::MissingKey(0));
}

#[test]
fn a_cursor_record_that_ends_mid_value_asks_for_more_bytes() {
    // A record is at most 16 bytes and self-delimiting, so a reader that sees
    // half of one reads more rather than closing the connection: only an
    // undefined reserved level is a violation.
    let mut whole = Vec::new();
    encode_cursor_record(
        CursorLevel::Known(Acknowledgement::Stored),
        1_000_000,
        &mut whole,
    )
    .unwrap();
    for cut in 0..whole.len() {
        assert_eq!(
            decode_cursor_record(&whole[..cut]),
            Ok(None),
            "a {cut}-byte prefix is not a violation"
        );
    }
    assert_eq!(
        decode_cursor_record(&[0x0F, 0x00]),
        Err(HeaderError::UnknownLevel {
            dimension: "cursor",
            value: 15
        })
    );
}

#[test]
fn golden_flow_frame() {
    let h = FlowHeader::new("/v", 7);
    assert_frame(
        "FLOW",
        FrameKind::Flow,
        h.encode(),
        &[0x57, 0x07, 0x07, 0xA2, 0x00, 0x62, 0x2F, 0x76, 0x01, 0x07],
    );
    assert_eq!(FlowHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn a_flow_header_without_a_flow_id_is_refused() {
    // `A1 00 62 2F 76`: an endpoint and no key 1. A flow no datagram can
    // name is not a flow (`docs/PROTOCOL.md` §6.8).
    assert_eq!(
        FlowHeader::decode(&[0xA1, 0x00, 0x62, 0x2F, 0x76]),
        Err(HeaderError::MissingKey(1))
    );
}

#[test]
fn golden_flow_datagram_payload() {
    // Not a frame: the payload of one QUIC DATAGRAM frame, flow 7, "hi".
    let (prefix, len) = flow_prefix(7);
    let mut datagram = prefix[..len].to_vec();
    datagram.extend_from_slice(b"hi");
    assert_eq!(datagram, [0x07, 0x68, 0x69]);
    assert_eq!(split_flow_datagram(&datagram), Some((7, 1)));
}

#[test]
fn golden_segment_data_frame() {
    let h = DataHeader {
        segment: Some(5),
        ..DataHeader::addressed("/t")
    };
    assert_frame(
        "DATA segment",
        FrameKind::Data,
        h.encode(),
        &[0x57, 0x01, 0x07, 0xA2, 0x00, 0x62, 0x2F, 0x74, 0x0D, 0x05],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_subscribe_with_max_age_frame() {
    let h = SubscriptionHeader {
        max_age_ms: Some(150),
        ..SubscriptionHeader::new("/t", "a")
    };
    assert_frame(
        "SUB max_age_ms",
        FrameKind::Subscribe,
        h.encode(),
        &[
            0x57, 0x03, 0x0B, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x61, 0x61, 0x02, 0x18, 0x96,
        ],
    );
    assert_eq!(SubscriptionHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_layered_segment_data_frame() {
    let h = DataHeader {
        segment: Some(5),
        layer: Some(2),
        ..DataHeader::addressed("/t")
    };
    assert_frame(
        "DATA layered segment",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x09, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x0D, 0x05, 0x0E, 0x02,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_subscribe_with_max_layer_frame() {
    let h = SubscriptionHeader {
        max_layer: Some(1),
        ..SubscriptionHeader::new("/t", "a")
    };
    assert_frame(
        "SUB max_layer",
        FrameKind::Subscribe,
        h.encode(),
        &[
            0x57, 0x03, 0x0A, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x61, 0x61, 0x03, 0x01,
        ],
    );
    assert_eq!(SubscriptionHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn a_layer_above_fifteen_or_without_a_segment_is_a_violation() {
    // `docs/PROTOCOL.md` §8's three layer violations, each a header that
    // closes the connection with `PROTOCOL_VIOLATION`.
    assert_eq!(
        DataHeader::decode(&[0xA2, 0x0D, 0x05, 0x0E, 0x10]),
        Err(HeaderError::InvalidLayer("layer above 15"))
    );
    assert_eq!(
        DataHeader::decode(&[0xA1, 0x0E, 0x01]),
        Err(HeaderError::InvalidLayer("layer without segment"))
    );
    assert_eq!(
        SubscriptionHeader::decode(&[0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x61, 0x61, 0x03, 0x10]),
        Err(HeaderError::InvalidLayer("max_layer above 15"))
    );
}

#[test]
fn golden_hello_with_datagram_frame() {
    let mut h = Hello::v0(16384, 1024);
    h.capabilities = vec![CAPABILITY_DATAGRAM];
    assert_frame(
        "HELLO datagram",
        FrameKind::Hello,
        h.encode(),
        &[
            0x57, 0x00, 0x11, 0xA5, 0x00, 0x81, 0x00, 0x01, 0x19, 0x40, 0x00, 0x02, 0x19, 0x04,
            0x00, 0x03, 0x81, 0x01, 0x04, 0x80,
        ],
    );
    assert_eq!(Hello::decode(&h.encode()).unwrap(), h);
}
