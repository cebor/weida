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

use weida_core::{AckMode, TransferId};
use weida_protocol::{
    AckHeader, CancelHeader, DataHeader, FrameKind, Hello, SubscriptionHeader, encode_frame,
};

fn tid(v: u64) -> TransferId {
    TransferId::new(v).expect("non-zero")
}

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
    let h = DataHeader::request("/t", tid(1), AckMode::Accepted);
    assert_frame(
        "DATA request",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x0B, 0xA4, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x01, 0x02, 0x01, 0x04, 0x01,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
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
fn golden_ack_frame() {
    let h = AckHeader::accepted(tid(1));
    assert_frame(
        "ACK",
        FrameKind::Ack,
        h.encode(),
        &[0x57, 0x02, 0x05, 0xA2, 0x00, 0x01, 0x01, 0x01],
    );
    assert_eq!(AckHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_cancel_frame() {
    let h = CancelHeader { id: tid(1) };
    assert_frame(
        "CANCEL",
        FrameKind::Cancel,
        h.encode(),
        &[0x57, 0x04, 0x03, 0xA1, 0x00, 0x01],
    );
    assert_eq!(CancelHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_oneshot_data_frame() {
    // `ack_mode = 0` is the default and is omitted; `role = 0` is written,
    // because it selects behaviour rather than restating a default.
    let h = DataHeader::oneshot("/t", tid(1), AckMode::None);
    assert_frame(
        "DATA oneshot",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x09, 0xA3, 0x00, 0x62, 0x2F, 0x74, 0x01, 0x01, 0x02, 0x00,
        ],
    );
    assert_eq!(DataHeader::decode(&h.encode()).unwrap(), h);
}

#[test]
fn golden_fanout_data_frame() {
    // The shape of one copy a publisher writes to one subscriber.
    let mut h = DataHeader::oneshot("/md", tid(1), AckMode::None);
    h.topic = Some("px.eur".into());
    assert_frame(
        "DATA fan-out copy",
        FrameKind::Data,
        h.encode(),
        &[
            0x57, 0x01, 0x12, 0xA4, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x01, 0x02, 0x00, 0x09,
            0x66, 0x70, 0x78, 0x2E, 0x65, 0x75, 0x72,
        ],
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
            0x57, 0x05, 0x0B, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E,
        ],
    );
    assert_frame(
        "UNSUBSCRIBE",
        FrameKind::Unsubscribe,
        header.clone(),
        &[
            0x57, 0x06, 0x0B, 0xA2, 0x00, 0x63, 0x2F, 0x6D, 0x64, 0x01, 0x63, 0x70, 0x78, 0x2E,
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
