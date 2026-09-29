//! Structured encode/decode roundtrip.
//!
//! Property: for every representable header, `decode(encode(h)) == h`.
//!
//! `Arbitrary` is derived here rather than in the library so that the protocol
//! crate keeps no fuzzing dependency. What the generator builds is deliberately
//! narrower than the type: text fields are truncated to their wire caps, every
//! level named is one this version defines, a report order is generated as the
//! two-key unit the decoder requires and is canonicalized to the strictly
//! ascending, capped form the encoder emits. A header that exceeds a cap,
//! names a reserved value or splits keys 9 and 10 is intentionally not
//! representable on the wire; feeding those bytes to a decoder is the
//! `data_header` target's job.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use weida_protocol::DataHeader;
use weida_protocol::header::{Acknowledgement, CursorLevel, ReportMode, limits};

#[derive(Arbitrary, Debug)]
struct ArbHeader {
    endpoint: Option<String>,
    content_len: Option<u64>,
    content_type: Option<String>,
    traceparent: Option<String>,
    tracestate: Option<String>,
    topic: Option<String>,
    sequence: Option<u64>,
    /// The wire form is a fixed-length digest, so the input is an array
    /// rather than a `Vec`: a shorter value is not representable on the wire
    /// and is the `data_header` target's job.
    producer: Option<[u8; limits::PRODUCER_BYTES]>,
    /// A seed for key `8`, mapped onto a defined acknowledgement level: an
    /// undefined one is refused by the decoder rather than roundtripped.
    achieved: Option<u8>,
    /// Keys `9` and `10` are one statement in two halves — the decoder
    /// refuses either without the other — so they are generated as one value
    /// that is present or absent together.
    report: Option<ArbReport>,
    /// Key `11`. `false` is `Progress`, the default the encoder never writes.
    report_final_only: bool,
    /// Key `13`, a RADIO segment number.
    segment: Option<u64>,
}

/// An ordered report: the id of key `9` and one seed per level of key `10`.
#[derive(Arbitrary, Debug)]
struct ArbReport {
    id: u64,
    levels: Vec<u8>,
}

/// How many acknowledgement levels this version defines. Every value below
/// the width is a level, so a seed taken modulo it always names one.
const DEFINED_LEVELS: u64 = 6;

/// Truncates on a char boundary so the result stays valid UTF-8.
fn cap(text: Option<String>, max: usize) -> Option<String> {
    text.map(|mut s| {
        if s.len() > max {
            let mut end = max;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
        }
        s
    })
}

/// Maps a seed onto a level that exists on the wire: the defined
/// acknowledgement ladder, then the open application range above the floor.
/// The undefined values in between are a protocol violation, not a header
/// that roundtrips.
fn cursor_level(seed: u8) -> CursorLevel {
    let seed = u64::from(seed);
    if seed < DEFINED_LEVELS {
        CursorLevel::Known(Acknowledgement::from_wire(seed).expect("below the ladder's width"))
    } else {
        CursorLevel::Application(CursorLevel::APPLICATION_FLOOR + seed - DEFINED_LEVELS)
    }
}

/// The canonical key `10`: strictly ascending by wire value and no longer
/// than the cap, which is the only form the encoder emits and the decoder
/// accepts.
fn canonical_report(seeds: Vec<u8>) -> Vec<CursorLevel> {
    let mut levels: Vec<CursorLevel> = seeds.into_iter().map(cursor_level).collect();
    levels.sort_unstable_by_key(|level| level.to_wire());
    levels.dedup_by_key(|level| level.to_wire());
    levels.truncate(limits::MAX_REPORT_LEVELS);
    levels
}

fuzz_target!(|input: ArbHeader| {
    // An empty level list is not an order at all, so it takes the id with it:
    // key 9 without key 10 is exactly the header the decoder rejects.
    let ordered = input
        .report
        .map(|report| (report.id, canonical_report(report.levels)))
        .filter(|(_, levels)| !levels.is_empty());
    let (report_id, report) = match ordered {
        Some((id, levels)) => (Some(id), levels),
        None => (None, Vec::new()),
    };

    let header = DataHeader {
        endpoint: cap(input.endpoint, limits::MAX_ENDPOINT_BYTES),
        content_len: input.content_len,
        content_type: cap(input.content_type, limits::MAX_CONTENT_TYPE_BYTES),
        traceparent: cap(input.traceparent, limits::MAX_TRACEPARENT_BYTES),
        tracestate: cap(input.tracestate, limits::MAX_TRACESTATE_BYTES),
        topic: cap(input.topic, limits::MAX_TOPIC_BYTES),
        sequence: input.sequence,
        producer: input.producer,
        achieved: input.achieved.map(|seed| {
            Acknowledgement::from_wire(u64::from(seed) % DEFINED_LEVELS)
                .expect("below the ladder's width")
        }),
        report_id,
        report,
        report_mode: if input.report_final_only {
            ReportMode::FinalOnly
        } else {
            ReportMode::Progress
        },
        segment: input.segment,
    };

    let bytes = header.encode();
    assert_eq!(
        DataHeader::decode(&bytes).as_ref(),
        Ok(&header),
        "roundtrip mismatch for {header:?}"
    );
});
