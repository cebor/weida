//! Structured encode/decode roundtrip.
//!
//! Property: for every representable header, `decode(encode(h)) == h`.
//!
//! `Arbitrary` is derived here rather than in the library so that the protocol
//! crate keeps no fuzzing dependency. Text fields are truncated to their wire
//! caps, because a header that exceeds a cap is intentionally not
//! representable on the wire; oversized input is the `data_header` target's job.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use weida_protocol::DataHeader;
use weida_protocol::header::limits;

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
}

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

fuzz_target!(|input: ArbHeader| {
    // Every DATA field is optional on the wire, so every combination below is
    // representable: there are no conditional requirements left to satisfy.
    let header = DataHeader {
        endpoint: cap(input.endpoint, limits::MAX_ENDPOINT_BYTES),
        content_len: input.content_len,
        content_type: cap(input.content_type, limits::MAX_CONTENT_TYPE_BYTES),
        traceparent: cap(input.traceparent, limits::MAX_TRACEPARENT_BYTES),
        tracestate: cap(input.tracestate, limits::MAX_TRACESTATE_BYTES),
        topic: cap(input.topic, limits::MAX_TOPIC_BYTES),
        sequence: input.sequence,
        producer: input.producer,
    };

    let bytes = header.encode();
    assert_eq!(
        DataHeader::decode(&bytes).as_ref(),
        Ok(&header),
        "roundtrip mismatch for {header:?}"
    );
});
