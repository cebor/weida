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
use weida_core::TransferId;
use weida_protocol::DataHeader;
use weida_protocol::header::limits;

#[derive(Arbitrary, Debug)]
struct ArbHeader {
    endpoint: Option<String>,
    transfer_id: u64,
    role: u64,
    correlation_id: Option<u64>,
    ack_mode: u64,
    content_len: Option<u64>,
    content_type: Option<String>,
    traceparent: Option<String>,
    tracestate: Option<String>,
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
    let role = input.role;
    let endpoint = cap(input.endpoint, limits::MAX_ENDPOINT_BYTES);
    let correlation_id = input.correlation_id.and_then(TransferId::new);

    // The conditional requirements are part of the wire contract, so only
    // headers that satisfy them are representable.
    let endpoint = if role == 1 {
        Some(endpoint.unwrap_or_else(|| "/".to_owned()))
    } else {
        endpoint
    };
    let correlation_id = if role == 2 {
        Some(correlation_id.unwrap_or(TransferId::FIRST))
    } else {
        correlation_id
    };

    let header = DataHeader {
        endpoint,
        transfer_id: TransferId::new(input.transfer_id).unwrap_or(TransferId::FIRST),
        role,
        correlation_id,
        ack_mode: input.ack_mode,
        content_len: input.content_len,
        content_type: cap(input.content_type, limits::MAX_CONTENT_TYPE_BYTES),
        traceparent: cap(input.traceparent, limits::MAX_TRACEPARENT_BYTES),
        tracestate: cap(input.tracestate, limits::MAX_TRACESTATE_BYTES),
    };

    let bytes = header.encode();
    assert_eq!(
        DataHeader::decode(&bytes).as_ref(),
        Ok(&header),
        "roundtrip mismatch for {header:?}"
    );
});
