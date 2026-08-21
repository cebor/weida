//! W3C `traceparent` parser against arbitrary input.
//!
//! Properties: parsing never panics on arbitrary UTF-8, an accepted context
//! never holds a zero identifier, and format/parse is a roundtrip.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_core::TraceContext;
use weida_core::trace::MAX_TRACEPARENT_BYTES;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(ctx) = TraceContext::parse_traceparent(text) else {
        return;
    };

    assert_ne!(ctx.trace_id, [0u8; 16]);
    assert_ne!(ctx.span_id, [0u8; 8]);

    let formatted = ctx.to_traceparent();
    assert_eq!(formatted.len(), 55);
    assert!(formatted.len() <= MAX_TRACEPARENT_BYTES);
    assert_eq!(TraceContext::parse_traceparent(&formatted), Ok(ctx));
});
