//! DATA header decoder against arbitrary bytes.
//!
//! Properties: decoding never panics and never allocates unboundedly; an
//! accepted header respects every documented cap and the conditional key
//! requirements; and decoding is idempotent (re-encoding an accepted header and
//! decoding it again yields the same value).

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::DataHeader;
use weida_protocol::header::limits;

fuzz_target!(|data: &[u8]| {
    let Ok(header) = DataHeader::decode(data) else {
        return;
    };

    if let Some(e) = &header.endpoint {
        assert!(e.len() <= limits::MAX_ENDPOINT_BYTES);
    }
    if let Some(ct) = &header.content_type {
        assert!(ct.len() <= limits::MAX_CONTENT_TYPE_BYTES);
    }
    if let Some(tp) = &header.traceparent {
        assert!(tp.len() <= limits::MAX_TRACEPARENT_BYTES);
    }
    if let Some(ts) = &header.tracestate {
        assert!(ts.len() <= limits::MAX_TRACESTATE_BYTES);
    }
    match header.role {
        1 => assert!(header.endpoint.is_some(), "a request must carry an endpoint"),
        2 => assert!(
            header.correlation_id.is_some(),
            "a reply must carry a correlation id"
        ),
        _ => {}
    }

    let reencoded = header.encode();
    assert_eq!(DataHeader::decode(&reencoded).as_ref(), Ok(&header));
});
