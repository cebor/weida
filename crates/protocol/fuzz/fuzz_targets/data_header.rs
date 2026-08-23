//! DATA header decoder against arbitrary bytes.
//!
//! Properties: decoding never panics and never allocates unboundedly; an
//! accepted header respects every documented cap; and decoding is idempotent
//! (re-encoding an accepted header and decoding it again yields the same
//! value).
//!
//! There are no conditional key requirements to check: the decoder cannot see
//! whether the bytes came from an initiating or a reply stream, so
//! `endpoint`-on-initiating-streams is enforced by the transport's dispatch.

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
    if let Some(topic) = &header.topic {
        assert!(topic.len() <= limits::MAX_TOPIC_BYTES);
    }

    let reencoded = header.encode();
    assert_eq!(DataHeader::decode(&reencoded).as_ref(), Ok(&header));
});
