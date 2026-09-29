//! FLOW header and datagram prefix against arbitrary bytes.
//!
//! Properties: a FLOW header decodes without panicking, respects every cap,
//! and decoding is idempotent; a datagram prefix split never panics, never
//! claims more bytes than it was given, and re-encodes to no more bytes than
//! it consumed — which is what makes "payload starts at the offset" safe on
//! hostile input.
//!
//! A truncated prefix is deliberately *not* a failure: the datagram is
//! dropped and counted like one for an unknown flow (`docs/PROTOCOL.md` §6.9).

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_protocol::header::limits;
use weida_protocol::{FlowHeader, flow_prefix, split_flow_datagram};

fuzz_target!(|data: &[u8]| {
    if let Ok(header) = FlowHeader::decode(data) {
        assert!(header.endpoint.len() <= limits::MAX_ENDPOINT_BYTES);
        for (value, max) in [
            (&header.content_type, limits::MAX_CONTENT_TYPE_BYTES),
            (&header.traceparent, limits::MAX_TRACEPARENT_BYTES),
            (&header.tracestate, limits::MAX_TRACESTATE_BYTES),
            (&header.topic, limits::MAX_TOPIC_BYTES),
        ] {
            assert!(value.as_ref().is_none_or(|v| v.len() <= max));
        }
        assert_eq!(FlowHeader::decode(&header.encode()), Ok(header));
    }

    if let Some((flow, offset)) = split_flow_datagram(data) {
        assert!(offset > 0 && offset <= data.len(), "{offset} of {}", data.len());
        let (_, len) = flow_prefix(flow);
        assert!(len <= offset);
    }
});
