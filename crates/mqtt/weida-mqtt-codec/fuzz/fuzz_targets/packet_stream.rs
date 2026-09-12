//! A reader loop over arbitrary bytes.
//!
//! Property: the loop always terminates. Each round either consumes at least
//! one byte or stops, so no input makes a reader spin — the failure a
//! `Incomplete` returned for a packet that is already complete would produce,
//! and the reason MQTT's two length-mismatch cases are Malformed Packets here
//! rather than requests for more bytes.
//!
//! The second half feeds the same bytes one at a time, which is what a socket
//! actually does: every proper prefix of a packet MUST be `Incomplete` and MUST
//! NOT be a verdict, or a client would close a healthy connection because a
//! read boundary fell in the wrong place. Over WebSocket the specification
//! makes this explicit — a frame may hold a partial control packet and
//! receivers MUST NOT assume alignment ([MQTT-6.0.0-2]) [mqtt5 §3].

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::{DecodeError, Packet};

const CAP: u32 = 4096;

fuzz_target!(|data: &[u8]| {
    // 1. Read packets until the stream stops making sense.
    let mut offset = 0;
    let mut rounds = 0;
    while offset < data.len() {
        rounds += 1;
        assert!(rounds <= data.len() + 1, "the reader looped");
        match Packet::decode(&data[offset..], CAP) {
            Ok((_, used)) => {
                assert!(used > 0, "a packet consumed nothing");
                offset += used;
            }
            Err(_) => break,
        }
    }

    // 2. Whatever the first packet was, every proper prefix of it asks for
    //    more bytes rather than condemning the connection.
    if let Ok((_, used)) = Packet::decode(data, CAP) {
        for len in 0..used {
            let error = Packet::decode(&data[..len], CAP).expect_err("a prefix is not a packet");
            assert_eq!(error, DecodeError::Incomplete, "prefix of {len} bytes");
            assert!(!error.is_violation());
            assert_eq!(error.reason_code(), None);
        }
    }
});
