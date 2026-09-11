//! Deterministic hostile-input smoke tests.
//!
//! These are the always-on companion to the `cargo fuzz` targets under
//! `crates/protocol/fuzz`: the same properties, driven by a seeded xorshift
//! generator so they run on stable Rust in CI and in `cargo test`, with no
//! nightly toolchain and no corpus. The libFuzzer targets explore far deeper;
//! these guarantee the properties are never left unchecked.
//!
//! Properties asserted for every input:
//!
//! * no panic, no arithmetic overflow, no unbounded allocation;
//! * a decoded header re-encodes and decodes to an identical value
//!   (decoding is idempotent — the fixed point matters, not byte equality,
//!   because unknown keys are dropped);
//! * every accepted value respects the documented caps.

use weida_core::{ErrorCode, TraceContext};
use weida_protocol::header::limits;
use weida_protocol::{
    DataHeader, ErrorHeader, Hello, SubscriptionHeader, encode_frame, parse_preamble,
};

const ITERATIONS: usize = 100_000;
const MAX_HEADER_BYTES: u64 = 16 * 1024;

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.below(max_len + 1);
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    /// Flips one random bit of `buf`.
    fn flip_bit(&mut self, buf: &mut [u8]) {
        if buf.is_empty() {
            return;
        }
        let i = self.below(buf.len());
        buf[i] ^= 1 << (self.below(8));
    }
}

#[test]
fn fuzz_smoke_preamble() {
    let mut rng = Rng::new(0x9e37_79b9_7f4a_7c15);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(24);
        if let Ok((preamble, used)) = parse_preamble(&input, MAX_HEADER_BYTES) {
            assert!(used <= input.len());
            assert!(used >= 3);
            assert!(preamble.header_len <= MAX_HEADER_BYTES);
        }
    }
}

#[test]
fn fuzz_smoke_preamble_from_valid_frames() {
    let mut rng = Rng::new(0x1234_5678_9abc_def0);
    let valid = encode_frame(
        weida_protocol::FrameKind::Data,
        &DataHeader::addressed("/t").encode(),
    );
    for _ in 0..ITERATIONS {
        let mut buf = valid.clone();
        for _ in 0..=rng.below(3) {
            rng.flip_bit(&mut buf);
        }
        if let Ok((preamble, used)) = parse_preamble(&buf, MAX_HEADER_BYTES) {
            assert!(preamble.header_len <= MAX_HEADER_BYTES);
            // A DATA frame's payload starts right after the header.
            let _ = buf.get(used..);
        }
    }
}

#[test]
fn fuzz_smoke_data_header() {
    let mut rng = Rng::new(0xdead_beef_cafe_1234);
    let mut accepted = 0usize;
    for _ in 0..ITERATIONS {
        let input = rng.bytes(64);
        if let Ok(header) = DataHeader::decode(&input) {
            accepted += 1;
            check_data_header(&header);
            let reencoded = header.encode();
            assert_eq!(
                DataHeader::decode(&reencoded).as_ref(),
                Ok(&header),
                "decoding is not idempotent for {input:?}"
            );
        }
    }
    // Random bytes rarely form a valid header; the mutation test below is what
    // exercises the accept path. This only guards against a decoder that
    // accepts everything.
    assert!(
        accepted < ITERATIONS / 2,
        "{accepted} of {ITERATIONS} random inputs decoded; the decoder is too permissive"
    );
}

#[test]
fn fuzz_smoke_data_header_from_valid_bytes() {
    let mut rng = Rng::new(0x0bad_c0de_0bad_c0de);
    let seeds = [
        DataHeader::addressed("/transform"),
        DataHeader::reply(),
        DataHeader {
            endpoint: Some("/x".into()),
            content_len: Some(u64::MAX),
            content_type: Some("application/cbor".into()),
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("a=1,b=2".into()),
            topic: Some("px.eur".into()),
            sequence: Some(u64::MAX),
            producer: Some([0xA5; 32]),
        },
    ];
    let mut accepted = 0usize;
    for i in 0..ITERATIONS {
        let mut buf = seeds[i % seeds.len()].encode();
        for _ in 0..=rng.below(4) {
            rng.flip_bit(&mut buf);
        }
        if let Ok(header) = DataHeader::decode(&buf) {
            accepted += 1;
            check_data_header(&header);
            assert_eq!(DataHeader::decode(&header.encode()).as_ref(), Ok(&header));
        }
    }
    assert!(accepted > 0, "bit-flipped headers never decoded");
}

#[test]
fn fuzz_smoke_hello() {
    let mut rng = Rng::new(0xfeed_face_0000_0001);
    let seed = Hello::v0(MAX_HEADER_BYTES, 1024).encode();
    for i in 0..ITERATIONS {
        let mut buf = if i % 2 == 0 {
            rng.bytes(48)
        } else {
            seed.clone()
        };
        if i % 2 == 1 {
            for _ in 0..=rng.below(3) {
                rng.flip_bit(&mut buf);
            }
        }
        if let Ok(hello) = Hello::decode(&buf) {
            assert!(hello.versions.len() <= limits::MAX_LIST_ITEMS);
            assert!(hello.capabilities.len() <= limits::MAX_LIST_ITEMS);
            assert!(hello.required_capabilities.len() <= limits::MAX_LIST_ITEMS);
            assert_eq!(Hello::decode(&hello.encode()).as_ref(), Ok(&hello));
        }
    }
}

#[test]
fn fuzz_smoke_control_headers() {
    let mut rng = Rng::new(0x5555_aaaa_5555_aaaa);
    // Both ERROR shapes: bare (the only one the transport ever writes) and
    // one carrying a diagnostic message.
    let bare = ErrorHeader::new(ErrorCode::NoReply).encode();
    let detailed = ErrorHeader {
        code: 4,
        message: Some("detail".into()),
    }
    .encode();
    let subscription = SubscriptionHeader::new("/md", "px.").encode();

    for i in 0..ITERATIONS {
        let (seed, which) = match i % 3 {
            0 => (&bare, 0),
            1 => (&detailed, 1),
            _ => (&subscription, 2),
        };
        let mut buf = if i % 6 < 3 {
            seed.clone()
        } else {
            rng.bytes(32)
        };
        for _ in 0..=rng.below(3) {
            rng.flip_bit(&mut buf);
        }
        match which {
            0 | 1 => {
                if let Ok(h) = ErrorHeader::decode(&buf) {
                    if let Some(m) = &h.message {
                        assert!(m.len() <= limits::MAX_MESSAGE_BYTES);
                    }
                    assert_eq!(ErrorHeader::decode(&h.encode()).as_ref(), Ok(&h));
                }
            }
            _ => {
                if let Ok(h) = SubscriptionHeader::decode(&buf) {
                    assert!(h.endpoint.len() <= limits::MAX_ENDPOINT_BYTES);
                    assert!(h.filter.len() <= limits::MAX_FILTER_BYTES);
                    assert_eq!(SubscriptionHeader::decode(&h.encode()).as_ref(), Ok(&h));
                }
            }
        }
    }
}

#[test]
fn fuzz_smoke_traceparent() {
    let mut rng = Rng::new(0x00f0_67aa_0ba9_02b7);
    const ALPHABET: &[u8] = b"0123456789abcdefABCDEF-_ ";
    let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let mut accepted = 0usize;

    for i in 0..ITERATIONS {
        let candidate = if i % 2 == 0 {
            let len = rng.below(70);
            (0..len)
                .map(|_| ALPHABET[rng.below(ALPHABET.len())] as char)
                .collect::<String>()
        } else {
            let mut bytes = valid.as_bytes().to_vec();
            let n = 1 + rng.below(2);
            for _ in 0..n {
                let idx = rng.below(bytes.len());
                bytes[idx] = ALPHABET[rng.below(ALPHABET.len())];
            }
            String::from_utf8(bytes).expect("alphabet is ascii")
        };
        if let Ok(ctx) = TraceContext::parse_traceparent(&candidate) {
            accepted += 1;
            assert_eq!(ctx.trace_id_hex().len(), 32);
            assert_eq!(ctx.span_id_hex().len(), 16);
            assert_ne!(ctx.trace_id, [0u8; 16]);
            assert_ne!(ctx.span_id, [0u8; 8]);
            let formatted = ctx.to_traceparent();
            assert_eq!(formatted.len(), 55);
            assert!(formatted.len() <= weida_core::trace::MAX_TRACEPARENT_BYTES);
            assert_eq!(TraceContext::parse_traceparent(&formatted), Ok(ctx));
        }
    }
    assert!(accepted > 0, "mutated traceparents never parsed");
}

#[test]
fn fuzz_smoke_data_header_roundtrip() {
    let mut rng = Rng::new(0x2545_f491_4f6c_dd1d);
    for _ in 0..ITERATIONS {
        let header = arbitrary_data_header(&mut rng);
        let bytes = header.encode();
        assert!(
            bytes.len() as u64 <= MAX_HEADER_BYTES,
            "generated header exceeds the wire cap"
        );
        assert_eq!(
            DataHeader::decode(&bytes).as_ref(),
            Ok(&header),
            "roundtrip mismatch"
        );
    }
}

#[test]
fn fuzz_smoke_subscribe() {
    let mut rng = Rng::new(0x7a5b_3c1d_9e0f_2468);
    let seed = SubscriptionHeader::new("/md", "px.").encode();
    let mut accepted = 0usize;
    for i in 0..ITERATIONS {
        // Alternate between free-form bytes and bit-flipped valid headers: the
        // first explores the reject path, the second the accept path.
        let input = if i % 2 == 0 {
            rng.bytes(48)
        } else {
            let mut buf = seed.clone();
            for _ in 0..=rng.below(3) {
                rng.flip_bit(&mut buf);
            }
            buf
        };
        let Ok(header) = SubscriptionHeader::decode(&input) else {
            continue;
        };
        accepted += 1;
        assert!(header.endpoint.len() <= limits::MAX_ENDPOINT_BYTES);
        assert!(header.filter.len() <= limits::MAX_FILTER_BYTES);
        assert_eq!(
            SubscriptionHeader::decode(&header.encode()).as_ref(),
            Ok(&header),
            "decoding is not idempotent for {input:?}"
        );
    }
    assert!(accepted > 0, "no subscription header ever decoded");
    assert!(
        accepted < ITERATIONS,
        "the decoder accepted every input; it is too permissive"
    );
}

fn check_data_header(header: &DataHeader) {
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
}

/// Builds a header covering the whole shape space: present and absent optional
/// fields, and strings at their caps.
fn arbitrary_data_header(rng: &mut Rng) -> DataHeader {
    let text = |rng: &mut Rng, max: usize| -> Option<String> {
        match rng.below(4) {
            0 => None,
            1 => Some(String::new()),
            2 => Some("x".repeat(max)),
            _ => Some("x".repeat(rng.below(max + 1))),
        }
    };
    DataHeader {
        endpoint: text(rng, limits::MAX_ENDPOINT_BYTES),
        content_len: if rng.below(2) == 0 {
            Some(rng.next_u64())
        } else {
            None
        },
        content_type: text(rng, limits::MAX_CONTENT_TYPE_BYTES),
        traceparent: text(rng, limits::MAX_TRACEPARENT_BYTES),
        tracestate: text(rng, limits::MAX_TRACESTATE_BYTES),
        topic: text(rng, limits::MAX_TOPIC_BYTES),
        sequence: if rng.below(2) == 0 {
            Some(rng.next_u64())
        } else {
            None
        },
        producer: if rng.below(2) == 0 {
            let mut digest = [0u8; limits::PRODUCER_BYTES];
            for chunk in digest.chunks_mut(8) {
                chunk.copy_from_slice(&rng.next_u64().to_le_bytes()[..chunk.len()]);
            }
            Some(digest)
        } else {
            None
        },
    }
}
