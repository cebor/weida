//! Deterministic hostile-input smoke tests for the MQTT codec.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/mqtt/weida-mqtt-codec/fuzz`: the same properties, driven by a
//! seeded xorshift generator so they run on stable Rust, in `cargo test`, with
//! no nightly toolchain and no corpus. The libFuzzer targets explore far
//! deeper; these make sure the properties are never left unchecked.
//!
//! Four properties, and the first is the one MQTT gives no help with.
//!
//! 1. **An accepted packet never exceeds the cap, and a rejected one allocates
//!    nothing.** A packet may declare 268,435,455 bytes from the Remaining
//!    Length alone (2.1.4) and `Maximum Packet Size` is absent by default,
//!    meaning no limit below that (3.1.2.11.4) [mqtt5 §5]. So the decoder's
//!    cap is the whole defence. The tests assert the first directly and the
//!    second by construction — every input here is a few dozen bytes, so a
//!    decoder that reserved the declared length would abort the test process
//!    rather than fail it.
//! 2. **`Incomplete` is reachable only while bytes are genuinely missing.**
//!    A reader loop that parks on a packet the peer has finished sending is
//!    deadlocked, so once the whole declared packet is present the answer must
//!    be a packet or a violation and never a request for more. This is the
//!    property that caught the property-block and packet-body length cases,
//!    and it is asserted by feeding every decoder its own exact byte count.
//! 3. **Whatever decodes, re-encodes to something that decodes the same.**
//!    Not byte-for-byte: a peer may send properties in any order and may spell
//!    an all-zero PUBACK long or short, while this codec emits one canonical
//!    form [mqtt5 §3]. Re-decoding is the check that survives that.
//! 4. **Progress.** A stream reader fed arbitrary bytes always either consumes
//!    at least one packet or stops, never loops.

use weida_mqtt_codec::property::{Properties, PropertySet};
use weida_mqtt_codec::{DecodeError, Packet, Reader, data, varint};

const ITERATIONS: usize = 100_000;

/// Deliberately small: every declared packet above it must be refused from the
/// fixed header alone, and a 64-byte input can never satisfy it by accident.
const CAP: u32 = 4096;

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() >> 33) as usize % bound
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.byte()).collect()
    }
}

#[test]
fn fuzz_smoke_varint_never_panics_and_round_trips_what_it_accepts() {
    let mut rng = Rng::new(0x5150_4D51_5454_0001);
    for _ in 0..ITERATIONS {
        let len = rng.below(7);
        let input = rng.bytes(len);
        match varint::decode(&input) {
            Ok((value, used)) => {
                assert!(value <= varint::MAX);
                assert!((1..=varint::MAX_BYTES).contains(&used));
                assert!(used <= input.len());
                assert_eq!(used, varint::encoded_len(value), "{input:02X?} is minimal");

                let mut out = Vec::new();
                varint::encode(value, &mut out).expect("a decoded value re-encodes");
                assert_eq!(out, &input[..used], "the encoding is canonical");
            }
            Err(error) => assert!(
                matches!(
                    error,
                    DecodeError::Incomplete
                        | DecodeError::VarintNotMinimal
                        | DecodeError::VarintTooLong
                ),
                "{input:02X?}: {error}"
            ),
        }
    }
}

/// A string is length-prefixed, so this is where a decoder that trusts a
/// declared length would be caught: a two-byte prefix can claim 65,535 bytes
/// over a buffer holding six.
#[test]
fn fuzz_smoke_strings_and_binary_never_over_read() {
    let mut rng = Rng::new(0x5150_4D51_5454_0002);
    for _ in 0..ITERATIONS {
        let input = {
            let len = rng.below(12);
            rng.bytes(len)
        };

        let mut reader = Reader::new(&input);
        if let Ok(text) = reader.string() {
            assert!(text.len() <= data::MAX_FIELD_LEN);
            assert!(!text.as_bytes().contains(&0));
            assert_eq!(reader.position(), text.len() + 2);
            let mut out = Vec::new();
            data::put_string(text, &mut out).expect("re-encodes");
            assert_eq!(out, &input[..text.len() + 2]);
        }

        let mut reader = Reader::new(&input);
        if let Ok(bytes) = reader.binary() {
            assert!(bytes.len() <= data::MAX_FIELD_LEN);
            assert_eq!(reader.position(), bytes.len() + 2);
        }
    }
}

/// The property block is the one place where "not enough bytes" must *not* be
/// `Incomplete`: the block's own declared length is authoritative, so the
/// decoder is handed exactly that many bytes and anything short is malformed.
#[test]
fn fuzz_smoke_properties_over_an_exact_block_never_ask_for_more() {
    let mut rng = Rng::new(0x5150_4D51_5454_0003);
    let sets = [
        PropertySet::CONNECT,
        PropertySet::CONNACK,
        PropertySet::WILL,
        PropertySet::PUBLISH,
        PropertySet::NONE,
    ];

    for _ in 0..ITERATIONS {
        let body = {
            let len = rng.below(20);
            rng.bytes(len)
        };
        let allowed = sets[rng.below(sets.len())];

        // A block whose declared length is exactly what follows it: every
        // declared byte is present, so `Incomplete` would be a lie.
        let mut framed = Vec::new();
        varint::encode(body.len() as u32, &mut framed).expect("a small length");
        framed.extend_from_slice(&body);

        let mut reader = Reader::new(&framed);
        match Properties::decode(&mut reader, allowed, None) {
            Ok(properties) => {
                assert!(reader.is_empty(), "the whole block was consumed");
                // Whatever it accepted, it can say how long it is and write
                // it back out as something that decodes the same.
                let len = properties
                    .encoded_len(allowed)
                    .expect("a decoded set re-encodes");
                let mut out = Vec::new();
                properties.encode(allowed, &mut out).expect("re-encodes");
                assert_eq!(out.len() as u32, len);

                let mut again = Reader::new(&out);
                let reparsed =
                    Properties::decode(&mut again, allowed, None).expect("the canonical form");
                assert_eq!(reparsed, properties, "{body:02X?}");
            }
            Err(error) => assert_ne!(
                error,
                DecodeError::Incomplete,
                "{body:02X?}: an exact block never asks for more"
            ),
        }
    }
}

/// The whole decoder against arbitrary bytes: nothing panics, an accepted
/// packet fits the cap, and no rejection reserves memory.
#[test]
fn fuzz_smoke_packets_never_panic_and_respect_the_cap() {
    let mut rng = Rng::new(0x5150_4D51_5454_0004);
    for _ in 0..ITERATIONS {
        let input = {
            let len = rng.below(48);
            rng.bytes(len)
        };
        if let Ok((packet, used)) = Packet::decode(&input, CAP) {
            assert!(used <= input.len());
            assert!(used as u32 <= CAP);
            assert_eq!(packet.encoded_len(), Ok(used as u32));

            let mut out = Vec::new();
            packet
                .encode(&mut out)
                .expect("a decoded packet re-encodes");
            let (again, used2) = Packet::decode(&out, CAP).expect("the canonical form");
            assert_eq!(again, packet);
            assert_eq!(used2, out.len());
        }
    }
}

/// Mutations of a valid packet, which is how the decoder is reached past its
/// fixed header at all.
///
/// Uniformly random bytes almost never build a CONNECT: the four-byte protocol
/// name alone is a 1-in-2^32 event. So the generator starts from a packet that
/// is valid in every field — vectors 2 and 4 of `docs/adapters/mqtt5.md` §10.1
/// — and flips bytes in the body, which lands the decoder in the property
/// block, the Will Properties and the length-prefixed payload fields where the
/// interesting faults live.
///
/// Two counters are asserted rather than one. Without the accept counter the
/// test could pass while rejecting everything, which proves nothing; without
/// the reject counter it could pass while accepting everything, which proves
/// less.
#[test]
fn fuzz_smoke_mutations_of_a_valid_packet() {
    const CONNECT: [u8; 34] = [
        0x10, 0x20, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0xCE, 0x00, 0x3C, 0x00, 0x00, 0x01,
        0x63, 0x05, 0x18, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x01, 0x64, 0x00, 0x01, 0x01, 0x00, 0x01,
        0x75, 0x00, 0x01, 0x70,
    ];
    const CONNACK: [u8; 8] = [0x20, 0x06, 0x01, 0x00, 0x03, 0x21, 0x00, 0x0A];

    let mut rng = Rng::new(0x5150_4D51_5454_0005);
    let mut accepted = 0usize;
    let mut rejected = 0usize;

    for _ in 0..ITERATIONS {
        let mut input: Vec<u8> = if rng.below(2) == 0 {
            CONNECT.to_vec()
        } else {
            CONNACK.to_vec()
        };

        // Never the Remaining Length byte at index 1: the property this test
        // asserts is that a *complete* packet is never `Incomplete`, and
        // rewriting its declared length is how that stops being true.
        for _ in 0..1 + rng.below(3) {
            let at = 2 + rng.below(input.len() - 2);
            input[at] = rng.byte();
        }

        match Packet::decode(&input, CAP) {
            Ok((packet, used)) => {
                accepted += 1;
                assert_eq!(used, input.len());
                let mut out = Vec::new();
                packet.encode(&mut out).expect("re-encodes");
                let (again, _) = Packet::decode(&out, CAP).expect("the canonical form");
                assert_eq!(again, packet);
            }
            // The fixed header declares exactly the bytes that follow, so a
            // complete packet is present and `Incomplete` is never the answer.
            Err(error) => {
                rejected += 1;
                assert_ne!(error, DecodeError::Incomplete, "{input:02X?}");
                assert!(error.is_violation(), "{input:02X?}: {error}");
                assert!(error.reason_code().is_some(), "{error}");
            }
        }
    }

    assert!(accepted > 0, "no mutation was ever harmless");
    assert!(rejected > 0, "no mutation was ever caught");
}

/// A stream reader fed arbitrary bytes always terminates: each round either
/// consumes at least one byte or stops.
#[test]
fn fuzz_smoke_a_stream_reader_always_terminates() {
    let mut rng = Rng::new(0x5150_4D51_5454_0006);
    for _ in 0..ITERATIONS / 10 {
        let stream = {
            let len = rng.below(96);
            rng.bytes(len)
        };
        let mut offset = 0;
        let mut rounds = 0;

        while offset < stream.len() {
            rounds += 1;
            assert!(rounds <= stream.len() + 1, "the reader looped");
            match Packet::decode(&stream[offset..], CAP) {
                Ok((_, used)) => {
                    assert!(used > 0, "a packet consumed nothing");
                    offset += used;
                }
                Err(_) => break,
            }
        }
    }
}
