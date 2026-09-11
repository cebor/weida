//! Deterministic hostile-input smoke tests for the ZMTP codec.
//!
//! The always-on companion to the `cargo fuzz` targets under
//! `crates/adapters/weida-zmtp/fuzz`: the same properties, driven by a seeded
//! xorshift generator so they run on stable Rust, in `cargo test`, with no
//! nightly toolchain and no corpus. The libFuzzer targets explore far deeper;
//! these make sure the properties are never left unchecked.
//!
//! The property that matters most here is the one ZMTP gives no help with. A
//! frame may declare 2^63-1 octets, there is no credit on the wire, and
//! libzmq's `ZMQ_MAXMSGSIZE` is unlimited by default, so the decoder's cap is
//! the whole defence: **an accepted frame never reports a length above the cap,
//! and a rejected one allocates nothing.** The tests assert the first directly
//! and the second by construction - every input here is a few dozen octets, so
//! a decoder that reserved the declared length would abort the test process
//! rather than fail it.

use weida_zmtp::error::{FrameError, GreetingError};
use weida_zmtp::{Command, Greeting, Mechanism, Metadata, SocketType, frame, greeting};

const ITERATIONS: usize = 100_000;
/// Deliberately small: every declared length above it must be refused from the
/// header alone, and a 64-octet input can never satisfy it by accident.
const CAP: u64 = 4096;

/// Deterministic xorshift64* generator: no dependency, reproducible failures.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 33) as u8
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() >> 33) as usize % n
    }

    /// Random bytes, biased towards short inputs where the interesting
    /// truncations live.
    fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max + 1);
        (0..len).map(|_| self.byte()).collect()
    }
}

#[test]
fn fuzz_smoke_frame_header() {
    let mut rng = Rng::new(0x5A17);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(24);
        match frame::decode(&input, CAP) {
            Ok((header, body, used)) => {
                assert!(header.len <= CAP, "an accepted frame is within the cap");
                assert_eq!(body.len() as u64, header.len);
                assert!(used <= input.len());
                assert!(used >= 2, "a frame is at least a flags and a size octet");
                // Re-encoding an accepted frame yields the canonical form,
                // which decodes to the same header. Byte equality is not the
                // property: a peer may send a long size for a short body.
                let re = frame::encode(header.kind, body);
                let (again, body2, _) = frame::decode(&re, CAP).expect("re-decode");
                assert_eq!(again, header);
                assert_eq!(body2, body);
            }
            Err(e) => assert!(e == FrameError::Incomplete || e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_frame_header_declares_more_than_it_carries() {
    // The adversarial shape, built on purpose rather than stumbled upon: a
    // valid long header with an arbitrary 64-bit length and no body at all.
    let mut rng = Rng::new(0xD00D);
    for _ in 0..ITERATIONS {
        let mut input = vec![if rng.byte() & 1 == 0 { 0x02 } else { 0x06 }];
        input.extend_from_slice(&rng.next_u64().to_be_bytes());
        input.extend_from_slice(&rng.bytes(8));
        match frame::decode(&input, CAP) {
            // Accepted only when the declared length is both within the cap
            // and actually present.
            Ok((header, body, _)) => {
                assert!(header.len <= CAP);
                assert_eq!(body.len() as u64, header.len);
            }
            Err(e) => assert!(e.is_violation() || e == FrameError::Incomplete),
        }
        // And the header alone must decide: no body, no allocation.
        let header_only = &input[..9];
        match frame::decode_header(header_only, CAP) {
            Ok((header, used)) => {
                assert!(header.len <= CAP);
                assert_eq!(used, 9);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_command_bodies() {
    let mut rng = Rng::new(0xC0FFEE);
    for _ in 0..ITERATIONS {
        let body = rng.bytes(40);
        match Command::decode(&body) {
            Ok(command) => {
                check_command(&command);
                // Idempotence: an accepted command re-encodes and decodes to
                // the same value.
                let frame_bytes = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&frame_bytes, CAP).expect("decode");
                assert_eq!(Command::decode(body2).expect("re-decode"), command);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_command_bodies_from_valid_prefixes() {
    // Random bytes rarely start with a valid command name, so this half feeds
    // well-formed names with hostile data behind them.
    let names: [&[u8]; 7] = [
        b"\x05READY",
        b"\x05ERROR",
        b"\x09SUBSCRIBE",
        b"\x06CANCEL",
        b"\x04PING",
        b"\x04PONG",
        b"\x04JOIN",
    ];
    let mut rng = Rng::new(0xBEEF);
    for _ in 0..ITERATIONS {
        let mut body = names[rng.below(names.len())].to_vec();
        body.extend_from_slice(&rng.bytes(32));
        match Command::decode(&body) {
            Ok(command) => {
                check_command(&command);
                let re = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&re, CAP).expect("decode");
                assert_eq!(Command::decode(body2).expect("re-decode"), command);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_metadata() {
    let mut rng = Rng::new(0xFEED);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(32);
        match Metadata::decode(&input) {
            Ok(md) => {
                for (name, value) in md.properties() {
                    assert!(!name.is_empty() && name.len() <= 255);
                    assert!(value.len() <= i32::MAX as usize);
                }
                // A decoded dictionary re-encodes byte-for-byte: unlike a
                // weida header there are no unknown keys to drop, so the
                // fixed point here really is byte equality.
                let mut re = Vec::new();
                md.encode(&mut re)
                    .expect("an accepted dictionary re-encodes");
                assert_eq!(re, input);
                let _ = md.socket_type();
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_greeting() {
    let mut rng = Rng::new(0x515E);
    for _ in 0..ITERATIONS {
        let input = rng.bytes(70);
        match Greeting::decode(&input) {
            Ok(g) => {
                assert!(input.len() >= 64);
                assert!(!g.mechanism.name().is_empty());
                // Re-encoding is byte-exact over the fields that carry
                // meaning; padding and filler are not among them, so the
                // comparison is the field set, not the octets.
                assert_eq!(Greeting::decode(&g.encode()).expect("re-decode"), g);
                let _ = g.accept(Mechanism::NULL);
            }
            Err(e) => assert!(e == GreetingError::Incomplete || e.is_violation()),
        }
        match greeting::sniff_major(&input) {
            Ok(_) => assert!(input.len() >= 11),
            Err(e) => assert!(e == GreetingError::Incomplete || e.is_violation()),
        }
    }
}

#[test]
fn fuzz_smoke_greeting_from_valid_ones() {
    // A real greeting with one octet corrupted: the shape where a decoder that
    // validates too much or too little shows up.
    let mut rng = Rng::new(0x1234);
    for _ in 0..ITERATIONS {
        let original = Greeting::null().encode();
        let mut bytes = original;
        let at = rng.below(64);
        // Guaranteed to be a different octet, so "decoded to the same
        // greeting" means the field truly carries no meaning rather than that
        // the generator happened to rewrite the same value.
        let mut octet = rng.byte();
        if octet == original[at] {
            octet = octet.wrapping_add(1);
        }
        bytes[at] = octet;
        match Greeting::decode(&bytes) {
            Ok(g) => {
                // The octets that may differ without changing meaning are
                // exactly the padding and the filler. Everything else either
                // changes a field or is refused.
                let cosmetic = (1..9).contains(&at) || at >= 33;
                assert!(
                    cosmetic || g != Greeting::null(),
                    "octet {at} changed nothing and is not padding or filler"
                );
                assert_eq!(Greeting::decode(&g.encode()).expect("re-decode"), g);
            }
            Err(e) => assert!(e.is_violation()),
        }
    }
}

/// Every documented bound an accepted command must respect.
fn check_command(command: &Command<'_>) {
    match command {
        Command::Ready(md) => {
            for (name, _) in md.properties() {
                assert!(!name.is_empty());
            }
            if let Some(t) = md.socket_type() {
                assert!(SocketType::parse(t.as_str().as_bytes()) == Some(t));
            }
        }
        Command::Error(reason) => {
            assert!(reason.len() <= 255);
            assert!(reason.bytes().all(|b| (0x20..=0x7E).contains(&b)));
        }
        Command::Subscribe(_) | Command::Cancel(_) => {}
        Command::Ping { context, .. } | Command::Pong { context } => {
            assert!(context.len() <= weida_zmtp::MAX_PING_CONTEXT);
        }
    }
    // A command that decoded must also encode; the two halves share their
    // length rules, so one accepting what the other refuses is a bug.
    assert!(
        command.encode().is_ok(),
        "{} decoded but will not encode",
        command.name()
    );
}
