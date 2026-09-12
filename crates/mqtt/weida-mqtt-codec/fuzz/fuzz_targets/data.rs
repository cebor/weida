//! UTF-8 Encoded Strings and Binary Data against arbitrary bytes.
//!
//! Property: the two-byte length prefix is never believed. An accepted field
//! is at most 65,535 bytes, the reader advanced by exactly the prefix plus the
//! payload, and the borrowed slice lies inside the input.
//!
//! This is where a decoder that sized a buffer from a declared length would be
//! caught: a two-byte prefix can claim 65,535 bytes over a four-byte input,
//! and the fuzzer's memory limit — not an assertion — is what would report it.
//! The string half additionally pins the two MUSTs of 1.5.4: well-formed
//! UTF-8, and no U+0000.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_mqtt_codec::{Reader, data};

fuzz_target!(|input: &[u8]| {
    let mut reader = Reader::new(input);
    if let Ok(text) = reader.string() {
        assert!(text.len() <= data::MAX_FIELD_LEN);
        assert!(!text.as_bytes().contains(&0), "U+0000 must be refused");
        assert_eq!(reader.position(), text.len() + 2);

        let mut out = Vec::new();
        data::put_string(text, &mut out).expect("a decoded string re-encodes");
        assert_eq!(out, &input[..text.len() + 2]);
        assert_eq!(data::field_len(text.len()), Ok(out.len() as u32));
    }

    let mut reader = Reader::new(input);
    if let Ok(bytes) = reader.binary() {
        assert!(bytes.len() <= data::MAX_FIELD_LEN);
        assert_eq!(reader.position(), bytes.len() + 2);

        let mut out = Vec::new();
        data::put_binary(bytes, &mut out).expect("decoded binary data re-encodes");
        assert_eq!(out, &input[..bytes.len() + 2]);
    }
});
