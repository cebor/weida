//! A deliberately narrow JSON reader and writer for the two objects the NATS
//! control line carries.
//!
//! `INFO` and `CONNECT` are the only places in the protocol where JSON
//! appears, and this crate has no dependencies — not even a JSON one — so the
//! reader is hand-written. It is **not a JSON parser** and must not grow into
//! one. What it accepts is exactly the shapes the protocol reference types its
//! fields with:
//!
//! * a string, with `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t` and `\uXXXX`
//!   including surrogate pairs — `\u` is not optional in practice, because
//!   Go's `encoding/json`, which `nats-server` marshals `INFO` with, escapes
//!   `<`, `>` and `&` as `\u003c`, `\u003e` and `\u0026` by default;
//! * an integer, refusing a fraction, an exponent and a negative, since none
//!   of `port`, `proto` or `max_payload` has a meaningful negative value;
//! * `true` and `false`;
//! * an array of strings, bounded by a caller-supplied cap because a JSON
//!   array announces no length before its elements;
//! * `null`, which is read as "the field is absent".
//!
//! Anything else in a field this crate knows is a named error rather than a
//! best effort. A field it does *not* know is stepped over — including an
//! object or an array, to a bounded depth — so that a server which grows a
//! new `INFO` field does not make the whole `INFO` unreadable. That skip is
//! the only recursion in the file, and
//! [`MAX_JSON_DEPTH`](crate::limits::MAX_JSON_DEPTH) bounds it.
//!
//! The writer is narrower still: `CONNECT` is the only object this crate
//! produces, its keys are fixed, and its values are strings, booleans and one
//! integer. Escaping covers `"`, `\` and every C0 control, which is the whole
//! set that can end a JSON string or a NATS control line early.

use std::borrow::Cow;

use crate::error::DecodeError;
use crate::limits::MAX_JSON_DEPTH;

/// Append `value` in decimal ASCII.
///
/// Shared by the control line and the JSON writer: both need decimal digits
/// and neither has any business allocating a `String` to get them.
pub(crate) fn push_decimal(out: &mut Vec<u8>, mut value: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// Store `value` in `slot`, refusing a second copy of the same field.
///
/// Last-one-wins would make the meaning of an `INFO` depend on which reader
/// read it, so a repeated field is a violation.
pub(crate) fn set_once<T>(
    slot: &mut Option<T>,
    field: &'static str,
    value: T,
) -> Result<(), DecodeError> {
    if slot.is_some() {
        return Err(DecodeError::JsonDuplicateField { field });
    }
    *slot = Some(value);
    Ok(())
}

/// A cursor over one JSON object, reading it key by key.
pub(crate) struct Scanner<'a> {
    input: &'a [u8],
    at: usize,
    first: bool,
}

impl<'a> Scanner<'a> {
    /// Open `input` as a JSON object, positioned just after the `{`.
    pub(crate) fn object(input: &'a [u8]) -> Result<Self, DecodeError> {
        let mut scanner = Scanner {
            input,
            at: 0,
            first: true,
        };
        scanner.skip_whitespace();
        if scanner.peek() != Some(b'{') {
            return Err(DecodeError::JsonNotAnObject);
        }
        scanner.at += 1;
        Ok(scanner)
    }

    /// The next key, or `None` at the closing brace.
    ///
    /// On `Some`, the cursor stands on the value. The key is returned as raw
    /// octets: every key this crate matches is an unescaped ASCII identifier,
    /// so a key that did carry an escape simply matches nothing and its value
    /// is skipped.
    pub(crate) fn next_key(&mut self) -> Result<Option<&'a [u8]>, DecodeError> {
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.at += 1;
            self.skip_whitespace();
            if self.at != self.input.len() {
                return Err(DecodeError::JsonUnexpected { at: self.at });
            }
            return Ok(None);
        }
        if !self.first {
            if self.peek() != Some(b',') {
                return Err(self.unexpected());
            }
            self.at += 1;
            self.skip_whitespace();
        }
        self.first = false;
        let key = self.raw_string()?;
        self.skip_whitespace();
        if self.peek() != Some(b':') {
            return Err(self.unexpected());
        }
        self.at += 1;
        self.skip_whitespace();
        Ok(Some(key))
    }

    /// Consume a `null` value if that is what stands here.
    ///
    /// An explicit `null` is read as an absent field: `INFO` marks most of
    /// its fields optional, and a server that writes one out as `null`
    /// rather than omitting it means the same thing.
    pub(crate) fn value_is_null(&mut self) -> bool {
        if self.input[self.at..].starts_with(b"null") {
            self.at += 4;
            true
        } else {
            false
        }
    }

    /// A string value, borrowed when it carries no escape.
    pub(crate) fn string_value(
        &mut self,
        field: &'static str,
    ) -> Result<Cow<'a, str>, DecodeError> {
        if self.peek() != Some(b'"') {
            return Err(DecodeError::JsonWrongType {
                field,
                expected: "a string",
            });
        }
        let start = self.at;
        let raw = self.raw_string()?;
        unescape(raw, start + 1)
    }

    /// A `true` or `false` value.
    pub(crate) fn bool_value(&mut self, field: &'static str) -> Result<bool, DecodeError> {
        if self.input[self.at..].starts_with(b"true") {
            self.at += 4;
            Ok(true)
        } else if self.input[self.at..].starts_with(b"false") {
            self.at += 5;
            Ok(false)
        } else {
            Err(DecodeError::JsonWrongType {
                field,
                expected: "a boolean",
            })
        }
    }

    /// An integer value.
    pub(crate) fn u64_value(&mut self, field: &'static str) -> Result<u64, DecodeError> {
        let token = self.number_token();
        if token.is_empty() {
            return Err(DecodeError::JsonWrongType {
                field,
                expected: "an integer",
            });
        }
        if token.iter().any(|b| matches!(b, b'.' | b'e' | b'E')) {
            return Err(DecodeError::JsonNotAnInteger { field });
        }
        if token[0] == b'-' {
            return Err(DecodeError::JsonNumberOutOfRange { field });
        }
        let mut value: u64 = 0;
        for &byte in token {
            if !byte.is_ascii_digit() {
                return Err(DecodeError::JsonNotAnInteger { field });
            }
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(byte - b'0')))
                .ok_or(DecodeError::JsonNumberOutOfRange { field })?;
        }
        Ok(value)
    }

    /// An array of strings, refused at `cap` elements.
    ///
    /// The cap is checked as the elements arrive, not from a length, because
    /// a JSON array has none. Every element before the cap is already in the
    /// vector when the cap trips, which is why the cap has to be small enough
    /// that holding that many is affordable.
    pub(crate) fn string_array(
        &mut self,
        field: &'static str,
        cap: u32,
    ) -> Result<Vec<Cow<'a, str>>, DecodeError> {
        if self.peek() != Some(b'[') {
            return Err(DecodeError::JsonWrongType {
                field,
                expected: "an array of strings",
            });
        }
        self.at += 1;
        let mut elements = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b']') {
                self.at += 1;
                return Ok(elements);
            }
            if !elements.is_empty() {
                if self.peek() != Some(b',') {
                    return Err(self.unexpected());
                }
                self.at += 1;
                self.skip_whitespace();
            }
            if elements.len() as u64 >= u64::from(cap) {
                return Err(DecodeError::ArrayTooLong { field, cap });
            }
            elements.push(self.string_value(field)?);
        }
    }

    /// Step over the value of a field this crate does not read.
    pub(crate) fn skip_value(&mut self) -> Result<(), DecodeError> {
        self.skip_at_depth(1)
    }

    fn skip_at_depth(&mut self, depth: u32) -> Result<(), DecodeError> {
        if depth > MAX_JSON_DEPTH {
            return Err(DecodeError::JsonTooDeep {
                cap: MAX_JSON_DEPTH,
            });
        }
        self.skip_whitespace();
        match self.peek() {
            None => Err(DecodeError::JsonTruncated),
            Some(b'"') => {
                self.raw_string()?;
                Ok(())
            }
            Some(b'{') => {
                self.at += 1;
                let mut seen = false;
                loop {
                    self.skip_whitespace();
                    if self.peek() == Some(b'}') {
                        self.at += 1;
                        return Ok(());
                    }
                    if seen {
                        if self.peek() != Some(b',') {
                            return Err(self.unexpected());
                        }
                        self.at += 1;
                        self.skip_whitespace();
                    }
                    seen = true;
                    self.raw_string()?;
                    self.skip_whitespace();
                    if self.peek() != Some(b':') {
                        return Err(self.unexpected());
                    }
                    self.at += 1;
                    self.skip_at_depth(depth + 1)?;
                }
            }
            Some(b'[') => {
                self.at += 1;
                let mut seen = false;
                loop {
                    self.skip_whitespace();
                    if self.peek() == Some(b']') {
                        self.at += 1;
                        return Ok(());
                    }
                    if seen {
                        if self.peek() != Some(b',') {
                            return Err(self.unexpected());
                        }
                        self.at += 1;
                    }
                    seen = true;
                    self.skip_at_depth(depth + 1)?;
                }
            }
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => {
                self.number_token();
                Ok(())
            }
            Some(_) => Err(self.unexpected()),
        }
    }

    fn literal(&mut self, word: &[u8]) -> Result<(), DecodeError> {
        if self.input[self.at..].starts_with(word) {
            self.at += word.len();
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    /// The octets between the quotes, escapes left as they arrived.
    ///
    /// A backslash consumes the octet after it, which is the whole reason a
    /// string can be found without being decoded: `"\""` ends at the third
    /// quote, not the second.
    fn raw_string(&mut self) -> Result<&'a [u8], DecodeError> {
        if self.peek() != Some(b'"') {
            return Err(self.unexpected());
        }
        let start = self.at + 1;
        let mut at = start;
        loop {
            match self.input.get(at) {
                None => return Err(DecodeError::JsonTruncated),
                Some(b'"') => {
                    self.at = at + 1;
                    return Ok(&self.input[start..at]);
                }
                Some(b'\\') => at += 2,
                Some(_) => at += 1,
            }
        }
    }

    fn number_token(&mut self) -> &'a [u8] {
        let start = self.at;
        let mut end = start;
        while matches!(
            self.input.get(end),
            Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
        ) {
            end += 1;
        }
        self.at = end;
        &self.input[start..end]
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.at).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.at += 1;
        }
    }

    fn unexpected(&self) -> DecodeError {
        if self.at >= self.input.len() {
            DecodeError::JsonTruncated
        } else {
            DecodeError::JsonUnexpected { at: self.at }
        }
    }
}

/// Resolve the escapes of one string, borrowing when there are none.
///
/// `offset` is where the raw octets start inside the whole object, so that an
/// error names a position the caller can find.
fn unescape(raw: &[u8], offset: usize) -> Result<Cow<'_, str>, DecodeError> {
    if !raw.contains(&b'\\') {
        return std::str::from_utf8(raw)
            .map(Cow::Borrowed)
            .map_err(|_| DecodeError::JsonInvalidUtf8);
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut at = 0;
    while at < raw.len() {
        let byte = raw[at];
        if byte != b'\\' {
            out.push(byte);
            at += 1;
            continue;
        }
        let escape = *raw
            .get(at + 1)
            .ok_or(DecodeError::JsonBadEscape { at: offset + at })?;
        at += 2;
        let plain = match escape {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let (code, read) = unescape_code_point(raw, at, offset)?;
                at += read;
                let mut buf = [0u8; 4];
                out.extend_from_slice(code.encode_utf8(&mut buf).as_bytes());
                continue;
            }
            _ => {
                return Err(DecodeError::JsonBadEscape {
                    at: offset + at - 2,
                });
            }
        };
        out.push(plain);
    }
    String::from_utf8(out)
        .map(Cow::Owned)
        .map_err(|_| DecodeError::JsonInvalidUtf8)
}

/// One `\uXXXX`, and the low surrogate after it where the first is high.
///
/// Returns the character and how many octets past `at` it consumed.
fn unescape_code_point(raw: &[u8], at: usize, offset: usize) -> Result<(char, usize), DecodeError> {
    let bad = DecodeError::JsonBadEscape {
        at: offset + at - 2,
    };
    let first = hex4(raw, at).ok_or(bad)?;
    if !(0xd800..0xdc00).contains(&first) {
        return char::from_u32(u32::from(first)).map(|c| (c, 4)).ok_or(bad);
    }
    // A high surrogate carries only half a code point; JSON writes the other
    // half as a second `\u` escape immediately after it.
    if raw.get(at + 4) != Some(&b'\\') || raw.get(at + 5) != Some(&b'u') {
        return Err(bad);
    }
    let second = hex4(raw, at + 6).ok_or(bad)?;
    if !(0xdc00..0xe000).contains(&second) {
        return Err(bad);
    }
    let combined = 0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00);
    char::from_u32(combined).map(|c| (c, 10)).ok_or(bad)
}

fn hex4(raw: &[u8], at: usize) -> Option<u16> {
    let digits = raw.get(at..at + 4)?;
    let mut value: u16 = 0;
    for &digit in digits {
        let nibble = match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            b'A'..=b'F' => digit - b'A' + 10,
            _ => return None,
        };
        value = (value << 4) | u16::from(nibble);
    }
    Some(value)
}

/// Writes the one object this crate produces: `CONNECT`.
pub(crate) struct ObjectWriter<'o> {
    out: &'o mut Vec<u8>,
    first: bool,
}

impl<'o> ObjectWriter<'o> {
    pub(crate) fn new(out: &'o mut Vec<u8>) -> Self {
        out.push(b'{');
        Self { out, first: true }
    }

    fn key(&mut self, key: &str) {
        if !self.first {
            self.out.push(b',');
        }
        self.first = false;
        self.out.push(b'"');
        self.out.extend_from_slice(key.as_bytes());
        self.out.extend_from_slice(b"\":");
    }

    pub(crate) fn bool(&mut self, key: &str, value: bool) {
        self.key(key);
        self.out
            .extend_from_slice(if value { b"true" } else { b"false" });
    }

    pub(crate) fn u64(&mut self, key: &str, value: u64) {
        self.key(key);
        push_decimal(self.out, value);
    }

    pub(crate) fn string(&mut self, key: &str, value: &str) {
        self.key(key);
        escape_into(self.out, value);
    }

    pub(crate) fn finish(self) {
        self.out.push(b'}');
    }
}

/// Write `value` as a JSON string literal, quotes included.
///
/// Escaped: the quote and the backslash, which would end the literal, and
/// every C0 control — a raw `CR` or `LF` inside the object would end the NATS
/// *control line* early and make the rest of the object a second operation.
/// Everything above `0x1f` passes through as UTF-8, which is what JSON asks
/// for.
pub(crate) fn escape_into(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    for byte in value.bytes() {
        match byte {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x00..=0x1f => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.extend_from_slice(b"\\u00");
                out.push(HEX[usize::from(byte >> 4)]);
                out.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => out.push(byte),
        }
    }
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_string(input: &[u8]) -> Result<Cow<'_, str>, DecodeError> {
        let mut scanner = Scanner::object(input)?;
        let key = scanner.next_key()?.expect("a key");
        assert_eq!(key, b"k");
        let value = scanner.string_value("k")?;
        assert!(scanner.next_key()?.is_none());
        Ok(value)
    }

    #[test]
    fn a_string_without_escapes_is_borrowed() {
        let value = one_string(br#"{"k":"plain"}"#).expect("decodes");
        assert!(matches!(value, Cow::Borrowed("plain")));
    }

    #[test]
    fn go_escapes_the_html_characters_as_code_points() {
        // `nats-server` marshals INFO with Go's encoding/json, which escapes
        // `<`, `>` and `&` by default. A reader without `\u` support would
        // mangle any server name holding one.
        let value = one_string(br#"{"k":"a\u003cb\u0026c\u003ed"}"#).expect("decodes");
        assert_eq!(value, "a<b&c>d");
    }

    #[test]
    fn a_surrogate_pair_is_one_character() {
        let value = one_string(br#"{"k":"\ud83d\ude00"}"#).expect("decodes");
        assert_eq!(value, "\u{1f600}");
    }

    #[test]
    fn a_lone_high_surrogate_is_refused() {
        assert!(matches!(
            one_string(br#"{"k":"\ud83d!"}"#),
            Err(DecodeError::JsonBadEscape { .. })
        ));
    }

    #[test]
    fn an_unknown_field_may_nest_but_not_without_bound() {
        let mut nested = Vec::from(*br#"{"unknown":"#);
        for _ in 0..MAX_JSON_DEPTH + 2 {
            nested.extend_from_slice(b"[");
        }
        let mut scanner = Scanner::object(&nested).expect("an object");
        assert_eq!(scanner.next_key().expect("a key"), Some(&b"unknown"[..]));
        assert_eq!(
            scanner.skip_value(),
            Err(DecodeError::JsonTooDeep {
                cap: MAX_JSON_DEPTH
            })
        );
    }

    #[test]
    fn an_integer_field_refuses_a_fraction() {
        let input = br#"{"port":42.5}"#;
        let mut scanner = Scanner::object(input).expect("an object");
        scanner.next_key().expect("a key");
        assert_eq!(
            scanner.u64_value("port"),
            Err(DecodeError::JsonNotAnInteger { field: "port" })
        );
    }

    #[test]
    fn escaping_round_trips_through_the_reader() {
        let hostile = "quote\" slash\\ newline\n tab\t bell\u{7} unicode\u{1f600}";
        let mut written = Vec::from(*b"{\"k\":");
        escape_into(&mut written, hostile);
        written.push(b'}');
        assert!(
            !written.contains(&b'\r') && !written.contains(&b'\n'),
            "a control line may not hold a raw newline: {written:?}"
        );
        assert_eq!(one_string(&written).expect("decodes"), hostile);
    }
}
