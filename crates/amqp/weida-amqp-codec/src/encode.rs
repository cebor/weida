//! [`Value`] to bytes, in one canonical form.
//!
//! Part 1 gives most types several encodings and names none of them
//! preferred, so an encoder has to choose. This one chooses **the shortest
//! that carries the value**: `uint` 0 is `0x43` and not five octets, a `list`
//! of three small elements is a `list8`, and a composite's trailing null
//! fields are omitted. Two consequences are worth stating plainly:
//!
//! * `decode(encode(v)) == v` for every value this crate can hold, which the
//!   round-trip tests and the fuzz targets assert;
//! * `encode(decode(bytes)) == bytes` is **false** whenever the peer used a
//!   wider form than it needed, which is normal and is why the golden vectors
//!   record the two directions separately.
//!
//! The one exception to "shortest" is an `array`, whose element constructor
//! is part of its value rather than an encoding choice: an array declared
//! `ulong` keeps writing its elements as eight octets even where one would
//! do, because narrowing them would change the type the array declared and
//! break the round trip.
//!
//! # Allocation
//!
//! Every function appends to a caller-supplied `Vec<u8>` and none of them
//! stages bytes anywhere else. A compound's size is not known until its
//! elements are written, so the header goes down in its widest form and is
//! narrowed afterwards by moving the body back six octets — one `copy_within`
//! of a small body, against one scratch buffer per compound if the body were
//! built elsewhere.

use crate::codes;
use crate::error::EncodeError;
use crate::value::{Array, Descriptor, ElementKind, Value};

/// Appends the canonical encoding of `value` to `out`.
///
/// ```
/// use weida_amqp_codec::{Value, encode};
///
/// let mut out = Vec::new();
/// encode::value(&Value::Ulong(42), &mut out).expect("a ulong encodes");
/// assert_eq!(out, [0x53, 0x2a], "the shortest form that carries 42");
/// ```
pub fn value(value: &Value<'_>, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let code = match value {
        // The compound codes depend on the encoded size, which is not known
        // before the elements are written, so they are decided below rather
        // than by `canonical_code`.
        Value::List(items) if !items.is_empty() => return list(items, out),
        Value::Map(entries) => return map(entries, out),
        Value::Array(array) => return self::array(array, out),
        Value::Described(d) => return described(&d.descriptor, &d.value, out),
        other => other.canonical_code(),
    };
    out.push(code);
    untyped(value, code, out)
}

/// `uint`, in the shortest of its three forms.
pub fn uint(v: u32, out: &mut Vec<u8>) {
    if v == 0 {
        out.push(codes::UINT0);
    } else if let Ok(small) = u8::try_from(v) {
        out.push(codes::SMALLUINT);
        out.push(small);
    } else {
        out.push(codes::UINT);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// `ulong`, in the shortest of its three forms.
pub fn ulong(v: u64, out: &mut Vec<u8>) {
    if v == 0 {
        out.push(codes::ULONG0);
    } else if let Ok(small) = u8::try_from(v) {
        out.push(codes::SMALLULONG);
        out.push(small);
    } else {
        out.push(codes::ULONG);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// `int`, in the shorter of its two forms. The one-octet form is *signed*, so
/// it carries -128 to 127 and not 0 to 255.
pub fn int(v: i32, out: &mut Vec<u8>) {
    if let Ok(small) = i8::try_from(v) {
        out.push(codes::SMALLINT);
        out.push(small as u8);
    } else {
        out.push(codes::INT);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// `long`, in the shorter of its two forms.
pub fn long(v: i64, out: &mut Vec<u8>) {
    if let Ok(small) = i8::try_from(v) {
        out.push(codes::SMALLLONG);
        out.push(small as u8);
    } else {
        out.push(codes::LONG);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// `boolean`, in the zero-octet form every implementation writes.
pub fn boolean(v: bool, out: &mut Vec<u8>) {
    out.push(if v { codes::TRUE } else { codes::FALSE });
}

/// `binary`, in the shorter of its two forms.
pub fn binary(data: &[u8], out: &mut Vec<u8>) -> Result<(), EncodeError> {
    value(&Value::Binary(data), out)
}

/// `string`, UTF-8, in the shorter of its two forms.
pub fn string(text: &str, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    value(&Value::String(text), out)
}

/// `symbol`, in the shorter of its two forms, refused if it is not ASCII —
/// the restriction Part 1 puts on the type, enforced on the way out as well
/// as on the way in.
pub fn symbol(text: &str, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    value(&Value::Symbol(text), out)
}

fn wide_variable(data: &[u8], code: u8, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let len = u32::try_from(data.len()).map_err(|_| EncodeError::TooLarge {
        code,
        len: data.len(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(data);
    Ok(())
}

/// `list`.
pub fn list(items: &[Value<'_>], out: &mut Vec<u8>) -> Result<(), EncodeError> {
    if items.is_empty() {
        out.push(codes::LIST0);
        return Ok(());
    }
    let at = open_compound(out);
    for item in items {
        value(item, out)?;
    }
    close_compound(out, at, items.len(), codes::LIST8, codes::LIST32)
}

/// `map`, from key/value pairs. The wire count is twice the number of pairs.
pub fn map(entries: &[(Value<'_>, Value<'_>)], out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let at = open_compound(out);
    for (key, val) in entries {
        value(key, out)?;
        value(val, out)?;
    }
    close_compound(out, at, entries.len() * 2, codes::MAP8, codes::MAP32)
}

/// `array`: the shared constructor once, then each element's untyped data.
///
/// Every element is written **under the array's own constructor**, not under
/// its own canonical one, because the constructor is what the array declared
/// and the elements have no room to disagree. An element the declared
/// constructor cannot carry — a `Value::Ulong(300)` in an array declared
/// `smallulong`, or a `Value::String` in an array declared `sym8` — is
/// [`EncodeError::ArrayElementMismatch`].
pub fn array(a: &Array<'_>, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let at = open_compound(out);
    array_body(a, out)?;
    close_compound(out, at, a.items().len(), codes::ARRAY8, codes::ARRAY32)
}

/// The element constructor and the elements of an array, without the size
/// and count fields.
fn array_body(a: &Array<'_>, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    match a.element() {
        ElementKind::Primitive(code) => out.push(*code),
        ElementKind::Described(descriptor, code) => {
            out.push(codes::DESCRIBED);
            descriptor_bytes(descriptor, out)?;
            out.push(*code);
        }
    }
    let code = a.element().code();
    for item in a.items() {
        let item = match (a.element(), item) {
            (ElementKind::Described(_, _), Value::Described(described)) => &described.value,
            (ElementKind::Described(_, _), other) => {
                return Err(EncodeError::ArrayElementMismatch {
                    expected: codes::DESCRIBED,
                    found: other.canonical_code(),
                });
            }
            (ElementKind::Primitive(_), item) => item,
        };
        untyped(item, code, out)?;
    }
    Ok(())
}

/// A described type: `0x00`, the descriptor, the value.
pub fn described(
    descriptor: &Descriptor<'_>,
    body: &Value<'_>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    out.push(codes::DESCRIBED);
    descriptor_bytes(descriptor, out)?;
    value(body, out)
}

/// A composite type: `0x00`, the descriptor, and the field list with its
/// trailing nulls omitted.
///
/// Trailing-null trimming is what makes an `open` carrying only a
/// `container-id` a ten-octet body instead of a twenty-octet one, and Part 1
/// licenses it directly: "a trailing null element ... can optionally be
/// omitted according to the encoding rules". It is also why
/// [`crate::decode::ListCursor`] answers `Value::Null` past the declared
/// count — the two halves of the same rule.
///
/// ```
/// use weida_amqp_codec::{Descriptor, Value, encode};
///
/// let mut out = Vec::new();
/// // `open` carrying a container-id and nothing else: the nine trailing
/// // nulls go.
/// encode::composite(
///     &Descriptor::Code(0x10),
///     &[Value::String("c1"), Value::Null, Value::Null],
///     &mut out,
/// )
/// .expect("an open encodes");
/// assert_eq!(out, [0x00, 0x53, 0x10, 0xc0, 0x05, 0x01, 0xa1, 0x02, b'c', b'1']);
/// ```
pub fn composite(
    descriptor: &Descriptor<'_>,
    fields: &[Value<'_>],
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    out.push(codes::DESCRIBED);
    descriptor_bytes(descriptor, out)?;
    let count = fields
        .iter()
        .rposition(|f| !f.is_null())
        .map_or(0, |i| i + 1);
    list(&fields[..count], out)
}

fn descriptor_bytes(descriptor: &Descriptor<'_>, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    match descriptor {
        Descriptor::Code(code) => {
            ulong(*code, out);
            Ok(())
        }
        Descriptor::Symbol(text) => symbol(text, out),
    }
}

/// Appends a value's untyped data under the constructor `code`, without the
/// constructor octet.
///
/// This is what an array element is, and it is also how [`value`] writes
/// every non-compound value: push the code, then the data. A value the code
/// cannot carry is [`EncodeError::ArrayElementMismatch`], which names both
/// sides.
fn untyped(item: &Value<'_>, code: u8, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let mismatch = || EncodeError::ArrayElementMismatch {
        expected: code,
        found: item.canonical_code(),
    };
    match (code, item) {
        // Zero-width codes: the constructor is the whole value.
        (codes::NULL, Value::Null)
        | (codes::TRUE, Value::Boolean(true))
        | (codes::FALSE, Value::Boolean(false))
        | (codes::UINT0, Value::Uint(0))
        | (codes::ULONG0, Value::Ulong(0)) => {}
        (codes::LIST0, Value::List(items)) if items.is_empty() => {}

        (codes::BOOLEAN, Value::Boolean(v)) => out.push(u8::from(*v)),
        (codes::UBYTE, Value::Ubyte(v)) => out.push(*v),
        (codes::BYTE, Value::Byte(v)) => out.push(*v as u8),
        (codes::SMALLUINT, Value::Uint(v)) => {
            out.push(u8::try_from(*v).map_err(|_| mismatch())?);
        }
        (codes::SMALLULONG, Value::Ulong(v)) => {
            out.push(u8::try_from(*v).map_err(|_| mismatch())?);
        }
        (codes::SMALLINT, Value::Int(v)) => {
            out.push(i8::try_from(*v).map_err(|_| mismatch())? as u8);
        }
        (codes::SMALLLONG, Value::Long(v)) => {
            out.push(i8::try_from(*v).map_err(|_| mismatch())? as u8);
        }
        (codes::USHORT, Value::Ushort(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::SHORT, Value::Short(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::UINT, Value::Uint(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::INT, Value::Int(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::FLOAT, Value::Float(v)) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
        (codes::CHAR, Value::Char(v)) => out.extend_from_slice(&u32::from(*v).to_be_bytes()),
        (codes::DECIMAL32, Value::Decimal32(octets)) => out.extend_from_slice(octets),
        (codes::ULONG, Value::Ulong(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::LONG, Value::Long(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::DOUBLE, Value::Double(v)) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
        (codes::TIMESTAMP, Value::Timestamp(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (codes::DECIMAL64, Value::Decimal64(octets)) => out.extend_from_slice(octets),
        (codes::DECIMAL128, Value::Decimal128(octets)) => out.extend_from_slice(octets),
        (codes::UUID, Value::Uuid(octets)) => out.extend_from_slice(octets),

        (codes::VBIN8, Value::Binary(data)) => narrow_variable(data, code, out)?,
        (codes::VBIN32, Value::Binary(data)) => wide_variable(data, code, out)?,
        (codes::STR8, Value::String(text)) => narrow_variable(text.as_bytes(), code, out)?,
        (codes::STR32, Value::String(text)) => wide_variable(text.as_bytes(), code, out)?,
        (codes::SYM8, Value::Symbol(text)) => {
            if !text.is_ascii() {
                return Err(EncodeError::NonAsciiSymbol);
            }
            narrow_variable(text.as_bytes(), code, out)?;
        }
        (codes::SYM32, Value::Symbol(text)) => {
            if !text.is_ascii() {
                return Err(EncodeError::NonAsciiSymbol);
            }
            wide_variable(text.as_bytes(), code, out)?;
        }

        (codes::LIST8 | codes::LIST32, Value::List(items)) => {
            let width = header_width(code);
            let at = open_forced(out, width);
            for item in items {
                value(item, out)?;
            }
            close_forced(out, at, items.len(), width, code)?;
        }
        (codes::MAP8 | codes::MAP32, Value::Map(entries)) => {
            let width = header_width(code);
            let at = open_forced(out, width);
            for (key, val) in entries {
                value(key, out)?;
                value(val, out)?;
            }
            close_forced(out, at, entries.len() * 2, width, code)?;
        }
        (codes::ARRAY8 | codes::ARRAY32, Value::Array(inner)) => {
            let width = header_width(code);
            let at = open_forced(out, width);
            array_body(inner, out)?;
            close_forced(out, at, inner.items().len(), width, code)?;
        }

        _ => return Err(mismatch()),
    }
    Ok(())
}

fn narrow_variable(data: &[u8], code: u8, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let len = u8::try_from(data.len()).map_err(|_| EncodeError::TooLarge {
        code,
        len: data.len(),
    })?;
    out.push(len);
    out.extend_from_slice(data);
    Ok(())
}

const fn header_width(code: u8) -> usize {
    match code {
        codes::LIST8 | codes::MAP8 | codes::ARRAY8 => 1,
        _ => 4,
    }
}

/// Writes a nine-octet placeholder for a compound header: one constructor,
/// four size octets, four count octets.
fn open_compound(out: &mut Vec<u8>) -> usize {
    let at = out.len();
    out.extend_from_slice(&[0u8; 9]);
    at
}

/// Fills in a compound header, narrowing it to the one-octet form where the
/// body allows.
fn close_compound(
    out: &mut Vec<u8>,
    at: usize,
    count: usize,
    narrow: u8,
    wide: u8,
) -> Result<(), EncodeError> {
    let body = out.len() - at - 9;
    let count = u32::try_from(count).map_err(|_| EncodeError::TooLarge {
        code: wide,
        len: count,
    })?;
    // The narrow size field counts the count octet too, so the body has one
    // octet less than a u8 to play with.
    if body < usize::from(u8::MAX) && count <= u32::from(u8::MAX) {
        // Narrow: move the body back over six of the nine header octets.
        out.copy_within(at + 9.., at + 3);
        out.truncate(out.len() - 6);
        out[at] = narrow;
        out[at + 1] = (body + 1) as u8;
        out[at + 2] = count as u8;
        return Ok(());
    }
    let size = u32::try_from(body + 4).map_err(|_| EncodeError::TooLarge {
        code: wide,
        len: body,
    })?;
    out[at] = wide;
    out[at + 1..at + 5].copy_from_slice(&size.to_be_bytes());
    out[at + 5..at + 9].copy_from_slice(&count.to_be_bytes());
    Ok(())
}

/// A size-and-count placeholder of a fixed width, for a compound whose
/// constructor was chosen by an enclosing array rather than by this encoder.
fn open_forced(out: &mut Vec<u8>, width: usize) -> usize {
    let at = out.len();
    out.extend_from_slice(&[0u8; 8][..width * 2]);
    at
}

fn close_forced(
    out: &mut [u8],
    at: usize,
    count: usize,
    width: usize,
    code: u8,
) -> Result<(), EncodeError> {
    let body = out.len() - at - width * 2;
    let size = body + width;
    if width == 1 {
        let size = u8::try_from(size).map_err(|_| EncodeError::TooLarge { code, len: size })?;
        let count = u8::try_from(count).map_err(|_| EncodeError::TooLarge { code, len: count })?;
        out[at] = size;
        out[at + 1] = count;
        return Ok(());
    }
    let size = u32::try_from(size).map_err(|_| EncodeError::TooLarge { code, len: size })?;
    let count = u32::try_from(count).map_err(|_| EncodeError::TooLarge { code, len: count })?;
    out[at..at + 4].copy_from_slice(&size.to_be_bytes());
    out[at + 4..at + 8].copy_from_slice(&count.to_be_bytes());
    Ok(())
}

/// Encodes `value` into a fresh `Vec`, for tests and for callers with nothing
/// to append to.
pub fn to_vec(v: &Value<'_>) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();
    value(v, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;
    use crate::limits::Limits;
    use crate::value::Described;

    fn round_trip(v: Value<'_>) {
        let bytes = to_vec(&v).expect("encodes");
        let (back, used) = decode::value(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len(), "the encoding is consumed exactly");
        assert_eq!(back, v);
    }

    #[test]
    fn integers_take_their_shortest_form() {
        assert_eq!(to_vec(&Value::Uint(0)).unwrap(), [codes::UINT0]);
        assert_eq!(to_vec(&Value::Uint(1)).unwrap(), [codes::SMALLUINT, 1]);
        assert_eq!(
            to_vec(&Value::Uint(256)).unwrap(),
            [codes::UINT, 0, 0, 1, 0]
        );
        assert_eq!(to_vec(&Value::Ulong(0)).unwrap(), [codes::ULONG0]);
        assert_eq!(to_vec(&Value::Int(-1)).unwrap(), [codes::SMALLINT, 0xff]);
        assert_eq!(
            to_vec(&Value::Int(-129)).unwrap(),
            [codes::INT, 0xff, 0xff, 0xff, 0x7f]
        );
    }

    #[test]
    fn every_value_round_trips() {
        for v in [
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Ubyte(255),
            Value::Byte(-128),
            Value::Ushort(65535),
            Value::Short(-32768),
            Value::Uint(0),
            Value::Uint(4_000_000_000),
            Value::Int(i32::MIN),
            Value::Ulong(u64::MAX),
            Value::Long(i64::MIN),
            Value::Float(1.5),
            Value::Double(-0.25),
            Value::Decimal32([1, 2, 3, 4]),
            Value::Decimal64([1, 2, 3, 4, 5, 6, 7, 8]),
            Value::Decimal128([9; 16]),
            Value::Char('\u{1F600}'),
            Value::Timestamp(-1),
            Value::Uuid([7; 16]),
            Value::Binary(b"bytes"),
            Value::String("string"),
            Value::Symbol("amqp:open:list"),
            Value::List(vec![Value::Null, Value::Uint(1)]),
            Value::List(Vec::new()),
            Value::Map(vec![(Value::Symbol("k"), Value::String("v"))]),
            Value::Map(Vec::new()),
        ] {
            round_trip(v);
        }
    }

    #[test]
    fn a_wide_body_uses_the_wide_header() {
        // 300 one-octet elements: the count fits a u8 but the body does not,
        // so the wide form is the only one that can carry it.
        let items: Vec<Value<'_>> = (0..300).map(|_| Value::Null).collect();
        let bytes = to_vec(&Value::List(items.clone())).expect("encodes");
        assert_eq!(bytes[0], codes::LIST32);
        assert_eq!(&bytes[1..5], &304u32.to_be_bytes());
        assert_eq!(&bytes[5..9], &300u32.to_be_bytes());
        let (back, used) = decode::value(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len());
        assert_eq!(back, Value::List(items));
    }

    #[test]
    fn a_narrow_body_uses_the_narrow_header() {
        let bytes = to_vec(&Value::List(vec![Value::Null])).expect("encodes");
        assert_eq!(bytes, [codes::LIST8, 2, 1, codes::NULL]);
    }

    #[test]
    fn a_composite_omits_its_trailing_nulls() {
        let mut out = Vec::new();
        composite(
            &Descriptor::Code(0x17),
            &[Value::Null, Value::Null],
            &mut out,
        )
        .expect("encodes");
        assert_eq!(
            out,
            [codes::DESCRIBED, codes::SMALLULONG, 0x17, codes::LIST0],
            "every field null means list0"
        );

        let mut out = Vec::new();
        composite(
            &Descriptor::Code(0x11),
            &[Value::Null, Value::Uint(1), Value::Null],
            &mut out,
        )
        .expect("encodes");
        assert_eq!(
            out,
            [
                codes::DESCRIBED,
                codes::SMALLULONG,
                0x11,
                codes::LIST8,
                4,
                2,
                codes::NULL,
                codes::SMALLUINT,
                1
            ],
            "an interior null stays, a trailing one goes"
        );
    }

    #[test]
    fn an_array_writes_its_constructor_once() {
        let a = Array::new(
            ElementKind::Primitive(codes::SMALLULONG),
            vec![Value::Ulong(1), Value::Ulong(2)],
        );
        let bytes = to_vec(&Value::Array(a.clone())).expect("encodes");
        assert_eq!(bytes, [codes::ARRAY8, 4, 2, codes::SMALLULONG, 1, 2]);
        let (back, used) = decode::value(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len());
        assert_eq!(back, Value::Array(a));
    }

    #[test]
    fn an_array_keeps_the_width_it_declared() {
        // A peer that wrote its ulongs wide gets them back wide: the
        // constructor is the array's declared type, and narrowing the
        // elements would change it.
        let wide = [
            codes::ARRAY8,
            18,
            2,
            codes::ULONG,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            2,
        ];
        let (value, used) = decode::value(&wide, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, wide.len());
        assert_eq!(to_vec(&value).expect("re-encodes"), wide);
    }

    #[test]
    fn an_array_of_described_values_round_trips() {
        let a = Array::new(
            ElementKind::Described(Descriptor::Code(0x24), codes::LIST0),
            vec![
                Value::Described(Box::new(Described {
                    descriptor: Descriptor::Code(0x24),
                    value: Value::List(Vec::new()),
                })),
                Value::Described(Box::new(Described {
                    descriptor: Descriptor::Code(0x24),
                    value: Value::List(Vec::new()),
                })),
            ],
        );
        let bytes = to_vec(&Value::Array(a.clone())).expect("encodes");
        assert_eq!(
            bytes,
            [
                codes::ARRAY8,
                5,
                2,
                codes::DESCRIBED,
                codes::SMALLULONG,
                0x24,
                codes::LIST0
            ]
        );
        let (back, _) = decode::value(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(back, Value::Array(a));
    }

    #[test]
    fn an_element_the_constructor_cannot_carry_is_refused() {
        let a = Array::new(
            ElementKind::Primitive(codes::SMALLULONG),
            vec![Value::Ulong(1), Value::Ulong(300)],
        );
        assert_eq!(
            to_vec(&Value::Array(a)),
            Err(EncodeError::ArrayElementMismatch {
                expected: codes::SMALLULONG,
                found: codes::ULONG,
            })
        );

        // A different type entirely: a `string` in an array of `symbol`.
        let a = Array::new(
            ElementKind::Primitive(codes::SYM8),
            vec![Value::String("not a symbol")],
        );
        assert_eq!(
            to_vec(&Value::Array(a)),
            Err(EncodeError::ArrayElementMismatch {
                expected: codes::SYM8,
                found: codes::STR8,
            })
        );
    }

    #[test]
    fn an_array_of_compound_elements_round_trips() {
        // No array the specification defines has compound elements, but the
        // grammar permits them and a peer may send them, so the encoder must
        // be able to write back what the decoder accepted.
        let bytes = [
            codes::ARRAY8,
            8,
            2,
            codes::LIST8,
            2,
            1,
            codes::NULL,
            2,
            1,
            codes::TRUE,
        ];
        let (value, used) = decode::value(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len());
        assert_eq!(to_vec(&value).expect("re-encodes"), bytes);
    }

    #[test]
    fn a_non_ascii_symbol_is_refused_on_the_way_out() {
        assert_eq!(
            to_vec(&Value::Symbol("caf\u{e9}")),
            Err(EncodeError::NonAsciiSymbol)
        );
    }

    #[test]
    fn nesting_round_trips_through_both_header_widths() {
        let inner = Value::Map(vec![(
            Value::Symbol("x-opt-trace"),
            Value::Binary(&[0xab; 200]),
        )]);
        let v = Value::List(vec![inner, Value::Uint(7)]);
        round_trip(v);
    }
}
