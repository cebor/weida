//! Part 1's format codes, and the width each one implies.
//!
//! A constructor is one octet, or — for a described type — `0x00`, a
//! descriptor and then the constructor of the described value. The octet's
//! high nibble is its *category* and fixes how many octets of size information
//! follow; the low nibble picks the type inside that category
//! (Part 1 §1.6).
//!
//! ```text
//! 0x4n  fixed, 0 octets        0xAn  variable, 1-octet size
//! 0x5n  fixed, 1 octet         0xBn  variable, 4-octet size
//! 0x6n  fixed, 2 octets        0xCn  compound, 1-octet size and count
//! 0x7n  fixed, 4 octets        0xDn  compound, 4-octet size and count
//! 0x8n  fixed, 8 octets        0xEn  array, 1-octet size and count
//! 0x9n  fixed, 16 octets       0xFn  array, 4-octet size and count
//! ```
//!
//! The categories are not merely documentation: a decoder that meets an
//! unknown low nibble in a known category still knows how many octets to
//! skip. This crate does not skip them — an unknown format code is
//! [`DecodeError::UnknownFormatCode`](crate::DecodeError::UnknownFormatCode),
//! because a performative field this version cannot name is not a field it
//! may guess at — but the widths are what make the error the *only* outcome
//! rather than a desynchronized stream.

/// `0x00`: a descriptor and a described value follow (Part 1 §1.2).
pub const DESCRIBED: u8 = 0x00;

/// `null`, zero octets.
pub const NULL: u8 = 0x40;
/// `boolean` `true`, zero octets.
pub const TRUE: u8 = 0x41;
/// `boolean` `false`, zero octets.
pub const FALSE: u8 = 0x42;
/// `uint` zero, zero octets.
pub const UINT0: u8 = 0x43;
/// `ulong` zero, zero octets.
pub const ULONG0: u8 = 0x44;
/// `list` of zero elements, zero octets.
pub const LIST0: u8 = 0x45;

/// `boolean` as one octet, `0x00` or `0x01` and nothing else.
pub const BOOLEAN: u8 = 0x56;
/// `ubyte`.
pub const UBYTE: u8 = 0x50;
/// `byte`.
pub const BYTE: u8 = 0x51;
/// `uint` in one octet, 0 to 255.
pub const SMALLUINT: u8 = 0x52;
/// `ulong` in one octet, 0 to 255.
pub const SMALLULONG: u8 = 0x53;
/// `int` in one octet, signed.
pub const SMALLINT: u8 = 0x54;
/// `long` in one octet, signed.
pub const SMALLLONG: u8 = 0x55;

/// `ushort`.
pub const USHORT: u8 = 0x60;
/// `short`.
pub const SHORT: u8 = 0x61;

/// `uint`, four octets.
pub const UINT: u8 = 0x70;
/// `int`, four octets.
pub const INT: u8 = 0x71;
/// `float`, IEEE 754 binary32.
pub const FLOAT: u8 = 0x72;
/// `char`, a UTF-32BE code point.
pub const CHAR: u8 = 0x73;
/// `decimal32`, IEEE 754 decimal32, carried as four opaque octets.
pub const DECIMAL32: u8 = 0x74;

/// `ulong`, eight octets.
pub const ULONG: u8 = 0x80;
/// `long`, eight octets.
pub const LONG: u8 = 0x81;
/// `double`, IEEE 754 binary64.
pub const DOUBLE: u8 = 0x82;
/// `timestamp`, milliseconds since the epoch as a signed 64-bit integer.
pub const TIMESTAMP: u8 = 0x83;
/// `decimal64`, carried as eight opaque octets.
pub const DECIMAL64: u8 = 0x84;

/// `decimal128`, carried as sixteen opaque octets.
pub const DECIMAL128: u8 = 0x94;
/// `uuid`, sixteen octets.
pub const UUID: u8 = 0x98;

/// `binary` with a one-octet size.
pub const VBIN8: u8 = 0xa0;
/// `string`, UTF-8, with a one-octet size.
pub const STR8: u8 = 0xa1;
/// `symbol`, ASCII, with a one-octet size.
pub const SYM8: u8 = 0xa3;

/// `binary` with a four-octet size.
pub const VBIN32: u8 = 0xb0;
/// `string`, UTF-8, with a four-octet size.
pub const STR32: u8 = 0xb1;
/// `symbol`, ASCII, with a four-octet size.
pub const SYM32: u8 = 0xb3;

/// `list` with one-octet size and count.
pub const LIST8: u8 = 0xc0;
/// `map` with one-octet size and count.
pub const MAP8: u8 = 0xc1;

/// `list` with four-octet size and count.
pub const LIST32: u8 = 0xd0;
/// `map` with four-octet size and count.
pub const MAP32: u8 = 0xd1;

/// `array` with one-octet size and count.
pub const ARRAY8: u8 = 0xe0;
/// `array` with four-octet size and count.
pub const ARRAY32: u8 = 0xf0;

/// How many octets of untyped data a fixed-width format code carries, or
/// `None` if the code is not a fixed-width constructor.
///
/// The answer comes from the category alone, which is why it is total over
/// `0x40..=0x9f` rather than a table of the codes this crate names.
pub const fn fixed_width(code: u8) -> Option<usize> {
    match code >> 4 {
        0x4 => Some(0),
        0x5 => Some(1),
        0x6 => Some(2),
        0x7 => Some(4),
        0x8 => Some(8),
        0x9 => Some(16),
        _ => None,
    }
}

/// The AMQP name of a format code, for error messages and for the golden
/// vectors, or `None` where this crate does not implement the code.
pub const fn name(code: u8) -> Option<&'static str> {
    Some(match code {
        DESCRIBED => "described",
        NULL => "null",
        TRUE => "true",
        FALSE => "false",
        UINT0 => "uint0",
        ULONG0 => "ulong0",
        LIST0 => "list0",
        BOOLEAN => "boolean",
        UBYTE => "ubyte",
        BYTE => "byte",
        SMALLUINT => "smalluint",
        SMALLULONG => "smallulong",
        SMALLINT => "smallint",
        SMALLLONG => "smalllong",
        USHORT => "ushort",
        SHORT => "short",
        UINT => "uint",
        INT => "int",
        FLOAT => "float",
        CHAR => "char",
        DECIMAL32 => "decimal32",
        ULONG => "ulong",
        LONG => "long",
        DOUBLE => "double",
        TIMESTAMP => "timestamp",
        DECIMAL64 => "decimal64",
        DECIMAL128 => "decimal128",
        UUID => "uuid",
        VBIN8 => "vbin8",
        STR8 => "str8-utf8",
        SYM8 => "sym8",
        VBIN32 => "vbin32",
        STR32 => "str32-utf8",
        SYM32 => "sym32",
        LIST8 => "list8",
        MAP8 => "map8",
        LIST32 => "list32",
        MAP32 => "map32",
        ARRAY8 => "array8",
        ARRAY32 => "array32",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_category_fixes_its_width() {
        // The table of Part 1 §1.6, read as the specification writes it: the
        // high nibble alone decides, so one probe per category proves the
        // whole category.
        assert_eq!(fixed_width(0x4f), Some(0));
        assert_eq!(fixed_width(0x5f), Some(1));
        assert_eq!(fixed_width(0x6f), Some(2));
        assert_eq!(fixed_width(0x7f), Some(4));
        assert_eq!(fixed_width(0x8f), Some(8));
        assert_eq!(fixed_width(0x9f), Some(16));
        assert_eq!(fixed_width(0xa0), None);
        assert_eq!(fixed_width(0xf0), None);
        assert_eq!(
            fixed_width(DESCRIBED),
            None,
            "0x00 is a constructor, not a width"
        );
    }

    #[test]
    fn named_codes_are_the_implemented_ones() {
        assert_eq!(name(SMALLULONG), Some("smallulong"));
        assert_eq!(name(0x57), None, "0x57 is not a Part 1 format code");
        assert_eq!(name(0x99), None, "0x99 is not a Part 1 format code");
    }
}
