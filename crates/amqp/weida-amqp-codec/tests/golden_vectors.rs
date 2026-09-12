//! Golden vectors: the specification's own encoding table and its own worked
//! examples, as octets.
//!
//! Three things are asserted here that no unit test can assert:
//!
//! 1. **Every encoding of Part 1 §1.6 is covered.** The table below has one
//!    row per `<encoding>` element of the specification's type definitions —
//!    thirty-nine of them across twenty-four primitive types — and
//!    `the_table_covers_every_encoding_the_specification_defines` compares the
//!    set of format codes the rows use against
//!    [`weida_amqp_codec::codes::name`], so a code this crate implements and
//!    the table forgets, or the reverse, fails.
//! 2. **The published examples decode to the published values.** Figure 1.19,
//!    the `example:book:list` composite, is transcribed octet for octet from
//!    the OASIS text, including the array of two `str8-utf8` authors and the
//!    trailing `null` ISBN whose omission the figure's own caption calls
//!    optional.
//! 3. **Figure 1.2 is refused.** The specification's described-format-code
//!    figure uses a `string` descriptor and then says so: "this example shows
//!    a string-typed descriptor, which is considered reserved", and §1.5
//!    generalizes it — "Descriptor values other than symbolic (symbol) or
//!    numeric (ulong) are, while not syntactically invalid, reserved". A
//!    published figure this crate must *not* accept is worth a vector of its
//!    own.
//!
//! Source: *AMQP Version 1.0, Part 1: Types*, OASIS Standard, 29 October
//! 2012, §1.2, §1.5, §1.6 and Figure 1.19
//! (`docs/research/amqp10.md` §14 source [2]).

use weida_amqp_codec::{
    DecodeError, Descriptor, ElementKind, Limits, Value, codes, decode, encode,
};

/// One row of Part 1's encoding table.
struct Vector {
    /// The `name` attribute of the specification's `<encoding>` element, or
    /// the type name where the encoding carries no name of its own.
    encoding: &'static str,
    /// The octets, constructor included.
    bytes: &'static [u8],
    /// What they mean.
    value: Value<'static>,
    /// Whether this is the form [`encode`] writes for this value. Exactly one
    /// encoding per *value* is canonical; the wider forms of a small number
    /// are not, which is the asymmetry Part 1 creates by defining several
    /// encodings and preferring none.
    canonical: bool,
}

const UUID: [u8; 16] = [
    0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6,
];

fn table() -> Vec<Vector> {
    vec![
        Vector {
            encoding: "null",
            bytes: &[0x40],
            value: Value::Null,
            canonical: true,
        },
        Vector {
            encoding: "boolean",
            bytes: &[0x56, 0x01],
            value: Value::Boolean(true),
            canonical: false,
        },
        Vector {
            encoding: "true",
            bytes: &[0x41],
            value: Value::Boolean(true),
            canonical: true,
        },
        Vector {
            encoding: "false",
            bytes: &[0x42],
            value: Value::Boolean(false),
            canonical: true,
        },
        Vector {
            encoding: "ubyte",
            bytes: &[0x50, 0x2a],
            value: Value::Ubyte(42),
            canonical: true,
        },
        Vector {
            encoding: "ushort",
            bytes: &[0x60, 0x01, 0x00],
            value: Value::Ushort(256),
            canonical: true,
        },
        Vector {
            encoding: "uint",
            bytes: &[0x70, 0x00, 0x00, 0x01, 0x00],
            value: Value::Uint(256),
            canonical: true,
        },
        Vector {
            encoding: "smalluint",
            bytes: &[0x52, 0x2a],
            value: Value::Uint(42),
            canonical: true,
        },
        Vector {
            encoding: "uint0",
            bytes: &[0x43],
            value: Value::Uint(0),
            canonical: true,
        },
        Vector {
            encoding: "ulong",
            bytes: &[0x80, 0, 0, 0, 0, 0, 0, 0x01, 0x00],
            value: Value::Ulong(256),
            canonical: true,
        },
        Vector {
            encoding: "smallulong",
            bytes: &[0x53, 0x2a],
            value: Value::Ulong(42),
            canonical: true,
        },
        Vector {
            encoding: "ulong0",
            bytes: &[0x44],
            value: Value::Ulong(0),
            canonical: true,
        },
        Vector {
            encoding: "byte",
            bytes: &[0x51, 0xff],
            value: Value::Byte(-1),
            canonical: true,
        },
        Vector {
            encoding: "short",
            bytes: &[0x61, 0x80, 0x00],
            value: Value::Short(i16::MIN),
            canonical: true,
        },
        Vector {
            encoding: "int",
            bytes: &[0x71, 0xff, 0xff, 0xff, 0x7f],
            value: Value::Int(-129),
            canonical: true,
        },
        Vector {
            encoding: "smallint",
            bytes: &[0x54, 0xff],
            value: Value::Int(-1),
            canonical: true,
        },
        Vector {
            encoding: "long",
            bytes: &[0x81, 0, 0, 0, 0, 0, 0, 0x01, 0x00],
            value: Value::Long(256),
            canonical: true,
        },
        Vector {
            encoding: "smalllong",
            bytes: &[0x55, 0x80],
            value: Value::Long(-128),
            canonical: true,
        },
        Vector {
            // IEEE 754 binary32 for 1.0.
            encoding: "float/ieee-754",
            bytes: &[0x72, 0x3f, 0x80, 0x00, 0x00],
            value: Value::Float(1.0),
            canonical: true,
        },
        Vector {
            // IEEE 754 binary64 for -2.0.
            encoding: "double/ieee-754",
            bytes: &[0x82, 0xc0, 0x00, 0, 0, 0, 0, 0, 0],
            value: Value::Double(-2.0),
            canonical: true,
        },
        Vector {
            encoding: "decimal32/ieee-754",
            bytes: &[0x74, 0x22, 0x50, 0x00, 0x00],
            value: Value::Decimal32([0x22, 0x50, 0x00, 0x00]),
            canonical: true,
        },
        Vector {
            encoding: "decimal64/ieee-754",
            bytes: &[0x84, 0x22, 0x34, 0, 0, 0, 0, 0, 0],
            value: Value::Decimal64([0x22, 0x34, 0, 0, 0, 0, 0, 0]),
            canonical: true,
        },
        Vector {
            encoding: "decimal128/ieee-754",
            bytes: &[0x94, 0x22, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            value: Value::Decimal128([0x22, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            canonical: true,
        },
        Vector {
            // U+1F600, four octets of UTF-32BE rather than a surrogate pair.
            encoding: "char/utf32",
            bytes: &[0x73, 0x00, 0x01, 0xf6, 0x00],
            value: Value::Char('\u{1f600}'),
            canonical: true,
        },
        Vector {
            // ms64 is signed: -1 is one millisecond before the epoch.
            encoding: "timestamp/ms64",
            bytes: &[0x83, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            value: Value::Timestamp(-1),
            canonical: true,
        },
        Vector {
            encoding: "uuid",
            bytes: &[
                0x98, 0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9,
                0x1e, 0x6b, 0xf6,
            ],
            value: Value::Uuid(UUID),
            canonical: true,
        },
        Vector {
            encoding: "vbin8",
            bytes: &[0xa0, 0x03, 0xde, 0xad, 0xbe],
            value: Value::Binary(&[0xde, 0xad, 0xbe]),
            canonical: true,
        },
        Vector {
            encoding: "vbin32",
            bytes: &[0xb0, 0x00, 0x00, 0x00, 0x03, 0xde, 0xad, 0xbe],
            value: Value::Binary(&[0xde, 0xad, 0xbe]),
            canonical: false,
        },
        Vector {
            encoding: "str8-utf8",
            bytes: &[0xa1, 0x03, b'A', b'M', b'Q'],
            value: Value::String("AMQ"),
            canonical: true,
        },
        Vector {
            encoding: "str32-utf8",
            bytes: &[0xb1, 0x00, 0x00, 0x00, 0x03, b'A', b'M', b'Q'],
            value: Value::String("AMQ"),
            canonical: false,
        },
        Vector {
            encoding: "sym8",
            bytes: &[0xa3, 0x03, b'a', b'm', b'q'],
            value: Value::Symbol("amq"),
            canonical: true,
        },
        Vector {
            encoding: "sym32",
            bytes: &[0xb3, 0x00, 0x00, 0x00, 0x03, b'a', b'm', b'q'],
            value: Value::Symbol("amq"),
            canonical: false,
        },
        Vector {
            encoding: "list0",
            bytes: &[0x45],
            value: Value::List(Vec::new()),
            canonical: true,
        },
        Vector {
            encoding: "list8",
            bytes: &[0xc0, 0x02, 0x01, 0x40],
            value: Value::List(vec![Value::Null]),
            canonical: true,
        },
        Vector {
            encoding: "list32",
            bytes: &[0xd0, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x40],
            value: Value::List(vec![Value::Null]),
            canonical: false,
        },
        Vector {
            encoding: "map8",
            bytes: &[0xc1, 0x05, 0x02, 0xa3, 0x01, b'k', 0x43],
            value: Value::Map(vec![(Value::Symbol("k"), Value::Uint(0))]),
            canonical: true,
        },
        Vector {
            encoding: "map32",
            bytes: &[
                0xd1, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0xa3, 0x01, b'k', 0x43,
            ],
            value: Value::Map(vec![(Value::Symbol("k"), Value::Uint(0))]),
            canonical: false,
        },
        Vector {
            encoding: "array8",
            bytes: &[0xe0, 0x04, 0x02, 0x53, 0x01, 0x02],
            value: Value::Array(weida_amqp_codec::Array::new(
                ElementKind::Primitive(codes::SMALLULONG),
                vec![Value::Ulong(1), Value::Ulong(2)],
            )),
            canonical: true,
        },
        Vector {
            encoding: "array32",
            bytes: &[
                0xf0, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x02, 0x53, 0x01, 0x02,
            ],
            value: Value::Array(weida_amqp_codec::Array::new(
                ElementKind::Primitive(codes::SMALLULONG),
                vec![Value::Ulong(1), Value::Ulong(2)],
            )),
            canonical: false,
        },
    ]
}

#[test]
fn every_vector_decodes_to_its_value() {
    for vector in table() {
        let (value, used) = decode::value(vector.bytes, Limits::DEFAULT)
            .unwrap_or_else(|e| panic!("{} should decode: {e}", vector.encoding));
        assert_eq!(
            used,
            vector.bytes.len(),
            "{} must consume exactly its octets",
            vector.encoding
        );
        assert_eq!(value, vector.value, "{}", vector.encoding);
    }
}

#[test]
fn the_canonical_vectors_are_what_the_encoder_writes() {
    for vector in table() {
        let written = encode::to_vec(&vector.value)
            .unwrap_or_else(|e| panic!("{} should encode: {e}", vector.encoding));
        if vector.canonical {
            assert_eq!(
                written, vector.bytes,
                "{} is the canonical form of its value",
                vector.encoding
            );
        } else {
            assert_ne!(
                written, vector.bytes,
                "{} is a wider form than the value needs, so the encoder \
                 must not reproduce it",
                vector.encoding
            );
            // It is still the same value, which is the whole point of the
            // decoder accepting it.
            let (again, _) = decode::value(&written, Limits::DEFAULT).expect("decodes");
            assert_eq!(again, vector.value, "{}", vector.encoding);
        }
    }
}

#[test]
fn the_table_covers_every_encoding_the_specification_defines() {
    let vectors = table();
    // Part 1 §1.6 defines thirty-nine encodings over twenty-four primitive
    // types. One row each, and the count is stated so that adding a row
    // without adding an encoding fails too.
    assert_eq!(vectors.len(), 39, "one row per <encoding> of §1.6");

    let mut covered: Vec<u8> = vectors.iter().map(|v| v.bytes[0]).collect();
    covered.sort_unstable();
    covered.dedup();

    let implemented: Vec<u8> = (0u8..=255)
        .filter(|c| codes::name(*c).is_some() && *c != codes::DESCRIBED)
        .collect();

    assert_eq!(
        covered, implemented,
        "the table and the crate's code list must name the same encodings; \
         `described` is covered by the two worked-example tests instead"
    );
}

/// Figure 1.19, the example composite value of the `example:book:list` type,
/// transcribed from the OASIS text.
///
/// ```text
/// 0x00 0xA3 0x11 "example:book:list" 0xC0 0x40 0x03  title  authors  isbn
/// title   = 0xA1 0x15 "AMQP for & by Dummies"
/// authors = 0xE0 0x25 0x02 0xA1 0x0E "Rob J. Godfrey" 0x13 "Rafael H. Schloming"
/// isbn    = 0x40
/// ```
fn figure_1_19() -> Vec<u8> {
    let mut bytes = vec![0x00, 0xa3, 0x11];
    bytes.extend_from_slice(b"example:book:list");
    bytes.extend_from_slice(&[0xc0, 0x40, 0x03]);
    bytes.extend_from_slice(&[0xa1, 0x15]);
    bytes.extend_from_slice(b"AMQP for & by Dummies");
    bytes.extend_from_slice(&[0xe0, 0x25, 0x02, 0xa1, 0x0e]);
    bytes.extend_from_slice(b"Rob J. Godfrey");
    bytes.push(0x13);
    bytes.extend_from_slice(b"Rafael H. Schloming");
    bytes.push(0x40);
    bytes
}

#[test]
fn the_published_composite_example_decodes_field_by_field() {
    let bytes = figure_1_19();
    // The figure's own arithmetic: the described type is 86 octets, of which
    // the list8 header declares 0x40 = 64 for its count and elements.
    assert_eq!(bytes.len(), 86);

    let mut book = decode::composite(&bytes, Limits::DEFAULT).expect("a composite");
    assert_eq!(book.used, bytes.len());
    assert_eq!(book.descriptor, Descriptor::Symbol("example:book:list"));
    assert_eq!(book.fields.remaining(), 3);

    assert_eq!(
        book.fields.next_value().unwrap(),
        Value::String("AMQP for & by Dummies")
    );
    let authors = book.fields.next_value().unwrap();
    let Value::Array(authors) = authors else {
        panic!("authors is an array, because the field is `multiple`");
    };
    assert_eq!(
        authors.element(),
        &ElementKind::Primitive(codes::STR8),
        "one constructor for both elements, written once"
    );
    assert_eq!(
        authors.items(),
        &[
            Value::String("Rob J. Godfrey"),
            Value::String("Rafael H. Schloming")
        ]
    );
    assert_eq!(
        book.fields.next_value().unwrap(),
        Value::Null,
        "the ISBN is the explicit trailing null the figure depicts"
    );
}

#[test]
fn the_published_composite_reencodes_without_its_optional_trailing_null() {
    // The figure's caption: "A trailing null element corresponding to the
    // absence of an ISBN value is depicted in the example, but can
    // optionally be omitted according to the encoding rules." The canonical
    // encoder takes the option, so the re-encoding is the figure minus the
    // trailing 0x40 with the count and size adjusted — and it decodes to the
    // same book.
    let bytes = figure_1_19();
    let mut book = decode::composite(&bytes, Limits::DEFAULT).expect("a composite");
    let title = book.fields.next_value().unwrap();
    let authors = book.fields.next_value().unwrap();
    let isbn = book.fields.next_value().unwrap();

    let mut written = Vec::new();
    encode::composite(
        &Descriptor::Symbol("example:book:list"),
        &[title.clone(), authors.clone(), isbn],
        &mut written,
    )
    .expect("encodes");

    let mut expected = bytes.clone();
    expected.pop(); // the trailing null
    expected[21] = 0x3f; // the list8 size, one octet smaller
    expected[22] = 0x02; // and two fields rather than three
    assert_eq!(written, expected);

    let mut again = decode::composite(&written, Limits::DEFAULT).expect("a composite");
    assert_eq!(again.fields.next_value().unwrap(), title);
    assert_eq!(again.fields.next_value().unwrap(), authors);
    assert_eq!(
        again.fields.next_value().unwrap(),
        Value::Null,
        "the omitted field reads back as the null it stood for"
    );
}

#[test]
fn the_published_url_example_is_refused_because_its_descriptor_is_reserved() {
    // Figure 1.2: `0x00 0xA1 0x03 "URL" 0xA1 0x1E "http://..."`. The figure
    // is annotated "this example shows a string-typed descriptor, which is
    // considered reserved", and §1.5 makes the rule general. A crate that
    // accepted it would have to carry an arbitrary value in the one position
    // a decoder dispatches on.
    let mut bytes = vec![0x00, 0xa1, 0x03];
    bytes.extend_from_slice(b"URL");
    bytes.extend_from_slice(&[0xa1, 0x1e]);
    bytes.extend_from_slice(b"http://example.org/hello-world");
    assert_eq!(bytes.len(), 38);

    assert_eq!(
        decode::value(&bytes, Limits::DEFAULT),
        Err(DecodeError::DescriptorNotSymbolicOrNumeric(0xa1))
    );

    // The same URL under a symbolic descriptor is accepted, which is what
    // §1.5's assignment policy tells a domain to use.
    let mut allowed = vec![0x00, 0xa3, 0x0e];
    allowed.extend_from_slice(b"example:url:v1");
    allowed.extend_from_slice(&[0xa1, 0x1e]);
    allowed.extend_from_slice(b"http://example.org/hello-world");
    let (descriptor, value, used) = decode::described(&allowed, Limits::DEFAULT).expect("decodes");
    assert_eq!(descriptor, Descriptor::Symbol("example:url:v1"));
    assert_eq!(value, Value::String("http://example.org/hello-world"));
    assert_eq!(used, allowed.len());
}

#[test]
fn a_numeric_descriptor_follows_the_assignment_policy() {
    // §1.5: `(domain-id << 32) | descriptor-id`, with domain 0 reserved for
    // the specification's own descriptors. `amqp:open:list` is therefore
    // 0x0000_0000_0000_0010, and the shortest encoding of that ulong is the
    // one-octet form — which is why every AMQP frame on the wire begins
    // `00 53 <code>`.
    let mut bytes = Vec::new();
    encode::composite(&Descriptor::Code(0x10), &[], &mut bytes).expect("encodes");
    assert_eq!(bytes, [0x00, 0x53, 0x10, 0x45]);

    // A descriptor in a private domain needs all eight octets: IANA PEN
    // 18060 (Apache) shifted left by 32, plus descriptor 4, is
    // `apache.org:selector-filter:string`'s 0x0000468C:0x00000004.
    let apache = (0x0000_468c_u64 << 32) | 4;
    let mut bytes = Vec::new();
    encode::composite(&Descriptor::Code(apache), &[], &mut bytes).expect("encodes");
    assert_eq!(
        bytes,
        [
            0x00, 0x80, 0x00, 0x00, 0x46, 0x8c, 0x00, 0x00, 0x00, 0x04, 0x45
        ]
    );
    let composite = decode::composite(&bytes, Limits::DEFAULT).expect("decodes");
    assert!(composite.descriptor.is(apache));
}
