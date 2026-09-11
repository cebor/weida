//! Conformance: the golden test vectors of `docs/adapters/zmtp.md` §10.1.
//!
//! §10.1 publishes these octets, so an implementation - this one, or a future
//! reimplementation, or a reader checking libzmq against the document - MUST
//! encode exactly them and MUST decode them back to exactly these values. The
//! vectors are asserted here through the crate's public surface only, with no
//! access to private helpers: the unit tests beside each codec cover the
//! halves, and this file is what makes the published bytes binding.
//!
//! Every vector is asserted **in both directions**. Encoding alone would not
//! catch a decoder that is wrong in the same way, which is the failure mode
//! that matters when the other end of the wire is libzmq and not us.

use weida_zmtp::error::CommandError;
use weida_zmtp::{
    Command, FrameHeader, FrameKind, Greeting, Mechanism, Metadata, SocketType, VERSION, frame,
    greeting,
};

/// The cap every vector is decoded under: the adapter's own
/// `max_message_bytes` neighbour on the weida side is `subscriber_buffer_bytes`
/// (8 MiB), and no vector is anywhere near it.
const CAP: u64 = 8 * 1024 * 1024;

/// Asserts one command vector: the whole frame encodes to `expected`, and
/// `expected` decodes back to the same command.
#[track_caller]
fn assert_command(command: Command<'_>, expected: &[u8]) {
    let frame = command.encode().expect("encode");
    assert_eq!(frame, expected, "{}: frame bytes", command.name());

    let (header, body, used) = frame::decode(expected, CAP).expect("decode");
    assert_eq!(header.kind, FrameKind::Command, "{}", command.name());
    assert_eq!(used, expected.len(), "{}: octets consumed", command.name());
    // The declared length must agree with the body actually written; a
    // hardcoded size in a test cannot catch that drift on its own.
    assert_eq!(
        header.len as usize,
        expected.len() - (used - body.len()),
        "{}: declared length",
        command.name()
    );
    assert_eq!(
        Command::decode(body).expect("decode body"),
        command,
        "{}: decoded value",
        command.name()
    );
}

#[test]
fn golden_greeting_null() {
    #[rustfmt::skip]
    let expected: [u8; 64] = [
        // signature: 0xFF, eight padding octets, 0x7F
        0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F,
        // version 3.1
        0x03, 0x01,
        // mechanism "NULL", null-padded to 20 octets
        b'N', b'U', b'L', b'L', 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // as-server: zero, as NULL requires
        0x00,
        // filler: 31 zero octets
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0,
    ];

    let greeting = Greeting::null();
    assert_eq!(greeting.encode(), expected);

    let decoded = Greeting::decode(&expected).expect("decode");
    assert_eq!(decoded, greeting);
    assert_eq!(decoded.version, VERSION);
    assert_eq!(decoded.mechanism, Mechanism::NULL);
    assert!(!decoded.as_server);
    assert_eq!(decoded.accept(Mechanism::NULL), Ok(VERSION));
}

#[test]
fn golden_greeting_partial() {
    let expected: [u8; 11] = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F, 0x03];
    assert_eq!(greeting::encode_partial(), expected);
    assert_eq!(greeting::sniff_major(&expected), Ok(3));
}

#[test]
fn golden_ready_with_socket_type() {
    assert_command(
        Command::Ready(Metadata::new().with_socket_type(SocketType::Req)),
        b"\x04\x19\x05READY\x0BSocket-Type\x00\x00\x00\x03REQ",
    );
}

#[test]
fn golden_ready_empty() {
    // A READY with no properties at all is a complete NULL handshake: the
    // Socket-Type is a SHOULD, not a MUST.
    assert_command(Command::Ready(Metadata::new()), b"\x04\x06\x05READY");
}

#[test]
fn golden_error() {
    assert_command(
        Command::Error("bad socket type"),
        b"\x04\x16\x05ERROR\x0Fbad socket type",
    );
}

#[test]
fn golden_subscribe_and_cancel() {
    assert_command(
        Command::Subscribe(b"px.eur"),
        b"\x04\x10\x09SUBSCRIBEpx.eur",
    );
    assert_command(Command::Cancel(b"px.eur"), b"\x04\x0D\x06CANCELpx.eur");
    // The empty subscription: matches every message, and is not the absence of
    // a subscription.
    assert_command(Command::Subscribe(b""), b"\x04\x0A\x09SUBSCRIBE");
}

#[test]
fn golden_ping_and_pong() {
    assert_command(
        Command::Ping {
            ttl: 300,
            context: b"ctx",
        },
        b"\x04\x0A\x04PING\x01\x2Cctx",
    );
    assert_command(Command::Pong { context: b"ctx" }, b"\x04\x08\x04PONGctx");
}

#[test]
fn golden_plain_handshake() {
    // 24/ZMTP-PLAIN: `hello = command-size %d5 "HELLO" username password`,
    // each field a length octet followed by its value. 0x04 is the COMMAND
    // flag; 0x12 is the body length (1 + 5 + 1 + 5 + 1 + 6).
    assert_command(
        Command::Hello {
            username: b"admin",
            password: b"secret",
        },
        b"\x04\x13\x05HELLO\x05admin\x06secret",
    );

    // An empty username and password are legal: `*-value = *OCTET`, and the
    // two length octets are still there.
    assert_command(
        Command::Hello {
            username: b"",
            password: b"",
        },
        b"\x04\x08\x05HELLO\x00\x00",
    );

    // `welcome = command-size %d7 "WELCOME"` — no data at all.
    assert_command(Command::Welcome, b"\x04\x08\x07WELCOME");

    // `initiate = command-size %d8 "INITIATE" metadata`, carrying the
    // client's metadata where NULL would have put it in READY.
    assert_command(
        Command::Initiate(Metadata::new().with_socket_type(SocketType::Dealer)),
        b"\x04\x1F\x08INITIATE\x0BSocket-Type\x00\x00\x00\x06DEALER",
    );
}

#[test]
fn golden_plain_hello_from_a_foreign_peer() {
    // The direction that matters for interop: the octets libzmq puts on the
    // wire for ZMQ_PLAIN_USERNAME="user", ZMQ_PLAIN_PASSWORD="pass", read
    // back field by field.
    let bytes: &[u8] = b"\x04\x10\x05HELLO\x04user\x04pass";
    let (header, body, used) = frame::decode(bytes, CAP).expect("decode");
    assert_eq!(header.kind, FrameKind::Command);
    assert_eq!(used, bytes.len());
    let Command::Hello { username, password } = Command::decode(body).expect("decode body") else {
        panic!("expected HELLO");
    };
    assert_eq!(username, b"user");
    assert_eq!(password, b"pass");

    // A truncated field is a violation rather than a request for more
    // octets: a command body is complete when it is decoded.
    let short: &[u8] = b"\x05HELLO\x09user";
    assert!(Command::decode(short).is_err());
}
#[test]
fn golden_message_frame_at_the_short_long_boundary() {
    // 255 octets: the largest short frame. Header 0x00 0xFF.
    let body = vec![0xAB; 255];
    let encoded = frame::encode(FrameKind::Message { more: false }, &body);
    assert_eq!(&encoded[..2], &[0x00, 0xFF]);
    assert_eq!(encoded.len(), 257);
    assert_eq!(&encoded[2..], &body[..]);

    // 256 octets: the smallest long frame. Header 0x02 and eight octets in
    // network order.
    let body = vec![0xAB; 256];
    let encoded = frame::encode(FrameKind::Message { more: false }, &body);
    assert_eq!(&encoded[..9], &[0x02, 0, 0, 0, 0, 0, 0, 0x01, 0x00]);
    assert_eq!(encoded.len(), 265);
    assert_eq!(&encoded[9..], &body[..]);

    let (header, decoded, used) = frame::decode(&encoded, CAP).expect("decode");
    assert_eq!(
        header,
        FrameHeader {
            kind: FrameKind::Message { more: false },
            len: 256
        }
    );
    assert_eq!(decoded, &body[..]);
    assert_eq!(used, encoded.len());
}

#[test]
fn golden_two_frame_multipart_message() {
    // "A multipart message is multiple sequential ZMTP messages, where all but
    // the last message has the MORE flag set."
    let expected: &[u8] = b"\x01\x01A\x00\x01B";

    let mut encoded = frame::encode(FrameKind::Message { more: true }, b"A");
    encoded.extend_from_slice(&frame::encode(FrameKind::Message { more: false }, b"B"));
    assert_eq!(encoded, expected);

    let (first, body, used) = frame::decode(expected, CAP).expect("first frame");
    assert_eq!(first.kind, FrameKind::Message { more: true });
    assert_eq!(body, b"A");
    let (last, body, used2) = frame::decode(&expected[used..], CAP).expect("last frame");
    assert_eq!(last.kind, FrameKind::Message { more: false });
    assert_eq!(body, b"B");
    assert_eq!(used + used2, expected.len());
}

#[test]
fn golden_ready_from_a_foreign_peer_is_read_field_by_field() {
    // The direction that matters for interop: bytes libzmq would send for a
    // PULL socket announcing an identity, decoded into fields. Property names
    // arrive in whatever case the sender chose, and lookup is
    // case-insensitive.
    let bytes: &[u8] =
        b"\x04\x2A\x05READY\x0Bsocket-type\x00\x00\x00\x04PULL\x08Identity\x00\x00\x00\x03abc";
    let (header, body, used) = frame::decode(bytes, CAP).expect("decode");
    assert_eq!(header.kind, FrameKind::Command);
    assert_eq!(used, bytes.len());
    let Command::Ready(md) = Command::decode(body).expect("decode body") else {
        panic!("expected READY");
    };
    assert_eq!(md.socket_type(), Some(SocketType::Pull));
    assert_eq!(md.get("identity"), Some(&b"abc"[..]));
    assert_eq!(md.properties().len(), 2);
    // Push talks to Pull and nothing else.
    assert!(SocketType::Push.accepts(SocketType::Pull));
    assert!(!SocketType::Push.accepts(SocketType::Sub));
}

#[test]
fn golden_unknown_command_names_are_refused_by_name() {
    // JOIN is a real ZMTP 3.1 command this adapter does not carry, so the
    // vector is here to pin the refusal rather than the omission.
    let bytes: &[u8] = b"\x04\x0A\x04JOINgroup1";
    let (_, body, _) = frame::decode(bytes, CAP).expect("the frame is well formed");
    assert_eq!(Command::decode(body), Err(CommandError::UnknownName));
}
