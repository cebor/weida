//! Golden vectors: the protocol reference's own examples, as octets.
//!
//! Every vector is asserted in **both** directions — the octets decode to the
//! value, and the value encodes to exactly those octets — because a codec
//! that agrees with itself is not evidence of anything. The octets are
//! written out literally, with the fields named in a comment, so that a
//! change to the encoder shows up here as a diff against the published text
//! rather than as a passing test.
//!
//! Three things are asserted here that no unit test can assert:
//!
//! 1. **Every one of the twelve verbs has a vector.**
//!    `every_verb_the_reference_lists_has_a_vector` compares the verbs the
//!    table covers against the reference's own table of operations, so a verb
//!    this crate implements and the table forgets, or the reverse, fails.
//! 2. **Every optional middle argument has a pair.** The optional arguments
//!    of `PUB`, `HPUB`, `SUB`, `MSG` and `HMSG` are positional and are
//!    distinguished only by argument count, so each of the five appears both
//!    with and without, and
//!    `the_optional_middle_argument_is_decided_only_by_argument_count` pins
//!    the distinction directly.
//! 3. **A published example that is wrong is refused.** The reference's first
//!    `HMSG` example is missing its `sid`; see
//!    `the_references_first_hmsg_example_is_missing_its_sid`.
//!
//! Source: NATS Docs, *Client Protocol*, accessed 2026-09-08
//! (`docs/research/nats.md` §14 source [4]), and ADR-4, *NATS Message
//! Headers* (source [12]).

use std::borrow::Cow;

use weida_nats_codec::{Connect, DecodeError, Headers, Limits, Op, ServerInfo};

const LIMITS: Limits = Limits::DEFAULT;

/// One published example.
struct Vector {
    /// Where the reference puts it.
    name: &'static str,
    /// The octets, `CRLF`s included.
    bytes: &'static [u8],
    /// What they mean.
    op: Op<'static>,
}

/// A header block, since `Headers` holds a vector and cannot be a constant.
fn block(
    status: Option<u16>,
    description: Option<&'static str>,
    entries: &[(&'static str, &'static str)],
) -> Headers<'static> {
    let mut headers = Headers::new();
    headers.status = status;
    headers.description = description;
    for (name, value) in entries {
        headers.push(name, value);
    }
    headers
}

fn table() -> Vec<Vector> {
    vec![
        // ---- INFO ------------------------------------------------------
        Vector {
            // The reference's own telnet transcript from demo.nats.io.
            name: "INFO (telnet transcript)",
            bytes: b"INFO {\"server_id\":\"Zk0GQ3JBSrg3oyxCRRlE09\",\"version\":\"1.2.0\",\"proto\":1,\"go\":\"go1.10.3\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":1048576,\"client_id\":2392}\r\n",
            op: Op::Info {
                json: b"{\"server_id\":\"Zk0GQ3JBSrg3oyxCRRlE09\",\"version\":\"1.2.0\",\"proto\":1,\"go\":\"go1.10.3\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":1048576,\"client_id\":2392}",
            },
        },
        Vector {
            // The shape a current `nats-server` sends: a named server, the
            // header capability, a nonce to sign, and the cluster's
            // `connect_urls`.
            name: "INFO (clustered server with a nonce)",
            bytes: b"INFO {\"server_id\":\"NDHJEXAMPLE7ZQ\",\"server_name\":\"us-east-1\",\"version\":\"2.10.22\",\"proto\":1,\"host\":\"0.0.0.0\",\"port\":4222,\"headers\":true,\"max_payload\":1048576,\"jetstream\":true,\"auth_required\":true,\"tls_required\":false,\"nonce\":\"pAcXbTBjZ8Sd6Iw\",\"connect_urls\":[\"10.0.0.184:4333\",\"192.168.129.1:4333\"],\"ldm\":false}\r\n",
            op: Op::Info {
                json: b"{\"server_id\":\"NDHJEXAMPLE7ZQ\",\"server_name\":\"us-east-1\",\"version\":\"2.10.22\",\"proto\":1,\"host\":\"0.0.0.0\",\"port\":4222,\"headers\":true,\"max_payload\":1048576,\"jetstream\":true,\"auth_required\":true,\"tls_required\":false,\"nonce\":\"pAcXbTBjZ8Sd6Iw\",\"connect_urls\":[\"10.0.0.184:4333\",\"192.168.129.1:4333\"],\"ldm\":false}",
            },
        },
        // ---- CONNECT ---------------------------------------------------
        Vector {
            // "Here is an example from the default string of the Go client."
            name: "CONNECT (Go client default string)",
            bytes: b"CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"\",\"lang\":\"go\",\"version\":\"1.2.2\",\"protocol\":1}\r\n",
            op: Op::Connect(Connect {
                verbose: false,
                pedantic: false,
                tls_required: false,
                name: Some(Cow::Borrowed("")),
                lang: Some(Cow::Borrowed("go")),
                version: Some(Cow::Borrowed("1.2.2")),
                protocol: Some(1),
                ..connect_default()
            }),
        },
        Vector {
            // An NKey answer to the `INFO` above: the public key, the
            // signature over the server's nonce, and the two capabilities a
            // request/reply client needs for the `503` status to arrive.
            name: "CONNECT (NKey signature over the nonce)",
            bytes: b"CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"weida\",\"lang\":\"rust\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":false,\"sig\":\"BXlxxqXExample_sig\",\"no_responders\":true,\"headers\":true,\"nkey\":\"UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4\"}\r\n",
            op: Op::Connect(Connect {
                verbose: false,
                pedantic: false,
                tls_required: false,
                name: Some(Cow::Borrowed("weida")),
                lang: Some(Cow::Borrowed("rust")),
                version: Some(Cow::Borrowed("0.1.0")),
                protocol: Some(1),
                echo: Some(false),
                sig: Some(Cow::Borrowed("BXlxxqXExample_sig")),
                no_responders: Some(true),
                headers: Some(true),
                nkey: Some(Cow::Borrowed(
                    "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4",
                )),
                ..connect_default()
            }),
        },
        // ---- PUB -------------------------------------------------------
        Vector {
            // "To publish the ASCII string message payload 'Hello NATS!' to
            // subject FOO":  subject FOO, no reply-to, 11 octets.
            name: "PUB (no reply-to)",
            bytes: b"PUB FOO 11\r\nHello NATS!\r\n",
            op: Op::Pub {
                subject: b"FOO",
                reply_to: None,
                payload: b"Hello NATS!",
            },
        },
        Vector {
            // "To publish a request message 'Knock Knock' to subject
            // FRONT.DOOR with reply subject JOKE.22": subject FRONT.DOOR,
            // reply-to JOKE.22, 11 octets.
            name: "PUB (with reply-to)",
            bytes: b"PUB FRONT.DOOR JOKE.22 11\r\nKnock Knock\r\n",
            op: Op::Pub {
                subject: b"FRONT.DOOR",
                reply_to: Some(b"JOKE.22"),
                payload: b"Knock Knock",
            },
        },
        Vector {
            // "To publish an empty message to subject NOTIFY" — "set the
            // payload size to 0, but the second CRLF is still required".
            name: "PUB (empty payload)",
            bytes: b"PUB NOTIFY 0\r\n\r\n",
            op: Op::Pub {
                subject: b"NOTIFY",
                reply_to: None,
                payload: b"",
            },
        },
        // ---- HPUB ------------------------------------------------------
        Vector {
            // subject FOO, no reply-to, 22 header octets, 33 total: the
            // header block is NATS/1.0 + "Bar: Baz" + the blank line, and
            // 33 - 22 = 11 octets of payload.
            name: "HPUB (no reply-to)",
            bytes: b"HPUB FOO 22 33\r\nNATS/1.0\r\nBar: Baz\r\n\r\nHello NATS!\r\n",
            op: Op::Hpub {
                subject: b"FOO",
                reply_to: None,
                headers: block(None, None, &[("Bar", "Baz")]),
                payload: b"Hello NATS!",
            },
        },
        Vector {
            // subject FRONT.DOOR, reply-to JOKE.22, 45 header octets, 56
            // total, two headers.
            name: "HPUB (with reply-to)",
            bytes: b"HPUB FRONT.DOOR JOKE.22 45 56\r\nNATS/1.0\r\nBREAKFAST: donut\r\nLUNCH: burger\r\n\r\nKnock Knock\r\n",
            op: Op::Hpub {
                subject: b"FRONT.DOOR",
                reply_to: Some(b"JOKE.22"),
                headers: block(None, None, &[("BREAKFAST", "donut"), ("LUNCH", "burger")]),
                payload: b"Knock Knock",
            },
        },
        Vector {
            // "To publish an empty message to subject NOTIFY with one header
            // Bar with value Baz": total equals the header size, and the
            // trailing CRLF is still there.
            name: "HPUB (empty payload, total equals header size)",
            bytes: b"HPUB NOTIFY 22 22\r\nNATS/1.0\r\nBar: Baz\r\n\r\n\r\n",
            op: Op::Hpub {
                subject: b"NOTIFY",
                reply_to: None,
                headers: block(None, None, &[("Bar", "Baz")]),
                payload: b"",
            },
        },
        Vector {
            // "one header BREAKFAST having two values": both survive, in
            // order, under the same name.
            name: "HPUB (a repeated header name)",
            bytes: b"HPUB MORNING.MENU 47 51\r\nNATS/1.0\r\nBREAKFAST: donut\r\nBREAKFAST: eggs\r\n\r\nYum!\r\n",
            op: Op::Hpub {
                subject: b"MORNING.MENU",
                reply_to: None,
                headers: block(
                    None,
                    None,
                    &[("BREAKFAST", "donut"), ("BREAKFAST", "eggs")],
                ),
                payload: b"Yum!",
            },
        },
        // ---- SUB -------------------------------------------------------
        Vector {
            // "To subscribe to the subject FOO with the connection-unique
            // subscription identifier (sid) 1".
            name: "SUB (no queue group)",
            bytes: b"SUB FOO 1\r\n",
            op: Op::Sub {
                subject: b"FOO",
                queue_group: None,
                sid: b"1",
            },
        },
        Vector {
            // "To subscribe the current connection to the subject BAR as
            // part of distribution queue group G1 with sid 44".
            name: "SUB (with queue group)",
            bytes: b"SUB BAR G1 44\r\n",
            op: Op::Sub {
                subject: b"BAR",
                queue_group: Some(b"G1"),
                sid: b"44",
            },
        },
        // ---- UNSUB -----------------------------------------------------
        Vector {
            // "To unsubscribe from FOO".
            name: "UNSUB (no max_msgs)",
            bytes: b"UNSUB 1\r\n",
            op: Op::Unsub {
                sid: b"1",
                max_msgs: None,
            },
        },
        Vector {
            // "To auto-unsubscribe from FOO after 5 messages have been
            // received".
            name: "UNSUB (with max_msgs)",
            bytes: b"UNSUB 1 5\r\n",
            op: Op::Unsub {
                sid: b"1",
                max_msgs: Some(5),
            },
        },
        // ---- MSG -------------------------------------------------------
        Vector {
            // "The following message delivers an application message from
            // subject FOO.BAR": subject, sid 9, no reply-to, 11 octets.
            name: "MSG (no reply-to)",
            bytes: b"MSG FOO.BAR 9 11\r\nHello World\r\n",
            op: Op::Msg {
                subject: b"FOO.BAR",
                sid: b"9",
                reply_to: None,
                payload: b"Hello World",
            },
        },
        Vector {
            // "To deliver the same message along with a reply subject".
            name: "MSG (with reply-to)",
            bytes: b"MSG FOO.BAR 9 GREETING.34 11\r\nHello World\r\n",
            op: Op::Msg {
                subject: b"FOO.BAR",
                sid: b"9",
                reply_to: Some(b"GREETING.34"),
                payload: b"Hello World",
            },
        },
        // ---- HMSG ------------------------------------------------------
        Vector {
            // The reference's first HMSG example with the `sid` its own
            // grammar requires put back; see
            // `the_references_first_hmsg_example_is_missing_its_sid`.
            name: "HMSG (no reply-to)",
            bytes: b"HMSG FOO.BAR 9 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
            op: Op::Hmsg {
                subject: b"FOO.BAR",
                sid: b"9",
                reply_to: None,
                headers: block(None, None, &[("FoodGroup", "vegetable")]),
                payload: b"Hello World",
            },
        },
        Vector {
            // "To deliver the same message along with a reply subject":
            // subject, sid 9, reply-to BAZ.69, 34 header octets, 45 total.
            name: "HMSG (with reply-to)",
            bytes: b"HMSG FOO.BAR 9 BAZ.69 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
            op: Op::Hmsg {
                subject: b"FOO.BAR",
                sid: b"9",
                reply_to: Some(b"BAZ.69"),
                headers: block(None, None, &[("FoodGroup", "vegetable")]),
                payload: b"Hello World",
            },
        },
        Vector {
            // The no-responder answer of request/reply: a status on the
            // version line, no entries, no payload. `docs/research/nats.md`
            // §4 and ADR-4.
            name: "HMSG (503 no responders)",
            bytes: b"HMSG _INBOX.7Yz.1 3 16 16\r\nNATS/1.0 503\r\n\r\n\r\n",
            op: Op::Hmsg {
                subject: b"_INBOX.7Yz.1",
                sid: b"3",
                reply_to: None,
                headers: block(Some(503), None, &[]),
                payload: b"",
            },
        },
        Vector {
            // A push consumer's idle heartbeat: a status with a description.
            name: "HMSG (100 Idle Heartbeat)",
            bytes: b"HMSG _INBOX.hb 4 76 76\r\nNATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 7\r\nNats-Last-Stream: 42\r\n\r\n\r\n",
            op: Op::Hmsg {
                subject: b"_INBOX.hb",
                sid: b"4",
                reply_to: None,
                headers: block(
                    Some(100),
                    Some("Idle Heartbeat"),
                    &[("Nats-Last-Consumer", "7"), ("Nats-Last-Stream", "42")],
                ),
                payload: b"",
            },
        },
        // ---- PING / PONG / +OK / -ERR ----------------------------------
        Vector {
            name: "PING",
            bytes: b"PING\r\n",
            op: Op::Ping,
        },
        Vector {
            name: "PONG",
            bytes: b"PONG\r\n",
            op: Op::Pong,
        },
        Vector {
            name: "+OK",
            bytes: b"+OK\r\n",
            op: Op::Ok,
        },
        Vector {
            // From the reference's own telnet transcript, quotes included.
            name: "-ERR",
            bytes: b"-ERR 'Stale Connection'\r\n",
            op: Op::Err {
                reason: b"Stale Connection",
            },
        },
    ]
}

/// `Connect::default()`, spelled so the vectors can use `..`.
fn connect_default() -> Connect<'static> {
    Connect::default()
}

#[test]
fn the_octets_decode_to_the_value() {
    for vector in table() {
        let (op, used) = Op::decode(vector.bytes, LIMITS)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(op, vector.op, "{}", vector.name);
        assert_eq!(used, vector.bytes.len(), "{}", vector.name);
    }
}

#[test]
fn the_value_encodes_to_the_octets() {
    for vector in table() {
        let mut written = Vec::new();
        vector
            .op
            .encode(&mut written)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(
            written,
            vector.bytes,
            "{}\n  wrote {:?}\n  wanted {:?}",
            vector.name,
            String::from_utf8_lossy(&written),
            String::from_utf8_lossy(vector.bytes),
        );
    }
}

#[test]
fn every_verb_the_reference_lists_has_a_vector() {
    // The reference's own overview table, in its own order.
    const VERBS: [&str; 12] = [
        "INFO", "CONNECT", "PUB", "HPUB", "SUB", "UNSUB", "MSG", "HMSG", "PING", "PONG", "+OK",
        "-ERR",
    ];
    let mut covered: Vec<&'static str> = table().iter().map(|vector| vector.op.verb()).collect();
    covered.sort_unstable();
    covered.dedup();
    let mut expected = VERBS.to_vec();
    expected.sort_unstable();
    assert_eq!(covered, expected);
}

#[test]
fn every_verb_with_an_optional_middle_argument_has_both_forms() {
    // The five verbs whose grammar has an optional argument between two
    // required ones, and which therefore cannot be read without counting.
    for verb in ["PUB", "HPUB", "SUB", "MSG", "HMSG"] {
        let (present, absent) = table()
            .into_iter()
            .filter(|vector| vector.op.verb() == verb)
            .fold((0, 0), |(present, absent), vector| {
                let carries_the_optional_argument = match vector.op {
                    Op::Pub { reply_to, .. } | Op::Msg { reply_to, .. } => reply_to.is_some(),
                    Op::Hpub { reply_to, .. } | Op::Hmsg { reply_to, .. } => reply_to.is_some(),
                    Op::Sub { queue_group, .. } => queue_group.is_some(),
                    _ => unreachable!(),
                };
                if carries_the_optional_argument {
                    (present + 1, absent)
                } else {
                    (present, absent + 1)
                }
            });
        assert!(
            present >= 1 && absent >= 1,
            "{verb} needs a vector with and a vector without its optional \
             middle argument: {present} with, {absent} without"
        );
    }
}

#[test]
fn the_optional_middle_argument_is_decided_only_by_argument_count() {
    // The rule, stated as five pairs of lines that differ by one argument.
    // Nothing in the arguments themselves says which is which: `b` is a
    // reply subject in the first line and a byte count in none of them.
    let (three, _) = Op::decode(b"PUB a b 5\r\nhello\r\n", LIMITS).expect("decodes");
    let (two, _) = Op::decode(b"PUB a 5\r\nhello\r\n", LIMITS).expect("decodes");
    assert_eq!(
        three,
        Op::Pub {
            subject: b"a",
            reply_to: Some(b"b"),
            payload: b"hello"
        }
    );
    assert_eq!(
        two,
        Op::Pub {
            subject: b"a",
            reply_to: None,
            payload: b"hello"
        }
    );

    let (four, _) =
        Op::decode(b"HPUB a b 12 17\r\nNATS/1.0\r\n\r\nhello\r\n", LIMITS).expect("decodes");
    let (three, _) =
        Op::decode(b"HPUB a 12 17\r\nNATS/1.0\r\n\r\nhello\r\n", LIMITS).expect("decodes");
    assert!(matches!(
        four,
        Op::Hpub {
            reply_to: Some(b"b"),
            ..
        }
    ));
    assert!(matches!(three, Op::Hpub { reply_to: None, .. }));

    let (three, _) = Op::decode(b"SUB a g 1\r\n", LIMITS).expect("decodes");
    let (two, _) = Op::decode(b"SUB a 1\r\n", LIMITS).expect("decodes");
    assert_eq!(
        three,
        Op::Sub {
            subject: b"a",
            queue_group: Some(b"g"),
            sid: b"1"
        }
    );
    assert_eq!(
        two,
        Op::Sub {
            subject: b"a",
            queue_group: None,
            sid: b"1"
        }
    );

    let (four, _) = Op::decode(b"MSG a 1 b 5\r\nhello\r\n", LIMITS).expect("decodes");
    let (three, _) = Op::decode(b"MSG a 1 5\r\nhello\r\n", LIMITS).expect("decodes");
    assert!(matches!(
        four,
        Op::Msg {
            reply_to: Some(b"b"),
            ..
        }
    ));
    assert!(matches!(three, Op::Msg { reply_to: None, .. }));

    let (five, _) =
        Op::decode(b"HMSG a 1 b 12 17\r\nNATS/1.0\r\n\r\nhello\r\n", LIMITS).expect("decodes");
    let (four, _) =
        Op::decode(b"HMSG a 1 12 17\r\nNATS/1.0\r\n\r\nhello\r\n", LIMITS).expect("decodes");
    assert!(matches!(
        five,
        Op::Hmsg {
            reply_to: Some(b"b"),
            ..
        }
    ));
    assert!(matches!(four, Op::Hmsg { reply_to: None, .. }));

    // And one argument too many is refused rather than guessed at.
    assert_eq!(
        Op::decode(b"SUB a g 1 x\r\n", LIMITS),
        Err(DecodeError::ArgumentCount {
            verb: "SUB",
            found: 4
        })
    );
}

#[test]
fn the_references_first_hmsg_example_is_missing_its_sid() {
    // The document prints
    //   HMSG FOO.BAR 34 45␍␊NATS/1.0␍␊FoodGroup: vegetable␍␊␍␊Hello World␍␊
    // under "The following message delivers an application message from
    // subject FOO.BAR with a header". That is three arguments, and the same
    // section's syntax is
    //   HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
    // with `sid` marked "always". The next example in the same section,
    // `HMSG FOO.BAR 9 BAZ.69 34 45`, has it. It is a typo, and a decoder that
    // accepted it would have to read `34` as a `sid` and then find only one
    // count where two are required.
    assert_eq!(
        Op::decode(
            b"HMSG FOO.BAR 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n",
            LIMITS
        ),
        Err(DecodeError::ArgumentCount {
            verb: "HMSG",
            found: 3
        })
    );
}

#[test]
fn the_info_vectors_read_as_fields() {
    let vectors = table();
    let demo = &vectors[0];
    let Op::Info { json } = demo.op else {
        panic!("{} is an INFO", demo.name);
    };
    let info = ServerInfo::parse(json, LIMITS).expect("reads");
    assert_eq!(info.server_id.as_deref(), Some("Zk0GQ3JBSrg3oyxCRRlE09"));
    assert_eq!(info.max_payload, Some(1_048_576));
    assert_eq!(info.proto, Some(1));
    assert_eq!(info.port, Some(4222));

    let clustered = &vectors[1];
    let Op::Info { json } = clustered.op else {
        panic!("{} is an INFO", clustered.name);
    };
    let info = ServerInfo::parse(json, LIMITS).expect("reads");
    assert_eq!(info.server_name.as_deref(), Some("us-east-1"));
    assert_eq!(info.headers, Some(true));
    assert_eq!(info.auth_required, Some(true));
    assert_eq!(info.tls_required, Some(false));
    assert_eq!(info.nonce.as_deref(), Some("pAcXbTBjZ8Sd6Iw"));
    assert_eq!(info.ldm, Some(false));
    assert_eq!(
        info.connect_urls.expect("a cluster"),
        ["10.0.0.184:4333", "192.168.129.1:4333"]
    );
    // And the number a client then uses as its own bound.
    let learned = LIMITS.with_max_payload(info.max_payload.expect("always present"));
    assert_eq!(learned.max_payload, 1_048_576);
}

#[test]
fn a_header_block_keeps_repeats_and_case_across_the_round_trip() {
    let vectors = table();
    let repeated = vectors
        .iter()
        .find(|vector| vector.name == "HPUB (a repeated header name)")
        .expect("the vector");
    let headers = repeated.op.headers().expect("a block");
    assert_eq!(
        headers.entries(),
        [("BREAKFAST", "donut"), ("BREAKFAST", "eggs")]
    );
    assert_eq!(
        headers.get_all("BREAKFAST").collect::<Vec<_>>(),
        ["donut", "eggs"]
    );

    let mixed = vectors
        .iter()
        .find(|vector| vector.name == "HMSG (no reply-to)")
        .expect("the vector");
    let headers = mixed.op.headers().expect("a block");
    assert_eq!(headers.get("FoodGroup"), Some("vegetable"));
    assert_eq!(headers.get("foodgroup"), None, "case is not folded");
    assert_eq!(
        headers.get_ignore_ascii_case("foodgroup"),
        Some("vegetable")
    );
}
