//! Links against a scripted server: the two handle spaces, the settle modes,
//! the steal, and the two conditions the specification names for a wrong
//! handle.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use weida_amqp::link::{LinkEvent, LinkOptions, LinkState};
use weida_amqp::session::SessionOptions;
use weida_amqp::{
    Condition, Connection, ConnectionOptions, Source, State, Target, TerminusDurability,
    TerminusExpiryPolicy,
};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Attach, Begin, Close, Detach, Open, Performative};
use weida_amqp_codec::protocol_header::{self, ProtocolHeader};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, ReceiverSettleMode, Role, SenderSettleMode};
use weida_runtime::Exec;

const DEADLINE: Duration = Duration::from_secs(5);

/// What a test looks at in an `attach` it read, owned.
#[allow(dead_code, reason = "each field is asserted by at least one test")]
struct AttachSeen {
    name: String,
    handle: u32,
    role: Role,
    snd_settle_mode: SenderSettleMode,
    rcv_settle_mode: ReceiverSettleMode,
    initial_delivery_count: Option<u32>,
    max_message_size: Option<u64>,
}

struct Server {
    stream: TcpStream,
    buf: Vec<u8>,
    from: usize,
}

impl Server {
    async fn fill(&mut self, want: usize) {
        while self.buf.len() - self.from < want {
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "the client closed before sending {want} octets");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    async fn read_header(&mut self) -> ProtocolHeader {
        self.fill(protocol_header::LEN).await;
        let header = protocol_header::decode(&self.buf[self.from..]).unwrap();
        self.from += protocol_header::LEN;
        header
    }

    async fn read_frame(&mut self) -> (u16, Vec<u8>) {
        self.fill(8).await;
        let header = frame::decode_header(&self.buf[self.from..], u32::MAX).unwrap();
        let size = header.size as usize;
        self.fill(size).await;
        let bytes = self.buf[self.from..self.from + size].to_vec();
        self.from += size;
        (header.channel, bytes)
    }

    /// The next frame, decoded far enough to match on.
    async fn read_named(&mut self) -> (u16, String, Vec<u8>) {
        let (channel, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        if frame.is_empty() {
            return (channel, "empty".to_owned(), bytes);
        }
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        (channel, performative.name().to_owned(), bytes)
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn send(&mut self, channel: u16, performative: Performative<'_>) {
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, channel, u32::MAX, |body| {
            performative.encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    async fn handshake(&mut self) {
        assert_eq!(self.read_header().await, ProtocolHeader::AMQP);
        self.write(&ProtocolHeader::AMQP.encode()).await;
        let (_, name, _) = self.read_named().await;
        assert_eq!(name, "open");
        let mut open = Open::new("broker-1");
        open.channel_max = 15;
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(open).encode(body)
        })
        .unwrap();
        self.write(&out).await;
    }

    /// The answering `begin`, on the server's own channel, advertising
    /// `handle_max`.
    async fn answer_begin(&mut self, ours: u16, theirs: u16, handle_max: u32) {
        let mut begin = Begin::new(0, 400, 400);
        begin.remote_channel = Some(theirs);
        begin.handle_max = handle_max;
        self.send(ours, Performative::Begin(begin)).await;
    }

    /// Reads an `attach` and reports, owned, the fields a test cares about.
    ///
    /// Owned rather than borrowed because the frame's buffer dies at the end
    /// of the read and every assertion here outlives it.
    async fn read_attach(&mut self) -> AttachSeen {
        let (_, bytes) = self.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Attach(attach) => AttachSeen {
                name: attach.name.to_owned(),
                handle: attach.handle,
                role: attach.role,
                snd_settle_mode: attach.snd_settle_mode,
                rcv_settle_mode: attach.rcv_settle_mode,
                initial_delivery_count: attach.initial_delivery_count,
                max_message_size: attach.max_message_size,
            },
            other => panic!("expected attach, got {}", other.name()),
        }
    }

    /// The answering `attach`, with the server's own handle.
    async fn answer_attach(&mut self, channel: u16, answer: Answer<'_>) {
        let mut attach = Attach::new(answer.name, answer.handle, answer.role);
        attach.snd_settle_mode = answer.snd;
        attach.rcv_settle_mode = answer.rcv;
        attach.source = answer.source.map(Source::to_value);
        attach.target = answer.target.map(Target::to_value);
        if answer.role == Role::Sender {
            attach.initial_delivery_count = Some(0);
        }
        self.send(channel, Performative::Attach(attach)).await;
    }
}

/// The server's half of an `attach` exchange.
struct Answer<'a> {
    name: &'a str,
    handle: u32,
    role: Role,
    snd: SenderSettleMode,
    rcv: ReceiverSettleMode,
    source: Option<&'a Source>,
    target: Option<&'a Target>,
}

impl<'a> Answer<'a> {
    /// The common case: a broker answering with the modes it was offered and
    /// the termini it created.
    fn to(name: &'a str, handle: u32, role: Role) -> Self {
        Self {
            name,
            handle,
            role,
            snd: SenderSettleMode::Mixed,
            rcv: ReceiverSettleMode::First,
            source: None,
            target: None,
        }
    }

    fn source(mut self, source: &'a Source) -> Self {
        self.source = Some(source);
        self
    }

    fn target(mut self, target: &'a Target) -> Self {
        self.target = Some(target);
        self
    }

    fn modes(mut self, snd: SenderSettleMode, rcv: ReceiverSettleMode) -> Self {
        self.snd = snd;
        self.rcv = rcv;
        self
    }
}

async fn scripted() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

async fn accept(listener: &TcpListener) -> Server {
    let (stream, _) = listener.accept().await.unwrap();
    Server {
        stream,
        buf: Vec::new(),
        from: 0,
    }
}

fn options() -> ConnectionOptions {
    let mut options = ConnectionOptions::new("client-1");
    options.idle_time_out = None;
    options.handshake_timeout = Duration::from_secs(2);
    options.close_budget = Duration::from_secs(2);
    options
}

#[tokio::test]
async fn a_link_is_attached_in_each_direction_and_the_handles_do_not_agree() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        // A sending link: we describe both termini, the broker answers with
        // what it actually created, on its *own* handle.
        let sending = server.read_attach().await;
        assert_eq!(sending.name, "orders");
        assert_eq!(sending.handle, 0, "lowest free");
        assert_eq!(sending.role, Role::Sender);
        // The broker adjusted the address: it created a queue where we named
        // an exchange. "MUST then report what it actually created."
        let created = Target::at("/queues/orders");
        let empty_source = Source::default();
        server
            .answer_attach(
                4,
                Answer::to("orders", 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&created),
            )
            .await;

        let receiving = server.read_attach().await;
        assert_eq!(receiving.name, "invoices");
        assert_eq!(receiving.handle, 1, "lowest free again");
        assert_eq!(receiving.role, Role::Receiver);
        let invoices = Source::at("/queues/invoices");
        let empty_target = Target::default();
        server
            .answer_attach(
                4,
                Answer::to("invoices", 12, Role::Sender)
                    .modes(SenderSettleMode::Settled, ReceiverSettleMode::First)
                    .source(&invoices)
                    .target(&empty_target),
            )
            .await;

        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close");
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    let sender = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender(
            "orders",
            Target::at("/exchanges/amq.direct/orders"),
        )),
    )
    .await
    .unwrap()
    .expect("the sending link attached");
    assert_eq!(sender.name(), "orders");
    assert_eq!(sender.role(), Role::Sender);
    assert_eq!(sender.output_handle(), 0);
    assert_eq!(
        sender.input_handle(),
        Some(11),
        "the two ends choose their handles independently"
    );
    assert_eq!(sender.state(), LinkState::Attached);
    assert_eq!(
        sender.remote_target().and_then(|t| t.address),
        Some("/queues/orders".to_owned()),
        "the answering attach reports what was actually created"
    );

    let receiver = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver(
            "invoices",
            Source::at("/queues/invoices"),
        )),
    )
    .await
    .unwrap()
    .expect("the receiving link attached");
    assert_eq!(receiver.output_handle(), 1);
    assert_eq!(receiver.input_handle(), Some(12));
    assert_eq!(
        receiver.negotiated().unwrap().snd_settle_mode,
        SenderSettleMode::Settled,
        "we are the receiver, so snd-settle-mode is the peer's"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_five_terminus_fields_reach_the_wire() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        // Read the attach's source out of the raw frame, because that is the
        // only way to assert what actually went on the wire.
        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        let Performative::Attach(attach) = performative else {
            panic!("expected attach");
        };
        let source = Source::from_value(attach.source.clone().expect("a source")).unwrap();
        assert_eq!(source.address.as_deref(), Some("/queues/durable"));
        assert_eq!(source.durable, TerminusDurability::UnsettledState);
        assert_eq!(source.expiry_policy, TerminusExpiryPolicy::Never);
        assert_eq!(source.timeout, 600);
        assert!(!source.dynamic);

        let empty_target = Target::default();
        server
            .answer_attach(
                4,
                Answer::to(attach.name, 11, Role::Sender)
                    .modes(SenderSettleMode::Unsettled, ReceiverSettleMode::First)
                    .source(&source)
                    .target(&empty_target),
            )
            .await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close");
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    let link = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver(
            "durable",
            Source {
                address: Some("/queues/durable".into()),
                durable: TerminusDurability::UnsettledState,
                expiry_policy: TerminusExpiryPolicy::Never,
                timeout: 600,
                dynamic: false,
                ..Source::default()
            },
        )),
    )
    .await
    .unwrap()
    .expect("attached");
    // And it comes back, which is what makes the round trip observable at
    // the client rather than only in a unit test.
    let remote = link.remote_source().expect("the peer reported a source");
    assert_eq!(remote.durable, TerminusDurability::UnsettledState);
    assert_eq!(remote.expiry_policy, TerminusExpiryPolicy::Never);
    assert_eq!(remote.timeout, 600);

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_second_attach_of_the_same_name_steals_the_first() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        // The termini the answers carry, bound here so the builder can
        // borrow them for the life of the closure.
        let empty_source = Source::default();
        let queue = Target::at("q");
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        let first = server.read_attach().await;
        assert_eq!(first.handle, 0);
        server
            .answer_attach(
                4,
                Answer::to("orders", 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        // The steal: the incumbent is detached with amqp:link:stolen before
        // the newcomer's attach goes out.
        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Detach(Detach {
                handle,
                closed,
                error: Some(error),
            }) => {
                assert_eq!(handle, 0, "the incumbent's handle");
                assert!(closed, "a stolen link is destroyed, not merely unmapped");
                assert_eq!(error.condition, condition::LINK_STOLEN);
            }
            other => panic!(
                "expected detach with amqp:link:stolen, got {}",
                other.name()
            ),
        }

        let second = server.read_attach().await;
        assert_eq!(second.name, "orders");
        assert_ne!(
            second.handle, 0,
            "an errored handle is never reused, and a stolen link is errored"
        );
        server
            .answer_attach(
                4,
                Answer::to("orders", 12, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;
        second.handle
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    let mut incumbent = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("the first attach");
    assert_eq!(incumbent.output_handle(), 0);

    let newcomer = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("the second attach wins");

    // The loser is told, with the condition the specification names.
    match tokio::time::timeout(DEADLINE, incumbent.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(Some(condition))) => {
            assert_eq!(condition.condition, condition::LINK_STOLEN);
        }
        other => panic!("expected Detached with amqp:link:stolen, got {other:?}"),
    }
    assert!(!incumbent.state().is_usable());

    let handle = tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newcomer.output_handle(), handle);
    assert_ne!(newcomer.output_handle(), 0);
}

#[tokio::test]
async fn an_attach_on_a_handle_already_in_use_closes_the_connection() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let empty_source = Source::default();
        let queue = Target::at("q");
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        let first = server.read_attach().await;
        server
            .answer_attach(
                4,
                Answer::to(&first.name, 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        // A second attach on the same *input* handle. Part 2 §2.6.2: "MUST
        // be answered with an immediate close carrying
        // amqp:session:handle-in-use".
        server
            .answer_attach(
                4,
                Answer::to("another", 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        let (channel, bytes) = server.read_frame().await;
        assert_eq!(channel, 0, "close is on channel 0");
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Close(Close { error: Some(error) }) => {
                assert_eq!(error.condition, condition::SESSION_HANDLE_IN_USE);
            }
            other => panic!("expected close with handle-in-use, got {}", other.name()),
        }
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let _link = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("attached");

    match tokio::time::timeout(DEADLINE, connection.closed())
        .await
        .unwrap()
    {
        State::Closed(Some(condition)) => {
            assert_eq!(condition.condition, condition::SESSION_HANDLE_IN_USE);
        }
        other => panic!("expected Closed with handle-in-use, got {other:?}"),
    }
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_errored_detach_poisons_its_handle_for_the_life_of_the_session() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let empty_source = Source::default();
        let queue = Target::at("q");
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        let first = server.read_attach().await;
        assert_eq!(first.handle, 0);
        server
            .answer_attach(
                4,
                Answer::to(&first.name, 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        // The broker detaches the link with an error.
        let mut detach = Detach::new(11);
        detach.closed = true;
        let error =
            weida_amqp_codec::AmqpError::new(condition::LINK_DETACH_FORCED).described("evicted");
        detach.error = Some(error);
        server.send(4, Performative::Detach(detach)).await;

        // The next link must not be given handle 0: the broker is entitled
        // to still be sending frames for the dead one.
        let second = server.read_attach().await;
        assert_ne!(second.handle, 0, "an errored handle is never reused");
        second.handle
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut first = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("attached");

    match tokio::time::timeout(DEADLINE, first.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(Some(condition))) => {
            assert_eq!(condition.condition, condition::LINK_DETACH_FORCED);
            assert_eq!(condition.description.as_deref(), Some("evicted"));
        }
        other => panic!("expected Detached with its error, got {other:?}"),
    }
    assert_eq!(
        first.state(),
        LinkState::Detached(Some(Condition::described(
            condition::LINK_DETACH_FORCED,
            "evicted"
        )))
    );

    // Attaching again must not reuse handle 0. The attach will not be
    // answered, so the wait ends in our own deadline - what matters is the
    // handle the server saw.
    let second = session.attach(LinkOptions::sender("invoices", Target::at("q")));
    let _ = tokio::time::timeout(Duration::from_millis(500), second).await;
    let handle = tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handle, 1, "handle 0 stays poisoned");
}

#[tokio::test]
async fn the_peers_handle_max_bounds_the_link_table() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let empty_source = Source::default();
        let queue = Target::at("q");
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        // handle-max 0 admits exactly one link, on handle 0.
        server.answer_begin(4, 0, 0).await;

        let first = server.read_attach().await;
        assert_eq!(first.handle, 0);
        server
            .answer_attach(
                4,
                Answer::to(&first.name, 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "close", "the second attach never reached the wire");
        server
            .send(0, Performative::Close(Close { error: None }))
            .await;
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let _first = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("the one link the peer admits");

    let error = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("invoices", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect_err("no handle left");
    assert!(
        error.to_string().contains("handle-max"),
        "the refusal names the bound: {error}"
    );

    tokio::time::timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_frame_on_an_unattached_handle_ends_the_session() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        // A transfer naming a handle no link was ever attached on. Part 2
        // §2.8.17 names this one: amqp:session:unattached-handle.
        let mut transfer = weida_amqp_codec::performative::Transfer::new(9);
        transfer.delivery_id = Some(0);
        transfer.delivery_tag = Some(b"t");
        server.send(4, Performative::Transfer(transfer)).await;

        let (channel, bytes) = server.read_frame().await;
        assert_eq!(channel, 0, "the end is on our own outgoing channel");
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::End(weida_amqp_codec::performative::End { error: Some(error) }) => {
                assert_eq!(error.condition, condition::SESSION_UNATTACHED_HANDLE);
                assert!(
                    error.description.unwrap().contains("handle 9"),
                    "the end says which handle"
                );
            }
            other => panic!("expected end with unattached-handle, got {}", other.name()),
        }
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    assert!(session.state().is_usable());
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn an_unsolicited_attach_is_refused_with_a_null_terminus_and_a_detach() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let queue_source = Source::at("q");
        let empty_target = Target::default();
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        // The broker attaches a link to us. This client holds no nodes, so
        // the specified refusal is an answering attach with the terminus
        // null followed by an immediate detach (Part 2 §2.6.3).
        server
            .answer_attach(
                4,
                Answer::to("pushed", 20, Role::Sender)
                    .source(&queue_source)
                    .target(&empty_target),
            )
            .await;

        let (_, bytes) = server.read_frame().await;
        let frame = frame::decode(&bytes, u32::MAX).unwrap();
        let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT).unwrap();
        match performative {
            Performative::Attach(attach) => {
                assert_eq!(attach.name, "pushed");
                assert_eq!(attach.role, Role::Receiver, "the opposite role");
                assert!(
                    attach.source.is_none() && attach.target.is_none(),
                    "both termini null: nothing was created"
                );
            }
            other => panic!("expected an answering attach, got {}", other.name()),
        }

        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "detach", "and MUST then immediately detach");
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let _session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_detach_we_send_is_answered_and_the_handle_comes_back() {
    let exec = Exec::current().unwrap();
    let (listener, port) = scripted().await;

    let server = tokio::spawn(async move {
        let mut server = accept(&listener).await;
        let empty_source = Source::default();
        let queue = Target::at("q");
        server.handshake().await;
        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "begin");
        server.answer_begin(4, 0, 255).await;

        let first = server.read_attach().await;
        assert_eq!(first.handle, 0);
        server
            .answer_attach(
                4,
                Answer::to(&first.name, 11, Role::Receiver)
                    .source(&empty_source)
                    .target(&queue),
            )
            .await;

        let (_, name, _) = server.read_named().await;
        assert_eq!(name, "detach");
        let mut answer = Detach::new(11);
        answer.closed = true;
        server.send(4, Performative::Detach(answer)).await;

        // A clean close frees the handle, unlike an errored one.
        let second = server.read_attach().await;
        assert_eq!(second.handle, 0, "a cleanly closed handle is reusable");
    });

    let connection = tokio::time::timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = tokio::time::timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut link = tokio::time::timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("orders", Target::at("q"))),
    )
    .await
    .unwrap()
    .expect("attached");

    tokio::time::timeout(DEADLINE, link.detach())
        .await
        .unwrap()
        .unwrap();
    match tokio::time::timeout(DEADLINE, link.next_event())
        .await
        .unwrap()
    {
        Some(LinkEvent::Detached(None)) => {}
        other => panic!("expected Detached(None), got {other:?}"),
    }
    // Detaching twice is not an error.
    link.detach().await.expect("idempotent");

    let second = session.attach(LinkOptions::sender("invoices", Target::at("q")));
    let _ = tokio::time::timeout(Duration::from_millis(500), second).await;
    tokio::time::timeout(DEADLINE, server)
        .await
        .unwrap()
        .unwrap();
}
