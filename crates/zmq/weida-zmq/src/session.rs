//! ZMTP over one connection: the greeting, the NULL handshake, the framing
//! and the heartbeat.
//!
//! This is the [`Session`] the connection engine hands every established
//! connection to. It is the same code for every socket type, because the
//! pattern is a *property* in `READY` and not a different wire format
//! (`docs/research/zeromq.md` §3) — what differs per socket type is which
//! peer types are legal opposite it, and that table lives in the codec.
//!
//! **Nothing byte-exact is written here.** `weida-zmtp` owns the greeting,
//! the flags octet, the size field, the command bodies and the property
//! dictionary, and its `[dependencies]` is empty so that it can be checked
//! against 37/ZMTP rather than against our reading of it
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3). This
//! module is I/O, sequencing and policy: which command answers which, when a
//! `PING` may be sent, and what a mismatch does.
//!
//! **The 3.0 downgrade is load-bearing.** 37/ZMTP permits a peer to
//! "downgrade to a lower protocol version", and the interop bench showed why
//! it must: the pure-Rust `zeromq` crate announces ZMTP 3.0 and answers any
//! command but `READY` with "Unknown command received" and a close
//! (`docs/research/zeromq.md` §13). So a 3.0 peer is accepted, and it gets no
//! `PING` — the version decides whether the heartbeat can be honoured, and a
//! suppressed heartbeat says so once rather than leaving a silently different
//! behaviour to be found in a packet capture.
//!
//! The behaviour here is ported from `crates/adapters/weida-zmtp-bridge`'s
//! `wire.rs` and `Liveness`, which is where it was first proved against real
//! ZeroMQ peers. The bridge keeps running on its own copy until B-094 rebuilds
//! it on this crate; nothing here touches it.

use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use weida_runtime::Exec;
use weida_zmtp::{
    Command, CommandError, Greeting, GreetingError, Mechanism, Metadata, SocketType, Version,
    greeting,
};

use crate::engine::{Connection, Session, SessionFuture};
use crate::error::{Error, Result};
use crate::identity::RoutingId;
use crate::message::{Decoded, Message, MessageLimits, Multipart};
use crate::options::SocketOptions;
use crate::pipe::{Queue, Sent};

/// Read buffer growth step. A frame header is at most nine octets and a
/// command body a few hundred, so the interesting case is a payload, which is
/// read straight into this buffer.
const CHUNK: usize = 16 * 1024;

/// The context this library puts in its `PING`s. Opaque by specification —
/// the `PONG` echoes it and nothing interprets it.
const PING_CONTEXT: &[u8] = b"weida-zmq";

/// The ZMTP session every socket type of this crate uses.
///
/// Constructed with the socket type it announces, because that is the one
/// thing about the handshake that differs per pattern: `Socket-Type` in our
/// `READY`, and the compatibility check against the peer's.
#[derive(Clone, Copy, Debug)]
pub struct ZmtpSession {
    socket_type: SocketType,
}

impl ZmtpSession {
    /// A session that announces `socket_type`.
    pub const fn new(socket_type: SocketType) -> ZmtpSession {
        ZmtpSession { socket_type }
    }

    /// The socket type this session announces.
    pub const fn socket_type(&self) -> SocketType {
        self.socket_type
    }
}

impl Session for ZmtpSession {
    fn run(&self, connection: Connection) -> SessionFuture {
        let ours = self.socket_type;
        Box::pin(async move { drive(ours, connection).await })
    }
}

/// What the handshake agreed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Negotiated {
    /// The peer's socket type, from its `READY`.
    pub peer_type: SocketType,
    /// The version to speak: ours, or the peer's if it is lower and still
    /// 3.x. `PING`/`PONG` are gated on it.
    pub version: Version,
    /// The peer's `Identity` property, if it announced one. Self-asserted —
    /// see [`RoutingId`] — and what a ROUTER keys its queue by, which is that
    /// socket type's slice rather than this one.
    pub identity: Option<RoutingId>,
}

async fn drive(ours: SocketType, connection: Connection) -> Result<()> {
    let Connection {
        stream,
        pipe,
        peer,
        endpoint,
        options,
        exec,
        mut handshake,
        identity,
        role: _,
    } = connection;

    let mut wire = Wire::new(stream, options.message_limits());
    let negotiated = handshake_on(&mut wire, ours, &options).await?;
    // The engine's ZMQ_HANDSHAKE_IVL stops counting here.
    handshake.complete();
    // A ROUTER addresses this peer by what it just announced — and needs to
    // know that the handshake has happened at all, because a peer exists
    // before its READY is read.
    identity.announce(negotiated.identity.clone());
    tracing::debug!(
        %peer,
        endpoint = %endpoint,
        ours = ours.as_str(),
        theirs = negotiated.peer_type.as_str(),
        version = %negotiated.version,
        identity = ?negotiated.identity,
        "ZMTP handshake complete"
    );

    if options.probe_router {
        // ZMQ_PROBE_ROUTER: "send an empty message on every new connection",
        // so the peer's ROUTER learns this peer exists before it has
        // anything to say. Sent here rather than by the socket, because this
        // is where "a new connection" happens.
        wire.write_message(&Multipart::single(Message::empty()))
            .await?;
    }

    pump(&mut wire, &pipe, &options, &exec, negotiated.version).await
}

/// Drives the greeting and the NULL handshake.
///
/// The order is the specification's: send the full greeting, read the peer's,
/// then `READY` both ways. Two refusals happen here, both with an `ERROR`
/// before the close, which is what 37/ZMTP asks for: a socket type that may
/// not talk to `ours`, and a `READY` that names no socket type at all — the
/// second is a `SHOULD` in the specification and a MUST for an
/// implementation, which cannot check compatibility against a peer that will
/// not say what it is.
pub async fn handshake_on<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    ours: SocketType,
    options: &SocketOptions,
) -> Result<Negotiated> {
    wire.write_all(&Greeting::null().encode()).await?;

    let mut theirs = [0u8; greeting::GREETING_LEN];
    wire.read_exactly(&mut theirs).await?;
    let peer = Greeting::decode(&theirs).map_err(greeting_error)?;
    let version = peer
        .accept_downgrading(Mechanism::NULL)
        .map_err(greeting_error)?;

    let mut metadata = Metadata::new().with_socket_type(ours);
    if let Some(routing_id) = &options.routing_id {
        // libzmq deprecates the name `ZMQ_IDENTITY` for `ZMQ_ROUTING_ID`, but
        // the wire property is still `Identity` and a peer that reads the
        // other name would not find it.
        metadata = metadata.with("Identity", routing_id.as_bytes());
    }
    wire.write_command(&Command::Ready(metadata)).await?;

    let body = match wire.read_next().await? {
        Incoming::Command(body) => body,
        // Nothing may precede the peer's READY; a message here is a protocol
        // violation rather than early data.
        Incoming::Message(_) => {
            return Err(Error::ENOCOMPATPROTO(
                "the peer sent a message before its READY".into(),
            ));
        }
    };
    let metadata = match Command::decode(&body).map_err(command_error)? {
        Command::Ready(metadata) => metadata,
        // "The peer SHALL treat an incoming ERROR command as fatal."
        Command::Error(reason) => {
            return Err(Error::ENOCOMPATPROTO(
                format!("the peer refused the handshake: {}", sanitize(reason)).into(),
            ));
        }
        other => {
            return Err(Error::ENOCOMPATPROTO(
                format!("expected READY, got {}", other.name()).into(),
            ));
        }
    };

    let identity = match metadata.get("Identity") {
        Some(bytes) => Some(RoutingId::new(bytes)?),
        None => None,
    };

    let Some(peer_type) = metadata.socket_type() else {
        wire.refuse("READY carries no Socket-Type").await;
        return Err(Error::ENOCOMPATPROTO(
            "the peer announced no socket type, so no compatibility check is possible".into(),
        ));
    };
    if !ours.accepts(peer_type) {
        wire.refuse(&format!(
            "{} may not talk to {}",
            peer_type.as_str(),
            ours.as_str()
        ))
        .await;
        return Err(Error::ENOCOMPATPROTO(
            format!(
                "a {} peer may not talk to this {}",
                peer_type.as_str(),
                ours.as_str()
            )
            .into(),
        ));
    }

    Ok(Negotiated {
        peer_type,
        version,
        identity,
    })
}

/// Moves messages both ways until the connection or the pipe ends.
///
/// Three things happen in one loop, and they are one loop on purpose: reading
/// the wire, draining this peer's outgoing queue onto it, and beating the
/// heartbeat. A separate reader and writer task would need a lock over the
/// same socket and would not make either direction faster.
///
/// **Backpressure is the receive queue not being drained.** When this peer's
/// incoming queue is full and its socket type blocks, the `send` below waits,
/// which stops this loop reading — which is how a PULL slows a PUSH down.
/// When the socket type drops instead (SUB, XSUB per 29/PUBSUB), the message
/// is discarded and counted, and reading continues.
async fn pump<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
    pipe: &crate::pipe::Pipe,
    options: &SocketOptions,
    exec: &Exec,
    version: Version,
) -> Result<()> {
    let outgoing: std::sync::Arc<Queue> = pipe.outgoing();
    let incoming: std::sync::Arc<Queue> = pipe.incoming();
    let mut liveness = Liveness::new(options, version);

    loop {
        tokio::select! {
            arrived = wire.read_next() => {
                liveness.saw_traffic();
                match arrived? {
                    Incoming::Message(message) => {
                        if incoming.send(message).await? == Sent::Dropped {
                            tracing::trace!("dropped an inbound message: the queue is at its high-water mark");
                        }
                    }
                    Incoming::Command(body) => answer(wire, &body).await?,
                }
            }
            queued = outgoing.recv() => match queued {
                Ok(message) => wire.write_message(&message).await?,
                // The pipe was destroyed: the socket closed or disconnected
                // this endpoint, and this connection has nothing left to do.
                Err(_) => return Ok(()),
            },
            () = liveness.tick(exec) => liveness.beat(wire).await?,
        }
    }
}

/// Answers a command that needs no pattern knowledge.
///
/// `PING` gets its `PONG` echoing the context, `ERROR` is fatal, and anything
/// else is noted and ignored — which is what a peer must do with a command it
/// has no use for. `SUBSCRIBE`/`CANCEL` are such commands *here*: they belong
/// to SUB, XPUB and XSUB, and the slice that implements those socket types is
/// where they are applied. Ignoring them in a REQ or a PUSH is not a loss; it
/// is the only correct answer.
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(wire: &mut Wire<S>, body: &[u8]) -> Result<()> {
    match Command::decode(body).map_err(command_error)? {
        Command::Ping { context, .. } => {
            // "When a peer receives a PING command it SHALL respond with a
            // PONG command that echoes the ping-context."
            wire.write_command(&Command::Pong { context }).await
        }
        Command::Error(reason) => Err(Error::ENOCOMPATPROTO(
            format!("the peer sent ERROR: {}", sanitize(reason)).into(),
        )),
        other => {
            tracing::debug!(
                command = other.name(),
                "ignoring a command this socket has no use for"
            );
            Ok(())
        }
    }
}

/// The heartbeat: `PING` on a timer, and silence declared fatal.
///
/// `PING`/`PONG` are 3.1 commands, so a connection that negotiated 3.0 gets
/// no heartbeat at all whatever `ZMQ_HEARTBEAT_IVL` says. That is not
/// caution: sending a command a 3.0 peer does not know is a protocol
/// violation, and the interop tests show what it costs.
struct Liveness {
    interval: Option<Duration>,
    timeout: Duration,
    ttl_deciseconds: u16,
    last: Instant,
}

impl Liveness {
    fn new(options: &SocketOptions, version: Version) -> Liveness {
        let interval = match options.heartbeat_ivl {
            Some(interval) if version >= greeting::VERSION => Some(interval),
            Some(_) => {
                tracing::info!(
                    %version,
                    "the peer speaks ZMTP 3.0, which has no PING: the heartbeat is off for \
                     this connection and liveness is the transport's business"
                );
                None
            }
            None => None,
        };
        // ZMQ_HEARTBEAT_TIMEOUT of 0 means ZMQ_HEARTBEAT_IVL.
        let timeout = options
            .heartbeat_timeout
            .or(interval)
            .unwrap_or(Duration::ZERO);
        let ttl_deciseconds = options
            .heartbeat_ttl
            .map(|ttl| u16::try_from(ttl.as_millis() / 100).unwrap_or(u16::MAX))
            .unwrap_or(0);
        Liveness {
            interval,
            timeout,
            ttl_deciseconds,
            last: Instant::now(),
        }
    }

    /// "A peer SHOULD treat any incoming traffic (not just a PONG reply) as a
    /// sign of life."
    fn saw_traffic(&mut self) {
        self.last = Instant::now();
    }

    /// Sleeps until the next beat is due, or forever when the heartbeat is
    /// off. Cancel-safe, because it holds no state: the deadline is
    /// recomputed each time.
    async fn tick(&self, exec: &Exec) {
        match self.interval {
            Some(interval) => exec.sleep(interval).await,
            None => std::future::pending().await,
        }
    }

    /// Sends one `PING`, and reports the peer dead when it has been silent
    /// past `ZMQ_HEARTBEAT_TIMEOUT`.
    async fn beat<S: AsyncRead + AsyncWrite + Unpin>(&mut self, wire: &mut Wire<S>) -> Result<()> {
        if self.interval.is_none() {
            return Ok(());
        }
        if !self.timeout.is_zero() && self.last.elapsed() >= self.timeout {
            return Err(Error::ETIMEDOUT(
                format!(
                    "the peer has been silent for {:?} (ZMQ_HEARTBEAT_TIMEOUT)",
                    self.last.elapsed()
                )
                .into(),
            ));
        }
        wire.write_command(&Command::Ping {
            ttl: self.ttl_deciseconds,
            context: PING_CONTEXT,
        })
        .await
    }
}

/// What arrived on the connection.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// A whole message, every frame of it.
    Message(Multipart),
    /// A command frame's body, still encoded: `READY`, `PING`, `SUBSCRIBE`
    /// and the rest. "Commands always consist of one frame."
    Command(Vec<u8>),
}

/// ZMTP over a byte stream: the I/O the codec deliberately does not do.
///
/// Public because the socket types of the later slices drive it, and because
/// it is what makes the session testable against a raw stream.
#[derive(Debug)]
pub struct Wire<S> {
    io: S,
    /// Unparsed inbound bytes. Persisting across calls is half of what makes
    /// reading cancel-safe; the other half is that bytes are appended only
    /// once a read has **completed** — see [`Wire::fill`].
    buf: Vec<u8>,
    /// Where a read lands before it is appended. Reused, so a read costs no
    /// allocation, and separate from `buf` so that a cancelled read cannot
    /// change what is parsed.
    scratch: Vec<u8>,
    limits: MessageLimits,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Wire<S> {
    /// Wraps `io`, bounding every inbound message by `limits`:
    /// `ZMQ_MAXMSGSIZE` in octets — frame headers included — and a ceiling on
    /// the frame count. Both are needed; see [`Multipart::decode`].
    pub fn new(io: S, limits: MessageLimits) -> Wire<S> {
        Wire {
            io,
            buf: Vec::new(),
            scratch: vec![0u8; CHUNK],
            limits,
        }
    }

    /// Reads the next whole message or command.
    ///
    /// Cancel-safe: the buffer survives, so a dropped call loses nothing but
    /// the in-flight `read` — which the session's own `select!` relies on,
    /// because it drops this future on every message it writes.
    ///
    /// The parsing is [`Multipart::decode`]'s, so `ZMQ_MAXMSGSIZE` is judged
    /// from declared lengths and a multipart message is delivered only once
    /// its last frame has arrived.
    pub async fn read_next(&mut self) -> Result<Incoming> {
        loop {
            match Multipart::decode(&self.buf, self.limits)? {
                Decoded::Message { message, consumed } => {
                    self.buf.drain(..consumed);
                    return Ok(Incoming::Message(message));
                }
                Decoded::Command { body, consumed } => {
                    let body = body.to_vec();
                    self.buf.drain(..consumed);
                    return Ok(Incoming::Command(body));
                }
                Decoded::Incomplete => self.fill().await?,
            }
        }
    }

    /// Writes one whole message: every frame in one buffer, MORE on all but
    /// the last.
    ///
    /// One write, because "on sending, the peer SHALL queue all frames of a
    /// message in memory until the final frame is sent" — a peer must never
    /// see half a message.
    pub async fn write_message(&mut self, message: &Multipart) -> Result<()> {
        self.write_all(&message.encode()).await
    }

    /// Writes one command frame.
    pub async fn write_command(&mut self, command: &Command<'_>) -> Result<()> {
        let bytes = command.encode().map_err(command_error)?;
        self.write_all(&bytes).await
    }

    /// Reads exactly `out.len()` octets — a peer's greeting, in a test that
    /// drives the other side of a connection.
    #[cfg(test)]
    pub(crate) async fn read_exactly_for_test(&mut self, out: &mut [u8]) {
        self.read_exactly(out).await.expect("the peer's greeting");
    }

    /// Writes bytes with no framing — a greeting, in the same tests.
    #[cfg(test)]
    pub(crate) async fn write_raw_for_test(&mut self, bytes: &[u8]) {
        self.write_all(bytes).await.expect("write");
    }

    /// Writes an `ERROR` and gives up on the connection.
    ///
    /// Best effort by construction: the peer is being closed on, so a write
    /// that fails changes nothing.
    async fn refuse(&mut self, reason: &str) {
        if let Ok(bytes) = Command::Error(reason).encode() {
            let _ = self.io.write_all(&bytes).await;
            let _ = self.io.flush().await;
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.io.write_all(bytes).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Reads exactly `out.len()` octets, using whatever is already buffered.
    async fn read_exactly(&mut self, out: &mut [u8]) -> Result<()> {
        while self.buf.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.buf[..out.len()]);
        self.buf.drain(..out.len());
        Ok(())
    }

    /// Reads more bytes, or reports the peer's close.
    ///
    /// Reading into `scratch` and appending afterwards is what makes this
    /// cancel-safe: `AsyncReadExt::read` is cancel-safe, so a dropped call
    /// reads nothing and leaves `buf` exactly as it was. Growing `buf` first
    /// and truncating after the await is the version that looks equivalent
    /// and is not — a cancelled read leaves the slack behind, and a
    /// zero-filled buffer decodes as a stream of empty message frames.
    async fn fill(&mut self) -> Result<()> {
        let read = self.io.read(&mut self.scratch).await?;
        if read == 0 {
            return Err(Error::EHOSTUNREACH("the peer closed the connection".into()));
        }
        self.buf.extend_from_slice(&self.scratch[..read]);
        Ok(())
    }
}

/// Makes a reason printable, since `ERROR` carries printable ASCII only and
/// the reasons here quote what a peer sent.
fn sanitize(reason: &str) -> String {
    reason
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .take(255)
        .collect()
}

fn greeting_error(error: GreetingError) -> Error {
    Error::ENOCOMPATPROTO(format!("greeting refused: {error}").into())
}

fn command_error(error: CommandError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed command: {error}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::endpoint::Endpoint;
    use crate::engine::{Engine, HandshakeGate, PeerId, Role};
    use crate::message::Message;
    use crate::pipe::Pipe;
    use crate::transport::Stream;
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};
    use weida_zmtp::{FrameKind, frame};

    /// A peer driven directly by the codec: the only honest way to check a
    /// protocol implementation is against the specification's own bytes.
    struct CodecPeer {
        io: TcpStream,
        buf: Vec<u8>,
    }

    impl CodecPeer {
        fn new(io: TcpStream) -> CodecPeer {
            CodecPeer {
                io,
                buf: Vec::new(),
            }
        }

        async fn read_greeting(&mut self) -> [u8; greeting::GREETING_LEN] {
            let mut out = [0u8; greeting::GREETING_LEN];
            self.read_exactly(&mut out).await;
            out
        }

        async fn read_exactly(&mut self, out: &mut [u8]) {
            while self.buf.len() < out.len() {
                let mut chunk = [0u8; 4096];
                let read = self.io.read(&mut chunk).await.expect("read");
                assert_ne!(read, 0, "the peer closed while we waited");
                self.buf.extend_from_slice(&chunk[..read]);
            }
            out.copy_from_slice(&self.buf[..out.len()]);
            self.buf.drain(..out.len());
        }

        /// The next frame, header and body, decoded by the codec.
        async fn read_frame(&mut self) -> (FrameKind, Vec<u8>) {
            loop {
                match frame::decode(&self.buf, 1 << 20) {
                    Ok((header, body, used)) => {
                        let body = body.to_vec();
                        self.buf.drain(..used);
                        return (header.kind, body);
                    }
                    Err(e) if !e.is_violation() => {
                        let mut chunk = [0u8; 4096];
                        let read = self.io.read(&mut chunk).await.expect("read");
                        assert_ne!(read, 0, "the peer closed while we waited for a frame");
                        self.buf.extend_from_slice(&chunk[..read]);
                    }
                    Err(e) => panic!("the session sent something malformed: {e}"),
                }
            }
        }

        async fn send(&mut self, bytes: &[u8]) {
            self.io.write_all(bytes).await.expect("write");
            self.io.flush().await.expect("flush");
        }

        async fn greet(&mut self, version: Version) {
            let greeting = Greeting {
                version,
                ..Greeting::null()
            };
            self.send(&greeting.encode()).await;
        }

        async fn ready(&mut self, socket_type: SocketType) {
            let ready = Command::Ready(Metadata::new().with_socket_type(socket_type))
                .encode()
                .expect("READY");
            self.send(&ready).await;
        }
    }

    /// Runs a session over one end of a connected pair, with no engine, and
    /// hands back the pipe so a test can see both directions.
    fn drive_session(
        stream: TcpStream,
        ours: SocketType,
        options: SocketOptions,
    ) -> (Pipe, tokio::task::JoinHandle<Result<()>>) {
        let pipe = Pipe::new(options.pipe);
        let connection = Connection {
            stream: Stream::tcp(stream),
            pipe: pipe.clone(),
            peer: PeerId::detached(),
            role: Role::Connecter,
            endpoint: Endpoint::parse("tcp://127.0.0.1:1").expect("endpoint"),
            options,
            exec: Exec::current().expect("ambient reactor"),
            handshake: HandshakeGate::detached(),
            identity: crate::engine::AnnouncedIdentity::default(),
        };
        let session = ZmtpSession::new(ours);
        let task = tokio::spawn(async move { session.run(connection).await });
        (pipe, task)
    }

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("addr");
        let dialling = tokio::spawn(async move { TcpStream::connect(addr).await });
        let (accepted, _) = listener.accept().await.expect("accept");
        let dialled = dialling.await.expect("task").expect("connect");
        (dialled, accepted)
    }

    fn options() -> SocketOptions {
        SocketOptions::default()
    }

    /// Claim: the greeting we send is the specification's 64 octets, exactly
    /// as the codec encodes them, and our `READY` carries `Socket-Type` and —
    /// when `ZMQ_ROUTING_ID` is set — `Identity`.
    #[tokio::test]
    async fn the_handshake_sends_the_greeting_and_a_ready() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(
            ours,
            SocketType::Req,
            SocketOptions {
                routing_id: Some(RoutingId::new(b"client-7").expect("id")),
                ..options()
            },
        );

        // Byte-for-byte: what the codec says a NULL 3.1 greeting is.
        assert_eq!(peer.read_greeting().await, Greeting::null().encode());
        peer.greet(greeting::VERSION).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        let metadata = match Command::decode(&body).expect("a command") {
            Command::Ready(metadata) => metadata,
            other => panic!("expected READY, got {}", other.name()),
        };
        assert_eq!(metadata.socket_type(), Some(SocketType::Req));
        assert_eq!(metadata.get("Identity"), Some(b"client-7".as_slice()));

        peer.ready(SocketType::Rep).await;
        drop(peer);
        // The peer's close ends the session, which is not an error worth
        // asserting beyond its shape.
        let _ = task.await.expect("the session task");
    }

    /// Claim: messages cross in both directions, multipart intact, with MORE
    /// on every frame but the last.
    #[tokio::test]
    async fn messages_cross_in_both_directions() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (pipe, task) = drive_session(ours, SocketType::Dealer, options());

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Router).await;

        // Outbound: a two-frame message goes out as MORE then last.
        let envelope =
            Multipart::new(vec![Message::empty(), Message::from("hello")]).expect("frames");
        pipe.outgoing()
            .send(envelope.clone())
            .await
            .expect("queued");
        let (first, body) = peer.read_frame().await;
        assert_eq!(first, FrameKind::Message { more: true });
        assert!(body.is_empty(), "the delimiter frame is empty");
        let (second, body) = peer.read_frame().await;
        assert_eq!(second, FrameKind::Message { more: false });
        assert_eq!(body, b"hello");

        // Inbound: the same shape arrives as one Multipart.
        peer.send(&envelope.encode()).await;
        let arrived = tokio::time::timeout(Duration::from_secs(5), pipe.incoming().recv())
            .await
            .expect("delivered")
            .expect("a message");
        assert_eq!(arrived, envelope);

        drop(peer);
        let _ = task.await.expect("the session task");
    }

    /// Claim: a ZMTP 3.0 peer is accepted by downgrading, and **never
    /// receives a PING** even with the heartbeat configured — the interop
    /// lesson, where a command a 3.0 peer does not know ends the connection.
    #[tokio::test]
    async fn a_three_zero_peer_is_accepted_and_never_pinged() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (pipe, task) = drive_session(
            ours,
            SocketType::Push,
            SocketOptions {
                heartbeat_ivl: Some(Duration::from_millis(10)),
                ..options()
            },
        );

        peer.read_greeting().await;
        peer.greet(Version { major: 3, minor: 0 }).await;
        let (kind, _) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command, "READY is still a command");
        peer.ready(SocketType::Pull).await;

        // Margin: fifteen heartbeat intervals (150 ms over a 10 ms
        // ZMQ_HEARTBEAT_IVL). The phenomenon is a PING that must never come,
        // so the interval count is the margin: a session that sent one would
        // have sent fifteen. The message afterwards proves the silence is the
        // version's doing rather than a stalled session.
        tokio::time::sleep(Duration::from_millis(150)).await;
        pipe.outgoing()
            .send(Multipart::single("still alive"))
            .await
            .expect("queued");
        let (kind, body) = peer.read_frame().await;
        assert_eq!(
            kind,
            FrameKind::Message { more: false },
            "a 3.0 peer must receive no PING"
        );
        assert_eq!(body, b"still alive");

        drop(peer);
        let _ = task.await.expect("the session task");
    }

    /// Claim: a 3.1 peer does get `PING`s on `ZMQ_HEARTBEAT_IVL`, carrying
    /// the `ZMQ_HEARTBEAT_TTL` hint in deciseconds.
    ///
    /// Margin: none is needed in the assertion — the read below waits for
    /// the PING rather than sampling for it, so a 10 ms interval only sets
    /// how soon the test finishes, and a heartbeat that never came would
    /// hang the read until the harness kills it rather than pass.
    #[tokio::test]
    async fn a_three_one_peer_is_pinged_on_the_interval() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(
            ours,
            SocketType::Push,
            SocketOptions {
                heartbeat_ivl: Some(Duration::from_millis(10)),
                heartbeat_timeout: Some(Duration::from_secs(30)),
                heartbeat_ttl: Some(Duration::from_secs(3)),
                ..options()
            },
        );

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Pull).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        match Command::decode(&body).expect("a command") {
            Command::Ping { ttl, context } => {
                assert_eq!(ttl, 30, "3 s is 30 deciseconds on the wire");
                assert_eq!(context, PING_CONTEXT);
            }
            other => panic!("expected PING, got {}", other.name()),
        }

        drop(peer);
        let _ = task.await.expect("the session task");
    }

    /// Claim: our session answers a peer's `PING` with a `PONG` that echoes
    /// the context, whatever else is going on.
    #[tokio::test]
    async fn a_ping_is_answered_with_a_pong_that_echoes() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(ours, SocketType::Rep, options());

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Req).await;

        let ping = Command::Ping {
            ttl: 100,
            context: b"abcd",
        }
        .encode()
        .expect("PING");
        peer.send(&ping).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        match Command::decode(&body).expect("a command") {
            Command::Pong { context } => assert_eq!(context, b"abcd"),
            other => panic!("expected PONG, got {}", other.name()),
        }

        drop(peer);
        let _ = task.await.expect("the session task");
    }

    /// Claim: an incompatible socket type is refused with an `ERROR` on the
    /// wire and then a close — the specification's own way of saying no — and
    /// the session reports `ENOCOMPATPROTO`.
    #[tokio::test]
    async fn an_incompatible_socket_type_gets_an_error_then_a_close() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(ours, SocketType::Req, options());

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        // A PUB may not talk to a REQ.
        peer.ready(SocketType::Pub).await;

        let (kind, body) = peer.read_frame().await;
        assert_eq!(kind, FrameKind::Command);
        match Command::decode(&body).expect("a command") {
            Command::Error(reason) => {
                assert!(reason.contains("PUB"), "{reason}");
                assert!(reason.contains("REQ"), "{reason}");
            }
            other => panic!("expected ERROR, got {}", other.name()),
        }

        let err = task.await.expect("the session task").unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
    }

    /// Claim: a `READY` with no `Socket-Type` is refused, because no
    /// compatibility check is possible against a peer that will not say what
    /// it is.
    #[tokio::test]
    async fn a_ready_without_a_socket_type_is_refused() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(ours, SocketType::Pull, options());

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.send(&Command::Ready(Metadata::new()).encode().expect("READY"))
            .await;

        let (_, body) = peer.read_frame().await;
        assert!(matches!(
            Command::decode(&body).expect("a command"),
            Command::Error(_)
        ));
        let err = task.await.expect("the session task").unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
    }

    /// Claim: an `ERROR` from the peer is fatal, at the handshake and after
    /// it — "the peer SHALL treat an incoming ERROR command as fatal".
    #[tokio::test]
    async fn an_error_from_the_peer_is_fatal() {
        for during_handshake in [true, false] {
            let (ours, theirs) = pair().await;
            let mut peer = CodecPeer::new(theirs);
            let (_pipe, task) = drive_session(ours, SocketType::Pull, options());

            peer.read_greeting().await;
            peer.greet(greeting::VERSION).await;
            peer.read_frame().await;
            if !during_handshake {
                peer.ready(SocketType::Push).await;
            }
            peer.send(&Command::Error("go away").encode().expect("ERROR"))
                .await;

            let err = task.await.expect("the session task").unwrap_err();
            assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
            assert!(err.cause().contains("go away"), "{err}");
        }
    }

    /// Claim: `ZMQ_MAXMSGSIZE` is enforced on the wire from the declared
    /// length, so a peer cannot make us allocate by announcing a huge frame.
    #[tokio::test]
    async fn an_oversized_declaration_ends_the_connection() {
        let (ours, theirs) = pair().await;
        let mut peer = CodecPeer::new(theirs);
        let (_pipe, task) = drive_session(
            ours,
            SocketType::Pull,
            SocketOptions {
                max_message_size: 1024,
                ..options()
            },
        );

        peer.read_greeting().await;
        peer.greet(greeting::VERSION).await;
        peer.read_frame().await;
        peer.ready(SocketType::Push).await;

        // A long header declaring 2^31 octets, and no body at all.
        let mut header = vec![0x02u8];
        header.extend_from_slice(&(1u64 << 31).to_be_bytes());
        peer.send(&header).await;

        let err = task.await.expect("the session task").unwrap_err();
        assert_eq!(err.errno(), "EMSGSIZE", "{err}");
    }

    /// Claim: the session is the seam the engine expects — an `Engine`
    /// carrying `ZmtpSession` completes the handshake against a real ZeroMQ
    /// peer driven by the codec, and messages then flow through the engine's
    /// pipe.
    #[tokio::test]
    async fn the_engine_and_the_session_speak_zmtp_end_to_end() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let engine = Engine::new(
            &ctx,
            options(),
            Arc::new(ZmtpSession::new(SocketType::Push)) as Arc<dyn Session>,
        )
        .expect("engine");

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let peer_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut peer = CodecPeer::new(stream);
            assert_eq!(peer.read_greeting().await, Greeting::null().encode());
            peer.greet(greeting::VERSION).await;
            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Command);
            match Command::decode(&body).expect("a command") {
                Command::Ready(metadata) => {
                    assert_eq!(metadata.socket_type(), Some(SocketType::Push));
                }
                other => panic!("expected READY, got {}", other.name()),
            }
            peer.ready(SocketType::Pull).await;
            let (kind, body) = peer.read_frame().await;
            assert_eq!(kind, FrameKind::Message { more: false });
            body
        });

        let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        let peer_id = engine.connect(&endpoint).expect("connect");
        let pipe = engine.peer(peer_id).expect("peer").pipe;
        pipe.outgoing()
            .send(Multipart::single("through the engine"))
            .await
            .expect("queued");

        let delivered = tokio::time::timeout(Duration::from_secs(10), peer_task)
            .await
            .expect("the peer finished")
            .expect("the peer task");
        assert_eq!(delivered, b"through the engine");
    }
}
