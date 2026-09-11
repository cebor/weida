//! ZMTP over a byte stream: the I/O the codec deliberately does not do.
//!
//! `weida-zmtp` is sans-I/O — it turns bytes into frames and back and nothing
//! else — so this is where a socket, a buffer and a handshake sequence live.
//! The split is the point: everything byte-exact is checked against 37/ZMTP in
//! a crate that cannot see weida, and everything here is plumbing that can be
//! read for correctness against that crate's types.

use std::collections::VecDeque;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use weida_zmtp::{Command, FrameKind, Greeting, Mechanism, Metadata, SocketType, frame, greeting};

use crate::error::BridgeError;

/// Read buffer growth step. A ZMTP frame header is at most nine octets and a
/// command body a few hundred, so the interesting case is a payload, which is
/// read straight into this buffer.
const CHUNK: usize = 16 * 1024;

/// One ZMTP message, in the frames it arrived as.
///
/// Kept as parts rather than concatenated: the number of frames is what
/// decides whether a message is acceptable at all (a pattern envelope is
/// consumed, a genuine multipart message is refused — loss L1 of
/// `docs/adapters/zmtp.md` §8), and a bridge that joined them first would have
/// thrown that away.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Message(pub Vec<Vec<u8>>);

/// What arrived on the connection.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Incoming {
    /// A message, one or more frames.
    Message(Message),
    /// A command: `SUBSCRIBE`, `CANCEL`, `PING`, anything the codec parses.
    Command(Vec<u8>),
}

/// A ZMTP connection: the socket, the inbound buffer and the caps.
pub(crate) struct Session<S> {
    io: S,
    /// Unparsed inbound bytes. Persisting across calls is half of what makes
    /// reading cancel-safe; the other half is that bytes are appended here
    /// only once a read has **completed** — see [`Session::fill`], where
    /// getting that wrong left zero-filled slack in the buffer that decoded as
    /// empty frames.
    buf: Vec<u8>,
    /// Where a read lands before it is appended. Reused, so a read costs no
    /// allocation, and separate from `buf` so that a cancelled read cannot
    /// change what is parsed.
    scratch: Vec<u8>,
    /// How many octets one whole message may occupy, summed over its frames.
    ///
    /// Per message rather than per frame, because what the bridge has to hold
    /// is the message: a ZeroMQ peer cannot be handed a body before it is
    /// complete (`docs/adapters/zmtp.md` §3), so this is the one bound that
    /// keeps a remote peer from choosing the allocation.
    max_message_bytes: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Session<S> {
    pub(crate) fn new(io: S, max_message_bytes: u64) -> Session<S> {
        Session {
            io,
            buf: Vec::new(),
            scratch: vec![0u8; CHUNK],
            max_message_bytes,
        }
    }

    /// Drives the greeting and the NULL handshake, and returns the peer's
    /// socket type.
    ///
    /// The order is the specification's: send the full greeting, read the
    /// peer's, then `READY` both ways. A peer that read a full greeting must
    /// send a full one, so nothing here waits for a partial one
    /// ([37/ZMTP](https://rfc.zeromq.org/spec/37/), "Version Negotiation").
    ///
    /// Two refusals happen here, both with `ERROR` before the close, which is
    /// what the specification asks for: a socket type that may not talk to
    /// `ours`, and a `READY` that names no socket type at all. The second is a
    /// `SHOULD` in the specification and a MUST for a bridge — it has to know
    /// which pattern it is translating before it forwards anything.
    pub(crate) async fn handshake(&mut self, ours: SocketType) -> Result<SocketType, BridgeError> {
        self.io.write_all(&Greeting::null().encode()).await?;
        self.io.flush().await?;

        let mut theirs = [0u8; greeting::GREETING_LEN];
        self.read_exactly(&mut theirs).await?;
        let peer = Greeting::decode(&theirs)?;
        peer.accept(Mechanism::NULL)?;

        let ready = Command::Ready(Metadata::new().with_socket_type(ours));
        self.io.write_all(&ready.encode()?).await?;
        self.io.flush().await?;

        let body = match self.read_next().await? {
            Incoming::Command(body) => body,
            // Nothing may precede the peer's READY; a message here is a
            // protocol violation rather than early data.
            Incoming::Message(_) => {
                return Err(BridgeError::Handshake(
                    "peer sent a message before its READY".into(),
                ));
            }
        };
        let metadata = match Command::decode(&body)? {
            Command::Ready(metadata) => metadata,
            Command::Error(reason) => {
                return Err(BridgeError::Handshake(format!(
                    "peer refused the handshake: {reason}"
                )));
            }
            other => {
                return Err(BridgeError::Handshake(format!(
                    "expected READY, got {}",
                    other.name()
                )));
            }
        };

        let Some(theirs) = metadata.socket_type() else {
            self.refuse("READY carries no Socket-Type this bridge can check")
                .await;
            return Err(BridgeError::Handshake(
                "peer announced no socket type".into(),
            ));
        };
        if !ours.accepts(theirs) {
            self.refuse(&format!(
                "{} may not talk to {}",
                theirs.as_str(),
                ours.as_str()
            ))
            .await;
            return Err(BridgeError::SocketType { ours, theirs });
        }
        Ok(theirs)
    }

    /// Writes an `ERROR` command and gives up on the connection.
    ///
    /// Best effort by construction: the peer is being closed on, so a write
    /// that fails changes nothing. "The peer SHALL treat an incoming ERROR
    /// command as fatal."
    async fn refuse(&mut self, reason: &str) {
        if let Ok(bytes) = Command::Error(reason).encode() {
            let _ = self.io.write_all(&bytes).await;
            let _ = self.io.flush().await;
        }
    }

    /// Reads the next message or command.
    ///
    /// Cancel-safe: the buffer survives, so a dropped call loses nothing but
    /// the in-flight `read`.
    pub(crate) async fn read_next(&mut self) -> Result<Incoming, BridgeError> {
        let mut parts: Vec<Vec<u8>> = Vec::new();
        let mut held: u64 = 0;
        loop {
            let (header, body) = self.read_frame(self.max_message_bytes - held).await?;
            match header.kind {
                FrameKind::Command => {
                    // "Commands always consist of one frame", so a command
                    // never joins a message being assembled.
                    if parts.is_empty() {
                        return Ok(Incoming::Command(body));
                    }
                    return Err(BridgeError::Protocol(
                        "a command frame arrived inside a multipart message".into(),
                    ));
                }
                FrameKind::Message { more } => {
                    held += body.len() as u64;
                    parts.push(body);
                    if !more {
                        return Ok(Incoming::Message(Message(parts)));
                    }
                }
            }
        }
    }

    /// Reads one frame, refusing a body larger than `budget` before it is
    /// read.
    async fn read_frame(
        &mut self,
        budget: u64,
    ) -> Result<(frame::FrameHeader, Vec<u8>), BridgeError> {
        loop {
            match frame::decode(&self.buf, budget) {
                Ok((header, body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok((header, body));
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Writes one message, one frame per part, MORE on all but the last.
    pub(crate) async fn write_message(&mut self, parts: &[&[u8]]) -> Result<(), BridgeError> {
        let Some((last, leading)) = parts.split_last() else {
            return Err(BridgeError::Protocol(
                "a ZMTP message has at least one frame".into(),
            ));
        };
        let mut out = Vec::new();
        for part in leading {
            out.extend_from_slice(&frame::encode(FrameKind::Message { more: true }, part));
        }
        out.extend_from_slice(&frame::encode(FrameKind::Message { more: false }, last));
        self.io.write_all(&out).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Writes one command.
    pub(crate) async fn write_command(&mut self, command: &Command<'_>) -> Result<(), BridgeError> {
        let bytes = command.encode()?;
        self.io.write_all(&bytes).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Reads exactly `out.len()` octets, using whatever is already buffered.
    async fn read_exactly(&mut self, out: &mut [u8]) -> Result<(), BridgeError> {
        while self.buf.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.buf[..out.len()]);
        self.buf.drain(..out.len());
        Ok(())
    }

    /// Reads more bytes, or reports the peer's orderly close.
    ///
    /// **Cancel-safe, and it has to be**: the PUB loop selects over this and
    /// the weida subscriber, so this future is dropped routinely. Reading into
    /// `scratch` and appending afterwards is what makes that safe —
    /// `AsyncReadExt::read` is itself cancel-safe, so a dropped call reads
    /// nothing and leaves `buf` exactly as it was. Growing `buf` first and
    /// truncating after the await is the version that looks equivalent and is
    /// not: a cancelled read leaves the slack behind, and a zero-filled buffer
    /// decodes as a stream of empty message frames.
    async fn fill(&mut self) -> Result<(), BridgeError> {
        let n = self.io.read(&mut self.scratch).await?;
        if n == 0 {
            return Err(BridgeError::PeerClosed);
        }
        self.buf.extend_from_slice(&self.scratch[..n]);
        Ok(())
    }
}

/// Answers a command that needs no pattern knowledge: `PING` gets its `PONG`,
/// `ERROR` is fatal, everything else is noted and ignored.
///
/// Shared by both directions, because the rules are the peer's, not the
/// bridge's: "when a peer receives a PING command it SHALL respond with a PONG
/// command that echoes the ping-context", and "the peer SHALL treat an
/// incoming ERROR command as fatal".
pub(crate) async fn answer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut Session<S>,
    command: Command<'_>,
) -> Result<(), BridgeError> {
    match command {
        Command::Ping { context, .. } => session.write_command(&Command::Pong { context }).await,
        Command::Error(reason) => Err(BridgeError::Protocol(format!(
            "peer sent ERROR, which is fatal: {reason}"
        ))),
        other => {
            tracing::debug!(
                command = other.name(),
                "ignoring a command with no use here"
            );
            Ok(())
        }
    }
}

/// Decodes a command body and answers it.
pub(crate) async fn answer_command<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut Session<S>,
    body: &[u8],
) -> Result<(), BridgeError> {
    let command = Command::decode(body)?;
    answer(session, command).await
}

/// Makes a reason printable, since `ERROR` carries printable ASCII only and at
/// most 255 octets of it — and the reasons here quote what a peer sent.
pub(crate) fn sanitize(reason: &str) -> String {
    let mut out: String = reason
        .chars()
        .map(|c| if (' '..='~').contains(&c) { c } else { '?' })
        .collect();
    out.truncate(255);
    out
}

/// Queue of ZMTP messages waiting for a peer that is not reading.
///
/// Bounded, and the bound is the same one every other queue in this bridge
/// obeys: nothing a remote peer sends or fails to read may grow a structure
/// without a ceiling. It exists because a PUB-side peer that stops reading
/// must not stall the weida subscriber feeding it — which is exactly ZeroMQ's
/// own rule for PUB, "SHALL silently drop the message if the queue for a
/// subscriber is full" [zeromq §4.3].
pub(crate) struct DropQueue {
    queue: VecDeque<Vec<u8>>,
    max_messages: usize,
    dropped: u64,
}

impl DropQueue {
    pub(crate) fn new(max_messages: usize) -> DropQueue {
        DropQueue {
            queue: VecDeque::new(),
            max_messages,
            dropped: 0,
        }
    }

    /// Enqueues, dropping the **oldest** at the bound, and reports whether it
    /// had to.
    ///
    /// Oldest rather than newest: a subscriber that has fallen behind wants
    /// the freshest data it can still be given, which is the choice ZeroMQ's
    /// PUB makes by dropping what it cannot queue. The return value exists so
    /// that the drop is *counted and logged* rather than silent — ZeroMQ's PUB
    /// drops silently and the zguide names that as a debugging problem, while
    /// weida counts its fan-out drops [GUARANTEES §6].
    pub(crate) fn push(&mut self, message: Vec<u8>) -> bool {
        let dropped = self.queue.len() >= self.max_messages;
        if dropped {
            self.queue.pop_front();
            self.dropped += 1;
        }
        self.queue.push_back(message);
        dropped
    }

    pub(crate) fn pop(&mut self) -> Option<Vec<u8>> {
        self.queue.pop_front()
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped
    }
}
