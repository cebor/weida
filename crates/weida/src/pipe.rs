//! The named-pipe transport: `weida+pipe://<name>/<path>`.
//!
//! The third local transport of
//! [decision 0010](../../../docs/decisions/0010-local-transport.md) §4.1: a
//! pipe under `\\.\pipe\`, one connection per transfer, and the peer proved
//! by the client's token SID on the accepting side and by the pipe's owner
//! SID on the dialling side [0010 §4.4, §4.5]. The grouping of connections
//! into peers is [`crate::grouped`]; this module is what a pipe contributes
//! to it, and one thing a pipe lacks.
//!
//! **A pipe has no half-close.** `AF_UNIX` ends a stream by shutting down
//! one direction and the reader sees EOF; a pipe handle closes whole, and
//! closing it would take the reply direction with it. So a pipe connection
//! carries its stream in chunks, and the end of the stream is a chunk:
//!
//! ```text
//! 0x00 + u32 length + bytes    payload, as much as one write produced
//! 0x01                          FIN: the payload is complete
//! 0x02 + u64 code               RESET: the payload was abandoned, with why
//! ```
//!
//! The chunk layer costs five bytes per write and nothing per byte, and it
//! carries something the socket cannot: a reset code, so an abandoned
//! transfer is reported by name rather than as an early EOF. What it cannot
//! carry is the reader's refusal — there is no `STOP_SENDING`, because the
//! writer's direction is the only one the reader could signal on and the
//! reply already owns it. A reader that stops therefore **drains**: the rest
//! of the payload is read and discarded in the background, so the writer
//! finishes normally and learns of the refusal from the reply, as
//! [0012 §4.7](../../../docs/decisions/0012-local-connection-grouping.md)
//! names it for the socket. The two named losses of §4.7 become one on this
//! transport, and a third is added in its place: a refused writer is not
//! `Canceled` at the write.

use std::ffi::OsString;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};
use weida_core::{Error, PeerIdentity, WindowsPrincipal};
use weida_protocol::codes;
use weida_runtime::Exec;

use crate::grouped::Stream;

/// A payload chunk: `u32` little-endian length, then the bytes.
const CHUNK_DATA: u8 = 0x00;
/// The end of the payload.
const CHUNK_FIN: u8 = 0x01;
/// The payload abandoned, with a `u64` little-endian code.
const CHUNK_RESET: u8 = 0x02;
/// The longest chunk header: kind plus a reset code.
const HEADER_MAX: usize = 9;
/// Scratch for a draining reader.
const DRAIN_BUF: usize = 8 * 1024;

/// What a dial needs: the pipe's OS path and the runtime whose timer waits
/// out a busy pipe.
pub(crate) struct PipeEndpoint {
    pub(crate) path: OsString,
    pub(crate) exec: Exec,
}

/// One end of a pipe connection.
pub(crate) struct PipeStream {
    io: PipeIo,
    exec: Exec,
}

enum PipeIo {
    /// The instance that accepted, on the serving side.
    Server(NamedPipeServer),
    /// The handle that dialled.
    Client(NamedPipeClient),
}

impl PipeStream {
    /// An accepted server instance, once `connect` returned on it.
    pub(crate) fn accepted(server: NamedPipeServer, exec: Exec) -> PipeStream {
        PipeStream {
            io: PipeIo::Server(server),
            exec,
        }
    }
}

impl AsyncRead for PipeIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_read(cx, buf),
            PipeIo::Client(io) => Pin::new(io).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PipeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_write(cx, buf),
            PipeIo::Client(io) => Pin::new(io).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_flush(cx),
            PipeIo::Client(io) => Pin::new(io).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PipeIo::Server(io) => Pin::new(io).poll_shutdown(cx),
            PipeIo::Client(io) => Pin::new(io).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for PipeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for PipeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl Stream for PipeStream {
    type Endpoint = PipeEndpoint;
    type Principal = WindowsPrincipal;
    type Writer = PipeWriter;
    type Reader = PipeReader;

    async fn connect(endpoint: &PipeEndpoint) -> Result<PipeStream, Error> {
        let client = weida_runtime::connect_pipe(&endpoint.exec, &endpoint.path).await?;
        Ok(PipeStream {
            io: PipeIo::Client(client),
            exec: endpoint.exec.clone(),
        })
    }

    fn principal(&self) -> Result<WindowsPrincipal, Error> {
        match &self.io {
            PipeIo::Server(io) => weida_runtime::client_principal(io),
            PipeIo::Client(io) => weida_runtime::server_principal(io),
        }
    }

    fn split(self) -> (PipeReader, PipeWriter) {
        let (recv, send) = tokio::io::split(self.io);
        (
            PipeReader::new(recv, self.exec.clone()),
            PipeWriter::new(send, self.exec),
        )
    }

    /// The account always; the pid too, since the pipe reports one for
    /// both ends, and never the pid alone [0012 §4.2].
    fn same_peer(group: &WindowsPrincipal, asking: &WindowsPrincipal) -> bool {
        if group.sid != asking.sid {
            return false;
        }
        match (group.pid, asking.pid) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => true,
        }
    }

    fn identity(principal: &WindowsPrincipal) -> PeerIdentity {
        PeerIdentity::Windows(principal.clone())
    }

    fn finish(writer: PipeWriter) {
        writer.end(Marker::Fin);
    }

    fn reset(writer: PipeWriter, code: u64) {
        writer.end(Marker::Reset(code));
    }

    fn stop(reader: PipeReader, _code: u64) {
        reader.drain();
    }

    fn read_error(error: std::io::Error) -> Error {
        match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<PeerReset>())
        {
            Some(reset) => codes::stop_reason(reset.0).into(),
            None => Error::Transport(format!("local stream read failed: {error}")),
        }
    }
}

/// The peer abandoned its payload, with the code it gave.
#[derive(Debug)]
struct PeerReset(u64);

impl std::fmt::Display for PeerReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream reset by the peer with code {}", self.0)
    }
}

impl std::error::Error for PeerReset {}

enum Marker {
    Fin,
    Reset(u64),
}

impl Marker {
    fn encode(&self) -> ([u8; HEADER_MAX], usize) {
        let mut bytes = [0u8; HEADER_MAX];
        match self {
            Marker::Fin => {
                bytes[0] = CHUNK_FIN;
                (bytes, 1)
            }
            Marker::Reset(code) => {
                bytes[0] = CHUNK_RESET;
                bytes[1..].copy_from_slice(&code.to_le_bytes());
                (bytes, HEADER_MAX)
            }
        }
    }
}

/// The writing half: frames every write as a chunk.
pub(crate) struct PipeWriter {
    io: Option<WriteHalf<PipeIo>>,
    exec: Exec,
    /// The chunk header being written, and how much of it is still owed.
    header: [u8; HEADER_MAX],
    header_len: usize,
    header_written: usize,
    /// Payload bytes the current chunk's header promised and that have not
    /// been written yet.
    body_left: usize,
    /// A FIN went out through `poll_shutdown`; `finish` has nothing to add.
    finished: bool,
}

impl PipeWriter {
    fn new(io: WriteHalf<PipeIo>, exec: Exec) -> PipeWriter {
        PipeWriter {
            io: Some(io),
            exec,
            header: [0; HEADER_MAX],
            header_len: 0,
            header_written: 0,
            body_left: 0,
            finished: false,
        }
    }

    /// Writes the end-of-stream marker on the runtime, since the callers of
    /// `finish` and `reset` are synchronous.
    ///
    /// A chunk left half-written cannot be ended cleanly — the reader is
    /// owed bytes this side no longer has — so nothing is written and the
    /// reader sees the pipe close when the last half goes.
    fn end(mut self, marker: Marker) {
        let Some(mut io) = self.io.take() else {
            return;
        };
        if self.finished || self.body_left > 0 || self.header_written != self.header_len {
            return;
        }
        let (bytes, len) = marker.encode();
        self.exec.spawn(async move {
            if let Err(e) = io.write_all(&bytes[..len]).await {
                tracing::debug!(error = %e, "pipe stream end marker not written");
                return;
            }
            let _ = io.flush().await;
        });
    }

    /// Writes what is owed of the current header; `Ready(Ok(()))` when none
    /// is.
    fn poll_header(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Err(closed()));
        };
        while self.header_written < self.header_len {
            let n = ready!(
                Pin::new(&mut *io)
                    .poll_write(cx, &self.header[self.header_written..self.header_len])
            )?;
            if n == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "pipe accepted no bytes",
                )));
            }
            self.header_written += n;
        }
        Poll::Ready(Ok(()))
    }
}

fn closed() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stream already closed")
}

impl AsyncWrite for PipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.io.is_none() || this.finished {
            return Poll::Ready(Err(closed()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.body_left == 0 && this.header_written == this.header_len {
            // A new chunk: as long as this write, capped by the length
            // field.
            let len = buf.len().min(u32::MAX as usize);
            this.header[0] = CHUNK_DATA;
            this.header[1..5].copy_from_slice(&(len as u32).to_le_bytes());
            this.header_len = 5;
            this.header_written = 0;
            this.body_left = len;
        }
        ready!(this.poll_header(cx))?;
        let want = buf.len().min(this.body_left);
        let io = this.io.as_mut().expect("checked above");
        let n = ready!(Pin::new(io).poll_write(cx, &buf[..want]))?;
        this.body_left -= n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut().io.as_mut() {
            Some(io) => Pin::new(io).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    /// A shutdown is a FIN, as it is on every other transport, written in
    /// place through the polls rather than on the runtime.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.io.is_none() {
            return Poll::Ready(Ok(()));
        }
        if this.finished {
            return this.poll_flush_inner(cx);
        }
        if this.body_left > 0 {
            return Poll::Ready(Err(std::io::Error::other(
                "shutdown in the middle of a write",
            )));
        }
        if this.header_written == this.header_len {
            let (bytes, len) = Marker::Fin.encode();
            this.header = bytes;
            this.header_len = len;
            this.header_written = 0;
        }
        ready!(this.poll_header(cx))?;
        this.finished = true;
        this.poll_flush_inner(cx)
    }
}

impl PipeWriter {
    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.io.as_mut() {
            Some(io) => Pin::new(io).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}

/// The reading half: unframes chunks, ends at FIN, fails at RESET.
pub(crate) struct PipeReader {
    io: Option<ReadHalf<PipeIo>>,
    exec: Exec,
    state: ReadState,
}

enum ReadState {
    /// Reading a chunk header; the first byte says how long it is.
    Header {
        buf: [u8; HEADER_MAX],
        filled: usize,
    },
    /// Inside a payload chunk, with this many bytes still to come.
    Body { left: usize },
    /// FIN seen, RESET reported, or the pipe failed: nothing more to read.
    Ended,
}

/// What a completed header means.
enum Chunk {
    Data(usize),
    Fin,
    Reset(u64),
}

impl PipeReader {
    fn new(io: ReadHalf<PipeIo>, exec: Exec) -> PipeReader {
        PipeReader {
            io: Some(io),
            exec,
            state: ReadState::Header {
                buf: [0; HEADER_MAX],
                filled: 0,
            },
        }
    }

    /// Reads and discards the rest of the payload on the runtime, so the
    /// peer's writes complete instead of blocking on a pipe nobody reads.
    fn drain(mut self) {
        if matches!(self.state, ReadState::Ended) || self.io.is_none() {
            return;
        }
        let exec = self.exec.clone();
        exec.spawn(async move {
            let mut scratch = vec![0u8; DRAIN_BUF];
            loop {
                match self.read(&mut scratch).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        });
    }

    /// How many header bytes a chunk of `kind` has, or `None` for a kind
    /// this side does not know.
    fn header_len(kind: u8) -> Option<usize> {
        match kind {
            CHUNK_DATA => Some(5),
            CHUNK_FIN => Some(1),
            CHUNK_RESET => Some(HEADER_MAX),
            _ => None,
        }
    }

    fn poll_read_inner(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        loop {
            match &mut self.state {
                ReadState::Ended => return Poll::Ready(Ok(())),
                ReadState::Header {
                    buf: header,
                    filled,
                } => {
                    let need = if *filled == 0 {
                        1
                    } else {
                        match PipeReader::header_len(header[0]) {
                            Some(need) => need,
                            None => {
                                return Poll::Ready(Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("unknown pipe chunk kind {:#04x}", header[0]),
                                )));
                            }
                        }
                    };
                    if *filled < need {
                        let mut slice = ReadBuf::new(&mut header[*filled..need]);
                        ready!(Pin::new(&mut *io).poll_read(cx, &mut slice))?;
                        let n = slice.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "pipe closed before the end of the stream",
                            )));
                        }
                        *filled += n;
                        continue;
                    }
                    let chunk = match header[0] {
                        CHUNK_DATA => {
                            let len =
                                u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
                            Chunk::Data(len as usize)
                        }
                        CHUNK_FIN => Chunk::Fin,
                        _ => {
                            let mut code = [0u8; 8];
                            code.copy_from_slice(&header[1..HEADER_MAX]);
                            Chunk::Reset(u64::from_le_bytes(code))
                        }
                    };
                    match chunk {
                        Chunk::Data(0) => {
                            self.state = ReadState::Header {
                                buf: [0; HEADER_MAX],
                                filled: 0,
                            };
                        }
                        Chunk::Data(len) => self.state = ReadState::Body { left: len },
                        Chunk::Fin => {
                            self.state = ReadState::Ended;
                            return Poll::Ready(Ok(()));
                        }
                        Chunk::Reset(code) => {
                            self.state = ReadState::Ended;
                            return Poll::Ready(Err(std::io::Error::other(PeerReset(code))));
                        }
                    }
                }
                ReadState::Body { left } => {
                    let want = buf.remaining().min(*left);
                    if want == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    // Initialised up front so that `advance` below is sound
                    // without an `unsafe` `assume_init`.
                    buf.initialize_unfilled_to(want);
                    let mut slice = buf.take(want);
                    ready!(Pin::new(&mut *io).poll_read(cx, &mut slice))?;
                    let n = slice.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "pipe closed in the middle of a chunk",
                        )));
                    }
                    buf.advance(n);
                    *left -= n;
                    if *left == 0 {
                        self.state = ReadState::Header {
                            buf: [0; HEADER_MAX],
                            filled: 0,
                        };
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl AsyncRead for PipeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let polled = this.poll_read_inner(cx, buf);
        if let Poll::Ready(Err(_)) = &polled {
            // Any failure ends the stream: a drain must not spin on it.
            this.state = ReadState::Ended;
        }
        polled
    }
}
