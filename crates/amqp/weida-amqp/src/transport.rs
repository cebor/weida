//! The byte stream under the frames, and the two ways TLS gets under it.
//!
//! AMQP defines TCP and nothing else: `PORT` 5672 and `SECURE-PORT` 5671, with
//! UDP and SCTP assignments "reserved" and no mapping defined
//! (Part 2 §2.8.19). TLS is a *layer*, reached one of two ways
//! (Part 5 §5.2):
//!
//! ```text
//! Layered      client -> AMQP %d2 1.0.0     server -> AMQP %d2 1.0.0
//!              ...TLS handshake...
//!              client -> AMQP %d0 1.0.0     server -> AMQP %d0 1.0.0   (inside TLS)
//!
//! Direct       ...TLS handshake...                                      (no header)
//!              client -> AMQP %d0 1.0.0     server -> AMQP %d0 1.0.0   (inside TLS)
//! ```
//!
//! The difference is observable and neither substitutes for the other: the
//! layered form announces itself in the clear on 5672, the direct form is a
//! pure TLS listener on 5671 that would see `AMQP %d2 1.0.0` as a failed
//! handshake. [`TlsMode`](crate::TlsMode) is the caller's choice between them.
//!
//! # The trust anchors are not here
//!
//! `upgrade_tls` takes the caller's `rustls::ClientConfig`. Part 5 §5.2
//! requires the TLS client to validate the server certificate and this crate
//! has no opinion about which certificates are valid: an application talking
//! to a broker on its own network trusts a private CA, one talking to Service
//! Bus trusts the public roots, and a messaging library that shipped a
//! default would be making that decision for both. What this crate does
//! supply is the SNI name, because Part 5 §5.2.1 notes it "can select both
//! the back end and the domain against which to validate client
//! credentials" — so getting it from the dialled host rather than from
//! nowhere is a protocol behaviour, not a convenience.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use weida_amqp_codec::frame::{self, Frame, MIN_FRAME_SIZE};
use weida_amqp_codec::protocol_header::{self, LEN as HEADER_LEN, ProtocolHeader};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::options::ConnectionOptions;

/// The byte stream a connection runs on.
///
/// An enum rather than a boxed trait object: there are exactly two cases, the
/// dispatch is one branch per read, and a `Box<dyn AsyncRead + AsyncWrite>`
/// would add an allocation and a vtable call to every frame for no gain.
#[derive(Debug)]
pub enum Stream {
    /// Plain TCP.
    Plain(TcpStream),
    /// TCP under TLS.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Stream {
    /// The peer's address, for a log line.
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        match self {
            Self::Plain(stream) => Ok(stream.peer_addr()?),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Ok(stream.get_ref().0.peer_addr()?),
        }
    }

    /// Whether the stream is encrypted.
    ///
    /// Not cosmetic: `PLAIN` carries a password in the clear, so a caller
    /// deciding whether to offer it wants to be able to ask.
    #[must_use]
    pub const fn is_encrypted(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(_) => true,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Opens a plain TCP connection to `host:port`, trying every address the
/// resolver offered in its order.
///
/// Every address, not the first: `localhost` commonly resolves to both `::1`
/// and `127.0.0.1`, and a broker bound to one of them is unreachable through
/// the other. The count is capped by
/// [`ConnectionOptions::max_resolved_addresses`], because a resolver answer is
/// remote input.
pub async fn connect(
    exec: &Exec,
    host: &str,
    port: u16,
    options: &ConnectionOptions,
) -> Result<Stream> {
    let addrs = exec
        .resolve(host, port, options.max_resolved_addresses)
        .await?;
    let mut last = None;
    for addr in &addrs {
        match exec
            .within(options.handshake_timeout, TcpStream::connect(addr))
            .await
        {
            Some(Ok(stream)) => {
                // Nagle off: an AMQP handshake is a sequence of small frames
                // that each need an answer, and coalescing them adds a round
                // trip's worth of delay to every step.
                let _ = stream.set_nodelay(true);
                return Ok(Stream::Plain(stream));
            }
            Some(Err(error)) => last = Some(Error::Io(error)),
            None => {
                last = Some(Error::HandshakeTimeout {
                    step: "the TCP connect",
                });
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        Error::Runtime(weida_core::Error::InvalidAddress(format!(
            "{host}:{port} resolved to no addresses"
        )))
    }))
}

/// Puts TLS under an already-connected stream.
///
/// `server_name` is what the certificate is validated against and what goes
/// in SNI. The caller's `config` decides what "valid" means.
#[cfg(feature = "tls")]
pub async fn upgrade_tls(
    stream: Stream,
    config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    server_name: &str,
) -> Result<Stream> {
    let Stream::Plain(tcp) = stream else {
        return Err(Error::Tls("the stream is already under TLS".into()));
    };
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|_| Error::Tls(format!("{server_name} is not a valid server name")))?;
    let connector = tokio_rustls::TlsConnector::from(config);
    let tls = connector
        .connect(name, tcp)
        .await
        .map_err(|error| Error::Tls(error.to_string()))?;
    Ok(Stream::Tls(Box::new(tls)))
}

/// A framed AMQP byte stream: protocol headers and frames, with one read
/// buffer.
///
/// The buffer exists because a frame arrives in however many TCP segments the
/// network chose, and the codec needs it contiguous. It grows to at most one
/// frame plus whatever of the next frame arrived with it, which is why
/// `max_frame_size` is held here: the bound on the buffer is the bound on a
/// frame, and before `open` has been read that bound is
/// [`MIN_MAX_FRAME_SIZE`](weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE).
#[derive(Debug)]
pub struct Wire {
    stream: Stream,
    read: Vec<u8>,
    /// Where the unconsumed octets start inside `read`.
    from: usize,
    max_frame_size: u32,
    write: Vec<u8>,
}

impl Wire {
    /// Wraps a stream, with the pre-negotiation frame bound in force.
    #[must_use]
    pub fn new(stream: Stream) -> Self {
        Self {
            stream,
            read: Vec::new(),
            from: 0,
            max_frame_size: weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE,
            write: Vec::new(),
        }
    }

    /// Raises the frame bound to what the peer's `open` advertised.
    ///
    /// One-way on purpose: the bound starts at 512 and only ever grows, so a
    /// forgotten call leaves the strictest check in force rather than the
    /// loosest.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The frame bound currently in force.
    #[must_use]
    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// The stream underneath, for the TLS upgrade and for the peer address.
    #[must_use]
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Consumes the wire, handing back the stream.
    ///
    /// Used for exactly one thing: the `%d2` layering, where the header
    /// exchange happens on the plain stream and everything after it happens
    /// inside TLS. Any buffered octets are discarded, which is correct —
    /// a server that pipelined anything after its `%d2` header sent it before
    /// the handshake and it cannot be part of the TLS session.
    #[must_use]
    pub fn into_stream(self) -> Stream {
        self.stream
    }

    /// Sends `header` and flushes it.
    pub async fn write_protocol_header(&mut self, header: ProtocolHeader) -> Result<()> {
        self.stream.write_all(&header.encode()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads the peer's eight-octet protocol header.
    pub async fn read_protocol_header(&mut self) -> Result<ProtocolHeader> {
        self.fill_to(HEADER_LEN).await?;
        let header =
            protocol_header::decode(&self.read[self.from..]).map_err(Error::BadProtocolHeader)?;
        self.from += HEADER_LEN;
        Ok(header)
    }

    /// Appends to the write buffer. Nothing leaves until [`Wire::flush`].
    pub fn queue(&mut self) -> &mut Vec<u8> {
        &mut self.write
    }

    /// Writes and clears the write buffer.
    pub async fn flush(&mut self) -> Result<()> {
        if self.write.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write).await?;
        self.write.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads one frame, borrowing its body from the read buffer.
    ///
    /// The bound in force is applied to the declared `SIZE` from the
    /// eight-octet header alone, so a peer that declares four gigabytes
    /// before `open` has been read is refused on those eight octets and
    /// nothing is reserved.
    pub async fn read_frame(&mut self) -> Result<Frame<'_>> {
        self.fill_to(MIN_FRAME_SIZE as usize).await?;
        // The header is present; it decides how much more is needed, and the
        // ceiling is checked here rather than after the body arrives.
        let header = frame::decode_header(&self.read[self.from..], self.max_frame_size)?;
        let size = header.size as usize;
        self.fill_to(size).await?;
        let frame = frame::decode(&self.read[self.from..], self.max_frame_size)?;
        self.from += size;
        Ok(frame)
    }

    /// Splits the wire for the connection driver.
    ///
    /// The handshake is a strict request/response sequence and reads and
    /// writes on one object; everything after it is not — a `flow` may have
    /// to go out while a `transfer` is half-read — so the driver needs the
    /// two directions separately. Any octets already buffered travel with
    /// the reader, because a server that pipelined frames behind its `open`
    /// is doing something the specification explicitly permits
    /// (Part 2 §2.4.2) and dropping them would lose them.
    #[must_use]
    pub fn split(self) -> (FrameReader, FrameWriter) {
        let (read, write) = tokio::io::split(self.stream);
        (
            FrameReader {
                read,
                buf: self.read,
                from: self.from,
                max_frame_size: self.max_frame_size,
            },
            FrameWriter {
                write,
                max_frame_size: self.max_frame_size,
            },
        )
    }

    /// Ensures at least `want` unconsumed octets are buffered.
    async fn fill_to(&mut self, want: usize) -> Result<()> {
        fill_to(&mut self.stream, &mut self.read, &mut self.from, want).await
    }
}

/// Reads whole frames from one half of a split wire.
///
/// Hands back each frame as an owned `Vec<u8>` rather than a borrow. One
/// allocation per frame, deliberately: the borrow would tie the driver's
/// whole loop to the reader's buffer, and the driver has to be able to hold a
/// frame while writing an answer to it.
#[derive(Debug)]
pub struct FrameReader {
    read: tokio::io::ReadHalf<Stream>,
    buf: Vec<u8>,
    from: usize,
    max_frame_size: u32,
}

impl FrameReader {
    /// Raises the frame bound to what the peer's `open` advertised.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The next whole frame, header included.
    ///
    /// The ceiling is applied to the declared `SIZE` from the eight-octet
    /// header alone, before the body is waited for.
    pub async fn next_frame(&mut self) -> Result<Vec<u8>> {
        fill_to(
            &mut self.read,
            &mut self.buf,
            &mut self.from,
            MIN_FRAME_SIZE as usize,
        )
        .await?;
        let header = frame::decode_header(&self.buf[self.from..], self.max_frame_size)?;
        let size = header.size as usize;
        fill_to(&mut self.read, &mut self.buf, &mut self.from, size).await?;
        let bytes = self.buf[self.from..self.from + size].to_vec();
        self.from += size;
        Ok(bytes)
    }
}

/// Writes to one half of a split wire.
#[derive(Debug)]
pub struct FrameWriter {
    write: tokio::io::WriteHalf<Stream>,
    max_frame_size: u32,
}

impl FrameWriter {
    /// Raises the frame bound to what the peer's `open` advertised.
    pub fn negotiated(&mut self, max_frame_size: u32) {
        self.max_frame_size = self.max_frame_size.max(max_frame_size);
    }

    /// The bound a frame this client sends must stay inside.
    #[must_use]
    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Writes `bytes` and flushes.
    pub async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.write.write_all(bytes).await?;
        self.write.flush().await?;
        Ok(())
    }

    /// Closes the write half, which is what "`close` is the last thing ever
    /// written" comes to in practice.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.write.shutdown().await?;
        Ok(())
    }
}

/// Ensures at least `want` unconsumed octets sit in `buf` at or after
/// `from`.
///
/// Shared by [`Wire`] and [`FrameReader`], because the buffering rule is the
/// same before and after the split and writing it twice would let the two
/// drift.
async fn fill_to<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    from: &mut usize,
    want: usize,
) -> Result<()> {
    loop {
        if buf.len() - *from >= want {
            return Ok(());
        }
        // Reclaim the consumed prefix before growing. Done here rather than
        // after every frame so that a run of small frames inside one segment
        // is not memmoved once per frame.
        if *from > 0 {
            buf.drain(..*from);
            *from = 0;
            if buf.len() >= want {
                return Ok(());
            }
        }
        buf.reserve(want.saturating_sub(buf.len()));
        let read = reader.read_buf(buf).await?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the peer closed the transport",
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_amqp_codec::frame::FrameKind;

    /// A `Wire` over a pair of in-process sockets, for tests that need to put
    /// exact octets in front of the reader.
    async fn wired() -> (Wire, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        let client = client.await.unwrap();
        (Wire::new(Stream::Plain(client)), server)
    }

    #[tokio::test]
    async fn a_frame_split_across_reads_is_reassembled() {
        let (mut wire, mut server) = wired().await;
        let mut whole = Vec::new();
        frame::write(&mut whole, FrameKind::Amqp, 7, 512, |body| {
            body.extend_from_slice(b"performative");
            Ok(())
        })
        .unwrap();
        let (first, second) = whole.split_at(5);
        server.write_all(first).await.unwrap();
        server.flush().await.unwrap();
        let first = first.to_vec();
        let second = second.to_vec();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            server.write_all(&second).await.unwrap();
            server.flush().await.unwrap();
            // Hold the socket open so the reader does not see EOF.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        let frame = wire.read_frame().await.expect("a frame");
        assert_eq!(frame.header.channel, 7);
        assert_eq!(frame.body, b"performative");
        assert_eq!(first.len(), 5);
    }

    #[tokio::test]
    async fn two_frames_in_one_segment_are_read_one_at_a_time() {
        let (mut wire, mut server) = wired().await;
        let mut both = Vec::new();
        for channel in [1u16, 2] {
            frame::write(&mut both, FrameKind::Amqp, channel, 512, |body| {
                body.push(channel as u8);
                Ok(())
            })
            .unwrap();
        }
        server.write_all(&both).await.unwrap();
        server.flush().await.unwrap();
        let keep = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            drop(server);
        });
        for channel in [1u16, 2] {
            let frame = wire.read_frame().await.expect("a frame");
            assert_eq!(frame.header.channel, channel);
            assert_eq!(frame.body, &[channel as u8]);
        }
        keep.abort();
    }

    #[tokio::test]
    async fn the_pre_negotiation_ceiling_is_in_force_before_open() {
        let (mut wire, mut server) = wired().await;
        assert_eq!(
            wire.max_frame_size(),
            weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE
        );
        // Eight octets claiming four gigabytes. The refusal comes from the
        // header alone: no body is sent at all, and the test would hang
        // rather than fail if the reader waited for one.
        server
            .write_all(&[0xff, 0xff, 0xff, 0xff, 0x02, 0x00, 0x00, 0x00])
            .await
            .unwrap();
        server.flush().await.unwrap();
        let error = wire.read_frame().await.expect_err("refused");
        assert!(
            matches!(
                error,
                Error::Decode(weida_amqp_codec::DecodeError::FrameTooLarge { max: 512, .. })
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn the_ceiling_only_ever_grows() {
        let (mut wire, _server) = wired().await;
        wire.negotiated(128 * 1024);
        assert_eq!(wire.max_frame_size(), 128 * 1024);
        wire.negotiated(16);
        assert_eq!(
            wire.max_frame_size(),
            128 * 1024,
            "a forgotten or a smaller negotiation leaves the stricter bound"
        );
    }

    #[tokio::test]
    async fn a_closed_transport_is_reported_as_end_of_file() {
        let (mut wire, server) = wired().await;
        drop(server);
        let error = wire.read_frame().await.expect_err("the peer is gone");
        assert!(matches!(error, Error::Io(_)), "{error}");
    }
}
