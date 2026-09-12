//! The byte stream under the control lines, and how TLS gets under it.
//!
//! The NATS client protocol is TCP and text: "the client protocol's default
//! transport is TCP/IP; it also supports TLS over the connection"
//! (`docs/research/nats.md` §12 P18). There is **one** way to reach TLS and
//! it is not AMQP's two:
//!
//! ```text
//! TCP connect
//!       <---------------------- INFO {"tls_required":true,...}   (in the clear)
//! ...TLS handshake...
//! CONNECT {...} ------------->                                   (inside TLS)
//! PING          ------------->
//!       <---------------------- PONG
//! ```
//!
//! The `INFO` that *demands* TLS arrives before TLS, unencrypted, and is not
//! resent afterwards — it is the only protocol traffic that ever travels in
//! the clear on such a connection. Everything the client says is inside the
//! session, `CONNECT` and its credentials first of all. There is no port
//! split: TLS and plain share 4222, because `INFO` is what distinguishes
//! them.
//!
//! # The trust anchors are not here
//!
//! `upgrade_tls` takes the caller's `rustls::ClientConfig` — not a link,
//! because that function exists only under the `tls` feature and this
//! module's documentation is built without it too. Which
//! certificates are valid is the application's decision — a service on a
//! private network trusts a private CA, a hosted one trusts the public
//! roots — and a messaging library that shipped a default would be making
//! that choice for both. What this module does supply is the SNI name, taken
//! from the host that was dialled, because that is a protocol behaviour
//! rather than a convenience.
//!
//! # Reading is cancel-safe
//!
//! [`OpReader::next_op`] is used directly inside the driver's `select!`, with
//! no forwarding task in between, and that is only sound because every octet
//! it has read lives in `self` rather than in the future: a `next_op` dropped
//! half-way through a `MSG` leaves the partial `MSG` in the buffer and the
//! next call continues it. The alternative — a task that reads ahead and
//! forwards — would have to guess how far ahead to read, and the bound on
//! that guess would be one more bound nobody asked for.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use weida_nats_codec::error::DecodeError;
use weida_nats_codec::{Limits, Op};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::message::{Message, OwnedHeaders};
use crate::options::ConnectionOptions;

/// One operation from the server, owned.
///
/// Owned because it has to outlive the read buffer it was decoded from: a
/// `MSG` travels down a channel to whatever task holds the subscription, and
/// a borrow would tie that task's lifetime to the driver's buffer.
///
/// The four server-to-client verbs that carry no state — `PING`, `PONG`,
/// `+OK` — cost nothing to own. `INFO` keeps its JSON octets unparsed,
/// because the fields a client acts on are
/// [`ServerInfo`](weida_nats_codec::ServerInfo)'s to read and the connection
/// is the layer that acts on them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Incoming {
    /// `INFO {json}`: the object octets, `{` through `}`.
    Info(Vec<u8>),
    /// `MSG` or `HMSG`, dispatched by `sid`.
    Msg(Message),
    /// `PING`, which must be answered with `PONG`.
    Ping,
    /// `PONG`, which answers one of ours.
    Pong,
    /// `+OK`, sent per operation while `verbose` is on.
    Ok,
    /// `-ERR '<reason>'`. The reason is lossily decoded, because an error
    /// message a client could not report because it was not UTF-8 would be
    /// the one explanation it loses.
    Err(String),
}

/// The byte stream a connection runs on.
///
/// An enum rather than a boxed trait object: there are exactly two cases, the
/// dispatch is one branch per read, and a `Box<dyn AsyncRead + AsyncWrite>`
/// would add an allocation and a vtable call for no gain.
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
    /// Not cosmetic: `CONNECT` carries a password or a JWT, and a caller
    /// deciding whether to offer one wants to be able to ask.
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
/// and `127.0.0.1`, and a server bound to one of them is unreachable through
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
                // Nagle off: the handshake is a run of short control lines
                // that each need an answer, and a request-reply round trip is
                // two more. Coalescing them adds a delay to every one.
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

/// A NATS byte stream with one read buffer, for the handshake.
///
/// The buffer exists because a control line arrives in however many TCP
/// segments the network chose and the codec needs it contiguous. Its growth
/// is bounded by the two bounds the codec already applies: a line with no
/// `CRLF` inside `max_control_line` is refused, and a declared payload above
/// `max_payload` is refused from the control line alone — so nothing is ever
/// reserved on the server's word.
#[derive(Debug)]
pub struct Wire {
    stream: Stream,
    buf: Vec<u8>,
    /// Where the unconsumed octets start inside `buf`.
    from: usize,
}

impl Wire {
    /// Wraps a stream.
    #[must_use]
    pub fn new(stream: Stream) -> Self {
        Self {
            stream,
            buf: Vec::new(),
            from: 0,
        }
    }

    /// The stream underneath, for the peer address and the encryption flag.
    #[must_use]
    pub const fn stream(&self) -> &Stream {
        &self.stream
    }

    /// Writes `bytes` and flushes them.
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads one operation.
    pub async fn read_op(&mut self, limits: Limits) -> Result<Incoming> {
        next_op(&mut self.stream, &mut self.buf, &mut self.from, limits).await
    }

    /// Consumes the wire, handing back the stream for the TLS upgrade.
    ///
    /// Buffered octets are an error rather than something to discard. On a
    /// `tls_required` connection the server sends `INFO` and then waits for a
    /// TLS `ClientHello`; anything it sent after that `INFO` it sent in the
    /// clear, and carrying it into the session or dropping it would both mean
    /// treating unencrypted protocol as though it had been protected.
    pub fn into_stream(self) -> Result<Stream> {
        if self.buf.len() > self.from {
            return Err(Error::Protocol(format!(
                "{} octet(s) arrived after INFO and before the TLS handshake, \
                 in the clear",
                self.buf.len() - self.from
            )));
        }
        Ok(self.stream)
    }

    /// Splits the wire for the connection driver.
    ///
    /// The handshake is a strict sequence on one object; everything after it
    /// is not — a `PONG` has to go out while a `MSG` is half-read — so the
    /// driver needs the two directions separately. Octets already buffered
    /// travel with the reader.
    #[must_use]
    pub fn split(self) -> (OpReader, OpWriter) {
        let (read, write) = tokio::io::split(self.stream);
        (
            OpReader {
                read,
                buf: self.buf,
                from: self.from,
            },
            OpWriter { write },
        )
    }
}

/// Reads whole operations from one half of a split wire.
#[derive(Debug)]
pub struct OpReader {
    read: tokio::io::ReadHalf<Stream>,
    buf: Vec<u8>,
    from: usize,
}

impl OpReader {
    /// The next whole operation.
    ///
    /// **Cancel-safe**: every octet read lives in `self`, so a call dropped
    /// by a `select!` loses nothing and the next call continues where it
    /// stopped. `limits` is an argument rather than a field because
    /// `max_payload` changes when an asynchronous `INFO` arrives, and the
    /// value in force is the driver's to know.
    pub async fn next_op(&mut self, limits: Limits) -> Result<Incoming> {
        next_op(&mut self.read, &mut self.buf, &mut self.from, limits).await
    }
}

/// Writes to one half of a split wire.
#[derive(Debug)]
pub struct OpWriter {
    write: tokio::io::WriteHalf<Stream>,
}

impl OpWriter {
    /// Writes `bytes` and flushes.
    pub async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.write.write_all(bytes).await?;
        self.write.flush().await?;
        Ok(())
    }

    /// Closes the write half.
    ///
    /// This is the whole of a NATS close: "a normal client protocol close has
    /// no dedicated `CLOSE` verb; ending the transport ends the connection
    /// and its subscriptions" (`docs/research/nats.md` §1).
    pub async fn shutdown(&mut self) -> Result<()> {
        self.write.shutdown().await?;
        Ok(())
    }
}

/// One operation out of `buf`, filling from `reader` until there is one.
async fn next_op<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    from: &mut usize,
    limits: Limits,
) -> Result<Incoming> {
    loop {
        if buf.len() > *from {
            match Op::decode(&buf[*from..], limits) {
                Ok((op, used)) => {
                    let incoming = own(&op)?;
                    *from += used;
                    if *from == buf.len() {
                        // Everything buffered is spent, which is the common
                        // case: reset rather than grow and drain later.
                        buf.clear();
                        *from = 0;
                    }
                    return Ok(incoming);
                }
                Err(DecodeError::Incomplete { .. }) => {}
                Err(error) => return Err(Error::Decode(error)),
            }
        }
        // Reclaim the consumed prefix before reading more, so that a run of
        // short control lines inside one segment is not memmoved per line.
        if *from > 0 {
            buf.drain(..*from);
            *from = 0;
        }
        let read = reader.read_buf(buf).await?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the server closed the transport",
            )));
        }
    }
}

/// Copies one decoded operation out of the read buffer.
fn own(op: &Op<'_>) -> Result<Incoming> {
    Ok(match op {
        Op::Info { json } => Incoming::Info((*json).to_vec()),
        Op::Msg {
            subject,
            sid,
            reply_to,
            payload,
        } => Incoming::Msg(Message {
            subject: (*subject).to_vec(),
            sid: parse_sid(sid)?,
            reply_to: reply_to.map(<[u8]>::to_vec),
            headers: None,
            payload: (*payload).to_vec(),
        }),
        Op::Hmsg {
            subject,
            sid,
            reply_to,
            headers,
            payload,
        } => Incoming::Msg(Message {
            subject: (*subject).to_vec(),
            sid: parse_sid(sid)?,
            reply_to: reply_to.map(<[u8]>::to_vec),
            headers: Some(OwnedHeaders::from_borrowed(headers)),
            payload: (*payload).to_vec(),
        }),
        Op::Ping => Incoming::Ping,
        Op::Pong => Incoming::Pong,
        Op::Ok => Incoming::Ok,
        Op::Err { reason } => Incoming::Err(String::from_utf8_lossy(reason).into_owned()),
        // A client is never sent these. `CONNECT`, `PUB`, `HPUB`, `SUB` and
        // `UNSUB` are client-to-server verbs, so a server writing one is a
        // server this client cannot reason about.
        other => {
            return Err(Error::Protocol(format!(
                "the server sent {}, which is a client-to-server operation",
                other.verb()
            )));
        }
    })
}

/// The `sid` a `MSG` came back on.
///
/// "A unique alphanumeric subscription ID, generated by the client" — so
/// every `sid` on this connection is one this client wrote as decimal, and
/// one that is not is a `sid` the server invented.
fn parse_sid(sid: &[u8]) -> Result<u64> {
    std::str::from_utf8(sid)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| {
            Error::Protocol(format!(
                "the server delivered a message with sid {:?}, and every sid on \
                 this connection is one this client generated",
                String::from_utf8_lossy(sid)
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads every operation out of one buffer of octets, so the framing is
    /// tested without a socket.
    fn read_all(wire: &[u8]) -> Vec<Incoming> {
        let mut buf = Vec::new();
        let mut from = 0;
        let mut out = Vec::new();
        // A finite reader: once the octets are gone, `next_op` sees EOF.
        let mut reader = std::io::Cursor::new(wire.to_vec());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        loop {
            let next = runtime.block_on(next_op(&mut reader, &mut buf, &mut from, Limits::DEFAULT));
            match next {
                Ok(op) => out.push(op),
                Err(_) => return out,
            }
        }
    }

    /// Several operations inside one buffer are read as several operations,
    /// including a payload that itself holds a `CRLF` — the case a reader
    /// that searched for a terminator instead of trusting the declared count
    /// would split in the wrong place.
    #[test]
    fn a_pipelined_buffer_is_read_operation_by_operation() {
        let wire = b"INFO {\"max_payload\":1024}\r\n\
                     PING\r\n\
                     MSG a.b 7 5\r\nhi\r\nx\r\n\
                     PONG\r\n\
                     -ERR 'Stale Connection'\r\n";
        let ops = read_all(wire);
        assert_eq!(ops.len(), 5, "{ops:?}");
        assert_eq!(ops[0], Incoming::Info(b"{\"max_payload\":1024}".to_vec()));
        assert_eq!(ops[1], Incoming::Ping);
        let Incoming::Msg(message) = &ops[2] else {
            panic!("expected a MSG, got {:?}", ops[2]);
        };
        assert_eq!(message.sid, 7);
        assert_eq!(message.subject, b"a.b");
        assert_eq!(
            message.payload, b"hi\r\nx",
            "the declared count delimits the payload, not a search for CRLF"
        );
        assert_eq!(ops[3], Incoming::Pong);
        assert_eq!(ops[4], Incoming::Err("Stale Connection".to_owned()));
    }

    /// `HMSG` arrives with its block copied out, status and all.
    #[test]
    fn a_headered_message_keeps_its_block() {
        let wire = b"HMSG a.b 3 _INBOX.9 16 16\r\nNATS/1.0 503\r\n\r\n\r\n";
        let ops = read_all(wire);
        let Some(Incoming::Msg(message)) = ops.first() else {
            panic!("expected an HMSG, got {ops:?}");
        };
        assert_eq!(message.sid, 3);
        assert_eq!(message.reply_to.as_deref(), Some(&b"_INBOX.9"[..]));
        assert!(message.payload.is_empty());
        assert!(message.is_no_responders());
    }

    /// A `sid` this client could not have written is the server inventing
    /// one, and inventing a number to match it would route a message to a
    /// subscription that never asked for it.
    #[test]
    fn a_sid_the_client_did_not_generate_is_a_protocol_error() {
        let mut buf = Vec::new();
        let mut from = 0;
        let mut reader = std::io::Cursor::new(b"MSG a.b abc 0\r\n\r\n".to_vec());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        let error = runtime
            .block_on(next_op(&mut reader, &mut buf, &mut from, Limits::DEFAULT))
            .expect_err("refused");
        assert!(matches!(error, Error::Protocol(_)), "{error}");
    }

    /// A server-to-client reader that accepted `PUB` would be a reader with
    /// no idea which side of the connection it is on.
    #[test]
    fn a_client_to_server_verb_from_the_server_is_refused() {
        let mut buf = Vec::new();
        let mut from = 0;
        let mut reader = std::io::Cursor::new(b"PUB a.b 0\r\n\r\n".to_vec());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        let error = runtime
            .block_on(next_op(&mut reader, &mut buf, &mut from, Limits::DEFAULT))
            .expect_err("refused");
        assert!(
            matches!(&error, Error::Protocol(why) if why.contains("PUB")),
            "{error}"
        );
    }
}
