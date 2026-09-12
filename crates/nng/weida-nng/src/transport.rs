//! The one place this crate knows what a connection is made of.
//!
//! An enum rather than a trait object, for the reason weida's own transport
//! boundary gives (`docs/ARCHITECTURE.md` §5): the set of transports is
//! closed, small and decided in this crate, and the payload path must stay a
//! direct call. Above it the SP session is written once and cannot tell
//! which transport carried its octets, which is why the protocol is tested
//! once and runs over all of them.
//!
//! The four this library implements are the four
//! [`crate::Endpoint`](crate::endpoint::Endpoint) parses. They arrive in
//! their own slices — `tcp` here, the local pair and TLS after it — and a
//! transport whose variant is not yet carried is refused by name at the
//! endpoint rather than accepted and dropped somewhere quieter, because a
//! refusal SP can express is a close and a refusal it cannot is a hang
//! (§6).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// One established connection's byte stream, and the pipe that will run
/// over it.
#[derive(Debug)]
pub struct Stream(Inner);

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
}

impl Stream {
    /// Wraps an established TCP connection.
    ///
    /// Nagle is disabled: SP's request-reply and survey patterns are
    /// round-trip shaped, and a 40-millisecond delay on a four-octet tag
    /// stack is the difference between a working benchmark and a mystery.
    /// A failure to set it is not fatal — the connection works, slower — so
    /// it is logged rather than thrown.
    pub fn tcp(stream: TcpStream) -> Stream {
        if let Err(error) = stream.set_nodelay(true) {
            tracing::debug!(%error, "could not disable Nagle on an SP connection");
        }
        Stream(Inner::Tcp(stream))
    }

    /// The transport this stream came over, for a log line and for
    /// `NNG_OPT_URL`'s scheme.
    pub const fn transport(&self) -> &'static str {
        match &self.0 {
            Inner::Tcp(_) => "tcp",
        }
    }

    /// This end's address, as `NNG_OPT_LOCADDR` reports it, or `None` for a
    /// transport with no address.
    pub fn local_addr(&self) -> Option<String> {
        match &self.0 {
            Inner::Tcp(stream) => stream.local_addr().ok().map(|a| a.to_string()),
        }
    }

    /// The peer's address, as `NNG_OPT_REMADDR` reports it.
    pub fn remote_addr(&self) -> Option<String> {
        match &self.0 {
            Inner::Tcp(stream) => stream.peer_addr().ok().map(|a| a.to_string()),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
