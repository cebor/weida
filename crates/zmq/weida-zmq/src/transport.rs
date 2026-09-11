//! The one place this crate knows what a connection is made of.
//!
//! ZeroMQ's working set is `tcp`, `ipc` and `inproc`
//! (`docs/research/zeromq.md` §13). Only `tcp` is carried here; `ipc` and
//! `inproc` are their own slices
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §5.3), and
//! until they land the engine **refuses** those endpoints with
//! `EPROTONOSUPPORT` rather than holding a place for them. There is nothing
//! to stand in for: a variant that cannot carry bytes would be a lie in the
//! type system, and the type is the enum below.
//!
//! An enum rather than a trait object, for the reason weida's own transport
//! boundary gives (`docs/ARCHITECTURE.md` §5): the set of transports is
//! closed, small and decided in this crate, and the payload path must stay a
//! direct call.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// One established connection's byte stream.
///
/// `AsyncRead + AsyncWrite`, so the session layer above it is written once
/// and does not know which transport carried it — which is what makes the
/// ZMTP session testable against a plain `TcpStream` peer.
#[derive(Debug)]
pub struct Stream(Inner);

#[derive(Debug)]
enum Inner {
    Tcp(TcpStream),
}

impl Stream {
    /// Wraps an established TCP connection.
    pub fn tcp(stream: TcpStream) -> Stream {
        Stream(Inner::Tcp(stream))
    }

    /// Disables Nagle's algorithm, which a request-reply pattern on loopback
    /// notices and a bulk one does not.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match &self.0 {
            Inner::Tcp(stream) => stream.set_nodelay(nodelay),
        }
    }

    /// The transport this stream came over, for a log line.
    pub const fn transport(&self) -> &'static str {
        match &self.0 {
            Inner::Tcp(_) => "tcp",
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
