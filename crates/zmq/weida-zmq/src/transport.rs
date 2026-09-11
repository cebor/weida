//! The one place this crate knows what a connection is made of.
//!
//! ZeroMQ's working set is `tcp`, `ipc` and `inproc`
//! (`docs/research/zeromq.md` §13), and all three are carried here. `ipc` is
//! `#[cfg(unix)]`: `AF_UNIX` is not a transport a platform can be given, so
//! on a platform without it the engine **refuses** the endpoint with
//! `EPROTONOSUPPORT` rather than holding a variant that cannot carry bytes.
//!
//! `inproc` is a [`tokio::io::duplex`] pair rather than a socket, which is
//! the whole of "passes messages via memory directly between threads sharing
//! a single 0MQ context, involving no I/O threads" (§12): above this enum the
//! session code cannot tell the difference, and that is why the protocol is
//! tested once and runs over both.
//!
//! An enum rather than a trait object, for the reason weida's own transport
//! boundary gives (`docs/ARCHITECTURE.md` §5): the set of transports is
//! closed, small and decided in this crate, and the payload path must stay a
//! direct call.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use weida_core::LocalPrincipal;

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
    Inproc(DuplexStream),
    #[cfg(unix)]
    Unix {
        stream: UnixStream,
        /// Captured once, when the connection was made, because that is when
        /// the kernel takes the snapshot.
        principal: LocalPrincipal,
    },
}

impl Stream {
    /// Wraps an established TCP connection.
    pub fn tcp(stream: TcpStream) -> Stream {
        Stream(Inner::Tcp(stream))
    }

    /// Wraps one half of an in-process connection.
    pub fn inproc(stream: DuplexStream) -> Stream {
        Stream(Inner::Inproc(stream))
    }

    /// Wraps an established `AF_UNIX` connection, capturing the peer's
    /// credentials as the kernel reports them now.
    ///
    /// Fails when the kernel refuses to answer, which is a connection that
    /// has already gone: a local peer whose credentials cannot be read is not
    /// a peer this library carries, because for `ipc` the kernel's answer is
    /// the only thing there is to know about who is on the other end
    /// ([0010](../../../docs/decisions/0010-local-transport.md) §4.4).
    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> crate::error::Result<Stream> {
        let principal = crate::ipc::credentials(&stream)?;
        Ok(Stream(Inner::Unix { stream, principal }))
    }

    /// The credentials the kernel attributes to this connection's peer.
    ///
    /// `None` for every transport where there is no such fact: TCP has an
    /// address and no process, and `inproc` has no kernel at all. `None`
    /// rather than a zeroed principal, which would read as root.
    pub fn peer_credentials(&self) -> Option<weida_core::LocalPrincipal> {
        match &self.0 {
            #[cfg(unix)]
            Inner::Unix { principal, .. } => Some(*principal),
            _ => None,
        }
    }

    /// Disables Nagle's algorithm, which a request-reply pattern on loopback
    /// notices and a bulk one does not.
    ///
    /// Nothing to do for `inproc`: there is no kernel between the halves, so
    /// there is no algorithm to disable. Reported as success rather than as
    /// an error, because the caller asked for a property that already holds.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match &self.0 {
            Inner::Tcp(stream) => stream.set_nodelay(nodelay),
            #[cfg(unix)]
            Inner::Unix { .. } => Ok(()),
            Inner::Inproc(_) => Ok(()),
        }
    }

    /// The transport this stream came over, for a log line.
    pub const fn transport(&self) -> &'static str {
        match &self.0 {
            Inner::Tcp(_) => "tcp",
            #[cfg(unix)]
            Inner::Unix { .. } => "ipc",
            Inner::Inproc(_) => "inproc",
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
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_read(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_read(cx, buf),
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
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_write(cx, buf),
            Inner::Inproc(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_flush(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Inner::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Inner::Unix { stream, .. } => Pin::new(stream).poll_shutdown(cx),
            Inner::Inproc(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
