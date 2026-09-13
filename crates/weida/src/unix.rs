//! The `AF_UNIX` transport: `weida+unix://<percent-encoded-path>/<path>`.
//!
//! `SOCK_STREAM` on a filesystem path, and the peer proved by `SO_PEERCRED`
//! / `LOCAL_PEERCRED`
//! ([decision 0010](../../../docs/decisions/0010-local-transport.md) §4.4,
//! §4.5). The grouping of connections into peers is [`crate::grouped`]; this
//! module is only what a unix socket contributes to it: how it is dialled,
//! how the kernel names its peer, and that its halves end by half-close.

use std::path::PathBuf;

use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use weida_core::{Error, LocalPrincipal, LossCause, PeerIdentity};
use weida_runtime::peer_credentials;

use crate::grouped::Stream;

impl Stream for UnixStream {
    type Endpoint = PathBuf;
    type Principal = LocalPrincipal;
    type Writer = OwnedWriteHalf;
    type Reader = OwnedReadHalf;

    async fn connect(endpoint: &PathBuf) -> Result<UnixStream, Error> {
        UnixStream::connect(endpoint)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused => {
                    Error::ConnectionLost(LossCause::PeerClosed)
                }
                _ => Error::Io(e),
            })
    }

    fn principal(&self) -> Result<LocalPrincipal, Error> {
        peer_credentials(self)
    }

    fn split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        self.into_split()
    }

    /// The uid always; the pid where the platform reports one, and never
    /// the pid alone [0012 §4.2].
    fn same_peer(group: &LocalPrincipal, asking: &LocalPrincipal) -> bool {
        if group.uid != asking.uid {
            return false;
        }
        match (group.pid, asking.pid) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => true,
        }
    }

    fn identity(principal: &LocalPrincipal) -> PeerIdentity {
        PeerIdentity::Local(*principal)
    }

    /// Dropping the write half shuts it down, which is the FIN the reader
    /// sees.
    fn finish(writer: OwnedWriteHalf) {
        drop(writer);
    }

    /// A socket carries no reset code, so abandoning a stream is closing it
    /// [0012 §4.7].
    fn reset(writer: OwnedWriteHalf, _code: u64) {
        drop(writer);
    }

    /// Closing the read side makes the writer's next write `EPIPE`, which
    /// carries no code either [0012 §4.7].
    fn stop(reader: OwnedReadHalf, _code: u64) {
        drop(reader);
    }

    fn read_error(error: std::io::Error) -> Error {
        Error::Transport(format!("local stream read failed: {error}"))
    }
}
