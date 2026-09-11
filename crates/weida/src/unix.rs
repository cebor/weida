//! The `AF_UNIX` transport: `weida+unix://<percent-encoded-path>/<path>`.
//!
//! The second local transport of
//! [decision 0010](../../../docs/decisions/0010-local-transport.md) §4.1, and
//! the first one with a kernel between the peers. `SOCK_STREAM` on a
//! filesystem path, one connection per transfer, no TLS, and the peer proved
//! by `SO_PEERCRED` / `LOCAL_PEERCRED` rather than by a key [0010 §4.4, §4.5].
//!
//! **How connections become a peer** is
//! [decision 0012](../../../docs/decisions/0012-local-connection-grouping.md).
//! An accepted socket cannot be dialled back, so the connections of one peer
//! are grouped instead of multiplexed:
//!
//! ```text
//! byte 0x01            control connection: the peer itself
//!   <- 16 bytes        the group token, issued by the server
//!   then HELLO both ways on this connection
//!
//! byte 0x02 + token    transfer connection: one weida stream
//!   accepted only if the token names a live control connection *and* the
//!   kernel credentials match that connection's [0012 §4.2]
//! ```
//!
//! The token binds connections and resumes nothing: no subscriptions, no
//! sequence position, no dedup window, meaningless once the control
//! connection closes. That is why it is not the session state
//! [0008](../../../docs/decisions/0008-session-identity.md) §4.5 forbids
//! [0012 §4.5].
//!
//! **Dispatch is by path**, as `docs/PROTOCOL.md` §2.1 already specifies for
//! local transports: a transfer connection carries one frame, and the pattern
//! registered at the path it addresses says whether a reply is expected.
//!
//! **Not here yet:** a server cannot open a stream toward a local peer, so
//! Pub/Sub fan-out over this transport is refused with `Unsupported` until the
//! parked reverse connections of [0012 §4.4] land (B-047).

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
use tokio::sync::{Notify, mpsc};
use weida_core::{Error, LocalPrincipal, LossCause, PeerIdentity};
use weida_protocol::codes;

/// First byte of a control connection.
const KIND_CONTROL: u8 = 0x01;
/// First byte of a transfer connection, followed by the 16-byte group token.
const KIND_TRANSFER: u8 = 0x02;
/// Length of a group token [0012 §4.2].
const TOKEN_LEN: usize = 16;

/// Mode of a bound socket file.
///
/// Set explicitly after bind, never inherited: a new socket file gets every
/// permission bit `umask` does not mask, so the default is whatever the
/// process happened to inherit (`docs/research/ipc.md` §1.2, [0010 §4.5]).
const SOCKET_MODE: u32 = 0o600;

const NO_CODE: u64 = u64::MAX;

/// A bound `AF_UNIX` socket: the local counterpart of a QUIC binding.
#[derive(Debug)]
pub(crate) struct UnixBinding {
    path: PathBuf,
}

impl UnixBinding {
    /// Binds `path`, replacing a stale socket file left by a crash.
    ///
    /// **The directory is load-bearing.** Closing a socket does not remove its
    /// node, so a crash leaves one and `bind()` then fails with `EADDRINUSE`;
    /// unlink-then-bind is the usual answer and it opens a substitution race
    /// that is closed only "unless directory ownership and permissions prevent
    /// endpoint substitution" (`docs/research/ipc.md` §1.2, §7). This function
    /// therefore removes a stale node and sets the mode explicitly, and the
    /// caller MUST place the socket in a directory it owns and that no other
    /// user may write. The mode is `0600`; it is set after bind because a
    /// socket file is created with whatever `umask` allows [0010 §4.5].
    pub(crate) fn bind(path: &Path) -> Result<(UnixBinding, TokioUnixListener), Error> {
        // A stale node is a socket nobody is listening on. Anything else at
        // that path is not ours to remove.
        match std::fs::metadata(path) {
            Ok(meta) if is_socket(&meta) => std::fs::remove_file(path).map_err(Error::Io)?,
            Ok(_) => {
                return Err(Error::InvalidAddress(format!(
                    "{} exists and is not a socket",
                    path.display()
                )));
            }
            Err(_) => {}
        }
        let listener = TokioUnixListener::bind(path).map_err(Error::Io)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
            .map_err(Error::Io)?;
        Ok((
            UnixBinding {
                path: path.to_path_buf(),
            },
            listener,
        ))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for UnixBinding {
    fn drop(&mut self) {
        // Leaving the node behind is what forces the next bind into
        // unlink-then-bind; removing it on the way out keeps the common case
        // free of that race.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn is_socket(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    meta.file_type().is_socket()
}

/// The peers a binding has admitted, keyed by their group token.
#[derive(Default)]
pub(crate) struct Groups {
    entries: StdMutex<HashMap<[u8; TOKEN_LEN], Group>>,
}

struct Group {
    /// Credentials of the control connection: what a transfer connection must
    /// match [0012 §4.2].
    principal: LocalPrincipal,
    transfers: mpsc::UnboundedSender<(LocalSend, LocalRecv)>,
}

impl Groups {
    fn insert(
        &self,
        token: [u8; TOKEN_LEN],
        principal: LocalPrincipal,
        transfers: mpsc::UnboundedSender<(LocalSend, LocalRecv)>,
    ) {
        self.entries
            .lock()
            .expect("group registry poisoned")
            .insert(
                token,
                Group {
                    principal,
                    transfers,
                },
            );
    }

    fn remove(&self, token: &[u8; TOKEN_LEN]) {
        self.entries
            .lock()
            .expect("group registry poisoned")
            .remove(token);
    }

    /// Hands a transfer connection to its peer, if the token and the kernel
    /// agree.
    fn admit(
        &self,
        token: &[u8; TOKEN_LEN],
        principal: LocalPrincipal,
        halves: (LocalSend, LocalRecv),
    ) -> bool {
        let entries = self.entries.lock().expect("group registry poisoned");
        let Some(group) = entries.get(token) else {
            return false;
        };
        // The token names the group; the kernel says who is asking. A PID is
        // compared only where the platform reports one, and it is never the
        // *only* thing compared [0012 §4.2].
        if group.principal.uid != principal.uid {
            return false;
        }
        if let (Some(expected), Some(actual)) = (group.principal.pid, principal.pid)
            && expected != actual
        {
            return false;
        }
        group.transfers.send(halves).is_ok()
    }
}

/// Reads the local preamble of an accepted connection.
pub(crate) enum Accepted {
    Control(UnixStream, LocalPrincipal),
    Transfer([u8; TOKEN_LEN], UnixStream, LocalPrincipal),
}

pub(crate) async fn read_accepted(mut stream: UnixStream) -> Result<Accepted, Error> {
    let principal = principal_of(&stream)?;
    let mut kind = [0u8; 1];
    stream.read_exact(&mut kind).await.map_err(Error::Io)?;
    match kind[0] {
        KIND_CONTROL => Ok(Accepted::Control(stream, principal)),
        KIND_TRANSFER => {
            let mut token = [0u8; TOKEN_LEN];
            stream.read_exact(&mut token).await.map_err(Error::Io)?;
            Ok(Accepted::Transfer(token, stream, principal))
        }
        other => Err(Error::Protocol(format!(
            "unknown local connection kind {other:#04x}"
        ))),
    }
}

/// The credentials the kernel attributes to the peer of `stream`.
///
/// Taken at accept/connect time, which is when `SO_PEERCRED` captures them;
/// they are not re-read per message (`docs/research/ipc.md` §1.5).
fn principal_of(stream: &UnixStream) -> Result<LocalPrincipal, Error> {
    let cred = stream.peer_cred().map_err(Error::Io)?;
    Ok(LocalPrincipal {
        uid: cred.uid(),
        gid: cred.gid(),
        // macOS reports no PID at all, and a PID is an observation even where
        // it exists [0010 §4.4].
        pid: cred.pid().map(|pid| pid as u32),
    })
}

/// Serves one accepted control connection: issues the token and builds the
/// accepting side's link.
pub(crate) async fn accept_control(
    mut stream: UnixStream,
    principal: LocalPrincipal,
    groups: Arc<Groups>,
    max_streams: usize,
) -> Result<UnixLink, Error> {
    let token = random_token();
    stream.write_all(&token).await.map_err(Error::Io)?;
    let (transfers_tx, transfers_rx) = mpsc::unbounded_channel();
    groups.insert(token, principal, transfers_tx);
    let mut link = UnixLink::new(
        Side::Accept {
            groups,
            token,
            _principal: principal,
        },
        stream,
        Some(principal),
        max_streams,
    );
    link.transfers = tokio::sync::Mutex::new(Some(transfers_rx));
    Ok(link)
}

/// Hands an accepted transfer connection to the peer its token names, if the
/// kernel agrees that it is the same peer [0012 §4.2].
pub(crate) fn admit_transfer(
    groups: &Groups,
    token: &[u8; TOKEN_LEN],
    principal: LocalPrincipal,
    stream: UnixStream,
) -> bool {
    let (recv, send) = stream.into_split();
    groups.admit(
        token,
        principal,
        (LocalSend::new(send, None), LocalRecv::new(recv, None)),
    )
}

/// Dials `socket`, completing the control handshake of [0012 §4.1].
pub(crate) async fn dial(socket: &Path, max_streams: usize) -> Result<UnixLink, Error> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused => {
                Error::ConnectionLost(LossCause::PeerClosed)
            }
            _ => Error::Io(e),
        })?;
    let principal = principal_of(&stream)?;
    stream.write_all(&[KIND_CONTROL]).await.map_err(Error::Io)?;
    let mut token = [0u8; TOKEN_LEN];
    stream.read_exact(&mut token).await.map_err(Error::Io)?;
    Ok(UnixLink::new(
        Side::Dial {
            socket: socket.to_path_buf(),
            token,
        },
        stream,
        Some(principal),
        max_streams,
    ))
}

fn random_token() -> [u8; TOKEN_LEN] {
    use rand::RngCore;
    let mut token = [0u8; TOKEN_LEN];
    rand::rng().fill_bytes(&mut token);
    token
}

/// Which end of a local connection this is, and what it can do with that.
enum Side {
    /// The dialling side: it may open more connections.
    Dial {
        socket: PathBuf,
        token: [u8; TOKEN_LEN],
    },
    /// The accepting side: it receives connections and opens none
    /// (until the reverse pool of [0012 §4.4] exists).
    Accept {
        groups: Arc<Groups>,
        token: [u8; TOKEN_LEN],
        _principal: LocalPrincipal,
    },
}

/// One peer over `AF_UNIX`: its control connection plus the transfer
/// connections grouped with it.
pub(crate) struct UnixLink {
    side: Side,
    /// The control connection's halves, handed out once each: the first
    /// outbound stream is this side's HELLO, the first inbound one the peer's
    /// [0012 §4.1].
    control_send: StdMutex<Option<OwnedWriteHalf>>,
    control_recv: StdMutex<Option<OwnedReadHalf>>,
    /// Transfer connections the peer opened, for the accepting side.
    transfers: tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<(LocalSend, LocalRecv)>>>,
    peer: Option<LocalPrincipal>,
    live: Arc<AtomicUsize>,
    max_streams: usize,
    closed: AtomicU64,
    closed_notify: Notify,
    id: usize,
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

impl UnixLink {
    fn new(
        side: Side,
        control: UnixStream,
        peer: Option<LocalPrincipal>,
        max_streams: usize,
    ) -> UnixLink {
        let (recv, send) = control.into_split();
        UnixLink {
            side,
            control_send: StdMutex::new(Some(send)),
            control_recv: StdMutex::new(Some(recv)),
            transfers: tokio::sync::Mutex::new(None),
            peer,
            live: Arc::new(AtomicUsize::new(0)),
            max_streams,
            closed: AtomicU64::new(NO_CODE),
            closed_notify: Notify::new(),
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn stable_id(&self) -> usize {
        self.id
    }

    pub(crate) fn peer(&self) -> Option<PeerIdentity> {
        self.peer.map(PeerIdentity::Local)
    }

    pub(crate) fn close_reason(&self) -> Option<Error> {
        let code = self.closed.load(Ordering::Acquire);
        (code != NO_CODE).then(|| match code {
            codes::NEGOTIATION_FAILED => {
                Error::Negotiation("peer closed the connection: negotiation failed".into())
            }
            _ => Error::ConnectionLost(LossCause::PeerClosed),
        })
    }

    pub(crate) fn close(&self, code: u64, _reason: &str) {
        if self
            .closed
            .compare_exchange(NO_CODE, code, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Dropping the control halves is the close on the wire: there is
            // no frame for it and no code to carry [0012 §4.7].
            self.control_send.lock().expect("poisoned").take();
            self.control_recv.lock().expect("poisoned").take();
            if let Side::Accept { groups, token, .. } = &self.side {
                groups.remove(token);
            }
            self.closed_notify.notify_waiters();
        }
    }

    pub(crate) async fn closed(&self) -> Error {
        loop {
            if let Some(reason) = self.close_reason() {
                return reason;
            }
            self.closed_notify.notified().await;
        }
    }

    fn slot(&self) -> Result<StreamSlot, Error> {
        let live = self.live.fetch_add(1, Ordering::Relaxed);
        if live >= self.max_streams {
            self.live.fetch_sub(1, Ordering::Relaxed);
            return Err(Error::LimitExceeded);
        }
        Ok(StreamSlot {
            live: Arc::clone(&self.live),
        })
    }

    /// Opens one transfer connection, carrying one weida stream.
    ///
    /// The control connection's own write half is never handed out here: it
    /// belongs to `open_control`, so that a fan-out copy can never take the
    /// stream the HELLO is owed.
    pub(crate) async fn open_uni(&self) -> Result<LocalSend, Error> {
        let (send, _recv) = self.open_transfer().await?;
        Ok(send)
    }

    /// Hands out the control connection's write half, once.
    ///
    /// This is the one stream each side has toward the other without dialling
    /// anything, and both sides spend it on HELLO ([0012 §4.1]).
    pub(crate) fn open_control(&self) -> Result<LocalSend, Error> {
        match self.take_control_send() {
            Some(send) => Ok(LocalSend::new(send, None)),
            None => Err(Error::Transport("control stream already used".into())),
        }
    }

    pub(crate) async fn open_bi(&self) -> Result<(LocalSend, LocalRecv), Error> {
        self.open_transfer().await
    }

    fn take_control_send(&self) -> Option<OwnedWriteHalf> {
        self.control_send.lock().expect("poisoned").take()
    }

    async fn open_transfer(&self) -> Result<(LocalSend, LocalRecv), Error> {
        if let Some(closed) = self.close_reason() {
            return Err(closed);
        }
        let (socket, token) = match &self.side {
            Side::Dial { socket, token } => (socket, token),
            // A server cannot dial a peer that dialled it. Fan-out over this
            // transport waits for the parked reverse connections of
            // [0012 §4.4]; refusing is the honest answer until then.
            Side::Accept { .. } => {
                return Err(Error::Unsupported);
            }
        };
        let slot = self.slot()?;
        let mut stream = UnixStream::connect(socket).await.map_err(Error::Io)?;
        let mut preamble = [0u8; 1 + TOKEN_LEN];
        preamble[0] = KIND_TRANSFER;
        preamble[1..].copy_from_slice(token);
        stream.write_all(&preamble).await.map_err(Error::Io)?;
        let (recv, send) = stream.into_split();
        let slot = Arc::new(slot);
        Ok((
            LocalSend::new(send, Some(Arc::clone(&slot))),
            LocalRecv::new(recv, Some(slot)),
        ))
    }

    pub(crate) async fn accept_uni(&self) -> Result<LocalRecv, Error> {
        if let Some(recv) = self.control_recv.lock().expect("poisoned").take() {
            return Ok(LocalRecv::new(recv, None));
        }
        // Everything else arrives as a transfer connection, which is
        // dispatched by path rather than by stream kind [0012 §4.3].
        Err(self.closed().await)
    }

    /// Transfer connections, for the accepting side.
    pub(crate) async fn accept_bi(&self) -> Result<(LocalSend, LocalRecv), Error> {
        let mut queue = self.transfers.lock().await;
        let Some(queue) = queue.as_mut() else {
            return Err(self.closed().await);
        };
        tokio::select! {
            accepted = queue.recv() => accepted.ok_or(Error::ConnectionLost(LossCause::PeerClosed)),
            reason = self.closed() => Err(reason),
        }
    }
}

/// Keeps one live transfer connection counted against `max_local_streams`.
struct StreamSlot {
    live: Arc<AtomicUsize>,
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The writing half of one local stream.
pub(crate) struct LocalSend {
    io: Option<OwnedWriteHalf>,
    _slot: Option<Arc<StreamSlot>>,
}

impl LocalSend {
    fn new(io: OwnedWriteHalf, slot: Option<Arc<StreamSlot>>) -> LocalSend {
        LocalSend {
            io: Some(io),
            _slot: slot,
        }
    }

    pub(crate) fn io_mut(&mut self) -> Option<&mut OwnedWriteHalf> {
        self.io.as_mut()
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        let Some(io) = self.io.as_mut() else {
            return Err(Error::Transport("stream already closed".into()));
        };
        io.write_all(buf).await.map_err(|e| match e.kind() {
            // The peer closed its read side: the local equivalent of
            // `STOP_SENDING`, without a code to carry [0012 §4.7].
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset => Error::Canceled,
            _ => Error::Transport(format!("local stream write failed: {e}")),
        })
    }

    /// Ends the payload: dropping the write half shuts it down, which is the
    /// FIN the reader sees.
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        if self.io.take().is_none() {
            return Err(Error::Transport("stream already closed".into()));
        }
        Ok(())
    }

    /// A local stream carries no reset code, so abandoning it is closing it.
    pub(crate) fn reset(&mut self, _code: u64) {
        self.io = None;
    }

    /// The receipt: on a socket the bytes are the peer's transport's as soon
    /// as `write_all` returned, and the FIN follows when this half drops, so
    /// there is nothing left to wait for (`docs/GUARANTEES.md` §3).
    ///
    /// **Named loss:** a local stream carries no code, so a refusal cannot be
    /// reported here the way `STOP_SENDING` is on QUIC. A peer that refuses
    /// surfaces on the *write*, as `Error::Canceled`, not on the receipt
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.7).
    pub(crate) fn stopped(
        &self,
    ) -> impl Future<Output = Result<Option<u64>, Error>> + Send + Sync + use<> {
        async move { Ok(None) }
    }
}

/// The reading half of one local stream.
pub(crate) struct LocalRecv {
    io: Option<OwnedReadHalf>,
    _slot: Option<Arc<StreamSlot>>,
}

impl LocalRecv {
    fn new(io: OwnedReadHalf, slot: Option<Arc<StreamSlot>>) -> LocalRecv {
        LocalRecv {
            io: Some(io),
            _slot: slot,
        }
    }

    pub(crate) fn io_mut(&mut self) -> Option<&mut OwnedReadHalf> {
        self.io.as_mut()
    }

    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Error> {
        let Some(io) = self.io.as_mut() else {
            return Ok(None);
        };
        let read = io
            .read(buf)
            .await
            .map_err(|e| Error::Transport(format!("local stream read failed: {e}")))?;
        Ok((read > 0).then_some(read))
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.read(&mut buf[filled..]).await? {
                Some(n) => filled += n,
                None => return Err(Error::Protocol("stream ended mid-header".into())),
            }
        }
        Ok(())
    }

    /// Refuses the rest of the payload by closing the read side; the writer
    /// sees `EPIPE`, which carries no code [0012 §4.7].
    pub(crate) fn stop(&mut self, _code: u64) {
        self.io = None;
    }
}
