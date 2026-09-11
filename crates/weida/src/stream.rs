//! L0: the stream core.
//!
//! This is the socket replacement, ZeroMQ's idea rebuilt directly on QUIC. Its
//! primitives are the two stream kinds QUIC gives us and nothing more:
//!
//! * a **unidirectional stream** — bytes in one direction, ended by a FIN, with
//!   a transport receipt ([`crate::Delivery`]) and refusal by stop code;
//! * a **bidirectional stream** — an *exchange*: the initiating half carries a
//!   request, the return half carries a reply or an ERROR.
//!
//! The guarantees are exactly QUIC's: in-order bytes within a stream, no order
//! across streams, flow control per stream and per connection, and cancellation
//! via reset/stop. Nothing here invents delivery semantics on top of that; the
//! patterns in [`crate::endpoint`] are thin wrappers over these two calls, and
//! broker semantics (queues, publisher confirms, consumer acks) belong to a
//! layer that does not exist yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use weida_core::{Address, Error, LossCause};

use crate::config::ClientTls;
use crate::conn::ConnHandle;
use crate::runtime::RuntimeInner;
use crate::transfer::{
    IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta, data_header,
    write_data_preamble,
};

/// One connected peer: the connection and the path that was dialled on it.
struct PeerEntry {
    conn: ConnHandle,
    path: Arc<str>,
}

/// A dialling endpoint's peers plus its selection policy (primitive P3).
///
/// Peers accumulate: connecting again adds a peer rather than replacing one
/// (master doc §5). Shared by every dialling pattern — the policy differs (Req
/// and Push pick one, Sub addresses all of them), the set does not.
///
/// Dead peers are reaped whenever a live one is added, so a process that
/// reconnects after every loss holds at most one dead entry per loss until
/// its next `connect` — bounded by the application's own reconnect rate,
/// never growing with uptime.
pub(crate) struct PeerSet {
    peers: std::sync::Mutex<Vec<PeerEntry>>,
    cursor: AtomicUsize,
}

impl PeerSet {
    fn new() -> PeerSet {
        PeerSet {
            peers: std::sync::Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }

    fn add(&self, conn: ConnHandle, path: &str) {
        let mut peers = self.peers.lock().expect("peer list poisoned");
        peers.retain(|p| p.conn.conn.close_reason().is_none());
        peers.push(PeerEntry {
            conn,
            path: Arc::from(path),
        });
    }

    /// Number of peers whose connection is still open.
    fn len(&self) -> usize {
        self.peers
            .lock()
            .expect("peer list poisoned")
            .iter()
            .filter(|p| p.conn.conn.close_reason().is_none())
            .count()
    }

    /// Picks the next live peer, round-robin.
    ///
    /// When every peer is closed, the error says **why** the last one
    /// examined died rather than flattening all of them into a bare
    /// `ConnectionLost`: an application deciding whether to redial needs to
    /// tell an idle timeout from a peer that closed deliberately, and this is
    /// the only place that knowledge was being thrown away.
    fn pick(&self) -> Result<(ConnHandle, Arc<str>), Error> {
        let peers = self.peers.lock().expect("peer list poisoned");
        if peers.is_empty() {
            return Err(Error::NotConnected);
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let mut cause = None;
        for offset in 0..peers.len() {
            let peer = &peers[(start + offset) % peers.len()];
            match peer.conn.conn.close_reason() {
                None => return Ok((Arc::clone(&peer.conn), Arc::clone(&peer.path))),
                Some(reason) => cause = Some(reason),
            }
        }
        // A closed connection always has a reason, so the fallback is
        // unreachable in practice and exists to keep the mapping total.
        Err(cause.unwrap_or(Error::ConnectionLost(LossCause::LocallyClosed)))
    }

    /// Runs `f` for every peer whose connection is still open.
    fn for_each_live(&self, mut f: impl FnMut(&ConnHandle, &str)) {
        let peers = self.peers.lock().expect("peer list poisoned");
        for peer in peers.iter() {
            if peer.conn.conn.close_reason().is_none() {
                f(&peer.conn, &peer.path);
            }
        }
    }
}

/// The dialling side of the stream core.
///
/// A `Peer` owns a set of connections and the trust anchors they were
/// authenticated against, and opens streams on them. Every dialling pattern —
/// Req, Push, Sub — is this plus a selection policy and a bit of vocabulary.
pub struct Peer {
    runtime: Arc<RuntimeInner>,
    tls: Arc<ClientTls>,
    peers: PeerSet,
}

impl Peer {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> Peer {
        Peer {
            runtime,
            tls,
            peers: PeerSet::new(),
        }
    }

    /// Connects to `weida://[fingerprint@]host:port/path`.
    ///
    /// Connections are pooled per `host:port`, trust configuration and the
    /// fingerprint the address names, so several endpoints addressing the
    /// same peer on the same terms share one QUIC connection while two
    /// different sets of terms never do. Returns once the HELLO exchange has
    /// been negotiated, so the first stream cannot race negotiation.
    ///
    /// A peer that answers with an identity the terms do not cover fails with
    /// [`Error::Untrusted`] carrying the fingerprint it presented.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.dial(url).await.map(|_| ())
    }

    /// Number of peers whose connection is still open.
    ///
    /// A peer that went away no longer counts; nothing reconnects it, so the
    /// number drops until the application calls [`Peer::connect`] again.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Opens a unidirectional stream to the next peer, round-robin.
    ///
    /// The DATA header is written before this returns, so the caller gets a
    /// handle that is already addressed and can only carry payload.
    ///
    /// Under a negotiated `PerProducer` ordering the header also carries this
    /// producer's next sequence number for the dialled path (DATA key `6`);
    /// under `core` it carries none and the sequencer is never touched.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        let (conn, path) = self.peers.pick()?;
        let (mut header, trace) = data_header(Some(&path), &meta, None);
        header.sequence = conn.sequencer.next(&path);
        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;
        Ok(OutgoingTransfer::new(stream, trace, conn))
    }

    /// Opens a bidirectional stream — an exchange — to the next peer.
    ///
    /// The returned [`ReplyStream`] is the return half. It needs no correlation
    /// identifier: it *is* the correlation, and dropping it cancels the reply.
    pub async fn open_bi(
        &self,
        meta: TransferMeta,
    ) -> Result<(OutgoingTransfer, ReplyStream), Error> {
        let (conn, path) = self.peers.pick()?;
        let (header, trace) = data_header(Some(&path), &meta, None);
        let (mut send, recv) = conn.open_bi().await?;
        // The peer learns of the stream with this write, so it never sees a
        // bidirectional stream it cannot classify.
        write_data_preamble(&mut send, &header).await?;
        Ok((
            OutgoingTransfer::new(send, trace, Arc::clone(&conn)),
            ReplyStream::new(recv, conn),
        ))
    }

    /// Dials and records a peer, returning the connection and the path.
    ///
    /// The scheme picks the transport and nothing falls back to anything
    /// else: `weida://` dials QUIC on `tls`'s terms, `weida+inproc://`
    /// reaches a bus in this process with no TLS at all
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.6, §4.8). The trust configuration is simply unused on a local
    /// address — there is no key to check.
    ///
    /// Crate-internal because a `Sub` needs the connection handle itself: it
    /// registers its path in that connection's namespace so fanned-out copies
    /// have somewhere to land.
    pub(crate) async fn dial(&self, url: &str) -> Result<(ConnHandle, Arc<str>), Error> {
        let (conn, path) = match Address::parse(url)? {
            Address::Quic(addr) => {
                let conn = self
                    .runtime
                    .connect(&addr.host, addr.port, &self.tls, addr.peer)
                    .await?;
                (conn, addr.path)
            }
            Address::Inproc(addr) => {
                let conn = self.runtime.connect_local(&addr.bus).await?;
                (conn, addr.path)
            }
        };
        self.peers.add(ConnHandle::clone(&conn), &path);
        Ok((conn, Arc::from(path.as_str())))
    }

    pub(crate) fn for_each_live(&self, f: impl FnMut(&ConnHandle, &str)) {
        self.peers.for_each_live(f);
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("peer_count", &self.peer_count())
            .finish_non_exhaustive()
    }
}

/// One inbound stream, whichever kind the peer opened.
#[derive(Debug)]
pub enum Incoming {
    /// A unidirectional stream: payload only, no reply half.
    Stream(IncomingTransfer),
    /// A bidirectional stream: an exchange that owes a reply or an ERROR.
    Exchange(IncomingRequest),
}

/// The bound side of the stream core.
///
/// One path, both stream kinds, one queue. Where a [`crate::Replier`] accepts
/// exchanges and a [`crate::Puller`] receives one-way transfers, an `Acceptor`
/// takes whatever arrives and lets the application decide.
pub struct Acceptor {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`; contention is
    /// between deliberate concurrent acceptors only.
    queue: Mutex<mpsc::Receiver<Incoming>>,
}

impl Acceptor {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<Incoming>) -> Acceptor {
        Acceptor {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }

    /// The endpoint path this acceptor serves.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Waits for the next inbound stream.
    ///
    /// Bounded by `Limits::endpoint_queue`, so a full queue stalls the peer
    /// through QUIC flow control instead of growing without limit.
    pub async fn accept(&self) -> Result<Incoming, Error> {
        let mut queue = self.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptor")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
