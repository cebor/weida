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
    IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta,
    outgoing_header, write_data_preamble,
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
        let (mut header, trace, cursors) = outgoing_header(&conn, Some(&path), &meta, None)?;
        header.sequence = conn.sequencer.next(&path);
        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;
        Ok(OutgoingTransfer::new(stream, trace, conn, cursors))
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
        let (header, trace, cursors) = outgoing_header(&conn, Some(&path), &meta, None)?;
        let (mut send, recv) = conn.open_bi().await?;
        // The peer learns of the stream with this write, so it never sees a
        // bidirectional stream it cannot classify.
        write_data_preamble(&mut send, &header).await?;
        Ok((
            OutgoingTransfer::new(send, trace, Arc::clone(&conn), cursors),
            ReplyStream::new(recv, conn),
        ))
    }

    /// The runtime this peer's tasks run on.
    pub(crate) fn exec(&self) -> &crate::runtime::Exec {
        &self.runtime.exec
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
                // B-017: the pool keys on the dialled path as well as the
                // authority, and checks the fingerprint it gets back.
                let conn = self.runtime.connect(&addr, &self.tls).await?;
                (conn, addr.path)
            }
            Address::Inproc(addr) => {
                let conn = self.runtime.connect_local(&addr.bus).await?;
                (conn, addr.path)
            }
            #[cfg(unix)]
            Address::Unix(addr) => {
                let conn = self.runtime.connect_unix(&addr.socket).await?;
                (conn, addr.path)
            }
            #[cfg(not(unix))]
            Address::Unix(addr) => {
                // The address parses everywhere — it is data — but the
                // transport exists only where the kernel offers `AF_UNIX`.
                // Refused by name rather than falling through to another
                // transport [0010 §4.6].
                return Err(Error::InvalidAddress(format!(
                    "{}: AF_UNIX is not available on this platform",
                    addr.socket
                )));
            }
            #[cfg(windows)]
            Address::Pipe(addr) => {
                let conn = self.runtime.connect_pipe(&addr).await?;
                (conn, addr.path)
            }
            #[cfg(not(windows))]
            Address::Pipe(addr) => {
                return Err(Error::InvalidAddress(format!(
                    "{}: named pipes are not available on this platform",
                    addr.name
                )));
            }
        };
        self.peers.add(ConnHandle::clone(&conn), &path);
        Ok((conn, Arc::from(path.as_str())))
    }

    pub(crate) fn for_each_live(&self, f: impl FnMut(&ConnHandle, &str)) {
        self.peers.for_each_live(f);
    }
}

/// Opens one exchange on a **named** connection.
///
/// [`Peer::open_bi`] picks one peer, which is the selection policy Req/Rep
/// wants. A pattern that addresses *every* peer — a survey — needs the same
/// write against a connection it chose itself, so the body lives here once
/// rather than twice.
pub(crate) async fn open_exchange_on(
    conn: &ConnHandle,
    path: &str,
    meta: TransferMeta,
) -> Result<(OutgoingTransfer, ReplyStream), Error> {
    let (header, trace, cursors) = outgoing_header(conn, Some(path), &meta, None)?;
    let (mut send, recv) = conn.open_bi().await?;
    write_data_preamble(&mut send, &header).await?;
    Ok((
        OutgoingTransfer::new(send, trace, ConnHandle::clone(conn), cursors),
        ReplyStream::new(recv, ConnHandle::clone(conn)),
    ))
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("peer_count", &self.peer_count())
            .finish_non_exhaustive()
    }
}

/// One inbound thing on a raw acceptor's path, whichever kind the peer sent.
///
/// The two stream kinds are what an [`Acceptor`] was built for. The three
/// subscription events exist because an L2 queue is served on such a path
/// (`weida-broker`): a consumer registers with SUBSCRIBE like a subscriber,
/// grants credit with the frame of
/// [0003](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
/// §4.2, and its subscription ends with UNSUBSCRIBE or with its connection.
/// They arrive **in the order the peer caused them** per connection, which is
/// what lets a queue apply a grant to a subscription it has already seen.
#[derive(Debug)]
pub enum Incoming {
    /// A unidirectional stream: payload only, no reply half.
    Stream(IncomingTransfer),
    /// A bidirectional stream: an exchange that owes a reply or an ERROR.
    Exchange(IncomingRequest),
    /// A consumer subscribed to this path with one filter.
    ///
    /// One event per SUBSCRIBE frame, so a consumer holding several filters
    /// arrives several times with the same [`Consumer::id`].
    Subscribed(Consumer),
    /// A consumer stated the absolute delivery limit of one subscription.
    Credit(CreditGrant),
    /// A subscription ended: UNSUBSCRIBE, or the connection went away.
    ///
    /// A closed connection reports `filter: None` — it ends every
    /// subscription that connection held, and the peer is gone, so there is
    /// nothing to enumerate them against.
    Unsubscribed {
        /// Which consumer.
        id: ConsumerId,
        /// Which filter, or `None` for the whole connection.
        filter: Option<String>,
    },
}

/// Identifies one consuming connection on one path.
///
/// Stable for the life of the connection and never reused while it lives; it
/// is the peer's QUIC connection id, so it says nothing about *who* the peer
/// is — that is `IncomingMeta::peer`, proved in the handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(pub(crate) usize);

impl ConsumerId {
    pub(crate) fn from_conn(conn_id: usize) -> ConsumerId {
        ConsumerId(conn_id)
    }
}

/// A consumer's absolute delivery limit for one subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreditGrant {
    /// Which consumer granted it.
    pub id: ConsumerId,
    /// Which subscription: the filter it was granted for.
    pub filter: String,
    /// Messages that subscription will accept in total, counted from its
    /// creation.
    ///
    /// Absolute and cumulative, so a duplicate changes nothing. A receiver
    /// keeps the **highest** limit it has seen for the subscription, which is
    /// what makes a reordered grant harmless: control frames ride their own
    /// unidirectional streams and QUIC does not order those relative to each
    /// other.
    pub limit: u64,
}

/// One subscribed consumer, and the way to deliver to it.
///
/// Delivery is a one-way transfer on a fresh unidirectional stream, addressed
/// to the path the consumer subscribed on — the same shape a publisher's
/// fan-out copy has, because a consumer *is* an ordinary subscriber on the
/// wire (0018 §4.5). What differs is the selection: a queue picks one
/// consumer, a publisher writes to all of them.
#[derive(Clone)]
pub struct Consumer {
    conn: ConnHandle,
    path: Arc<str>,
    filter: String,
}

impl Consumer {
    pub(crate) fn new(conn: ConnHandle, path: Arc<str>, filter: String) -> Consumer {
        Consumer { conn, path, filter }
    }

    /// Which consuming connection this is.
    pub fn id(&self) -> ConsumerId {
        ConsumerId(self.conn.conn.stable_id())
    }

    /// The filter this subscription was created with.
    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// The path it subscribed on.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Opens one delivery to this consumer.
    ///
    /// The transfer is addressed to the subscribed path, so it arrives at the
    /// consumer's own route for it — a [`crate::Subscriber`], which needs no
    /// new API to receive a queue's delivery. The topic a consumer filters on
    /// travels in `meta`, so a delivery carries the label the producer gave
    /// the message rather than one this hop invents.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        let mut send = self.conn.open_uni().await?;
        let (header, trace, cursors) = outgoing_header(&self.conn, Some(&self.path), &meta, None)?;
        crate::transfer::write_data_preamble(&mut send, &header).await?;
        Ok(OutgoingTransfer::new(
            send,
            trace,
            ConnHandle::clone(&self.conn),
            cursors,
        ))
    }

    /// Delivers `body` as one whole transfer.
    pub async fn deliver(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        Ok(())
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("id", &self.id())
            .field("path", &self.path)
            .field("filter", &self.filter)
            .finish()
    }
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
