//! Typed endpoints: `Endpoint<Req>` and `Endpoint<Rep>`.
//!
//! The pattern is a type parameter rather than a runtime mode flag, so a
//! requester cannot be asked to accept and a replier cannot be asked to dial
//! (master doc §3). The trait is sealed: patterns are part of the protocol, and
//! v0 defines exactly two.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use weida_core::{EndpointAddr, Error};

use crate::conn::ConnHandle;
use crate::runtime::RuntimeInner;
use crate::transfer::{
    IncomingRequest, IncomingTransfer, OutgoingTransfer, PendingReply, TransferMeta, data_header,
    write_data_preamble,
};

mod sealed {
    pub trait Sealed {}
}

/// A messaging pattern.
pub trait Pattern: sealed::Sealed {
    /// Pattern-specific endpoint state.
    type State;
}

/// The requesting side of Req/Rep.
pub enum Req {}
/// The replying side of Req/Rep.
pub enum Rep {}

impl sealed::Sealed for Req {}
impl Pattern for Req {
    type State = ReqState;
}

impl sealed::Sealed for Rep {}
impl Pattern for Rep {
    type State = RepState;
}

/// A typed messaging endpoint.
pub struct Endpoint<P: Pattern> {
    state: P::State,
}

impl<P: Pattern> Endpoint<P> {
    pub(crate) fn from_state(state: P::State) -> Endpoint<P> {
        Endpoint { state }
    }
}

/// The requesting half of Req/Rep.
pub type Requester = Endpoint<Req>;
/// The replying half of Req/Rep.
pub type Replier = Endpoint<Rep>;

/// One connected peer of a requester.
struct Peer {
    conn: ConnHandle,
    path: Arc<str>,
}

/// State of a requester.
pub struct ReqState {
    runtime: Arc<RuntimeInner>,
    /// Peers accumulate: connecting again adds a peer rather than replacing one
    /// (master doc §5).
    peers: std::sync::Mutex<Vec<Peer>>,
    cursor: AtomicUsize,
}

impl ReqState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>) -> ReqState {
        ReqState {
            runtime,
            peers: std::sync::Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }
}

impl Requester {
    /// Connects to `weida://host:port/path`.
    ///
    /// Connections are pooled per `host:port`, so several endpoints addressing
    /// the same peer share one QUIC connection. Returns once the HELLO
    /// exchange has been negotiated, so the first request cannot race
    /// negotiation.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let addr = EndpointAddr::parse(url)?;
        let conn = self.state.runtime.connect(&addr.host, addr.port).await?;
        self.state
            .peers
            .lock()
            .expect("peer list poisoned")
            .push(Peer {
                conn,
                path: Arc::from(addr.path.as_str()),
            });
        Ok(())
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peers.lock().expect("peer list poisoned").len()
    }

    /// Picks the next peer, round-robin over live connections.
    fn pick_peer(&self) -> Result<(ConnHandle, Arc<str>), Error> {
        let peers = self.state.peers.lock().expect("peer list poisoned");
        if peers.is_empty() {
            return Err(Error::NotConnected);
        }
        let start = self.state.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..peers.len() {
            let peer = &peers[(start + offset) % peers.len()];
            if peer.conn.conn.close_reason().is_none() {
                return Ok((Arc::clone(&peer.conn), Arc::clone(&peer.path)));
            }
        }
        Err(Error::ConnectionLost)
    }

    /// Opens a request stream and the correlated reply slot.
    ///
    /// The transfer is registered with the connection actor *before* the stream
    /// is opened, so an ACK or a reply can never arrive before there is
    /// somewhere to put it.
    pub async fn open(
        &self,
        meta: TransferMeta,
    ) -> Result<(OutgoingTransfer, PendingReply), Error> {
        let (conn, path) = self.pick_peer()?;
        let reserved = conn.register(meta.ack_mode, true).await?;
        let (header, trace) = data_header(Some(&path), reserved.id, None, &meta, None);

        let mut stream = conn.open_uni().await?;
        write_data_preamble(&mut stream, &header).await?;

        let reply = PendingReply::new(
            Arc::clone(&conn),
            reserved.id,
            reserved.reply.expect("registered with a reply slot"),
        );
        let transfer = OutgoingTransfer::new(conn, stream, reserved.id, trace, reserved.outcome);
        Ok((transfer, reply))
    }

    /// Sends `body` as one request and returns the reply stream.
    ///
    /// Convenience over [`Requester::open`] for small payloads; it never
    /// materializes the reply.
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error> {
        self.request_with(TransferMeta::default(), body).await
    }

    /// Like [`Requester::request`], with explicit metadata.
    pub async fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<IncomingTransfer, Error> {
        let (mut transfer, pending) = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish().await?;
        pending.recv().await
    }
}

/// State of a replier.
pub struct RepState {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`; contention is
    /// between deliberate concurrent acceptors only.
    queue: Mutex<mpsc::Receiver<IncomingRequest>>,
}

impl RepState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingRequest>) -> RepState {
        RepState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Replier {
    /// The endpoint path this replier serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next request.
    ///
    /// Requests queue up to `Limits::endpoint_queue`; beyond that the sending
    /// side blocks, which propagates backpressure to the peer through QUIC flow
    /// control rather than growing an unbounded queue (master doc §50).
    pub async fn accept(&self) -> Result<IncomingRequest, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}
