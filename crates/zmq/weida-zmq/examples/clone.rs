//! The zguide's **Clone** pattern, chapter 5, on the wire
//! [12/CHP](https://rfc.zeromq.org/spec/12/) defines: `clonesrv4.c` and
//! `cloneclt4.c` - the snapshot dialog, the update stream, client writes and
//! the idle beat.
//!
//! ```text
//! cargo run -p weida-zmq --example clone
//! ```
//!
//! CHP's own architecture, and the reason all six commands share one frame
//! layout: "the server binds ROUTER at P, PUB at P+1, SUB at P+2; the client
//! connects DEALER to P, SUB to P+1, optionally PUB to P+2". Every message is
//! a `kvmsg`:
//!
//! ```text
//! frame 0: key         - a key name, or a command name for ICANHAZ?/KTHXBAI/HUGZ
//! frame 1: sequence    - 8 octets, network byte order
//! frame 2: uuid        - the update's identity, empty where there is none
//! frame 3: properties  - name=value\n pairs, and the subtree in a snapshot dialog
//! frame 4: value       - the body; empty in a KVSET means delete
//! ```
//!
//! The six commands, with the clauses this file implements:
//!
//! * `ICANHAZ?` plus a subtree, from the client's DEALER, "is answered by
//!   zero or more `KVSYNC` and then `KTHXBAI` carrying the highest sequence".
//! * `KVPUB` "carries updates under the strict-increment discard rule": a
//!   client applies an update only when its sequence is **greater** than the
//!   one it holds, which is what makes subscribing before the snapshot safe -
//!   the updates that raced the snapshot are already in it.
//! * `HUGZ` "beats about once a second when idle", and its absence a client
//!   "MAY treat as an indicator that the server has crashed".
//! * `KVSET` "comes from clients, with an empty value meaning delete and a
//!   `ttl` property meaning expire" - and the server "centralizes every
//!   change and imposes one sequence in arrival order", which is why clients
//!   do not publish to each other: "competing writes to the same key can
//!   leave clients with different values".
//!
//! # Which surface, and why
//!
//! **Async.** The server is three sockets in one `zloop` in the C - snapshot
//! requests, client updates and a timer - and a client must read its SUB
//! socket while talking on its DEALER. There is no blocking shape for either.
//!
//! # The three differences from the C
//!
//! * **The snapshot is a call, not a hidden state machine.** `cloneclt4.c`
//!   drives the dialog inside `zloop` callbacks; here it is
//!   `CloneClient::snapshot`, and the client's own sequence and discard count
//!   are fields a test can read. The order the RFC requires - subscribe,
//!   *then* ask - is the constructor's job, so it cannot be got wrong by a
//!   caller.
//! * **No `ttl` expiry thread.** The `ttl` property is parsed and carried,
//!   and an empty value deletes, but ephemeral expiry is Clone Model Five
//!   and a reactor of its own; the property is preserved rather than
//!   pretended.
//! * **The port triple is found, not configured.** The C takes `P` on the
//!   command line and trusts P+1 and P+2 to be free; a test cannot, so
//!   [`clone_server`] binds P and then P+1 and P+2, retrying with a new P
//!   when either is taken. The convention is the same; the search is what
//!   makes it runnable twice at once.
//!
//! The `uuid` frame is a client-generated identity on every update, which is
//! Clone Model Six's device for recognising its own writes coming back; it is
//! carried here and echoed in the `KVPUB`, so a reader can see the round
//! trip.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, DealerSocket, Error, Message, Multipart, PubSocket, Result,
    RouterSocket, SubSocket,
};

/// `#define ICANHAZ "ICANHAZ?"`: the snapshot request, in the key frame.
pub const ICANHAZ: &[u8] = b"ICANHAZ?";
/// The snapshot terminator, carrying the highest sequence sent.
pub const KTHXBAI: &[u8] = b"KTHXBAI";
/// The idle beat.
pub const HUGZ: &[u8] = b"HUGZ";

/// CHP's idle beat interval: "about once a second".
pub const HUGZ_INTERVAL: Duration = Duration::from_secs(1);

/// One CHP message, the same five frames for all six commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvMsg {
    /// A key name, or a command name for `ICANHAZ?`, `KTHXBAI` and `HUGZ`.
    pub key: Vec<u8>,
    /// The server's sequence number; zero where the sender has none.
    pub sequence: u64,
    /// The update's identity, client-generated, empty where there is none.
    pub uuid: Vec<u8>,
    /// `name=value\n` pairs - the subtree in a snapshot dialog, `ttl` on an
    /// ephemeral value.
    pub properties: Vec<u8>,
    /// The body. Empty in a `KVSET` means delete.
    pub value: Vec<u8>,
}

impl KvMsg {
    /// A message with only a key and a value, which is what most updates are.
    pub fn new(key: &str, value: &[u8]) -> KvMsg {
        KvMsg {
            key: key.as_bytes().to_vec(),
            sequence: 0,
            uuid: Vec::new(),
            properties: Vec::new(),
            value: value.to_vec(),
        }
    }

    /// The key as text, for a map and for printing.
    pub fn key_text(&self) -> String {
        String::from_utf8_lossy(&self.key).into_owned()
    }

    /// One named property, `name=value\n` as CHP writes them.
    pub fn property(&self, name: &str) -> Option<String> {
        String::from_utf8_lossy(&self.properties)
            .lines()
            .find_map(|line| {
                line.strip_prefix(&format!("{name}="))
                    .map(std::borrow::ToOwned::to_owned)
            })
    }

    /// Sets one property.
    pub fn set_property(&mut self, name: &str, value: &str) {
        self.properties
            .extend_from_slice(format!("{name}={value}\n").as_bytes());
    }

    /// The five frames, with the sequence in network byte order.
    pub fn encode(&self) -> Multipart {
        Multipart::new(vec![
            Message::from(self.key.clone()),
            Message::from(self.sequence.to_be_bytes().to_vec()),
            Message::from(self.uuid.clone()),
            Message::from(self.properties.clone()),
            Message::from(self.value.clone()),
        ])
        .expect("five frames")
    }

    /// The five frames back.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for anything that is not five frames with an
    /// eight-octet sequence, because a `kvmsg` of another shape is a peer
    /// speaking a different protocol and not a recoverable message.
    pub fn decode(message: &Multipart) -> Result<KvMsg> {
        let frames = message.frames();
        if frames.len() != 5 || frames[1].as_slice().len() != 8 {
            return Err(Error::ENOCOMPATPROTO(
                "a CHP kvmsg is five frames with an eight-octet sequence".into(),
            ));
        }
        let mut sequence = [0u8; 8];
        sequence.copy_from_slice(frames[1].as_slice());
        Ok(KvMsg {
            key: frames[0].as_slice().to_vec(),
            sequence: u64::from_be_bytes(sequence),
            uuid: frames[2].as_slice().to_vec(),
            properties: frames[3].as_slice().to_vec(),
            value: frames[4].as_slice().to_vec(),
        })
    }
}

/// The three ports CHP's convention fixes: P, P+1, P+2.
#[derive(Clone, Debug)]
pub struct Ports {
    /// The ROUTER at P, for snapshot requests.
    pub snapshot: String,
    /// The PUB at P+1, for updates and beats.
    pub publisher: String,
    /// The SUB at P+2, for client updates.
    pub collector: String,
}

/// A running CHP server.
pub struct CloneServer {
    /// Where a client connects its three sockets.
    pub ports: Ports,
    /// The sequence the server has reached - "one sequence in arrival order".
    pub sequence: Arc<AtomicU64>,
    /// Beats sent, so a test can wait for the stream to be live.
    pub beats: Arc<AtomicU64>,
    _running: tokio::task::JoinHandle<()>,
}

/// `clonesrv4.c`: ROUTER at P for snapshots, PUB at P+1 for updates and
/// beats, SUB at P+2 for client writes, and the one authoritative sequence.
///
/// # Errors
///
/// `EADDRINUSE` when no consecutive port triple was free after several
/// attempts, and whatever binding a socket reports.
pub async fn clone_server(context: &Context, hugz: Duration) -> Result<CloneServer> {
    //  P is picked by the OS, P+1 and P+2 have to be free: CHP's convention
    //  is three consecutive ports and a wildcard bind only gives one.
    let mut attempt = 0;
    let (mut snapshot, mut publisher, mut collector, ports) = loop {
        let snapshot = RouterSocket::new(context)?;
        let base = snapshot.bind("tcp://127.0.0.1:0").await?;
        let base_port: u16 = base
            .to_string()
            .rsplit(':')
            .next()
            .and_then(|port| port.parse().ok())
            .expect("a tcp endpoint ends in a port");
        let publisher_endpoint = format!("tcp://127.0.0.1:{}", base_port + 1);
        let collector_endpoint = format!("tcp://127.0.0.1:{}", base_port + 2);
        let publisher = PubSocket::new(context)?;
        let mut collector = SubSocket::new(context)?;
        collector.subscribe("")?;
        if publisher.bind(&publisher_endpoint).await.is_ok()
            && collector.bind(&collector_endpoint).await.is_ok()
        {
            break (
                snapshot,
                publisher,
                collector,
                Ports {
                    snapshot: base.to_string(),
                    publisher: publisher_endpoint,
                    collector: collector_endpoint,
                },
            );
        }
        attempt += 1;
        if attempt == 16 {
            return Err(Error::EADDRINUSE(
                "no free consecutive port triple for CHP's P/P+1/P+2".into(),
            ));
        }
    };

    let sequence = Arc::new(AtomicU64::new(0));
    let beats = Arc::new(AtomicU64::new(0));
    let running = tokio::spawn({
        let sequence = Arc::clone(&sequence);
        let beats = Arc::clone(&beats);
        async move {
            let mut state: HashMap<String, KvMsg> = HashMap::new();
            loop {
                tokio::select! {
                    //  A client write: the server is the single ordering
                    //  point, so the sequence is assigned here and nowhere
                    //  else.
                    arrived = collector.recv() => {
                        let Ok(message) = arrived else { return };
                        let Ok(mut update) = KvMsg::decode(&message) else { continue };
                        let next = sequence.fetch_add(1, Ordering::Relaxed) + 1;
                        update.sequence = next;
                        let key = update.key_text();
                        if update.value.is_empty() {
                            //  "an empty value meaning delete"
                            state.remove(&key);
                        } else {
                            state.insert(key, update.clone());
                        }
                        //  KVPUB: the same message the client sent, now
                        //  numbered, to everyone including its author.
                        publisher.publish(update.encode());
                    }
                    arrived = snapshot.recv() => {
                        let Ok(message) = arrived else { return };
                        let mut frames = message.into_frames();
                        if frames.is_empty() {
                            continue;
                        }
                        let identity = frames.remove(0);
                        let Ok(request) = Multipart::new(frames) else { continue };
                        let Ok(request) = KvMsg::decode(&request) else { continue };
                        if request.key.as_slice() != ICANHAZ {
                            continue;
                        }
                        let subtree = request.property("subtree").unwrap_or_default();
                        //  "zero or more KVSYNC"
                        for entry in state.values() {
                            if !entry.key_text().starts_with(&subtree) {
                                continue;
                            }
                            let mut frames = vec![identity.clone()];
                            frames.extend(entry.encode().into_frames());
                            let Ok(sync) = Multipart::new(frames) else { continue };
                            let _ = snapshot.send(sync).await;
                        }
                        //  "and then KTHXBAI carrying the highest sequence"
                        let mut goodbye = KvMsg::new("", b"");
                        goodbye.key = KTHXBAI.to_vec();
                        goodbye.sequence = sequence.load(Ordering::Relaxed);
                        goodbye.set_property("subtree", &subtree);
                        let mut frames = vec![identity];
                        frames.extend(goodbye.encode().into_frames());
                        let Ok(goodbye) = Multipart::new(frames) else { continue };
                        let _ = snapshot.send(goodbye).await;
                    }
                    () = tokio::time::sleep(hugz) => {
                        let mut beat = KvMsg::new("", b"");
                        beat.key = HUGZ.to_vec();
                        publisher.publish(beat.encode());
                        beats.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });

    Ok(CloneServer {
        ports,
        sequence,
        beats,
        _running: running,
    })
}

/// `cloneclt4.c`: the three client sockets, the local map, and the sequence
/// the strict-increment rule is measured against.
pub struct CloneClient {
    snapshot: DealerSocket,
    updates: SubSocket,
    collector: PubSocket,
    subtree: String,
    /// The client's copy of the server's state.
    pub map: HashMap<String, Vec<u8>>,
    /// The highest sequence this client has applied.
    pub sequence: u64,
    /// Updates thrown away by the strict-increment rule.
    pub discarded: usize,
    /// `HUGZ` beats seen - the liveness signal, and a handy sign that the
    /// subscription is live.
    pub beats: usize,
}

impl CloneClient {
    /// Connects all three sockets, **subscribing before asking for the
    /// snapshot**, which is the order CHP requires: "the client subscribing
    /// *first*, queuing updates while it waits, and discarding those at or
    /// below the snapshot sequence".
    ///
    /// # Errors
    ///
    /// What constructing, connecting or subscribing a socket reports.
    pub fn connect(context: &Context, ports: &Ports, subtree: &str) -> Result<CloneClient> {
        let snapshot = DealerSocket::new(context)?;
        snapshot.connect(&ports.snapshot)?;
        let mut updates = SubSocket::new(context)?;
        //  The subtree is "requested identically in the snapshot request and
        //  the subscription" - Clone Model Four's rule.
        updates.subscribe(subtree)?;
        //  HUGZ is not in the subtree and still has to arrive, or the client
        //  could not tell a quiet server from a dead one.
        updates.subscribe(HUGZ)?;
        updates.connect(&ports.publisher)?;
        let collector = PubSocket::new(context)?;
        collector.connect(&ports.collector)?;
        Ok(CloneClient {
            snapshot,
            updates,
            collector,
            subtree: subtree.to_owned(),
            map: HashMap::new(),
            sequence: 0,
            discarded: 0,
            beats: 0,
        })
    }

    /// Waits for one `HUGZ`, which is how this client knows its subscription
    /// has reached the publisher - the guide's slow joiner, answered with the
    /// beat CHP already has rather than a sleep.
    ///
    /// # Errors
    ///
    /// `EAGAIN` when no beat arrived inside `patience`.
    pub async fn wait_for_beat(&mut self, patience: Duration) -> Result<()> {
        loop {
            let message = self.updates.recv_timeout(patience).await?;
            let update = KvMsg::decode(&message)?;
            if update.key.as_slice() == HUGZ {
                self.beats += 1;
                return Ok(());
            }
            self.absorb(update);
        }
    }

    /// The snapshot dialog: `ICANHAZ?` with the subtree, then every `KVSYNC`
    /// until `KTHXBAI`, whose sequence becomes this client's.
    ///
    /// # Errors
    ///
    /// `EAGAIN` when the server did not finish the dialog inside `patience`,
    /// and `ENOCOMPATPROTO` for a malformed `kvmsg`.
    pub async fn snapshot(&mut self, patience: Duration) -> Result<()> {
        let mut request = KvMsg::new("", b"");
        request.key = ICANHAZ.to_vec();
        request.set_property("subtree", &self.subtree);
        self.snapshot.send(request.encode()).await?;
        loop {
            let message = self.snapshot.recv_timeout(patience).await?;
            let record = KvMsg::decode(&message)?;
            if record.key.as_slice() == KTHXBAI {
                self.sequence = record.sequence;
                return Ok(());
            }
            //  A KVSYNC: state, not an update, so no sequence rule applies.
            self.map.insert(record.key_text(), record.value.clone());
        }
    }

    /// Reads the update stream for `window`, applying what is newer than
    /// this client's sequence and discarding the rest.
    ///
    /// The bound is the **total** time and not the gap between messages: a
    /// CHP server beats, so "read until nothing arrives for a while" is a
    /// loop that never ends against a server whose `HUGZ` interval is
    /// shorter than the gap being waited for. That is the recipe's own
    /// liveness signal turning into a hang, and it is worth stating rather
    /// than tuning around.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for a malformed `kvmsg`; running out of time is the
    /// normal ending and not an error.
    pub async fn apply_pending(&mut self, window: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + window;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Ok(());
            }
            let Ok(message) = self.updates.recv_timeout(left).await else {
                return Ok(());
            };
            let update = KvMsg::decode(&message)?;
            if update.key.as_slice() == HUGZ {
                self.beats += 1;
                continue;
            }
            self.absorb(update);
        }
    }

    /// The strict-increment discard rule, in one place: an update is applied
    /// only when its sequence is **greater** than the one this client holds.
    fn absorb(&mut self, update: KvMsg) {
        if update.sequence <= self.sequence {
            self.discarded += 1;
            return;
        }
        self.sequence = update.sequence;
        if update.value.is_empty() {
            self.map.remove(&update.key_text());
        } else {
            self.map.insert(update.key_text(), update.value.clone());
        }
    }

    /// `KVSET`: a client write, sent to the server and numbered there.
    ///
    /// Returns once the update has reached at least one publisher-side
    /// subscriber - the server - which is the slow joiner again: a `PUB`
    /// socket with no peer yet drops silently.
    ///
    /// # Errors
    ///
    /// `EAGAIN` when the server's SUB socket never appeared inside
    /// `patience`.
    pub async fn set(&mut self, key: &str, value: &[u8], patience: Duration) -> Result<()> {
        let mut update = KvMsg::new(key, value);
        //  "a client-generated UUID on every update", Clone Model Six.
        update.uuid = format!("{key}-{}", value.len()).into_bytes();
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            if self.collector.publish(update.encode()).delivered > 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::EAGAIN(
                    "the collector has no subscriber, so the update was dropped".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let server = clone_server(&context, HUGZ_INTERVAL).await?;
    println!(
        "I: server at P={} P+1={} P+2={}",
        server.ports.snapshot, server.ports.publisher, server.ports.collector
    );

    //  A reader that subscribes *before* anything is written, which is the
    //  order CHP prescribes - so every update below reaches its SUB queue
    //  and the snapshot contains all of them. The strict-increment rule
    //  therefore has to discard all of them, and a reader can see that
    //  happen rather than take it on trust.
    let mut writer = CloneClient::connect(&context, &server.ports, "/client/")?;
    let mut reader = CloneClient::connect(&context, &server.ports, "/client/")?;
    reader.wait_for_beat(Duration::from_secs(3)).await?;

    for (key, value) in [("/client/a", "1"), ("/client/b", "2"), ("/client/a", "3")] {
        writer
            .set(key, value.as_bytes(), Duration::from_secs(1))
            .await?;
    }
    while server.sequence.load(Ordering::Relaxed) < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    reader.snapshot(Duration::from_secs(3)).await?;
    reader.apply_pending(Duration::from_millis(300)).await?;
    let mut keys: Vec<_> = reader.map.keys().cloned().collect();
    keys.sort();
    println!(
        "I: reader at sequence {} holds {keys:?}, discarded {} update(s)",
        reader.sequence, reader.discarded
    );

    //  An empty value deletes.
    writer.set("/client/b", b"", Duration::from_secs(1)).await?;
    reader.apply_pending(Duration::from_millis(200)).await?;
    let mut keys: Vec<_> = reader.map.keys().cloned().collect();
    keys.sort();
    println!("I: after the delete the reader holds {keys:?}");
    Ok(())
}
