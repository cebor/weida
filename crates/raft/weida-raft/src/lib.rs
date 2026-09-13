//! openraft plus the I/O it deliberately leaves out.
//!
//! openraft is the Raft mechanics, the clock and the task driving; what it
//! does **not** have is a transport and a store, and it says so by asking for
//! them as traits. This crate fills the first half with weida — QUIC
//! connections, exchanges, TLS-proved node identity and a streamed snapshot
//! channel — so that building a replicated service is writing a state machine
//! rather than writing a network layer
//! ([0021](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0021-consensus-openraft.md)).
//!
//! # What exists today, stated before the table below promises anything
//!
//! **The transport is not written yet** (B-224). What is here is the engine
//! wired to our types, a reference in-memory store, and a single-node group
//! that elects itself and commits — the verified go/no-go of B-222. The table
//! below is what the crate is *for*; the row that is missing is the one that
//! makes it useful to anyone but a test.
//!
//! One design question is still open and belongs to that slice: this crate's
//! [`TypeConfig`] fixes `D` and `R` to the example [`Command`] and [`Applied`],
//! which is right for a probe and wrong for a library. An application must be
//! able to declare its own request and response types while keeping **our**
//! `NodeId`, `Node`, `Entry` and `SnapshotData`; whether that is a macro here
//! or a documented call to openraft's own `declare_raft_types!` is decided
//! with the transport, because the transport has to be generic over it anyway.
//!
//! # What this crate provides, and what is left to you
//!
//! | Provided here | Yours |
//! | --- | --- |
//! | the transport: one exchange per RPC on the ALPN `weida-raft/0` | the **state machine**: `apply`, and what a committed entry means |
//! | node identity: the fingerprint a peer proves in the handshake ([0020](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0020-cluster-and-discovery.md) §4.4) | the request and response types (`D` and `R`) |
//! | membership bootstrap, and the two-node configuration refused by name | the **log store**, until this crate ships the segmented one `docs/STORE.md` §7 specifies |
//! | the snapshot channel as a stream, not a chunked RPC | what a snapshot of your state machine *is* |
//!
//! # openraft is re-exported, on purpose
//!
//! A user of this crate implements openraft's own traits, so it needs
//! openraft's own types — and two versions of them in one binary do not
//! compose. [`openraft`] is therefore re-exported and **is** the version this
//! crate was built against. The coupling is stated rather than hidden:
//! openraft is pre-1.0 and promises incompatible changes before 1.0, so a
//! `weida-raft` release names one openraft minor line.
//!
//! # Where the tasks run, and why that is a construction-site rule
//!
//! openraft's `AsyncRuntime` is a **type-level** choice whose functions are
//! *associated* rather than methods: `fn spawn<T>(future: T)` takes no `self`.
//! There is therefore nothing to thread a `weida_runtime::Exec` handle
//! through, and no way to bind one instance of the engine to one runtime by
//! implementing the trait. What decides which runtime openraft's tasks land on
//! is the **ambient** one at the construction site.
//!
//! So the rule this crate follows is: a [`Consensus`] is built and driven from
//! inside its runtime's own `Exec`, which makes the ambient runtime the
//! caller's runtime, which is where every task openraft spawns ends up. That
//! is an invariant of *where* it is called, verified by a test, rather than a
//! property of a type.

pub use openraft;

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};

/// What the replicated state machine of a broker is asked to do.
///
/// One variant for now, because this slice is the go/no-go for openraft rather
/// than the broker's state machine: 0021 §4.5 lists what the log will hold —
/// queue registry, membership, leadership, the per-message commit record and
/// consumer state — and B-225 is the slice that puts it there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// A no-op entry, which is what a leader commits to prove it leads.
    Noop,
}

/// What applying a [`Command`] answers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    /// How many commands this state machine has applied, including this one.
    pub count: u64,
}

/// The node id type.
///
/// `u64` here, and **not** the proved fingerprint 0021 §4.4 names as the real
/// id: a fingerprint is 32 bytes and `NodeId` carries trait bounds that a
/// newtype has to satisfy, which is work that belongs to the slice adding the
/// transport (B-224) rather than to the go/no-go. Truncating a digest into a
/// `u64` would be the wrong shortcut — etcd's rule is that an id identifies a
/// node *for all time*, and a truncated digest collides.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// The type configuration of a broker's consensus group.
    pub TypeConfig:
        D = Command,
        R = Applied,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// An in-memory log and state machine.
///
/// Deliberately not the Phase 5 store: this is the reference the store has to
/// match, and writing it is how the thirteen-method requirement list of
/// 0021 §4.3 was checked against the actual traits. What it proved, and what
/// B-223 must carry: the v2 traits are `RaftLogStorage` — `get_log_state`,
/// `get_log_reader`, `save_vote`/`read_vote`, `save_committed`/
/// `read_committed`, `append` **with a flush callback**, `truncate` backwards
/// and `purge` forwards — plus `RaftStateMachine`: `applied_state`, `apply`,
/// `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`,
/// `get_current_snapshot`. The flush callback is the persist-before-send rule
/// of the Raft thesis made explicit in a signature, which is the part a
/// hand-rolled store gets wrong.
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<tokio::sync::Mutex<StoreState>>,
}

#[derive(Default)]
struct StoreState {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
    state: Applied,
    snapshot: Option<(SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>)>,
    snapshot_index: u64,
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl MemoryStore {
    /// Seeds entries without the flush callback `append` requires.
    ///
    /// Test-only, and it exists because `LogFlushed::new` is `pub(crate)` in
    /// openraft: a third-party store cannot construct the callback its own
    /// `append` is handed.
    #[cfg(test)]
    async fn seed(&self, indexes: std::ops::RangeInclusive<u64>) {
        let mut inner = self.inner.lock().await;
        for index in indexes {
            inner.log.insert(
                index,
                Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
                    payload: EntryPayload::Normal(Command::Noop),
                },
            );
        }
    }
}

impl RaftLogReader<TypeConfig> for MemoryStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemoryStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last.or(inner.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                inner.log.insert(entry.get_log_id().index, entry);
            }
        }
        // In memory the write is already durable by the time this returns, so
        // the callback fires immediately. A real store fires it **after** the
        // bytes are on the medium, and nothing may be sent to a peer before
        // that: this callback is where that ordering lives.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemoryStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.snapshot_index += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.applied,
            last_membership: inner.membership.clone(),
            snapshot_id: format!("{}", inner.snapshot_index),
        };
        let data = inner.state.count.to_be_bytes().to_vec();
        inner.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for MemoryStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.inner.lock().await;
        Ok((inner.applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Applied>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().await;
        let mut answers = Vec::new();
        for entry in entries {
            inner.applied = Some(*entry.get_log_id());
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(Command::Noop) => inner.state.count += 1,
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(inner.applied, membership);
                }
            }
            answers.push(inner.state.clone());
        }
        Ok(answers)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let mut inner = self.inner.lock().await;
        let mut count = [0u8; 8];
        if data.len() == 8 {
            count.copy_from_slice(&data);
            inner.state.count = u64::from_be_bytes(count);
        }
        inner.applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}

/// The peer transport, which this slice does not have.
///
/// B-224 implements it as one weida exchange per RPC on the separate ALPN
/// `weida-raft/0` (0021 §4.4). Until then a single-node group needs a factory
/// that is never called, and "never called" is asserted rather than assumed:
/// every method here is unreachable for a one-member membership.
#[derive(Clone, Debug, Default)]
pub struct NoNetwork;

impl RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network = NoNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &openraft::BasicNode) -> Self::Network {
        NoNetwork
    }
}

impl RaftNetwork<TypeConfig> for NoNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>,
    > {
        unreachable!("a single-node group replicates to nobody (B-224 adds the transport)")
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        unreachable!("a single-node group replicates to nobody (B-224 adds the transport)")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>>
    {
        unreachable!("a single-node group votes for itself without asking")
    }
}

/// One broker's consensus group.
///
/// Holds the engine and nothing else; the broker's own state lives in its
/// queues until B-225 moves the replicated part into the log.
pub struct Consensus {
    raft: openraft::Raft<TypeConfig>,
    store: MemoryStore,
}

impl Consensus {
    /// Starts a single-node group and makes it lead.
    ///
    /// **Call this from inside the broker's own `Exec`**: openraft's
    /// `AsyncRuntime::spawn` is an associated function with no handle to
    /// carry, so the tasks it spawns land on the *ambient* runtime. That is
    /// the construction-site invariant this module's header states.
    pub async fn single_node(id: NodeId) -> Result<Consensus, String> {
        let config = openraft::Config {
            cluster_name: "weida".to_owned(),
            // Ticks are cheap in a test and this is not a tuning decision:
            // B-224 chooses the real numbers once there is a network to
            // measure them against.
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        let config = Arc::new(config.validate().map_err(|e| e.to_string())?);
        let store = MemoryStore::default();
        let raft = openraft::Raft::new(id, config, NoNetwork, store.clone(), store.clone())
            .await
            .map_err(|e| e.to_string())?;
        let mut members = BTreeMap::new();
        members.insert(id, openraft::BasicNode::default());
        raft.initialize(members).await.map_err(|e| e.to_string())?;
        Ok(Consensus { raft, store })
    }

    /// Proposes one command and returns once it is committed and applied.
    pub async fn propose(&self, command: Command) -> Result<Applied, String> {
        let answer = self
            .raft
            .client_write(command)
            .await
            .map_err(|e| e.to_string())?;
        Ok(answer.data)
    }

    /// Who this group believes leads it.
    pub async fn leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// How many commands the state machine has applied.
    pub async fn applied_count(&self) -> u64 {
        self.store.inner.lock().await.state.count
    }

    /// Stops the engine.
    pub async fn shutdown(self) {
        let _ = self.raft.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// The go/no-go of [0021](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0021-consensus-openraft.md):
    /// the engine runs on our types, elects itself and commits.
    #[tokio::test]
    async fn a_single_node_group_elects_itself_and_commits() {
        let consensus = Consensus::single_node(1).await.expect("start");

        let deadline = Instant::now() + Duration::from_secs(10);
        while consensus.leader().await != Some(1) {
            assert!(Instant::now() < deadline, "the group never elected itself");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let applied = consensus.propose(Command::Noop).await.expect("commit");
        assert_eq!(applied.count, 1, "the state machine applied the command");
        assert_eq!(consensus.applied_count().await, 1);

        // A second proposal goes through the same log, so the count is the
        // state machine's rather than the response's.
        consensus
            .propose(Command::Noop)
            .await
            .expect("commit again");
        assert_eq!(consensus.applied_count().await, 2);

        consensus.shutdown().await;
    }

    /// The construction-site invariant of this module's header: the engine is
    /// built and driven inside the broker's own `Exec`, so the tasks openraft
    /// spawns land on that runtime. There is no ambient runtime at this call
    /// site — a plain `#[test]` — so if the engine did not inherit weida's,
    /// `spawn` would panic and the channel would never receive.
    #[test]
    fn the_engine_runs_on_the_runtime_that_built_it() {
        // `owned`, not `new`: there is deliberately no ambient runtime at this
        // call site, which is the whole point of the test — the engine must
        // inherit the runtime weida owns.
        let runtime =
            weida::Runtime::owned(weida::RuntimeConfig::default()).expect("weida runtime");
        let (tx, rx) = std::sync::mpsc::channel();
        runtime.exec().spawn(async move {
            let consensus = Consensus::single_node(7).await.expect("start");
            let deadline = Instant::now() + Duration::from_secs(10);
            while consensus.leader().await != Some(7) {
                if Instant::now() >= deadline {
                    let _ = tx.send(Err("never elected".to_owned()));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let applied = consensus.propose(Command::Noop).await;
            let _ = tx.send(applied.map(|a| a.count).map_err(|e| e.to_string()));
            consensus.shutdown().await;
        });

        let outcome = rx
            .recv_timeout(Duration::from_secs(15))
            .expect("the engine reported nothing: it never ran");
        assert_eq!(outcome, Ok(1));
    }

    /// `truncate` discards a suffix and `purge` a prefix — the two operations
    /// an append-only store cannot do, and the reason B-223 exists before the
    /// store.
    ///
    /// The entries are seeded directly rather than through
    /// [`RaftLogStorage::append`], and that is a finding rather than a
    /// shortcut: `LogFlushed::new` is `pub(crate)` in openraft, so a
    /// third-party store **cannot construct the flush callback** its own
    /// `append` receives. A store's trait conformance is therefore only
    /// exercisable through a live `Raft`, which is what the first test does;
    /// what a unit test can reach is everything else.
    #[tokio::test]
    async fn the_log_truncates_backwards_and_purges_forwards() {
        let mut store = MemoryStore::default();
        store.seed(1..=5).await;
        assert_eq!(last_index(&mut store).await, Some(5));

        store.truncate(log_id(4)).await.expect("truncate");
        assert_eq!(
            last_index(&mut store).await,
            Some(3),
            "truncate discards the suffix from the given index on"
        );

        store.purge(log_id(2)).await.expect("purge");
        let left = store.try_get_log_entries(..).await.expect("read");
        assert_eq!(left.len(), 1, "1 and 2 are gone, 3 remains");
        assert_eq!(
            store
                .get_log_state()
                .await
                .expect("state")
                .last_purged_log_id
                .map(|id| id.index),
            Some(2),
            "a purged prefix is remembered, or a restart would replay it"
        );
    }

    /// A purged log still reports the last log id it had, which is what lets a
    /// restarted node answer a vote request about entries it no longer holds.
    #[tokio::test]
    async fn a_fully_purged_log_still_knows_where_it_ended() {
        let mut store = MemoryStore::default();
        store.seed(1..=3).await;
        store.purge(log_id(3)).await.expect("purge everything");

        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id.map(|id| id.index), Some(3));
        assert_eq!(
            state.last_log_id.map(|id| id.index),
            Some(3),
            "an empty log whose prefix was purged is not an empty history"
        );
    }

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(openraft::CommittedLeaderId::new(1, 1), index)
    }

    async fn last_index(store: &mut MemoryStore) -> Option<u64> {
        store
            .get_log_state()
            .await
            .expect("state")
            .last_log_id
            .map(|id| id.index)
    }
}
