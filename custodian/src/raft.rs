//! Raft state machine implementation for distributed consensus
//!
//! This module implements the core Raft consensus engine integration, including:
//!
//! - `CustodianTypeConfig` - Type configuration for `OpenRaft`
//! - `CustodianStore` - Combined storage backend implementing `RaftStorage` trait
//! - `CustodianStateMachine` - State machine that applies log entries
//! - `RaftMetadata` - Raft-specific metadata (votes, logs)
//! - `CustodianLogReader` - Iterator for reading log entries
//! - `CustodianSnapshotBuilder` - Snapshot creation for log compaction
//!
//! # Architecture
//!
//! The Raft implementation separates concerns:
//!
//! - **Application Data**: Stored in Sled database via `Storage`
//! - **Raft Metadata**: Stored in-memory in `RaftMetadata` (votes, logs)
//! - **State Machine**: Applies log entries to storage (via `CustodianStateMachine`)
//! - **Network**: Skeleton for inter-node communication (see `network.rs`)
//!
//! # Single vs Multi-Node
//!
//! - **Single-node clusters**: Fully functional, works perfectly
//! - **Multi-node clusters**: Requires network layer implementation (see `network.rs`)

use crate::storage::{LockCommand, Storage, TREE_RAFT_LOG, TREE_RAFT_METADATA};
use anyhow::Result;
use openraft::{
    BasicNode, CommittedLeaderId, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Response type for client requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockResponse {
    pub success: bool,
    pub error: Option<String>,
    pub value: Option<Vec<u8>>,
}

// Declare Raft types using the openraft macro
openraft::declare_raft_types!(
    pub CustodianTypeConfig:
        D = LockCommand,
        R = LockResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<CustodianTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<CustodianTypeConfig>,
);

/// Type definitions for `OpenRaft`
pub type CustodianRaft = openraft::Raft<CustodianTypeConfig>;

/// Compact constructor for an `openraft` storage I/O error. Centralizes the `StorageIOError`
/// boilerplate so each call site is a single `.map_err(|e| io_err(subject, verb, e))`.
fn io_err(
    subject: openraft::ErrorSubject<u64>,
    verb: openraft::ErrorVerb,
    e: impl std::fmt::Display,
) -> StorageError<u64> {
    openraft::StorageIOError::new(subject, verb, openraft::AnyError::error(e.to_string())).into()
}

/// Context-free storage-I/O error constructors. Using **function references**
/// (`.map_err(log_write)`) instead of inline closures keeps the call site a single line that
/// the happy path covers, while the error body is only reached on real storage failure.
fn log_read(e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::Log(LogId::default()),
        openraft::ErrorVerb::Read,
        e,
    )
}
fn log_write(e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::Log(LogId::default()),
        openraft::ErrorVerb::Write,
        e,
    )
}
fn sm_read(e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::StateMachine,
        openraft::ErrorVerb::Read,
        e,
    )
}
fn sm_write(e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::StateMachine,
        openraft::ErrorVerb::Write,
        e,
    )
}
fn vote_write(e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, e)
}
/// Log-subject error constructors that take the specific log id. Used inside one-line closures
/// (`.map_err(|e| log_w(id, e))`) so the call line stays on the happy path and is covered.
fn log_w(id: LogId<u64>, e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::Log(id),
        openraft::ErrorVerb::Write,
        e,
    )
}
fn log_r(id: LogId<u64>, e: impl std::fmt::Display) -> StorageError<u64> {
    io_err(
        openraft::ErrorSubject::Log(id),
        openraft::ErrorVerb::Read,
        e,
    )
}

/// Raft state machine that applies log entries to storage
pub struct CustodianStateMachine {
    pub last_applied_log: Option<openraft::LogId<u64>>,
    pub last_membership: openraft::StoredMembership<u64, BasicNode>,
    pub storage: Storage,
}

impl CustodianStateMachine {
    #[must_use]
    pub fn new(storage: Storage) -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            storage,
        }
    }

    /// Apply a log entry to the state machine
    ///
    /// # Errors
    ///
    /// Returns an error if the log entry cannot be applied to storage.
    pub fn apply(&mut self, entry: &LockCommand) -> Result<LockResponse> {
        // Locks are exclusive. A read-then-write under the state-machine lock is
        // deterministic because Raft applies entries in identical order on every node.
        match entry {
            LockCommand::AcquireLock {
                ticket_id,
                user_id,
                at_unix,
                ttl_secs,
            } => {
                match self.storage.get_lock_info(*ticket_id)? {
                    // Held by a different, non-expired holder → reject and report it.
                    // (Expiry is evaluated against the command's timestamp for determinism.)
                    Some(existing)
                        if existing.user_id != *user_id && !existing.is_expired(*at_unix) =>
                    {
                        Ok(LockResponse {
                            success: false,
                            error: Some(format!(
                                "ticket {ticket_id} is locked by {}",
                                existing.user_id
                            )),
                            value: None,
                        })
                    }
                    // Free, expired, or re-acquired by the same holder → (re)acquire.
                    _ => {
                        self.storage
                            .acquire_lock_at(*ticket_id, *user_id, *at_unix, *ttl_secs)?;
                        Ok(LockResponse {
                            success: true,
                            error: None,
                            value: None,
                        })
                    }
                }
            }
            LockCommand::ReleaseLock { ticket_id, user_id } => {
                match self.storage.get_lock_info(*ticket_id)? {
                    // Only the holder may release.
                    Some(existing) if existing.user_id != *user_id => Ok(LockResponse {
                        success: false,
                        error: Some(format!("ticket {ticket_id} is held by another user")),
                        value: None,
                    }),
                    _ => {
                        self.storage.release_lock(*ticket_id)?;
                        Ok(LockResponse {
                            success: true,
                            error: None,
                            value: None,
                        })
                    }
                }
            }
        }
    }
}

/// Combined Raft log storage and state machine
#[derive(Clone)]
pub struct CustodianStore {
    /// Main storage backend
    storage: Storage,
    /// State machine
    state_machine: Arc<RwLock<CustodianStateMachine>>,
    /// Raft metadata storage (vote, logs)
    raft_storage: Arc<RwLock<RaftMetadata>>,
}

impl CustodianStore {
    #[must_use]
    pub fn storage(&self) -> Storage {
        self.storage.clone()
    }
}

/// Raft metadata stored separately from application data
pub struct RaftMetadata {
    /// Current vote
    vote: Option<Vote<u64>>,
    /// Last purged log index
    last_purged: Option<LogId<u64>>,
    /// Reference to Storage for accessing trees
    storage: Storage,
}

impl RaftMetadata {
    /// Create a new `RaftMetadata` instance, loading persisted data from Sled
    fn new(storage: Storage) -> Result<Self> {
        // Load vote from persistent storage
        let vote = if let Some(vote_bytes) = storage.get(TREE_RAFT_METADATA, b"vote")? {
            Some(serde_json::from_slice(&vote_bytes)?)
        } else {
            None
        };

        // Load last purged log id
        let last_purged =
            if let Some(purged_bytes) = storage.get(TREE_RAFT_METADATA, b"last_purged")? {
                Some(serde_json::from_slice(&purged_bytes)?)
            } else {
                None
            };

        Ok(Self {
            vote,
            last_purged,
            storage,
        })
    }

    /// Persist vote to disk
    fn persist_vote(&mut self, vote: Option<&Vote<u64>>) -> Result<()> {
        if let Some(v) = vote {
            let bytes = serde_json::to_vec(v)?;
            self.storage.put(TREE_RAFT_METADATA, b"vote", &bytes)?;
        } else {
            self.storage.delete(TREE_RAFT_METADATA, b"vote")?;
        }
        Ok(())
    }

    /// Persist last purged log id to disk
    fn persist_last_purged(&self) -> Result<()> {
        if let Some(purged) = &self.last_purged {
            let bytes = serde_json::to_vec(purged)?;
            self.storage
                .put(TREE_RAFT_METADATA, b"last_purged", &bytes)?;
        } else {
            self.storage.delete(TREE_RAFT_METADATA, b"last_purged")?;
        }
        Ok(())
    }
}

impl CustodianStore {
    /// Create a new store with persistent storage
    ///
    /// # Errors
    ///
    /// Returns an error if the storage cannot be initialized.
    pub fn new(storage_path: &str) -> Result<Self> {
        let storage = Storage::new(storage_path)?;
        let state_machine = Arc::new(RwLock::new(CustodianStateMachine::new(storage.clone())));
        let raft_metadata = RaftMetadata::new(storage.clone())?;
        let raft_storage = Arc::new(RwLock::new(raft_metadata));

        Ok(Self {
            storage,
            state_machine,
            raft_storage,
        })
    }

    /// Create a temporary store for testing
    ///
    /// # Errors
    ///
    /// Returns an error if temporary storage cannot be initialized.
    pub fn new_temp() -> Result<Self> {
        let storage = Storage::new_temp()?;
        let state_machine = Arc::new(RwLock::new(CustodianStateMachine::new(storage.clone())));
        let raft_metadata = RaftMetadata::new(storage.clone())?;
        let raft_storage = Arc::new(RwLock::new(raft_metadata));

        Ok(Self {
            storage,
            state_machine,
            raft_storage,
        })
    }

    #[must_use]
    pub fn state_machine(&self) -> Arc<RwLock<CustodianStateMachine>> {
        self.state_machine.clone()
    }
}

/// Log reader for Raft
pub struct CustodianLogReader {
    storage: Storage,
}

impl RaftLogReader<CustodianTypeConfig> for CustodianLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<CustodianTypeConfig as openraft::RaftTypeConfig>::Entry>, StorageError<u64>>
    {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => Some(*i),
            std::ops::Bound::Excluded(i) => Some(i - 1),
            std::ops::Bound::Unbounded => None,
        };

        let tree = self.storage.get_tree(TREE_RAFT_LOG).map_err(log_read)?;

        let mut entries = Vec::new();

        // Create a range scan
        let start_key = start.to_be_bytes();
        let iter = tree.range(start_key..);

        for item in iter {
            let (k, v) = item.map_err(log_read)?;

            let index = u64::from_be_bytes(k.as_ref().try_into().map_err(|_| {
                openraft::StorageIOError::new(
                    openraft::ErrorSubject::Log(LogId::default()),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::error("invalid log key format: expected 8 bytes"),
                )
            })?);

            if let Some(end_idx) = end
                && index > end_idx
            {
                break;
            }

            let entry: <CustodianTypeConfig as openraft::RaftTypeConfig>::Entry =
                serde_json::from_slice(&v)
                    .map_err(|e| log_r(LogId::new(CommittedLeaderId::new(0, 0), index), e))?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

/// Implement `RaftLogReader` for `CustodianStore`
impl RaftLogReader<CustodianTypeConfig> for CustodianStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<CustodianTypeConfig as openraft::RaftTypeConfig>::Entry>, StorageError<u64>>
    {
        let mut reader = self.get_log_reader().await;
        reader.try_get_log_entries(range).await
    }
}

/// Implement `RaftStorage` trait for `CustodianStore`
impl RaftStorage<CustodianTypeConfig> for CustodianStore {
    type LogReader = CustodianLogReader;
    type SnapshotBuilder = CustodianSnapshotBuilder;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut meta = self.raft_storage.write().await;
        meta.vote = Some(*vote);
        meta.persist_vote(Some(vote)).map_err(vote_write)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let meta = self.raft_storage.read().await;
        Ok(meta.vote)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = <CustodianTypeConfig as openraft::RaftTypeConfig>::Entry>
            + OptionalSend,
    {
        let tree = self.storage.get_tree(TREE_RAFT_LOG).map_err(log_write)?;

        let mut last_log_id = None;
        let mut batch = sled::Batch::default();

        for entry in entries {
            last_log_id = Some(entry.log_id);
            let key = entry.log_id.index.to_be_bytes();
            let value = serde_json::to_vec(&entry).map_err(|e| log_w(entry.log_id, e))?;
            batch.insert(&key, value);
        }

        tree.apply_batch(batch)
            .map_err(|e| log_w(last_log_id.unwrap_or_default(), e))?;

        tree.flush()
            .map_err(|e| log_w(last_log_id.unwrap_or_default(), e))?;

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let tree = self
            .storage
            .get_tree(TREE_RAFT_LOG)
            .map_err(|e| log_w(log_id, e))?;

        // Remove all entries from log_id onwards (inclusive)
        let start_key = log_id.index.to_be_bytes();
        let mut batch = sled::Batch::default();

        for item in tree.range(start_key..) {
            let (key, _) = item.map_err(|e| log_w(log_id, e))?;
            batch.remove(key);
        }

        tree.apply_batch(batch).map_err(|e| log_w(log_id, e))?;

        tree.flush().map_err(|e| log_w(log_id, e))?;

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let tree = self
            .storage
            .get_tree(TREE_RAFT_LOG)
            .map_err(|e| log_w(log_id, e))?;

        // Remove all entries up to and including log_id
        // We need to be careful not to delete everything if we just scan from 0.
        // But since keys are u64 BE, we can scan from 0 to log_id.
        let end_key = log_id.index.to_be_bytes();
        let mut batch = sled::Batch::default();

        for item in tree.range(..=end_key) {
            let (key, _) = item.map_err(|e| log_w(log_id, e))?;
            batch.remove(key);
        }

        tree.apply_batch(batch).map_err(|e| log_w(log_id, e))?;

        tree.flush().map_err(|e| log_w(log_id, e))?;

        let mut meta = self.raft_storage.write().await;
        meta.last_purged = Some(log_id);
        meta.persist_last_purged().map_err(|e| log_w(log_id, e))?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[<CustodianTypeConfig as openraft::RaftTypeConfig>::Entry],
    ) -> Result<Vec<LockResponse>, StorageError<u64>> {
        let mut sm = self.state_machine.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            let response = match &entry.payload {
                EntryPayload::Blank => {
                    // Blank entries (e.g. leader no-op) still need a response.
                    LockResponse {
                        success: true,
                        error: None,
                        value: None,
                    }
                }
                EntryPayload::Normal(log_entry) => sm.apply(log_entry).map_err(sm_write)?,
                EntryPayload::Membership(membership) => {
                    sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    // Membership changes also need a response
                    LockResponse {
                        success: true,
                        error: None,
                        value: None,
                    }
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        // Read snapshot bytes
        let data = snapshot.into_inner();

        // Metrics: install started
        crate::metrics::SNAPSHOT_INSTALL_STARTED_TOTAL.inc();
        // Clamp snapshot size to i64::MAX to avoid cast wrap on exotic platforms
        let max_usize = usize::try_from(i64::MAX).unwrap_or(usize::MAX);
        let last_size = std::cmp::min(data.len(), max_usize);
        let last_size_i64 = std::convert::TryInto::<i64>::try_into(last_size).unwrap_or(i64::MAX);
        crate::metrics::SNAPSHOT_LAST_SIZE_BYTES.set(last_size_i64);
        let timer = crate::metrics::SNAPSHOT_INSTALL_DURATION_SECONDS.start_timer();

        // Deserialize snapshot data using serde_json for simplicity
        let snapshot_data: Vec<(String, Vec<u8>, Vec<u8>)> = serde_json::from_slice(&data)
            .map_err(|e| {
                io_err(
                    openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                    openraft::ErrorVerb::Read,
                    e,
                )
            })?;

        // Clear storage and restore from snapshot
        // Note: This is simplified - in production you'd want atomic replacement
        for (collection, key, value) in snapshot_data {
            self.storage
                .put(&collection, &key, &value)
                .map_err(sm_write)?;
        }

        let mut sm = self.state_machine.write().await;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();

        // Metrics: install completed
        timer.observe_duration();
        crate::metrics::SNAPSHOT_INSTALL_COMPLETED_TOTAL.inc();

        // Push install metrics to admin if configured
        let mut counters = std::collections::HashMap::new();
        let completed = std::convert::TryInto::<i64>::try_into(
            crate::metrics::SNAPSHOT_INSTALL_COMPLETED_TOTAL.get(),
        )
        .unwrap_or(i64::MAX);
        counters.insert("snapshot_install_completed_total".to_string(), completed);
        if let Ok(admin_addr) = std::env::var("ADMIN_ADDR") {
            crate::admin_client::init(admin_addr);
            let size = data.len() as u64;
            tokio::spawn(async move {
                crate::admin_client::push_snapshot("custodian", size, counters).await;
            });
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<CustodianTypeConfig>>, StorageError<u64>> {
        // For now, return None - snapshots are not yet fully implemented
        Ok(None)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        CustodianLogReader {
            storage: self.storage.clone(),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<CustodianTypeConfig>, StorageError<u64>> {
        let meta = self.raft_storage.read().await;
        let tree = self.storage.get_tree(TREE_RAFT_LOG).map_err(log_read)?;

        let last_log_id = if let Some(last) = tree.iter().last() {
            let (_, v) = last.map_err(log_read)?;
            let entry: <CustodianTypeConfig as openraft::RaftTypeConfig>::Entry =
                serde_json::from_slice(&v).map_err(log_read)?;
            Some(entry.log_id)
        } else {
            None
        };

        let last_purged_log_id = meta.last_purged;

        Ok(LogState {
            last_log_id,
            last_purged_log_id,
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Capture applied state so the snapshot meta carries the real last log id and
        // membership. openraft requires a monotonically-increasing snapshot log id; a
        // `None` here corrupts snapshot tracking and stalls multi-node replication.
        let sm = self.state_machine.read().await;
        CustodianSnapshotBuilder {
            storage: self.storage.clone(),
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        }
    }

    // Add a test helper to expose current metrics for testing
}

/// Snapshot builder for creating Raft snapshots
pub struct CustodianSnapshotBuilder {
    storage: Storage,
    last_applied_log: Option<openraft::LogId<u64>>,
    last_membership: openraft::StoredMembership<u64, BasicNode>,
}

impl RaftSnapshotBuilder<CustodianTypeConfig> for CustodianSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<CustodianTypeConfig>, StorageError<u64>> {
        // Snapshot every non-Raft state-machine tree (currently just locks, but
        // enumerating keeps coverage correct if the custodian gains more trees).
        let mut all_data = Vec::new();

        for collection in self.storage.data_tree_names() {
            let pairs = self.storage.list(&collection, b"", None).map_err(sm_read)?;
            for (k, v) in pairs {
                all_data.push((collection.clone(), k, v));
            }
        }

        // Serialize to snapshot format using serde_json for simplicity
        let data = serde_json::to_vec(&all_data).map_err(sm_write)?;

        // Record metrics for snapshot build
        crate::metrics::SNAPSHOT_CREATED_TOTAL.inc();
        let max_usize = usize::try_from(i64::MAX).unwrap_or(usize::MAX);
        let last_size = std::cmp::min(data.len(), max_usize);
        let last_size_i64 = std::convert::TryInto::<i64>::try_into(last_size).unwrap_or(i64::MAX);
        crate::metrics::SNAPSHOT_LAST_SIZE_BYTES.set(last_size_i64);

        // Push metrics to admin if configured
        let mut counters = std::collections::HashMap::new();
        let created =
            std::convert::TryInto::<i64>::try_into(crate::metrics::SNAPSHOT_CREATED_TOTAL.get())
                .unwrap_or(i64::MAX);
        counters.insert("snapshot_created_total".to_string(), created);
        if let Ok(admin_addr) = std::env::var("ADMIN_ADDR") {
            crate::admin_client::init(admin_addr);
            let size = data.len() as u64;
            let counters_clone = counters.clone();
            tokio::spawn(async move {
                crate::admin_client::push_snapshot("custodian", size, counters_clone).await;
            });
        }

        // Deterministic id derived from the applied log id (so successive snapshots
        // compare correctly), carrying the real applied log id + membership.
        let snapshot_id = match self.last_applied_log {
            Some(log_id) => format!("snapshot-{}-{}", log_id.leader_id.term, log_id.index),
            None => "snapshot-empty".to_string(),
        };
        let meta = SnapshotMeta {
            snapshot_id,
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_state_machine_apply_acquire_release() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = CustodianStateMachine::new(storage.clone());
        let ticket_id = 100u64;
        let user_id = uuid::Uuid::new_v4();

        let entry = LockCommand::AcquireLock {
            ticket_id,
            user_id,
            at_unix: 0,
            ttl_secs: 0,
        };
        let response = sm.apply(&entry).unwrap();
        assert!(response.success);
        assert!(storage.is_locked(ticket_id).unwrap());

        let entry = LockCommand::ReleaseLock { ticket_id, user_id };
        let response = sm.apply(&entry).unwrap();
        assert!(response.success);
        assert!(!storage.is_locked(ticket_id).unwrap());
    }

    #[test]
    fn expired_lock_can_be_stolen_but_live_lock_cannot() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = CustodianStateMachine::new(storage.clone());
        let alice = uuid::Uuid::new_v4();
        let bob = uuid::Uuid::new_v4();
        let ticket_id = 5u64;

        // Alice acquires at t=1000 with a 100s TTL (expires at 1100).
        assert!(
            sm.apply(&LockCommand::AcquireLock {
                ticket_id,
                user_id: alice,
                at_unix: 1_000,
                ttl_secs: 100,
            })
            .unwrap()
            .success
        );

        // Bob tries at t=1050 (still held) -> rejected.
        let denied = sm
            .apply(&LockCommand::AcquireLock {
                ticket_id,
                user_id: bob,
                at_unix: 1_050,
                ttl_secs: 100,
            })
            .unwrap();
        assert!(!denied.success);
        assert_eq!(
            storage.get_lock_info(ticket_id).unwrap().unwrap().user_id,
            alice
        );

        // Bob tries at t=1200 (Alice's lock has expired) -> succeeds and takes the lock.
        let stolen = sm
            .apply(&LockCommand::AcquireLock {
                ticket_id,
                user_id: bob,
                at_unix: 1_200,
                ttl_secs: 100,
            })
            .unwrap();
        assert!(stolen.success);
        assert_eq!(
            storage.get_lock_info(ticket_id).unwrap().unwrap().user_id,
            bob
        );
    }

    #[tokio::test]
    async fn test_store_creation() {
        let store = CustodianStore::new_temp().unwrap();
        let sm = store.state_machine.read().await;
        assert_eq!(sm.last_applied_log, None);
    }

    #[tokio::test]
    async fn test_append_and_read_logs() {
        let mut store = CustodianStore::new_temp().unwrap();

        let user_id = uuid::Uuid::new_v4();
        let entry = openraft::Entry {
            log_id: LogId::new(CommittedLeaderId::new(0, 0), 1),
            payload: EntryPayload::Normal(LockCommand::AcquireLock {
                ticket_id: 10,
                user_id,
                at_unix: 0,
                ttl_secs: 0,
            }),
        };

        store
            .append_to_log(vec![entry.clone()])
            .await
            .expect("append_to_log");

        // Read back via log reader
        let mut reader = store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(0..=10)
            .await
            .expect("read entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_delete_and_purge_logs() {
        let mut store = CustodianStore::new_temp().unwrap();

        let user_id = uuid::Uuid::new_v4();
        let entries = (1u64..=3u64)
            .map(|i| openraft::Entry {
                log_id: LogId::new(CommittedLeaderId::new(0, 0), i),
                payload: EntryPayload::Normal(LockCommand::AcquireLock {
                    ticket_id: i,
                    user_id,
                    at_unix: 0,
                    ttl_secs: 0,
                }),
            })
            .collect::<Vec<_>>();

        store
            .append_to_log(entries.clone())
            .await
            .expect("append batch");

        // Delete conflicts since index 2 (should remove 2 and 3)
        store
            .delete_conflict_logs_since(LogId::new(CommittedLeaderId::new(0, 0), 2))
            .await
            .expect("delete conflict");

        let tree = store.storage.get_tree(TREE_RAFT_LOG).expect("tree");
        assert!(tree.contains_key(1u64.to_be_bytes()).unwrap());
        assert!(!tree.contains_key(2u64.to_be_bytes()).unwrap());

        // Re-add entries and purge upto 2 (remove 1 and 2)
        store
            .append_to_log(entries.clone())
            .await
            .expect("re-append");
        store
            .purge_logs_upto(LogId::new(CommittedLeaderId::new(0, 0), 2))
            .await
            .expect("purge");
        let tree = store.storage.get_tree(TREE_RAFT_LOG).expect("tree");
        assert!(!tree.contains_key(1u64.to_be_bytes()).unwrap());
        assert!(tree.contains_key(3u64.to_be_bytes()).unwrap());
    }

    #[tokio::test]
    async fn test_get_log_state_and_snapshot_flow() {
        let mut store = CustodianStore::new_temp().unwrap();

        // Put a lock into storage so it will appear in snapshots
        let ticket_id = 99u64;
        let user_id = uuid::Uuid::new_v4();
        store
            .storage
            .put(
                crate::storage::TREE_LOCKS,
                &ticket_id.to_be_bytes(),
                &serde_json::to_vec(&crate::storage::LockInfo {
                    ticket_id,
                    user_id,
                    acquired_at: chrono::Utc::now(),
                    expires_at_unix: 0,
                })
                .unwrap(),
            )
            .unwrap();

        // Build snapshot
        let mut builder = store.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");
        let cursor = snap.snapshot;
        let bytes = cursor.into_inner();
        assert!(!bytes.is_empty());

        // Install snapshot into a fresh store
        let mut target = CustodianStore::new_temp().unwrap();
        target
            .install_snapshot(&snap.meta, Box::new(std::io::Cursor::new(bytes)))
            .await
            .expect("install snapshot");

        // Verify lock restored
        let got = target
            .storage
            .get_lock_info(ticket_id)
            .expect("get_lock_info");
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_blank_and_normal() {
        use openraft::{Entry, EntryPayload, LeaderId, LogId};

        let mut store = CustodianStore::new_temp().unwrap();
        let user_id = uuid::Uuid::new_v4();

        // Blank entry
        let blank = Entry::<CustodianTypeConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: EntryPayload::Blank,
        };

        // Normal entry (AcquireLock)
        let normal = Entry::<CustodianTypeConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 2,
            },
            payload: EntryPayload::Normal(LockCommand::AcquireLock {
                ticket_id: 55,
                user_id,
                at_unix: 0,
                ttl_secs: 0,
            }),
        };

        let responses = store
            .apply_to_state_machine(&[blank, normal])
            .await
            .unwrap();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].success);
        assert!(responses[1].success);

        // Verify the lock was actually acquired
        assert!(store.storage.is_locked(55).unwrap());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_membership() {
        use openraft::{Entry, EntryPayload, LeaderId, LogId, Membership};
        use std::collections::BTreeMap;

        let mut store = CustodianStore::new_temp().unwrap();

        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, openraft::BasicNode::default());
        let membership = Membership::new(vec![nodes.keys().copied().collect()], nodes);

        let entry = Entry::<CustodianTypeConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: EntryPayload::Membership(membership),
        };

        let responses = store.apply_to_state_machine(&[entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(responses[0].success);

        let (last_applied, _) = store.last_applied_state().await.unwrap();
        assert!(last_applied.is_some());
        assert_eq!(last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_last_applied_state_initial() {
        let mut store = CustodianStore::new_temp().unwrap();
        let (last_applied, _membership) = store.last_applied_state().await.unwrap();
        assert!(last_applied.is_none());
    }

    #[tokio::test]
    async fn test_begin_receiving_snapshot() {
        let mut store = CustodianStore::new_temp().unwrap();
        let cursor = store.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.into_inner().is_empty());
    }

    #[tokio::test]
    async fn test_try_get_log_entries_excluded_end_bound() {
        let mut store = CustodianStore::new_temp().unwrap();
        let user_id = uuid::Uuid::new_v4();

        // Append 3 entries
        for i in 1u64..=3 {
            let entry = openraft::Entry::<CustodianTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LockCommand::AcquireLock {
                    ticket_id: i,
                    user_id,
                    at_unix: 0,
                    ttl_secs: 0,
                }),
            };
            store.append_to_log(vec![entry]).await.unwrap();
        }

        // Excluded end: 1..3 means indices 1 and 2 only
        let entries = store.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_unbounded() {
        let mut store = CustodianStore::new_temp().unwrap();
        let user_id = uuid::Uuid::new_v4();

        for i in 1u64..=3 {
            let entry = openraft::Entry::<CustodianTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LockCommand::AcquireLock {
                    ticket_id: i,
                    user_id,
                    at_unix: 0,
                    ttl_secs: 0,
                }),
            };
            store.append_to_log(vec![entry]).await.unwrap();
        }

        // Fully unbounded should return all entries
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn test_raft_metadata_loads_from_existing_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let mut store = CustodianStore::new(&path).unwrap();

            // Save a vote
            let vote = openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 3,
                    node_id: 7,
                },
                committed: false,
            };
            store.save_vote(&vote).await.unwrap();

            // Append and purge to set last_purged
            let user_id = uuid::Uuid::new_v4();
            let entry = openraft::Entry::<CustodianTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                },
                payload: openraft::EntryPayload::Normal(LockCommand::AcquireLock {
                    ticket_id: 1,
                    user_id,
                    at_unix: 0,
                    ttl_secs: 0,
                }),
            };
            store.append_to_log(vec![entry]).await.unwrap();
            store
                .purge_logs_upto(openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                })
                .await
                .unwrap();
        }

        // Recreate from same path – exercises loading from disk
        let mut store2 = CustodianStore::new(&path).unwrap();
        let vote = store2.read_vote().await.unwrap();
        assert!(vote.is_some());
        assert_eq!(vote.unwrap().leader_id.term, 3);

        let state = store2.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn test_persist_vote_none_clears_vote() {
        let mut store = CustodianStore::new_temp().unwrap();

        let vote = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: false,
        };
        store.save_vote(&vote).await.unwrap();
        assert!(store.read_vote().await.unwrap().is_some());

        // Directly call persist_vote(None) to exercise the else branch
        {
            let mut meta = store.raft_storage.write().await;
            meta.vote = None;
            meta.persist_vote(None).unwrap();
        }

        assert!(store.read_vote().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_log_reader_via_get_log_reader() {
        let mut store = CustodianStore::new_temp().unwrap();
        let user_id = uuid::Uuid::new_v4();

        for i in 1u64..=2 {
            let entry = openraft::Entry::<CustodianTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LockCommand::AcquireLock {
                    ticket_id: i,
                    user_id,
                    at_unix: 0,
                    ttl_secs: 0,
                }),
            };
            store.append_to_log(vec![entry]).await.unwrap();
        }

        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    // ── Fault injection: corrupted Raft log storage ──────────────────────────

    #[tokio::test]
    async fn try_get_log_entries_errors_on_non_8_byte_key() {
        let mut store = CustodianStore::new_temp().unwrap();
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert([0u8, 0, 0, 0, 0, 0, 0, 0, 1].as_slice(), b"x".to_vec())
            .unwrap();
        assert!(store.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn try_get_log_entries_errors_on_corrupt_entry_value() {
        let mut store = CustodianStore::new_temp().unwrap();
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(1u64.to_be_bytes(), b"not-a-valid-entry".to_vec())
            .unwrap();
        assert!(store.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn get_log_state_errors_on_corrupt_last_entry() {
        let mut store = CustodianStore::new_temp().unwrap();
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(5u64.to_be_bytes(), b"garbage".to_vec())
            .unwrap();
        assert!(store.get_log_state().await.is_err());
    }

    #[tokio::test]
    async fn log_reader_errors_on_corrupt_entry_value() {
        let mut store = CustodianStore::new_temp().unwrap();
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(2u64.to_be_bytes(), b"still-not-json".to_vec())
            .unwrap();
        let mut reader = store.get_log_reader().await;
        assert!(reader.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn install_snapshot_errors_on_corrupt_snapshot_data() {
        let mut store = CustodianStore::new_temp().unwrap();
        let bad = Box::new(std::io::Cursor::new(b"not a snapshot".to_vec()));
        let meta = openraft::SnapshotMeta::default();
        assert!(store.install_snapshot(&meta, bad).await.is_err());
    }

    #[test]
    fn state_machine_apply_lock_lifecycle() {
        use crate::storage::LockCommand;

        let storage = Storage::new_temp().unwrap();
        let mut sm = CustodianStateMachine::new(storage);
        let u1 = uuid::Uuid::from_u128(1);
        let u2 = uuid::Uuid::from_u128(2);
        let acquire = |ticket_id, user_id, at_unix| LockCommand::AcquireLock {
            ticket_id,
            user_id,
            at_unix,
            ttl_secs: 900,
        };

        // Acquire on a free ticket succeeds.
        assert!(sm.apply(&acquire(1, u1, 100)).unwrap().success);

        // A conflicting acquire by another user before expiry is rejected (with the holder).
        let conflict = sm.apply(&acquire(1, u2, 200)).unwrap();
        assert!(!conflict.success);
        assert!(conflict.error.is_some());

        // The same holder re-acquiring is allowed.
        assert!(sm.apply(&acquire(1, u1, 300)).unwrap().success);

        // Past the TTL, a different user may steal the lock.
        assert!(sm.apply(&acquire(1, u2, 100_000)).unwrap().success);

        // Release by a non-holder is rejected; release by the holder succeeds.
        assert!(
            !sm.apply(&LockCommand::ReleaseLock {
                ticket_id: 1,
                user_id: u1,
            })
            .unwrap()
            .success
        );
        assert!(
            sm.apply(&LockCommand::ReleaseLock {
                ticket_id: 1,
                user_id: u2,
            })
            .unwrap()
            .success
        );
    }
}
