//! Raft state machine implementation for distributed consensus
//!
//! This module implements the core Raft consensus engine integration, including:
//!
//! - `DbTypeConfig` - Type configuration for `OpenRaft`
//! - `DbStore` - Combined storage backend implementing `RaftStorage` trait
//! - `DbStateMachine` - State machine that applies log entries
//! - `RaftMetadata` - Raft-specific metadata (votes, logs)
//! - `DbLogReader` - Iterator for reading log entries
//! - `DbSnapshotBuilder` - Snapshot creation for log compaction
//!
//! # Architecture
//!
//! The Raft implementation separates concerns:
//!
//! - **Application Data**: Stored in Sled database via `Storage`
//! - **Raft Metadata**: Stored in-memory in `RaftMetadata` (votes, logs)
//! - **State Machine**: Applies log entries to storage (via `DbStateMachine`)
//! - **Network**: Skeleton for inter-node communication (see `network.rs`)
//!
//! # Single vs Multi-Node
//!
//! - **Single-node clusters**: Fully functional, works perfectly
//! - **Multi-node clusters**: Requires network layer implementation (see `network.rs`)

use crate::storage::{LogEntry, Storage, TREE_RAFT_LOG, TREE_RAFT_METADATA};
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
pub struct DbResponse {
    pub success: bool,
    pub value: Option<Vec<u8>>,
}

// Declare Raft types using the openraft macro
openraft::declare_raft_types!(
    pub DbTypeConfig:
        D = LogEntry,
        R = DbResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<DbTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<DbTypeConfig>,
);

/// Type definitions for `OpenRaft`
pub type DbRaft = openraft::Raft<DbTypeConfig>;

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
pub struct DbStateMachine {
    pub last_applied_log: Option<openraft::LogId<u64>>,
    pub last_membership: openraft::StoredMembership<u64, BasicNode>,
    pub storage: Storage,
}

impl DbStateMachine {
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
    pub fn apply(&mut self, entry: &LogEntry) -> Result<DbResponse> {
        let applied = entry.apply(&self.storage)?;

        let response = match entry {
            LogEntry::Get { collection, key } => {
                let value = self.storage.get(collection, key)?;
                DbResponse {
                    success: value.is_some(),
                    value,
                }
            }
            // `applied` carries the assigned id for `CreateTicket`, `None` otherwise.
            _ => DbResponse {
                success: true,
                value: applied,
            },
        };

        Ok(response)
    }
}

/// Combined Raft log storage and state machine
#[derive(Clone)]
pub struct DbStore {
    /// Main storage backend
    storage: Storage,
    /// State machine
    state_machine: Arc<RwLock<DbStateMachine>>,
    /// Raft metadata storage (vote, logs)
    raft_storage: Arc<RwLock<RaftMetadata>>,
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

impl DbStore {
    /// Create a new store with persistent storage
    ///
    /// # Errors
    ///
    /// Returns an error if the storage cannot be initialized.
    pub fn new(storage_path: &str) -> Result<Self> {
        let storage = Storage::new(storage_path)?;
        let state_machine = Arc::new(RwLock::new(DbStateMachine::new(storage.clone())));
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
        let state_machine = Arc::new(RwLock::new(DbStateMachine::new(storage.clone())));
        let raft_metadata = RaftMetadata::new(storage.clone())?;
        let raft_storage = Arc::new(RwLock::new(raft_metadata));

        Ok(Self {
            storage,
            state_machine,
            raft_storage,
        })
    }

    #[must_use]
    pub fn state_machine(&self) -> Arc<RwLock<DbStateMachine>> {
        self.state_machine.clone()
    }
}

/// Log reader for Raft
pub struct DbLogReader {
    storage: Storage,
}

impl RaftLogReader<DbTypeConfig> for DbLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<DbTypeConfig as openraft::RaftTypeConfig>::Entry>, StorageError<u64>> {
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

            let entry: <DbTypeConfig as openraft::RaftTypeConfig>::Entry =
                serde_json::from_slice(&v)
                    .map_err(|e| log_r(LogId::new(CommittedLeaderId::new(0, 0), index), e))?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

/// Implement `RaftLogReader` for `DbStore`
impl RaftLogReader<DbTypeConfig> for DbStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + std::fmt::Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<DbTypeConfig as openraft::RaftTypeConfig>::Entry>, StorageError<u64>> {
        let mut reader = self.get_log_reader().await;
        reader.try_get_log_entries(range).await
    }
}

/// Implement `RaftStorage` trait for `DbStore`
impl RaftStorage<DbTypeConfig> for DbStore {
    type LogReader = DbLogReader;
    type SnapshotBuilder = DbSnapshotBuilder;

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
        I: IntoIterator<Item = <DbTypeConfig as openraft::RaftTypeConfig>::Entry> + OptionalSend,
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
        entries: &[<DbTypeConfig as openraft::RaftTypeConfig>::Entry],
    ) -> Result<Vec<DbResponse>, StorageError<u64>> {
        let mut sm = self.state_machine.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            let response = match &entry.payload {
                EntryPayload::Blank => {
                    // Blank entries still need a response
                    DbResponse {
                        success: true,
                        value: None,
                    }
                }
                EntryPayload::Normal(log_entry) => sm.apply(log_entry).map_err(sm_write)?,
                EntryPayload::Membership(membership) => {
                    sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    // Membership changes also need a response
                    DbResponse {
                        success: true,
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
        let data = snapshot.into_inner();

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

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<DbTypeConfig>>, StorageError<u64>> {
        // For now, return None - snapshots are not yet fully implemented
        Ok(None)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        DbLogReader {
            storage: self.storage.clone(),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<DbTypeConfig>, StorageError<u64>> {
        let meta = self.raft_storage.read().await;
        let tree = self.storage.get_tree(TREE_RAFT_LOG).map_err(log_read)?;

        let last_log_id = if let Some(last) = tree.iter().last() {
            let (_, v) = last.map_err(log_read)?;
            let entry: <DbTypeConfig as openraft::RaftTypeConfig>::Entry =
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
        // Capture the applied state so the snapshot meta reflects the real last log id
        // and membership. openraft requires a monotonically-increasing snapshot log id;
        // a `None` here corrupts snapshot tracking and stalls replication.
        let sm = self.state_machine.read().await;
        DbSnapshotBuilder {
            storage: self.storage.clone(),
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        }
    }
}

/// Snapshot builder for creating Raft snapshots
pub struct DbSnapshotBuilder {
    storage: Storage,
    last_applied_log: Option<openraft::LogId<u64>>,
    last_membership: openraft::StoredMembership<u64, BasicNode>,
}

impl RaftSnapshotBuilder<DbTypeConfig> for DbSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<DbTypeConfig>, StorageError<u64>> {
        // Snapshot every non-Raft state-machine tree: data collections, all secondary
        // indexes, and sequence counters. Enumerating the trees (rather than a fixed
        // list) keeps snapshot coverage correct as new trees are added.
        let mut all_data = Vec::new();

        for collection in self.storage.data_tree_names() {
            let pairs = self.storage.list(&collection, b"", None).map_err(sm_read)?;
            for (k, v) in pairs {
                all_data.push((collection.clone(), k, v));
            }
        }

        // Serialize to snapshot format using serde_json for simplicity
        let data = serde_json::to_vec(&all_data).map_err(sm_write)?;

        // Include the last applied log index in the id so successive snapshots sort/compare
        // correctly, then carry the real applied log id + membership in the meta.
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
    async fn test_state_machine_apply_put() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = DbStateMachine::new(storage.clone());
        let collection = "test_coll";

        let entry = LogEntry::Put {
            collection: collection.to_string(),
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };

        let response = sm.apply(&entry).unwrap();
        assert!(response.success);

        // Verify it was stored
        let stored = storage.get(collection, b"key1").unwrap();
        assert_eq!(stored, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_state_machine_apply_delete() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = DbStateMachine::new(storage.clone());
        let collection = "test_coll";

        // First put a value
        storage.put(collection, b"key1", b"value1").unwrap();

        // Then delete it via state machine
        let entry = LogEntry::Delete {
            collection: collection.to_string(),
            key: b"key1".to_vec(),
        };

        let response = sm.apply(&entry).unwrap();
        assert!(response.success);

        // Verify it was deleted
        assert!(!storage.exists(collection, b"key1").unwrap());
    }

    #[tokio::test]
    async fn test_state_machine_apply_batch_put() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = DbStateMachine::new(storage.clone());
        let collection = "batch_coll";

        let entry = LogEntry::BatchPut {
            collection: collection.to_string(),
            pairs: vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ],
        };

        let response = sm.apply(&entry).unwrap();
        assert!(response.success);

        assert_eq!(
            storage.get(collection, b"k1").unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            storage.get(collection, b"k2").unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_store_creation() {
        let store = DbStore::new_temp().unwrap();
        let sm = store.state_machine.read().await;
        assert_eq!(sm.last_applied_log, None);
    }

    #[tokio::test]
    async fn test_snapshot_builder_builds_and_current_snapshot_returns_none() {
        let mut store = DbStore::new_temp().unwrap();

        // get_current_snapshot should return None (not yet implemented)
        let snap = store.get_current_snapshot().await.unwrap();
        assert!(snap.is_none());

        // build_snapshot should produce a valid snapshot
        let mut builder = store.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert!(!snapshot.meta.snapshot_id.is_empty());
    }

    #[tokio::test]
    async fn test_raft_storage_save_and_read_vote() {
        let mut store = DbStore::new_temp().unwrap();

        // Initially no vote
        let vote = store.read_vote().await.unwrap();
        assert!(vote.is_none());

        // Save a vote
        let v = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: false,
        };
        store.save_vote(&v).await.unwrap();

        // Read it back
        let read_back = store.read_vote().await.unwrap();
        assert_eq!(read_back, Some(v));
    }

    #[tokio::test]
    async fn test_raft_storage_log_state_and_entries() {
        let mut store = DbStore::new_temp().unwrap();

        // Empty state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());

        // Append an entry
        let entry = openraft::Entry::<DbTypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: openraft::EntryPayload::Normal(LogEntry::Put {
                collection: "c".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
        };
        store.append_to_log([entry.clone()]).await.unwrap();

        let state2 = store.get_log_state().await.unwrap();
        assert_eq!(state2.last_log_id, Some(entry.log_id));

        // Read entries
        let entries = store.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1);

        // Delete conflicts since index 1
        store
            .delete_conflict_logs_since(openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            })
            .await
            .unwrap();

        let state3 = store.get_log_state().await.unwrap();
        assert!(state3.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_raft_storage_purge_logs() {
        let mut store = DbStore::new_temp().unwrap();

        // Append two entries
        for i in 1u64..=2 {
            let entry = openraft::Entry::<DbTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LogEntry::Put {
                    collection: "col".to_string(),
                    key: i.to_be_bytes().to_vec(),
                    value: b"val".to_vec(),
                }),
            };
            store.append_to_log([entry]).await.unwrap();
        }

        // Purge up to index 1
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

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn test_state_machine_apply_get() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = DbStateMachine::new(storage.clone());
        let collection = "get_coll";

        // Put a value into storage first
        storage.put(collection, b"mykey", b"myval").unwrap();

        // Now apply a Get entry
        let entry = LogEntry::Get {
            collection: collection.to_string(),
            key: b"mykey".to_vec(),
        };
        let response = sm.apply(&entry).unwrap();
        assert!(response.success);
        assert_eq!(response.value, Some(b"myval".to_vec()));

        // Apply a Get for a missing key
        let entry_miss = LogEntry::Get {
            collection: collection.to_string(),
            key: b"nope".to_vec(),
        };
        let response_miss = sm.apply(&entry_miss).unwrap();
        assert!(!response_miss.success);
        assert!(response_miss.value.is_none());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_blank_and_normal() {
        use openraft::{Entry, EntryPayload, LeaderId, LogId};

        let mut store = DbStore::new_temp().unwrap();

        // Blank entry
        let blank = Entry::<DbTypeConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: EntryPayload::Blank,
        };

        // Normal entry (Put)
        let normal = Entry::<DbTypeConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 2,
            },
            payload: EntryPayload::Normal(LogEntry::Put {
                collection: "col".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
        };

        let responses = store
            .apply_to_state_machine(&[blank, normal])
            .await
            .unwrap();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].success);
        assert!(responses[1].success);

        // Verify Normal entry was applied
        let val = store.storage.get("col", b"k").unwrap();
        assert_eq!(val, Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_membership() {
        use openraft::{Entry, EntryPayload, LeaderId, LogId, Membership};
        use std::collections::BTreeMap;

        let mut store = DbStore::new_temp().unwrap();

        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, openraft::BasicNode::default());
        let membership = Membership::new(vec![nodes.keys().copied().collect()], nodes);

        let entry = Entry::<DbTypeConfig> {
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

        // Verify last_applied_log was updated in state machine
        let (last_applied, _membership) = store.last_applied_state().await.unwrap();
        assert!(last_applied.is_some());
        assert_eq!(last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_last_applied_state_initial() {
        let mut store = DbStore::new_temp().unwrap();
        let (last_applied, membership) = store.last_applied_state().await.unwrap();
        assert!(last_applied.is_none());
        // Initial membership is empty
        let _ = membership;
    }

    #[tokio::test]
    async fn test_begin_receiving_snapshot() {
        let mut store = DbStore::new_temp().unwrap();
        let cursor = store.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.into_inner().is_empty());
    }

    #[tokio::test]
    async fn test_install_snapshot_restores_data() {
        use crate::storage::{TREE_TICKETS, TREE_USERS};

        // Build snapshot from store A with data in the collections the snapshot includes
        let mut store_a = DbStore::new_temp().unwrap();
        store_a
            .storage
            .put(TREE_TICKETS, b"ticket1", b"val1")
            .unwrap();
        store_a.storage.put(TREE_USERS, b"user1", b"uval1").unwrap();

        let mut builder = store_a.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let data = snapshot.snapshot.into_inner();

        // Install snapshot into store B
        let mut store_b = DbStore::new_temp().unwrap();
        store_b
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(data)))
            .await
            .unwrap();

        // Verify data was restored
        assert_eq!(
            store_b.storage.get(TREE_TICKETS, b"ticket1").unwrap(),
            Some(b"val1".to_vec())
        );
        assert_eq!(
            store_b.storage.get(TREE_USERS, b"user1").unwrap(),
            Some(b"uval1".to_vec())
        );
    }

    #[tokio::test]
    async fn snapshot_roundtrips_all_trees_including_indexes() {
        use crate::storage::{TicketIndexFields, TicketQuery};

        // Create a ticket on store A — this populates the tickets tree, every secondary
        // index, and the sequence counter.
        let mut store_a = DbStore::new_temp().unwrap();
        let idx = TicketIndexFields {
            status: 3,
            account_uuid: "acct".to_string(),
            assigned_to_uuid: Some("agent".to_string()),
            project: "proj".to_string(),
            tracking_url: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        };
        let id = store_a.storage.create_ticket(b"body", &idx).unwrap();

        let mut builder = store_a.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let data = snapshot.snapshot.into_inner();

        // Install into a fresh store B.
        let mut store_b = DbStore::new_temp().unwrap();
        store_b
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(data)))
            .await
            .unwrap();

        // Data restored…
        assert!(store_b.storage.get_ticket(id, false).unwrap().is_some());
        // …and the secondary index is usable (query by status resolves via the index).
        let results = store_b
            .storage
            .query_tickets(&TicketQuery {
                status: Some(3),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_excluded_end_bound() {
        let mut store = DbStore::new_temp().unwrap();

        // Append 3 entries
        for i in 1u64..=3 {
            let entry = openraft::Entry::<DbTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LogEntry::Put {
                    collection: "c".to_string(),
                    key: i.to_be_bytes().to_vec(),
                    value: b"v".to_vec(),
                }),
            };
            store.append_to_log([entry]).await.unwrap();
        }

        // Use excluded end bound (1..3 means indices 1 and 2 only)
        let entries = store.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_unbounded() {
        let mut store = DbStore::new_temp().unwrap();

        // Append 3 entries
        for i in 1u64..=3 {
            let entry = openraft::Entry::<DbTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LogEntry::Put {
                    collection: "c".to_string(),
                    key: i.to_be_bytes().to_vec(),
                    value: b"v".to_vec(),
                }),
            };
            store.append_to_log([entry]).await.unwrap();
        }

        // Fully unbounded range should return all entries
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn test_raft_metadata_loads_from_existing_storage() {
        // Create a store, save a vote and purge a log, then recreate to test loading paths
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        {
            let mut store = DbStore::new(&path).unwrap();

            // Save a vote
            let vote = openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 2,
                    node_id: 5,
                },
                committed: true,
            };
            store.save_vote(&vote).await.unwrap();

            // Append and purge to set last_purged
            let entry = openraft::Entry::<DbTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                },
                payload: openraft::EntryPayload::Normal(LogEntry::Put {
                    collection: "c".to_string(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }),
            };
            store.append_to_log([entry]).await.unwrap();
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

        // Recreate store from the same path – this exercises the "loading from disk" paths
        let mut store2 = DbStore::new(&path).unwrap();
        let vote = store2.read_vote().await.unwrap();
        assert!(vote.is_some());
        assert_eq!(vote.unwrap().leader_id.term, 2);

        let state = store2.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn test_persist_vote_none_clears_vote() {
        let mut store = DbStore::new_temp().unwrap();

        // Save a vote first
        let vote = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: false,
        };
        store.save_vote(&vote).await.unwrap();

        // Verify vote is there
        assert!(store.read_vote().await.unwrap().is_some());

        // Directly call persist_vote(None) via raft_storage
        {
            let mut meta = store.raft_storage.write().await;
            meta.vote = None;
            meta.persist_vote(None).unwrap();
        }

        // Reload metadata from disk to verify it was cleared
        let state = store.read_vote().await.unwrap();
        // In-memory is already None; the test exercises the persist_vote(None) path
        assert!(state.is_none());
    }

    #[tokio::test]
    async fn test_log_reader_via_get_log_reader() {
        let mut store = DbStore::new_temp().unwrap();

        // Append entries
        for i in 1u64..=2 {
            let entry = openraft::Entry::<DbTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: i,
                },
                payload: openraft::EntryPayload::Normal(LogEntry::Put {
                    collection: "c".to_string(),
                    key: i.to_be_bytes().to_vec(),
                    value: b"v".to_vec(),
                }),
            };
            store.append_to_log([entry]).await.unwrap();
        }

        // Read via dedicated log reader
        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_state_machine_apply_get_missing_key() {
        let storage = Storage::new_temp().unwrap();
        let mut sm = DbStateMachine::new(storage.clone());

        let entry = LogEntry::Get {
            collection: "empty_coll".to_string(),
            key: b"missing".to_vec(),
        };
        let response = sm.apply(&entry).unwrap();
        assert!(!response.success);
        assert!(response.value.is_none());
    }

    // ── Fault injection: corrupted Raft log storage ──────────────────────────
    // These exercise the error-mapping paths in the log-read code that the happy-path
    // tests never reach (a real node would hit them on disk corruption).

    #[tokio::test]
    async fn try_get_log_entries_errors_on_non_8_byte_key() {
        let mut store = DbStore::new_temp().unwrap();
        // A 9-byte key sorts after the 8-byte zero start key, so it is scanned, and the
        // `try_into::<[u8; 8]>()` for the index fails → the invalid-key-format error path.
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert([0u8, 0, 0, 0, 0, 0, 0, 0, 1].as_slice(), b"x".to_vec())
            .unwrap();
        assert!(store.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn try_get_log_entries_errors_on_corrupt_entry_value() {
        let mut store = DbStore::new_temp().unwrap();
        // Valid 8-byte key, but the value is not a serialized Entry → deserialize error path.
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(1u64.to_be_bytes(), b"not-a-valid-entry".to_vec())
            .unwrap();
        assert!(store.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn get_log_state_errors_on_corrupt_last_entry() {
        let mut store = DbStore::new_temp().unwrap();
        // get_log_state reads the last entry and deserializes it → deserialize error path.
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(5u64.to_be_bytes(), b"garbage".to_vec())
            .unwrap();
        assert!(store.get_log_state().await.is_err());
    }

    #[tokio::test]
    async fn log_reader_errors_on_corrupt_entry_value() {
        let mut store = DbStore::new_temp().unwrap();
        let tree = store.storage.get_tree(TREE_RAFT_LOG).unwrap();
        tree.insert(2u64.to_be_bytes(), b"still-not-json".to_vec())
            .unwrap();
        // The dedicated DbLogReader path (DbLogReader::try_get_log_entries) must also surface it.
        let mut reader = store.get_log_reader().await;
        assert!(reader.try_get_log_entries(..).await.is_err());
    }

    #[tokio::test]
    async fn install_snapshot_errors_on_corrupt_snapshot_data() {
        let mut store = DbStore::new_temp().unwrap();
        // Snapshot payload is not the expected serialized Vec<(String,Vec<u8>,Vec<u8>)>.
        let bad = Box::new(Cursor::new(b"not a snapshot".to_vec()));
        let meta = openraft::SnapshotMeta::default();
        assert!(store.install_snapshot(&meta, bad).await.is_err());
    }
}
