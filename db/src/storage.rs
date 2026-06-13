//! Storage backend using Sled embedded database
//!
//! This module provides a persistent key-value store wrapper around Sled,
//! a high-performance embedded database written in Rust.
//!
//! # Operations
//!
//! - Single-key: `put`, `get`, `delete`, `exists`
//! - Multi-key: `list` (with prefix filtering), `batch_put`
//! - Testing: `new_temp` (creates temporary in-memory store)
//!
//! # Persistence
//!
//! All write operations are automatically flushed to disk, ensuring durability.
//! Data persists across process restarts.
//!
//! # Raft Integration
//!
//! The `LogEntry` enum represents all database operations that can be replicated
//! through Raft consensus. Each entry type can be applied to storage independently.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::path::Path;

// Tree Names
pub const TREE_RAFT_METADATA: &str = "raft_metadata";
pub const TREE_RAFT_STATE: &str = "raft_state";
pub const TREE_RAFT_LOG: &str = "raft_log";
pub const TREE_TICKETS: &str = "tickets";
pub const TREE_USERS: &str = "users";
pub const TREE_SESSIONS: &str = "sessions";
pub const TREE_AUDIT: &str = "audit";

// Sequence counters (monotonic ID allocation, applied deterministically via Raft)
pub const TREE_SEQ: &str = "seq";
pub const SEQ_TICKET: &[u8] = b"ticket";

// Index Trees
pub const IDX_TICKET_STATUS: &str = "idx_ticket_status";
pub const IDX_TICKET_ASSIGNEE: &str = "idx_ticket_assignee";
pub const IDX_TICKET_PROJECT: &str = "idx_ticket_project";
pub const IDX_TICKET_ACCOUNT: &str = "idx_ticket_account";
pub const IDX_TICKET_CREATED: &str = "idx_ticket_created";
pub const IDX_TICKET_UPDATED: &str = "idx_ticket_updated";
pub const IDX_TICKET_TRACKING: &str = "idx_ticket_tracking";
pub const IDX_USER_NAME: &str = "idx_user_name";
pub const IDX_USER_EMAIL: &str = "idx_user_email";
pub const IDX_USER_ROLE: &str = "idx_user_role";

/// Storage layer wrapping Sled with Namespaced Trees
#[derive(Clone)]
pub struct Storage {
    db: Db,
}

impl Storage {
    /// Create a new storage instance
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened at the specified path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Create an in-memory storage for testing
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary database cannot be created.
    pub fn new_temp() -> Result<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(Self { db })
    }

    /// Get the underlying Sled database for metadata access
    #[must_use]
    pub fn inner(&self) -> &Db {
        &self.db
    }

    /// Open or get a handle to a specific tree (namespace)
    ///
    /// # Errors
    ///
    /// Returns an error if the tree cannot be opened.
    pub fn get_tree(&self, name: &str) -> Result<Tree> {
        self.db.open_tree(name).context("failed to open tree")
    }

    /// Names of all non-Raft state-machine trees: data collections, every secondary
    /// index, and sequence counters. Raft-internal trees and sled's default tree are
    /// excluded. Snapshots iterate this so coverage tracks new trees automatically.
    #[must_use]
    pub fn data_tree_names(&self) -> Vec<String> {
        let skip: [&[u8]; 4] = [
            TREE_RAFT_METADATA.as_bytes(),
            TREE_RAFT_STATE.as_bytes(),
            TREE_RAFT_LOG.as_bytes(),
            b"__sled__default",
        ];
        self.db
            .tree_names()
            .into_iter()
            .filter(|name| !skip.contains(&name.as_ref()))
            .map(|name| String::from_utf8_lossy(&name).into_owned())
            .collect()
    }

    /// Store a key-value pair in a specific collection (tree)
    ///
    /// # Errors
    ///
    /// Returns an error if the key-value pair cannot be stored.
    pub fn put(&self, collection: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let tree = self.get_tree(collection)?;
        tree.insert(key, value)?;
        // We might want to flush periodically rather than every put for performance,
        // but for safety in this critical DB, explicit flush is safer.
        // However, Sled flushes asynchronously by default.
        // For Raft, we usually rely on the Raft log flush.
        // Let's keep it simple for now.
        tree.flush()?;
        Ok(())
    }

    /// Retrieve a value by key from a specific collection
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be retrieved.
    pub fn get(&self, collection: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree = self.get_tree(collection)?;
        Ok(tree.get(key)?.map(|v| v.to_vec()))
    }

    /// Delete a key from a specific collection
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be deleted.
    pub fn delete(&self, collection: &str, key: &[u8]) -> Result<()> {
        let tree = self.get_tree(collection)?;
        tree.remove(key)?;
        tree.flush()?;
        Ok(())
    }

    /// List key-value pairs with optional prefix from a specific collection
    ///
    /// # Errors
    ///
    /// Returns an error if the collection cannot be listed.
    pub fn list(
        &self,
        collection: &str,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tree = self.get_tree(collection)?;
        let iter = if prefix.is_empty() {
            tree.iter()
        } else {
            tree.scan_prefix(prefix)
        };

        let pairs: Result<Vec<_>, _> = iter
            .take(limit.unwrap_or(usize::MAX))
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();

        Ok(pairs?)
    }

    /// Check if key exists in a collection
    ///
    /// # Errors
    ///
    /// Returns an error if the existence check fails.
    pub fn exists(&self, collection: &str, key: &[u8]) -> Result<bool> {
        let tree = self.get_tree(collection)?;
        Ok(tree.contains_key(key)?)
    }

    /// Batch put operation into a specific collection
    ///
    /// # Errors
    ///
    /// Returns an error if the batch operation fails.
    pub fn batch_put(&self, collection: &str, pairs: &Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize> {
        let tree = self.get_tree(collection)?;
        let mut batch = sled::Batch::default();
        for (key, value) in pairs {
            batch.insert(key.as_slice(), value.as_slice());
        }
        tree.apply_batch(batch)?;
        tree.flush()?;
        Ok(pairs.len())
    }
}

// ============================================================================
// Domain records — hybrid storage model
//
// Tickets and users are stored as an opaque, custodian-encrypted `body` blob
// alongside a small set of *plaintext* index fields. The DB never decrypts the
// body; it only reads the plaintext index fields to maintain secondary indexes
// and to answer `QueryTickets` filters. This preserves encryption-at-rest for
// ticket contents while still enabling server-side query/indexing.
// ============================================================================

/// Plaintext, indexable fields extracted from a ticket by the custodian.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketIndexFields {
    /// Domain `TicketStatus` as its `#[repr(u8)]` discriminant.
    pub status: u8,
    pub account_uuid: String,
    pub assigned_to_uuid: Option<String>,
    pub project: String,
    pub tracking_url: Option<String>,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

/// A stored ticket: opaque encrypted body + plaintext index fields + soft-delete state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTicket {
    pub body: Vec<u8>,
    pub index: TicketIndexFields,
    pub deleted: bool,
    pub deleted_at_unix: i64,
}

/// Plaintext, indexable fields extracted from a user.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserIndexFields {
    pub username: String,
    pub email: String,
    /// Domain `Role` as its `#[repr(u8)]` discriminant.
    pub role: u8,
}

/// A stored user: opaque encrypted body + plaintext index fields + soft-delete state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredUser {
    pub body: Vec<u8>,
    pub index: UserIndexFields,
    pub deleted: bool,
    pub deleted_at_unix: i64,
}

/// Filter for [`Storage::query_tickets`]. Set fields are combined with AND; `None` means any.
#[derive(Debug, Clone, Default)]
pub struct TicketQuery {
    pub status: Option<u8>,
    pub assigned_to_uuid: Option<String>,
    pub account_uuid: Option<String>,
    pub project: Option<String>,
    pub include_deleted: bool,
    /// Maximum results; `0` means unlimited.
    pub limit: usize,
}

/// Build a secondary-index key: `<value>\x00<entity_key>` with value stored as the entity key.
fn index_key(value: &[u8], entity_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(value.len() + 1 + entity_key.len());
    k.extend_from_slice(value);
    k.push(0u8);
    k.extend_from_slice(entity_key);
    k
}

/// Build the scan prefix for all entities matching `value` in a secondary index.
fn index_prefix(value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(value.len() + 1);
    k.extend_from_slice(value);
    k.push(0u8);
    k
}

impl Storage {
    /// Allocate the next monotonic id for `name` from [`TREE_SEQ`].
    ///
    /// Deterministic across nodes: Raft applies entries in identical order under the
    /// state-machine lock, so each node derives the same sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if the counter cannot be read or written.
    pub fn next_id(&self, name: &[u8]) -> Result<u64> {
        let tree = self.get_tree(TREE_SEQ)?;
        let updated = tree.update_and_fetch(name, |old| {
            let next = match old {
                Some(bytes) => {
                    let arr: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                    u64::from_be_bytes(arr).saturating_add(1)
                }
                None => 1,
            };
            Some(next.to_be_bytes().to_vec())
        })?;
        tree.flush()?;
        let bytes = updated.context("sequence counter missing after update")?;
        let arr: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| anyhow::anyhow!("corrupt sequence counter"))?;
        Ok(u64::from_be_bytes(arr))
    }

    fn ticket_index_entries(index: &TicketIndexFields) -> Vec<(&'static str, Vec<u8>)> {
        let mut entries = vec![
            (IDX_TICKET_STATUS, vec![index.status]),
            (IDX_TICKET_ACCOUNT, index.account_uuid.clone().into_bytes()),
            (IDX_TICKET_PROJECT, index.project.clone().into_bytes()),
            (
                IDX_TICKET_CREATED,
                index.created_at_unix.to_be_bytes().to_vec(),
            ),
            (
                IDX_TICKET_UPDATED,
                index.updated_at_unix.to_be_bytes().to_vec(),
            ),
        ];
        if let Some(assignee) = &index.assigned_to_uuid {
            entries.push((IDX_TICKET_ASSIGNEE, assignee.clone().into_bytes()));
        }
        if let Some(tracking) = &index.tracking_url {
            entries.push((IDX_TICKET_TRACKING, tracking.clone().into_bytes()));
        }
        entries
    }

    fn write_ticket_indexes(&self, id: u64, index: &TicketIndexFields, insert: bool) -> Result<()> {
        let entity = id.to_be_bytes();
        for (tree_name, value) in Self::ticket_index_entries(index) {
            let tree = self.get_tree(tree_name)?;
            let key = index_key(&value, &entity);
            if insert {
                tree.insert(key, &entity)?;
            } else {
                tree.remove(key)?;
            }
            tree.flush()?;
        }
        Ok(())
    }

    /// Create a ticket from an encrypted body + plaintext index fields, returning the assigned id.
    ///
    /// # Errors
    ///
    /// Returns an error if id allocation or persistence fails.
    pub fn create_ticket(&self, body: &[u8], index: &TicketIndexFields) -> Result<u64> {
        let id = self.next_id(SEQ_TICKET)?;
        let stored = StoredTicket {
            body: body.to_vec(),
            index: index.clone(),
            deleted: false,
            deleted_at_unix: 0,
        };
        self.put(
            TREE_TICKETS,
            &id.to_be_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        self.write_ticket_indexes(id, index, true)?;
        Ok(id)
    }

    /// Read a stored ticket. Returns `None` if missing, or if soft-deleted and `include_deleted` is false.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be read or deserialized.
    pub fn get_ticket(&self, id: u64, include_deleted: bool) -> Result<Option<StoredTicket>> {
        match self.get(TREE_TICKETS, &id.to_be_bytes())? {
            Some(bytes) => {
                let stored: StoredTicket = serde_json::from_slice(&bytes)?;
                if stored.deleted && !include_deleted {
                    Ok(None)
                } else {
                    Ok(Some(stored))
                }
            }
            None => Ok(None),
        }
    }

    /// Update an existing ticket, re-pointing its secondary indexes (old entries removed, new added).
    ///
    /// # Errors
    ///
    /// Returns an error if the ticket does not exist or persistence fails.
    pub fn update_ticket(&self, id: u64, body: &[u8], index: &TicketIndexFields) -> Result<()> {
        let old_bytes = self
            .get(TREE_TICKETS, &id.to_be_bytes())?
            .with_context(|| format!("ticket {id} not found"))?;
        let old: StoredTicket = serde_json::from_slice(&old_bytes)?;

        // A soft-deleted ticket has no active index entries to remove.
        if !old.deleted {
            self.write_ticket_indexes(id, &old.index, false)?;
        }
        let stored = StoredTicket {
            body: body.to_vec(),
            index: index.clone(),
            deleted: old.deleted,
            deleted_at_unix: old.deleted_at_unix,
        };
        self.put(
            TREE_TICKETS,
            &id.to_be_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        if !stored.deleted {
            self.write_ticket_indexes(id, index, true)?;
        }
        Ok(())
    }

    /// Soft-delete a ticket: mark deleted (audit row kept) and drop it from active indexes.
    /// `at_unix` is supplied by the caller (not read from the clock) to keep Raft apply deterministic.
    ///
    /// # Errors
    ///
    /// Returns an error if the ticket does not exist or persistence fails.
    pub fn soft_delete_ticket(&self, id: u64, at_unix: i64) -> Result<()> {
        let bytes = self
            .get(TREE_TICKETS, &id.to_be_bytes())?
            .with_context(|| format!("ticket {id} not found"))?;
        let mut stored: StoredTicket = serde_json::from_slice(&bytes)?;
        if !stored.deleted {
            self.write_ticket_indexes(id, &stored.index, false)?;
        }
        stored.deleted = true;
        stored.deleted_at_unix = at_unix;
        self.put(
            TREE_TICKETS,
            &id.to_be_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        Ok(())
    }

    fn all_ticket_ids(&self) -> Result<Vec<u64>> {
        Ok(self
            .list(TREE_TICKETS, b"", None)?
            .into_iter()
            .filter_map(|(k, _)| k.try_into().ok().map(u64::from_be_bytes))
            .collect())
    }

    fn scan_index_ids(&self, tree_name: &str, value: &[u8]) -> Result<Vec<u64>> {
        let tree = self.get_tree(tree_name)?;
        let mut ids = Vec::new();
        for item in tree.scan_prefix(index_prefix(value)) {
            let (_, v) = item?;
            if let Ok(arr) = v.as_ref().try_into() {
                ids.push(u64::from_be_bytes(arr));
            }
        }
        Ok(ids)
    }

    /// Scan a secondary index whose entity keys are UTF-8 strings (e.g. user UUIDs).
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails.
    pub fn scan_index_ids_str(&self, tree_name: &str, value: &[u8]) -> Result<Vec<String>> {
        let tree = self.get_tree(tree_name)?;
        let mut ids = Vec::new();
        for item in tree.scan_prefix(index_prefix(value)) {
            let (_, v) = item?;
            ids.push(String::from_utf8_lossy(v.as_ref()).into_owned());
        }
        Ok(ids)
    }

    /// Query tickets by index, applying remaining filters in memory. Returns `(id, ticket)` pairs.
    ///
    /// When `include_deleted` is set the active indexes (which exclude deleted rows) cannot be
    /// used, so a full scan is performed for correctness.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying scans fail.
    pub fn query_tickets(&self, query: &TicketQuery) -> Result<Vec<(u64, StoredTicket)>> {
        let candidates: Vec<u64> = if query.include_deleted {
            self.all_ticket_ids()?
        } else if let Some(status) = query.status {
            self.scan_index_ids(IDX_TICKET_STATUS, &[status])?
        } else if let Some(assignee) = &query.assigned_to_uuid {
            self.scan_index_ids(IDX_TICKET_ASSIGNEE, assignee.as_bytes())?
        } else if let Some(account) = &query.account_uuid {
            self.scan_index_ids(IDX_TICKET_ACCOUNT, account.as_bytes())?
        } else if let Some(project) = &query.project {
            self.scan_index_ids(IDX_TICKET_PROJECT, project.as_bytes())?
        } else {
            self.all_ticket_ids()?
        };

        let mut out = Vec::new();
        for id in candidates {
            let Some(stored) = self.get_ticket(id, query.include_deleted)? else {
                continue;
            };
            if query.status.is_some_and(|s| stored.index.status != s) {
                continue;
            }
            if let Some(assignee) = &query.assigned_to_uuid
                && stored.index.assigned_to_uuid.as_deref() != Some(assignee.as_str())
            {
                continue;
            }
            if query
                .account_uuid
                .as_ref()
                .is_some_and(|a| &stored.index.account_uuid != a)
            {
                continue;
            }
            if query
                .project
                .as_ref()
                .is_some_and(|p| &stored.index.project != p)
            {
                continue;
            }
            out.push((id, stored));
            if query.limit > 0 && out.len() >= query.limit {
                break;
            }
        }
        Ok(out)
    }

    fn user_index_entries(index: &UserIndexFields) -> Vec<(&'static str, Vec<u8>)> {
        vec![
            (IDX_USER_NAME, index.username.clone().into_bytes()),
            (IDX_USER_EMAIL, index.email.clone().into_bytes()),
            (IDX_USER_ROLE, vec![index.role]),
        ]
    }

    fn write_user_indexes(
        &self,
        user_uuid: &str,
        index: &UserIndexFields,
        insert: bool,
    ) -> Result<()> {
        let entity = user_uuid.as_bytes();
        for (tree_name, value) in Self::user_index_entries(index) {
            let tree = self.get_tree(tree_name)?;
            let key = index_key(&value, entity);
            if insert {
                tree.insert(key, entity)?;
            } else {
                tree.remove(key)?;
            }
            tree.flush()?;
        }
        Ok(())
    }

    /// Create a user keyed by `user_uuid` from an encrypted body + plaintext index fields.
    ///
    /// # Errors
    ///
    /// Returns an error if persistence fails.
    pub fn create_user(&self, user_uuid: &str, body: &[u8], index: &UserIndexFields) -> Result<()> {
        let stored = StoredUser {
            body: body.to_vec(),
            index: index.clone(),
            deleted: false,
            deleted_at_unix: 0,
        };
        self.put(
            TREE_USERS,
            user_uuid.as_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        self.write_user_indexes(user_uuid, index, true)?;
        Ok(())
    }

    /// Read a stored user. Returns `None` if missing, or if soft-deleted and `include_deleted` is false.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be read or deserialized.
    pub fn get_user(&self, user_uuid: &str, include_deleted: bool) -> Result<Option<StoredUser>> {
        match self.get(TREE_USERS, user_uuid.as_bytes())? {
            Some(bytes) => {
                let stored: StoredUser = serde_json::from_slice(&bytes)?;
                if stored.deleted && !include_deleted {
                    Ok(None)
                } else {
                    Ok(Some(stored))
                }
            }
            None => Ok(None),
        }
    }

    /// Update an existing user, re-pointing its secondary indexes.
    ///
    /// # Errors
    ///
    /// Returns an error if the user does not exist or persistence fails.
    pub fn update_user(&self, user_uuid: &str, body: &[u8], index: &UserIndexFields) -> Result<()> {
        let old_bytes = self
            .get(TREE_USERS, user_uuid.as_bytes())?
            .with_context(|| format!("user {user_uuid} not found"))?;
        let old: StoredUser = serde_json::from_slice(&old_bytes)?;
        if !old.deleted {
            self.write_user_indexes(user_uuid, &old.index, false)?;
        }
        let stored = StoredUser {
            body: body.to_vec(),
            index: index.clone(),
            deleted: old.deleted,
            deleted_at_unix: old.deleted_at_unix,
        };
        self.put(
            TREE_USERS,
            user_uuid.as_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        if !stored.deleted {
            self.write_user_indexes(user_uuid, index, true)?;
        }
        Ok(())
    }

    /// Soft-delete a user: mark deleted and drop from active indexes. `at_unix` supplied by caller.
    ///
    /// # Errors
    ///
    /// Returns an error if the user does not exist or persistence fails.
    pub fn soft_delete_user(&self, user_uuid: &str, at_unix: i64) -> Result<()> {
        let bytes = self
            .get(TREE_USERS, user_uuid.as_bytes())?
            .with_context(|| format!("user {user_uuid} not found"))?;
        let mut stored: StoredUser = serde_json::from_slice(&bytes)?;
        if !stored.deleted {
            self.write_user_indexes(user_uuid, &stored.index, false)?;
        }
        stored.deleted = true;
        stored.deleted_at_unix = at_unix;
        self.put(
            TREE_USERS,
            user_uuid.as_bytes(),
            &serde_json::to_vec(&stored)?,
        )?;
        Ok(())
    }
}

/// Raft log entry representing a database operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntry {
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        collection: String,
        key: Vec<u8>,
    }, // Reads usually don't go through Raft log, but kept for consistency if needed
    Delete {
        collection: String,
        key: Vec<u8>,
    },
    BatchPut {
        collection: String,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    // Domain writes (hybrid model). Index maintenance happens deterministically in `apply`.
    CreateTicket {
        body: Vec<u8>,
        index: TicketIndexFields,
    },
    UpdateTicket {
        ticket_id: u64,
        body: Vec<u8>,
        index: TicketIndexFields,
    },
    SoftDeleteTicket {
        ticket_id: u64,
        at_unix: i64,
    },
    CreateUser {
        user_uuid: String,
        body: Vec<u8>,
        index: UserIndexFields,
    },
    UpdateUser {
        user_uuid: String,
        body: Vec<u8>,
        index: UserIndexFields,
    },
    SoftDeleteUser {
        user_uuid: String,
        at_unix: i64,
    },
}

impl LogEntry {
    /// Apply this log entry to storage.
    ///
    /// Returns an optional payload for the client response — currently the
    /// big-endian assigned id for [`LogEntry::CreateTicket`]; `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the log entry cannot be applied.
    pub fn apply(&self, storage: &Storage) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Put {
                collection,
                key,
                value,
            } => {
                storage.put(collection, key, value)?;
                Ok(None)
            }
            Self::Get { .. } => Ok(None), // Reads don't modify state
            Self::Delete { collection, key } => {
                storage.delete(collection, key)?;
                Ok(None)
            }
            Self::BatchPut { collection, pairs } => {
                storage.batch_put(collection, pairs)?;
                Ok(None)
            }
            Self::CreateTicket { body, index } => {
                let id = storage.create_ticket(body, index)?;
                Ok(Some(id.to_be_bytes().to_vec()))
            }
            Self::UpdateTicket {
                ticket_id,
                body,
                index,
            } => {
                storage.update_ticket(*ticket_id, body, index)?;
                Ok(None)
            }
            Self::SoftDeleteTicket { ticket_id, at_unix } => {
                storage.soft_delete_ticket(*ticket_id, *at_unix)?;
                Ok(None)
            }
            Self::CreateUser {
                user_uuid,
                body,
                index,
            } => {
                storage.create_user(user_uuid, body, index)?;
                Ok(None)
            }
            Self::UpdateUser {
                user_uuid,
                body,
                index,
            } => {
                storage.update_user(user_uuid, body, index)?;
                Ok(None)
            }
            Self::SoftDeleteUser { user_uuid, at_unix } => {
                storage.soft_delete_user(user_uuid, *at_unix)?;
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_put_get() {
        let storage = Storage::new_temp().unwrap();
        let collection = "test_coll";

        storage.put(collection, b"test_key", b"test_value").unwrap();
        let value = storage.get(collection, b"test_key").unwrap();

        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_storage_delete() {
        let storage = Storage::new_temp().unwrap();
        let collection = "test_coll";

        storage.put(collection, b"key1", b"value1").unwrap();
        assert!(storage.exists(collection, b"key1").unwrap());

        storage.delete(collection, b"key1").unwrap();
        assert!(!storage.exists(collection, b"key1").unwrap());
    }

    #[test]
    fn test_storage_list() {
        let storage = Storage::new_temp().unwrap();
        let collection = "test_coll";

        storage.put(collection, b"user:1", b"alice").unwrap();
        storage.put(collection, b"user:2", b"bob").unwrap();
        storage.put(collection, b"post:1", b"hello").unwrap();

        let pairs = storage.list(collection, b"user:", None).unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|(k, _)| k == b"user:1"));
        assert!(pairs.iter().any(|(k, _)| k == b"user:2"));
    }

    #[test]
    fn test_storage_batch_put() {
        let storage = Storage::new_temp().unwrap();
        let collection = "test_coll";

        let pairs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        let count = storage.batch_put(collection, &pairs).unwrap();
        assert_eq!(count, 3);

        assert_eq!(storage.get(collection, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(storage.get(collection, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(storage.get(collection, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_log_entry_apply() {
        let storage = Storage::new_temp().unwrap();
        let collection = "test_coll";

        let entry = LogEntry::Put {
            collection: collection.to_string(),
            key: b"test".to_vec(),
            value: b"data".to_vec(),
        };

        assert!(entry.apply(&storage).unwrap().is_none());
        assert_eq!(
            storage.get(collection, b"test").unwrap(),
            Some(b"data".to_vec())
        );
    }

    #[test]
    fn log_entry_apply_covers_all_variants() {
        let storage = Storage::new_temp().unwrap();

        // Generic KV variants via the apply() dispatch.
        LogEntry::Put {
            collection: "c".to_string(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }
        .apply(&storage)
        .unwrap();
        assert!(
            LogEntry::Get {
                collection: "c".to_string(),
                key: b"k".to_vec(),
            }
            .apply(&storage)
            .unwrap()
            .is_none()
        );
        LogEntry::BatchPut {
            collection: "c".to_string(),
            pairs: vec![(b"a".to_vec(), b"1".to_vec())],
        }
        .apply(&storage)
        .unwrap();
        LogEntry::Delete {
            collection: "c".to_string(),
            key: b"k".to_vec(),
        }
        .apply(&storage)
        .unwrap();
        assert!(!storage.exists("c", b"k").unwrap());

        // Ticket domain variants: CreateTicket returns the assigned id.
        let created = LogEntry::CreateTicket {
            body: b"tbody".to_vec(),
            index: sample_index(1),
        }
        .apply(&storage)
        .unwrap()
        .expect("create returns id");
        let id = u64::from_be_bytes(created.try_into().expect("8-byte id"));
        LogEntry::UpdateTicket {
            ticket_id: id,
            body: b"tbody2".to_vec(),
            index: sample_index(2),
        }
        .apply(&storage)
        .unwrap();
        assert_eq!(
            storage.get_ticket(id, false).unwrap().unwrap().body,
            b"tbody2"
        );
        LogEntry::SoftDeleteTicket {
            ticket_id: id,
            at_unix: 5,
        }
        .apply(&storage)
        .unwrap();
        assert!(storage.get_ticket(id, false).unwrap().is_none());

        // User domain variants.
        let uidx = || UserIndexFields {
            username: "bob".to_string(),
            email: "b@e.com".to_string(),
            role: 2,
        };
        LogEntry::CreateUser {
            user_uuid: "u1".to_string(),
            body: b"ub".to_vec(),
            index: uidx(),
        }
        .apply(&storage)
        .unwrap();
        LogEntry::UpdateUser {
            user_uuid: "u1".to_string(),
            body: b"ub2".to_vec(),
            index: uidx(),
        }
        .apply(&storage)
        .unwrap();
        assert_eq!(storage.get_user("u1", false).unwrap().unwrap().body, b"ub2");
        LogEntry::SoftDeleteUser {
            user_uuid: "u1".to_string(),
            at_unix: 7,
        }
        .apply(&storage)
        .unwrap();
        assert!(storage.get_user("u1", false).unwrap().is_none());
    }

    #[test]
    fn storage_misc_paths_are_covered() {
        let storage = Storage::new_temp().unwrap();

        // list honours the limit
        for i in 0..5u8 {
            storage.put("c", &[b'k', i], b"v").unwrap();
        }
        assert_eq!(storage.list("c", b"k", Some(2)).unwrap().len(), 2);

        // get of a missing key
        assert!(storage.get("c", b"absent").unwrap().is_none());

        // next_id is monotonic per sequence name
        let a = storage.next_id(b"seq").unwrap();
        let b = storage.next_id(b"seq").unwrap();
        assert_eq!(b, a + 1);

        // data_tree_names skips the Raft bookkeeping trees
        assert!(!storage.data_tree_names().iter().any(|n| n.contains("raft")));

        // a soft-deleted ticket is hidden by default but visible with include_deleted
        let id = storage.create_ticket(b"body", &sample_index(1)).unwrap();
        storage.soft_delete_ticket(id, 1).unwrap();
        assert!(storage.get_ticket(id, false).unwrap().is_none());
        assert!(storage.get_ticket(id, true).unwrap().unwrap().deleted);
    }

    #[test]
    fn query_tickets_by_each_index_dimension() {
        let storage = Storage::new_temp().unwrap();
        let mk = |status, account: &str, assignee: &str, project: &str| TicketIndexFields {
            status,
            account_uuid: account.to_string(),
            assigned_to_uuid: Some(assignee.to_string()),
            project: project.to_string(),
            tracking_url: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        };
        storage
            .create_ticket(b"b1", &mk(1, "acctA", "agentX", "projP"))
            .unwrap();
        storage
            .create_ticket(b"b2", &mk(2, "acctB", "agentY", "projQ"))
            .unwrap();

        // Each branch selects candidates via a different secondary index.
        let by_assignee = storage
            .query_tickets(&TicketQuery {
                assigned_to_uuid: Some("agentX".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_assignee.len(), 1);

        let by_account = storage
            .query_tickets(&TicketQuery {
                account_uuid: Some("acctB".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_account.len(), 1);

        let by_project = storage
            .query_tickets(&TicketQuery {
                project: Some("projP".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_project.len(), 1);

        // No filter → all active tickets.
        assert_eq!(
            storage
                .query_tickets(&TicketQuery::default())
                .unwrap()
                .len(),
            2
        );

        // include_deleted scans all ids (including a soft-deleted one).
        let id = storage
            .create_ticket(b"b3", &mk(1, "acctC", "agentZ", "projR"))
            .unwrap();
        storage.soft_delete_ticket(id, 1).unwrap();
        let with_deleted = storage
            .query_tickets(&TicketQuery {
                include_deleted: true,
                ..Default::default()
            })
            .unwrap();
        assert!(with_deleted.iter().any(|(i, _)| *i == id));

        // scan_index_ids_str resolves the assignee index to ticket ids.
        assert!(
            !storage
                .scan_index_ids_str(IDX_TICKET_ASSIGNEE, b"agentX")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn update_user_moves_index_entries() {
        let storage = Storage::new_temp().unwrap();
        storage
            .create_user(
                "u1",
                b"body",
                &UserIndexFields {
                    username: "old".to_string(),
                    email: "old@e.com".to_string(),
                    role: 1,
                },
            )
            .unwrap();
        // Rename: the old username index entry must be removed and a new one written.
        storage
            .update_user(
                "u1",
                b"body2",
                &UserIndexFields {
                    username: "new".to_string(),
                    email: "new@e.com".to_string(),
                    role: 2,
                },
            )
            .unwrap();
        assert!(
            storage
                .scan_index_ids_str(IDX_USER_NAME, b"old")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage.scan_index_ids_str(IDX_USER_NAME, b"new").unwrap(),
            vec!["u1".to_string()]
        );
        // The deleted user is hidden by default but visible with include_deleted.
        storage.soft_delete_user("u1", 9).unwrap();
        assert!(storage.get_user("u1", false).unwrap().is_none());
        assert!(storage.get_user("u1", true).unwrap().unwrap().deleted);
    }

    fn sample_index(status: u8) -> TicketIndexFields {
        TicketIndexFields {
            status,
            account_uuid: "acct-1".to_string(),
            assigned_to_uuid: Some("agent-1".to_string()),
            project: "proj-x".to_string(),
            tracking_url: None,
            created_at_unix: 1_000,
            updated_at_unix: 1_000,
        }
    }

    #[test]
    fn create_ticket_assigns_monotonic_ids() {
        let storage = Storage::new_temp().unwrap();
        let id1 = storage.create_ticket(b"body1", &sample_index(1)).unwrap();
        let id2 = storage.create_ticket(b"body2", &sample_index(1)).unwrap();
        let id3 = storage.create_ticket(b"body3", &sample_index(1)).unwrap();
        assert_eq!((id1, id2, id3), (1, 2, 3));
    }

    #[test]
    fn create_ticket_roundtrips_body_and_populates_indexes() {
        let storage = Storage::new_temp().unwrap();
        let id = storage.create_ticket(b"secret", &sample_index(1)).unwrap();

        let stored = storage.get_ticket(id, false).unwrap().unwrap();
        assert_eq!(stored.body, b"secret");
        assert!(!stored.deleted);

        // Status index points at the ticket.
        let ids = storage.scan_index_ids(IDX_TICKET_STATUS, &[1]).unwrap();
        assert_eq!(ids, vec![id]);
        // Assignee index populated (it was Some).
        let by_assignee = storage
            .scan_index_ids(IDX_TICKET_ASSIGNEE, b"agent-1")
            .unwrap();
        assert_eq!(by_assignee, vec![id]);
    }

    #[test]
    fn update_ticket_moves_index_entries() {
        let storage = Storage::new_temp().unwrap();
        let id = storage.create_ticket(b"body", &sample_index(1)).unwrap();

        let mut updated = sample_index(2);
        updated.assigned_to_uuid = Some("agent-2".to_string());
        storage.update_ticket(id, b"body2", &updated).unwrap();

        // Old status/assignee entries gone, new ones present.
        assert!(
            storage
                .scan_index_ids(IDX_TICKET_STATUS, &[1])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage.scan_index_ids(IDX_TICKET_STATUS, &[2]).unwrap(),
            vec![id]
        );
        assert!(
            storage
                .scan_index_ids(IDX_TICKET_ASSIGNEE, b"agent-1")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .scan_index_ids(IDX_TICKET_ASSIGNEE, b"agent-2")
                .unwrap(),
            vec![id]
        );
        assert_eq!(
            storage.get_ticket(id, false).unwrap().unwrap().body,
            b"body2"
        );
    }

    #[test]
    fn soft_delete_removes_from_active_indexes_but_keeps_row() {
        let storage = Storage::new_temp().unwrap();
        let id = storage.create_ticket(b"body", &sample_index(1)).unwrap();

        storage.soft_delete_ticket(id, 5_000).unwrap();

        // Dropped from active indexes.
        assert!(
            storage
                .scan_index_ids(IDX_TICKET_STATUS, &[1])
                .unwrap()
                .is_empty()
        );
        // Hidden from normal reads, visible with include_deleted (audit row retained).
        assert!(storage.get_ticket(id, false).unwrap().is_none());
        let stored = storage.get_ticket(id, true).unwrap().unwrap();
        assert!(stored.deleted);
        assert_eq!(stored.deleted_at_unix, 5_000);
    }

    #[test]
    fn query_by_status_returns_only_matching_active() {
        let storage = Storage::new_temp().unwrap();
        let open = storage.create_ticket(b"a", &sample_index(1)).unwrap();
        let _closed = storage.create_ticket(b"b", &sample_index(9)).unwrap();
        let deleted = storage.create_ticket(b"c", &sample_index(1)).unwrap();
        storage.soft_delete_ticket(deleted, 1).unwrap();

        let q = TicketQuery {
            status: Some(1),
            ..Default::default()
        };
        let results = storage.query_tickets(&q).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, open);
    }

    #[test]
    fn query_applies_multiple_filters_and_limit() {
        let storage = Storage::new_temp().unwrap();
        // Two open tickets for agent-1, one for agent-2.
        let t1 = storage.create_ticket(b"a", &sample_index(1)).unwrap();
        let t2 = storage.create_ticket(b"b", &sample_index(1)).unwrap();
        let mut other = sample_index(1);
        other.assigned_to_uuid = Some("agent-2".to_string());
        let t3 = storage.create_ticket(b"c", &other).unwrap();

        // status=1 AND assignee=agent-1 matches t1 and t2 (not t3); limit caps at 1.
        let q = TicketQuery {
            status: Some(1),
            assigned_to_uuid: Some("agent-1".to_string()),
            limit: 1,
            ..Default::default()
        };
        let results = storage.query_tickets(&q).unwrap();
        assert_eq!(results.len(), 1);
        let matched = results[0].0;
        assert!(matched == t1 || matched == t2);
        assert_ne!(matched, t3);
    }

    #[test]
    fn user_crud_roundtrip_and_soft_delete() {
        let storage = Storage::new_temp().unwrap();
        let idx = UserIndexFields {
            username: "alice".to_string(),
            email: "alice@example.com".to_string(),
            role: 2,
        };
        storage.create_user("uuid-1", b"ubody", &idx).unwrap();

        let stored = storage.get_user("uuid-1", false).unwrap().unwrap();
        assert_eq!(stored.body, b"ubody");
        assert_eq!(
            storage.scan_index_ids_str(IDX_USER_NAME, b"alice").unwrap(),
            vec!["uuid-1".to_string()]
        );

        storage.soft_delete_user("uuid-1", 9).unwrap();
        assert!(storage.get_user("uuid-1", false).unwrap().is_none());
        assert!(storage.get_user("uuid-1", true).unwrap().unwrap().deleted);
        assert!(
            storage
                .scan_index_ids_str(IDX_USER_NAME, b"alice")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn inner_handle_get_ticket_missing_and_tracking_index() {
        let storage = Storage::new_temp().unwrap();

        // inner() exposes the underlying sled Db handle.
        assert!(storage.inner().open_tree(b"probe").is_ok());

        // get_ticket on a never-created id takes the `None` arm.
        assert!(storage.get_ticket(123_456, false).unwrap().is_none());

        // A ticket carrying a tracking_url populates the tracking secondary index.
        let mut idx = sample_index(1);
        idx.tracking_url = Some("http://portal/track/1".to_string());
        let id = storage.create_ticket(b"body", &idx).unwrap();
        assert!(
            !storage
                .scan_index_ids(IDX_TICKET_TRACKING, b"http://portal/track/1")
                .unwrap()
                .is_empty()
        );

        // A full-scan query (no filter) skips a soft-deleted candidate.
        storage.soft_delete_ticket(id, 1).unwrap();
        let active = storage.query_tickets(&TicketQuery::default()).unwrap();
        assert!(active.iter().all(|(i, _)| *i != id));
    }

    #[test]
    fn query_in_memory_filters_skip_nonmatching_candidates() {
        let storage = Storage::new_temp().unwrap();
        let mk = |account: &str, assignee: &str, project: &str| TicketIndexFields {
            status: 1,
            account_uuid: account.to_string(),
            assigned_to_uuid: Some(assignee.to_string()),
            project: project.to_string(),
            tracking_url: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        };
        // Both share status=1 so candidate selection uses the status index and the secondary
        // predicates run in memory — exercising each predicate's skip (`continue`) arm.
        storage
            .create_ticket(b"a", &mk("acctA", "agentX", "projP"))
            .unwrap();
        storage
            .create_ticket(b"b", &mk("acctB", "agentY", "projQ"))
            .unwrap();

        let by_assignee = storage
            .query_tickets(&TicketQuery {
                status: Some(1),
                assigned_to_uuid: Some("agentX".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_assignee.len(), 1);

        let by_account = storage
            .query_tickets(&TicketQuery {
                status: Some(1),
                account_uuid: Some("acctA".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_account.len(), 1);

        let by_project = storage
            .query_tickets(&TicketQuery {
                status: Some(1),
                project: Some("projP".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(by_project.len(), 1);
    }
}
