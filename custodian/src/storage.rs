//! Storage backend for custodian service
//!
//! This module provides persistent storage for ticket locks using Sled,
//! with Raft consensus for distributed coordination.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

// Tree Names
pub const TREE_LOCKS: &str = "locks";
pub const TREE_RAFT_METADATA: &str = "raft_metadata";
pub const TREE_RAFT_STATE: &str = "raft_state";
pub const TREE_RAFT_LOG: &str = "raft_log";

/// Lock information stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    pub ticket_id: u64,
    pub user_id: Uuid,
    pub acquired_at: chrono::DateTime<chrono::Utc>,
    /// Unix-seconds expiry. `0` means the lock never auto-expires.
    #[serde(default)]
    pub expires_at_unix: i64,
}

impl LockInfo {
    /// Whether this lock has auto-expired as of `now_unix`. A `0` expiry never expires.
    /// Time is supplied by the caller (not read from the clock) so the Raft state machine
    /// can evaluate expiry deterministically using the command's timestamp.
    #[must_use]
    pub fn is_expired(&self, now_unix: i64) -> bool {
        self.expires_at_unix != 0 && self.expires_at_unix <= now_unix
    }
}

/// Storage layer wrapping Sled
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

    /// Get the underlying Sled database
    #[must_use]
    pub fn inner(&self) -> &Db {
        &self.db
    }

    /// Open or get a handle to a specific tree
    ///
    /// # Errors
    ///
    /// Returns an error if the tree cannot be opened.
    pub fn get_tree(&self, name: &str) -> Result<Tree> {
        self.db.open_tree(name).context("failed to open tree")
    }

    /// Names of all non-Raft state-machine trees. Enumerating (rather than hard-coding)
    /// keeps snapshot coverage correct as new trees are added.
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

    /// Store a key-value pair
    ///
    /// # Errors
    ///
    /// Returns an error if the key-value pair cannot be stored.
    pub fn put(&self, tree: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let tree = self.get_tree(tree)?;
        tree.insert(key, value)?;
        Ok(())
    }

    /// Retrieve a value by key
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be retrieved.
    pub fn get(&self, tree: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree = self.get_tree(tree)?;
        Ok(tree.get(key)?.map(|v| v.to_vec()))
    }

    /// Delete a key
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be deleted.
    pub fn delete(&self, tree: &str, key: &[u8]) -> Result<()> {
        let tree = self.get_tree(tree)?;
        tree.remove(key)?;
        Ok(())
    }

    /// List all key-value pairs in a tree with optional prefix
    ///
    /// # Errors
    ///
    /// Returns an error if the tree cannot be accessed.
    pub fn list(
        &self,
        tree: &str,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tree = self.get_tree(tree)?;
        let mut results = Vec::new();

        let iter = if prefix.is_empty() {
            tree.iter()
        } else {
            tree.scan_prefix(prefix)
        };

        for item in iter {
            let (key, value) = item?;
            results.push((key.to_vec(), value.to_vec()));

            if let Some(limit) = limit
                && results.len() >= limit
            {
                break;
            }
        }

        Ok(results)
    }

    /// Check if a key exists
    ///
    /// # Errors
    ///
    /// Returns an error if the existence check fails.
    pub fn exists(&self, tree: &str, key: &[u8]) -> Result<bool> {
        let tree = self.get_tree(tree)?;
        Ok(tree.contains_key(key)?)
    }

    /// Acquire a non-expiring lock at the current wall-clock time. Convenience wrapper used
    /// by tests and non-Raft paths; the Raft state machine uses [`Storage::acquire_lock_at`].
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired.
    pub fn acquire_lock(&self, ticket_id: u64, user_id: Uuid) -> Result<()> {
        self.acquire_lock_at(ticket_id, user_id, chrono::Utc::now().timestamp(), 0)
    }

    /// Acquire a lock with an explicit acquisition time and TTL.
    ///
    /// `at_unix` is the acquisition time (unix seconds) and `ttl_secs` the lock lifetime
    /// (`0` = never expires). Both are supplied by the caller so the Raft state machine
    /// records identical expiry on every node.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired.
    pub fn acquire_lock_at(
        &self,
        ticket_id: u64,
        user_id: Uuid,
        at_unix: i64,
        ttl_secs: i64,
    ) -> Result<()> {
        let key = ticket_id.to_be_bytes();
        let expires_at_unix = if ttl_secs > 0 { at_unix + ttl_secs } else { 0 };
        let lock_info = LockInfo {
            ticket_id,
            user_id,
            acquired_at: chrono::DateTime::from_timestamp(at_unix, 0).unwrap_or_default(),
            expires_at_unix,
        };
        let value = serde_json::to_vec(&lock_info)?;
        self.put(TREE_LOCKS, &key, &value)
    }

    /// Release a lock on a ticket
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be released.
    pub fn release_lock(&self, ticket_id: u64) -> Result<()> {
        let key = ticket_id.to_be_bytes();
        self.delete(TREE_LOCKS, &key)
    }

    /// Check if a ticket is locked
    ///
    /// # Errors
    ///
    /// Returns an error if the lock status cannot be checked.
    pub fn is_locked(&self, ticket_id: u64) -> Result<bool> {
        let key = ticket_id.to_be_bytes();
        self.exists(TREE_LOCKS, &key)
    }

    /// Get lock information for a ticket
    ///
    /// # Errors
    ///
    /// Returns an error if the lock information cannot be retrieved.
    pub fn get_lock_info(&self, ticket_id: u64) -> Result<Option<LockInfo>> {
        let key = ticket_id.to_be_bytes();
        match self.get(TREE_LOCKS, &key)? {
            Some(value) => {
                let lock_info = serde_json::from_slice(&value)?;
                Ok(Some(lock_info))
            }
            None => Ok(None),
        }
    }

    /// Get all current locks
    ///
    /// # Errors
    ///
    /// Returns an error if the locks cannot be retrieved.
    pub fn get_all_locks(&self) -> Result<HashMap<u64, LockInfo>> {
        let tree = self.get_tree(TREE_LOCKS)?;
        let mut locks = HashMap::new();

        for item in &tree {
            let (key, value) = item?;
            let ticket_id = u64::from_be_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid key length"))?,
            );
            let lock_info: LockInfo = serde_json::from_slice(&value)?;
            locks.insert(ticket_id, lock_info);
        }

        Ok(locks)
    }
}

/// Raft log entry representing a lock operation. `at_unix`/`ttl_secs` are stamped by the
/// leader before submission so lock acquisition and expiry are deterministic across nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LockCommand {
    AcquireLock {
        ticket_id: u64,
        user_id: Uuid,
        #[serde(default)]
        at_unix: i64,
        #[serde(default)]
        ttl_secs: i64,
    },
    ReleaseLock {
        ticket_id: u64,
        user_id: Uuid,
    },
}

impl LockCommand {
    /// Apply this command to storage (unconditionally — exclusivity/expiry checks live in the
    /// state machine; this is the raw mutation used by lower-level paths and tests).
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be applied.
    pub fn apply(&self, storage: &Storage) -> Result<()> {
        match self {
            Self::AcquireLock {
                ticket_id,
                user_id,
                at_unix,
                ttl_secs,
            } => storage.acquire_lock_at(*ticket_id, *user_id, *at_unix, *ttl_secs),
            Self::ReleaseLock { ticket_id, .. } => storage.release_lock(*ticket_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_operations() {
        let storage = Storage::new_temp().unwrap();
        let ticket_id = 42;
        let user_id = Uuid::new_v4();

        // Initially not locked
        assert!(!storage.is_locked(ticket_id).unwrap());

        // Acquire lock
        storage.acquire_lock(ticket_id, user_id).unwrap();
        assert!(storage.is_locked(ticket_id).unwrap());

        // Get lock info
        let lock_info = storage.get_lock_info(ticket_id).unwrap().unwrap();
        assert_eq!(lock_info.ticket_id, ticket_id);
        assert_eq!(lock_info.user_id, user_id);

        // Release lock
        storage.release_lock(ticket_id).unwrap();
        assert!(!storage.is_locked(ticket_id).unwrap());
    }

    #[test]
    fn lock_expiry_is_recorded_and_evaluated() {
        let storage = Storage::new_temp().unwrap();
        let user = Uuid::new_v4();

        // TTL of 100s acquired at t=1000 -> expires at 1100.
        storage.acquire_lock_at(7, user, 1_000, 100).unwrap();
        let lock = storage.get_lock_info(7).unwrap().unwrap();
        assert_eq!(lock.expires_at_unix, 1_100);
        assert!(!lock.is_expired(1_099));
        assert!(lock.is_expired(1_100));
        assert!(lock.is_expired(2_000));

        // TTL of 0 never expires.
        storage.acquire_lock_at(8, user, 1_000, 0).unwrap();
        let never = storage.get_lock_info(8).unwrap().unwrap();
        assert_eq!(never.expires_at_unix, 0);
        assert!(!never.is_expired(i64::MAX));
    }

    #[test]
    fn test_get_all_locks() {
        let storage = Storage::new_temp().unwrap();
        let user_id = Uuid::new_v4();

        // Add multiple locks
        storage.acquire_lock(1, user_id).unwrap();
        storage.acquire_lock(2, user_id).unwrap();

        let locks = storage.get_all_locks().unwrap();
        assert_eq!(locks.len(), 2);
        assert!(locks.contains_key(&1));
        assert!(locks.contains_key(&2));
    }

    #[test]
    fn test_storage_inner_and_list() {
        let storage = Storage::new_temp().unwrap();

        // inner() should return the underlying sled Db handle
        let _db: &sled::Db = storage.inner();

        // Populate a couple of entries via the raw put API
        storage.put("test_tree", b"key1", b"val1").unwrap();
        storage.put("test_tree", b"key2", b"val2").unwrap();
        storage.put("test_tree", b"other", b"valx").unwrap();

        // list() with empty prefix returns all entries
        let all = storage.list("test_tree", b"", None).unwrap();
        assert_eq!(all.len(), 3);

        // list() with non-empty prefix filters by prefix
        let filtered = storage.list("test_tree", b"key", None).unwrap();
        assert_eq!(filtered.len(), 2);

        // list() with limit stops early
        let limited = storage.list("test_tree", b"", Some(2)).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_get_lock_info_returns_none_for_unknown_ticket() {
        let storage = Storage::new_temp().unwrap();
        let result = storage.get_lock_info(9999).unwrap();
        assert!(result.is_none());
    }
}
