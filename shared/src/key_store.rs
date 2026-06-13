//! Shared keypair persistence for `InfoVulcan` services.
//!
//! Provides a single canonical function for loading or generating a Kyber-768
//! keypair so that any two services sharing the same `STORAGE_PATH` will
//! automatically share the same keypair.

use crate::encryption::EncryptionService;
use anyhow::Context;
use std::fs;
use std::path::Path;
use tracing::info;

/// The deterministic filename used by all services for the Kyber keypair.
pub const KEYPAIR_FILENAME: &str = "keys.bin";

/// Loads an existing Kyber-768 keypair from `storage_path`, or generates a new
/// one and persists it.
///
/// The keypair is stored as a JSON-serialized `(Vec<u8>, Vec<u8>)` tuple at
/// `<storage_path>/keys.bin`.
///
/// # Errors
/// Returns an error if:
/// - Key generation fails
/// - Serialization / deserialization fails
/// - File I/O fails
pub fn load_or_generate_keypair(storage_path: &Path) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let keys_path = storage_path.join(KEYPAIR_FILENAME);

    if keys_path.exists() {
        info!("Loading encryption keys from {:?}", keys_path);
        let bytes = fs::read(&keys_path)
            .with_context(|| format!("failed to read keypair from {}", keys_path.display()))?;
        let keys: (Vec<u8>, Vec<u8>) = serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to deserialize keypair from {}", keys_path.display())
        })?;
        return Ok(keys);
    }

    info!("Generating new encryption keys");
    let keys = EncryptionService::generate_keypair()
        .map_err(|e| anyhow::anyhow!("Failed to generate keypair: {e}"))?;
    let bytes = serde_json::to_vec(&keys).context("failed to serialize keypair")?;
    fs::write(&keys_path, bytes)
        .with_context(|| format!("failed to write keypair to {}", keys_path.display()))?;
    info!("Saved encryption keys to {:?}", keys_path);
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_or_generate_keypair_when_no_file_exists_returns_valid_keypair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys = load_or_generate_keypair(dir.path()).expect("should generate keypair");

        assert!(!keys.0.is_empty(), "public key must not be empty");
        assert!(!keys.1.is_empty(), "secret key must not be empty");
    }

    #[test]
    fn test_load_or_generate_keypair_when_called_twice_returns_same_keypair() {
        let dir = tempfile::tempdir().expect("tempdir");

        let first = load_or_generate_keypair(dir.path()).expect("first call");
        let second = load_or_generate_keypair(dir.path()).expect("second call");

        assert_eq!(
            first, second,
            "calling twice on the same path must return the same keypair"
        );
    }

    #[test]
    fn test_load_or_generate_keypair_creates_file_at_expected_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let expected_file = dir.path().join(KEYPAIR_FILENAME);

        assert!(
            !expected_file.exists(),
            "file should not exist before first call"
        );

        let _keys = load_or_generate_keypair(dir.path()).expect("should generate keypair");

        assert!(
            expected_file.exists(),
            "file should be created at <storage_path>/keys.bin"
        );
    }

    #[test]
    fn test_load_or_generate_keypair_errors_on_corrupt_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A keys.bin that exists but isn't valid JSON → deserialization error path.
        fs::write(dir.path().join(KEYPAIR_FILENAME), b"not valid json").expect("write");
        let result = load_or_generate_keypair(dir.path());
        assert!(result.is_err(), "corrupt keypair file must surface an error");
    }

    #[test]
    fn test_load_or_generate_keypair_when_different_paths_returns_different_keypairs() {
        let dir1 = tempfile::tempdir().expect("tempdir1");
        let dir2 = tempfile::tempdir().expect("tempdir2");

        let keys1 = load_or_generate_keypair(dir1.path()).expect("keypair 1");
        let keys2 = load_or_generate_keypair(dir2.path()).expect("keypair 2");

        // Kyber keypairs are random, so they should differ with overwhelming probability
        assert_ne!(
            keys1, keys2,
            "independent paths should produce different keypairs"
        );
    }
}
