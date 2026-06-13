//! Trap endpoints and fake data generation for the honeypot service.
//!
//! This module provides deceptive responses that mimic real high-value targets
//! to attract and study attacker behavior. All data is synthetic.

#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

/// Generate a fake Bitcoin wallet address (honeytoken).
#[must_use]
pub fn generate_fake_wallet() -> String {
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string()
}

/// Generate fake backup archive names with believable, high-value-looking labels.
#[must_use]
pub fn generate_fake_backup_list() -> Vec<String> {
    vec![
        "production_db_2025-12-08.tar.gz".to_string(),
        "user_credentials_backup.zip".to_string(),
        "ssl_certificates_archive.tar.gz".to_string(),
        "admin_passwords.json.enc".to_string(),
    ]
}

/// Generate `size_mb` MB of junk data (tarpit filler).
#[must_use]
pub fn generate_junk_data(size_mb: usize) -> Vec<u8> {
    vec![0x42; size_mb * 1024 * 1024]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_wallet_is_plausible() {
        let wallet = generate_fake_wallet();
        assert!(wallet.starts_with("bc1"));
        assert!(wallet.len() > 20);
    }

    #[test]
    fn fake_backup_list_has_archive_names() {
        let backups = generate_fake_backup_list();
        assert!(!backups.is_empty());
        assert!(
            backups
                .iter()
                .any(|b| b.contains(".tar.gz") || b.contains(".zip"))
        );
    }

    #[test]
    fn junk_data_has_requested_size() {
        assert_eq!(generate_junk_data(1).len(), 1024 * 1024);
    }
}
