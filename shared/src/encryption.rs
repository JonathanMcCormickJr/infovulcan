//! Encryption utilities for `InfoVulcan`.
//!
//! This module provides encryption capabilities for sensitive data.
//! Currently implements symmetric encryption with AES-GCM and ChaCha20-Poly1305.
//! Post-quantum key exchange will be added when stable libraries become available.
//!
//! # Usage
//!
//! ```rust
//! use shared::encryption::{EncryptionService, EncryptionAlgorithm};
//!
//! // Encrypt data with a password
//! let encrypted = EncryptionService::encrypt_with_password(b"sensitive data", "my-password")
//!     .unwrap();
//!
//! // Decrypt the data
//! let decrypted = EncryptionService::decrypt_with_password(&encrypted, "my-password")
//!     .unwrap();
//! assert_eq!(decrypted, b"sensitive data");
//! ```

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use chacha20poly1305::ChaCha20Poly1305;
use rand::TryRng;
use safe_pqc_kyber::{
    KYBER_PUBLICKEYBYTES, KYBER_SECRETKEYBYTES, decapsulate, encapsulate, keypair,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self};

/// Encryption algorithm options
#[derive(Debug, Clone, Copy, Default)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (hardware accelerated on many platforms)
    Aes256Gcm,
    /// ChaCha20-Poly1305 (constant-time, no hardware acceleration needed)
    #[default]
    ChaCha20Poly1305,
}

impl Serialize for EncryptionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(match self {
            EncryptionAlgorithm::Aes256Gcm => 0,
            EncryptionAlgorithm::ChaCha20Poly1305 => 1,
        })
    }
}

impl<'de> Deserialize<'de> for EncryptionAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u32::deserialize(deserializer)?;
        match v {
            0 => Ok(EncryptionAlgorithm::Aes256Gcm),
            1 => Ok(EncryptionAlgorithm::ChaCha20Poly1305),
            _ => Err(serde::de::Error::custom(format!("Invalid algorithm: {v}"))),
        }
    }
}

/// Encrypted data structure
#[derive(Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    /// Encryption algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Salt for key derivation (used for password-based encryption)
    pub salt: Option<Vec<u8>>,
    /// KEM ciphertext (encapsulated key) if using post-quantum encryption
    pub kem_ciphertext: Option<Vec<u8>>,
    /// Nonce/IV for encryption
    pub nonce: Vec<u8>,
    /// Encrypted payload
    pub ciphertext: Vec<u8>,
}

impl fmt::Debug for EncryptedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("EncryptedData");
        d.field("algorithm", &self.algorithm);
        if let Some(salt) = &self.salt {
            d.field("salt", &format!("[{} bytes]", salt.len()));
        }
        if let Some(kem) = &self.kem_ciphertext {
            d.field("kem_ciphertext", &format!("[{} bytes]", kem.len()));
        }
        d.field("nonce", &format!("[{} bytes]", self.nonce.len()))
            .field("ciphertext", &format!("[{} bytes]", self.ciphertext.len()))
            .finish()
    }
}

/// Main encryption service
pub struct EncryptionService;

impl EncryptionService {
    /// Encrypt data using a password
    ///
    /// # Errors
    /// Returns `EncryptionError` if encryption fails.
    pub fn encrypt_with_password(
        data: &[u8],
        password: &str,
    ) -> Result<EncryptedData, EncryptionError> {
        Self::encrypt_with_password_and_algorithm(data, password, EncryptionAlgorithm::default())
    }

    /// Encrypt data using a password and specific algorithm
    ///
    /// # Errors
    /// Returns `EncryptionError` if encryption fails.
    pub fn encrypt_with_password_and_algorithm(
        data: &[u8],
        password: &str,
        algorithm: EncryptionAlgorithm,
    ) -> Result<EncryptedData, EncryptionError> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        rand::rng().try_fill_bytes(&mut salt).map_err(|e| {
            EncryptionError::RandomNumberGeneration(format!("Error generating random number: {e}"))
        })?;

        // Derive key from password and salt
        let key = Self::derive_key_from_password(password, &salt)?;

        // Encrypt the data
        let (nonce, ciphertext) = Self::encrypt_symmetric(&key, data, algorithm)?;

        Ok(EncryptedData {
            algorithm,
            salt: Some(salt.to_vec()),
            kem_ciphertext: None,
            nonce,
            ciphertext,
        })
    }

    /// Decrypt data using a password
    ///
    /// # Errors
    /// Returns `EncryptionError` if decryption fails or password is incorrect.
    pub fn decrypt_with_password(
        encrypted_data: &EncryptedData,
        password: &str,
    ) -> Result<Vec<u8>, EncryptionError> {
        // Check if salt is present
        let salt = encrypted_data.salt.as_ref().ok_or_else(|| {
            EncryptionError::InvalidInput("Missing salt for password decryption".to_string())
        })?;

        // Derive key from password and salt
        let key = Self::derive_key_from_password(password, salt)?;

        // Decrypt the data
        Self::decrypt_symmetric(&key, encrypted_data, encrypted_data.algorithm)
    }

    /// Generate a new Kyber-768 keypair for post-quantum encryption
    ///
    /// # Errors
    /// Returns `EncryptionError` if key generation fails.
    pub fn generate_keypair() -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        let keys = keypair(&mut OsRng);

        Ok((keys.public.to_vec(), keys.secret.to_vec()))
    }

    /// Encrypt data using a public key (Post-Quantum KEM)
    ///
    /// # Errors
    /// Returns `EncryptionError` if encryption fails or public key is invalid.
    pub fn encrypt_with_public_key(
        data: &[u8],
        public_key: &[u8],
    ) -> Result<EncryptedData, EncryptionError> {
        Self::encrypt_with_public_key_and_algorithm(
            data,
            public_key,
            EncryptionAlgorithm::default(),
        )
    }

    /// Encrypt data using a public key and specific algorithm
    ///
    /// # Errors
    /// Returns `EncryptionError` if encryption fails or public key is invalid.
    pub fn encrypt_with_public_key_and_algorithm(
        data: &[u8],
        public_key: &[u8],
        algorithm: EncryptionAlgorithm,
    ) -> Result<EncryptedData, EncryptionError> {
        // Validate public key length
        if public_key.len() != KYBER_PUBLICKEYBYTES {
            return Err(EncryptionError::InvalidInput(format!(
                "Invalid public key length: expected {}, got {}",
                KYBER_PUBLICKEYBYTES,
                public_key.len()
            )));
        }

        // Encapsulate key
        let (ciphertext, shared_secret) = encapsulate(public_key, &mut OsRng).map_err(|e| {
            EncryptionError::KeyEncapsulation(format!("Kyber encapsulation failed: {e:?}"))
        })?;

        // Use shared secret as symmetric key
        // Shared secret is 32 bytes, perfect for AES-256 or ChaCha20
        let key: [u8; 32] = shared_secret;

        // Encrypt the data
        let (nonce, payload) = Self::encrypt_symmetric(&key, data, algorithm)?;

        Ok(EncryptedData {
            algorithm,
            salt: None,
            kem_ciphertext: Some(ciphertext.to_vec()),
            nonce,
            ciphertext: payload,
        })
    }

    /// Decrypt data using a private key
    ///
    /// # Errors
    /// Returns `EncryptionError` if decryption fails or private key is invalid.
    pub fn decrypt_with_private_key(
        encrypted_data: &EncryptedData,
        private_key: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        // Check if KEM ciphertext is present
        let kem_ciphertext = encrypted_data.kem_ciphertext.as_ref().ok_or_else(|| {
            EncryptionError::InvalidInput(
                "Missing KEM ciphertext for private key decryption".to_string(),
            )
        })?;

        // Validate private key length
        if private_key.len() != KYBER_SECRETKEYBYTES {
            return Err(EncryptionError::InvalidInput(format!(
                "Invalid private key length: expected {}, got {}",
                KYBER_SECRETKEYBYTES,
                private_key.len()
            )));
        }

        // Decapsulate key
        let shared_secret = decapsulate(kem_ciphertext, private_key).map_err(|e| {
            EncryptionError::KeyDecapsulation(format!("Kyber decapsulation failed: {e:?}"))
        })?;

        // Use shared secret as symmetric key
        let key: [u8; 32] = shared_secret;

        // Decrypt the data
        Self::decrypt_symmetric(&key, encrypted_data, encrypted_data.algorithm)
    }

    /// Derive a 32-byte key from password and salt using Argon2id.
    ///
    /// In release builds, parameters follow OWASP minimums: m=19456 KiB, t=2
    /// iterations, p=1 lane. In debug/test builds, lighter parameters (m=256
    /// KiB, t=1, p=1) are used to keep the test suite fast.
    ///
    /// # Errors
    /// Returns `EncryptionError::KeyDerivation` if Argon2 hashing fails.
    fn derive_key_from_password(password: &str, salt: &[u8]) -> Result<[u8; 32], EncryptionError> {
        use argon2::{Algorithm, Argon2, Params, Version};

        // In debug/test builds, use fast params to keep test suite snappy.
        // In release builds, use OWASP minimums: m=19456 KiB, t=2, p=1.
        #[cfg(debug_assertions)]
        let params = Params::new(256, 1, 1, Some(32))
            .map_err(|e| EncryptionError::KeyDerivation(format!("Argon2 parameter error: {e}")))?;
        #[cfg(not(debug_assertions))]
        let params = Params::new(19_456, 2, 1, Some(32))
            .map_err(|e| EncryptionError::KeyDerivation(format!("Argon2 parameter error: {e}")))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        let mut key = [0u8; 32];
        argon2
            .hash_password_into(password.as_bytes(), salt, &mut key)
            .map_err(|e| {
                EncryptionError::KeyDerivation(format!("Argon2id key derivation failed: {e}"))
            })?;

        Ok(key)
    }

    /// Perform symmetric encryption
    fn encrypt_symmetric(
        key: &[u8; 32],
        data: &[u8],
        algorithm: EncryptionAlgorithm,
    ) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        match algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = Aes256Gcm::new(key.into());
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                let nonce_bytes = nonce.to_vec();

                let ciphertext = cipher.encrypt(&nonce, data).map_err(|e| {
                    EncryptionError::SymmetricEncryption(format!("AES-GCM encryption failed: {e}"))
                })?;

                Ok((nonce_bytes, ciphertext))
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305::new(key.into());
                let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
                let nonce_bytes = nonce.to_vec();

                let ciphertext = cipher.encrypt(&nonce, data).map_err(|e| {
                    EncryptionError::SymmetricEncryption(format!(
                        "ChaCha20-Poly1305 encryption failed: {e}"
                    ))
                })?;

                Ok((nonce_bytes, ciphertext))
            }
        }
    }

    /// Perform symmetric decryption
    fn decrypt_symmetric(
        key: &[u8; 32],
        encrypted_data: &EncryptedData,
        algorithm: EncryptionAlgorithm,
    ) -> Result<Vec<u8>, EncryptionError> {
        match algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = Aes256Gcm::new(key.into());
                let nonce = Nonce::from_slice(&encrypted_data.nonce);

                cipher
                    .decrypt(nonce, encrypted_data.ciphertext.as_ref())
                    .map_err(|e| {
                        EncryptionError::SymmetricDecryption(format!(
                            "AES-GCM decryption failed: {e}"
                        ))
                    })
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305::new(key.into());
                let nonce = chacha20poly1305::Nonce::from_slice(&encrypted_data.nonce);

                cipher
                    .decrypt(nonce, encrypted_data.ciphertext.as_ref())
                    .map_err(|e| {
                        EncryptionError::SymmetricDecryption(format!(
                            "ChaCha20-Poly1305 decryption failed: {e}"
                        ))
                    })
            }
        }
    }
}

/// Encryption-related errors
#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("Random number generation failed: {0}")]
    RandomNumberGeneration(String),

    #[error("Symmetric encryption failed: {0}")]
    SymmetricEncryption(String),

    #[error("Symmetric decryption failed: {0}")]
    SymmetricDecryption(String),

    #[error("Key generation failed: {0}")]
    KeyGeneration(String),

    #[error("Key derivation failed: {0}")]
    KeyDerivation(String),

    #[error("Key encapsulation failed: {0}")]
    KeyEncapsulation(String),

    #[error("Key decapsulation failed: {0}")]
    KeyDecapsulation(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypt_with_password_requires_salt() {
        let data = EncryptedData {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            salt: None,
            kem_ciphertext: None,
            nonce: vec![0u8; 12],
            ciphertext: vec![1, 2, 3],
        };
        assert!(EncryptionService::decrypt_with_password(&data, "pw").is_err());
    }

    #[test]
    fn encrypt_with_public_key_rejects_malformed_key() {
        assert!(EncryptionService::encrypt_with_public_key(b"x", &[0u8; 4]).is_err());
    }

    #[test]
    fn decrypt_with_private_key_rejects_bad_inputs_but_round_trips() {
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();
        let enc = EncryptionService::encrypt_with_public_key(b"secret", &pk).unwrap();

        // Wrong-size private key → error.
        assert!(EncryptionService::decrypt_with_private_key(&enc, &[0u8; 4]).is_err());

        // Missing KEM ciphertext → error.
        let mut no_kem = enc.clone();
        no_kem.kem_ciphertext = None;
        assert!(EncryptionService::decrypt_with_private_key(&no_kem, &sk).is_err());

        // A different keypair's private key → decapsulation/decryption fails.
        let (_pk2, sk2) = EncryptionService::generate_keypair().unwrap();
        assert!(EncryptionService::decrypt_with_private_key(&enc, &sk2).is_err());

        // The genuine key still round-trips.
        assert_eq!(
            EncryptionService::decrypt_with_private_key(&enc, &sk).unwrap(),
            b"secret"
        );
    }

    #[test]
    fn test_password_encryption_round_trip() {
        let original_data = b"Hello, encrypted world!";
        let password = "my-secret-password";

        // Encrypt
        let encrypted = EncryptionService::encrypt_with_password(original_data, password).unwrap();

        // Decrypt
        let decrypted = EncryptionService::decrypt_with_password(&encrypted, password).unwrap();

        assert_eq!(original_data, decrypted.as_slice());
    }

    #[test]
    fn test_different_algorithms() {
        let data = b"Test data for different algorithms";
        let password = "test-password";

        // Test AES-GCM
        let encrypted_aes = EncryptionService::encrypt_with_password_and_algorithm(
            data,
            password,
            EncryptionAlgorithm::Aes256Gcm,
        )
        .unwrap();
        let decrypted_aes =
            EncryptionService::decrypt_with_password(&encrypted_aes, password).unwrap();
        assert_eq!(data, decrypted_aes.as_slice());

        // Test ChaCha20-Poly1305
        let encrypted_chacha = EncryptionService::encrypt_with_password_and_algorithm(
            data,
            password,
            EncryptionAlgorithm::ChaCha20Poly1305,
        )
        .unwrap();
        let decrypted_chacha =
            EncryptionService::decrypt_with_password(&encrypted_chacha, password).unwrap();
        assert_eq!(data, decrypted_chacha.as_slice());
    }

    #[test]
    fn test_wrong_password_fails() {
        let data = b"Secret message";
        let correct_password = "correct-password";
        let wrong_password = "wrong-password";

        // Encrypt with correct password
        let encrypted = EncryptionService::encrypt_with_password(data, correct_password).unwrap();

        // Try to decrypt with wrong password (should fail)
        let result = EncryptionService::decrypt_with_password(&encrypted, wrong_password);
        assert!(result.is_err());
    }

    #[test]
    fn test_post_quantum_encryption_round_trip() {
        let original_data = b"Sensitive data protected by Kyber-768";

        // Generate keys
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();

        // Encrypt with public key
        let encrypted = EncryptionService::encrypt_with_public_key(original_data, &pk).unwrap();

        // Verify structure
        assert!(encrypted.kem_ciphertext.is_some());
        assert!(encrypted.salt.is_none());

        // Decrypt with private key
        let decrypted = EncryptionService::decrypt_with_private_key(&encrypted, &sk).unwrap();

        assert_eq!(original_data, decrypted.as_slice());
    }

    #[test]
    fn test_pq_encrypt_with_aes_algorithm() {
        let data = b"Test data for AES-based PQ encryption";
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();

        let encrypted = EncryptionService::encrypt_with_public_key_and_algorithm(
            data,
            &pk,
            EncryptionAlgorithm::Aes256Gcm,
        )
        .unwrap();
        assert!(encrypted.kem_ciphertext.is_some());

        let decrypted = EncryptionService::decrypt_with_private_key(&encrypted, &sk).unwrap();
        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_decrypt_with_private_key_fails_missing_kem_ciphertext() {
        // Password-encrypted data has no kem_ciphertext field.
        let encrypted = EncryptionService::encrypt_with_password(b"data", "pass").unwrap();
        let (_pk, sk) = EncryptionService::generate_keypair().unwrap();

        let result = EncryptionService::decrypt_with_private_key(&encrypted, &sk);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_private_key_fails_invalid_key_length() {
        let (pk, _sk) = EncryptionService::generate_keypair().unwrap();
        let encrypted = EncryptionService::encrypt_with_public_key(b"data", &pk).unwrap();

        // Supply a key that is too short (wrong length).
        let short_sk = vec![0u8; 32];
        let result = EncryptionService::decrypt_with_private_key(&encrypted, &short_sk);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_algorithm_serde_invalid_variant() {
        // Deserializing an unknown variant number must return an error.
        let result: Result<EncryptionAlgorithm, _> = serde_json::from_str("99");
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypted_data_debug_format() {
        let encrypted = EncryptionService::encrypt_with_password(b"hello", "pass").unwrap();
        let debug_str = format!("{encrypted:?}");
        assert!(debug_str.contains("EncryptedData"));
        assert!(debug_str.contains("nonce"));
        assert!(debug_str.contains("ciphertext"));
        // Salt is present for password-based encryption.
        assert!(debug_str.contains("salt"));
    }

    #[test]
    fn test_encryption_algorithm_serialize_both_variants() {
        let aes = serde_json::to_string(&EncryptionAlgorithm::Aes256Gcm).unwrap();
        assert_eq!(aes, "0");

        let chacha = serde_json::to_string(&EncryptionAlgorithm::ChaCha20Poly1305).unwrap();
        assert_eq!(chacha, "1");
    }

    #[test]
    fn test_encryption_algorithm_deserialize_both_variants() {
        let aes: EncryptionAlgorithm = serde_json::from_str("0").unwrap();
        assert!(matches!(aes, EncryptionAlgorithm::Aes256Gcm));

        let chacha: EncryptionAlgorithm = serde_json::from_str("1").unwrap();
        assert!(matches!(chacha, EncryptionAlgorithm::ChaCha20Poly1305));
    }

    #[test]
    fn test_pq_encrypt_with_chacha20_algorithm() {
        let data = b"Test data for ChaCha20-based PQ encryption";
        let (pk, sk) = EncryptionService::generate_keypair().unwrap();

        let encrypted = EncryptionService::encrypt_with_public_key_and_algorithm(
            data,
            &pk,
            EncryptionAlgorithm::ChaCha20Poly1305,
        )
        .unwrap();
        assert!(encrypted.kem_ciphertext.is_some());

        let decrypted = EncryptionService::decrypt_with_private_key(&encrypted, &sk).unwrap();
        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_password_encrypt_chacha20_debug_contains_kem_section() {
        // Encrypt with PQ key to get a kem_ciphertext in the EncryptedData
        let (pk, _sk) = EncryptionService::generate_keypair().unwrap();
        let encrypted = EncryptionService::encrypt_with_public_key(b"data", &pk).unwrap();
        // kem_ciphertext should be present for PQ encryption
        let debug_str = format!("{encrypted:?}");
        assert!(debug_str.contains("kem_ciphertext"));
    }

    #[test]
    fn test_encrypt_with_public_key_fails_with_wrong_key_length() {
        let too_short = vec![0u8; 32]; // Kyber public key is 1184 bytes
        let result = EncryptionService::encrypt_with_public_key(b"data", &too_short);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_password_fails_when_salt_is_missing() {
        // Construct an EncryptedData without a salt (simulating PQ-encrypted data).
        let encrypted_without_salt = EncryptedData {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            salt: None,
            kem_ciphertext: Some(vec![0u8; 8]),
            nonce: vec![0u8; 12],
            ciphertext: vec![0u8; 16],
        };
        let result = EncryptionService::decrypt_with_password(&encrypted_without_salt, "pass");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Missing salt"));
    }

    #[test]
    fn test_derive_key_deterministic_for_same_password_and_salt() {
        let password = "deterministic-test-password";
        let salt = [0xABu8; 32];

        let key1 = EncryptionService::derive_key_from_password(password, &salt).unwrap();
        let key2 = EncryptionService::derive_key_from_password(password, &salt).unwrap();

        assert_eq!(
            key1, key2,
            "Same password and salt must produce identical keys"
        );
    }

    #[test]
    fn test_derive_key_different_passwords_produce_different_keys() {
        let salt = [0xCDu8; 32];

        let key1 = EncryptionService::derive_key_from_password("password-one", &salt).unwrap();
        let key2 = EncryptionService::derive_key_from_password("password-two", &salt).unwrap();

        assert_ne!(
            key1, key2,
            "Different passwords must produce different keys"
        );
    }

    #[test]
    fn test_derive_key_different_salts_produce_different_keys() {
        let password = "same-password";
        let salt1 = [0x01u8; 32];
        let salt2 = [0x02u8; 32];

        let key1 = EncryptionService::derive_key_from_password(password, &salt1).unwrap();
        let key2 = EncryptionService::derive_key_from_password(password, &salt2).unwrap();

        assert_ne!(key1, key2, "Different salts must produce different keys");
    }

    #[test]
    fn test_derive_key_returns_32_bytes() {
        let key = EncryptionService::derive_key_from_password("test", &[0u8; 32]).unwrap();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_password_round_trip_with_cfg_conditional_argon2_params() {
        // This test verifies that encrypt/decrypt round-trip works correctly
        // regardless of which Argon2 params are active (debug vs release).
        let data = b"cfg-conditional argon2 params test";
        let password = "test-password-for-cfg";

        let encrypted = EncryptionService::encrypt_with_password(data, password).unwrap();
        let decrypted = EncryptionService::decrypt_with_password(&encrypted, password).unwrap();
        assert_eq!(data.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn decrypt_with_private_key_fails_on_malformed_kem_ciphertext() {
        // A correctly-sized private key but a KEM ciphertext of the wrong length makes the
        // Kyber decapsulation step itself fail (the error closure on the decapsulate path).
        let (_pk, sk) = EncryptionService::generate_keypair().unwrap();
        let bad = EncryptedData {
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            salt: None,
            kem_ciphertext: Some(vec![0u8; 8]), // not KYBER_CIPHERTEXTBYTES
            nonce: vec![0u8; 12],
            ciphertext: vec![0u8; 16],
        };
        assert!(EncryptionService::decrypt_with_private_key(&bad, &sk).is_err());
    }

    #[test]
    fn decrypt_with_password_fails_on_too_short_salt() {
        // Argon2 rejects an undersized salt, exercising the key-derivation error path.
        let bad = EncryptedData {
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            salt: Some(vec![0u8; 2]), // Argon2 requires >= 8 bytes
            kem_ciphertext: None,
            nonce: vec![0u8; 12],
            ciphertext: vec![0u8; 16],
        };
        assert!(EncryptionService::decrypt_with_password(&bad, "pw").is_err());
    }

    #[test]
    fn aes_gcm_decrypt_fails_on_tampered_ciphertext() {
        // Encrypt with AES-256-GCM, then corrupt the ciphertext so the authentication tag
        // no longer verifies — covers the AES-GCM symmetric-decryption error closure.
        let mut enc = EncryptionService::encrypt_with_password_and_algorithm(
            b"data",
            "pw",
            EncryptionAlgorithm::Aes256Gcm,
        )
        .unwrap();
        enc.ciphertext[0] ^= 0xFF;
        assert!(EncryptionService::decrypt_with_password(&enc, "pw").is_err());
    }

    #[test]
    fn test_derive_key_not_trivially_repeated() {
        // The old broken implementation repeated the same 8 bytes 4 times.
        // Verify that the derived key does NOT have this pattern.
        let key =
            EncryptionService::derive_key_from_password("test-password", &[0x42u8; 32]).unwrap();
        // Check that at least one of the 8-byte blocks differs from the first
        let first_block = &key[..8];
        let all_same = key[8..16] == *first_block
            && key[16..24] == *first_block
            && key[24..32] == *first_block;
        assert!(
            !all_same,
            "Key must not consist of the same 8 bytes repeated 4 times"
        );
    }
}
