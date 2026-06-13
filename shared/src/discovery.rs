//! Static service discovery via a `services.toml` file (ARCHITECTURE.md).
//!
//! The file maps logical service names to gRPC endpoint URLs:
//!
//! ```toml
//! [services]
//! auth = "http://auth:8082"
//! admin = "http://admin:8083"
//! custodian = "http://custodian-leader:8081"
//! db = "http://[::1]:50051"
//! ```
//!
//! Resolution order for a service is **env-var override → registry entry → caller default**,
//! so an operator can pin one service without editing the file. The registry is designed for
//! *periodic reload*: [`ServiceRegistry::load`] is cheap and side-effect-free, so a caller can
//! re-read it on an interval and diff the result against the previous snapshot to detect changes
//! (see `ServiceRegistry`'s `PartialEq`). LBRP wires this into a background reload task.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::InfoVulcanError;

/// A parsed `services.toml`: logical service name → endpoint URL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct ServiceRegistry {
    /// The `[services]` table. Missing/empty is allowed (everything falls back to defaults).
    #[serde(default)]
    services: BTreeMap<String, String>,
}

impl ServiceRegistry {
    /// Parse a registry from TOML source.
    ///
    /// # Errors
    ///
    /// Returns [`InfoVulcanError::ValidationError`] if the TOML is malformed.
    pub fn from_toml_str(source: &str) -> Result<Self, InfoVulcanError> {
        toml::from_str(source)
            .map_err(|e| InfoVulcanError::ValidationError(format!("invalid services.toml: {e}")))
    }

    /// Load a registry from a file on disk.
    ///
    /// # Errors
    ///
    /// Returns [`InfoVulcanError::NotFound`] if the file cannot be read, or
    /// [`InfoVulcanError::ValidationError`] if its contents are not valid TOML.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, InfoVulcanError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|e| {
            InfoVulcanError::NotFound(format!("services.toml at {}: {e}", path.display()))
        })?;
        Self::from_toml_str(&source)
    }

    /// The configured endpoint for `service`, if present.
    #[must_use]
    pub fn get(&self, service: &str) -> Option<&str> {
        self.services.get(service).map(String::as_str)
    }

    /// Number of configured services.
    #[must_use]
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Whether no services are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Resolve an endpoint for `service` using **env override → registry → default**.
    ///
    /// `env_var` is consulted first (a non-empty value wins), then this registry, then `default`.
    /// This lets a single service be pinned via the environment without editing the file, while
    /// the file remains the source of truth for everything else.
    #[must_use]
    pub fn resolve(&self, service: &str, env_var: &str, default: &str) -> String {
        let override_value = std::env::var(env_var).ok();
        self.resolve_with(override_value.as_deref(), service, default)
    }

    /// Pure core of [`resolve`](Self::resolve): the env value is supplied explicitly so the
    /// precedence rules can be exercised without touching process-global state.
    ///
    /// A `Some` override wins only when it is non-blank; otherwise the registry entry is used,
    /// falling back to `default`.
    #[must_use]
    pub fn resolve_with(
        &self,
        override_value: Option<&str>,
        service: &str,
        default: &str,
    ) -> String {
        if let Some(value) = override_value
            && !value.trim().is_empty()
        {
            return value.to_string();
        }
        self.get(service)
            .map_or_else(|| default.to_string(), ToString::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
        [services]
        auth = "http://auth:8082"
        admin = "http://admin:8083"
        custodian = "http://custodian-leader:8081"
    "#;

    #[test]
    fn parses_services_table() {
        let reg = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        assert_eq!(reg.len(), 3);
        assert!(!reg.is_empty());
        assert_eq!(reg.get("auth"), Some("http://auth:8082"));
        assert_eq!(reg.get("custodian"), Some("http://custodian-leader:8081"));
        assert_eq!(reg.get("missing"), None);
    }

    #[test]
    fn empty_or_absent_table_is_ok() {
        let empty = ServiceRegistry::from_toml_str("").unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        let no_table = ServiceRegistry::from_toml_str("# just a comment\n").unwrap();
        assert!(no_table.is_empty());
    }

    #[test]
    fn malformed_toml_is_an_error() {
        assert!(ServiceRegistry::from_toml_str("services = [").is_err());
    }

    #[test]
    fn load_missing_file_is_not_found() {
        let err = ServiceRegistry::load("/nonexistent/services.toml").unwrap_err();
        assert!(matches!(err, InfoVulcanError::NotFound(_)));
    }

    #[test]
    fn load_reads_a_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("services.toml");
        std::fs::write(&path, SAMPLE).unwrap();
        let reg = ServiceRegistry::load(&path).unwrap();
        assert_eq!(reg.get("admin"), Some("http://admin:8083"));
    }

    #[test]
    fn resolve_with_prefers_registry_over_default() {
        let reg = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        let addr = reg.resolve_with(None, "auth", "http://fallback:1");
        assert_eq!(addr, "http://auth:8082");
    }

    #[test]
    fn resolve_with_falls_back_to_default_when_absent() {
        let reg = ServiceRegistry::default();
        let addr = reg.resolve_with(None, "auth", "http://fallback:1");
        assert_eq!(addr, "http://fallback:1");
    }

    #[test]
    fn resolve_with_env_override_wins() {
        let reg = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        let addr = reg.resolve_with(Some("http://override:9999"), "auth", "http://fallback:1");
        assert_eq!(addr, "http://override:9999");
    }

    #[test]
    fn resolve_with_blank_override_is_ignored() {
        let reg = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        let addr = reg.resolve_with(Some("  "), "auth", "http://fallback:1");
        assert_eq!(addr, "http://auth:8082");
    }

    #[test]
    fn resolve_reads_the_environment() {
        // The env var is not set in the test environment, so this exercises the env→registry
        // fall-through path of the public `resolve` wrapper.
        let reg = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        let addr = reg.resolve(
            "auth",
            "INFOVULCAN_TEST_DEFINITELY_UNSET",
            "http://fallback:1",
        );
        assert_eq!(addr, "http://auth:8082");
    }

    #[test]
    fn registries_compare_for_change_detection() {
        let a = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        let b = ServiceRegistry::from_toml_str(SAMPLE).unwrap();
        assert_eq!(a, b);
        let c = ServiceRegistry::from_toml_str("[services]\nauth = \"http://other:1\"\n").unwrap();
        assert_ne!(a, c);
    }
}
