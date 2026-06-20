//! `services.toml`-backed static discovery with periodic reload.
//!
//! At startup LBRP resolves its three backend endpoints from (in precedence order) an env-var
//! override, the `services.toml` registry, then a built-in default. When `SERVICES_TOML` is set,
//! [`spawn_reloader`] re-reads the file every `SERVICES_RELOAD_SECS` (default 30s) and, for any
//! endpoint that changed, hot-reconnects the corresponding gRPC client without restarting LBRP.

use std::time::Duration;

use shared::ServiceRegistry;
use tracing::{info, warn};

use crate::clients::{AdminClient, AuthClient, CustodianClient};

/// The three backend endpoints LBRP routes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendAddrs {
    pub auth: String,
    pub admin: String,
    pub custodian: String,
}

impl BackendAddrs {
    /// Built-in defaults (suitable for container service-name routing).
    pub const DEFAULT_AUTH: &'static str = "http://auth:8082";
    pub const DEFAULT_ADMIN: &'static str = "http://admin:8083";
    pub const DEFAULT_CUSTODIAN: &'static str = "http://custodian-leader:8081";

    /// Resolve all three endpoints from a registry, with explicit per-service env overrides.
    ///
    /// Precedence for each: `*_override` (if non-blank) → registry entry → built-in default.
    #[must_use]
    pub fn from_registry(
        reg: &ServiceRegistry,
        auth_override: Option<&str>,
        admin_override: Option<&str>,
        custodian_override: Option<&str>,
    ) -> Self {
        Self {
            auth: reg.resolve_with(auth_override, "auth", Self::DEFAULT_AUTH),
            admin: reg.resolve_with(admin_override, "admin", Self::DEFAULT_ADMIN),
            custodian: reg.resolve_with(custodian_override, "custodian", Self::DEFAULT_CUSTODIAN),
        }
    }

    /// Resolve from a registry, reading the `AUTH_ADDR` / `ADMIN_ADDR` / `CUSTODIAN_ADDR`
    /// overrides from the process environment.
    #[must_use]
    pub fn from_env_registry(reg: &ServiceRegistry) -> Self {
        let auth = std::env::var("AUTH_ADDR").ok();
        let admin = std::env::var("ADMIN_ADDR").ok();
        let custodian = std::env::var("CUSTODIAN_ADDR").ok();
        Self::from_registry(reg, auth.as_deref(), admin.as_deref(), custodian.as_deref())
    }
}

/// The clients a reload may redirect.
#[derive(Clone)]
pub struct ReloadableClients {
    pub auth: AuthClient,
    pub admin: AdminClient,
    pub custodian: CustodianClient,
}

/// Reconnect exactly the clients whose endpoint differs between `current` and `next`.
///
/// Returns the set of services that were redirected (so callers can log/assert). A failed
/// reconnect is logged and the old channel is kept (the service name is *not* returned).
pub fn apply_changes(
    current: &BackendAddrs,
    next: &BackendAddrs,
    clients: &ReloadableClients,
) -> Vec<&'static str> {
    let mut changed = Vec::new();

    if current.auth != next.auth {
        match clients.auth.reconnect(&next.auth) {
            Ok(()) => {
                info!(service = "auth", addr = %next.auth, "discovery: reconnected");
                changed.push("auth");
            }
            Err(e) => {
                warn!(service = "auth", addr = %next.auth, error = %e, "discovery: reconnect failed");
            }
        }
    }
    if current.admin != next.admin {
        match clients.admin.reconnect(&next.admin) {
            Ok(()) => {
                info!(service = "admin", addr = %next.admin, "discovery: reconnected");
                changed.push("admin");
            }
            Err(e) => {
                warn!(service = "admin", addr = %next.admin, error = %e, "discovery: reconnect failed");
            }
        }
    }
    if current.custodian != next.custodian {
        match clients.custodian.reconnect(&next.custodian) {
            Ok(()) => {
                info!(service = "custodian", addr = %next.custodian, "discovery: reconnected");
                changed.push("custodian");
            }
            Err(e) => {
                warn!(service = "custodian", addr = %next.custodian, error = %e, "discovery: reconnect failed");
            }
        }
    }

    changed
}

/// Perform one reload: re-read `path`, and if the resolved endpoints differ from `current`,
/// hot-reconnect the changed clients. Returns the endpoints in effect after this tick (the new
/// set on success+change, otherwise `current` unchanged). A read/parse failure is logged and
/// keeps the current endpoints.
pub fn reload_once(path: &str, current: BackendAddrs, clients: &ReloadableClients) -> BackendAddrs {
    match ServiceRegistry::load(path) {
        Ok(reg) => {
            let next = BackendAddrs::from_env_registry(&reg);
            if next == current {
                return current;
            }
            let changed = apply_changes(&current, &next, clients);
            if !changed.is_empty() {
                info!(?changed, "discovery: applied services.toml changes");
            }
            next
        }
        Err(e) => {
            warn!(path = %path, error = %e, "discovery: reload failed; keeping current endpoints");
            current
        }
    }
}

/// Spawn the periodic reload loop. Calls [`reload_once`] every `interval`. Runs until exit.
pub fn spawn_reloader(
    path: String,
    interval: Duration,
    clients: ReloadableClients,
    initial: BackendAddrs,
) {
    tokio::spawn(async move {
        let mut current = initial;
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it so we don't re-read the file we just loaded.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            current = reload_once(&path, current, &clients);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(toml: &str) -> ServiceRegistry {
        ServiceRegistry::from_toml_str(toml).unwrap()
    }

    #[test]
    fn from_registry_uses_defaults_when_empty() {
        let addrs = BackendAddrs::from_registry(&ServiceRegistry::default(), None, None, None);
        assert_eq!(addrs.auth, BackendAddrs::DEFAULT_AUTH);
        assert_eq!(addrs.admin, BackendAddrs::DEFAULT_ADMIN);
        assert_eq!(addrs.custodian, BackendAddrs::DEFAULT_CUSTODIAN);
    }

    #[test]
    fn from_registry_uses_file_entries() {
        let reg = registry(
            r#"
            [services]
            auth = "http://a:1"
            admin = "http://b:2"
            custodian = "http://c:3"
        "#,
        );
        let addrs = BackendAddrs::from_registry(&reg, None, None, None);
        assert_eq!(addrs.auth, "http://a:1");
        assert_eq!(addrs.admin, "http://b:2");
        assert_eq!(addrs.custodian, "http://c:3");
    }

    #[test]
    fn env_override_beats_file() {
        let reg = registry("[services]\nauth = \"http://file:1\"\n");
        let addrs = BackendAddrs::from_registry(&reg, Some("http://env:9"), None, None);
        assert_eq!(addrs.auth, "http://env:9");
        // admin/custodian fall through to defaults
        assert_eq!(addrs.admin, BackendAddrs::DEFAULT_ADMIN);
    }

    fn lazy_clients() -> ReloadableClients {
        let lazy = || proto::tls::connect_lazy("http://127.0.0.1:9").unwrap();
        ReloadableClients {
            auth: AuthClient::from_channel(lazy()),
            admin: AdminClient::from_channel(lazy()),
            custodian: CustodianClient::from_channel(lazy()),
        }
    }

    #[tokio::test]
    async fn apply_changes_skips_unchanged_services() {
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://a:1".to_string(),
            admin: "http://b:2".to_string(),
            custodian: "http://c:3".to_string(),
        };
        // No differences → no reconnect attempts → empty result.
        let same = current.clone();
        assert!(apply_changes(&current, &same, &clients).is_empty());
    }

    #[tokio::test]
    async fn apply_changes_reconnects_changed_services() {
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://a:1".to_string(),
            admin: "http://b:2".to_string(),
            custodian: "http://c:3".to_string(),
        };
        // Only auth changes; reconnect to a lazy (valid-format) endpoint succeeds without I/O.
        let next = BackendAddrs {
            auth: "http://127.0.0.1:9".to_string(),
            ..current.clone()
        };
        let changed = apply_changes(&current, &next, &clients);
        assert_eq!(changed, vec!["auth"]);
    }

    #[tokio::test]
    async fn apply_changes_reconnects_all_three_when_all_differ() {
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://a:1".to_string(),
            admin: "http://b:2".to_string(),
            custodian: "http://c:3".to_string(),
        };
        let next = BackendAddrs {
            auth: "http://127.0.0.1:9".to_string(),
            admin: "http://127.0.0.1:10".to_string(),
            custodian: "http://127.0.0.1:11".to_string(),
        };
        let changed = apply_changes(&current, &next, &clients);
        assert_eq!(changed, vec!["auth", "admin", "custodian"]);
    }

    #[test]
    fn from_env_registry_resolves_from_file() {
        // No AUTH_ADDR/ADMIN_ADDR/CUSTODIAN_ADDR set in the test env → file/defaults win.
        let reg = registry("[services]\ncustodian = \"http://cust:8081\"\n");
        let addrs = BackendAddrs::from_env_registry(&reg);
        assert_eq!(addrs.custodian, "http://cust:8081");
        assert_eq!(addrs.auth, BackendAddrs::DEFAULT_AUTH);
    }

    #[tokio::test]
    async fn reload_once_applies_a_changed_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("services.toml");
        std::fs::write(&path, "[services]\nauth = \"http://127.0.0.1:9\"\n").unwrap();

        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://old:1".to_string(),
            admin: BackendAddrs::DEFAULT_ADMIN.to_string(),
            custodian: BackendAddrs::DEFAULT_CUSTODIAN.to_string(),
        };
        let next = reload_once(path.to_str().unwrap(), current, &clients);
        // auth was redirected to the file's value; the others fell back to defaults (unchanged).
        assert_eq!(next.auth, "http://127.0.0.1:9");
    }

    #[tokio::test]
    async fn reload_once_keeps_current_on_missing_file() {
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://keep:1".to_string(),
            admin: "http://keep:2".to_string(),
            custodian: "http://keep:3".to_string(),
        };
        let next = reload_once("/no/such/services.toml", current.clone(), &clients);
        assert_eq!(next, current);
    }

    #[tokio::test]
    async fn reload_once_is_noop_when_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("services.toml");
        // A file that resolves to exactly the defaults → no change.
        std::fs::write(&path, "[services]\n").unwrap();
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: BackendAddrs::DEFAULT_AUTH.to_string(),
            admin: BackendAddrs::DEFAULT_ADMIN.to_string(),
            custodian: BackendAddrs::DEFAULT_CUSTODIAN.to_string(),
        };
        let next = reload_once(path.to_str().unwrap(), current.clone(), &clients);
        assert_eq!(next, current);
    }

    #[tokio::test]
    async fn apply_changes_keeps_old_channel_when_reconnect_fails() {
        let clients = lazy_clients();
        let current = BackendAddrs {
            auth: "http://a:1".to_string(),
            admin: "http://b:2".to_string(),
            custodian: "http://c:3".to_string(),
        };
        // All three move to malformed endpoints → every reconnect fails → none reported changed.
        let next = BackendAddrs {
            auth: "::::bad uri::::".to_string(),
            admin: "::::bad uri::::".to_string(),
            custodian: "::::bad uri::::".to_string(),
        };
        let changed = apply_changes(&current, &next, &clients);
        assert!(changed.is_empty());
    }

    #[tokio::test]
    async fn spawn_reloader_runs_at_least_one_reload_tick() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("services.toml");
        std::fs::write(&path, "[services]\n").unwrap();
        let initial = BackendAddrs {
            auth: BackendAddrs::DEFAULT_AUTH.to_string(),
            admin: BackendAddrs::DEFAULT_ADMIN.to_string(),
            custodian: BackendAddrs::DEFAULT_CUSTODIAN.to_string(),
        };
        spawn_reloader(
            path.to_str().unwrap().to_string(),
            Duration::from_millis(5),
            lazy_clients(),
            initial,
        );
        // Give the detached loop time to fire reload_once at least once before the test ends.
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}
