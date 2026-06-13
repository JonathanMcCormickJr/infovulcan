#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

mod clients;
mod discovery;
mod error;
mod middleware;
mod routes;

use clients::{AdminClient, AuthClient, CustodianClient};
use discovery::{BackendAddrs, ReloadableClients};
use middleware::AuthState;
use routes::AppState;
use shared::ServiceRegistry;

pub(crate) fn parse_listen_addr(
    raw: Option<String>,
) -> Result<SocketAddr, std::net::AddrParseError> {
    raw.unwrap_or_else(|| "0.0.0.0:8080".to_string()).parse()
}

pub(crate) fn env_or_default(raw: Option<String>, default_value: &str) -> String {
    raw.unwrap_or_else(|| default_value.to_string())
}

pub(crate) fn jwt_secret_from_env(raw: Option<String>) -> Vec<u8> {
    raw.map_or_else(|| b"secret".to_vec(), String::into_bytes)
}

/// Resolves the three backend service addresses from optional raw values, falling back to
/// defaults suitable for container service-name routing.
///
/// Returns `(auth_addr, admin_addr, custodian_addr)`.
pub(crate) fn resolve_backend_addrs(
    auth_raw: Option<String>,
    admin_raw: Option<String>,
    custodian_raw: Option<String>,
) -> (String, String, String) {
    (
        env_or_default(auth_raw, "http://auth:8082"),
        env_or_default(admin_raw, "http://admin:8083"),
        env_or_default(custodian_raw, "http://custodian-leader:8081"),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = parse_listen_addr(std::env::var("LISTEN_ADDR").ok())?;

    // Service addresses. When `SERVICES_TOML` points at a `services.toml`, addresses are resolved
    // from it (env vars still override per service) and a background task reloads it periodically.
    // Otherwise we fall back to the env-or-default behaviour.
    let services_toml = std::env::var("SERVICES_TOML").ok();
    let registry = services_toml
        .as_deref()
        .and_then(|path| match ServiceRegistry::load(path) {
            Ok(reg) => {
                info!(
                    "LBRP loaded service discovery from {path} ({} entries)",
                    reg.len()
                );
                Some(reg)
            }
            Err(e) => {
                tracing::warn!("LBRP could not load {path}: {e}; using env/defaults");
                None
            }
        });

    let backend = if let Some(reg) = &registry {
        BackendAddrs::from_env_registry(reg)
    } else {
        let (auth, admin, custodian) = resolve_backend_addrs(
            std::env::var("AUTH_ADDR").ok(),
            std::env::var("ADMIN_ADDR").ok(),
            std::env::var("CUSTODIAN_ADDR").ok(),
        );
        BackendAddrs {
            auth,
            admin,
            custodian,
        }
    };

    info!("LBRP Service starting on {}", addr);

    // Connect to backend services
    let auth_client = AuthClient::connect(backend.auth.clone()).await?;
    let admin_client = AdminClient::connect(backend.admin.clone()).await?;
    let custodian_client = CustodianClient::connect(backend.custodian.clone()).await?;

    // Periodic services.toml reload: hot-reconnect clients when an endpoint changes.
    if let (Some(path), true) = (services_toml, registry.is_some()) {
        let reload_secs: u64 = std::env::var("SERVICES_RELOAD_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        discovery::spawn_reloader(
            path,
            std::time::Duration::from_secs(reload_secs),
            ReloadableClients {
                auth: auth_client.clone(),
                admin: admin_client.clone(),
                custodian: custodian_client.clone(),
            },
            backend.clone(),
        );
        info!("LBRP services.toml reload enabled (every {reload_secs}s)");
    }

    // JWT Secret (must match Auth service)
    // In production, load from secure vault/env
    let jwt_secret = jwt_secret_from_env(std::env::var("JWT_SECRET").ok());

    let app_state = AppState {
        auth_client,
        admin_client,
        custodian_client,
        auth_state: Arc::new(AuthState { jwt_secret }),
    };

    let web_dist = std::env::var("WEB_DIST_DIR").unwrap_or_else(|_| "../web/dist".to_string());

    // Per-client (peer-IP) rate limiting. Tunable via RATE_LIMIT_PER_SEC / RATE_LIMIT_BURST.
    let per_sec: u64 = std::env::var("RATE_LIMIT_PER_SEC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let burst: u32 = std::env::var("RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let governor_conf = Arc::new(
        tower_governor::governor::GovernorConfigBuilder::default()
            .per_second(per_sec)
            .burst_size(burst)
            .finish()
            .expect("valid governor config"),
    );

    let app = routes::app(app_state)
        .fallback_service(tower_http::services::ServeDir::new(&web_dist).fallback(
            tower_http::services::ServeFile::new(format!("{web_dist}/index.html")),
        ))
        .layer(tower_governor::GovernorLayer {
            config: governor_conf,
        });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // ConnectInfo is required so the rate limiter can key on the client's peer IP.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
