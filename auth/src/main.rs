#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use anyhow::Result;
use server::AuthServiceImpl;
use server::auth::auth_service_server::AuthServiceServer;
use std::env;
use std::fs;
use std::path::Path;
use tonic::transport::Server;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

mod server;

pub(crate) fn load_or_generate_jwt_secret(storage_path: &Path) -> Result<Vec<u8>> {
    let jwt_secret_path = storage_path.join("jwt.secret");
    if jwt_secret_path.exists() {
        return Ok(fs::read(&jwt_secret_path)?);
    }

    let secret: [u8; 32] = rand::random();
    fs::write(&jwt_secret_path, secret)?;
    Ok(secret.to_vec())
}

/// Resolves the JWT secret: returns `env_secret` as bytes if provided, otherwise
/// loads (or generates) the on-disk secret.
///
/// # Errors
/// Returns an error if the on-disk secret cannot be read or created.
pub(crate) fn resolve_jwt_secret(
    env_secret: Option<String>,
    storage_path: &Path,
) -> Result<Vec<u8>> {
    if let Some(secret) = env_secret {
        Ok(secret.into_bytes())
    } else {
        load_or_generate_jwt_secret(storage_path)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Auth service...");

    let listen_addr = env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| "[::1]:8082".to_string())
        .parse()?;
    let db_addr = env::var("DB_ADDR").unwrap_or_else(|_| "http://[::1]:8080".to_string());
    let storage_path =
        env::var("STORAGE_PATH").unwrap_or_else(|_| "/tmp/infovulcan-auth".to_string());

    // Ensure storage path exists
    fs::create_dir_all(&storage_path)?;

    // Load or generate encryption keys (shared with other services via key_store)
    let keys = shared::key_store::load_or_generate_keypair(Path::new(&storage_path))?;

    // Load or generate JWT secret
    let jwt_secret = resolve_jwt_secret(env::var("JWT_SECRET").ok(), Path::new(&storage_path))?;

    // Connect to DB
    info!("Connecting to DB at {}", db_addr);
    let channel = proto::tls::connect(&db_addr).await?;
    let db_client = server::db::database_client::DatabaseClient::new(channel);

    // Create service
    let auth_service = AuthServiceImpl::new(db_client, jwt_secret, keys);

    info!("Auth service listening on {}", listen_addr);

    proto::tls::apply_server_tls(Server::builder())?
        .add_service(AuthServiceServer::new(auth_service))
        .serve(listen_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
