#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

mod server;

use server::AdminServiceImpl;
use server::admin::admin_service_server::AdminServiceServer;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::info;

/// Parses a listen address from an optional raw string, falling back to a default.
///
/// # Errors
/// Returns an [`std::net::AddrParseError`] if the string is not a valid socket address.
pub(crate) fn parse_listen_addr(
    raw: Option<String>,
) -> Result<SocketAddr, std::net::AddrParseError> {
    raw.unwrap_or_else(|| "0.0.0.0:8083".to_string()).parse()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = parse_listen_addr(std::env::var("LISTEN_ADDR").ok())?;
    let db_addr = std::env::var("DB_ADDR").unwrap_or_else(|_| "http://db-leader:8080".to_string());
    let storage_path =
        std::env::var("STORAGE_PATH").unwrap_or_else(|_| "/tmp/infovulcan-admin".to_string());

    info!("Admin Service starting on {}", addr);

    // Ensure storage path exists
    fs::create_dir_all(&storage_path)?;

    let keys = shared::key_store::load_or_generate_keypair(Path::new(&storage_path))?;

    // Connect to DB
    info!("Connecting to DB at {}", db_addr);
    let channel = proto::tls::connect(&db_addr).await?;
    let db_client = server::db::database_client::DatabaseClient::new(channel);
    let db_client = Arc::new(Mutex::new(db_client));

    let admin_service = AdminServiceImpl::new(db_client, keys);

    proto::tls::apply_server_tls(Server::builder())?
        .add_service(AdminServiceServer::new(admin_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
