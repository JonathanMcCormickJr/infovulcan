#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Honeypot binary: serves the deceptive `HoneypotService` (advertised as `CriticalBackups`)
//! and reports every access to the admin service as an intrusion event.

use honeypot::reporter::Reporter;
use honeypot::service::HoneypotServiceImpl;
use proto::honeypot::honeypot_service_server::HoneypotServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = std::env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8085".to_string())
        .parse()?;
    let service = HoneypotServiceImpl::new(Reporter::from_env());

    tracing::info!("CriticalBackups service (honeypot) listening on {addr}");

    // mTLS applied when configured (see proto::tls); plaintext otherwise.
    proto::tls::apply_server_tls(Server::builder())?
        .add_service(HoneypotServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
