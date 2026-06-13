#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use tonic::transport::Server;

mod chaos_service;
use chaos_service::ChaosServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Chaos service starting...");

    let addr = "0.0.0.0:8084".parse()?;
    let chaos_service = ChaosServiceImpl::default();

    println!("Chaos service listening on {addr}");

    proto::tls::apply_server_tls(Server::builder())?
        .add_service(
            chaos_service::chaos::chaos_service_server::ChaosServiceServer::new(chaos_service),
        )
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
