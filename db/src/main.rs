#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use anyhow::Result;
use db::network::DbNetworkFactory;
use db::raft::{DbRaft, DbStore};
use db::raft_service::RaftServiceImpl;
use db::server::DatabaseService;
use db::server::db::{database_server::DatabaseServer, raft_service_server::RaftServiceServer};
use openraft::Config;
use openraft::storage::Adaptor;
use std::env;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerConfig {
    pub node_id: u64,
    pub address: String,
}

pub(crate) fn parse_peer_configs(peers_str: &str) -> Vec<PeerConfig> {
    let mut peers = Vec::new();

    for peer_config in peers_str.split(',') {
        let parts: Vec<&str> = peer_config.trim().split(':').collect();
        if parts.len() < 2 {
            continue;
        }

        let peer_id: u64 = parts[0].parse().unwrap_or(0);
        if peer_id == 0 {
            continue;
        }

        peers.push(PeerConfig {
            node_id: peer_id,
            address: parts[1..].join(":"),
        });
    }

    peers
}

pub(crate) fn should_initialize_cluster(node_id: u64, all_peers: &[u64]) -> bool {
    node_id == all_peers.first().copied().unwrap_or(1) && !all_peers.is_empty()
}

/// Parses a raw listen-address string into a [`std::net::SocketAddr`].
///
/// # Errors
/// Returns an [`std::net::AddrParseError`] if the string is not a valid socket address.
pub(crate) fn parse_listen_addr(
    raw: &str,
) -> Result<std::net::SocketAddr, std::net::AddrParseError> {
    raw.parse()
}

/// Builds the standard Raft configuration for the DB service.
///
/// # Errors
/// Returns an error if the configuration fails validation.
pub(crate) fn make_raft_config() -> Result<Arc<Config>> {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    Ok(Arc::new(config.validate()?))
}

/// Builds a [`DbNetworkFactory`] populated from a `RAFT_PEERS`-format string and returns
/// the list of [`PeerConfig`] entries in encounter order.
pub(crate) fn build_network_factory_and_peers(
    peers_str: &str,
) -> (DbNetworkFactory, Vec<PeerConfig>) {
    let mut factory = DbNetworkFactory::new();
    let peers = parse_peer_configs(peers_str);
    for peer in &peers {
        factory.add_node(peer.node_id, peer.address.clone());
    }
    (factory, peers)
}

/// Creates a [`DbStore`] and initialises a [`DbRaft`] consensus node.
///
/// # Errors
/// Returns an error if storage initialisation or Raft node creation fails.
pub(crate) async fn build_raft_node(
    node_id: u64,
    config: Arc<Config>,
    network_factory: DbNetworkFactory,
    storage_path: &str,
) -> Result<(Arc<DbRaft>, DbStore)> {
    let store = DbStore::new(storage_path)?;
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let raft =
        Arc::new(DbRaft::new(node_id, config, network_factory, log_store, state_machine).await?);
    Ok((raft, store))
}

/// Initialises the Raft cluster when this node is the designated first peer.
///
/// Errors from [`openraft::Raft::initialize`] are treated as non-fatal because
/// re-bootstrapping an already-initialised cluster returns a benign error on restart.
pub(crate) async fn initialize_cluster_if_leader(raft: &DbRaft, node_id: u64, all_peers: &[u64]) {
    if should_initialize_cluster(node_id, all_peers) {
        let mut members = std::collections::BTreeSet::new();
        for peer_id in all_peers {
            members.insert(*peer_id);
        }
        // Non-fatal: may fail if cluster was already bootstrapped.
        let _ = raft.initialize(members).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Get configuration from environment
    let node_id: u64 = env::var("NODE_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse()?;
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "[::1]:50051".to_string());
    let storage_path =
        env::var("STORAGE_PATH").unwrap_or_else(|_| format!("/tmp/infovulcan-db-{node_id}"));

    // Parse peer nodes configuration
    // Format: "1:http://127.0.0.1:50051,2:http://127.0.0.1:50052,3:http://127.0.0.1:50053"
    let peers_str = env::var("RAFT_PEERS").unwrap_or_else(|_| {
        // Default single-node cluster
        format!("{node_id}:{listen_addr}")
    });

    let (network_factory, peers) = build_network_factory_and_peers(&peers_str);
    let all_peers: Vec<u64> = peers.iter().map(|p| p.node_id).collect();
    for peer in &peers {
        info!(
            "Configured peer: node_id={}, addr={}",
            peer.node_id, peer.address
        );
    }

    info!("Starting DB service node {} on {}", node_id, listen_addr);
    info!("Storage path: {}", storage_path);
    info!("Cluster peers: {:?}", all_peers);

    // Create storage, Raft config, and consensus node
    let config = make_raft_config()?;
    let (raft, store) = build_raft_node(node_id, config, network_factory, &storage_path).await?;

    info!("Storage initialized at {}", storage_path);
    info!("Raft node {} initialized", node_id);

    // Initialize cluster if this node is the designated first peer
    initialize_cluster_if_leader(&raft, node_id, &all_peers).await;

    info!("DB service node {} ready", node_id);

    // Parse address
    let addr = parse_listen_addr(&listen_addr)?;

    // Get storage for read operations
    let storage = store.state_machine().read().await.storage.clone();

    // Create gRPC services
    let db_service = DatabaseService::new((*raft).clone(), storage);
    let raft_service = RaftServiceImpl::new(raft);

    info!("Starting gRPC server on {}", addr);

    // Start gRPC server with both services
    proto::tls::apply_server_tls(Server::builder())?
        .add_service(DatabaseServer::new(db_service))
        .add_service(RaftServiceServer::new(raft_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
