#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use anyhow::Result;
use custodian::network::CustodianNetworkFactory;
use custodian::raft::{CustodianRaft, CustodianStore};
use custodian::server::{CustodianServiceImpl, create_server};
use openraft::Config;
use openraft::storage::Adaptor;
use shared::encryption::EncryptionService;
use std::env;
use std::fs;
use std::path::Path;
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

/// Builds the standard Raft configuration for the Custodian service.
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

/// Builds a [`CustodianNetworkFactory`] populated from a `RAFT_PEERS`-format string and
/// returns the list of [`PeerConfig`] entries in encounter order.
pub(crate) fn build_network_factory_and_peers(
    peers_str: &str,
) -> (CustodianNetworkFactory, Vec<PeerConfig>) {
    let mut factory = CustodianNetworkFactory::new();
    let peers = parse_peer_configs(peers_str);
    for peer in &peers {
        factory.add_node(peer.node_id, peer.address.clone());
    }
    (factory, peers)
}

/// Loads encryption keys from `keys_path` if the file exists, or generates a fresh keypair
/// and writes it to disk.
///
/// # Errors
/// Returns an error if key generation, serialization, or I/O fails.
pub(crate) fn load_or_generate_keys(keys_path: &Path) -> Result<(Vec<u8>, Vec<u8>)> {
    if keys_path.exists() {
        let bytes = fs::read(keys_path)?;
        let keys: (Vec<u8>, Vec<u8>) = postcard::from_bytes(&bytes)?;
        Ok(keys)
    } else {
        let keys = EncryptionService::generate_keypair()
            .map_err(|e| anyhow::anyhow!("Failed to generate keys: {e}"))?;
        let bytes = postcard::to_allocvec(&keys)?;
        fs::write(keys_path, bytes)?;
        Ok(keys)
    }
}

/// Creates a [`CustodianStore`] and initialises a [`CustodianRaft`] consensus node.
///
/// # Errors
/// Returns an error if storage initialisation or Raft node creation fails.
pub(crate) async fn build_raft_node(
    node_id: u64,
    config: Arc<Config>,
    network_factory: CustodianNetworkFactory,
    storage_path: &str,
) -> Result<(Arc<CustodianRaft>, CustodianStore)> {
    let store = CustodianStore::new(storage_path)?;
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let raft: Arc<CustodianRaft> = Arc::new(
        CustodianRaft::new(node_id, config, network_factory, log_store, state_machine).await?,
    );
    Ok((raft, store))
}

/// Initialises the Raft cluster when this node is the designated first peer.
///
/// Errors from [`openraft::Raft::initialize`] are treated as non-fatal because
/// re-bootstrapping an already-initialised cluster returns a benign error on restart.
pub(crate) async fn initialize_cluster_if_leader(
    raft: &CustodianRaft,
    node_id: u64,
    all_peers: &[u64],
) {
    if should_initialize_cluster(node_id, all_peers) {
        let mut members = std::collections::BTreeSet::new();
        for peer_id in all_peers {
            members.insert(*peer_id);
        }
        // Non-fatal: may fail if cluster was already bootstrapped.
        let _ = raft.initialize(members).await;
    }
}

/// Connects to the DB service leader if an endpoint is provided.
///
/// Returns `None` and emits a warning if the connection fails.
pub(crate) async fn maybe_connect_db(
    db_endpoint: Option<String>,
) -> Option<Arc<tokio::sync::Mutex<custodian::db_client::DbClient>>> {
    let endpoint = db_endpoint?;
    match custodian::db_client::DbClient::connect(endpoint).await {
        Ok(c) => Some(Arc::new(tokio::sync::Mutex::new(c))),
        Err(e) => {
            tracing::warn!("failed to connect to db: {}", e);
            None
        }
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
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
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "[::1]:8081".to_string());
    let storage_path =
        env::var("STORAGE_PATH").unwrap_or_else(|_| format!("/tmp/infovulcan-custodian-{node_id}"));

    // Parse peer nodes configuration
    // Format: "1:http://127.0.0.1:8081,2:http://127.0.0.1:8082,3:http://127.0.0.1:8083"
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

    info!(
        "Starting Custodian service node {} on {}",
        node_id, listen_addr
    );
    info!("Storage path: {}", storage_path);
    info!("Cluster peers: {:?}", all_peers);

    // Create Raft config and consensus node (also initialises the storage directory)
    let config = make_raft_config()?;
    let (raft, store) = build_raft_node(node_id, config, network_factory, &storage_path).await?;
    let storage = store.storage().clone();

    info!("Storage initialized at {}", storage_path);

    // Load or generate encryption keys (storage directory is now guaranteed to exist)
    let keys_path = Path::new(&storage_path).join("keys.bin");
    let keys = load_or_generate_keys(&keys_path)?;

    info!("Raft node {} initialized", node_id);

    // Initialize cluster if this node is the designated first peer
    initialize_cluster_if_leader(&raft, node_id, &all_peers).await;

    info!("Custodian service node {} ready", node_id);

    // Parse address
    let addr = parse_listen_addr(&listen_addr)?;

    // Ticket persistence requires the DB service, so DB_LEADER_ADDR is a hard
    // startup requirement: fail fast rather than silently dropping ticket writes.
    let db_endpoint = std::env::var("DB_LEADER_ADDR").map_err(|_| {
        anyhow::anyhow!(
            "DB_LEADER_ADDR is required (e.g. http://[::1]:50051) — the custodian persists \
             tickets to the DB service and will not start without it"
        )
    })?;
    let db_client = maybe_connect_db(Some(db_endpoint)).await.ok_or_else(|| {
        anyhow::anyhow!("failed to connect to DB_LEADER_ADDR — is the DB service running?")
    })?;

    let custodian_service =
        CustodianServiceImpl::with_db_client((*raft).clone(), storage.clone(), db_client, keys);

    info!("Starting gRPC server on {}", addr);

    // Serve the inter-node Raft service alongside the public custodian service, so peers
    // can actually replicate in a multi-node cluster (without this, only single-node works).
    let raft_service = custodian::raft_service::RaftServiceImpl::new(raft.clone());

    // Start gRPC server
    proto::tls::apply_server_tls(Server::builder())?
        .add_service(create_server(custodian_service))
        .add_service(
            custodian::server::custodian::raft_service_server::RaftServiceServer::new(raft_service),
        )
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
