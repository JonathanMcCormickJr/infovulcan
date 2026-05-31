#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Spike crate: a tiny openraft cluster wired with an in-process loopback
//! network.
//!
//! ## What this proves
//!
//! - `Raft::new` + `Raft::initialize` bootstrap a working node.
//! - `RaftNetworkFactory` / `RaftNetwork` only need to forward 3 RPCs
//!   (`append_entries`, `vote`, `install_snapshot`); routing them in-process
//!   is enough for replication, election, and recovery.
//! - Followers reach the same state-machine snapshot as the leader once
//!   `Raft::client_write` returns success — i.e. linearizable writes.
//! - Shutting down the leader yields a new election; quorum survives one
//!   failure in a 3-node cluster.
//!
//! ## Out of scope (intentionally)
//!
//! Real persistence, real network transport, snapshot rotation under load.
//! Those belong in the production `db` / `custodian` services, not here.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::Raft;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::Adaptor;
use tokio::sync::RwLock;

pub use openraft_memstore::ClientRequest;
pub use openraft_memstore::ClientResponse;
pub use openraft_memstore::MemNodeId as NodeId;
pub use openraft_memstore::MemStore;
pub use openraft_memstore::TypeConfig;

pub type ToyRaft = Raft<TypeConfig>;

/// Shared registry of every Raft node in the cluster. The loopback network
/// reads from this map to dispatch RPCs without ever touching a socket.
#[derive(Clone, Default)]
pub struct Cluster {
    nodes: Arc<RwLock<HashMap<NodeId, ToyRaft>>>,
}

impl Cluster {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn insert(&self, id: NodeId, raft: ToyRaft) {
        self.nodes.write().await.insert(id, raft);
    }

    /// Remove a node from the registry. After this, any in-flight RPC
    /// targeting `id` will fail with `Unreachable` — which is exactly the
    /// signal Raft needs to start an election if `id` was the leader.
    pub async fn remove(&self, id: NodeId) {
        self.nodes.write().await.remove(&id);
    }

    async fn get(&self, id: NodeId) -> Option<ToyRaft> {
        self.nodes.read().await.get(&id).cloned()
    }
}

/// Factory the Raft engine calls to mint a per-target `RaftNetwork` instance.
#[derive(Clone)]
pub struct LoopbackFactory {
    cluster: Cluster,
}

impl LoopbackFactory {
    #[must_use]
    pub fn new(cluster: Cluster) -> Self {
        Self { cluster }
    }
}

impl RaftNetworkFactory<TypeConfig> for LoopbackFactory {
    type Network = LoopbackConn;

    async fn new_client(&mut self, target: NodeId, _node: &()) -> Self::Network {
        LoopbackConn {
            target,
            cluster: self.cluster.clone(),
        }
    }
}

/// Per-target RPC client. Each replication stream in the engine holds one of
/// these and calls into it whenever it has something to send.
pub struct LoopbackConn {
    target: NodeId,
    cluster: Cluster,
}

impl LoopbackConn {
    async fn peer(&self) -> Result<ToyRaft, RPCError<NodeId, (), RaftError<NodeId>>> {
        self.cluster.get(self.target).await.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("node {} not in cluster", self.target),
            ))))
        })
    }
}

impl RaftNetwork<TypeConfig> for LoopbackConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        let peer = self.peer().await?;
        peer.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, (), RaftError<NodeId, InstallSnapshotError>>,
    > {
        let peer = self.cluster.get(self.target).await.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("node {} not in cluster", self.target),
            ))))
        })?;
        peer.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        let peer = self.peer().await?;
        peer.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

/// Standard Raft config the spike uses everywhere. Tight timings keep tests
/// fast — production would use multi-second election timeouts.
///
/// # Panics
/// Panics if the hard-coded timings fail `Config::validate`, which would
/// indicate a bug in this function rather than a runtime condition.
#[must_use]
pub fn raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: "spike-toy".to_string(),
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .expect("config validates"),
    )
}

/// Spin up a single Raft node, register it in `cluster`, and hand back both
/// the `Raft` handle and the underlying `MemStore` (so tests can inspect the
/// state machine directly).
///
/// # Errors
/// Propagates any fatal error from `Raft::new`.
pub async fn spawn_node(
    id: NodeId,
    cluster: &Cluster,
    config: Arc<Config>,
) -> anyhow::Result<(ToyRaft, Arc<MemStore>)> {
    let store = MemStore::new_async().await;
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let network = LoopbackFactory::new(cluster.clone());
    let raft = Raft::new(id, config, network, log_store, state_machine).await?;
    cluster.insert(id, raft.clone()).await;
    Ok((raft, store))
}

/// Wait up to `timeout` for `predicate` on the node's metrics to return true.
/// Returns the matching metrics or `None` on timeout. Useful for "wait until
/// node N has applied entry M" assertions.
pub async fn wait_for_metrics<F>(
    raft: &ToyRaft,
    timeout: Duration,
    predicate: F,
) -> Option<openraft::RaftMetrics<NodeId, ()>>
where
    F: Fn(&openraft::RaftMetrics<NodeId, ()>) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut rx = raft.metrics();
    loop {
        let snapshot = rx.borrow().clone();
        if predicate(&snapshot) {
            return Some(snapshot);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        // Either a metrics change or our deadline tick will wake us.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining, rx.changed()).await;
    }
}

/// Build a `ClientRequest` with the spike's conventional shape: each request
/// records a status string keyed by `client`.
#[must_use]
pub fn make_request(client: &str, serial: u64, status: &str) -> ClientRequest {
    ClientRequest {
        client: client.to_string(),
        serial,
        status: status.to_string(),
    }
}

/// Snapshot the `client_status` map out of a `MemStore`'s state machine.
/// Returns a sorted clone, so tests can compare directly.
pub async fn read_status(store: &MemStore) -> BTreeMap<String, String> {
    let sm = store.get_state_machine().await;
    sm.client_status.into_iter().collect()
}
