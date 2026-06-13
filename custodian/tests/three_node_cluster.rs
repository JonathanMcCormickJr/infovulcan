//! Real 3-node Raft cluster integration test for the custodian (distributed ticket locks).
//!
//! Mirrors `db/tests/three_node_cluster.rs`. Spins up three actual custodian Raft nodes as
//! in-process gRPC servers wired over the real tonic Raft network, then exercises:
//!
//! 1. cluster formation + leader election,
//! 2. replication of lock acquisitions to all followers,
//! 3. **leader failover**: killing the leader yields a new leader that can still commit locks,
//! 4. **snapshot recovery**: a node that was down rejoins after the leader snapshotted and
//!    purged its log, so catch-up goes through `InstallSnapshot`.
//!
//! Ignored by default (binds localhost ports, spawns servers, timing-sensitive). Run with
//! `cargo test -p custodian --test three_node_cluster -- --ignored`.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use custodian::CustodianRaft;
use custodian::network::CustodianNetworkFactory;
use custodian::raft::CustodianStore;
use custodian::raft_service::RaftServiceImpl;
use custodian::server::custodian::raft_service_server::RaftServiceServer;
use custodian::storage::{LockCommand, Storage};
use openraft::storage::Adaptor;
use openraft::{Config, SnapshotPolicy};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tonic::transport::Server;
use uuid::Uuid;

const NODE_IDS: [u64; 3] = [1, 2, 3];

fn raft_config() -> Arc<Config> {
    // Automatic snapshotting disabled; we trigger snapshot + purge explicitly at a controlled
    // point so the rejoining node deterministically recovers via InstallSnapshot.
    Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(100_000),
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

type Ports = std::collections::BTreeMap<u64, std::net::SocketAddr>;

fn allocate_ports() -> Ports {
    let mut ports = Ports::new();
    for id in NODE_IDS {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        ports.insert(id, listener.local_addr().expect("local addr"));
    }
    ports
}

struct Node {
    id: u64,
    raft: Option<Arc<CustodianRaft>>,
    storage: Option<Storage>,
    path: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn raft(&self) -> &CustodianRaft {
        self.raft.as_ref().expect("node is running")
    }

    fn storage(&self) -> &Storage {
        self.storage.as_ref().expect("node is running")
    }

    /// Fully stop the node: shut down the gRPC server + Raft engine and drop the store
    /// handles so the on-disk sled lock is released (required to reopen the same path).
    async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server.take() {
            let _ = handle.await;
        }
        if let Some(raft) = &self.raft {
            let _ = raft.shutdown().await;
        }
        self.raft = None;
        self.storage = None;
    }
}

async fn start_node(node_id: u64, path: PathBuf, ports: &Ports) -> Node {
    let mut factory = CustodianNetworkFactory::new();
    for (&peer, addr) in ports {
        factory.add_node(peer, format!("http://{addr}"));
    }

    let store = {
        let path_str = path.to_str().expect("utf8 path");
        let mut attempt = 0;
        loop {
            match CustodianStore::new(path_str) {
                Ok(store) => break store,
                Err(e) if attempt < 50 => {
                    attempt += 1;
                    sleep(Duration::from_millis(50)).await;
                    let _ = e;
                }
                Err(e) => panic!("open store: {e}"),
            }
        }
    };
    let storage = store.state_machine().read().await.storage.clone();
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let raft = Arc::new(
        CustodianRaft::new(node_id, raft_config(), factory, log_store, state_machine)
            .await
            .expect("create raft"),
    );

    let raft_service = RaftServiceImpl::new(raft.clone());
    let (tx, rx) = oneshot::channel::<()>();
    let addr = ports[&node_id];
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftServiceServer::new(raft_service))
            .serve_with_shutdown(addr, async {
                let _ = rx.await;
            })
            .await;
    });

    Node {
        id: node_id,
        raft: Some(raft),
        storage: Some(storage),
        path,
        shutdown: Some(tx),
        server: Some(server),
    }
}

async fn wait_for_leader(nodes: &[Node], live: &[usize]) -> usize {
    timeout(Duration::from_secs(15), async {
        loop {
            for &i in live {
                let m = nodes[i].raft().metrics().borrow().clone();
                if m.current_leader == Some(nodes[i].id)
                    && matches!(m.state, openraft::ServerState::Leader)
                {
                    return i;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a leader should be elected")
}

/// Acquire a lock on `ticket_id` for `user` via whichever live node is currently leader.
async fn acquire_one(nodes: &[Node], live: &[usize], ticket_id: u64, user: Uuid) {
    let result = timeout(Duration::from_secs(15), async {
        loop {
            let mut leader = None;
            for &i in live {
                let m = nodes[i].raft().metrics().borrow().clone();
                if m.current_leader == Some(nodes[i].id)
                    && matches!(m.state, openraft::ServerState::Leader)
                {
                    leader = Some(i);
                    break;
                }
            }
            if let Some(i) = leader {
                let cmd = LockCommand::AcquireLock {
                    ticket_id,
                    user_id: user,
                    at_unix: 1_000,
                    ttl_secs: 0,
                };
                if let Ok(resp) = nodes[i].raft().client_write(cmd).await
                    && resp.data.success
                {
                    return;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "acquire of ticket {ticket_id} timed out");
}

/// Poll until a node's local storage shows `ticket_id` locked by `user`.
async fn wait_for_lock(storage: &Storage, ticket_id: u64, user: Uuid, what: &str) {
    let ok = timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(Some(info)) = storage.get_lock_info(ticket_id)
                && info.user_id == user
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        ok.is_ok(),
        "{what}: ticket {ticket_id} not locked by expected user"
    );
}

#[tokio::test]
#[ignore = "spawns a real 3-node custodian cluster on localhost ports; run with --ignored"]
async fn three_node_lock_replication_failover_and_snapshot_rejoin() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths: Vec<PathBuf> = NODE_IDS
        .iter()
        .map(|id| tmp.path().join(format!("node-{id}")))
        .collect();
    let ports = allocate_ports();

    let mut nodes = Vec::new();
    for (idx, &id) in NODE_IDS.iter().enumerate() {
        nodes.push(start_node(id, paths[idx].clone(), &ports).await);
    }

    // Node 1 bootstraps the 3-member cluster (retry until peers are reachable).
    let members: BTreeSet<u64> = NODE_IDS.into_iter().collect();
    timeout(Duration::from_secs(15), async {
        loop {
            if nodes[0].raft().initialize(members.clone()).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("cluster initialize");

    let all = [0usize, 1, 2];
    let _ = wait_for_leader(&nodes, &all).await;

    // 1+2. Acquire locks on tickets 1..=3; every node must replicate them.
    let user = Uuid::new_v4();
    for ticket in 1..=3u64 {
        acquire_one(&nodes, &all, ticket, user).await;
    }
    for node in &nodes {
        wait_for_lock(
            node.storage(),
            1,
            user,
            &format!("node {} replication", node.id),
        )
        .await;
        wait_for_lock(
            node.storage(),
            3,
            user,
            &format!("node {} replication", node.id),
        )
        .await;
    }

    // 3. Leader failover: kill the leader; the other two must elect a new leader and commit.
    let leader_idx = wait_for_leader(&nodes, &all).await;
    nodes[leader_idx].stop().await;
    let live: Vec<usize> = (0..3).filter(|&i| i != leader_idx).collect();
    let _ = wait_for_leader(&nodes, &live).await;

    acquire_one(&nodes, &live, 4, user).await;
    for &i in &live {
        wait_for_lock(
            nodes[i].storage(),
            4,
            user,
            &format!("node {} post-failover", nodes[i].id),
        )
        .await;
    }

    // 4. Snapshot recovery: snapshot + purge on the new leader, then restart the old leader.
    // Its needed logs are gone, so it must catch up via InstallSnapshot.
    let new_leader = wait_for_leader(&nodes, &live).await;
    let applied_index = nodes[new_leader]
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .map_or(0, |log_id| log_id.index);
    nodes[new_leader]
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("trigger snapshot");
    timeout(Duration::from_secs(15), async {
        loop {
            let snap = nodes[new_leader].raft().metrics().borrow().snapshot;
            if snap.is_some_and(|s| s.index >= applied_index) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("snapshot should be built");
    nodes[new_leader]
        .raft()
        .trigger()
        .purge_log(applied_index)
        .await
        .expect("purge log");

    let rejoin_id = nodes[leader_idx].id;
    let rejoin_path = nodes[leader_idx].path.clone();
    nodes[leader_idx] = start_node(rejoin_id, rejoin_path, &ports).await;
    // The restarted node must converge to all locks (1..=4) via snapshot install.
    wait_for_lock(
        nodes[leader_idx].storage(),
        4,
        user,
        &format!("node {rejoin_id} snapshot-based rejoin"),
    )
    .await;

    for node in &mut nodes {
        node.stop().await;
    }
}
