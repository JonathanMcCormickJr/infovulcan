//! Real 3-node Raft cluster integration test.
//!
//! Unlike `multi_node_test.rs` (which uses placeholder no-ops), this spins up three
//! actual DB nodes as in-process gRPC servers wired together over the real tonic Raft
//! network, then exercises:
//!
//! 1. cluster formation + leader election,
//! 2. replication of domain writes to all followers,
//! 3. quorum tolerance: progress continues while one node is down,
//! 4. **snapshot-based recovery**: the down node is restarted after the leader has
//!    snapshotted and purged its log, so catch-up must go through `InstallSnapshot`,
//! 5. post-rejoin: the whole cluster still accepts writes that reach the rejoined node.
//!
//! Ignored by default: it binds localhost ports, spawns servers, and is timing-sensitive.
//! Run explicitly with `cargo test -p db --test three_node_cluster -- --ignored`.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use db::network::DbNetworkFactory;
use db::raft::{DbRaft, DbStore};
use db::raft_service::RaftServiceImpl;
use db::server::DatabaseService;
use db::server::db::{database_server::DatabaseServer, raft_service_server::RaftServiceServer};
use db::storage::{LogEntry, Storage, TicketIndexFields};
use openraft::storage::Adaptor;
use openraft::{Config, SnapshotPolicy};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tonic::transport::Server;

const NODE_IDS: [u64; 3] = [1, 2, 3];

fn raft_config() -> Arc<Config> {
    // Disable *automatic* snapshotting during the test (it would churn snapshot-streaming
    // between the live nodes and destabilize replication). Instead we trigger a snapshot
    // and a log purge explicitly at a controlled point, which deterministically forces the
    // rejoining node to recover via InstallSnapshot.
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

/// node_id -> bound socket address for that node's gRPC server.
type Ports = std::collections::BTreeMap<u64, std::net::SocketAddr>;

/// Allocate an OS-assigned localhost port for each node up front (bind to :0, record the
/// address, then drop the listener). Using fresh ports per run avoids collisions / TIME_WAIT
/// issues when the test runs back-to-back.
fn allocate_ports() -> Ports {
    let mut ports = Ports::new();
    for id in NODE_IDS {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        ports.insert(id, listener.local_addr().expect("local addr"));
        // listener dropped here; the port is reused by the node's server below.
    }
    ports
}

struct Node {
    id: u64,
    raft: Option<Arc<DbRaft>>,
    storage: Option<Storage>,
    path: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn raft(&self) -> &DbRaft {
        self.raft.as_ref().expect("node is running")
    }

    fn storage(&self) -> &Storage {
        self.storage.as_ref().expect("node is running")
    }

    /// Fully stop the node: shut down the gRPC server, the Raft engine, and drop the
    /// store handles so the on-disk sled database lock is released (required to reopen it).
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
        // Drop the store handles (engine + local clone) to release the sled file lock.
        self.raft = None;
        self.storage = None;
    }
}

/// Build a node from a storage path and start its gRPC server (Database + Raft).
async fn start_node(node_id: u64, path: PathBuf, ports: &Ports) -> Node {
    let mut factory = DbNetworkFactory::new();
    for (&peer, addr) in ports {
        factory.add_node(peer, format!("http://{addr}"));
    }

    // Retry the open: a just-stopped node may take a moment to release the sled lock.
    let store = {
        let path_str = path.to_str().expect("utf8 path");
        let mut attempt = 0;
        loop {
            match DbStore::new(path_str) {
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
        DbRaft::new(node_id, raft_config(), factory, log_store, state_machine)
            .await
            .expect("create raft"),
    );

    let db_service = DatabaseService::new((*raft).clone(), storage.clone());
    let raft_service = RaftServiceImpl::new(raft.clone());
    let (tx, rx) = oneshot::channel::<()>();
    let addr = ports[&node_id];
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(DatabaseServer::new(db_service))
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

/// Poll until some live node reports itself the leader; return its index in `nodes`.
async fn wait_for_leader(nodes: &[Node]) -> usize {
    timeout(Duration::from_secs(15), async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                let m = node.raft().metrics().borrow().clone();
                if m.current_leader == Some(node.id)
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

fn sample_entry(seq: u8) -> LogEntry {
    LogEntry::CreateTicket {
        body: vec![seq; 4],
        index: TicketIndexFields {
            status: 1,
            account_uuid: "acct".to_string(),
            assigned_to_uuid: None,
            project: "proj".to_string(),
            tracking_url: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        },
    }
}

/// Write one ticket via whichever live node is currently leader, retrying across
/// leadership changes. Returns once the write is committed.
async fn write_one(nodes: &[Node], live: &[usize], seq: u8) {
    let result = timeout(Duration::from_secs(15), async {
        loop {
            // Find the current leader among live nodes.
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
            if let Some(i) = leader
                && nodes[i]
                    .raft()
                    .client_write(sample_entry(seq))
                    .await
                    .is_ok()
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "write {seq} timed out");
}

/// Count non-deleted tickets currently visible in a node's local storage.
fn ticket_count(storage: &Storage) -> usize {
    storage
        .query_tickets(&db::storage::TicketQuery::default())
        .expect("query")
        .len()
}

/// Poll until a node's local storage reflects at least `expected` tickets.
async fn wait_for_count(storage: &Storage, expected: usize, what: &str) {
    let ok = timeout(Duration::from_secs(20), async {
        loop {
            if ticket_count(storage) >= expected {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        ok.is_ok(),
        "{what}: expected >= {expected} tickets, got {}",
        ticket_count(storage)
    );
}

#[tokio::test]
#[ignore = "spawns a real 3-node cluster on localhost ports; run with --ignored"]
async fn three_node_replicate_kill_and_snapshot_rejoin() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths: Vec<PathBuf> = NODE_IDS
        .iter()
        .map(|id| tmp.path().join(format!("node-{id}")))
        .collect();
    let ports = allocate_ports();

    // Start all three nodes.
    let mut nodes = Vec::new();
    for (idx, &id) in NODE_IDS.iter().enumerate() {
        nodes.push(start_node(id, paths[idx].clone(), &ports).await);
    }

    // Node 1 bootstraps the 3-member cluster.
    let members: BTreeSet<u64> = NODE_IDS.into_iter().collect();
    // Retry initialize until peers are reachable.
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

    let _leader = wait_for_leader(&nodes).await;

    // 1+2. Write a few tickets with all three up; every node must replicate them.
    let all = [0usize, 1, 2];
    for seq in 0..3u8 {
        write_one(&nodes, &all, seq).await;
    }
    for node in &nodes {
        wait_for_count(
            node.storage(),
            3,
            &format!("node {} initial replication", node.id),
        )
        .await;
    }

    // 3. Kill a follower (pick one that isn't the current leader).
    let leader_idx = wait_for_leader(&nodes).await;
    let victim = (0..3).find(|&i| i != leader_idx).expect("a follower");
    nodes[victim].stop().await;
    let live: Vec<usize> = (0..3).filter(|&i| i != victim).collect();

    // 4. Write more tickets while the victim is down (the two live nodes form a quorum).
    for seq in 3..18u8 {
        write_one(&nodes, &live, seq).await;
    }
    for &i in &live {
        wait_for_count(
            nodes[i].storage(),
            18,
            &format!("node {} quorum progress", nodes[i].id),
        )
        .await;
    }

    // Deterministically force log compaction on the leader: snapshot, then purge the log
    // up to the applied index. The victim is now far behind the purge point, so it can
    // only catch up via InstallSnapshot.
    let leader_idx = wait_for_leader(&nodes).await;
    let applied_index = nodes[leader_idx]
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .map_or(0, |log_id| log_id.index);
    nodes[leader_idx]
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("trigger snapshot");
    // Wait until the snapshot covering the applied index has been built.
    timeout(Duration::from_secs(15), async {
        loop {
            let snap = nodes[leader_idx].raft().metrics().borrow().snapshot;
            if snap.is_some_and(|s| s.index >= applied_index) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("snapshot should be built");
    nodes[leader_idx]
        .raft()
        .trigger()
        .purge_log(applied_index)
        .await
        .expect("purge log");

    // 5. Restart the victim from its on-disk storage. Its needed logs are purged on the
    //    leader, so it must catch up via InstallSnapshot. Verify convergence.
    let victim_id = nodes[victim].id;
    let victim_path = nodes[victim].path.clone();
    nodes[victim] = start_node(victim_id, victim_path, &ports).await;
    wait_for_count(
        nodes[victim].storage(),
        18,
        &format!("node {victim_id} snapshot-based rejoin"),
    )
    .await;

    // 6. After rejoin the cluster is whole again and still accepts writes that replicate
    //    to the previously-down node.
    let all = [0usize, 1, 2];
    write_one(&nodes, &all, 18).await;
    wait_for_count(
        nodes[victim].storage(),
        19,
        &format!("node {victim_id} post-rejoin replication"),
    )
    .await;

    // Clean shutdown.
    for node in &mut nodes {
        node.stop().await;
    }
}
