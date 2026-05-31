//! Three-node cluster test — the case the production `db` and `custodian`
//! services actually run in.
//!
//! Two scenarios:
//!
//! 1. **Replication.** Bring up 3 nodes, initialize them as a single
//!    cluster, write through the leader, and assert the followers' state
//!    machines converge to the same contents.
//!
//! 2. **Leader failover.** Kill the leader and confirm one of the
//!    surviving nodes wins an election — i.e. quorum (2 of 3) is enough
//!    to keep the cluster live after one failure, which is the whole
//!    point of running 3 nodes instead of 1.

use std::collections::BTreeMap;
use std::time::Duration;

use spike_raft_toy::{
    Cluster, MemStore, NodeId, ToyRaft, make_request, raft_config, read_status, spawn_node,
    wait_for_metrics,
};
use std::sync::Arc;

async fn boot_three_node_cluster() -> anyhow::Result<Vec<(NodeId, ToyRaft, Arc<MemStore>)>> {
    let cluster = Cluster::new();
    let config = raft_config();

    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let (raft, store) = spawn_node(id, &cluster, config.clone()).await?;
        nodes.push((id, raft, store));
    }

    // Any node can initialize — Raft will gossip membership through the
    // first AppendEntries. Pick node 1 by convention.
    nodes[0]
        .1
        .initialize(BTreeMap::from([(1, ()), (2, ()), (3, ())]))
        .await?;

    // Wait until *some* node sees a leader. Don't pin it to node 1: an
    // unlucky early-election timeout could elect someone else.
    let metrics = wait_for_metrics(&nodes[0].1, Duration::from_secs(3), |m| {
        m.current_leader.is_some()
    })
    .await
    .expect("initial leader election should complete");
    assert!(metrics.current_leader.is_some());

    Ok(nodes)
}

async fn find_leader(nodes: &[(NodeId, ToyRaft, Arc<MemStore>)]) -> Option<NodeId> {
    nodes[0].1.metrics().borrow().current_leader
}

#[tokio::test]
async fn replicates_writes_to_all_followers() -> anyhow::Result<()> {
    let nodes = boot_three_node_cluster().await?;
    let leader_id = find_leader(&nodes).await.expect("leader exists");
    let leader = &nodes
        .iter()
        .find(|(id, _, _)| *id == leader_id)
        .expect("leader is one of the spawned nodes")
        .1;

    for (serial, (client, status)) in [("alice", "active"), ("bob", "pending"), ("carol", "vip")]
        .into_iter()
        .enumerate()
    {
        leader
            .client_write(make_request(client, serial as u64, status))
            .await?;
    }

    // Each node should converge — the leader has already applied (it's how
    // client_write returns), and followers should catch up within a few
    // heartbeats.
    for (id, raft, store) in &nodes {
        let _ = wait_for_metrics(raft, Duration::from_secs(2), |m| {
            m.last_applied.map(|l| l.index) >= Some(3)
        })
        .await
        .unwrap_or_else(|| panic!("node {id} did not apply all entries"));

        let snapshot = read_status(store).await;
        assert_eq!(
            snapshot.get("alice").map(String::as_str),
            Some("active"),
            "node {id} missing alice"
        );
        assert_eq!(
            snapshot.get("bob").map(String::as_str),
            Some("pending"),
            "node {id} missing bob"
        );
        assert_eq!(
            snapshot.get("carol").map(String::as_str),
            Some("vip"),
            "node {id} missing carol"
        );
    }

    Ok(())
}

#[tokio::test]
async fn elects_new_leader_after_failover() -> anyhow::Result<()> {
    let nodes = boot_three_node_cluster().await?;
    let original_leader = find_leader(&nodes).await.expect("leader exists");

    // Take the leader offline. shutdown() returns once the engine task has
    // exited; after that, any RPC targeting it must fail at the network.
    let leader_raft = nodes
        .iter()
        .find(|(id, _, _)| *id == original_leader)
        .map(|(_, raft, _)| raft.clone())
        .expect("leader is in the cluster");
    leader_raft.shutdown().await?;

    // Watch any surviving node's metrics until it reports a *different*
    // leader (or itself as leader). One election cycle after the heartbeat
    // gap is usually enough.
    let survivor = nodes
        .iter()
        .find(|(id, _, _)| *id != original_leader)
        .expect("at least one survivor");
    let metrics = wait_for_metrics(
        &survivor.1,
        Duration::from_secs(5),
        |m| matches!(m.current_leader, Some(id) if id != original_leader),
    )
    .await
    .expect("a new leader must be elected within 5s");

    let new_leader = metrics.current_leader.expect("leader is set");
    assert_ne!(
        new_leader, original_leader,
        "a stale view of the dead leader is not a failover"
    );

    // Quorum survived — the new leader must accept writes from clients.
    let new_leader_raft = &nodes
        .iter()
        .find(|(id, _, _)| *id == new_leader)
        .expect("new leader is a known node")
        .1;
    new_leader_raft
        .client_write(make_request("dave", 99, "post-failover"))
        .await?;

    Ok(())
}
