//! Single-node smoke test.
//!
//! Walks the minimum lifecycle: spawn one node, initialize it as a
//! single-voter cluster, propose a handful of writes through the leader,
//! and assert they land in the state machine. With one node there's no
//! replication and no election — this is just "does the engine plumbing
//! work end-to-end".

use std::collections::BTreeMap;
use std::time::Duration;

use spike_raft_toy::{
    Cluster, make_request, raft_config, read_status, spawn_node, wait_for_metrics,
};

#[tokio::test]
async fn single_node_applies_writes() -> anyhow::Result<()> {
    let cluster = Cluster::new();
    let (raft, store) = spawn_node(1, &cluster, raft_config()).await?;

    // A node has to be told it forms a cluster (even of one) before it'll
    // accept writes — Raft requires explicit membership, not auto-discovery.
    raft.initialize(BTreeMap::from([(1, ())])).await?;

    // Wait for self-election. Even single-node clusters go through the
    // candidate → leader transition before they'll accept client writes.
    let metrics = wait_for_metrics(&raft, Duration::from_secs(2), |m| {
        m.current_leader == Some(1)
    })
    .await
    .expect("self-election should complete");
    assert_eq!(metrics.current_leader, Some(1));

    for (serial, (client, status)) in [
        ("alice", "active"),
        ("bob", "pending"),
        ("alice", "closed"), // overwrites the first write — last-write-wins
    ]
    .into_iter()
    .enumerate()
    {
        raft.client_write(make_request(client, serial as u64, status))
            .await?;
    }

    let snapshot = read_status(&store).await;
    assert_eq!(snapshot.get("alice").map(String::as_str), Some("closed"));
    assert_eq!(snapshot.get("bob").map(String::as_str), Some("pending"));

    raft.shutdown().await?;
    Ok(())
}
