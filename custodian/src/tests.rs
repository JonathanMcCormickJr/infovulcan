#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::{
        build_network_factory_and_peers, build_raft_node, initialize_cluster_if_leader,
        load_or_generate_keys, make_raft_config, maybe_connect_db, parse_listen_addr,
        parse_peer_configs, should_initialize_cluster,
    };
    use custodian::LockCommand;
    use custodian::storage::Storage;
    use uuid::Uuid;

    #[test]
    fn test_lock_operations() {
        let storage = Storage::new_temp().unwrap();
        let ticket_id = 42;
        let user_id = Uuid::new_v4();

        // Initially not locked
        assert!(!storage.is_locked(ticket_id).unwrap());

        // Acquire lock
        storage.acquire_lock(ticket_id, user_id).unwrap();
        assert!(storage.is_locked(ticket_id).unwrap());

        // Get lock info
        let lock_info = storage.get_lock_info(ticket_id).unwrap().unwrap();
        assert_eq!(lock_info.ticket_id, ticket_id);
        assert_eq!(lock_info.user_id, user_id);

        // Release lock
        storage.release_lock(ticket_id).unwrap();
        assert!(!storage.is_locked(ticket_id).unwrap());
    }

    #[test]
    fn test_get_all_locks() {
        let storage = Storage::new_temp().unwrap();
        let user_id = Uuid::new_v4();

        // Add multiple locks
        storage.acquire_lock(1, user_id).unwrap();
        storage.acquire_lock(2, user_id).unwrap();

        let locks = storage.get_all_locks().unwrap();
        assert_eq!(locks.len(), 2);
        assert!(locks.contains_key(&1));
        assert!(locks.contains_key(&2));
    }

    #[test]
    fn test_lock_command_apply() {
        let storage = Storage::new_temp().unwrap();
        let ticket_id = 123;
        let user_id = Uuid::new_v4();

        // Test acquire lock command
        let acquire_cmd = LockCommand::AcquireLock {
            ticket_id,
            user_id,
            at_unix: 0,
            ttl_secs: 0,
        };
        acquire_cmd.apply(&storage).unwrap();
        assert!(storage.is_locked(ticket_id).unwrap());

        // Test release lock command
        let release_cmd = LockCommand::ReleaseLock { ticket_id, user_id };
        release_cmd.apply(&storage).unwrap();
        assert!(!storage.is_locked(ticket_id).unwrap());
    }

    #[test]
    fn test_parse_peer_configs() {
        let peers = parse_peer_configs(
            "1:http://127.0.0.1:8081,2:http://127.0.0.1:8082,invalid,0:http://x",
        );
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[1].node_id, 2);
    }

    #[test]
    fn test_should_initialize_cluster() {
        assert!(should_initialize_cluster(1, &[1, 2, 3]));
        assert!(!should_initialize_cluster(2, &[1, 2, 3]));
        assert!(!should_initialize_cluster(1, &[]));
    }

    // ── parse_peer_configs edge cases ─────────────────────────────────────────

    #[test]
    fn parse_peer_configs_empty_string_returns_empty() {
        assert!(parse_peer_configs("").is_empty());
    }

    #[test]
    fn parse_peer_configs_single_entry() {
        let peers = parse_peer_configs("2:http://10.0.0.2:8081");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, 2);
        assert_eq!(peers[0].address, "http://10.0.0.2:8081");
    }

    #[test]
    fn parse_peer_configs_preserves_ipv6_colons() {
        let peers = parse_peer_configs("1:[::1]:8081");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].address, "[::1]:8081");
    }

    // ── should_initialize_cluster edge cases ──────────────────────────────────

    #[test]
    fn should_initialize_cluster_true_for_sole_peer() {
        assert!(should_initialize_cluster(5, &[5]));
    }

    #[test]
    fn should_initialize_cluster_false_when_not_first() {
        assert!(!should_initialize_cluster(2, &[1]));
    }

    // ── parse_listen_addr ─────────────────────────────────────────────────────

    #[test]
    fn parse_listen_addr_accepts_valid_ipv4() {
        let addr = parse_listen_addr("0.0.0.0:8081").expect("valid IPv4 should parse");
        assert_eq!(addr.port(), 8081);
    }

    #[test]
    fn parse_listen_addr_accepts_valid_ipv6() {
        let addr = parse_listen_addr("[::1]:8081").expect("valid IPv6 should parse");
        assert_eq!(addr.port(), 8081);
    }

    #[test]
    fn parse_listen_addr_rejects_invalid_string() {
        assert!(parse_listen_addr("not-valid").is_err());
    }

    // ── make_raft_config ──────────────────────────────────────────────────────

    #[test]
    fn make_raft_config_returns_valid_config() {
        let config = make_raft_config().expect("config should be valid");
        assert_eq!(config.heartbeat_interval, 500);
        assert_eq!(config.election_timeout_min, 1500);
        assert_eq!(config.election_timeout_max, 3000);
    }

    // ── build_network_factory_and_peers ───────────────────────────────────────

    #[test]
    fn build_network_factory_and_peers_populates_peers() {
        let (_, peers) =
            build_network_factory_and_peers("1:http://127.0.0.1:8081,2:http://127.0.0.1:8082");
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[1].node_id, 2);
    }

    #[test]
    fn build_network_factory_and_peers_empty_returns_empty() {
        let (_, peers) = build_network_factory_and_peers("");
        assert!(peers.is_empty());
    }

    // ── load_or_generate_keys ─────────────────────────────────────────────────

    #[test]
    fn load_or_generate_keys_creates_file_when_missing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let keys_path = dir.path().join("keys.bin");
        assert!(!keys_path.exists());
        let (pk, sk) = load_or_generate_keys(&keys_path).expect("should generate keys");
        assert!(!pk.is_empty());
        assert!(!sk.is_empty());
        assert!(keys_path.exists(), "key file should be created");
    }

    #[test]
    fn load_or_generate_keys_round_trips() {
        let dir = tempfile::tempdir().expect("temp dir");
        let keys_path = dir.path().join("keys.bin");
        let (pk1, sk1) = load_or_generate_keys(&keys_path).expect("first generate");
        let (pk2, sk2) = load_or_generate_keys(&keys_path).expect("second load");
        assert_eq!(pk1, pk2);
        assert_eq!(sk1, sk2);
    }

    // ── build_raft_node ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn build_raft_node_creates_store_and_raft_instance() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().to_str().expect("utf-8 path").to_string();
        let config = make_raft_config().expect("config");
        let (factory, _) = build_network_factory_and_peers("1:http://127.0.0.1:50090");
        let result = build_raft_node(1, config, factory, &path).await;
        assert!(result.is_ok());
    }

    // ── initialize_cluster_if_leader ───────────────────────────────────────────────

    #[tokio::test]
    async fn initialize_cluster_noop_when_not_first_peer() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().to_str().expect("utf-8 path").to_string();
        let config = make_raft_config().expect("config");
        let (factory, _) = build_network_factory_and_peers("1:http://127.0.0.1:50091");
        let (raft, _) = build_raft_node(1, config, factory, &path)
            .await
            .expect("raft node");
        // node_id=2 is not first in [1,2,3] → noop
        initialize_cluster_if_leader(&raft, 2, &[1, 2, 3]).await;
    }

    #[tokio::test]
    async fn initialize_cluster_succeeds_for_sole_member() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().to_str().expect("utf-8 path").to_string();
        let config = make_raft_config().expect("config");
        let (factory, _) = build_network_factory_and_peers("1:http://127.0.0.1:50092");
        let (raft, _) = build_raft_node(1, config, factory, &path)
            .await
            .expect("raft node");
        // sole member bootstraps the cluster
        initialize_cluster_if_leader(&raft, 1, &[1]).await;
    }

    // ── maybe_connect_db ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn maybe_connect_db_returns_none_when_no_endpoint() {
        let result = maybe_connect_db(None).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn maybe_connect_db_returns_none_on_connection_failure() {
        // Port 9 is the DISCARD protocol and is not expected to be open.
        let result = maybe_connect_db(Some("http://127.0.0.1:9".to_string())).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_maybe_connect_db_returns_none_without_endpoint() {
        let result = maybe_connect_db(None).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_maybe_connect_db_returns_none_on_connection_failure() {
        let result = maybe_connect_db(Some(
            "http://invalid-host-that-does-not-exist:99999".to_string(),
        ))
        .await;
        assert!(result.is_none());
    }
}
