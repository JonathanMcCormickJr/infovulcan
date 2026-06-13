//! Network layer for inter-node communication via gRPC
//!
//! This module implements the `RaftNetwork` trait from openraft to provide
//! inter-node RPC communication for distributed Raft consensus.

use crate::raft::DbTypeConfig;
use openraft::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Network client for communicating with a specific Raft peer
pub struct DbNetwork {
    _target: u64,
    address: String,
    client: Option<
        crate::server::db::raft_service_client::RaftServiceClient<tonic::transport::Channel>,
    >,
}

/// Factory for creating network clients
pub struct DbNetworkFactory {
    peers: Arc<HashMap<u64, String>>,
}

impl Default for DbNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DbNetworkFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            peers: Arc::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_peers(peers: HashMap<u64, String>) -> Self {
        Self {
            peers: Arc::new(peers),
        }
    }

    pub fn add_node(&mut self, node_id: u64, address: String) {
        let mut peers = (*self.peers).clone();
        peers.insert(node_id, address);
        self.peers = Arc::new(peers);
    }

    fn get_peer_address(&self, node_id: u64) -> Option<String> {
        self.peers.get(&node_id).cloned()
    }
}

/// Implementation of openraft `RaftNetworkFactory`
impl RaftNetworkFactory<DbTypeConfig> for DbNetworkFactory {
    type Network = DbNetwork;

    fn new_client(
        &mut self,
        target: u64,
        node: &openraft::BasicNode,
    ) -> impl std::future::Future<Output = Self::Network> + Send {
        let address = self
            .get_peer_address(target)
            .unwrap_or_else(|| format!("http://{}:8080", node.addr));

        async move {
            // For now, create without connecting - connect on first use
            DbNetwork {
                _target: target,
                address,
                client: None,
            }
        }
    }
}

impl DbNetwork {
    async fn get_client(
        &mut self,
    ) -> Result<
        &mut crate::server::db::raft_service_client::RaftServiceClient<tonic::transport::Channel>,
        openraft::error::NetworkError,
    > {
        if self.client.is_none() {
            // Apply client mTLS if configured (otherwise plaintext); `proto::tls` rewrites
            // http:// -> https:// when TLS is on.
            let channel = proto::tls::connect(&self.address).await.map_err(|e| {
                openraft::error::NetworkError::new(&std::io::Error::other(e.to_string()))
            })?;
            self.client =
                Some(crate::server::db::raft_service_client::RaftServiceClient::new(channel));
        }
        // Client should be initialized above, but handle gracefully
        self.client.as_mut().ok_or_else(|| {
            openraft::error::NetworkError::new(&std::io::Error::other("client not initialized"))
        })
    }
}

impl RaftNetwork<DbTypeConfig> for DbNetwork {
    fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> impl std::future::Future<
        Output = Result<
            openraft::raft::VoteResponse<u64>,
            openraft::error::RPCError<
                u64,
                openraft::BasicNode,
                openraft::error::RaftError<u64, openraft::error::Infallible>,
            >,
        >,
    > + Send {
        let rpc = rpc.clone(); // Clone for the async block
        async move {
            let client = self
                .get_client()
                .await
                .map_err(openraft::error::RPCError::Network)?;

            // Convert to proto
            let proto_req = crate::server::db::VoteRequest {
                vote: Some(crate::server::db::ProtoVote {
                    term: rpc.vote.leader_id.term,
                    node_id: rpc.vote.leader_id.node_id,
                    committed: rpc.vote.committed,
                }),
                last_log_id: rpc.last_log_id.map(|log_id| crate::server::db::ProtoLogId {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                }),
            };

            // Call gRPC
            let response = client.vote(proto_req).await.map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
            })?;

            let resp = response.into_inner();

            // Convert back to OpenRaft types
            let proto_vote = resp.vote.ok_or_else(|| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, "missing vote"),
                ))
            })?;

            let vote_response = openraft::raft::VoteResponse {
                vote: openraft::Vote {
                    leader_id: openraft::LeaderId {
                        term: proto_vote.term,
                        node_id: proto_vote.node_id,
                    },
                    committed: proto_vote.committed,
                },
                vote_granted: resp.vote_granted,
                last_log_id: resp.last_log_id.map(|log_id| openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: log_id.term,
                        node_id: proto_vote.node_id, // Use the node_id from vote
                    },
                    index: log_id.index,
                }),
            };

            Ok(vote_response)
        }
    }

    fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<DbTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> impl std::future::Future<
        Output = Result<
            openraft::raft::AppendEntriesResponse<u64>,
            openraft::error::RPCError<
                u64,
                openraft::BasicNode,
                openraft::error::RaftError<u64, openraft::error::Infallible>,
            >,
        >,
    > + Send {
        let rpc = rpc.clone(); // Clone for the async block
        async move {
            let client = self
                .get_client()
                .await
                .map_err(openraft::error::RPCError::Network)?;

            // Convert entries to protobuf
            let proto_entries: Vec<crate::server::db::Entry> = rpc
                .entries
                .into_iter()
                .map(|entry| {
                    Ok::<_, serde_json::Error>(crate::server::db::Entry {
                        data: serde_json::to_vec(&entry)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
                })?;

            // Convert to proto
            let proto_req = crate::server::db::AppendEntriesRequest {
                vote: Some(crate::server::db::ProtoVote {
                    term: rpc.vote.leader_id.term,
                    node_id: rpc.vote.leader_id.node_id,
                    committed: rpc.vote.committed,
                }),
                prev_log_id: rpc.prev_log_id.map(|log_id| crate::server::db::ProtoLogId {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                }),
                entries: proto_entries,
                leader_commit: rpc
                    .leader_commit
                    .map(|log_id| crate::server::db::ProtoLogId {
                        term: log_id.leader_id.term,
                        index: log_id.index,
                    }),
            };

            // Call gRPC
            let response = client.append_entries(proto_req).await.map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
            })?;

            let resp = response.into_inner();

            // Convert back to OpenRaft types
            let proto_vote = resp.vote.ok_or_else(|| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, "missing vote"),
                ))
            })?;

            // Decode the typed response: 0=Success, 1=PartialSuccess, 2=Conflict, 3=HigherVote.
            let append_response = match resp.response_type {
                0 => openraft::raft::AppendEntriesResponse::Success,
                1 => openraft::raft::AppendEntriesResponse::PartialSuccess(
                    resp.partial_success_index.map(|log_id| openraft::LogId {
                        leader_id: openraft::LeaderId {
                            term: log_id.term,
                            node_id: proto_vote.node_id,
                        },
                        index: log_id.index,
                    }),
                ),
                2 => openraft::raft::AppendEntriesResponse::Conflict,
                3 => openraft::raft::AppendEntriesResponse::HigherVote(openraft::Vote {
                    leader_id: openraft::LeaderId {
                        term: proto_vote.term,
                        node_id: proto_vote.node_id,
                    },
                    committed: proto_vote.committed,
                }),
                other => {
                    return Err(openraft::error::RPCError::Network(
                        openraft::error::NetworkError::new(&std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown append_entries response_type: {other}"),
                        )),
                    ));
                }
            };

            Ok(append_response)
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<u64>,
        snapshot: openraft::Snapshot<DbTypeConfig>,
        cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed> + Send + 'static,
        _opt: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::SnapshotResponse<u64>,
        openraft::error::StreamingError<DbTypeConfig, openraft::error::Fatal<u64>>,
    > {
        // Pin the cancel future so we can poll it in tokio::select!
        let cancel = std::pin::pin!(cancel);

        // Extract snapshot data
        let data = snapshot.snapshot.into_inner();

        // Build the proto request (same structure as install_snapshot)
        let proto_req = crate::server::db::InstallSnapshotRequest {
            vote: Some(crate::server::db::ProtoVote {
                term: vote.leader_id.term,
                node_id: vote.leader_id.node_id,
                committed: vote.committed,
            }),
            meta: Some(crate::server::db::SnapshotMeta {
                last_log_id: snapshot.meta.last_log_id.map(|log_id| {
                    crate::server::db::ProtoLogId {
                        term: log_id.leader_id.term,
                        index: log_id.index,
                    }
                }),
                last_applied: snapshot.meta.last_log_id.map_or(0, |log_id| log_id.index),
                last_membership: u32::try_from(
                    snapshot
                        .meta
                        .last_membership
                        .log_id()
                        .map_or(0, |log_id| log_id.index),
                )
                .unwrap_or(u32::MAX),
                snapshot_id: snapshot.meta.snapshot_id,
            }),
            offset: 0,
            data,
            done: true,
        };

        // Race the gRPC call against the cancellation future
        tokio::select! {
            closed = cancel => {
                Err(openraft::error::StreamingError::Closed(closed))
            }
            client_result = self.get_client() => {
                let client = client_result.map_err(|e| {
                    openraft::error::StreamingError::Network(e)
                })?;

                let response = client.install_snapshot(proto_req).await.map_err(|e| {
                    openraft::error::StreamingError::Network(
                        openraft::error::NetworkError::new(&e),
                    )
                })?;

                let resp = response.into_inner();

                let proto_vote = resp.vote.ok_or_else(|| {
                    openraft::error::StreamingError::Network(
                        openraft::error::NetworkError::new(&std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "missing vote in snapshot response",
                        )),
                    )
                })?;

                let response_vote = openraft::Vote {
                    leader_id: openraft::LeaderId {
                        term: proto_vote.term,
                        node_id: proto_vote.node_id,
                    },
                    committed: proto_vote.committed,
                };

                Ok(openraft::raft::SnapshotResponse::new(response_vote))
            }
        }
    }

    fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<DbTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> impl std::future::Future<
        Output = Result<
            openraft::raft::InstallSnapshotResponse<u64>,
            openraft::error::RPCError<
                u64,
                openraft::BasicNode,
                openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
            >,
        >,
    > + Send {
        let rpc = rpc.clone(); // Clone for the async block
        async move {
            let client = self
                .get_client()
                .await
                .map_err(openraft::error::RPCError::Network)?;

            // Convert to proto
            let proto_req = crate::server::db::InstallSnapshotRequest {
                vote: Some(crate::server::db::ProtoVote {
                    term: rpc.vote.leader_id.term,
                    node_id: rpc.vote.leader_id.node_id,
                    committed: rpc.vote.committed,
                }),
                meta: Some(crate::server::db::SnapshotMeta {
                    last_log_id: rpc
                        .meta
                        .last_log_id
                        .map(|log_id| crate::server::db::ProtoLogId {
                            term: log_id.leader_id.term,
                            index: log_id.index,
                        }),
                    last_applied: rpc.meta.last_log_id.map_or(0, |log_id| log_id.index),
                    last_membership: u32::try_from(
                        rpc.meta
                            .last_membership
                            .log_id()
                            .map_or(0, |log_id| log_id.index),
                    )
                    .unwrap_or(u32::MAX),
                    snapshot_id: rpc.meta.snapshot_id,
                }),
                offset: rpc.offset,
                data: rpc.data,
                done: rpc.done,
            };

            // Call gRPC
            let response = client.install_snapshot(proto_req).await.map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
            })?;

            let resp = response.into_inner();

            // Convert back to OpenRaft types
            let proto_vote = resp.vote.ok_or_else(|| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, "missing vote"),
                ))
            })?;

            let install_response = openraft::raft::InstallSnapshotResponse {
                vote: openraft::Vote {
                    leader_id: openraft::LeaderId {
                        term: proto_vote.term,
                        node_id: proto_vote.node_id,
                    },
                    committed: proto_vote.committed,
                },
            };

            Ok(install_response)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::raft_service_server::RaftService;
    use crate::server::db::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, ProtoVote, VoteRequest, VoteResponse,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::task;
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct TestRaftSvc {
        missing_vote: Arc<RwLock<bool>>,
        /// `AppendEntries` `response_type` to return (0=Success, 1=PartialSuccess, 2=Conflict,
        /// 3=HigherVote, anything else exercises the unknown-type decode error path).
        append_response_type: Arc<RwLock<u32>>,
    }

    impl TestRaftSvc {
        fn new() -> Self {
            Self {
                missing_vote: Arc::new(RwLock::new(false)),
                append_response_type: Arc::new(RwLock::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl RaftService for TestRaftSvc {
        async fn vote(
            &self,
            request: Request<VoteRequest>,
        ) -> Result<Response<VoteResponse>, Status> {
            let req = request.into_inner();
            let missing_vote = *self.missing_vote.read().await;

            Ok(Response::new(VoteResponse {
                vote: if missing_vote {
                    None
                } else {
                    Some(ProtoVote {
                        term: req.vote.as_ref().map_or(1, |v| v.term),
                        node_id: req.vote.as_ref().map_or(1, |v| v.node_id),
                        committed: true,
                    })
                },
                vote_granted: true,
                last_log_id: None,
            }))
        }

        async fn append_entries(
            &self,
            request: Request<AppendEntriesRequest>,
        ) -> Result<Response<AppendEntriesResponse>, Status> {
            let req = request.into_inner();
            let missing_vote = *self.missing_vote.read().await;
            let response_type = *self.append_response_type.read().await;

            Ok(Response::new(AppendEntriesResponse {
                vote: if missing_vote {
                    None
                } else {
                    Some(ProtoVote {
                        term: req.vote.as_ref().map_or(1, |v| v.term),
                        node_id: req.vote.as_ref().map_or(1, |v| v.node_id),
                        committed: true,
                    })
                },
                response_type,
                // PartialSuccess (type 1) carries an index; other types leave it None.
                partial_success_index: if response_type == 1 {
                    Some(crate::server::db::ProtoLogId { term: 1, index: 7 })
                } else {
                    None
                },
            }))
        }

        async fn install_snapshot(
            &self,
            request: Request<InstallSnapshotRequest>,
        ) -> Result<Response<InstallSnapshotResponse>, Status> {
            let req = request.into_inner();
            let missing_vote = *self.missing_vote.read().await;

            Ok(Response::new(InstallSnapshotResponse {
                vote: if missing_vote {
                    None
                } else {
                    Some(ProtoVote {
                        term: req.vote.as_ref().map_or(1, |v| v.term),
                        node_id: req.vote.as_ref().map_or(1, |v| v.node_id),
                        committed: true,
                    })
                },
            }))
        }
    }

    fn start_test_server(addr: &str, svc: TestRaftSvc) -> tokio::task::JoinHandle<()> {
        let socket_addr = addr.parse().expect("parse test address");
        let service = crate::server::db::raft_service_server::RaftServiceServer::new(svc);

        task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve(socket_addr)
                .await
                .expect("serve raft test server");
        })
    }

    #[test]
    fn test_network_factory_creation() {
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50051".to_string());
        let factory = DbNetworkFactory::with_peers(peers);

        assert!(factory.get_peer_address(1).is_some());
        assert!(factory.get_peer_address(99).is_none());
    }

    #[test]
    fn test_network_factory_add_node() {
        let mut factory = DbNetworkFactory::new();
        assert!(factory.get_peer_address(42).is_none());

        factory.add_node(42, "http://127.0.0.1:50042".to_string());
        assert_eq!(
            factory.get_peer_address(42).as_deref(),
            Some("http://127.0.0.1:50042")
        );

        // Overwrite existing node
        factory.add_node(42, "http://127.0.0.1:50043".to_string());
        assert_eq!(
            factory.get_peer_address(42).as_deref(),
            Some("http://127.0.0.1:50043")
        );
    }

    #[test]
    fn test_network_factory_default_is_empty() {
        let factory = DbNetworkFactory::default();
        assert!(factory.get_peer_address(1).is_none());
    }

    #[tokio::test]
    async fn test_new_client_falls_back_to_node_addr() {
        // When the node_id is not in the peers map, the factory should fall back
        // to the node's addr field.
        let factory = DbNetworkFactory::new();
        // Use a mutable reference for `new_client`.
        let mut factory = factory;
        let node = openraft::BasicNode {
            addr: "127.0.0.1:19999".to_string(),
        };
        // Should use node.addr since node_id 99 is not in peers
        let _client = factory.new_client(99, &node).await;
        // Just verify no panic; we can't easily inspect the address field.
    }

    #[tokio::test]
    async fn test_full_snapshot_when_server_running_returns_snapshot_response() {
        let svc = TestRaftSvc::new();
        let server = start_test_server("127.0.0.1:50070", svc);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50070".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let vote = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 2,
                node_id: 1,
            },
            committed: true,
        };

        let snapshot_data = b"full snapshot data".to_vec();

        // cancel future that never resolves (no cancellation)
        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let result = client
            .full_snapshot(
                vote,
                openraft::Snapshot {
                    meta: openraft::SnapshotMeta::default(),
                    snapshot: Box::new(std::io::Cursor::new(snapshot_data)),
                },
                async move {
                    let _ = cancel_rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(Duration::from_secs(5)),
            )
            .await;

        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.vote.leader_id.term, 2);
        assert_eq!(resp.vote.leader_id.node_id, 1);

        server.abort();
    }

    #[tokio::test]
    async fn test_full_snapshot_when_no_server_returns_network_error() {
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:59997".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let result = client
            .full_snapshot(
                openraft::Vote::default(),
                openraft::Snapshot {
                    meta: openraft::SnapshotMeta::default(),
                    snapshot: Box::new(std::io::Cursor::new(vec![1, 2, 3])),
                },
                async move {
                    let _ = cancel_rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(Duration::from_secs(1)),
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_full_snapshot_when_missing_vote_returns_error() {
        let svc = TestRaftSvc::new();
        *svc.missing_vote.write().await = true;
        let server = start_test_server("127.0.0.1:50071", svc);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50071".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let result = client
            .full_snapshot(
                openraft::Vote::default(),
                openraft::Snapshot {
                    meta: openraft::SnapshotMeta::default(),
                    snapshot: Box::new(std::io::Cursor::new(vec![])),
                },
                async move {
                    let _ = cancel_rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(Duration::from_secs(5)),
            )
            .await;

        assert!(result.is_err());

        server.abort();
    }

    #[tokio::test]
    async fn test_full_snapshot_when_cancelled_returns_closed_error() {
        // Use a port that will take time to connect (no server), but cancel fires first
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50072".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        // Cancel fires immediately
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        drop(cancel_tx);

        let result = client
            .full_snapshot(
                openraft::Vote::default(),
                openraft::Snapshot {
                    meta: openraft::SnapshotMeta::default(),
                    snapshot: Box::new(std::io::Cursor::new(vec![])),
                },
                async move {
                    let _ = cancel_rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(Duration::from_secs(1)),
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_network_vote_append_and_install_snapshot_success() {
        let svc = TestRaftSvc::new();
        let server = start_test_server("127.0.0.1:50061", svc);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50061".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let vote_req = openraft::raft::VoteRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            last_log_id: None,
        };
        assert!(
            client
                .vote(
                    vote_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1))
                )
                .await
                .is_ok()
        );

        let append_req = openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        assert!(
            client
                .append_entries(
                    append_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .is_ok()
        );

        let snapshot_req = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            meta: openraft::SnapshotMeta::default(),
            offset: 0,
            data: b"chunk".to_vec(),
            done: true,
        };
        assert!(
            client
                .install_snapshot(
                    snapshot_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .is_ok()
        );

        server.abort();
    }

    #[tokio::test]
    async fn test_append_entries_decodes_all_response_variants() {
        // Fault-injection: the peer returns each non-Success response_type so the client-side
        // decode of PartialSuccess / Conflict / HigherVote / unknown is exercised.
        let svc = TestRaftSvc::new();
        let server = start_test_server("127.0.0.1:50063", svc.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50063".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let make_req = || openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let opt = || openraft::network::RPCOption::new(Duration::from_secs(1));

        // 1 = PartialSuccess(Some(log_id))
        *svc.append_response_type.write().await = 1;
        let r = client.append_entries(make_req(), opt()).await.unwrap();
        assert!(matches!(
            r,
            openraft::raft::AppendEntriesResponse::PartialSuccess(Some(_))
        ));

        // 2 = Conflict
        *svc.append_response_type.write().await = 2;
        let r = client.append_entries(make_req(), opt()).await.unwrap();
        assert!(matches!(r, openraft::raft::AppendEntriesResponse::Conflict));

        // 3 = HigherVote
        *svc.append_response_type.write().await = 3;
        let r = client.append_entries(make_req(), opt()).await.unwrap();
        assert!(matches!(
            r,
            openraft::raft::AppendEntriesResponse::HigherVote(_)
        ));

        // anything else = decode error
        *svc.append_response_type.write().await = 99;
        assert!(client.append_entries(make_req(), opt()).await.is_err());

        server.abort();
    }

    #[tokio::test]
    async fn test_network_rpc_reports_missing_vote_errors() {
        let svc = TestRaftSvc::new();
        *svc.missing_vote.write().await = true;
        let server = start_test_server("127.0.0.1:50062", svc);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50062".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let vote_req = openraft::raft::VoteRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            last_log_id: None,
        };
        assert!(
            client
                .vote(
                    vote_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1))
                )
                .await
                .is_err()
        );

        let append_req = openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        assert!(
            client
                .append_entries(
                    append_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .is_err()
        );

        let snapshot_req = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            meta: openraft::SnapshotMeta::default(),
            offset: 0,
            data: b"chunk".to_vec(),
            done: true,
        };
        assert!(
            client
                .install_snapshot(
                    snapshot_req,
                    openraft::network::RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .is_err()
        );

        server.abort();
    }

    #[tokio::test]
    async fn test_network_rpc_reports_transport_errors() {
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:59999".to_string());
        let mut factory = DbNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let vote_req = openraft::raft::VoteRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            last_log_id: None,
        };

        assert!(
            client
                .vote(
                    vote_req,
                    openraft::network::RPCOption::new(Duration::from_millis(100))
                )
                .await
                .is_err()
        );
    }
}
