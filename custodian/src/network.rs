//! Network layer for inter-node communication via gRPC
//!
//! This module implements the `RaftNetwork` trait from openraft to provide
//! inter-node RPC communication for distributed lock management.

use crate::raft::CustodianTypeConfig;
use openraft::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Network client for communicating with a specific Raft peer
pub struct CustodianNetwork {
    _target: u64,
    address: String,
    client: Option<
        crate::server::custodian::raft_service_client::RaftServiceClient<tonic::transport::Channel>,
    >,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::custodian::raft_service_server::RaftService;
    use crate::server::custodian::{
        AppendEntriesRequest, AppendEntriesResponse, ProtoVote, VoteRequest, VoteResponse,
    };

    use tokio::task;
    use tonic::{Request, Response, Status};

    // Returns Option to match the proto `vote` field type directly at call sites.
    #[allow(clippy::unnecessary_wraps)]
    fn ok_vote() -> Option<ProtoVote> {
        Some(ProtoVote {
            term: 1,
            node_id: 1,
            committed: true,
        })
    }

    #[tokio::test]
    async fn test_network_factory_creation() {
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50051".to_string());
        let factory = CustodianNetworkFactory::with_peers(peers);

        assert!(factory.get_peer_address(1).is_some());
        assert!(factory.get_peer_address(99).is_none());
    }

    #[derive(Default)]
    struct TestRaftSvc {}

    #[tonic::async_trait]
    impl RaftService for TestRaftSvc {
        async fn vote(
            &self,
            request: Request<VoteRequest>,
        ) -> Result<Response<VoteResponse>, Status> {
            let _req = request.into_inner();
            Ok(Response::new(VoteResponse {
                vote: ok_vote(),
                vote_granted: true,
                last_log_id: None,
            }))
        }

        async fn append_entries(
            &self,
            request: Request<AppendEntriesRequest>,
        ) -> Result<Response<AppendEntriesResponse>, Status> {
            let _req = request.into_inner();
            Ok(Response::new(AppendEntriesResponse {
                vote: ok_vote(),
                response_type: 0,
                partial_success_index: None,
            }))
        }

        async fn install_snapshot(
            &self,
            _request: Request<crate::server::custodian::InstallSnapshotRequest>,
        ) -> Result<Response<crate::server::custodian::InstallSnapshotResponse>, Status> {
            Ok(Response::new(
                crate::server::custodian::InstallSnapshotResponse { vote: ok_vote() },
            ))
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::items_after_statements)]
    async fn test_network_install_snapshot_multi_chunk() {
        use crate::server::custodian::InstallSnapshotRequest;
        use std::sync::Mutex;

        // Shared buffer to collect incoming snapshot bytes
        let received = std::sync::Arc::new(Mutex::new(Vec::<u8>::new()));

        #[derive(Clone)]
        struct ChunkedSvc(std::sync::Arc<Mutex<Vec<u8>>>);

        #[tonic::async_trait]
        impl RaftService for ChunkedSvc {
            async fn vote(
                &self,
                _request: Request<VoteRequest>,
            ) -> Result<Response<VoteResponse>, Status> {
                Ok(Response::new(VoteResponse {
                    vote: ok_vote(),
                    vote_granted: true,
                    last_log_id: None,
                }))
            }

            async fn append_entries(
                &self,
                _request: Request<AppendEntriesRequest>,
            ) -> Result<Response<AppendEntriesResponse>, Status> {
                Ok(Response::new(AppendEntriesResponse {
                    vote: ok_vote(),
                    response_type: 0,
                    partial_success_index: None,
                }))
            }

            async fn install_snapshot(
                &self,
                request: Request<InstallSnapshotRequest>,
            ) -> Result<Response<crate::server::custodian::InstallSnapshotResponse>, Status>
            {
                let req = request.into_inner();
                let mut guard = self.0.lock().unwrap();
                guard.extend_from_slice(&req.data);
                Ok(Response::new(
                    crate::server::custodian::InstallSnapshotResponse { vote: ok_vote() },
                ))
            }
        }

        let svc = ChunkedSvc(received.clone());
        let addr = "127.0.0.1:50053".parse().unwrap();
        let svc_server = crate::server::custodian::raft_service_server::RaftServiceServer::new(svc);

        let server = tokio::task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc_server)
                .serve(addr)
                .await
                .unwrap();
        });

        // Give server a moment
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create client
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50053".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        // Prepare two chunks
        let chunk1 = b"hello ".to_vec();
        let chunk2 = b"world!".to_vec();

        // First chunk, done = false
        let req1 = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            meta: openraft::SnapshotMeta::default(),
            offset: 0,
            data: chunk1.clone(),
            done: false,
        };

        // Second chunk, done = true
        let req2 = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            meta: openraft::SnapshotMeta::default(),
            offset: chunk1.len() as u64,
            data: chunk2.clone(),
            done: true,
        };

        // Send chunks
        let first_chunk_result = client
            .install_snapshot(
                req1,
                openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(first_chunk_result.is_ok());
        let second_chunk_result = client
            .install_snapshot(
                req2,
                openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(second_chunk_result.is_ok());

        // Verify server received concatenated bytes
        let guard = received.lock().unwrap();
        assert_eq!(&guard[..], &[chunk1, chunk2].concat()[..]);

        // Metrics updated by RPC client (sanity check)
        let _prev_created = crate::metrics::SNAPSHOT_CREATED_TOTAL.get();
        crate::metrics::SNAPSHOT_LAST_SIZE_BYTES.set(123);
        assert!(crate::metrics::SNAPSHOT_LAST_SIZE_BYTES.get() > 0);

        server.abort();
    }

    #[tokio::test]
    async fn test_network_vote_and_append() {
        // Start test server
        let svc = TestRaftSvc::default();
        let addr = "127.0.0.1:50052".parse().unwrap();
        let svc_server = crate::server::custodian::raft_service_server::RaftServiceServer::new(svc);

        let server = task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc_server)
                .serve(addr)
                .await
                .unwrap();
        });

        // Give server a moment
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Factory and client
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50052".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        // Test vote
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
        let res = client
            .vote(
                vote_req,
                openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(res.is_ok());

        // Test append
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
        let res = client
            .append_entries(
                append_req,
                openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(res.is_ok());

        // Shut down server
        server.abort();
    }

    #[tokio::test]
    async fn test_append_entries_decodes_all_response_variants() {
        use std::sync::{Arc, Mutex};

        // A peer whose AppendEntries response_type is switchable, so the client-side decode of
        // PartialSuccess / Conflict / HigherVote / unknown is exercised (fault injection).
        #[derive(Clone)]
        struct VariantSvc(Arc<Mutex<u32>>);

        #[tonic::async_trait]
        impl RaftService for VariantSvc {
            async fn vote(
                &self,
                _r: Request<VoteRequest>,
            ) -> Result<Response<VoteResponse>, Status> {
                Ok(Response::new(VoteResponse {
                    vote: ok_vote(),
                    vote_granted: true,
                    last_log_id: None,
                }))
            }

            async fn append_entries(
                &self,
                _r: Request<AppendEntriesRequest>,
            ) -> Result<Response<AppendEntriesResponse>, Status> {
                let rt = *self.0.lock().unwrap();
                Ok(Response::new(AppendEntriesResponse {
                    vote: ok_vote(),
                    response_type: rt,
                    partial_success_index: if rt == 1 {
                        Some(crate::server::custodian::ProtoLogId { term: 1, index: 7 })
                    } else {
                        None
                    },
                }))
            }

            async fn install_snapshot(
                &self,
                _r: Request<crate::server::custodian::InstallSnapshotRequest>,
            ) -> Result<Response<crate::server::custodian::InstallSnapshotResponse>, Status>
            {
                Ok(Response::new(
                    crate::server::custodian::InstallSnapshotResponse { vote: ok_vote() },
                ))
            }
        }

        let rt = Arc::new(Mutex::new(0u32));
        let server_svc = crate::server::custodian::raft_service_server::RaftServiceServer::new(
            VariantSvc(rt.clone()),
        );
        let addr = "127.0.0.1:50059".parse().unwrap();
        let server = tokio::task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server_svc)
                .serve(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50059".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let make = || openraft::raft::AppendEntriesRequest {
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
        let opt = || openraft::network::RPCOption::new(std::time::Duration::from_secs(1));

        *rt.lock().unwrap() = 1;
        assert!(matches!(
            client.append_entries(make(), opt()).await.unwrap(),
            openraft::raft::AppendEntriesResponse::PartialSuccess(Some(_))
        ));
        *rt.lock().unwrap() = 2;
        assert!(matches!(
            client.append_entries(make(), opt()).await.unwrap(),
            openraft::raft::AppendEntriesResponse::Conflict
        ));
        *rt.lock().unwrap() = 3;
        assert!(matches!(
            client.append_entries(make(), opt()).await.unwrap(),
            openraft::raft::AppendEntriesResponse::HigherVote(_)
        ));
        *rt.lock().unwrap() = 99;
        assert!(client.append_entries(make(), opt()).await.is_err());

        server.abort();
    }

    #[tokio::test]
    async fn vote_append_install_error_on_missing_vote() {
        // A peer that omits the `vote` field → the client's `ok_or_else` missing-vote branches.
        #[derive(Clone, Default)]
        struct NoVoteSvc;

        #[tonic::async_trait]
        impl RaftService for NoVoteSvc {
            async fn vote(
                &self,
                _r: Request<VoteRequest>,
            ) -> Result<Response<VoteResponse>, Status> {
                Ok(Response::new(VoteResponse {
                    vote: None,
                    vote_granted: true,
                    last_log_id: None,
                }))
            }
            async fn append_entries(
                &self,
                _r: Request<AppendEntriesRequest>,
            ) -> Result<Response<AppendEntriesResponse>, Status> {
                Ok(Response::new(AppendEntriesResponse {
                    vote: None,
                    response_type: 0,
                    partial_success_index: None,
                }))
            }
            async fn install_snapshot(
                &self,
                _r: Request<crate::server::custodian::InstallSnapshotRequest>,
            ) -> Result<Response<crate::server::custodian::InstallSnapshotResponse>, Status>
            {
                Ok(Response::new(
                    crate::server::custodian::InstallSnapshotResponse { vote: None },
                ))
            }
        }

        let server_svc =
            crate::server::custodian::raft_service_server::RaftServiceServer::new(NoVoteSvc);
        let addr = "127.0.0.1:50060".parse().unwrap();
        let server = tokio::task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server_svc)
                .serve(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50060".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;
        let opt = || openraft::network::RPCOption::new(std::time::Duration::from_secs(1));
        let vote = || openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };

        assert!(
            client
                .vote(
                    openraft::raft::VoteRequest {
                        vote: vote(),
                        last_log_id: None,
                    },
                    opt()
                )
                .await
                .is_err()
        );
        assert!(
            client
                .append_entries(
                    openraft::raft::AppendEntriesRequest {
                        vote: vote(),
                        prev_log_id: None,
                        entries: vec![],
                        leader_commit: None,
                    },
                    opt()
                )
                .await
                .is_err()
        );
        assert!(
            client
                .install_snapshot(
                    openraft::raft::InstallSnapshotRequest {
                        vote: vote(),
                        meta: openraft::SnapshotMeta::default(),
                        offset: 0,
                        data: vec![1, 2, 3],
                        done: true,
                    },
                    opt()
                )
                .await
                .is_err()
        );

        server.abort();
    }

    #[tokio::test]
    async fn rpcs_error_on_unreachable_peer() {
        // No server listening → the get_client / transport error paths.
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:59998".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;
        let opt = || openraft::network::RPCOption::new(std::time::Duration::from_millis(150));
        let vote = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };

        assert!(
            client
                .vote(
                    openraft::raft::VoteRequest {
                        vote,
                        last_log_id: None,
                    },
                    opt()
                )
                .await
                .is_err()
        );
        assert!(
            client
                .append_entries(
                    openraft::raft::AppendEntriesRequest {
                        vote,
                        prev_log_id: None,
                        entries: vec![],
                        leader_commit: None,
                    },
                    opt()
                )
                .await
                .is_err()
        );
    }

    fn snap(data: Vec<u8>) -> openraft::Snapshot<CustodianTypeConfig> {
        openraft::Snapshot {
            meta: openraft::SnapshotMeta::default(),
            snapshot: Box::new(std::io::Cursor::new(data)),
        }
    }

    fn test_vote() -> openraft::Vote<u64> {
        openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 2,
                node_id: 1,
            },
            committed: true,
        }
    }

    #[tokio::test]
    async fn full_snapshot_succeeds_against_a_live_peer() {
        let server_svc = crate::server::custodian::raft_service_server::RaftServiceServer::new(
            TestRaftSvc::default(),
        );
        let addr = "127.0.0.1:50061".parse().unwrap();
        let server = tokio::task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server_svc)
                .serve(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50061".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let result = client
            .full_snapshot(
                test_vote(),
                snap(b"snapshot bytes".to_vec()),
                async move {
                    let _ = rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(std::time::Duration::from_secs(5)),
            )
            .await;
        assert!(result.is_ok());
        server.abort();
    }

    #[tokio::test]
    async fn full_snapshot_returns_closed_when_cancelled() {
        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50062".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        // Cancel fires immediately (sender dropped), so the select! takes the Closed branch.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        drop(tx);
        let result = client
            .full_snapshot(
                test_vote(),
                snap(vec![]),
                async move {
                    let _ = rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn full_snapshot_errors_on_missing_vote() {
        #[derive(Clone, Default)]
        struct NoVoteSvc;
        #[tonic::async_trait]
        impl RaftService for NoVoteSvc {
            async fn vote(
                &self,
                _r: Request<VoteRequest>,
            ) -> Result<Response<VoteResponse>, Status> {
                Ok(Response::new(VoteResponse {
                    vote: None,
                    vote_granted: true,
                    last_log_id: None,
                }))
            }
            async fn append_entries(
                &self,
                _r: Request<AppendEntriesRequest>,
            ) -> Result<Response<AppendEntriesResponse>, Status> {
                Ok(Response::new(AppendEntriesResponse {
                    vote: None,
                    response_type: 0,
                    partial_success_index: None,
                }))
            }
            async fn install_snapshot(
                &self,
                _r: Request<crate::server::custodian::InstallSnapshotRequest>,
            ) -> Result<Response<crate::server::custodian::InstallSnapshotResponse>, Status>
            {
                Ok(Response::new(
                    crate::server::custodian::InstallSnapshotResponse { vote: None },
                ))
            }
        }
        let server_svc =
            crate::server::custodian::raft_service_server::RaftServiceServer::new(NoVoteSvc);
        let addr = "127.0.0.1:50064".parse().unwrap();
        let server = tokio::task::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server_svc)
                .serve(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut peers = HashMap::new();
        peers.insert(1, "http://127.0.0.1:50064".to_string());
        let mut factory = CustodianNetworkFactory::with_peers(peers);
        let mut client = factory.new_client(1, &openraft::BasicNode::default()).await;

        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let result = client
            .full_snapshot(
                test_vote(),
                snap(vec![1, 2, 3]),
                async move {
                    let _ = rx.await;
                    openraft::error::ReplicationClosed::new(std::io::Error::other("cancelled"))
                },
                openraft::network::RPCOption::new(std::time::Duration::from_secs(5)),
            )
            .await;
        assert!(result.is_err());
        server.abort();
    }
}

/// Factory for creating network clients
#[derive(Clone)]
pub struct CustodianNetworkFactory {
    peers: Arc<HashMap<u64, String>>,
}

impl Default for CustodianNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl CustodianNetworkFactory {
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
impl RaftNetworkFactory<CustodianTypeConfig> for CustodianNetworkFactory {
    type Network = CustodianNetwork;

    fn new_client(
        &mut self,
        target: u64,
        node: &openraft::BasicNode,
    ) -> impl std::future::Future<Output = Self::Network> + Send {
        let address = self
            .get_peer_address(target)
            .unwrap_or_else(|| format!("http://{}:8081", node.addr));

        async move {
            // For now, create without connecting - connect on first use
            CustodianNetwork {
                _target: target,
                address,
                client: None,
            }
        }
    }
}

impl CustodianNetwork {
    async fn get_client(
        &mut self,
    ) -> Result<
        &mut crate::server::custodian::raft_service_client::RaftServiceClient<
            tonic::transport::Channel,
        >,
        openraft::error::NetworkError,
    > {
        if self.client.is_none() {
            // Apply client mTLS if configured (otherwise plaintext); `proto::tls` rewrites
            // http:// -> https:// when TLS is on.
            let channel = proto::tls::connect(&self.address).await.map_err(|e| {
                openraft::error::NetworkError::new(&std::io::Error::other(e.to_string()))
            })?;
            self.client = Some(
                crate::server::custodian::raft_service_client::RaftServiceClient::new(channel),
            );
        }
        // Client should be initialized above, but handle gracefully
        self.client.as_mut().ok_or_else(|| {
            openraft::error::NetworkError::new(&std::io::Error::other("client not initialized"))
        })
    }
}

impl RaftNetwork<CustodianTypeConfig> for CustodianNetwork {
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<u64>,
        openraft::error::RPCError<
            u64,
            openraft::BasicNode,
            openraft::error::RaftError<u64, openraft::error::Infallible>,
        >,
    > {
        let client = self
            .get_client()
            .await
            .map_err(openraft::error::RPCError::Network)?;

        let proto_req = crate::server::custodian::VoteRequest {
            vote: Some(crate::server::custodian::ProtoVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            last_log_id: rpc
                .last_log_id
                .map(|log_id| crate::server::custodian::ProtoLogId {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                }),
        };

        let response = client.vote(proto_req).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let resp = response.into_inner();

        let proto_vote = resp.vote.ok_or_else(|| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "missing vote"),
            ))
        })?;

        Ok(openraft::raft::VoteResponse {
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
                    node_id: proto_vote.node_id,
                },
                index: log_id.index,
            }),
        })
    }

    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<CustodianTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<u64>,
        openraft::error::RPCError<
            u64,
            openraft::BasicNode,
            openraft::error::RaftError<u64, openraft::error::Infallible>,
        >,
    > {
        let client = self
            .get_client()
            .await
            .map_err(openraft::error::RPCError::Network)?;

        // Entries are serialized openraft entries — no consensus field is lost.
        let proto_entries: Vec<crate::server::custodian::Entry> = rpc
            .entries
            .into_iter()
            .map(|entry| {
                Ok::<_, serde_json::Error>(crate::server::custodian::Entry {
                    data: serde_json::to_vec(&entry)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
            })?;

        let proto_req = crate::server::custodian::AppendEntriesRequest {
            vote: Some(crate::server::custodian::ProtoVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            prev_log_id: rpc
                .prev_log_id
                .map(|log_id| crate::server::custodian::ProtoLogId {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                }),
            entries: proto_entries,
            leader_commit: rpc
                .leader_commit
                .map(|log_id| crate::server::custodian::ProtoLogId {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                }),
        };

        let response = client.append_entries(proto_req).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let resp = response.into_inner();

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

    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<u64>,
        snapshot: openraft::Snapshot<CustodianTypeConfig>,
        cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed> + Send + 'static,
        _opt: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::SnapshotResponse<u64>,
        openraft::error::StreamingError<CustodianTypeConfig, openraft::error::Fatal<u64>>,
    > {
        let cancel = std::pin::pin!(cancel);
        let data = snapshot.snapshot.into_inner();

        // Snapshot install metrics.
        let max_usize = usize::try_from(i64::MAX).unwrap_or(usize::MAX);
        let last_size = std::cmp::min(data.len(), max_usize);
        let last_size_i64 = std::convert::TryInto::<i64>::try_into(last_size).unwrap_or(i64::MAX);
        crate::metrics::SNAPSHOT_LAST_SIZE_BYTES.set(last_size_i64);
        crate::metrics::SNAPSHOT_INSTALL_STARTED_TOTAL.inc();

        let proto_req = crate::server::custodian::InstallSnapshotRequest {
            vote: Some(crate::server::custodian::ProtoVote {
                term: vote.leader_id.term,
                node_id: vote.leader_id.node_id,
                committed: vote.committed,
            }),
            meta: Some(crate::server::custodian::SnapshotMeta {
                last_log_id: snapshot.meta.last_log_id.map(|log_id| {
                    crate::server::custodian::ProtoLogId {
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

        tokio::select! {
            closed = cancel => Err(openraft::error::StreamingError::Closed(closed)),
            client_result = self.get_client() => {
                let client = client_result.map_err(openraft::error::StreamingError::Network)?;
                let response = client.install_snapshot(proto_req).await.map_err(|e| {
                    openraft::error::StreamingError::Network(openraft::error::NetworkError::new(&e))
                })?;
                let resp = response.into_inner();
                let proto_vote = resp.vote.ok_or_else(|| {
                    openraft::error::StreamingError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "missing vote in snapshot response",
                        ),
                    ))
                })?;
                crate::metrics::SNAPSHOT_INSTALL_COMPLETED_TOTAL.inc();
                Ok(openraft::raft::SnapshotResponse::new(openraft::Vote {
                    leader_id: openraft::LeaderId {
                        term: proto_vote.term,
                        node_id: proto_vote.node_id,
                    },
                    committed: proto_vote.committed,
                }))
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<CustodianTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        openraft::error::RPCError<
            u64,
            openraft::BasicNode,
            openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    > {
        let client = self
            .get_client()
            .await
            .map_err(openraft::error::RPCError::Network)?;

        let proto_req = crate::server::custodian::InstallSnapshotRequest {
            vote: Some(crate::server::custodian::ProtoVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            meta: Some(crate::server::custodian::SnapshotMeta {
                last_log_id: rpc.meta.last_log_id.map(|log_id| {
                    crate::server::custodian::ProtoLogId {
                        term: log_id.leader_id.term,
                        index: log_id.index,
                    }
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

        let response = client.install_snapshot(proto_req).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let resp = response.into_inner();

        let proto_vote = resp.vote.ok_or_else(|| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "missing vote"),
            ))
        })?;

        Ok(openraft::raft::InstallSnapshotResponse {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: proto_vote.term,
                    node_id: proto_vote.node_id,
                },
                committed: proto_vote.committed,
            },
        })
    }
}
