//! gRPC service implementation for Raft consensus RPCs
//!
//! This module provides the server-side implementation of the `RaftService` gRPC service,
//! handling incoming RPC calls from peer nodes in the Raft cluster.

use crate::raft::{DbRaft, DbTypeConfig};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::server::db::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ProtoLogId, ProtoVote, VoteRequest, VoteResponse, raft_service_server::RaftService,
};

/// Build a wire [`ProtoVote`] from the shared vote parts `(term, node_id, committed)`.
fn to_proto_vote(parts: raft_rpc::VoteParts) -> ProtoVote {
    let (term, node_id, committed) = parts;
    ProtoVote {
        term,
        node_id,
        committed,
    }
}

/// Build a wire [`ProtoLogId`] from the shared log-id parts `(term, index)`.
fn to_proto_log_id(parts: raft_rpc::LogIdParts) -> ProtoLogId {
    let (term, index) = parts;
    ProtoLogId { term, index }
}

/// Implementation of the Raft service
pub struct RaftServiceImpl {
    /// Reference to the Raft instance
    raft: Arc<DbRaft>,
}

impl RaftServiceImpl {
    #[must_use]
    pub fn new(raft: Arc<DbRaft>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    #[tracing::instrument(skip_all)]
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;

        let vote_req = raft_rpc::vote_request(
            (proto_vote.term, proto_vote.node_id, proto_vote.committed),
            req.last_log_id.map(|l| (l.term, l.index)),
        );

        let raft_response = self
            .raft
            .vote(vote_req)
            .await
            .map_err(|e| Status::internal(format!("vote failed: {e}")))?;

        let proto_response = VoteResponse {
            vote: Some(to_proto_vote(raft_rpc::vote_parts(&raft_response.vote))),
            vote_granted: raft_response.vote_granted,
            last_log_id: raft_response
                .last_log_id
                .as_ref()
                .map(|l| to_proto_log_id(raft_rpc::log_id_parts(l))),
        };

        Ok(Response::new(proto_response))
    }

    #[tracing::instrument(skip_all, fields(entries = request.get_ref().entries.len()))]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;
        let vote_parts = (proto_vote.term, proto_vote.node_id, proto_vote.committed);

        // Entries are opaque serialized openraft entries.
        let entries = raft_rpc::decode_entries::<DbTypeConfig>(
            req.entries.into_iter().map(|entry| entry.data),
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let append_req = raft_rpc::append_request(
            vote_parts,
            req.prev_log_id.map(|l| (l.term, l.index)),
            entries,
            req.leader_commit.map(|l| (l.term, l.index)),
        );

        // Forward to Raft
        let raft_response = self
            .raft
            .append_entries(append_req)
            .await
            .map_err(|e| Status::internal(format!("append_entries failed: {e}")))?;

        // Map the openraft response into the typed wire response so the leader can
        // distinguish success / partial-success / log-conflict / higher-vote rejection.
        let wire = raft_rpc::classify_append_response(&raft_response, vote_parts);

        let proto_response = AppendEntriesResponse {
            vote: Some(to_proto_vote(wire.vote)),
            response_type: wire.response_type,
            partial_success_index: wire.partial_index.map(to_proto_log_id),
        };

        Ok(Response::new(proto_response))
    }

    #[tracing::instrument(skip_all, fields(offset = request.get_ref().offset, done = request.get_ref().done))]
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;
        let proto_meta = req
            .meta
            .ok_or_else(|| Status::invalid_argument("missing snapshot meta"))?;

        let install_req = openraft::raft::InstallSnapshotRequest {
            vote: raft_rpc::vote((proto_vote.term, proto_vote.node_id, proto_vote.committed)),
            meta: raft_rpc::snapshot_meta(
                proto_meta.last_log_id.map(|l| (l.term, l.index)),
                proto_meta.last_membership,
                proto_meta.snapshot_id,
                proto_vote.node_id,
            ),
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        // Forward to Raft
        let raft_response = self
            .raft
            .install_snapshot(install_req)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot failed: {e}")))?;

        let proto_response = InstallSnapshotResponse {
            vote: Some(to_proto_vote(raft_rpc::vote_parts(&raft_response.vote))),
        };

        Ok(Response::new(proto_response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::DbNetworkFactory;
    use crate::raft::DbStore;
    use crate::storage::LogEntry;
    use openraft::storage::Adaptor;
    use tonic::Request;

    // The openraft<->wire conversion logic (formerly `encode_append_response`) now lives in the
    // shared `raft-rpc` crate and is unit-tested there; these handlers exercise the full path.

    async fn test_service() -> RaftServiceImpl {
        let store = DbStore::new_temp().expect("temp store");
        let cfg = Arc::new(openraft::Config::default().validate().expect("raft config"));
        let network_factory = DbNetworkFactory::new();
        let (log_store, state_machine) = Adaptor::new(store);
        let raft = Arc::new(
            DbRaft::new(1, cfg, network_factory, log_store, state_machine)
                .await
                .expect("raft node"),
        );

        let mut members = std::collections::BTreeSet::new();
        members.insert(1);
        let _ = raft.initialize(members).await;

        RaftServiceImpl::new(raft)
    }

    #[tokio::test]
    async fn vote_rejects_missing_vote() {
        let service = test_service().await;
        let err = service
            .vote(Request::new(VoteRequest {
                vote: None,
                last_log_id: None,
            }))
            .await
            .expect_err("missing vote must fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn vote_accepts_well_formed_request() {
        let service = test_service().await;
        let resp = service
            .vote(Request::new(VoteRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: false,
                }),
                last_log_id: None,
            }))
            .await
            .expect("vote request");

        let body = resp.into_inner();
        assert!(body.vote.is_some());
    }

    #[tokio::test]
    async fn append_entries_rejects_missing_vote() {
        let service = test_service().await;
        let err = service
            .append_entries(Request::new(AppendEntriesRequest {
                vote: None,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }))
            .await
            .expect_err("missing vote must fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn append_entries_rejects_invalid_entry_data() {
        let service = test_service().await;
        let err = service
            .append_entries(Request::new(AppendEntriesRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: false,
                }),
                prev_log_id: None,
                entries: vec![crate::server::db::Entry {
                    data: b"not-json".to_vec(),
                }],
                leader_commit: None,
            }))
            .await
            .expect_err("invalid entry payload must fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn append_entries_handles_well_formed_entry() {
        let service = test_service().await;
        let entry = openraft::Entry::<DbTypeConfig> {
            log_id: openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: openraft::EntryPayload::Normal(LogEntry::Put {
                collection: "c".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
        };

        let entry_bytes = serde_json::to_vec(&entry).expect("serialize entry");

        let response = service
            .append_entries(Request::new(AppendEntriesRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: false,
                }),
                prev_log_id: None,
                entries: vec![crate::server::db::Entry { data: entry_bytes }],
                leader_commit: None,
            }))
            .await
            .expect("append entries should succeed");

        let body = response.into_inner();
        // The exact class (Success/PartialSuccess/Conflict/HigherVote) depends on the
        // node's current vote/log state; assert the handler returns a valid typed response.
        // (Previously this was hardcoded to 0 regardless of the real Raft outcome.)
        assert!(body.response_type <= 3);
        assert!(body.vote.is_some());
    }

    #[tokio::test]
    async fn install_snapshot_rejects_missing_vote_or_meta() {
        let service = test_service().await;

        let err_vote = service
            .install_snapshot(Request::new(InstallSnapshotRequest {
                vote: None,
                meta: Some(crate::server::db::SnapshotMeta {
                    last_log_id: None,
                    last_applied: 0,
                    last_membership: 0,
                    snapshot_id: "snap".to_string(),
                }),
                offset: 0,
                data: vec![],
                done: true,
            }))
            .await
            .expect_err("missing vote must fail");
        assert_eq!(err_vote.code(), tonic::Code::InvalidArgument);

        let err_meta = service
            .install_snapshot(Request::new(InstallSnapshotRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: false,
                }),
                meta: None,
                offset: 0,
                data: vec![],
                done: true,
            }))
            .await
            .expect_err("missing meta must fail");
        assert_eq!(err_meta.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn install_snapshot_accepts_well_formed_request() {
        let service = test_service().await;

        let response = service
            .install_snapshot(Request::new(InstallSnapshotRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: false,
                }),
                meta: Some(crate::server::db::SnapshotMeta {
                    last_log_id: None,
                    last_applied: 0,
                    last_membership: 1,
                    snapshot_id: "snap-1".to_string(),
                }),
                offset: 0,
                data: b"snapshot-chunk".to_vec(),
                done: true,
            }))
            .await
            .expect("install snapshot should succeed");

        let body = response.into_inner();
        assert!(body.vote.is_some());
    }
}
