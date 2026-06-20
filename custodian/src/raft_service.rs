//! gRPC Raft service server for the custodian.
//!
//! Mirrors the DB service `RaftService`: it forwards incoming Raft RPCs to the local
//! `CustodianRaft` and faithfully translates openraft types to/from the wire protocol.
//! Log entries are carried as opaque serialized bytes so no consensus-relevant field is
//! lost, and the real append-entries outcome (`Success` / `PartialSuccess` / `Conflict` /
//! `HigherVote`) is reported back to the leader.

use crate::raft::{CustodianRaft, CustodianTypeConfig};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::server::custodian::{
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

/// Raft service implementation.
pub struct RaftServiceImpl {
    raft: Arc<CustodianRaft>,
}

impl RaftServiceImpl {
    #[must_use]
    pub fn new(raft: Arc<CustodianRaft>) -> Self {
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
        let entries = raft_rpc::decode_entries::<CustodianTypeConfig>(
            req.entries.into_iter().map(|entry| entry.data),
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let append_req = raft_rpc::append_request(
            vote_parts,
            req.prev_log_id.map(|l| (l.term, l.index)),
            entries,
            req.leader_commit.map(|l| (l.term, l.index)),
        );

        let raft_response = self
            .raft
            .append_entries(append_req)
            .await
            .map_err(|e| Status::internal(format!("append_entries failed: {e}")))?;

        let wire = raft_rpc::classify_append_response(&raft_response, vote_parts);

        Ok(Response::new(AppendEntriesResponse {
            vote: Some(to_proto_vote(wire.vote)),
            response_type: wire.response_type,
            partial_success_index: wire.partial_index.map(to_proto_log_id),
        }))
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

        let raft_response = self
            .raft
            .install_snapshot(install_req)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot failed: {e}")))?;

        Ok(Response::new(InstallSnapshotResponse {
            vote: Some(to_proto_vote(raft_rpc::vote_parts(&raft_response.vote))),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Config;
    use openraft::storage::Adaptor;

    fn sample_vote() -> ProtoVote {
        ProtoVote {
            term: 7,
            node_id: 2,
            committed: true,
        }
    }

    // The openraft<->wire conversion logic (formerly `encode_append_response`) now lives in the
    // shared `raft-rpc` crate and is unit-tested there; the handlers below exercise the full path.

    async fn create_raft_service() -> RaftServiceImpl {
        let store = crate::raft::CustodianStore::new_temp().unwrap();
        let cfg = Arc::new(Config::default().validate().expect("config"));
        let network_factory = crate::network::CustodianNetworkFactory::new();
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = CustodianRaft::new(1u64, cfg, network_factory, log_store, state_machine)
            .await
            .expect("create raft");
        RaftServiceImpl::new(Arc::new(raft))
    }

    #[tokio::test]
    async fn vote_and_append_entries_handlers_respond() {
        let svc = create_raft_service().await;

        let resp = svc
            .vote(Request::new(VoteRequest {
                vote: Some(sample_vote()),
                last_log_id: None,
            }))
            .await
            .expect("vote rpc")
            .into_inner();
        assert!(resp.vote.is_some());

        // A well-formed append (no entries) returns a valid typed response.
        let resp = svc
            .append_entries(Request::new(AppendEntriesRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: true,
                }),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }))
            .await
            .expect("append rpc")
            .into_inner();
        assert!(resp.response_type <= 3);
        assert!(resp.vote.is_some());
    }

    #[tokio::test]
    async fn append_entries_rejects_invalid_entry_bytes() {
        let svc = create_raft_service().await;
        let err = svc
            .append_entries(Request::new(AppendEntriesRequest {
                vote: Some(ProtoVote {
                    term: 1,
                    node_id: 1,
                    committed: true,
                }),
                prev_log_id: None,
                entries: vec![crate::server::custodian::Entry {
                    data: b"not json".to_vec(),
                }],
                leader_commit: None,
            }))
            .await
            .expect_err("invalid entry bytes");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
