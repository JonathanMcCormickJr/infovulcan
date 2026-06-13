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
use tracing::debug;

use crate::server::custodian::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    ProtoLogId, ProtoVote, VoteRequest, VoteResponse, raft_service_server::RaftService,
};

/// Encode an openraft `AppendEntriesResponse` into the wire triple
/// `(response_type, partial_success_index, vote)`.
///
/// Wire encoding: `0`=success, `1`=partial-success, `2`=conflict, `3`=higher-vote.
/// For every variant except higher-vote the responder echoes the leader's vote; higher-vote
/// carries the responder's strictly-greater vote that caused the rejection.
fn encode_append_response(
    resp: &openraft::raft::AppendEntriesResponse<u64>,
    echoed_vote: ProtoVote,
) -> (u32, Option<ProtoLogId>, ProtoVote) {
    use openraft::raft::AppendEntriesResponse as Aer;
    match resp {
        Aer::Success => (0, None, echoed_vote),
        Aer::PartialSuccess(matching) => (
            1,
            matching.as_ref().map(|log_id| ProtoLogId {
                term: log_id.leader_id.term,
                index: log_id.index,
            }),
            echoed_vote,
        ),
        Aer::Conflict => (2, None, echoed_vote),
        Aer::HigherVote(higher) => (
            3,
            None,
            ProtoVote {
                term: higher.leader_id.term,
                node_id: higher.leader_id.node_id,
                committed: higher.committed,
            },
        ),
    }
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
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();
        debug!("received vote request");

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;

        let vote_req = openraft::raft::VoteRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: proto_vote.term,
                    node_id: proto_vote.node_id,
                },
                committed: proto_vote.committed,
            },
            last_log_id: req.last_log_id.map(|log_id| openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: log_id.term,
                    node_id: proto_vote.node_id,
                },
                index: log_id.index,
            }),
        };

        let raft_response = self
            .raft
            .vote(vote_req)
            .await
            .map_err(|e| Status::internal(format!("vote failed: {e}")))?;

        let proto_response = VoteResponse {
            vote: Some(ProtoVote {
                term: raft_response.vote.leader_id.term,
                node_id: raft_response.vote.leader_id.node_id,
                committed: raft_response.vote.committed,
            }),
            vote_granted: raft_response.vote_granted,
            last_log_id: raft_response.last_log_id.map(|log_id| ProtoLogId {
                term: log_id.leader_id.term,
                index: log_id.index,
            }),
        };

        Ok(Response::new(proto_response))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "received append_entries request: entries={}",
            req.entries.len()
        );

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;

        // Entries are opaque serialized openraft entries.
        let entries: Vec<openraft::Entry<CustodianTypeConfig>> = req
            .entries
            .into_iter()
            .map(|entry| serde_json::from_slice(&entry.data))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(format!("invalid entry data: {e}")))?;

        let append_req = openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: proto_vote.term,
                    node_id: proto_vote.node_id,
                },
                committed: proto_vote.committed,
            },
            prev_log_id: req.prev_log_id.map(|log_id| openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: log_id.term,
                    node_id: proto_vote.node_id,
                },
                index: log_id.index,
            }),
            entries,
            leader_commit: req.leader_commit.map(|log_id| openraft::LogId {
                leader_id: openraft::LeaderId {
                    term: log_id.term,
                    node_id: proto_vote.node_id,
                },
                index: log_id.index,
            }),
        };

        let raft_response = self
            .raft
            .append_entries(append_req)
            .await
            .map_err(|e| Status::internal(format!("append_entries failed: {e}")))?;

        let echoed_vote = ProtoVote {
            term: proto_vote.term,
            node_id: proto_vote.node_id,
            committed: proto_vote.committed,
        };
        let (response_type, partial_success_index, vote) =
            encode_append_response(&raft_response, echoed_vote);

        Ok(Response::new(AppendEntriesResponse {
            vote: Some(vote),
            response_type,
            partial_success_index,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "received install_snapshot request: offset={}, done={}",
            req.offset, req.done
        );

        let proto_vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;
        let proto_meta = req
            .meta
            .ok_or_else(|| Status::invalid_argument("missing snapshot meta"))?;

        let install_req = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote {
                leader_id: openraft::LeaderId {
                    term: proto_vote.term,
                    node_id: proto_vote.node_id,
                },
                committed: proto_vote.committed,
            },
            meta: openraft::SnapshotMeta {
                last_log_id: proto_meta.last_log_id.map(|log_id| openraft::LogId {
                    leader_id: openraft::LeaderId {
                        term: log_id.term,
                        node_id: proto_vote.node_id,
                    },
                    index: log_id.index,
                }),
                last_membership: openraft::StoredMembership::new(
                    Some(openraft::LogId {
                        leader_id: openraft::LeaderId {
                            term: proto_meta.last_log_id.as_ref().map_or(0, |l| l.term),
                            node_id: proto_vote.node_id,
                        },
                        index: u64::from(proto_meta.last_membership),
                    }),
                    openraft::Membership::new(vec![], ()),
                ),
                snapshot_id: proto_meta.snapshot_id,
            },
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
            vote: Some(ProtoVote {
                term: raft_response.vote.leader_id.term,
                node_id: raft_response.vote.leader_id.node_id,
                committed: raft_response.vote.committed,
            }),
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

    #[test]
    fn encode_append_response_variants() {
        use openraft::raft::AppendEntriesResponse as Aer;

        let (rt, idx, vote) = encode_append_response(&Aer::Success, sample_vote());
        assert_eq!(rt, 0);
        assert!(idx.is_none());
        assert_eq!(vote.term, 7);

        let log_id = openraft::LogId {
            leader_id: openraft::LeaderId {
                term: 4,
                node_id: 1,
            },
            index: 11,
        };
        let (rt, idx, _) =
            encode_append_response(&Aer::PartialSuccess(Some(log_id)), sample_vote());
        assert_eq!(rt, 1);
        let idx = idx.expect("partial success carries an index");
        assert_eq!((idx.term, idx.index), (4, 11));

        let (rt, _, _) = encode_append_response(&Aer::Conflict, sample_vote());
        assert_eq!(rt, 2);

        let higher = openraft::Vote {
            leader_id: openraft::LeaderId {
                term: 99,
                node_id: 5,
            },
            committed: false,
        };
        let (rt, _, vote) = encode_append_response(&Aer::HigherVote(higher), sample_vote());
        assert_eq!(rt, 3);
        assert_eq!((vote.term, vote.node_id), (99, 5));
    }

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
