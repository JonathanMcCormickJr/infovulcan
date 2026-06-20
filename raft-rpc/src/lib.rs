#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Shared openraft ⇄ wire-protocol conversions for the `InfoVulcan` Raft RPC layer.
//!
//! The `db` and `custodian` services each run their own Raft cluster, but the wire shapes of
//! their `RaftService` RPCs are identical. This crate centralizes the fiddly,
//! correctness-critical openraft⇄wire field-shuffling so it lives — and is unit-tested — in
//! exactly one place, instead of being copy-pasted into each service's `raft_service.rs`
//! (server) and `network.rs` (client).
//!
//! It is deliberately **wire-agnostic**: it works in terms of primitive tuples, so each service
//! keeps its own generated proto types and the gRPC wire format is completely unchanged. The two
//! quirks of the existing protocol are preserved exactly:
//!   1. the wire `LogId` only carries `(term, index)`; its `node_id` is taken from the vote, and
//!   2. an append-entries outcome is encoded as `0`=`Success`, `1`=`PartialSuccess`,
//!      `2`=`Conflict`, `3`=`HigherVote`, echoing the leader's vote except for `HigherVote`
//!      (which carries the responder's vote).

use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest};
use openraft::{
    BasicNode, Entry, LeaderId, LogId, Membership, RaftTypeConfig, SnapshotMeta, StoredMembership,
    Vote,
};

/// Errors from decoding the Raft wire protocol back into openraft types.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The `response_type` field of an `AppendEntriesResponse` was outside the known range `0..=3`.
    #[error("unknown append_entries response_type: {0}")]
    UnknownResponseType(u32),
    /// A log entry's opaque bytes failed to deserialize into an openraft `Entry`.
    #[error("invalid entry data: {0}")]
    EntryDecode(#[from] serde_json::Error),
}

/// Wire parts of a vote: `(term, node_id, committed)`.
pub type VoteParts = (u64, u64, bool);

/// Wire parts of a log id: `(term, index)`. The `node_id` lives on the vote, not the wire log id,
/// so it is supplied separately when reconstructing an openraft [`LogId`].
pub type LogIdParts = (u64, u64);

/// Build an openraft [`Vote`] from its wire parts.
#[must_use]
pub fn vote(parts: VoteParts) -> Vote<u64> {
    let (term, node_id, committed) = parts;
    Vote {
        leader_id: LeaderId { term, node_id },
        committed,
    }
}

/// Extract the wire parts of an openraft [`Vote`].
#[must_use]
pub fn vote_parts(v: &Vote<u64>) -> VoteParts {
    (v.leader_id.term, v.leader_id.node_id, v.committed)
}

/// Reconstruct an openraft [`LogId`] from its wire parts, taking `node_id` from the vote.
#[must_use]
pub fn log_id(parts: LogIdParts, node_id: u64) -> LogId<u64> {
    let (term, index) = parts;
    LogId {
        leader_id: LeaderId { term, node_id },
        index,
    }
}

/// Extract the wire parts `(term, index)` of an openraft [`LogId`].
#[must_use]
pub fn log_id_parts(l: &LogId<u64>) -> LogIdParts {
    (l.leader_id.term, l.index)
}

/// A classified, wire-ready append-entries outcome.
pub struct AppendWire {
    /// `0`=`Success`, `1`=`PartialSuccess`, `2`=`Conflict`, `3`=`HigherVote`.
    pub response_type: u32,
    /// For `PartialSuccess`: the `(term, index)` of the last matching log id, if any.
    pub partial_index: Option<LogIdParts>,
    /// The vote to send back: the leader's echoed vote, except for `HigherVote`.
    pub vote: VoteParts,
}

/// **Server side.** Classify an openraft [`AppendEntriesResponse`] into wire parts.
///
/// `echoed_vote` is the leader's vote — echoed for every variant except `HigherVote`, which
/// instead reports the responder's strictly-greater vote that caused the rejection.
#[must_use]
pub fn classify_append_response(
    resp: &AppendEntriesResponse<u64>,
    echoed_vote: VoteParts,
) -> AppendWire {
    match resp {
        AppendEntriesResponse::Success => AppendWire {
            response_type: 0,
            partial_index: None,
            vote: echoed_vote,
        },
        AppendEntriesResponse::PartialSuccess(matching) => AppendWire {
            response_type: 1,
            partial_index: matching.as_ref().map(log_id_parts),
            vote: echoed_vote,
        },
        AppendEntriesResponse::Conflict => AppendWire {
            response_type: 2,
            partial_index: None,
            vote: echoed_vote,
        },
        AppendEntriesResponse::HigherVote(higher) => AppendWire {
            response_type: 3,
            partial_index: None,
            vote: vote_parts(higher),
        },
    }
}

/// **Client side.** Rebuild an openraft [`AppendEntriesResponse`] from wire parts.
///
/// `responder_vote` is the vote returned on the wire; its `node_id` is used to reconstruct the
/// partial-success log id and the higher vote.
///
/// # Errors
/// Returns [`WireError::UnknownResponseType`] if `response_type` is not in `0..=3`.
pub fn append_response_from_wire(
    response_type: u32,
    partial_index: Option<LogIdParts>,
    responder_vote: VoteParts,
) -> Result<AppendEntriesResponse<u64>, WireError> {
    let (_term, node_id, _committed) = responder_vote;
    match response_type {
        0 => Ok(AppendEntriesResponse::Success),
        1 => Ok(AppendEntriesResponse::PartialSuccess(
            partial_index.map(|parts| log_id(parts, node_id)),
        )),
        2 => Ok(AppendEntriesResponse::Conflict),
        3 => Ok(AppendEntriesResponse::HigherVote(vote(responder_vote))),
        other => Err(WireError::UnknownResponseType(other)),
    }
}

/// Build an openraft [`VoteRequest`] from wire parts.
#[must_use]
pub fn vote_request(v: VoteParts, last_log_id: Option<LogIdParts>) -> VoteRequest<u64> {
    let (_term, node_id, _committed) = v;
    VoteRequest {
        vote: vote(v),
        last_log_id: last_log_id.map(|parts| log_id(parts, node_id)),
    }
}

/// Build an openraft [`AppendEntriesRequest`] from wire parts and already-decoded entries.
///
/// `prev_log_id` and `leader_commit` take their `node_id` from the vote, matching the wire format.
#[must_use]
pub fn append_request<C>(
    v: VoteParts,
    prev_log_id: Option<LogIdParts>,
    entries: Vec<Entry<C>>,
    leader_commit: Option<LogIdParts>,
) -> AppendEntriesRequest<C>
where
    C: RaftTypeConfig<NodeId = u64, Entry = Entry<C>>,
{
    let (_term, node_id, _committed) = v;
    AppendEntriesRequest {
        vote: vote(v),
        prev_log_id: prev_log_id.map(|parts| log_id(parts, node_id)),
        entries,
        leader_commit: leader_commit.map(|parts| log_id(parts, node_id)),
    }
}

/// Build the openraft [`SnapshotMeta`] used by `install_snapshot` from wire parts.
///
/// The membership is reconstructed minimally (empty config carrying only the membership version),
/// matching what both services do today — the wire carries a membership *version*, not the full
/// config. `node_id` comes from the vote.
#[must_use]
pub fn snapshot_meta(
    last_log_id: Option<LogIdParts>,
    last_membership: u32,
    snapshot_id: String,
    node_id: u64,
) -> SnapshotMeta<u64, BasicNode> {
    SnapshotMeta {
        last_log_id: last_log_id.map(|parts| log_id(parts, node_id)),
        last_membership: StoredMembership::new(
            Some(LogId {
                leader_id: LeaderId {
                    term: last_log_id.map_or(0, |(term, _)| term),
                    node_id,
                },
                index: u64::from(last_membership),
            }),
            Membership::new(vec![], ()),
        ),
        snapshot_id,
    }
}

/// Decode a sequence of opaque, serialized openraft entries (as carried in the wire `Entry.data`).
///
/// # Errors
/// Returns [`WireError::EntryDecode`] if any entry fails to deserialize.
pub fn decode_entries<C>(raw: impl IntoIterator<Item = Vec<u8>>) -> Result<Vec<Entry<C>>, WireError>
where
    C: RaftTypeConfig,
    Entry<C>: serde::de::DeserializeOwned,
{
    raw.into_iter()
        .map(|bytes| serde_json::from_slice::<Entry<C>>(&bytes))
        .collect::<Result<Vec<_>, _>>()
        .map_err(WireError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_round_trips_through_parts() {
        let parts = (7, 2, true);
        let v = vote(parts);
        assert_eq!(v.leader_id.term, 7);
        assert_eq!(v.leader_id.node_id, 2);
        assert!(v.committed);
        assert_eq!(vote_parts(&v), parts);
    }

    #[test]
    fn log_id_round_trips_and_takes_node_id_from_vote() {
        let l = log_id((4, 11), 5);
        assert_eq!(l.leader_id.term, 4);
        assert_eq!(l.leader_id.node_id, 5);
        assert_eq!(l.index, 11);
        assert_eq!(log_id_parts(&l), (4, 11));
    }

    fn sample_vote() -> VoteParts {
        (7, 2, true)
    }

    #[test]
    fn classify_success() {
        let w = classify_append_response(&AppendEntriesResponse::Success, sample_vote());
        assert_eq!(w.response_type, 0);
        assert!(w.partial_index.is_none());
        assert_eq!(w.vote, sample_vote());
    }

    #[test]
    fn classify_partial_success_carries_index() {
        let log_id = LogId {
            leader_id: LeaderId {
                term: 4,
                node_id: 1,
            },
            index: 11,
        };
        let w = classify_append_response(
            &AppendEntriesResponse::PartialSuccess(Some(log_id)),
            sample_vote(),
        );
        assert_eq!(w.response_type, 1);
        assert_eq!(w.partial_index, Some((4, 11)));
    }

    #[test]
    fn classify_partial_success_without_index() {
        let w =
            classify_append_response(&AppendEntriesResponse::PartialSuccess(None), sample_vote());
        assert_eq!(w.response_type, 1);
        assert!(w.partial_index.is_none());
    }

    #[test]
    fn classify_conflict() {
        let w = classify_append_response(&AppendEntriesResponse::Conflict, sample_vote());
        assert_eq!(w.response_type, 2);
        assert!(w.partial_index.is_none());
    }

    #[test]
    fn classify_higher_vote_reports_responder_vote() {
        let higher = Vote {
            leader_id: LeaderId {
                term: 99,
                node_id: 5,
            },
            committed: false,
        };
        let w = classify_append_response(&AppendEntriesResponse::HigherVote(higher), sample_vote());
        assert_eq!(w.response_type, 3);
        assert_eq!(w.vote, (99, 5, false));
    }

    #[test]
    fn from_wire_inverts_classify_for_each_variant() {
        // Success
        assert!(matches!(
            append_response_from_wire(0, None, sample_vote()),
            Ok(AppendEntriesResponse::Success)
        ));
        // PartialSuccess with index — node_id comes from the responder vote (node_id 2).
        let Ok(AppendEntriesResponse::PartialSuccess(Some(l))) =
            append_response_from_wire(1, Some((4, 11)), sample_vote())
        else {
            panic!("expected partial success with an index");
        };
        assert_eq!((l.leader_id.term, l.leader_id.node_id, l.index), (4, 2, 11));
        // Conflict
        assert!(matches!(
            append_response_from_wire(2, None, sample_vote()),
            Ok(AppendEntriesResponse::Conflict)
        ));
        // HigherVote
        let Ok(AppendEntriesResponse::HigherVote(v)) =
            append_response_from_wire(3, None, (99, 5, false))
        else {
            panic!("expected higher vote");
        };
        assert_eq!(vote_parts(&v), (99, 5, false));
    }

    #[test]
    fn from_wire_rejects_unknown_response_type() {
        assert!(append_response_from_wire(9, None, sample_vote()).is_err());
    }

    #[test]
    fn vote_request_uses_vote_node_id_for_last_log_id() {
        let req = vote_request((3, 8, false), Some((2, 5)));
        let last = req.last_log_id.expect("last log id present");
        assert_eq!(
            (last.leader_id.term, last.leader_id.node_id, last.index),
            (2, 8, 5)
        );
    }

    #[test]
    fn snapshot_meta_carries_version_and_node_id() {
        let meta = snapshot_meta(Some((6, 20)), 3, "snap-1".to_string(), 4);
        let last = meta.last_log_id.expect("last log id");
        // log_id((term=6, index=20), node_id=4) → (term, node_id, index)
        assert_eq!(
            (last.leader_id.term, last.leader_id.node_id, last.index),
            (6, 4, 20)
        );
        assert_eq!(meta.snapshot_id, "snap-1");
    }

    #[test]
    fn decode_entries_rejects_garbage() {
        // Use a minimal config-free deserialize target via the db-style entry is not available
        // here; garbage bytes must always fail regardless of the concrete type.
        let result = decode_entries::<TestConfig>(vec![b"not json".to_vec()]);
        assert!(result.is_err());
    }

    // A minimal RaftTypeConfig so the generic builders can be exercised in this crate's own tests.
    openraft::declare_raft_types!(
        pub TestConfig:
            D = TestData,
            R = TestData,
            NodeId = u64,
            Node = BasicNode,
            Entry = openraft::Entry<TestConfig>,
            SnapshotData = std::io::Cursor<Vec<u8>>,
            AsyncRuntime = openraft::TokioRuntime,
            Responder = openraft::impls::OneshotResponder<TestConfig>,
    );

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct TestData(u64);

    #[test]
    fn append_request_takes_node_id_from_vote() {
        let req = append_request::<TestConfig>((1, 9, true), Some((1, 2)), vec![], Some((1, 4)));
        assert_eq!(req.prev_log_id.expect("prev").leader_id.node_id, 9);
        assert_eq!(req.leader_commit.expect("commit").leader_id.node_id, 9);
        assert!(req.entries.is_empty());
    }

    #[test]
    fn decode_entries_round_trips_a_valid_entry() {
        let entry = openraft::Entry::<TestConfig> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 1,
            },
            payload: openraft::EntryPayload::Normal(TestData(42)),
        };
        let bytes = serde_json::to_vec(&entry).expect("serialize");
        let decoded = decode_entries::<TestConfig>(vec![bytes]).expect("decode");
        assert_eq!(decoded.len(), 1);
    }
}
