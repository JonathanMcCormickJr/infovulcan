//! gRPC server implementation for the Database service
//!
//! This module implements the gRPC endpoint handlers for the Database service,
//! routing requests appropriately through either Raft consensus (writes) or
//! direct storage access (reads).

use crate::raft::DbRaft;
use crate::storage::{self, LogEntry, Storage};
use tonic::{Request, Response, Status};

pub use proto::db;

use db::database_server::Database;
use db::{
    BatchPutRequest, BatchPutResponse, ClusterStatusRequest, ClusterStatusResponse, DeleteAck,
    DeleteRequest, DeleteResponse, ExistsRequest, ExistsResponse, GetRequest, GetResponse,
    HealthRequest, HealthResponse, KeyValue, ListRequest, ListResponse, PutRequest, PutResponse,
    TicketLookup, TicketQuery, TicketRecord, TicketWrite, UserLookup, UserRecord, UserWrite,
};

/// Convert a protobuf ticket index payload into the storage representation.
fn ticket_index_from_proto(idx: Option<db::TicketIndexFields>) -> storage::TicketIndexFields {
    let idx = idx.unwrap_or_default();
    storage::TicketIndexFields {
        status: u8::try_from(idx.status).unwrap_or(0),
        account_uuid: idx.account_uuid,
        assigned_to_uuid: idx.assigned_to_uuid,
        project: idx.project,
        tracking_url: idx.tracking_url,
        created_at_unix: idx.created_at_unix,
        updated_at_unix: idx.updated_at_unix,
    }
}

/// Convert a protobuf user index payload into the storage representation.
fn user_index_from_proto(idx: Option<db::UserIndexFields>) -> storage::UserIndexFields {
    let idx = idx.unwrap_or_default();
    storage::UserIndexFields {
        username: idx.username,
        email: idx.email,
        role: u8::try_from(idx.role).unwrap_or(0),
    }
}

fn ticket_record(id: u64, stored: &storage::StoredTicket) -> TicketRecord {
    TicketRecord {
        ticket_id: id,
        encrypted_body: stored.body.clone(),
        deleted: stored.deleted,
        deleted_at_unix: stored.deleted_at_unix,
    }
}

fn user_record(uuid: String, stored: &storage::StoredUser) -> UserRecord {
    UserRecord {
        user_uuid: uuid,
        encrypted_body: stored.body.clone(),
        deleted: stored.deleted,
        deleted_at_unix: stored.deleted_at_unix,
    }
}

/// Database service implementation
///
/// Implements the gRPC Database service with the following behavior:
/// - Write operations (`Put`, `Delete`, `BatchPut`) are submitted to Raft for consensus
/// - Read operations (Get, List, Exists) are read directly from local storage
/// - Meta operations (`Health`, `ClusterStatus`) report Raft cluster state
pub struct DatabaseService {
    raft: DbRaft,
    storage: Storage,
}

impl DatabaseService {
    #[must_use]
    pub fn new(raft: DbRaft, storage: Storage) -> Self {
        Self { raft, storage }
    }
}

#[tonic::async_trait]
impl Database for DatabaseService {
    // ---- Domain RPCs: tickets ----

    async fn create_ticket(
        &self,
        request: Request<TicketWrite>,
    ) -> Result<Response<TicketRecord>, Status> {
        let req = request.into_inner();
        let entry = LogEntry::CreateTicket {
            body: req.encrypted_body.clone(),
            index: ticket_index_from_proto(req.index),
        };
        let resp = self
            .raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;

        // CreateTicket returns the assigned id (big-endian) in the response value.
        let id = resp
            .data
            .value
            .as_deref()
            .and_then(|b| b.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| Status::internal("create_ticket did not return an id"))?;

        Ok(Response::new(TicketRecord {
            ticket_id: id,
            encrypted_body: req.encrypted_body,
            deleted: false,
            deleted_at_unix: 0,
        }))
    }

    async fn get_ticket(
        &self,
        request: Request<TicketLookup>,
    ) -> Result<Response<TicketRecord>, Status> {
        let req = request.into_inner();
        match self
            .storage
            .get_ticket(req.ticket_id, req.include_deleted)
            .map_err(|e| Status::internal(format!("Storage read failed: {e}")))?
        {
            Some(stored) => Ok(Response::new(ticket_record(req.ticket_id, &stored))),
            None => Err(Status::not_found("ticket not found")),
        }
    }

    async fn update_ticket(
        &self,
        request: Request<TicketWrite>,
    ) -> Result<Response<TicketRecord>, Status> {
        let req = request.into_inner();
        if req.ticket_id == 0 {
            return Err(Status::invalid_argument(
                "update_ticket requires a ticket_id",
            ));
        }
        let entry = LogEntry::UpdateTicket {
            ticket_id: req.ticket_id,
            body: req.encrypted_body.clone(),
            index: ticket_index_from_proto(req.index),
        };
        self.raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;

        // Re-read so the returned record reflects persisted soft-delete state.
        match self
            .storage
            .get_ticket(req.ticket_id, true)
            .map_err(|e| Status::internal(format!("Storage read failed: {e}")))?
        {
            Some(stored) => Ok(Response::new(ticket_record(req.ticket_id, &stored))),
            None => Err(Status::not_found("ticket not found")),
        }
    }

    async fn soft_delete_ticket(
        &self,
        request: Request<TicketLookup>,
    ) -> Result<Response<DeleteAck>, Status> {
        let req = request.into_inner();
        let entry = LogEntry::SoftDeleteTicket {
            ticket_id: req.ticket_id,
            at_unix: chrono::Utc::now().timestamp(),
        };
        self.raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;
        Ok(Response::new(DeleteAck { success: true }))
    }

    type QueryTicketsStream = tokio_stream::Iter<std::vec::IntoIter<Result<TicketRecord, Status>>>;

    async fn query_tickets(
        &self,
        request: Request<TicketQuery>,
    ) -> Result<Response<Self::QueryTicketsStream>, Status> {
        let req = request.into_inner();
        let query = storage::TicketQuery {
            status: req.status.and_then(|s| u8::try_from(s).ok()),
            assigned_to_uuid: req.assigned_to_uuid,
            account_uuid: req.account_uuid,
            project: req.project,
            include_deleted: req.include_deleted,
            limit: req.limit as usize,
        };
        let records: Vec<Result<TicketRecord, Status>> = self
            .storage
            .query_tickets(&query)
            .map_err(|e| Status::internal(format!("Storage query failed: {e}")))?
            .into_iter()
            .map(|(id, stored)| Ok(ticket_record(id, &stored)))
            .collect();
        Ok(Response::new(tokio_stream::iter(records)))
    }

    // ---- Domain RPCs: users ----

    async fn create_user(
        &self,
        request: Request<UserWrite>,
    ) -> Result<Response<UserRecord>, Status> {
        let req = request.into_inner();
        if req.user_uuid.is_empty() {
            return Err(Status::invalid_argument("create_user requires a user_uuid"));
        }
        let entry = LogEntry::CreateUser {
            user_uuid: req.user_uuid.clone(),
            body: req.encrypted_body.clone(),
            index: user_index_from_proto(req.index),
        };
        self.raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;
        Ok(Response::new(UserRecord {
            user_uuid: req.user_uuid,
            encrypted_body: req.encrypted_body,
            deleted: false,
            deleted_at_unix: 0,
        }))
    }

    async fn get_user(&self, request: Request<UserLookup>) -> Result<Response<UserRecord>, Status> {
        let req = request.into_inner();
        match self
            .storage
            .get_user(&req.user_uuid, req.include_deleted)
            .map_err(|e| Status::internal(format!("Storage read failed: {e}")))?
        {
            Some(stored) => Ok(Response::new(user_record(req.user_uuid, &stored))),
            None => Err(Status::not_found("user not found")),
        }
    }

    async fn update_user(
        &self,
        request: Request<UserWrite>,
    ) -> Result<Response<UserRecord>, Status> {
        let req = request.into_inner();
        if req.user_uuid.is_empty() {
            return Err(Status::invalid_argument("update_user requires a user_uuid"));
        }
        let entry = LogEntry::UpdateUser {
            user_uuid: req.user_uuid.clone(),
            body: req.encrypted_body.clone(),
            index: user_index_from_proto(req.index),
        };
        self.raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;
        match self
            .storage
            .get_user(&req.user_uuid, true)
            .map_err(|e| Status::internal(format!("Storage read failed: {e}")))?
        {
            Some(stored) => Ok(Response::new(user_record(req.user_uuid, &stored))),
            None => Err(Status::not_found("user not found")),
        }
    }

    async fn soft_delete_user(
        &self,
        request: Request<UserLookup>,
    ) -> Result<Response<DeleteAck>, Status> {
        let req = request.into_inner();
        let entry = LogEntry::SoftDeleteUser {
            user_uuid: req.user_uuid,
            at_unix: chrono::Utc::now().timestamp(),
        };
        self.raft
            .client_write(entry)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;
        Ok(Response::new(DeleteAck { success: true }))
    }

    // ---- Generic KV ----

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        let entry = LogEntry::Put {
            collection: req.collection,
            key: req.key,
            value: req.value,
        };

        // Submit to Raft for consensus
        match self.raft.client_write(entry).await {
            Ok(_) => Ok(Response::new(PutResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(format!("Raft write failed: {e}"))),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        // Reads can go directly to local storage (linearizable reads via leader lease)
        match self.storage.get(&req.collection, &req.key) {
            Ok(Some(value)) => Ok(Response::new(GetResponse {
                found: true,
                value,
                error: String::new(),
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                found: false,
                value: vec![],
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(format!("Storage read failed: {e}"))),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        let entry = LogEntry::Delete {
            collection: req.collection,
            key: req.key,
        };

        match self.raft.client_write(entry).await {
            Ok(_) => Ok(Response::new(DeleteResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(format!("Raft write failed: {e}"))),
        }
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };

        match self.storage.list(&req.collection, &req.prefix, limit) {
            Ok(pairs) => {
                let items = pairs
                    .into_iter()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect();
                Ok(Response::new(ListResponse { items }))
            }
            Err(e) => Err(Status::internal(format!("Storage list failed: {e}"))),
        }
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        let req = request.into_inner();

        match self.storage.exists(&req.collection, &req.key) {
            Ok(exists) => Ok(Response::new(ExistsResponse { exists })),
            Err(e) => Err(Status::internal(format!("Storage check failed: {e}"))),
        }
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutResponse>, Status> {
        let req = request.into_inner();

        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            req.items.into_iter().map(|kv| (kv.key, kv.value)).collect();

        let count = u32::try_from(pairs.len()).unwrap_or(u32::MAX);
        let entry = LogEntry::BatchPut {
            collection: req.collection,
            pairs,
        };

        match self.raft.client_write(entry).await {
            Ok(_) => Ok(Response::new(BatchPutResponse {
                success: true,
                written: count,
                error: String::new(),
            })),
            Err(e) => Err(Status::internal(format!("Raft write failed: {e}"))),
        }
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        let role = match metrics.state {
            openraft::ServerState::Leader => "leader".to_string(),
            openraft::ServerState::Follower => "follower".to_string(),
            openraft::ServerState::Candidate => "candidate".to_string(),
            openraft::ServerState::Learner => "learner".to_string(),
            openraft::ServerState::Shutdown => "shutdown".to_string(),
        };

        Ok(Response::new(HealthResponse {
            healthy: matches!(
                metrics.state,
                openraft::ServerState::Leader | openraft::ServerState::Follower
            ),
            node_id: metrics.id.to_string(),
            role,
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        let leader_id = metrics
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_default();
        let member_ids: Vec<String> = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _node)| id.to_string())
            .collect();
        let term = metrics.current_term;
        let commit_index = metrics.last_applied.map_or(0, |id| id.index);

        Ok(Response::new(ClusterStatusResponse {
            leader_id,
            member_ids,
            term,
            commit_index,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::DbNetworkFactory;
    use crate::raft::DbStore;
    use openraft::Config;
    use openraft::storage::Adaptor;
    use std::sync::Arc;

    async fn create_service() -> DatabaseService {
        let store = DbStore::new_temp().expect("temp store");
        let cfg = Arc::new(Config::default().validate().expect("raft config"));
        let network_factory = DbNetworkFactory::new();
        let (log_store, state_machine) = Adaptor::new(store.clone());

        let raft = crate::raft::DbRaft::new(1, cfg, network_factory, log_store, state_machine)
            .await
            .expect("raft node");

        let mut members = std::collections::BTreeSet::new();
        members.insert(1);
        let _ = raft.initialize(members).await;

        let storage = store.state_machine().read().await.storage.clone();
        DatabaseService::new(raft, storage)
    }

    #[tokio::test]
    async fn database_service_put_get_exists_delete_list_and_batch_put() {
        let svc = create_service().await;

        let put_resp = svc
            .put(Request::new(PutRequest {
                collection: "tickets".to_string(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }))
            .await
            .expect("put")
            .into_inner();
        assert!(put_resp.success);

        let exists_resp = svc
            .exists(Request::new(ExistsRequest {
                collection: "tickets".to_string(),
                key: b"k1".to_vec(),
            }))
            .await
            .expect("exists")
            .into_inner();
        assert!(exists_resp.exists);

        let get_resp = svc
            .get(Request::new(GetRequest {
                collection: "tickets".to_string(),
                key: b"k1".to_vec(),
            }))
            .await
            .expect("get")
            .into_inner();
        assert!(get_resp.found);
        assert_eq!(get_resp.value, b"v1");

        let batch_resp = svc
            .batch_put(Request::new(BatchPutRequest {
                collection: "tickets".to_string(),
                items: vec![
                    KeyValue {
                        key: b"k2".to_vec(),
                        value: b"v2".to_vec(),
                    },
                    KeyValue {
                        key: b"k3".to_vec(),
                        value: b"v3".to_vec(),
                    },
                ],
            }))
            .await
            .expect("batch_put")
            .into_inner();
        assert!(batch_resp.success);
        assert_eq!(batch_resp.written, 2);

        let list_resp = svc
            .list(Request::new(ListRequest {
                collection: "tickets".to_string(),
                prefix: b"k".to_vec(),
                limit: 10,
            }))
            .await
            .expect("list")
            .into_inner();
        assert!(list_resp.items.len() >= 3);

        let delete_resp = svc
            .delete(Request::new(DeleteRequest {
                collection: "tickets".to_string(),
                key: b"k1".to_vec(),
            }))
            .await
            .expect("delete")
            .into_inner();
        assert!(delete_resp.success);

        let get_deleted = svc
            .get(Request::new(GetRequest {
                collection: "tickets".to_string(),
                key: b"k1".to_vec(),
            }))
            .await
            .expect("get deleted")
            .into_inner();
        assert!(!get_deleted.found);
    }

    fn sample_proto_index(status: u32) -> db::TicketIndexFields {
        db::TicketIndexFields {
            status,
            account_uuid: "acct".to_string(),
            assigned_to_uuid: Some("agent".to_string()),
            project: "proj".to_string(),
            tracking_url: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        }
    }

    #[tokio::test]
    async fn create_then_get_ticket_roundtrip() {
        let svc = create_service().await;

        let created = svc
            .create_ticket(Request::new(TicketWrite {
                ticket_id: 0,
                encrypted_body: b"cipher".to_vec(),
                index: Some(sample_proto_index(1)),
            }))
            .await
            .expect("create")
            .into_inner();
        assert_eq!(created.ticket_id, 1);

        let got = svc
            .get_ticket(Request::new(TicketLookup {
                ticket_id: created.ticket_id,
                include_deleted: false,
            }))
            .await
            .expect("get")
            .into_inner();
        assert_eq!(got.encrypted_body, b"cipher");
        assert!(!got.deleted);
    }

    #[tokio::test]
    async fn query_streams_matching_tickets() {
        use tokio_stream::StreamExt;
        let svc = create_service().await;

        let open = svc
            .create_ticket(Request::new(TicketWrite {
                ticket_id: 0,
                encrypted_body: b"a".to_vec(),
                index: Some(sample_proto_index(1)),
            }))
            .await
            .unwrap()
            .into_inner();
        let _closed = svc
            .create_ticket(Request::new(TicketWrite {
                ticket_id: 0,
                encrypted_body: b"b".to_vec(),
                index: Some(sample_proto_index(9)),
            }))
            .await
            .unwrap();

        let stream = svc
            .query_tickets(Request::new(TicketQuery {
                status: Some(1),
                assigned_to_uuid: None,
                account_uuid: None,
                project: None,
                include_deleted: false,
                limit: 0,
            }))
            .await
            .expect("query")
            .into_inner();
        let results: Vec<_> = stream.collect::<Vec<_>>().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().ticket_id, open.ticket_id);
    }

    #[tokio::test]
    async fn soft_delete_hides_ticket_unless_include_deleted() {
        let svc = create_service().await;
        let created = svc
            .create_ticket(Request::new(TicketWrite {
                ticket_id: 0,
                encrypted_body: b"x".to_vec(),
                index: Some(sample_proto_index(1)),
            }))
            .await
            .unwrap()
            .into_inner();

        svc.soft_delete_ticket(Request::new(TicketLookup {
            ticket_id: created.ticket_id,
            include_deleted: false,
        }))
        .await
        .expect("soft delete");

        // Hidden from a normal read…
        assert!(
            svc.get_ticket(Request::new(TicketLookup {
                ticket_id: created.ticket_id,
                include_deleted: false,
            }))
            .await
            .is_err()
        );
        // …but the audit row is retained.
        let got = svc
            .get_ticket(Request::new(TicketLookup {
                ticket_id: created.ticket_id,
                include_deleted: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(got.deleted);
    }

    #[tokio::test]
    async fn create_then_get_user_roundtrip_and_soft_delete() {
        let svc = create_service().await;
        let idx = db::UserIndexFields {
            username: "alice".to_string(),
            email: "alice@example.com".to_string(),
            role: 2,
        };
        svc.create_user(Request::new(UserWrite {
            user_uuid: "u-1".to_string(),
            encrypted_body: b"ubody".to_vec(),
            index: Some(idx),
        }))
        .await
        .expect("create user");

        let got = svc
            .get_user(Request::new(UserLookup {
                user_uuid: "u-1".to_string(),
                include_deleted: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.encrypted_body, b"ubody");

        svc.soft_delete_user(Request::new(UserLookup {
            user_uuid: "u-1".to_string(),
            include_deleted: false,
        }))
        .await
        .expect("soft delete user");
        assert!(
            svc.get_user(Request::new(UserLookup {
                user_uuid: "u-1".to_string(),
                include_deleted: false,
            }))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn database_service_reports_health_and_cluster_status() {
        let svc = create_service().await;

        let health = svc
            .health(Request::new(HealthRequest {}))
            .await
            .expect("health")
            .into_inner();
        assert_eq!(health.node_id, "1");
        assert!(!health.role.is_empty());

        let status = svc
            .cluster_status(Request::new(ClusterStatusRequest {}))
            .await
            .expect("cluster status")
            .into_inner();
        assert!(status.member_ids.contains(&"1".to_string()));
    }
}
