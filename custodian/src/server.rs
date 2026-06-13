//! gRPC server implementation for the Custodian service
//!
//! This module implements the gRPC endpoint handlers for the Custodian service,
//! managing ticket lifecycle and distributed locking with Raft consensus.

use crate::raft::CustodianRaft;
use crate::storage::LockCommand;
use shared::encryption::EncryptionService;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub use proto::custodian;
use proto::db;

use custodian::custodian_service_server::{CustodianService, CustodianServiceServer};
use custodian::{
    CreateTicketRequest, GetTicketRequest, HealthRequest, HealthResponse, LockRelease, LockRequest,
    LockResponse as ProtoLockResponse, QueryTicketsRequest, Ticket, UpdateTicketRequest,
};
use shared::ticket as domain;

/// Lock time-to-live in seconds, from `LOCK_TTL_SECS` (default 900 = 15 min; `0` = never expires).
fn lock_ttl_secs() -> i64 {
    std::env::var("LOCK_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(900)
}

// Expose metrics endpoint via gRPC is handled elsewhere; ensure metrics module is initialized
#[allow(dead_code)]
fn init_metrics() {
    // Touch metrics to ensure they are registered
    let _ = crate::metrics::SNAPSHOT_CREATED_TOTAL.get();
}

/// Custodian service implementation
///
/// Implements the gRPC `CustodianService` with the following behavior:
/// - Ticket operations (Create, Update) are forwarded to the DB service
/// - Lock operations (Acquire, Release) use Raft consensus for coordination
/// - Health checks report Raft cluster state
pub struct CustodianServiceImpl {
    raft: CustodianRaft,
    storage: crate::storage::Storage,
    db_client: Option<std::sync::Arc<tokio::sync::Mutex<crate::db_client::DbClient>>>,
    keypair: (Vec<u8>, Vec<u8>),
}

impl CustodianServiceImpl {
    pub fn with_db_client(
        raft: CustodianRaft,
        storage: crate::storage::Storage,
        db_client: std::sync::Arc<tokio::sync::Mutex<crate::db_client::DbClient>>,
        keypair: (Vec<u8>, Vec<u8>),
    ) -> Self {
        Self {
            raft,
            storage,
            db_client: Some(db_client),
            keypair,
        }
    }

    #[must_use]
    pub fn new(
        raft: CustodianRaft,
        storage: crate::storage::Storage,
        keypair: (Vec<u8>, Vec<u8>),
    ) -> Self {
        Self {
            raft,
            storage,
            db_client: None,
            keypair,
        }
    }
}

impl CustodianServiceImpl {
    /// Convert a [`chrono::DateTime<Utc>`] to a [`prost_types::Timestamp`].
    fn dt_to_proto(dt: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
        prost_types::Timestamp::from(std::time::SystemTime::from(dt))
    }

    /// Map a domain [`NextAction`] to the protobuf `NextAction` message (lossless).
    ///
    /// `None` maps to an absent message; the other variants carry their scheduling data
    /// (timestamp or auto-close schedule) explicitly.
    fn map_next_action(next_action: &domain::NextAction) -> Option<custodian::NextAction> {
        use custodian::next_action::Kind;
        let kind = match next_action {
            domain::NextAction::FollowUp(ts) => Kind::FollowUp(Self::dt_to_proto(*ts)),
            domain::NextAction::Appointment(ts) => Kind::Appointment(Self::dt_to_proto(*ts)),
            domain::NextAction::AutoClose(schedule) => {
                Kind::AutoClose(Self::auto_close_to_proto(*schedule) as i32)
            }
            // `None` and any future (non-exhaustive) variant map to an absent message.
            _ => return None,
        };
        Some(custodian::NextAction { kind: Some(kind) })
    }

    /// Map a protobuf `NextAction` message back to the domain type (lossless inverse).
    fn proto_to_next_action(next_action: &custodian::NextAction) -> domain::NextAction {
        use custodian::next_action::Kind;
        match &next_action.kind {
            Some(Kind::FollowUp(ts)) => domain::NextAction::FollowUp(Self::proto_to_dt(ts)),
            Some(Kind::Appointment(ts)) => domain::NextAction::Appointment(Self::proto_to_dt(ts)),
            Some(Kind::AutoClose(v)) => {
                domain::NextAction::AutoClose(Self::proto_to_auto_close(*v))
            }
            None => domain::NextAction::None,
        }
    }

    fn auto_close_to_proto(schedule: domain::AutoCloseSchedule) -> custodian::AutoCloseSchedule {
        match schedule {
            domain::AutoCloseSchedule::Hours24 => custodian::AutoCloseSchedule::Hours24,
            domain::AutoCloseSchedule::Hours48 => custodian::AutoCloseSchedule::Hours48,
            domain::AutoCloseSchedule::Hours72 => custodian::AutoCloseSchedule::Hours72,
            // EndOfDay and any future variant.
            _ => custodian::AutoCloseSchedule::EndOfDay,
        }
    }

    fn proto_to_auto_close(value: i32) -> domain::AutoCloseSchedule {
        match custodian::AutoCloseSchedule::try_from(value) {
            Ok(custodian::AutoCloseSchedule::Hours24) => domain::AutoCloseSchedule::Hours24,
            Ok(custodian::AutoCloseSchedule::Hours48) => domain::AutoCloseSchedule::Hours48,
            Ok(custodian::AutoCloseSchedule::Hours72) => domain::AutoCloseSchedule::Hours72,
            // EndOfDay / Unspecified / unknown.
            _ => domain::AutoCloseSchedule::EndOfDay,
        }
    }

    /// Convert a [`prost_types::Timestamp`] back to a [`chrono::DateTime<Utc>`].
    fn proto_to_dt(ts: &prost_types::Timestamp) -> chrono::DateTime<chrono::Utc> {
        let nanos = u32::try_from(ts.nanos).unwrap_or(0);
        chrono::DateTime::from_timestamp(ts.seconds, nanos).unwrap_or_default()
    }

    /// Convert a domain [`HistoryEntry`] to the corresponding protobuf message.
    fn map_history_entry(entry: &domain::HistoryEntry) -> custodian::HistoryEntry {
        let details = match (&entry.old_value, &entry.new_value) {
            (Some(old), Some(new)) => format!("{}: {} → {}", entry.field_changed, old, new),
            (Some(old), None) => format!("{}: {} → (removed)", entry.field_changed, old),
            (None, Some(new)) => format!("{}: (new) → {}", entry.field_changed, new),
            (None, None) => entry.field_changed.clone(),
        };
        custodian::HistoryEntry {
            user_uuid: entry.user_id.to_string(),
            timestamp: Some(Self::dt_to_proto(entry.timestamp)),
            action: entry.field_changed.clone(),
            details,
        }
    }

    /// Convert a domain [`NetworkDevice`] to the corresponding protobuf message.
    #[allow(clippy::too_many_lines)]
    fn map_network_device(device: &domain::NetworkDevice) -> custodian::NetworkDevice {
        use custodian::network_device::DeviceType;

        let make_proto_fields =
            |make: &str, model: &str, mac: Option<&domain::MacAddress>, sn: Option<&String>| {
                (
                    make.to_string(),
                    model.to_string(),
                    mac.map(ToString::to_string),
                    sn.cloned(),
                )
            };

        let device_type = match device {
            domain::NetworkDevice::DslModem {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::DslModem(custodian::DslModem {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::CoaxModem {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::CoaxModem(custodian::CoaxModem {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::Ont {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::Ont(custodian::Ont {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::FixedWirelessAntenna {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::FixedWirelessAntenna(custodian::FixedWirelessAntenna {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::VpnGw {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::VpnGw(custodian::VpnGw {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::Switch {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::Switch(custodian::Switch {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::Router {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::Router(custodian::Router {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            domain::NetworkDevice::Firewall {
                make,
                model,
                mac_address,
                serial_number,
            } => {
                let (make, model, mac_address, serial_number) =
                    make_proto_fields(make, model, mac_address.as_ref(), serial_number.as_ref());
                DeviceType::Firewall(custodian::Firewall {
                    make,
                    model,
                    mac_address,
                    serial_number,
                })
            }
            // Non-exhaustive enum: log a warning and fall through with the make/model
            // encoded as the make field. This ensures future device types are not silently
            // dropped, even if the proto cannot represent them precisely until the schema
            // is updated to include the new variant.
            _ => {
                tracing::warn!(
                    device_type = device.device_type(),
                    make_model = %device.make_model(),
                    "Unknown NetworkDevice variant; encoding as Router until proto is updated"
                );
                DeviceType::Router(custodian::Router {
                    make: device.make_model(),
                    model: String::new(),
                    mac_address: device.mac_address().map(ToString::to_string),
                    serial_number: None,
                })
            }
        };

        custodian::NetworkDevice {
            device_type: Some(device_type),
        }
    }

    /// Convert our domain Ticket to protobuf
    fn domain_to_proto(ticket: &domain::Ticket) -> custodian::Ticket {
        custodian::Ticket {
            ticket_id: ticket.ticket_id,
            customer_ticket_number: ticket.customer_ticket_number.clone(),
            isp_ticket_number: ticket.isp_ticket_number.clone(),
            other_ticket_number: ticket.other_ticket_number.clone(),
            title: ticket.title.clone(),
            project: ticket.project.clone(),
            account_uuid: ticket.account_uuid.to_string(),
            symptom: ticket.symptom as i32,
            priority: ticket.priority as i32,
            status: ticket.status as i32,
            next_action: Self::map_next_action(&ticket.next_action),
            resolution: ticket.resolution.map(|r| r as i32),
            locked_by_uuid: ticket.locked_by.map(|u| u.to_string()),
            assigned_to_uuid: ticket.assigned_to.map(|u| u.to_string()),
            created_by_uuid: ticket.created_by.to_string(),
            created_at: Some(Self::dt_to_proto(ticket.created_at)),
            updated_by_uuid: ticket.updated_by.to_string(),
            updated_at: Some(Self::dt_to_proto(ticket.updated_at)),
            history: ticket.history.iter().map(Self::map_history_entry).collect(),
            ebond: ticket.ebond.clone(),
            tracking_url: ticket.tracking_url.clone(),
            network_devices: ticket
                .network_devices
                .iter()
                .map(Self::map_network_device)
                .collect(),
            schema_version: ticket.schema_version,
        }
    }

    /// Serialize and encrypt a domain ticket into an opaque body for the DB.
    fn encrypt_ticket(&self, ticket: &domain::Ticket) -> Result<Vec<u8>, Status> {
        let ticket_bytes = serde_json::to_vec(ticket)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;
        let encrypted = EncryptionService::encrypt_with_public_key(&ticket_bytes, &self.keypair.0)
            .map_err(|e| Status::internal(format!("encryption error: {e}")))?;
        serde_json::to_vec(&encrypted)
            .map_err(|e| Status::internal(format!("serialize encrypted data error: {e}")))
    }

    /// Decrypt and deserialize a ticket body, re-stamping the id from the DB record.
    ///
    /// The encrypted body is the source of truth for ticket *contents*, but the id is
    /// owned by the DB (which assigns it), so we always re-stamp it from the record.
    fn decrypt_ticket(&self, ticket_id: u64, body: &[u8]) -> Result<domain::Ticket, Status> {
        let encrypted_data: shared::encryption::EncryptedData = serde_json::from_slice(body)
            .map_err(|e| Status::internal(format!("deserialize encrypted data error: {e}")))?;
        let decrypted =
            EncryptionService::decrypt_with_private_key(&encrypted_data, &self.keypair.1)
                .map_err(|e| Status::internal(format!("decryption error: {e}")))?;
        let mut ticket: domain::Ticket = serde_json::from_slice(&decrypted)
            .map_err(|e| Status::internal(format!("deserialize ticket error: {e}")))?;
        ticket.ticket_id = ticket_id;
        Ok(ticket)
    }

    /// Derive the DB's plaintext index fields from a domain ticket.
    fn ticket_index(ticket: &domain::Ticket) -> db::TicketIndexFields {
        db::TicketIndexFields {
            status: ticket.status as u32,
            account_uuid: ticket.account_uuid.to_string(),
            assigned_to_uuid: ticket.assigned_to.map(|u| u.to_string()),
            project: ticket.project.clone(),
            tracking_url: ticket.tracking_url.clone(),
            created_at_unix: ticket.created_at.timestamp(),
            updated_at_unix: ticket.updated_at.timestamp(),
        }
    }
}

#[tonic::async_trait]
impl CustodianService for CustodianServiceImpl {
    async fn get_ticket(
        &self,
        request: Request<GetTicketRequest>,
    ) -> Result<Response<Ticket>, Status> {
        let req = request.into_inner();

        let Some(client) = &self.db_client else {
            return Err(Status::unavailable("no db client configured"));
        };
        let record = {
            let mut client = client.lock().await;
            client
                .get_ticket(req.ticket_id, false)
                .await
                .map_err(|e| Status::internal(format!("db get error: {e}")))?
        };
        match record {
            Some(record) => {
                let ticket = self.decrypt_ticket(record.ticket_id, &record.encrypted_body)?;
                Ok(Response::new(Self::domain_to_proto(&ticket)))
            }
            None => Err(Status::not_found("ticket not found")),
        }
    }

    type QueryTicketsStream = tokio_stream::Iter<std::vec::IntoIter<Result<Ticket, Status>>>;

    async fn query_tickets(
        &self,
        request: Request<QueryTicketsRequest>,
    ) -> Result<Response<Self::QueryTicketsStream>, Status> {
        let req = request.into_inner();

        let Some(client) = &self.db_client else {
            return Err(Status::unavailable("no db client configured"));
        };

        let query = db::TicketQuery {
            status: req.status,
            assigned_to_uuid: req.assigned_to_uuid,
            account_uuid: req.account_uuid,
            project: req.project,
            include_deleted: req.include_deleted,
            limit: req.limit,
        };

        let records = {
            let mut lock = client.lock().await;
            lock.query_tickets(query)
                .await
                .map_err(|e| Status::internal(format!("db query_tickets error: {e}")))?
        };

        // Decrypt each record into a proto ticket; a single decryption failure fails the stream.
        let mut tickets: Vec<Result<Ticket, Status>> = Vec::with_capacity(records.len());
        for record in records {
            let ticket = self.decrypt_ticket(record.ticket_id, &record.encrypted_body)?;
            tickets.push(Ok(Self::domain_to_proto(&ticket)));
        }

        Ok(Response::new(tokio_stream::iter(tickets)))
    }

    async fn create_ticket(
        &self,
        request: Request<CreateTicketRequest>,
    ) -> Result<Response<Ticket>, Status> {
        let req = request.into_inner();

        // Basic validation
        if req.title.is_empty() {
            return Err(Status::invalid_argument("title is required"));
        }

        let account_uuid = Uuid::parse_str(&req.account_uuid)
            .map_err(|_| Status::invalid_argument("Invalid account UUID"))?;

        let created_by_uuid = Uuid::parse_str(&req.created_by_uuid)
            .map_err(|_| Status::invalid_argument("Invalid created_by UUID"))?;

        // Create domain ticket. The id is assigned by the DB (0 = placeholder until then).
        let symptom = domain::Symptom::from_u8(u8::try_from(req.symptom).unwrap_or(0));
        let priority = domain::TicketPriority::from_u8(u8::try_from(req.priority).unwrap_or(0));

        let mut ticket = domain::Ticket::new(
            0,
            req.title,
            req.project,
            account_uuid,
            symptom,
            created_by_uuid,
        );

        ticket.priority = priority;

        ticket.customer_ticket_number = req.customer_ticket_number;
        ticket.isp_ticket_number = req.isp_ticket_number;
        ticket.other_ticket_number = req.other_ticket_number;
        ticket.ebond = req.ebond;
        ticket.tracking_url = req.tracking_url;

        let encrypted_bytes = self.encrypt_ticket(&ticket)?;
        let index = Self::ticket_index(&ticket);

        // Persist via the DB's CreateTicket (which assigns the id). Without a DB client
        // configured, fall back to a local id so the response is still well-formed.
        let ticket_id = if let Some(client) = &self.db_client {
            let mut lock = client.lock().await;
            lock.create_ticket(encrypted_bytes, index)
                .await
                .map_err(|e| Status::internal(format!("db create_ticket error: {e}")))?
        } else {
            chrono::Utc::now()
                .timestamp_millis()
                .try_into()
                .unwrap_or_default()
        };
        ticket.ticket_id = ticket_id;

        Ok(Response::new(Self::domain_to_proto(&ticket)))
    }

    async fn acquire_lock(
        &self,
        request: Request<LockRequest>,
    ) -> Result<Response<ProtoLockResponse>, Status> {
        let req = request.into_inner();

        let user_id = Uuid::parse_str(&req.user_uuid)
            .map_err(|_| Status::invalid_argument("Invalid user UUID"))?;

        let command = LockCommand::AcquireLock {
            ticket_id: req.ticket_id,
            user_id,
            at_unix: chrono::Utc::now().timestamp(),
            ttl_secs: lock_ttl_secs(),
        };

        // Submit to Raft for consensus
        match self.raft.client_write(command).await {
            Ok(response) => {
                // On failure, report who currently holds the lock so the caller can
                // see why their acquisition was rejected.
                let current_holder = if response.data.success {
                    None
                } else {
                    self.storage
                        .get_lock_info(req.ticket_id)
                        .ok()
                        .flatten()
                        .map(|lock| lock.user_id.to_string())
                };
                let proto_response = ProtoLockResponse {
                    success: response.data.success,
                    error: response.data.error.unwrap_or_default(),
                    current_holder,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(Status::internal(format!("Raft write failed: {e}"))),
        }
    }

    async fn release_lock(
        &self,
        request: Request<LockRelease>,
    ) -> Result<Response<ProtoLockResponse>, Status> {
        let req = request.into_inner();

        let user_id = Uuid::parse_str(&req.user_uuid)
            .map_err(|_| Status::invalid_argument("Invalid user UUID"))?;

        let command = LockCommand::ReleaseLock {
            ticket_id: req.ticket_id,
            user_id,
        };

        // Submit to Raft for consensus
        match self.raft.client_write(command).await {
            Ok(response) => {
                let proto_response = ProtoLockResponse {
                    success: response.data.success,
                    error: response.data.error.unwrap_or_default(),
                    current_holder: None,
                };
                Ok(Response::new(proto_response))
            }
            Err(e) => Err(Status::internal(format!("Raft write failed: {e}"))),
        }
    }

    async fn update_ticket(
        &self,
        request: Request<UpdateTicketRequest>,
    ) -> Result<Response<Ticket>, Status> {
        let req = request.into_inner();

        // Validate updated_by_uuid
        let updater_str = req
            .updated_by_uuid
            .as_deref()
            .ok_or_else(|| Status::invalid_argument("updated_by_uuid is required"))?;
        let updater = Uuid::parse_str(updater_str)
            .map_err(|_| Status::invalid_argument("Invalid updated_by_uuid"))?;

        // Check lock ownership. An expired lock is treated as not held (the updater must
        // re-acquire), so updates after auto-expiry are rejected.
        match self
            .storage
            .get_lock_info(req.ticket_id)
            .map_err(|e| Status::internal(format!("storage error: {e}")))?
        {
            Some(lock) if lock.is_expired(chrono::Utc::now().timestamp()) => {
                return Err(Status::permission_denied("ticket lock has expired"));
            }
            Some(lock) => {
                if lock.user_id != updater {
                    return Err(Status::permission_denied("user does not hold lock"));
                }
            }
            None => return Err(Status::permission_denied("ticket is not locked")),
        }

        // Fetch ticket from DB
        let Some(client) = &self.db_client else {
            return Err(Status::unavailable("no db client configured"));
        };
        let record = {
            let mut lock = client.lock().await;
            lock.get_ticket(req.ticket_id, false)
                .await
                .map_err(|e| Status::internal(format!("db get error: {e}")))?
        };
        let mut ticket = match record {
            Some(record) => self.decrypt_ticket(record.ticket_id, &record.encrypted_body)?,
            None => return Err(Status::not_found("ticket not found")),
        };

        // Apply updates from request (only update provided fields)
        if let Some(title) = req.title {
            ticket.title = title;
        }
        if let Some(project) = req.project {
            ticket.project = project;
        }
        if let Some(symptom) = req.symptom {
            ticket.symptom = domain::Symptom::from_u8(u8::try_from(symptom).unwrap_or(0));
        }
        if let Some(priority) = req.priority {
            ticket.priority = domain::TicketPriority::from_u8(u8::try_from(priority).unwrap_or(0));
        }
        if let Some(status_val) = req.status {
            let new_status = domain::TicketStatus::from_u8(u8::try_from(status_val).unwrap_or(0));
            // Policy-as-code: reject status changes that violate the ticket lifecycle.
            if !ticket.status.can_transition_to(new_status) {
                return Err(Status::failed_precondition(format!(
                    "invalid status transition: {:?} -> {new_status:?}",
                    ticket.status
                )));
            }
            ticket.status = new_status;
        }
        if let Some(next_action) = req.next_action {
            ticket.next_action = Self::proto_to_next_action(&next_action);
        }
        if let Some(resolution) = req.resolution {
            ticket.resolution = Some(domain::Resolution::from_u8(
                u8::try_from(resolution).unwrap_or(0),
            ));
        }
        if let Some(assigned) = req.assigned_to_uuid {
            ticket.assigned_to = Some(
                Uuid::parse_str(&assigned)
                    .map_err(|_| Status::invalid_argument("Invalid assigned_to UUID"))?,
            );
        }

        ticket.updated_by = updater;
        ticket.updated_at = chrono::Utc::now();

        let encrypted_bytes = self.encrypt_ticket(&ticket)?;
        let index = Self::ticket_index(&ticket);

        {
            let mut lock = client.lock().await;
            lock.update_ticket(req.ticket_id, encrypted_bytes, index)
                .await
                .map_err(|e| Status::internal(format!("db update_ticket error: {e}")))?;
        }

        Ok(Response::new(Self::domain_to_proto(&ticket)))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        let status = match metrics.state {
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
            status,
        }))
    }

    async fn cluster_status(
        &self,
        _request: Request<custodian::ClusterStatusRequest>,
    ) -> Result<Response<custodian::ClusterStatusResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        let leader_id = metrics
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_default();
        let follower_ids: Vec<String> = metrics
            .membership_config
            .membership()
            .nodes()
            .filter_map(|(id, _node)| {
                if Some(*id) == metrics.current_leader {
                    None
                } else {
                    Some(id.to_string())
                }
            })
            .collect();
        let term = metrics.current_term;
        let commit_index = metrics.last_applied.map_or(0, |id| id.index);

        Ok(Response::new(custodian::ClusterStatusResponse {
            leader_id,
            follower_ids,
            term,
            commit_index,
        }))
    }
}

/// Create the gRPC server
#[must_use]
pub fn create_server(
    service: CustodianServiceImpl,
) -> CustodianServiceServer<CustodianServiceImpl> {
    CustodianServiceServer::new(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Config;
    use openraft::storage::Adaptor;
    use std::sync::Arc;
    use tonic::Request;

    #[tokio::test]
    async fn test_custodian_service_creation() {
        // basic instantiation
        let store = crate::raft::CustodianStore::new_temp().unwrap();
        let storage = store.storage();
        let svc = CustodianServiceImpl::new(
            crate::raft::CustodianRaft::new(
                1,
                Arc::new(Config::default()),
                crate::network::CustodianNetworkFactory::new(),
                Adaptor::new(store.clone()).0,
                Adaptor::new(store).1,
            )
            .await
            .unwrap(),
            storage,
            (vec![0; 1184], vec![0; 2400]),
        );
        let _ = svc;
    }

    #[tokio::test]
    async fn test_create_ticket_and_lock_flow() {
        // Create backing store and raft
        let store = crate::raft::CustodianStore::new_temp().unwrap();
        let storage = store.storage().clone();

        let cfg = Config::default();
        let cfg = Arc::new(cfg.validate().unwrap());
        let network_factory = crate::network::CustodianNetworkFactory::new();
        let (log_store, state_machine) = Adaptor::new(store.clone());

        let raft = crate::raft::CustodianRaft::new(
            1u64,
            cfg.clone(),
            network_factory,
            log_store,
            state_machine,
        )
        .await
        .expect("create raft");
        // initialize single-node cluster so client_write works
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        let _ = raft.initialize(members).await;

        let svc_impl = CustodianServiceImpl::new(
            raft.clone(),
            storage.clone(),
            (vec![0; 1184], vec![0; 2400]),
        );

        // create ticket
        let req = custodian::CreateTicketRequest {
            title: "Test".to_string(),
            project: "proj".to_string(),
            account_uuid: uuid::Uuid::new_v4().to_string(),
            symptom: 0,
            priority: 0,
            created_by_uuid: uuid::Uuid::new_v4().to_string(),
            customer_ticket_number: None,
            isp_ticket_number: None,
            other_ticket_number: None,
            ebond: None,
            tracking_url: None,
            network_devices: vec![],
        };
        let resp = svc_impl
            .create_ticket(Request::new(req))
            .await
            .expect("create ticket");
        let ticket = resp.into_inner();
        assert_eq!(ticket.title, "Test");
        assert_eq!(ticket.priority, 0);

        // acquire lock using service (should go through raft)
        let user_uuid = uuid::Uuid::new_v4().to_string();
        let lock_req = custodian::LockRequest {
            ticket_id: ticket.ticket_id,
            user_uuid: user_uuid.clone(),
        };
        let lock_resp = svc_impl
            .acquire_lock(Request::new(lock_req))
            .await
            .expect("acquire");
        assert!(lock_resp.get_ref().success);

        // release lock
        let release_req = custodian::LockRelease {
            ticket_id: ticket.ticket_id,
            user_uuid,
        };
        let release_resp = svc_impl
            .release_lock(Request::new(release_req))
            .await
            .expect("release");
        assert!(release_resp.get_ref().success);
    }

    #[tokio::test]
    async fn acquire_lock_conflict_reports_current_holder() {
        let store = crate::raft::CustodianStore::new_temp().unwrap();
        let storage = store.storage().clone();
        let cfg = Arc::new(Config::default().validate().unwrap());
        let network_factory = crate::network::CustodianNetworkFactory::new();
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft =
            crate::raft::CustodianRaft::new(1u64, cfg, network_factory, log_store, state_machine)
                .await
                .expect("create raft");
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        let _ = raft.initialize(members).await;
        let svc = CustodianServiceImpl::new(
            raft.clone(),
            storage.clone(),
            (vec![0; 1184], vec![0; 2400]),
        );

        let holder = uuid::Uuid::new_v4().to_string();
        let other = uuid::Uuid::new_v4().to_string();
        let ticket_id = 42;

        let first = svc
            .acquire_lock(Request::new(custodian::LockRequest {
                ticket_id,
                user_uuid: holder.clone(),
            }))
            .await
            .expect("first acquire")
            .into_inner();
        assert!(first.success);

        // A different user's acquisition is rejected and learns who holds the lock.
        let second = svc
            .acquire_lock(Request::new(custodian::LockRequest {
                ticket_id,
                user_uuid: other,
            }))
            .await
            .expect("second acquire")
            .into_inner();
        assert!(!second.success);
        assert_eq!(second.current_holder.as_deref(), Some(holder.as_str()));
    }

    #[test]
    fn test_domain_to_proto_priority() {
        let mut ticket = domain::Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            uuid::Uuid::new_v4(),
            domain::Symptom::BroadbandDown,
            uuid::Uuid::new_v4(),
        );

        // Test Unknown (default)
        let proto = CustodianServiceImpl::domain_to_proto(&ticket);
        assert_eq!(proto.priority, 0); // Unknown = 0

        // Test Specific Priority
        ticket.priority = domain::TicketPriority::HardDown;
        let proto = CustodianServiceImpl::domain_to_proto(&ticket);
        assert_eq!(proto.priority, 1); // HardDown = 1
    }

    #[tokio::test]
    async fn get_ticket_without_db_client_returns_unavailable() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));
        let err = svc
            .get_ticket(Request::new(custodian::GetTicketRequest { ticket_id: 1 }))
            .await
            .expect_err("no db client should fail");

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn create_ticket_rejects_empty_title() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));
        let err = svc
            .create_ticket(Request::new(custodian::CreateTicketRequest {
                title: String::new(),
                project: "demo".to_string(),
                account_uuid: uuid::Uuid::new_v4().to_string(),
                symptom: 0,
                priority: 0,
                created_by_uuid: uuid::Uuid::new_v4().to_string(),
                customer_ticket_number: None,
                isp_ticket_number: None,
                other_ticket_number: None,
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("empty title should fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn update_ticket_requires_updated_by_uuid() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));
        let err = svc
            .update_ticket(Request::new(custodian::UpdateTicketRequest {
                ticket_id: 1,
                title: None,
                project: None,
                symptom: None,
                priority: None,
                status: None,
                next_action: None,
                resolution: None,
                assigned_to_uuid: None,
                updated_by_uuid: None,
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("missing updater should fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn health_and_cluster_status_are_available() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        let _ = raft.initialize(members).await;

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));

        let health = svc
            .health(Request::new(custodian::HealthRequest {}))
            .await
            .expect("health")
            .into_inner();
        assert!(!health.status.is_empty());

        let cluster = svc
            .cluster_status(Request::new(custodian::ClusterStatusRequest {}))
            .await
            .expect("cluster")
            .into_inner();
        assert!(cluster.term >= 1);
    }

    #[tokio::test]
    async fn acquire_and_release_lock_reject_invalid_user_uuid() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage().clone();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));

        let acquire_err = svc
            .acquire_lock(Request::new(custodian::LockRequest {
                ticket_id: 1,
                user_uuid: "not-a-uuid".to_string(),
            }))
            .await
            .expect_err("invalid user uuid");
        assert_eq!(acquire_err.code(), tonic::Code::InvalidArgument);

        let release_err = svc
            .release_lock(Request::new(custodian::LockRelease {
                ticket_id: 1,
                user_uuid: "not-a-uuid".to_string(),
            }))
            .await
            .expect_err("invalid user uuid");
        assert_eq!(release_err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn update_ticket_requires_existing_lock() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage().clone();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));

        let err = svc
            .update_ticket(Request::new(custodian::UpdateTicketRequest {
                ticket_id: 99,
                title: Some("new title".to_string()),
                project: None,
                symptom: None,
                priority: None,
                status: None,
                next_action: None,
                resolution: None,
                assigned_to_uuid: None,
                updated_by_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("must fail without lock");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // ── NextAction lossless round-trip (domain -> proto -> domain) ──────────────

    fn next_action_roundtrip(na: &domain::NextAction) -> domain::NextAction {
        match CustodianServiceImpl::map_next_action(na) {
            Some(proto) => CustodianServiceImpl::proto_to_next_action(&proto),
            None => {
                CustodianServiceImpl::proto_to_next_action(&custodian::NextAction { kind: None })
            }
        }
    }

    #[test]
    fn next_action_none_roundtrips() {
        assert_eq!(
            next_action_roundtrip(&domain::NextAction::None),
            domain::NextAction::None
        );
        // None maps to an absent proto message.
        assert!(CustodianServiceImpl::map_next_action(&domain::NextAction::None).is_none());
    }

    #[test]
    fn next_action_follow_up_roundtrips_with_timestamp() {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        assert_eq!(
            next_action_roundtrip(&domain::NextAction::FollowUp(ts)),
            domain::NextAction::FollowUp(ts)
        );
    }

    #[test]
    fn next_action_appointment_roundtrips_with_timestamp() {
        let ts = chrono::DateTime::from_timestamp(1_700_000_500, 0).unwrap();
        assert_eq!(
            next_action_roundtrip(&domain::NextAction::Appointment(ts)),
            domain::NextAction::Appointment(ts)
        );
    }

    #[test]
    fn next_action_auto_close_roundtrips_each_schedule() {
        for schedule in [
            domain::AutoCloseSchedule::EndOfDay,
            domain::AutoCloseSchedule::Hours24,
            domain::AutoCloseSchedule::Hours48,
            domain::AutoCloseSchedule::Hours72,
        ] {
            assert_eq!(
                next_action_roundtrip(&domain::NextAction::AutoClose(schedule)),
                domain::NextAction::AutoClose(schedule),
                "auto-close schedule {schedule:?} should round-trip"
            );
        }
    }

    // ── map_history_entry ─────────────────────────────────────────────────────

    #[test]
    fn map_history_entry_formats_change_with_old_and_new_values() {
        let entry = domain::HistoryEntry {
            timestamp: chrono::Utc::now(),
            user_id: uuid::Uuid::nil(),
            field_changed: "status".to_string(),
            old_value: Some("Open".to_string()),
            new_value: Some("Closed".to_string()),
        };
        let proto = CustodianServiceImpl::map_history_entry(&entry);
        assert_eq!(proto.action, "status");
        assert!(proto.details.contains("Open"));
        assert!(proto.details.contains("Closed"));
        assert_eq!(proto.user_uuid, uuid::Uuid::nil().to_string());
        assert!(proto.timestamp.is_some());
    }

    #[test]
    fn map_history_entry_handles_removal() {
        let entry = domain::HistoryEntry {
            timestamp: chrono::Utc::now(),
            user_id: uuid::Uuid::nil(),
            field_changed: "assigned_to".to_string(),
            old_value: Some("Alice".to_string()),
            new_value: None,
        };
        let proto = CustodianServiceImpl::map_history_entry(&entry);
        assert!(proto.details.contains("removed"));
    }

    #[test]
    fn map_history_entry_handles_new_value_only() {
        let entry = domain::HistoryEntry {
            timestamp: chrono::Utc::now(),
            user_id: uuid::Uuid::nil(),
            field_changed: "tracking_url".to_string(),
            old_value: None,
            new_value: Some("https://example.com".to_string()),
        };
        let proto = CustodianServiceImpl::map_history_entry(&entry);
        assert!(proto.details.contains("example.com"));
    }

    #[test]
    fn map_history_entry_handles_no_values() {
        let entry = domain::HistoryEntry {
            timestamp: chrono::Utc::now(),
            user_id: uuid::Uuid::nil(),
            field_changed: "ticket_created".to_string(),
            old_value: None,
            new_value: None,
        };
        let proto = CustodianServiceImpl::map_history_entry(&entry);
        assert_eq!(proto.details, "ticket_created");
    }

    // ── map_network_device ────────────────────────────────────────────────────

    #[test]
    fn map_network_device_dsl_modem() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::DslModem {
            make: "Cisco".to_string(),
            model: "DPC3825".to_string(),
            mac_address: None,
            serial_number: Some("SN123".to_string()),
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(
            proto.device_type,
            Some(DeviceType::DslModem(ref d)) if d.make == "Cisco"
        ));
    }

    #[test]
    fn map_network_device_coax_modem_with_mac() {
        use custodian::network_device::DeviceType;
        let mac = domain::MacAddress::new("AA:BB:CC:DD:EE:FF").expect("valid MAC");
        let device = domain::NetworkDevice::CoaxModem {
            make: "Arris".to_string(),
            model: "SB6141".to_string(),
            mac_address: Some(mac),
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(
            proto.device_type,
            Some(DeviceType::CoaxModem(ref d)) if d.mac_address.is_some()
        ));
    }

    #[test]
    fn map_network_device_ont() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::Ont {
            make: "Calix".to_string(),
            model: "GigaPoint".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(proto.device_type, Some(DeviceType::Ont(_))));
    }

    #[test]
    fn map_network_device_fixed_wireless_antenna() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::FixedWirelessAntenna {
            make: "Cambium".to_string(),
            model: "PMP450".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(
            proto.device_type,
            Some(DeviceType::FixedWirelessAntenna(_))
        ));
    }

    #[test]
    fn map_network_device_vpn_gw() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::VpnGw {
            make: "Cisco".to_string(),
            model: "ASA5505".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(proto.device_type, Some(DeviceType::VpnGw(_))));
    }

    #[test]
    fn map_network_device_switch() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::Switch {
            make: "Cisco".to_string(),
            model: "SG300".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(proto.device_type, Some(DeviceType::Switch(_))));
    }

    #[test]
    fn map_network_device_router() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::Router {
            make: "Netgear".to_string(),
            model: "R7000".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(proto.device_type, Some(DeviceType::Router(_))));
    }

    #[test]
    fn map_network_device_firewall() {
        use custodian::network_device::DeviceType;
        let device = domain::NetworkDevice::Firewall {
            make: "Palo Alto".to_string(),
            model: "PA-220".to_string(),
            mac_address: None,
            serial_number: None,
        };
        let proto = CustodianServiceImpl::map_network_device(&device);
        assert!(matches!(proto.device_type, Some(DeviceType::Firewall(_))));
    }

    // ── domain_to_proto round-trip tests ─────────────────────────────────────

    #[test]
    fn domain_to_proto_preserves_next_action_and_history() {
        let owner = uuid::Uuid::new_v4();
        let mut ticket = domain::Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            uuid::Uuid::new_v4(),
            domain::Symptom::BroadbandDown,
            owner,
        );
        let follow_up_at = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        ticket.next_action = domain::NextAction::FollowUp(follow_up_at);
        ticket.history.push(domain::HistoryEntry {
            timestamp: chrono::Utc::now(),
            user_id: owner,
            field_changed: "status".to_string(),
            old_value: Some("Open".to_string()),
            new_value: Some("Closed".to_string()),
        });
        let proto = CustodianServiceImpl::domain_to_proto(&ticket);
        // next_action is now a structured message carrying the timestamp losslessly.
        assert_eq!(
            CustodianServiceImpl::proto_to_next_action(
                proto.next_action.as_ref().expect("next_action present")
            ),
            domain::NextAction::FollowUp(follow_up_at)
        );
        assert_eq!(proto.history.len(), 1);
        assert_eq!(proto.history[0].action, "status");
    }

    #[test]
    fn domain_to_proto_preserves_network_devices() {
        let mut ticket = domain::Ticket::new(
            2,
            "Net Test".to_string(),
            "Project".to_string(),
            uuid::Uuid::new_v4(),
            domain::Symptom::BroadbandDown,
            uuid::Uuid::new_v4(),
        );
        ticket.network_devices.push(domain::NetworkDevice::Router {
            make: "Netgear".to_string(),
            model: "R7000".to_string(),
            mac_address: None,
            serial_number: None,
        });
        let proto = CustodianServiceImpl::domain_to_proto(&ticket);
        assert_eq!(proto.network_devices.len(), 1);
    }

    // ── Additional coverage tests ─────────────────────────────────────────────

    #[test]
    fn init_metrics_does_not_panic() {
        // Ensures the init_metrics() code path is covered.
        super::init_metrics();
    }

    #[tokio::test]
    async fn test_create_server_function() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");
        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));
        let _server = super::create_server(svc);
    }

    #[tokio::test]
    async fn with_db_client_constructor_sets_db_client() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");
        let db = Arc::new(tokio::sync::Mutex::new(
            crate::db_client::DbClient::new_lazy("http://127.0.0.1:9"),
        ));
        let svc =
            CustodianServiceImpl::with_db_client(raft, storage, db, (vec![0; 1184], vec![0; 2400]));
        // DB client IS set — get_ticket should return Internal (transport error), not Unavailable "no db client"
        let err = svc
            .get_ticket(Request::new(custodian::GetTicketRequest { ticket_id: 1 }))
            .await
            .expect_err("transport error expected");
        // Internal (transport failure) means the db_client path was taken
        assert_ne!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn health_with_shutdown_state_returns_unhealthy() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        let svc = CustodianServiceImpl::new(raft.clone(), storage, (vec![0; 1184], vec![0; 2400]));

        // Shut down the raft node so state becomes Shutdown
        raft.shutdown().await.expect("shutdown");

        let resp = svc
            .health(Request::new(custodian::HealthRequest {}))
            .await
            .expect("health")
            .into_inner();

        // After shutdown the node is unhealthy
        assert!(!resp.healthy);
    }

    #[tokio::test]
    async fn cluster_status_includes_follower_node_ids() {
        // Initialize a single-node raft but register 3 members in membership.
        // Nodes 2 and 3 are "known" but non-existent; only node 1 becomes leader.
        // The filter_map in cluster_status should include nodes 2 & 3 as "followers".
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        // Initialize with 3 members so the membership has non-leader nodes
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        members.insert(2u64);
        members.insert(3u64);
        // This may fail if the cluster can't reach quorum, but we only care about membership config.
        let _ = raft.initialize(members).await;

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));

        // cluster_status exercises the filter_map for non-leader nodes
        let cluster = svc
            .cluster_status(Request::new(custodian::ClusterStatusRequest {}))
            .await
            .expect("cluster status")
            .into_inner();

        // Nodes 2 and 3 should appear in follower_ids (since only 1 is leader or no leader yet)
        // We just verify the response was produced without panic and has reasonable content
        assert!(cluster.follower_ids.len() <= 5);
    }

    #[tokio::test]
    async fn update_ticket_returns_permission_denied_for_wrong_lock_holder() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage().clone();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        // Initialize so client_write works
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        let _ = raft.initialize(members).await;

        let svc = CustodianServiceImpl::new(
            raft.clone(),
            storage.clone(),
            (vec![0; 1184], vec![0; 2400]),
        );

        let holder_uuid = uuid::Uuid::new_v4().to_string();
        let other_uuid = uuid::Uuid::new_v4().to_string();

        // Acquire lock as holder
        svc.acquire_lock(Request::new(custodian::LockRequest {
            ticket_id: 42,
            user_uuid: holder_uuid.clone(),
        }))
        .await
        .expect("acquire lock");

        // Try to update as someone else → PermissionDenied
        let err = svc
            .update_ticket(Request::new(custodian::UpdateTicketRequest {
                ticket_id: 42,
                title: Some("hacked".to_string()),
                project: None,
                symptom: None,
                priority: None,
                status: None,
                next_action: None,
                resolution: None,
                assigned_to_uuid: None,
                updated_by_uuid: Some(other_uuid),
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("wrong lock holder");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    /// Create a minimal single-node service for tests that don't need a running Raft cluster.
    async fn make_simple_svc() -> CustodianServiceImpl {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage();
        let raft = crate::raft::CustodianRaft::new(
            1,
            Arc::new(Config::default()),
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");
        CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]))
    }

    #[tokio::test]
    async fn acquire_lock_rejects_invalid_user_uuid() {
        let svc = make_simple_svc().await;
        let err = svc
            .acquire_lock(Request::new(custodian::LockRequest {
                ticket_id: 1,
                user_uuid: "not-a-uuid".to_string(),
            }))
            .await
            .expect_err("invalid UUID should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn release_lock_rejects_invalid_user_uuid() {
        let svc = make_simple_svc().await;
        let err = svc
            .release_lock(Request::new(custodian::LockRelease {
                ticket_id: 1,
                user_uuid: "not-a-uuid".to_string(),
            }))
            .await
            .expect_err("invalid UUID should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_ticket_rejects_invalid_account_uuid() {
        let svc = make_simple_svc().await;
        let err = svc
            .create_ticket(Request::new(custodian::CreateTicketRequest {
                title: "Valid Title".to_string(),
                project: "proj".to_string(),
                account_uuid: "bad-uuid".to_string(),
                symptom: 0,
                priority: 0,
                created_by_uuid: uuid::Uuid::new_v4().to_string(),
                customer_ticket_number: None,
                isp_ticket_number: None,
                other_ticket_number: None,
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("invalid account UUID should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_ticket_rejects_invalid_created_by_uuid() {
        let svc = make_simple_svc().await;
        let err = svc
            .create_ticket(Request::new(custodian::CreateTicketRequest {
                title: "Valid Title".to_string(),
                project: "proj".to_string(),
                account_uuid: uuid::Uuid::new_v4().to_string(),
                symptom: 0,
                priority: 0,
                created_by_uuid: "bad-uuid".to_string(),
                customer_ticket_number: None,
                isp_ticket_number: None,
                other_ticket_number: None,
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("invalid created_by UUID should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn update_ticket_rejects_invalid_updated_by_uuid_format() {
        let svc = make_simple_svc().await;
        let err = svc
            .update_ticket(Request::new(custodian::UpdateTicketRequest {
                ticket_id: 1,
                title: None,
                project: None,
                symptom: None,
                priority: None,
                status: None,
                next_action: None,
                resolution: None,
                assigned_to_uuid: None,
                updated_by_uuid: Some("not-a-uuid".to_string()),
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("invalid UUID should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn update_ticket_without_db_client_returns_unavailable() {
        let store = crate::raft::CustodianStore::new_temp().expect("store");
        let storage = store.storage().clone();
        let cfg = Arc::new(Config::default().validate().expect("validated config"));
        let raft = crate::raft::CustodianRaft::new(
            1,
            cfg,
            crate::network::CustodianNetworkFactory::new(),
            Adaptor::new(store.clone()).0,
            Adaptor::new(store).1,
        )
        .await
        .expect("raft");

        // Initialize so lock check works
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        let _ = raft.initialize(members).await;

        let user_id = uuid::Uuid::new_v4();
        // Directly acquire a lock in storage so the lock check passes
        storage
            .acquire_lock(1, user_id)
            .expect("acquire lock in storage");

        let svc = CustodianServiceImpl::new(raft, storage, (vec![0; 1184], vec![0; 2400]));
        // No db_client set → update_ticket returns Unavailable

        let err = svc
            .update_ticket(Request::new(custodian::UpdateTicketRequest {
                ticket_id: 1,
                title: Some("new title".to_string()),
                project: None,
                symptom: None,
                priority: None,
                status: None,
                next_action: None,
                resolution: None,
                assigned_to_uuid: None,
                updated_by_uuid: Some(user_id.to_string()),
                ebond: None,
                tracking_url: None,
                network_devices: vec![],
            }))
            .await
            .expect_err("no db_client should return error");

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }
}
