//! gRPC client implementations for LBRP service communication

use anyhow::Result;
use arc_swap::ArcSwap;
use std::sync::Arc;
use tonic::transport::Channel;

pub use proto::{admin, auth, custodian};

type CustodianGrpc = custodian::custodian_service_client::CustodianServiceClient<Channel>;
type AuthGrpc = auth::auth_service_client::AuthServiceClient<Channel>;
type AdminGrpc = admin::admin_service_client::AdminServiceClient<Channel>;

/// Custodian service client.
///
/// The inner gRPC client is held in an [`ArcSwap`] so the `services.toml` reload task can swap in a
/// new connection atomically and lock-free; per-RPC access just loads and clones the current client
/// (cheap — it's a handle over a multiplexing `Channel`), so concurrent requests are never
/// serialized through a mutex.
#[derive(Clone)]
pub struct CustodianClient {
    client: Arc<ArcSwap<CustodianGrpc>>,
}

impl CustodianClient {
    /// Snapshot the current gRPC client (cheap clone of a `Channel` handle).
    fn current(&self) -> CustodianGrpc {
        (**self.client.load()).clone()
    }

    /// Wrap an existing gRPC client over a channel.
    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: Arc::new(ArcSwap::from_pointee(CustodianGrpc::new(channel))),
        }
    }

    pub async fn connect(addr: String) -> Result<Self> {
        Ok(Self::from_channel(proto::tls::connect(&addr).await?))
    }

    /// Re-establish the underlying channel to a (possibly new) address and swap it in atomically.
    ///
    /// Used by the `services.toml` reload task to hot-redirect traffic when an endpoint changes
    /// without restarting LBRP. Uses a *lazy* channel so the reload loop never blocks on a
    /// handshake; the connection is established on the next RPC. In-flight calls finish on the old
    /// client; subsequent `current()` loads return the new one.
    pub fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        self.client.store(Arc::new(CustodianGrpc::new(channel)));
        Ok(())
    }

    pub async fn create_ticket(
        &self,
        req: custodian::CreateTicketRequest,
    ) -> Result<custodian::Ticket> {
        let mut client = self.current();
        let response = client.create_ticket(req).await?;
        Ok(response.into_inner())
    }

    // Deferred client API: the custodian exposes lock RPCs, but LBRP does not yet surface
    // REST lock endpoints (locking is currently internal to the custodian). Kept so the wiring
    // is ready when those endpoints land; remove the allow once they're used.
    #[allow(dead_code)]
    pub async fn acquire_lock(
        &self,
        req: custodian::LockRequest,
    ) -> Result<custodian::LockResponse> {
        let mut client = self.current();
        let response = client.acquire_lock(req).await?;
        Ok(response.into_inner())
    }

    #[allow(dead_code)]
    pub async fn release_lock(
        &self,
        req: custodian::LockRelease,
    ) -> Result<custodian::LockResponse> {
        let mut client = self.current();
        let response = client.release_lock(req).await?;
        Ok(response.into_inner())
    }

    pub async fn update_ticket(
        &self,
        req: custodian::UpdateTicketRequest,
    ) -> Result<custodian::Ticket> {
        let mut client = self.current();
        let response = client.update_ticket(req).await?;
        Ok(response.into_inner())
    }

    pub async fn get_ticket(&self, req: custodian::GetTicketRequest) -> Result<custodian::Ticket> {
        let mut client = self.current();
        let response = client.get_ticket(req).await?;
        Ok(response.into_inner())
    }

    pub async fn query_tickets(
        &self,
        req: custodian::QueryTicketsRequest,
    ) -> Result<Vec<custodian::Ticket>> {
        let mut client = self.current();
        let mut stream = client.query_tickets(req).await?.into_inner();
        let mut tickets = Vec::new();
        while let Some(ticket) = stream.message().await? {
            tickets.push(ticket);
        }
        Ok(tickets)
    }

    pub async fn cluster_status(&self) -> Result<custodian::ClusterStatusResponse> {
        let mut client = self.current();
        let response = client
            .cluster_status(custodian::ClusterStatusRequest {})
            .await?;
        Ok(response.into_inner())
    }
}

/// Auth service client. See [`CustodianClient`] for the `ArcSwap` rationale.
#[derive(Clone)]
pub struct AuthClient {
    client: Arc<ArcSwap<AuthGrpc>>,
}

impl AuthClient {
    fn current(&self) -> AuthGrpc {
        (**self.client.load()).clone()
    }

    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: Arc::new(ArcSwap::from_pointee(AuthGrpc::new(channel))),
        }
    }

    pub async fn connect(addr: String) -> Result<Self> {
        Ok(Self::from_channel(proto::tls::connect(&addr).await?))
    }

    /// Re-establish the channel to a new address and swap it in atomically (see
    /// [`CustodianClient::reconnect`]).
    pub fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        self.client.store(Arc::new(AuthGrpc::new(channel)));
        Ok(())
    }

    pub async fn authenticate(
        &self,
        req: auth::AuthenticateRequest,
    ) -> Result<auth::AuthenticateResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.authenticate(req).await?.into_inner())
    }

    /// Lightweight liveness probe for `/health`.
    ///
    /// Calls `validate_session` with an empty token: the auth service decodes (and rejects)
    /// it *without* touching the DB, so a successful gRPC round-trip proves the auth service
    /// is reachable. The (invalid) response body is intentionally ignored — only transport
    /// success/failure matters here.
    pub async fn health(&self) -> Result<(), tonic::Status> {
        let mut client = self.current();
        client
            .validate_session(auth::ValidateSessionRequest {
                session_token: String::new(),
            })
            .await?;
        Ok(())
    }
}

/// Admin service client. See [`CustodianClient`] for the `ArcSwap` rationale.
#[derive(Clone)]
pub struct AdminClient {
    client: Arc<ArcSwap<AdminGrpc>>,
}

impl AdminClient {
    fn current(&self) -> AdminGrpc {
        (**self.client.load()).clone()
    }

    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: Arc::new(ArcSwap::from_pointee(AdminGrpc::new(channel))),
        }
    }

    pub async fn connect(addr: String) -> Result<Self> {
        Ok(Self::from_channel(proto::tls::connect(&addr).await?))
    }

    /// Re-establish the channel to a new address and swap it in atomically (see
    /// [`CustodianClient::reconnect`]).
    pub fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        self.client.store(Arc::new(AdminGrpc::new(channel)));
        Ok(())
    }

    pub async fn create_user(
        &self,
        req: admin::CreateUserRequest,
    ) -> Result<admin::CreateUserResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.create_user(req).await?.into_inner())
    }

    pub async fn get_user(
        &self,
        req: admin::GetUserRequest,
    ) -> Result<admin::GetUserResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.get_user(req).await?.into_inner())
    }

    pub async fn list_users(
        &self,
        req: admin::ListUsersRequest,
    ) -> Result<admin::ListUsersResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.list_users(req).await?.into_inner())
    }

    pub async fn update_user(
        &self,
        req: admin::UpdateUserRequest,
    ) -> Result<admin::UpdateUserResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.update_user(req).await?.into_inner())
    }

    pub async fn delete_user(
        &self,
        req: admin::DeleteUserRequest,
    ) -> Result<admin::DeleteUserResponse, tonic::Status> {
        let mut client = self.current();
        Ok(client.delete_user(req).await?.into_inner())
    }
}

// Note: LBRP does not hold a direct DB client by design — it reaches data only through the
// custodian and auth services. A `DbClient` was removed as dead scaffolding.

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn unreachable_channel() -> Channel {
        Channel::from_static("http://127.0.0.1:9").connect_lazy()
    }

    fn test_custodian_client() -> CustodianClient {
        CustodianClient::from_channel(unreachable_channel())
    }

    // ── Minimal mock implementations ─────────────────────────────────────────

    #[derive(Clone, Default)]
    struct MinimalCustodianSvc;

    #[tonic::async_trait]
    impl custodian::custodian_service_server::CustodianService for MinimalCustodianSvc {
        type QueryTicketsStream =
            tokio_stream::Iter<std::vec::IntoIter<Result<custodian::Ticket, tonic::Status>>>;
        async fn query_tickets(
            &self,
            _: tonic::Request<custodian::QueryTicketsRequest>,
        ) -> Result<tonic::Response<Self::QueryTicketsStream>, tonic::Status> {
            Ok(tonic::Response::new(tokio_stream::iter(vec![])))
        }
        async fn create_ticket(
            &self,
            _req: tonic::Request<custodian::CreateTicketRequest>,
        ) -> Result<tonic::Response<custodian::Ticket>, tonic::Status> {
            Ok(tonic::Response::new(custodian::Ticket::default()))
        }
        async fn acquire_lock(
            &self,
            _req: tonic::Request<custodian::LockRequest>,
        ) -> Result<tonic::Response<custodian::LockResponse>, tonic::Status> {
            Ok(tonic::Response::new(custodian::LockResponse {
                success: true,
                error: String::new(),
                current_holder: None,
            }))
        }
        async fn release_lock(
            &self,
            _req: tonic::Request<custodian::LockRelease>,
        ) -> Result<tonic::Response<custodian::LockResponse>, tonic::Status> {
            Ok(tonic::Response::new(custodian::LockResponse {
                success: true,
                error: String::new(),
                current_holder: None,
            }))
        }
        async fn update_ticket(
            &self,
            _req: tonic::Request<custodian::UpdateTicketRequest>,
        ) -> Result<tonic::Response<custodian::Ticket>, tonic::Status> {
            Ok(tonic::Response::new(custodian::Ticket::default()))
        }
        async fn get_ticket(
            &self,
            req: tonic::Request<custodian::GetTicketRequest>,
        ) -> Result<tonic::Response<custodian::Ticket>, tonic::Status> {
            Ok(tonic::Response::new(custodian::Ticket {
                ticket_id: req.into_inner().ticket_id,
                ..Default::default()
            }))
        }
        async fn health(
            &self,
            _req: tonic::Request<custodian::HealthRequest>,
        ) -> Result<tonic::Response<custodian::HealthResponse>, tonic::Status> {
            Ok(tonic::Response::new(custodian::HealthResponse {
                healthy: true,
                status: "leader".to_string(),
            }))
        }
        async fn cluster_status(
            &self,
            _req: tonic::Request<custodian::ClusterStatusRequest>,
        ) -> Result<tonic::Response<custodian::ClusterStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(custodian::ClusterStatusResponse {
                leader_id: "1".to_string(),
                follower_ids: vec![],
                term: 1,
                commit_index: 0,
            }))
        }
    }

    #[derive(Clone, Default)]
    struct MinimalAuthSvc;

    #[tonic::async_trait]
    impl auth::auth_service_server::AuthService for MinimalAuthSvc {
        async fn authenticate(
            &self,
            _req: tonic::Request<auth::AuthenticateRequest>,
        ) -> Result<tonic::Response<auth::AuthenticateResponse>, tonic::Status> {
            Ok(tonic::Response::new(auth::AuthenticateResponse {
                success: true,
                session_token: "tok".to_string(),
                error: String::new(),
                user: None,
            }))
        }
        async fn validate_session(
            &self,
            _req: tonic::Request<auth::ValidateSessionRequest>,
        ) -> Result<tonic::Response<auth::ValidateSessionResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn logout(
            &self,
            _req: tonic::Request<auth::LogoutRequest>,
        ) -> Result<tonic::Response<auth::LogoutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    #[derive(Clone, Default)]
    struct MinimalAdminSvc;

    #[tonic::async_trait]
    impl admin::admin_service_server::AdminService for MinimalAdminSvc {
        async fn create_user(
            &self,
            _req: tonic::Request<admin::CreateUserRequest>,
        ) -> Result<tonic::Response<admin::CreateUserResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn get_user(
            &self,
            _req: tonic::Request<admin::GetUserRequest>,
        ) -> Result<tonic::Response<admin::GetUserResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn list_users(
            &self,
            _req: tonic::Request<admin::ListUsersRequest>,
        ) -> Result<tonic::Response<admin::ListUsersResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn update_user(
            &self,
            _req: tonic::Request<admin::UpdateUserRequest>,
        ) -> Result<tonic::Response<admin::UpdateUserResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn delete_user(
            &self,
            _req: tonic::Request<admin::DeleteUserRequest>,
        ) -> Result<tonic::Response<admin::DeleteUserResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn push_metrics(
            &self,
            _req: tonic::Request<admin::MetricsSnapshot>,
        ) -> Result<tonic::Response<admin::PushAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn record_intrusion(
            &self,
            _req: tonic::Request<admin::IntrusionEvent>,
        ) -> Result<tonic::Response<admin::IntrusionAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    // ── Server start helpers ──────────────────────────────────────────────────

    fn start_custodian(svc: MinimalCustodianSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        test_support::spawn_grpc!(
            custodian::custodian_service_server::CustodianServiceServer::new(svc)
        )
    }

    fn start_auth(svc: MinimalAuthSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        test_support::spawn_grpc!(auth::auth_service_server::AuthServiceServer::new(svc))
    }

    fn start_admin(svc: MinimalAdminSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        test_support::spawn_grpc!(admin::admin_service_server::AdminServiceServer::new(svc))
    }

    async fn connect_retry(addr: std::net::SocketAddr) -> Channel {
        let endpoint = format!("http://{addr}");
        for _ in 0..20 {
            if let Ok(ch) = Channel::from_shared(endpoint.clone())
                .expect("valid uri")
                .connect()
                .await
            {
                return ch;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("could not connect to {addr}");
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn connect_rejects_invalid_address_format() {
        assert!(
            CustodianClient::connect("not-a-url".to_string())
                .await
                .is_err()
        );
        assert!(AuthClient::connect("not-a-url".to_string()).await.is_err());
        assert!(AdminClient::connect("not-a-url".to_string()).await.is_err());
    }

    #[tokio::test]
    async fn connect_succeeds_with_valid_custodian_server() {
        let (addr, shutdown) = start_custodian(MinimalCustodianSvc);
        let _ = connect_retry(addr).await;
        let result = CustodianClient::connect(format!("http://{addr}")).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn connect_succeeds_with_valid_auth_server() {
        let (addr, shutdown) = start_auth(MinimalAuthSvc);
        let _ = connect_retry(addr).await;
        let result = AuthClient::connect(format!("http://{addr}")).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn connect_succeeds_with_valid_admin_server() {
        let (addr, shutdown) = start_admin(MinimalAdminSvc);
        let _ = connect_retry(addr).await;
        let result = AdminClient::connect(format!("http://{addr}")).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn custodian_wrappers_propagate_transport_errors() {
        let client = test_custodian_client();

        assert!(
            client
                .create_ticket(custodian::CreateTicketRequest {
                    title: "t".to_string(),
                    project: "p".to_string(),
                    account_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                    symptom: 0,
                    priority: 0,
                    created_by_uuid: "00000000-0000-0000-0000-000000000002".to_string(),
                    customer_ticket_number: None,
                    isp_ticket_number: None,
                    other_ticket_number: None,
                    ebond: None,
                    tracking_url: None,
                    network_devices: vec![],
                })
                .await
                .is_err()
        );

        assert!(
            client
                .acquire_lock(custodian::LockRequest {
                    ticket_id: 1,
                    user_uuid: "00000000-0000-0000-0000-000000000003".to_string(),
                })
                .await
                .is_err()
        );

        assert!(
            client
                .release_lock(custodian::LockRelease {
                    ticket_id: 1,
                    user_uuid: "00000000-0000-0000-0000-000000000004".to_string(),
                })
                .await
                .is_err()
        );

        assert!(
            client
                .update_ticket(custodian::UpdateTicketRequest {
                    ticket_id: 1,
                    title: None,
                    project: None,
                    symptom: None,
                    priority: None,
                    status: None,
                    next_action: None,
                    resolution: None,
                    assigned_to_uuid: None,
                    updated_by_uuid: Some("00000000-0000-0000-0000-000000000005".to_string()),
                    ebond: None,
                    tracking_url: None,
                    network_devices: vec![],
                })
                .await
                .is_err()
        );

        assert!(
            client
                .get_ticket(custodian::GetTicketRequest { ticket_id: 1 })
                .await
                .is_err()
        );

        assert!(client.cluster_status().await.is_err());
    }

    #[tokio::test]
    async fn custodian_wrappers_return_ok_with_working_server() {
        let (addr, shutdown) = start_custodian(MinimalCustodianSvc);
        let ch = connect_retry(addr).await;
        let client = CustodianClient::from_channel(ch);

        assert!(
            client
                .acquire_lock(custodian::LockRequest {
                    ticket_id: 1,
                    user_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                })
                .await
                .is_ok()
        );

        assert!(
            client
                .release_lock(custodian::LockRelease {
                    ticket_id: 1,
                    user_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                })
                .await
                .is_ok()
        );

        assert!(client.cluster_status().await.is_ok());

        let _ = shutdown.send(());
    }
}
