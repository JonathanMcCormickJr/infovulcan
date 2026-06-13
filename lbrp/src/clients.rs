//! gRPC client implementations for LBRP service communication
#![allow(dead_code)]

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub use proto::{admin, auth, custodian, db};

/// Custodian service client
#[derive(Clone)]
pub struct CustodianClient {
    pub client: Arc<Mutex<custodian::custodian_service_client::CustodianServiceClient<Channel>>>,
}

impl CustodianClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let channel = proto::tls::connect(&addr).await?;
        let client = custodian::custodian_service_client::CustodianServiceClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Re-establish the underlying channel to a (possibly new) address and swap it in place.
    ///
    /// Used by the `services.toml` reload task to hot-redirect traffic when an endpoint changes
    /// without restarting LBRP. Uses a *lazy* channel so the reload loop never blocks on a
    /// handshake; the connection is established on the next RPC. Existing in-flight calls finish
    /// on the old channel; subsequent calls use the new one.
    pub async fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        let mut client = self.client.lock().await;
        *client = custodian::custodian_service_client::CustodianServiceClient::new(channel);
        Ok(())
    }

    pub async fn create_ticket(
        &self,
        req: custodian::CreateTicketRequest,
    ) -> Result<custodian::Ticket> {
        let mut client = self.client.lock().await;
        let response = client.create_ticket(req).await?;
        Ok(response.into_inner())
    }

    pub async fn acquire_lock(
        &self,
        req: custodian::LockRequest,
    ) -> Result<custodian::LockResponse> {
        let mut client = self.client.lock().await;
        let response = client.acquire_lock(req).await?;
        Ok(response.into_inner())
    }

    pub async fn release_lock(
        &self,
        req: custodian::LockRelease,
    ) -> Result<custodian::LockResponse> {
        let mut client = self.client.lock().await;
        let response = client.release_lock(req).await?;
        Ok(response.into_inner())
    }

    pub async fn update_ticket(
        &self,
        req: custodian::UpdateTicketRequest,
    ) -> Result<custodian::Ticket> {
        let mut client = self.client.lock().await;
        let response = client.update_ticket(req).await?;
        Ok(response.into_inner())
    }

    pub async fn get_ticket(&self, req: custodian::GetTicketRequest) -> Result<custodian::Ticket> {
        let mut client = self.client.lock().await;
        let response = client.get_ticket(req).await?;
        Ok(response.into_inner())
    }

    pub async fn query_tickets(
        &self,
        req: custodian::QueryTicketsRequest,
    ) -> Result<Vec<custodian::Ticket>> {
        let mut client = self.client.lock().await;
        let mut stream = client.query_tickets(req).await?.into_inner();
        let mut tickets = Vec::new();
        while let Some(ticket) = stream.message().await? {
            tickets.push(ticket);
        }
        Ok(tickets)
    }

    pub async fn cluster_status(&self) -> Result<custodian::ClusterStatusResponse> {
        let mut client = self.client.lock().await;
        let response = client
            .cluster_status(custodian::ClusterStatusRequest {})
            .await?;
        Ok(response.into_inner())
    }
}

/// Auth service client
#[derive(Clone)]
pub struct AuthClient {
    pub client: Arc<Mutex<auth::auth_service_client::AuthServiceClient<Channel>>>,
}

impl AuthClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let channel = proto::tls::connect(&addr).await?;
        let client = auth::auth_service_client::AuthServiceClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Re-establish the channel to a new address and swap it in place (see
    /// [`CustodianClient::reconnect`]).
    pub async fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        let mut client = self.client.lock().await;
        *client = auth::auth_service_client::AuthServiceClient::new(channel);
        Ok(())
    }
}

/// Admin service client
#[derive(Clone)]
pub struct AdminClient {
    pub client: Arc<Mutex<admin::admin_service_client::AdminServiceClient<Channel>>>,
}

impl AdminClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let channel = proto::tls::connect(&addr).await?;
        let client = admin::admin_service_client::AdminServiceClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Re-establish the channel to a new address and swap it in place (see
    /// [`CustodianClient::reconnect`]).
    pub async fn reconnect(&self, addr: &str) -> Result<()> {
        let channel = proto::tls::connect_lazy(addr)?;
        let mut client = self.client.lock().await;
        *client = admin::admin_service_client::AdminServiceClient::new(channel);
        Ok(())
    }
}

/// DB service client
#[derive(Clone)]
pub struct DbClient {
    pub client: Arc<Mutex<db::database_client::DatabaseClient<Channel>>>,
}

impl DbClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let channel = proto::tls::connect(&addr).await?;
        let client = db::database_client::DatabaseClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    // Add DB client methods as needed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::transport::Server;

    fn unreachable_channel() -> Channel {
        Channel::from_static("http://127.0.0.1:9").connect_lazy()
    }

    fn test_custodian_client() -> CustodianClient {
        CustodianClient {
            client: Arc::new(Mutex::new(
                custodian::custodian_service_client::CustodianServiceClient::new(
                    unreachable_channel(),
                ),
            )),
        }
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

    #[derive(Clone, Default)]
    struct MinimalDbSvc;

    #[tonic::async_trait]
    impl db::database_server::Database for MinimalDbSvc {
        // --- Domain RPC stubs (this mock only exercises generic KV) ---
        type QueryTicketsStream =
            tokio_stream::Iter<std::vec::IntoIter<Result<db::TicketRecord, tonic::Status>>>;
        async fn create_ticket(
            &self,
            _: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get_ticket(
            &self,
            _: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn update_ticket(
            &self,
            _: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn soft_delete_ticket(
            &self,
            _: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn query_tickets(
            &self,
            _: tonic::Request<db::TicketQuery>,
        ) -> Result<tonic::Response<Self::QueryTicketsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn create_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn update_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn soft_delete_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }

        async fn put(
            &self,
            _req: tonic::Request<db::PutRequest>,
        ) -> Result<tonic::Response<db::PutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn get(
            &self,
            _req: tonic::Request<db::GetRequest>,
        ) -> Result<tonic::Response<db::GetResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn delete(
            &self,
            _req: tonic::Request<db::DeleteRequest>,
        ) -> Result<tonic::Response<db::DeleteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn list(
            &self,
            _req: tonic::Request<db::ListRequest>,
        ) -> Result<tonic::Response<db::ListResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn exists(
            &self,
            _req: tonic::Request<db::ExistsRequest>,
        ) -> Result<tonic::Response<db::ExistsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn batch_put(
            &self,
            _req: tonic::Request<db::BatchPutRequest>,
        ) -> Result<tonic::Response<db::BatchPutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn health(
            &self,
            _req: tonic::Request<db::HealthRequest>,
        ) -> Result<tonic::Response<db::HealthResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn cluster_status(
            &self,
            _req: tonic::Request<db::ClusterStatusRequest>,
        ) -> Result<tonic::Response<db::ClusterStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    // ── Server start helpers ──────────────────────────────────────────────────

    fn start_custodian(svc: MinimalCustodianSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(custodian::custodian_service_server::CustodianServiceServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    fn start_auth(svc: MinimalAuthSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(auth::auth_service_server::AuthServiceServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    fn start_admin(svc: MinimalAdminSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(admin::admin_service_server::AdminServiceServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    fn start_db(svc: MinimalDbSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(db::database_server::DatabaseServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
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
        assert!(DbClient::connect("not-a-url".to_string()).await.is_err());
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
    async fn connect_succeeds_with_valid_db_server() {
        let (addr, shutdown) = start_db(MinimalDbSvc);
        let _ = connect_retry(addr).await;
        let result = DbClient::connect(format!("http://{addr}")).await;
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
        let client = CustodianClient {
            client: Arc::new(Mutex::new(
                custodian::custodian_service_client::CustodianServiceClient::new(ch),
            )),
        };

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
