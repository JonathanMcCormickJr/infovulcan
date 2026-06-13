//! End-to-end: hitting a honeypot trap endpoint reports an intrusion to the admin service.

use std::sync::Arc;
use std::time::Duration;

use proto::admin::admin_service_client::AdminServiceClient;
use proto::admin::admin_service_server::{AdminService, AdminServiceServer};
use proto::admin::{
    CreateUserRequest, CreateUserResponse, DeleteUserRequest, DeleteUserResponse, GetUserRequest,
    GetUserResponse, IntrusionAck, IntrusionEvent, ListUsersRequest, ListUsersResponse,
    MetricsSnapshot, PushAck, UpdateUserRequest, UpdateUserResponse,
};
use proto::honeypot::WalletRequest;
use proto::honeypot::honeypot_service_server::HoneypotService;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use honeypot::reporter::Reporter;
use honeypot::service::HoneypotServiceImpl;

/// Mock admin server that records every intrusion it receives.
#[derive(Clone, Default)]
struct CapturingAdmin {
    events: Arc<Mutex<Vec<IntrusionEvent>>>,
}

#[tonic::async_trait]
impl AdminService for CapturingAdmin {
    async fn record_intrusion(
        &self,
        request: Request<IntrusionEvent>,
    ) -> Result<Response<IntrusionAck>, Status> {
        self.events.lock().await.push(request.into_inner());
        Ok(Response::new(IntrusionAck { recorded: true }))
    }

    async fn create_user(
        &self,
        _: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn get_user(
        &self,
        _: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn list_users(
        &self,
        _: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn update_user(
        &self,
        _: Request<UpdateUserRequest>,
    ) -> Result<Response<UpdateUserResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn delete_user(
        &self,
        _: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn push_metrics(&self, _: Request<MetricsSnapshot>) -> Result<Response<PushAck>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

#[tokio::test]
async fn honeypot_hit_reports_intrusion_to_admin() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let admin = CapturingAdmin {
        events: events.clone(),
    };

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(AdminServiceServer::new(admin))
            .serve(addr)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Honeypot service wired to report to the mock admin.
    let channel = proto::tls::connect(&format!("http://{addr}"))
        .await
        .unwrap();
    let reporter = Reporter::with_client(AdminServiceClient::new(channel));
    let svc = HoneypotServiceImpl::new(reporter);

    // Attacker hits the fake wallet endpoint.
    let resp = svc
        .get_wallet_balance(Request::new(WalletRequest {
            wallet_id: "victim".to_string(),
        }))
        .await
        .expect("wallet")
        .into_inner();
    assert_eq!(resp.currency, "BTC");

    // Admin recorded exactly one intrusion for that endpoint.
    let captured = events.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].endpoint_accessed, "/wallet/balance");
    assert_eq!(captured[0].request_method, "GetWalletBalance");

    server.abort();
}
