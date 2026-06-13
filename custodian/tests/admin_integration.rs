use custodian::metrics;
use std::collections::HashMap;
use tonic::{Request, Response, Status};

#[tokio::test]
async fn test_push_metrics_to_admin() {
    // Start a local admin server to accept PushMetrics
    use custodian::admin_client::admin::admin_service_server::{AdminService, AdminServiceServer};
    use custodian::admin_client::admin::{
        CreateUserRequest, CreateUserResponse, DeleteUserRequest, DeleteUserResponse,
        GetUserRequest, GetUserResponse, IntrusionAck, IntrusionEvent, ListUsersRequest,
        ListUsersResponse, MetricsSnapshot, PushAck, UpdateUserRequest, UpdateUserResponse,
    };

    #[derive(Default)]
    struct TestSvc(tokio::sync::Mutex<Option<MetricsSnapshot>>);

    #[tonic::async_trait]
    impl AdminService for TestSvc {
        async fn create_user(
            &self,
            _request: Request<CreateUserRequest>,
        ) -> Result<Response<CreateUserResponse>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
        async fn get_user(
            &self,
            _request: Request<GetUserRequest>,
        ) -> Result<Response<GetUserResponse>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
        async fn list_users(
            &self,
            _request: Request<ListUsersRequest>,
        ) -> Result<Response<ListUsersResponse>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
        async fn update_user(
            &self,
            _request: Request<UpdateUserRequest>,
        ) -> Result<Response<UpdateUserResponse>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
        async fn delete_user(
            &self,
            _request: Request<DeleteUserRequest>,
        ) -> Result<Response<DeleteUserResponse>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }

        async fn push_metrics(
            &self,
            request: Request<MetricsSnapshot>,
        ) -> Result<Response<PushAck>, Status> {
            let msg = request.into_inner();
            let mut g = self.0.lock().await;
            *g = Some(msg);
            Ok(Response::new(PushAck { ok: true }))
        }
        async fn record_intrusion(
            &self,
            _request: Request<IntrusionEvent>,
        ) -> Result<Response<IntrusionAck>, Status> {
            Err(Status::unimplemented("not needed for test"))
        }
    }

    let svc = TestSvc::default();
    let addr = "127.0.0.1:50061".parse().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AdminServiceServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    });

    // Configure admin client and push a metric
    metrics::SNAPSHOT_CREATED_TOTAL.inc();
    let mut counters = HashMap::new();
    counters.insert(
        "snapshot_created_total".to_string(),
        metrics::SNAPSHOT_CREATED_TOTAL.get() as i64,
    );

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    custodian::admin_client::init("http://127.0.0.1:50061".to_string());
    custodian::admin_client::push_snapshot("custodian", 42, counters).await;

    // Give admin a moment to receive
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Shutdown
    server.abort();
}
