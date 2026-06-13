//! REST API route handlers for LBRP

use crate::clients::{AdminClient, AuthClient, CustodianClient};
use crate::error::ApiError;
use crate::middleware::{AuthState, Claims, auth_middleware};
use axum::{
    Extension, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub auth_client: AuthClient,
    pub admin_client: AdminClient,
    pub custodian_client: CustodianClient,
    pub auth_state: Arc<AuthState>,
}

// --- Auth Handlers ---

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    pub mfa_token: Option<String>,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
}

/// JSON representation of a ticket's next action (read-only in the REST API).
/// Absent (`null`) means no action is scheduled.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiNextAction {
    /// Follow up at the given unix-seconds timestamp.
    FollowUp { at: i64 },
    /// Appointment at the given unix-seconds timestamp.
    Appointment { at: i64 },
    /// Auto-close on a named schedule (`end_of_day` | `hours_24` | `hours_48` | `hours_72`).
    AutoClose { schedule: String },
}

fn map_next_action(
    next_action: Option<crate::clients::custodian::NextAction>,
) -> Option<ApiNextAction> {
    use crate::clients::custodian::{AutoCloseSchedule, next_action::Kind};
    let kind = next_action?.kind?;
    Some(match kind {
        Kind::FollowUp(ts) => ApiNextAction::FollowUp { at: ts.seconds },
        Kind::Appointment(ts) => ApiNextAction::Appointment { at: ts.seconds },
        Kind::AutoClose(value) => {
            let schedule = match AutoCloseSchedule::try_from(value) {
                Ok(AutoCloseSchedule::Hours24) => "hours_24",
                Ok(AutoCloseSchedule::Hours48) => "hours_48",
                Ok(AutoCloseSchedule::Hours72) => "hours_72",
                _ => "end_of_day",
            };
            ApiNextAction::AutoClose {
                schedule: schedule.to_string(),
            }
        }
    })
}

#[derive(Serialize)]
pub struct ApiTicket {
    pub ticket_id: u64,
    pub title: String,
    pub project: String,
    pub priority: i32,
    pub status: i32,
    pub next_action: Option<ApiNextAction>,
}

fn map_ticket(ticket: crate::clients::custodian::Ticket) -> ApiTicket {
    ApiTicket {
        ticket_id: ticket.ticket_id,
        title: ticket.title,
        project: ticket.project,
        priority: ticket.priority,
        status: ticket.status,
        next_action: map_next_action(ticket.next_action),
    }
}

async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut client = state.auth_client.client.lock().await;

    let req = crate::clients::auth::AuthenticateRequest {
        username: payload.username,
        password: payload.password,
        mfa_token: payload.mfa_token.unwrap_or_default(),
    };

    let resp = client.authenticate(req).await.map_err(|e| {
        tracing::error!("Auth service error: {}", e);
        ApiError::from(e)
    })?;

    let resp_inner = resp.into_inner();
    if resp_inner.success {
        Ok(Json(LoginResponse {
            token: resp_inner.session_token,
        }))
    } else {
        Err(ApiError::unauthorized(resp_inner.error))
    }
}

// --- Admin Handlers ---

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub email: String,
    pub display_name: String,
    pub role: i32,
}

async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut client = state.admin_client.client.lock().await;

    let req = crate::clients::admin::CreateUserRequest {
        username: payload.username,
        password: payload.password,
        email: payload.email,
        display_name: payload.display_name,
        role: payload.role,
    };

    let _resp = client.create_user(req).await.map_err(ApiError::from)?;

    Ok(StatusCode::CREATED)
}

// --- Custodian Handlers ---

#[derive(Deserialize)]
pub struct CreateTicketRequest {
    pub title: String,
    pub project: String,
    pub account_uuid: String,
    pub symptom: i32,
    pub priority: i32,
}

async fn create_ticket(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(payload): Json<CreateTicketRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::custodian::CreateTicketRequest {
        title: payload.title,
        project: payload.project,
        account_uuid: payload.account_uuid,
        symptom: payload.symptom,
        priority: payload.priority,
        created_by_uuid: claims.sub,
        customer_ticket_number: None,
        isp_ticket_number: None,
        other_ticket_number: None,
        ebond: None,
        tracking_url: None,
        network_devices: vec![],
    };

    let resp = state
        .custodian_client
        .create_ticket(req)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok((StatusCode::CREATED, Json(map_ticket(resp))))
}

async fn get_ticket(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::custodian::GetTicketRequest { ticket_id: id };

    let resp = state
        .custodian_client
        .get_ticket(req)
        .await
        .map_err(|e| ApiError::not_found(e.to_string()))?;

    Ok(Json(map_ticket(resp)))
}

/// Query params for `GET /api/tickets` (e.g. `?status=1&assignee=<uuid>&project=foo`).
#[derive(Deserialize)]
pub struct ListTicketsParams {
    pub status: Option<u32>,
    pub assignee: Option<String>,
    pub account: Option<String>,
    pub project: Option<String>,
    pub include_deleted: Option<bool>,
    pub limit: Option<u32>,
}

async fn list_tickets(
    State(state): State<AppState>,
    Query(params): Query<ListTicketsParams>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::custodian::QueryTicketsRequest {
        status: params.status,
        assigned_to_uuid: params.assignee,
        account_uuid: params.account,
        project: params.project,
        include_deleted: params.include_deleted.unwrap_or(false),
        limit: params.limit.unwrap_or(0),
    };

    let tickets = state
        .custodian_client
        .query_tickets(req)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let out: Vec<ApiTicket> = tickets.into_iter().map(map_ticket).collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
pub struct UpdateTicketRequest {
    pub title: Option<String>,
    pub project: Option<String>,
    pub priority: Option<i32>,
    pub status: Option<i32>,
}

async fn update_ticket(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Extension(claims): Extension<Claims>,
    Json(payload): Json<UpdateTicketRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::custodian::UpdateTicketRequest {
        ticket_id: id,
        title: payload.title,
        project: payload.project,
        symptom: None,
        priority: payload.priority,
        status: payload.status,
        next_action: None,
        resolution: None,
        assigned_to_uuid: None,
        updated_by_uuid: Some(claims.sub),
        ebond: None,
        tracking_url: None,
        network_devices: vec![],
    };

    let resp = state
        .custodian_client
        .update_ticket(req)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(map_ticket(resp)))
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub services: std::collections::BTreeMap<String, &'static str>,
}

/// Aggregated health endpoint. Probes reachable downstream services and reports overall
/// status (`200 ok` / `503 degraded`). Currently probes the custodian (via its cluster
/// status); auth/db lack a Health RPC reachable from LBRP and are a tracked follow-up.
async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let mut services = std::collections::BTreeMap::new();

    let custodian_ok = state.custodian_client.cluster_status().await.is_ok();
    services.insert(
        "custodian".to_string(),
        if custodian_ok { "up" } else { "down" },
    );

    let all_ok = custodian_ok;
    let status = if all_ok { "ok" } else { "degraded" };
    let code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(HealthResponse { status, services }))
}

pub fn app(state: AppState) -> Router {
    let auth_routes = Router::new().route("/login", post(login));

    // User creation is outside the auth middleware so the first admin user
    // can be bootstrapped before any JWT exists.
    let admin_routes = Router::new().route("/admin/users", post(create_user));

    let api_routes = Router::new()
        .route("/tickets", post(create_ticket).get(list_tickets))
        .route("/tickets/{id}", get(get_ticket).put(update_ticket))
        .layer(middleware::from_fn_with_state(
            state.auth_state.clone(),
            auth_middleware,
        ));

    Router::new()
        .route("/health", get(health))
        .nest("/auth", auth_routes)
        .nest("/api", admin_routes)
        .nest("/api", api_routes)
        .with_state(state)
        // Response compression (gzip/brotli) negotiated via Accept-Encoding.
        .layer(tower_http::compression::CompressionLayer::new())
        // CORS so the WASM frontend can call the API cross-origin during development.
        .layer(cors_layer())
}

/// CORS policy. Allowed origins come from `CORS_ALLOWED_ORIGINS` (comma-separated); if unset,
/// any origin is allowed (convenient for local dev / same-origin static serving).
fn cors_layer() -> tower_http::cors::CorsLayer {
    use axum::http::{HeaderValue, Method, header};
    let base = tower_http::cors::CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    match std::env::var("CORS_ALLOWED_ORIGINS") {
        Ok(raw) if !raw.trim().is_empty() => {
            let origins: Vec<HeaderValue> = raw
                .split(',')
                .filter_map(|o| o.trim().parse().ok())
                .collect();
            base.allow_origin(origins)
        }
        _ => base.allow_origin(tower_http::cors::Any),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::sync::oneshot;
    use tonic::transport::Channel;
    use tonic::transport::Server;
    use tower::ServiceExt;

    // ===== Mock gRPC service implementations =====

    #[derive(Clone)]
    struct MockAuthSvc {
        /// When Some, the `authenticate` RPC returns this gRPC error code.
        error_code: Option<tonic::Code>,
        /// When `error_code` is None, controls whether `success` is true.
        auth_success: bool,
    }

    #[tonic::async_trait]
    impl crate::clients::auth::auth_service_server::AuthService for MockAuthSvc {
        async fn authenticate(
            &self,
            _req: tonic::Request<crate::clients::auth::AuthenticateRequest>,
        ) -> Result<tonic::Response<crate::clients::auth::AuthenticateResponse>, tonic::Status>
        {
            if let Some(code) = self.error_code {
                return Err(tonic::Status::new(code, "mock error"));
            }
            Ok(tonic::Response::new(
                crate::clients::auth::AuthenticateResponse {
                    success: self.auth_success,
                    session_token: if self.auth_success {
                        "mock-token".to_string()
                    } else {
                        String::new()
                    },
                    error: if self.auth_success {
                        String::new()
                    } else {
                        "bad creds".to_string()
                    },
                    user: None,
                },
            ))
        }

        async fn validate_session(
            &self,
            _req: tonic::Request<crate::clients::auth::ValidateSessionRequest>,
        ) -> Result<tonic::Response<crate::clients::auth::ValidateSessionResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn logout(
            &self,
            _req: tonic::Request<crate::clients::auth::LogoutRequest>,
        ) -> Result<tonic::Response<crate::clients::auth::LogoutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    #[derive(Clone, Default)]
    struct MockAdminSvc;

    #[tonic::async_trait]
    impl crate::clients::admin::admin_service_server::AdminService for MockAdminSvc {
        async fn create_user(
            &self,
            _req: tonic::Request<crate::clients::admin::CreateUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::CreateUserResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(
                crate::clients::admin::CreateUserResponse {
                    user: Some(crate::clients::admin::User {
                        id: "test-id".to_string(),
                        username: "new-user".to_string(),
                        email: "u@example.com".to_string(),
                        display_name: "User".to_string(),
                        role: 0,
                        active: true,
                        created_at: 0,
                    }),
                },
            ))
        }

        async fn get_user(
            &self,
            _req: tonic::Request<crate::clients::admin::GetUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::GetUserResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn list_users(
            &self,
            _req: tonic::Request<crate::clients::admin::ListUsersRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::ListUsersResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn update_user(
            &self,
            _req: tonic::Request<crate::clients::admin::UpdateUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::UpdateUserResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn delete_user(
            &self,
            _req: tonic::Request<crate::clients::admin::DeleteUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::DeleteUserResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn push_metrics(
            &self,
            _req: tonic::Request<crate::clients::admin::MetricsSnapshot>,
        ) -> Result<tonic::Response<crate::clients::admin::PushAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
        async fn record_intrusion(
            &self,
            _req: tonic::Request<crate::clients::admin::IntrusionEvent>,
        ) -> Result<tonic::Response<crate::clients::admin::IntrusionAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    #[derive(Clone, Default)]
    struct MockCustodianSvc;

    #[tonic::async_trait]
    impl crate::clients::custodian::custodian_service_server::CustodianService for MockCustodianSvc {
        async fn create_ticket(
            &self,
            req: tonic::Request<crate::clients::custodian::CreateTicketRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::Ticket>, tonic::Status> {
            let r = req.into_inner();
            Ok(tonic::Response::new(crate::clients::custodian::Ticket {
                ticket_id: 1,
                title: r.title,
                project: r.project,
                priority: r.priority,
                status: 0,
                ..Default::default()
            }))
        }

        async fn acquire_lock(
            &self,
            _req: tonic::Request<crate::clients::custodian::LockRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::LockResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn release_lock(
            &self,
            _req: tonic::Request<crate::clients::custodian::LockRelease>,
        ) -> Result<tonic::Response<crate::clients::custodian::LockResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn update_ticket(
            &self,
            _req: tonic::Request<crate::clients::custodian::UpdateTicketRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::Ticket>, tonic::Status> {
            Ok(tonic::Response::new(crate::clients::custodian::Ticket {
                ticket_id: 1,
                title: "Updated".to_string(),
                project: "InfoVulcan".to_string(),
                ..Default::default()
            }))
        }

        async fn get_ticket(
            &self,
            req: tonic::Request<crate::clients::custodian::GetTicketRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::Ticket>, tonic::Status> {
            Ok(tonic::Response::new(crate::clients::custodian::Ticket {
                ticket_id: req.into_inner().ticket_id,
                title: "Test Ticket".to_string(),
                project: "InfoVulcan".to_string(),
                ..Default::default()
            }))
        }

        type QueryTicketsStream = tokio_stream::Iter<
            std::vec::IntoIter<Result<crate::clients::custodian::Ticket, tonic::Status>>,
        >;
        async fn query_tickets(
            &self,
            _req: tonic::Request<crate::clients::custodian::QueryTicketsRequest>,
        ) -> Result<tonic::Response<Self::QueryTicketsStream>, tonic::Status> {
            let tickets = vec![
                Ok(crate::clients::custodian::Ticket {
                    ticket_id: 1,
                    title: "First".to_string(),
                    project: "InfoVulcan".to_string(),
                    status: 1,
                    ..Default::default()
                }),
                Ok(crate::clients::custodian::Ticket {
                    ticket_id: 2,
                    title: "Second".to_string(),
                    project: "InfoVulcan".to_string(),
                    status: 1,
                    ..Default::default()
                }),
            ];
            Ok(tonic::Response::new(tokio_stream::iter(tickets)))
        }

        async fn health(
            &self,
            _req: tonic::Request<crate::clients::custodian::HealthRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::HealthResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn cluster_status(
            &self,
            _req: tonic::Request<crate::clients::custodian::ClusterStatusRequest>,
        ) -> Result<tonic::Response<crate::clients::custodian::ClusterStatusResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(
                crate::clients::custodian::ClusterStatusResponse::default(),
            ))
        }
    }

    // ===== Server startup helpers =====

    fn start_mock_auth(svc: MockAuthSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(crate::clients::auth::auth_service_server::AuthServiceServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    fn start_mock_admin(svc: MockAdminSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    crate::clients::admin::admin_service_server::AdminServiceServer::new(svc),
                )
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    fn start_mock_custodian(svc: MockCustodianSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    crate::clients::custodian::custodian_service_server::CustodianServiceServer::new(svc),
                )
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
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("failed to connect to mock server at {addr}");
    }

    fn make_state_with_auth_ch(ch: Channel) -> AppState {
        AppState {
            auth_client: AuthClient {
                client: Arc::new(Mutex::new(
                    crate::clients::auth::auth_service_client::AuthServiceClient::new(ch),
                )),
            },
            ..test_state()
        }
    }

    fn make_state_with_admin_ch(ch: Channel) -> AppState {
        AppState {
            admin_client: AdminClient {
                client: Arc::new(Mutex::new(
                    crate::clients::admin::admin_service_client::AdminServiceClient::new(ch),
                )),
            },
            ..test_state()
        }
    }

    fn make_state_with_custodian_ch(ch: Channel) -> AppState {
        AppState {
            custodian_client: CustodianClient {
                client: Arc::new(Mutex::new(
                    crate::clients::custodian::custodian_service_client::CustodianServiceClient::new(
                        ch,
                    ),
                )),
            },
            ..test_state()
        }
    }

    fn test_claims() -> Claims {
        Claims {
            sub: "00000000-0000-0000-0000-000000000042".to_string(),
            exp: 4_102_444_800,
            role: "Admin".to_string(),
        }
    }

    // ===== Tests for previously-uncovered handler paths =====

    #[tokio::test]
    async fn login_succeeds_when_auth_backend_returns_success() {
        let (addr, shutdown) = start_mock_auth(MockAuthSvc {
            error_code: None,
            auth_success: true,
        });
        let ch = connect_retry(addr).await;
        let result = login(
            State(make_state_with_auth_ch(ch)),
            Json(LoginRequest {
                username: "alice".into(),
                password: "pass".into(),
                mfa_token: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn login_returns_unauthorized_when_credentials_denied() {
        let (addr, shutdown) = start_mock_auth(MockAuthSvc {
            error_code: None,
            auth_success: false,
        });
        let ch = connect_retry(addr).await;
        let result = login(
            State(make_state_with_auth_ch(ch)),
            Json(LoginRequest {
                username: "alice".into(),
                password: "wrong".into(),
                mfa_token: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        let Err(err) = result else {
            panic!("expected error response when credentials denied");
        };
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn login_maps_unauthenticated_grpc_error_to_401() {
        let (addr, shutdown) = start_mock_auth(MockAuthSvc {
            error_code: Some(tonic::Code::Unauthenticated),
            auth_success: false,
        });
        let ch = connect_retry(addr).await;
        let result = login(
            State(make_state_with_auth_ch(ch)),
            Json(LoginRequest {
                username: "alice".into(),
                password: "pass".into(),
                mfa_token: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        let Err(err) = result else {
            panic!("expected error when backend returns Unauthenticated");
        };
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn login_maps_invalid_argument_grpc_error_to_400() {
        let (addr, shutdown) = start_mock_auth(MockAuthSvc {
            error_code: Some(tonic::Code::InvalidArgument),
            auth_success: false,
        });
        let ch = connect_retry(addr).await;
        let result = login(
            State(make_state_with_auth_ch(ch)),
            Json(LoginRequest {
                username: "alice".into(),
                password: "pass".into(),
                mfa_token: Some(String::new()),
            }),
        )
        .await;
        let _ = shutdown.send(());
        let Err(err) = result else {
            panic!("expected error when backend returns InvalidArgument");
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_user_returns_created_on_backend_success() {
        let (addr, shutdown) = start_mock_admin(MockAdminSvc);
        let ch = connect_retry(addr).await;
        let result = create_user(
            State(make_state_with_admin_ch(ch)),
            Json(CreateUserRequest {
                username: "new-user".into(),
                password: "pass".into(),
                email: "u@example.com".into(),
                display_name: "User".into(),
                role: 0,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_ticket_returns_ticket_on_backend_success() {
        let (addr, shutdown) = start_mock_custodian(MockCustodianSvc);
        let ch = connect_retry(addr).await;
        let result = get_ticket(State(make_state_with_custodian_ch(ch)), Path(7_u64)).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn create_ticket_returns_created_on_backend_success() {
        let (addr, shutdown) = start_mock_custodian(MockCustodianSvc);
        let ch = connect_retry(addr).await;
        let result = create_ticket(
            State(make_state_with_custodian_ch(ch)),
            Extension(test_claims()),
            Json(CreateTicketRequest {
                title: "Test".into(),
                project: "InfoVulcan".into(),
                account_uuid: "00000000-0000-0000-0000-000000000001".into(),
                symptom: 0,
                priority: 0,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn update_ticket_returns_ticket_on_backend_success() {
        let (addr, shutdown) = start_mock_custodian(MockCustodianSvc);
        let ch = connect_retry(addr).await;
        let result = update_ticket(
            State(make_state_with_custodian_ch(ch)),
            Path(7_u64),
            Extension(test_claims()),
            Json(UpdateTicketRequest {
                title: Some("Updated".into()),
                project: None,
                priority: None,
                status: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn list_tickets_returns_tickets_from_backend() {
        let (addr, shutdown) = start_mock_custodian(MockCustodianSvc);
        let ch = connect_retry(addr).await;
        let result = list_tickets(
            State(make_state_with_custodian_ch(ch)),
            Query(ListTicketsParams {
                status: Some(1),
                assignee: None,
                account: None,
                project: None,
                include_deleted: None,
                limit: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn health_reports_ok_when_custodian_reachable() {
        let (addr, shutdown) = start_mock_custodian(MockCustodianSvc);
        let ch = connect_retry(addr).await;
        let resp = health(State(make_state_with_custodian_ch(ch)))
            .await
            .into_response();
        let _ = shutdown.send(());
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_reports_degraded_when_custodian_unreachable() {
        // Lazy channel to a dead port: cluster_status fails -> degraded/503.
        let resp = health(State(test_state())).await.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn api_error_maps_grpc_codes_and_renders_envelope() {
        use crate::error::ApiError;
        let err = ApiError::from(tonic::Status::not_found("missing"));
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.code, "not_found");
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ===== Existing tests =====

    fn test_state() -> AppState {
        let channel = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        AppState {
            auth_client: AuthClient {
                client: Arc::new(Mutex::new(
                    crate::clients::auth::auth_service_client::AuthServiceClient::new(
                        channel.clone(),
                    ),
                )),
            },
            admin_client: AdminClient {
                client: Arc::new(Mutex::new(
                    crate::clients::admin::admin_service_client::AdminServiceClient::new(
                        channel.clone(),
                    ),
                )),
            },
            custodian_client: CustodianClient {
                client: Arc::new(Mutex::new(
                    crate::clients::custodian::custodian_service_client::CustodianServiceClient::new(channel),
                )),
            },
            auth_state: Arc::new(AuthState {
                jwt_secret: b"secret".to_vec(),
            }),
        }
    }

    fn test_bearer_token() -> String {
        let claims = Claims {
            sub: "00000000-0000-0000-0000-000000000042".to_string(),
            exp: 4_102_444_800,
            role: "Admin".to_string(),
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"secret"),
        )
        .expect("token generation");
        format!("Bearer {token}")
    }

    #[test]
    fn map_ticket_maps_expected_fields() {
        let source = crate::clients::custodian::Ticket {
            ticket_id: 42,
            title: "Demo".to_string(),
            project: "InfoVulcan".to_string(),
            priority: 3,
            status: 1,
            ..Default::default()
        };

        let mapped = map_ticket(source);
        assert_eq!(mapped.ticket_id, 42);
        assert_eq!(mapped.title, "Demo");
        assert_eq!(mapped.project, "InfoVulcan");
        assert_eq!(mapped.priority, 3);
        assert_eq!(mapped.status, 1);
    }

    #[tokio::test]
    async fn protected_route_rejects_unauthenticated_request() {
        let app = app(test_state());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tickets/1")
                    .method("PUT")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"title":"x"}"#))
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn login_returns_service_error_when_backend_is_unreachable() {
        let result = login(
            State(test_state()),
            Json(LoginRequest {
                username: "user".to_string(),
                password: "pass".to_string(),
                mfa_token: None,
            }),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_user_returns_service_error_when_backend_is_unreachable() {
        let result = create_user(
            State(test_state()),
            Json(CreateUserRequest {
                username: "new-user".to_string(),
                password: "pass".to_string(),
                email: "u@example.com".to_string(),
                display_name: "User".to_string(),
                role: 1,
            }),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_ticket_route_executes_and_returns_error_without_backend() {
        let app = app(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tickets")
                    .method("POST")
                    .header("authorization", test_bearer_token())
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"title":"Demo","project":"InfoVulcan","account_uuid":"00000000-0000-0000-0000-000000000001","symptom":0,"priority":0}"#,
                    ))
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn get_ticket_route_executes_and_maps_error() {
        let app = app(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tickets/7")
                    .method("GET")
                    .header("authorization", test_bearer_token())
                    .body(Body::empty())
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_ticket_route_executes_and_returns_error_without_backend() {
        let app = app(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tickets/7")
                    .method("PUT")
                    .header("authorization", test_bearer_token())
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"title":"Updated"}"#))
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ===== Static file fallback tests =====

    #[tokio::test]
    async fn test_static_file_serving_when_root_requested_returns_index_html() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let index_content = "<html><body>InfoVulcan Test App</body></html>";
        std::fs::write(tmp.path().join("index.html"), index_content).expect("write index.html");

        let app = app(test_state()).fallback_service(
            tower_http::services::ServeDir::new(tmp.path()).fallback(
                tower_http::services::ServeFile::new(tmp.path().join("index.html")),
            ),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body_str = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(
            body_str.contains("InfoVulcan Test App"),
            "expected body to contain test content, got: {body_str}"
        );
    }

    #[tokio::test]
    async fn test_static_file_serving_when_spa_route_requested_falls_back_to_index() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let index_content = "<html><body>SPA Fallback Content</body></html>";
        std::fs::write(tmp.path().join("index.html"), index_content).expect("write index.html");

        let app = app(test_state()).fallback_service(
            tower_http::services::ServeDir::new(tmp.path()).fallback(
                tower_http::services::ServeFile::new(tmp.path().join("index.html")),
            ),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/some/spa/route")
                    .body(Body::empty())
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body_str = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(
            body_str.contains("SPA Fallback Content"),
            "expected SPA route to fall back to index.html, got: {body_str}"
        );
    }

    #[tokio::test]
    async fn test_static_file_serving_when_specific_file_requested_returns_that_file() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let index_content = "<html><body>Index</body></html>";
        let css_content = "body { color: red; }";
        std::fs::write(tmp.path().join("index.html"), index_content).expect("write index.html");
        std::fs::write(tmp.path().join("style.css"), css_content).expect("write style.css");

        let app = app(test_state()).fallback_service(
            tower_http::services::ServeDir::new(tmp.path()).fallback(
                tower_http::services::ServeFile::new(tmp.path().join("index.html")),
            ),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/style.css")
                    .body(Body::empty())
                    .expect("request build"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body_str = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(
            body_str.contains("color: red"),
            "expected specific file content, got: {body_str}"
        );
    }

    #[tokio::test]
    async fn login_maps_other_grpc_error_to_500() {
        let (addr, shutdown) = start_mock_auth(MockAuthSvc {
            error_code: Some(tonic::Code::Internal),
            auth_success: false,
        });
        let ch = connect_retry(addr).await;
        let result = login(
            State(make_state_with_auth_ch(ch)),
            Json(LoginRequest {
                username: "alice".into(),
                password: "pass".into(),
                mfa_token: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        let Err(err) = result else {
            panic!("expected error when backend returns Internal");
        };
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
