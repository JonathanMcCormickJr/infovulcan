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

/// Map a custodian `NextAction` to an `Option<ApiNextAction>` (an unset `kind` means "no action
/// scheduled" → `null` in JSON). This stays a free function rather than a `From` impl: the orphan
/// rule forbids `impl From<custodian::NextAction> for Option<ApiNextAction>` (the local type sits
/// behind `Option`'s uncovered type parameter).
fn map_next_action(next_action: crate::clients::custodian::NextAction) -> Option<ApiNextAction> {
    use crate::clients::custodian::{AutoCloseSchedule, next_action::Kind};
    let kind = next_action.kind?;
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

impl From<crate::clients::custodian::Ticket> for ApiTicket {
    fn from(ticket: crate::clients::custodian::Ticket) -> Self {
        Self {
            ticket_id: ticket.ticket_id,
            title: ticket.title,
            project: ticket.project,
            priority: ticket.priority,
            status: ticket.status,
            next_action: ticket.next_action.and_then(map_next_action),
        }
    }
}

async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::auth::AuthenticateRequest {
        username: payload.username,
        password: payload.password,
        mfa_token: payload.mfa_token.unwrap_or_default(),
    };

    let resp_inner = state.auth_client.authenticate(req).await.map_err(|e| {
        tracing::error!("Auth service error: {}", e);
        ApiError::from(e)
    })?;

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
    let req = crate::clients::admin::CreateUserRequest {
        username: payload.username,
        password: payload.password,
        email: payload.email,
        display_name: payload.display_name,
        role: payload.role,
    };

    state
        .admin_client
        .create_user(req)
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::CREATED)
}

/// JSON representation of a user returned by the admin endpoints.
#[derive(Serialize)]
pub struct ApiUser {
    pub id: String,
    pub username: String,
    pub email: String,
    pub display_name: String,
    pub role: i32,
    pub active: bool,
    pub created_at: u64,
}

impl From<crate::clients::admin::User> for ApiUser {
    fn from(user: crate::clients::admin::User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            display_name: user.display_name,
            role: user.role,
            active: user.active,
            created_at: user.created_at,
        }
    }
}

/// Query params for `GET /api/admin/users` (e.g. `?page=0&page_size=50`).
#[derive(Deserialize)]
pub struct ListUsersParams {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Serialize)]
pub struct ListUsersResponse {
    pub users: Vec<ApiUser>,
    pub total_count: u32,
}

async fn list_users(
    State(state): State<AppState>,
    Query(params): Query<ListUsersParams>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::admin::ListUsersRequest {
        page: params.page.unwrap_or(0),
        page_size: params.page_size.unwrap_or(0),
    };

    let resp = state
        .admin_client
        .list_users(req)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(ListUsersResponse {
        users: resp.users.into_iter().map(ApiUser::from).collect(),
        total_count: resp.total_count,
    }))
}

async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let resp = state
        .admin_client
        .get_user(crate::clients::admin::GetUserRequest { id })
        .await
        .map_err(ApiError::from)?;

    let user = resp
        .user
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    Ok(Json(ApiUser::from(user)))
}

#[derive(Deserialize)]
pub struct UpdateUserRequest {
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub role: Option<i32>,
    pub active: Option<bool>,
    pub password: Option<String>,
}

async fn update_user(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateUserRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let req = crate::clients::admin::UpdateUserRequest {
        id,
        email: payload.email,
        display_name: payload.display_name,
        role: payload.role,
        active: payload.active,
        password: payload.password,
    };

    let resp = state
        .admin_client
        .update_user(req)
        .await
        .map_err(ApiError::from)?;

    let user = resp
        .user
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    Ok(Json(ApiUser::from(user)))
}

async fn delete_user(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Soft delete (audit-trail requirement): the admin service marks the user inactive.
    state
        .admin_client
        .delete_user(crate::clients::admin::DeleteUserRequest { id })
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT)
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

    Ok((StatusCode::CREATED, Json(ApiTicket::from(resp))))
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

    Ok(Json(ApiTicket::from(resp)))
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

    let out: Vec<ApiTicket> = tickets.into_iter().map(ApiTicket::from).collect();
    Ok(Json(out))
}

/// Inbound (writable) form of a ticket's next action. Mirrors [`ApiNextAction`] but adds an
/// explicit `none` variant to *clear* a scheduled action. Omitting the `next_action` field
/// entirely leaves the existing action unchanged.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiNextActionInput {
    /// Clear any scheduled action.
    None,
    /// Follow up at the given unix-seconds timestamp.
    FollowUp { at: i64 },
    /// Appointment at the given unix-seconds timestamp.
    Appointment { at: i64 },
    /// Auto-close on a named schedule (`end_of_day` | `hours_24` | `hours_48` | `hours_72`).
    AutoClose { schedule: String },
}

/// Map a writable next-action into the custodian proto message.
///
/// The returned message is always wrapped in `Some(..)` by the caller so the custodian
/// *applies* it (a `kind: None` message clears the action; an absent field leaves it alone).
fn to_proto_next_action(input: ApiNextActionInput) -> crate::clients::custodian::NextAction {
    use crate::clients::custodian::{AutoCloseSchedule, NextAction, next_action::Kind};
    let ts = |at: i64| prost_types::Timestamp {
        seconds: at,
        nanos: 0,
    };
    let kind = match input {
        ApiNextActionInput::None => None,
        ApiNextActionInput::FollowUp { at } => Some(Kind::FollowUp(ts(at))),
        ApiNextActionInput::Appointment { at } => Some(Kind::Appointment(ts(at))),
        ApiNextActionInput::AutoClose { schedule } => {
            let sched = match schedule.as_str() {
                "hours_24" => AutoCloseSchedule::Hours24,
                "hours_48" => AutoCloseSchedule::Hours48,
                "hours_72" => AutoCloseSchedule::Hours72,
                _ => AutoCloseSchedule::EndOfDay,
            };
            Some(Kind::AutoClose(sched as i32))
        }
    };
    NextAction { kind }
}

#[derive(Deserialize)]
pub struct UpdateTicketRequest {
    pub title: Option<String>,
    pub project: Option<String>,
    pub priority: Option<i32>,
    pub status: Option<i32>,
    /// Optional. Present → set/clear the scheduled next action; absent → leave unchanged.
    #[serde(default)]
    pub next_action: Option<ApiNextActionInput>,
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
        next_action: payload.next_action.map(to_proto_next_action),
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

    Ok(Json(ApiTicket::from(resp)))
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub services: std::collections::BTreeMap<String, &'static str>,
}

/// Aggregated health endpoint. Probes the backends LBRP connects to directly and reports
/// overall status (`200 ok` / `503 degraded`). Probes run concurrently:
/// - **auth** via a DB-free `validate_session` liveness call,
/// - **custodian** via its cluster status.
///
/// The DB is intentionally *not* probed here: LBRP does not hold a direct DB connection
/// (it reaches data only through custodian/auth), so DB health is covered transitively —
/// a custodian or auth data call fails if the DB is down. `admin` is Hardened-only and not
/// part of the MVP topology, so it is likewise omitted from the baseline probe.
async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let mut services = std::collections::BTreeMap::new();

    let (auth_ok, custodian_ok) =
        tokio::join!(async { state.auth_client.health().await.is_ok() }, async {
            state.custodian_client.cluster_status().await.is_ok()
        },);

    services.insert("auth".to_string(), if auth_ok { "up" } else { "down" });
    services.insert(
        "custodian".to_string(),
        if custodian_ok { "up" } else { "down" },
    );

    let all_ok = auth_ok && custodian_ok;
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
        // User management (admin). Creation is bootstrap-only above (unauthenticated);
        // listing, reads, updates, and soft-deletes require a valid JWT.
        .route("/admin/users", get(list_users))
        .route(
            "/admin/users/{id}",
            get(get_user).put(update_user).delete(delete_user),
        )
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
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tonic::transport::Channel;
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
            // A benign, DB-free response so the `/health` liveness probe sees a successful
            // round-trip (the empty token is "invalid", which is exactly what the probe expects).
            Ok(tonic::Response::new(
                crate::clients::auth::ValidateSessionResponse {
                    valid: false,
                    user: None,
                    error: "invalid".to_string(),
                },
            ))
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
            req: tonic::Request<crate::clients::admin::GetUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::GetUserResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(
                crate::clients::admin::GetUserResponse {
                    user: Some(crate::clients::admin::User {
                        id: req.into_inner().id,
                        username: "alice".to_string(),
                        email: "a@example.com".to_string(),
                        display_name: "Alice".to_string(),
                        role: 0,
                        active: true,
                        created_at: 123,
                    }),
                },
            ))
        }

        async fn list_users(
            &self,
            _req: tonic::Request<crate::clients::admin::ListUsersRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::ListUsersResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(
                crate::clients::admin::ListUsersResponse {
                    users: vec![crate::clients::admin::User {
                        id: "id-1".to_string(),
                        username: "alice".to_string(),
                        email: "a@example.com".to_string(),
                        display_name: "Alice".to_string(),
                        role: 0,
                        active: true,
                        created_at: 123,
                    }],
                    total_count: 1,
                },
            ))
        }

        async fn update_user(
            &self,
            req: tonic::Request<crate::clients::admin::UpdateUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::UpdateUserResponse>, tonic::Status>
        {
            let r = req.into_inner();
            Ok(tonic::Response::new(
                crate::clients::admin::UpdateUserResponse {
                    user: Some(crate::clients::admin::User {
                        id: r.id,
                        username: "alice".to_string(),
                        email: r.email.unwrap_or_else(|| "a@example.com".to_string()),
                        display_name: r.display_name.unwrap_or_else(|| "Alice".to_string()),
                        role: r.role.unwrap_or(0),
                        active: r.active.unwrap_or(true),
                        created_at: 123,
                    }),
                },
            ))
        }

        async fn delete_user(
            &self,
            _req: tonic::Request<crate::clients::admin::DeleteUserRequest>,
        ) -> Result<tonic::Response<crate::clients::admin::DeleteUserResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(
                crate::clients::admin::DeleteUserResponse { success: true },
            ))
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
        test_support::spawn_grpc!(
            crate::clients::auth::auth_service_server::AuthServiceServer::new(svc)
        )
    }

    fn start_mock_admin(svc: MockAdminSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        test_support::spawn_grpc!(
            crate::clients::admin::admin_service_server::AdminServiceServer::new(svc)
        )
    }

    fn start_mock_custodian(svc: MockCustodianSvc) -> (std::net::SocketAddr, oneshot::Sender<()>) {
        test_support::spawn_grpc!(
            crate::clients::custodian::custodian_service_server::CustodianServiceServer::new(svc)
        )
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
            auth_client: AuthClient::from_channel(ch),
            ..test_state()
        }
    }

    fn make_state_with_admin_ch(ch: Channel) -> AppState {
        AppState {
            admin_client: AdminClient::from_channel(ch),
            ..test_state()
        }
    }

    fn make_state_with_custodian_ch(ch: Channel) -> AppState {
        AppState {
            custodian_client: CustodianClient::from_channel(ch),
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
    async fn list_users_returns_users_from_backend() {
        let (addr, shutdown) = start_mock_admin(MockAdminSvc);
        let ch = connect_retry(addr).await;
        let result = list_users(
            State(make_state_with_admin_ch(ch)),
            Query(ListUsersParams {
                page: Some(0),
                page_size: Some(50),
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_user_returns_user_on_backend_success() {
        let (addr, shutdown) = start_mock_admin(MockAdminSvc);
        let ch = connect_retry(addr).await;
        let result = get_user(State(make_state_with_admin_ch(ch)), Path("id-1".into())).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn update_user_returns_user_on_backend_success() {
        let (addr, shutdown) = start_mock_admin(MockAdminSvc);
        let ch = connect_retry(addr).await;
        let result = update_user(
            State(make_state_with_admin_ch(ch)),
            Path("id-1".into()),
            Json(UpdateUserRequest {
                email: Some("new@example.com".into()),
                display_name: None,
                role: Some(1),
                active: Some(false),
                password: None,
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_user_returns_no_content_on_backend_success() {
        let (addr, shutdown) = start_mock_admin(MockAdminSvc);
        let ch = connect_retry(addr).await;
        let result = delete_user(State(make_state_with_admin_ch(ch)), Path("id-1".into())).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn user_management_handlers_error_when_backend_unreachable() {
        assert!(
            list_users(
                State(test_state()),
                Query(ListUsersParams {
                    page: None,
                    page_size: None,
                }),
            )
            .await
            .is_err()
        );
        assert!(
            get_user(State(test_state()), Path("id-1".into()))
                .await
                .is_err()
        );
        assert!(
            delete_user(State(test_state()), Path("id-1".into()))
                .await
                .is_err()
        );
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
                next_action: Some(ApiNextActionInput::FollowUp { at: 1_700_000_000 }),
            }),
        )
        .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[test]
    fn to_proto_next_action_maps_each_variant() {
        use crate::clients::custodian::{AutoCloseSchedule, next_action::Kind};

        assert!(
            to_proto_next_action(ApiNextActionInput::None)
                .kind
                .is_none()
        );

        let follow = to_proto_next_action(ApiNextActionInput::FollowUp { at: 42 }).kind;
        assert!(matches!(follow, Some(Kind::FollowUp(ts)) if ts.seconds == 42));

        let appt = to_proto_next_action(ApiNextActionInput::Appointment { at: 99 }).kind;
        assert!(matches!(appt, Some(Kind::Appointment(ts)) if ts.seconds == 99));

        let ac = to_proto_next_action(ApiNextActionInput::AutoClose {
            schedule: "hours_48".into(),
        })
        .kind;
        assert!(matches!(ac, Some(Kind::AutoClose(v)) if v == AutoCloseSchedule::Hours48 as i32));

        // Unknown schedule falls back to end-of-day.
        let eod = to_proto_next_action(ApiNextActionInput::AutoClose {
            schedule: "bogus".into(),
        })
        .kind;
        assert!(matches!(eod, Some(Kind::AutoClose(v)) if v == AutoCloseSchedule::EndOfDay as i32));
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
    async fn health_reports_ok_when_auth_and_custodian_reachable() {
        let (auth_addr, auth_shutdown) = start_mock_auth(MockAuthSvc {
            error_code: None,
            auth_success: true,
        });
        let (cust_addr, cust_shutdown) = start_mock_custodian(MockCustodianSvc);
        let auth_ch = connect_retry(auth_addr).await;
        let cust_ch = connect_retry(cust_addr).await;

        let state = AppState {
            auth_client: AuthClient::from_channel(auth_ch),
            custodian_client: CustodianClient::from_channel(cust_ch),
            ..test_state()
        };

        let resp = health(State(state)).await.into_response();
        let _ = auth_shutdown.send(());
        let _ = cust_shutdown.send(());
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_reports_degraded_when_auth_down_but_custodian_up() {
        // Only custodian is reachable; auth points at a dead port → degraded/503.
        let (cust_addr, cust_shutdown) = start_mock_custodian(MockCustodianSvc);
        let cust_ch = connect_retry(cust_addr).await;
        let resp = health(State(make_state_with_custodian_ch(cust_ch)))
            .await
            .into_response();
        let _ = cust_shutdown.send(());
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn health_reports_degraded_when_all_backends_unreachable() {
        // Lazy channels to dead ports: both probes fail -> degraded/503.
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
            auth_client: AuthClient::from_channel(channel.clone()),
            admin_client: AdminClient::from_channel(channel.clone()),
            custodian_client: CustodianClient::from_channel(channel),
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

        let mapped = ApiTicket::from(source);
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
