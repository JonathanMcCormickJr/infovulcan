use gloo_net::http::Request;
use serde::{Deserialize, Serialize};

const TOKEN_KEY: &str = "infovulcan_demo_token";

/// Errors surfaced by the REST client. `Display` renders a user-facing message (the UI shows it
/// directly), while the variants let callers distinguish a transport failure from a server-side
/// rejection if they need to.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The request could not be sent / completed, or the response body could not be parsed.
    #[error("network error: {0}")]
    Transport(#[from] gloo_net::Error),
    /// The server returned a non-success status; the payload is its error body.
    #[error("{0}")]
    Server(String),
}

/// A ticket's scheduled next action (read-only mirror of the REST `next_action` field).
/// `None` (JSON `null`) means no action is scheduled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NextAction {
    FollowUp { at: i64 },
    Appointment { at: i64 },
    AutoClose { schedule: String },
}

impl NextAction {
    /// A short human-readable summary for display.
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            NextAction::FollowUp { at } => format!("Follow up @ {at}"),
            NextAction::Appointment { at } => format!("Appointment @ {at}"),
            NextAction::AutoClose { schedule } => format!("Auto-close ({schedule})"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ticket {
    pub ticket_id: u64,
    pub title: String,
    pub project: String,
    pub priority: i32,
    pub status: i32,
    #[serde(default)]
    pub next_action: Option<NextAction>,
}

#[derive(Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    pub mfa_token: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Serialize)]
pub struct CreateTicketRequest {
    pub title: String,
    pub project: String,
    pub account_uuid: String,
    pub symptom: i32,
    pub priority: i32,
}

/// Writable next action sent with a ticket update. Mirrors the LBRP REST input:
/// `None` (the `none` variant) clears the action; omitting the field leaves it unchanged.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NextActionInput {
    None,
    FollowUp { at: i64 },
    Appointment { at: i64 },
    AutoClose { schedule: String },
}

#[derive(Serialize)]
pub struct UpdateTicketRequest {
    pub title: Option<String>,
    pub project: Option<String>,
    pub priority: Option<i32>,
    pub status: Option<i32>,
    /// Omitted from the request body when `None` → the backend leaves the action unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_action: Option<NextActionInput>,
}

#[derive(Serialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub email: String,
    pub display_name: String,
    pub role: i32,
}

fn storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok()?
}

pub fn get_token() -> Option<String> {
    storage()?.get_item(TOKEN_KEY).ok()?
}

pub fn clear_token() {
    if let Some(storage) = storage() {
        let _ = storage.remove_item(TOKEN_KEY);
    }
}

fn set_token(token: &str) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(TOKEN_KEY, token);
    }
}

/// Read a non-success response's body as the server error message.
async fn server_error(response: gloo_net::http::Response, fallback: &str) -> ApiError {
    ApiError::Server(
        response
            .text()
            .await
            .unwrap_or_else(|_| fallback.to_string()),
    )
}

pub async fn login(username: String, password: String) -> Result<(), ApiError> {
    let payload = LoginRequest {
        username,
        password,
        mfa_token: None,
    };

    let response = Request::post("/auth/login").json(&payload)?.send().await?;

    if !response.ok() {
        return Err(server_error(response, "Login failed").await);
    }

    let login_response: LoginResponse = response.json().await?;
    set_token(&login_response.token);
    Ok(())
}

pub async fn create_user(token: &str, payload: &CreateUserRequest) -> Result<(), ApiError> {
    let response = Request::post("/api/admin/users")
        .header("Authorization", &format!("Bearer {token}"))
        .json(payload)?
        .send()
        .await?;

    if response.ok() {
        Ok(())
    } else {
        Err(server_error(response, "User creation failed").await)
    }
}

pub async fn create_ticket(token: &str, payload: &CreateTicketRequest) -> Result<Ticket, ApiError> {
    let response = Request::post("/api/tickets")
        .header("Authorization", &format!("Bearer {token}"))
        .json(payload)?
        .send()
        .await?;

    if !response.ok() {
        return Err(server_error(response, "Ticket creation failed").await);
    }

    Ok(response.json().await?)
}

/// Filters for the ticket list query (`GET /api/tickets`). Empty fields are omitted.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListTicketsFilter {
    pub status: Option<i32>,
    pub assignee: String,
    pub account: String,
    pub project: String,
    pub include_deleted: bool,
    pub limit: Option<u32>,
}

impl ListTicketsFilter {
    /// Build the `?key=value&…` query string (without a leading `?`). Blank/None fields are
    /// skipped so the backend treats them as "no filter".
    #[must_use]
    pub fn to_query_string(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(status) = self.status {
            parts.push(format!("status={status}"));
        }
        if !self.assignee.trim().is_empty() {
            parts.push(format!("assignee={}", self.assignee.trim()));
        }
        if !self.account.trim().is_empty() {
            parts.push(format!("account={}", self.account.trim()));
        }
        if !self.project.trim().is_empty() {
            parts.push(format!("project={}", self.project.trim()));
        }
        if self.include_deleted {
            parts.push("include_deleted=true".to_string());
        }
        if let Some(limit) = self.limit {
            parts.push(format!("limit={limit}"));
        }
        parts.join("&")
    }
}

pub async fn list_tickets(
    token: &str,
    filter: &ListTicketsFilter,
) -> Result<Vec<Ticket>, ApiError> {
    let query = filter.to_query_string();
    let url = if query.is_empty() {
        "/api/tickets".to_string()
    } else {
        format!("/api/tickets?{query}")
    };

    let response = Request::get(&url)
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await?;

    if !response.ok() {
        return Err(server_error(response, "Ticket list failed").await);
    }

    Ok(response.json().await?)
}

pub async fn fetch_ticket(token: &str, ticket_id: u64) -> Result<Ticket, ApiError> {
    let response = Request::get(&format!("/api/tickets/{ticket_id}"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await?;

    if !response.ok() {
        return Err(server_error(response, "Ticket lookup failed").await);
    }

    Ok(response.json().await?)
}

pub async fn update_ticket(
    token: &str,
    ticket_id: u64,
    payload: &UpdateTicketRequest,
) -> Result<Ticket, ApiError> {
    let response = Request::put(&format!("/api/tickets/{ticket_id}"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(payload)?
        .send()
        .await?;

    if !response.ok() {
        return Err(server_error(response, "Ticket update failed").await);
    }

    Ok(response.json().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_produces_no_query() {
        assert_eq!(ListTicketsFilter::default().to_query_string(), "");
    }

    #[test]
    fn filter_builds_query_in_field_order() {
        let filter = ListTicketsFilter {
            status: Some(2),
            assignee: "  alice  ".to_string(),
            account: String::new(),
            project: "acme".to_string(),
            include_deleted: true,
            limit: Some(50),
        };
        assert_eq!(
            filter.to_query_string(),
            "status=2&assignee=alice&project=acme&include_deleted=true&limit=50"
        );
    }

    #[test]
    fn blank_strings_are_skipped() {
        let filter = ListTicketsFilter {
            assignee: "   ".to_string(),
            ..Default::default()
        };
        assert_eq!(filter.to_query_string(), "");
    }

    #[test]
    fn next_action_input_serializes_with_type_tag() {
        let none = serde_json::to_string(&NextActionInput::None).expect("serialize");
        assert_eq!(none, r#"{"type":"none"}"#);

        let follow = serde_json::to_string(&NextActionInput::FollowUp { at: 42 }).expect("ser");
        assert_eq!(follow, r#"{"type":"follow_up","at":42}"#);

        let ac = serde_json::to_string(&NextActionInput::AutoClose {
            schedule: "hours_24".to_string(),
        })
        .expect("ser");
        assert_eq!(ac, r#"{"type":"auto_close","schedule":"hours_24"}"#);
    }

    #[test]
    fn update_request_omits_next_action_when_unchanged() {
        let req = UpdateTicketRequest {
            title: Some("t".to_string()),
            project: None,
            priority: None,
            status: None,
            next_action: None,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(
            !json.contains("next_action"),
            "next_action must be omitted when None, got: {json}"
        );
    }

    #[test]
    fn next_action_summaries_render() {
        assert_eq!(
            NextAction::FollowUp { at: 5 }.summary(),
            "Follow up @ 5".to_string()
        );
        assert_eq!(
            NextAction::Appointment { at: 9 }.summary(),
            "Appointment @ 9".to_string()
        );
        assert_eq!(
            NextAction::AutoClose {
                schedule: "hours_24".to_string()
            }
            .summary(),
            "Auto-close (hours_24)".to_string()
        );
    }
}
