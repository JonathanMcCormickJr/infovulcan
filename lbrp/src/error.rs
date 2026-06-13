//! Consistent JSON error envelope for the REST API.
//!
//! Every handler error is rendered as `{ "error": { "code": "...", "message": "..." } }`
//! with an appropriate HTTP status, so the web frontend can handle failures uniformly.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// A REST API error carrying an HTTP status, a stable machine-readable `code`, and a
/// human-readable `message`.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    #[must_use]
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: String,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code.to_string(),
                message: self.message,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<tonic::Status> for ApiError {
    fn from(status: tonic::Status) -> Self {
        let (http, code) = match status.code() {
            tonic::Code::Unauthenticated => (StatusCode::UNAUTHORIZED, "unauthenticated"),
            tonic::Code::PermissionDenied => (StatusCode::FORBIDDEN, "permission_denied"),
            tonic::Code::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid_argument"),
            tonic::Code::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            tonic::Code::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        Self::new(http, code, status.message().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_set_status_and_code() {
        assert_eq!(
            ApiError::unauthorized("nope").status,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(ApiError::unauthorized("nope").code, "unauthenticated");
        assert_eq!(
            ApiError::internal("boom").status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(ApiError::internal("boom").code, "internal");
        assert_eq!(ApiError::not_found("gone").status, StatusCode::NOT_FOUND);
        assert_eq!(ApiError::not_found("gone").code, "not_found");
    }

    #[test]
    fn from_tonic_status_maps_every_code() {
        let cases = [
            (
                tonic::Code::Unauthenticated,
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
            ),
            (
                tonic::Code::PermissionDenied,
                StatusCode::FORBIDDEN,
                "permission_denied",
            ),
            (
                tonic::Code::InvalidArgument,
                StatusCode::BAD_REQUEST,
                "invalid_argument",
            ),
            (tonic::Code::NotFound, StatusCode::NOT_FOUND, "not_found"),
            (
                tonic::Code::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            // Any other code falls through to the internal mapping.
            (
                tonic::Code::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        ];
        for (code, http, expected_code) in cases {
            let err: ApiError = tonic::Status::new(code, "msg").into();
            assert_eq!(err.status, http);
            assert_eq!(err.code, expected_code);
            assert_eq!(err.message, "msg");
        }
    }

    #[test]
    fn into_response_uses_the_error_status() {
        let response = ApiError::not_found("missing").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
