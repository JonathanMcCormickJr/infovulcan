//! Error types for `InfoVulcan` system operations.

use thiserror::Error;

/// `InfoVulcan` system errors
#[derive(Error, Debug)]
pub enum InfoVulcanError {
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    #[error("Authorization failed: insufficient permissions")]
    AuthorizationError,

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Resource conflict: {0}")]
    Conflict(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Internal server error: {0}")]
    InternalError(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),
}

/// Result type alias for `InfoVulcan` operations
pub type InfoVulcanResult<T> = Result<T, InfoVulcanError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infovulcan_error_display_all_variants() {
        let errors: Vec<InfoVulcanError> = vec![
            InfoVulcanError::ValidationError("bad input".to_string()),
            InfoVulcanError::AuthenticationError("wrong password".to_string()),
            InfoVulcanError::AuthorizationError,
            InfoVulcanError::NotFound("ticket 42".to_string()),
            InfoVulcanError::Conflict("duplicate".to_string()),
            InfoVulcanError::DatabaseError("connection lost".to_string()),
            InfoVulcanError::NetworkError("timeout".to_string()),
            InfoVulcanError::InternalError("unexpected".to_string()),
            InfoVulcanError::RateLimitExceeded,
            InfoVulcanError::ServiceUnavailable("maintenance".to_string()),
        ];

        let expected_substrings = [
            "bad input",
            "wrong password",
            "insufficient permissions",
            "ticket 42",
            "duplicate",
            "connection lost",
            "timeout",
            "unexpected",
            "Rate limit",
            "maintenance",
        ];

        for (error, expected) in errors.iter().zip(expected_substrings.iter()) {
            let msg = error.to_string();
            assert!(
                msg.contains(expected),
                "Error message '{msg}' should contain '{expected}'"
            );
        }
    }

    #[test]
    fn test_infovulcan_result_type_alias() {
        let ok: InfoVulcanResult<u32> = Ok(42);
        assert!(ok.is_ok());

        let err: InfoVulcanResult<u32> = Err(InfoVulcanError::NotFound("x".to_string()));
        assert!(err.is_err());
    }
}
