//! Alert reporting: forwards honeypot intrusion events to the admin service via gRPC.

#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

use std::time::{SystemTime, UNIX_EPOCH};

use proto::admin::admin_service_client::AdminServiceClient;
use tonic::transport::Channel;

/// Intrusion event captured by the honeypot when one of its trap endpoints is hit.
#[derive(Debug, Clone)]
pub struct IntrusionEvent {
    pub timestamp: SystemTime,
    pub source_ip: String,
    pub user_agent: Option<String>,
    pub endpoint_accessed: String,
    pub request_method: String,
    pub request_body: Option<String>,
    pub tls_fingerprint: Option<String>,
}

impl IntrusionEvent {
    /// Create a new intrusion event stamped with the current time.
    #[must_use]
    pub fn new(source_ip: String, endpoint: String, method: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            source_ip,
            user_agent: None,
            endpoint_accessed: endpoint,
            request_method: method,
            request_body: None,
            tls_fingerprint: None,
        }
    }

    /// Log the event locally (stderr). Always called, even when admin is unreachable.
    pub fn report(&self) {
        eprintln!("🚨 INTRUSION DETECTED: {self:?}");
    }

    /// Convert to the admin-service wire representation.
    #[must_use]
    pub fn to_proto(&self) -> proto::admin::IntrusionEvent {
        let timestamp_unix = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        proto::admin::IntrusionEvent {
            timestamp_unix,
            source_ip: self.source_ip.clone(),
            endpoint_accessed: self.endpoint_accessed.clone(),
            request_method: self.request_method.clone(),
            user_agent: self.user_agent.clone(),
            request_body: self.request_body.clone(),
            tls_fingerprint: self.tls_fingerprint.clone(),
        }
    }
}

/// Forwards intrusion events to the admin service's `RecordIntrusion` RPC. When `ADMIN_ADDR`
/// is unset the reporter only logs locally, so the honeypot still runs standalone.
#[derive(Clone, Default)]
pub struct Reporter {
    client: Option<AdminServiceClient<Channel>>,
}

impl Reporter {
    /// Build a reporter from `ADMIN_ADDR` (mTLS applied via `proto::tls` when configured).
    #[must_use]
    pub fn from_env() -> Self {
        let client = std::env::var("ADMIN_ADDR")
            .ok()
            .and_then(|addr| proto::tls::connect_lazy(&addr).ok())
            .map(AdminServiceClient::new);
        Self { client }
    }

    /// Wrap an existing admin client (used in tests).
    #[must_use]
    pub fn with_client(client: AdminServiceClient<Channel>) -> Self {
        Self {
            client: Some(client),
        }
    }

    /// Report an event: always logged locally, and (if configured) sent to the admin service.
    pub async fn report(&self, event: &IntrusionEvent) {
        event.report();
        if let Some(client) = &self.client {
            let mut client = client.clone();
            if let Err(e) = client.record_intrusion(event.to_proto()).await {
                eprintln!("honeypot: failed to report intrusion to admin: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intrusion_event_creation_and_optional_fields() {
        let mut event = IntrusionEvent::new(
            "10.0.0.1".to_string(),
            "/api/backup".to_string(),
            "POST".to_string(),
        );
        assert_eq!(event.source_ip, "10.0.0.1");
        assert_eq!(event.endpoint_accessed, "/api/backup");
        assert!(event.user_agent.is_none());

        event.user_agent = Some("Mozilla/5.0".to_string());
        event.report(); // must not panic with fields populated
        assert_eq!(event.user_agent.as_deref(), Some("Mozilla/5.0"));
    }

    #[test]
    fn to_proto_carries_all_fields() {
        let mut event = IntrusionEvent::new(
            "1.2.3.4".to_string(),
            "/wallet".to_string(),
            "GET".to_string(),
        );
        event.user_agent = Some("curl".to_string());
        event.tls_fingerprint = Some("JA3".to_string());
        let proto = event.to_proto();
        assert_eq!(proto.source_ip, "1.2.3.4");
        assert_eq!(proto.endpoint_accessed, "/wallet");
        assert_eq!(proto.request_method, "GET");
        assert_eq!(proto.user_agent.as_deref(), Some("curl"));
        assert_eq!(proto.tls_fingerprint.as_deref(), Some("JA3"));
        assert!(proto.timestamp_unix > 0);
    }

    #[test]
    fn reporter_without_admin_addr_is_inert() {
        let reporter = Reporter::default();
        assert!(reporter.client.is_none());
    }

    #[test]
    fn from_env_without_admin_addr_is_inert() {
        // ADMIN_ADDR is unset in the test environment → no client is configured.
        let reporter = Reporter::from_env();
        assert!(reporter.client.is_none());
    }

    #[tokio::test]
    async fn report_logs_when_admin_send_fails() {
        // A lazily-connected client pointed at an address with nothing listening: report() logs
        // locally and the subsequent gRPC send fails, exercising the error branch.
        let channel = proto::tls::connect_lazy("http://127.0.0.1:1").unwrap();
        let reporter = Reporter::with_client(AdminServiceClient::new(channel));
        let event =
            IntrusionEvent::new("9.9.9.9".to_string(), "/x".to_string(), "GET".to_string());
        reporter.report(&event).await; // must not panic; the failed send is logged
    }
}
