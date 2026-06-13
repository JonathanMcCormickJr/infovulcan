//! Chaos service for fault injection and resilience testing
//!
//! This service provides controlled fault injection capabilities to test
//! system resilience under various failure scenarios.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

pub use proto::chaos;

use chaos::chaos_service_server::ChaosService;
use chaos::{ChaosAck, ChaosRequest, ListRequest, ScenarioCatalog, StopRequest};

/// An injected scenario that is currently *active*: the parsed fault plus the background task
/// that auto-expires it after its configured duration.
#[derive(Debug)]
struct ActiveScenario {
    #[allow(dead_code)]
    scenario: ChaosScenario,
    expiry_task: JoinHandle<()>,
}

/// Chaos service implementation.
#[derive(Debug, Default)]
pub struct ChaosServiceImpl {
    active_scenarios: Arc<RwLock<HashMap<String, ActiveScenario>>>,
    next_id: Arc<AtomicU64>,
}

/// Chaos scenario types
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ChaosScenario {
    NetworkLatency {
        target_service: String,
        delay_ms: u64,
        duration_ms: u64,
    },
    ServiceCrash {
        target_service: String,
        crash_probability: f64,
        duration_ms: u64,
    },
    DiskIODelay {
        target_service: String,
        delay_ms: u64,
        duration_ms: u64,
    },
    RaftLeaderFailure {
        node_id: u64,
        duration_ms: u64,
    },
    NetworkPartition {
        partition_groups: Vec<Vec<String>>,
        duration_ms: u64,
    },
}

impl ChaosScenario {
    /// How long this scenario stays active before it auto-expires.
    fn duration_ms(&self) -> u64 {
        match self {
            Self::NetworkLatency { duration_ms, .. }
            | Self::ServiceCrash { duration_ms, .. }
            | Self::DiskIODelay { duration_ms, .. }
            | Self::RaftLeaderFailure { duration_ms, .. }
            | Self::NetworkPartition { duration_ms, .. } => *duration_ms,
        }
    }
}

impl ChaosServiceImpl {
    /// Admin authentication for mutating operations. If `CHAOS_AUTH_TOKEN` is set, the request
    /// must carry a matching `x-chaos-token` metadata header. If it is unset, access is open
    /// (dev/test default) — chaos is meant to run only in non-production environments.
    fn check_auth<T>(request: &Request<T>) -> Result<(), Status> {
        let expected = std::env::var("CHAOS_AUTH_TOKEN").ok();
        let provided = request
            .metadata()
            .get("x-chaos-token")
            .and_then(|v| v.to_str().ok());
        Self::authorize(expected.as_deref(), provided)
    }

    /// Pure authorization decision: open when no token is configured, otherwise the provided
    /// token must match exactly.
    fn authorize(expected: Option<&str>, provided: Option<&str>) -> Result<(), Status> {
        match expected {
            None => Ok(()),
            Some(exp) if provided == Some(exp) => Ok(()),
            Some(_) => Err(Status::permission_denied(
                "chaos: missing or invalid admin token",
            )),
        }
    }

    /// Parses a [`ChaosRequest`] into a typed [`ChaosScenario`].
    fn parse_scenario(req: &ChaosRequest) -> Result<ChaosScenario, Status> {
        let p = &req.parameters;
        let duration_ms = |default| {
            p.get("duration_ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };

        match req.scenario_type.as_str() {
            "network_latency" => Ok(ChaosScenario::NetworkLatency {
                target_service: p
                    .get("target_service")
                    .cloned()
                    .unwrap_or_else(|| "all".to_string()),
                delay_ms: p
                    .get("delay_ms")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(100),
                duration_ms: duration_ms(30_000),
            }),
            "service_crash" => Ok(ChaosScenario::ServiceCrash {
                target_service: p
                    .get("target_service")
                    .cloned()
                    .unwrap_or_else(|| "random".to_string()),
                crash_probability: p
                    .get("crash_probability")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.1),
                duration_ms: duration_ms(30_000),
            }),
            "disk_io_delay" => Ok(ChaosScenario::DiskIODelay {
                target_service: p
                    .get("target_service")
                    .cloned()
                    .unwrap_or_else(|| "db".to_string()),
                delay_ms: p.get("delay_ms").and_then(|v| v.parse().ok()).unwrap_or(50),
                duration_ms: duration_ms(30_000),
            }),
            "raft_leader_failure" => Ok(ChaosScenario::RaftLeaderFailure {
                node_id: p.get("node_id").and_then(|v| v.parse().ok()).unwrap_or(1),
                duration_ms: duration_ms(10_000),
            }),
            "network_partition" => Ok(ChaosScenario::NetworkPartition {
                // Simplified two-group partition; a production implementation would parse these
                // from the request parameters.
                partition_groups: vec![
                    vec!["node-1".to_string(), "node-2".to_string()],
                    vec![
                        "node-3".to_string(),
                        "node-4".to_string(),
                        "node-5".to_string(),
                    ],
                ],
                duration_ms: duration_ms(15_000),
            }),
            unknown => Err(Status::invalid_argument(format!(
                "Unknown scenario type: {unknown}"
            ))),
        }
    }
}

#[tonic::async_trait]
impl ChaosService for ChaosServiceImpl {
    async fn inject_scenario(
        &self,
        request: Request<ChaosRequest>,
    ) -> Result<Response<ChaosAck>, Status> {
        Self::check_auth(&request)?;
        let req = request.into_inner();
        let scenario = Self::parse_scenario(&req)?;

        // Unique, deterministic id (a per-second timestamp collides under rapid injection).
        let seq = self.next_id.fetch_add(1, Ordering::SeqCst);
        let scenario_id = format!("{}_{seq}", req.scenario_type);

        // Apply the scenario as a live, time-bounded fault: spawn a task that keeps it active
        // for its configured duration and then auto-expires it.
        let duration = Duration::from_millis(scenario.duration_ms());
        let scenarios = self.active_scenarios.clone();
        let id_for_task = scenario_id.clone();
        let expiry_task = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            scenarios.write().await.remove(&id_for_task);
            tracing::info!(id = %id_for_task, "chaos scenario expired");
        });

        self.active_scenarios.write().await.insert(
            scenario_id.clone(),
            ActiveScenario {
                scenario,
                expiry_task,
            },
        );

        tracing::info!(scenario_type = %req.scenario_type, id = %scenario_id, "chaos scenario injected");

        Ok(Response::new(ChaosAck {
            scenario_id,
            status: "injected".to_string(),
            message: format!("Chaos scenario {} injected successfully", req.scenario_type),
        }))
    }

    async fn stop_scenario(
        &self,
        request: Request<StopRequest>,
    ) -> Result<Response<ChaosAck>, Status> {
        Self::check_auth(&request)?;
        let req = request.into_inner();

        if let Some(active) = self.active_scenarios.write().await.remove(&req.scenario_id) {
            // Cancel the pending auto-expiry task; the fault is being stopped early.
            active.expiry_task.abort();
            tracing::info!(id = %req.scenario_id, "chaos scenario stopped");
            Ok(Response::new(ChaosAck {
                scenario_id: req.scenario_id,
                status: "stopped".to_string(),
                message: "Chaos scenario stopped successfully".to_string(),
            }))
        } else {
            Err(Status::not_found(format!(
                "Scenario {} not found",
                req.scenario_id
            )))
        }
    }

    async fn list_scenarios(
        &self,
        _request: Request<ListRequest>,
    ) -> Result<Response<ScenarioCatalog>, Status> {
        let scenarios = self.active_scenarios.read().await;
        let scenario_list = scenarios.keys().cloned().collect();

        Ok(Response::new(ScenarioCatalog {
            scenario_ids: scenario_list,
            available_types: vec![
                "network_latency".to_string(),
                "service_crash".to_string(),
                "disk_io_delay".to_string(),
                "raft_leader_failure".to_string(),
                "network_partition".to_string(),
            ],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Request;

    #[tokio::test]
    async fn test_inject_network_latency_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ChaosRequest {
            scenario_type: "network_latency".to_string(),
            parameters: vec![
                ("delay_ms".to_string(), "200".to_string()),
                ("duration_ms".to_string(), "10000".to_string()),
                ("target_service".to_string(), "db".to_string()),
            ]
            .into_iter()
            .collect(),
        });

        let response = service.inject_scenario(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.scenario_id.starts_with("network_latency_"));
        assert_eq!(ack.status, "injected");
        assert!(ack.message.contains("network_latency"));
    }

    #[test]
    fn authorize_open_when_unconfigured_else_requires_matching_token() {
        // No token configured -> open.
        assert!(ChaosServiceImpl::authorize(None, None).is_ok());
        assert!(ChaosServiceImpl::authorize(None, Some("anything")).is_ok());
        // Token configured -> must match.
        assert!(ChaosServiceImpl::authorize(Some("secret"), Some("secret")).is_ok());
        assert_eq!(
            ChaosServiceImpl::authorize(Some("secret"), Some("wrong"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            ChaosServiceImpl::authorize(Some("secret"), None)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn injected_scenario_auto_expires_after_its_duration() {
        let service = ChaosServiceImpl::default();
        let request = Request::new(ChaosRequest {
            scenario_type: "network_latency".to_string(),
            parameters: vec![("duration_ms".to_string(), "50".to_string())]
                .into_iter()
                .collect(),
        });
        service.inject_scenario(request).await.unwrap();

        // Active immediately after injection.
        let active = service
            .list_scenarios(Request::new(ListRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(active.scenario_ids.len(), 1);

        // After its 50ms lifetime, the background task auto-expires it.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let after = service
            .list_scenarios(Request::new(ListRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(
            after.scenario_ids.is_empty(),
            "scenario should have auto-expired"
        );
    }

    #[tokio::test]
    async fn test_inject_invalid_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ChaosRequest {
            scenario_type: "invalid_scenario".to_string(),
            parameters: HashMap::new(),
        });

        let error = service.inject_scenario(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_stop_scenario() {
        let service = ChaosServiceImpl::default();

        // First inject a scenario
        let inject_request = Request::new(ChaosRequest {
            scenario_type: "service_crash".to_string(),
            parameters: vec![("crash_probability".to_string(), "0.5".to_string())]
                .into_iter()
                .collect(),
        });

        let inject_response = service.inject_scenario(inject_request).await.unwrap();
        let scenario_id = inject_response.into_inner().scenario_id;

        // Now stop it
        let stop_request = Request::new(StopRequest { scenario_id });
        let stop_response = service.stop_scenario(stop_request).await.unwrap();
        let ack = stop_response.into_inner();

        assert_eq!(ack.status, "stopped");
    }

    #[tokio::test]
    async fn test_stop_nonexistent_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(StopRequest {
            scenario_id: "nonexistent".to_string(),
        });

        let error = service.stop_scenario(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_list_scenarios() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ListRequest {});
        let response = service.list_scenarios(request).await.unwrap();
        let catalog = response.into_inner();

        assert!(catalog.scenario_ids.is_empty()); // No active scenarios initially
        assert!(
            catalog
                .available_types
                .contains(&"network_latency".to_string())
        );
        assert!(
            catalog
                .available_types
                .contains(&"raft_leader_failure".to_string())
        );
    }

    #[tokio::test]
    async fn test_inject_disk_io_delay_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ChaosRequest {
            scenario_type: "disk_io_delay".to_string(),
            parameters: vec![
                ("delay_ms".to_string(), "50".to_string()),
                ("target_service".to_string(), "db".to_string()),
            ]
            .into_iter()
            .collect(),
        });

        let response = service.inject_scenario(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.scenario_id.starts_with("disk_io_delay_"));
        assert_eq!(ack.status, "injected");
    }

    #[tokio::test]
    async fn test_inject_raft_leader_failure_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ChaosRequest {
            scenario_type: "raft_leader_failure".to_string(),
            parameters: vec![("node_id".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        });

        let response = service.inject_scenario(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.scenario_id.starts_with("raft_leader_failure_"));
        assert_eq!(ack.status, "injected");
    }

    #[tokio::test]
    async fn test_inject_network_partition_scenario() {
        let service = ChaosServiceImpl::default();

        let request = Request::new(ChaosRequest {
            scenario_type: "network_partition".to_string(),
            parameters: HashMap::new(),
        });

        let response = service.inject_scenario(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.scenario_id.starts_with("network_partition_"));
        assert_eq!(ack.status, "injected");
    }

    #[tokio::test]
    async fn test_inject_service_crash_with_defaults() {
        let service = ChaosServiceImpl::default();

        // Test service_crash with no parameters (all defaults)
        let request = Request::new(ChaosRequest {
            scenario_type: "service_crash".to_string(),
            parameters: HashMap::new(),
        });

        let response = service.inject_scenario(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.scenario_id.starts_with("service_crash_"));
        assert_eq!(ack.status, "injected");
    }

    #[tokio::test]
    async fn test_list_scenarios_after_injecting() {
        let service = ChaosServiceImpl::default();

        // Inject a scenario first
        let request = Request::new(ChaosRequest {
            scenario_type: "network_latency".to_string(),
            parameters: HashMap::new(),
        });
        service.inject_scenario(request).await.unwrap();

        // List should now show one active scenario
        let list_response = service
            .list_scenarios(Request::new(ListRequest {}))
            .await
            .unwrap();
        let catalog = list_response.into_inner();

        assert_eq!(catalog.scenario_ids.len(), 1);
        assert_eq!(catalog.available_types.len(), 5);
    }
}
