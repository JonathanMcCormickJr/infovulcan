use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::sleep;

use proto::admin;

struct ServiceProcess {
    name: String,
    child: Child,
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        println!("Stopped {}", self.name);
    }
}

async fn wait_for_port(port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow::anyhow!("Timed out waiting for port {}", port))
}

fn build_service_binary(bin: &str) -> Result<()> {
    let status = Command::new("cargo")
        .args([
            "build",
            "--package",
            bin,
            "--bin",
            bin,
            "--manifest-path",
            "../Cargo.toml",
        ])
        .status()
        .context(format!("failed to build binary {bin}"))?;

    if !status.success() {
        return Err(anyhow::anyhow!("cargo build failed for binary {bin}"));
    }

    Ok(())
}

fn start_service(name: &str, bin: &str, env: Vec<(&str, &str)>) -> Result<ServiceProcess> {
    let exe_path = format!("../target/debug/{}", bin);
    let child = Command::new(&exe_path)
        .envs(env)
        .spawn()
        .context(format!("Failed to start {}", name))?;

    println!("Started {} (pid {})", name, child.id());
    Ok(ServiceProcess {
        name: name.to_string(),
        child,
    })
}

/// Full MVP lifecycle over the real REST -> gRPC -> Raft path:
/// create admin user -> login -> create ticket -> get ticket -> query/list tickets.
#[tokio::test]
#[ignore = "E2E test spawns 5 services with port polling; run explicitly with --ignored"]
async fn test_e2e_flow() -> Result<()> {
    // Build service binaries first so e2e always runs against fresh proto-compatible executables.
    for bin in ["db", "custodian", "auth", "admin", "lbrp"] {
        build_service_binary(bin)?;
    }

    // 1. Start DB
    // Using a temp dir for storage
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("db");
    let _db = start_service(
        "DB",
        "db",
        vec![
            ("NODE_ID", "1"),
            ("LISTEN_ADDR", "127.0.0.1:50051"),
            ("STORAGE_PATH", db_path.to_str().unwrap()),
            ("RAFT_PEERS", "1:127.0.0.1:50051"),
            ("RUST_LOG", "info"),
        ],
    )?;
    wait_for_port(50051).await.context("DB port")?;

    // 2. Start Custodian
    let cust_path = temp_dir.path().join("custodian");
    let _custodian = start_service(
        "Custodian",
        "custodian",
        vec![
            ("NODE_ID", "1"),
            ("LISTEN_ADDR", "127.0.0.1:8081"),
            ("STORAGE_PATH", cust_path.to_str().unwrap()),
            ("RAFT_PEERS", "1:127.0.0.1:8081"),
            ("DB_ADDR", "http://127.0.0.1:50051"),
            ("DB_LEADER_ADDR", "http://127.0.0.1:50051"),
            ("RUST_LOG", "info"),
        ],
    )?;
    wait_for_port(8081).await.context("Custodian port")?;

    // Shared keys path for Auth and Admin so they can encrypt/decrypt user data
    let keys_path = temp_dir.path().join("keys");
    std::fs::create_dir_all(&keys_path)?;

    // 3. Start Auth
    let _auth = start_service(
        "Auth",
        "auth",
        vec![
            ("LISTEN_ADDR", "127.0.0.1:8082"),
            ("DB_ADDR", "http://127.0.0.1:50051"),
            ("STORAGE_PATH", keys_path.to_str().unwrap()),
            ("RUST_LOG", "info"),
            ("JWT_SECRET", "supersecretkey123"),
        ],
    )?;
    wait_for_port(8082).await.context("Auth port")?;

    // 4. Start Admin
    let _admin = start_service(
        "Admin",
        "admin",
        vec![
            ("LISTEN_ADDR", "127.0.0.1:8083"),
            ("DB_ADDR", "http://127.0.0.1:50051"),
            ("STORAGE_PATH", keys_path.to_str().unwrap()),
            ("RUST_LOG", "info"),
        ],
    )?;
    wait_for_port(8083).await.context("Admin port")?;

    // 5. Start LBRP
    let _lbrp = start_service(
        "LBRP",
        "lbrp",
        vec![
            ("LISTEN_ADDR", "127.0.0.1:8080"),
            ("AUTH_ADDR", "http://127.0.0.1:8082"),
            ("ADMIN_ADDR", "http://127.0.0.1:8083"),
            ("CUSTODIAN_ADDR", "http://127.0.0.1:8081"),
            ("RUST_LOG", "info"),
            ("JWT_SECRET", "supersecretkey123"),
        ],
    )?;
    wait_for_port(8080).await.context("LBRP port")?;

    // --- Test Scenario ---

    // A. Create Admin User directly via Admin Service (gRPC)
    // We need to connect to Admin Service
    let mut admin_client =
        admin::admin_service_client::AdminServiceClient::connect("http://127.0.0.1:8083").await?;

    let create_user_req = admin::CreateUserRequest {
        username: "admin".to_string(),
        password: "password123".to_string(),
        email: "admin@infovulcan.local".to_string(),
        display_name: "Admin User".to_string(),
        role: 0, // Admin
    };

    let _user = admin_client
        .create_user(tonic::Request::new(create_user_req))
        .await?
        .into_inner();
    println!("Created admin user");

    // B. Login via LBRP (HTTP)
    let client = Client::new();
    let login_resp = client
        .post("http://127.0.0.1:8080/auth/login")
        .json(&json!({
            "username": "admin",
            "password": "password123"
        }))
        .send()
        .await?;

    assert_eq!(login_resp.status(), 200);
    let login_body: serde_json::Value = login_resp.json().await?;
    let token = login_body["token"].as_str().context("missing token")?;
    println!("Got token: {}", token);

    // C. Create Ticket via LBRP (HTTP)
    let ticket_resp = client
        .post("http://127.0.0.1:8080/api/tickets")
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({
            "title": "System Down",
            "project": "Internal",
            "account_uuid": uuid::Uuid::new_v4().to_string(),
            "symptom": 1, // No Internet
            "priority": 2 // Medium priority
        }))
        .send()
        .await?;

    let ticket_status = ticket_resp.status();
    let ticket_body = ticket_resp.text().await?;
    assert_eq!(
        ticket_status, 201,
        "unexpected create ticket response: status={}, body={}",
        ticket_status, ticket_body
    );
    let created: serde_json::Value = serde_json::from_str(&ticket_body)?;
    let ticket_id = created["ticket_id"]
        .as_u64()
        .context("create response missing ticket_id")?;
    println!("Created ticket {ticket_id}");

    // D. Get the ticket by id via LBRP -> Custodian -> DB (read round-trip).
    let get_resp = client
        .get(format!("http://127.0.0.1:8080/api/tickets/{ticket_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(get_resp.status(), 200, "get ticket should succeed");
    let got: serde_json::Value = get_resp.json().await?;
    assert_eq!(
        got["ticket_id"].as_u64(),
        Some(ticket_id),
        "fetched ticket id mismatch"
    );
    assert_eq!(got["title"].as_str(), Some("System Down"));
    println!("Fetched ticket {ticket_id}");

    // E. List/query tickets via LBRP -> Custodian (QueryTickets stream) -> DB.
    let list_resp = client
        .get("http://127.0.0.1:8080/api/tickets")
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(list_resp.status(), 200, "list tickets should succeed");
    let listed: Vec<serde_json::Value> = list_resp.json().await?;
    assert!(
        listed
            .iter()
            .any(|t| t["ticket_id"].as_u64() == Some(ticket_id)),
        "expected QueryTickets to include ticket {ticket_id}, got {listed:?}"
    );
    println!("Listed {} ticket(s) via QueryTickets", listed.len());

    Ok(())
}
