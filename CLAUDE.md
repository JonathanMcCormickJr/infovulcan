# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

InfoVulcan is a distributed, microservices-based tech support ticketing system built entirely in Rust. It prioritizes memory safety, strong consistency, and post-quantum security.

## Commands

### Development

```bash
cargo fmt --all                          # Format code
cargo clippy --all-targets --all-features -- -D warnings  # Lint (warnings are errors)
cargo build --all-targets --all-features # Build everything
cargo test --all-targets --all-features  # Run all tests
cargo test -p <crate>                    # Run tests for a single crate
cargo test <test_name>                   # Run a single test by name
cargo watch -x test                      # Continuous test execution
```

### Stage-Specific Builds (via `.cargo/config.toml` aliases)

```bash
cargo build-mvp      # Build MVP services only (excludes admin, chaos, honeypot)
cargo check-mvp      # Check MVP services
cargo test-mvp       # Test MVP services
cargo build-hardened # Build all services with production profile
cargo check-hardened # Check all services
cargo test-hardened  # Test all services
```

### Quality & Security

```bash
cargo tarpaulin --workspace --fail-under 90  # Code coverage (90% minimum required)
cargo audit                                   # Dependency vulnerability scan
cargo deny check                              # License & supply chain check
cargo geiger                                  # Unsafe code detection
```

## Architecture

### Service Graph

```
Internet
    │
    ▼
LBRP (REST/HTTPS, port 443)     ← Public entry point; Axum-based
    ├──→ Auth (gRPC, :8082)     ← Stateless authentication & JWT
    ├──→ Custodian (gRPC, :8081) ← Raft cluster; distributed ticket locks
    └──→ Admin (gRPC, :8083)    ← Stateless user/role management (Hardened+)
              ↓                            ↓                        ↓
         DB Service (gRPC, :50051) ← Raft cluster; all data persistence (Sled)

Hardened-only additions:
  Chaos (:8084) — fault injection for resilience testing
  Honeypot      — deceptive intrusion detection
```

### Deployment Stages

- **MVP:** `auth`, `custodian`, `db`, `lbrp`, `web`
- **Hardened:** All MVP services + `admin`, `chaos`, `honeypot`

### Crate Roles

| Crate | Type | Purpose |
|-------|------|---------|
| `shared` | Library | Common types: `Ticket`, `User`, `InfoVulcanError`, `EncryptionService` |
| `db` | Raft service | Data persistence; Sled KV + Raft consensus (3+ instances) |
| `custodian` | Raft service | Ticket lifecycle; distributed ticket locks (3+ instances) |
| `auth` | Stateless | Authentication, JWT issuance/validation |
| `lbrp` | Stateless | REST↔gRPC translation, request routing, static file serving |
| `admin` | Stateless | User/role management, system monitoring (Hardened+) |
| `chaos` | Stateless | Fault injection for resilience testing (Hardened+) |
| `honeypot` | Stateless | Deceptive intrusion detection (Hardened+) |
| `tests` | Test suite | Integration and E2E tests spanning multiple services |
| `web` | WASM frontend | Browser UI; compiled to WASM, served as static files by LBRP |

### Inter-Service Communication

- **External:** REST/JSON over HTTPS (LBRP only, via Axum)
- **Internal:** gRPC (tonic + prost) with mTLS over rustls TLS 1.3
- Each service has a `.proto` file defining its gRPC API; `build.rs` compiles protos at build time

### Key Technology Choices

- **Async runtime:** `tokio`
- **Consensus:** `openraft` (Raft; used by `db` and `custodian`, each requiring 3+ nodes)
- **Storage:** `sled` embedded KV store (ACID)
- **Crypto:** `rustls` (TLS 1.3) + `pqc_kyber` (post-quantum CRYSTALS-Kyber KEM)
- **Serialization:** `serde` + `bincode`/`postcard`

## Code Standards

- `#![forbid(unsafe_code)]` everywhere in business logic
- `#![warn(clippy::all, clippy::pedantic)]` — linting is pedantic and warnings fail CI
- **No panics in production code.** `unwrap`/`expect`/`panic!`/`assert!` are only acceptable during startup initialization (must be documented) or inside `#[test]` functions
- Return `Result<T, E>` with `thiserror` error types; use `anyhow::Context` to add context
- **Soft deletes only** — no hard deletes via API (audit trail requirement)
- **Enums** use `#[repr(u8)]` and `#[non_exhaustive]` for forward compatibility
- List ordering: logical groups first, then alphabetical within groups
- TDD workflow: write tests before implementation; 90% coverage is a hard CI gate

## Environment Variables

Each service is configured via environment variables:

| Service | Key Vars |
|---------|----------|
| `db` | `NODE_ID`, `LISTEN_ADDR` (default `[::1]:50051`), `RAFT_PEERS`, `STORAGE_PATH` |
| `custodian` | `NODE_ID`, `LISTEN_ADDR` (default `[::1]:8081`), `RAFT_PEERS`, `STORAGE_PATH`, `DB_LEADER_ADDR` |
| `auth` | `LISTEN_ADDR` (default `[::1]:8082`), `DB_ADDR`, `STORAGE_PATH`, `JWT_SECRET` |
| `lbrp` | `LISTEN_ADDR`, `AUTH_ADDR`, `ADMIN_ADDR`, `CUSTODIAN_ADDR`, `JWT_SECRET` |
| `admin` | `LISTEN_ADDR` (default `0.0.0.0:8083`), `DB_ADDR`, `STORAGE_PATH` |

`JWT_SECRET` must match between `auth` and `lbrp`.

## Deployment Notes

- Raft services (`db`, `custodian`) require **minimum 3 instances** for quorum (tolerates 1 failure)
- No Docker — deployment uses NanoVMs OPS (unikernel) to minimize attack surface
- All inter-service communication uses mTLS with service-unique certificates
