# TODO: MVP Demo Readiness

MVP services: `db`, `custodian`, `auth`, `lbrp`, `web` (plus `shared` and `proto`).
Goal: a working single-node demo of the full ticket lifecycle:
**Login -> Create Ticket -> View Ticket -> Update Ticket -> Logout**

---

## Critical — Demo Does Not Work Without These

- [x] **Implement snapshot streaming in DB network layer**
  `db/src/network.rs:266` returns an unimplemented error for snapshot streaming.
  Raft followers cannot receive snapshots, so multi-node clusters break during
  log recovery. Single-node works, but the architecture requires 3-node minimum.

- [x] **Implement snapshot streaming in Custodian network layer**
  Same issue as DB — `custodian/src/raft.rs:671` has an incomplete snapshot
  implementation. Both Raft services need functioning snapshot transfer for
  any multi-node deployment.

- [x] **Ensure Custodian persists tickets to DB**
  `custodian/src/main.rs:223` makes `DB_LEADER_ADDR` optional (defaults to None).
  When unset, the custodian stores tickets in its local Raft log only — they
  aren't persisted to the DB service. The demo must set `DB_LEADER_ADDR` or the
  E2E flow (LBRP -> Custodian -> DB) is broken. Either make it required at startup
  or document the required env var in a demo launch script.

- [x] **Create a demo launch script**
  No turnkey way to start the MVP services with correct env vars. Need a script
  (e.g., `scripts/demo.sh`) that starts DB, Custodian, Auth, and LBRP with
  compatible addresses, ports, shared JWT secret, storage paths, and Raft peer
  configs. Should support single-node mode for simplicity.

- [x] **Build the web frontend and serve via LBRP**
  The web crate compiles to WASM but there's no build step integrated with the
  demo. LBRP serves static files from `../web/dist` (fallback route). Need:
  (1) `trunk build` or equivalent to produce `web/dist/`, (2) verify LBRP
  serves the built files correctly at `/`.

## High — Core Functionality Gaps

- [x] **Complete DB gRPC API to match ARCHITECTURE.md spec** *(Week 2, Task 2-1-1)*
  Added domain RPCs to the `Database` service: `CreateTicket`, `GetTicket`,
  `UpdateTicket`, `SoftDeleteTicket`, `QueryTickets` (streaming), `CreateUser`,
  `GetUser`, `UpdateUser`, `SoftDeleteUser`. Hybrid storage model: opaque
  encrypted body + plaintext index fields; index maintenance happens in the Raft
  state machine (`LogEntry::apply`), the DB assigns ticket ids, soft-deletes keep
  the row but drop it from active indexes. Custodian migrated to the domain RPCs
  (`custodian/src/db_client.rs`, `custodian/src/server.rs`).
  Generic `Put`/`Get`/`Delete`/`List`/`Exists`/`BatchPut` are **retained
  (additive)** for sessions/audit used by auth/admin — full removal is a tracked
  follow-up (avoids a big-bang break of three services). See `WEEK2_PLAN.md`.

- [x] **Complete Raft snapshot collection for all data** *(Week 2, Task 2-2-1)*
  Snapshots now enumerate **all** non-Raft state-machine trees via
  `Storage::data_tree_names()` (data collections, every secondary index, and the
  sequence counter) instead of a hardcoded list — coverage tracks new trees
  automatically. Applies to both `db` and `custodian`. Covered by
  `snapshot_roundtrips_all_trees_including_indexes`.
  Also fixed a snapshot-meta bug: `build_snapshot` emitted `last_log_id: None` and
  empty membership, corrupting openraft's snapshot tracking under real multi-node use
  (it now carries the applied log id + membership).
  Validated by a real 3-node cluster test (`db/tests/three_node_cluster.rs`): cluster
  formation, replication, quorum tolerance with one node down, explicit snapshot+purge,
  and **snapshot-based rejoin via `InstallSnapshot`**. Run with
  `cargo test -p db --test three_node_cluster -- --ignored`.

- [x] **Handle Raft response types properly in DB service** *(Week 2, Task 2-2-1)*
  `db/src/raft_service.rs` now maps the openraft `AppendEntriesResponse` into the
  typed wire fields (0=Success, 1=PartialSuccess+index, 2=Conflict, 3=HigherVote)
  via `encode_append_response`, and `db/src/network.rs` decodes them back into the
  openraft enum (previously both hardcoded Success). Unit-tested per variant.
  Note: the custodian's simplified Raft proto still carries only term+success —
  extending it to match is a tracked follow-up (locks-only cluster).

- [x] **Add LBRP route for ticket listing** *(Week 2, Task 2-1-2)*
  Added `GET /api/tickets` (auth-protected) with query params
  (`?status=&assignee=&account=&project=&include_deleted=&limit=`). Backed by a new
  custodian `QueryTickets` server-streaming RPC, which forwards to the DB's
  `QueryTickets` and decrypts each record. Full path: REST → Custodian → DB.

- [ ] **Add LBRP route for user management**
  ARCHITECTURE.md specifies user CRUD through the admin service. LBRP has
  `POST /api/admin/users` for creation but no GET/PUT/DELETE user endpoints.

## Medium — Important for a Polished Demo

- [x] **Implement MFA verification in Auth service** *(Week 2, Task 2-2-2)*
  Implemented real TOTP (RFC 6238) verification via `totp-rs` (`verify_totp`): when an
  account has an enrolled `mfa_secret`, a valid token is required (missing → "MFA token
  required", wrong → "Invalid MFA token"). Added an **audited escape hatch**
  `MFA_DISABLED=1` that skips enforcement with a loud warning log. Unit-tested with a
  known TOTP vector. (`totp-rs` adds no new advisories; the 4 pre-existing `cargo audit`
  findings come from `jsonwebtoken`→`rsa` and `reqwest`→`rustls-webpki`, unchanged here.)

- [x] **Return lock holder on acquisition failure** *(Week 2, Task 2-2-2)*
  Fixed a deeper bug first: the lock state machine wasn't actually exclusive
  (`acquire_lock` overwrote any existing holder). Locks are now exclusive — a
  conflicting `AcquireLock` is rejected, and the response reports `current_holder`
  (looked up from the lock store). Covered by
  `acquire_lock_conflict_reports_current_holder`.

- [x] **Add health check endpoint to LBRP** *(Week 2, Task 2-2-2)*
  Added `GET /health` (unauthenticated) returning `{ status, services }` with
  `200 ok` / `503 degraded`. Probes the custodian (via cluster status). Auth/DB lack
  a Health RPC reachable from LBRP today — aggregating them is a tracked follow-up.

- [x] **Add error responses with meaningful messages** *(Week 2, Task 2-2-2)*
  Added `lbrp/src/error.rs`: `ApiError { status, code, message }` rendering a JSON
  envelope `{ "error": { "code", "message" } }`, with `From<tonic::Status>` code
  mapping. All REST handlers now return `ApiError`.

- [x] **Wire up GetTicket through LBRP -> Custodian -> DB round-trip** *(Week 2, Task 2-1-2)*
  Read path confirmed: `GET /api/tickets/{id}` → Custodian `GetTicket` →
  DB `GetTicket`, decrypt, return. `DB_LEADER_ADDR` is now **required** at custodian
  startup (fail-fast) so the demo always uses the DB path. Covered by the extended
  E2E test (`tests/src/e2e.rs`: create → get → query).

## Low — Nice to Have for Demo

- [x] **Add request rate limiting to LBRP** *(Week 3, Task 3-1-2)*
  Per-IP rate limiting via `tower_governor` (keyed on peer IP), wired in `lbrp/src/main.rs`;
  tunable with `RATE_LIMIT_PER_SEC` (default 10) and `RATE_LIMIT_BURST` (default 30).

- [x] **Add CORS headers to LBRP** *(Week 3, Task 3-1-2)*
  `tower-http` CorsLayer applied in the router; origins from `CORS_ALLOWED_ORIGINS`
  (comma-separated) or any-origin if unset.

- [x] **Add response compression to LBRP** *(Week 3, Task 3-1-2)*
  `tower-http` CompressionLayer (gzip + brotli) applied in the router.

- [x] **Implement auto-lock expiry in Custodian** *(Week 3, Task 3-1-2)*
  `LockInfo.expires_at_unix` + `is_expired`; deterministic via command-stamped
  `at_unix`/`ttl_secs` replicated through the Raft proto. Expired locks are stealable;
  `update_ticket` rejects edits under an expired lock. TTL via `LOCK_TTL_SECS` (default 900).
  Tested (`lock_expiry_is_recorded_and_evaluated`, `expired_lock_can_be_stolen_but_live_lock_cannot`).

- [x] **Policy-as-code ticket state-transition validation** *(Week 3, Task 3-1-2)*
  `TicketStatus::can_transition_to` + allow-list matrix in `shared/src/ticket.rs`; enforced in
  custodian `update_ticket`. Terminal states (`Closed`/`AutoClose`) are sinks. Tested.

- [x] **Add `services.toml` config file support** *(Week 3, Task 3-1-1)*
  `shared::discovery::ServiceRegistry` + LBRP env-overridable resolution with a periodic reload
  task that hot-reconnects changed backends. See the Hardened Stage section below.

- [x] **Complete NextAction enum mapping in Custodian** *(Week 2, Task 2-2-2)*
  Redesigned the proto: replaced the mismatched `NextAction` enum vocabulary with a
  structured `NextAction` message (oneof `follow_up`/`appointment`/`auto_close`) plus an
  `AutoCloseSchedule` enum, mirroring the domain type. Mapping is now **lossless both
  directions** (`map_next_action` / `proto_to_next_action`), with per-variant round-trip
  tests. Surfaced read-only end-to-end: custodian message → LBRP JSON (`ApiNextAction`)
  → web ticket display. (Interactive editing in the web form is a follow-up.)

---

## Hardened Stage (Week 3)

- [x] **Admin service: `ListUsers`, `UpdateUser`, `DeleteUser`** *(Week 3, Task 3-1-1)*
  Implemented (list paginated; update incl. password re-hash preserving MFA; delete = soft
  delete). Fixed a real user-key inconsistency (`create` raw bytes vs `get` string bytes).
- [x] **Custodian multi-node Raft** *(Week 3, Task 3-1-2)* — fixed: production now serves the
  custodian `RaftService`; snapshot meta carries real `last_log_id`/membership; and the Raft
  **wire protocol was rewritten** (opaque-bytes entries + `ProtoVote`/`ProtoLogId`/`response_type`,
  ported from db) so consensus state is no longer lost. `custodian/tests/three_node_cluster.rs`
  now passes (cluster formation, lock replication, **leader failover**, **snapshot recovery**).
  Both db and custodian 3-node burn-in are validated.
- [x] **Chaos service: apply fault-injection scenarios** *(Week 3, Task 3-1-1)* — injected
  scenarios are now live, time-bounded faults: a background task keeps each active for its
  `duration_ms` then auto-expires it (`StopScenario` cancels it early). Added an admin-auth gate
  (`CHAOS_AUTH_TOKEN` + `x-chaos-token` metadata; open in dev). Tested (auto-expiry + authz).
- [x] **Honeypot service** *(Week 3, Task 3-1-1)* — full gRPC `HoneypotService` (advertised as
  `CriticalBackups`): `GetWalletBalance` (honeytoken), `ListBackups` (stream of fake archives),
  `DownloadBackup` (clamped junk-data tarpit). Every hit reports an `IntrusionEvent` to the new
  admin `RecordIntrusion` RPC (which persists it to the audit log). mTLS-aware. End-to-end test
  (`honeypot/tests/reporting.rs`) proves a trap hit reaches admin.
- [x] **mTLS between services** *(Week 3, Task 3-1-1)* — `proto::tls` builds mutual-TLS
  server/client configs (rustls / TLS 1.3) from a shared CA + service-unique certs. **Opt-in**
  via `TLS_CA_CERT`/`TLS_CERT`/`TLS_KEY` env (plaintext default for dev/test). Wired into all
  gRPC servers (db, custodian, auth, admin, chaos), all data-plane clients (LBRP→services,
  services→DB), and **both Raft peer meshes** (db + custodian). Cert-gen: `scripts/gen-certs.sh`.
  End-to-end test `db/tests/mtls.rs` (CA-signed client succeeds; no-identity client rejected).
- [x] **Post-quantum Kyber on the wire** *(Week 3, Task 3-1-1)* — `proto::pqc`: `seal`/`open`
  wrap payloads with Kyber-768 KEM + AEAD (built on `shared::EncryptionService`), plus a
  drop-in `KyberCodec` (`tonic::codec::Codec`) for transparent per-RPC body encryption. Rides
  *inside* the mTLS tunnel (double layer). Opt-in via `PQC_PUBLIC_KEY`/`PQC_PRIVATE_KEY`. Tested:
  round-trip, wrong-key-fails, opaque ciphertext, codec pipeline, and **end-to-end over real
  gRPC** (`db/tests/pqc_wire.rs` — sealed payload survives a Put/Get; the DB only sees ciphertext).
- [x] `services.toml` static discovery + periodic reload *(Week 3, Task 3-1-1)* —
  `shared::discovery::ServiceRegistry` parses `services.toml` (env-override → registry → default);
  LBRP resolves backends from it when `SERVICES_TOML` is set and a background task re-reads it every
  `SERVICES_RELOAD_SECS` (default 30s), hot-reconnecting changed clients (`lbrp/src/discovery.rs`).
  Sample at repo root `services.toml`.
- [x] NanoVMs OPS unikernel deployment (config + build script + docs) *(Week 3, Task 3-1-1)* —
  per-service `deploy/ops/*.json`, `scripts/build-images.sh` (release build + `ops image create`,
  with cert fan-out), and `deploy/README.md`. **Unvalidated** (this env cannot boot a unikernel).
- [x] Modern UI: search/filter, dark-mode toggle, analytics dashboards *(Week 3, Task 3-1-2)* —
  Leptos console with ticket search/filter (`GET /api/tickets`), persisted dark/light theme,
  analytics (counts by status/priority), and policy-as-code transition validation in the edit form
  (`web/src/domain.rs` mirrors `shared::TicketStatus`). New themed `style.css`; WASM bundle rebuilt.
- [x] Supply-chain: clear the 4 pre-existing `cargo audit` advisories (upgrade-only) *(Week 3, Task 3-1-2)* —
  bumped `rustls-webpki` 0.103.10→0.103.13 (clears 3 webpki advisories) and `rand` 0.8/0.9/0.10
  to patched releases (clears the `rand` unsound advisory); switched `jsonwebtoken` to the
  `aws_lc_rs` crypto provider so the Marvin-affected `rsa` crate (RUSTSEC-2023-0071) is no longer
  compiled in. **No `ignore` suppressions.** `cargo audit` and `cargo deny check advisories` both
  exit 0. (Migrated `deny.toml` to the cargo-deny v2 schema so the gate runs.)
- [x] Coverage to 90% (`cargo tarpaulin --fail-under 90`) *(Week 3, Task 3-1-2)* — **90.48%**, gate
  green via `scripts/coverage.sh` (business packages only; `tarpaulin.toml` mirrors it). Root-caused
  the broken config (settings weren't applied, so `web`/`main.rs`/`tests`/spikes/un-run ignored
  tests dragged it to a false 66%). Then drove the openraft consensus layer up with **fault-injection
  tests** — corrupted-log deserialize/invalid-key paths, every `AppendEntries` decode variant
  (PartialSuccess/Conflict/HigherVote/unknown), missing-vote and transport errors, `full_snapshot`
  success/cancel/missing-vote, and the lock state-machine lifecycle — plus refactoring the repetitive
  `StorageIOError` boilerplate into helper fns/refs (DRY + coverable). The timing-sensitive 3-node
  burn-in tests are run **separately** (they flake under instrumentation), not under the gate.
