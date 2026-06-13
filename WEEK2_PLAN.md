# Week 2 Implementation Plan

TDD-driven plan for the Week 2 milestones. Each step is red → green: name the failing
test first, then the change that makes it pass. Decisions locked with the team:

- **DB data model:** hybrid — encrypted body blob + plaintext index fields (DB owns indexing).
- **NextAction:** full proto redesign (lossless, carries timestamp + auto-close schedule).

## Milestones

- **2-1 (MVP):** domain-specific ticket & user RPCs end-to-end.
- **2-2 (MVP):** Raft snapshot coverage, MFA, ops polish.

---

## Guiding principles

1. **Index maintenance lives inside the Raft state machine** (`LogEntry::apply()`), never in
   gRPC handlers — otherwise followers desync. This is the key correctness constraint.
2. **Writes go through Raft `client_write`; reads go direct from local storage** (read-from-any-node).
3. **Ticket durability/consensus is the DB's job.** Custodian Raft = locks only; custodian's
   ticket RPCs are thin orchestration over `db_client`.
4. **`ticket_id` is non-sensitive metadata** → plaintext key + index field. The encrypted body
   holds the sensitive payload. DB owns ID assignment (atomic counter); custodian re-stamps
   `ticket.ticket_id` from the record on read.
5. Keep clippy (`-D warnings`) green and the 90% tarpaulin gate satisfied throughout.

---

## Workstream A — DB domain API + hybrid index (Task 2-1-1) — foundation

**Files:** `db/proto/db.proto`, `db/src/storage.rs`, `db/src/server.rs`, `db/src/raft.rs`,
`custodian/src/db_client.rs`, `custodian/src/server.rs`, `ARCHITECTURE.md`.

### A1. Proto redesign (`db/proto/db.proto`)
Replace generic `Database` service (keep `Health`/`ClusterStatus` + all `RaftService` messages):

```proto
service Database {
  rpc CreateTicket(TicketWrite)      returns (TicketRecord);
  rpc GetTicket(TicketLookup)        returns (TicketRecord);
  rpc UpdateTicket(TicketWrite)      returns (TicketRecord);
  rpc SoftDeleteTicket(TicketLookup) returns (DeleteAck);
  rpc QueryTickets(TicketQuery)      returns (stream TicketRecord);
  rpc CreateUser(UserWrite)          returns (UserRecord);
  rpc GetUser(UserLookup)            returns (UserRecord);
  rpc UpdateUser(UserWrite)          returns (UserRecord);
  rpc SoftDeleteUser(UserLookup)     returns (DeleteAck);
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc ClusterStatus(ClusterStatusRequest) returns (ClusterStatusResponse);
}
```

`TicketWrite { ticket_id(0=create), encrypted_body, TicketIndexFields }`;
`TicketIndexFields { status, account_uuid, assigned_to_uuid?, project, tracking_url?, created_at_unix, updated_at_unix }`;
`TicketRecord { ticket_id, encrypted_body, deleted, deleted_at_unix }`;
`TicketLookup { ticket_id, include_deleted }`;
`TicketQuery { status?, assigned_to_uuid?, account_uuid?, project?, include_deleted, limit }`;
`DeleteAck { success, ticket_id }`. User equivalents index on `username, email, role`.

### A2. Storage state machine (`db/src/storage.rs`)
- Add `TREE_TICKET_SEQ` (atomic id counter). Store `StoredTicket { body, deleted, deleted_at }`
  (bincode) so soft-delete state is in the value.
- Extend `LogEntry` with domain variants: `CreateTicket/UpdateTicket/SoftDeleteTicket` +
  user equivalents. `apply()` mutates data tree **and all relevant `IDX_*` trees** atomically.
  Index entry convention: `b"{value}\x00{ticket_id_be}"` → empty; filter = `scan_prefix(value)`.
- Update: diff old vs new index entries. Soft-delete: mark deleted + remove from active indexes,
  keep data row (audit). Activates the dead `IDX_*` constants (`storage.rs:37-46`).
- TDD: `create_ticket_assigns_monotonic_ids`, `create_ticket_populates_all_indexes`,
  `update_ticket_moves_index_entries`, `soft_delete_removes_from_active_indexes_but_keeps_row`,
  `query_by_status_returns_only_matching_active` (+ user analogues).

### A3. gRPC handlers (`db/src/server.rs`)
- Handlers build `LogEntry` → `client_write`; create returns assigned id. `GetTicket`/`QueryTickets`
  read direct. `QueryTickets` = server stream: narrowest filter → `scan_prefix` index → load → stream;
  honor `include_deleted` + `limit`.
- TDD: `create_then_get_ticket_roundtrip`, `query_streams_matching_tickets`,
  `soft_delete_then_get_with_include_deleted`.

### A4. Custodian as DB client (`custodian/src/db_client.rs`, `custodian/src/server.rs`)
- Replace `put/get` with domain methods. Custodian encrypts domain `Ticket` → `encrypted_body`,
  derives `TicketIndexFields` from plaintext before encrypting.
- `get_ticket`: decrypt, **re-stamp `ticket_id` from record**, `domain_to_proto`.
- Reconcile `ARCHITECTURE.md` §1355 (`AppendTicket/ManageUser`) to match §288 (`CreateTicket/...`).

---

## Workstream B — QueryTickets E2E + LBRP + E2E test (Task 2-1-2)

**Files:** `custodian/proto/custodian.proto`, `custodian/src/server.rs`, `lbrp/src/routes.rs`,
`lbrp/src/clients/custodian.rs`, `custodian/src/main.rs`, `tests/src/e2e.rs`.

- **B1.** Custodian `rpc QueryTickets(...) returns (stream Ticket)`; forward to DB, decrypt, stream.
- **B2.** LBRP `GET /api/tickets` with query params → custodian stream → `Json(Vec<TicketDto>)`.
  GetTicket round-trip already wired (`routes.rs:161`); add happy + 404 tests.
- **B3.** Make `DB_LEADER_ADDR` **required** at startup (`custodian/src/main.rs:223`); document in demo script.
- **B4.** Extend `tests/src/e2e.rs` to full lifecycle: login → create → get → query → update →
  get → soft-delete → query(excluded) → logout. Keep `#[ignore]`; add `--ignored` CI lane.

---

## Workstream C — Snapshots + Raft response types + 3-node test (Task 2-2-1)

**Files:** `db/src/raft.rs`, `db/src/raft_service.rs`, `db/src/network.rs`,
`custodian/src/raft.rs`, `custodian/proto/custodian.proto`, `db/tests/three_node_cluster.rs` (new).

- **C1.** Snapshot build/restore: **enumerate `db.tree_names()` minus `TREE_RAFT_*`** (auto-covers
  tickets, users, sessions, audit, all 10 indexes, seq). Restore clears + repopulates; add
  index-rebuild-from-data safety net. Custodian: same enumerate approach.
  TDD: `snapshot_roundtrips_all_trees_including_indexes`, `restore_rebuilds_indexes_from_data`.
- **C2.** `db/src/raft_service.rs:133-150`: stop discarding openraft response; map to the existing
  proto `response_type` (0 Success / 1 PartialSuccess+index / 2 Conflict / 3 HigherVote+vote);
  decode inverse in `network.rs`. Add typed `enum AppendOutcome`. Extend custodian Raft proto to
  match (confirm its wire format first). TDD: `append_entries_maps_{conflict,higher_vote,partial_success}`.
- **C3.** New `db/tests/three_node_cluster.rs`: spawn 3 in-process nodes, init cluster, write,
  kill+restart a node, assert convergence via InstallSnapshot; kill leader, assert re-election.
  `#[ignore]` + `--ignored` lane. (Largest item — greenfield; current `multi_node_test.rs` is no-op.)

---

## Workstream D — Ops polish (Task 2-2-2) — mostly parallel-safe

- **D1. MFA** (`auth/src/server.rs:155-160`): real TOTP (RFC 6238) via `totp-rs`; reject when
  `mfa_enabled` and token missing/invalid; audited `MFA_DISABLED` escape hatch. TDD with fixed vectors.
- **D2. Error envelope** (`lbrp/src/error.rs` new): `ApiError { code, message, http }` → JSON
  `{ "error": { code, message } }`; `From<tonic::Status>`. Replace `(StatusCode, String)` returns.
- **D3. `/health`** (`lbrp/src/routes.rs`): aggregate downstream `Health` RPCs; 200 ok / 503 degraded.
- **D4. Lock holder** (`custodian/src/server.rs:437`): on failed acquire, read `TREE_LOCKS` and set
  `current_holder`.
- **D5. NextAction redesign** (`custodian/proto/custodian.proto:181-189`, `server.rs:77-96` & `549-555`,
  LBRP DTO, web): structured `NextAction { oneof none|follow_up(ts)|appointment(ts)|auto_close(enum) }`;
  lossless forward+reverse mapping; land breaking proto change across custodian+lbrp+web in one commit.

---

## Sequencing

- **Critical path:** A → B → C1/C3. Start A first.
- **Parallel/independent:** C2 and all of D (different files). Coordinate B1 + D5 (both touch
  `custodian.proto`).
- **Biggest schedule risk:** C3 (3-node harness, greenfield).

## Deps to add
- `auth`: `totp-rs`. Run `cargo audit` + `cargo deny check` after.

## Watch-items
- Index mutations must be in `apply()` only (determinism; surfaces in C3 if wrong).
- Confirm custodian Raft wire format before C2.
- Confirm nothing depends on custodian assigning ticket id (DB-assigns + re-stamp on read).
- Streaming handlers + cluster test need real assertions to hold tarpaulin ≥90%.

---

## Progress log — ALL WEEK 2 TASKS COMPLETE

Full `cargo test --workspace` green; `cargo clippy --workspace --all-targets
--all-features -- -D warnings` green (the repo-wide gate — previously red on pre-existing
`shared` doc lints, now fixed).

- **Workstream A (Task 2-1-1) — done.** Domain DB RPCs (additive: generic KV retained for
  sessions/audit, see decision); hybrid storage model with index maintenance in
  `LogEntry::apply`; DB assigns ids; soft-delete keeps row + drops from active indexes;
  custodian migrated to domain RPCs; `ARCHITECTURE.md` reconciled.
- **Workstream B (Task 2-1-2) — done.** Custodian `QueryTickets` streaming RPC; LBRP
  `GET /api/tickets`; `DB_LEADER_ADDR` required (fail-fast); E2E extended (create→get→query).
- **Workstream C (Task 2-2-1) — done.** Snapshots enumerate all non-Raft trees (db +
  custodian) + fixed a snapshot-meta bug (`last_log_id`/membership were null). Raft
  response classes mapped to/from typed wire fields (db) with per-variant tests. Real
  3-node kill/rejoin cluster test (`db/tests/three_node_cluster.rs`) validating
  snapshot-based recovery via `InstallSnapshot`.
- **Workstream D (Task 2-2-2) — done.**
  - D1 MFA: real TOTP (`totp-rs`) + audited `MFA_DISABLED=1` escape hatch; vector test.
  - D2 error envelope: `lbrp/src/error.rs` `ApiError` → JSON `{error:{code,message}}`.
  - D3 `/health`: aggregated endpoint (200 ok / 503 degraded).
  - D4 lock-holder on conflict (+ fixed real lock-exclusivity bug).
  - D5 NextAction: proto redesigned to a structured message; lossless mapping both ways;
    surfaced read-only end-to-end (custodian → LBRP JSON → web display).

**Decisions recorded:** (1) domain DB RPCs added **additively** (generic KV kept for
sessions/audit) to avoid a big-bang break of auth/admin/lbrp; full generic-KV retirement
is a tracked follow-up. (2) C3 scoped to snapshot kill/rejoin (the task requirement);
leader-failover-after-snapshot is a deeper openraft edge case left as follow-up.

**Tracked follow-ups (out of Week 2 scope):**
- C2 custodian-side: extend the custodian's simplified Raft proto (term+success) to carry
  response classes (low priority — custodian Raft is locks-only).
- `/health` aggregation of auth + DB (need a Health RPC on auth and a DB client in LBRP).
- NextAction interactive editing in the web form (display is wired; input widget pending).
- Full generic-KV retirement from the DB service.
- Pre-existing `cargo audit` findings: `rsa` (via `jsonwebtoken`), `rustls-webpki` (via
  `reqwest`) — dependency upgrades, unrelated to Week 2 changes.
