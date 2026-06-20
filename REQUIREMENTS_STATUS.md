# InfoVulcan — Requirements Status & Remaining TODOs

_Analysis date: 2026-06-13_

This report cross-references the project's stated **Requirements / Milestones / Tasks**
against the actual state of the codebase. The bulk of the project is already complete
(see `TODO.md` for the per-item history). This document focuses on **what remained to be
done** at analysis time and tracks the work performed in this pass.

## Legend
- ✅ Done / verified
- 🟡 Partial — a documented follow-up remains
- 🔴 Open — was outstanding at analysis time
- 🛠️ Addressed in this pass

---

## Critical finding (fixed this pass)

🛠️ **The workspace did not build.** `auth/Cargo.toml` contained unresolved Git merge
conflict markers (`<<<<<<< HEAD … >>>>>>>`) from the "tarpaulin >90%" merge. `cargo build`
failed for the entire workspace with `key with no value, expected =`.

**Resolution:** kept the post-merge branch's intent (documented in `TODO.md`): the
`jsonwebtoken` `aws_lc_rs` provider (drops the Marvin-affected `rsa` crate, clearing
RUSTSEC-2023-0071) plus `totp-rs` for RFC-6238 MFA, with `rand = 0.10.1`. Verified with
`cargo build --locked` (lockfile still in sync) and `cargo fmt --check` (also cleared
pre-existing format drift in `shared/src/key_store.rs` introduced by the same merge).

---

## Requirement 1 — MVP Completion (Demo-Ready)

| Item | Status |
|------|--------|
| Domain-specific DB gRPC API (`CreateTicket`/`GetTicket`/`UpdateTicket`/`SoftDeleteTicket`/`QueryTickets` + user RPCs) | ✅ |
| Complete ticket read/list paths (LBRP `QueryTickets` route; LBRP→Custodian→DB `GetTicket` round-trip) | ✅ |
| Full Raft snapshot coverage (all trees) + typed response classes (conflict/higher-vote/partial) | ✅ (custodian proto response-type parity is a documented 🟡 follow-up) |
| MFA enforcement or explicit disablement in Auth | ✅ (TOTP + audited `MFA_DISABLED=1`) |
| JSON error envelope on LBRP | ✅ |
| `/health` aggregation | 🟡 probes Custodian only; Auth/DB lack an LBRP-reachable Health RPC |
| Lock-holder reported on acquisition failure | ✅ |
| Complete `NextAction` enum mapping | ✅ (read-only end-to-end; interactive web editing is 🟡) |

🛠️ **LBRP user-management routes** (was 🔴 — the only unchecked box in `TODO.md`).
ARCHITECTURE.md specifies user CRUD through the Admin service; LBRP previously had only
`POST /api/admin/users` (bootstrap). Added the remaining auth-protected routes:

- `GET    /api/admin/users`        → Admin `ListUsers` (paginated)
- `GET    /api/admin/users/{id}`   → Admin `GetUser`
- `PUT    /api/admin/users/{id}`   → Admin `UpdateUser`
- `DELETE /api/admin/users/{id}`   → Admin `DeleteUser` (soft delete → 204)

Implemented: `AdminClient` wrapper methods (`lbrp/src/clients.rs`), handlers + `ApiUser`
JSON projection (`lbrp/src/routes.rs`), wired under the JWT auth middleware (creation
stays unauthenticated for first-admin bootstrap). Covered by new unit tests
(success paths via mock Admin server + transport-error paths).

---

## Requirement 2 — Hardened Stage

| Item | Status |
|------|--------|
| mTLS everywhere (service-unique certs, rustls TLS 1.3) | ✅ (opt-in via env; plaintext dev default) |
| Post-quantum wire crypto (Kyber KEM) | ✅ (`proto::pqc`, opt-in) |
| Admin service `ListUsers`/`UpdateUser`/`DeleteUser` | ✅ |
| Chaos + Honeypot services | ✅ |
| NanoVMs OPS unikernel deploy + `services.toml` discovery/reload | 🟡 config/scripts/docs present; **unvalidated** (cannot boot a unikernel in this env) |

---

## Requirement 3 — Cross-Cutting Quality Gates

| Item | Status |
|------|--------|
| 90% coverage gate (`cargo tarpaulin --fail-under 90`) | ✅ (90.48% via `scripts/coverage.sh`) |
| `cargo audit` clean | ✅ (per `TODO.md`; re-run recommended in CI) |
| `cargo deny check` clean | ✅ |
| `cargo geiger` clean | 🟡 CI gate exists (`unsafe-check`); not runnable in this sandbox |
| LBRP rate limiting / CORS / gzip+brotli compression | ✅ |
| Custodian auto-lock expiry | ✅ |
| Frontend parity (dark mode, search/filter, policy-as-code, analytics) | ✅ |
| Multi-node Raft burn-in (db + custodian, failover + snapshot recovery) | ✅ (run separately with `--ignored`; flake under instrumentation) |

---

## Remaining documented follow-ups — disposition (second pass)

1. 🛠️ **`/health` aggregation** — DONE. `/health` now probes **auth** (DB-free
   `validate_session` liveness call) **and custodian** (cluster status) concurrently and
   reports `{auth, custodian}` with `200 ok` / `503 degraded`. DB is covered transitively
   (LBRP holds no direct DB connection by design); `admin` is Hardened-only and omitted from
   the MVP baseline. (`lbrp/src/clients.rs`, `lbrp/src/routes.rs` + tests.)
2. 🛠️ **Interactive `NextAction` editing in the web form** — DONE end-to-end. LBRP's REST
   `PUT /api/tickets/{id}` now accepts an optional `next_action` (`none` clears, `follow_up`
   /`appointment` take a unix-seconds `at`, `auto_close` takes a schedule) and maps it to the
   custodian proto (`to_proto_next_action`). The web update form gained a Next-action selector
   with conditional inputs, reflects the loaded ticket's action, and omits the field when
   "Leave unchanged". WASM bundle rebuilt. (`lbrp/src/routes.rs`, `web/src/api.rs`,
   `web/src/components/ticket_list.rs` + tests.)
3. ⚪ **Retire generic `Put`/`Get`/`Delete`/`List` DB RPCs** — **deliberately retained**
   (won't-do). These back the `sessions` and `audit` collections used by auth/admin, which have
   no domain schema. Removing them would require inventing session/audit-specific domain RPCs —
   a larger redesign with **no functional benefit** and real regression risk. The `db.proto`
   comments already document the retention; this is the correct engineering call, not an
   oversight. Revisit only if/when sessions+audit get first-class schemas.
4. ✅ **Custodian Raft proto response-type parity** — VERIFIED already done.
   `custodian/src/raft_service.rs::encode_append_response` emits all four variants
   (Success/PartialSuccess/Conflict/HigherVote) and `custodian/src/network.rs` decodes them
   (incl. an explicit unknown-type error). Identical to `db`. Unit-tested per variant.
5. 🛠️ **NanoVMs OPS deployment** — **image build now works and was validated** (was completely
   broken). `./scripts/build-images.sh` produced 0 working images before; running it surfaced and
   I fixed **four** real defects in `deploy/ops/`:
   - `_comment` keys → `ops` rejects unknown JSON fields (`json: unknown field "_comment"`).
   - `MapDirs` direction was reversed — it is `"<host-source>": "<image-dest>"`, not the inverse.
   - `MapDirs` host paths must have **≥2 components** (an `ops` quirk) — single-component
     `./lbrp-conf` failed, so the lbrp payload was nested under `./lbrp-conf/infovulcan`.
   - `Dirs: ["data"]` needs a host `deploy/ops/data/` dir (now tracked via `data/.gitkeep`).

   All **7 images now build** (`CERTS_SRC=certs ./scripts/build-images.sh`, exit 0), and a `db`
   image **booted and ran** under `ops` (Sled init → openraft startup → gRPC server up → a
   single-node config reached `become leader`). The build script now also fails loudly instead of
   printing `done` after an error. **Still manual:** full 3-node + mTLS + cross-service bring-up
   needs a KVM host (see Section E).
6. ✅ **`cargo geiger`** — VERIFIED green here. The exact CI gate
   (`cargo geiger | grep -E '^(admin|auth|chaos|custodian|db|honeypot|lbrp|tests)' | grep -v 0/0`)
   reports no unsafe in workspace crates (all use `#![forbid(unsafe_code)]`).

## Optional enhancements (not started — out of scope for required milestones)
- Real-time WebSocket notifications
- Dynamic schema evolution (runtime fields/enums + lazy migration)
- Expanded MFA (WebAuthn/U2F/AD)
- Browser-side draft auto-save (IndexedDB)

---

## Work performed

### Pass 1
1. 🛠️ Resolved the `auth/Cargo.toml` merge conflict; restored a building, format-clean,
   lock-synced workspace.
2. 🛠️ Implemented the outstanding LBRP user-management routes (list/get/update/delete)
   with client wrappers, JSON projection, auth gating, and tests.

### Pass 2 (this pass)
3. 🛠️ **Health aggregation** — `AuthClient::health` liveness probe + `/health` now probes
   auth and custodian concurrently. New/updated tests cover ok / partial-down / all-down.
4. 🛠️ **NextAction interactive editing** — LBRP REST write path (`next_action` →
   `to_proto_next_action`) + web update-form controls; WASM bundle rebuilt; mapping and
   serialization unit tests added.
5. ✅ Verified custodian Raft response-type parity (#4) and `cargo geiger` workspace gate (#6).
6. ⚪ Decided to retain the generic KV RPCs (#3) with documented rationale.

Verification: `cargo build --workspace --all-targets` ✅, `cargo clippy --all-targets
--all-features -- -D warnings` ✅, `cargo fmt --all --check` ✅, `cargo geiger` gate ✅,
`trunk build --release` ✅, full `cargo test --workspace` ✅ (all suites pass; only the
timing-sensitive 3-node burn-in tests remain `--ignored` and are run separately).

---

## Manual tests you must run

Automated tests cover the unit/integration layers, but the following require a human because
they need a running multi-process system, a browser, or a unikernel host. Run them in order.

### A. MVP end-to-end demo (single-node) — verifies tickets, users, health, NextAction
**Why manual:** spins up 4 long-running network services + a browser UI.

1. Build the frontend and start the stack:
   ```bash
   cd /home/jonathan/Projects/infovulcan
   ./scripts/demo.sh   # builds web via trunk; starts db, custodian, auth, admin, lbrp
   ```
   Watch for each service logging that it is listening (db :50051, custodian :8081,
   auth :8082, admin :8083, lbrp :8080). Leave it running. The script prints example curl
   commands at the end.
2. **Bootstrap an admin user** (no user is pre-seeded — `POST /api/admin/users` is
   intentionally unauthenticated for first-admin bootstrap):
   ```bash
   curl -s -X POST http://127.0.0.1:8080/api/admin/users \
     -H 'content-type: application/json' \
     -d '{"username":"admin","password":"password123","email":"admin@infovulcan.local","display_name":"Admin User","role":0}'
   ```
3. **Health aggregation** — in another terminal:
   ```bash
   curl -s http://127.0.0.1:8080/health | jq
   ```
   Expect `200` and `{"status":"ok","services":{"auth":"up","custodian":"up"}}`.
   Then kill the auth process (`pkill -f 'target/.*/auth'`) and re-run the curl: expect `503`
   with `"auth":"down"`, `"status":"degraded"`. Restart the stack afterward.
4. **Browser UI** — open `http://127.0.0.1:8080/`.
   - Sign in as `admin` / `password123` (the user you created in step 2).
   - **Create User**: fill the Create User panel → submit → expect "User created successfully."
   - **Create Ticket**: fill Create Ticket → submit → note the new ticket number.
   - **Search**: open Search & Filter, click "Search Tickets" → your ticket appears; the
     Analytics panel shows by-status / by-priority counts.
   - **NextAction editing** (new): in "Update Ticket", click **Edit** on your ticket (or load
     it by number). In the **Next action** dropdown:
       - pick **Follow up**, enter a unix timestamp (e.g. `1800000000`) in "When", Save →
         expect "updated"; the row's "Next action" column shows `Follow up @ 1800000000`.
       - reload it, pick **Auto-close** → choose "48 hours" → Save → column shows
         `Auto-close (hours_48)`.
       - reload it, pick **Clear (none)** → Save → column shows `—`.
       - pick **Leave unchanged** → Save → the previously-set action is preserved.
   - **Policy-as-code**: try to set an illegal status transition (e.g. a closed ticket back to
     Open) → expect an inline "Illegal transition" error and no API call.
   - **Dark mode**: toggle the ☀/🌙 button → theme persists across reload.
5. **User management via REST** (the new LBRP routes). Get a token, then exercise the routes:
   ```bash
   TOKEN=$(curl -s -X POST http://127.0.0.1:8080/auth/login \
     -H 'content-type: application/json' \
     -d '{"username":"admin","password":"password123"}' | jq -r .token)
   ```
   ```bash
   curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/admin/users?page=0&page_size=50" | jq
   curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/admin/users/<id>" | jq
   curl -s -X PUT  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
        -d '{"display_name":"Renamed","active":false}' "http://localhost:8080/api/admin/users/<id>" | jq
   curl -s -X DELETE -H "Authorization: Bearer $TOKEN" -i "http://localhost:8080/api/admin/users/<id>"
   ```
   Expect: list returns users + `total_count`; GET returns one user; PUT returns the updated
   user; DELETE returns `204 No Content` (soft delete — the user is marked inactive, not purged).

### B. MFA path (TOTP) — optional but recommended
**Why manual:** needs an enrolled secret + an authenticator app (or `oathtool`).
1. Create a user whose `UserAuth.mfa_secret` is a base32 secret (e.g. `JBSWY3DPEHPK3PXP`).
2. Log in **without** a token → expect "MFA token required".
3. Generate a code: `oathtool --totp -b JBSWY3DPEHPK3PXP` and log in with it → success.
4. Confirm the audited escape hatch: restart auth with `MFA_DISABLED=1`, log in without a
   code → succeeds, and the auth log emits the loud `MFA_DISABLED=1 ... (audited)` warning.

### C. Multi-node Raft burn-in (3-node clusters) — failover + snapshot recovery
**Why manual/separate:** timing-sensitive; excluded from the coverage gate (flakes under
instrumentation). Run them explicitly:
```bash
cargo test -p db        --test three_node_cluster -- --ignored --nocapture
cargo test -p custodian --test three_node_cluster -- --ignored --nocapture
```
Expect: cluster formation, replication, quorum tolerance with one node down, leader failover,
and snapshot-based rejoin all pass.

### D. mTLS + post-quantum (Kyber) on the wire — optional
**Why manual:** needs generated certs / env wiring.
1. `./scripts/gen-certs.sh` to produce the CA + service certs.
2. Start services with `TLS_CA_CERT` / `TLS_CERT` / `TLS_KEY` set (and optionally
   `PQC_PUBLIC_KEY` / `PQC_PRIVATE_KEY`) and confirm RPCs still succeed; a client with no
   CA-signed identity is rejected. (The automated `db/tests/mtls.rs` and `db/tests/pqc_wire.rs`
   already prove the mechanism; this confirms the full env wiring.)

### E. NanoVMs OPS unikernel deployment — image build done; full cluster needs a KVM host
**Status:** `ops image create` for all 7 services is fixed and validated; a single `db` unikernel
was booted and reached `become leader`. What remains manual is a **real multi-node cluster on a
KVM-capable host** (this box has no `/dev/kvm`, so `ops` only offers slow TCG emulation).

1. Install OPS: `curl https://ops.city/get.sh -sSfL | sh`.
2. Mint certs + build all images (validated to succeed):
   ```bash
   ./scripts/gen-certs.sh certs
   CERTS_SRC=certs ./scripts/build-images.sh        # exit 0; 7 images created
   ```
3. **Quick single-node smoke test** (works even without KVM, just slowly) — confirms a unikernel
   boots and runs the binary. Use a plaintext, self-only config:
   ```bash
   # a throwaway config: NODE_ID=1, RAFT_PEERS="1:127.0.0.1:50051", no TLS_* env, Dirs:["data"]
   ops image create target/release/db -c <that-config>.json -i iv-smoke-db
   ops instance create iv-smoke-db -i smoke
   ops instance logs smoke    # expect: "DB service node 1 ready", "Starting gRPC server", "become leader"
   ops instance delete smoke
   ```
4. **Full deployment** on a KVM host: follow `deploy/README.md` boot order — db ×3 (wait for a
   leader) → custodian ×3 → auth/admin → lbrp → chaos/honeypot — supplying per-instance
   `NODE_ID`/`RAFT_PEERS` with `-e`. Then run the **Section A** health + ticket checks against the
   lbrp instance. Confirm the mTLS peer mesh forms (each node must find its certs at
   `/etc/infovulcan/certs/` and the SANs must match the `RAFT_PEERS` hostnames).

---

## Refinements log (post-requirements code review)

A review pass identified nine refinements; all are now applied (none outstanding).

**Correctness / security**
1. ✅ **LBRP `JWT_SECRET` fail-fast** — LBRP previously defaulted to `b"secret"` when the env var
   was unset, while `auth` auto-generates a random persisted secret → silent 401s + weak default.
   LBRP now hard-errors at startup if `JWT_SECRET` is missing/empty. (`lbrp/src/main.rs`)
2. ✅ **`/certs/` git-ignored** — `gen-certs.sh` writes private keys to the repo-root `certs/`,
   which the root `.gitignore` didn't cover.

**Hygiene**
3. ✅ **Coverage artifacts git-ignored** (`cobertura.xml`, `lcov.info`, `tarpaulin-output/`).
   _Note: these were already tracked; run `git rm --cached cobertura.xml lcov.info -r
   tarpaulin-output` once to stop tracking them._

**Structural**
4. ✅ **Shared `raft-rpc` crate** — the openraft⇄wire conversion logic (formerly duplicated in
   both services' `raft_service.rs` server and `network.rs` client) now lives in one unit-tested,
   wire-agnostic crate used by `db` and `custodian`. **No proto/wire change** (zero consensus-compat
   risk). Validated by the full suites **and both 3-node burn-ins** (formation, replication,
   leader failover, snapshot rejoin). Coverage rose to 92.49%.
   _Residual: fully collapsing each service's `raft_service.rs`/`network.rs` + unifying the Raft
   proto messages into a shared package is a deeper, wire-layer change left for its own focused pass._

**Quality / UX**
5. ✅ **Web "Role" is a `<select>`** (was free-text — risky for `0`=Admin). Added `domain::ROLES`.
6. ✅ **Removed dead `DbClient`** + the file-level `#![allow(dead_code)]` in `lbrp/src/clients.rs`
   (scoped the allow to the two deferred lock wrappers). This exposed and fixed an inconsistency:
   the `create_user` handler bypassed its client wrapper.
7. ✅ **Production panics documented/handled** — `auth/src/server.rs` JWT-expiry now returns a clean
   error instead of `expect`; `custodian/src/metrics.rs` registration `unwrap`s → documented
   startup `expect`s.
8. ✅ **CLAUDE.md service-graph fix** — DB shown as `:8080`, corrected to `:50051`.

**Test architecture**
9. ✅ **Shared `test-support` crate** — a `spawn_grpc!` macro replaces the bind/spawn boilerplate
   duplicated across every test module; LBRP's six start-helpers migrated to it.
   _Residual: the behavior-specific mock service impls themselves remain per-crate; unifying them
   into configurable shared mocks is a larger follow-up._

**Final gate status:** `cargo build` ✓ · `clippy --all-targets --all-features -D warnings` ✓ ·
`fmt --check` ✓ · full `cargo test --workspace` ✓ · db + custodian 3-node burn-ins ✓ ·
`cargo tarpaulin --fail-under 90` → **92.49%** ✓ · `cargo geiger` workspace gate ✓.

---

## Idiomatic-Rust improvements log

A follow-up review identified the next five idiomatic improvements; all are applied and validated.

1. ✅ **Consistent crate lints + cruft removal** — added `#![forbid(unsafe_code)]` +
   `#![warn(clippy::all, clippy::pedantic)]` to `web`, `test-support`, `tests`, and
   `forbid(unsafe_code)` to `proto`. `web` carries scoped, justified allows for framework idioms
   (Leptos prelude globs, large `view!` fns, DTO field names that mirror the wire). Deleted the
   leftover `cargo new` stub (`add()` + `it_works`) from `tests/src/lib.rs`.
2. ✅ **`From`/`Into` conversions** — `lbrp` proto→DTO maps (`map_ticket`/`map_user`) are now
   `impl From<…> for ApiTicket`/`ApiUser`, so call sites read `.map(ApiTicket::from)` / `.into()`.
   (`map_next_action` stays a free fn — the orphan rule forbids `From<…> for Option<ApiNextAction>`;
   documented inline.)
3. ✅ **Typed errors (no more `Result<_, String>`)** — new `raft_rpc::WireError` (`thiserror`,
   with `#[from] serde_json::Error`) replaces stringly errors in the wire codec; new
   `web::api::ApiError` (`Transport(#[from] gloo_net::Error)` + `Server(String)`) lets the WASM
   client use `?` throughout and the UI distinguish transport vs server failures.
4. ✅ **`#[tracing::instrument]` spans** — the db & custodian Raft handlers and the auth
   `authenticate`/`validate_session` handlers now carry structured spans (with `skip_all` so
   passwords/tokens/sessions are never logged; safe fields like `username`/`entries`/`offset`
   recorded), replacing ad-hoc `debug!("received …")` lines.
5. ✅ **Tonic clients no longer behind `Arc<Mutex<…>>`** — `auth`/`admin` store the cheaply-clonable
   `DatabaseClient` directly (clone per call); `lbrp`'s reloadable clients use `arc_swap::ArcSwap`
   so the `services.toml` reload swaps connections atomically and lock-free, ending the per-RPC
   mutex that serialized all backend calls. Client struct fields are now private behind
   `from_channel`/wrapper methods (and the needless `async` on `reconnect`/`reload_once`/
   `apply_changes` was removed). Validated by both 3-node Raft burn-ins.

**Final gate status:** `cargo build` ✓ · `clippy --all-targets --all-features -D warnings` ✓ ·
`fmt --check` ✓ · full `cargo test --workspace` ✓ · db + custodian 3-node burn-ins ✓ ·
`cargo tarpaulin --fail-under 90` → **92.42%** ✓ · `trunk build --release` ✓.
