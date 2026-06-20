# InfoVulcan Unikernel Deployment (NanoVMs OPS)

InfoVulcan's Hardened topology deploys each service as its own **NanoVMs OPS unikernel** — a
single Rust binary booting directly on a hypervisor with no shell, package manager, or
multi-process surface. This is the minimal-attack-surface model from `ARCHITECTURE.md` (no Docker).

> ✅ **Image build validated.** `scripts/build-images.sh` (with `CERTS_SRC` provisioning) creates
> all 7 service images successfully via `ops image create`, and a single `db` image has been
> observed to **boot and run** under `ops` (Sled storage init, openraft startup, gRPC server up).
>
> ⚠️ **Booting/clustering not fully validated.** Full multi-node bring-up (3-node Raft quorum,
> mTLS peer mesh, lbrp→backends) requires a host with hardware virtualization (**KVM**) and proper
> instance networking. Validate the boot order and cross-service connectivity there before relying
> on it. (Without KVM, `ops` falls back to slow TCG emulation — fine for a single-binary smoke
> test, impractical for a live cluster.)

### OPS config conventions (do not regress)

The `deploy/ops/*.json` files must obey several `ops` rules — violating them makes
`ops image create` fail, sometimes with cryptic errors:

- **No comment fields.** `ops` rejects unknown JSON keys (e.g. a `_comment` field →
  `json: unknown field "_comment"`). Keep notes here in the README, not in the JSON.
- **`MapDirs` is `"<host-source>": "<image-dest>"`.** The *key* is the host path (resolved
  **relative to the config file's directory**, `deploy/ops/`), the *value* is the path inside the
  image. Reversing it yields `lstat <image-path>: no such file or directory`.
- **`MapDirs` host paths need ≥2 components.** A single-component host path like `./lbrp-conf`
  fails with an empty `lstat : no such file or directory`; use a nested path like
  `./lbrp-conf/infovulcan`. (This is why the lbrp `/etc/infovulcan` payload lives one level deep.)
- **`Dirs` requires the host dir to exist.** The Raft services bake an empty `/data` via
  `"Dirs": ["data"]`, so `deploy/ops/data/` must exist (tracked via `data/.gitkeep`;
  `build-images.sh` also `mkdir -p`s it).

## Components

| Image | Service | Port | Notes |
|-------|---------|------|-------|
| `infovulcan-db` | db | 50051 | Raft + Sled; **3+ instances** for quorum |
| `infovulcan-custodian` | custodian | 8081 | Raft locks; **3+ instances**; needs a db leader |
| `infovulcan-auth` | auth | 8082 | Stateless; `JWT_SECRET` must match lbrp |
| `infovulcan-admin` | admin | 8083 | Stateless; user mgmt + intrusion audit log |
| `infovulcan-lbrp` | lbrp | 8080 | **Only internet-facing image**; serves web bundle |
| `infovulcan-chaos` | chaos | 8084 | Fault injection; gate with `CHAOS_AUTH_TOKEN` |
| `infovulcan-honeypot` | honeypot | 8085 | Deceptive trap; reports to admin |

The `web` crate is **not** a separate image — its WASM bundle is built with `trunk` and baked into
the lbrp image under `/web/dist`.

## Prerequisites

```bash
# Rust toolchain (build the service binaries)
rustup toolchain install stable

# OPS CLI (build/boot images) — https://ops.city
curl https://ops.city/get.sh -sSfL | sh

# trunk (build the web bundle baked into lbrp)
cargo install trunk
```

A host that can boot images needs hardware virtualization (KVM on Linux).

## 1. Provision the PKI (mTLS)

All inter-service traffic is mutual TLS with a shared CA and service-unique certificates. Mint a
dev PKI and fan it out into the per-service cert directories the image configs expect:

```bash
./scripts/gen-certs.sh certs                 # writes flat ca/<svc> certs into ./certs
CERTS_SRC=certs ./scripts/build-images.sh    # provisions deploy/ops/certs/<svc>/ + lbrp-conf/infovulcan/certs/
```

Each non-lbrp config maps `./certs/<svc>` → `/etc/infovulcan/certs`; lbrp maps
`./lbrp-conf/infovulcan` → `/etc/infovulcan` (so its `certs/` **and** `services.toml` ride along).
The generated cert dirs are git-ignored — never commit private keys. For production, replace the
dev CA with your real PKI and ensure each certificate's SAN matches the DNS name used in
`RAFT_PEERS` / `services.toml`.

## 2. Set secrets

Edit the configs (or override env at instance-create time) before building:

- `JWT_SECRET` — **identical** in `auth.json` and `lbrp.json`.
- `CHAOS_AUTH_TOKEN` — a strong token in `chaos.json` (chaos is open if unset — never do that in prod).
- `RAFT_PEERS` / `DB_LEADER_ADDR` / `services.toml` — real hostnames for your network.

## 3. Build images

```bash
./scripts/build-images.sh                 # all services
./scripts/build-images.sh db lbrp         # a subset
IMAGE_PREFIX=acme ./scripts/build-images.sh   # custom image name prefix
```

This compiles `--release` binaries (and the web bundle for lbrp), then runs `ops image create`
per service using `deploy/ops/<svc>.json`.

## 4. Boot order

Raft clusters must reach quorum before dependents come up:

1. **db** ×3 — start all three, wait for a leader.
2. **custodian** ×3 — needs `DB_LEADER_ADDR` reachable.
3. **auth**, **admin** — need db.
4. **lbrp** — needs auth/admin/custodian; this is the only image you expose publicly.
5. **chaos**, **honeypot** — optional hardening services, any time after admin.

```bash
ops instance create infovulcan-db --instance-name db-1 \
  -e NODE_ID=1 -e RAFT_PEERS="1:db-1:50051,2:db-2:50051,3:db-3:50051"
# …repeat for db-2 / db-3 with NODE_ID 2 / 3, then custodian, etc.
```

Per-instance values (`NODE_ID`, peer lists) are best supplied with `-e` at create time rather than
baked into the shared config.

## Static discovery & hot reload

lbrp reads `/etc/infovulcan/services.toml` (baked from `deploy/ops/lbrp-conf/infovulcan/services.toml`) and
re-reads it every `SERVICES_RELOAD_SECS` (default 30s), hot-reconnecting any backend whose endpoint
changed — no lbrp restart needed to repoint traffic. Per-service env vars (`AUTH_ADDR`,
`ADMIN_ADDR`, `CUSTODIAN_ADDR`) still override the file.

## Persistence

`db` and `custodian` keep Sled state under `STORAGE_PATH` (`/data/...`). Attach a persistent
volume to those instances so Raft logs/snapshots survive reboots; the stateless services
(auth/admin/lbrp/chaos/honeypot) need none.
