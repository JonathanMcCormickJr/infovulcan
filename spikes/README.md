# Spikes

Proofs-of-concept for quick experiments — not production code.

Each spike is its own workspace member so it shares the lockfile (and therefore
the resolved crate versions) with the rest of InfoVulcan, but its target is
purely educational.

## Week 1

| Crate | Validates | Run |
|-------|-----------|-----|
| [`tonic-mtls`](./tonic-mtls/) | A tonic gRPC client/server over rustls TLS 1.3 with **mutual** auth (self-signed PKI generated at test time via `rcgen`). Both the happy-path round-trip and the "client without identity is rejected" negative case are covered. | `cargo test -p spike-tonic-mtls` |
| [`raft-toy`](./raft-toy/) | A 3-node `openraft` cluster wired with an in-process loopback network on top of `openraft-memstore`. Exercises leader election, log replication to followers, and leader failover (kill the leader, watch a new one get elected and keep accepting writes). | `cargo test -p spike-raft-toy` |

Both spikes are intentionally small — the goal is to internalize the mental
model behind the production `db` / `custodian` / `lbrp` / `auth` services
before extending them, not to ship anything reusable.
