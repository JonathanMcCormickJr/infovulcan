#!/usr/bin/env bash
# scripts/coverage.sh — Measure code coverage with cargo-tarpaulin under the project's policy.
#
# Coverage is measured for business-logic packages only, via deterministic unit/integration tests.
# The #[ignore]d multi-node Raft burn-in tests (db/custodian three_node_cluster) are intentionally
# NOT run here: under LLVM instrumentation their timing-sensitive elections flake, which would make
# the gate unreliable. The Raft consensus/network code is covered directly by unit + fault-injection
# tests instead; run the burn-in separately:
#   cargo test -p db -p custodian --test three_node_cluster -- --ignored
# Mirrors tarpaulin.toml, but passed explicitly so the scope applies regardless of whether the
# installed tarpaulin auto-reads the config file.
#
# Usage:
#   ./scripts/coverage.sh                 # enforce the 90% gate (fails under)
#   ./scripts/coverage.sh --no-fail       # report only, don't fail under 90%
#   ./scripts/coverage.sh --out Html      # also emit an HTML report
#
# Requires: cargo install cargo-tarpaulin

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

fail_under=(--fail-under 90)
extra=()
for arg in "$@"; do
    case "$arg" in
        --no-fail) fail_under=() ;;
        *) extra+=("$arg") ;;
    esac
done

exec cargo tarpaulin \
    --ignore-config \
    --engine llvm \
    --skip-clean \
    --timeout 300 \
    --workspace \
    --exclude web --exclude tests --exclude spike-raft-toy --exclude spike-tonic-mtls \
    --exclude-files '*/main.rs' \
    --exclude-files '*/build.rs' \
    --exclude-files 'spikes/*' \
    --exclude-files 'db/tests/performance_test.rs' \
    --exclude-files '*/tests/three_node_cluster.rs' \
    "${fail_under[@]}" \
    "${extra[@]}"
