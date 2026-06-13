#!/usr/bin/env bash
# scripts/build-images.sh — Package InfoVulcan services as NanoVMs OPS unikernel images.
#
# Each Hardened-topology service becomes its own minimal unikernel (one process, no shell, no
# package manager) — the small-attack-surface deployment model from ARCHITECTURE.md. The lbrp
# image additionally bundles the WASM web bundle and a services.toml.
#
# Usage:
#   ./scripts/build-images.sh                 # build release binaries + create all images
#   ./scripts/build-images.sh --skip-build    # reuse existing target/release binaries
#   ./scripts/build-images.sh db lbrp         # only the named services
#   IMAGE_PREFIX=acme ./scripts/build-images.sh   # name images acme-<svc>
#
# Requirements:
#   - Rust toolchain (cargo) to build the service binaries.
#   - The `ops` CLI (https://ops.city). Install: curl https://ops.city/get.sh -sSfL | sh
#   - `trunk` (for the lbrp web bundle): cargo install trunk
#
# NOTE: This environment cannot itself boot a unikernel; the per-service configs in deploy/ops/
# and the images this produces are UNVALIDATED. Run on a host with KVM + ops to validate.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OPS_DIR="$ROOT_DIR/deploy/ops"
BIN_DIR="$ROOT_DIR/target/release"
IMAGE_PREFIX="${IMAGE_PREFIX:-infovulcan}"

ALL_SERVICES=(db custodian auth admin lbrp chaos honeypot)

skip_build=false
requested=()
for arg in "$@"; do
    case "$arg" in
        --skip-build) skip_build=true ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) requested+=("$arg") ;;
    esac
done

services=("${requested[@]:-${ALL_SERVICES[@]}}")

# --- Preflight ----------------------------------------------------------------
if ! command -v ops &>/dev/null; then
    echo "ERROR: the 'ops' CLI is not installed."
    echo "  Install: curl https://ops.city/get.sh -sSfL | sh"
    echo "  Then re-run. (Configs in $OPS_DIR are still valid as a reference.)"
    exit 1
fi

# --- Optional: fan out a flat PKI into the per-service cert dirs --------------
# scripts/gen-certs.sh writes a flat dir (ca.crt, <svc>.crt, <svc>.key). The image configs
# expect deploy/ops/certs/<svc>/ (and deploy/ops/lbrp-conf/certs/ for lbrp). Set CERTS_SRC to
# that flat dir to provision them here.
if [[ -n "${CERTS_SRC:-}" ]]; then
    echo "Provisioning per-service certs from $CERTS_SRC"
    for svc in "${ALL_SERVICES[@]}"; do
        if [[ "$svc" == lbrp ]]; then
            dest="$OPS_DIR/lbrp-conf/certs"
        else
            dest="$OPS_DIR/certs/$svc"
        fi
        mkdir -p "$dest"
        cp "$CERTS_SRC/ca.crt" "$CERTS_SRC/$svc.crt" "$CERTS_SRC/$svc.key" "$dest/"
    done
fi

# --- Build release binaries ---------------------------------------------------
if [[ "$skip_build" == false ]]; then
    echo "Building release binaries for: ${services[*]}"
    build_args=()
    for svc in "${services[@]}"; do build_args+=(-p "$svc"); done
    cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml" "${build_args[@]}"

    # lbrp serves the WASM bundle — build it if we're packaging lbrp.
    if [[ " ${services[*]} " == *" lbrp "* ]]; then
        if command -v trunk &>/dev/null; then
            echo "Building web frontend (trunk)..."
            (cd "$ROOT_DIR/web" && trunk build --release)
        else
            echo "WARNING: 'trunk' not found — lbrp image will ship without an up-to-date web/dist."
        fi
    fi
fi

# --- Create one image per service ---------------------------------------------
for svc in "${services[@]}"; do
    config="$OPS_DIR/$svc.json"
    binary="$BIN_DIR/$svc"
    image="$IMAGE_PREFIX-$svc"

    if [[ ! -f "$config" ]]; then
        echo "SKIP $svc: no config at $config"
        continue
    fi
    if [[ ! -f "$binary" ]]; then
        echo "SKIP $svc: no binary at $binary (build first, or drop --skip-build)"
        continue
    fi

    echo "==> Creating image '$image' from $binary"
    # `ops image create` bakes the ELF + config (env, ports, mapped dirs) into a bootable image.
    ( cd "$OPS_DIR" && ops image create "$binary" -c "$svc.json" -i "$image" )
    echo "    done: $image"
done

cat <<EOF

------------------------------------------------------------
 Images created with prefix '$IMAGE_PREFIX-'.
 List:   ops image list
 Run:    ops instance create $IMAGE_PREFIX-db        # (repeat per service)
 Deploy: see deploy/README.md for cert provisioning, Raft peer wiring, and ordering.
------------------------------------------------------------
EOF
