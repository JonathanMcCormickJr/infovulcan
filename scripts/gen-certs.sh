#!/usr/bin/env bash
#
# Generate a development PKI for InfoVulcan internal mTLS:
#   - one self-signed internal CA
#   - one service-unique leaf certificate per service (serverAuth + clientAuth),
#     with SANs covering `localhost` and `127.0.0.1` so any in-cluster dial verifies.
#
# Usage:  scripts/gen-certs.sh [OUTPUT_DIR]   (default: ./certs)
#
# Then point each service at its cert via env (see proto::tls):
#   TLS_CA_CERT=certs/ca.crt TLS_CERT=certs/db.crt TLS_KEY=certs/db.key ./db
#
# These are DEV certs. Production should mint service certs from a managed CA.

set -euo pipefail

OUT_DIR="${1:-certs}"
DAYS_CA=3650
DAYS_LEAF=825
SERVICES=(db custodian auth admin lbrp chaos honeypot)

command -v openssl >/dev/null 2>&1 || { echo "error: openssl not found on PATH" >&2; exit 1; }

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo "Generating internal CA..."
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key 2>/dev/null
openssl req -x509 -new -key ca.key -days "$DAYS_CA" \
  -subj "/CN=infovulcan-internal-ca/O=InfoVulcan" -out ca.crt 2>/dev/null

gen_service() {
  local name="$1"
  echo "Generating cert for: $name"
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$name.key" 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$name/O=InfoVulcan" -out "$name.csr" 2>/dev/null
  cat > "$name.ext" <<EOF
subjectAltName = DNS:localhost, DNS:$name, IP:127.0.0.1
extendedKeyUsage = serverAuth, clientAuth
keyUsage = digitalSignature, keyEncipherment
EOF
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS_LEAF" -extfile "$name.ext" -out "$name.crt" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
}

for svc in "${SERVICES[@]}"; do
  gen_service "$svc"
done
rm -f ca.srl

echo
echo "PKI written to: $OUT_DIR"
echo "  CA:        ca.crt / ca.key"
echo "  Services:  ${SERVICES[*]}"
echo
echo "Enable mTLS for a service, e.g. db:"
echo "  TLS_CA_CERT=$OUT_DIR/ca.crt TLS_CERT=$OUT_DIR/db.crt TLS_KEY=$OUT_DIR/db.key ./db"
