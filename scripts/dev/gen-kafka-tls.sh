#!/usr/bin/env bash
# DEV ONLY: generates a throwaway self-signed CA + Kafka broker certificate for
# the dev stack's extra Kafka listener (docker-compose.dev.yml service "kafka",
# listener name DEVTLS, SASL_SSL on 127.0.0.1:${KAFKA_TLS_HOST_PORT:-19093}).
#
# Uses PEM-format keys/certs directly (ssl.keystore.type=PEM, supported by the
# apache/kafka:3.9.0 image since Kafka 3.7 / KIP-651) so no keytool/JKS keystore
# is needed. The broker's private key is encrypted with a fixed DEV-ONLY
# placeholder password; it MUST match KAFKA_LISTENER_NAME_DEVTLS_SSL_KEY_PASSWORD
# in docker-compose.dev.yml.
#
# Output goes to infra/docker/.devcerts/kafka/ (gitignored - never commit it).
# `make dev-up` / scripts/dev/up.sh runs this automatically before starting the
# stack, since the cert directory is bind-mounted read-only into the kafka
# container and must exist first. Safe to re-run; skips if certs already exist
# unless --force is passed (e.g. after rotating the key password).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/infra/docker/.devcerts/kafka"
KEY_PASSWORD="${ASTRYON_KAFKA_TLS_KEY_PASSWORD:-astryon_dev_only}"

if [ "${1:-}" != "--force" ] && [ -f "$OUT/ca.crt" ] && [ -f "$OUT/broker.keystore.pem" ]; then
  echo "dev Kafka TLS CA + broker cert already present at $OUT (pass --force to regenerate)"
  exit 0
fi

command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

mkdir -p "$OUT"
cd "$OUT"
rm -f ca.key ca.crt ca.srl broker.key.plain broker.csr broker.crt broker.key.enc broker.keystore.pem ext.cnf

# CA (self-signed, dev-only, 10y so devs don't have to think about it)
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out ca.key 2>/dev/null
openssl req -x509 -new -key ca.key -sha256 -days 3650 \
  -subj "/CN=ASTRYON Dev Kafka CA" -out ca.crt 2>/dev/null

# Broker key + cert, signed by the CA above. SANs cover both the in-network
# hostname ("kafka", used by other containers and by scripts/dev/create-topics.sh)
# and the host-mapped address (used by services/gateway tests and local tools).
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out broker.key.plain 2>/dev/null
openssl req -new -key broker.key.plain -subj "/CN=kafka" -out broker.csr 2>/dev/null
cat > ext.cnf <<EOF
subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
  -sha256 -out broker.crt -extfile ext.cnf 2>/dev/null

# Encrypt the broker private key (PKCS8, PBES2/AES-256-CBC) so it's not stored
# plaintext on disk, then concatenate cert + CA + encrypted key into a single
# PEM file: this is what ssl.keystore.type=PEM / ssl.keystore.location expects.
openssl pkcs8 -topk8 -v2 aes-256-cbc -in broker.key.plain \
  -out broker.key.enc -passout pass:"$KEY_PASSWORD" 2>/dev/null
cat broker.crt ca.crt broker.key.enc > broker.keystore.pem

rm -f broker.key.plain broker.csr broker.crt broker.key.enc ext.cnf ca.srl
chmod 600 ca.key broker.keystore.pem

echo "generated dev Kafka TLS CA + broker cert in $OUT"
echo "  CA cert (truststore for clients):      $OUT/ca.crt"
echo "  broker keystore (cert chain + key):    $OUT/broker.keystore.pem"
