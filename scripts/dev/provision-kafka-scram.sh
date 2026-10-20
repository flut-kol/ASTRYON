#!/usr/bin/env bash
# DEV ONLY: provisions the SCRAM-SHA-512 credential used by the dev stack's
# DEVTLS (SASL_SSL, port 19093) Kafka listener, with a placeholder dev
# username/password consistent with other placeholder creds in this repo
# (astryon_dev / astryon_dev_only).
#
# In KRaft combined broker+controller mode, SCRAM users normally need either
# `kafka-storage.sh format --add-scram` at bootstrap (to create an initial
# super user before any listener accepts connections) or `kafka-configs.sh
# --alter --add-config 'SCRAM-SHA-512=[...]' --entity-type users` against a
# *running* broker. This stack's INTERNAL listener stays PLAINTEXT (per
# AGENTS.md: the plain EXTERNAL dev listener is never changed), so the
# simpler kafka-configs.sh path works: it is run here against that PLAINTEXT
# listener from inside the compose network, after the broker is healthy.
# Idempotent (--alter overwrites the existing credential), and safe to re-run
# any time without restarting the broker or regenerating certs.
#
# `make dev-up` / scripts/dev/up.sh runs this automatically once the kafka
# service reports healthy.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && COMPOSE+=(--env-file "$ROOT/.env")

USER="${ASTRYON_KAFKA_SASL_DEV_USER:-astryon_dev}"
PASS="${ASTRYON_KAFKA_SASL_DEV_PASS:-astryon_dev_only}"

"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 --alter \
  --add-config "SCRAM-SHA-512=[password=$PASS]" \
  --entity-type users --entity-name "$USER" </dev/null

echo "provisioned SCRAM-SHA-512 dev user '$USER' for the Kafka DEVTLS listener"
