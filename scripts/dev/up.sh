#!/usr/bin/env bash
# Піднімає dev-стек і чекає на healthy. Без аргументів - усі сервіси;
# аргументи - підмножина (напр. `up.sh kafka clickhouse`). Імя проєкту: COMPOSE_PROJECT_NAME (напр. для одноразового e2e).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ARGS=(-f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && ARGS+=(--env-file "$ROOT/.env")

# Kafka's dev-only TLS/SASL listener (DEVTLS, 19093) bind-mounts a cert
# directory read-only, so it must exist before the container starts.
# Idempotent/cheap: always run, regardless of which services were requested.
"$ROOT/scripts/dev/gen-kafka-tls.sh"

docker compose "${ARGS[@]}" up -d --wait "$@"
docker compose "${ARGS[@]}" ps

# Provision the DEVTLS listener's SCRAM-SHA-512 dev user, only when kafka is
# actually part of this run (no args = all services).
if [ $# -eq 0 ] || printf '%s\n' "$@" | grep -qx kafka; then
  "$ROOT/scripts/dev/provision-kafka-scram.sh"
fi
