#!/usr/bin/env bash
# Піднімає dev-стек і чекає на healthy. Без аргументів - усі сервіси;
# аргументи - підмножина (напр. `up.sh kafka clickhouse`). Імя проєкту: COMPOSE_PROJECT_NAME (напр. для одноразового e2e).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ARGS=(-f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && ARGS+=(--env-file "$ROOT/.env")
docker compose "${ARGS[@]}" up -d --wait --remove-orphans "$@"
docker compose "${ARGS[@]}" ps
