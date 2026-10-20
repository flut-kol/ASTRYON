#!/usr/bin/env bash
# Зупиняє dev-стек. З аргументом --volumes видаляє також дані.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ARGS=(-f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && ARGS+=(--env-file "$ROOT/.env")
if [ "${1:-}" = "--volumes" ]; then docker compose "${ARGS[@]}" down --remove-orphans -v; else docker compose "${ARGS[@]}" down --remove-orphans; fi
