#!/usr/bin/env bash
# Stops the dev extras. `--volumes` also drops the CDC PostgreSQL data (and Kafka keeps the Connect topics).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
docker compose -f "$ROOT/infra/docker/docker-compose.extras.yml" down "$@"
