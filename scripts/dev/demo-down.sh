#!/usr/bin/env bash
# Stops the processes started by demo-up.sh. With --all also stops the compose project (data volumes are kept; --purge removes them).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-astryon-demo}"
DEMO_DIR="${DEMO_DIR:-${TMPDIR:-/tmp}/astryon-demo}"
for f in "$DEMO_DIR"/pids/*.pid; do
  [ -f "$f" ] || continue
  pid=$(cat "$f")
  # admin-ui pid is the npm wrapper: also stop its children (vite)
  pkill -P "$pid" 2>/dev/null || true
  kill "$pid" 2>/dev/null && echo "stopped $(basename "$f" .pid) ($pid)" || true; rm -f "$f"
done
case "${1:-}" in
  --all) docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml" down ;;
  --purge) docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml" down -v ;;
esac
