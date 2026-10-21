#!/usr/bin/env bash
# DEV DEMO: brings up the compose stack (own project name) and starts gateway, clickhouse-sink, file-api, file-worker,
# query-api as detached host processes with static DEV auth. DEV ONLY: placeholder credentials, never use outside dev.
# Repeatable. Env overrides: DEMO_DIR, DEMO_TOKEN (default: random, persisted), DEMO_TENANT, DEMO_SOURCE, GATEWAY_PORT, FILE_API_PORT, QUERY_API_PORT.
# NOTE: the Flink processing job is not started; clickhouse-sink reads raw.events.v1 directly (it accepts raw envelopes).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-astryon-demo}"
DEMO_DIR="${DEMO_DIR:-${TMPDIR:-/tmp}/astryon-demo}"
# Dev token: random, generated on first start, kept in a 0600 file under DEMO_DIR (reused on restart; never in the repo).
# DEMO_TOKEN overrides. Passed to gateway/file-api/query-api as the static dev token and to the Vite dev server (server-side injection).
TENANT="${DEMO_TENANT:-00000000-0000-0000-0000-0000000000a1}"
SOURCE="${DEMO_SOURCE:-00000000-0000-0000-0000-0000000000b1}"
GW_PORT="${GATEWAY_PORT:-18080}"; FILE_PORT="${FILE_API_PORT:-18085}"; Q_PORT="${QUERY_API_PORT:-18082}"
KAFKA="127.0.0.1:${KAFKA_HOST_PORT:-19092}"; CH_URL="http://127.0.0.1:${CLICKHOUSE_HTTP_HOST_PORT:-18124}"
CH_USER="${CLICKHOUSE_USER:-astryon}"; CH_PASS="${CLICKHOUSE_PASSWORD:-astryon_dev_only}"
PG_PORT="${POSTGRES_HOST_PORT:-15432}"; S3="http://127.0.0.1:${S3_HOST_PORT:-19200}"
APP_PG="postgres://demo_app:demo_dev_only@127.0.0.1:${PG_PORT}/${POSTGRES_DB:-astryon}"
mkdir -p "$DEMO_DIR/bin" "$DEMO_DIR/pids" "$DEMO_DIR/logs"
TOKEN_FILE="$DEMO_DIR/dev-token"
if [ -n "${DEMO_TOKEN:-}" ]; then TOKEN="$DEMO_TOKEN"
else
  if [ ! -s "$TOKEN_FILE" ]; then (umask 077; openssl rand -hex 24 >"$TOKEN_FILE"); fi
  chmod 600 "$TOKEN_FILE"; TOKEN="$(cat "$TOKEN_FILE")"
fi

./scripts/dev/up.sh
./scripts/dev/migrate.sh
./scripts/dev/create-topics.sh >/dev/null
PSQL=(docker compose -f infra/docker/docker-compose.dev.yml exec -T postgres psql -U "${POSTGRES_USER:-astryon}" -d "${POSTGRES_DB:-astryon}" -v ON_ERROR_STOP=1 -q)
"${PSQL[@]}" <db/seeds/0001_dev_tenant.sql >/dev/null
"${PSQL[@]}" -o /dev/null <<SQL
DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='demo_app') THEN
  CREATE ROLE demo_app LOGIN PASSWORD 'demo_dev_only' NOSUPERUSER NOBYPASSRLS; END IF; END \$\$;
GRANT ALL ON ALL TABLES IN SCHEMA public TO demo_app; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO demo_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO demo_app;
BEGIN;
SELECT set_config('astryon.platform_admin','on',true);
INSERT INTO sources (id,tenant_id,name,kind) VALUES ('$SOURCE','$TENANT','demo-files','s3_files') ON CONFLICT DO NOTHING;
COMMIT;
SQL
curl -sS -o /dev/null -X PUT "$S3/files" || true # bucket (409 = already exists)

build() { # module dir, cmd
  (cd "services/$1" && go build -o "$DEMO_DIR/bin/$2" "./cmd/$2")
}
start() { # name, env... -- uses current exported env via env(1)
  local name=$1; shift
  if [ -f "$DEMO_DIR/pids/$name.pid" ] && kill -0 "$(cat "$DEMO_DIR/pids/$name.pid")" 2>/dev/null; then echo "$name already running"; return; fi
  nohup env "$@" "$DEMO_DIR/bin/$name" >"$DEMO_DIR/logs/$name.log" 2>&1 </dev/null &
  echo $! >"$DEMO_DIR/pids/$name.pid"; echo "started $name pid $!"
}
build gateway gateway; build clickhouse-sink clickhouse-sink; build file-ingestion file-api; build file-ingestion file-worker; build query-api query-api

start gateway ASTRYON_GATEWAY_ADDR=":$GW_PORT" ASTRYON_DEV_AUTH=1 ASTRYON_DEV_TOKEN="$TOKEN" ASTRYON_DEV_TENANT_ID="$TENANT" \
  ASTRYON_KAFKA_BROKERS="$KAFKA"
start clickhouse-sink CHSINK_KAFKA_BROKERS="$KAFKA" CHSINK_TOPIC=raw.events.v1 CHSINK_GROUP_ID=demo-sink \
  CHSINK_CH_URL="$CH_URL" CHSINK_CH_USER="$CH_USER" CHSINK_CH_PASSWORD="$CH_PASS"
S3ENV=(FILE_S3_ENDPOINT="$S3" FILE_S3_BUCKET=files FILE_S3_REGION=us-east-1 FILE_S3_ACCESS_KEY=demo FILE_S3_SECRET_KEY=demo-dev-only)
start file-api LISTEN_ADDR=":$FILE_PORT" FILE_API_DEV_TOKEN="$TOKEN" FILE_API_DEV_TENANT="$TENANT" FILE_API_PG_URL="$APP_PG" "${S3ENV[@]}"
start file-worker FILE_WORKER_PG_URL="$APP_PG" "${S3ENV[@]}" CH_URL="$CH_URL" CH_DATABASE="${CLICKHOUSE_DB:-astryon}" CH_USER="$CH_USER" \
  CH_PASSWORD="$CH_PASS" FILE_EVENT_TYPE=file.row FILE_CREDENTIALS_POLICY=quarantine
start query-api LISTEN_ADDR=":$Q_PORT" QUERY_DEV_TOKEN="$TOKEN" QUERY_DEV_TENANT="$TENANT" CH_URL="$CH_URL" \
  CH_DATABASE="${CLICKHOUSE_DB:-astryon}" CH_USER="$CH_USER" CH_PASSWORD="$CH_PASS" QUERY_PG_URL="$APP_PG"
# Admin UI dev server (127.0.0.1 only). ADMIN_UI_DEV_TOKEN is read by vite.config.ts at start; the proxy adds the Authorization header.
if [ -d web/admin-ui/node_modules ]; then
  if [ -f "$DEMO_DIR/pids/admin-ui.pid" ] && kill -0 "$(cat "$DEMO_DIR/pids/admin-ui.pid")" 2>/dev/null; then echo "admin-ui already running"
  else
    (cd web/admin-ui && ADMIN_UI_DEV_TOKEN="$TOKEN" nohup npm run dev >"$DEMO_DIR/logs/admin-ui.log" 2>&1 </dev/null & echo $! >"$DEMO_DIR/pids/admin-ui.pid")
    echo "started admin-ui http://127.0.0.1:15173"
  fi
else echo "admin-ui: run 'npm install' in web/admin-ui, then re-run to start it"; fi
cat <<EOT
gateway   http://127.0.0.1:$GW_PORT   file-api http://127.0.0.1:$FILE_PORT   query-api http://127.0.0.1:$Q_PORT
token: $TOKEN (file $TOKEN_FILE)   tenant: $TENANT   source_id: $SOURCE   logs: $DEMO_DIR/logs   stop: scripts/dev/demo-down.sh
EOT
