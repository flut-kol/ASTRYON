#!/usr/bin/env bash
# DEV ONLY: dev infra (astryon_* names) + topics + migrations + seed + the single application container.
# Default = dev token mode; AAS mode only when AAS_BASE_URL is exported (see infra/docker/apps/README.md).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f infra/docker/docker-compose.dev.yml -f infra/docker/docker-compose.apps.yml -f infra/docker/docker-compose.scanner.yml)
[ -f .env ] && COMPOSE+=(--env-file .env)
export APPS_UID="$(id -u)" APPS_GID="$(id -g)"
TENANT=00000000-0000-0000-0000-0000000000a1; SOURCE=00000000-0000-0000-0000-0000000000b1

mkdir -p .astryon-dev
TOKEN_FILE="$ROOT/.astryon-dev/dev-token"
if [ ! -s "$TOKEN_FILE" ]; then (umask 077; openssl rand -hex 24 >"$TOKEN_FILE"); fi   # never printed
chmod 600 "$TOKEN_FILE"

"${COMPOSE[@]}" up -d --wait kafka postgres clickhouse s3mock
./scripts/dev/create-topics.sh >/dev/null
./scripts/dev/migrate.sh >/dev/null

PSQL=("${COMPOSE[@]}" exec -T postgres psql -U "${POSTGRES_USER:-astryon}" -d "${POSTGRES_DB:-astryon}" -v ON_ERROR_STOP=1 -q)
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
S3="http://127.0.0.1:${S3_HOST_PORT:-19200}"
for b in files exports; do curl -sS -o /dev/null -X PUT "$S3/$b" || true; done   # 409/200 = fine

"${COMPOSE[@]}" up -d --build --force-recreate apps
if [ -n "${AAS_BASE_URL:-}" ]; then mode="AAS ($AAS_BASE_URL)"; else mode="dev token (file .astryon-dev/dev-token)"; fi
cat <<EOT
apps up, auth mode: $mode
gateway :18080  query-api :18082  export-worker :18083  file-api :18085  quarantine :18086  replay-controller :18087  connector-manager :18088
tenant $TENANT  source $SOURCE   logs: make apps-logs   stop: make apps-down
EOT
