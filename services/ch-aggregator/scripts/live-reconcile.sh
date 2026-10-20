#!/usr/bin/env bash
# Live check of `ch-aggregator reconcile` on throwaway containers (ClickHouse 24.8 + postgres:16-alpine).
# Run from the repo root. Creates chagg-recon-ch (127.0.0.1:18924) and chagg-recon-pg (127.0.0.1:16632), applies
# db/clickhouse + db/postgres migrations, seeds data, asserts exit codes, and ALWAYS removes both containers.
# Passwords are generated per run. Does not use docker compose and touches nothing else.
set -uo pipefail
CH=chagg-recon-ch; PG=chagg-recon-pg
PGPW=$(openssl rand -hex 12); CHPW=$(openssl rand -hex 12); RPW=$(openssl rand -hex 12)
WORK=$(mktemp -d); BIN=$WORK/ch-aggregator; FAILS=0
cleanup() { docker rm -f "$CH" "$PG" >/dev/null 2>&1; rm -rf "$WORK"; }
trap cleanup EXIT
ok() { echo "PASS $*"; }
bad() { echo "FAIL $*"; FAILS=$((FAILS+1)); }
chq() { docker exec -i "$CH" clickhouse-client --password "$CHPW" --database astryon "$@"; }
pgq() { docker exec -i "$PG" psql -U postgres -d astryon -v ON_ERROR_STOP=1 -q "$@"; }

(cd services/ch-aggregator && go build -o "$BIN" ./cmd/ch-aggregator) || exit 1
docker run -d --name "$PG" -e POSTGRES_PASSWORD="$PGPW" -p 127.0.0.1:16632:5432 postgres:16-alpine >/dev/null || exit 1
docker run -d --name "$CH" -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD="$CHPW" -p 127.0.0.1:18924:8123 clickhouse/clickhouse-server:24.8 >/dev/null || exit 1
for i in $(seq 1 90); do
  docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && docker exec "$CH" clickhouse-client --password "$CHPW" -q 'select 1' >/dev/null 2>&1 && break
  sleep 1
done
sleep 3
docker exec "$PG" psql -U postgres -q -c 'CREATE DATABASE astryon' >/dev/null || exit 1
for f in db/postgres/migrations/*.up.sql; do pgq < "$f" >/dev/null || { echo "pg migration failed: $f"; exit 1; }; done
docker exec "$CH" clickhouse-client --password "$CHPW" -q 'CREATE DATABASE IF NOT EXISTS astryon' || exit 1
for f in db/clickhouse/migrations/*.up.sql; do
  sed -e 's/__ON_CLUSTER__//g' -e 's/__RMT__/MergeTree()/g' -e 's/__CLUSTER__/default/g' "$f" | chq --multiquery >/dev/null || { echo "ch migration failed: $f"; exit 1; }
done
# Non-superuser, NOBYPASSRLS reconciler role with SELECT on the registry table only.
pgq >/dev/null <<EOF || exit 1
CREATE ROLE reconciler LOGIN NOBYPASSRLS PASSWORD '$RPW';
GRANT USAGE ON SCHEMA public TO reconciler;
GRANT SELECT ON ingestion_batches TO reconciler;
EOF

TA=00000000-0000-4000-8000-00000000000a; TB=00000000-0000-4000-8000-00000000000b
SA=00000000-0000-4000-8000-0000000000a1; SB=00000000-0000-4000-8000-0000000000b1
C1=cccccccc-0000-4000-8000-000000000001; C2=cccccccc-0000-4000-8000-000000000002
OPENB=cccccccc-0000-4000-8000-000000000003; YOUNG=cccccccc-0000-4000-8000-000000000004
pgq >/dev/null <<EOF || exit 1
INSERT INTO tenants (id, slug, name) VALUES ('$TA','tenant-a','A'), ('$TB','tenant-b','B');
INSERT INTO sources (id, tenant_id, name, kind) VALUES ('$SA','$TA','sa','http_push'), ('$SB','$TB','sb','http_push');
INSERT INTO ingestion_batches (id, tenant_id, source_id, status, events_accepted) VALUES
 ('$C1','$TA','$SA','COMPLETED',50), ('$C2','$TB','$SB','COMPLETED',30);
EOF
# events: n rows for (tenant, source, batch); ids come from a seed so reruns of the same call are identical
ins() { # tenant source batch n offset
  chq -q "INSERT INTO events_normalized_local (tenant_id, source_id, event_id, batch_id, processing_version, pipeline_id, pipeline_version, event_type, event_time, payload, raw_ref, quality_flags, attributes)
  SELECT '$1','$2', reinterpretAsUUID(unhex(lpad(hex(cityHash64('$3', number + $5)), 16, '0') || lpad(hex(number + $5), 16, '0'))), '$3', 1, '$1', 1, 'reading', now64(3,'UTC') - toIntervalSecond(60 + number),
  concat('{\"n\":', toString(number + $5), '}'), 'live', [], map() FROM numbers($4)" ; }
ins $TA $SA $C1 50 0; ins $TB $SB $C2 30 0

export CH_URL=http://127.0.0.1:18924 CH_DATABASE=astryon CH_USER=default CH_PASSWORD="$CHPW" AGG_LOOKBACK=3h REGISTRY_GRACE=1s
export REGISTRY_PG_URL="postgres://reconciler:$RPW@127.0.0.1:16632/astryon?sslmode=disable"
recompute() { "$BIN" >/dev/null 2>&1; }
rec() { "$BIN" reconcile "$@" 2>"$WORK/out.txt"; echo $? ; }
expect() { # name want-exit [grep-pattern]
  local got; got=$(cat "$WORK/code"); 
  if [ "$got" = "$2" ] && { [ -z "${3:-}" ] || grep -q -- "$3" "$WORK/out.txt"; }; then ok "$1 (exit $got)"; else bad "$1: exit $got want $2 pattern '${3:-}'"; sed 's/^/    /' "$WORK/out.txt"; fi
}
run() { rec "$@" > "$WORK/code"; }
sleep 2

recompute; run --snapshot "$WORK/snap1.json"; expect "clean: no drift, registry matches" 0 "registry-check: .* 0 mismatches"
# the reconciler role is subject to RLS: without the platform switch it sees nothing
n=$(docker exec -i "$PG" psql -U reconciler -d astryon -tA -c 'select count(*) from ingestion_batches' 2>/dev/null | tr -d ' ')
[ "$n" = "0" ] && ok "RLS: reconciler role without admin switch sees 0 batches" || bad "RLS check got '$n'"

# 1) lost rows: 10 events of C1 deleted from ClickHouse
chq -q "ALTER TABLE events_normalized_local DELETE WHERE batch_id='$C1' AND event_id IN (SELECT event_id FROM events_normalized_local WHERE batch_id='$C1' LIMIT 10) SETTINGS mutations_sync=2"
recompute; run; expect "deleted rows: registry mismatch (missing)" 2 "kind=missing class=persistent .*batch=$C1 .*ch_events=40 registry_accepted=50"
# restore, then foreign/extra rows
ins $TA $SA $C1 10 1000; recompute; run; expect "restored: clean again" 0
ins $TA $SA $C1 5 2000      # extra rows in a registered batch
ins $TB $SB $C1 4 3000      # foreign rows: same batch id under another tenant/source
recompute; sleep 2; run; expect "extra rows in registered batch" 2 "kind=extra class=persistent .*batch=$C1 .*ch_events=55 registry_accepted=50"
grep -q "kind=unregistered class=persistent .*tenant=$TB .*batch=$C1" "$WORK/out.txt" && ok "foreign rows flagged unregistered" || bad "foreign rows not flagged"
chq -q "ALTER TABLE events_normalized_local DELETE WHERE batch_id='$C1' AND (tenant_id='$TB' OR event_id IN (SELECT event_id FROM events_normalized_local WHERE batch_id='$C1' AND tenant_id='$TA' AND JSONExtractUInt(payload,'n') >= 2000)) SETTINGS mutations_sync=2"
recompute; run --snapshot "$WORK/snap2.json"; expect "extra rows removed: clean" 0

# 2) silent corruption: payload tampered, count and ingestion time unchanged
chq -q "ALTER TABLE events_normalized_local UPDATE payload='{\"n\":-1}' WHERE batch_id='$C2' AND JSONExtractUInt(payload,'n')=7 SETTINGS mutations_sync=2"
recompute; run --compare "$WORK/snap2.json" --snapshot "$WORK/snap3.json"; expect "tampered payload, same count: checksum change" 2 "checksum-change SILENT"
run --compare "$WORK/snap3.json"; expect "compare against fresh snapshot: stable" 0 "compare: 0 changed"

# 3) transient: open young batch, and a completed batch younger than the grace period
pgq >/dev/null <<EOF || exit 1
INSERT INTO ingestion_batches (id, tenant_id, source_id, status, events_accepted) VALUES ('$OPENB','$TA','$SA','OPEN',20), ('$YOUNG','$TA','$SA','COMPLETED',9);
EOF
ins $TA $SA $OPENB 12 5000; ins $TA $SA $YOUNG 4 6000; recompute
REGISTRY_GRACE=1h run --compare "$WORK/snap3.json"; expect "open/young batch: transient, not failing" 0 "kind=missing class=transient"
sleep 2; run; expect "same COMPLETED batch after grace: persistent" 2 "kind=missing class=persistent .*batch=$YOUNG"
REGISTRY_PG_URL= run; expect "no REGISTRY_PG_URL: registry check skipped" 0 "registry-check skipped"
[ $FAILS -eq 0 ] && echo "ALL PASS" || echo "$FAILS FAILED"
exit $FAILS
