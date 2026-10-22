#!/usr/bin/env bash
# DPP-062 ClickHouse BACKUP/RESTORE test on THROWAWAY containers. DEV/CI ONLY.
# Usage: scripts/backup/ch-restore-test.sh [--execute]      (default: dry run, prints the plan)
# Plan: container A (clickhouse-server 24.8, random password, no published ports, backup path allow-listed) gets
# db/clickhouse/migrations applied and sample rows copied READ-ONLY from the dev stack's astryon_clickhouse
# (events_normalized_local) when it is running, else synthetic-free (empty tables, test reports it). BACKUP DATABASE
# to a File() target on a throwaway volume; container B (fresh, empty) mounts the same volume and RESTOREs.
# Compares count() and a cityHash64 aggregate per table, then removes containers and the volume.
# Backup target is a local file disk: the S3 (Ceph) backup destination and cross-site copy are NOT tested.
# Exit: 0 pass, 1 check failed, 64 usage, 77 refused.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
EXECUTE=0; [ "${1:-}" = "--execute" ] && EXECUTE=1
{ [ $# -eq 0 ] || [ "$EXECUTE" = 1 ]; } || { echo "usage: $0 [--execute]" >&2; exit 64; }
case "${DOCKER_HOST:-}" in ''|unix://*) ;; *) echo "refusing: non-local DOCKER_HOST" >&2; exit 77 ;; esac
A="astryon-bkptest-chA-$$"; B="astryon-bkptest-chB-$$"; V="astryon-bkptest-chvol-$$"
if [ "$EXECUTE" != 1 ]; then
  echo "DRY RUN. With --execute: start $A, apply CH migrations, copy sample rows from astryon_clickhouse (read-only, if up),"
  echo "BACKUP DATABASE astryon TO File('/backups/b1'), start empty $B on volume $V, RESTORE, compare count+hash, remove all."
  exit 0
fi
WORK="$(mktemp -d)"
trap 'docker rm -f "$A" "$B" >/dev/null 2>&1; docker volume rm "$V" >/dev/null 2>&1; rm -rf "$WORK"' EXIT
PW="$(openssl rand -hex 12)"
FAILS=0; ok() { echo "PASS $*"; }; bad() { echo "FAIL $*"; FAILS=$((FAILS+1)); }
cat >"$WORK/backups.xml" <<'XML'
<clickhouse><backups><allowed_path>/backups</allowed_path><allow_concurrent_backups>false</allow_concurrent_backups></backups></clickhouse>
XML
chmod 644 "$WORK/backups.xml"; chmod 755 "$WORK"
docker volume create "$V" >/dev/null
start() {  # start <name>
  docker run -d --name "$1" -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD="$PW" -v "$V:/backups" \
    -v "$WORK/backups.xml:/etc/clickhouse-server/config.d/backups.xml:ro" clickhouse/clickhouse-server:24.8 >/dev/null || return 1
  docker exec -u root "$1" chown clickhouse:clickhouse /backups 2>/dev/null
  for _ in $(seq 1 90); do docker exec "$1" clickhouse-client --password "$PW" -q 'select 1' >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}
chq() { local c="$1"; shift; docker exec -i "$c" clickhouse-client --password "$PW" "$@"; }
start "$A" || { echo "A did not start"; exit 1; }
chq "$A" -q 'CREATE DATABASE astryon' || exit 1
for f in db/clickhouse/migrations/*.up.sql; do
  sed -e 's/__ON_CLUSTER__//g' -e 's/__RMT__/MergeTree()/g' -e 's/__CLUSTER__/default/g' "$f" | chq "$A" -d astryon --multiquery >/dev/null \
    || { echo "migration failed: $f"; exit 1; }
done
COPIED=0
if docker inspect -f '{{.State.Running}}' astryon_clickhouse 2>/dev/null | grep -q true; then
  if docker exec astryon_clickhouse clickhouse-client -u "${CLICKHOUSE_USER:-astryon}" --password "${CLICKHOUSE_PASSWORD:-astryon_dev_only}" \
      -d astryon -q 'SELECT * FROM events_normalized_local FORMAT Native' 2>/dev/null \
      | chq "$A" -d astryon -q 'INSERT INTO events_normalized_local FORMAT Native'; then COPIED=1; fi
fi
hash_all() {  # count + order-independent hash per MergeTree table
  local c="$1" t out=""
  for t in $(chq "$c" -q "SELECT name FROM system.tables WHERE database='astryon' AND engine LIKE '%MergeTree' ORDER BY name"); do
    out+="$t=$(chq "$c" -d astryon -q "SELECT count(), sum(cityHash64(*)) FROM $t" | tr '\t' ':');"
  done; echo "$out"
}
SRC="$(hash_all "$A")"
ROWS="$(chq "$A" -d astryon -q 'SELECT count() FROM events_normalized_local')"
[ "$COPIED" = 1 ] && [ "$ROWS" -gt 0 ] && ok "source has $ROWS sample rows copied from dev" || echo "NOTE: no sample rows (dev ClickHouse not running or empty); restore of empty tables is a weak test"

t0=$(date +%s)
chq "$A" -q "BACKUP DATABASE astryon TO File('/backups/b1') SETTINGS id='bkp1'" >/dev/null && ok "BACKUP DATABASE finished" || bad "BACKUP failed"
BACKUP_S=$(( $(date +%s)-t0 ))
docker rm -f "$A" >/dev/null
start "$B" || { echo "B did not start"; exit 1; }
[ "$(chq "$B" -q "SELECT count() FROM system.databases WHERE name='astryon'")" = 0 ] && ok "restore target is clean (no astryon database)" || bad "restore target not clean"
t0=$(date +%s)
chq "$B" -q "RESTORE DATABASE astryon FROM File('/backups/b1')" >/dev/null && ok "RESTORE DATABASE finished" || bad "RESTORE failed"
RESTORE_S=$(( $(date +%s)-t0 ))
DST="$(hash_all "$B")"
[ -n "$SRC" ] && [ "$SRC" = "$DST" ] && ok "count+hash equal for all MergeTree tables" || bad "mismatch: src=[$SRC] dst=[$DST]"
echo "MEASURED backup_seconds=$BACKUP_S restore_seconds=$RESTORE_S rows=$ROWS (dev-size data; NOT an RTO); target=local File() (S3 leg untested)"
[ "$FAILS" = 0 ] && echo "RESULT pass" && exit 0
echo "RESULT FAIL ($FAILS)"; exit 1
