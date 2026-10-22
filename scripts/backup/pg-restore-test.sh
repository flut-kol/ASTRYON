#!/usr/bin/env bash
# DPP-062 PostgreSQL restore test with pgBackRest against a THROWAWAY container. DEV/CI ONLY.
# Usage: scripts/backup/pg-restore-test.sh [--execute]      (default: dry run, prints the plan)
# What it does (all in one disposable postgres:16-alpine container, no published ports, random password, removed on exit):
#   1. installs pgbackrest (apk; needs network), applies db/postgres/migrations/*.up.sql + dev seed
#   2. configures stanza `astryon` like infra/postgres/pgbackrest.conf.tmpl (same stanza, retention, archive-async,
#      process-max) EXCEPT repo1-type=posix instead of s3 (no real S3 repo in dev; the S3 leg is NOT tested here)
#   3. full backup, then writes a marker row and switches WAL (post-backup change that must survive via WAL replay)
#   4. restores into a NEW data directory, starts it on another socket/port, replays WAL, and compares per-table row
#      counts + RLS-enabled table count with the source; prints elapsed backup/restore seconds.
# It never connects to the dev stack, to any compose project, or to any non-local Docker host.
# Exit: 0 all checks equal, 1 check failed, 64 usage, 77 refused.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
EXECUTE=0; [ "${1:-}" = "--execute" ] && EXECUTE=1
[ $# -le 1 ] && { [ "$EXECUTE" = 1 ] || [ $# -eq 0 ]; } || { echo "usage: $0 [--execute]" >&2; exit 64; }
case "${DOCKER_HOST:-}" in ''|unix://*) ;; *) echo "refusing: non-local DOCKER_HOST" >&2; exit 77 ;; esac
C="astryon-bkptest-pg-$$"
if [ "$EXECUTE" != 1 ]; then
  cat <<PLAN
DRY RUN. With --execute this would: docker run throwaway $C (postgres:16-alpine + pgbackrest), apply migrations,
pgbackrest stanza-create + full backup, marker insert + WAL switch, restore to /restore, start, compare counts, docker rm -f $C.
PLAN
  exit 0
fi
trap 'docker rm -f "$C" >/dev/null 2>&1' EXIT
PW="$(openssl rand -hex 12)"
now() { date +%s; }
pgx() { docker exec -u postgres "$C" "$@"; }
FAILS=0; ok() { echo "PASS $*"; }; bad() { echo "FAIL $*"; FAILS=$((FAILS+1)); }

docker run -d --name "$C" -e POSTGRES_PASSWORD="$PW" -e POSTGRES_DB=astryon --entrypoint sh postgres:16-alpine -c '
  apk add --no-cache pgbackrest >/dev/null 2>&1 || exit 1
  mkdir -p /var/lib/pgbackrest /restore /tmp/rsock /var/log/pgbackrest /etc/pgbackrest
  chown postgres:postgres /var/lib/pgbackrest /restore /tmp/rsock /var/log/pgbackrest
  chmod 700 /restore
  cat >/etc/pgbackrest/pgbackrest.conf <<CONF
[global]
repo1-type=posix
repo1-path=/var/lib/pgbackrest
repo1-retention-full=2
repo1-retention-diff=7
archive-async=y
process-max=4
spool-path=/var/spool/pgbackrest
log-path=/var/log/pgbackrest
[astryon]
pg1-path=/var/lib/postgresql/data
CONF
  mkdir -p /var/spool/pgbackrest; chown postgres:postgres /var/spool/pgbackrest
  exec docker-entrypoint.sh postgres -c wal_level=replica -c archive_mode=on -c archive_timeout=30 \
    -c "archive_command=pgbackrest --stanza=astryon archive-push %p"' >/dev/null || exit 1
for _ in $(seq 1 120); do  # the image starts a temporary init server first; wait for the final one
  docker logs "$C" 2>&1 | grep -q 'init process complete' && pgx psql -d astryon -qtAc 'select 1' >/dev/null 2>&1 && break; sleep 1
done
pgx pgbackrest version >/dev/null 2>&1 || { echo "pgbackrest not installed (no network for apk?)"; exit 1; }

for f in db/postgres/migrations/*.up.sql db/seeds/0001_dev_tenant.sql; do
  docker exec -i -u postgres "$C" psql -d astryon -v ON_ERROR_STOP=1 -q <"$f" >/dev/null || { echo "apply failed: $f"; exit 1; }
done
SNAP='select string_agg(relname||'"'"'='"'"'||(xpath('"'"'/row/c/text()'"'"', query_to_xml(format('"'"'select count(*) as c from %I'"'"', relname), false, true, '"'"''"'"')))[1]::text, '"'"','"'"' order by relname) from pg_class where relnamespace='"'"'public'"'"'::regnamespace and relkind='"'"'r'"'"''
RLS='select count(*) from pg_class where relnamespace='"'"'public'"'"'::regnamespace and relkind='"'"'r'"'"' and relrowsecurity'

pgx pgbackrest --stanza=astryon stanza-create >/dev/null || { echo "stanza-create failed"; exit 1; }
t0=$(now); pgx pgbackrest --stanza=astryon --type=full backup >/dev/null || { echo "backup failed"; exit 1; }
BACKUP_S=$(( $(now)-t0 ))
pgx psql -d astryon -qc "create table if not exists restore_marker(id int primary key, at timestamptz default now()); insert into restore_marker(id) values (1)" >/dev/null
pgx psql -d astryon -qc 'select pg_switch_wal()' >/dev/null
for _ in $(seq 1 60); do  # wait until the switched segment is archived
  [ "$(pgx psql -d astryon -qtAc 'select last_archived_wal is not null and last_archived_time > now() - interval '"'"'60 seconds'"'"' from pg_stat_archiver')" = t ] && break; sleep 1
done
sleep 5
SRC_SNAP="$(pgx psql -d astryon -qtAc "$SNAP")"; SRC_RLS="$(pgx psql -d astryon -qtAc "$RLS")"

t0=$(now)
pgx pgbackrest --stanza=astryon --pg1-path=/restore restore >/dev/null || { echo "restore failed"; exit 1; }
pgx pg_ctl -D /restore -w -t 120 -o "-p 5433 -c unix_socket_directories=/tmp/rsock -c archive_mode=off -c listen_addresses=''" start >/dev/null || { echo "restored cluster did not start"; exit 1; }
for _ in $(seq 1 60); do [ "$(pgx psql -h /tmp/rsock -p 5433 -d astryon -qtAc 'select pg_is_in_recovery()' 2>/dev/null)" = f ] && break; sleep 1; done
RESTORE_S=$(( $(now)-t0 ))
DST_SNAP="$(pgx psql -h /tmp/rsock -p 5433 -d astryon -qtAc "$SNAP")"; DST_RLS="$(pgx psql -h /tmp/rsock -p 5433 -d astryon -qtAc "$RLS")"
MARK="$(pgx psql -h /tmp/rsock -p 5433 -d astryon -qtAc 'select count(*) from restore_marker')"

[ -n "$SRC_SNAP" ] && [ "$SRC_SNAP" = "$DST_SNAP" ] && ok "per-table row counts equal ($(echo "$SRC_SNAP" | tr ',' '\n' | wc -l) tables)" || bad "row counts differ: src=[$SRC_SNAP] dst=[$DST_SNAP]"
[ "$SRC_RLS" = "$DST_RLS" ] && [ "${SRC_RLS:-0}" -gt 0 ] && ok "RLS-enabled tables equal ($SRC_RLS)" || bad "RLS tables src=$SRC_RLS dst=$DST_RLS"
[ "$MARK" = 1 ] && ok "post-backup marker row recovered via WAL replay" || bad "marker row missing after restore (WAL replay gap)"
echo "MEASURED backup_seconds=$BACKUP_S restore_seconds=$RESTORE_S (tiny dev dataset; NOT an RTO) repo=posix (S3 leg untested)"
pgx pgbackrest --stanza=astryon info 2>/dev/null | sed -n 1,12p
[ "$FAILS" = 0 ] && echo "RESULT pass" && exit 0
echo "RESULT FAIL ($FAILS)"; exit 1
