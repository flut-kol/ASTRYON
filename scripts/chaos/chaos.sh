#!/usr/bin/env bash
# DPP-061/063 dev-stack fault injection. DEV ONLY (local Docker, astryon_* containers).
# Usage: scripts/chaos/chaos.sh <scenario|all> [--execute]
#   scenarios: broker | clickhouse | postgres | s3 | flink-taskmanager | all
# Default is a DRY RUN that only prints what would happen. Every scenario runs `ch-aggregator reconcile` before and
# after the fault (via astryon/ch-aggregator:dev, built by `make aggregator-run` / scripts/dev/aggregator-run.sh).
# Env: CHAOS_DOWN_SECONDS (10), CHAOS_RECOVER_TIMEOUT (180). Result lines are the evidence to paste into
# docs/operations/runbooks/chaos-gamedays.md. This is a single-node dev stack: it proves restart behaviour only,
# not HA failover (no replicas, single broker/controller, s3mock instead of Ceph).
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CHAOS_TMP="$(mktemp -d)"; export CHAOS_TMP

scenario="${1:-}"; [ -n "$scenario" ] || { sed -n 2,10p "$0"; exit 64; }
shift; parse_args "$@"

flink_tm() {
  local c=astryon_processing label=flink-taskmanager
  guard_dev_only; guard_container "$c"
  if [ "$EXECUTE" != 1 ]; then
    say "DRY  [$label] reconcile baseline; pkill TaskManagerRunner in $c; wait; taskmanager.sh start; wait job RUNNING; reconcile"
    record "$label: dry-run only (not executed)"; return 0
  fi
  running "$c" || { record "$label: SKIPPED ($c not running)"; return 0; }
  local st=ok before after t=0 tm_before tm_after
  tmid() { docker exec "$c" curl -s localhost:8081/taskmanagers 2>/dev/null | grep -o '"id":"[^"]*"' | sort | tr '\n' ' '; }
  tm_before="$(tmid)"
  before="$(ch_count || echo '?')"; reconcile "$label-before"; local rcb=$RECONCILE_RC
  say "[$label] kill -9 TaskManager process inside $c"
  docker exec "$c" sh -c 'pkill -9 -f TaskManagerRunner || true'
  sleep "$DOWN_SECONDS"
  docker exec "$c" /opt/flink/bin/taskmanager.sh start >/dev/null 2>&1
  # recovered = job RUNNING again with a TaskManager id different from the killed one
  while [ "$t" -lt "$RECOVER_TIMEOUT" ]; do
    tm_after="$(tmid)"
    if [ -n "$tm_after" ] && [ "$tm_after" != "$tm_before" ] \
       && docker exec "$c" curl -s localhost:8081/jobs/overview 2>/dev/null | grep -q '"state":"RUNNING"'; then break; fi
    sleep 3; t=$((t+3))
  done
  [ "$t" -lt "$RECOVER_TIMEOUT" ] || st=NOT_RECOVERED
  after="$(ch_count || echo '?')"; reconcile "$label-after"
  reconcile_delta "$label" "$rcb" || st="$st,RECONCILE_CHANGED"
  record "$label: $st; down=${DOWN_SECONDS}s job_running_after=${t}s ch_rows $before->$after reconcile_rc $rcb->$RECONCILE_RC"
}

run() {
  case "$1" in
    broker)            kill_and_restore astryon_kafka broker ;;
    clickhouse)        kill_and_restore astryon_clickhouse clickhouse ;;
    postgres)          kill_and_restore astryon_postgres postgres ;;
    s3)                kill_and_restore astryon_s3mock s3 ;;
    flink-taskmanager) flink_tm ;;
    *) say "unknown scenario: $1"; exit 64 ;;
  esac
}

if [ "$scenario" = all ]; then for s in s3 flink-taskmanager clickhouse postgres broker; do run "$s"; done; else run "$scenario"; fi
summary
rm -rf "$CHAOS_TMP"
