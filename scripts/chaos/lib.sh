#!/usr/bin/env bash
# Shared helpers for scripts/chaos/*. DEV ONLY. Sourced, not executed.
# Safety: dry-run unless --execute is passed; only local-socket Docker; only containers named astryon_* that carry the
# compose label project=astryon-dev or astryon-extras. Nothing here can address a remote host.

CHAOS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXECUTE=0
DOWN_SECONDS="${CHAOS_DOWN_SECONDS:-10}"     # how long the victim stays down
RECOVER_TIMEOUT="${CHAOS_RECOVER_TIMEOUT:-180}"
NET="${ASTRYON_DEV_NET:-astryon-dev_default}"
CH_USER="${CLICKHOUSE_USER:-astryon}"; CH_PASS="${CLICKHOUSE_PASSWORD:-astryon_dev_only}"; CH_DB="${CLICKHOUSE_DB:-astryon}"
RESULTS=()

say() { printf '[chaos] %s\n' "$*"; }
act() {  # act <description> <cmd...>: print in dry-run, run with --execute
  if [ "$EXECUTE" = 1 ]; then say "RUN  $1"; shift; "$@"; else say "DRY  $1 (would run: $*)"; fi
}

parse_args() {
  for a in "$@"; do
    case "$a" in
      --execute) EXECUTE=1 ;;
      --dry-run) EXECUTE=0 ;;
      *) say "unknown argument: $a"; exit 64 ;;
    esac
  done
}

guard_dev_only() {
  local host="${DOCKER_HOST:-}"
  case "$host" in ''|unix://*) ;; *) say "refusing: DOCKER_HOST=$host is not a local socket"; exit 77 ;; esac
  local ctx; ctx="$(docker context show 2>/dev/null || echo default)"
  [ "$ctx" = default ] || { say "refusing: docker context '$ctx' is not 'default'"; exit 77; }
}

# guard_container <name>: must be astryon_* and belong to a dev compose project.
guard_container() {
  local c="$1" proj
  case "$c" in astryon_*) ;; *) say "refusing: $c is not an astryon_* dev container"; exit 77 ;; esac
  proj="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$c" 2>/dev/null || true)"
  case "$proj" in astryon-dev|astryon-extras) ;; *) say "refusing: $c compose project '$proj' is not astryon-dev/astryon-extras"; exit 77 ;; esac
}

running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }

wait_healthy() {  # wait_healthy <container>
  local c="$1" t=0 h
  while [ "$t" -lt "$RECOVER_TIMEOUT" ]; do
    h="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$c" 2>/dev/null)"
    case "$h" in healthy|running) say "$c $h after ${t}s"; RECOVERY_SECONDS=$t; return 0 ;; esac
    sleep 2; t=$((t+2))
  done
  say "$c NOT healthy after ${RECOVER_TIMEOUT}s"; return 1
}

ch_count() {
  docker exec astryon_clickhouse clickhouse-client -u "$CH_USER" --password "$CH_PASS" -d "$CH_DB" \
    -q 'SELECT count() FROM events_normalized_local' 2>/dev/null
}

# reconcile <label>: ch-aggregator reconcile on the dev network. Exit 0 clean, 2 findings, other = check failed.
reconcile() {
  local label="$1" img=astryon/ch-aggregator:dev rc
  if [ "$EXECUTE" != 1 ]; then say "DRY  reconcile ($label): ch-aggregator reconcile via $img on $NET"; return 0; fi
  if docker image inspect "$img" >/dev/null 2>&1; then
    docker run --rm --network "$NET" \
      -e CH_URL=http://clickhouse:8123 -e CH_DATABASE="$CH_DB" -e CH_USER="$CH_USER" -e CH_PASSWORD="$CH_PASS" \
      -e REGISTRY_PG_URL="postgres://${POSTGRES_USER:-astryon}:${POSTGRES_PASSWORD:-astryon_dev_only}@postgres:5432/${POSTGRES_DB:-astryon}" \
      "$img" reconcile >"${CHAOS_TMP:-/tmp}/reconcile-$label.txt" 2>&1
  else
    "$CHAOS_ROOT/scripts/dev/aggregator-run.sh" reconcile >"${CHAOS_TMP:-/tmp}/reconcile-$label.txt" 2>&1
  fi
  rc=$?
  say "reconcile[$label] exit=$rc (0 clean, 2 findings); output: ${CHAOS_TMP:-/tmp}/reconcile-$label.txt"
  RECONCILE_RC=$rc
  return 0
}

# reconcile_delta <label> <rc_before>: ok only if the post-fault findings equal the pre-fault findings (a dev stack
# may carry pre-existing findings; the fault must not add or remove any) and the check itself did not fail.
reconcile_delta() {
  local f="${CHAOS_TMP:-/tmp}/reconcile"
  [ "$RECONCILE_RC" = 0 ] || [ "$RECONCILE_RC" = 2 ] || return 1
  diff -q <(sort "$f-$1-before.txt") <(sort "$f-$1-after.txt") >/dev/null
}

record() { RESULTS+=("$1"); }
summary() {
  say "---- summary (execute=$EXECUTE) ----"
  local r; for r in "${RESULTS[@]}"; do say "$r"; done
}

# kill_and_restore <container> <label>: SIGKILL, keep down DOWN_SECONDS, start, wait healthy, reconcile.
kill_and_restore() {
  local c="$1" label="$2" before after status=ok
  guard_dev_only; guard_container "$c"
  if [ "$EXECUTE" != 1 ]; then
    say "DRY  [$label] reconcile baseline; docker kill $c; sleep $DOWN_SECONDS; docker start $c; wait healthy; reconcile"
    record "$label: dry-run only (not executed)"; return 0
  fi
  running "$c" || { say "$c is not running; start the dev stack first (make up)"; record "$label: SKIPPED ($c not running)"; return 0; }
  before="$(ch_count || echo '?')"
  reconcile "$label-before"; local rc_before=$RECONCILE_RC
  say "[$label] SIGKILL $c"; docker kill "$c" >/dev/null
  sleep "$DOWN_SECONDS"
  docker start "$c" >/dev/null
  RECOVERY_SECONDS=-1
  wait_healthy "$c" || status=NOT_RECOVERED
  after="$(ch_count || echo '?')"
  reconcile "$label-after"
  reconcile_delta "$label" "$rc_before" || status="$status,RECONCILE_CHANGED"
  record "$label: $status; down=${DOWN_SECONDS}s recovery=${RECOVERY_SECONDS}s ch_rows $before->$after reconcile_rc $rc_before->$RECONCILE_RC"
}
