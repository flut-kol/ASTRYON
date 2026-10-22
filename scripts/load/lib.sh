# Shared helpers for scripts/load/*.sh (sourced). Reports go to $OUT (default reports/load/<UTC stamp>).
# Credentials: LOADGEN_TOKEN + LOADGEN_TENANT_ID or LOADGEN_TENANTS (never on the command line).
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
GATEWAY_URL=${GATEWAY_URL:-http://127.0.0.1:8080}
OUT=${OUT:-$ROOT/reports/load/$(date -u +%Y%m%dT%H%M%SZ)}
LOADGEN_BIN=${LOADGEN_BIN:-$OUT/.loadgen}
DEV_SCALE=${DEV_SCALE:-0.05}        # fraction of the real target rate to run on a dev stack
DURATION=${DURATION:-60s}
CONCURRENCY=${CONCURRENCY:-16}

die() { echo "load: $*" >&2; exit 2; }

preflight() {
  mkdir -p "$OUT"
  [ -n "${LOADGEN_TOKEN:-}${LOADGEN_TENANTS:-}" ] || die "set LOADGEN_TOKEN+LOADGEN_TENANT_ID or LOADGEN_TENANTS"
  # Refuse to produce a report when the gateway is not reachable: nothing would be measured.
  curl -fsS -m 3 -o /dev/null "$GATEWAY_URL/metrics" 2>/dev/null \
    || curl -sS -m 3 -o /dev/null "$GATEWAY_URL/v1/events/batch" 2>/dev/null \
    || die "gateway not reachable at $GATEWAY_URL; nothing measured (start the dev stack: scripts/dev/up.sh)"
  [ -x "$LOADGEN_BIN" ] || (cd "$ROOT/tests/load" && go build -o "$LOADGEN_BIN" .)
}

# scaled <rate>: integer events/s after DEV_SCALE (min 1)
scaled() { awk -v r="$1" -v s="$DEV_SCALE" 'BEGIN{v=int(r*s); if(v<1)v=1; print v}'; }

# lg <name> <loadgen args...>: runs loadgen writing $OUT/<name>.json; returns its exit code.
lg() {
  local name=$1; shift
  echo "== $name: loadgen $*" >&2
  "$LOADGEN_BIN" -url "$GATEWAY_URL/v1/events/batch" -concurrency "$CONCURRENCY" -json-out "$OUT/$name.json" "$@"
}

# env_note: writes host/scale context next to the reports, so a report is never read without it.
env_note() {
  jq -n --arg scale "$DEV_SCALE" --arg host "$(hostname)" --arg cpus "$(nproc)" --arg when "$(date -u +%FT%TZ)" \
    --arg commit "$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)" --arg gw "$GATEWAY_URL" \
    '{dev_scale:($scale|tonumber),host:$host,cpus:($cpus|tonumber),started_at:$when,commit:$commit,gateway:$gw,
      note:"dev-scale run; numbers are not production capacity"}' > "$OUT/environment.json"
}
