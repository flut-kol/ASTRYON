#!/usr/bin/env bash
# DPP-056: run each representative workload profile at one fixed rate (no scaling of the profile itself).
#   RATE=500 DURATION=30s scripts/load/profiles.sh [profile ...]
# Multi-tenant profiles (skewed-tenants) need LOADGEN_TENANTS with several tenants.
. "$(dirname "$0")/lib.sh"
RATE=${RATE:-500}
preflight; env_note
profiles=("$@"); [ ${#profiles[@]} -gt 0 ] || profiles=(small-json wide-json skewed-tenants duplicates late-events invalid-schemas)
rc=0
for p in "${profiles[@]}"; do
  lg "profile-$p" -profile "$p" -rate "$RATE" -duration "$DURATION" -ids-out "$OUT/profile-$p.ids" -enforce || rc=1
done
echo "reports in $OUT"; exit $rc
