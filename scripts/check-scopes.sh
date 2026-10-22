#!/usr/bin/env bash
# Fails if an "astryon:*" scope used in non-test Go code is missing from docs/architecture/roles.md.
set -euo pipefail
cd "$(dirname "$0")/.."
rc=0
for s in $(grep -rhoE '"astryon:[a-z_:.-]+"' --include=*.go --exclude='*_test.go' . | tr -d '"' | sort -u); do
  grep -qF "\`$s\`" docs/architecture/roles.md || { echo "scope not documented: $s" >&2; rc=1; }
done
exit $rc
