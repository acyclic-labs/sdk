#!/bin/bash
# One payload and one cache boundary for type checks and consumer codegen.
set -uo pipefail
rc=0
node "${GATE_FEATURE_SCRIPT:-scripts/check-feature-matrix.mjs}" "$@" || rc=$?
# Keep each consumer's feature combinations isolated, under the same owned
# target lifetime. A successful type check cannot excuse failed code generation.
consumer_scope=()
if [ -f .gate-scope-mode ] && [ "$(tr -d '\r' < .gate-scope-mode)" = affected ]; then
  consumer_scope=(--scope-file=.gate-clippy-packages)
fi
node scripts/check-consumer-builds.mjs "${consumer_scope[@]}"; consumer_rc=$?
[ "$rc" != 0 ] || rc=$consumer_rc
exit "$rc"
