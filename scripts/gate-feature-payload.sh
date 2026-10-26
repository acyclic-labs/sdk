#!/bin/bash
# One payload and one cache boundary for type checks and consumer codegen.
set -uo pipefail
rc=0
affected=0
if [ -f .gate-scope-mode ] && [ "$(tr -d '\r' < .gate-scope-mode)" = affected ]; then affected=1; fi
# A PR checks its affected crates; main (full scope) checks every crate. An
# explicit --package selection (the Windows caller) is already scoped.
matrix_scope=()
if [ "$affected" = 1 ] && ! printf '%s\n' "$@" | grep -q '^--package='; then
  while IFS= read -r package || [ -n "$package" ]; do
    package=${package%$'\r'}
    [ -z "$package" ] || matrix_scope+=("--package=$package")
  done < .gate-clippy-packages
fi
if [ "$affected" = 1 ] && [ ${#matrix_scope[@]} = 0 ] && ! printf '%s\n' "$@" | grep -q '^--package='; then
  echo 'feature matrix: 0/0 sets ok (no affected crates)'
else
  node "${GATE_FEATURE_SCRIPT:-scripts/check-feature-matrix.mjs}" "$@" "${matrix_scope[@]}" || rc=$?
fi
# Host code generation belongs to the native matrix. The Windows matrix
# already checks its own cross-target libraries and targets; invoking the host
# consumer generator there only repeats the same native builds in a cold root.
for argument in "$@"; do
  if [ "$argument" = --windows-msvc ]; then exit "$rc"; fi
done
# Keep each consumer's feature combinations isolated, under the same owned
# target lifetime. A successful type check cannot excuse failed code generation.
consumer_scope=()
if [ "$affected" = 1 ]; then
  consumer_scope=(--scope-file=.gate-clippy-packages)
fi
node scripts/check-consumer-builds.mjs --workers="$1" "${consumer_scope[@]}"; consumer_rc=$?
[ "$rc" != 0 ] || rc=$consumer_rc
exit "$rc"
