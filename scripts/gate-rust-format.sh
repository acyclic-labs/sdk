#!/usr/bin/env bash
# Every gate uses the existing edition-aware formatter, including standalone crates.
set -u
started=$SECONDS
bun scripts/qualify-rustfmt.mjs "$@"
rc=$?
echo "GATE_RUSTFMT rc=$rc seconds=$((SECONDS-started)) scope=$*"
if [ "$rc" != 0 ]; then echo "GATE_DONE rc=$rc"; fi
exit "$rc"
