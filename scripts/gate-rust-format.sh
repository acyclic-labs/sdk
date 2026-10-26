#!/usr/bin/env bash
# Every gate uses the existing edition-aware formatter, including standalone crates.
set -u
stage=0
if [ "${1:-}" = --stage ]; then stage=1; shift; fi
started=$SECONDS
bun scripts/qualify-rustfmt.mjs "$@"
rc=$?
echo "GATE_RUSTFMT rc=$rc seconds=$((SECONDS-started)) scope=$*"
# A parent gate owns its terminal verdict; standalone failures own theirs.
if [ "$rc" != 0 ] && [ "$stage" = 0 ]; then echo "GATE_DONE rc=$rc"; fi
exit "$rc"
