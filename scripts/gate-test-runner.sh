#!/bin/bash
# Cargo's --all-targets selects libFuzzer bins even when test=false. They have
# their own main, so libtest's --skip arguments cannot be passed to them.
set -eu
program=$1; shift
name=${program##*/}
if [ -n "${GATE_FUZZ_TEST_BIN_FILE:-}" ]; then
  while IFS= read -r bin; do
    case "$name" in
      "$bin"-*)
        echo "GATE_FUZZ_SMOKE bin=$bin runs=1 seed=1"
        set -- -runs=1 -seed=1 -max_total_time=10
        break;;
    esac
  done < "$GATE_FUZZ_TEST_BIN_FILE"
fi
if [ -n "${GATE_BASE_TEST_RUNNER:-}" ]; then
  exec "$GATE_BASE_TEST_RUNNER" "$program" "$@"
fi
exec "$program" "$@"
