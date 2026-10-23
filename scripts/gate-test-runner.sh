#!/bin/bash
# Cargo's --all-targets selects libFuzzer bins even when test=false. They have
# their own main, so libtest's --skip arguments cannot be passed to them.
set -euo pipefail
program=$1; shift
name=${program##*/}
if [ -n "${GATE_FUZZ_TEST_BIN_FILE:-}" ]; then
  while IFS= read -r bin; do
    case "$name" in
      "$bin"-*)
        # Ordinary integration tests can share a fuzz target's basename.
        # Require its linked libFuzzer driver before replacing libtest args.
        if ! symbols=$(nm --defined-only -- "$program"); then
          echo "error: GATE_RUNNER_INFRA cannot inspect $program" >&2
          exit 2
        fi
        if awk '$NF == "LLVMFuzzerTestOneInput" { found=1 } END { exit !found }' <<< "$symbols"; then
          echo "GATE_FUZZ_SMOKE bin=$bin runs=1 seed=1"
          set -- -runs=1 -seed=1 -max_total_time=10
        fi
        break;;
    esac
  done < "$GATE_FUZZ_TEST_BIN_FILE"
fi
if [ -n "${GATE_BASE_TEST_RUNNER:-}" ]; then
  exec "$GATE_BASE_TEST_RUNNER" "$program" "$@"
fi
exec "$program" "$@"
