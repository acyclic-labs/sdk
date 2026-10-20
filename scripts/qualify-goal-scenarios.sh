#!/usr/bin/env bash
set -euo pipefail

# Each invocation is one build-slot job, never a cold build plus another suite.
phase=${1:-}
started=$SECONDS
artifacts=
finish() {
  status=$?
  if [[ -n "$artifacts" ]]; then rm -f "$artifacts"; fi
  printf 'goal qualification: phase=%s elapsed=%ss status=%s\n' \
    "$phase" "$((SECONDS - started))" "$status"
}
trap finish EXIT
case "$phase" in
  authority)
    # Main's atomic locators replace the former rejected-slot retry model.
    artifacts=$(mktemp)
    cargo test --locked -p acyclic-fs --lib --no-run --message-format=json > "$artifacts"
    python3 scripts/qualify-graphcoder-artifact.py \
      distributed::tests::operation_tests:: < "$artifacts"
    ;;
  fuzz-build)
    # Preserve build-slot's shared target directory across jobs and revisions.
    env -u RUSTFLAGS cargo +nightly-2026-07-01 fuzz build goal_scenario \
      --fuzz-dir rust/crates/harness-fuzz
    ;;
  fuzz-sweep)
    # Requiring the built artifact makes compilation impossible in this phase.
    if [[ $# != 2 || ! -x "$2" ]]; then
      echo 'fuzz-sweep requires the existing goal_scenario executable' >&2
      exit 2
    fi
    "$2" -runs=512 rust/crates/harness-fuzz/corpus/goal_scenario
    ;;
  *)
    echo 'usage: qualify-goal-scenarios.sh authority|fuzz-build|fuzz-sweep <artifact>' >&2
    exit 2
    ;;
esac
