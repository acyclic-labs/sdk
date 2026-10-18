#!/usr/bin/env bash
set -euo pipefail

# Each invocation is one build-slot job, never a cold build plus another suite.
phase=${1:-}
started=$SECONDS
models=
finish() {
  status=$?
  if [[ -n "$models" ]]; then rm -f "$models"; fi
  printf 'goal qualification: phase=%s elapsed=%ss status=%s\n' \
    "$phase" "$((SECONDS - started))" "$status"
}
trap finish EXIT
case "$phase" in
  models)
    models=$(mktemp)
    RUSTFLAGS="${RUSTFLAGS:-} --cfg acyclic_fs_loom --cfg acyclic_loom" \
      cargo test --locked -p acyclic-fs --lib --no-run --message-format=json > "$models"
    python3 scripts/qualify-graphcoder-artifact.py \
      distributed::append_loom_tests::a_new_gate_proposal_survives_every_publication_cut \
      distributed::append_loom_tests::concurrent_retries_publish_the_operation_once \
      distributed::append_loom_tests::reusing_the_operation_only_key_is_detected < "$models"
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
    echo 'usage: qualify-goal-scenarios.sh models|fuzz-build|fuzz-sweep <artifact>' >&2
    exit 2
    ;;
esac
