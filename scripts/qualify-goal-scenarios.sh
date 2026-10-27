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
  *)
    echo 'usage: qualify-goal-scenarios.sh authority' >&2
    exit 2
    ;;
esac
