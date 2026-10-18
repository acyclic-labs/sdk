#!/usr/bin/env bash
set -euo pipefail

# Keep the exact model artifact and reject missing tests, including the control.
models=$(mktemp)
trap 'rm -f "$models"' EXIT
# Instrument the provider locks used by the authority's real publication path.
RUSTFLAGS="${RUSTFLAGS:-} --cfg acyclic_fs_loom --cfg acyclic_loom" \
  cargo test --locked -p acyclic-fs --lib --no-run --message-format=json > "$models"
python3 scripts/qualify-graphcoder-artifact.py \
  distributed::append_loom_tests::a_new_gate_proposal_survives_every_publication_cut \
  distributed::append_loom_tests::concurrent_retries_publish_the_operation_once \
  distributed::append_loom_tests::reusing_the_operation_only_key_is_detected < "$models"

# The e2e properties and this target compile the same scenario codec.
env -u RUSTFLAGS cargo +nightly-2026-07-01 fuzz run goal_scenario \
  --fuzz-dir rust/crates/harness-fuzz -- -runs=512
