#!/usr/bin/env bash
# Retained scoped validation and root-only mutation control for transcript export.
set -euo pipefail
printf 'TRANSCRIPTS_SOURCE_SHA=%s\n' "$(git rev-parse HEAD)"
packages=(-p acyclic-harness-core -p acyclic-harness -p graphcoder)
features=acyclic-harness/test-support,acyclic-harness-core/test-support
cargo fmt "${packages[@]}" --check
cargo clippy --locked "${packages[@]}" --all-targets --features "$features" -- -D warnings
cargo nextest run --locked "${packages[@]}" --features "$features" \
  -E '(package(acyclic-harness-core) & (test(agent::) | test(agent_model::) | test(agent_control_mapping::))) | (package(acyclic-harness) & test(local::tests::)) | package(graphcoder)'

source_file=rust/crates/harness/src/local/agent_conversation/transcripts.rs
original=$(mktemp)
log=$(mktemp)
cp "$source_file" "$original"
trap 'cp "$original" "$source_file"; rm -f "$original" "$log"' EXIT
statement='add(&jobs, selected, directory, operation, finish.clone()).await?;'
test "$(rg -F -c "$statement" "$source_file")" = 1
sed -i 's/add(\&jobs, selected, directory, operation, finish.clone()).await?;/drop(selected);/' "$source_file"
set +e
cargo nextest run --locked "${packages[@]}" --lib --features "$features" \
  -E 'test(=local::tests::transcripts::transcripts_follow_two_spawn_levels_and_match_original_journals)' >"$log" 2>&1
status=$?
set -e
cat "$log"
test "$status" = 100
rg -F 'every spawned agent needs a transcript' "$log"
printf 'TRANSCRIPTS_NEGATIVE_CONTROL=PASS (root-only mutation rejected)\n'
