#!/usr/bin/env bash
set -euo pipefail
artifact_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
cargo test -p graphcoder --features acyclic-harness/grpc,acyclic-harness/search-http --bin graphcoder
cargo test -p graphcoder --features acyclic-harness/grpc,acyclic-harness/search-http --test cli
cargo test -p acyclic-harness-tui --features tui --test main screen::
# Retain Cargo's exact native artifact across the separate instrumented build.
native_artifact=$(mktemp "$artifact_dir/graphcoder-native.XXXXXX.jsonl")
core_artifact=$(mktemp "$artifact_dir/graphcoder-core.XXXXXX.jsonl")
trap 'rm -f "$native_artifact" "$core_artifact"' EXIT
native_features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
cargo test -p acyclic-harness --features "$native_features" --lib --no-run --message-format=json > "$native_artifact"
cargo test -p acyclic-harness-core --features "$native_features" --lib --no-run --message-format=json > "$core_artifact"
python3 scripts/qualify-graphcoder-artifact.py \
    contract::errors::tests:: task_volume::schema::tests:: task_volume::files:: task_volume::segment::tests:: executor::tests::invocation_bounds:: \
    tools::git::view::tests:: < "$core_artifact"
python3 scripts/qualify-graphcoder-artifact.py \
    client::local_headless::tests:: client::task_files::tests:: local::tests::task_files:: local::tests::documents:: local::tests::command_files:: local::tests::headless:: local::tests::live_ui:: client::commands::scheduling::owner::tests:: \
    local::checkout_tools::publisher::tests:: < "$native_artifact"

bun install --cwd scripts/graphcoder-pty --frozen-lockfile --ignore-scripts
node --test scripts/graphcoder-pty/screen.test.mjs

node scripts/test-graphcoder.mjs "$artifact_dir/debug/graphcoder"
node scripts/test-graphcoder-resume.mjs "$artifact_dir/debug/graphcoder"
node scripts/test-graphcoder-resume.mjs "$artifact_dir/debug/graphcoder" interactive
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder"
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder" command-recovery
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder" uncertain
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder" typeahead
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder" cancel
node scripts/test-graphcoder-pty.mjs "$artifact_dir/debug/graphcoder" cancel-preparation

bash scripts/qualify-graphcoder-client.sh

# Dependency integration gates use the same already-built native binary.
python3 scripts/qualify-graphcoder-artifact.py \
    local::tests::checkout:: local::tests::volume_tools:: local::tests::volumes:: \
    local::tests::checkout_reopen:: local::tests::checkout_reconciliation:: \
    local::tests::reconciliation_interruptions:: < "$native_artifact"
