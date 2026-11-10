#!/usr/bin/env bash
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
artifact_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
cargo test -p graphcoder --bin graphcoder
cargo test -p graphcoder --test cli
cargo test -p acyclic-harness-tui --test main screen::
# Retain Cargo's exact native artifact across the separate instrumented build.
native_artifact=$(mktemp "$artifact_dir/graphcoder-native.XXXXXX.jsonl")
core_artifact=$(mktemp "$artifact_dir/graphcoder-core.XXXXXX.jsonl")
trap 'rm -f "$native_artifact" "$core_artifact"' EXIT
native_features=test-support
core_features=test-support
cargo test -p acyclic-harness --features "$native_features" --lib --no-run --message-format=json > "$native_artifact"
cargo test -p acyclic-harness-core --features "$core_features" --lib --no-run --message-format=json > "$core_artifact"
python3 scripts/qualify-graphcoder-artifact.py \
    contract::errors::tests:: executor::tests::invocation_bounds:: \
    tools::git::view::tests:: < "$core_artifact"
python3 scripts/qualify-graphcoder-artifact.py \
    local::tests:: < "$native_artifact"

bun install --cwd scripts/graphcoder-pty --frozen-lockfile --ignore-scripts
node --test scripts/graphcoder-pty/screen.test.mjs

cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .sessions-shipping.jsonl > .sessions-binaries.env
source .sessions-binaries.env
for case in pty-conversation pty-markdown pty-slash pty-cancel-ctrl-c \
    pty-cancel-preparation kill-during-count pty-kill-and-resume pty-quit-resume provider-error; do
    node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" "$case"
done

bash scripts/qualify-graphcoder-client.sh
