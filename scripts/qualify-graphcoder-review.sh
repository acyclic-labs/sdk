#!/usr/bin/env bash
# Scoped follow-up to the combined lane proofs; build time is outside E2E_WALL.
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p acyclic-fs --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py workspace::tests::change_sets:: workspace::tests::content_equality:: workspace::read_core::tests::
cargo test --locked -p acyclic-harness --features "$features" --test main --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py file_query::diff_tree:: file_query::diffs:: client_diff::
python3 scripts/qualify-graphcoder-run.test.py
bun install --cwd scripts/graphcoder-pty --frozen-lockfile
node --test scripts/graphcoder-e2e/*.test.mjs
cargo build --locked -p graphcoder -p harness-mock-model --features "$binary_features" --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
cargo check --locked -p graphcoder --target x86_64-pc-windows-gnu --features "$binary_features"
E2E_OUTPUT=e2e-review-results python3 scripts/qualify-graphcoder-run.py .bb-sessions-shipping.jsonl
