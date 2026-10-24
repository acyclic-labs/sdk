#!/usr/bin/env bash
# Follow-up proof after the combined lane qualification; do not rebuild unchanged targets.
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py client::commands::session::archive::settings_tests:: tui::render::tests::compaction_completion_ tui::render::tests::negative_control_error_only_
bun install --cwd scripts/graphcoder-pty --frozen-lockfile
node --test scripts/graphcoder-e2e/*.test.mjs
cargo build --locked -p graphcoder -p harness-mock-model --features "$binary_features" --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" cargo test --locked -p acyclic-harness --features "$features,model-checking" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py client::commands::session::scroll_models::
python3 scripts/qualify-graphcoder-run.test.py
E2E_OUTPUT=e2e-followed-cut-results python3 scripts/qualify-graphcoder-run.py .bb-sessions-shipping.jsonl
