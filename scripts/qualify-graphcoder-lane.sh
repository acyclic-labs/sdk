#!/usr/bin/env bash
# One canonical build closure per touched crate; build time is outside E2E_WALL.
set -euo pipefail
artifact_args=()
case "${1:-}" in
  '') ;;
  --inventory) artifact_args=(--inventory) ;;
  *) echo 'STEP_FAILED qualification-mode; recovery: bash scripts/qualify-graphcoder-lane.sh --inventory' >&2; exit 2 ;;
esac
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
core_features=filesystem-local,native-execution,tui,test-support,grpc,search-http
cargo test --locked -p acyclic-harness-core --features "$core_features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" executor::tool_execution::cancellation_tests:: command_request::diff_tests:: native_settings::model::tests::
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" local::checkout_tools::capture::tests::
cargo test --locked -p acyclic-harness --features "$features" --test main --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" client_diff::
cargo test --locked -p acyclic-harness-search --features filesystem,search-http,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" config:: web::executor::tests::
cargo test --locked -p graphcoder --features "$binary_features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" args::tests::
cargo test --locked -p graphcoder --features "$binary_features" --test cli --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py "${artifact_args[@]}" a_prompt_runs_one_agent_turn_and_its_operation_replays_exactly jsonl_prompts_share_one_conversation_and_exit_once_settled catalog_errors_fail_through_the_binary
if [ ${#artifact_args[@]} -ne 0 ]; then exit 0; fi
cargo test --locked -p harness-mock-model --bin harness-mock-model recorder::tests::
bun install --cwd scripts/graphcoder-pty --frozen-lockfile
node --test scripts/graphcoder-e2e/*.test.mjs
cargo build --locked -p graphcoder -p harness-mock-model --features "$binary_features" --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
node scripts/qualify-web-lifecycle.mjs "$SESSIONS_GRAPHCODER_BINARY"
node scripts/qualify-fetch-cancellation.mjs "$SESSIONS_GRAPHCODER_BINARY"
python3 scripts/qualify-graphcoder-run.test.py
python3 scripts/qualify-remote-command.test.py
E2E_OUTPUT=e2e-consolidated-results python3 scripts/qualify-graphcoder-run.py .bb-sessions-shipping.jsonl
cargo check --locked -p graphcoder --target x86_64-pc-windows-gnu --features "$binary_features"
