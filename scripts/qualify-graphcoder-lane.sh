#!/usr/bin/env bash
# One canonical build closure per touched crate; build time is outside E2E_WALL.
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
core_features=filesystem-local,native-execution,tui,test-support,grpc,search-http
cargo test --locked -p acyclic-harness-core --features "$core_features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py executor::tool_execution::cancellation_tests:: command_request::diff_tests:: native_settings::model::tests::
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py local::checkout_tools::capture::tests:: tui::render::tests::diff_errors_are_visible_and_replaceable tui::render::tests::compaction_completion_ tui::render::tests::negative_control_error_only_ client::input_files::shortcuts:: client::commands::session::consumption::tests:: local::compact::rewind::tests:: local::tests::compact::checkpoints_reject_stale_replay_pin_branches_and_continue_completed_tasks local::tests::headless::explicit_resume_retains_selection_and_clears_only_exit local::tests::tool_hooks::ask_guard_waits_for_exact_file_decision_before_dispatch local::tests::tool_hooks::approval_decisions_cannot_substitute_another_intent client::commands::session::approval::tests:: client::commands::session::archive::settings_tests::
cargo test --locked -p acyclic-harness --features "$features" --test main --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py file_query::diffs:: file_query::diff_tree:: client_diff::
cargo test --locked -p acyclic-harness-search --features filesystem,search-http,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py config:: files:: tools:: web::executor::tests::
cargo test --locked -p graphcoder --features "$binary_features" --bin graphcoder --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py args::tests view::diff::tests::
cargo test --locked -p graphcoder --features "$binary_features" --test tui --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py settlement_ three_prompts_
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
