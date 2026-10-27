#!/usr/bin/env bash
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
cargo test --locked -p acyclic-harness-core --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py command_request::diff_tests::
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py tui::render::tests::diff_errors_are_visible_and_replaceable client::input_files::shortcuts:: client::commands::session::consumption::tests:: local::compact::rewind::tests:: local::tests::compact::checkpoints_reject_stale_replay_pin_branches_and_continue_completed_tasks local::tests::headless::explicit_resume_retains_selection_and_clears_only_exit
cargo test --locked -p acyclic-harness --features "$features" --test main --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py file_query::diffs:: file_query::diff_tree:: client_diff::
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p graphcoder --features "$binary_features" --bin graphcoder --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py args::tests view::diff::tests::
cargo test --locked -p graphcoder --features "$binary_features" --test tui --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py settlement_ three_prompts_
node --test scripts/graphcoder-e2e/*.test.mjs
