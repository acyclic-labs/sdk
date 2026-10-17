#!/usr/bin/env bash
set -euo pipefail
artifact_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test client_session --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py documents:: views:: jsonl_tests:: admissions:: recovery:: files::
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test client-commands --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py client_command_session::
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test context-discovery manifest::
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test default_tools
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test git_executor facade_commits_branches_merges_and_inspects_managed_files
cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http --test main saved_tool_metadata::
cargo build -p acyclic-harness-fuzz --features runtime --bin ui_prompt --bin client_recovery
timeout 8 "$artifact_dir/debug/ui_prompt" rust/crates/harness-fuzz/corpus/ui_prompt -runs=64 -max_len=4096
timeout 8 "$artifact_dir/debug/client_recovery" rust/crates/harness-fuzz/corpus/client_recovery -runs=32 -max_len=4096
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" cargo test -p acyclic-harness --features filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http,model-checking --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py task_volume::files::models:: local::checkout_tools::tests:: client::commands::session::init_models::

cargo test -p acyclic-harness-tui --features tui --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py term::session::tests:: term::tty::tests::

RUSTFLAGS="--cfg acyclic_harness_tui_loom" cargo test -p acyclic-harness-tui --features tui --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py term::lifecycle::terminal_lifecycle_model:: term::console_owner::tests::input_observes_output_close term::console_owner::tests::negative_control_detects_input_without_close
