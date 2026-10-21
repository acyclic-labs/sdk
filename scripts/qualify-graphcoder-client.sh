#!/usr/bin/env bash
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http,model-checking
artifact_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
cargo test -p acyclic-harness --features "$features" --test client-query --test client-file-submit
cargo test -p acyclic-harness --features "$features" --test client-commands --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py client_command_session::
cargo test -p acyclic-harness --features "$features" --test context-discovery manifest::
cargo test -p acyclic-harness --features "$features" --test default_tools
cargo test -p acyclic-harness-core --features "$features" --test git_executor facade_commits_branches_merges_and_inspects_managed_files
cargo test -p acyclic-harness --features "$features" --test main saved_tool_metadata::
for target in command ui_prompt client_swarm_views; do
  cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-fuzz --features runtime "$target" --target x86_64-unknown-linux-gnu --dev -- -runs=64 -max_len=4096 -timeout=2
done
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" cargo test -p acyclic-harness-core --features "$features" --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py task_volume::files::models::
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" cargo test -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py local::checkout_tools::tests:: client::commands::session::init_models::

cargo test -p acyclic-harness-tui --features tui --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py term::session::tests:: term::tty::tests:: term::region::tests:: term::frame:: completion::

cargo test --locked -p acyclic-harness-tui --features tui --test main -- completion:: terminal:: terminal_regions:: terminal_frames:: terminal_frame_faults::
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-tui/fuzz completion_events --target x86_64-unknown-linux-gnu --dev -- -runs=128 -max_len=128 -timeout=5 -max_total_time=5

RUSTFLAGS="--cfg acyclic_harness_tui_loom" cargo test -p acyclic-harness-tui --features tui --lib --no-run --message-format=json |
python3 scripts/qualify-graphcoder-artifact.py term::frame::frame_model:: term::lifecycle::terminal_lifecycle_model:: term::console_owner::tests::input_observes_output_close term::console_owner::tests::negative_control_detects_input_without_close
