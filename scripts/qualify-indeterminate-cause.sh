#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
features=filesystem-local,native-execution,terminal,tui,grpc,test-support,search-http
cargo test --locked -p acyclic-harness --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py \
    scheduler::tests::indeterminate:: \
    filesystem::task_runtime::tests::worker::indeterminate_command_ \
    durable_host::observations::tests:: \
    runtime::tests::tasks::indeterminate_live_join_
# The shared records target covers outcomes, completion events and snapshots.
cargo +nightly-2026-07-01 fuzz run --dev --fuzz-dir rust/crates/harness-fuzz \
  records --target x86_64-unknown-linux-gnu -- -runs=64 -max_len=4096 -timeout=2
# Public outcome changes must compile the shipping Pi recovery caller too.
qualified="acyclic-harness/${features//,/,acyclic-harness/}"
cargo check --locked -p acyclic-plugin --bin acyclic --tests --features "$qualified"
cargo check --locked -p acyclic-plugin --bin acyclic --tests --features "$qualified" \
  --target x86_64-pc-windows-gnu
