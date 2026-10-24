#!/usr/bin/env bash
set -euo pipefail
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
cargo test --locked -p acyclic-harness-search --features filesystem,search-http,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py config:: files:: tools::
binary_features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
cargo test --locked -p graphcoder --features "$binary_features" --bin graphcoder args::tests
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-fuzz --features runtime search_request --target x86_64-unknown-linux-gnu --dev -- -runs=64 -max_len=4096 -timeout=2
