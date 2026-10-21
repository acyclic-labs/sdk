#!/usr/bin/env bash
set -euo pipefail
cargo test --locked -p harness-mock-model --test script --test http
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-fuzz mock_model_script rust/crates/harness-fuzz/corpus/mock_model_script --no-default-features -- -runs=256
