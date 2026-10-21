#!/usr/bin/env bash
set -euo pipefail
cargo test --locked -p acyclic-harness-providers --lib tests::files
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-providers/fuzz native_image rust/crates/harness-providers/fuzz/corpus/native_image --target x86_64-unknown-linux-gnu --dev -- -runs=256 -max_len=4096
