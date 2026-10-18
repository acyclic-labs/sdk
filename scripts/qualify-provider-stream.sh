#!/usr/bin/env bash
# The parser properties and checked-in corpus run in the providers lib suite; keep the
# cargo-fuzz entry point compiling too. Run from the repository root on the build box.
set -euo pipefail
cargo check --locked --manifest-path rust/crates/harness-providers/fuzz/Cargo.toml --bin provider_stream
