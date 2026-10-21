#!/usr/bin/env bash
set -euo pipefail

# Production libraries only: test/dev features must not mask missing helpers.
cargo check --locked -p acyclic-fs --lib --no-default-features --features memory
cargo check --locked -p acyclic-harness -p acyclic-harness-core -p acyclic-harness-search --lib --no-default-features --features filesystem
cargo check --locked -p acyclic-harness -p acyclic-harness-core -p acyclic-harness-search --lib --no-default-features --features filesystem-local
