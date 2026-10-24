#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Dedicated concurrency closure: only crate-local cfgs select modeled locks.
# The same built library target also covers the pure pending-owner properties.
features=terminal,filesystem-local,native-execution,grpc,test-support,search-http,model-checking
RUSTFLAGS="--cfg acyclic_harness_loom --cfg acyclic_loom" \
  cargo +nightly-2026-07-01 test --locked -p acyclic-harness \
    --no-default-features --features "$features" --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py \
    client::file_submit::model:: client::file_submit::pending::tests::
