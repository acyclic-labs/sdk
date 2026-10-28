#!/usr/bin/env bash
# Caller input admission declares no comparison receipts or borrowed artifacts:
# this job qualifies only the uploaded source and locked packages.
set -euo pipefail
step=format
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: bash scripts/backends/compile-storage.sh" >&2; exit "$rc"' ERR
bun scripts/qualify-rustfmt.mjs
step=compile-storage-conformance
cargo check --locked -p acyclic-conformance --features storage-backends,acyclic-objects/local --bin storage-conformance
step=compile-postgres-checks
cargo check --locked -p acyclic-stream --no-default-features --features postgres,conformance --example provider-conformance
step=compile-shared-objects-regressions
cargo test --locked -p acyclic-objects --features local,s3 --lib --no-run
