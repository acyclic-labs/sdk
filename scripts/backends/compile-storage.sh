#!/usr/bin/env bash
# Caller input admission declares no comparison receipts or borrowed artifacts:
# this job qualifies only the uploaded source and locked packages.
set -euo pipefail
step=format
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/compile-storage.sh" >&2; exit "$rc"' ERR
bash scripts/gate-rust-format.sh
step=compile-storage-conformance
cargo check --locked -p acyclic-conformance --features storage-backends,acyclic-objects/local --bin storage-conformance
step=compile-default-server-consumer
cargo check --locked -p graphcoder
step=compile-postgres-checks
cargo check --locked -p acyclic-stream --no-default-features --features postgres --example postgres-conformance
step=compile-shared-stream-regressions
cargo test --locked -p acyclic-stream --features local,postgres --lib --no-run
step=compile-shared-objects-regressions
cargo test --locked -p acyclic-objects --features local,s3 --lib --no-run
step=compile-agent-host-baseline
cargo test --locked -p acyclic-agent-host-conformance --features local --test local --no-run
step=compile-portable-host-fixtures
cargo check --locked --target wasm32-unknown-unknown -p acyclic-cloudflare-conformance -p acyclic-agent-host-conformance --features acyclic-agent-host-conformance/browser --tests
