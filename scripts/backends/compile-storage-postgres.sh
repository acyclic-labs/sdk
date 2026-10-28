#!/usr/bin/env bash
set -euo pipefail
step=format-storage-postgres
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/compile-storage-postgres.sh" >&2; exit "$rc"' ERR
rustfmt --edition 2024 rust/crates/stream/src/postgres.rs rust/crates/native-runtime/src/linux/{mod,backend,backend_tests}.rs rust/crates/native-runtime/src/workers/job.rs
step=compile-storage-postgres
cargo check --locked -p acyclic-conformance --features storage-backends --bins
step=compile-native-batch-oracles
cargo check --locked -p acyclic-native-runtime --tests
