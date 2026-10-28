#!/usr/bin/env bash
set -euo pipefail
step=format-storage-s3
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/compile-storage-s3.sh" >&2; exit "$rc"' ERR
rustfmt --edition 2024 rust/crates/objects/src/s3.rs rust/crates/conformance/src/bin/storage-conformance.rs rust/crates/conformance/src/bin/storage_conformance/{s3,observed_store,batch,host,endpoint}.rs
step=compile-storage-s3
cargo check --locked -p acyclic-conformance --features storage-backends,s3-fixture --bins
