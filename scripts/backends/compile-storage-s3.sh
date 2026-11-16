#!/usr/bin/env bash
set -euo pipefail
step=format-storage-s3
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: bash scripts/backends/compile-storage-s3.sh" >&2; exit "$rc"' ERR
node scripts/qualify-rustfmt.mjs --write
step=compile-storage-s3
cargo check --locked -p acyclic-conformance --features storage-backends,s3-fixture --bins
