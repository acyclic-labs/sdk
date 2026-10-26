#!/usr/bin/env bash
# Caller input admission declares no comparison receipts or borrowed artifacts:
# this job qualifies only the uploaded source and locked packages.
set -euo pipefail
step=format
trap 'rc=$?; echo "step=$step failed (exit $rc); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/compile-storage.sh" >&2; exit "$rc"' ERR
rustfmt --edition 2024 --config skip_children=true \
    rust/crates/objects/src/v1/s3.rs \
    rust/crates/objects/src/body.rs \
    rust/crates/objects/src/body/remote.rs \
    rust/crates/objects/src/v1/memory/mod.rs \
    rust/crates/objects/src/v1/memory/state.rs \
    rust/crates/objects/src/v1/memory/remote.rs \
    rust/crates/objects/src/v1/memory/durable/*.rs \
    rust/crates/objects/src/v1/memory/persistence/{mod,protocol,replay,publication,delta,journal,checkpoint}.rs \
    rust/crates/objects/src/v1/memory/persistence/checkpoint/{snapshot,packing}.rs \
    rust/crates/objects/src/v1/memory/provider.rs \
    rust/crates/objects/src/v1/memory/publication.rs \
    rust/crates/objects/src/v1/memory/transaction.rs \
    rust/crates/objects/src/v1/memory/transaction/{model,tests}.rs \
    rust/crates/objects/src/v1/memory/persistence/work.rs \
    rust/crates/stream/src/postgres.rs \
    rust/crates/stream/src/lib.rs \
    rust/crates/stream/src/wire_codec/requests.rs \
    rust/crates/stream/src/wire_codec/conversion_0.rs \
    rust/crates/stream/examples/postgres-conformance.rs \
    rust/crates/stream/examples/postgres_conformance/checks.rs \
    rust/crates/stream/examples/postgres_conformance/scale.rs \
    rust/crates/conformance/src/bin/storage-conformance.rs \
    rust/crates/conformance/src/bin/storage_conformance/s3.rs \
    rust/crates/conformance/src/bin/storage_conformance/observed_store.rs \
    rust/crates/conformance/src/bin/storage_conformance/filesystem.rs
step=compile-storage-conformance
cargo check --locked -p acyclic-conformance --features storage-backends,acyclic-objects/local --bin storage-conformance
step=compile-postgres-checks
cargo check --locked -p acyclic-stream --no-default-features --features postgres --example postgres-conformance
step=compile-shared-objects-regressions
cargo test --locked -p acyclic-objects --features local,s3 --lib --no-run
