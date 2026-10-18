#!/usr/bin/env bash
set -euo pipefail
node --test scripts/qualify-objects-batch-mutants.test.mjs
cargo test --locked -p acyclic-objects --features local,http,test-support --lib batch_
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg objects_loom" cargo test --locked -p acyclic-objects --lib v1::grpc::batch:: -- --test-threads=1
bash scripts/fuzz-objects-batch-reply.sh
node scripts/qualify-objects-batch-mutants.mjs
