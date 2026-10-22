#!/usr/bin/env bash
set -euo pipefail

# The identity seam serves portable constructors too. Native feature unification
# must not conceal missing modules or native-only test-support imports.
cargo check --locked -p acyclic-fs --no-default-features --features memory --lib
bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
cargo check --locked -p acyclic-fs --no-default-features --features memory,browser,test-support --lib --target wasm32-unknown-unknown

# Build each focused target once; the artifact runner executes the binaries.
cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py task:: testing::entropy:: testing::executor:: testing::clock:: testing::queue:: testing::durability:: testing::host:: testing::process::
RUSTFLAGS="--cfg acyclic_native_runtime_loom" cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py testing::model:: testing::entropy::models:: testing::executor::models:: testing::queue_model:: testing::durability::models:: testing::host::models:: health::owners::model:: claim::model:: descriptor::model::
cargo test --locked -p acyclic-harness-core --features filesystem-local,native-execution,grpc,test-support --lib --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py filesystem::native_process::capture::tests::
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution,grpc,test-support --test dst --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py --runner scripts/qualify-dst-replay.py

# Keep transitive native features identical across the touched packages' tests.
cargo check --locked -p acyclic-harness -p acyclic-native-runtime -p acyclic-stream -p acyclic-objects -p acyclic-fs --lib --tests \
  --features acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/grpc,acyclic-harness/test-support,acyclic-native-runtime/test-support,acyclic-stream/local,acyclic-stream/test-support,acyclic-objects/local,acyclic-objects/test-support,acyclic-fs/local,acyclic-fs/test-support \
  --target x86_64-pc-windows-gnu
