#!/usr/bin/env bash
# Keep related tests in one process, with libtest's normal parallel execution.
# nextest's per-test processes cannot expose process-local fixture leakage.
set -euo pipefail
cd "$(dirname "$0")/../rust"
export RUSTC_BOOTSTRAP=1
cargo test --locked --offline \
  -p acyclic-harness-core -p acyclic-harness --lib \
  --features acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/test-support \
  -- executor::transcript::projection::tests:: local::tests::replay:: \
  local::tests::agents::a_spawn_chain_binds_its_volumes_at_every_depth \
  -Z unstable-options --shuffle-seed "${1:-17}" --nocapture
