#!/usr/bin/env bash
set -euo pipefail

# Explicit long-running target; the ordinary test retains its small seed set.
: "${ACYCLIC_FS_SEEDS:?set the number of seeds to sweep}"
export ACYCLIC_FS_SEEDS
export ACYCLIC_FS_SEED="${ACYCLIC_FS_SEED:-0}"
exec nice -n 19 cargo test --locked -p acyclic-fs --features test-support --lib \
  workspace::tests::simulation::seeded_volume_history -- --exact --nocapture
