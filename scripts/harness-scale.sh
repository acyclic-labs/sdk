#!/usr/bin/env bash
# Ramps concurrent Harness agents on this machine and prints per-agent cost:
# resident memory, threads, descriptors, mounts, inotify watches, disk,
# first-dispatch and turn latency, and idle CPU.
#
#   scripts/harness-scale.sh [AGENTS...]        default: 100 1000 10000 100000
#
# ACYCLIC_SCALE_CHILDREN=C makes every root spawn and wait for C children.
# ACYCLIC_SCALE_IDLE_MS sets the idle CPU window (default 500).
# ACYCLIC_SCALE_DIAG=1 prints descriptor, thread and file breakdowns.
# ACYCLIC_CACHED_DESCRIPTORS=D bounds the descriptors idle journals and logs
# keep open (default: a quarter of the descriptor limit).
# A count that fails stops the ramp and reports which count it was.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
counts=("$@")
[[ ${#counts[@]} -gt 0 ]] || counts=(100 1000 10000 100000)
cd "$root"
cargo test --locked -p acyclic-harness --all-features --lib --no-run
for count in "${counts[@]}"; do
  [[ "$count" =~ ^[1-9][0-9]*$ ]] || { echo "agent count must be a positive integer: $count" >&2; exit 2; }
  if ! ACYCLIC_SCALE_AGENTS="$count" cargo test --locked -p acyclic-harness --all-features --lib \
    local::tests::scale::ramp -- --exact --ignored --nocapture --test-threads=1; then
    echo "harness-scale: $count agents failed" >&2
    exit 1
  fi
done
