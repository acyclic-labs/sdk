#!/bin/bash
# Shared by main and PR gates. Keep the cache outside either synced checkout.
set -euo pipefail
target=${GATE_FEATURE_TARGET_DIR:-/mnt/bb/targets/sdk/gate-features}
workers=${GATE_FEATURE_WORKERS:-6}
[[ "$workers" =~ ^[1-9][0-9]*$ ]] || { echo 'error: invalid feature worker count'; exit 2; }
started=$SECONDS
# The build-box sweeper uses these unit locks, not Cargo's internal locks.
# Acquire them before creating/using the external target directories.
if [[ "$target" == /mnt/bb/targets/*/* ]]; then
  mkdir -p /mnt/bb/locks/unit
  for ((i=0; i<workers; i++)); do
    unit=$target; ((i == 0)) || unit="$target-features-$i"
    unit=${unit#/mnt/bb/targets/}; unit=${unit//\//%}
    exec {fd}>>"/mnt/bb/locks/unit/$unit.lock"
    flock -s "$fd"
  done
fi
mkdir -p "$target"
exec 9>"$target/.gate.lock"
flock -x 9
waited=$((SECONDS-started))
echo "GATE_FEATURES_CACHE target=$target workers=$workers lock_wait_seconds=$waited"
rc=0
CARGO_TARGET_DIR="$target" node "${GATE_FEATURE_SCRIPT:-scripts/check-feature-matrix.mjs}" "$workers" "$@" || rc=$?
# The target-pruner reads the unit's own mtime, not its nested artifacts.
for ((i=0; i<workers; i++)); do
  unit=$target; ((i == 0)) || unit="$target-features-$i"
  [ ! -d "$unit" ] || touch "$unit"
done
echo "GATE_FEATURES_WALL seconds=$((SECONDS-started)) rc=$rc target=$target lock_wait_seconds=$waited"
exit "$rc"
