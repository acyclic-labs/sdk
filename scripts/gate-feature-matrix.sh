#!/bin/bash
# On-demand feature checks: seed a private job target when custody is proven.
set -euo pipefail
candidate=${GATE_FEATURE_TARGET_DIR:-${CARGO_TARGET_DIR:-target}/.gate-features}
private=0
if [ -f scripts/gate-target-cache.py ] && python3 scripts/gate-target-cache.py private "$candidate"; then
  target=$candidate
  private=1
else
  # Manual callers without admitted mount custody still share the legacy cache.
  target=${GATE_FEATURE_TARGET_DIR:-/mnt/bb/targets/sdk/gate-features}
fi
workers=${GATE_FEATURE_WORKERS:-6}
[[ "$workers" =~ ^[1-9][0-9]*$ ]] || { echo 'STEP_FAILED feature-workers: invalid worker count; recovery: GATE_FEATURE_WORKERS=6 bash scripts/gate-feature-matrix.sh'; exit 2; }
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
if [ "$private" = 0 ]; then
  exec 9>"$target/.gate.lock"
  flock -x 9
fi
waited=$((SECONDS-started))
echo "GATE_FEATURES_CACHE target=$target workers=$workers lock_wait_seconds=$waited private=$private"
rc=0
CARGO_TARGET_DIR="$target" python3 scripts/gate-target-cache.py matrix "$target" -- \
  bash scripts/gate-feature-payload.sh "$workers" "$@" || rc=$?
# The target-pruner reads the unit's own mtime, not its nested artifacts.
for ((i=0; i<workers; i++)); do
  unit=$target; ((i == 0)) || unit="$target-features-$i"
  [ ! -d "$unit" ] || touch "$unit"
done
echo "GATE_FEATURES_WALL seconds=$((SECONDS-started)) rc=$rc target=$target lock_wait_seconds=$waited private=$private"
exit "$rc"
