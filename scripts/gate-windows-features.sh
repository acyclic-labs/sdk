#!/bin/bash
# Main checks every published consumer; PRs check their affected consumers.
# Feature selection is shared with Linux, including each single feature.
set -euo pipefail
args=(--windows-msvc --each-feature)
case "${1:-full}" in
  full) ;;
  affected)
    for scope in .gate-tests-packages .gate-clippy-packages; do
      while IFS= read -r package || [ -n "$package" ]; do
        package=${package%$'\r'}
        [ -z "$package" ] || args+=("--package=$package")
      done < "$scope"
    done
    [ ${#args[@]} -gt 2 ] || { echo 'error: empty affected Windows scope'; exit 2; };;
  *) echo 'error: invalid Windows feature scope'; exit 2;;
esac
if [ -n "${GATE_WINDOWS_FEATURE_TARGET_DIR:-}" ]; then
  export GATE_FEATURE_TARGET_DIR=$GATE_WINDOWS_FEATURE_TARGET_DIR
elif [ -n "${GATE_FEATURE_TARGET_DIR:-}" ] && [ -f scripts/gate-target-cache.py ] && \
     python3 scripts/gate-target-cache.py private "$GATE_FEATURE_TARGET_DIR"; then
  # Linux publishes before Windows starts. A separate private root lets each
  # completed matrix publish/restore instead of inheriting an occupied target.
  export GATE_FEATURE_TARGET_DIR="$GATE_FEATURE_TARGET_DIR-windows-msvc"
else
  export GATE_FEATURE_TARGET_DIR=${GATE_FEATURE_TARGET_DIR:-/mnt/bb/targets/sdk/gate-windows-features}
fi
echo "GATE_WINDOWS_FEATURES_START scope=${1:-full} target=x86_64-pc-windows-msvc libraries+all-targets each-feature"
rc=0
python3 scripts/with-xwin-cache.py --workers "${GATE_FEATURE_WORKERS:-6}" -- \
  bash scripts/gate-feature-matrix.sh "${args[@]}" || rc=$?
echo "GATE_WINDOWS_FEATURES rc=$rc"
exit "$rc"
