#!/bin/bash
gate_preflight_verdict() (
  log=$1
  count=$(grep -ac '^GATE_RUSTFMT ' "$log")
  [ "$count" != 0 ] || exit 0
  result=$(sed -nE 's/^GATE_RUSTFMT rc=(-?[0-9]+) seconds=[0-9]+ scope=.*$/\1/p' "$log")
  if [ "$count" != 1 ] || ! [[ "$result" =~ ^-?[0-9]+$ ]]; then
    echo 'Rust formatting completion has missing or inconsistent identity'
    exit 4
  fi
  [ "$result" != 0 ] || exit 0
  if grep -aqE 'No space left on device|signal: (9|15), SIG(KILL|TERM)' "$log"; then
    echo 'Rust formatter was interrupted or its build infrastructure failed'
    exit 4
  fi
  if [ "$result" != 1 ] || grep -aq '^spawn rustfmt ENOENT$' "$log"; then
    echo "Rust formatter executor/tool failed (rc=$result)"
    exit 4
  fi
  echo 'Rust formatting qualification failed'
  grep -aE '^Diff in |^error:|^GATE_RUSTFMT ' "$log" | head -20
  exit 1
)

gate_required_phases_verdict() (
  log=$1
  gate_preflight_verdict "$log"; preflight_rc=$?
  [ "$preflight_rc" = 0 ] || exit "$preflight_rc"
  # Strict phase: never subtract historical main findings. Exit4 is incomplete.
  if ! grep -aq '^GATE_WINDOWS_RUNTIME_REQUIRED ' "$log"; then exit 0; fi
  mapfile -t completions < <(grep -a '^GATE_WINDOWS_RUNTIME_CLIPPY ' "$log")
  if [ "${#completions[@]}" != 1 ]; then
    echo 'Windows runtime clippy did not record exactly one completion'
    exit 4
  fi
  identity='^GATE_WINDOWS_RUNTIME_CLIPPY rc=(-?[0-9]+) seconds=[0-9]+ target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all$'
  if ! [[ "${completions[0]}" =~ $identity ]]; then
    echo 'Windows runtime clippy completion has missing or inconsistent dispatch identity'
    exit 4
  fi
  runtime_rc=${BASH_REMATCH[1]}
  if [ "$runtime_rc" != 0 ] && [ "$runtime_rc" != 101 ]; then
    echo "Windows runtime executor or output retention failed (rc=$runtime_rc)"
    grep -aE '^error: could not retain Windows runtime clippy output|cargo: command not found|^GATE_WINDOWS_RUNTIME_CLIPPY ' "$log" | head -12
    exit 4
  fi
  if grep -aqE 'No space left on device|signal: (9|15), SIG(KILL|TERM)' "$log"; then
    echo 'Windows runtime check was interrupted or its build infrastructure failed'
    exit 4
  fi
  if [ "$runtime_rc" = 101 ]; then
    echo 'strict Windows runtime clippy failed'
    awk '/^GATE_WINDOWS_RUNTIME_START / { inside=1 } inside { print } /^GATE_WINDOWS_RUNTIME_CLIPPY / { inside=0 }' "$log" \
      | grep -aE '^GATE_WINDOWS_RUNTIME_CLIPPY |\.rs:[0-9]+:[0-9]+: error|^error(\[E[0-9]+\])?:' | head -12
    exit 1
  fi
  exit 0
)

# One strict Windows runtime phase, used by PRs, main and lane prechecks.
gate_windows_runtime_clippy() (
  set -u
  exec 2>&1
  phase_started=$SECONDS
  target=${GATE_WINDOWS_RUNTIME_TARGET_DIR:-${CARGO_TARGET_DIR:+$CARGO_TARGET_DIR/.gate-windows-runtime}}
  target=${target:-/mnt/bb/targets/sdk/gate-windows-runtime}
  gate_output=${GATE_WINDOWS_RUNTIME_OUTPUT_DIR:-}
  if [ -z "$gate_output" ]; then
    gate_output=$(mktemp -d "${TMPDIR:-/tmp}/gate-windows-runtime.XXXXXXXX") || exit 2
  fi
  mkdir -p "$gate_output" || exit 2
  echo 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all'
  if [[ "$target" == /mnt/bb/targets/*/* ]]; then
    unit=${target#/mnt/bb/targets/}; unit=${unit//\//%}
    mkdir -p /mnt/bb/locks/unit || exit 2
    exec 8>>"/mnt/bb/locks/unit/$unit.lock"
    flock -s 8 || exit 2
  fi
  mkdir -p "$target" || exit 2
  exec 7>"$target/.gate.lock"
  flock -x 7 || exit 2
  trap 'touch "$target"' EXIT
  echo "GATE_WINDOWS_RUNTIME_START target_dir=$target"
  # A caller's general warning cap must not weaken this package's denied lints.
  RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS= CARGO_TARGET_DIR="$target" cargo clippy --locked --target x86_64-pc-windows-gnu \
    -p acyclic-native-runtime --all-targets --all-features --message-format=short 2>&1 | tee "$gate_output/.gate-windows-runtime.out"
  status=("${PIPESTATUS[@]}")
  rc=${status[0]}
  if [ "${status[1]}" != 0 ]; then
    echo "error: could not retain Windows runtime clippy output (tee rc=${status[1]})"
    rc=2
  fi
  echo "GATE_WINDOWS_RUNTIME_CLIPPY rc=$rc seconds=$((SECONDS-phase_started)) target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all"
  exit "$rc"
)

if [ "${1:-}" = --windows-runtime-only ]; then
  [ "$#" = 1 ] || { echo 'error: --windows-runtime-only accepts no other arguments' >&2; exit 2; }
  gate_windows_runtime_clippy
  exit "$?"
fi
if [ "${1:-}" = --required-phases ] || [ "${1:-}" = --preflight-only ]; then
  [ "$#" = 2 ] && [ -f "$2" ] || { echo 'error: phase verdict requires one readable log' >&2; exit 4; }
  if [ "$1" = --preflight-only ]; then gate_preflight_verdict "$2"; else gate_required_phases_verdict "$2"; fi
  exit "$?"
fi

exec 2>&1
# The reviewed runner owns workspace, browser and feature coverage. Missing
# code is INFRA, never permission to execute a second embedded implementation.
if [ ! -f scripts/main-gate-checks.sh ]; then
  echo 'preparation-INFRA: missing reviewed scripts/main-gate-checks.sh'
  exit 224
fi
main_output=$(mktemp -d "${TMPDIR:-/tmp}/gate-main-qualification.XXXXXXXX") || exit 224
bash scripts/main-gate-checks.sh "$@" | tee "$main_output/main.out" | sed -u '/^GATE_DONE rc=/d'
main_status=("${PIPESTATUS[@]}")
main_rc=${main_status[0]}
if [ "${main_status[1]}" != 0 ] || [ "${main_status[2]}" != 0 ]; then
  echo 'error: could not retain reviewed main runner output'
  echo 'GATE_DONE rc=224'
  exit 224
fi
mapfile -t completed < <(grep -a '^GATE_DONE rc=' "$main_output/main.out")
if [ "${#completed[@]}" != 1 ] || [ "${completed[0]:-}" != "GATE_DONE rc=$main_rc" ]; then
  echo 'error: reviewed main runner completion does not match its executor result'
  echo 'GATE_DONE rc=224'
  exit 224
fi
gate_preflight_verdict "$main_output/main.out"; preflight_rc=$?
if [ "$preflight_rc" != 0 ]; then
  rc=$main_rc
  [ "$rc" != 0 ] || rc=$preflight_rc
  # A failed tool cannot be qualified as a source formatting verdict.
  [ "$preflight_rc" != 4 ] || rc=224
  echo "GATE_MAIN_QUALIFICATION main_rc=$main_rc preflight_rc=$preflight_rc"
  echo "GATE_DONE rc=$rc"
  exit "$rc"
fi
# The canonical runner must complete its owned phase; never attempt it again.
windows_features_rc=0
if ! grep -aq '^GATE_WINDOWS_FEATURES rc=0$' "$main_output/main.out"; then
  windows_features_rc=2
  echo 'error: reviewed main runner did not complete Windows features successfully'
fi
gate_windows_runtime_clippy; windows_rc=$?
rc=$main_rc
[ "$rc" != 0 ] || rc=$windows_features_rc
[ "$rc" != 0 ] || rc=$windows_rc
echo "GATE_MAIN_QUALIFICATION main_rc=$main_rc windows_features_rc=$windows_features_rc windows_runtime_rc=$windows_rc"
echo "GATE_DONE rc=$rc"
exit "$rc"
