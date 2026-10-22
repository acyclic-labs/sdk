#!/bin/bash
# Full workspace baseline: ordinary targets and doctests require separate runs.
# Main browser target coverage v1
set -u
exec 2>&1
started=$SECONDS
each=(); harness=(); cargo_args=()
while (($#)); do
  case "$1" in
    --each-feature) each=(--each-feature); shift;;
    # Older coordinators supply the harness closure. It cannot narrow main.
    -p|--package) [ "$#" -ge 2 ] || { echo 'error: missing legacy package argument'; exit 2; }; shift 2;;
    --package=*|-p?*) shift;;
    --exclude|--exclude=*) echo 'error: main gate cannot exclude workspace packages'; exit 2;;
    --workspace|--all-features|--locked|--no-fail-fast|--all-targets|--doc) shift;;
    --) shift; harness=("$@"); break;;
    *) cargo_args+=("$1"); shift;;
  esac
done
cache=${GATE_MAIN_TARGET_DIR:-/mnt/bb/targets/sdk/gate-main}
if [[ "$cache" == /mnt/bb/targets/*/* ]]; then
  unit=${cache#/mnt/bb/targets/}; unit=${unit//\//%}
  mkdir -p /mnt/bb/locks/unit
  exec 8>>"/mnt/bb/locks/unit/$unit.lock"
  flock -s 8
fi
mkdir -p "$cache"
# Serialize manual qualification and the coordinator, including the gap between
# target and doctest invocations. Each phase reuses the same compiled libraries.
exec 7>"$cache/.gate.lock"
flock -x 7
trap 'touch "$cache"' EXIT
target="$cache/tests"
clippy_target="$cache/clippy"
echo "GATE_MAIN_START target=$target clippy_target=$clippy_target"
cargo metadata --locked --offline --no-deps --format-version 1 > .gate-main-metadata.json || exit 2
members=$(node -e 'const m=JSON.parse(require("fs").readFileSync(".gate-main-metadata.json","utf8")); const ids=new Set(m.workspace_members); console.log(m.packages.filter(p=>ids.has(p.id)).map(p=>p.name).sort().join(","))') || exit 2
node -e 'const m=JSON.parse(require("fs").readFileSync(".gate-main-metadata.json","utf8")); for(const p of m.packages) if(p.metadata?.["cargo-fuzz"]) for(const t of p.targets) if(t.kind.includes("bin")) console.log(t.name)' > .gate-fuzz-bins || exit 2
export GATE_FUZZ_TEST_BIN_FILE="$PWD/.gate-fuzz-bins"
export GATE_BASE_TEST_RUNNER=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}
test_runner="$PWD/${GATE_TEST_RUNNER_SCRIPT:-scripts/gate-test-runner.sh}"
chmod +x "$test_runner" || exit 2
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$test_runner"
( CARGO_TARGET_DIR="$clippy_target" cargo clippy --locked --keep-going --workspace --all-targets --all-features --message-format=short \
    -- --cap-lints warn > .gate-clippy.out 2>&1; echo $? > .gate-clippy.rc ) &
clippy=$!
( bash scripts/gate-feature-matrix.sh "${each[@]}" > .gate-features.out 2>&1; echo $? > .gate-features.rc ) &
features=$!
run_tests(){
  local phase=$1 status phase_started=$SECONDS; shift
  echo "GATE_TEST_START phase=$phase packages=$members"
  CARGO_TARGET_DIR="$target" cargo test --locked --workspace --all-features --no-fail-fast \
    "${cargo_args[@]}" "$@" -- "${harness[@]}" 2>&1 | tee ".gate-$phase.out"
  status=("${PIPESTATUS[@]}")
  if [ "${status[1]}" != 0 ]; then echo 'error: gate could not retain Cargo output'; return 2; fi
  echo "GATE_TEST_WALL phase=$phase seconds=$((SECONDS-phase_started)) rc=${status[0]}"
  if grep -aq '^test result:' ".gate-$phase.out"; then
    echo "GATE_TEST_COVERAGE phase=$phase packages=$members"
  fi
  return "${status[0]}"
}
run_tests targets --all-targets; targets_rc=$?
# Always collect doctest findings even if an ordinary target failed.
run_tests docs --doc; docs_rc=$?
# Each browser consumer gets its own Cargo invocation in the shared helper.
# Keep this sequential with tests to avoid adding more compiler contention.
browser_started=$SECONDS
echo "GATE_BROWSER_START target=$cache/browser"
CARGO_TARGET_DIR="$cache/browser" bash "${GATE_BROWSER_SCRIPT:-scripts/check-browser-targets.sh}" 2>&1 | tee .gate-browser.out
browser_status=("${PIPESTATUS[@]}")
browser_rc=${browser_status[0]}
if [ "${browser_status[1]}" != 0 ]; then
  echo 'error: gate could not retain browser check output'
  browser_rc=2
fi
echo "GATE_BROWSER_WALL seconds=$((SECONDS-browser_started)) rc=$browser_rc"
wait "$clippy"
echo "GATE_CLIPPY rc=$(cat .gate-clippy.rc)"
if [ "$(cat .gate-clippy.rc)" != 0 ]; then sed 's/^/GATE_CLIPPY_RAW /' .gate-clippy.out; fi
grep -aE '^[^ ]+\.rs:[0-9]+:[0-9]+: (warning|error): ' .gate-clippy.out | sed -E 's/:[0-9]+:[0-9]+: (warning|error): /: /' \
  | sort | uniq -c | sed -E 's/^ *([0-9]+) /GATE_CLIPPY_FINDING \1 /'
grep -aE '^error(\[E[0-9]+\])?: ' .gate-clippy.out | grep -v 'could not compile' | sort -u | sed 's/^/GATE_CLIPPY_ERROR /'
wait "$features"
echo "GATE_FEATURES rc=$(cat .gate-features.rc)"
grep -aE '^(feature matrix:|FAILED cargo)' .gate-features.out | sed -E 's|--manifest-path [^ ]*/([^/ ]+/Cargo\.toml)|--manifest-path \1|; s/^/GATE_FEATURES /'
grep -a '^feature check: ' .gate-features.out | sed 's/^/GATE_FEATURE_CHECK /'
grep -aE '^GATE_FEATURES_(CACHE|WALL) ' .gate-features.out
if [ "$(cat .gate-features.rc)" != 0 ]; then cat .gate-features.out; fi
rc=$targets_rc; [ "$rc" != 0 ] || rc=$docs_rc
[ "$rc" != 0 ] || rc=$browser_rc
# A partial auxiliary step must not leave the main baseline falsely green.
if [ "$rc" = 0 ] && { [ "$(cat .gate-clippy.rc)" != 0 ] || [ "$(cat .gate-features.rc)" != 0 ]; }; then rc=1; fi
if grep -aq '^test result:' .gate-targets.out && grep -aq '^test result:' .gate-docs.out; then
  echo "GATE_TEST_COVERAGE all-targets+doc packages=$members"
fi
echo "GATE_WALL seconds=$((SECONDS-started)) test_rc=$targets_rc doc_rc=$docs_rc browser_rc=$browser_rc"
echo "GATE_DONE rc=$rc"
exit "$rc"
