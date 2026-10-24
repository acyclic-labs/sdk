#!/bin/bash
# PR-only runner. The main gate continues using the unchanged gate-checks.sh.
# The coordinator writes all scope files before submitting this one build job.
set -u
# bb2 keeps stdout/stderr on separate transports. Send all job diagnostics
# through stdout so native-shell stderr forwarding cannot discard Cargo errors.
exec 2>&1
started=$SECONDS
# Shared by the self-contained main and affected-PR gate payloads.
gate_windows_runtime_clippy() (
  set -u
  exec 2>&1
  phase_started=$SECONDS
  target=${GATE_WINDOWS_RUNTIME_TARGET_DIR:-/mnt/bb/targets/sdk/gate-windows-runtime}
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
  CARGO_TARGET_DIR="$target" cargo clippy --locked --target x86_64-pc-windows-gnu \
    -p acyclic-native-runtime --all-targets --all-features --message-format=short 2>&1 | tee .gate-windows-runtime.out
  status=("${PIPESTATUS[@]}")
  rc=${status[0]}
  if [ "${status[1]}" != 0 ]; then
    echo "error: could not retain Windows runtime clippy output (tee rc=${status[1]})"
    rc=2
  fi
  echo "GATE_WINDOWS_RUNTIME_CLIPPY rc=$rc seconds=$((SECONDS-phase_started)) target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all"
  exit "$rc"
)

windows_required=0
if [ "$(tr -d '\r' < .gate-scope-mode)" = full ] || grep -qx 'acyclic-native-runtime' .gate-clippy-packages .gate-tests-packages; then
  windows_required=1
fi
mode=$(tr -d '\r' < .gate-scope-mode) || exit 2
test_pkgs=(); clippy_pkgs=()
while IFS= read -r p; do p=${p%$'\r'}; [ -n "$p" ] && test_pkgs+=(-p "$p"); done < .gate-tests-packages
while IFS= read -r p; do p=${p%$'\r'}; [ -n "$p" ] && clippy_pkgs+=(-p "$p"); done < .gate-clippy-packages
case "$mode" in
  full) clippy_pkgs=(--workspace);;
  affected) ;;
  *) echo "error: invalid PR gate scope: $mode"; exit 2;;
esac
echo "GATE_PACKAGES tests ${test_pkgs[*]}"
echo "GATE_PACKAGES clippy ${clippy_pkgs[*]}"
if [ ${#test_pkgs[@]} -eq 0 ]; then
  echo 'GATE_EMPTY no changed Cargo inputs'
  echo 'GATE_DONE rc=0'
  exit 0
fi
# Validate dispatch before any background compiler can outlive an early exit.
# Match main's target coverage, including test=false libFuzzer binaries.
cargo metadata --locked --offline --no-deps --format-version 1 > .gate-pr-metadata.json || exit 2
export GATE_FUZZ_EXECUTABLES="$PWD/.gate-fuzz-executables"
: > "$GATE_FUZZ_EXECUTABLES" || exit 2
export GATE_BASE_TEST_RUNNER=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/${GATE_TEST_RUNNER_SCRIPT:-scripts/gate-test-runner.sh}"
chmod +x "$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER" || exit 2
# Keep registry dependencies warm, but never reuse workspace codegen from an
# interrupted job or another boot. Cargo's incremental objects can survive an
# aborted build with references to anonymous constants from another LLVM unit.
# The coordinator serializes PR jobs; cargo clean also takes Cargo's target lock.
gate_cache_prepare(){
  local target=$1 boot=$2 prior workspace_args=() name
  mkdir -p "$target" || return 2
  prior=$(cat "$target/.gate-cache-boot" 2>/dev/null || true)
  if [ "$prior" != "$boot" ] || [ -e "$target/.gate-cache-inflight" ]; then
    echo "GATE_CACHE_INVALIDATE target=$target reason=boot-change-or-interrupted-build workspace-only"
    # Resolve the complete workspace, including dependencies outside this PR's
    # selected roots. Registry downloads and compiled third-party crates survive.
    cargo metadata --offline --no-deps --format-version 1 > .gate-cache-metadata.json || return 2
    node -e 'const m=JSON.parse(require("fs").readFileSync(".gate-cache-metadata.json","utf8")); const ids=new Set(m.workspace_members); for(const p of m.packages) if(ids.has(p.id)) console.log(p.name)' > .gate-cache-members || return 2
    while IFS= read -r name; do [ -z "$name" ] || workspace_args+=(-p "$name"); done < .gate-cache-members
    [ ${#workspace_args[@]} -gt 0 ] || return 2
    CARGO_TARGET_DIR="$target" cargo clean --locked "${workspace_args[@]}" || return 2
  fi
  printf '%s\n' "$boot" > "$target/.gate-cache-boot.tmp" && mv "$target/.gate-cache-boot.tmp" "$target/.gate-cache-boot" || return 2
  printf '%s pid=%s\n' "$boot" "$$" > "$target/.gate-cache-inflight" || return 2
}
gate_cache_finish(){
  local target=$1 output=$2 status=${3:-0}
  # A normal Cargo/test failure remains a verdict. An externally terminated
  # compiler leaves this cache dirty; the next job invalidates it, never retries
  # or changes the verdict of the current run.
  if [ "$status" -ge 128 ] || grep -aqE 'signal: (9|15), SIG(KILL|TERM)|internal compiler error' "$output"; then
    echo "GATE_CACHE_DIRTY target=$target compiler-terminated"
  else
    rm -f "$target/.gate-cache-inflight"
  fi
}
gate_boot=$(cat /proc/sys/kernel/random/boot_id) || exit 2
target_root=${CARGO_TARGET_DIR:-target}
# Parent snapshots exclude descendant writers. Keep independent gate phases
# as siblings so the hierarchy leases permit their intended parallel work.
test_target="$target_root/.gate-pr-tests"
clippy_target="$target_root/.gate-pr-clippy"
export CARGO_TARGET_DIR="$test_target"
gate_cache_prepare "$test_target" "$gate_boot" || exit 2
gate_cache_prepare "$clippy_target" "$gate_boot" || exit 2
( CARGO_TARGET_DIR="$clippy_target" cargo clippy --locked --keep-going "${clippy_pkgs[@]}" --all-targets --all-features --message-format=short \
    -- --cap-lints warn > .gate-clippy.out 2>&1; echo $? > .gate-clippy.rc ) &
clippy=$!
features=""
# Retain the existing feature matrix if the checkout provides it. Its published
# feature sets stay comparable to main; tests and clippy use only the PR scope.
if [ -f scripts/check-feature-matrix.mjs ]; then
  ( feature_started=$SECONDS
    feature_target=${GATE_FEATURE_TARGET_DIR:-/mnt/bb/targets/sdk/gate-features}
    mkdir -p "$feature_target" && (
      # Also protect measurement/manual jobs that do not hold the coordinator's
      # lock. Cargo's own target lock alone would interleave the matrix's jobs.
      flock -x 9 || exit 2
      if [ -f scripts/gate-feature-matrix.sh ]; then
        # The helper owns this same lock; release the legacy outer lock first.
        flock -u 9
        bash scripts/gate-feature-matrix.sh
      else
        CARGO_TARGET_DIR="$feature_target" node scripts/check-feature-matrix.mjs 6
      fi
    ) 9> "$feature_target/.gate.lock" > .gate-features.out 2>&1
    feature_rc=$?
    echo "$feature_rc" > .gate-features.rc
    echo "GATE_FEATURES_WALL seconds=$((SECONDS-feature_started)) rc=$feature_rc target=$feature_target" > .gate-features.time ) &
  features=$!
fi
run_tests(){
  local phase=$1 phase_started=$SECONDS status=(0 0); shift
  if [ "$phase" = targets ]; then
    bash "$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER" --build .gate-pr-metadata.json "$GATE_FUZZ_EXECUTABLES" -- \
      cargo test --locked "${test_pkgs[@]}" --all-features --no-fail-fast "$@" 2>&1 | tee -a .gate-tests.out
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[0]}" = 0 ] && [ "${status[1]}" = 0 ]; then
    cargo test --locked "${test_pkgs[@]}" --all-features --no-fail-fast "$@" -- "${harness[@]}" 2>&1 | tee -a .gate-tests.out
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[1]}" != 0 ]; then echo 'error: gate could not retain Cargo test output'; status[0]=2; fi
  echo "GATE_TEST_PHASE phase=$phase rc=${status[0]} seconds=$((SECONDS-phase_started))"
  return "${status[0]}"
}
harness=("$@")
: > .gate-tests.out
run_tests targets --all-targets; rc=$?
run_tests docs --doc; doc_rc=$?
[ "$rc" != 0 ] || rc=$doc_rc
wait "$clippy"
gate_cache_finish "$test_target" .gate-tests.out "$rc"
gate_cache_finish "$clippy_target" .gate-clippy.out "$(cat .gate-clippy.rc)"
echo "GATE_CLIPPY rc=$(cat .gate-clippy.rc)"
# Preserve every clippy diagnostic and its cause without confusing the test
# compile verdict with the separate clippy ratchet.
sed 's/^/GATE_CLIPPY_RAW /' .gate-clippy.out
grep -aE '^[^ ]+\.rs:[0-9]+:[0-9]+: (warning|error): ' .gate-clippy.out | sed -E 's/:[0-9]+:[0-9]+: (warning|error): /: /' \
  | sort | uniq -c | sed -E 's/^ *([0-9]+) /GATE_CLIPPY_FINDING \1 /'
grep -aE '^error(\[E[0-9]+\])?: ' .gate-clippy.out | grep -v 'could not compile' | sort -u | sed 's/^/GATE_CLIPPY_ERROR /'
if [ "$(cat .gate-clippy.rc)" != 0 ] && ! grep -aqE '^error(\[E[0-9]+\])?: |\.rs:[0-9]+:[0-9]+: error' .gate-clippy.out; then
  echo 'GATE_CLIPPY_ERROR clippy exited without parsed diagnostics'
  cat .gate-clippy.out
fi
if [ -n "$features" ]; then
  wait "$features"
  echo "GATE_FEATURES rc=$(cat .gate-features.rc)"
  grep -aE '^(feature matrix:|FAILED cargo)' .gate-features.out | sed -E 's|--manifest-path [^ ]*/([^/ ]+/Cargo\.toml)|--manifest-path \1|; s/^/GATE_FEATURES /'
  if [ "$(cat .gate-features.rc)" != 0 ] && ! grep -aq '^FAILED cargo' .gate-features.out; then
    echo "GATE_FEATURES FAILED matrix did not complete rc=$(cat .gate-features.rc)"
    cat .gate-features.out
  fi
  grep -a '^feature check: ' .gate-features.out | sed 's/^/GATE_FEATURE_CHECK /'
  cat .gate-features.time
else
  echo 'GATE_FEATURES absent'
fi
echo "GATE_WALL seconds=$((SECONDS-started)) test_rc=$rc"
if [ "$windows_required" = 1 ]; then
  gate_windows_runtime_clippy; windows_rc=$?
  [ "$rc" != 0 ] || rc=$windows_rc
fi
echo "GATE_DONE rc=$rc"
exit "$rc"
