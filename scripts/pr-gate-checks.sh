#!/bin/bash
# Affected-PR runner; main uses the reviewed whole-workspace runner.
# The coordinator writes all scope files before submitting this one build job.
set -u
# bb2 keeps stdout/stderr on separate transports. Send all job diagnostics
# through stdout so native-shell stderr forwarding cannot discard Cargo errors.
exec 2>&1
started=$SECONDS
gate_output=$PWD
if [ -f scripts/gate-target-cache.py ]; then
  gate_output=$(python3 scripts/gate-target-cache.py output) || exit 2
fi
printf 'GATE_OUTPUT_DIRECTORY %s\n' "$gate_output"
windows_required=0
if [ "$(tr -d '\r' < .gate-scope-mode)" = full ] || grep -qx 'acyclic-native-runtime' .gate-clippy-packages .gate-tests-packages; then
  windows_required=1
fi
mode=$(tr -d '\r' < .gate-scope-mode) || exit 2
test_pkgs=(); clippy_pkgs=()
while IFS= read -r p; do p=${p%$'\r'}; [ -n "$p" ] && test_pkgs+=(-p "$p"); done < .gate-tests-packages
while IFS= read -r p; do p=${p%$'\r'}; [ -n "$p" ] && clippy_pkgs+=(-p "$p"); done < .gate-clippy-packages
case "$mode" in
  full) ;; # Resolve the original default-member set after metadata below.
  affected) ;;
  *) echo "error: invalid PR gate scope: $mode"; exit 2;;
esac
echo "GATE_PACKAGES tests ${test_pkgs[*]}"
echo "GATE_PACKAGES clippy ${clippy_pkgs[*]}"
if [ "$mode" = affected ] && [ ${#test_pkgs[@]} -eq 0 ]; then
  echo 'GATE_EMPTY no changed Cargo inputs'
  echo 'GATE_DONE rc=0'
  exit 0
fi
# Validate dispatch before any background compiler can outlive an early exit.
bash scripts/gate-rust-format.sh || exit $?
# Match main's target coverage, including test=false libFuzzer binaries.
cargo metadata --locked --offline --no-deps --format-version 1 > "$gate_output/.gate-pr-metadata.json" || exit 2
if [ "$mode" = full ]; then
  names=$(python3 scripts/gate-packages.py --defaults < "$gate_output/.gate-pr-metadata.json") || exit 2
  test_pkgs=(); clippy_pkgs=()
  while IFS= read -r name; do
    test_pkgs+=(-p "$name"); clippy_pkgs+=(-p "$name")
  done <<< "$names"
fi
gate_fuzz_executables="$gate_output/.gate-fuzz-executables"
: > "$gate_fuzz_executables" || exit 2
gate_base_test_runner=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}
test_runner="$PWD/${GATE_TEST_RUNNER_SCRIPT:-scripts/gate-test-runner.sh}"
if [ -f scripts/gate-target-cache.py ]; then
  test_runner=$(python3 scripts/gate-target-cache.py runner "$test_runner") || exit 2
else
  chmod +x "$test_runner" || exit 2
fi
# Keep registry dependencies warm, but never reuse workspace codegen from an
# interrupted job or another boot. Cargo's incremental objects can survive an
# aborted build with references to anonymous constants from another LLVM unit.
# The coordinator serializes PR jobs; cargo clean also takes Cargo's target lock.
gate_cache_prepare(){
  local target=$1 boot=$2 prior workspace_args=() name
  mkdir -p "$target" || return 2
  # Job-owned targets have no prior mutable state. Only the receipt/mount
  # oracle may select this path; retain legacy interruption protection outside
  # canonical jobs. Cargo validates completed immutable snapshot fingerprints.
  if [ "$gate_private_targets" = 1 ]; then return 0; fi
  prior=$(cat "$target/.gate-cache-boot" 2>/dev/null || true)
  if [ "$prior" != "$boot" ] || [ -e "$target/.gate-cache-inflight" ]; then
    echo "GATE_CACHE_INVALIDATE target=$target reason=boot-change-or-interrupted-build workspace-only"
    # Resolve the complete workspace, including dependencies outside this PR's
    # selected roots. Registry downloads and compiled third-party crates survive.
    cargo metadata --offline --no-deps --format-version 1 > "$gate_output/.gate-cache-metadata.json" || return 2
    node -e 'const m=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")); const ids=new Set(m.workspace_members); for(const p of m.packages) if(ids.has(p.id)) console.log(p.name)' "$gate_output/.gate-cache-metadata.json" > "$gate_output/.gate-cache-members" || return 2
    while IFS= read -r name; do [ -z "$name" ] || workspace_args+=(-p "$name"); done < "$gate_output/.gate-cache-members"
    [ ${#workspace_args[@]} -gt 0 ] || return 2
    CARGO_TARGET_DIR="$target" cargo clean --locked "${workspace_args[@]}" || return 2
  fi
  printf '%s\n' "$boot" > "$target/.gate-cache-boot.tmp" && mv "$target/.gate-cache-boot.tmp" "$target/.gate-cache-boot" || return 2
  printf '%s pid=%s\n' "$boot" "$$" > "$target/.gate-cache-inflight" || return 2
}
gate_cache_finish(){
  local target=$1 output=$2 status=${3:-0}
  if [ "$gate_private_targets" = 1 ]; then return 0; fi
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
gate_private_targets=0
if [ -f scripts/gate-target-cache.py ] && \
   python3 scripts/gate-target-cache.py private "$test_target" "$clippy_target"; then
  gate_private_targets=1
fi
gate_cache_prepare "$test_target" "$gate_boot" || exit 2
gate_cache_prepare "$clippy_target" "$gate_boot" || exit 2
( CARGO_TARGET_DIR="$clippy_target" cargo clippy --locked --keep-going "${clippy_pkgs[@]}" --all-targets --all-features --message-format=short \
    -- --cap-lints warn > "$gate_output/.gate-clippy.out" 2>&1; echo $? > "$gate_output/.gate-clippy.rc" ) &
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
    ) 9> "$feature_target/.gate.lock" > "$gate_output/.gate-features.out" 2>&1
    feature_rc=$?
    echo "$feature_rc" > "$gate_output/.gate-features.rc"
    echo "GATE_FEATURES_WALL seconds=$((SECONDS-feature_started)) rc=$feature_rc target=$feature_target" > "$gate_output/.gate-features.time" ) &
  features=$!
fi
# Preserve the caller's compiler environment; only test execution consumes
# these invocation-owned runtime paths. Matrix requests keep stable cache keys.
export GATE_FUZZ_EXECUTABLES="$gate_fuzz_executables" GATE_BASE_TEST_RUNNER="$gate_base_test_runner"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$test_runner"
run_tests(){
  local phase=$1 phase_started=$SECONDS status=(0 0); shift
  if [ "$phase" = targets ]; then
    bash "$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER" --build "$gate_output/.gate-pr-metadata.json" "$GATE_FUZZ_EXECUTABLES" -- \
      cargo test --locked "${test_pkgs[@]}" --all-features --no-fail-fast "$@" 2>&1 | tee -a "$gate_output/.gate-tests.out"
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[0]}" = 0 ] && [ "${status[1]}" = 0 ]; then
    bash scripts/with-owner-keychain.sh cargo test --locked "${test_pkgs[@]}" --all-features --no-fail-fast "$@" -- "${harness[@]}" 2>&1 | tee -a "$gate_output/.gate-tests.out"
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[1]}" != 0 ]; then echo 'error: gate could not retain Cargo test output'; status[0]=2; fi
  echo "GATE_TEST_PHASE phase=$phase rc=${status[0]} seconds=$((SECONDS-phase_started))"
  return "${status[0]}"
}
harness=("$@")
: > "$gate_output/.gate-tests.out"
run_tests targets --all-targets; rc=$?
run_tests docs --doc; doc_rc=$?
[ "$rc" != 0 ] || rc=$doc_rc
loom_started=$SECONDS
echo "GATE_HARNESS_LOOM_START target=$target_root/.gate-pr-loom"
CARGO_TARGET_DIR="$target_root/.gate-pr-loom" bash scripts/check-harness-models.sh --packages-file .gate-tests-packages 2>&1 | tee "$gate_output/.gate-harness-loom.out"
loom_status=("${PIPESTATUS[@]}")
loom_rc=${loom_status[0]}
if [ "${loom_status[1]}" != 0 ]; then
  echo 'error: gate could not retain harness Loom output'
  loom_rc=2
fi
echo "GATE_HARNESS_LOOM_WALL seconds=$((SECONDS-loom_started)) rc=$loom_rc"
[ "$rc" != 0 ] || rc=$loom_rc
wait "$clippy"
gate_cache_finish "$test_target" "$gate_output/.gate-tests.out" "$rc"
gate_cache_finish "$clippy_target" "$gate_output/.gate-clippy.out" "$(cat "$gate_output/.gate-clippy.rc")"
echo "GATE_CLIPPY rc=$(cat "$gate_output/.gate-clippy.rc")"
# Preserve every clippy diagnostic and its cause without confusing the test
# compile verdict with the separate clippy ratchet.
sed 's/^/GATE_CLIPPY_RAW /' "$gate_output/.gate-clippy.out"
gate_clippy=${SDK_GATE_CLIPPY:-/home/var/bin/gate-clippy.py}
if [ ! -f "$gate_clippy" ]; then
  echo "STEP_FAILED gate-clippy: external decoder missing at $gate_clippy; recovery: SDK_GATE_CLIPPY=/absolute/path/to/coordinator/gate-clippy.py bash scripts/pr-gate-checks.sh" >&2
  exit 224
fi
python3 "$gate_clippy" emit "$gate_output/.gate-clippy.out" "$(cat "$gate_output/.gate-clippy.rc")" || exit 224
if [ -n "$features" ]; then
  wait "$features"
  printf '\nGATE_FEATURES rc=%s\n' "$(cat "$gate_output/.gate-features.rc")"
  [ "$rc" != 0 ] || rc=$(cat "$gate_output/.gate-features.rc")
  grep -aE '^(feature matrix:|consumer builds:|FAILED cargo)' "$gate_output/.gate-features.out" | sed -E 's|--manifest-path [^ ]*/([^/ ]+/Cargo\.toml)|--manifest-path \1|; s/^/GATE_FEATURES /'
  if [ "$(cat "$gate_output/.gate-features.rc")" != 0 ] && ! grep -aq '^FAILED cargo' "$gate_output/.gate-features.out"; then
    echo "GATE_FEATURES FAILED matrix did not complete rc=$(cat "$gate_output/.gate-features.rc")"
  fi
  # The private output directory retires with the job. Preserve each compiler
  # diagnostic in the terminal log before retirement, even for parsed failures.
  if [ "$(cat "$gate_output/.gate-features.rc")" != 0 ]; then cat "$gate_output/.gate-features.out"; fi
  grep -a '^feature check: ' "$gate_output/.gate-features.out" | sed 's/^/GATE_FEATURE_CHECK /'
  cat "$gate_output/.gate-features.time"
else
  echo 'GATE_FEATURES absent'
fi
echo "GATE_WALL seconds=$((SECONDS-started)) test_rc=$rc"
if [ "$windows_required" = 1 ]; then
  windows_gate=${SDK_WINDOWS_GATE:-/home/var/bin/gate-checks.sh}
  if [ ! -f "$windows_gate" ]; then
    echo "STEP_FAILED windows-runtime: external gate missing at $windows_gate; recovery: SDK_WINDOWS_GATE=/absolute/path/to/coordinator/gate-checks.sh bash scripts/pr-gate-checks.sh" >&2
    windows_rc=224
  else
    GATE_WINDOWS_RUNTIME_OUTPUT_DIR="$gate_output" bash "$windows_gate" --windows-runtime-only; windows_rc=$?
  fi
  [ "$rc" != 0 ] || rc=$windows_rc
fi
bash scripts/gate-windows-features.sh "$mode"; windows_features_rc=$?
[ "$rc" != 0 ] || rc=$windows_features_rc
echo "GATE_DONE rc=$rc"
exit "$rc"
