#!/bin/bash
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

gate_main_existing() (
#!/bin/bash
# Browser helper owned by main-fixes, consumed verbatim from PR #1374 / 8ba7960ec.
# Bootstrap older main tips until that reviewed helper is in their source tree.
if [ ! -f scripts/check-browser-targets.sh ]; then
cat > .gate-browser-targets.sh <<'GATE_BROWSER_BOOTSTRAP'
#!/usr/bin/env bash
set -euo pipefail

# Host feature checks cannot compile the browser-only paths. Check libraries
# on their actual target, with each consumer's features resolved independently.
# In particular, binding crates must not mask missing cfgs in acyclic-fs.
bash scripts/ensure-rust-target.sh wasm32-unknown-unknown

check() {
  echo "browser target check: $*"
  cargo check --locked --target wasm32-unknown-unknown --lib "$@"
}

check -p acyclic-fs --no-default-features
check -p acyclic-fs --no-default-features --features memory,browser
for package in acyclic-auth-wasm acyclic-actors-wasm acyclic-fs-wasm \
  acyclic-objects-wasm acyclic-machines-wasm acyclic-inference-wasm acyclic-manifest; do
  check -p "$package"
done
check -p acyclic-stream --no-default-features --features wasm
check -p acyclic-harness-core --no-default-features --features browser
check -p acyclic-harness --no-default-features --features browser
check -p acyclic-harness-providers --features browser
GATE_BROWSER_BOOTSTRAP
export GATE_BROWSER_SCRIPT=.gate-browser-targets.sh
fi
cat > .gate-test-runner.sh <<'GATE_FUZZ_BOOTSTRAP'
#!/bin/bash
# Compiler artifacts, not executable basenames, identify libFuzzer drivers.
set -euo pipefail
if [ "${1:-}" = --build ]; then
  metadata=$2; manifest=$3; shift 3
  [ "$1" = -- ] || exit 2
  shift
  # Complete artifact discovery before executing anything. Cargo's JSON stream
  # otherwise races its runner, which may start before a consumer sees the event.
  "$@" --no-run --message-format=json-render-diagnostics > "$manifest.artifacts" || exit $?
  exec bash "$0" --collect "$metadata" "$manifest.artifacts" "$manifest"
fi
if [ "${1:-}" = --collect ]; then
  shift
  exec python3 - "$@" <<'PY'
import json, os, sys
from pathlib import Path
try:
    metadata, artifacts, output = map(Path, sys.argv[1:])
    m = json.loads(metadata.read_text())
    fuzz = {(p['id'], t['name']) for p in m['packages'] if (p.get('metadata') or {}).get('cargo-fuzz')
            for t in p['targets'] if t['kind'] == ['bin']}
    paths = set()
    for line in artifacts.read_text().splitlines():
        item = json.loads(line)
        if item.get('reason') != 'compiler-artifact':
            continue
        if (item['package_id'], item['target']['name']) in fuzz and item['target']['kind'] == ['bin'] and item.get('executable'):
            executable = Path(item['executable'])
            if not executable.is_absolute() or not executable.is_file():
                raise RuntimeError('invalid fuzz executable artifact: ' + str(executable))
            paths.add(str(executable.resolve()))
    tmp = Path(str(output) + '.tmp')
    tmp.write_text(''.join(p + '\n' for p in sorted(paths)))
    os.replace(tmp, output)
except Exception as error:
    print('error: GATE_RUNNER_INFRA compiler-artifact discovery: ' + str(error), file=sys.stderr)
    sys.exit(2)
PY
fi
program=$1; shift
if [ -z "${GATE_FUZZ_EXECUTABLES:-}" ] || [ ! -f "$GATE_FUZZ_EXECUTABLES" ]; then
  echo 'error: GATE_RUNNER_INFRA missing completed compiler-artifact manifest' >&2
  exit 2
fi
program=$(realpath -e -- "$program")
match=0
grep -Fxq -- "$program" "$GATE_FUZZ_EXECUTABLES" || match=$?
if [ "$match" -gt 1 ]; then
  echo 'error: GATE_RUNNER_INFRA cannot read compiler-artifact manifest' >&2
  exit 2
fi
if [ "$match" = 0 ]; then
  echo "GATE_FUZZ_SMOKE executable=$program runs=1 seed=1"
  set -- -runs=1 -seed=1
fi
if [ -n "${GATE_BASE_TEST_RUNNER:-}" ]; then
  exec "$GATE_BASE_TEST_RUNNER" "$program" "$@"
fi
exec "$program" "$@"
GATE_FUZZ_BOOTSTRAP
export GATE_TEST_RUNNER_SCRIPT=.gate-test-runner.sh
# Prefer a reviewed runner with complete phase coverage markers.
if [ -f scripts/main-gate-checks.sh ] && [ -f scripts/gate-feature-matrix.sh ] && grep -q '^# Main baseline completion v2$' scripts/main-gate-checks.sh && grep -q '^# Fuzz executable identity v2$' scripts/main-gate-checks.sh; then
  exec bash scripts/main-gate-checks.sh "$@"
fi
bash scripts/gate-rust-format.sh || exit $?
cat > .gate-feature-matrix.mjs <<'GATE_MATRIX_BOOTSTRAP'
#!/usr/bin/env node
// Type-checks every published crate's library in each feature set a consumer
// can select on its own: no default features, the defaults and every feature,
// and with --each-feature each feature alone as well. Every set is checked in its own Cargo invocation
// for one package, selected by its manifest, so no other workspace member's
// features unify into it and hide a missing gate. The crates and sets come
// from the manifests, so a new crate or feature is covered without editing
// this script.
//
// Usage: check-feature-matrix.mjs [WORKERS] [--each-feature] [--list]
// The merge gate runs the three sets per crate on every PR and adds each
// feature alone on main (gate-checks.sh --each-feature); the policy
// qualification lane runs every set.
// Every extra worker uses its own target directory, so Cargo's build lock never
// serializes them; the compiler cache still deduplicates their dependencies.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { metadata } from "./scripts/publish-cargo-crates.mjs";

const root = dirname(fileURLToPath(import.meta.url));

/** Everything `features` enables in this package: its own features and the
 *  optional dependencies and dependency features they name, transitively. */
export function enabled(item, features) {
  const seen = new Set();
  const pending = [...features];
  while (pending.length) {
    const entry = pending.pop();
    if (seen.has(entry)) continue;
    seen.add(entry);
    if (Object.hasOwn(item.features, entry)) pending.push(...item.features[entry]);
  }
  // No code here gates on `feature = "default"`, so it names no code of its own.
  seen.delete("default");
  return [...seen].sort().join(" ");
}

/** The Cargo argument sets for one package from its `cargo metadata` entry.
 *  Sets that enable exactly the same features and optional dependencies (a
 *  lone feature that is the whole default set, defaults that are every
 *  feature, no defaults at all) compile the same code, so each is checked
 *  once. */
export function featureSets(item, eachFeature = true) {
  const own = Object.keys(item.features).filter(name => name !== "default").sort();
  const defaults = Object.hasOwn(item.features, "default") ? ["default"] : [];
  const sets = [
    [["--no-default-features"], []],
    [[], defaults],
    [["--all-features"], Object.keys(item.features)],
    ...(eachFeature ? own : []).map(feature => [["--no-default-features", "--features", feature], [feature]]),
  ];
  const checked = new Set();
  return sets
    .filter(([, features]) => {
      const key = enabled(item, features);
      return !checked.has(key) && checked.add(key);
    })
    .map(([set]) => ["check", "--locked", "--manifest-path", item.manifest_path, "--lib", ...set]);
}

// Keep each (package, feature selection) in the same cache when another crate
// or feature is added, or when PR and main use different matrix sizes. Absolute
// manifest paths differ between checkouts and must not be part of the key.
export function featureWorker(name, args, workers) {
  const key = [name, ...args.slice(5)].join("\0");
  return createHash("sha256").update(key).digest().readUInt32LE(0) % workers;
}

async function main() {
  const args = process.argv.slice(2);
  const list = args.includes("--list");
  const workers = Number(args.find(arg => /^\d+$/u.test(arg)) ?? 1);
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("WORKERS must be a positive integer");
  // Every publishable local crate, in publication order (the publish check
  // already proves the order lists each exactly once).
  const eachFeature = args.includes("--each-feature");
  const sets = [...metadata().values()].flatMap(item => featureSets(item, eachFeature)
    .map(args => ({ name: item.name, args })));
  if (list) {
    for (const { args } of sets) console.log(`cargo ${args.join(" ")}`);
    return;
  }
  const failures = [];
  let passed = 0;
  const queues = Array.from({ length: workers }, () => []);
  for (const { name, args } of sets) queues[featureWorker(name, args, workers)].push(args);
  async function worker(index) {
    const env = index === 0 ? process.env
      : { ...process.env, CARGO_TARGET_DIR: `${process.env.CARGO_TARGET_DIR ?? join(root, "target")}-features-${index}` };
    for (const set of queues[index]) {
      const checkStarted = Date.now();
      const child = spawn("cargo", set, { cwd: root, env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
      let output = "";
      child.stdout.on("data", chunk => (output += chunk));
      child.stderr.on("data", chunk => (output += chunk));
      const status = await new Promise((done, reject) => child.on("error", reject).on("close", done));
      const rebuilt = [...output.matchAll(/^\s+(?:Checking|Compiling) /gmu)].length;
      console.log(`feature check: worker=${index} seconds=${((Date.now() - checkStarted) / 1000).toFixed(1)} rebuilt=${rebuilt} manifest=${set[3]} flags=${set.slice(5).join(" ") || "defaults"}`);
      if (status === 0) passed += 1;
      else {
        failures.push(`cargo ${set.join(" ")}`);
        process.stderr.write(`FAILED cargo ${set.join(" ")}\n${output}\n`);
      }
    }
  }
  const started = Date.now();
  const total = sets.length;
  await Promise.all(Array.from({ length: workers }, (_, index) => worker(index)));
  // Count the sets that ran and passed, so a set that never ran is a failure.
  console.log(`feature matrix: ${passed}/${total} sets ok in ${((Date.now() - started) / 1000).toFixed(1)}s`);
  for (const failure of failures) console.log(`FAILED ${failure}`);
  process.exitCode = passed === total ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
GATE_MATRIX_BOOTSTRAP
export GATE_FEATURE_SCRIPT=.gate-feature-matrix.mjs
cat > .gate-feature-matrix.sh <<'GATE_FEATURE_BOOTSTRAP'
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
GATE_FEATURE_BOOTSTRAP
#!/bin/bash
# Full workspace baseline: ordinary targets and doctests require separate runs.
# Main browser target coverage v1
# Main baseline completion v2
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
export GATE_FUZZ_EXECUTABLES="$PWD/.gate-fuzz-executables"
: > "$GATE_FUZZ_EXECUTABLES" || exit 2
export GATE_BASE_TEST_RUNNER=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}
test_runner="$PWD/${GATE_TEST_RUNNER_SCRIPT:-scripts/gate-test-runner.sh}"
chmod +x "$test_runner" || exit 2
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$test_runner"
( CARGO_TARGET_DIR="$clippy_target" cargo clippy --locked --keep-going --workspace --all-targets --all-features --message-format=short \
    -- --cap-lints warn > .gate-clippy.out 2>&1; echo $? > .gate-clippy.rc ) &
clippy=$!
( bash .gate-feature-matrix.sh "${each[@]}" > .gate-features.out 2>&1; echo $? > .gate-features.rc ) &
features=$!
run_tests(){
  local phase=$1 status phase_started=$SECONDS; shift
  echo "GATE_TEST_START phase=$phase packages=$members"
  status=(0 0)
  if [ "$phase" = targets ]; then
    CARGO_TARGET_DIR="$target" bash "$test_runner" --build .gate-main-metadata.json "$GATE_FUZZ_EXECUTABLES" -- \
      cargo test --locked --workspace --all-features --no-fail-fast "${cargo_args[@]}" "$@" 2>&1 | tee ".gate-$phase.out"
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[0]}" = 0 ] && [ "${status[1]}" = 0 ]; then
    CARGO_TARGET_DIR="$target" cargo test --locked --workspace --all-features --no-fail-fast \
      "${cargo_args[@]}" "$@" -- "${harness[@]}" 2>&1 | tee -a ".gate-$phase.out"
    status=("${PIPESTATUS[@]}")
  fi
  if [ "${status[1]}" != 0 ]; then echo 'error: gate could not retain Cargo output'; status[0]=2; fi
  echo "GATE_TEST_WALL phase=$phase seconds=$((SECONDS-phase_started)) rc=${status[0]}"
  if [ "${status[0]}" = 0 ]; then
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
if [ "$targets_rc" = 0 ] && [ "$docs_rc" = 0 ]; then
  echo "GATE_TEST_COVERAGE all-targets+doc packages=$members"
fi
echo "GATE_WALL seconds=$((SECONDS-started)) test_rc=$targets_rc doc_rc=$docs_rc browser_rc=$browser_rc"
echo "GATE_DONE rc=$rc"
exit "$rc"

)

exec 2>&1
gate_main_existing "$@" | sed -u '/^GATE_DONE rc=/d' | tee .gate-main-qualification.out
main_status=("${PIPESTATUS[@]}")
main_rc=${main_status[0]}
if [ "${main_status[1]}" != 0 ] || [ "${main_status[2]}" != 0 ]; then main_rc=2; fi
# The fallback runner predates the MSVC matrix. Require it here unless the
# selected checked-in main runner already reported the phase's verdict.
windows_features_rc=0
if grep -aqE '^GATE_WINDOWS_FEATURES(_START | rc=)' .gate-main-qualification.out; then
  # An interrupted owned phase remains a refusal, never a second attempt.
  grep -aq '^GATE_WINDOWS_FEATURES rc=0$' .gate-main-qualification.out || windows_features_rc=1
else
  bash scripts/gate-windows-features.sh full; windows_features_rc=$?
fi
gate_windows_runtime_clippy; windows_rc=$?
rc=$main_rc
[ "$rc" != 0 ] || rc=$windows_features_rc
[ "$rc" != 0 ] || rc=$windows_rc
echo "GATE_MAIN_QUALIFICATION main_rc=$main_rc windows_features_rc=$windows_features_rc windows_runtime_rc=$windows_rc"
echo "GATE_DONE rc=$rc"
exit "$rc"
