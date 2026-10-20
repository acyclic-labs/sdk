#!/usr/bin/env bash
# Focused checkout identity gate. Compile separately from bounded runtime suites.
set -euo pipefail
cd "$(dirname "$0")/.."
export CODEX_HOME=$(mktemp -d)
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
harness_features="acyclic-harness/${features//,/,acyclic-harness/}"
timed() {
    local label=$1
    shift
    TIMEFORMAT="$label: %3R s"
    time "$@"
}
run_built() {
    python3 - "$artifacts" "$@" <<'PY'
import json,re,subprocess,sys,time
records=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
target,cwd,*filters=sys.argv[2:]
binaries={record['executable'] for record in records
          if record.get('reason')=='compiler-artifact' and record.get('executable')
          and record['profile']['test'] and record['target']['name']==target}
assert len(binaries)==1,binaries
binary=binaries.pop()
for selected in filters:
    start=time.monotonic()
    result=subprocess.run([binary,selected,'--nocapture'],cwd=cwd,
                          timeout=9.5,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    print(f'{selected}: {time.monotonic()-start:.3f}s\n{result.stdout}',flush=True)
    assert result.returncode==0,result.returncode
    assert re.search(r'^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;',result.stdout,re.M),result.stdout
if target=='acyclic_harness' and sys.platform.startswith('linux'):
    subprocess.run([sys.executable,'scripts/check-exclusion-flush.py',binary],check=True)
PY
}
case "${1:-all}" in
all)
    for phase in static native model windows fuzz; do bash "$0" "$phase"; done
    ;;
static)
    # Durable flushes go through acyclic-native-runtime, which issues each
    # platform's native full sync (F_FULLFSYNC on Apple) once per metered
    # attempt. A bare fsync elsewhere is not.
    if grep -rnE '(libc|rustix::fs)::f(data)?sync\b' rust/crates --include='*.rs' |
        grep -v '^rust/crates/native-runtime/'; then
        echo "raw fsync bypasses the native durability mechanism" >&2
        exit 1
    fi
    ;;
native)
    artifacts=$(mktemp)
    trap 'rm -f "$artifacts"' EXIT
    timed build cargo test --locked -p acyclic-fs -p acyclic-harness \
        --features "$features" --lib --no-run --message-format=json >"$artifacts"
    run_built acyclic_fs rust/crates/filesystem \
        native_capture::held_root_tests native_host::held_files::record native_host::leaf \
        native_host::interrupted_sync_tests \
        native_exchange::tests::exchange_refuses_normalized_directory_aliases \
        demand::tests::a_held_root \
        demand::tests::negative_control_binding
    run_built acyclic_harness rust/crates/harness \
        local::tests::checkout_incarnation local::owner:: local::owner_record:: \
        local::tests::checkout_reopen \
        local::tests::checkout::checkout_without_bash \
        local::tests::volumes::bash_crosses_granted \
        local::workspace::volumes::recovery_tests
    run_built acyclic_harness rust/crates/harness \
        local::tests::hermetic::default_state_overlap_is_refused_before_checkout_publication \
        local::tests::hermetic::explicit_state_overlap_is_refused_before_checkout_publication \
        local::tests::hermetic::canonical_separation_rejects_both_overlap_directions
    timed cli-build cargo test --locked -p graphcoder --test cli --features "$harness_features" \
        --no-run --message-format=json >"$artifacts"
    run_built cli rust/crates/harness \
        recreated_checkout_runs_a_fresh_operation_through_the_real_cli_and_http
    ;;
model)
    # This cfg, never a feature or build.rs decision, instruments the actual
    # read/rename/flush operations used by HeldRoot::read_or_publish.
    artifacts=$(mktemp)
    trap 'rm -f "$artifacts"' EXIT
    timed model-build env RUSTFLAGS="${RUSTFLAGS:-} --cfg acyclic_fs_native_loom" \
        cargo test --locked -p acyclic-fs --lib --no-run --message-format=json >"$artifacts"
    run_built acyclic_fs rust/crates/filesystem native_host::held_files::models
    ;;
windows)
    for phase in windows-base windows-cli windows-fuzz; do bash "$0" "$phase"; done
    ;;
windows-base)
    timed windows cargo check --locked -p acyclic-fs -p acyclic-harness -p acyclic-native-runtime \
        --lib --tests --features "$features" --target x86_64-pc-windows-gnu
    ;;
windows-cli)
    timed windows-cli cargo check --locked -p graphcoder --tests \
        --features "$harness_features" --target x86_64-pc-windows-gnu
    ;;
windows-fuzz)
    # libFuzzer's Windows weak aliases use Clang builtins and MS extensions.
    # Supply MinGW's C++ headers explicitly for the Linux-hosted Clang driver.
    cxx_headers=$(x86_64-w64-mingw32-g++ -print-file-name=include)/c++
    cxx_target=$(x86_64-w64-mingw32-g++ -dumpmachine)
    timed windows-fuzz env CXX_x86_64_pc_windows_gnu=clang++ \
        CXXFLAGS_x86_64_pc_windows_gnu="-fms-extensions -isystem $cxx_headers -isystem $cxx_headers/$cxx_target -isystem $cxx_headers/backward" \
        cargo check --locked -p acyclic-harness-fuzz --bin records \
        --target x86_64-pc-windows-gnu
    ;;
fuzz)
    fuzz_target="${CARGO_TARGET_DIR:-$PWD/target}-nightly-incarnation-fuzz"
    timed fuzz-build env CARGO_TARGET_DIR="$fuzz_target" \
        cargo +nightly-2026-07-01 fuzz build --dev --target x86_64-unknown-linux-gnu \
        --fuzz-dir rust/crates/harness-fuzz records
    python3 - "$fuzz_target/x86_64-unknown-linux-gnu/debug/records" <<'PYFUZZ'
import re,subprocess,sys,time
start=time.monotonic()
result=subprocess.run([sys.argv[1],"rust/crates/harness-fuzz/corpus/records",
                       "-runs=64","-max_len=4096","-timeout=2","-seed=1"],
                      timeout=9.5,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
print(f"record corpus: {time.monotonic()-start:.3f}s\n{result.stdout}",flush=True)
assert result.returncode==0,result.returncode
runs=re.search(r"Done ([1-9][0-9]*) runs in",result.stdout)
assert runs and int(runs[1])>=64,result.stdout
PYFUZZ
    ;;
darwin)
    # Run on the macOS host: Linux and Windows cannot prove F_FULLFSYNC on
    # regular files and the directories used for checkout token publication.
    artifacts=$(mktemp)
    trap 'rm -f "$artifacts"' EXIT
    timed darwin-build cargo test --locked -p acyclic-native-runtime --lib \
        --no-run --message-format=json >"$artifacts"
    run_built acyclic_native_runtime rust/crates/native-runtime held_sync::tests
    ;;
*) echo "usage: $0 [all|static|native|model|windows|windows-base|windows-cli|windows-fuzz|fuzz|darwin]" >&2; exit 2 ;;
esac
