#!/usr/bin/env bash
# Focused checkout identity gate. Invoke through the shared build slot with
# --env BUILD_SLOT_TIER=qual: cold model builds can exceed the lane job ceiling.
set -euo pipefail
cd "$(dirname "$0")/.."
features=filesystem-local,native-execution,terminal,tui,test-support,grpc,search-http
harness_features="acyclic-harness/${features//,/,acyclic-harness/}"
timed() {
    local label=$1
    shift
    TIMEFORMAT="$label: %3R s"
    time "$@"
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
    binary() {
        python3 -c 'import json,sys
for line in open(sys.argv[1]):
    item=json.loads(line)
    if item.get("executable") and item["target"]["name"]==sys.argv[2]:
        print(item["executable"])' "$artifacts" "$1"
    }
    filesystem=$(binary acyclic_fs)
    harness=$(binary acyclic_harness)
    (cd rust/crates/filesystem; timed filesystem "$filesystem" \
        native_capture::held_root_tests native_host::held_files::record native_host::leaf \
        native_host::interrupted_sync_tests \
        native_exchange::tests::exchange_refuses_normalized_directory_aliases \
        demand::tests::a_held_root \
        demand::tests::negative_control_binding)
    (cd rust/crates/harness; timed incarnation "$harness" \
        local::tests::checkout_incarnation local::owner:: local::owner_record::)
    (cd rust/crates/harness; timed reopen "$harness" local::tests::checkout_reopen)
    (cd rust/crates/harness; timed setup "$harness" \
        local::tests::checkout::checkout_without_bash \
        local::tests::volumes::bash_crosses_granted \
        local::workspace::volumes::recovery_tests)
    timed cli cargo test --locked -p graphcoder --test cli --features "$harness_features" \
        recreated_checkout_runs_a_fresh_operation_through_the_real_cli_and_http
    ;;
model)
    # This cfg, never a feature or build.rs decision, instruments the actual
    # read/rename/flush operations used by HeldRoot::read_or_publish.
    timed model env RUSTFLAGS="${RUSTFLAGS:-} --cfg acyclic_fs_native_loom" \
        cargo test --locked -p acyclic-fs --lib native_host::held_files::models
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
    timed fuzz cargo +nightly-2026-07-01 check --locked -p acyclic-harness-fuzz --bin records
    ;;
darwin)
    # Run on the macOS host: Linux and Windows cannot prove F_FULLFSYNC on
    # regular files and the directories used for checkout token publication.
    timed darwin cargo test --locked -p acyclic-native-runtime --lib held_sync::tests
    ;;
*) echo "usage: $0 [all|static|native|model|windows|windows-base|windows-cli|windows-fuzz|fuzz|darwin]" >&2; exit 2 ;;
esac
