#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
artifacts=$(mktemp -d)
trap 'rm -rf "$artifacts"' EXIT
# Separate compilation from timing: the ten-second bound covers every runtime
# proof and source control, without counting compiler or admission time.
compile() {
  local output=$1
  shift
  if "$@" > "$output"; then return; fi
  python3 scripts/qualify-graphcoder-artifact.py deadline:: < "$output"
  return 1
}
run_timed() {
  /usr/bin/time -a -o "$artifacts/timings" -f '%e' "$@"
}
run_timed python3 scripts/qualify-dst-waits.py
compile "$artifacts/native" cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json
run_timed python3 scripts/qualify-graphcoder-artifact.py deadline:: tests::completion::measured_sync_wait process_tree::tests::owned_process_wait_expires_on_virtual_time_and_retains_cleanup_custody process_tree::tests::capture_timeout_and_output_overflow_reap_owned_tree < "$artifacts/native"
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg acyclic_native_runtime_loom" \
  compile "$artifacts/model" cargo test --locked -p acyclic-native-runtime --features test-support --lib --no-run --message-format=json
run_timed python3 scripts/qualify-graphcoder-artifact.py deadline:: testing::queue_model::physical_completion testing::queue_model::broken_terminal < "$artifacts/model"
compile "$artifacts/filesystem" cargo test --locked -p acyclic-fs --features test-support --lib --no-run --message-format=json
filesystem_filters=(core_state::tests::nodes::held_record_lock core_state::tests::nodes::record_lock_retry watch::deadline_tests:: watch::tests::fence_queues_every_completed_write watch::tests::live_native_backend_fences)
case "$(uname -s)" in
  Darwin) filesystem_filters+=(native_mount::darwin_mount::tests::deadline::) ;;
  MINGW*|MSYS*|CYGWIN*) filesystem_filters+=(native_mount::projfs::projector::deadline_tests::) ;;
esac
run_timed python3 scripts/qualify-graphcoder-artifact.py "${filesystem_filters[@]}" < "$artifacts/filesystem"
python3 - "$artifacts/timings" <<'PY'
from pathlib import Path
import sys
elapsed = sum(map(float, Path(sys.argv[1]).read_text().splitlines()))
assert elapsed < 10, f"DST wait proofs exceeded 10s: {elapsed:.3f}s"
print(f"DST wait proofs: {elapsed:.3f}s total runtime")
PY
cargo check --locked -p acyclic-native-runtime -p acyclic-fs --lib --tests --features acyclic-native-runtime/test-support,acyclic-fs/test-support --target x86_64-pc-windows-gnu
