#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-gate-rustup-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin"

cat >"$work/bin/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_RUSTUP_LOG"
case "${1:-} ${2:-}" in
  'component list')
    [[ "$FAKE_RUSTUP_MODE" == healthy ]] && printf '%s\n' 'llvm-tools-x86_64-unknown-linux-gnu (installed)'
    ;;
  'component add')
    if [[ "$FAKE_RUSTUP_MODE" == network ]]; then
      echo 'network unavailable' >&2
    else
      echo 'error: detected conflict: file already exists: llc' >&2
    fi
    exit 1
    ;;
  'show active-toolchain')
    if [[ "$FAKE_RUSTUP_MODE" == unexpected ]]; then
      echo 'nightly-x86_64-unknown-linux-gnu (overridden)'
    else
      echo '1.94.0-x86_64-unknown-linux-gnu (overridden)'
    fi
    ;;
  'toolchain uninstall'|'toolchain install') ;;
  *) exit 2 ;;
esac
EOF

cat >"$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${FAKE_CARGO_LOG:-/dev/null}"
exit "${FAKE_CARGO_STATUS:-0}"
EOF
cat >"$work/bin/cargo-llvm-cov" <<'EOF'
#!/usr/bin/env bash
[[ "$*" == 'llvm-cov --version' ]] && echo 'cargo-llvm-cov 0.9.1'
exit 0
EOF
chmod +x "$work/bin/rustup" "$work/bin/cargo" "$work/bin/cargo-llvm-cov"

invoke() {
  local mode="$1" case_dir="$work/$1"
  mkdir -p "$case_dir/temp" "$case_dir/artifacts" "$case_dir/tools/cargo/bin"
  cp "$work/bin/cargo-llvm-cov" "$case_dir/tools/cargo/bin/cargo-llvm-cov"
  : >"$case_dir/rustup.log"
  FAKE_RUSTUP_MODE="$mode" FAKE_RUSTUP_LOG="$case_dir/rustup.log" \
    FORCE=true SDK_TEMP_DIR="$case_dir/temp" \
    SDK_ARTIFACT_DIR="$case_dir/artifacts" \
    TOOLS_DIR="$case_dir/tools" PATH="$work/bin:$PATH" \
    bash "$root/scripts/qualify-ci.sh" gate
}

invoke healthy
! grep -q '^component add ' "$work/healthy/rustup.log"

invoke conflict
grep -Fxq 'toolchain uninstall 1.94.0-x86_64-unknown-linux-gnu' "$work/conflict/rustup.log"
grep -Fq 'toolchain install 1.94.0-x86_64-unknown-linux-gnu --profile minimal --component clippy --component rustfmt --component llvm-tools-preview' "$work/conflict/rustup.log"
! find "$work/conflict/temp" -name 'rustup-component.*' -print -quit | grep -q .

if invoke network; then
  echo 'unrelated component failure unexpectedly triggered recovery' >&2
  exit 1
fi
! grep -q '^toolchain uninstall ' "$work/network/rustup.log"
! find "$work/network/temp" -name 'rustup-component.*' -print -quit | grep -q .

if invoke unexpected; then
  echo 'unexpected toolchain identity was accepted' >&2
  exit 1
fi
! grep -q '^toolchain uninstall ' "$work/unexpected/rustup.log"
! find "$work/unexpected/temp" -name 'rustup-component.*' -print -quit | grep -q .

echo 'gate rustup recovery tests passed'

# Ordinary core checks never provision coverage tooling or run downstream
# packages; a failed Rust test must not leave a success receipt.
for status in 0 9; do
  core="$work/core-$status"
  mkdir -p "$core/temp" "$core/artifacts" "$core/tools"
  : >"$core/rustup.log"
  : >"$core/cargo.log"
  result=0
  FORCE=false FAKE_CARGO_STATUS="$status" FAKE_CARGO_LOG="$core/cargo.log" \
    FAKE_RUSTUP_MODE=healthy FAKE_RUSTUP_LOG="$core/rustup.log" \
    SDK_TEMP_DIR="$core/temp" SDK_ARTIFACT_DIR="$core/artifacts" \
    TOOLS_DIR="$core/tools" PATH="$work/bin:$PATH" \
    bash "$root/scripts/qualify-ci.sh" gate || result=$?
  [[ "$result" == "$status" ]]
  [[ ! -s "$core/rustup.log" ]]
  grep -Fxq 'test -p acyclic-sdk-contract-wire --locked --lib --bins' "$core/cargo.log"
  if [[ "$status" == 0 ]]; then
    grep -q '"coverage_instrumented":false' "$core/artifacts/coverage/core-check.json"
  else
    [[ ! -e "$core/artifacts/coverage/core-check.json" ]]
  fi
done
echo 'core gate scope and failure propagation tests passed'
