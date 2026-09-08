#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
subject="$root/scripts/ensure-rust-target.sh"
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-rust-target-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
fake_bin="$work/bin"
mkdir -p "$fake_bin"

cat > "$fake_bin/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-} ${2:-}" in
  "target list")
    [[ "${3:-}" == "--installed" ]]
    if [[ -f "$FAKE_STATE/installed" ]]; then
      if [[ "${FAKE_LIST_CRLF:-0}" == 1 ]]; then
        printf '%s\r\n' "$FAKE_TARGET"
      else
        printf '%s\n' "$FAKE_TARGET"
      fi
    fi
    ;;
  "target add")
    [[ "${3:-}" == "$FAKE_TARGET" ]]
    printf '%s\n' add >> "$FAKE_STATE/add-calls"
    if [[ "${FAKE_ADD_MODE:-conflict}" == network ]]; then
      echo 'network request failed' >&2
      exit 1
    fi
    target_dir="$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
    if [[ -d "$target_dir" ]]; then
      echo "error: detected conflict: lib/rustlib/$FAKE_TARGET/lib/libaddr2line.rlib" >&2
      exit 1
    fi
    manifest="$FAKE_FS_SYSROOT/lib/rustlib/manifest-rust-std-$FAKE_TARGET"
    if [[ -e "$manifest" ]]; then
      echo "error: detected conflict: lib/rustlib/manifest-rust-std-$FAKE_TARGET" >&2
      exit 1
    fi
    mkdir -p "$target_dir"
    : > "$FAKE_STATE/installed"
    ;;
  "show home")
    printf '%s\n' "$FAKE_HOME_OUTPUT"
    ;;
  *) exit 64 ;;
esac
EOF

cat > "$fake_bin/rustc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-} ${2:-}" == "--print sysroot" ]]
printf '%s\n' "$FAKE_SYSROOT_OUTPUT"
EOF

cat > "$fake_bin/wslpath" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == -u && $# == 2 ]]
case "$2" in
  "$FAKE_HOME_OUTPUT") printf '%s\n' "$FAKE_FS_HOME" ;;
  "$FAKE_SYSROOT_OUTPUT") printf '%s\n' "$FAKE_FS_SYSROOT" ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$fake_bin/rustup" "$fake_bin/rustc" "$fake_bin/wslpath"

prepare_case() {
  FAKE_STATE="$work/$1"
  FAKE_FS_HOME="$FAKE_STATE/home"
  FAKE_FS_SYSROOT="$FAKE_FS_HOME/toolchains/stable"
  FAKE_HOME_OUTPUT="$FAKE_FS_HOME"
  FAKE_SYSROOT_OUTPUT="$FAKE_FS_SYSROOT"
  FAKE_TARGET=wasm32-unknown-unknown
  FAKE_ADD_MODE=conflict
  FAKE_LIST_CRLF=0
  mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib"
}

invoke() {
  FAKE_STATE="$FAKE_STATE" \
  FAKE_FS_HOME="$FAKE_FS_HOME" \
  FAKE_FS_SYSROOT="$FAKE_FS_SYSROOT" \
  FAKE_HOME_OUTPUT="$FAKE_HOME_OUTPUT" \
  FAKE_SYSROOT_OUTPUT="$FAKE_SYSROOT_OUTPUT" \
  FAKE_TARGET="$FAKE_TARGET" \
  FAKE_ADD_MODE="$FAKE_ADD_MODE" \
  FAKE_LIST_CRLF="$FAKE_LIST_CRLF" \
  PATH="$fake_bin:$PATH" \
    bash "$subject" "$FAKE_TARGET" "$fake_bin/rustup" "$fake_bin/rustc"
}

expect_failure() {
  if invoke; then
    echo "expected Rust target repair to fail: $1" >&2
    exit 1
  fi
}

prepare_case installed
: > "$FAKE_STATE/installed"
FAKE_LIST_CRLF=1
invoke
[[ ! -e "$FAKE_STATE/add-calls" ]]

prepare_case conflict
mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/lib"
: > "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/lib/libaddr2line.rlib"
: > "$FAKE_FS_SYSROOT/lib/rustlib/manifest-rust-std-$FAKE_TARGET"
: > "$FAKE_STATE/unrelated"
invoke
[[ "$(wc -l < "$FAKE_STATE/add-calls" | tr -d ' ')" == 2 ]]
[[ -f "$FAKE_STATE/installed" && -d "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET" ]]
[[ ! -e "$FAKE_FS_SYSROOT/lib/rustlib/manifest-rust-std-$FAKE_TARGET" ]]
[[ -f "$FAKE_STATE/unrelated" ]]

prepare_case unrelated-error
mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
: > "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/partial"
FAKE_ADD_MODE=network
expect_failure unrelated-error
[[ -f "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/partial" ]]

prepare_case symlink-target
outside_target="$FAKE_STATE/outside-target"
mkdir -p "$outside_target"
: > "$outside_target/preserved"
ln -s "$outside_target" "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
expect_failure symlink-target
[[ -f "$outside_target/preserved" && -L "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET" ]]

prepare_case symlink-manifest
mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
outside_manifest="$FAKE_STATE/outside-manifest"
: > "$outside_manifest"
ln -s "$outside_manifest" "$FAKE_FS_SYSROOT/lib/rustlib/manifest-rust-std-$FAKE_TARGET"
expect_failure symlink-manifest
[[ -f "$outside_manifest" && -L "$FAKE_FS_SYSROOT/lib/rustlib/manifest-rust-std-$FAKE_TARGET" ]]
[[ -d "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET" ]]

prepare_case redirected-rustlib
outside_rustlib="$FAKE_STATE/outside-rustlib"
mkdir -p "$outside_rustlib/$FAKE_TARGET"
: > "$outside_rustlib/$FAKE_TARGET/preserved"
rmdir "$FAKE_FS_SYSROOT/lib/rustlib"
ln -s "$outside_rustlib" "$FAKE_FS_SYSROOT/lib/rustlib"
expect_failure redirected-rustlib
[[ -f "$outside_rustlib/$FAKE_TARGET/preserved" ]]

prepare_case outside-toolchain
FAKE_FS_SYSROOT="$FAKE_STATE/outside-toolchain"
FAKE_SYSROOT_OUTPUT="$FAKE_FS_SYSROOT"
mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
: > "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/preserved"
expect_failure outside-toolchain
[[ -f "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET/preserved" ]]

prepare_case windows-paths
mkdir -p "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET"
FAKE_HOME_OUTPUT='C:/fake/rustup-home'
FAKE_SYSROOT_OUTPUT='C:/fake/rustup-home/toolchains/stable'
invoke
[[ -f "$FAKE_STATE/installed" && -d "$FAKE_FS_SYSROOT/lib/rustlib/$FAKE_TARGET" ]]

echo 'ensure-rust-target tests passed'
