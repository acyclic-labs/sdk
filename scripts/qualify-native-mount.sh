#!/usr/bin/env bash
set -euo pipefail

platform="${1:?usage: qualify-native-mount.sh linux-fuse|macos-nfs}"
case "$platform" in
  linux-fuse)
    [[ "$(uname -s)" == Linux ]] || { echo 'linux-fuse requires Linux' >&2; exit 2; }
    [[ -c /dev/fuse && -r /dev/fuse && -w /dev/fuse ]] || {
      echo '{"code":"native_mount_prerequisite_missing","backend":"linux-fuse","reason":"/dev/fuse is not a readable and writable character device"}' >&2
      exit 2
    }
    command -v fusermount3 >/dev/null || {
      echo '{"code":"native_mount_prerequisite_missing","backend":"linux-fuse","reason":"fusermount3 is not installed"}' >&2
      exit 2
    }
    ;;
  macos-nfs)
    [[ "$(uname -s)" == Darwin ]] || { echo 'macos-nfs requires macOS' >&2; exit 2; }
    [[ -x /sbin/mount_nfs && -x /sbin/umount ]] || {
      echo '{"code":"native_mount_prerequisite_missing","backend":"macos-nfs","reason":"/sbin/mount_nfs or /sbin/umount is unavailable"}' >&2
      exit 2
    }
    ;;
  *) echo "unknown native mount backend: $platform" >&2; exit 2 ;;
esac

repository="$(git rev-parse --show-toplevel)"
artifact_dir="${SDK_ARTIFACT_DIR:-${RUNNER_TEMP:-/tmp}/acyclic-native-mount}"
mkdir -p "$artifact_dir"
if [[ -n "${ACYCLIC_RELEASE_EXECUTABLE:-}" ]]; then
  release_executable="$ACYCLIC_RELEASE_EXECUTABLE"
else
  cargo build --locked -p acyclic-plugin --release
  release_executable="${CARGO_TARGET_DIR:-$repository/target}/release/acyclic"
fi
[[ -x "$release_executable" ]] || {
  echo "release executable is unavailable or not executable: $release_executable" >&2
  exit 2
}
arguments=(
  --require-kind "$platform"
  --release-executable "$release_executable"
  --checkout-root "$repository"
  --output "$artifact_dir/$platform.json"
)
if [[ -n "${ACYCLIC_QUALIFIER_EXECUTABLE:-}" ]]; then
  [[ -x "$ACYCLIC_QUALIFIER_EXECUTABLE" ]] || {
    echo "qualification executable is unavailable or not executable: $ACYCLIC_QUALIFIER_EXECUTABLE" >&2
    exit 2
  }
  "$ACYCLIC_QUALIFIER_EXECUTABLE" "${arguments[@]}"
else
  cargo run --locked -p acyclic-conformance --features local-runner \
    --bin native-mount-qualify -- "${arguments[@]}"
fi
