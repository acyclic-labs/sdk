#!/usr/bin/env bash
set -euo pipefail

[[ $# -ge 1 && $# -le 3 ]] || {
  echo 'usage: ensure-rust-target.sh TARGET [RUSTUP [RUSTC]]' >&2
  exit 2
}
target="$1"
rustup_bin="${2:-rustup}"
rustc_bin="${3:-rustc}"
[[ "$target" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$ ]] || {
  echo 'invalid Rust target name' >&2
  exit 2
}

installed="$("$rustup_bin" target list --installed | tr -d '\r')"
if grep -Fqx "$target" <<<"$installed"; then
  exit 0
fi

rustup_log="$(mktemp -t acyclic-rust-target.XXXXXXXX)"
trap 'rm -f -- "$rustup_log"' EXIT
if "$rustup_bin" target add "$target" 2>&1 | tee "$rustup_log"; then
  exit 0
fi
grep -Eqi 'detected conflict|could not rename|File exists' "$rustup_log" || exit 1

sysroot="$("$rustc_bin" --print sysroot | tr -d '\r')"
rustup_home="$("$rustup_bin" show home | tr -d '\r')"
if [[ "$sysroot" =~ ^[A-Za-z]:[\\/].* || "$rustup_home" =~ ^[A-Za-z]:[\\/].* ]]; then
  [[ "$sysroot" =~ ^[A-Za-z]:[\\/].* && "$rustup_home" =~ ^[A-Za-z]:[\\/].* ]] || {
    echo 'refusing to repair inconsistent Windows Rust paths' >&2
    exit 1
  }
  if command -v cygpath >/dev/null 2>&1; then
    sysroot="$(cygpath -u "$sysroot")"
    rustup_home="$(cygpath -u "$rustup_home")"
  elif command -v wslpath >/dev/null 2>&1; then
    sysroot="$(wslpath -u "$sysroot")"
    rustup_home="$(wslpath -u "$rustup_home")"
  else
    echo 'cannot resolve the Windows Rust sysroot from this shell' >&2
    exit 1
  fi
fi
[[ -d "$sysroot" && -d "$rustup_home" ]] || {
  echo 'refusing to repair an invalid Rust toolchain' >&2
  exit 1
}
sysroot="$(cd "$sysroot" && pwd -P)"
rustup_home="$(cd "$rustup_home" && pwd -P)"
[[ "$sysroot" == "$rustup_home"/toolchains/* ]] || {
  echo 'refusing to repair a Rust target outside the rustup toolchain root' >&2
  exit 1
}
rustlib="$sysroot/lib/rustlib"
[[ -d "$rustlib" && ! -L "$rustlib" && "$rustlib" != / ]] || {
  echo 'refusing to repair an invalid Rust sysroot' >&2
  exit 1
}
rustlib="$(cd "$rustlib" && pwd -P)"
[[ "$rustlib" == "$sysroot/lib/rustlib" ]] || {
  echo 'refusing to repair a Rust target through a redirected library path' >&2
  exit 1
}
target_dir="$rustlib/$target"
[[ "$(dirname "$target_dir")" == "$rustlib" \
  && "$(basename "$target_dir")" == "$target" \
  && -d "$target_dir" && ! -L "$target_dir" ]] || {
  echo 'refusing to repair an unexpected Rust target path' >&2
  exit 1
}
manifest="$rustlib/manifest-rust-std-$target"
[[ "$(dirname "$manifest")" == "$rustlib" \
  && "$(basename "$manifest")" == "manifest-rust-std-$target" ]] || {
  echo 'refusing to repair an unexpected Rust target manifest path' >&2
  exit 1
}
if [[ -e "$manifest" || -L "$manifest" ]]; then
  [[ -f "$manifest" && ! -L "$manifest" ]] || {
    echo 'refusing to repair an unexpected Rust target manifest' >&2
    exit 1
  }
fi
rm -rf -- "$target_dir"
rm -f -- "$manifest"
"$rustup_bin" target add "$target"
