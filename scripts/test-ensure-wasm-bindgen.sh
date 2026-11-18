#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
subject="$root/scripts/ensure-wasm-bindgen.sh"
work=$(mktemp -d "${TMPDIR:-/tmp}/acyclic-wasm-bindgen-test.XXXXXXXX")
trap 'rm -rf -- "$work"' EXIT

case "$(bun -e 'process.stdout.write(process.platform + ":" + process.arch)')" in
  linux:x64) archive=x86_64-unknown-linux-musl ;;
  linux:arm64) archive=aarch64-unknown-linux-musl ;;
  darwin:x64) archive=x86_64-apple-darwin ;;
  darwin:arm64) archive=aarch64-apple-darwin ;;
  win32:x64) archive=x86_64-pc-windows-msvc ;;
  *) exit 2 ;;
esac

# Run in a native toolchain environment without the pinned CLI on PATH so
# setup must exercise the downloaded executable.
for binary in wasm-bindgen wasm-bindgen.exe; do
  if [[ "$("$binary" --version 2>/dev/null || true)" == 'wasm-bindgen 0.2.117' ]]; then
    printf 'Remove %s 0.2.117 from PATH before running this check\n' "$binary" >&2
    exit 2
  fi
done

export TOOLS_DIR="$work/valid"
installed=$("$BASH" "$subject")
[[ "$installed" == *"wasm-bindgen-0.2.117-$archive"* ]]
[[ -f "$TOOLS_DIR/wasm-bindgen-0.2.117-$archive.tar.gz" ]]

cat > "$work/fetch" <<'EOF'
#!/usr/bin/env bash
printf 'fetch called\n' > "$FETCH_MARKER"
printf 'invalid release archive\n' > "$3"
EOF
chmod +x "$work/fetch"
export PINNED_ASSET_FETCHER="$work/fetch"
export FETCH_MARKER="$work/fetch-called"
[[ "$("$BASH" "$subject")" == "$installed" ]]
[[ ! -e "$FETCH_MARKER" ]]

export TOOLS_DIR="$work/invalid"
if "$BASH" "$subject" > "$work/result" 2>&1; then
  printf 'Corrupt wasm-bindgen archive was accepted\n' >&2
  exit 1
fi
[[ -f "$FETCH_MARKER" ]]
grep -F 'step=fetch pinned wasm-bindgen' "$work/result"
[[ -z "$(find "$TOOLS_DIR" -type f -print)" ]]
