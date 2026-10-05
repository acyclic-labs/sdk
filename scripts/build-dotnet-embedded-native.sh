#!/usr/bin/env bash
set -euo pipefail

root=
target=
output=
target_dir=
while (($#)); do
  case "$1" in
    --root) root=$2; shift 2 ;;
    --target) target=$2; shift 2 ;;
    --output) output=$2; shift 2 ;;
    --target-dir) target_dir=$2; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
: "${root:?--root is required}"
: "${target:?--target is required}"
: "${output:?--output is required}"
root=$(cd "$root" && pwd)
mkdir -p "$output"
source_revision=$(git -C "$root" rev-parse HEAD)
[[ "$source_revision" =~ ^[0-9a-fA-F]{40}$ ]]
closure_json="$output/source-closure.json"
python3 "$root/scripts/embedded-source-closure.py" \
  --repository "$root" \
  --manifest "$root/rust/crates/sdk-embedded-prototype/Cargo.toml" \
  --output "$closure_json"
source_inputs_sha256=$(python3 - "$closure_json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding='utf-8'))['digest'])
PY
)
source_inputs_json=$(python3 - "$closure_json" <<'PY'
import json
import sys
print(json.dumps(json.load(open(sys.argv[1], encoding='utf-8'))['paths'], separators=(',', ':')))
PY
)
lockfile_sha256=$(sha256sum "$root/rust/crates/sdk-embedded-prototype/Cargo.lock" | awk '{print $1}')
cargo_command='cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
target_dir=${target_dir:-"$output/rust-target"}
manifest="$root/rust/crates/sdk-embedded-prototype/Cargo.toml"

case "$target" in
  x86_64-unknown-linux-musl)
    rid=linux-musl-x64
    file=libacyclic_sdk_embedded_prototype.so
    compiler=${MUSL_CC:-$(command -v x86_64-linux-musl-gcc || command -v musl-gcc || { test -x /usr/lib/musl/bin/musl-gcc && printf '%s' /usr/lib/musl/bin/musl-gcc; } || command -v gcc || true)}
    expected_machine='Advanced Micro Devices X86-64'
    ;;
  aarch64-unknown-linux-musl)
    rid=linux-musl-arm64
    file=libacyclic_sdk_embedded_prototype.so
    compiler=${MUSL_CC:-$(command -v aarch64-linux-musl-gcc || command -v musl-gcc || { test -x /usr/lib/musl/bin/musl-gcc && printf '%s' /usr/lib/musl/bin/musl-gcc; } || command -v gcc || true)}
    expected_machine='AArch64'
    ;;
  *)
    echo "unsupported musl target: $target" >&2
    exit 2
    ;;
esac
: "${compiler:?no musl compiler found for $target}"
linker_var="CARGO_TARGET_${target^^}_LINKER"
linker_var=${linker_var//-/_}
rustup target add "$target"
export RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=-crt-static"
export "$linker_var=$compiler"
cargo build --locked --release --manifest-path "$manifest" --target "$target" --target-dir "$target_dir"
binary="$target_dir/$target/release/$file"
test -f "$binary"
machine=$(readelf -h "$binary" | sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
test "$machine" = "$expected_machine"
readelf -d "$binary" | grep -Eiq 'libc\.musl|ld-musl'
destination="$output/native/$rid"
mkdir -p "$destination"
cp "$binary" "$destination/$file"
sha256=$(sha256sum "$destination/$file" | awk '{print $1}')
bytes=$(stat -c '%s' "$destination/$file")
cat > "$output/native/$rid/native-producer.json" <<EOF
{
  "schema": "acyclic.sdk.dotnet.embedded.native.v1",
  "source_revision": "$source_revision",
  "source_revision_kind": "git-oid",
  "source_inputs": $source_inputs_json,
  "source_inputs_sha256": "$source_inputs_sha256",
  "cargo_manifest": "rust/crates/sdk-embedded-prototype/Cargo.toml",
  "cargo_lock": "rust/crates/sdk-embedded-prototype/Cargo.lock",
  "cargo_lock_sha256": "$lockfile_sha256",
  "cargo_command": "$cargo_command",
  "rust_target": "$target",
  "rid": "$rid",
  "file": "$file",
  "machine": "$machine",
  "dynamic_musl": true,
  "sha256": "$sha256",
  "bytes": $bytes
}
EOF
cat > "$output/native/native-manifest.json" <<EOF
{
  "schema": "acyclic.sdk.dotnet.embedded.native-manifest.v1",
  "source_revision": "$source_revision",
  "source_revision_kind": "git-oid",
  "source_inputs": $source_inputs_json,
  "source_inputs_sha256": "$source_inputs_sha256",
  "cargo_manifest": "rust/crates/sdk-embedded-prototype/Cargo.toml",
  "cargo_lock": "rust/crates/sdk-embedded-prototype/Cargo.lock",
  "cargo_lock_sha256": "$lockfile_sha256",
  "cargo_command": "$cargo_command",
  "assets": [
    {
      "rust_target": "$target",
      "rid": "$rid",
      "file": "$file",
      "machine": "$machine",
      "dynamic_musl": true,
      "sha256": "$sha256",
      "bytes": $bytes
    }
  ]
}
EOF
echo "staged $target -> $destination/$file"
