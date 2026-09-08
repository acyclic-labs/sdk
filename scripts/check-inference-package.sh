#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if command -v wslpath >/dev/null 2>&1; then
  windows_temp="$(cmd.exe /d /c echo %TEMP% | tr -d '\r')"
  work="$(mktemp -d "$(wslpath -u "$windows_temp")/sdk-inference-package.XXXXXXXX")"
else
  work="$(mktemp -d)"
fi
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
typescript_archive="$work/acyclic-inference.tgz"
bun_archive="$typescript_archive"
bun_archive_url="$typescript_archive"
if command -v cygpath >/dev/null 2>&1; then
  bun_archive="$(cygpath -w "$typescript_archive")"
  bun_archive_url="$(cygpath -m "$typescript_archive")"
elif command -v wslpath >/dev/null 2>&1; then
  bun_archive="$(wslpath -w "$typescript_archive")"
  bun_archive_url="$(wslpath -m "$typescript_archive")"
fi

cd "$root"
bun run --filter '@acyclic/inference' build
bun test typescript/packages/inference/test
cd typescript/packages/inference
bun pm pack --ignore-scripts --filename "$bun_archive" --quiet
mkdir "$work/consumer"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic/inference":"file:$bun_archive_url"}}
EOF
cat >"$work/consumer/smoke.mjs" <<'EOF'
import { InferenceClient, ListModelsResponseSchema } from "@acyclic/inference";
import { RunViewSchema } from "@acyclic/inference/proto";
if (ListModelsResponseSchema.typeName !== "inference.customer.v1.ListModelsResponse" ||
    RunViewSchema.typeName !== "inference.customer.v1.RunView") throw new Error("inference schemas missing");
const client = new InferenceClient({
  async listModels() { return { $typeName: "inference.customer.v1.ListModelsResponse", models: [] }; },
});
if ((await client.listModels()).models.length !== 0) throw new Error("inference client did not execute");
EOF
cd "$work/consumer"
bun install --ignore-scripts
bun smoke.mjs

cd "$root"
source_root="$work/source"
test_root="$work/test"
mkdir -p "$source_root" "$test_root"
git archive HEAD | tar -x -C "$source_root"

cargo_bin="cargo"
source_manifest="$source_root/Cargo.toml"
package_target="$work/package-target"
package_target_argument="$package_target"
if command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
  source_manifest="$(wslpath -w "$source_manifest")"
  package_target_argument="$(wslpath -w "$package_target")"
fi
version="$("$cargo_bin" metadata --no-deps --format-version 1 --manifest-path "$source_manifest" | python3 -c 'import json,sys; print(next(package["version"] for package in json.load(sys.stdin)["packages"] if package["name"] == "inference-sdk"))')"
"$cargo_bin" package --locked --no-verify -p inference-sdk --manifest-path "$source_manifest" --target-dir "$package_target_argument"
crate="$package_target/package/inference-sdk-${version}.crate"
actual="$(sha256sum "$crate" | cut -d ' ' -f 1)"
expected="$(python3 - "$version" <<'PY'
import json, pathlib, sys

entries = [
    json.loads(line)
    for line in pathlib.Path("registry/in/fe/inference-sdk").read_text(encoding="utf-8").splitlines()
    if line
]
matches = [entry for entry in entries if entry["vers"] == sys.argv[1]]
if len(matches) != 1:
    raise SystemExit("sparse index must contain exactly one current inference-sdk release")
print(matches[0]["cksum"])
PY
)"
test "$actual" = "$expected"

tar -xf "$crate" -C "$test_root"
test_manifest="$test_root/inference-sdk-${version}/Cargo.toml"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  test_manifest="$(wslpath -w "$test_manifest")"
fi
"$cargo_bin" test --manifest-path "$test_manifest"

if [[ "$#" -eq 1 ]]; then
  output="$1"
  mkdir -p "$output"
  install -m 0644 "$crate" "$typescript_archive" "$output/"
  (cd "$output" && sha256sum "$(basename "$crate")" acyclic-inference.tgz > SHA256SUMS)
elif [[ "$#" -ne 0 ]]; then
  echo "usage: check-inference-package.sh [OUTPUT]" >&2
  exit 2
fi
