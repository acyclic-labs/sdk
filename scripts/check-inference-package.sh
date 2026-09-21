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

cd "$root"
expected_source="${CI_HEAD_SHA:-${GITHUB_SHA:-}}"
source_sha=$(git rev-parse --verify HEAD)
if [[ -n "$expected_source" && "$source_sha" != "$expected_source" ]]; then
  echo "package checkout differs from the selected source commit" >&2
  exit 1
fi
git_status="$work/git-status"
git status --porcelain=v1 --untracked-files=all >"$git_status"
if [[ -s "$git_status" ]]; then
  echo "package checkout is not clean" >&2
  exit 1
fi
git_index="$work/git-index"
git ls-files -v >"$git_index"
if grep -Eq '^[a-zS] ' "$git_index"; then
  echo "package checkout contains concealed index changes" >&2
  exit 1
fi

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
bun run --filter '@acyclic-labs/inference' build
bun test typescript/packages/inference/test
npm_stage="$work/npm-package"
cp -R typescript/packages/inference "$npm_stage"
install -m 0644 CHANGELOG.md "$npm_stage/CHANGELOG.md"
cd "$npm_stage"
bun pm pack --ignore-scripts --filename "$bun_archive" --quiet
mkdir "$work/consumer"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic-labs/inference":"file:$bun_archive_url"}}
EOF
cat >"$work/consumer/smoke.mjs" <<'EOF'
import { InferenceClient, ListModelsResponseSchema } from "@acyclic-labs/inference";
import { RunViewSchema } from "@acyclic-labs/inference/proto";
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
test_root="$work/test"
mkdir -p "$test_root"

cargo_bin="cargo"
source_manifest="$root/Cargo.toml"
package_target="$work/package-target"
package_target_argument="$package_target"
if command -v wslpath >/dev/null 2>&1 && command -v cargo.exe >/dev/null 2>&1; then
  cargo_bin="cargo.exe"
  source_manifest="$(wslpath -w "$source_manifest")"
  package_target_argument="$(wslpath -w "$package_target")"
fi
version="$("$cargo_bin" metadata --no-deps --format-version 1 --manifest-path "$source_manifest" | node -e 'let input=""; process.stdin.on("data", chunk => input += chunk).on("end", () => console.log(JSON.parse(input).packages.find(item => item.name === "acyclic-inference").version))')"
"$cargo_bin" package --locked --no-verify -p acyclic-inference --manifest-path "$source_manifest" --target-dir "$package_target_argument"
crate="$package_target/package/acyclic-inference-${version}.crate"

tar -xf "$crate" -C "$test_root"
test_manifest="$test_root/acyclic-inference-${version}/Cargo.toml"
if [[ "$cargo_bin" == "cargo.exe" ]]; then
  test_manifest="$(wslpath -w "$test_manifest")"
fi
"$cargo_bin" test --manifest-path "$test_manifest"

if [[ "$#" -eq 1 ]]; then
  output="$1"
  mkdir -p "$output"
  install -m 0644 "$crate" "$typescript_archive" "$output/"
  (cd "$output" && sha256sum "$(basename "$crate")" acyclic-inference.tgz > SHA256SUMS)
  printf '%s\n' "$source_sha" >"$output/SOURCE_COMMIT"
elif [[ "$#" -ne 0 ]]; then
  echo "usage: check-inference-package.sh [OUTPUT]" >&2
  exit 2
fi
