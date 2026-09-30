#!/usr/bin/env bash
set -euo pipefail

[[ $# == 4 && "$1" == /* && "$2" == /* && "$3" == /* && "$4" == /* ]] || {
  echo 'usage: check-typescript-packages.sh ABSOLUTE_OUTPUT INFERENCE_ARCHIVE FILESYSTEM_ARCHIVE HARNESS_ARCHIVE' >&2
  exit 2
}
output=$1
inference_archive=$2
filesystem_archive=$3
harness_archive=$4
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d -t sdk-typescript-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
cd "$root"
expected_source="${CI_HEAD_SHA:-${GITHUB_SHA:-}}"
source_sha=$(git rev-parse --verify HEAD)
[[ -z "$expected_source" || "$source_sha" == "$expected_source" ]] || { echo 'package checkout differs from the selected source commit' >&2; exit 1; }
git status --porcelain=v1 --untracked-files=all >"$work/git-status"
if [[ -s "$work/git-status" ]]; then
  echo 'package checkout is not clean; changed paths:' >&2
  sed -n '1,40p' "$work/git-status" >&2
  exit 1
fi
git ls-files -v >"$work/git-index"
! grep -Eq '^[a-zS] ' "$work/git-index" || { echo 'package checkout contains concealed index changes' >&2; exit 1; }

verify_staged_input() {
  local archive=$1 directory observed filename
  directory=$(dirname "$archive")
  [[ -f "$directory/SOURCE_COMMIT" && -f "$directory/SHA256SUMS" ]] || { echo 'staged package qualification evidence is missing' >&2; exit 1; }
  [[ "$(tr -d '\r\n' <"$directory/SOURCE_COMMIT")" == "$source_sha" ]] || { echo 'staged package belongs to another source commit' >&2; exit 1; }
  observed=$(sha256sum "$archive" | cut -d ' ' -f 1)
  filename=$(basename "$archive")
  awk -v hash="$observed" -v file="$filename" '
    NF == 2 { name=$2; sub(/^\*/, "", name); if ($1 == hash && name == file) found=1 }
    END { exit !found }
  ' "$directory/SHA256SUMS" || { echo 'staged package differs from its qualified checksum' >&2; exit 1; }
}
verify_staged_input "$inference_archive"
verify_staged_input "$filesystem_archive"
verify_staged_input "$harness_archive"

mkdir "$output"
while IFS=$'\t' read -r slug directory name version; do
  archive="$output/acyclic-labs-${slug}-${version}.tgz"
  case "$slug" in
    inference) install -m 0644 "$inference_archive" "$archive" ;;
    fs) install -m 0644 "$filesystem_archive" "$archive" ;;
    harness) install -m 0644 "$harness_archive" "$archive" ;;
    *)
      stage="$work/npm-$slug"
      bash "$root/scripts/stage-npm-package.sh" "$root/typescript/packages/$directory" "$stage"
      packed=$(cd "$stage" && npm pack --ignore-scripts --pack-destination "$output" --silent)
      [[ "$packed" == "$(basename "$archive")" && -f "$archive" ]] || {
        echo "npm pack produced an unexpected archive for $name: $packed" >&2
        exit 1
      }
      ;;
  esac
  node "$root/scripts/validate-npm-package.mjs" "$archive" "$name" "$version" "typescript/packages/$directory"
done < <(node -e '
  const fs = require("node:fs");
  const path = require("node:path");
  for (const item of JSON.parse(fs.readFileSync("release/npm-packages.json", "utf8")).filter(item => item.source === "typescript")) {
    const manifest = JSON.parse(fs.readFileSync(path.join("typescript/packages", item.directory, "package.json"), "utf8"));
    if (manifest.name !== item.name) throw new Error(`release identity differs for ${item.directory}`);
    console.log([item.slug, item.directory, item.name, manifest.version].join("\t"));
  }
')
(cd "$output" && sha256sum ./*.tgz > SHA256SUMS)
node scripts/typescript-qualification.mjs create "$output" "$source_sha"

mkdir "$work/consumer"
node --input-type=module - "$output" "$work/consumer" <<'EOF'
import { readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve, sep } from "node:path";
const [, , output, consumer] = process.argv;
const inventory = JSON.parse(readFileSync("release/npm-packages.json", "utf8"))
  .filter(item => item.source === "typescript");
const overrides = {};
for (const item of inventory) {
  const manifest = JSON.parse(readFileSync(join("typescript/packages", item.directory, "package.json"), "utf8"));
  if (manifest.name !== item.name) throw new Error(`release identity differs for ${item.directory}`);
  const archive = resolve(output, `acyclic-labs-${item.slug}-${manifest.version}.tgz`);
  if (!statSync(archive).isFile()) throw new Error(`qualified tarball is absent for ${item.name}`);
  overrides[item.name] = `file:./${relative(resolve(consumer), archive).split(sep).join("/")}`;
}
if (!overrides["@acyclic-labs/sdk"]) throw new Error("SDK tarball is absent from the release inventory");
writeFileSync(join(consumer, "package.json"), JSON.stringify({
  private: true, type: "module", workspaces: [],
  dependencies: { "@acyclic-labs/sdk": overrides["@acyclic-labs/sdk"] }, overrides,
}, null, 2));
EOF
cat >"$work/consumer/smoke.mjs" <<'EOF'
import { actors, filesystem, harness, harnessObjects, inference, machines, objects, stream, workers } from "@acyclic-labs/sdk";
import { ObjectContentStore } from "@acyclic-labs/harness/objects";
if (typeof filesystem.openBrowserFs !== "function" || typeof inference.InferenceClient !== "function" ||
    typeof harness.Harness !== "function" || typeof machines.SimulatedMachines !== "function" || typeof stream.StreamClient !== "function" ||
    typeof objects !== "object" || typeof actors.HttpActorsClient !== "function" ||
    typeof workers.HttpWorkersClient !== "function" || typeof harnessObjects.ObjectContentStore !== "function" ||
    harnessObjects.ObjectContentStore !== ObjectContentStore) throw new Error("SDK exports are incomplete");
EOF
(cd "$work/consumer" && BUN_INSTALL_CACHE_DIR="$work/install-cache" bun install --ignore-scripts && bun smoke.mjs && node smoke.mjs)
