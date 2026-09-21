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
[[ ! -s "$work/git-status" ]] || { echo 'package checkout is not clean' >&2; exit 1; }
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
    *) (cd "$root/typescript/packages/$directory" && bun pm pack --ignore-scripts --filename "$archive" --quiet) ;;
  esac
  node "$root/scripts/validate-npm-package.mjs" "$archive" "$name" "$version" "typescript/packages/$directory"
done < <(node -e '
  const fs = require("node:fs");
  const path = require("node:path");
  for (const item of JSON.parse(fs.readFileSync("release/npm-packages.json", "utf8"))) {
    const manifest = JSON.parse(fs.readFileSync(path.join("typescript/packages", item.directory, "package.json"), "utf8"));
    if (manifest.name !== item.name) throw new Error(`release identity differs for ${item.directory}`);
    console.log([item.slug, item.directory, item.name, manifest.version].join("\t"));
  }
')
(cd "$output" && sha256sum ./*.tgz > SHA256SUMS)
node scripts/typescript-qualification.mjs create "$output" "$source_sha"

file_url() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"
  elif command -v wslpath >/dev/null 2>&1; then wslpath -m "$1"
  else printf '%s\n' "$1"
  fi
}
version=$(node -p "require('./typescript/packages/sdk/package.json').version")
harness_url=$(file_url "$output/acyclic-labs-harness-${version}.tgz")
objects_url=$(file_url "$output/acyclic-labs-objects-${version}.tgz")
stream_url=$(file_url "$output/acyclic-labs-stream-${version}.tgz")
inference_url=$(file_url "$output/acyclic-labs-inference-${version}.tgz")
machines_url=$(file_url "$output/acyclic-labs-machines-${version}.tgz")
filesystem_url=$(file_url "$output/acyclic-labs-fs-${version}.tgz")
sdk_url=$(file_url "$output/acyclic-labs-sdk-${version}.tgz")
mkdir "$work/consumer"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic-labs/sdk":"file:$sdk_url"},"overrides":{"@acyclic-labs/harness":"file:$harness_url","@acyclic-labs/objects":"file:$objects_url","@acyclic-labs/stream":"file:$stream_url","@acyclic-labs/inference":"file:$inference_url","@acyclic-labs/machines":"file:$machines_url","@acyclic-labs/fs":"file:$filesystem_url"}}
EOF
cat >"$work/consumer/smoke.mjs" <<'EOF'
import { filesystem, harness, inference, machines, objects, stream } from "@acyclic-labs/sdk";
if (typeof filesystem.openBrowserFs !== "function" || typeof inference.InferenceClient !== "function" ||
    typeof harness.Harness !== "function" || typeof machines.SimulatedMachines !== "function" || typeof stream.StreamClient !== "function" ||
    typeof objects !== "object") throw new Error("SDK exports are incomplete");
EOF
(cd "$work/consumer" && bun install --ignore-scripts && bun smoke.mjs)
