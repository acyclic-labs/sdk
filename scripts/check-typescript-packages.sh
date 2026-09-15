#!/usr/bin/env bash
set -euo pipefail

[[ $# == 3 && "$1" == /* && "$2" == /* && "$3" == /* ]] || {
  echo 'usage: check-typescript-packages.sh ABSOLUTE_OUTPUT INFERENCE_ARCHIVE FILESYSTEM_ARCHIVE' >&2
  exit 2
}
output=$1
inference_archive=$2
filesystem_archive=$3
[[ ! -e "$output" && ! -L "$output" ]] || { echo 'package output must be absent' >&2; exit 2; }
[[ -f "$inference_archive" && -f "$filesystem_archive" ]] || { echo 'qualified dependency archives are missing' >&2; exit 2; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d -t sdk-typescript-package.XXXXXXXX)"
trap 'status=$?; rm -rf -- "$work"; exit "$status"' EXIT
cd "$root"
expected_source="${BUILD_SOURCEVERSION:-${GITHUB_SHA:-}}"
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

mkdir "$output"
package_version() {
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["version"])' "$root/typescript/packages/$1/package.json"
}
objects_version=$(package_version objects)
stream_version=$(package_version stream)
inference_version=$(package_version inference)
machines_version=$(package_version machines)
filesystem_version=$(package_version filesystem)
sdk_version=$(package_version sdk)
pack() {
  local directory=$1 name=$2 version=$3 archive
  archive="$output/acyclic-labs-${directory}-${version}.tgz"
  (cd "$root/typescript/packages/$directory" && bun pm pack --ignore-scripts --filename "$archive" --quiet)
  python3 "$root/scripts/validate-npm-package.py" "$archive" "$name" "$version" "typescript/packages/$directory"
}
pack objects @acyclic-labs/objects "$objects_version"
pack stream @acyclic-labs/stream "$stream_version"
pack machines @acyclic-labs/machines "$machines_version"
pack sdk @acyclic-labs/sdk "$sdk_version"
install -m 0644 "$inference_archive" "$output/acyclic-labs-inference-${inference_version}.tgz"
install -m 0644 "$filesystem_archive" "$output/acyclic-labs-fs-${filesystem_version}.tgz"
python3 scripts/validate-npm-package.py "$output/acyclic-labs-inference-${inference_version}.tgz" @acyclic-labs/inference "$inference_version" typescript/packages/inference
python3 scripts/validate-npm-package.py "$output/acyclic-labs-fs-${filesystem_version}.tgz" @acyclic-labs/fs "$filesystem_version" typescript/packages/filesystem
(cd "$output" && sha256sum ./*.tgz > SHA256SUMS)
python3 scripts/typescript-qualification.py create "$output" "$source_sha"

file_url() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"
  elif command -v wslpath >/dev/null 2>&1; then wslpath -m "$1"
  else printf '%s\n' "$1"
  fi
}
objects_url=$(file_url "$output/acyclic-labs-objects-${objects_version}.tgz")
stream_url=$(file_url "$output/acyclic-labs-stream-${stream_version}.tgz")
inference_url=$(file_url "$output/acyclic-labs-inference-${inference_version}.tgz")
machines_url=$(file_url "$output/acyclic-labs-machines-${machines_version}.tgz")
filesystem_url=$(file_url "$output/acyclic-labs-fs-${filesystem_version}.tgz")
sdk_url=$(file_url "$output/acyclic-labs-sdk-${sdk_version}.tgz")
mkdir "$work/consumer"
cat >"$work/consumer/package.json" <<EOF
{"private":true,"type":"module","dependencies":{"@acyclic-labs/sdk":"file:$sdk_url"},"overrides":{"@acyclic-labs/objects":"file:$objects_url","@acyclic-labs/stream":"file:$stream_url","@acyclic-labs/inference":"file:$inference_url","@acyclic-labs/machines":"file:$machines_url","@acyclic-labs/fs":"file:$filesystem_url"}}
EOF
cat >"$work/consumer/smoke.mjs" <<'EOF'
import { filesystem, harness, inference, machines, objects, recursiveSum, stream } from "@acyclic-labs/sdk";
if (typeof filesystem.openBrowserFs !== "function" || typeof inference.InferenceClient !== "function" ||
    typeof harness.Harness !== "function" || typeof machines.SimulatedMachines !== "function" || typeof stream.StreamClient !== "function" ||
    typeof objects !== "object" || await recursiveSum([1, 2, 3]) !== 6) throw new Error("SDK exports are incomplete");
EOF
(cd "$work/consumer" && bun install --ignore-scripts && bun smoke.mjs)
