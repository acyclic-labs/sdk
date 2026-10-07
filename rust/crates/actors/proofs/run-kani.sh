#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-kani.sh --isolated-root DIR [options]

Runs the eleven source-owned Actors Kani harnesses in a parameterized isolated
tool/cache directory and writes a receipt only after successful verification
and unchanged source hashes.

Required:
  --isolated-root DIR       Isolated Kani/Rustup/Cargo/cache root

Options:
  --sdk-root DIR            SDK checkout (default: current Git root)
  --receipt FILE             Receipt path (default: source current receipt)
  --log FILE                 Verification log path (default: isolated root)
  --kani-bin FILE            cargo-kani executable
  --toolchain NAME           Verification toolchain
  --unwind N                 Kani default unwind bound
  --jobs N                   Kani worker count
  -h, --help                 Show this help
USAGE
}

sdk_root=""
isolated_root=""
receipt_path=""
log_path=""
kani_bin=""
toolchain="nightly-2026-08-21-x86_64-unknown-linux-gnu"
default_unwind="65"
jobs="1"

while (($# > 0)); do
  case "$1" in
    --sdk-root) sdk_root=$2; shift 2 ;;
    --isolated-root) isolated_root=$2; shift 2 ;;
    --receipt) receipt_path=$2; shift 2 ;;
    --log) log_path=$2; shift 2 ;;
    --kani-bin) kani_bin=$2; shift 2 ;;
    --toolchain) toolchain=$2; shift 2 ;;
    --unwind) default_unwind=$2; shift 2 ;;
    --jobs) jobs=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$isolated_root" ]]; then
  echo "--isolated-root is required" >&2
  usage >&2
  exit 2
fi

if [[ -z "$sdk_root" ]]; then
  sdk_root=$(git rev-parse --show-toplevel)
fi
sdk_root=$(cd "$sdk_root" && pwd)
mkdir -p "$isolated_root"
isolated_root=$(cd "$isolated_root" && pwd)

# Git worktree pointer files can contain a Windows drive path when this runner
# is invoked from WSL against a Windows-created checkout. Translate that path
# only when ordinary Git discovery cannot read it; native checkouts use the
# first branch unchanged.
git_sdk() {
  if git -C "$sdk_root" "$@" >/dev/null 2>&1; then
    git -C "$sdk_root" "$@"
    return
  fi
  if [[ -f "$sdk_root/.git" ]]; then
    local gitdir
    gitdir=$(sed -n 's/^gitdir: //p' "$sdk_root/.git")
    if [[ "$gitdir" =~ ^([[:alpha:]]):/(.*)$ ]]; then
      local drive=${BASH_REMATCH[1],,}
      local translated="/mnt/$drive/${BASH_REMATCH[2]}"
      GIT_DIR="$translated" git --work-tree="$sdk_root" "$@"
      return
    fi
  fi
  echo "Unable to read Git metadata for $sdk_root" >&2
  return 1
}

if [[ -z "$receipt_path" ]]; then
  receipt_path="$sdk_root/rust/crates/actors/proofs/kani-domain-invariants-current.json"
fi
if [[ -z "$log_path" ]]; then
  log_path="$isolated_root/kani-domain-invariants-current.log"
fi
if [[ -z "$kani_bin" ]]; then
  kani_bin="$isolated_root/cargo-home/bin/cargo-kani"
fi

export RUSTUP_HOME="$isolated_root/rustup-home"
export KANI_HOME="$isolated_root/kani-home"
export CARGO_HOME="$isolated_root/cargo-home"
export CARGO_TARGET_DIR="$isolated_root/target"
export RUSTUP_TOOLCHAIN="$toolchain"
export PATH="$CARGO_HOME/bin:$PATH"

if [[ ! -x "$kani_bin" ]]; then
  echo "Kani executable is not executable: $kani_bin" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for canonical JSON receipt serialization" >&2
  exit 1
fi

source_manifest() {
  (cd "$sdk_root" && find rust/crates/actors/src -type f -name '*.rs' -print | sort)
  cat <<'FILES'
rust/crates/actors/build.rs
rust/crates/actors/Cargo.toml
Cargo.toml
Cargo.lock
.cargo/config.toml
rust-toolchain.toml
rust/crates/actors/proofs/run-kani.sh
FILES
}

source_files=$(source_manifest)
while IFS= read -r relative_path; do
  [[ -z "$relative_path" ]] && continue
  if [[ ! -f "$sdk_root/$relative_path" ]]; then
    echo "Missing proof source input: $sdk_root/$relative_path" >&2
    exit 1
  fi
done <<< "$source_files"

mkdir -p "$(dirname "$receipt_path")" "$(dirname "$log_path")"
tmp_root=$(mktemp -d "$isolated_root/kani-run.XXXXXX")
trap 'rm -rf "$tmp_root"' EXIT
before_hashes="$tmp_root/before.tsv"
after_hashes="$tmp_root/after.tsv"
source_list="$tmp_root/source-files.txt"
printf '%s\n' "$source_files" | sed '/^$/d' > "$source_list"

capture_hashes() {
  local output_file=$1
  : > "$output_file"
  while IFS= read -r relative_path; do
    [[ -z "$relative_path" ]] && continue
    (cd "$sdk_root" && sha256sum "$relative_path") >> "$output_file"
  done <<< "$source_files"
}

capture_hashes "$before_hashes"
revision=$(git_sdk rev-parse HEAD)
kani_version_raw=$("$kani_bin" --version 2>&1)
kani_version=$(printf '%s\n' "$kani_version_raw" | sed -n '1p')
if command -v cbmc >/dev/null 2>&1; then
  cbmc_version=$(cbmc --version 2>&1)
else
  cbmc_version=$(printf '%s\n' "$kani_version_raw" | sed -n '/^CBMC /p' | sed -n '1p')
  [[ -n "$cbmc_version" ]] || cbmc_version="unavailable"
fi
rustc_version=$(rustc --version 2>&1)
cargo_version=$(cargo --version 2>&1)

echo "Running source-owned Actors Kani harnesses (unwind=$default_unwind, jobs=$jobs)"
set +e
(
  cd "$sdk_root"
  "$kani_bin" \
    --manifest-path "$sdk_root/rust/crates/actors/Cargo.toml" \
    --package acyclic-actors \
    --harness domain::kani_proofs::code_sha256_fixed_length_matches_contract \
    --harness domain::kani_proofs::code_sha256_constructor_preserves_valid_bytes \
    --harness domain::kani_proofs::code_sha256_rejects_empty \
    --harness domain::kani_proofs::code_sha256_rejects_31_bytes \
    --harness domain::kani_proofs::code_sha256_rejects_33_bytes \
    --harness domain::kani_proofs::code_sha256_rejects_64_bytes \
    --harness domain::kani_proofs::subscription_start_preserves_cursor_and_current_head_presence \
    --harness domain::kani_proofs::subscription_start_valid_wire_round_trip_preserves_oneof_identity \
    --harness domain::kani_proofs::positive_u64_constructor_accepts_exactly_nonzero_values \
    --harness domain::kani_proofs::actor_limits_constructor_accepts_exactly_positive_values \
    --harness domain::kani_proofs::enum_numeric_mappings_are_inverse_and_lossless \
    --exact --default-unwind "$default_unwind" "-j$jobs"
) 2>&1 | tee "$log_path"
kani_status=${PIPESTATUS[0]}
set -e
printf 'CARGO_KANI_EXIT=%s\n' "$kani_status" | tee -a "$log_path"

capture_hashes "$after_hashes"
revision_after=$(git_sdk rev-parse HEAD)
source_files_after=$(source_manifest)

if [[ "$kani_status" != "0" ]]; then
  echo "Kani verification failed; receipt was not written." >&2
  exit "$kani_status"
fi
if [[ "$revision" != "$revision_after" ]]; then
  echo "Source revision changed during verification; receipt was not written." >&2
  diff -u "$before_hashes" "$after_hashes" || true
  exit 1
fi
if ! diff -u "$before_hashes" "$after_hashes"; then
  echo "Proof source changed during verification; receipt was not written." >&2
  exit 1
fi
if [[ "$source_files" != "$source_files_after" ]]; then
  echo "Proof source inventory changed during verification; receipt was not written." >&2
  diff -u <(printf '%s\n' "$source_files") <(printf '%s\n' "$source_files_after") || true
  exit 1
fi

export SDK_ROOT="$sdk_root"
export ISOLATED_ROOT="$isolated_root"
export RECEIPT_PATH="$receipt_path"
export LOG_PATH="$log_path"
export REVISION="$revision"
export KANI_VERSION="$kani_version"
export CBMC_VERSION="$cbmc_version"
export RUSTC_VERSION="$rustc_version"
export CARGO_VERSION="$cargo_version"
export TOOLCHAIN="$toolchain"
export DEFAULT_UNWIND="$default_unwind"
export JOBS="$jobs"
export BEFORE_HASHES="$before_hashes"
export AFTER_HASHES="$after_hashes"
export SOURCE_LIST="$source_list"

python3 - <<'PY'
import hashlib
import json
import os
import pathlib
import re
from datetime import datetime, timezone

sdk_root = pathlib.Path(os.environ["SDK_ROOT"])
isolated_root = pathlib.Path(os.environ["ISOLATED_ROOT"])
receipt_path = pathlib.Path(os.environ["RECEIPT_PATH"])
log_path = pathlib.Path(os.environ["LOG_PATH"])
source_files = [
    line.strip()
    for line in pathlib.Path(os.environ["SOURCE_LIST"]).read_text(encoding="utf-8").splitlines()
    if line.strip()
]

def read_hashes(path):
    result = {}
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        digest, relative = line.split(None, 1)
        result[relative.replace("/", "\\")] = digest.upper()
    return result

before = read_hashes(os.environ["BEFORE_HASHES"])
after = read_hashes(os.environ["AFTER_HASHES"])
if before != after:
    raise SystemExit("source hashes changed during proof run")
if set(before) != {path.replace("/", "\\") for path in source_files}:
    raise SystemExit("source hash inventory is incomplete")

log_text = log_path.read_text(encoding="utf-8", errors="replace")
summary = re.search(
    r"Manual Harness Summary:\s*Complete - (\d+) successfully verified harnesses, "
    r"(\d+) failures, (\d+) total",
    log_text,
)
if not summary or tuple(map(int, summary.groups())) != (11, 0, 11):
    raise SystemExit("Kani log does not report exactly eleven successful harnesses")
if "VERIFICATION:- SUCCESSFUL" not in log_text:
    raise SystemExit("Kani log does not contain a successful verification marker")
if "CARGO_KANI_EXIT=0" not in log_text:
    raise SystemExit("Kani process status marker is missing or nonzero")

harnesses = [
    {
        "name": "domain::kani_proofs::code_sha256_fixed_length_matches_contract",
        "obligation": "A symbolic 32-byte digest is accepted by the code_sha256 constructor.",
    },
    {
        "name": "domain::kani_proofs::code_sha256_constructor_preserves_valid_bytes",
        "obligation": "CodeSha256::new preserves every byte for a symbolic valid 32-byte input.",
    },
    {
        "name": "domain::kani_proofs::code_sha256_rejects_empty",
        "obligation": "The code_sha256 constructor rejects an empty digest.",
    },
    {
        "name": "domain::kani_proofs::code_sha256_rejects_31_bytes",
        "obligation": "The code_sha256 constructor rejects a 31-byte digest.",
    },
    {
        "name": "domain::kani_proofs::code_sha256_rejects_33_bytes",
        "obligation": "The code_sha256 constructor rejects a 33-byte digest.",
    },
    {
        "name": "domain::kani_proofs::code_sha256_rejects_64_bytes",
        "obligation": "The code_sha256 constructor rejects a 64-byte digest.",
    },
    {
        "name": "domain::kani_proofs::subscription_start_preserves_cursor_and_current_head_presence",
        "obligation": "subscription_start preserves the cursor and optional current-head presence.",
    },
    {
        "name": "domain::kani_proofs::subscription_start_valid_wire_round_trip_preserves_oneof_identity",
        "obligation": "Valid subscription_start wire values round-trip through the domain without changing oneof identity.",
    },
    {
        "name": "domain::kani_proofs::positive_u64_constructor_accepts_exactly_nonzero_values",
        "obligation": "PositiveU64 accepts every nonzero u64 and rejects zero.",
    },
    {
        "name": "domain::kani_proofs::actor_limits_constructor_accepts_exactly_positive_values",
        "obligation": "ActorLimits accepts exactly three positive u64 values and preserves each value.",
    },
    {
        "name": "domain::kani_proofs::enum_numeric_mappings_are_inverse_and_lossless",
        "obligation": "The source enum numeric mappings are inverse and lossless for valid values.",
    },
]
log_digest = hashlib.sha256(log_path.read_bytes()).hexdigest().upper()
external_log = log_path
try:
    external_log = log_path.relative_to(isolated_root)
except ValueError:
    external_log = pathlib.Path(log_path.name)

receipt = {
    "schema": "acyclic.kani-proof/v1",
    "captured_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "status": "proved",
    "source": {
        "revision": os.environ["REVISION"],
        "files": before,
        "note": "Receipt is valid only for this exact revision and source-hash inventory.",
    },
    "tools": {
        "kani": os.environ["KANI_VERSION"],
        "cbmc": os.environ["CBMC_VERSION"],
        "rustc": os.environ["RUSTC_VERSION"],
        "cargo": os.environ["CARGO_VERSION"],
        "toolchain": os.environ["TOOLCHAIN"],
    },
    "command": {
        "entrypoint": "cargo-kani",
        "manifest": "<sdk>/rust/crates/actors/Cargo.toml",
        "package": "acyclic-actors",
        "harnesses": [item["name"] for item in harnesses],
        "flags": ["--exact", "--default-unwind", os.environ["DEFAULT_UNWIND"], "-j" + os.environ["JOBS"]],
        "portable_runner": "rust/crates/actors/proofs/run-kani.sh",
        "invocation": "run-kani.sh --sdk-root <sdk> --isolated-root <isolated> --toolchain <toolchain>",
    },
    "harnesses": harnesses,
    "result": {
        "successful": 10,
        "failures": 0,
        "total": 10,
        "unwind": int(os.environ["DEFAULT_UNWIND"]),
        "jobs": int(os.environ["JOBS"]),
    },
    "evidence": {
        "log": str(external_log).replace("\\", "/"),
        "log_sha256": log_digest,
    },
    "verification_policy": {
        "summary": "Manual Harness Summary: Complete - 10 successfully verified harnesses, 0 failures, 10 total",
        "required_markers": ["VERIFICATION:- SUCCESSFUL", "CARGO_KANI_EXIT=0"],
    },
    "scope": [
        "This receipt covers only the listed pure domain properties.",
        "It does not prove transport, generated SDK, packaging, service, or website behavior.",
    ],
}
receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
PY

echo "Wrote proof receipt: $receipt_path"
