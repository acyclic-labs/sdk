#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: run-kani.sh --isolated-root DIR [options]

Runs the four source-owned Objects request Kani harnesses and writes a
source-hash receipt only after every harness succeeds.

Required:
  --isolated-root DIR       Temporary Kani/target/log directory

Options:
  --sdk-root DIR            SDK checkout (default: Git root)
  --receipt FILE            Receipt path
  --log FILE                Verification log path
  --kani-bin FILE           cargo-kani executable
  --kani-home DIR           KANI_HOME (use an installed Kani bundle)
  --target-dir DIR          CARGO_TARGET_DIR
USAGE
}

sdk_root=""
isolated_root=""
receipt_path=""
log_path=""
kani_bin=""
kani_home=""
target_dir=""

while (($# > 0)); do
    case "$1" in
        --sdk-root) sdk_root=$2; shift 2 ;;
        --isolated-root) isolated_root=$2; shift 2 ;;
        --receipt) receipt_path=$2; shift 2 ;;
        --log) log_path=$2; shift 2 ;;
        --kani-bin) kani_bin=$2; shift 2 ;;
        --kani-home) kani_home=$2; shift 2 ;;
        --target-dir) target_dir=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n "$isolated_root" ]] || { usage >&2; exit 2; }
if [[ -z "$sdk_root" ]]; then
    sdk_root=$(git rev-parse --show-toplevel)
fi
sdk_root=$(cd "$sdk_root" && pwd)
isolated_root=$(mkdir -p "$isolated_root" && cd "$isolated_root" && pwd)
receipt_path=${receipt_path:-"$sdk_root/rust/crates/objects/proofs/kani-request-invariants-current.json"}
log_path=${log_path:-"$sdk_root/rust/crates/objects/proofs/objects-request-kani-proof.log"}
target_dir=${target_dir:-"$isolated_root/target"}
if [[ -z "$kani_bin" ]]; then
    kani_bin=$(command -v cargo-kani || true)
fi
[[ -n "$kani_bin" && -x "$kani_bin" ]] || {
    echo "cargo-kani is not executable; pass --kani-bin" >&2
    exit 1
}

source_manifest() {
    cat <<'FILES'
Cargo.toml
Cargo.lock
.cargo/config.toml
rust-toolchain.toml
rust/crates/native-runtime/Cargo.toml
rust/crates/native-runtime/README.md
rust/crates/native-runtime/src/lib.rs
rust/crates/native-runtime/src/linux.rs
rust/crates/native-runtime/src/process_tree.rs
rust/crates/objects/Cargo.toml
rust/crates/objects/README.md
rust/crates/objects/conformance/objects-v2.json
rust/crates/objects/src/body.rs
rust/crates/objects/src/generated/acyclic-objects-v2.bin
rust/crates/objects/src/generated/acyclic.objects.v2.rs
rust/crates/objects/src/generated/acyclic.objects.v2.tonic.rs
rust/crates/objects/src/lib.rs
rust/crates/objects/src/v2/conformance.rs
rust/crates/objects/src/v2/domain.rs
rust/crates/objects/src/v2/grpc.rs
rust/crates/objects/src/v2/memory.rs
rust/crates/objects/src/v2/mod.rs
rust/crates/objects/src/v2/request.rs
rust/crates/objects/src/v2/response.rs
rust/crates/objects/src/v2/typed.rs
rust/crates/objects/src/v2/upload.rs
rust/crates/objects/proofs/run-kani.sh
FILES
}

source_list=$(mktemp "$isolated_root/source-files.XXXXXX")
before_hashes=$(mktemp "$isolated_root/before-hashes.XXXXXX")
after_hashes=$(mktemp "$isolated_root/after-hashes.XXXXXX")
trap 'rm -f "$source_list" "$before_hashes" "$after_hashes"' EXIT
source_manifest > "$source_list"
while IFS= read -r relative_path; do
    [[ -f "$sdk_root/$relative_path" ]] || {
        echo "Missing proof source input: $sdk_root/$relative_path" >&2
        exit 1
    }
    (cd "$sdk_root" && sha256sum "$relative_path") >> "$before_hashes"
done < "$source_list"

mkdir -p "$(dirname "$receipt_path")" "$(dirname "$log_path")" "$target_dir"
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
revision=$(git_sdk rev-parse HEAD)
kani_version_raw=$("$kani_bin" --version 2>&1)
kani_version=$(printf '%s\n' "$kani_version_raw" | sed -n '1p')
cbmc_version=$(printf '%s\n' "$kani_version_raw" | sed -n '/^CBMC /p' | sed -n '1p')
[[ -n "$cbmc_version" ]] || cbmc_version="unavailable"
host_rustc_version=$(rustc --version 2>&1)
host_cargo_version=$(cargo --version 2>&1)

[[ -z "$kani_home" ]] || export KANI_HOME="$kani_home"
kani_home_effective="${KANI_HOME:-$kani_home}"
kani_rustc_version="unavailable"
kani_toolchain_version="unavailable"
if [[ -n "$kani_home_effective" ]]; then
    kani_rustc_file=$(find "$kani_home_effective" -mindepth 2 -maxdepth 2 -type f -name rustc-version 2>/dev/null | sort | tail -n 1 || true)
    kani_toolchain_file=$(find "$kani_home_effective" -mindepth 2 -maxdepth 2 -type f -name rust-toolchain-version 2>/dev/null | sort | tail -n 1 || true)
    [[ -z "$kani_rustc_file" ]] || kani_rustc_version=$(cat "$kani_rustc_file")
    [[ -z "$kani_toolchain_file" ]] || kani_toolchain_version=$(cat "$kani_toolchain_file")
fi

export CARGO_TARGET_DIR="$target_dir"
set +e
(
    cd "$sdk_root"
    "$kani_bin" \
        --manifest-path "$sdk_root/rust/crates/objects/Cargo.toml" \
        --package acyclic-objects \
        --harness v2::request::kani_proofs::byte_ranges_are_exact_for_nonempty_representations \
        --harness v2::request::kani_proofs::suffix_ranges_are_exact_for_nonempty_representations \
        --harness v2::request::kani_proofs::upload_length_has_the_inclusive_contract_bound \
        --harness v2::request::kani_proofs::part_number_has_the_inclusive_contract_bound \
        --exact --default-unwind 1 -j1 --output-format terse
) 2>&1 | tee "$log_path"
kani_status=${PIPESTATUS[0]}
set -e
printf 'CARGO_KANI_EXIT=%s\n' "$kani_status" | tee -a "$log_path"

while IFS= read -r relative_path; do
    (cd "$sdk_root" && sha256sum "$relative_path") >> "$after_hashes"
done < "$source_list"
revision_after=$(git_sdk rev-parse HEAD)
if [[ "$kani_status" != 0 ]]; then
    echo "Kani verification failed; receipt was not written." >&2
    exit "$kani_status"
fi
if [[ "$revision" != "$revision_after" ]]; then
    echo "Git revision changed during verification; receipt was not written." >&2
    exit 1
fi
if ! cmp -s "$before_hashes" "$after_hashes"; then
    echo "Proof source changed during verification; receipt was not written." >&2
    diff -u "$before_hashes" "$after_hashes" || true
    exit 1
fi

export RECEIPT_PATH="$receipt_path" LOG_PATH="$log_path" REVISION="$revision"
export KANI_VERSION="$kani_version" CBMC_VERSION="$cbmc_version"
export HOST_RUSTC_VERSION="$host_rustc_version" HOST_CARGO_VERSION="$host_cargo_version"
export KANI_RUSTC_VERSION="$kani_rustc_version" KANI_TOOLCHAIN_VERSION="$kani_toolchain_version"
export KANI_HOME_EFFECTIVE="$kani_home_effective"
export SOURCE_LIST="$source_list" BEFORE_HASHES="$before_hashes" SDK_ROOT="$sdk_root"

python3 - <<'PY'
import hashlib
import json
import os
import pathlib
from datetime import datetime, timezone

root = pathlib.Path(os.environ["SDK_ROOT"])
receipt_path = pathlib.Path(os.environ["RECEIPT_PATH"])
log_path = pathlib.Path(os.environ["LOG_PATH"])
files = {}
for line in pathlib.Path(os.environ["BEFORE_HASHES"]).read_text().splitlines():
    digest, path = line.split("  ", 1)
    files[path.replace("\\", "/")] = digest.upper()

harnesses = [
    {
        "name": "v2::request::kani_proofs::byte_ranges_are_exact_for_nonempty_representations",
        "status": "successful",
        "obligation": "For every nonempty representation, byte ranges reject out-of-bounds starts and descending ends, preserve the start and total, and clip an optional end to the last byte.",
    },
    {
        "name": "v2::request::kani_proofs::suffix_ranges_are_exact_for_nonempty_representations",
        "status": "successful",
        "obligation": "For every nonempty representation, positive suffix lengths resolve with saturating subtraction and zero suffix length is rejected.",
    },
    {
        "name": "v2::request::kani_proofs::upload_length_has_the_inclusive_contract_bound",
        "status": "successful",
        "obligation": "Decoded upload sizes are accepted exactly through the canonical five GiB inclusive limit.",
    },
    {
        "name": "v2::request::kani_proofs::part_number_has_the_inclusive_contract_bound",
        "status": "successful",
        "obligation": "Multipart part numbers are accepted exactly in the canonical one-through-MaxMultipartParts interval.",
    },
]
receipt = {
    "schema": "acyclic.kani-proof/v1",
    "captured_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "status": "proved",
    "source": {
        "revision": os.environ["REVISION"],
        "files_sha256": files,
        "closure": "rustc dependency inputs for acyclic-objects default-feature Kani compilation, including generated wire files and the native-runtime path dependency",
    },
    "tools": {
        "kani": os.environ["KANI_VERSION"],
        "cbmc": os.environ["CBMC_VERSION"],
        "host_rustc": os.environ["HOST_RUSTC_VERSION"],
        "host_cargo": os.environ["HOST_CARGO_VERSION"],
        "kani_rustc": os.environ["KANI_RUSTC_VERSION"],
        "kani_toolchain": os.environ["KANI_TOOLCHAIN_VERSION"],
        "kani_home": os.environ.get("KANI_HOME_EFFECTIVE", "inherited"),
    },
    "command": {
        "entrypoint": "cargo-kani",
        "manifest": "<sdk>/rust/crates/objects/Cargo.toml",
        "package": "acyclic-objects",
        "harnesses": [item["name"] for item in harnesses],
        "flags": ["--exact", "--default-unwind", "1", "-j1", "--output-format", "terse"],
        "portable_runner": "rust/crates/objects/proofs/run-kani.sh",
        "invocation": "run-kani.sh --sdk-root <sdk> --isolated-root <isolated> --kani-bin <cargo-kani> --kani-home <kani-home>",
    },
    "harnesses": harnesses,
    "result": {
        "successful_harnesses": 4,
        "failed_harnesses": 0,
        "total_harnesses": 4,
        "verifier_result": "VERIFICATION:- SUCCESSFUL",
        "exit_code": 0,
    },
    "evidence": {
        "external_log": os.path.relpath(log_path, root).replace("\\", "/"),
        "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest().upper(),
        "required_markers": ["VERIFICATION:- SUCCESSFUL", "CARGO_KANI_EXIT=0"],
    },
    "scope": {
        "formal": "Only the four listed pure request predicates are proved. This receipt does not prove the full Objects provider, transport, generated SDKs, packaging, network behavior, streaming, cancellation, or platform behavior.",
        "runtime_followups": "Provider conformance and integration properties remain separate runtime obligations.",
    },
}
receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
PY
echo "Wrote proof receipt: $receipt_path"
