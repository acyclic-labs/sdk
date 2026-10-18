#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo +nightly-2026-07-01 fuzz run --dev --target x86_64-unknown-linux-gnu \
  --fuzz-dir rust/crates/harness/fuzz-settings mount_exclusion \
  rust/crates/harness/fuzz-settings/corpus/mount_exclusion -- "$@"
