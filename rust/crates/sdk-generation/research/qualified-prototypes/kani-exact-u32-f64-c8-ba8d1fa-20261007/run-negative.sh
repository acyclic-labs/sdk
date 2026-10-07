#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
: "${KANI_BIN:=/tmp/kani68/bin/cargo-kani}"
: "${KANI_TARGET_DIR:=/tmp/kani-exact-u32-f64-latest265-negative}"
export RUSTC_WRAPPER=
export CARGO_BUILD_JOBS=1
export CARGO_NET_OFFLINE=true
export CARGO_TARGET_DIR="$KANI_TARGET_DIR"
export RUSTUP_TOOLCHAIN=nightly-2026-08-21
set +e
"$KANI_BIN" --manifest-path Cargo.toml --package kani-exact-u32-f64-latest265-20261007 --exact --default-unwind 2 -j1 --harness negative_control_wrongly_accepts_fraction 2>&1 | tee audit/exact-u32-f64-negative-control-kani068.raw.txt
status=${PIPESTATUS[0]}
set -e
echo "TERMINAL_EXIT=${status}"
exit "${status}"

