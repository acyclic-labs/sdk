#!/usr/bin/env bash
set -euo pipefail
corpus="$(mktemp -d)"
log="$(mktemp)"
trap 'rm -rf -- "$corpus"; rm -f -- "$log"' EXIT
cp rust/crates/objects/fuzz/corpus/batch_reply/*.pb "$corpus/"
target="${CARGO_BUILD_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
# Stable LLVM coverage gives libFuzzer real production branch feedback.
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table" \
  cargo run --locked --manifest-path fuzz/Cargo.toml --target "$target" --bin objects_batch_reply -- "$corpus" -runs=256 -max_len=4096 -timeout=5 -artifact_prefix="$corpus/" "$@" 2>&1 | tee "$log"
grep -Eq 'cov: [1-9][0-9]*' "$log"