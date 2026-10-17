#!/usr/bin/env bash
set -euo pipefail

cargo +nightly-2026-07-01 kani \
  --manifest-path "$(dirname "${BASH_SOURCE[0]}")/Cargo.toml" --lib \
  --jobs --output-format terse -Z unstable-options -Z function-contracts -Z stubbing \
  --cbmc-args --object-bits 8
