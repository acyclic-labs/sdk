#!/usr/bin/env bash
set -euo pipefail

# Host feature checks cannot compile the browser-only paths. Check libraries
# on their actual target, with each consumer's features resolved independently.
# In particular, binding crates must not mask missing cfgs in acyclic-fs.
bash scripts/ensure-rust-target.sh wasm32-unknown-unknown

check() {
  echo "browser target check: $*"
  cargo check --locked --target wasm32-unknown-unknown --lib "$@"
}

check -p acyclic-fs --no-default-features
check -p acyclic-fs --no-default-features --features memory,browser
for package in acyclic-auth-wasm acyclic-actors-wasm acyclic-fs-wasm \
  acyclic-objects-wasm acyclic-machines-wasm acyclic-inference-wasm acyclic-manifest; do
  check -p "$package"
done
check -p acyclic-stream --no-default-features --features wasm
check -p acyclic-harness-core --no-default-features --features browser
check -p acyclic-harness --no-default-features --features browser
check -p acyclic-harness-providers --features browser
