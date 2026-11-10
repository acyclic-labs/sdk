#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
printf 'SOURCE_SHA=%s\n' "$(git rev-parse HEAD)"
control=preparation::tests::effect_selection_and_condition_membership_walk_the_original_slices_once
selection=rust/crates/stream/src/preparation/repeated_search.patch
shape=rust/crates/stream/src/preparation/repeated_shape_search.patch
temporary=$(mktemp -d)
selection_applied=0
shape_applied=0
restore() {
  if (( shape_applied )); then git apply -R "$shape"; fi
  if (( selection_applied )); then git apply -R "$selection"; fi
  rm -r "$temporary"
}
trap restore EXIT
cargo test --locked -p acyclic-stream --no-default-features --lib "$control" -- --exact
git apply "$selection"
selection_applied=1
git apply "$shape"
shape_applied=1
printf 'SOURCE_SHA=%s\n' "$(git rev-parse HEAD)" > "$temporary/mutant.log"
if cargo test --locked -p acyclic-stream --no-default-features --lib "$control" -- --exact >> "$temporary/mutant.log" 2>&1; then
  cat "$temporary/mutant.log"
  echo 'repeated-search mutation was accepted' >&2
  exit 1
fi
cat "$temporary/mutant.log"
grep -F 'selection and shape repeated searches' "$temporary/mutant.log"
grep -E 'left: \[[1-9][0-9]*, [1-9][0-9]*\]' "$temporary/mutant.log"
git apply -R "$shape"
shape_applied=0
git apply -R "$selection"
selection_applied=0
cargo test --locked -p acyclic-stream --no-default-features --lib "$control" -- --exact
