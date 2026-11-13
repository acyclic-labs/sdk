#!/usr/bin/env bash
# Mutate production rendering, keeping each real regression unchanged.
set -euo pipefail
cd "$(dirname "$0")/.."
git log -1 --format=TUI_DIFF_SOURCE_SHA=%H
logs=$(mktemp -d)
applied=
cleanup() {
  if [ -n "$applied" ]; then git apply -R "$applied"; fi
  rm -rf "$logs"
}
trap cleanup EXIT
cargo nextest run --locked -p acyclic-harness-tui -p acyclic-plugin --lib \
  -E 'test(=diff::tests::diff_word_emphasis_preserves_unicode_context_across_themes) | test(=diff::tests::diff_wide_reflow_and_hunk_folding_retain_every_source_grapheme) | test(=runner::view::query::tests::query_controller_retains_reads_coalesces_turns_and_ends_on_original_events)'
negative() {
  local patch=$1 package=$2 test=$3 assertion=$4 result=0
  git apply "rust/proofs/tui-diff/$patch.patch"
  applied="rust/proofs/tui-diff/$patch.patch"
  cargo nextest run --locked -p "$package" --lib -E "test(=$test)" \
    >"$logs/$patch" 2>&1 || result=$?
  git apply -R "$applied"
  applied=
  cat "$logs/$patch"
  [ "$result" != 0 ]
  grep -F "FAIL" "$logs/$patch" | grep -F "$test"
  grep -Eq "$assertion" "$logs/$patch"
  printf 'TUI_DIFF_NEGATIVE_CONFIRMED=%s\n' "$patch"
}
negative whole-line acyclic-harness-tui \
  diff::tests::diff_word_emphasis_preserves_unicode_context_across_themes 'assertion.*failed'
negative truncated-reflow acyclic-harness-tui \
  diff::tests::diff_wide_reflow_and_hunk_folding_retain_every_source_grapheme 'assertion.*failed'
negative dropped-context acyclic-plugin \
  runner::view::query::tests::query_controller_retains_reads_coalesces_turns_and_ends_on_original_events \
  'the real diff query lost its admitted presentation'
