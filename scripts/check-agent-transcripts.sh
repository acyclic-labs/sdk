#!/usr/bin/env bash
# Retained scoped validation and root-only mutation control for transcript export.
set -euo pipefail
printf 'TRANSCRIPTS_SOURCE_SHA=%s\n' "$(git rev-parse HEAD)"
spawn_transcript_test() {
  cargo nextest run --locked -p acyclic-harness --lib --features filesystem-local,test-support \
    -E 'test(=local::tests::transcripts::transcripts_follow_two_spawn_levels_and_match_original_journals)'
}
spawn_transcript_test
printf 'TRANSCRIPTS_SPAWN=PASS\n'

source_file=rust/crates/harness/src/local/agent_conversation/transcripts.rs
original=$(mktemp)
log=$(mktemp)
cp "$source_file" "$original"
trap 'cp "$original" "$source_file"; rm -f "$original" "$log"' EXIT
statement='add(&jobs, selected, directory, operation, finish.clone()).await?;'
test "$(grep -F -c "$statement" "$source_file")" = 1
sed -i 's/add(\&jobs, selected, directory, operation, finish.clone()).await?;/drop(selected);/' "$source_file"
set +e
spawn_transcript_test >"$log" 2>&1
status=$?
set -e
cat "$log"
test "$status" = 100
grep -F 'every spawned agent needs a transcript' "$log"
printf 'TRANSCRIPTS_NEGATIVE_CONTROL=PASS (root-only mutation rejected)\n'
