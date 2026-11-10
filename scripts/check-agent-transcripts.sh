#!/usr/bin/env bash
# Scoped tree export and completion-during-payload regressions.
set -euo pipefail
printf 'TRANSCRIPTS_SOURCE_SHA=%s\n' "$(git rev-parse HEAD)"
cargo nextest run --locked -p acyclic-harness --lib --features filesystem-local,test-support \
  --success-output immediate \
  -E 'test(=local::tests::transcripts::transcripts_follow_two_spawn_levels_and_match_original_journals) | test(=local::agent_conversation::transcripts::tests::finish_during_payload_read_drains_committed_records)'
printf 'TRANSCRIPTS_SPAWN=PASS TRANSCRIPTS_DRAIN=PASS\n'
