#!/usr/bin/env bash
# One scoped spawn test, including its wrong-agent identity negative control.
set -euo pipefail
printf 'TRANSCRIPTS_SOURCE_SHA=%s\n' "$(git rev-parse HEAD)"
cargo nextest run --locked -p acyclic-harness --lib --features filesystem-local,test-support \
  --success-output immediate \
  -E 'test(=local::tests::transcripts::transcripts_follow_two_spawn_levels_and_match_original_journals)'
printf 'TRANSCRIPTS_SPAWN=PASS\n'
