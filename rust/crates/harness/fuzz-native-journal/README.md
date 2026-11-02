Seeds are unchanged copies of the original native-host captures in
`../src/local/host_process/fixtures/`: `codex-completed.jsonl`,
`claude-code-completed.jsonl`, and `pi-assistant-error.jsonl`. That directory's
README records capture provenance. The copies retain the original bytes.

`native_reply` includes the production event decoder and terminal fold directly.
It checks bounded replies, invalidation after lost frames, and preservation of
an original refusal until an original new-turn boundary. Mutations are fuzz
inputs, not additional captured fixtures.

Run from the repository root with
`cargo fuzz run native_reply --fuzz-dir rust/crates/harness/fuzz-native-journal`.
