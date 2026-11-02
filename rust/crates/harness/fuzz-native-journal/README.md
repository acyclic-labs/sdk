Seeds are unchanged copies of the original native-host captures in
`../src/local/host_process/fixtures/`: `codex-completed.jsonl`,
`claude-code-completed.jsonl`, and `pi-assistant-error.jsonl`. That directory's
README records capture provenance. The copies retain the original bytes.

`native_reply` feeds original input bytes through the deployed `LineBuffer` and
byte bound, then includes the production event decoder and terminal fold
directly. It compares fragmented pipe reads with full pages and checks bounded
replies, invalidation after lost frames, and preservation of an original refusal
until an original new-turn boundary. Incomplete lines remain incomplete, just
as they do on the native pipe. Mutations are fuzz inputs, not additional captured
fixtures. This target does not exercise process launch or durable settlement.

The same target also includes the production closed-log resume-id fold. It
compares fragmented reads with pages, including the parser-only EOF delimiter
for a complete final JSON value without a newline. Artifact bytes are unchanged.
The original Claude seed also derives an equivalent escaped `session_id` member
and checks it through the same fold. This is a JSON representation negative
control, not an additional captured response.

Run from the repository root with
`cargo fuzz run native_reply --fuzz-dir rust/crates/harness/fuzz-native-journal`.
