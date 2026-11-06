# GraphCoder JSONL request corpus

The files are authored user requests, not fabricated host or provider output.
They cover prompt identity and escaping, both cancel values, unknown and mixed
actions, missing and duplicate fields, partial/trailing input and wrong types.
The target additionally generates both approval decisions with the original
`ApprovalBinding` serializer and caller-chosen `OperationId`, and checks all
three actions on every invocation. No credential or external service is used.

The shipping headless stdin owner and this target call the same `input::parse`
function and serde union. Accepted input must preserve its selected action,
original fields and binding, and must not contain another action or an unknown
field. Independently serialized prompts, approvals and cancels must be decoded,
so rejecting everything is not a valid implementation.

The shared kept control removes the production union's `deny_unknown_fields`,
then requires the unchanged oracle's exact mixed-action assertion to fail.
It restores the original source before replaying the corpus. Compiler errors
cannot satisfy the negative control. Run from the repository root:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py graphcoder_input
```

Binding identity is compared after the original typed decoder: UUID spelling
may change without changing its identity. The alternate UUID seed is an authored
user request. The kept regression restores the exact original oracle (verified
by its source digest), requires its false refusal at that seed, and restores the
corrected oracle before qualifying production admission.

This is decoder qualification, not conversation or authority qualification.
`cancel: false` decodes and is refused later by dispatch. A structurally decoded
approval still needs the existing intent/binding authorization before execution.
Native UTF-8 framing, dispatch, approval retries, provider replies, persisted
cursors and native process retirement are separate boundaries. The target does
not claim independent duplicate-key or full rejection-branch coverage.

LibFuzzer's replay edge count is not a source branch coverage report or a fuzz
campaign. Collect source coverage with the existing cargo-fuzz coverage command;
retain its actual report before making a branch-completeness claim.
