The `native_answer` target includes the shipping
`harness/src/local/host_process/children/answer.rs` reducer directly, with the
original `Outcome` type and answer bound. Input uses the production `LineBuffer`
and event bound used by `children::output`. The same semantic oracle compares
fragmented pipe reads with full pages, including malformed and incomplete frames;
no EOF delimiter is invented for an open pipe. It does not model process execution
or physical retirement. The old recorded-terminal
example is subsumed by this same corpus property; its unique lazy truncation
work assertion remains in the replay control.

The oracle checks event redelivery, previews without terminals, original
failure/cancellation, reset, unknown events, maximal UTF-8 answer prefixes and
loss of a preview or successful terminal. It checks every prefix of each
JSONL input. Invalid UTF-8 passes through the original framer's error path and
exercises the reducer loss transition; invalid JSON has no reducer effect.
No alternate framer or transition implementation is used.

The three `*-completed.jsonl` / `pi-assistant-error.jsonl` corpus files are
byte-for-byte copies of the existing recorded host fixtures. Their capture
versions, source revisions, provenance and SHA-256 values are retained in
[`fixtures/README.md`](../harness/src/local/host_process/fixtures/README.md).
The sources were installed native hosts; the model endpoint used during capture
was a loopback fixture. These inputs qualify the reply reducer, not live provider
interoperability.

`agy-authentication-error.jsonl` is the unchanged stdout of Antigravity CLI
1.5.2 on Windows, invoked with `--print=Reply with a greeting.` and
`--output-format stream-json --sandbox --mode auto` in an empty home. No model
request completed. Its SHA-256 is
`4a2b7f3c944be8f8ebf50e2dc83a022c7ae47a91bdd28b9da0d3b735a548c552`;
the native executable's SHA-256 is
`3f6f64a331fcd75c4ea3a25f89fdb0a9192feaa6e49bb828510b65dc0030b61e`.

`agy-hook-refusal.jsonl` is unchanged stdout from the same native version on
Windows, invoked with stream JSON input/output, `--sandbox --mode plan` and
`--settings '{"modelProvider":"gemini"}'`. An installed fixture plugin refused
`PreInvocation` with `terminationBehavior: "terminate"`; the native process
reported `ERROR` and exited three without making a provider request. Its SHA-256
is `93177162cfaa24c8739119ade68e215a30967d82d2daaecadd832b02e4f667dc`.
The adjacent `native_input/agy-SessionStart.json`, `agy-PreInvocation.json` and
`agy-Stop.json` seeds are exact hook stdin captures from that run. The fixture
used a plugin-relative Node recorder executable under a profile containing
spaces. This establishes native hook discovery, input and refusal behavior;
it does not establish Acyclic workspace coordination or a successful model turn.

`opencode-incomplete.jsonl` is unchanged stdout from OpenCode 1.15.9 on
Windows, invoked with `run --format json --model fixture/tiny` and the prompt
`Reply with a greeting.` against the recorded llama.cpp text fixture in
`conformance/fixtures/inference/llamacpp/text.body`. The native process exited
zero after emitting only `step_start`. This is an incomplete reply, never
successful completion. Its SHA-256 is
`0b67ef577be3976b10ee55a172f103392429176f5a75dd43b337bea996699c15`;
the native executable's SHA-256 is
`d282a3d68567587b452087a0c894a68e87e0bc9ac6270c2faeb3f3b75c3f9f7a`.
Both captures were made on 2026-11-07 using isolated profiles. They qualify
neither a successful live model turn nor workspace coordination.

`opencode-events.jsonl` in the adjacent host fixtures is the unchanged native
event bus captured by a local V1 plugin during that same run, SHA-256
`ad882bc125e022a00a3663cf85045a749b44ae261d4d9a399bcee496a217e340`.

`opencode-completed.jsonl` retains all three unchanged stdout lines from
OpenCode 1.19.5 on Linux x86_64, captured on 2026-11-07 with the same native
arguments, prompt and recorded llama.cpp provider bytes. The original capture
producer and request recorder were reused with the shared physical process
owner and an isolated credential-free profile. The native process exited zero
with completed text and a `step_finish` whose reason is `stop`. The older
incomplete capture remains a separate negative input.

SHA-256:

- Complete original stdout: `6032315bdf2105a128cea434899fc6d640fff5d64a32341b3473d82d0bae61c7`.
- Original provider bytes: `9291f29c22062298aff55dd764b31a3632d9e6560fd55da78a35ed1673c5bd54`.
- Native executable: `7df4e4136301f16589b0f30bf91a61380fca5f8ef22dd36570f5ebe1d4ec802c`.
- Official release archive: `588310a336fa3b9df36b3f0225b26f679d453efaad0cf3d66c3651ef3ec1647a`.

The executable came from the `opencode-linux-x64.tar.gz` asset of the
[official v1.19.5 release](https://github.com/anomalyco/opencode/releases/tag/v1.19.5).
The SDK capture source was `9ffd014b52a2bc4eb647e739fa36bec59e3f5b5f`.
This qualifies native output delivery for the recorded response; it does not
qualify installed workspace coordination, asynchronous idle-hook completion,
native tool execution or managed descendants.

The same shared corpus oracle detects the retained terminal-omission mutation
`mutations/native-answer-opencode-terminal.patch`. Apply it from the repository
root, replay the named corpus test below, then reverse it before accepting the
positive result.

`opencode-derived-transitions.jsonl` selects its original step/text parts and
wraps them in the CLI formatter's event shape, then mutates completion timestamps,
repeats a text part, and adds a multipart UTF-8 boundary: after a truncated emoji,
a later ASCII part cannot fill the remaining byte and skip the emoji.
These derived inputs exercise the reducer; they do not
establish that the native CLI delivered those lines. `agy-derived-transitions.jsonl`
mutates the captured refusal's statuses, response and optional failure fields.
Its synthetic success cases likewise provide no native interoperability claim.

`derived-transitions.jsonl` mutates copies of those captured events: absent and
wrong-typed text, terminal type, refusal metadata, success flags, Pi roles,
stop reasons and content, replay after refusal, and multi-byte answer boundaries.
Unrelated captured metadata is removed from those copies. The inserted text and
mutated events are negative/property inputs, never claimed as captured output.
`malformed-derived.jsonl` removes only the original Codex turn-start event,
prefixes its unchanged preview and terminal with invalid UTF-8, and appends
scalar/array JSON inputs plus an incomplete copy of the terminal. The original
preview and terminal must recover after framing loss without a reset. This
replaces the old malformed seed, whose intervening turn-start erased the loss
state before recovery was checked.

`oversize-derived.jsonl` pads the original Codex preview with leading JSON
whitespace to one byte beyond the production event bound, then sends the unchanged captured
preview and terminal without a turn-start. It exercises oversized-frame discard
and subsequent recovery through the same framer and reducer. Padding is a derived
input, not captured host output. The shared line-framer tests retain their own
bound-refusal control; this corpus entry adds composition coverage without a
second bound oracle. The fuzz input limit must exceed this seed's size to reach
the discard branch; it does not change either production bound.

`opencode-loss-derived.jsonl` retains the original complete OpenCode capture,
but first pads a copy of its text frame with leading JSON whitespace to one
byte beyond the production event bound, followed by the unchanged text and
terminal. Losing one part of an incremental reply must prevent publication of
the surviving tail as a complete answer. The appended unchanged capture starts
a new step and recovers. This is a derived loss input, never captured host output.
`mutations/native-answer-opencode-loss.patch` restores the old incremental path
after framing loss; the same property must reject that path. Complete snapshot
replies from other hosts still recover without a turn-start, as checked by the
existing Codex loss inputs.

Replay the same oracle without libFuzzer instrumentation:

```sh
cargo test --locked -p acyclic-harness-fuzz --no-default-features --features native-answer \
  --lib native_answer::recorded_and_derived_native_events_satisfy_the_same_fuzz_oracle -- --exact
```

From `rust/crates/harness-fuzz`, use the retained target and corpus:

```sh
cargo +nightly fuzz run --fuzz-dir . native_answer --no-default-features --features native-answer -- -runs=10000 -max_len=131072
cargo +nightly fuzz coverage --fuzz-dir . native_answer --no-default-features --features native-answer
```

Coverage must be read for the included shipping `answer.rs` and production
`native-runtime/src/lines.rs`, separately from the oracle and serde code. Request
Rust branch instrumentation with `-Zcoverage-options=branch`, preserve the full
`llvm-cov export` and annotated source branch map, and use LLVM tools from the
compiling toolchain. Missing branch maps remain unqualified. Target compilation,
seed replay and a finite fuzz campaign are separate evidence; none establishes
exhaustive state-space coverage or all functions in the native line module.

The planted-bug control adds premature `Outcome::Succeeded` publication directly
to the shipping `item.completed` branch after it retains a preview. The unchanged
oracle must reject the original recorded Codex seed with `preview completed a
turn`. Restore the source before accepting the target's positive result. This
control mutates production code, rather than weakening the oracle or inventing a
second decoder.

The retained `mutations/native-answer-sticky-loss.patch` plants permanent loss
until a new turn-start in the production reducer. Apply it from the repository
root and replay the same named corpus test above; it must fail with `lost
preview` on `malformed-derived.jsonl`. Reverse the patch, then require the same
unchanged corpus oracle to pass. This distinguishes recovery from reset, without
adding an example test or a parallel state model. The mutation is never linked
into the shipping implementation.

`mutations/native-answer-agy-refusal.patch` admits every native result status as
success. The unchanged corpus oracle must reject the captured hook refusal.
`mutations/native-input-agy-retirement.patch` maps `fullyIdle: false` to `Stop`;
the existing `native_input` target must reject retirement of a running native
turn. Both patches mutate the production boundary included by the target.
Reverse each patch before checking the positive source. These new controls are
retained for qualification; their execution and coverage remain unqualified.
