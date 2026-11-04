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

Replay the same oracle without libFuzzer instrumentation:

```sh
cargo test --locked -p acyclic-harness-fuzz --no-default-features --features native-answer \
  --test native_answer recorded_and_derived_native_events_satisfy_the_same_fuzz_oracle -- --exact
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
