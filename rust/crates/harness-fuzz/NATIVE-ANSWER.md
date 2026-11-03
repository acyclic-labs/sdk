The `native_answer` target includes the shipping
`harness/src/local/host_process/children/answer.rs` reducer directly, with the
original `Outcome` type and answer bound. It starts at the parsed event boundary
used by `children::output`; it does not model process execution, physical
retirement or the line codec's frame-size admission.

The oracle checks event redelivery, previews without terminals, original
failure/cancellation, reset, unknown events, maximal UTF-8 answer prefixes and
loss of a preview or successful terminal. It checks every prefix of each
JSONL input. Invalid UTF-8 exercises the same reducer loss transition; invalid
JSON has no reducer effect. No alternate transition implementation is used.

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
`malformed-derived.jsonl` prefixes the original Codex bytes with invalid UTF-8,
truncates its last event, and appends scalar/array JSON inputs.

Replay the same oracle without libFuzzer instrumentation:

```sh
cargo test --locked -p acyclic-harness-fuzz --no-default-features --features core \
  --test native_answer recorded_and_derived_native_events_satisfy_the_same_fuzz_oracle -- --exact
```

From `rust/crates/harness-fuzz`, use the retained target and corpus:

```sh
cargo +nightly fuzz run native_answer --no-default-features --features core -- -runs=10000
cargo +nightly fuzz coverage native_answer --no-default-features --features core
```

Coverage must be read for the included shipping `answer.rs`, separately from the
oracle and serde code. Target compilation, seed replay and a finite fuzz campaign
are separate evidence; none establishes exhaustive state-space coverage.

The planted-bug control adds premature `Outcome::Succeeded` publication directly
to the shipping `item.completed` branch after it retains a preview. The unchanged
oracle must reject the original recorded Codex seed with `preview completed a
turn`. Restore the source before accepting the target's positive result. This
control mutates production code, rather than weakening the oracle or inventing a
second decoder.
