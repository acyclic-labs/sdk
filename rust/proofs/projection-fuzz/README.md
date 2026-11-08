# Projection through the shared decoder fuzz oracle

`node rust/proofs/qualify.mjs projection-fuzz` runs only
`follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding`
in acyclic-stream with `test-support`. That property checks every original writer
seed through the shared fuzz oracle as well as its projection and physical-cost
matrix. The retained mutation makes the actual projector keep
excluded entries. The unchanged shared fuzz oracle must refuse an original
writer output, then pass after byte-identical restoration.

The same contract also runs in the existing default projection property, without
`test-support`. Qualify that caller with
`node rust/proofs/qualify.mjs default-projection-fuzz`; it uses the same
production mutation. Conflicting requested and authority ranges must select no
entry, even when different entries satisfy those ranges separately.

The existing `stream_persistence` target calls that same oracle. For admitted
append records, records with explicit or followed origins, and read batches,
it projects the original record unrestricted and on both sides of an actual
indexed key. It compares selected entries and context with full decoding by
the canonical format reader, checks headers and publication identity, and
checks idempotence. Opaque records retain their whole-record authority rule.
There is no parallel parser or copied projector.

The original producer adds three-entry v1, v2 and opaque inputs through the
actual changes/protobuf and transport writers. Historical captures stay
unchanged. Capture only the additions from `rust/codec-fuzz` with:

```sh
cargo run --locked --example capture -- --only-stream-projection --extend-goldens
```

Challenge that same production mutation through the instrumented target, restore
the exact source, then explore the retained corpus with 1000 deterministic runs:

```sh
node rust/proofs/qualify.mjs projection-fuzz --instrumented
```

Run this command from the repository root. The negative uses the original
captured v1 append packet; the restored run uses the existing `stream_persistence`
corpus. New discoveries go to `target/projection-fuzz/corpus`, preserving the
captured inputs. The logs retain libFuzzer's executed edge/feature counts; those
counts do not establish complete source-branch coverage.

The selected unit control qualifies the shared oracle and its negative control;
it does not establish instrumented coverage or platform acceptance. The check
is finite and proportional to admitted bounded input bytes and entries. It
adds no production work, wait, transport, durability operation or parser.
