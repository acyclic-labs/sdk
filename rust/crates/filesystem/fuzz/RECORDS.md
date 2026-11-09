# Filesystem record admission

`decoder_work` retains the six page accounting selectors and also calls the
shared fixed-record oracle. `filesystem_codecs` calls that same oracle for source
facts, fixed-width generation locators, generation lineage and publication gates.
It compares original production writer inputs with decoded values before testing
arbitrary bytes. Re-encoding or decoding a canonical value must panic on failure;
refusal cannot silently end an oracle check.

Source facts cover both modes, every state and invalidation reason, all three
nonzero bounds, version refusal and complete framing. Stream records retain both
locator words, the typed generation identity and free/active gate identity.
These are format checks, not publication authorization or physical recovery proofs.
The generated typed inputs are library writer evidence, not captured remote wire.
The retained `inline-file-table-over-bound.bin` is an authored 65-byte fuzz input:
the writer must bound its inline fixture to `MAXIMUM_INLINE_FILE_BYTES` while the
arbitrary decoder still receives the complete input. Repeating the record capture
replaces the same selected provenance and golden names without changing other rows.

Run the shared controls:

```sh
cargo test --locked -p acyclic-fs --lib --no-default-features --features local,memory,test-support kernel::decoder_fuzz
cargo run --locked --manifest-path rust/codec-fuzz/Cargo.toml --example capture -- --only-filesystem-records --extend-goldens
cargo +nightly fuzz run --fuzz-dir rust/crates/filesystem/fuzz decoder_work --features fuzz-targets -- -runs=1000
cargo +nightly fuzz run --fuzz-dir rust/codec-fuzz filesystem_codecs -- -runs=1000
cargo +nightly fuzz run --fuzz-dir rust/codec-fuzz filesystem_pages -- -runs=1000
```

The patches in `mutations` change original production functions. Apply a patch,
select its filename stem through `ACYCLIC_FS_DECODER_DEFECT`, and run the unchanged
oracle. For example:

```sh
git apply rust/crates/filesystem/fuzz/mutations/generation-locator.patch
ACYCLIC_FS_DECODER_DEFECT=generation-locator cargo +nightly fuzz run --fuzz-dir rust/crates/filesystem/fuzz decoder_work --features fuzz-targets -- rust/codec-fuzz/corpus/filesystem_codecs/generation-locator-distinct -runs=1
git apply -R rust/crates/filesystem/fuzz/mutations/generation-locator.patch
```

The six page refusal patches must fail `decoder_work` at its original-writer
admission assertion; select each kind through the input's first byte, from zero
through five. The four record patches must fail the shared record oracle through
that same executable. A distinct-byte input exercises unequal locator words.
Several patches can be compiled together; the environment selects one original
source defect at a time to avoid repeated compilation. Production source is
restored after controls. Require original writer admission before and after the
negative phase; a compiler error is not mutation detection.

No product syscall, durability barrier, RPC, allocation, copy or wait is added.
Lineage and gate encoding use the same original buffer, field slices and funded
writer as before. The oracle performs fixed work per input and selected framing
cuts; the six page accounting controls retain their actual completion receipts.
Finite fuzz feedback does not establish complete source or branch coverage.

## Composition boundaries

| Existing path | Safety and progress checked here | Work and remaining evidence |
| --- | --- | --- |
| Source facts and stream records | Original typed writer identity, complete framing and invalid tags/bounds. Synchronous decoders return a value or their original error; no wait or producer is added. | Fixed fields and selected framing cuts, independent of agents or history. This does not measure native IO or allocation retirement. |
| `decoder_work` page accounting | Original `KernelAccounting` and `OriginalReadAccounting`, before-effect refusal/cancellation and copy/hash/item completion receipts. The existing lost-completion control is retained. | Exact completed-byte counters and denied-effect checks remain shared across standalone and native accounting modes. Physical syscalls and durability barriers are outside this byte decoder. |
| Native read/write/fsync and client transport | These format checks do not establish worker completion, durable publication, retained outcome or transport refusal provenance. | Reuse their original conformance, schedule and real-binary controls. Physical cuts, owner loss, native release and three-platform composition require separate source-bound evidence. |

The fixed-record property lives in the retained fuzz oracle and corpus producer;
there is no separate unit driver restating it. Existing page accounting properties
and controls remain because they cover fault schedules and actual completion work.
