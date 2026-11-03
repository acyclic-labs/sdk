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
ACYCLIC_FS_DECODER_DEFECT=generation-locator cargo test --locked -p acyclic-fs --lib --no-default-features --features local,memory,test-support original_writer_records_and_arbitrary_bytes_share_the_oracle
git apply -R rust/crates/filesystem/fuzz/mutations/generation-locator.patch
```

The six page refusal patches must fail the existing
`a_planted_encoder_byte_is_detected_for_every_kind` control at its original-writer
admission assertion. The four record patches must fail the shared record oracle.
Several patches can be compiled together; the environment selects one original
source defect at a time to avoid repeated compilation. Production source is
restored after controls. Require original writer admission before and after the
negative phase; a compiler error is not mutation detection.

No product syscall, durability barrier, RPC, allocation, copy or wait is added.
Lineage and gate encoding use the same original buffer, field slices and funded
writer as before. The oracle performs fixed work per input and selected framing
cuts; the six page accounting controls retain their actual completion receipts.
Finite fuzz feedback does not establish complete source or branch coverage.
