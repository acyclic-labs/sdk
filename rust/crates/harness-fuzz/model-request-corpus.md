# Model request corpus

The seven current request seeds below are exact bytes emitted by the production
`contract::canonical_json_bytes` writer, captured from
`model::tests::projection_reuse::request_writer_corpus_preserves_admission_and_refuses_obsolete_fields`
at commit `d85c6706f79569158a0bf2be9112931fcb53517d`.
The selected Linux invocation captured every writer output before the old corpus
comparison rejected its missing required `summary` field:

```sh
cargo nextest run --locked -p acyclic-harness-core --all-features --lib -E 'test(=executor::tests::target_replay::filesystem_calls_record_the_provider_binding_and_refuse_retargeted_recovery) | test(=filesystem::host::seed::tests::seed_receipt_preserves_edits_deletions_and_all_preexisting_files) | test(=filesystem::tests::staged_content_is_version_pinned_and_requires_exact_grants) | test(=model::tests::projection_reuse::request_writer_corpus_preserves_admission_and_refuses_obsolete_fields)' --profile ci --no-fail-fast --retries 0
```

These are library writer captures, not provider response recordings or end-to-end
execution evidence. The control records valid pinned, error and instruction
exchanges plus malformed control text and unpaired result refusals. Historical
`obsolete-*` seeds retain historical writer bytes as invalid-input corpus; the
reader must reject them rather than migrate them. The existing seeds with
`model.options` retain that removed field. `obsolete-no-summary` is the unchanged
704-byte `pinned` capture from commit
`7ffc209e2732cb54130c1afedf58c0cb9b6a9f3b`, SHA-256
`3c09904be00d35d35affc528d24351441ec1c31d19eb18ce678f66421f6132d6`.
The writer control checks current admission and rejection of both historical
shapes; the same files remain inputs to the model-request fuzz target.
`captured-instructions` remains the existing separate ContextManifest seed.

| Seed | Bytes | SHA-256 |
| --- | ---: | --- |
| `pinned` | 720 | `0f3f600dd318784a0fe17275d7a70228a03a717e423c85b0aeea336a3a413027` |
| `error-bound` | 724 | `fe7679e971d0fec3573856d58a45e4ba83736afd6c0a79d8e078e72d15320398` |
| `error-refused` | 730 | `6ed220fbd8824f8a6b5e3246f7a12e8b623a14639d15e50d2fb35fc9b98ed821` |
| `error-control` | 715 | `9ef345f6b4493d4891e04e3b0c35420573d8b9750275a04e782928cae2df9360` |
| `error-unpaired` | 726 | `09d6a21ff4006cc2da60ff6d15c1947babb6d1b16124951bc478e6da265ff03f` |
| `refused` | 590 | `2058aee92e81cfa9cccc81eaa313d28cd54463d304caf49b1f5835f102cdf477` |
| `project-instructions` | 820 | `4a1bbcbcffabd4853577bae42d8eb7261d6848e8d4bfd1de87ebf85c87408f6a` |
