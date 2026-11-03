# Model request corpus

The seven current request seeds below are exact bytes emitted by the production
`contract::canonical_json_bytes` writer, captured from
`model::tests::projection_reuse::request_writer_corpus_preserves_admission_and_refuses_obsolete_fields`
at commit `7ffc209e2732cb54130c1afedf58c0cb9b6a9f3b`.
The writer control passed in this scoped invocation; its independent custody
control failed and was corrected separately:

```sh
cargo test --locked -p acyclic-harness-core --all-features --lib -- --exact model::instruction_review_tests::read_projection_has_exact_linear_index_work_and_quadratic_negative model::request::encoding::tests::native_prepared_inventory_tracks_request_and_cached_hash_storage model::tests::projection_reuse::request_writer_corpus_preserves_admission_and_refuses_obsolete_fields --nocapture
```

These are library writer captures, not provider response recordings or end-to-end
execution evidence. The control records valid pinned, error and instruction
exchanges plus malformed control text and unpaired result refusals. Historical
`obsolete-*` seeds retain the original removed `model.options` field byte for byte
as invalid-input corpus; the reader must reject it rather than migrate it.
`captured-instructions` remains the existing separate ContextManifest seed.

| Seed | Bytes | SHA-256 |
| --- | ---: | --- |
| `pinned` | 704 | `3c09904be00d35d35affc528d24351441ec1c31d19eb18ce678f66421f6132d6` |
| `error-bound` | 708 | `492743f4fed2239c6d39aa5ccba1379c39a953f541f79a20e6f3818cbefff50f` |
| `error-refused` | 714 | `8afd6dfb51e14745c54c08c8458b6b4f89e06daaf9e2dcd97aee639fe7ab829a` |
| `error-control` | 699 | `c604d747aa7c1166f3da9fe335ff20eb3aa8c293dbe975e7fd2134612a6305ad` |
| `error-unpaired` | 710 | `b115a2c54b5498cfd0d455e7d83c5064385a2fd755aefd16ac810c1f9a5188e6` |
| `refused` | 574 | `541a728bff2454929caa0b26695c417a137bc03607eca506eefad804b55e613c` |
| `project-instructions` | 804 | `b6faa4b9d9cee2ce5561fe933b9878f97ca8495e924b53ad5ec8431ade604b23` |
