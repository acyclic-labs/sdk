# Cargo output admission controls

Apply one patch at a time with `git apply`, then run:

```sh
cargo test --locked -p sdk-docs --lib child::tests::metadata_output_preserves_original_fields_and_failures -- --exact --nocapture
```

Revert that patch before applying the next one. The refusal, field and status
mutations change the production decoder. They must fail the unchanged shared
oracle with `Cargo output lost refusal details`, `Cargo output lost metadata
fields` and `Cargo output lost failure status`, respectively. Restored source
must pass. Compiler/setup failures are not semantic defect detections.

The former empty-workspace, success-only property misses the refusal mutation.
Its roundtrip did not compare original package fields or exercise stderr. One
property and the existing fuzz target now share the same original-byte oracle.
The capture provenance is beside the corpus. These controls do not establish
instrumented source coverage or execution of the coverage-guided fuzz target.
