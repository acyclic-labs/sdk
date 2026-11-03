The `inference_manifest` target calls the same `Manifest::decode` used by
`inference::replay::{captured,qualify,local}` before response files are opened.
The strict decoder retains the original rate-response provenance independently
of the model-server provenance. It rejects unknown fields and absent, mistyped
or blank provenance. The shared oracle first requires the original recording
to decode, then checks these refusals; arbitrary accepted inputs also undergo
an unknown-field insertion without re-encoding their original fields.

`corpus/inference_manifest/llamacpp.json` is a byte-for-byte copy of
`rust/crates/conformance/fixtures/inference/llamacpp/manifest.json`.
That recording's source, original requests, headers, response bodies and hashes
are documented in the adjacent `README.md`. Files prefixed `derived-` are
explicit mutations of that manifest, not new captures. The target never contacts
an endpoint or opens a manifest-supplied path.

Run the retained shared oracle and both existing recorded inference controls:

```sh
cargo test --locked -p acyclic-conformance --features inference --test main inference::captured_
cargo fuzz run --fuzz-dir fuzz inference_manifest -- -runs=128 -seed=1 -max_len=65536
```

`mutations/inference_manifest_rate.patch` removes the production blank-rate-source
guard. With that defect applied, the unchanged shared oracle must fail at the
blank provenance refusal. Restore the patch before the positive command.
The fuzzer uses the identical oracle. Execution, sanitizer and source-branch
coverage results must be recorded separately; retaining this target does not
establish those results.
