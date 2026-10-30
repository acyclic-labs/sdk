Run provider decoders on demand from this directory, with an explicit campaign
budget and the Rust toolchain supported by cargo-fuzz:

```sh
cargo +nightly fuzz run provider_stream -- -max_total_time=30
cargo +nightly fuzz run response_sequence -- -max_total_time=30
cargo +nightly fuzz run turns -- -max_total_time=30
cargo +nightly fuzz run tool_arguments -- -max_total_time=30
cargo +nightly fuzz run native_image -- -max_total_time=30
cargo +nightly fuzz run catalog -- -max_total_time=30
```

These targets call the production streaming, projection and catalog paths.
Their presence does not establish an executed campaign. Replay a saved crash
with `cargo +nightly fuzz run TARGET artifacts/TARGET/crash-HASH` and fix its
production decoder before considering the campaign complete.

`corpus/SOURCES.tsv` records three real llama.cpp response captures, retained
under `rust/crates/conformance/fixtures/inference/llamacpp/` at commit
`07e830a629ec08e5533a7fc28daef0b2cf04a9e8`. Each seed is selector byte `02`
(the existing Chat adapter selector) followed by the unmodified response body.
The table records both the body SHA-256 and the complete seed SHA-256. The
original capture manifest independently records those body hashes, request
inputs, response status, server image and model identity. These captures cover
text, tool invocation and the result continuation; they do not qualify other
provider endpoints. Other existing corpus entries are not claimed as real
provider captures by this table.
