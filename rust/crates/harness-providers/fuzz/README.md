Run provider decoders on demand from this directory, with an explicit campaign
budget and the Rust toolchain supported by cargo-fuzz:

```sh
cargo +nightly fuzz run --fuzz-dir . provider_stream -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . response_sequence -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . turns -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . tool_arguments -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . native_image -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . catalog -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . http_failure -- -max_total_time=30
```

These targets call the production streaming, projection and catalog paths.
Their presence does not establish an executed campaign. Replay a saved crash
with `cargo +nightly fuzz run --fuzz-dir . TARGET artifacts/TARGET/crash-HASH` and fix its
production decoder before considering the campaign complete.

For `provider_stream`, `corpus/SOURCES.tsv` records three real llama.cpp response captures, retained
under `rust/crates/conformance/fixtures/inference/llamacpp/` at commit
`07e830a629ec08e5533a7fc28daef0b2cf04a9e8`. Each seed is selector byte `02`
(the existing Chat adapter selector) followed by the unmodified response body.
The table records both the body SHA-256 and the complete seed SHA-256. The
original capture manifest independently records those body hashes, request
inputs, response status, server image and model identity. These captures cover
text, tool invocation and the result continuation; they do not qualify other
provider endpoints. Other existing corpus entries are not claimed as real
provider captures by this table.

The existing `checked_in_stream_corpus_obeys_the_properties` control replays
all three recorded SSE bodies through the real Chat adapter and chunk-invariance
oracle. Each must also decode successfully, so equal refusals cannot qualify a
positive capture. Run it individually with the `fuzzing` feature before claiming
that replay at a changed source revision.

The `http_failure` target reaches the actual HTTP refusal path, including
context overflow JSON, status-sensitive body consumption, bounded diagnostics
and a body producer that ends in a typed error. Its first two bytes are the
little-endian HTTP status, followed by the unchanged response body. It checks
chunk independence and that equivalent escaped JSON strings preserve the error
class. Three more rows in `SOURCES.tsv` retain the real 400 context and 401
authentication responses and a 429 HTML response from the rate-limiting proxy.
The proxy response is not a model-server protocol response. The named recorded
HTTP-refusal replay passed with equivalent escapes, chunking, producer loss and
native funding/cancellation controls at its recorded revision. Sanitizer campaigns
remain separate obligations.
