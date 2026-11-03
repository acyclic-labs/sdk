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

The same stream oracle passes each original `Completed` metadata value to
`executor::model_usage_codec_fuzz::check_metadata`. This checks all five token
counters against independent set consensus over the admitted metadata paths,
including explicit zero, absent values, malformed values and contradictory
aliases. It does not prove that a provider parser retained every wire field;
that requires an independent captured-response expectation. The existing
`harness_codecs` target also reaches this oracle on arbitrary JSON and checks
the durable usage row's typed JSON round trip. Row syntax does not establish
that a journal committed an authoritative terminal observation.

For accepted Chat streams, the oracle also compares the complete retained usage
object with the original framed JSON, including unknown fields and explicit
zero. It uses the shared production SSE framer and stops at the original
`[DONE]` event. This prevents equally missing metadata at every chunk width from
passing the output-only consensus check. The three recorded llama.cpp captures
above supply the real wire inputs; arbitrary input mutations use the same check.
The oracle uses the adapter's literal-map JSON decoder, so reserved serde keys
in ignored fields cannot produce false crashes. The existing corpus control
inserts both reserved key forms into the original Chat frames and requires
unchanged production events and the same conservation checks.

The retained `mutations/chat-drop-wire-usage.patch` removes the production Chat
adapter's usage admission. From the repository root, apply it and run
`cargo test --locked -p acyclic-harness-providers --features fuzzing --lib -- checked_in_stream_corpus_obeys_the_properties --nocapture`.
The unchanged oracle must fail with `Chat completion lost original wire usage`.
The earlier output-only oracle accepts that same defect. Restore the source
before running the positive control. This checks parser-to-completion metadata
conservation; it does not establish a durable journal or authenticated source.

The retained `rust/codec-fuzz/mutations/model-usage-default-zero.patch` plants a
production decoder defect that substitutes zero for absent counters. Apply it
from the repository root and run the unchanged
`usage_dialects_preserve_units_absence_and_malformed_controls` property. The
control must fail; a compilation failure does not qualify the negative control.
Target source, corpus replay, sanitizer campaign, source coverage and mutation
execution are distinct results and must be reported separately.

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

The retained `http_failure/gemini-signature` seed is status400 followed by the
unchanged original Gemini tool-continuation refusal. Its source and hashes are
in `SOURCES.tsv`; the original response headers were not retained. Replaying
that body does not qualify socket delivery, restart or distributed recovery.
The oracle compares valid UTF-8 raw diagnostic prefixes independently with the
returned bounded, sanitized text, so equal chunked outputs cannot hide dropped
refusal metadata. Invalid UTF-8 retains bounds and chunk-invariance checks;
its exact lossy text is not independently qualified by this expectation.

The kept `mutations/http-refusal-drop-diagnostic.patch` restores the original
status-only400/413 branch in the production decoder. Apply it from the
repository root and run the unchanged
`recorded_http_refusals_preserve_class_under_equivalent_escapes_and_chunking`
control with `--features fuzzing`. It must fail with
`HTTP refusal lost original diagnostic`; a compiler failure is not evidence.
Equivalent JSON escapes preserve the class, while their raw diagnostic text
may differ. Source-branch coverage and a sanitizer campaign remain separate
from this finite corpus and source-mutation control.
