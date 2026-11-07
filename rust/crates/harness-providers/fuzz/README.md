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

The three `provider_stream/llamacpp-*-request` seeds retain the matching original
request JSON files from that same capture and source commit, prefixed only by
selector `V` and Chat adapter byte `02`. `SOURCES.tsv` records the original
request and seed hashes. The existing corpus control requires the unmodified
requests to pass production input validation and sends both reserved-key
ignored-field mutations through the same input round-trip oracle. These mutations
are not additional recordings. The generated diagnostics property reuses that
oracle rather than keeping a separate decoder and comparison. Captured request
acceptance does not establish remote acceptance for arbitrary input or other
providers.

Apply only the input-helper hunk of the retained admission mutation with
`git apply --include=rust/crates/harness-providers/src/fuzzing.rs rust/crates/harness-providers/fuzz/mutations/literal-input-default-decoder.patch`.
The unchanged `checked_in_stream_corpus_obeys_the_properties` control must fail
at `accepted captured request skipped input validation`; restore the source
before the positive control. This is separate from the production Chat usage-loss
negative control below.

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
The oracle compares raw diagnostic prefixes independently with the returned
bounded, sanitized text, so equal chunked outputs cannot hide dropped refusal
metadata. Its expectation normalizes invalid UTF-8 before rendering and removes
only the replacement for an incomplete trailing scalar when no earlier invalid
input exists. The corpus replay also mutates the original captured Gemini body
at the byte bound with valid, malformed and cut scalars, explicitly changing its
status to429. Those mutations are not additional captured provider responses.

The kept `mutations/http-refusal-drop-diagnostic.patch` restores the original
status-only400/413 branch in the production decoder. Apply it from the
repository root and run the unchanged
`recorded_http_refusals_preserve_class_under_equivalent_escapes_and_chunking`
control with `--features fuzzing`. It must fail with
`HTTP refusal lost original diagnostic`; a compiler failure is not evidence.
Equivalent JSON escapes preserve the class, while their raw diagnostic text
may differ. Source-branch coverage and a sanitizer campaign remain separate
from this finite corpus and source-mutation control.

`mutations/http-excerpt-replace-cut-scalar.patch` plants a production defect
that emits a replacement for a scalar cut by the byte bound. Apply it from the
repository root and run that SAME unchanged recorded-refusal control. It must
fail with `HTTP refusal lost original diagnostic` on the source-derived byte
mutations. The planted defect changes production decoding, not its oracle.

The `provider_stream` input-validation selector and `http_failure` escape
generator also use the shared literal-map JSON decoder for admission and replay.
Their existing controls require both reserved key forms to reach the comparisons;
equal rejection or a skipped generator cannot qualify accepted input. The HTTP
variants are explicit ignored-field mutations of the original Gemini refusal,
and the input-validation values are generated component inputs, not recordings.
The decoder-only marker example was removed because these controls cover its
case through the actual fuzz helpers.

`mutations/literal-input-default-decoder.patch` restores default Value admission
in both helpers. Apply it from the repository root and run:

```sh
cargo test --locked -p acyclic-harness-providers --features fuzzing --lib -- tests::argument_projection_tests::raw_marker_names_and_non_object_fixtures_are_preserved fuzzing::http_failure::recorded_http_refusals_preserve_class_under_equivalent_escapes_and_chunking --exact --nocapture
```

Both unchanged controls must fail at runtime with `accepted literal-key input
skipped validation` and `production-accepted refusal skipped fuzz spelling comparison`.
Restore both source files before running the positive control. This establishes
generator admission conformance, not complete parser coverage or a sanitizer
campaign.

The input selector and refusal spelling generator check rejected admission
against the production JSON decoder before returning. A helper that silently
skips a production-valid value therefore crashes the actual fuzz entry point.
Accepted inputs retain one parse and replay; only a rejected helper input needs
the independent production-admission check. This adds no product work.

`corpus/DERIVED.tsv` records two ignored-field mutations of the original
llama.cpp request and Gemini refusal, with exact source/derived hashes and the
byte insertion. These are explicitly generated corpus entries, not new provider
captures. The original seeds remain unchanged. With
`mutations/literal-input-default-decoder.patch` applied, the existing
`provider_stream` and `http_failure` fuzz targets must crash on their respective
`*-literal-extension` inputs with `production-accepted input skipped fuzz
validation` and `production-accepted refusal skipped fuzz spelling comparison`.
A compile failure is not a negative control. Restore the source before replaying
the corpus; finite fuzzer feedback does not establish complete source branches
or a sanitizer campaign.
