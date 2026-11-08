The `status` target passes arbitrary `grpc-message` field bytes through tonic's
HTTP trailer parser and the shipping `wire_codec::status` decoder at all 17
gRPC codes. Its independent contract covers every message-sensitive branch,
fallback, canonical encoder and mutation-uncertainty classification. Unavailable
categories must retain the original code, message, details and product metadata
through encoding. Arbitrary reason bytes pass through tonic's metadata boundary;
opaque details use its actual writer, rather than fabricated base64 trailers.

Capture the corpus from the SDK's original status producer:

```sh
cargo run --locked -p acyclic-stream --features test-support --example status-corpus
cargo test --locked -p acyclic-stream --lib wire_codec::status_contract::tests::status_contract_and_planted_decoder_faults -- --exact
cargo +nightly fuzz run --fuzz-dir rust/crates/stream/fuzz status -- -runs=1000 -seed=1 -max_len=256
```

`corpus/status-SOURCES.tsv` identifies the producer and SHA-256 of each captured
field. These are SDK writer outputs, not Cloud or external provider captures.
The property and fuzzer call the same oracle. The property also checks planted
unsupported/deadline/argument decoder faults, stripping the original refusal,
and a symmetric path/limit swap that a round-trip-only property accepts. No
remote membership, admission, retry, persistence, restart or captured service
refusal coverage follows from this component target.
