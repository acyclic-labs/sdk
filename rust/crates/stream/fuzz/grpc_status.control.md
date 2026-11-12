# Original status retention

From the repository root, run the retained target with the same pinned nightly
used by the other native fuzz qualifications:

```sh
cargo +nightly-2026-07-01 fuzz run --dev --target x86_64-unknown-linux-gnu \
  --fuzz-dir rust/crates/stream/fuzz grpc_status \
  rust/crates/stream/fuzz/corpus/grpc_status -- -runs=128 -seed=1 -print_final_stats=1
```

The run count bounds a qualification sample; it is not a parser or product limit.
Retain the original libFuzzer coverage and feature counters, rather than inferring
instrumentation from an ordinary crate build. Apply the omission below to the
same instrumented target to verify it finds the planted retention defect.
The target calls the production status decoder and encoder. It varies diagnostic
bytes across all 17 gRPC codes, comparing original status, message, opaque details,
repeated product metadata, category and recovery classification. The same oracle
is reached by `grpc::tests::status_decode_then_encode_is_the_identity`.

The seed is the unchanged output of the Rust Objects ErrorDetail writer retained
at `rust/codec-fuzz/corpus/protobuf/acyclic.objects.v1.ErrorDetail`. These bytes
seed generated diagnostics; they are not presented as captured gRPC trailers.
The actual HTTP/2 producer is exercised by the GrpcStreamProvider property.

Apply `status-category.patch` from the repository root and run the named property.
It removes retention for known categories at the real decoder boundary. The
property must reject it; restore the patch before running the passing control.