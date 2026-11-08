# Original status retention

Run `cargo fuzz run grpc_status fuzz/corpus/grpc_status` from the stream crate.
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