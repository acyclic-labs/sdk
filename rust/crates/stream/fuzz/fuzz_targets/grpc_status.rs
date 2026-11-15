//! Checks gRPC status decoding through the production codec.
#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    assert!(acyclic_stream::codec_fuzz::grpc_status(input));
});
