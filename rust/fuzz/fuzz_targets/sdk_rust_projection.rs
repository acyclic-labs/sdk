//! Fuzz generated Rust splitting and include-expansion round-trip stability.
#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../crates/proto-codegen/src/rust_projection/roundtrip.rs"]
mod roundtrip;

fuzz_target!(|source: &str| {
    // The protobuf generator emits syntax, never preexisting includes.
    if source.contains("include") {
        return;
    }
    if let Ok(files) =
        sdk_proto_codegen::rust_projection::project("acyclic/machines/v1/test.tonic.rs", source)
    {
        assert!(roundtrip::assert_roundtrip(source, files).is_ok());
    }
});
