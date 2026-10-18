//! Fuzz descriptor identity canonicalization and protobuf round-trip stability.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if bytes.len() > 65_536 {
        return;
    }
    if let Some(canonical) = fuzz_support::FuzzInput::parse(fuzz_input, |raw_input| {
        acyclic_proto_build::canonicalize(raw_input, &["sample.proto"])
    }) {
        let _ = fuzz_support::round_trip(
            &canonical,
            acyclic_proto_build::canonicalize(&canonical, &["sample.proto"]),
        );
    }
});
