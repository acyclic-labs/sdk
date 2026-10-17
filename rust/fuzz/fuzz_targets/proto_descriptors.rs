//! Fuzz descriptor identity canonicalization and protobuf round-trip stability.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 65_536 {
        return;
    }
    if let Ok(canonical) = acyclic_proto_build::canonicalize(bytes, &["sample.proto"]) {
        assert_eq!(
            acyclic_proto_build::canonicalize(&canonical, &["sample.proto"])
                .ok()
                .as_deref(),
            Some(canonical.as_slice())
        );
    }
});
