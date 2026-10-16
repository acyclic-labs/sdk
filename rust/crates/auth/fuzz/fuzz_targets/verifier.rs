#![no_main]
#[path = "../../tests/common/signed.rs"]
mod signed;
use acyclic_auth::verify::verify;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let trust = signed::Snapshot::default();
    if let Ok(capability) = verify(bytes, &trust, signed::NOW) {
        let encoded = capability.encode();
        assert_eq!(encoded.as_slice(), bytes);
        let decoded = verify(&encoded, &trust, signed::NOW).unwrap();
        assert_eq!(decoded.block_ids(), capability.block_ids());
    }
});
