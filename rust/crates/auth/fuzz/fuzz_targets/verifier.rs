#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
#[path = "../../tests/common/trust.rs"]
mod trust;
use acyclic_auth::verify::verify;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let trust = trust::Snapshot::default();
    if let Some(capability) = fuzz_support::FuzzInput::parse(fuzz_input, |bytes| {
        verify(bytes, &trust::Snapshot::default(), trust::NOW)
    }) {
        let encoded = capability.encode();
        assert_eq!(encoded.as_slice(), bytes);
        let decoded = fuzz_support::must_decode(verify(&encoded, &trust, trust::NOW));
        assert_eq!(decoded.block_ids(), capability.block_ids());
    }
});
