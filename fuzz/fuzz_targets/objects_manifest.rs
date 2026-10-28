#![no_main]
use acyclic_objects::Manifest;
use libfuzzer_sys::fuzz_target;

// Manifest decoding is canonical: anything it accepts re-encodes to the same
// bytes, so one manifest has exactly one digest.
fuzz_target!(|bytes: &[u8]| {
    if let Ok(manifest) = Manifest::decode(bytes) {
        assert_eq!(manifest.encode(), bytes);
    }
});
