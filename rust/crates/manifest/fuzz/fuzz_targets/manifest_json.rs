#![no_main]
use acyclic_manifest::{SecretReference, environment_typescript, parse};
use libfuzzer_sys::fuzz_target;
#[path = "../../tests/support/admission.rs"]
mod admission;
fuzz_target!(|input: &[u8]| {
    if let Ok(text) = std::str::from_utf8(input) {
        if let Ok(reference) = SecretReference::new(text.into()) {
            assert!(reference.as_str().starts_with("secrets:/"));
            assert!(!reference.as_str().contains(['@', '?']));
            assert_eq!(reference.as_str().matches(':').count(), 1);
        }
    }
    if let Ok(manifest) = parse(input) {
        assert_eq!(
            admission::check(&manifest),
            Ok(()),
            "independent admission invariant"
        );
        let json = serde_json::to_vec(&manifest).expect("admitted manifest encodes");
        assert_eq!(parse(&json).expect("round trip"), manifest);
        for environment in &manifest.environments {
            for entry in &environment.entries {
                assert!(environment_typescript(&manifest, &environment.name, &entry.name).is_ok());
            }
        }
    }
});
