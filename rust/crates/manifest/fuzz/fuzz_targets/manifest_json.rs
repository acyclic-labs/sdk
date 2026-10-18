#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_manifest::{SecretReference, environment_typescript, parse};
#[path = "../../tests/support/admission.rs"]
mod admission;
fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if let Some(reference) =
        fuzz_support::FuzzInput::parse_text(fuzz_input, |text| SecretReference::new(text.into()))
    {
        assert!(reference.as_str().starts_with("secrets:/"));
        assert!(!reference.as_str().contains(['@', '?']));
        assert_eq!(reference.as_str().matches(':').count(), 1);
    }
    if let Some(manifest) = fuzz_support::FuzzInput::parse(fuzz_input, parse) {
        assert_eq!(
            admission::check(&manifest),
            Ok(()),
            "independent admission invariant"
        );
        let json = serde_json::to_vec(&manifest).expect("admitted manifest encodes");
        let _ = fuzz_support::round_trip(&(manifest), parse(&json));
        for environment in &manifest.environments {
            for entry in &environment.entries {
                assert!(environment_typescript(&manifest, &environment.name, &entry.name).is_ok());
            }
        }
    }
});
