#![no_main]
//! Check manifest admission, serialization and generated entry environments.

use acyclic_manifest::{environment_typescript, parse};
use libfuzzer_sys::fuzz_target;
#[path = "../../tests/support/admission.rs"]
mod admission;
fuzz_target!(|input: &[u8]| {
    if let Ok(manifest) = parse(input) {
        assert_eq!(
            admission::check(&manifest),
            Ok(()),
            "independent admission invariant"
        );
        assert!(
            serde_json::to_vec(&manifest)
                .is_ok_and(|json| parse(&json).is_ok_and(|decoded| decoded == manifest))
        );
        for environment in &manifest.environments {
            for entry in &environment.entries {
                assert!(environment_typescript(&manifest, &environment.name, &entry.name).is_ok());
            }
        }
    }
});
