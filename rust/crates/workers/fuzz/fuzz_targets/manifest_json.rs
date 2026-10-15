#![no_main]

use acyclic_workers::manifest::{environment_typescript, parse};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if let Ok(manifest) = parse(input) {
        let encoded = serde_json::to_vec(&manifest).expect("admitted manifest must encode");
        assert_eq!(parse(&encoded).expect("manifest round trip"), manifest);
        for environment in &manifest.environments {
            for entry in &environment.entries {
                environment_typescript(&manifest, &environment.name, &entry.name)
                    .expect("admitted entry env must generate");
            }
        }
    }
});
