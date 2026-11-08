#![no_main]
//! Check context and hook admission against their configured bounds and round trips.

use acyclic_harness::{
    context::{Context, ContextDiscoveryLimits, ContextManifest, ContextSummary},
    conversation::Limits,
};
use libfuzzer_sys::fuzz_target;

fn round_trip<T: serde::de::DeserializeOwned + serde::Serialize + PartialEq>(input: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<T>(input) {
        assert!(serde_json::to_vec(&value).is_ok_and(|encoded| {
            serde_json::from_slice::<T>(&encoded).is_ok_and(|decoded| decoded == value)
        }));
    }
}

fuzz_target!(|input: &[u8]| {
    for instruction_bytes in [0, 1, 16, 4096] {
        let bounds = ContextDiscoveryLimits {
            instruction_bytes,
            ..Default::default()
        };
        if bounds.validate_instruction(input).is_ok() {
            assert!(instruction_bytes > 0 && input.len() as u64 <= instruction_bytes);
            assert!(!input.contains(&0));
            assert!(std::str::from_utf8(input).is_ok());
        }
    }
    let limits = Limits {
        render_bytes: 4096,
        file_bytes: 4096,
        context_messages: 16,
        ..Default::default()
    };
    if let Ok(hooks) = serde_json::from_slice::<acyclic_harness::hooks::Hooks>(input)
        && let Ok(encoded) = hooks.encode(limits.file_bytes)
    {
        assert!(
            serde_json::from_slice::<acyclic_harness::hooks::Hooks>(&encoded).is_ok_and(
                |decoded| decoded
                    .encode(limits.file_bytes)
                    .is_ok_and(|bytes| bytes == encoded)
                    && hooks.is_empty() == decoded.is_empty()
            )
        );
    }
    round_trip::<Context>(input);
    round_trip::<ContextSummary>(input);
    for path_bytes in [1, 16, 64, 4096] {
        let limits = Limits {
            path_bytes,
            ..limits
        };
        if let Ok(manifest) = ContextManifest::decode(input, limits) {
            assert!(
                manifest
                    .discovery
                    .skills
                    .iter()
                    .all(|skill| skill.source.path.len() <= path_bytes)
            );
            assert!(manifest.encode(limits).is_ok_and(|bytes| {
                ContextManifest::decode(&bytes, limits).is_ok_and(|decoded| decoded == manifest)
            }));
        }
    }
});
