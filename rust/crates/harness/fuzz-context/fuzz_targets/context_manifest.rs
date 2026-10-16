#![no_main]

use acyclic_harness::{
    context::{Context, ContextDiscoveryLimits, ContextManifest},
    conversation::Limits,
    filesystem::ModelTaskCommand,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    for instruction_bytes in [0, 1, 16, 4096] {
        let bounds = ContextDiscoveryLimits {
            instruction_bytes,
            ..Default::default()
        };
        if bounds.validate_instruction(input).is_ok() {
            assert!(instruction_bytes > 0 && input.len() as u64 <= instruction_bytes);
            assert!(!input.contains(&0));
            assert_eq!(std::str::from_utf8(input).unwrap().as_bytes(), input);
        }
    }
    let limits = Limits {
        render_bytes: 4096,
        file_bytes: 4096,
        context_messages: 16,
        ..Default::default()
    };
    if input.len() > limits.render_bytes as usize {
        return;
    }
    if let Ok(hooks) = serde_json::from_slice::<acyclic_harness::hooks::Hooks>(input) {
        if let Ok(encoded) = hooks.encode(limits.file_bytes) {
            let decoded: acyclic_harness::hooks::Hooks = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded.encode(limits.file_bytes).unwrap(), encoded);
            assert_eq!(hooks.is_empty(), decoded.is_empty());
        }
    }
    if let Ok(command) = serde_json::from_slice::<ModelTaskCommand>(input) {
        if command.context_manifest.validate(&mut None).is_ok() {
            let encoded = serde_json::to_vec(&command).unwrap();
            assert_eq!(
                serde_json::from_slice::<ModelTaskCommand>(&encoded).unwrap(),
                command
            );
        }
    }
    if let Ok(source) = serde_json::from_slice::<Context>(input) {
        let encoded = serde_json::to_vec(&source).unwrap();
        assert_eq!(serde_json::from_slice::<Context>(&encoded).unwrap(), source);
    }
    macro_rules! round_trip {
        ($ty:ty) => {
            if let Ok(value) = serde_json::from_slice::<$ty>(input) {
                let encoded = serde_json::to_vec(&value).unwrap();
                assert_eq!(serde_json::from_slice::<$ty>(&encoded).unwrap(), value);
            }
        };
    }
    round_trip!(acyclic_harness::context::SummarySource);
    round_trip!(acyclic_harness::context::SummaryCheckpoint);
    round_trip!(acyclic_harness::context::SummarySelection);
    round_trip!(acyclic_harness::context::ContextSummary);
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
            let bytes = manifest.encode(limits).unwrap();
            assert_eq!(ContextManifest::decode(&bytes, limits).unwrap(), manifest);
        }
    }
});
