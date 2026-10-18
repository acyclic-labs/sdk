#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    context::{Context, ContextDiscoveryLimits, ContextManifest},
    conversation::Limits,
    filesystem::ModelTaskCommand,
};

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    for instruction_bytes in [0, 1, 16, 4096] {
        let bounds = ContextDiscoveryLimits {
            instruction_bytes,
            ..Default::default()
        };
        if bounds.validate_instruction(input).is_ok() {
            assert!(instruction_bytes > 0 && input.len() as u64 <= instruction_bytes);
            assert!(!input.contains(&0));
            assert_eq!(
                fuzz_support::must_decode(std::str::from_utf8(input)).as_bytes(),
                input
            );
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
    if let Some(hooks) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::hooks::Hooks>,
    ) {
        if let Ok(encoded) = acyclic_harness::hooks::Hooks::encode(&hooks, limits.file_bytes) {
            let decoded = fuzz_support::round_trip_by(
                &encoded,
                serde_json::from_slice::<acyclic_harness::hooks::Hooks>(&encoded),
                |decoded| decoded.encode(limits.file_bytes),
            );
            assert_eq!(hooks.is_empty(), decoded.is_empty());
        }
    }
    if let Some(command) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<ModelTaskCommand>)
    {
        if command.context_manifest.validate(&mut None).is_ok() {
            let encoded = serde_json::to_vec(&command).unwrap();
            let _ = fuzz_support::round_trip(
                &(command),
                serde_json::from_slice::<ModelTaskCommand>(&encoded),
            );
        }
    }
    if let Some(source) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Context>)
    {
        let encoded = serde_json::to_vec(&source).unwrap();
        let _ = fuzz_support::round_trip(&(source), serde_json::from_slice::<Context>(&encoded));
    }
    macro_rules! round_trip {
        ($ty:ty) => {
            if let Some(value) =
                fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<$ty>)
            {
                let encoded = serde_json::to_vec(&value).unwrap();
                let _ = fuzz_support::round_trip(&(value), serde_json::from_slice::<$ty>(&encoded));
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
        if let Some(manifest) = fuzz_support::FuzzInput::parse_bounded(
            fuzz_input,
            path_bytes,
            |raw_input, path_bytes| {
                ContextManifest::decode(
                    raw_input,
                    Limits {
                        path_bytes,
                        render_bytes: 4096,
                        file_bytes: 4096,
                        context_messages: 16,
                        ..Default::default()
                    },
                )
            },
        ) {
            assert!(
                manifest
                    .discovery
                    .skills
                    .iter()
                    .all(|skill| skill.source.path.len() <= path_bytes)
            );
            let bytes = manifest.encode(limits).unwrap();
            let _ = fuzz_support::round_trip(&(manifest), ContextManifest::decode(&bytes, limits));
        }
    }
});
