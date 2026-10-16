#![no_main]

use acyclic_harness::{
    context::ContextManifest, conversation::Limits, filesystem::ModelTaskCommand,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let limits = Limits {
        render_bytes: 4096,
        file_bytes: 4096,
        context_messages: 16,
        ..Default::default()
    };
    if input.len() > limits.render_bytes as usize {
        return;
    }
    if let Ok(command) = serde_json::from_slice::<ModelTaskCommand>(input) {
        if command
            .context_manifest
            .as_ref()
            .is_none_or(|reference| reference.validate(&mut None).is_ok())
        {
            let encoded = serde_json::to_vec(&command).unwrap();
            assert_eq!(
                serde_json::from_slice::<ModelTaskCommand>(&encoded).unwrap(),
                command
            );
        }
    }
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
