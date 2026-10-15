#![no_main]

use acyclic_harness::{context::ContextManifest, conversation::Limits};
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
    if let Ok(manifest) = ContextManifest::decode(input, limits) {
        let bytes = manifest.encode(limits).unwrap();
        assert_eq!(ContextManifest::decode(&bytes, limits).unwrap(), manifest);
    }
});
