//! Exercises the production file-tool and protected file-metadata codecs.
#![no_main]

use acyclic_harness::{conversation::FileRef, tool::files::ReadFileInput, tools::ShellInput};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(input) = serde_json::from_slice::<ShellInput>(bytes) {
        let restored = serde_json::to_vec(&input)
            .ok()
            .and_then(|wire| serde_json::from_slice::<ShellInput>(&wire).ok());
        assert_eq!(restored.as_ref(), Some(&input));
        let _ = input.validate();
    }
    if let Ok(input) = serde_json::from_slice::<ReadFileInput>(bytes) {
        let restored = serde_json::to_vec(&input)
            .ok()
            .and_then(|encoded| serde_json::from_slice::<ReadFileInput>(&encoded).ok());
        assert_eq!(restored.as_ref(), Some(&input));
    }
    if let Ok(metadata) = serde_json::from_slice::<FileRef>(bytes) {
        if metadata.validate(&mut None).is_ok() {
            let restored = serde_json::to_vec(&metadata)
                .ok()
                .and_then(|encoded| serde_json::from_slice::<FileRef>(&encoded).ok());
            assert_eq!(restored.as_ref(), Some(&metadata));
        }
    }
});
