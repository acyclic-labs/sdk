//! Checks normalized facade paths stay contained and round-trip without traversal.
#![no_main]

use acyclic_harness::Error;
use libfuzzer_sys::fuzz_target;
use std::path::{Component, Path};

// Compile the production parser, with the production Error type, without
// exporting a second public facade entry point just for verification.
#[path = "../../crates/harness/src/tools/git/path.rs"]
mod path;

fuzz_target!(|bytes: &[u8]| {
    let Ok(argument) = std::str::from_utf8(bytes) else {
        return;
    };
    let root = Path::new("/workspace");
    let destination = Path::new(argument);
    if let Ok(paths) = acyclic_fs::native_mount_control_paths(destination) {
        assert!(destination.is_absolute());
        assert!(
            !destination
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        );
        for path in &paths {
            assert_eq!(path.parent(), destination.parent());
        }
        let restored = serde_json::to_vec(destination)
            .ok()
            .and_then(|wire| serde_json::from_slice::<std::path::PathBuf>(&wire).ok())
            .and_then(|path| acyclic_fs::native_mount_control_paths(&path).ok());
        assert_eq!(restored, Some(paths));
    }
    if let Ok(parsed) = path::workspace_path_argument(root, argument) {
        assert!(!parsed.contains('\0'));
        assert!(parsed.starts_with('/'));
        assert!(
            Path::new(&parsed)
                .components()
                .all(|part| !matches!(part, Component::ParentDir))
        );
        let absolute = root.join(parsed.trim_start_matches('/'));
        let restored = absolute
            .to_str()
            .and_then(|argument| path::workspace_path_argument(root, argument).ok());
        assert_eq!(restored.as_deref(), Some(parsed.as_str()));
    }
});
