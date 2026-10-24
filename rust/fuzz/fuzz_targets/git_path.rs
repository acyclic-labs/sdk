//! Checks normalized facade paths stay contained and round-trip without traversal.
#![no_main]
#[cfg(test)]
use acyclic_harness::build_metadata;

use libfuzzer_sys::fuzz_target;
use std::path::{Component, Path};

fn check_control_paths(destination: &Path, registry: &Path) {
    if let Ok(paths) = acyclic_fs::native_mount_control_paths(destination) {
        assert!(destination.is_absolute());
        assert!(
            !destination
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        );
        assert_eq!(paths[0].parent(), destination.parent());
        assert_ne!(paths[0], paths[1]);
        assert_ne!(&paths[0], destination);
        for path in &paths {
            assert!(path.is_absolute());
        }
        assert_eq!(&paths[1], registry);
        let restored: std::path::PathBuf =
            serde_json::from_slice(&serde_json::to_vec(destination).unwrap()).unwrap();
        assert_eq!(
            acyclic_fs::native_mount_control_paths(&restored).ok(),
            Some(paths)
        );
    }
}

fuzz_target!(|bytes: &[u8]| {
    let Ok(argument) = std::str::from_utf8(bytes) else {
        return;
    };
    let root = Path::new("/workspace");
    if let Ok(root_paths) = acyclic_fs::native_mount_control_paths(root) {
        check_control_paths(Path::new(argument), &root_paths[1]);
        // A shared registry may itself be queried as a destination. Only the
        // sibling fence must differ from its destination.
        check_control_paths(&root_paths[1], &root_paths[1]);
    }
    if let Ok(parsed) = acyclic_harness::tools::git::fuzz_workspace_path_argument(root, argument) {
        assert!(!parsed.contains('\0'));
        assert!(parsed.starts_with('/'));
        assert!(
            Path::new(&parsed)
                .components()
                .all(|part| !matches!(part, Component::ParentDir))
        );
        let absolute = root.join(parsed.trim_start_matches('/'));
        let restored = absolute.to_str().and_then(|argument| {
            acyclic_harness::tools::git::fuzz_workspace_path_argument(root, argument).ok()
        });
        assert_eq!(restored.as_deref(), Some(parsed.as_str()));
    }
});
