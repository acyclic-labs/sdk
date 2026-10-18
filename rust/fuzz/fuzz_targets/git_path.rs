//! Checks normalized facade paths stay contained and round-trip without traversal.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::Error;
use std::path::{Component, Path};

// Compile the production parser, with the production Error type, without
// exporting a second public facade entry point just for verification.
#[path = "../../crates/harness/src/tools/git/path.rs"]
mod path;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(argument) = fuzz_input.text() else {
        return;
    };
    let root = Path::new("/workspace");
    let destination = Path::new(argument);
    if let Some(paths) = fuzz_support::FuzzInput::parse_text(fuzz_input, |argument| {
        acyclic_fs::native_mount_control_paths(Path::new(argument))
    }) {
        assert!(destination.is_absolute());
        assert!(
            !destination
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        );
        for path in &paths {
            assert_eq!(path.parent(), destination.parent());
        }
        let encoded = serde_json::to_vec(destination)
            .unwrap_or_else(|error| unreachable!("path encoding failed: {error}"));
        let restored = fuzz_support::round_trip(
            &destination.to_path_buf(),
            serde_json::from_slice::<std::path::PathBuf>(&encoded),
        );
        let _ = fuzz_support::round_trip(&paths, acyclic_fs::native_mount_control_paths(&restored));
    }
    if let Some(parsed) = fuzz_support::FuzzInput::parse_text(fuzz_input, |argument| {
        path::workspace_path_argument(Path::new("/workspace"), argument)
    }) {
        assert!(!parsed.contains('\0'));
        assert!(parsed.starts_with('/'));
        assert!(
            Path::new(&parsed)
                .components()
                .all(|part| !matches!(part, Component::ParentDir))
        );
        let absolute = root.join(parsed.trim_start_matches('/'));
        let argument = absolute
            .to_str()
            .unwrap_or_else(|| unreachable!("UTF-8 path"));
        let _ = fuzz_support::round_trip(&parsed, path::workspace_path_argument(root, argument));
    }
});
