use super::*;
use proptest::prelude::*;

proptest! {
    // Property: portable relative paths and their absolute workspace spelling
    // normalize to the same logical file; parent traversal is never accepted.
    #[test]
    fn selected_workspace_paths_round_trip(parts in prop::collection::vec("[a-zA-Z0-9_-]{1,12}", 1..8)) {
        let root = std::env::temp_dir().join("managed-workspace");
        let relative = parts.join("/");
        let normalized = workspace_path_argument(&root, &relative).map_err(TestCaseError::fail)?;
        prop_assert_eq!(&normalized, &format!("/{relative}"));
        prop_assert_eq!(workspace_path_argument(&root, &root.join(&relative).to_string_lossy()).map_err(TestCaseError::fail)?, normalized);
        let traversal = format!("../{relative}");
        prop_assert!(workspace_path_argument(&root, &traversal).is_err());
    }
}
