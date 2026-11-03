use super::*;

#[test]
fn relative_workspace_uses_normalized_absolute_drive_and_unc_keys() {
    for (key, canonical) in [
        ("q:/Repo/Project", r"\\?\Q:\repo\project"),
        (r"\\Server\Share\Project", r"\\?\UNC\server\share\project"),
        (r"\\?\UNC\Server\Share\Project", r"\\server\share\project"),
    ] {
        assert!(matches(key, Path::new("."), Path::new(canonical)));
    }
}
