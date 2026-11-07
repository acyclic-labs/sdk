use super::path_identity;

/// Check relocation and normalized identity against the independently known suffix.
///
/// # Panics
/// Panics if the admitted result violates the independent contract checks.
pub fn check(path: &str, old: &str, new: &str, relative: &str, windows: bool) -> String {
    let rebased = path_identity::rebase(path, old, new, windows);
    let destination = if windows {
        new.replace('\\', "/")
    } else {
        new.to_owned()
    };
    let expected = format!("{destination}/{relative}");
    assert_eq!(
        rebased.as_deref(),
        Some(expected.as_str()),
        "relocation changed the source suffix"
    );
    let identity = format!("<root>/{relative}");
    assert_eq!(
        path_identity::stable_filename(path, old),
        identity,
        "normalization changed the owned source filename"
    );
    assert_eq!(
        path_identity::stable_filename(&expected, new),
        identity,
        "relocation changed the owned source identity"
    );
    expected
}
