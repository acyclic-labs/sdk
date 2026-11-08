use super::path_identity;

/// Check relocation against the constructed destination and return that path.
///
/// # Panics
///
/// Panics when the production path operations refuse or change an owned filename.
pub fn check(path: &str, old: &str, new: &str, relative: &str, windows: bool) -> String {
    let destination = if windows {
        new.replace('\\', "/")
    } else {
        new.to_owned()
    };
    let rebased = format!("{destination}/{relative}");
    assert_eq!(
        path_identity::rebase(path, old, new, windows).as_deref(),
        Some(rebased.as_str()),
        "relocation refused or changed the source suffix"
    );
    let identity = format!("<root>/{relative}");
    assert_eq!(
        path_identity::stable_filename(path, old),
        identity,
        "normalization changed the owned source filename"
    );
    assert_eq!(
        path_identity::stable_filename(&rebased, new),
        identity,
        "relocation changed the owned source identity"
    );
    rebased
}
