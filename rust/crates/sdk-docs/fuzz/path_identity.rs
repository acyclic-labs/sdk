use super::path_identity;

pub fn check(path: &str, old: &str, new: &str, relative: &str, windows: bool) -> String {
    let rebased = path_identity::rebase(path, old, new, windows)
        .expect("owned source filename refused relocation");
    let destination = if windows {
        new.replace('\\', "/")
    } else {
        new.to_owned()
    };
    assert_eq!(
        rebased,
        format!("{destination}/{relative}"),
        "relocation changed the source suffix"
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
