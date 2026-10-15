//! Resource and semantic action grammar. Canonicalization belongs to the owner.

/// Filesystem actions shared with the filesystem access decision model.
pub const FS_ACTIONS: &[&str] = &[
    "list",
    "read",
    "create",
    "write",
    "append",
    "delete",
    "rename",
    "fork",
    "merge-into",
    "read-history",
    "lock",
    "admin",
];

/// Accepts a canonical resource name. Callers must fold filesystem names using
/// the volume's case configuration before constructing or matching a name.
/// No account, glob, dot segment, or alternate separator is admitted.
pub fn valid_resource(value: &str) -> bool {
    let Some((product, path)) = value.split_once(":/") else {
        return false;
    };
    matches!(
        product,
        "streams"
            | "objects"
            | "fs"
            | "actors"
            | "workers"
            | "machines"
            | "execution"
            | "billing"
            | "infer"
            | "identity"
    ) && !path.contains(['*', '?', '\\', '\0', ':'])
        && !path.split('/').any(|part| part == "." || part == "..")
        && !path.contains("//")
        && !path.starts_with('/')
}

/// Checks semantic action syntax; product declarations define its vocabulary.
pub fn valid_action(value: &str) -> bool {
    let Some((product, action)) = value.split_once('/') else {
        return false;
    };
    valid_resource(&format!("{product}:/"))
        && !action.is_empty()
        && action.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
        && (product != "fs" || FS_ACTIONS.contains(&action))
}
