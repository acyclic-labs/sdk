use super::*;

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub(super) fn validate_native_operation_location(
    root: &Path,
    operation_directory: &Path,
) -> Result<(), NativeTreeMaterializationError> {
    let root = root.canonicalize()?;
    let operation = prospective_canonical_path(operation_directory)?;
    if operation == root || operation.starts_with(&root) || root.starts_with(&operation) {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let existing = nearest_existing_ancestor(operation_directory)?;
    if !same_native_volume(&root, &existing.canonicalize()?)? {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    Ok(())
}

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub(super) fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, std::io::Error> {
    let mut cursor = path;
    loop {
        match std::fs::symlink_metadata(cursor) {
            Ok(_) => return Ok(cursor.to_path_buf()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                cursor = cursor.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub(super) fn prospective_canonical_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    let existing = nearest_existing_ancestor(path)?;
    let mut canonical = existing.canonicalize()?;
    let suffix = path
        .strip_prefix(&existing)
        .map_err(|_| std::io::Error::other("invalid prospective path"))?;
    for component in suffix.components() {
        match component {
            std::path::Component::Normal(name) => canonical.push(name),
            _ => return Err(std::io::Error::other("invalid prospective path component")),
        }
    }
    Ok(canonical)
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
pub(super) fn same_native_volume(left: &Path, right: &Path) -> Result<bool, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(std::fs::metadata(left)?.dev() == std::fs::metadata(right)?.dev())
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
pub(super) fn same_native_volume(left: &Path, right: &Path) -> Result<bool, std::io::Error> {
    use std::path::Component;
    let prefix = |path: &Path| {
        path.components().find_map(|component| match component {
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_os_string()),
            _ => None,
        })
    };
    Ok(prefix(left) == prefix(right))
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
pub(super) fn same_native_volume(_left: &Path, _right: &Path) -> Result<bool, std::io::Error> {
    Ok(true)
}
