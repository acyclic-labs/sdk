use super::*;
use crate::native_host::HostRoot;

pub(super) use crate::native_host::host_path;

pub(super) struct PreparationLocation<'a> {
    pub root: &'a Path,
    pub operation: &'a Path,
}

/// Validates a recovery location against the held checkout allocation.
pub fn validate_native_preparation_location(
    root: &Path,
    operation: &Path,
    work: &NativeMaterializationWork,
) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
    let location = PreparationLocation { root, operation };
    let root = HostRoot::open(root, &mut &*work)?;
    let mut existing = operation;
    let parent = loop {
        match HostRoot::open(existing, &mut &*work) {
            Ok(parent) => break parent,
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                existing = existing.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    };
    let suffix = operation
        .strip_prefix(existing)
        .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
    for component in suffix.components() {
        work.charge(NativeResourceCredit::Items.delta(1))?;
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
    }
    validate_layout(&root, &parent, suffix, location, work)?;
    Ok(root.identity())
}

pub(super) fn validate_layout<'a>(
    root: &HostRoot,
    operation: &HostRoot,
    suffix: &Path,
    location: PreparationLocation<'a>,
    work: &NativeMaterializationWork,
) -> Result<Option<&'a str>, NativeTreeMaterializationError> {
    if root.identity().device != operation.identity().device {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    if let Ok(relative) = location.operation.strip_prefix(location.root) {
        let depth = relative.components().count();
        let first = relative
            .components()
            .next()
            .and_then(|component| match component {
                std::path::Component::Normal(name) => name.to_str(),
                _ => None,
            });
        // A nested operation must use the checkout's durably owned binding;
        // no arbitrary authored ancestor can become recovery custody.
        let owned = crate::NativeRecoveryRoot::name(root.identity());
        if depth != 1 && !(depth == 2 && first.is_some_and(|name| name.as_bytes() == owned)) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        for component in relative.components() {
            work.charge(NativeResourceCredit::Items.delta(1))?;
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let name = match relative.components().next() {
            Some(std::path::Component::Normal(name)) => name.to_str(),
            _ => None,
        }
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let existing = location
            .operation
            .ancestors()
            .nth(suffix.components().count())
            .and_then(|path| path.strip_prefix(location.root).ok())
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        // Reopen through the checkout capability. A path spelling cannot
        // conceal an authored symlink or select a different held directory.
        let child = held_child(root, existing, work)?;
        if child.identity() != operation.identity() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        return Ok(Some(name));
    }
    if root.identity() == operation.identity() {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    // A missing path cannot be an ancestor of an existing checkout.
    #[cfg(unix)]
    let nested = crate::native_host::directory_has_ancestor(
        &operation.directory(),
        root.identity(),
        &mut &*work,
    )? || suffix.as_os_str().is_empty()
        && crate::native_host::directory_has_ancestor(
            &root.directory(),
            operation.identity(),
            &mut &*work,
        )?;
    #[cfg(windows)]
    let nested = {
        let (root, _root) =
            crate::native_host::canonical_directory_wide(&root.directory(), &mut &*work)?;
        let (operation, _operation) =
            crate::native_host::canonical_directory_wide(&operation.directory(), &mut &*work)?;
        work.charge(NativeResourceCredit::Items.delta((root.len() + operation.len()) as u64))?;
        fn contains(parent: &[u16], child: &[u16]) -> bool {
            child.starts_with(parent)
                && (parent.last() == Some(&92)
                    || child.len() == parent.len()
                    || child.get(parent.len()) == Some(&92))
        }
        contains(&root, &operation) || suffix.as_os_str().is_empty() && contains(&operation, &root)
    };
    if nested {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    Ok(None)
}
