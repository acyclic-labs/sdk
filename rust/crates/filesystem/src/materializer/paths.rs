use super::*;
use crate::native_host::HostRoot;

/// Validates a recovery location against the held checkout allocation.
pub fn validate_native_preparation_location(
    root: &Path,
    operation: &Path,
    work: &NativeMaterializationWork,
) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
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
    validate_layout(&root, &parent, suffix, work)?;
    Ok(root.identity())
}

pub(super) fn validate_layout(
    root: &HostRoot,
    operation: &HostRoot,
    suffix: &Path,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    if root.identity().device != operation.identity().device
        || root.identity() == operation.identity()
    {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    // A missing path cannot be an ancestor of an existing checkout.
    #[cfg(unix)]
    let nested = crate::native_host::directory_has_ancestor(
        &operation.directory,
        root.identity(),
        &mut &*work,
    )? || suffix.as_os_str().is_empty()
        && crate::native_host::directory_has_ancestor(
            &root.directory,
            operation.identity(),
            &mut &*work,
        )?;
    #[cfg(windows)]
    let nested = {
        let (root, _root) =
            crate::native_host::canonical_directory_wide(&root.directory, &mut &*work)?;
        let (operation, _operation) =
            crate::native_host::canonical_directory_wide(&operation.directory, &mut &*work)?;
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
    Ok(())
}
