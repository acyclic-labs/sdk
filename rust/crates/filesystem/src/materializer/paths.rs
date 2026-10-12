use super::*;
use crate::native_host::HostRoot;

/// Checks the existing root and prospective operation through held identities.
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
        work.charge(crate::WorkCounters {
            source_entries_visited: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
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
    if root.identity().device != operation.identity().device {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let root = root.open_dir_held(Path::new(""), &mut &*work)?;
    let operation = operation.open_dir_held(Path::new(""), &mut &*work)?;
    #[cfg(unix)]
    let nested = {
        let (root, _root_allocation) =
            crate::native_host::canonical_directory_path_held(root, &mut &*work)?;
        let (operation, operation_allocation) =
            crate::native_host::canonical_directory_path_held(operation, &mut &*work)?;
        let (prospective, _allocation) = work.path(&operation, Some(suffix))?;
        drop(operation);
        drop(operation_allocation);
        work.charge(crate::WorkCounters {
            items_examined: root
                .as_os_str()
                .len()
                .checked_add(prospective.as_os_str().len())
                .ok_or(crate::WorkError::Overflow)? as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        prospective.starts_with(&root) || root.starts_with(&prospective)
    };
    #[cfg(windows)]
    let nested = {
        let (root, _root_allocation) =
            crate::native_host::canonical_directory_wide(&root, &mut &*work)?;
        let (mut operation, mut allocation) =
            crate::native_host::canonical_directory_wide(&operation, &mut &*work)?;
        if !suffix.as_os_str().is_empty() {
            with_native_walk_leaf(suffix, work, |suffix| {
                let separator = usize::from(operation.last() != Some(&92));
                let needed = operation
                    .len()
                    .checked_add(separator)
                    .and_then(|length| length.checked_add(suffix.len()))
                    .ok_or(crate::WorkError::Overflow)?;
                let delta = crate::WorkCounters {
                    bytes_copied: owned_size(suffix.len() + separator, 2)?,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(delta)?;
                if needed > operation.capacity() {
                    allocation.grow(owned_size(needed - operation.capacity(), 2)?)?;
                    operation
                        .try_reserve_exact(needed - operation.len())
                        .map_err(NativeTreeMaterializationError::AllocationFailed)?;
                    allocation.reconcile_capacity(owned_size(operation.capacity(), 2)?)?;
                }
                if separator != 0 {
                    operation.push(92);
                }
                operation.extend_from_slice(suffix);
                work.spent(delta)
            })?;
        }
        work.charge(crate::WorkCounters {
            items_examined: root
                .len()
                .checked_add(operation.len())
                .ok_or(crate::WorkError::Overflow)? as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        fn contains(parent: &[u16], child: &[u16]) -> bool {
            child.starts_with(parent)
                && (parent.last() == Some(&92)
                    || child.len() == parent.len()
                    || child.get(parent.len()) == Some(&92))
        }
        contains(&root, &operation) || contains(&operation, &root)
    };
    if nested {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    Ok(())
}
pub(super) fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. }
        | MaterializationEdit::Remove { path }
        | MaterializationEdit::SetMetadata { path, .. } => path,
        MaterializationEdit::Rename { from, .. } => from,
    }
}
