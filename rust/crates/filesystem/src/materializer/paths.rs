use super::*;
use crate::native_host::HostRoot;

/// Validates a prospective recovery location using held root identities.
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
                existing = existing.parent().ok_or(error)?
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
    if root.identity().device != operation.identity().device {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let root = root.open_dir_held(Path::new(""), &mut &*work)?;
    let operation = operation.open_dir_held(Path::new(""), &mut &*work)?;
    #[cfg(unix)]
    let nested = {
        let (root, _root) = crate::native_host::canonical_directory_path_held(root, &mut &*work)?;
        let (operation, _operation) =
            crate::native_host::canonical_directory_path_held(operation, &mut &*work)?;
        let (prospective, _prospective) = work.path(&operation, Some(suffix))?;
        work.charge(
            NativeResourceCredit::Items
                .delta((root.as_os_str().len() + prospective.as_os_str().len()) as u64),
        )?;
        prospective.starts_with(&root) || root.starts_with(&prospective)
    };
    #[cfg(windows)]
    let nested = {
        let (root, _root) = crate::native_host::canonical_directory_wide(&root, &mut &*work)?;
        let (mut operation, mut allocation) =
            crate::native_host::canonical_directory_wide(&operation, &mut &*work)?;
        if !suffix.as_os_str().is_empty() {
            crate::native_host::with_wide_path(suffix, &mut &*work, |suffix, _meter| {
                let separator = usize::from(operation.last() != Some(&92));
                let count = separator
                    .checked_add(suffix.len())
                    .ok_or(crate::WorkError::Overflow)?;
                let needed = operation
                    .len()
                    .checked_add(count)
                    .ok_or(crate::WorkError::Overflow)?;
                let delta = NativeResourceCredit::Copy.delta(owned_size(count, 2)?);
                work.admit(delta)?;
                if needed > operation.capacity() {
                    allocation.grow(owned_size(needed - operation.capacity(), 2)?)?;
                    let reserved = operation.try_reserve_exact(needed - operation.len());
                    let reconciled =
                        allocation.reconcile_capacity(owned_size(operation.capacity(), 2)?);
                    reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
                    reconciled?;
                }
                if separator != 0 {
                    operation.push(92);
                }
                operation.extend_from_slice(suffix);
                work.spent(delta)
            })?;
        }
        work.charge(NativeResourceCredit::Items.delta((root.len() + operation.len()) as u64))?;
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
