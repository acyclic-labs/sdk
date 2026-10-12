use super::*;

pub(crate) fn customer_path(
    path: &str,
    config: crate::model::VolumeConfig,
) -> Result<NamespacePath, WorkspaceError> {
    let portable = PortablePath::parse(path, config.limits).map_err(WorkspaceError::path)?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
        .map_err(WorkspaceError::path)
}

pub(super) fn path_conflict_kind(
    base: Option<crate::kernel::FileRecord>,
    current: Option<crate::kernel::FileRecord>,
    source: Option<crate::kernel::FileRecord>,
) -> ConflictKind {
    let (Some(base), Some(current), Some(source)) = (base, current, source) else {
        return ConflictKind::Binding;
    };
    if base.kind != current.kind
        || base.kind != source.kind
        || base.file_id != current.file_id
        || base.file_id != source.file_id
    {
        return ConflictKind::Binding;
    }
    if base.link_count != current.link_count || base.link_count != source.link_count {
        return ConflictKind::HardLink;
    }
    if base.metadata != current.metadata || base.metadata != source.metadata {
        return ConflictKind::Metadata;
    }
    match base.kind {
        FileKind::Regular => ConflictKind::Binary,
        FileKind::Directory => ConflictKind::Directory,
        FileKind::SymbolicLink => ConflictKind::SymbolicLink,
        FileKind::Fifo
        | FileKind::Socket
        | FileKind::CharacterDevice
        | FileKind::BlockDevice
        | FileKind::ReparsePoint
        | FileKind::MountBoundary => ConflictKind::Special,
    }
}

pub(super) fn regular_file_bytes(record: crate::kernel::FileRecord) -> Result<u64, WorkspaceError> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            Ok(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX))
        }
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        _ => Err(WorkspaceError::NotRegularFile),
    }
}

pub(crate) fn workspace_vec_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}

pub(crate) fn workspace_owned_strings_bytes(values: &Vec<String>) -> Result<u64, WorkspaceError> {
    let bytes = values
        .capacity()
        .checked_mul(std::mem::size_of::<String>())
        .ok_or(crate::WorkError::Overflow)?;
    values.iter().try_fold(
        u64::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?,
        |bytes, value| {
            bytes
                .checked_add(
                    u64::try_from(value.capacity()).map_err(|_| crate::WorkError::Overflow)?,
                )
                .ok_or_else(|| crate::WorkError::Overflow.into())
        },
    )
}

pub(crate) fn customer_path_with_accounting(
    path: &str,
    config: crate::model::VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(NamespacePath, KernelOwnedAllocation), WorkspaceError> {
    NamespacePath::parse_in_profile_with_accounting(
        path,
        config.profile,
        config.limits,
        accounting,
        cancellation,
    )
    .map_err(WorkspaceError::from)
}

pub(super) fn copy_workspace_label(
    identity: &WorkspaceName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    let copied = copy_work(identity.retained_bytes());
    accounting.admit(copied)?;
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(identity.as_str().len(), accounting)?;
    accounting.admit(copied)?;
    label.value.extend_from_slice(identity.as_str().as_bytes());
    accounting.record_completed(copied)?;
    Ok(crate::kernel::Owned::new(
        String::from_utf8(label.value).map_err(WorkspaceError::engine)?,
        label.allocation,
        label.bytes,
    ))
}

pub(super) fn merge_workspace_backend<T, E: Into<WorkspaceError>>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    work: &mut crate::WorkCounters,
    live: u64,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    let backend = match &result {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    *work = work
        .with_backend(backend, live)
        .map_err(|error| WorkspaceError::WorkMerge {
            prior: *work,
            backend,
            error,
        })?;
    result.map_err(|failure| failure.error.into())
}

pub(super) fn workspace_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

pub(super) fn restore_absolute_path(
    relative: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    cancellation.check()?;
    let length = relative
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let copied = copy_work(length as u64);
    accounting.admit(copied)?;
    let mut path = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    path.value.push(b'/');
    path.value.extend_from_slice(relative.as_bytes());
    let path = crate::kernel::Owned::new(
        String::from_utf8(path.value).map_err(WorkspaceError::engine)?,
        path.allocation,
        path.bytes,
    );
    if let Err(error) = accounting.record_completed(copied) {
        path.retire(accounting)?;
        return Err(error.into());
    }
    Ok(path)
}

pub(super) fn workspace_allocation(
    allocation: KernelOwnedAllocation,
    accounting: &KernelAccounting<'_>,
) -> Result<crate::AuthorityAllocation, WorkspaceError> {
    match allocation {
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(Some(allocation)),
        KernelOwnedAllocation::Standalone => {
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
            Ok(None)
        }
    }
}
