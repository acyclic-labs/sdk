use super::*;

pub(crate) fn customer_path(path: &str, config: crate::model::VolumeConfig) -> Result<NamespacePath, WorkspaceError> {
    let portable = PortablePath::parse(path, config.limits)
        .map_err(|error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)))?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits).map_err(WorkspaceError::from)
}

/// Parses a path using the caller's accounting owner.
pub(crate) fn customer_path_with_accounting(
    path: &str, config: crate::model::VolumeConfig, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(NamespacePath, KernelOwnedAllocation), WorkspaceError> {
    NamespacePath::parse_in_profile_with_accounting(path, config.profile, config.limits, accounting, cancellation)
        .map_err(WorkspaceError::from)
}

pub(super) fn logical_name_text(name: &LogicalName) -> Result<std::borrow::Cow<'_, str>, WorkspaceError> {
    name.unicode_text().ok_or(WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::NonUnicodeProjection))
}

pub(super) fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
    if path.is_root() { return Ok("/".to_owned()); }
    let components = path.components().iter().map(logical_name_text).collect::<Result<Vec<_>, _>>()?;
    Ok(format!("/{}", components.join("/")))
}

pub(super) fn regular_file_bytes(record: crate::kernel::FileRecord) -> Result<u64, WorkspaceError> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => Ok(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)),
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        _ => Err(WorkspaceError::NotRegularFile),
    }
}

pub(super) fn restore_absolute_path(
    relative: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    cancellation.check()?;
    let length = relative.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
    accounting.admit(copied)?;
    let mut path = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    path.value.push(b'/');
    path.value.extend_from_slice(relative.as_bytes());
    let path = crate::kernel::Owned::new(String::from_utf8(path.value).map_err(WorkspaceError::engine)?, path.allocation, path.bytes);
    if let Err(error) = accounting.record_completed(copied) { path.retire(accounting)?; return Err(error.into()); }
    Ok(path)
}

pub(super) fn copy_workspace_label(identity: &WorkspaceName, accounting: &mut KernelAccounting<'_>) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    let copied = crate::WorkCounters { bytes_copied: identity.retained_bytes(), ..crate::WorkCounters::UNCHARGED };
    accounting.admit(copied)?;
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(identity.as_str().len(), accounting)?;
    accounting.admit(copied)?;
    label.value.extend_from_slice(identity.as_str().as_bytes());
    accounting.record_completed(copied)?;
    Ok(crate::kernel::Owned::new(String::from_utf8(label.value).map_err(WorkspaceError::engine)?, label.allocation, label.bytes))
}
