use super::*;

#[derive(Clone, Copy)]
pub(super) struct HostSnapshot {
    pub(super) identity: NativeRootIdentity,
    pub(super) length: u64,
    pub(super) metadata: FileMetadata,
}

impl HostSnapshot {
    pub(super) fn from_metadata(metadata: &HostStat) -> Result<Self, CaptureError> {
        Ok(Self {
            identity: metadata.identity(),
            length: metadata.len(),
            metadata: capture_metadata(metadata),
        })
    }
}

pub(super) fn ensure_same_host_node(
    expected: &HostSnapshot,
    observed: &HostStat,
) -> Result<(), CaptureError> {
    ensure_same_host_snapshot(expected, &HostSnapshot::from_metadata(observed)?)
}

pub(super) fn ensure_same_host_snapshot(
    expected: &HostSnapshot,
    observed: &HostSnapshot,
) -> Result<(), CaptureError> {
    let mut expected_metadata = expected.metadata;
    let mut observed_metadata = observed.metadata;
    // Reading the file can legitimately update its access time.
    expected_metadata.accessed_ns = MetadataField::Unavailable;
    observed_metadata.accessed_ns = MetadataField::Unavailable;
    // Compare birth time only when both stat paths expose it; revalidate the final path snapshot.
    if matches!(expected_metadata.created_ns, MetadataField::Unavailable)
        || matches!(observed_metadata.created_ns, MetadataField::Unavailable)
    {
        expected_metadata.created_ns = MetadataField::Unavailable;
        observed_metadata.created_ns = MetadataField::Unavailable;
    }
    // Reading a projected placeholder hydrates it.
    #[cfg(windows)]
    if let (MetadataField::Value(before), MetadataField::Value(after)) = (
        expected_metadata.windows_attributes,
        observed_metadata.windows_attributes,
    ) && crate::native_host::same_windows_attributes(before, after)
    {
        observed_metadata.windows_attributes = expected_metadata.windows_attributes;
    }
    if expected.identity != observed.identity
        || expected.length != observed.length
        || expected_metadata != observed_metadata
    {
        return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
    }
    Ok(())
}

pub(super) fn ensure_current_host_node(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    expected: &HostSnapshot,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    cancellation.check()?;
    meter.native.clone().check().map_err(capture_demand_error)?;
    let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
    ensure_same_host_node(expected, &observed)?;
    cancellation.check()?;
    meter.native.clone().check().map_err(capture_demand_error)
}

#[cfg(test)]
pub(super) fn test_host_stat(root: &HostRoot, path: &Path) -> Result<HostStat, CaptureError> {
    let mut meter =
        crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat(path, &mut meter).map_err(capture_demand_error)
}

#[cfg(test)]
pub(super) fn test_held_stat(
    root: &HostRoot,
    file: &cap_std::fs::File,
) -> Result<HostStat, CaptureError> {
    let mut meter =
        crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat_file(&file.try_clone()?.into_std(), &mut meter)
        .map_err(capture_demand_error)
}

#[cfg(test)]
pub(super) fn test_host_path(path: &Path) -> CaptureHostPath {
    #[cfg(unix)]
    {
        path.to_path_buf()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        path.as_os_str().encode_wide().collect()
    }
}

pub(super) fn host_kind(metadata: &HostStat) -> Result<FileKind, CaptureError> {
    let kind = metadata.file_type();
    if kind.is_file() {
        return Ok(FileKind::Regular);
    }
    if kind.is_dir() {
        return Ok(FileKind::Directory);
    }
    if kind.is_symlink() {
        return Ok(FileKind::SymbolicLink);
    }
    #[cfg(unix)]
    {
        if kind.is_fifo() {
            return Ok(FileKind::Fifo);
        }
        if kind.is_socket() {
            return Ok(FileKind::Socket);
        }
        if kind.is_char_device() {
            return Ok(FileKind::CharacterDevice);
        }
        if kind.is_block_device() {
            return Ok(FileKind::BlockDevice);
        }
    }
    Err(CaptureError::UnsupportedKind)
}

#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
pub(super) fn host_device_identity(metadata: &HostStat) -> Result<(u32, u32), CaptureError> {
    Ok(split_device(metadata.rdev()))
}

#[cfg(target_os = "linux")]
pub(super) fn split_device(device: u64) -> (u32, u32) {
    (libc::major(device), libc::minor(device))
}

#[cfg(target_os = "macos")]
pub(super) fn split_device(device: u64) -> (u32, u32) {
    let low = u32::try_from(device & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    let native = i32::from_ne_bytes(low.to_ne_bytes());
    (
        u32::try_from(libc::major(native)).unwrap_or(u32::MAX),
        u32::try_from(libc::minor(native)).unwrap_or(u32::MAX),
    )
}

#[cfg(not(unix))]
pub(super) fn host_device_identity(_: &HostStat) -> Result<(u32, u32), CaptureError> {
    Err(CaptureError::UnsupportedKind)
}

pub(super) fn capture_metadata(metadata: &HostStat) -> FileMetadata {
    let mut result = FileMetadata {
        created_ns: system_time(metadata.created()),
        modified_ns: system_time(metadata.modified()),
        accessed_ns: system_time(metadata.accessed()),
        ..FileMetadata::default()
    };
    populate_platform_metadata(metadata, &mut result);
    result
}

pub(super) fn preserve_unobserved_metadata(
    observed: FileMetadata,
    prior: FileMetadata,
) -> FileMetadata {
    FileMetadata {
        posix_mode: preserve_field(observed.posix_mode, prior.posix_mode),
        posix_uid: preserve_field(observed.posix_uid, prior.posix_uid),
        posix_gid: preserve_field(observed.posix_gid, prior.posix_gid),
        posix_flags: preserve_field(observed.posix_flags, prior.posix_flags),
        windows_attributes: preserve_field(observed.windows_attributes, prior.windows_attributes),
        created_ns: preserve_field(observed.created_ns, prior.created_ns),
        modified_ns: preserve_field(observed.modified_ns, prior.modified_ns),
        accessed_ns: preserve_field(observed.accessed_ns, prior.accessed_ns),
        changed_ns: preserve_field(observed.changed_ns, prior.changed_ns),
        named_attributes: preserve_field(observed.named_attributes, prior.named_attributes),
        acl: preserve_field(observed.acl, prior.acl),
        security_descriptor: preserve_field(
            observed.security_descriptor,
            prior.security_descriptor,
        ),
    }
}

pub(super) fn preserve_field<T: Copy>(
    observed: MetadataField<T>,
    prior: MetadataField<T>,
) -> MetadataField<T> {
    match observed {
        MetadataField::Unavailable => prior,
        MetadataField::Value(_) => observed,
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn system_time(value: std::io::Result<cap_std::time::SystemTime>) -> MetadataField<i64> {
    let Ok(value) = value else {
        return MetadataField::Unavailable;
    };
    let nanos = match value.into_std().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[cfg(unix)]
pub(super) fn populate_platform_metadata(metadata: &HostStat, result: &mut FileMetadata) {
    result.posix_mode = MetadataField::Value(metadata.mode());
    result.posix_uid = MetadataField::Value(metadata.uid());
    result.posix_gid = MetadataField::Value(metadata.gid());
    let nanos = i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec());
    result.changed_ns =
        i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value);
}

#[cfg(windows)]
pub(super) fn populate_platform_metadata(metadata: &HostStat, result: &mut FileMetadata) {
    result.windows_attributes = MetadataField::Value(metadata.file_attributes());
}
