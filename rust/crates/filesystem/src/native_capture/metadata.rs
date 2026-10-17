use super::*;

pub(super) fn ensure_same_host_node(
    expected: &HostStat,
    observed: &HostStat,
) -> Result<(), CaptureError> {
    let mut expected_metadata = capture_metadata(expected);
    let mut observed_metadata = capture_metadata(observed);
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
    if expected.identity() != observed.identity()
        || expected.len() != observed.len()
        || expected_metadata != observed_metadata
    {
        return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
    }
    Ok(())
}

pub(super) fn ensure_current_host_node(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    expected: &HostStat,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    meter.clone().check().map_err(capture_demand_error)?;
    let observed = source_root
        .stat_source_metadata(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(expected, &observed)?;
    meter.clone().check().map_err(capture_demand_error)
}

#[cfg(test)]
pub(super) fn test_host_stat(root: &HostRoot, path: &Path) -> Result<HostStat, CaptureError> {
    let mut meter =
        crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    #[cfg(unix)]
    let result = root.stat(path, &mut meter);
    #[cfg(windows)]
    let result =
        crate::native_host::with_wide_path(path, &mut meter, |path, work| root.stat(path, work));
    result.map_err(capture_demand_error)
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
    use crate::demand::SourceNodeKind as Kind;
    Ok(match metadata.file_type() {
        Kind::RegularFile => FileKind::Regular,
        Kind::Directory => FileKind::Directory,
        Kind::SymbolicLink => FileKind::SymbolicLink,
        #[cfg(unix)]
        Kind::Fifo => FileKind::Fifo,
        #[cfg(unix)]
        Kind::Socket => FileKind::Socket,
        #[cfg(unix)]
        Kind::CharacterDevice => FileKind::CharacterDevice,
        #[cfg(unix)]
        Kind::BlockDevice => FileKind::BlockDevice,
        _ => return Err(CaptureError::UnsupportedKind),
    })
}

#[cfg(unix)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "The shared capture path also handles platforms without device identities."
)]
pub(super) fn host_device_identity(metadata: &HostStat) -> Result<(u32, u32), CaptureError> {
    let device = metadata.raw.st_rdev;
    #[cfg(target_os = "linux")]
    let identity = (libc::major(device), libc::minor(device));
    #[cfg(target_os = "macos")]
    let identity = (
        u32::try_from(libc::major(device)).unwrap_or(u32::MAX),
        u32::try_from(libc::minor(device)).unwrap_or(u32::MAX),
    );
    Ok(identity)
}

#[cfg(not(unix))]
pub(super) fn host_device_identity(_: &HostStat) -> Result<(u32, u32), CaptureError> {
    Err(CaptureError::UnsupportedKind)
}

pub(super) fn capture_metadata(metadata: &HostStat) -> FileMetadata {
    let created = metadata.birth_time();
    #[cfg(all(test, any(target_os = "linux", windows)))]
    let created = if crate::native_host::stat_probe::legacy_birth_time() {
        metadata.created().ok()
    } else {
        created
    };
    let mut result = FileMetadata {
        created_ns: system_time(created),
        modified_ns: system_time(metadata.modified().ok()),
        accessed_ns: system_time(metadata.accessed().ok()),
        ..FileMetadata::default()
    };
    populate_platform_metadata(metadata, &mut result);
    result
}

pub(super) fn preserve_unobserved_metadata(
    mut observed: FileMetadata,
    prior: FileMetadata,
) -> FileMetadata {
    macro_rules! preserve {
        ($($field:ident),+) => {$(
            if matches!(observed.$field, MetadataField::Unavailable) {
                observed.$field = prior.$field;
            }
        )+};
    }
    preserve!(
        posix_mode,
        posix_uid,
        posix_gid,
        posix_flags,
        windows_attributes,
        created_ns,
        modified_ns,
        accessed_ns,
        changed_ns,
        named_attributes,
        acl,
        security_descriptor
    );
    observed
}

pub(crate) fn system_time(value: Option<cap_std::time::SystemTime>) -> MetadataField<i64> {
    let Some(value) = value else {
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
    #[allow(
        clippy::unnecessary_cast,
        clippy::cast_lossless,
        reason = "mode_t is u16 on macOS and u32 on Linux."
    )]
    let mode = metadata.raw.st_mode as u32;
    result.posix_mode = MetadataField::Value(mode);
    result.posix_uid = MetadataField::Value(metadata.raw.st_uid);
    result.posix_gid = MetadataField::Value(metadata.raw.st_gid);
    let nanos =
        i128::from(metadata.raw.st_ctime) * 1_000_000_000 + i128::from(metadata.raw.st_ctime_nsec);
    result.changed_ns =
        i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value);
}

#[cfg(windows)]
pub(super) fn populate_platform_metadata(metadata: &HostStat, result: &mut FileMetadata) {
    result.windows_attributes = MetadataField::Value(metadata.raw.FileAttributes);
}

#[cfg(test)]
pub(super) fn test_open_file(root: &HostRoot, path: &Path) -> std::io::Result<cap_std::fs::File> {
    let mut work = crate::native_host::ordinary_work();
    #[cfg(unix)]
    let result = root.open_file(path, &mut work);
    #[cfg(windows)]
    let result = crate::native_host::with_wide_path(path, &mut work, |path, work| {
        root.open_file(
            path,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
            work,
        )
    });
    result.map_err(crate::native_host::ordinary_error)
}
