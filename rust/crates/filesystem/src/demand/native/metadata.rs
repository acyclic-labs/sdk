pub(crate) fn path_key(path: &NamespacePath) -> Vec<u8> {
    let mut key = Vec::new();
    for name in path.components() {
        key.extend_from_slice(&(name.as_bytes().len() as u64).to_le_bytes());
        key.extend_from_slice(name.as_bytes());
    }
    key
}

fn child_path_key(parent: &NamespacePath, name: &crate::kernel::LogicalName) -> Vec<u8> {
    let mut key = path_key(parent);
    key.extend_from_slice(&(name.as_bytes().len() as u64).to_le_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

#[cfg(feature = "test-support")]
fn fixture_times(metadata: &HostStat) -> Option<acyclic_native_runtime::testing::NativeTimes> {
    use acyclic_native_runtime::testing::{NativeMetadataInput, NativeTimes, capture_native_times};
    let identity = metadata.identity();
    let nanos = |time: Option<i128>| time.and_then(|time| i64::try_from(time).ok());
    let times = NativeTimes {
        created_ns: nanos(metadata.birth_time().and_then(system_time_nanos)),
        modified_ns: nanos(metadata.modified().ok().and_then(system_time_nanos)),
        accessed_ns: nanos(metadata.accessed().ok().and_then(system_time_nanos)),
        changed_ns: nanos(source_metadata(metadata).changed_ns),
    };
    capture_native_times(NativeMetadataInput {
        device: identity.device,
        object: identity.object,
        directory: metadata.is_dir(),
        times,
    })
}

fn version(path: &[u8], metadata: &HostStat) -> SourceVersion {
    #[cfg(feature = "test-support")]
    if let Some(times) = fixture_times(metadata) {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-version-v1\0");
        hash.update(&metadata.len().to_le_bytes());
        hash.update(path);
        hash.update(&times.modified_ns.unwrap_or_default().to_le_bytes());
        hash.update(&times.changed_ns.unwrap_or_default().to_le_bytes());
        return SourceVersion(*hash.finalize().as_bytes());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-source-version-v1\0");
    hash.update(&metadata.len().to_le_bytes());
    #[cfg(unix)]
    {
        hash.update(&(metadata.raw.st_dev as u64).to_le_bytes());
        hash.update(&(metadata.raw.st_ino as u64).to_le_bytes());
        hash.update(&(metadata.raw.st_mtime as i64).to_le_bytes());
        hash.update(&(metadata.raw.st_mtime_nsec as i64).to_le_bytes());
        hash.update(&(metadata.raw.st_ctime as i64).to_le_bytes());
        hash.update(&(metadata.raw.st_ctime_nsec as i64).to_le_bytes());
    }
    #[cfg(windows)]
    {
        hash.update(&metadata.raw.FileAttributes.to_le_bytes());
        hash.update(&metadata.raw.LastWriteTime.to_le_bytes());
        hash.update(&metadata.raw.CreationTime.to_le_bytes());
        hash.update(&metadata.volume_serial_number.to_le_bytes());
        hash.update(&metadata.raw.FileId.to_le_bytes());
    }
    SourceVersion(*hash.finalize().as_bytes())
}

/// A file's identity: its device and inode, its file handle, which carries
/// the inode's generation where the file system issues handles, and its
/// birth time. Either tells a new file apart from a removed one whose inode
/// the host reused; the birth time alone does not when both were born within
/// one tick of its clock. Every name of one file shares it. A file system
/// with neither handles nor birth times (a few) cannot tell reuse apart by
/// identity alone.
#[cfg(unix)]
pub(crate) fn file_identity(path: &[u8], metadata: &HostStat) -> [u8; 32] {
    #[cfg(feature = "test-support")]
    if fixture_times(metadata).is_some() {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-fixture-v1\0");
        hash.update(path);
        hash.update(&metadata.len().to_le_bytes());
        return *hash.finalize().as_bytes();
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-source-file-v1\0");
    hash.update(&(metadata.raw.st_dev as u64).to_le_bytes());
    hash.update(&(metadata.raw.st_ino as u64).to_le_bytes());
    if let Some(handle) = metadata.handle() {
        hash.update(handle);
    }
    if let Some(born) = metadata
        .birth_time()
        .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok())
    {
        hash.update(&born.as_nanos().to_le_bytes());
    }
    *hash.finalize().as_bytes()
}

#[cfg(windows)]
pub(crate) fn file_identity(path: &[u8], metadata: &HostStat) -> [u8; 32] {
    #[cfg(feature = "test-support")]
    if fixture_times(metadata).is_some() {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-fixture-v1\0");
        hash.update(path);
        hash.update(&metadata.len().to_le_bytes());
        return *hash.finalize().as_bytes();
    }
    windows_file_identity(
        u64::from(metadata.volume_serial_number),
        metadata.raw.FileId.cast_unsigned(),
    )
}

/// The identity of the file with host index `index` on the volume whose
#[cfg(windows)]
fn windows_file_identity(volume: u64, index: u64) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-source-file-v1\0");
    hash.update(&u32::try_from(volume).unwrap_or_default().to_le_bytes());
    hash.update(&index.to_le_bytes());
    *hash.finalize().as_bytes()
}

fn source_metadata(metadata: &HostStat) -> SourceMetadata {
    let mut result = SourceMetadata {
        created_ns: metadata.birth_time().and_then(system_time_nanos),
        modified_ns: metadata.modified().ok().and_then(system_time_nanos),
        accessed_ns: metadata.accessed().ok().and_then(system_time_nanos),
        ..SourceMetadata::default()
    };
    #[cfg(unix)]
    {
        result.posix_mode = Some(metadata.raw.st_mode as u32);
        result.posix_uid = Some(metadata.raw.st_uid as u32);
        result.posix_gid = Some(metadata.raw.st_gid as u32);
        result.changed_ns = Some(
            i128::from(metadata.raw.st_ctime as i64) * 1_000_000_000
                + i128::from(metadata.raw.st_ctime_nsec as i64),
        );
    }
    #[cfg(windows)]
    {
        result.windows_attributes = Some(metadata.raw.FileAttributes);
    }
    result
}

fn system_time_nanos(time: cap_std::time::SystemTime) -> Option<i128> {
    match time.into_std().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => Some(
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()),
        ),
        Err(error) => Some(
            -(i128::from(error.duration().as_secs()) * 1_000_000_000
                + i128::from(error.duration().subsec_nanos())),
        ),
    }
}
