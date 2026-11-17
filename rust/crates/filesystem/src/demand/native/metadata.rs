pub(crate) fn path_key(path: &NamespacePath) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-path-key-v1\0");
    for name in path.components() {
        hash.update(&(name.as_bytes().len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
    }
    *hash.finalize().as_bytes()
}

fn child_path_key(parent: &NamespacePath, name: &crate::kernel::LogicalName) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-path-key-v1\0");
    for component in parent.components() {
        hash.update(&(component.as_bytes().len() as u64).to_le_bytes());
        hash.update(component.as_bytes());
    }
    hash.update(&(name.as_bytes().len() as u64).to_le_bytes());
    hash.update(name.as_bytes());
    *hash.finalize().as_bytes()
}

#[cfg(feature = "test-support")]
pub(crate) fn fixture_times(metadata: &HostStat) -> Option<acyclic_native_runtime::testing::NativeTimes> {
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

#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct FixtureObject {
    device: u64,
    object: u64,
    birth_ns: Option<i128>,
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct FixtureVersions {
    identities: std::collections::HashMap<FixtureObject, [u8; 32]>,
}

#[cfg(feature = "test-support")]
fn fixture_versions() -> &'static std::sync::Mutex<FixtureVersions> {
    static VERSIONS: std::sync::OnceLock<std::sync::Mutex<FixtureVersions>> =
        std::sync::OnceLock::new();
    VERSIONS.get_or_init(|| std::sync::Mutex::new(FixtureVersions::default()))
}

/// Starts a new replay's fixture identity sequence. A world owns one physical
/// directory, so its first observation of each object is the same in every
/// replay.
#[cfg(feature = "test-support")]
pub fn reset_fixture_versions() {
    *fixture_versions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = FixtureVersions::default();
}

/// A fixture identity is the first logical name observed for one physical
/// object. Every alias of that object shares it, and a rename keeps it, while
/// a replacement is a new object with a new identity. The physical key is the
/// host's, so a replay's recreated files each get the identity of the name
/// they are first observed under, independent of the host's inode numbers.
#[cfg(feature = "test-support")]
fn fixture_identity(path: &[u8], metadata: &HostStat) -> Option<[u8; 32]> {
    fixture_times(metadata)?;
    let identity = metadata.identity();
    let object = FixtureObject {
        device: identity.device,
        object: identity.object,
        birth_ns: metadata.birth_time().and_then(system_time_nanos),
    };
    let mut versions = fixture_versions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Some(*versions.identities.entry(object).or_insert_with(|| {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-file-fixture-v2\0");
        hash.update(path);
        hash.update(&metadata.len().to_le_bytes());
        *hash.finalize().as_bytes()
    }))
}

#[cfg(feature = "test-support")]
pub(crate) type FixtureContent = Option<[u8; 32]>;
#[cfg(not(feature = "test-support"))]
pub(crate) type FixtureContent = ();

/// Hashes a regular file's bytes for its fixture version. The closure is not
/// called outside a fixture world, so production never reads content here.
#[cfg(feature = "test-support")]
pub(crate) fn fixture_content(
    metadata: &HostStat,
    read: impl FnOnce(&mut blake3::Hasher) -> std::io::Result<()>,
) -> FixtureContent {
    if metadata.file_type() != SourceNodeKind::RegularFile || fixture_times(metadata).is_none() {
        return None;
    }
    let mut hasher = blake3::Hasher::new();
    read(&mut hasher).ok()?;
    Some(*hasher.finalize().as_bytes())
}

#[cfg(not(feature = "test-support"))]
pub(crate) fn fixture_content(
    _: &HostStat,
    _: impl FnOnce(&mut blake3::Hasher) -> std::io::Result<()>,
) -> FixtureContent {
}

fn version(path: &[u8], content: FixtureContent, metadata: &HostStat) -> SourceVersion {
    #[cfg(feature = "test-support")]
    if let Some(identity) = fixture_identity(path, metadata) {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-version-fixture-v2\0");
        hash.update(&identity);
        if let Some(content) = content {
            hash.update(b"content\0");
            hash.update(&content);
        } else {
            hash.update(b"path\0");
            hash.update(path);
            hash.update(&metadata.len().to_le_bytes());
        }
        return SourceVersion(*hash.finalize().as_bytes());
    }
    #[cfg(not(feature = "test-support"))]
    let _ = (path, content);
    physical_version(metadata)
}

/// The host's own change token: length, identity and timestamps. It is never
/// written into a replay trace; fixture versions above are.
pub(crate) fn physical_version(metadata: &HostStat) -> SourceVersion {
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
pub(crate) fn file_identity(
    path: &[u8],
    content: FixtureContent,
    metadata: &HostStat,
) -> [u8; 32] {
    #[cfg(feature = "test-support")]
    if let Some(identity) = fixture_identity(path, metadata) {
        if let Some(content) = content {
            let mut hash = blake3::Hasher::new();
            hash.update(b"acyclic-fs-native-file-fixture-content-v1\0");
            hash.update(&content);
            return *hash.finalize().as_bytes();
        }
        return identity;
    }
    #[cfg(not(feature = "test-support"))]
    let _ = (path, content);
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
pub(crate) fn file_identity(
    path: &[u8],
    content: FixtureContent,
    metadata: &HostStat,
) -> [u8; 32] {
    #[cfg(feature = "test-support")]
    if let Some(identity) = fixture_identity(path, metadata) {
        if let Some(content) = content {
            let mut hash = blake3::Hasher::new();
            hash.update(b"acyclic-fs-native-file-fixture-content-v1\0");
            hash.update(&content);
            return *hash.finalize().as_bytes();
        }
        return identity;
    }
    #[cfg(not(feature = "test-support"))]
    let _ = (path, content);
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
