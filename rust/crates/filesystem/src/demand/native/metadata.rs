fn node_kind(file_type: cap_std::fs::FileType) -> SourceNodeKind {
    if file_type.is_dir() {
        SourceNodeKind::Directory
    } else if file_type.is_file() {
        SourceNodeKind::RegularFile
    } else if file_type.is_symlink() {
        SourceNodeKind::SymbolicLink
    } else {
        special_node_kind(&file_type)
    }
}

#[cfg(unix)]
fn special_node_kind(file_type: &cap_std::fs::FileType) -> SourceNodeKind {
    if file_type.is_fifo() {
        SourceNodeKind::Fifo
    } else if file_type.is_socket() {
        SourceNodeKind::Socket
    } else if file_type.is_char_device() {
        SourceNodeKind::CharacterDevice
    } else if file_type.is_block_device() {
        SourceNodeKind::BlockDevice
    } else {
        SourceNodeKind::Unsupported
    }
}

#[cfg(not(unix))]
fn special_node_kind(_file_type: &cap_std::fs::FileType) -> SourceNodeKind {
    SourceNodeKind::Unsupported
}

#[cfg(unix)]
fn os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt as _;
    value.encode_wide().flat_map(u16::to_le_bytes).collect()
}

fn version(metadata: &HostStat) -> SourceVersion {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-source-version-v1\0");
    hash.update(&metadata.len().to_le_bytes());
    #[cfg(unix)]
    {
        hash.update(&metadata.dev().to_le_bytes());
        hash.update(&metadata.ino().to_le_bytes());
        hash.update(&metadata.mtime().to_le_bytes());
        hash.update(&metadata.mtime_nsec().to_le_bytes());
        hash.update(&metadata.ctime().to_le_bytes());
        hash.update(&metadata.ctime_nsec().to_le_bytes());
    }
    #[cfg(windows)]
    {
        hash.update(&metadata.file_attributes().to_le_bytes());
        hash.update(&metadata.last_write_time().to_le_bytes());
        hash.update(&metadata.creation_time().to_le_bytes());
        hash.update(&metadata.volume_serial_number().to_le_bytes());
        hash.update(&metadata.file_index().to_le_bytes());
    }
    SourceVersion(*hash.finalize().as_bytes())
}

/// A file's identity: its device and inode, and its birth time, which
#[cfg(unix)]
fn file_identity(metadata: &HostStat) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-fs-native-source-file-v1\0");
    hash.update(&metadata.dev().to_le_bytes());
    hash.update(&metadata.ino().to_le_bytes());
    if let Some(born) = metadata
        .created()
        .ok()
        .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok())
    {
        hash.update(&born.as_nanos().to_le_bytes());
    }
    *hash.finalize().as_bytes()
}

#[cfg(windows)]
fn file_identity(metadata: &HostStat) -> [u8; 32] {
    windows_file_identity(
        u64::from(metadata.volume_serial_number()),
        metadata.file_index(),
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

fn link_count(metadata: &HostStat) -> Option<u64> {
    #[cfg(unix)]
    {
        Some(metadata.nlink())
    }
    #[cfg(not(unix))]
    {
        metadata.number_of_links().map(u64::from)
    }
}

#[allow(
    clippy::useless_conversion,
    reason = "dev_t and major/minor result widths differ across Unix targets"
)]
fn device_identity(metadata: &HostStat, kind: SourceNodeKind) -> Option<(u32, u32)> {
    #[cfg(unix)]
    {
        if !matches!(
            kind,
            SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice
        ) {
            return None;
        }
        let raw: libc::dev_t = metadata.rdev().try_into().ok()?;
        Some((
            libc::major(raw).try_into().ok()?,
            libc::minor(raw).try_into().ok()?,
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = (metadata, kind);
        None
    }
}

fn source_metadata(metadata: &HostStat) -> SourceMetadata {
    let mut result = SourceMetadata {
        created_ns: metadata.created().ok().and_then(system_time_nanos),
        modified_ns: metadata.modified().ok().and_then(system_time_nanos),
        accessed_ns: metadata.accessed().ok().and_then(system_time_nanos),
        ..SourceMetadata::default()
    };
    #[cfg(unix)]
    {
        result.posix_mode = Some(metadata.mode());
        result.posix_uid = Some(metadata.uid());
        result.posix_gid = Some(metadata.gid());
        result.changed_ns =
            Some(i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()));
    }
    #[cfg(windows)]
    {
        result.windows_attributes = Some(metadata.file_attributes());
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
