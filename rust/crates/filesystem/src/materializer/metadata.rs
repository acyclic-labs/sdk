use super::*;
use crate::native_host::{HostRoot, HostStat};

pub(super) fn native_entry_fingerprint(
    entry: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    let metadata = match entry.0.stat(entry.1, &mut &*work) {
        Ok(metadata) => metadata,
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let mut hasher = blake3::Hasher::new();
    hash_native_entry(entry, &metadata, &mut hasher, work)?;
    Ok(Some(*hasher.finalize().as_bytes()))
}
pub(super) fn native_entry_fingerprint_for_edit(
    entry: &(&HostRoot, &Path),
    edit: &MaterializationEdit,
    work: &NativeMaterializationWork,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
        native_metadata_fingerprint(entry, None, work)
    } else {
        native_entry_fingerprint(entry, work)
    }
}
pub(super) fn native_metadata_fingerprint(
    entry: &(&HostRoot, &Path),
    desired: Option<&NativeMetadataImage>,
    work: &NativeMaterializationWork,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    let metadata = match entry.0.stat(entry.1, &mut &*work) {
        Ok(metadata) => metadata,
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let mut hasher = blake3::Hasher::new();
    hash_native_metadata_edit(&metadata, desired, &mut hasher, work)?;
    Ok(Some(*hasher.finalize().as_bytes()))
}
fn hash_bytes(
    bytes: &[u8],
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    work.charge(crate::WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    hasher.update(bytes);
    Ok(())
}
fn hash_native_metadata_edit(
    metadata: &HostStat,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let kind = metadata.file_type();
    hash_bytes(
        if kind.is_symlink() {
            b"link"
        } else if kind.is_dir() {
            b"directory"
        } else if kind.is_file() {
            b"file"
        } else {
            b"special"
        },
        hasher,
        work,
    )?;
    hash_bytes(
        &metadata
            .identity()
            .map_err(native_materialization_work_error)?
            .to_bytes(),
        hasher,
        work,
    )?;
    #[cfg(unix)]
    hash_bytes(
        &u64::from(
            desired
                .and_then(|value| value.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        )
        .to_le_bytes(),
        hasher,
        work,
    )?;
    #[cfg(windows)]
    {
        let attributes = desired
            .and_then(|value| value.windows_attributes)
            .unwrap_or_else(|| match desired {
                Some(value) if value.readonly => metadata.file_attributes() | 1,
                Some(_) => metadata.file_attributes() & !1,
                None => metadata.file_attributes(),
            });
        hash_bytes(&attributes.to_le_bytes(), hasher, work)?;
    }
    Ok(())
}
fn hash_native_entry(
    entry: &(&HostRoot, &Path),
    metadata: &HostStat,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    hash_native_metadata_edit(metadata, None, hasher, work)?;
    hash_bytes(&metadata.len().to_le_bytes(), hasher, work)?;
    #[cfg(unix)]
    {
        for value in [
            metadata.nlink(),
            u64::from(metadata.uid()),
            u64::from(metadata.gid()),
            metadata.rdev(),
        ] {
            hash_bytes(&value.to_le_bytes(), hasher, work)?;
        }
        hash_bytes(&metadata.mtime().to_le_bytes(), hasher, work)?;
        hash_bytes(&metadata.mtime_nsec().to_le_bytes(), hasher, work)?;
    }
    #[cfg(windows)]
    hash_bytes(&metadata.last_write_time().to_le_bytes(), hasher, work)?;
    let kind = metadata.file_type();
    if kind.is_symlink() {
        #[cfg(unix)]
        {
            let (target, _allocation) =
                entry
                    .0
                    .read_link(entry.1, work.remaining()?.source_bytes_read, &mut &*work)?;
            hash_bytes(&target, hasher, work)?;
        }
        #[cfg(windows)]
        {
            let (target, _allocation) =
                crate::native_host::with_wide_path(entry.1, &mut &*work, |path, meter| {
                    entry
                        .0
                        .read_link_wide(path, work.remaining()?.source_bytes_read, meter)
                })?;
            for unit in target {
                hash_bytes(&unit.to_le_bytes(), hasher, work)?;
            }
        }
    } else if kind.is_dir() {
        let directory = entry.0.open_dir_held(entry.1, &mut &*work)?;
        let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
        if identity
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let root = HostRoot {
            directory,
            identity,
        };
        let (mut reader, _buffer) = root.read_dir_stats(Path::new(""), &mut &*work)?;
        let mut names = Vec::new();
        let mut capacity = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        #[cfg(unix)]
        while let Some((entry, allocation)) = reader.next(&mut &*work)? {
            native_vec_push(&mut names, (entry.name, allocation), &mut capacity)?;
        }
        #[cfg(windows)]
        while let Some((entry, _wide)) = reader.next_wide(&mut &*work)? {
            use std::os::windows::ffi::OsStringExt as _;
            let mut allocation = work.claim(
                owned_size(entry.name.len(), 3)?,
                1,
                crate::WorkCounters::UNCHARGED,
            )?;
            let name = std::ffi::OsString::from_wide(&entry.name);
            allocation.reconcile_capacity(name.capacity() as u64)?;
            native_vec_push(&mut names, (name, allocation), &mut capacity)?;
        }
        work.charge(NativeResourceCredit::Items.delta(names.len() as u64))?;
        names.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for (name, _allocation) in names {
            hash_native_os_str(&name, hasher, work)?;
            let path = Path::new(&name);
            let metadata = root.stat(path, &mut &*work)?;
            hash_native_entry(&(&root, path), &metadata, hasher, work)?;
        }
    } else if kind.is_file() {
        let file = entry.0.open_file(entry.1, &mut &*work)?.into_std();
        let actual = crate::native_host::stat_file(&file, &mut &*work)?;
        if actual
            .identity()
            .map_err(native_materialization_work_error)?
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let size =
            usize::try_from(metadata.len().min(4096)).map_err(|_| crate::WorkError::Overflow)?;
        let mut buffer = crate::native_host::native_buffer::<u8>(size, work)?;
        work.charge(NativeResourceCredit::Copy.delta(size as u64))?;
        buffer.data.resize(size, 0);
        let mut remaining = metadata.len();
        while remaining != 0 {
            let maximum = buffer
                .data
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX))
                .min(usize::try_from(work.remaining()?.source_bytes_read).unwrap_or(usize::MAX));
            if maximum == 0 {
                work.admit(NativeResourceCredit::ReadBytes.delta(1))?;
            }
            let read =
                crate::native_host::read_builtin_file(&file, &mut buffer.data[..maximum], work)?;
            if read == 0 {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
            remaining -= read as u64;
            hash_bytes(&buffer.data[..read], hasher, work)?;
        }
        let after = crate::native_host::stat_file(&file, &mut &*work)?;
        if after.len() != metadata.len() || after.modified()? != metadata.modified()? {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
    }
    Ok(())
}
pub(super) fn native_metadata(metadata: &HostStat) -> NativeMetadataImage {
    NativeMetadataImage {
        #[cfg(unix)]
        readonly: metadata.mode() & 0o222 == 0,
        #[cfg(windows)]
        readonly: metadata.file_attributes() & 1 != 0,
        #[cfg(unix)]
        posix_mode: Some(metadata.mode()),
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: Some(metadata.file_attributes()),
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}
pub(super) fn apply_native_metadata(
    entry: &(&HostRoot, &Path),
    desired: &NativeMetadataImage,
    expected: Option<[u8; 32]>,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let parent = entry.0.open_dir_held(
        entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    let leaf = Path::new(
        entry
            .1
            .file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let metadata = entry.0.stat(entry.1, &mut &*work)?;
    let file = if metadata.is_dir() {
        entry.0.open_dir_held(entry.1, &mut &*work)?.into_std_file()
    } else {
        crate::native_host::open_leaf(
            &parent,
            leaf,
            &crate::native_host::NativeOpenOptions::Read,
            &mut &*work,
        )?
    };
    let actual = crate::native_host::stat_file(&file, &mut &*work)?;
    let mut hasher = blake3::Hasher::new();
    hash_native_metadata_edit(&actual, None, &mut hasher, work)?;
    if actual
        .identity()
        .map_err(native_materialization_work_error)?
        != metadata
            .identity()
            .map_err(native_materialization_work_error)?
        || Some(*hasher.finalize().as_bytes()) != expected
    {
        return Err(NativeTreeMaterializationError::ExternalMutation);
    }
    #[cfg(windows)]
    {
        let attributes = desired.windows_attributes.unwrap_or_else(|| {
            if desired.readonly {
                actual.file_attributes() | 1
            } else {
                actual.file_attributes() & !1
            }
        });
        crate::native_host::set_file_attributes_held(
            &parent,
            leaf,
            attributes,
            Some(
                actual
                    .identity()
                    .map_err(native_materialization_work_error)?,
            ),
            work,
        )?;
    }
    #[cfg(unix)]
    if let Some(mode) = desired.posix_mode {
        use std::os::fd::AsRawFd as _;
        loop {
            work.charge(NativeResourceCredit::WriteOperations.delta(1))?;
            // SAFETY: the descriptor holds the witnessed entry throughout this call.
            if unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) } == 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
        crate::native_host::sync_file_held(&file, &mut &*work)?;
    }
    Ok(())
}
#[cfg(all(feature = "local", feature = "native-mount"))]
pub(super) fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let metadata = NativeMetadataImage {
        readonly: metadata
            .windows_attributes
            .is_some_and(|value| value & 1 != 0)
            || metadata.posix_mode.is_some_and(|value| value & 0o222 == 0),
        posix_mode: metadata.posix_mode,
        windows_attributes: metadata.windows_attributes,
    };
    let mut writer = ImageWriter {
        bytes: Vec::new(),
        allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        failure: None,
    };
    if serde_json::to_writer(&mut writer, &metadata).is_err() {
        return Err(writer
            .failure
            .take()
            .unwrap_or(NativeTreeMaterializationError::InvalidLayout));
    }
    Ok((writer.bytes, writer.allocation))
}
#[cfg(unix)]
fn hash_native_os_str(
    value: &std::ffi::OsStr,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    use std::os::unix::ffi::OsStrExt as _;
    hash_bytes(value.as_bytes(), hasher, work)
}
#[cfg(windows)]
fn hash_native_os_str(
    value: &std::ffi::OsStr,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    use std::os::windows::ffi::OsStrExt as _;
    for unit in value.encode_wide() {
        hash_bytes(&unit.to_le_bytes(), hasher, work)?;
    }
    Ok(())
}
