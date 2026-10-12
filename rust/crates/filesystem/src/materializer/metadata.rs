use super::*;
use crate::native_host::{HostRoot, HostStat};

pub(super) fn same_file_version(
    before: &HostStat,
    after: &HostStat,
) -> Result<bool, NativeTreeMaterializationError> {
    Ok(before.len() == after.len() && before.modified()? == after.modified()?)
}

pub(super) fn native_entry_fingerprint(
    entry: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    let Some(metadata) = host_stat(entry.0, entry.1, work)? else {
        return Ok(None);
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
    let Some(metadata) = host_stat(entry.0, entry.1, work)? else {
        return Ok(None);
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
    hash_bytes(&metadata.identity().to_bytes(), hasher, work)?;
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
        let attributes = if attributes & !0x80 == 0 {
            0x80
        } else {
            attributes & !0x80
        };
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
                        .read_link(path, work.remaining()?.source_bytes_read, meter)
                })?;
            for unit in target {
                hash_bytes(&unit.to_le_bytes(), hasher, work)?;
            }
        }
    } else if kind.is_dir() {
        let directory = { #[cfg(unix)] { entry.0.open_dir_held(entry.1, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(entry.1, &mut &*work, |path, work| entry.0.open_dir_held(path, work)) } }?;
        let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
        if identity != metadata.identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let root = HostRoot {
            directory,
            identity,
        };
        let (mut reader, _buffer) = { #[cfg(unix)] { root.read_dir_stats(Path::new(""), &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new(""), &mut &*work, |path, work| root.read_dir_stats(path, work)) } }?;
        let mut names = Vec::new();
        let mut capacity = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        while let Some(name) = next_name(&mut reader, work)? {
            native_vec_push(&mut names, name, &mut capacity)?;
        }
        work.charge(NativeResourceCredit::Items.delta(names.len() as u64))?;
        names.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for (name, _allocation) in names {
            hash_native_os_str(&name, hasher, work)?;
            let path = Path::new(&name);
            let metadata = { #[cfg(unix)] { root.stat(path, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(path, &mut &*work, |path, work| root.stat(path, work)) } }?;
            hash_native_entry(&(&root, path), &metadata, hasher, work)?;
        }
    } else if kind.is_file() {
        let file = { #[cfg(unix)] { entry.0.open_file(entry.1, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(entry.1, &mut &*work, |path, work| entry.0.open_file(path, ::windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT, work)) } }?.into_std();
        let actual = crate::native_host::stat_file(&file, &mut &*work)?;
        if actual.identity() != metadata.identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let size =
            usize::try_from(metadata.len().min(4096)).map_err(|_| crate::WorkError::Overflow)?;
        let (mut buffer, _allocation) = crate::native_host::buffer::<u8, _>(size, &mut &*work)?;
        work.charge(NativeResourceCredit::Copy.delta(size as u64))?;
        buffer.resize(size, 0);
        let mut remaining = metadata.len();
        while remaining != 0 {
            let maximum = buffer
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX))
                .min(usize::try_from(work.remaining()?.source_bytes_read).unwrap_or(usize::MAX));
            if maximum == 0 {
                work.admit(NativeResourceCredit::ReadBytes.delta(1))?;
            }
            let read = crate::native_host::builtin_io(
                acyclic_native_runtime::NativeOperationKind::Read,
                maximum,
                work,
                || std::io::Read::read(&mut &file, &mut buffer[..maximum]),
            )?;
            if read == 0 {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
            remaining -= read as u64;
            hash_bytes(&buffer[..read], hasher, work)?;
        }
        let after = crate::native_host::stat_file(&file, &mut &*work)?;
        if !same_file_version(metadata, &after)? {
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
#[cfg_attr(
    windows,
    allow(
        unsafe_code,
        reason = "updates metadata through a validated held handle"
    )
)]
pub(super) fn apply_native_metadata(
    entry: &(&HostRoot, &Path),
    desired: &NativeMetadataImage,
    expected: Option<[u8; 32]>,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let parent = { #[cfg(unix)] { entry.0.open_dir_held(entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut &*work, |path, work| entry.0.open_dir_held(path, work)) } }?;
    let leaf = Path::new(
        entry
            .1
            .file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let metadata = { #[cfg(unix)] { entry.0.stat(entry.1, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(entry.1, &mut &*work, |path, work| entry.0.stat(path, work)) } }?;
    #[cfg(unix)]
    let file = if metadata.is_dir() {
        { #[cfg(unix)] { entry.0.open_dir_held(entry.1, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(entry.1, &mut &*work, |path, work| entry.0.open_dir_held(path, work)) } }?.into_std_file()
    } else {
        crate::native_host::open_leaf(
            &parent,
            leaf,
            &crate::native_host::NativeOpenOptions::Read,
            &mut &*work,
        )?
    };
    #[cfg(windows)]
    let file = crate::native_host::with_wide_path(leaf, &mut &*work, |name, meter| {
        use windows::Win32::Storage::FileSystem::{
            FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
        };
        crate::native_host::open_windows_at(&parent, name, FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE, windows::Wdk::Storage::FileSystem::FILE_OPEN, windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT | if metadata.is_dir() { windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE } else { windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE }, meter)
    })?;
    let actual = crate::native_host::stat_file(&file, &mut &*work)?;
    let mut hasher = blake3::Hasher::new();
    hash_native_metadata_edit(&actual, None, &mut hasher, work)?;
    if actual.identity() != metadata.identity() || Some(*hasher.finalize().as_bytes()) != expected {
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
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_BASIC_INFO, FileBasicInfo, SetFileInformationByHandle,
        };
        let information = FILE_BASIC_INFO {
            FileAttributes: if attributes & !0x80 == 0 {
                0x80
            } else {
                attributes & !0x80
            },
            ..Default::default()
        };
        work.charge(NativeResourceCredit::WriteOperations.delta(1))?;
        // SAFETY: the witnessed handle and fixed-size input remain live through the call.
        unsafe {
            SetFileInformationByHandle(
                windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                FileBasicInfo,
                std::ptr::from_ref(&information).cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        }
        .map_err(|error| std::io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        crate::native_host::sync_directory_held(&parent, &mut &*work)?;
    }
    #[cfg(unix)]
    if let Some(mode) = desired.posix_mode {
        use std::os::unix::fs::PermissionsExt as _;
        loop {
            work.charge(NativeResourceCredit::WriteOperations.delta(1))?;
            match file.set_permissions(std::fs::Permissions::from_mode(mode)) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
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
    let mut writer = ImageWriter::new(work)?;
    writer.json(&metadata)?;
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
