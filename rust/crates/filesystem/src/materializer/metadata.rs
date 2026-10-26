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
    host_path(entry.1, &mut &*work, |path, _| {
        hash_native_entry(entry.0, path, &metadata, &mut hasher, work)
    })?;
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
    desired: Option<&crate::WorkspaceMetadata>,
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
    desired: Option<&crate::WorkspaceMetadata>,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let kind = metadata.file_type();
    hash_bytes(
        if kind == crate::demand::SourceNodeKind::SymbolicLink {
            b"link"
        } else if kind == crate::demand::SourceNodeKind::Directory {
            b"directory"
        } else if kind == crate::demand::SourceNodeKind::RegularFile {
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
                .unwrap_or_else(|| metadata.raw.st_mode as u32),
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
                Some(value)
                    if value
                        .windows_attributes
                        .is_some_and(|attributes| attributes & 1 != 0) =>
                {
                    metadata.raw.FileAttributes | 1
                }
                Some(_) => metadata.raw.FileAttributes & !1,
                None => metadata.raw.FileAttributes,
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
    root: &HostRoot,
    #[cfg(unix)] path: &Path,
    #[cfg(windows)] path: &[u16],
    metadata: &HostStat,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    hash_native_metadata_edit(metadata, None, hasher, work)?;
    #[cfg(unix)]
    let facts = [
        metadata.len(),
        metadata.nlink(),
        u64::from(metadata.raw.st_uid as u32),
        u64::from(metadata.raw.st_gid as u32),
        (metadata.raw.st_rdev as u64),
        (metadata.raw.st_mtime as i64) as u64,
        (metadata.raw.st_mtime_nsec as i64) as u64,
    ];
    #[cfg(windows)]
    let facts = [metadata.len(), metadata.raw.LastWriteTime.cast_unsigned()];
    for value in facts {
        hash_bytes(&value.to_le_bytes(), hasher, work)?;
    }
    let kind = metadata.file_type();
    if kind == crate::demand::SourceNodeKind::SymbolicLink {
        let (target, _allocation) =
            root.read_link(path, work.remaining()?.source_bytes_read, &mut &*work)?;
        #[cfg(unix)]
        hash_bytes(&target, hasher, work)?;
        #[cfg(windows)]
        for unit in target {
            hash_bytes(&unit.to_le_bytes(), hasher, work)?;
        }
    } else if kind == crate::demand::SourceNodeKind::Directory {
        let directory = root.open_dir_held(path, &mut &*work)?;
        let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
        if identity != metadata.identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let root = HostRoot::from_parts(directory, identity);
        let (mut reader, _buffer) =
            crate::native_host::read_dir_held(&root.directory, &mut &*work)?;
        let mut names = Vec::new();
        let mut capacity = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        while let Some((entry, allocation)) = reader.next(&mut &*work)? {
            push_buffer(&mut names, (entry.name, allocation), &mut capacity)?;
        }
        work.charge(NativeResourceCredit::Items.delta(names.len() as u64))?;
        names.sort_unstable_by(|a, b| {
            #[cfg(unix)]
            {
                a.0.cmp(&b.0)
            }
            #[cfg(windows)]
            {
                native_utf16_name_bytes(a.0.iter().copied())
                    .cmp(native_utf16_name_bytes(b.0.iter().copied()))
            }
        });
        for (name, _allocation) in names {
            hash_native_name(&name, hasher, work)?;
            #[cfg(unix)]
            let path = Path::new(&name);
            #[cfg(windows)]
            let path = name.as_slice();
            let metadata = root.stat(path, &mut &*work)?;
            hash_native_entry(&root, path, &metadata, hasher, work)?;
        }
    } else if kind == crate::demand::SourceNodeKind::RegularFile {
        let file = root
            .open_file(
                path,
                #[cfg(windows)]
                ::windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
                &mut &*work,
            )?
            .into_std();
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
            let read = crate::native_host::file_transfer(
                acyclic_native_runtime::NativeOperationKind::Read,
                maximum,
                work,
                || {
                    std::io::Read::read(
                        &mut &file,
                        buffer.get_mut(..maximum).ok_or_else(|| {
                            std::io::Error::from(std::io::ErrorKind::InvalidInput)
                        })?,
                    )
                },
            )?;
            if read == 0 {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
            remaining = remaining
                .checked_sub(read as u64)
                .ok_or(NativeTreeMaterializationError::ExternalMutation)?;
            hash_bytes(
                buffer
                    .get(..read)
                    .ok_or(NativeTreeMaterializationError::ExternalMutation)?,
                hasher,
                work,
            )?;
        }
        if !same_file_version(
            metadata,
            &crate::native_host::stat_file(&file, &mut &*work)?,
        )? {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
    }
    Ok(())
}
pub(super) fn native_metadata(metadata: &HostStat) -> crate::WorkspaceMetadata {
    let mut value = crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
    #[cfg(unix)]
    {
        value.posix_mode = Some(metadata.raw.st_mode as u32);
    }
    #[cfg(windows)]
    {
        value.windows_attributes = Some(metadata.raw.FileAttributes);
    }
    value
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
    desired: &crate::WorkspaceMetadata,
    expected: Option<[u8; 32]>,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let parent = host_path(
        entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
        |path, work| entry.0.open_dir_held(path, work),
    )?;
    let leaf = Path::new(
        entry
            .1
            .file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let metadata = host_path(entry.1, &mut &*work, |path, work| entry.0.stat(path, work))?;
    #[cfg(unix)]
    let file = if metadata.is_dir() {
        host_path(entry.1, &mut &*work, |path, work| {
            entry.0.open_dir_held(path, work)
        })?
        .into_std_file()
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
        crate::native_host::open_windows_at(
            &parent,
            name,
            FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
            windows::Wdk::Storage::FileSystem::FILE_OPEN,
            windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT
                | if metadata.is_dir() {
                    windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE
                } else {
                    windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE
                },
            meter,
        )
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
            if desired
                .windows_attributes
                .is_some_and(|attributes| attributes & 1 != 0)
            {
                actual.raw.FileAttributes | 1
            } else {
                actual.raw.FileAttributes & !1
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
        let length = u32::try_from(std::mem::size_of::<FILE_BASIC_INFO>())
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        work.charge(NativeResourceCredit::WriteOperations.delta(1))?;
        // SAFETY: the witnessed handle and fixed-size input remain live through the call.
        unsafe {
            SetFileInformationByHandle(
                windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                FileBasicInfo,
                std::ptr::from_ref(&information).cast(),
                length,
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
fn hash_native_name(
    #[cfg(unix)] value: &std::ffi::OsStr,
    #[cfg(windows)] value: &[u16],
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        hash_bytes(value.as_bytes(), hasher, work)
    }
    #[cfg(windows)]
    {
        for unit in value {
            hash_bytes(&unit.to_le_bytes(), hasher, work)?;
        }
        Ok(())
    }
}
