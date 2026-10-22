use super::*;

pub(super) fn link_placeholder(info: PRJ_FILE_BASIC_INFO) -> PRJ_PLACEHOLDER_INFO {
    PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        ..PRJ_PLACEHOLDER_INFO::default()
    }
}

pub(super) fn reject_stale_projection(
    destination: &std::path::Path,
) -> Result<(), NativeMountError> {
    match reparse_tag(destination)?.0 {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {
            Err(NativeMountError::RecoveryOwnershipUnknown)
        }
        Some(tag) => Err(NativeMountError::UnsafeRecoveryDestination {
            reparse_tag: Some(tag),
        }),
        None => Ok(()),
    }
}

// ProjFS cannot start a virtualization root below another one: marking the
// nested directory fails with ERROR_FILE_SYSTEM_VIRTUALIZATION_INVALID_OPERATION
// only after the destination fence and driver state exist. Reject that layout
// first, as its own typed cause, while nothing physical has been created.
pub(super) fn reject_nested_projection(
    destination: &std::path::Path,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Foundation::ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    use windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS;
    // Open with normal traversal: a lexical ancestor walk can miss a ProjFS
    // root hidden behind a directory junction or symbolic link. Resolve the
    // held candidate through GetFinalPathNameByHandleW before walking parents.
    let directory = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
        std::fs::OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES.0)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0)
            .open(destination)
    })?;
    let directory = cap_std::fs::Dir::from_std_file(directory);
    let (canonical, _canonical_allocation) =
        crate::native_host::canonical_directory_wide(&directory, &mut &*work)?;
    let (canonical, _path_allocation) =
        crate::native_host::decode_windows_name(&canonical, &mut &*work)?;
    let destination = std::path::PathBuf::from(canonical);
    for ancestor in destination.ancestors().skip(1) {
        // The ProjFS filter hides a virtualization root's tag from attribute
        // queries; only the reparse buffer itself reports it.
        let tag = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
            let file = std::fs::OpenOptions::new()
                .access_mode(FILE_READ_ATTRIBUTES.0)
                .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
                .open(ancestor)?;
            match reparse_data(&file, |data| {
                data.first_chunk().map(|tag| u32::from_le_bytes(*tag))
            }) {
                Err(error)
                    if error.raw_os_error()
                        == i32::try_from(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0).ok() =>
                {
                    Ok(Some(IO_REPARSE_TAG_PROJFS))
                }
                result => result.map(Option::flatten),
            }
        })?;
        if tag == Some(IO_REPARSE_TAG_PROJFS) {
            return Err(NativeMountError::NestedDestination);
        }
    }
    Ok(())
}

pub(super) fn reparse_tag(
    path: &std::path::Path,
) -> Result<(Option<u32>, crate::NativeRootIdentity), NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Foundation::ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let identity = crate::NativeRootIdentity::from_file(&file)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    match reparse_data(&file, |data| data.first_chunk().copied()) {
        Ok(None) => Ok((None, identity)),
        Ok(Some(data)) => {
            let tag = data.ok_or_else(|| {
                NativeMountError::Driver("reparse response omitted its tag".to_owned())
            })?;
            Ok((Some(u32::from_le_bytes(tag)), identity))
        }
        Err(error)
            if error.raw_os_error()
                == i32::try_from(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0).ok() =>
        {
            Ok((
                Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS),
                identity,
            ))
        }
        Err(error) => Err(NativeMountError::Driver(error.to_string())),
    }
}

pub(super) fn reparse_data<T>(
    file: &impl std::os::windows::io::AsRawHandle,
    inspect: impl FnOnce(&[u8]) -> T,
) -> std::io::Result<Option<T>> {
    use windows::Win32::Foundation::{ERROR_NOT_A_REPARSE_POINT, HANDLE};
    use windows::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;

    let mut output = [0_u8; MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize];
    let mut returned = 0_u32;
    // SAFETY: the handle and output buffer remain valid for the synchronous
    let result = unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            Some(output.as_mut_ptr().cast()),
            MAXIMUM_REPARSE_DATA_BUFFER_SIZE,
            Some(&raw mut returned),
            None,
        )
    };
    match result {
        Ok(()) => {
            let data = output
                .get(..returned as usize)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
            Ok(Some(inspect(data)))
        }
        Err(error) if error.code() == ERROR_NOT_A_REPARSE_POINT.to_hresult() => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn driver_error(error: &windows::core::Error) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

pub(super) struct KeyedLocks<K>(
    pub(super) RankedMutex<HashMap<K, Arc<RankedMutex<(), OPERATION>>>, VIEW>,
);
