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
            Err(NativeMountError::Driver(format!(
                "stale ProjFS root at {} may contain unpublished authored state; preserved for recovery",
                destination.display()
            )))
        }
        Some(tag) => Err(NativeMountError::Driver(format!(
            "destination has non-ProjFS reparse tag 0x{tag:08x}"
        ))),
        None => Ok(()),
    }
}

pub(in crate::native_mount) fn quarantine_crashed_destination(
    destination: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, NativeMountError> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(NativeMountError::Driver(error.to_string())),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    let (tag, expected_identity) = reparse_tag(destination)?;
    match tag {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => {
            return Err(NativeMountError::Driver(format!(
                "destination has non-ProjFS reparse tag 0x{tag:08x}"
            )));
        }
        None => {
            let mut entries = std::fs::read_dir(destination)
                .map_err(|error| NativeMountError::Driver(error.to_string()))?;
            if entries.next().is_none() {
                return Ok(None);
            }
        }
    }

    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?;
    let preserved = parent.join(format!(
        ".acyclic-residue-{}",
        uuid::Uuid::new_v4().simple()
    ));
    if preserved
        .try_exists()
        .map_err(|error| NativeMountError::Driver(error.to_string()))?
    {
        return Err(NativeMountError::Driver(
            "native recovery residue name collided; retry recovery".to_owned(),
        ));
    }
    std::fs::rename(destination, &preserved).map_err(|error| {
        NativeMountError::Driver(format!(
            "cannot preserve crashed ProjFS destination {}: {error}",
            destination.display()
        ))
    })?;
    let (_, moved_identity) = reparse_tag(&preserved)?;
    if moved_identity != expected_identity {
        return Err(NativeMountError::Driver(format!(
            "recovered ProjFS root identity changed; residue retained at {}",
            preserved.display()
        )));
    }
    Ok(Some(preserved))
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
    use std::os::windows::io::AsRawHandle as _;
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

pub(super) struct KeyedLocks<K>(pub(super) Mutex<HashMap<K, Arc<Mutex<()>>>>);
