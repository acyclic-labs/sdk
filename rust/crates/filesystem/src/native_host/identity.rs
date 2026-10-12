use super::*;

pub(crate) fn directory_identity<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    work.attempt_io( WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
        || held_root_identity(directory))
}

#[cfg(unix)]
pub(crate) fn directory_has_ancestor<W: NativeHostWork>(
    directory: &Dir, ancestor: NativeRootIdentity, work: &mut W,
) -> Result<bool, W::Error> {
    let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    work.admit(examined)?;
    work.record(examined)?;
    let mut identity = loop {
        match directory_identity(directory, work) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => break result?,
        }
    };
    if identity == ancestor { return Ok(true); }
    let mut current = None::<Dir>;
    loop {
        work.admit(examined)?;
        work.record(examined)?;
        let child = current.as_ref().unwrap_or(directory);
        let parent = Dir::from_std_file(open_unix(child.as_raw_fd(), c"..",
            cwd_metadata_flags(), || native_read(work))?);
        let parent_identity = loop {
            match directory_identity(&parent, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
                result => break result?,
            }
        };
        if parent_identity == ancestor { return Ok(true); }
        if parent_identity == identity { return Ok(false); }
        identity = parent_identity;
        current = Some(parent);
    }
}

pub(crate) fn file_identity_and_length<W: NativeHostWork>(
    file: &File, work: &mut W,
) -> Result<(NativeRootIdentity, u64), W::Error> {
    #[cfg(unix)]
    {
        loop {
            let result = work.attempt_io( WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
                    return Ok(Err(io::Error::last_os_error()));
                }
                let stat = unsafe { stat.assume_init() };
                let length = u64::try_from(stat.st_size)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative file length"));
                Ok(length.map(|length| (NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 }, length)))
            })?;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
        work.attempt_io( WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            unsafe { GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &raw mut information) }
                .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
            Ok((NativeRootIdentity { device: u64::from(information.dwVolumeSerialNumber),
                object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow) },
                (u64::from(information.nFileSizeHigh) << 32) | u64::from(information.nFileSizeLow)))
        })
    }
}

pub(crate) fn entry_identity<W: NativeHostWork>(
    parent: &Dir, name: &Path, work: &mut W,
) -> Result<Option<NativeRootIdentity>, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        unix_name(name.as_os_str(), work, |name, work| loop {
            let result = work.attempt_io( WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: held parent, terminated leaf, and correctly sized output stay live; the entry itself is never followed.
                if unsafe { libc::fstatat(parent.as_raw_fd(), name.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
                    return Ok(Err(io::Error::last_os_error()));
                }
                let stat = unsafe { stat.assume_init() };
                Ok(Ok(NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 }))
            })?;
            match result {
                Ok(identity) => return Ok(Some(identity)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        })
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let file = match open_windows_held(parent, name, FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work) {
            Ok(file) => file,
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        file_identity_and_length(&file, work).map(|(identity, _)| Some(identity))
    }
}
