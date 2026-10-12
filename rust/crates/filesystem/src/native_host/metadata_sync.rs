use super::*;

pub(crate) fn sync_metadata_held<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    if name != Path::new(".") {
        require_leaf(name)?;
    }
    #[cfg(unix)]
    {
        let file = unix_name(name.as_os_str(), work, |name, work| {
            open_unix(
                parent.as_raw_fd(),
                name,
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
                work,
            )
        })?;
        sync_file_held(&file, work)
    }
    #[cfg(windows)]
    {
        sync_windows_metadata_held(parent, name, None, None, work)
    }
}

#[cfg(windows)]
pub(super) fn sync_windows_metadata_held<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    requested: Option<u32>,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    use windows::Wdk::Storage::FileSystem::{
        FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY, FILE_BASIC_INFO,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_WRITE,
        FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
    };
    let file = open_windows_held(
        parent,
        name,
        FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    if let Some(expected) = expected {
        if file_identity_and_length(&file, work)?.0 != expected {
            return Err(io::Error::other("metadata target identity changed").into());
        }
    }
    let current = query_windows_basic_info(&file, work)?;
    let writable = if current.FileAttributes & FILE_ATTRIBUTE_READONLY.0 == 0 {
        reopen_windows_file(
            &file,
            FILE_GENERIC_WRITE,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            work,
        )?
    } else {
        let restore = work.reserve_cleanup_write()?;
        let delta = WRITE;
        if let Err(error) = work.admit(delta).and_then(|()| work.record(delta)) {
            work.cancel_cleanup_write(restore);
            return Err(error);
        }
        let unlocked = current.FileAttributes & !FILE_ATTRIBUTE_READONLY.0;
        let information = FILE_BASIC_INFO {
            FileAttributes: if unlocked == 0 {
                FILE_ATTRIBUTE_NORMAL.0
            } else {
                unlocked
            },
            ..Default::default()
        };
        let reopened = match set_windows_basic_info(&file, information) {
            Ok(()) => reopen_windows_file(
                &file,
                FILE_GENERIC_WRITE,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                work,
            ),
            Err(error) => Err(error.into()),
        };
        let (restored, recorded) = work.attempt_cleanup_write(restore, || {
            set_windows_basic_info(
                &file,
                FILE_BASIC_INFO {
                    FileAttributes: current.FileAttributes,
                    ..Default::default()
                },
            )
        });
        restored?;
        recorded?;
        reopened?
    };
    if let Some(attributes) = requested {
        work.attempt_io(WRITE, || {
            set_windows_basic_info(
                &file,
                FILE_BASIC_INFO {
                    FileAttributes: attributes,
                    ..Default::default()
                },
            )
        })?;
    }
    sync_file_held(&writable, work)
}

#[cfg(windows)]
pub(super) fn reopen_windows_file<W: NativeHostWork>(
    file: &File,
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    flags: windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES,
    work: &mut W,
) -> Result<File, W::Error> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, ReOpenFile,
    };
    work.attempt_io(READ, || {
        // SAFETY: the live handle pins the same inode across rename/replacement.
        let handle = unsafe {
            ReOpenFile(
                HANDLE(file.as_raw_handle()),
                access.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                flags,
            )
        }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        // SAFETY: successful ReOpenFile transferred a new exclusively owned handle.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })
}

#[cfg(windows)]
pub(super) fn query_windows_basic_info<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<windows::Win32::Storage::FileSystem::FILE_BASIC_INFO, W::Error> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx,
    };
    work.attempt_io(READ, || {
        let mut information = FILE_BASIC_INFO::default();
        // SAFETY: the held handle and correctly sized output remain live.
        unsafe {
            GetFileInformationByHandleEx(
                HANDLE(file.as_raw_handle()),
                FileBasicInfo,
                (&raw mut information).cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        Ok(information)
    })
}

#[cfg(windows)]
pub(super) fn set_windows_basic_info(
    file: &impl std::os::windows::io::AsRawHandle,
    information: windows::Win32::Storage::FileSystem::FILE_BASIC_INFO,
) -> io::Result<()> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FileBasicInfo, SetFileInformationByHandle,
    };
    // SAFETY: the held handle and complete fixed-size input remain live.
    unsafe {
        SetFileInformationByHandle(
            HANDLE(file.as_raw_handle()),
            FileBasicInfo,
            (&raw const information).cast(),
            std::mem::size_of::<FILE_BASIC_INFO>() as u32,
        )
    }
    .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}
