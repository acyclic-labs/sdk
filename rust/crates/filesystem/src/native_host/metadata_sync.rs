use super::*;

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
