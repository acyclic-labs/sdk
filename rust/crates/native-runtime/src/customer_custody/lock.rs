use super::CustodyError;

#[cfg(windows)]
pub(super) struct NamespaceLock(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl NamespaceLock {
    pub(super) fn acquire(name: &str) -> Result<Self, CustodyError> {
        use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, WAIT_ABANDONED, WAIT_OBJECT_0};
        use windows_sys::Win32::System::Threading::{CreateMutexW, INFINITE, WaitForSingleObject};
        let name: Vec<u16> = format!("Local\\acyclic.customer-leaf.v1.{name}")
            .encode_utf16().chain(Some(0)).collect();
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(CustodyError::Platform(unsafe { GetLastError() }.into()));
        }
        match unsafe { WaitForSingleObject(handle, INFINITE) } {
            WAIT_OBJECT_0 | WAIT_ABANDONED => Ok(Self(handle)),
            _ => {
                let error = unsafe { GetLastError() };
                unsafe { CloseHandle(handle) };
                Err(CustodyError::Platform(error.into()))
            }
        }
    }
}

#[cfg(windows)]
impl Drop for NamespaceLock {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::System::Threading::ReleaseMutex(self.0);
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

#[cfg(unix)]
pub(super) struct NamespaceLock(std::fs::File);

#[cfg(unix)]
impl NamespaceLock {
    pub(super) fn acquire(name: &str) -> Result<Self, CustodyError> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
        // Only lock metadata is stored on disk. Keys and sessions never enter this directory.
        let uid = unsafe { libc::geteuid() };
        let directory = std::path::PathBuf::from(format!("/tmp/acyclic-customer-custody-{uid}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(_) => return Err(CustodyError::Unavailable),
        }
        let metadata = std::fs::symlink_metadata(&directory).map_err(|_| CustodyError::Unavailable)?;
        if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
            return Err(CustodyError::UnsafeLock);
        }
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true)
            .mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(directory.join(name)).map_err(|_| CustodyError::Unavailable)?;
        let metadata = file.metadata().map_err(|_| CustodyError::Unavailable)?;
        if !metadata.is_file() || metadata.uid() != uid || metadata.nlink() != 1
            || metadata.mode() & 0o777 != 0o600 {
            return Err(CustodyError::UnsafeLock);
        }
        loop {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Self(file));
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(CustodyError::Unavailable);
            }
        }
    }
}

#[cfg(unix)]
impl Drop for NamespaceLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}
