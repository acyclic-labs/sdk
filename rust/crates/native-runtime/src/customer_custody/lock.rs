#![allow(unsafe_code, reason = "this private OS-FFI boundary owns validated mutex handles and held file descriptors")]
use super::CustodyError;

#[cfg(windows)]
pub(super) struct NamespaceLock(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
fn resolve_user_scope() -> Result<String, CustodyError> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    use windows_sys::Win32::Foundation::{CloseHandle, GetLastError};
    use windows_sys::Win32::Security::{GetLengthSid, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    let mut token = std::ptr::null_mut();
    // SAFETY: the pseudo process handle is valid and token points to owned output storage.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        // SAFETY: querying the current thread's last OS error requires no borrowed pointers.
        return Err(CustodyError::Platform(unsafe { GetLastError() }.into()));
    }
    let result = (|| {
        let mut size = 0;
        // SAFETY: this size query uses a live owned token and writable size output.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut size) };
        if size == 0 {
            // SAFETY: the failed size query set this thread's OS error.
            return Err(CustodyError::Platform(unsafe { GetLastError() }.into()));
        }
        // usize storage supplies TOKEN_USER alignment; the SID is public OS identity.
        let mut buffer = vec![0usize; (size as usize).div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: aligned buffer owns the queried byte capacity throughout this call.
        if unsafe { GetTokenInformation(token, TokenUser, buffer.as_mut_ptr().cast(), size, &mut size) } == 0 {
            // SAFETY: the failed token query set this thread's OS error.
            return Err(CustodyError::Platform(unsafe { GetLastError() }.into()));
        }
        // SAFETY: a successful TokenUser query initialized this aligned TOKEN_USER buffer.
        let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
        if user.User.Sid.is_null() {
            return Err(CustodyError::Unavailable);
        }
        // SAFETY: the live TOKEN_USER buffer retains its validated non-null SID.
        let length = unsafe { GetLengthSid(user.User.Sid) };
        if length == 0 {
            return Err(CustodyError::Unavailable);
        }
        // SAFETY: Windows returned this SID length; its storage lives in buffer for this borrow.
        let sid = unsafe { std::slice::from_raw_parts(user.User.Sid.cast::<u8>(), length as usize) };
        let mut scope = String::with_capacity(64);
        for byte in Sha256::digest(sid) {
            write!(scope, "{byte:02x}").map_err(|_| CustodyError::Unavailable)?;
        }
        Ok(scope)
    })();
    // SAFETY: token is the owned handle returned by the successful OpenProcessToken call.
    unsafe { CloseHandle(token) };
    result
}

#[cfg(windows)]
fn user_scope() -> Result<&'static str, CustodyError> {
    static SCOPE: std::sync::LazyLock<Result<String, CustodyError>> =
        std::sync::LazyLock::new(resolve_user_scope);
    match &*SCOPE {
        Ok(scope) => Ok(scope.as_str()),
        Err(CustodyError::Platform(code)) => Err(CustodyError::Platform(*code)),
        Err(_) => Err(CustodyError::Unavailable),
    }
}

#[cfg(windows)]
impl NamespaceLock {
    pub(super) fn acquire(name: &str) -> Result<Self, CustodyError> {
        use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, WAIT_ABANDONED, WAIT_OBJECT_0};
        use windows_sys::Win32::System::Threading::{CreateMutexW, INFINITE, WaitForSingleObject};
        // Credential Manager survives terminal sessions, so its fence must too.
        // Per-user SID isolation avoids collisions between separate users' vaults.
        let scope = user_scope()?;
        let name: Vec<u16> = format!("Global\\acyclic.customer-leaf.v1.{scope}.{name}")
            .encode_utf16().chain(Some(0)).collect();
        // SAFETY: name is live NUL-terminated UTF-16, and no security-attribute pointer is supplied.
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            // SAFETY: the failed mutex creation set this thread's OS error.
            return Err(CustodyError::Platform(unsafe { GetLastError() }.into()));
        }
        // SAFETY: handle is the successfully created live mutex retained until wait completes.
        match unsafe { WaitForSingleObject(handle, INFINITE) } {
            WAIT_OBJECT_0 | WAIT_ABANDONED => Ok(Self(handle)),
            _ => {
                // SAFETY: the failed wait set this thread's OS error.
                let error = unsafe { GetLastError() };
                // SAFETY: this failed acquisition still owns the created mutex handle.
                unsafe { CloseHandle(handle) };
                Err(CustodyError::Platform(error.into()))
            }
        }
    }
}

#[cfg(windows)]
impl Drop for NamespaceLock {
    fn drop(&mut self) {
        // SAFETY: this guard owns both the mutex acquisition and its live handle.
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
        // SAFETY: geteuid has no pointer arguments and reads this process's OS identity.
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
            // SAFETY: file owns this live descriptor for the entire lock operation.
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
        // SAFETY: self still owns the descriptor and is releasing only its own lock.
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}
