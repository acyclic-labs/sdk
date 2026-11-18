//! Whole-file advisory locks that are released before their file closes.
//!
//! An OS file lock (`flock` on Unix, `LockFileEx` on Windows) belongs to the
//! open file description, not to one descriptor. Closing a descriptor releases
//! it only once every copy of that description is closed, and any process
//! spawned concurrently holds copies of all descriptors until it execs, so a
//! lock "released" by close can outlive its owner by an arbitrary delay. An
//! explicit unlock releases the description for every holder at once.
//!
//! [`FileLock`] is the only way the workspace takes such a lock. It unlocks
//! explicitly when released or dropped, so close-to-release cannot be written.
//!
//! The lock is advisory on every platform: it excludes other lock holders and
//! nothing else. A Windows byte-range lock is mandatory for I/O on its range,
//! so locking the file's bytes, as `std::fs::File::lock` does, would make every
//! read of a locked file through another handle fail with
//! `ERROR_LOCK_VIOLATION`. Windows holders instead lock one byte that no file
//! can contain, which leaves file data readable and writable everywhere.
//! Contention is [`io::ErrorKind::WouldBlock`] on every platform.

#![expect(
    clippy::disallowed_methods,
    reason = "the one module that calls the raw file-lock primitives"
)]

use std::fs::File;
use std::io;
use std::sync::Mutex;

/// Which whole-file lock to take.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileLockMode {
    /// Any number of shared holders exclude every exclusive holder.
    Shared,
    /// One exclusive holder excludes every other holder.
    Exclusive,
}

/// One open file and the advisory lock this handle holds on it, if any.
///
/// Dropping a held lock unlocks it before the file closes. Contention is
/// reported as [`io::ErrorKind::WouldBlock`] on every platform.
#[derive(Debug)]
pub struct FileLock {
    file: File,
    held: Mutex<bool>,
}

impl FileLock {
    /// Wraps `file` without locking it.
    #[must_use]
    pub const fn new(file: File) -> Self {
        Self {
            file,
            held: Mutex::new(false),
        }
    }

    /// Locks `file`, waiting for every conflicting holder to release.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the lock cannot be taken.
    pub fn acquire(file: File, mode: FileLockMode) -> io::Result<Self> {
        let lock = Self::new(file);
        lock.lock(mode)?;
        Ok(lock)
    }

    /// Locks `file` only if no conflicting holder exists.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error.
    pub fn try_acquire(file: File, mode: FileLockMode) -> io::Result<Self> {
        let lock = Self::new(file);
        lock.try_lock(mode)?;
        Ok(lock)
    }

    /// Takes the lock on this file, waiting for conflicting holders.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the lock cannot be taken.
    pub fn lock(&self, mode: FileLockMode) -> io::Result<()> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::ensure_unheld(*held)?;
        sys::lock(&self.file, mode)?;
        *held = true;
        Ok(())
    }

    /// Runs `operation` while holding the requested lock, releasing it on
    /// return or unwind. The file and any other holders remain owned by their
    /// callers; releasing this scope does not close the file.
    ///
    /// # Errors
    ///
    /// Returns the original acquisition or operation error, otherwise the
    /// release error. Unwinding attempts release without replacing the panic.
    pub fn with_lock<T>(
        &self,
        mode: FileLockMode,
        operation: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        struct Release<'a>(Option<&'a FileLock>);

        impl Drop for Release<'_> {
            fn drop(&mut self) {
                if let Some(lock) = self.0.take() {
                    let _ = lock.unlock();
                }
            }
        }

        self.lock(mode)?;
        let mut release = Release(Some(self));
        let result = operation();
        // Disarm before unlocking: one release attempt, including on error.
        release.0 = None;
        let unlocked = self.unlock();
        match result {
            Ok(value) => unlocked.map(|()| value),
            Err(error) => Err(error),
        }
    }

    /// Takes the lock on this file only if no conflicting holder exists.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error.
    pub fn try_lock(&self, mode: FileLockMode) -> io::Result<()> {
        let mut held = match self.held.try_lock() {
            Ok(held) => held,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        Self::ensure_unheld(*held)?;
        sys::try_lock(&self.file, mode)?;
        *held = true;
        Ok(())
    }

    /// Takes the lock on `file` only if no conflicting holder exists, and
    /// keeps it, with `file` open, until this process exits. Only the exit
    /// releases it, so it suits a process that holds the lock for its whole
    /// life and shares the description with no process that outlives it.
    ///
    /// It makes only the lock system call and allocates nothing, so a
    /// forked child may call it before exec.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error; `file` is then closed.
    #[cfg(unix)]
    pub fn try_hold_until_exit(file: File, mode: FileLockMode) -> io::Result<()> {
        sys::try_lock(&file, mode)?;
        // The descriptor, and with it the lock, stays until exit.
        std::mem::forget(file);
        Ok(())
    }

    /// Releases the lock for every copy of this open file description.
    ///
    /// One release attempt per acquisition: after this returns, even with an
    /// error, the handle no longer holds the lock and dropping it does not
    /// unlock again. That keeps a later lock taken through another handle on
    /// the same description intact. Unlocking an unheld handle does nothing.
    ///
    /// # Errors
    ///
    /// Returns the OS error from the unlock.
    pub fn unlock(&self) -> io::Result<()> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*held {
            return Ok(());
        }
        *held = false;
        sys::unlock(&self.file)
    }

    /// Whether this handle currently holds its lock.
    #[must_use]
    pub fn is_held(&self) -> bool {
        *self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ensure_unheld(held: bool) -> io::Result<()> {
        if held {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file lock is already held by this handle",
            ));
        }
        Ok(())
    }
}

impl std::ops::Deref for FileLock {
    type Target = File;

    fn deref(&self) -> &File {
        &self.file
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Close alone would leave the lock to any concurrently forked child.
        let _ = self.unlock();
    }
}

/// `flock`, which locks the open file description and never its bytes.
#[cfg(not(windows))]
mod sys {
    use super::FileLockMode;
    use std::fs::File;
    use std::io;

    pub(super) fn lock(file: &File, mode: FileLockMode) -> io::Result<()> {
        match mode {
            FileLockMode::Shared => file.lock_shared(),
            FileLockMode::Exclusive => file.lock(),
        }
    }

    pub(super) fn try_lock(file: &File, mode: FileLockMode) -> io::Result<()> {
        match mode {
            FileLockMode::Shared => file.try_lock_shared(),
            FileLockMode::Exclusive => file.try_lock(),
        }
        .map_err(io::Error::from)
    }

    pub(super) fn unlock(file: &File) -> io::Result<()> {
        file.unlock()
    }
}

/// `LockFileEx` on the one lock byte, never on file data.
#[cfg(windows)]
mod sys {
    use super::FileLockMode;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, ERROR_LOCK_VIOLATION, TRUE};
    use windows_sys::Win32::Storage::FileSystem::{
        LOCK_FILE_FLAGS, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
        UnlockFileEx,
    };
    use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};
    use windows_sys::Win32::System::Threading::CreateEventW;

    /// The lock byte's offset is `2^62` (high word `2^30`, low word 0). No
    /// file system Windows supports stores a file that long, so no read or
    /// write of file data ever overlaps the locked range.
    const OFFSET_HIGH: u32 = 1 << 30;
    const LENGTH_LOW: u32 = 1;

    fn lock_byte() -> OVERLAPPED {
        let mut overlapped = OVERLAPPED::default();
        overlapped.Anonymous.Anonymous.OffsetHigh = OFFSET_HIGH;
        overlapped
    }

    fn flags(mode: FileLockMode) -> LOCK_FILE_FLAGS {
        match mode {
            FileLockMode::Shared => 0,
            FileLockMode::Exclusive => LOCKFILE_EXCLUSIVE_LOCK,
        }
    }

    fn is_os_error(error: &io::Error, code: u32) -> bool {
        error
            .raw_os_error()
            .is_some_and(|raw| u32::try_from(raw) == Ok(code))
    }

    pub(super) fn lock(file: &File, mode: FileLockMode) -> io::Result<()> {
        let handle = file.as_raw_handle();
        // A wait on an overlapped handle completes through this event.
        #[allow(unsafe_code, reason = "creates an owned unnamed event")]
        // SAFETY: every pointer argument is null, which requests defaults.
        let created = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        if created.is_null() {
            return Err(io::Error::last_os_error());
        }
        #[allow(unsafe_code, reason = "takes sole ownership of the new event")]
        // SAFETY: `created` is a valid event handle nothing else owns.
        let event = unsafe { OwnedHandle::from_raw_handle(created) };
        let mut overlapped = lock_byte();
        overlapped.hEvent = event.as_raw_handle();
        #[allow(unsafe_code, reason = "LockFileEx on a borrowed open file")]
        // SAFETY: `file` keeps the handle open, and `overlapped` and its
        // event outlive the call and any pending completion awaited below.
        let locked =
            unsafe { LockFileEx(handle, flags(mode), 0, LENGTH_LOW, 0, &raw mut overlapped) };
        if locked != 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if !is_os_error(&error, ERROR_IO_PENDING) {
            return Err(error);
        }
        let mut transferred = 0;
        #[allow(unsafe_code, reason = "awaits the pending lock's completion")]
        // SAFETY: the pending operation owns `overlapped` until this wait
        // returns, and both it and `transferred` live past the call.
        let completed = unsafe {
            GetOverlappedResult(handle, &raw const overlapped, &raw mut transferred, TRUE)
        };
        if completed == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn try_lock(file: &File, mode: FileLockMode) -> io::Result<()> {
        let mut overlapped = lock_byte();
        #[allow(unsafe_code, reason = "LockFileEx on a borrowed open file")]
        // SAFETY: `file` keeps the handle open and `overlapped` outlives the
        // call; a fail-immediately lock never stays pending.
        let locked = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                flags(mode) | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                LENGTH_LOW,
                0,
                &raw mut overlapped,
            )
        };
        if locked != 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if is_os_error(&error, ERROR_LOCK_VIOLATION) || is_os_error(&error, ERROR_IO_PENDING) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Err(error)
    }

    pub(super) fn unlock(file: &File) -> io::Result<()> {
        let mut overlapped = lock_byte();
        #[allow(unsafe_code, reason = "UnlockFileEx on a borrowed open file")]
        // SAFETY: `file` keeps the handle open and `overlapped` outlives the
        // call.
        let unlocked =
            unsafe { UnlockFileEx(file.as_raw_handle(), 0, LENGTH_LOW, 0, &raw mut overlapped) };
        if unlocked == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "file_lock_tests.rs"]
mod tests;
