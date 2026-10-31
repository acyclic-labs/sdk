//! The system calls the record and warden code makes, over the C library.
//!
//! The Linux runner, a separate image without the C library, provides the
//! same names over raw system calls (`runner/sys.rs`), so the record and
//! warden code is one implementation for both.

use core::ffi::CStr;
use libc::c_int;

pub(super) type Fd = c_int;
pub(super) type Pid = libc::pid_t;

/// A system call's error number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Errno(pub(super) c_int);

impl From<Errno> for std::io::Error {
    fn from(error: Errno) -> Self {
        Self::from_raw_os_error(error.0)
    }
}

#[cfg(target_os = "linux")]
pub(super) use libc::O_RDONLY;
pub(super) use libc::{EINTR, EINVAL, O_CLOEXEC};

fn errno() -> Errno {
    Errno(
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO),
    )
}

fn check(result: c_int) -> Result<c_int, Errno> {
    if result < 0 { Err(errno()) } else { Ok(result) }
}

#[cfg(target_os = "linux")]
pub(super) fn open(path: &CStr, flags: c_int) -> Result<Fd, Errno> {
    // SAFETY: a NUL-terminated path.
    check(unsafe { libc::open(path.as_ptr(), flags) })
}

pub(super) fn read(fd: Fd, buffer: &mut [u8]) -> Result<usize, Errno> {
    // SAFETY: writable storage of the stated length.
    let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
    usize::try_from(read).map_err(|_| errno())
}

pub(super) fn close(fd: Fd) {
    // SAFETY: closes a descriptor this process owns.
    unsafe { libc::close(fd) };
}

#[cfg(target_vendor = "apple")]
pub(super) use apple::*;

/// The calls only the macOS warden makes here; the Linux warden runs in the
/// runner.
#[cfg(target_vendor = "apple")]
mod apple {
    use super::{CStr, Errno, Fd, Pid, c_int, check, errno};

    pub(in super::super) use libc::{
        ENOENT, EPERM, EPIPE, O_CREAT, O_EXCL, O_RDWR, SIGCHLD, SIGHUP, SIGINT, SIGKILL, SIGPIPE,
        SIGQUIT, SIGTERM, WNOHANG,
    };

    pub(in super::super) fn openat(
        directory: Fd,
        name: &CStr,
        flags: c_int,
        mode: u32,
    ) -> Result<Fd, Errno> {
        // SAFETY: a live directory descriptor and a NUL-terminated name.
        check(unsafe { libc::openat(directory, name.as_ptr(), flags, mode) })
    }

    pub(in super::super) fn unlinkat(directory: Fd, name: &CStr) -> Result<(), Errno> {
        // SAFETY: a live directory descriptor and a NUL-terminated name.
        check(unsafe { libc::unlinkat(directory, name.as_ptr(), 0) }).map(drop)
    }

    pub(in super::super) fn write(fd: Fd, bytes: &[u8]) -> Result<usize, Errno> {
        // SAFETY: readable storage of the stated length.
        let written = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        usize::try_from(written).map_err(|_| errno())
    }

    /// A pipe whose ends a forked child keeps.
    pub(in super::super) fn pipe() -> Result<[Fd; 2], Errno> {
        let mut ends = [0; 2];
        // SAFETY: writable storage for two descriptors.
        check(unsafe { libc::pipe(ends.as_mut_ptr()) })?;
        Ok(ends)
    }

    pub(in super::super) fn fork() -> Result<Pid, Errno> {
        // SAFETY: fork in a single-threaded child; both branches make only
        // async-signal-safe calls until exec or exit.
        check(unsafe { libc::fork() })
    }

    pub(in super::super) fn getpid() -> Pid {
        // SAFETY: no preconditions.
        unsafe { libc::getpid() }
    }

    pub(in super::super) fn getppid() -> Pid {
        // SAFETY: no preconditions.
        unsafe { libc::getppid() }
    }

    pub(in super::super) fn setpgid(pid: Pid, group: Pid) -> Result<(), Errno> {
        // SAFETY: changes the group of this process or its unexeced child.
        check(unsafe { libc::setpgid(pid, group) }).map(drop)
    }

    pub(in super::super) fn kill(pid: Pid, signal: c_int) -> Result<(), Errno> {
        // SAFETY: sends a signal; no memory is involved.
        check(unsafe { libc::kill(pid, signal) }).map(drop)
    }

    /// Waits for `pid` and returns it with its raw status.
    pub(in super::super) fn wait(pid: Pid, flags: c_int) -> Result<(Pid, c_int), Errno> {
        let mut status = 0;
        // SAFETY: writable status storage.
        let reaped = check(unsafe { libc::waitpid(pid, &raw mut status, flags) })?;
        Ok((reaped, status))
    }

    /// Takes `fd`'s exclusive lock without waiting, held until this
    /// process exits.
    pub(in super::super) fn try_lock(fd: Fd) -> Result<(), Errno> {
        use std::os::fd::FromRawFd as _;
        // SAFETY: the record descriptor the warden opened, which stays open
        // for the warden's whole life.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        crate::FileLock::try_hold_until_exit(file, crate::FileLockMode::Exclusive)
            .map_err(|error| Errno(error.raw_os_error().unwrap_or(libc::EIO)))
    }

    /// Sets `signal`'s action to ignore it, or to its default.
    pub(in super::super) fn set_signal(signal: c_int, ignore: bool) {
        let action = if ignore { libc::SIG_IGN } else { libc::SIG_DFL };
        // SAFETY: setting one disposition.
        unsafe { libc::signal(signal, action) };
    }

    pub(in super::super) fn block_all() {
        // SAFETY: a filled set from zeroed storage, then setting the mask.
        unsafe {
            let mut set: libc::sigset_t = core::mem::zeroed();
            libc::sigfillset(&raw mut set);
            libc::sigprocmask(libc::SIG_SETMASK, &raw const set, core::ptr::null_mut());
        }
    }

    pub(in super::super) fn unblock(signal: c_int) {
        // SAFETY: a one-signal set from zeroed storage, then unblocking it.
        unsafe {
            let mut set: libc::sigset_t = core::mem::zeroed();
            libc::sigemptyset(&raw mut set);
            libc::sigaddset(&raw mut set, signal);
            libc::sigprocmask(libc::SIG_UNBLOCK, &raw const set, core::ptr::null_mut());
        }
    }

    pub(in super::super) fn exit(code: c_int) -> ! {
        // SAFETY: immediate exit without running host destructors.
        unsafe { libc::_exit(code) }
    }
}
