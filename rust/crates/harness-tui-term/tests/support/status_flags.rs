//! The one way terminal restoration tests compare descriptor status flags.
use rustix::fs::{OFlags, fcntl_getfl};
use std::os::fd::AsFd;

/// Exactly the status flags `F_SETFL` can change, which are the only flags a
/// terminal session can save and restore. Everything else `F_GETFL` reports
/// is fixed for the open file: the access mode, and on Darwin the kernel's
/// `FWASWRITTEN` bit, set once the file has been written.
///
/// - Darwin: `FCNTLFLAGS` in xnu `bsd/sys/fcntl.h` (`FAPPEND`, `FASYNC`,
///   `FFSYNC`, `FFDSYNC`, `FNONBLOCK`).
/// - Linux: `fcntl(2)` (`O_APPEND`, `O_ASYNC`, `O_DIRECT`, `O_NOATIME`,
///   `O_NONBLOCK`).
pub fn settable_status_flags(file: impl AsFd) -> std::io::Result<OFlags> {
    #[cfg(target_vendor = "apple")]
    let settable =
        OFlags::APPEND | OFlags::ASYNC | OFlags::FSYNC | OFlags::DSYNC | OFlags::NONBLOCK;
    #[cfg(target_os = "linux")]
    let settable =
        OFlags::APPEND | OFlags::ASYNC | OFlags::DIRECT | OFlags::NOATIME | OFlags::NONBLOCK;
    #[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
    let settable = OFlags::APPEND | OFlags::NONBLOCK;
    Ok(fcntl_getfl(file)? & settable)
}
