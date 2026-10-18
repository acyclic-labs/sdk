//! One native full flush of a held descriptor, with no internal retry.
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

/// Issues exactly one native full flush of `descriptor`: `fcntl(F_FULLFSYNC)`
/// on Apple, where a bare `fsync` stops at the drive cache, and `fsync` on
/// Linux. `EINTR` and every other failure return to the caller, whose metered
/// loop owns retry, charging and cancellation for each native attempt.
/// Unlike std's `sync_all`, nothing is retried here, and Apple never falls
/// back to a weaker flush.
#[allow(unsafe_code, reason = "one flush call on the caller's borrowed descriptor")]
pub fn sync_descriptor_once(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    // SAFETY: the borrowed descriptor stays open for this one flush.
    let result = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_FULLFSYNC) };
    #[cfg(target_os = "linux")]
    // SAFETY: the borrowed descriptor stays open for this one flush.
    let result = unsafe { libc::fsync(descriptor.as_raw_fd()) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
