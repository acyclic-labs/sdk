//! Retained native handles must survive a child's standard-stream remapping.

#![allow(unsafe_code, reason = "owned close-on-exec descriptor relocation")]

use crate::{NativeOperationKind, NativeReadMeter, meter::native_control_attempt};
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

/// Consumes one handle, relocating it only when standard-stream installation
/// would overwrite it. The ordinary path performs no syscall or allocation.
/// Both old and new handles own RAII custody before any late recording error.
pub(crate) fn outside_standard_streams(
    descriptor: OwnedFd,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<OwnedFd> {
    if descriptor.as_raw_fd() >= 3 {
        return Ok(descriptor);
    }
    native_control_attempt(meter, NativeOperationKind::Write, || {
        // SAFETY: the original live owned descriptor remains borrowed through
        // this call; F_DUPFD_CLOEXEC returns a new unique descriptor >= 3.
        let moved = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if moved < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful fcntl transfers this new descriptor to its owner.
        Ok(unsafe { OwnedFd::from_raw_fd(moved) })
    })
}
