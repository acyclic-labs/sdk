//! Physical descriptor setup shared by the host and socket transports.

use std::{io, os::fd::AsRawFd};

pub(super) fn nonblocking(file: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: the caller retains the descriptor throughout this operation.
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
