use std::{io, os::fd::AsRawFd};

pub(super) fn flags(file: &impl AsRawFd) -> io::Result<i32> {
    #[expect(
        unsafe_code,
        reason = "checks that the original shared flags are unchanged"
    )]
    // SAFETY: F_GETFL reads flags on the still-borrowed descriptor.
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(flags)
    }
}
