use super::ReportPort;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    io,
    os::fd::{AsRawFd as _, FromRawFd as _},
};

static TAKEN: AtomicBool = AtomicBool::new(false);

pub(super) fn open() -> io::Result<Option<ReportPort>> {
    let Some(descriptor) = std::env::var_os("ACYCLIC_REPORT_FD") else {
        return Ok(None);
    };
    let descriptor: i32 = descriptor
        .to_str()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 3)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid inherited report descriptor",
            )
        })?;
    let generation = std::env::var("ACYCLIC_REPORT_GENERATION").map_err(io::Error::other)?;
    if TAKEN.swap(true, Ordering::AcqRel) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "inherited report endpoint already taken",
        ));
    }
    // The launch contract transfers this descriptor exactly once. Make it
    // close-on-exec so later children cannot retain the selected owner's port.
    #[expect(
        unsafe_code,
        reason = "takes the original inherited report socket and fences child inheritance"
    )]
    let stream = unsafe {
        let flags = libc::fcntl(descriptor, libc::F_GETFD);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::FD_CLOEXEC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "report descriptor was not inherited by this launch",
            ));
        }
        let mut kind: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&kind) as libc::socklen_t;
        if libc::getsockopt(
            descriptor,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&raw mut kind).cast(),
            &raw mut length,
        ) < 0
        {
            return Err(io::Error::last_os_error());
        }
        if kind != libc::SOCK_STREAM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "report endpoint is not a stream socket",
            ));
        }
        let stream = std::os::unix::net::UnixStream::from_raw_fd(descriptor);
        if libc::fcntl(stream.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
        stream
    };
    stream.set_nonblocking(true)?;
    ReportPort::new(generation, tokio::net::UnixStream::from_std(stream)?).map(Some)
}
