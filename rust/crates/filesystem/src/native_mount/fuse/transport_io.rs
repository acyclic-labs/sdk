//! Physical descriptor setup shared by the host and socket transports.

use acyclic_native_runtime::ProcessWake;
use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd},
};

#[derive(PartialEq)]
pub(super) enum Ready {
    Device(i16),
    Wake,
    Closed,
}

/// Physical readiness retains the actual borrowed descriptors. The caller
/// consumes its wake at its own configuration boundary; this wait never
/// consumes a kick belonging to a replaced queue.
pub(super) fn wait(
    wake: &ProcessWake,
    descriptor: Option<BorrowedFd<'_>>,
    events: i16,
) -> io::Result<Ready> {
    let mut fds = [
        libc::pollfd {
            fd: wake.as_fd().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: descriptor.map_or(-1, |fd| fd.as_raw_fd()),
            events,
            revents: 0,
        },
    ];
    loop {
        if wake.is_stopped() {
            return Ok(Ready::Closed);
        }
        // SAFETY: the wake and optional borrowed descriptor remain owned
        // through poll; no queue reconfiguration can close either borrower.
        if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } >= 0 {
            if wake.is_stopped() {
                return Ok(Ready::Closed);
            }
            if fds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return Err(io::Error::from_raw_os_error(libc::EIO));
            }
            if fds[0].revents & libc::POLLIN != 0 {
                return Ok(Ready::Wake);
            }
            if fds[1].revents != 0 {
                return Ok(Ready::Device(fds[1].revents));
            }
        } else {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

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
