//! Physical descriptor setup shared by the host and socket transports.

use acyclic_native_runtime::ProcessWake;
use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
};

#[derive(PartialEq)]
pub(super) enum Ready {
    Device(i16),
    Wake,
    Closed,
}

/// One packet worker's lifetime interest in the shared input queue. Device
/// readiness selects exclusive recipients; stop must still wake every worker.
/// The registration borrows no device ownership, so retirement can close the
/// sole physical descriptor after waking and joining its current borrowers.
pub(super) struct PacketReader(OwnedFd);

impl PacketReader {
    pub(super) fn new(wake: &ProcessWake, device: BorrowedFd<'_>) -> io::Result<Self> {
        // SAFETY: flags only; a successful call returns a fresh owned fd.
        let queue = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this is the fresh descriptor returned above.
        let reader = Self(unsafe { OwnedFd::from_raw_fd(queue) });
        reader.register(wake.as_fd(), libc::EPOLLIN, 0)?;
        reader.register(device, libc::EPOLLIN | libc::EPOLLEXCLUSIVE, 1)?;
        Ok(reader)
    }

    fn register(&self, descriptor: BorrowedFd<'_>, events: i32, token: u64) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: events.cast_unsigned(),
            u64: token,
        };
        // SAFETY: both descriptors remain live during registration and event
        // is initialized. Closing the target automatically removes interest.
        if unsafe {
            libc::epoll_ctl(
                self.0.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                descriptor.as_raw_fd(),
                &raw mut event,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn wait(&self, wake: &ProcessWake) -> io::Result<Ready> {
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 2];
        loop {
            if wake.is_stopped() {
                return Ok(Ready::Closed);
            }
            // SAFETY: the owned queue writes at most two initialized events.
            let count = unsafe { libc::epoll_wait(self.0.as_raw_fd(), events.as_mut_ptr(), 2, -1) };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if wake.is_stopped() {
                return Ok(Ready::Closed);
            }
            let ready = &events[..count as usize];
            if let Some(event) = ready.iter().find(|event| event.u64 == 0) {
                if event.events & (libc::EPOLLERR | libc::EPOLLHUP).cast_unsigned() != 0 {
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                return Ok(Ready::Wake);
            }
            return Ok(Ready::Device(libc::POLLIN));
        }
    }
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
