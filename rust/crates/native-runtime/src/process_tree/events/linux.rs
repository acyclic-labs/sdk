//! Linux readiness: one epoll set, one-shot pipe and pidfd interest, and an
//! eventfd that wakes the hub when an earlier wake time is armed.

use super::Source;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::time::Duration;

const WAKE: u64 = u64::MAX;

pub(super) struct Poller {
    epoll: OwnedFd,
    wake: OwnedFd,
}

/// Linux interest needs no release: a one-shot entry disarms itself when it
/// fires, a stale one is re-armed under the next token, and closing the fd
/// removes it from the set.
pub(super) struct Registration;

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: epoll_create1 takes only flags and returns a new fd.
        let epoll =
            unsafe { OwnedFd::from_raw_fd(check(libc::epoll_create1(libc::EPOLL_CLOEXEC))?) };
        // SAFETY: eventfd takes an initial count and flags and returns a new fd.
        let wake = unsafe {
            OwnedFd::from_raw_fd(check(libc::eventfd(
                0,
                libc::EFD_CLOEXEC | libc::EFD_NONBLOCK,
            ))?)
        };
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN.cast_unsigned(),
            u64: WAKE,
        };
        // SAFETY: both fds are owned and live; the event is initialized.
        check(unsafe {
            libc::epoll_ctl(
                epoll.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                wake.as_raw_fd(),
                &raw mut event,
            )
        })?;
        Ok(Self { epoll, wake })
    }

    fn interest(&self, fd: RawFd, events: libc::c_int, token: u64) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: (events | libc::EPOLLONESHOT).cast_unsigned(),
            u64: token,
        };
        // SAFETY: the caller's borrowed fd is live for this call, and the
        // epoll fd is owned by this poller.
        let modified = check(unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                libc::EPOLL_CTL_MOD,
                fd,
                &raw mut event,
            )
        });
        match modified {
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                // SAFETY: as above, adding the same live fd.
                check(unsafe {
                    libc::epoll_ctl(
                        self.epoll.as_raw_fd(),
                        libc::EPOLL_CTL_ADD,
                        fd,
                        &raw mut event,
                    )
                })
                .map(drop)
            }
            other => other.map(drop),
        }
    }

    pub(super) fn register(
        &self,
        source: Source<'_>,
        token: u64,
    ) -> io::Result<Option<Registration>> {
        let (fd, events) = match source {
            Source::Readable(fd) | Source::Exit(fd) => {
                (fd.as_raw_fd(), libc::EPOLLIN | libc::EPOLLRDHUP)
            }
            Source::Writable(fd) => (fd.as_raw_fd(), libc::EPOLLOUT),
        };
        self.interest(fd, events, token)?;
        Ok(Some(Registration))
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        let one = 1_u64;
        // SAFETY: an eight-byte write of an initialized counter to the owned eventfd.
        let written = unsafe {
            libc::write(
                self.wake.as_raw_fd(),
                (&raw const one).cast(),
                size_of::<u64>(),
            )
        };
        if written == 8 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        // A saturated counter already holds a pending wake.
        if error.kind() == io::ErrorKind::WouldBlock {
            Ok(())
        } else {
            Err(error)
        }
    }

    pub(super) fn wait(&self, timeout: Option<Duration>, ready: &mut Vec<u64>) -> io::Result<()> {
        let milliseconds = timeout.map_or(-1, |timeout| {
            libc::c_int::try_from(timeout.as_nanos().div_ceil(1_000_000))
                .unwrap_or(libc::c_int::MAX)
        });
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 256];
        // SAFETY: the owned epoll fd fills at most the initialized array.
        let count = unsafe {
            libc::epoll_wait(
                self.epoll.as_raw_fd(),
                events.as_mut_ptr(),
                libc::c_int::try_from(events.len()).unwrap_or(libc::c_int::MAX),
                milliseconds,
            )
        };
        let count = match check(count) {
            Ok(count) => usize::try_from(count).unwrap_or(0),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => 0,
            Err(error) => return Err(error),
        };
        for event in events.iter().take(count) {
            let token = event.u64;
            if token == WAKE {
                let mut counter = 0_u64;
                // SAFETY: an eight-byte read into an owned counter drains the eventfd.
                unsafe {
                    libc::read(
                        self.wake.as_raw_fd(),
                        (&raw mut counter).cast(),
                        size_of::<u64>(),
                    );
                }
            } else {
                ready.push(token);
            }
        }
        Ok(())
    }
}
