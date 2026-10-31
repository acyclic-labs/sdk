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
    /// Capture a real native refusal using a live pipe in place of a queue.
    #[cfg(test)]
    pub(super) fn refused_wait() -> io::Result<Self> {
        let (reader, writer) = std::io::pipe()?;
        Ok(Self {
            epoll: reader.into(),
            wake: writer.into(),
        })
    }

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
        let count = usize::try_from(check(count)?).map_err(io::Error::other)?;
        for event in events.iter().take(count) {
            let token = event.u64;
            if token == WAKE {
                let mut counter = 0_u64;
                // SAFETY: an eight-byte read into an owned counter drains the eventfd.
                let drained = unsafe {
                    libc::read(
                        self.wake.as_raw_fd(),
                        (&raw mut counter).cast(),
                        size_of::<u64>(),
                    )
                };
                if drained < 0 {
                    return Err(io::Error::last_os_error());
                }
                if drained != 8 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            } else {
                ready.push(token);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};

    #[test]
    fn a_failed_native_wake_drain_returns_the_original_cause() -> io::Result<()> {
        let mut poller = Poller::new()?;
        let (mut reader, mut writer) = std::io::pipe()?;
        // Publish a real WAKE-key readiness packet. A write-only descriptor
        // then supplies the actual read refusal at the original drain call.
        poller.interest(reader.as_raw_fd(), libc::EPOLLIN, WAKE)?;
        assert_eq!(writer.write(b"x")?, 1);
        poller.wake = writer.into();
        let mut ready = Vec::new();
        let error = poller.wait(None, &mut ready).unwrap_err();
        // The old discarded read reported Ok and immediately re-entered the
        // still-readable kernel interest. This oracle rejects that one turn.
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
        assert!(ready.is_empty());
        let mut byte = [0];
        assert_eq!(reader.read(&mut byte)?, 1);
        assert_eq!(byte, [b'x']);
        Ok(())
    }
}
