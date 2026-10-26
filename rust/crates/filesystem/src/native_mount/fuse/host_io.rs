//! The host transport: one nonblocking FUSE device and a shared stop wake.

use super::notification::PacketSink;
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct HostIo {
    pub(super) device: Arc<File>,
    wake: OwnedFd,
    stopped: AtomicBool,
}

impl HostIo {
    pub(super) fn new(device: Arc<File>) -> io::Result<Self> {
        super::transport_io::nonblocking(device.as_ref())?;
        // SAFETY: eventfd returns a newly owned descriptor or -1.
        let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            device,
            // SAFETY: the successful eventfd has one owner and has not escaped.
            wake: unsafe { OwnedFd::from_raw_fd(wake) },
            stopped: AtomicBool::new(false),
        })
    }

    pub(super) fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            let value = 1_u64.to_ne_bytes();
            loop {
                // SAFETY: wake is open and value is readable for the full length.
                let result = unsafe {
                    libc::write(self.wake.as_raw_fd(), value.as_ptr().cast(), value.len())
                };
                if result >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                    break;
                }
            }
        }
        // Never drain this eventfd: persistent readiness wakes every worker.
    }

    fn wait(&self, events: i16) -> io::Result<bool> {
        let mut fds = [
            libc::pollfd {
                fd: self.device.as_raw_fd(),
                events,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Ok(false);
            }
            // SAFETY: both pollfd entries remain valid for this call; owned FDs
            // cannot be closed while this HostIo is borrowed by a worker.
            if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } >= 0 {
                return Ok(!self.stopped.load(Ordering::Acquire));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    pub(super) fn receive(&self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        loop {
            if !self.wait(libc::POLLIN)? {
                return Ok(None);
            }
            match (&*self.device).read(bytes) {
                Ok(0) => return Ok(None),
                Ok(length) => return Ok(Some(length)),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                Err(error) if error.raw_os_error() == Some(libc::ENODEV) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
    }
}

impl PacketSink for HostIo {
    fn publish(&self, packet: &[IoSlice<'_>]) -> io::Result<()> {
        let total = packet.iter().try_fold(0_usize, |length, part| {
            length
                .checked_add(part.len())
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EOVERFLOW))
        })?;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Err(io::Error::from_raw_os_error(libc::ENODEV));
            }
            match (&*self.device).write_vectored(packet) {
                Ok(length) if length == total => return Ok(()),
                // A short publication has uncertain completion. Never retry it.
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "FUSE packet publication was short; recovery: retire this session",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if !self.wait(libc::POLLOUT)? {
                        return Err(io::Error::from_raw_os_error(libc::ENODEV));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl AsFd for HostIo {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.device.as_fd()
    }
}
