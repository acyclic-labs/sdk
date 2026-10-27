//! The host transport: one nonblocking FUSE device and a shared stop wake.

use super::notification::PacketSink;
use acyclic_native_runtime::ProcessWake;
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::sync::Arc;

pub(super) struct HostIo {
    pub(super) device: Arc<File>,
    wake: ProcessWake,
    _allocation: crate::NativeAllocation,
}

#[derive(Debug, PartialEq)]
pub(super) enum Received {
    Packet(usize),
    Wake,
    Closed,
}

#[derive(PartialEq)]
enum Ready {
    Device,
    Wake,
    Closed,
}

impl HostIo {
    pub(super) fn new(
        device: Arc<File>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, super::NativeMountError> {
        let allocation = work.claim(
            (crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
                .map_err(|_| libc::EOVERFLOW)?
                + ProcessWake::allocation_bytes()) as u64,
            2,
            crate::WorkCounters::UNCHARGED,
        )?;
        super::transport_io::nonblocking(device.as_ref())?;
        Ok(Self {
            device,
            wake: ProcessWake::new()?,
            _allocation: allocation,
        })
    }

    pub(super) fn stop(&self) {
        let _ = self.wake.stop();
    }

    fn wait(&self, events: i16) -> io::Result<Ready> {
        let mut fds = [
            libc::pollfd {
                fd: self.device.as_raw_fd(),
                events,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.as_fd().as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            if self.wake.is_stopped() {
                return Ok(Ready::Closed);
            }
            // SAFETY: both pollfd entries remain valid for this call; owned FDs
            // cannot be closed while this HostIo is borrowed by a worker.
            if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } >= 0 {
                if self.wake.is_stopped() {
                    return Ok(Ready::Closed);
                }
                if fds[1].revents & libc::POLLIN != 0 {
                    self.wake.consume()?;
                    return Ok(Ready::Wake);
                }
                if fds[0].revents != 0 {
                    // Read/write reports the actual device EOF or error.
                    return Ok(Ready::Device);
                }
                continue;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    pub(super) fn receive(&self, bytes: &mut [u8]) -> io::Result<Received> {
        loop {
            match self.wait(libc::POLLIN)? {
                Ready::Closed => return Ok(Received::Closed),
                Ready::Wake => return Ok(Received::Wake),
                Ready::Device => {}
            }
            match (&*self.device).read(bytes) {
                Ok(0) => return Ok(Received::Closed),
                Ok(length) => return Ok(Received::Packet(length)),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                Err(error) if error.raw_os_error() == Some(libc::ENODEV) => {
                    return Ok(Received::Closed);
                }
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
            if self.wake.is_stopped() {
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
                    if self.wait(libc::POLLOUT)? == Ready::Closed {
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

impl std::task::Wake for HostIo {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Cancellation interrupts the wait, but the host connection must keep
        // answering refusals until physical unmount. Stopping every reader
        // while the retirement owner retains /dev/fuse strands kernel calls.
        let _ = self.wake.notify();
    }
}
