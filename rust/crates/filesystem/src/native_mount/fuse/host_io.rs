//! The host transport: one nonblocking FUSE device and a shared stop wake.

use super::notification::PacketSink;
use super::transport_io::{self, Ready};
use acyclic_native_runtime::ProcessWake;
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::AsFd;
use std::sync::Arc;

/// An acquired abort control for one kernel FUSE connection.
#[derive(Debug)]
pub(super) struct Connection(File);

impl Connection {
    /// Reads the mount's device from cached attributes only: no reader runs
    /// yet, so a synchronous getattr would wait on this very connection.
    pub(super) fn of(mountpoint: &std::path::Path) -> io::Result<Self> {
        let metadata = rustix::fs::statx(
            rustix::fs::CWD,
            mountpoint,
            rustix::fs::AtFlags::STATX_DONT_SYNC,
            rustix::fs::StatxFlags::empty(),
        )?;
        // Acquire teardown authority before accepting the mount. Retaining the
        // control also survives fusectl becoming inaccessible during service.
        let device = (u64::from(metadata.stx_dev_major) << 20) | u64::from(metadata.stx_dev_minor);
        Ok(Self(std::fs::OpenOptions::new().write(true).open(
            format!("/sys/fs/fuse/connections/{device}/abort"),
        )?))
    }

    /// Fails every pending and later request on the connection. Calls into a
    /// connection that nothing reads would otherwise wait forever, because
    /// any retained device descriptor keeps the kernel from aborting it.
    pub(super) fn abort(&self) -> io::Result<()> {
        // fusectl acknowledges the write even if the kernel already destroyed
        // the connection. No path lookup or permission acquisition remains.
        (&self.0).write_all(b"1")
    }
}

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
        let ready = transport_io::wait(&self.wake, Some(self.device.as_fd()), events)?;
        if ready == Ready::Wake {
            self.wake.consume()?;
        }
        Ok(ready)
    }

    pub(super) fn receive(&self, bytes: &mut [u8]) -> io::Result<Received> {
        loop {
            match self.wait(libc::POLLIN)? {
                Ready::Closed => return Ok(Received::Closed),
                Ready::Wake => return Ok(Received::Wake),
                Ready::Device(_) => {}
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
