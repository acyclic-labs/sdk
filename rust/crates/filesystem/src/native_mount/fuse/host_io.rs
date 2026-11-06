//! The host transport: one nonblocking FUSE device and a shared stop wake.

use super::notification::PacketSink;
use super::transport_io::{self, Ready};
use acyclic_native_runtime::ProcessWake;
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::AsFd;
use std::sync::{Arc, PoisonError, RwLock};

pub(super) struct HostIo {
    // The session is the sole descriptor owner. Retained notification sinks
    // retain this control, never a live device after its server has stopped.
    device: RwLock<Option<File>>,
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
        device: File,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, super::NativeMountError> {
        let allocation = work.claim(
            (crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
                .map_err(|_| libc::EOVERFLOW)?
                + ProcessWake::allocation_bytes()) as u64,
            2,
            crate::WorkCounters::UNCHARGED,
        )?;
        super::transport_io::nonblocking(&device)?;
        Ok(Self {
            device: RwLock::new(Some(device)),
            wake: ProcessWake::new()?,
            _allocation: allocation,
        })
    }

    pub(super) fn stop(&self) {
        let _ = self.wake.stop();
        // Wake every physical borrower before waiting for its read guard.
        // Closing the last device aborts unanswered kernel requests.
        self.device
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    pub(super) fn with_device<T>(
        &self,
        use_device: impl FnOnce(Option<std::os::fd::BorrowedFd<'_>>) -> T,
    ) -> T {
        let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
        use_device(device.as_ref().map(AsFd::as_fd))
    }

    pub(super) fn wait_detached(&self, mount: &mut super::mount::Mount) -> io::Result<bool> {
        self.with_device(|device| match device {
            Some(device) => mount.wait_device_detached(device, Some(&self.wake)),
            None => mount.observe_closed(),
        })
    }

    fn wait(&self, events: i16) -> io::Result<Ready> {
        let ready = self.with_device(|device| transport_io::wait(&self.wake, device, events))?;
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
            let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
            let Some(device) = device.as_ref() else {
                return Ok(Received::Closed);
            };
            match (&*device).read(bytes) {
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
            let result = {
                let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
                let device = device
                    .as_ref()
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::ENODEV))?;
                (&*device).write_vectored(packet)
            };
            match result {
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
