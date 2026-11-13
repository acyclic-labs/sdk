//! The host transport: one nonblocking FUSE device and a shared stop wake.

use super::notification::PacketSink;
use super::transport_io::{self, Ready};
use acyclic_native_runtime::{PacketDevicePublication, PacketDeviceRetirement, ProcessWake};
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::AsFd;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};

#[cfg(test)]
thread_local! {
    static SOCKET_COMPLETION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static BEFORE_RECEIPT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
#[path = "host_io/tests.rs"]
mod tests;

/// An acquired abort control for one kernel FUSE connection.
#[derive(Debug)]
pub(super) struct Connection(File);

impl Connection {
    /// Acquires abort authority after INIT has been answered. DONT_SYNC alone
    /// does not make a pre-INIT lookup safe in container mount adapters.
    pub(super) fn of(
        mountpoint: &std::path::Path,
    ) -> Result<Option<Self>, super::NativeMountError> {
        let metadata = rustix::fs::statx(
            rustix::fs::CWD,
            mountpoint,
            rustix::fs::AtFlags::STATX_DONT_SYNC,
            rustix::fs::StatxFlags::empty(),
        )
        .map_err(|error| {
            super::NativeMountError::host_operation(
                "statx mountpoint after FUSE INIT",
                mountpoint,
                error.into(),
            )
        })?;
        // fusectl is an optional cleanup aid: containers can admit FUSE mounts
        // without exposing its control filesystem. Retain it when available.
        let device = (u64::from(metadata.stx_dev_major) << 20) | u64::from(metadata.stx_dev_minor);
        let path = std::path::PathBuf::from(format!("/sys/fs/fuse/connections/{device}/abort"));
        match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(control) => Ok(Some(Self(control))),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::PermissionDenied
                        | io::ErrorKind::ReadOnlyFilesystem
                ) =>
            {
                tracing::debug!(%error, path = %path.display(), "FUSE abort control unavailable; device closure owns disconnect");
                Ok(None)
            }
            Err(error) => Err(super::NativeMountError::host_operation(
                "open FUSE abort control",
                &path,
                error,
            )),
        }
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
    pub(super) connection: OnceLock<Connection>,
    // The session is the sole descriptor owner. Retained notification sinks
    // retain this control, never a live device after its server has stopped.
    device: RwLock<Option<File>>,
    custody: RwLock<Option<(PacketDeviceRetirement, crate::NativeAllocation)>>,
    pub(super) maximum_write: u32,
    publication: Mutex<()>,
    pub(super) framed: bool,
    wake: ProcessWake,
    // Readiness is advisory. One owner keeps this queue's claim through the
    // physical read, then releases it before any source callback or reply.
    receiver: Mutex<Option<transport_io::PacketReader>>,
    #[cfg(test)]
    receive_counts: [std::sync::atomic::AtomicU64; 3],
    _allocation: crate::NativeAllocation,
}

#[derive(Debug, PartialEq)]
pub(super) enum Received {
    Packet(usize),
    Wake,
    Closed,
}

impl HostIo {
    pub(super) fn host(
        device: super::mount::Device,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, super::NativeMountError> {
        let (device, allocation, maximum_write) = match device {
            super::mount::Device::Hosted {
                device,
                allocation,
                maximum_write,
            } => (device, allocation, maximum_write),
            #[cfg(test)]
            super::mount::Device::InProcess(device) => return Self::new(device, work),
        };
        let (stream, retirement) = device.into_parts();
        Self::from_stream(stream, Some((retirement, allocation)), maximum_write, work)
    }

    #[cfg(test)]
    pub(super) fn host_id(&self) -> Option<u32> {
        self.custody
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(retirement, _)| retirement.host_id())
    }

    #[cfg(test)]
    pub(super) fn new(
        device: File,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, super::NativeMountError> {
        Self::from_stream(device, None, super::packet_session::MAXIMUM_WRITE, work)
    }

    fn from_stream(
        device: File,
        custody: Option<(PacketDeviceRetirement, crate::NativeAllocation)>,
        maximum_write: u32,
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
        let wake = ProcessWake::new()?;
        let receiver = transport_io::PacketReader::new(&wake, device.as_fd())?;
        Ok(Self {
            connection: OnceLock::new(),
            device: RwLock::new(Some(device)),
            framed: custody.is_some(),
            custody: RwLock::new(custody),
            maximum_write,
            publication: Mutex::new(()),
            wake,
            receiver: Mutex::new(Some(receiver)),
            #[cfg(test)]
            receive_counts: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            _allocation: allocation,
        })
    }

    pub(super) fn stop(&self) {
        let _ = self.wake.stop();
        // Wake the physical borrower before closing the device and its queue.
        // Other receivers observe closure through the same receive lock.
        self.device
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // The host acknowledges only after the actual /dev/fuse has closed.
        // No kernel caller can remain pinned behind a stopped owner transport.
        // A half-close is an event the host already observes. It completes a
        // blocked publisher with physical retirement, without a readiness
        // probe or another wake descriptor on every successful publication.
        if let Some((custody, _)) = self
            .custody
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && let Err(error) = custody.request_stop()
        {
            tracing::error!(%error, "FUSE device host stop request failed");
        }
        let mut custody = self.custody.write().unwrap_or_else(PoisonError::into_inner);
        if let Some((owner, _)) = custody.as_mut() {
            match owner.retire() {
                Ok(()) => {
                    custody.take();
                }
                Err(error) => {
                    // Keep the original allocation and custody: a missing
                    // receipt cannot certify physical device retirement.
                    tracing::error!(%error, "FUSE device host retirement failed");
                }
            }
        }
    }

    pub(super) fn with_device<T>(
        &self,
        use_device: impl FnOnce(Option<std::os::fd::BorrowedFd<'_>>) -> T,
    ) -> T {
        let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
        use_device(device.as_ref().map(AsFd::as_fd))
    }

    pub(super) fn wait_detached(&self, mount: &mut super::mount::Mount) -> io::Result<bool> {
        if self.framed
            && self
                .custody
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some()
        {
            return Err(io::Error::other("FUSE host retirement is unacknowledged"));
        }
        self.with_device(|device| match device {
            Some(device) => mount.wait_device_detached(device, Some(&self.wake)),
            None => mount.observe_closed(),
        })
    }

    pub(super) fn start_fuse(
        &self,
        configuration: acyclic_native_runtime::fuse_protocol::Configuration,
        initialization_trace: Option<&File>,
    ) -> Result<(), super::NativeMountError> {
        let mut custody = self.custody.write().unwrap_or_else(PoisonError::into_inner);
        let (custody, _) = custody
            .as_mut()
            .ok_or(super::NativeMountError::ControlPending)?;
        custody
            .start_fuse(configuration, initialization_trace)
            .map_err(|error| {
                if let Some(cause) = error.get_ref().and_then(|cause| {
                    cause.downcast_ref::<acyclic_native_runtime::fuse_protocol::Error>()
                }) {
                    super::packet_dispatch::PacketFailure::from(*cause).into_native_error()
                } else {
                    error.into()
                }
            })
    }

    fn publication(&self) -> io::Result<PacketDevicePublication> {
        #[cfg(test)]
        if SOCKET_COMPLETION.get() {
            return Ok(PacketDevicePublication::Published);
        }
        let custody = self.custody.read().unwrap_or_else(PoisonError::into_inner);
        if let Some((custody, _)) = custody.as_ref() {
            #[cfg(test)]
            if let Some(probe) = BEFORE_RECEIPT.with_borrow_mut(Option::take) {
                probe();
            }
            custody.publication()
        } else if self.framed {
            Err(io::Error::from_raw_os_error(libc::ENODEV))
        } else {
            Ok(PacketDevicePublication::Published)
        }
    }

    fn wait(&self, events: i16) -> io::Result<Ready> {
        let ready = self.with_device(|device| transport_io::wait(&self.wake, device, events))?;
        if ready == Ready::Wake {
            self.wake.consume()?;
        }
        Ok(ready)
    }

    #[cfg(test)]
    pub(super) fn receive_counts(&self) -> [u64; 3] {
        self.receive_counts
            .each_ref()
            .map(|count| count.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(super) fn receive(&self, bytes: &mut [u8]) -> io::Result<Received> {
        let receiver = self.receiver.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(reader) = receiver.as_ref() else {
            return Ok(Received::Closed);
        };
        let mut filled = 0;
        let mut needed = if self.framed { 4 } else { bytes.len() };
        if needed > bytes.len() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        loop {
            match reader.wait(&self.wake)? {
                Ready::Closed => return Ok(Received::Closed),
                Ready::Wake => {
                    self.wake.consume()?;
                    if filled == 0 {
                        return Ok(Received::Wake);
                    }
                    continue;
                }
                Ready::Device(_) => {
                    #[cfg(test)]
                    self.receive_counts[0].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
            let Some(device) = device.as_ref() else {
                return Ok(Received::Closed);
            };
            match (&*device).read(&mut bytes[filled..needed]) {
                Ok(0) if filled == 0 => return Ok(Received::Closed),
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(length) => {
                    filled += length;
                    if self.framed && filled >= 4 {
                        needed = u32::from_ne_bytes(
                            bytes[..4]
                                .try_into()
                                .map_err(|_| io::ErrorKind::InvalidData)?,
                        ) as usize;
                        if needed < 4 || needed > bytes.len() {
                            return Err(io::ErrorKind::InvalidData.into());
                        }
                    }
                    if self.framed && filled != needed {
                        continue;
                    }
                    #[cfg(test)]
                    self.receive_counts[1].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(Received::Packet(filled));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) =>
                {
                    #[cfg(test)]
                    if error.kind() == io::ErrorKind::WouldBlock {
                        self.receive_counts[2].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Err(error) if error.raw_os_error() == Some(libc::ENODEV) => {
                    return Ok(Received::Closed);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl HostIo {
    /// Only transport loss is an error here. A physical rejection completes
    /// the original publication obligation without closing other requests.
    pub(super) fn publish_packet(
        &self,
        packet: &[IoSlice<'_>],
    ) -> io::Result<PacketDevicePublication> {
        // Stream writes may split a frame; retain its one publication claim
        // until every slice is sent, without interleaving another response.
        let _publication = self
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let total = packet.iter().try_fold(0_usize, |length, part| {
            length
                .checked_add(part.len())
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EOVERFLOW))
        })?;
        let mut sent = 0;
        loop {
            if self.wake.is_stopped() {
                return Err(io::Error::from_raw_os_error(libc::ENODEV));
            }
            let result = {
                let device = self.device.read().unwrap_or_else(PoisonError::into_inner);
                let device = device
                    .as_ref()
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::ENODEV))?;
                if sent == 0 {
                    (&*device).write_vectored(packet)
                } else {
                    let mut offset = sent;
                    let tail = packet
                        .iter()
                        .find_map(|part| {
                            if offset < part.len() {
                                Some(&part[offset..])
                            } else {
                                offset -= part.len();
                                None
                            }
                        })
                        .ok_or(io::ErrorKind::InvalidData)?;
                    (&*device).write(tail)
                }
            };
            match result {
                Ok(length) if sent + length == total => return self.publication(),
                Ok(length) if self.framed && length > 0 => {
                    sent += length;
                }
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

impl PacketSink for HostIo {
    fn publish(&self, packet: &[IoSlice<'_>]) -> io::Result<()> {
        match self.publish_packet(packet)? {
            PacketDevicePublication::Published => Ok(()),
            PacketDevicePublication::Rejected(errno) => Err(io::Error::from_raw_os_error(errno)),
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

/// Fund the external packet owner before handing it a physical device.
/// One additional buffer pair participates in the same negotiation as the
/// callback workers; it cannot silently exceed the caller's original budget.
pub(super) struct FundedDevice {
    pub(super) capacity: u32,
    pub(super) maximum_write: u32,
    pub(super) allocation: crate::NativeAllocation,
}

pub(super) fn fund_device(
    work: &crate::NativeMaterializationWork,
) -> Result<FundedDevice, super::NativeMountError> {
    let workers =
        std::num::NonZeroU32::new(crate::native_mount::CALLBACK_THREADS + 1).ok_or(libc::EINVAL)?;
    // SAFETY: sysconf takes no pointer arguments.
    let page =
        u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).map_err(|_| libc::EOVERFLOW)?;
    let page = std::num::NonZeroU32::new(page).ok_or(libc::EIO)?;
    let maximum_write = super::packet_buffers::maximum_write(work, workers, page)?;
    let capacity = maximum_write.checked_add(4096).ok_or(libc::EOVERFLOW)?;
    let bytes = acyclic_native_runtime::PacketDevice::storage_bytes(capacity)?;
    let allocation = work.claim(bytes, 3, crate::WorkCounters::UNCHARGED)?;
    Ok(FundedDevice {
        capacity,
        maximum_write,
        allocation,
    })
}
