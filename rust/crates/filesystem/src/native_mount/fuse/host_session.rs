//! Physical host session over the common packet entry.

use super::host_io::HostIo;
use super::notification::{CacheNotifier, PacketSink};
use super::packet::PacketRequest;
use super::packet_buffers::{PacketBuffers, maximum_write};
use super::packet_router::PacketRouter;
use super::packet_session::PacketSession;
use super::*;
use crate::NativeMaterializationWork;
use std::io::IoSlice;

pub(super) struct HostSession {
    protocol: PacketSession,
    // Its waker only stops physical I/O. Keep the actual registration alive
    // through every worker's exit, then recover the typed cause from work.
    _cancellation: crate::Cancelled<'static>,
    io: Arc<HostIo>,
    work: NativeMaterializationWork,
    owner_uid: u32,
    packet_capacity: usize,
    #[cfg(test)]
    capture: Option<Arc<super::tests::packets::Capture>>,
}

struct WorkerStop<'a>(&'a HostSession);

impl Drop for WorkerStop<'_> {
    fn drop(&mut self) {
        self.0.io.stop();
        self.0.protocol.close();
    }
}

impl HostSession {
    pub(super) fn new(
        router: PacketRouter,
        device: Arc<std::fs::File>,
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let workers =
            std::num::NonZeroU32::new(crate::native_mount::CALLBACK_THREADS).ok_or(libc::EINVAL)?;
        // SAFETY: sysconf has no pointer arguments or ownership effects.
        let page_size = u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
            .map_err(|_| libc::EOVERFLOW)?;
        let page_size = std::num::NonZeroU32::new(page_size).ok_or(libc::EIO)?;
        let maximum_write = maximum_write(work, workers, page_size)?;
        let io = Arc::new(HostIo::new(device, work)?);
        let cancellation = super::packet_cancellation::register(work, &io)?;
        Ok(Self {
            protocol: PacketSession::new(router, maximum_write, page_size)?,
            _cancellation: cancellation,
            io,
            work: work.clone(),
            owner_uid: nix::unistd::geteuid().as_raw(),
            packet_capacity: maximum_write as usize + 4096,
            #[cfg(test)]
            capture: super::tests::packets::Capture::current(),
        })
    }

    pub(super) fn notifier(&self) -> CacheNotifier {
        CacheNotifier::new(self.io.clone())
    }

    pub(super) fn initialize(&self) -> Result<(), NativeMountError> {
        let _stop_on_failure = HandshakeStop(self);
        let mut buffers = PacketBuffers::new(self.packet_capacity, &self.work)?;
        while !self.protocol.ready() {
            if !self.serve(&mut buffers.input, &mut buffers.output)? {
                self.work.admit(crate::WorkCounters::UNCHARGED)?;
                return Err(libc::ENODEV.into());
            }
        }
        Ok(())
    }

    pub(super) fn run(self) -> Result<(), NativeMountError> {
        let _stop = WorkerStop(&self);
        std::thread::scope(|scope| {
            let mut workers = fuse_vec(crate::native_mount::CALLBACK_THREADS as usize, &self.work)?;
            let mut result = Ok(());
            for _ in 0..crate::native_mount::CALLBACK_THREADS {
                match std::thread::Builder::new().spawn_scoped(scope, || {
                    let _stop = WorkerStop(&self);
                    let mut buffers = PacketBuffers::new(self.packet_capacity, &self.work)?;
                    while self.serve(&mut buffers.input, &mut buffers.output)? {}
                    Ok(())
                }) {
                    Ok(worker) => workers.push(worker),
                    Err(error) => {
                        self.io.stop();
                        result = Err(error.into());
                        break;
                    }
                }
            }
            for worker in workers.drain(..) {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if result.is_ok() => result = Err(error),
                    Ok(Err(_)) => {}
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            result.and_then(|()| {
                self.work
                    .admit(crate::WorkCounters::UNCHARGED)
                    .map_err(Into::into)
            })
        })
    }

    fn serve(&self, input: &mut [u8], output: &mut [u8]) -> Result<bool, NativeMountError> {
        let Some(length) = self.io.receive(input)? else {
            return Ok(false);
        };
        let packet = input.get(..length).ok_or(libc::EPROTO)?;
        let request = PacketRequest::parse(packet)?;
        #[cfg(test)]
        let sequence = self.capture.as_ref().map(|capture| capture.begin());
        let publish = |bytes: Option<&[u8]>| {
            if let Some(bytes) = bytes {
                self.io.publish(&[IoSlice::new(bytes)])?;
            }
            #[cfg(test)]
            if let Some((capture, sequence)) = self.capture.as_ref().zip(sequence) {
                capture.completed(sequence, packet, bytes);
            }
            Ok(())
        };
        if !owner_allowed(self.owner_uid, &request) {
            return self
                .protocol
                .reject(&request, output, libc::EACCES, publish)
                .map_err(super::packet_dispatch::PacketFailure::into_native_error)
                .map(|()| true);
        }
        self.protocol
            .publish(&request, output, publish)
            .map_err(super::packet_dispatch::PacketFailure::into_native_error)
    }
}

impl Drop for HostSession {
    fn drop(&mut self) {
        self.io.stop();
        self.protocol.close();
    }
}

struct HandshakeStop<'a>(&'a HostSession);

impl Drop for HandshakeStop<'_> {
    fn drop(&mut self) {
        if !self.0.protocol.ready() {
            self.0.io.stop();
            self.0.protocol.close();
        }
    }
}

fn owner_allowed(owner_uid: u32, request: &PacketRequest<'_>) -> bool {
    request.owner.0 == owner_uid
        || matches!(
            request.opcode,
            2 | 15 | 16 | 18 | 20 | 26 | 28 | 29 | 30 | 38 | 42 | 44
        )
}
