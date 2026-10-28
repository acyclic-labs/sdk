//! Physical host session over the common packet entry.

use super::host_io::{HostIo, Received};
use super::notification::{CacheNotifier, PacketSink};
use super::packet::PacketRequest;
use super::packet_buffers::{PacketBufferPool, maximum_write};
use super::packet_response::PacketResponse;
use super::packet_router::PacketRouter;
use super::packet_session::PacketSession;
use super::*;
use crate::NativeMaterializationWork;

pub(super) struct HostSession {
    protocol: PacketSession,
    // Its waker interrupts physical I/O. Keep the actual registration alive
    // through every worker's exit, then recover the typed cause from work.
    _cancellation: crate::Cancelled<'static>,
    io: Arc<HostIo>,
    work: NativeMaterializationWork,
    owner_uid: u32,
    buffers: PacketBufferPool,
    #[cfg(test)]
    capture: Option<Arc<super::tests::packets::Capture>>,
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
        let buffers = PacketBufferPool::new(workers, maximum_write as usize + 4096, work)?;
        let io = Arc::new(HostIo::new(device, work)?);
        let cancellation = super::packet_cancellation::register(work, &io)?;
        Ok(Self {
            protocol: PacketSession::new(router, maximum_write, page_size)?,
            _cancellation: cancellation,
            io,
            work: work.clone(),
            owner_uid: nix::unistd::geteuid().as_raw(),
            buffers,
            #[cfg(test)]
            capture: super::tests::packets::Capture::current(),
        })
    }

    pub(super) fn notifier(&self) -> CacheNotifier {
        CacheNotifier::new(self.io.clone())
    }

    pub(super) fn initialize(&self) -> Result<(), NativeMountError> {
        let _stop_on_failure = HandshakeStop(self);
        let mut buffers = self.buffers.take(0)?;
        while !self.protocol.ready() {
            if !self.serve(&mut buffers.input, &mut buffers.output)? {
                self.work.admit(crate::WorkCounters::UNCHARGED)?;
                return Err(libc::ENODEV.into());
            }
        }
        self.buffers.put(0, buffers)?;
        Ok(())
    }

    pub(super) fn run(self) -> Result<(), NativeMountError> {
        super::packet_workers::run(
            &self,
            std::num::NonZeroU32::new(crate::native_mount::CALLBACK_THREADS).ok_or(libc::EINVAL)?,
        )
    }

    fn serve(&self, input: &mut [u8], output: &mut [u8]) -> Result<bool, NativeMountError> {
        let length = match self.io.receive(input)? {
            Received::Closed => return Ok(false),
            Received::Wake => return Ok(true),
            Received::Packet(length) => length,
        };
        let packet = input.get(..length).ok_or(libc::EPROTO)?;
        let request = PacketRequest::parse(packet)?;
        #[cfg(test)]
        let sequence = self.capture.as_ref().map(|capture| capture.begin());
        let publish = |reply: PacketResponse<'_>| {
            if let Some(slices) = reply.slices() {
                self.io.publish(&slices)?;
            }
            #[cfg(test)]
            if let Some((capture, sequence)) = self.capture.as_ref().zip(sequence) {
                capture.completed(sequence, packet, reply.contiguous());
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
        match self.protocol.publish(&request, output, publish) {
            // The negative reply physically completed. Its actual typed cause
            // remains in the original core. Continue serving refusals while
            // the existing retirement owner unmounts this kernel connection;
            // otherwise retained device ownership can strand pending calls.
            Err(super::packet_dispatch::PacketFailure::Source { .. }) => Ok(true),
            other => other.map_err(super::packet_dispatch::PacketFailure::into_native_error),
        }
    }
}

impl super::packet_workers::PacketWorkers for HostSession {
    fn stop(&self) {
        self.io.stop();
        self.protocol.close();
    }

    fn run_worker(&self, index: u32) -> Result<(), NativeMountError> {
        let mut buffers = self.buffers.take(index)?;
        while self.serve(&mut buffers.input, &mut buffers.output)? {}
        Ok(())
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
