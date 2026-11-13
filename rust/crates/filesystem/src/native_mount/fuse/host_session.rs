//! SDK source endpoint for the external native FUSE server.

use super::host_io::{Connection, HostIo, Received};
use super::notification::CacheNotifier;
#[cfg(test)]
use super::notification::PacketSink;
use super::packet::PacketRequest;
use super::packet_buffers::{PacketBufferPool, PacketBuffers, maximum_write};
use super::packet_response::PacketResponse;
use super::packet_router::PacketRouter;
use super::packet_session::PacketSession;
use super::*;
use crate::NativeMaterializationWork;

pub(super) struct HostSession {
    protocol: PacketSession,
    #[cfg(test)]
    worker_lifetime: Arc<()>,
    // Its waker interrupts physical I/O. Keep the actual registration alive
    // through every worker's exit, then recover the typed cause from work.
    _cancellation: crate::Cancelled<'static>,
    io: Arc<HostIo>,
    work: NativeMaterializationWork,
    #[cfg(test)]
    owner_uid: u32,
    buffers: PacketBufferPool<PacketBuffers>,
    #[cfg(test)]
    capture: Option<Arc<super::tests::packets::Capture>>,
}

impl HostSession {
    /// The device's INIT reply is the readiness event. No access to the
    /// mounted namespace is allowed before this event has completed.
    pub(super) fn start_mount(
        self,
        path: &Path,
        builder: std::thread::Builder,
        worker: &mut Option<std::thread::JoinHandle<Result<(), NativeMountError>>>,
        operation: &std::cell::Cell<&'static str>,
    ) -> Result<(), NativeMountError> {
        operation.set("join FUSE initialization");
        self.after_init(path, || Ok(()))?;
        let io = self.transport();
        // Publish worker custody before any post-INIT namespace access.
        // Even an adapter that performs GETATTR for statx can now be served.
        operation.set("start FUSE callback workers");
        *worker = Some(
            self.start(builder)
                .map_err(|error| error.during("start FUSE callback workers", path))?,
        );
        operation.set("acquire initialized FUSE abort control");
        if let Some(connection) = Connection::of(path)? {
            io.connection
                .set(connection)
                .map_err(|_| NativeMountError::ControlPending)?;
        }
        Ok(())
    }

    pub(super) fn after_init<T>(
        &self,
        path: &Path,
        acquire: impl FnOnce() -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        self.initialize()
            .map_err(|error| error.during("join FUSE initialization", path))?;
        acquire()
    }

    pub(super) fn new(
        router: PacketRouter,
        io: Arc<HostIo>,
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let workers =
            std::num::NonZeroU32::new(crate::native_mount::CALLBACK_THREADS).ok_or(libc::EINVAL)?;
        // SAFETY: sysconf has no pointer arguments or ownership effects.
        let page_size = u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
            .map_err(|_| libc::EOVERFLOW)?;
        let page_size = std::num::NonZeroU32::new(page_size).ok_or(libc::EIO)?;
        let maximum_write = maximum_write(work, workers, page_size)?.min(io.maximum_write);
        let buffers = PacketBufferPool::new(workers, work, || {
            PacketBuffers::new(maximum_write as usize + 4096, work)
        })?;
        let cancellation = super::packet_cancellation::register(work, &io)?;
        let protocol = source_protocol(router, &io, maximum_write, page_size, &buffers)?;
        Ok(Self {
            protocol,
            #[cfg(test)]
            worker_lifetime: Arc::new(()),
            _cancellation: cancellation,
            io,
            work: work.clone(),
            #[cfg(test)]
            owner_uid: nix::unistd::geteuid().as_raw(),
            buffers,
            #[cfg(test)]
            capture: super::tests::packets::Capture::current(),
        })
    }

    pub(super) fn notifier(&self) -> CacheNotifier {
        CacheNotifier::new(self.io.clone())
    }

    /// The physical owner stops this transport after closing view admission
    /// and attempting namespace detach, then joins every packet worker.
    pub(super) fn transport(&self) -> Arc<HostIo> {
        Arc::clone(&self.io)
    }

    pub(super) fn initialize(&self) -> Result<(), NativeMountError> {
        let _stop_on_failure = HandshakeStop(self);
        let mut buffers = self.buffers.take(0)?;
        while !self.protocol.ready() {
            if !self.serve(&mut buffers)? {
                self.work.admit(crate::WorkCounters::UNCHARGED)?;
                return Err(libc::ENODEV.into());
            }
        }
        self.buffers.put(0, buffers)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn worker_lifetime(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.worker_lifetime)
    }

    /// Startup owns the worker until all callback threads exist or have joined.
    pub(super) fn start(
        self,
        builder: std::thread::Builder,
    ) -> Result<std::thread::JoinHandle<Result<(), NativeMountError>>, NativeMountError> {
        let (started, ready) = std::sync::mpsc::sync_channel(1);
        let worker = builder.spawn(move || {
            super::packet_workers::run(
                &self,
                std::num::NonZeroU32::new(crate::native_mount::CALLBACK_THREADS)
                    .ok_or(libc::EINVAL)?,
                |_| {
                    started
                        .send(())
                        .map_err(|_| NativeMountError::Lifecycle("FUSE startup observer retired"))
                },
            )
        })?;
        if ready.recv().is_ok() {
            return Ok(worker);
        }
        // A partial spawn drops the producer. Recover the original failure
        // only after every physical worker has relinquished its custody.
        match worker.join() {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => Err(NativeMountError::Lifecycle(
                "FUSE startup ended without completion",
            )),
            Err(panic) => Err(NativeMountError::DriverPanicked {
                operation: "start FUSE callback workers",
                panic: std::sync::Mutex::new(panic),
            }),
        }
    }

    pub(super) fn serve(&self, buffers: &mut PacketBuffers) -> Result<bool, NativeMountError> {
        let capacity = self.protocol.packet_capacity();
        // /dev/fuse needs the complete negotiated receive envelope.
        let input = PacketBuffers::input_for(&mut buffers.input, capacity, capacity)?;
        let length = match self.io.receive(input)? {
            Received::Closed => return Ok(false),
            Received::Wake => return Ok(true),
            Received::Packet(length) => length,
        };
        let packet = input.get(..length).ok_or(libc::EPROTO)?;
        let request = PacketRequest::parse(packet)?;
        let pending = PendingReply::new(&self.io, &request);
        #[cfg(test)]
        let sequence = self.capture.as_ref().map(|capture| capture.begin());
        let publish = |reply: PacketResponse<'_>| {
            let outcome = match reply.slices() {
                Some(slices) => pending.publish(&slices)?,
                None => acyclic_native_runtime::PacketDevicePublication::Published,
            };
            #[cfg(test)]
            if outcome == acyclic_native_runtime::PacketDevicePublication::Published
                && let Some((capture, sequence)) = self.capture.as_ref().zip(sequence)
            {
                capture.completed(sequence, packet, reply.contiguous());
            }
            Ok(outcome)
        };
        #[cfg(test)]
        if !self.io.framed && !owner_allowed(self.owner_uid, &request) {
            let output = buffers
                .output
                .get_mut(..super::packet::OUTPUT_HEADER)
                .ok_or(libc::ENOBUFS)?;
            return self
                .protocol
                .reject(&request, output, libc::EACCES, publish)
                .map_err(super::packet_dispatch::PacketFailure::into_native_error)
                .map(|()| true);
        }
        let output = PacketBuffers::output_for(&mut buffers.output, &request, capacity)?;
        match self.protocol.publish_or_defer_with_outcome(
            &request,
            output,
            publish,
            |reply, response| {
                super::deferred_reply::DeferredReply::enqueue(
                    reply,
                    packet,
                    response,
                    pending.transfer(),
                    #[cfg(test)]
                    self.capture.as_ref().cloned().zip(sequence),
                )
            },
        ) {
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
        while self.serve(&mut buffers)? {}
        Ok(())
    }
}

impl Drop for HostSession {
    fn drop(&mut self) {
        self.io.stop();
        self.protocol.close();
        // Every reader has exited: `run` joins them before the session drops.
        if let Some(connection) = self.io.as_ref().connection.get()
            && let Err(error) = connection.abort()
        {
            tracing::error!(%error, "aborting an unserved FUSE connection failed");
        }
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

/// Own the kernel's reply obligation before buffer preparation or dispatch.
/// PacketReply only exists after dispatch; protocol closure alone cannot finish
/// a received request while another worker or notifier retains the device.
pub(super) struct PendingReply<I: AsRef<HostIo>> {
    io: I,
    unique: std::cell::Cell<Option<u64>>,
}

impl<I: AsRef<HostIo>> PendingReply<I> {
    fn new(io: I, request: &PacketRequest<'_>) -> Self {
        Self {
            io,
            unique: std::cell::Cell::new(
                (!matches!(request.opcode, 2 | 42)).then_some(request.unique),
            ),
        }
    }

    pub(super) fn publish(
        &self,
        slices: &[std::io::IoSlice<'_>],
    ) -> std::io::Result<acyclic_native_runtime::PacketDevicePublication> {
        // ENOENT confirms that this request is already gone (for example,
        // interrupted). It completes this obligation without disconnecting
        // unrelated requests. Other failures do not certify request retirement.
        self.unique.set(None);
        self.io
            .as_ref()
            .publish_packet(slices)
            .and_then(|outcome| match outcome {
                acyclic_native_runtime::PacketDevicePublication::Published
                | acyclic_native_runtime::PacketDevicePublication::Rejected(libc::ENOENT) => {
                    Ok(outcome)
                }
                acyclic_native_runtime::PacketDevicePublication::Rejected(errno) => {
                    Err(std::io::Error::from_raw_os_error(errno))
                }
            })
            .inspect_err(|_| self.abort())
    }

    pub(super) fn owes_reply(&self) -> bool {
        self.unique.get().is_some()
    }

    fn abort(&self) {
        if let Some(connection) = self.io.as_ref().connection.get()
            && let Err(error) = connection.abort()
        {
            tracing::error!(%error, "aborting a failed FUSE reply failed");
        }
    }
}

impl PendingReply<&Arc<HostIo>> {
    fn transfer(&self) -> PendingReply<Arc<HostIo>> {
        PendingReply {
            io: Arc::clone(self.io),
            unique: std::cell::Cell::new(self.unique.take()),
        }
    }
}
impl<I: AsRef<HostIo>> Drop for PendingReply<I> {
    fn drop(&mut self) {
        if let Some(unique) = self.unique.take() {
            let mut error = [0; super::packet::OUTPUT_HEADER];
            error[..4].copy_from_slice(&(super::packet::OUTPUT_HEADER as u32).to_ne_bytes());
            error[4..8].copy_from_slice(&(-libc::EIO).to_ne_bytes());
            error[8..].copy_from_slice(&unique.to_ne_bytes());
            let _ = self.publish(&[std::io::IoSlice::new(&error)]);
        }
    }
}

/// Exercise the real pending-reply publisher with a kernel-expired identity.
#[cfg(test)]
pub(super) fn publish_expired_packet_for_test(
    io: &Arc<HostIo>,
    protocol: &PacketSession,
    request: &PacketRequest<'_>,
    response: &mut [u8],
    old_delivery: bool,
) -> Result<(), NativeMountError> {
    let pending = PendingReply::new(io, request);
    protocol
        .publish_with_outcome(request, response, |reply| {
            let slices = reply.slices().ok_or(libc::EPROTO)?;
            let outcome = pending.publish(&slices)?;
            assert_eq!(
                outcome,
                acyclic_native_runtime::PacketDevicePublication::Rejected(libc::ENOENT)
            );
            Ok(if old_delivery {
                // Exact former policy: label a rejected reply as delivered.
                acyclic_native_runtime::PacketDevicePublication::Published
            } else {
                outcome
            })
        })
        .map(|_| ())
        .map_err(super::packet_dispatch::PacketFailure::into_native_error)
}

/// Exercise the real pending-reply publisher with a kernel-expired identity.
#[cfg(test)]
pub(super) fn publish_expired_reply_for_test(
    io: &Arc<HostIo>,
    old_abort: bool,
) -> std::io::Result<()> {
    let mut input = [0; acyclic_native_runtime::fuse_protocol::INPUT_HEADER];
    let length = u32::try_from(input.len()).map_err(std::io::Error::other)?;
    input[..4].copy_from_slice(&length.to_ne_bytes());
    input[4..8].copy_from_slice(&3_u32.to_ne_bytes());
    input[8..16].copy_from_slice(&u64::MAX.to_ne_bytes());
    input[16..24].copy_from_slice(&ROOT_INODE.to_ne_bytes());
    let request = PacketRequest::parse(&input).map_err(std::io::Error::from_raw_os_error)?;
    let pending = PendingReply::new(io, &request);
    let mut reply = [0; super::packet::OUTPUT_HEADER];
    acyclic_native_runtime::fuse_protocol::reply_header(
        &mut reply,
        super::packet::OUTPUT_HEADER,
        request.unique,
        Some(libc::EIO),
    )
    .map_err(std::io::Error::from_raw_os_error)?;
    let slices = [std::io::IoSlice::new(&reply)];
    if old_abort {
        // Exact former policy: a confirmed device rejection is flattened to
        // an I/O error, aborts the connection, and escapes to worker teardown.
        pending.unique.set(None);
        io.publish(&slices).inspect_err(|_| pending.abort())
    } else {
        pending.publish(&slices).map(|_| ())
    }
}

#[cfg(test)]
fn owner_allowed(owner_uid: u32, request: &PacketRequest<'_>) -> bool {
    acyclic_native_runtime::fuse_protocol::Request {
        opcode: request.opcode,
        unique: request.unique,
        inode: request.inode,
        owner: request.owner,
        body: request.body,
    }
    .owner_allowed(owner_uid)
}

fn source_protocol(
    router: PacketRouter,
    io: &Arc<HostIo>,
    maximum_write: u32,
    page_size: std::num::NonZeroU32,
    _buffers: &PacketBufferPool<PacketBuffers>,
) -> Result<PacketSession, NativeMountError> {
    // Only the explicit old-path control owns an in-process physical device.
    #[cfg(test)]
    if !io.framed {
        return PacketSession::new(router, maximum_write, page_size);
    }
    let capabilities = router.cache_capabilities()?.bits();
    // Original source admission survives the positive physical INIT receipt.
    let _initialization = router
        .initialize()
        .map_err(super::packet_dispatch::PacketFailure::into_native_error)?;
    #[cfg(test)]
    let capture = super::tests::packets::Capture::current();
    #[cfg(test)]
    let trace = capture
        .as_ref()
        .map(|_| {
            rustix::fs::memfd_create(
                c"fuse-initialization-capture",
                rustix::fs::MemfdFlags::CLOEXEC | rustix::fs::MemfdFlags::ALLOW_SEALING,
            )
            .map_err(std::io::Error::from)
            .map(std::fs::File::from)
        })
        .transpose()?;
    #[cfg(not(test))]
    let trace: Option<std::fs::File> = None;
    io.start_fuse(
        acyclic_native_runtime::fuse_protocol::Configuration {
            maximum_write,
            page_size: page_size.get(),
            cache_capabilities: capabilities,
        },
        trace.as_ref(),
    )?;
    #[cfg(test)]
    if let (Some(capture), Some(mut trace)) = (capture, trace) {
        use std::io::{Read, Seek};
        trace.rewind()?;
        let mut buffers = _buffers.take(0)?;
        let capacity = maximum_write as usize + 4096;
        loop {
            let mut lengths = [0; 16];
            if trace.read(&mut lengths[..1])? == 0 {
                break;
            }
            trace.read_exact(&mut lengths[1..])?;
            let request_length = usize::try_from(u64::from_ne_bytes(
                lengths[..8].try_into().map_err(|_| libc::EPROTO)?,
            ))
            .map_err(|_| libc::EOVERFLOW)?;
            let reply_length = usize::try_from(u64::from_ne_bytes(
                lengths[8..].try_into().map_err(|_| libc::EPROTO)?,
            ))
            .map_err(|_| libc::EOVERFLOW)?;
            let request = PacketBuffers::input_for(&mut buffers.input, request_length, capacity)?;
            trace.read_exact(request)?;
            let reply = PacketBuffers::input_for(&mut buffers.output, reply_length, capacity)?;
            trace.read_exact(reply)?;
            let sequence = capture.begin();
            capture.completed(sequence, request, Some(reply));
        }
        _buffers.put(0, buffers)?;
    }
    PacketSession::backend(router, maximum_write, page_size)
}
