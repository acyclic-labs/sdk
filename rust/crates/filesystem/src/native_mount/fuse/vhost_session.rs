//! Standard socket control and queue workers retain one physical session.
//!
//! Construction here does not admit a filesystem source or a guest cache
//! contract. The public admitted source entry must supply the PacketSession.

use super::packet_session::PacketSession;
use super::packet_workers::{self, PacketWorkers};
use super::vhost_control::VhostControl;
use super::vring_control::{QUEUE_COUNT, VringControl};
use super::{Arc, Mutex, NativeMountError, ProjectionCore};
use crate::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::num::{NonZeroU16, NonZeroU32};
use std::os::unix::net::UnixStream;
use vhost::vhost_user::BackendReqHandler;
use vhost::vhost_user::message::{MAX_ATTACHED_FD_ENTRIES, MAX_MSG_SIZE};

pub(super) struct VhostSession {
    protocol: PacketSession,
    core: Arc<ProjectionCore>,
    handler: Mutex<BackendReqHandler<Mutex<VhostControl>>>,
    _cancellation: crate::Cancelled<'static>,
    control: Arc<VringControl>,
    // Includes the handler Arc and helper framing storage, retained through
    // physical replies, partial I/O, and every worker's positive join.
    _allocation: NativeAllocation,
}

/// Physical workers have joined. The original core and its actual callback
/// cause/counters remain owned until the admitted source's retirement owner
/// consumes them. A transport errno is not a substitute for that receipt.
pub(super) struct VhostRetired {
    pub(super) transport: Result<(), NativeMountError>,
    core: Arc<ProjectionCore>,
}

impl VhostRetired {
    pub(super) fn take_callback_failure(&self) -> Option<(NativeMountError, WorkCounters)> {
        self.core.take_handler_failure()
    }

    pub(super) fn completed_callback_work(&self) -> WorkCounters {
        self.core.completed_callback_work()
    }
}

impl VhostSession {
    pub(super) fn new(
        protocol: PacketSession,
        connection: UnixStream,
        maximum_queue: NonZeroU16,
        tag: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        // One standard volume attachment retains its exact admitted source.
        // Shared host namespace routing is not a guest source admission.
        let core = Arc::clone(protocol.single_core().ok_or(libc::ENOTSUP)?);
        // The maintained decoder is blocking. Shutdown on this duplicate
        // interrupts it without closing an fd under a recvmsg/sendmsg borrower.
        connection.set_nonblocking(false)?;
        let control = VringControl::new(connection.try_clone()?, maximum_queue, work)?;
        let allocation = framing_storage(work)?;
        let handler = Arc::new(Mutex::new(VhostControl::new(Arc::clone(&control), tag)?));
        let cancellation = super::packet_cancellation::register(work, &control)?;
        Ok(Self {
            protocol,
            core,
            handler: Mutex::new(BackendReqHandler::from_stream(connection, handler)),
            _cancellation: cancellation,
            control,
            _allocation: allocation,
        })
    }

    pub(super) fn run(self) -> VhostRetired {
        let result = packet_workers::run(
            &self,
            NonZeroU32::MIN.saturating_add(QUEUE_COUNT as u32),
            &self.control.work,
        );
        // Only the owner reaches this point, after the actual control and data
        // workers have ALL joined. A socket errno cannot replace the original
        // callback/admission/publication failure retained before shutdown.
        VhostRetired {
            transport: match self.control.take_failure() {
                Some(error) => Err(error),
                None => result,
            },
            core: Arc::clone(&self.core),
        }
    }

    fn serve_control(&self) -> Result<(), NativeMountError> {
        let mut handler = self.handler.lock().map_err(|_| libc::EIO)?;
        while !self.control.is_stopped() {
            self.control.check()?;
            self.control.work.charge(super::Items.delta(1))?;
            if let Err(error) = handler.handle_request() {
                // A local stop interrupts an in-progress receive. It already
                // has its original cause or a completed FUSE DESTROY. An
                // unsolicited EOF/partial message is always a typed failure.
                if !self.control.is_stopped() {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl PacketWorkers for VhostSession {
    fn stop(&self) {
        self.control.stop();
    }

    fn run_worker(&self, index: u32) -> Result<(), NativeMountError> {
        let result = if index == QUEUE_COUNT as u32 {
            self.serve_control()
        } else {
            super::vring_worker::run(
                &self.control,
                &self.protocol,
                index,
                self.protocol.packet_capacity(),
            )
        };
        if let Err(error) = result {
            // Retain before wake; otherwise control could observe its induced
            // EOF first and mask the failure that actually stopped the device.
            self.control.fail(error);
        }
        Ok(())
    }
}

impl Drop for VhostSession {
    fn drop(&mut self) {
        self.control.stop();
        self.protocol.close();
    }
}

fn framing_storage(work: &NativeMaterializationWork) -> Result<NativeAllocation, NativeMountError> {
    use std::mem::size_of;
    // vhost0.16 Endpoint: <=4KiB payload, <=32 rights; recv_header's
    // partial-header loop may retain one File vector while receiving another.
    // recvmsg's FamStructWrapper rounds header+rights to cmsghdr-sized slots.
    // Replies here have <=3 iovs and no rights. Reserve both phases before
    // entering the helper, without copying its framing or SCM_RIGHTS decoder.
    let ancillary = (size_of::<libc::cmsghdr>()
        + MAX_ATTACHED_FD_ENTRIES * size_of::<std::os::fd::RawFd>())
    .next_multiple_of(size_of::<libc::cmsghdr>());
    let storage = [
        MAX_MSG_SIZE,
        MAX_ATTACHED_FD_ENTRIES * size_of::<std::os::fd::RawFd>(),
        MAX_ATTACHED_FD_ENTRIES * size_of::<std::fs::File>(),
        MAX_ATTACHED_FD_ENTRIES * size_of::<std::fs::File>(),
        ancillary,
        size_of::<usize>(),
        size_of::<libc::iovec>(),
        3 * size_of::<usize>(),
        3 * size_of::<&[u8]>(),
        3 * size_of::<libc::iovec>(),
        size_of::<libc::cmsghdr>(),
    ];
    let owner =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Mutex<VhostControl>>())
            .map_err(|_| libc::EOVERFLOW)?;
    let bytes = storage.iter().try_fold(owner, |total, bytes| {
        total.checked_add(*bytes).ok_or(libc::EOVERFLOW)
    })?;
    work.claim(
        bytes as u64,
        storage.len() as u64 + 1,
        WorkCounters::UNCHARGED,
    )
    .map_err(Into::into)
}
