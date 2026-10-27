//! Physical queue readiness over the shared packet dispatcher.

use super::NativeMountError;
use super::packet_buffers::PacketBuffers;
use super::packet_session::PacketSession;
use super::vring_control::{QueueKick, VringControl};
use super::vring_packet::{QueueProgress, serve_vring_packet};
use acyclic_native_runtime::ProcessWake;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::sync::{Arc, PoisonError};
use virtio_queue::QueueT;

pub(super) fn run(
    control: &VringControl,
    session: &PacketSession,
    index: u32,
    mut buffers: PacketBuffers,
) -> Result<(), NativeMountError> {
    let change = control.changes.get(index as usize).ok_or(libc::EINVAL)?;
    loop {
        if control.is_stopped() {
            return Ok(());
        }
        control.work.admit(crate::WorkCounters::UNCHARGED)?;
        // Consume before taking the configuration snapshot. A later change
        // remains readable even if it races the following physical wait.
        change.consume()?;
        let (kick, pending) = batch(control, session, index, &mut buffers)?;
        if !pending && !wait(change, kick.as_ref())? {
            return Ok(());
        }
    }
}

fn batch(
    control: &VringControl,
    session: &PacketSession,
    index: u32,
    buffers: &mut PacketBuffers,
) -> Result<(Option<Arc<QueueKick>>, bool), NativeMountError> {
    // This order also belongs to every memory replacement/control path.
    let memory = control
        .memory
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let mut queue = control.queue(index)?;
    let kick = queue.kick.as_ref().filter(|_| queue.running()).cloned();
    let Some(kick) = kick else {
        return Ok((None, false));
    };
    let result = (|| {
        // Retain and validate the same counter under the original queue lock;
        // a detached polling borrower cannot consume a replacement's event.
        if !queue.accepts_kick(&kick) {
            return Ok(false);
        }
        match kick.consume() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
        let memory = memory.as_ref().ok_or(libc::EPROTO)?;
        queue
            .queue
            .disable_notification(memory.memory().as_ref())
            .map_err(io::Error::other)?;
        // Release both locks at least once per actual queue capacity. Control
        // can disable/drain even if a frontend continually replenishes work.
        for _ in 0..queue.queue.size() {
            if control.is_stopped() {
                return Ok(false);
            }
            match serve_vring_packet(
                session,
                &mut queue,
                memory,
                buffers,
                &control.work,
                index == 0,
                control.features(),
            )? {
                QueueProgress::Completed => {}
                QueueProgress::Stopped => {
                    control.stop();
                    return Ok(false);
                }
                QueueProgress::Empty => {
                    // Maintained queue code arms then checks avail with the
                    // required fence. Pending work is pumped without a wait.
                    return queue
                        .queue
                        .enable_notification(memory.memory().as_ref())
                        .map_err(io::Error::other)
                        .map_err(Into::into);
                }
            }
        }
        Ok(true)
    })();
    if result.is_err() {
        // Includes counter/notification failure before or after a callback.
        // GET_VRING_BASE must observe this before it can report completion.
        queue.fail();
    }
    result.map(|pending| (Some(kick), pending))
}

fn wait(change: &ProcessWake, kick: Option<&Arc<QueueKick>>) -> io::Result<bool> {
    use super::transport_io::{self, Ready};
    let descriptor = kick.map(|kick| {
        // SAFETY: the actual kick Arc is borrowed for this entire wait, so
        // replacement cannot close its EventConsumer during the shared poll.
        unsafe { BorrowedFd::borrow_raw(kick.as_raw_fd()) }
    });
    match transport_io::wait(change, descriptor, libc::POLLIN)? {
        Ready::Closed => Ok(false),
        Ready::Wake => Ok(true),
        Ready::Device(events) if events & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 => {
            Err(io::Error::from_raw_os_error(libc::EIO))
        }
        Ready::Device(_) => Ok(true),
    }
}
