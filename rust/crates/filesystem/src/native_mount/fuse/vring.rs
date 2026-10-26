//! Original physical queue custody; filesystem state belongs to PacketSession.

use super::NativeMountError;
use super::vring_control::{QueueCall, QueueKick};
use std::sync::Arc;
use virtio_queue::{Queue, QueueT};
use vm_memory::{GuestAddress, GuestMemoryMmap};

#[derive(Clone, Copy)]
pub(super) struct RingFeatures {
    pub(super) event_idx: bool,
    pub(super) indirect: bool,
}

pub(super) struct PacketVring {
    pub(super) queue: Queue,
    // A worker retains the actual event while polling outside the queue lock.
    // Drain can detach it without closing an fd under that borrower.
    pub(super) kick: Option<Arc<QueueKick>>,
    pub(super) call: Option<QueueCall>,
    pub(super) enabled: bool,
    failed: bool,
    sized: bool,
    addressed: bool,
}

impl PacketVring {
    pub(super) fn new(maximum: std::num::NonZeroU16) -> Result<Self, NativeMountError> {
        Ok(Self {
            queue: Queue::new(maximum.get()).map_err(std::io::Error::other)?,
            kick: None,
            call: None,
            enabled: false,
            failed: false,
            sized: false,
            addressed: false,
        })
    }

    pub(super) fn running(&self) -> bool {
        self.enabled && self.queue.ready() && !self.failed
    }

    /// Check while holding the queue lock, immediately before consumption.
    /// A late wake from a detached kick cannot consume a reconfigured queue.
    pub(super) fn accepts_kick(&self, kick: &Arc<QueueKick>) -> bool {
        self.running()
            && self
                .kick
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, kick))
    }

    /// Must run while the caller still holds this original queue's lock.
    /// Control operations then observe failure before they can acknowledge a
    /// freeze, including when used-index publication succeeded but signalling
    /// the frontend failed. The caller retains the actual typed error.
    pub(super) fn fail(&mut self) {
        self.failed = true;
        self.enabled = false;
    }

    pub(super) fn configurable(&self) -> Result<(), NativeMountError> {
        if self.failed || self.queue.next_avail() != self.queue.next_used() {
            return Err(NativeMountError::VirtioFsCompletionUnknown);
        }
        if self.enabled {
            return Err(libc::EBUSY.into());
        }
        Ok(())
    }

    pub(super) fn set_size(&mut self, size: u32) -> Result<(), NativeMountError> {
        self.configurable()?;
        let size = u16::try_from(size).map_err(|_| libc::EINVAL)?;
        self.queue
            .try_set_size(size)
            .map_err(std::io::Error::other)?;
        self.sized = true;
        self.invalidate_addresses();
        Ok(())
    }

    pub(super) fn invalidate_addresses(&mut self) {
        self.addressed = false;
        self.queue.set_ready(false);
    }

    pub(super) fn set_addresses(
        &mut self,
        descriptor: GuestAddress,
        used: GuestAddress,
        available: GuestAddress,
        memory: &GuestMemoryMmap<()>,
    ) -> Result<(), NativeMountError> {
        self.configurable()?;
        if !self.sized {
            return Err(libc::EINVAL.into());
        }
        // Configure a stack candidate. Failure cannot leave a partly changed
        // live ring, or make the old mapping look ready for a new address set.
        let mut candidate = Queue::new(self.queue.max_size()).map_err(std::io::Error::other)?;
        candidate.set_size(self.queue.size());
        candidate.set_next_avail(self.queue.next_avail());
        candidate.set_next_used(self.queue.next_used());
        candidate
            .try_set_desc_table_address(descriptor)
            .and_then(|()| candidate.try_set_used_ring_address(used))
            .and_then(|()| candidate.try_set_avail_ring_address(available))
            .map_err(std::io::Error::other)?;
        candidate.set_ready(true);
        if !candidate.is_valid(memory) {
            return Err(libc::EFAULT.into());
        }
        self.queue = candidate;
        self.addressed = true;
        Ok(())
    }

    pub(super) fn set_base(&mut self, base: u32) -> Result<(), NativeMountError> {
        self.configurable()?;
        let base = u16::try_from(base).map_err(|_| libc::EINVAL)?;
        self.queue.set_next_avail(base);
        self.queue.set_next_used(base);
        Ok(())
    }

    pub(super) fn enable(
        &mut self,
        memory: &GuestMemoryMmap<()>,
        event_idx: bool,
    ) -> Result<(), NativeMountError> {
        self.configurable()?;
        if !self.sized || !self.addressed || self.kick.is_none() {
            return Err(libc::EINVAL.into());
        }
        self.queue.set_ready(true);
        if !self.queue.is_valid(memory) {
            return Err(libc::EFAULT.into());
        }
        // No inflight restoration is advertised: every accepted request must
        // start at the frontend's actual, completed used boundary.
        let used = self
            .queue
            .used_idx(memory, std::sync::atomic::Ordering::Acquire)
            .map_err(std::io::Error::other)?
            .0;
        if used != self.queue.next_used() {
            return Err(NativeMountError::VirtioFsCompletionUnknown);
        }
        self.queue.set_event_idx(event_idx);
        self.enabled = true;
        Ok(())
    }

    /// Positive GET_VRING_BASE boundary. The control owner holds this queue's
    /// lock, excluding callbacks through their physical completion. An unknown
    /// result remains failed even if the two ring indices happen to match.
    pub(super) fn drain(&mut self) -> Result<u16, NativeMountError> {
        self.enabled = false;
        if self.failed || self.queue.next_avail() != self.queue.next_used() {
            return Err(NativeMountError::VirtioFsCompletionUnknown);
        }
        self.queue.set_ready(false);
        self.kick = None;
        self.call = None;
        Ok(self.queue.next_avail())
    }
}
