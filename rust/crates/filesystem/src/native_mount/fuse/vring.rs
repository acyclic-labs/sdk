//! Original physical queue custody; filesystem state belongs to PacketSession.

use super::NativeMountError;
use std::sync::Arc;
use virtio_queue::{Queue, QueueT};
use vmm_sys_util::event::{EventConsumer, EventNotifier};

pub(super) struct PacketVring {
    pub(super) queue: Queue,
    // A worker retains the actual event while polling outside the queue lock.
    // Drain can detach it without closing an fd under that borrower.
    pub(super) kick: Option<Arc<EventConsumer>>,
    pub(super) call: Option<EventNotifier>,
    pub(super) enabled: bool,
    failed: bool,
}

impl PacketVring {
    pub(super) fn new(maximum: std::num::NonZeroU16) -> Result<Self, NativeMountError> {
        Ok(Self {
            queue: Queue::new(maximum.get()).map_err(std::io::Error::other)?,
            kick: None,
            call: None,
            enabled: false,
            failed: false,
        })
    }

    pub(super) fn running(&self) -> bool {
        self.enabled && self.queue.ready() && !self.failed
    }

    /// Check while holding the queue lock, immediately before consumption.
    /// A late wake from a detached kick cannot consume a reconfigured queue.
    pub(super) fn accepts_kick(&self, kick: &Arc<EventConsumer>) -> bool {
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
