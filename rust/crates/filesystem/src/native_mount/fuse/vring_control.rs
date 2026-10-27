//! Standard split-ring control over the original mapping and completion owner.

use super::vring::{PacketVring, RingFeatures};
use super::vring_event::CounterId;
use super::vring_memory::GuestRam;
use super::*;
use crate::NativeMaterializationWork;
use acyclic_native_runtime::ProcessWake;
use std::net::Shutdown;
use std::num::NonZeroU16;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64};
use vhost::vhost_user::message::VhostUserMemoryRegion;
use virtio_queue::QueueT;
use vmm_sys_util::event::{EventConsumer, EventNotifier};

// One request queue plus the standard hiprio queue. This is a queue count,
// not a per-provider policy or a claim of notification-queue support.
pub(super) const QUEUE_COUNT: usize = 2;
pub(super) const EVENT_IDX: u64 = 1 << 29;
pub(super) const PROTOCOL_FEATURES: u64 = 1 << 30;
pub(super) const VERSION_1: u64 = 1 << 32;
const INDIRECT: u64 = 1 << 28;
pub(super) const FEATURES: u64 = INDIRECT | EVENT_IDX | PROTOCOL_FEATURES | VERSION_1;

pub(super) struct QueueKick {
    event: EventConsumer,
    pub(super) id: CounterId,
    _allocation: crate::NativeAllocation,
}

pub(super) struct QueueCall {
    pub(super) event: EventNotifier,
    pub(super) id: CounterId,
}

impl std::ops::Deref for QueueKick {
    type Target = EventConsumer;
    fn deref(&self) -> &EventConsumer {
        &self.event
    }
}

pub(super) struct VringControl {
    // Workers borrow memory BEFORE locking a queue, retaining this table until
    // its actual used-ring completion. Replacement takes the write lock first.
    pub(super) memory: RwLock<Option<GuestRam>>,
    pub(super) queues: [Mutex<PacketVring>; QUEUE_COUNT],
    pub(super) work: NativeMaterializationWork,
    pub(super) changes: [ProcessWake; QUEUE_COUNT],
    features: AtomicU64,
    stopped: AtomicBool,
    failure: Mutex<Option<NativeMountError>>,
    connection: UnixStream,
    _allocation: crate::NativeAllocation,
}

impl VringControl {
    pub(super) fn new(
        connection: UnixStream,
        maximum: NonZeroU16,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeMountError> {
        let allocation = work.claim(
            arc_size::<Self>()?
                .checked_add((QUEUE_COUNT * ProcessWake::allocation_bytes()) as u64)
                .ok_or(libc::EOVERFLOW)?,
            1 + QUEUE_COUNT as u64,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            memory: RwLock::new(None),
            queues: [
                Mutex::new(PacketVring::new(maximum)?),
                Mutex::new(PacketVring::new(maximum)?),
            ],
            work: work.clone(),
            changes: [ProcessWake::new()?, ProcessWake::new()?],
            features: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            failure: Mutex::new(None),
            connection,
            _allocation: allocation,
        }))
    }

    pub(super) fn check(&self) -> Result<(), NativeMountError> {
        self.work.admit(crate::WorkCounters::UNCHARGED)?;
        if self.stopped.load(Ordering::Acquire) {
            return Err(libc::ECANCELED.into());
        }
        Ok(())
    }

    /// Preserve the actual typed cause before waking the socket owner. A
    /// protocol errno is only the wire refusal, never the drain result.
    pub(super) fn fail(&self, error: NativeMountError) {
        let mut failure = self.failure.lock().unwrap_or_else(PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error);
        }
        self.stop();
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(super) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        for change in &self.changes {
            let _ = change.stop();
        }
        // This only interrupts control I/O; it does not acknowledge or release
        // active callbacks. The session owner still joins the actual workers.
        let _ = self.connection.shutdown(Shutdown::Both);
    }

    /// Called by the owner only after its actual callback workers have joined.
    pub(super) fn take_failure(&self) -> Option<NativeMountError> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(super) fn features(&self) -> RingFeatures {
        let features = self.features.load(Ordering::Acquire);
        RingFeatures {
            event_idx: features & EVENT_IDX != 0,
            indirect: features & INDIRECT != 0,
        }
    }

    pub(super) fn changed(&self, index: u32) -> Result<(), NativeMountError> {
        self.changes
            .get(index as usize)
            .ok_or(libc::EINVAL)?
            .notify()?;
        Ok(())
    }

    pub(super) fn set_features(&self, features: u64) -> Result<(), NativeMountError> {
        self.check()?;
        if features & !FEATURES != 0
            || features & (VERSION_1 | PROTOCOL_FEATURES) != (VERSION_1 | PROTOCOL_FEATURES)
        {
            return Err(libc::ENOTSUP.into());
        }
        // Feature negotiation precedes ring setup. No active callback can
        // acquire a different interpretation of its original descriptors.
        let _memory = self.memory.write().unwrap_or_else(PoisonError::into_inner);
        let first = self.queue(0)?;
        let second = self.queue(1)?;
        for queue in [&*first, &*second] {
            queue.configurable()?;
            if queue.queue.ready() {
                return Err(libc::EBUSY.into());
            }
        }
        self.features.store(features, Ordering::Release);
        Ok(())
    }

    pub(super) fn set_memory(
        &self,
        offers: &[VhostUserMemoryRegion],
        files: Vec<std::fs::File>,
    ) -> Result<(), NativeMountError> {
        self.check()?;
        // No queue lock is held while awaiting existing memory borrowers.
        let mut memory = self.memory.write().unwrap_or_else(PoisonError::into_inner);
        let mut first = self.queue(0)?;
        let mut second = self.queue(1)?;
        for queue in [&*first, &*second] {
            queue.configurable()?;
            if queue.queue.ready() {
                return Err(libc::EBUSY.into());
            }
        }
        let replacement = GuestRam::new(offers, files, &self.work)?;
        self.check()?;
        first.invalidate_addresses();
        second.invalidate_addresses();
        *memory = Some(replacement);
        Ok(())
    }

    pub(super) fn queue(
        &self,
        index: u32,
    ) -> Result<std::sync::MutexGuard<'_, PacketVring>, NativeMountError> {
        self.queues
            .get(index as usize)
            .ok_or(libc::EINVAL)?
            .lock()
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EIO).into())
    }

    pub(super) fn set_addresses(
        &self,
        index: u32,
        descriptor: u64,
        used: u64,
        available: u64,
    ) -> Result<(), NativeMountError> {
        self.check()?;
        let memory = self.memory.read().unwrap_or_else(PoisonError::into_inner);
        let memory = memory.as_ref().ok_or(libc::EINVAL)?;
        let mut queue = self.queue(index)?;
        queue.configurable()?;
        let size = u64::from(queue.queue.size());
        let descriptor = memory.translate(descriptor, size * 16, &self.work)?;
        let used = memory.translate(used, 6 + size * 8, &self.work)?;
        let available = memory.translate(available, 6 + size * 2, &self.work)?;
        queue.set_addresses(descriptor, used, available, memory.memory().as_ref())
    }

    pub(super) fn set_kick(
        &self,
        index: u32,
        file: Option<std::fs::File>,
    ) -> Result<(), NativeMountError> {
        self.check()?;
        let replacement = match file {
            Some(file) => {
                let id = CounterId::of(&file, &self.work)?;
                let allocation =
                    self.work
                        .claim(arc_size::<QueueKick>()?, 1, crate::WorkCounters::UNCHARGED)?;
                super::transport_io::nonblocking(&file)?;
                // SAFETY: consume the SCM_RIGHTS File's ownership exactly once.
                let event = unsafe { EventConsumer::from_raw_fd(file.into_raw_fd()) };
                Some(Arc::new(QueueKick {
                    event,
                    id,
                    _allocation: allocation,
                }))
            }
            None => None,
        };
        let _memory = self.memory.write().unwrap_or_else(PoisonError::into_inner);
        let mut queues = [self.queue(0)?, self.queue(1)?];
        if let Some(replacement) = &replacement {
            for (other, queue) in queues.iter().enumerate() {
                if (other != index as usize
                    && queue
                        .kick
                        .as_ref()
                        .is_some_and(|kick| kick.id == replacement.id))
                    || queue
                        .call
                        .as_ref()
                        .is_some_and(|call| call.id == replacement.id)
                {
                    return Err(libc::EINVAL.into());
                }
            }
        }
        let queue = queues.get_mut(index as usize).ok_or(libc::EINVAL)?;
        queue.configurable()?;
        queue.kick = replacement;
        self.changed(index)?;
        Ok(())
    }

    pub(super) fn set_call(
        &self,
        index: u32,
        file: Option<std::fs::File>,
    ) -> Result<(), NativeMountError> {
        self.check()?;
        let replacement = match file {
            Some(file) => {
                let id = CounterId::of(&file, &self.work)?;
                super::transport_io::nonblocking(&file)?;
                // SAFETY: consume the SCM_RIGHTS File's ownership exactly once.
                let event = unsafe { EventNotifier::from_raw_fd(file.into_raw_fd()) };
                Some(QueueCall { event, id })
            }
            None => None,
        };
        let _memory = self.memory.write().unwrap_or_else(PoisonError::into_inner);
        let mut queues = [self.queue(0)?, self.queue(1)?];
        if let Some(replacement) = &replacement {
            if queues.iter().any(|queue| {
                queue
                    .kick
                    .as_ref()
                    .is_some_and(|kick| kick.id == replacement.id)
            }) {
                return Err(libc::EINVAL.into());
            }
        }
        let queue = queues.get_mut(index as usize).ok_or(libc::EINVAL)?;
        queue.configurable()?;
        queue.call = replacement;
        self.changed(index)?;
        Ok(())
    }

    pub(super) fn set_enabled(&self, index: u32, enabled: bool) -> Result<(), NativeMountError> {
        self.check()?;
        let memory = self.memory.read().unwrap_or_else(PoisonError::into_inner);
        let mut queue = self.queue(index)?;
        if enabled {
            let memory = memory.as_ref().ok_or(libc::EINVAL)?;
            queue.enable(memory.memory().as_ref(), self.features().event_idx)?;
        } else {
            // Holding the actual queue lock excludes all callbacks through
            // physical publication; disabling cannot race an accepted head.
            queue.enabled = false;
            queue.configurable()?;
        }
        self.changed(index)?;
        Ok(())
    }
}

impl std::task::Wake for VringControl {
    fn wake(self: Arc<Self>) {
        self.stop();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.stop();
    }
}

fn arc_size<T>() -> Result<u64, NativeMountError> {
    crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<T>())
        .map(|bytes| bytes as u64)
        .map_err(|_| libc::EOVERFLOW.into())
}
