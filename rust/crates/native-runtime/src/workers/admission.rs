use crate::sync::{Mutex, mpsc};
use crate::*;

// Lock order: queue admission, then channel or waiter nodes. Admission serializes
// edits to adjacent nodes and the release cursor. Notification never follows a
// retired node: unlinking advances the shared cursor before clearing its edges.
// Wakes run after every lock is released. A notifier may still hold a node
// while it wakes the owner, so the charge stays with the shared node. The Loom
// models exercise both registration/release orders and retired shared owners.

pub(crate) struct AdmissionState {
    pub(crate) head: Option<Arc<CapacityWaiter>>,
    pub(crate) tail: Weak<CapacityWaiter>,
    pub(crate) waiter_count: usize,
    pub(crate) queued_jobs: usize,
    notifying: bool,
    notify_again: bool,
    notify_next: Option<Arc<CapacityWaiter>>,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(crate) broken_wake: bool,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(crate) broken_accounting: bool,
    /// Refunds a live shared node early, for the Loom negative control.
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(crate) broken_charge: bool,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(crate) broken_broadcast: bool,
}

pub(crate) struct CapacityWaiter {
    pub(crate) state: Mutex<CapacityWaiterState>,
}

pub(crate) struct CapacityWaiterState {
    pub(crate) previous: Weak<CapacityWaiter>,
    pub(crate) next: Option<Arc<CapacityWaiter>>,
    pub(crate) waker: Waker,
    pub(crate) queued: bool,
    /// The node's original allocation charge, retained through its final owner.
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl AdmissionState {
    pub(crate) fn new() -> Self {
        Self {
            head: None,
            tail: Weak::new(),
            waiter_count: 0,
            queued_jobs: 0,
            notifying: false,
            notify_again: false,
            notify_next: None,
            #[cfg(all(test, acyclic_native_runtime_loom))]
            broken_wake: false,
            #[cfg(all(test, acyclic_native_runtime_loom))]
            broken_accounting: false,
            #[cfg(all(test, acyclic_native_runtime_loom))]
            broken_charge: false,
            #[cfg(all(test, acyclic_native_runtime_loom))]
            broken_broadcast: false,
        }
    }

    /// Queues a new node carrying the job's prepaid charge, if metered.
    pub(crate) fn register(
        &mut self,
        waker: &Waker,
        charge: Option<ReadAllocation>,
    ) -> Arc<CapacityWaiter> {
        let node = capacity_waiter(charge);
        self.register_existing(&node, waker);
        node
    }

    pub(crate) fn register_existing(&mut self, node: &Arc<CapacityWaiter>, waker: &Waker) {
        {
            let mut state = node
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            debug_assert!(!state.queued);
            state.previous = self.tail.clone();
            state.next = None;
            state.waker.clone_from(waker);
            state.queued = true;
        }
        if let Some(tail) = self.tail.upgrade() {
            tail.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next = Some(Arc::clone(node));
        } else {
            self.head = Some(Arc::clone(node));
        }
        self.tail = Arc::downgrade(node);
        self.waiter_count += 1;
    }

    /// Unlinks the owner's waiter and returns the next waiter's waker. The
    /// allocation stays funded while a notifier still owns the retired node.
    pub(crate) fn retire_owner(&mut self, node: Arc<CapacityWaiter>) -> Option<Waker> {
        let mut state = node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(all(test, acyclic_native_runtime_loom))]
        if self.broken_charge {
            state._allocation.take();
        }
        if !state.queued {
            return None;
        }
        state.queued = false;
        let previous = std::mem::take(&mut state.previous).upgrade();
        let next = state.next.take();
        if self
            .notify_next
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &node))
        {
            self.notify_next = next.clone();
        }
        if let Some(previous) = &previous {
            previous
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next = next.clone();
        } else {
            self.head = next.clone();
        }
        if let Some(next) = next {
            next.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .previous = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        } else {
            self.tail = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        }
        self.waiter_count -= 1;
        // The owner's reference ends here; a notifier may still hold the node.
        drop(state);
        drop(node);
        #[cfg(all(test, acyclic_native_runtime_loom))]
        let notify = !self.broken_wake;
        #[cfg(not(all(test, acyclic_native_runtime_loom)))]
        let notify = true;
        if previous.is_none() && notify {
            self.head.as_ref().map(|head| {
                head.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .waker
                    .clone()
            })
        } else {
            None
        }
    }
}

/// Pays for the waiter node a metered task needs if it must wait for queue
/// capacity. Waiting is host load, not request work, so every metered task
/// pays exactly this once, whether or not it ever waits.
pub(crate) fn prepay_capacity_waiter(
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<ReadAllocation> {
    ReadAllocation::new(meter, arc_allocation_bytes::<CapacityWaiter>())
}

pub(crate) fn capacity_waiter(allocation: Option<ReadAllocation>) -> Arc<CapacityWaiter> {
    Arc::new(CapacityWaiter {
        state: Mutex::new(CapacityWaiterState {
            previous: Weak::new(),
            next: None,
            waker: Waker::noop().clone(),
            queued: false,
            _allocation: allocation,
        }),
    })
}

pub(crate) fn poll_submission(
    pool: &'static LazyLock<io::Result<NativeWorkers>>,
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<Arc<CapacityWaiter>>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    if pending.is_none() {
        return Poll::Ready(Ok(()));
    }
    let measured = pending.as_ref().is_some_and(|job| job.meter().is_some());
    let workers = match if measured {
        workers(pool, true)
    } else {
        workers(pool, false)
    } {
        Ok(workers) => workers,
        Err(error) => return Poll::Ready(Err(error)),
    };
    poll_submission_with(workers, pending, waiter, context)
}

pub(crate) fn poll_submission_with(
    workers: &NativeWorkers,
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<Arc<CapacityWaiter>>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    let Some(mut job) = pending.take() else {
        return Poll::Ready(Ok(()));
    };
    let meter = job.meter().cloned();
    if let Some(meter) = &meter
        && let Err(error) = meter.check()
    {
        remove_capacity_waiter(workers, waiter.take());
        return Poll::Ready(Err(error));
    }
    let mut admission = workers
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(node) = waiter.as_ref() {
        node.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker
            .clone_from(context.waker());
    }
    // A job that never waits retires its prepaid waiter charge on admission.
    let prepaid = job.take_admission();
    match workers.sender.try_send(job) {
        Ok(()) => {
            admission.queued_jobs += 1;
            let next = waiter.take().and_then(|node| admission.retire_owner(node));
            drop(admission);
            drop(prepaid);
            wake_capacity_waiter(next);
            Poll::Ready(Ok(()))
        }
        Err(mpsc::TrySendError::Full(job)) => {
            if let Some(node) = waiter.as_ref() {
                // The existing node already carries the job's charge.
                debug_assert!(prepaid.is_none(), "admission waiter paid twice");
                let queued = node
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .queued;
                if !queued {
                    admission.register_existing(node, context.waker());
                }
            } else {
                *waiter = Some(admission.register(context.waker(), prepaid));
            }
            *pending = Some(job);
            Poll::Pending
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            let next = waiter.take().and_then(|node| admission.retire_owner(node));
            drop(admission);
            drop(prepaid);
            wake_capacity_waiter(next);
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }
    }
}

pub(crate) fn remove_capacity_waiter(workers: &NativeWorkers, waiter: Option<Arc<CapacityWaiter>>) {
    let Some(waiter) = waiter else {
        return;
    };
    let mut admission = workers
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let next = admission.retire_owner(waiter);
    drop(admission);
    wake_capacity_waiter(next);
}

pub(crate) fn wake_capacity_waiter(waiter: Option<Waker>) {
    if let Some(waiter) = waiter {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waiter.wake()));
    }
}

pub(crate) fn notify_capacity_released(admission: &Mutex<AdmissionState>) {
    {
        let mut admission = admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_ne!(admission.queued_jobs, 0, "native queue accounting lost");
        #[cfg(all(test, acyclic_native_runtime_loom))]
        if admission.broken_accounting {
            return;
        }
        admission.queued_jobs = admission.queued_jobs.saturating_sub(1);
        #[cfg(all(test, acyclic_native_runtime_loom))]
        if admission.broken_wake {
            return;
        }
        if admission.notifying {
            // A waiter already visited by the active notifier may have polled
            // while the channel was full. Cover this release in a fresh pass.
            admission.notify_again = true;
            return;
        }
        admission.notifying = true;
        admission.notify_next = admission.head.clone();
    }
    loop {
        let waker = {
            let mut admission = admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if admission.notify_next.is_none() && admission.notify_again {
                admission.notify_again = false;
                admission.notify_next = admission.head.clone();
            }
            let Some(node) = admission.notify_next.take() else {
                admission.notifying = false;
                return;
            };
            let state = node
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            admission.notify_next = state.next.clone();
            #[cfg(all(test, acyclic_native_runtime_loom))]
            if admission.broken_broadcast {
                admission.notify_next = None;
            }
            state.waker.clone()
        };
        // A consumer may cancel, poll, or release more capacity synchronously.
        // The cursor lives under admission, so those edits cannot strand the
        // remainder and concurrent releases coalesce without allocating.
        wake_capacity_waiter(Some(waker));
    }
}
