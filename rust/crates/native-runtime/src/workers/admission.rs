use crate::sync::{Mutex, mpsc};
use crate::*;

// Lock order: queue admission, then channel or waiter nodes. Admission serializes
// edits to adjacent nodes; release notification holds only one node at a time.
// Wakes run after every lock is released. A notifier may still hold a node
// while it wakes the owner, so the owner takes the node's charge out under the
// lock that unlinks it and drops it after admission. The Loom models exercise
// both orders of registration and capacity release, and owner admission or
// cancellation against a notifier that still holds the node, on these locks.

pub(crate) struct AdmissionState {
    pub(crate) head: Option<Arc<CapacityWaiter>>,
    pub(crate) tail: Weak<CapacityWaiter>,
    pub(crate) waiter_count: usize,
    pub(crate) queued_jobs: usize,
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_wake: bool,
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_accounting: bool,
    /// Leaves a retired waiter's charge on its node, for the Loom control.
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_charge: bool,
}

pub(crate) struct CapacityWaiter {
    pub(crate) state: Mutex<CapacityWaiterState>,
}

pub(crate) struct CapacityWaiterState {
    pub(crate) previous: Weak<CapacityWaiter>,
    pub(crate) next: Option<Arc<CapacityWaiter>>,
    pub(crate) waker: Waker,
    pub(crate) queued: bool,
    /// The node's own allocation charge, taken back by its owner on retirement.
    pub(crate) charge: Option<ReadAllocation>,
}

impl AdmissionState {
    pub(crate) fn new() -> Self {
        Self {
            head: None,
            tail: Weak::new(),
            waiter_count: 0,
            queued_jobs: 0,
            #[cfg(test)]
            #[cfg(acyclic_native_runtime_loom)]
            broken_wake: false,
            #[cfg(test)]
            #[cfg(acyclic_native_runtime_loom)]
            broken_accounting: false,
            #[cfg(test)]
            #[cfg(acyclic_native_runtime_loom)]
            broken_charge: false,
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

    /// Retires the owner's waiter and returns the next waiter's waker with the
    /// owner's charge, which the caller drops after releasing admission. The
    /// charge leaves the node under the same lock that unlinks it, queued or
    /// not, because a notifier may still hold the node after the owner lets go.
    pub(crate) fn retire_owner(
        &mut self,
        node: Arc<CapacityWaiter>,
    ) -> (Option<Waker>, Option<ReadAllocation>) {
        let mut state = node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        let charge = if self.broken_charge {
            None
        } else {
            state.charge.take()
        };
        #[cfg(not(all(test, acyclic_native_runtime_loom)))]
        let charge = state.charge.take();
        if !state.queued {
            return (None, charge);
        }
        state.queued = false;
        let previous = state.previous.upgrade();
        let next = state.next.clone();
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
        let next = if previous.is_none() {
            self.head.as_ref().map(|head| {
                head.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .waker
                    .clone()
            })
        } else {
            None
        };
        (next, charge)
    }
}

/// Pays for the waiter node a metered task needs if it must wait for queue
/// capacity. Waiting is host load, not request work, so every metered task
/// pays exactly this once, whether or not it ever waits.
pub(crate) fn prepay_capacity_waiter(
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<ReadAllocation> {
    ReadAllocation::new(meter, std::mem::size_of::<CapacityWaiter>())
}

pub(crate) fn capacity_waiter(allocation: Option<ReadAllocation>) -> Arc<CapacityWaiter> {
    Arc::new(CapacityWaiter {
        state: Mutex::new(CapacityWaiterState {
            previous: Weak::new(),
            next: None,
            waker: Waker::noop().clone(),
            queued: false,
            charge: allocation,
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
        let queued = node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .queued;
        if !queued && admission.queued_jobs != 0 && admission.head.is_some() {
            admission.register_existing(node, context.waker());
        }
        node.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker
            .clone_from(context.waker());
        if admission.queued_jobs != 0
            && admission
                .head
                .as_ref()
                .is_some_and(|head| !Arc::ptr_eq(head, node))
        {
            *pending = Some(job);
            return Poll::Pending;
        }
    } else if admission.queued_jobs != 0 && admission.head.is_some() {
        *waiter = Some(admission.register(context.waker(), job.take_admission()));
        *pending = Some(job);
        return Poll::Pending;
    }
    // A job that never waits retires its prepaid waiter charge on admission.
    let prepaid = job.take_admission();
    match workers.sender.try_send(job) {
        Ok(()) => {
            admission.queued_jobs += 1;
            let (next, charge) = waiter
                .take()
                .map_or((None, None), |node| admission.retire_owner(node));
            drop(admission);
            drop((charge, prepaid));
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
            let (next, charge) = waiter
                .take()
                .map_or((None, None), |node| admission.retire_owner(node));
            drop(admission);
            drop((charge, prepaid));
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
    let (next, charge) = admission.retire_owner(waiter);
    drop(admission);
    drop(charge);
    wake_capacity_waiter(next);
}

pub(crate) fn wake_capacity_waiter(waiter: Option<Waker>) {
    if let Some(waiter) = waiter {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waiter.wake()));
    }
}

pub(crate) fn notify_capacity_released(admission: &Mutex<AdmissionState>) {
    let (mut node, count) = {
        let mut admission = admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_ne!(admission.queued_jobs, 0, "native queue accounting lost");
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        if admission.broken_accounting {
            return;
        }
        admission.queued_jobs = admission.queued_jobs.saturating_sub(1);
        let count = if admission.queued_jobs == 0 {
            admission.waiter_count
        } else {
            usize::from(admission.head.is_some())
        };
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        let count = if admission.broken_wake { 0 } else { count };
        (admission.head.clone(), count)
    };
    for _ in 0..count {
        let Some(current) = node else {
            break;
        };
        let (next, waker) = {
            let state = current
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.next.clone(), state.waker.clone())
        };
        node = next;
        wake_capacity_waiter(Some(waker));
    }
}
