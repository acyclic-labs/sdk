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
}

pub(crate) struct CapacityWaiter {
    pub(crate) state: Mutex<CapacityWaiterState>,
}

pub(crate) struct CapacityWaiterState {
    pub(crate) previous: Weak<CapacityWaiter>,
    pub(crate) next: Option<Arc<CapacityWaiter>>,
    pub(crate) waker: Option<Waker>,
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
        }
    }

    /// Installs an already prepared observer and returns its displaced owner.
    /// The caller retires that owner only after unlocking admission.
    pub(crate) fn register_existing(
        &mut self,
        node: &Arc<CapacityWaiter>,
        waker: Waker,
    ) -> Option<Waker> {
        let retired = {
            let mut state = node
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let retired = state.waker.replace(waker);
            if state.queued {
                return retired;
            }
            state.previous = self.tail.clone();
            state.next = None;
            state.queued = true;
            retired
        };
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
        retired
    }

    /// Unlinks the owner's waiter and returns the next waiter's waker. The
    /// caller retains the retired owner until the admission lock is released.
    /// Its allocation stays funded while any notifier still owns the node.
    pub(crate) fn retire_owner(&mut self, node: &Arc<CapacityWaiter>) -> Option<Waker> {
        let mut state = node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.queued {
            return None;
        }
        state.queued = false;
        let previous = std::mem::take(&mut state.previous).upgrade();
        let next = state.next.take();
        if self
            .notify_next
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, node))
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
        drop(state);
        if previous.is_none() {
            self.head.as_ref().and_then(|head| {
                head.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .waker
                    .take()
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
            waker: None,
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
        Err(error) => {
            if let Some(job) = pending.as_ref() {
                job.record_submission_error(&error);
            }
            return Poll::Ready(Err(error));
        }
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
        job.record_submission_error(&error);
        remove_capacity_waiter(workers, waiter.take());
        return Poll::Ready(Err(error));
    }
    let observation = job.queue_observation();
    job.stage(1);
    // Declare the owner before the guard so callback unwind also unlocks
    // admission before retiring this node.
    let retired;
    let mut displaced = None;
    let mut admission = workers
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A job that never waits retires its prepaid waiter charge on admission.
    let mut prepaid = job.take_admission();
    let submission = match workers.sender.try_send(job) {
        Err(mpsc::TrySendError::Full(mut job)) => {
            let registered = waiter.as_ref().is_some_and(|node| {
                let state = node
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.queued
                    && state
                        .waker
                        .as_ref()
                        .is_some_and(|waker| waker.will_wake(context.waker()))
            });
            if registered {
                Err(mpsc::TrySendError::Full(job))
            } else {
                // RawWaker callbacks and node allocation may reenter admission.
                // Rejoin the serialized capacity event with one transfer after
                // preparing the observer: a release during clone cannot pass
                // registration unnoticed. Accepted and unchanged polls keep
                // their single transfer and never clone an observer.
                job.set_queued(false);
                drop(admission);
                displaced = Some(context.waker().clone());
                let node = waiter.get_or_insert_with(|| capacity_waiter(prepaid.take()));
                admission = workers
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                job.set_queued(true);
                let submission = workers.sender.try_send(job);
                if matches!(&submission, Err(mpsc::TrySendError::Full(_))) {
                    displaced = admission.register_existing(
                        node,
                        displaced.take().expect("prepared capacity observer"),
                    );
                }
                submission
            }
        }
        submission => submission,
    };
    match submission {
        Ok(()) => {
            admission.queued_jobs += 1;
            retired = waiter.take();
            let next = retired
                .as_ref()
                .and_then(|node| admission.retire_owner(node));
            drop(admission);
            wake_capacity_waiter(next);
            drop(retired);
            drop(displaced);
            if let Some(observation) = observation {
                obs::project(move || {
                    observation.record("native.queued", true);
                    drop(obs::OperationSpan::new_root(observation));
                });
            }
            drop(prepaid);
            Poll::Ready(Ok(()))
        }
        Err(mpsc::TrySendError::Full(mut job)) => {
            // No acceptance callback for a failed candidate transfer.
            job.set_queued(false);
            debug_assert!(prepaid.is_none(), "admission waiter paid twice");
            drop(admission);
            job.stage(0);
            *pending = Some(job);
            drop(displaced);
            Poll::Pending
        }
        Err(mpsc::TrySendError::Disconnected(job)) => {
            retired = waiter.take();
            let next = retired
                .as_ref()
                .and_then(|node| admission.retire_owner(node));
            drop(admission);
            wake_capacity_waiter(next);
            drop(retired);
            drop(displaced);
            let error = io::Error::from(io::ErrorKind::BrokenPipe);
            job.record_submission_error(&error);
            drop(job);
            drop(prepaid);
            Poll::Ready(Err(error))
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
    let next = admission.retire_owner(&waiter);
    drop(admission);
    wake_capacity_waiter(next);
    drop(waiter);
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
        admission.queued_jobs = admission.queued_jobs.saturating_sub(1);
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
            let mut state = node
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            admission.notify_next = state.next.clone();
            state.waker.take()
        };
        // A consumer may cancel, poll, or release more capacity synchronously.
        // The cursor lives under admission, so those edits cannot strand the
        // remainder and concurrent releases coalesce without allocating.
        wake_capacity_waiter(waker);
    }
}
