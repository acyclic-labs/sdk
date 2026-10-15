use crate::*;

#[derive(Default)]
pub(crate) struct OperationFence {
    pub(crate) state: Mutex<OperationFenceState>,
    pub(crate) settled: Condvar,
    pub(crate) health: Option<Weak<FileHealth>>,
    pub(crate) _allocation: Option<ReadAllocation>,
}

#[derive(Default)]
pub(crate) struct OperationFenceState {
    pub(crate) complete: bool,
    pub(crate) successor: Option<Waker>,
    pub(crate) skipped_successor: Option<Arc<OperationFence>>,
}

impl OperationFence {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    pub(crate) fn wait_until_complete(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.complete {
            tracing::trace!(name: "acyclic.runtime.fence_wait", "waiting for an earlier operation");
        }
        while !state.complete {
            state = self
                .settled
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn complete_after(self: &Arc<Self>, predecessor: Option<Arc<Self>>) {
        let Some(predecessor) = predecessor else {
            self.complete();
            return;
        };
        let mut state = predecessor
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.complete {
            drop(state);
            self.complete();
        } else {
            state.successor = None;
            state.skipped_successor = Some(Arc::clone(self));
        }
    }

    pub(crate) fn ready_or_register(&self, waker: &Waker) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.complete {
            true
        } else {
            state.successor = Some(waker.clone());
            false
        }
    }

    pub(crate) fn complete(&self) {
        let mut next: Option<Arc<Self>> = None;
        loop {
            let current = next.as_deref().unwrap_or(self);
            let mut state = current
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.complete = true;
            let successor = state.successor.take();
            let following = state.skipped_successor.take();
            drop(state);
            if let Some(health) = current.health.as_ref().and_then(Weak::upgrade) {
                let retired = {
                    let mut tail = health
                        .tail
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if tail
                        .as_ref()
                        .is_some_and(|tail| std::ptr::eq(Arc::as_ptr(tail), current))
                    {
                        tail.take()
                    } else {
                        None
                    }
                };
                drop(retired);
            }
            current.settled.notify_all();
            wake_capacity_waiter(successor);
            let Some(following) = following else {
                break;
            };
            next = Some(following);
        }
    }
}

pub(crate) fn reserve_if_needed(
    predecessor: &mut Option<Arc<OperationFence>>,
    completion: &mut Option<Arc<OperationFence>>,
    pending: &mut Option<NativeJob>,
) -> io::Result<()> {
    if completion.is_some() {
        return Ok(());
    }
    let Some(
        NativeJob::Read { health, .. }
        | NativeJob::Write { health, .. }
        | NativeJob::Task { health, .. },
    ) = pending.as_ref()
    else {
        return Ok(());
    };
    let Some(health) = health.clone() else {
        return Ok(());
    };
    let mut tail = health
        .tail
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *predecessor = tail.clone();
    let allocation = match pending.as_ref() {
        Some(NativeJob::Task {
            meter: Some(meter), ..
        }) => Some(ReadAllocation::new(
            meter,
            std::mem::size_of::<OperationFence>(),
        )?),
        _ => None,
    };
    let fence = Arc::new(OperationFence {
        health: Some(Arc::downgrade(&health)),
        _allocation: allocation,
        ..OperationFence::default()
    });
    if let Some(
        NativeJob::Read { completion, .. }
        | NativeJob::Write { completion, .. }
        | NativeJob::Task { completion, .. },
    ) = pending
    {
        *completion = Some(Arc::clone(&fence));
    }
    *tail = Some(Arc::clone(&fence));
    *completion = Some(fence);
    Ok(())
}
