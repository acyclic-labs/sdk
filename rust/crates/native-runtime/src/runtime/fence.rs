use super::*;

#[derive(Default)]
pub(crate) struct OperationFence {
    pub(crate) state: Mutex<OperationFenceState>,
    pub(crate) settled: Condvar,
    pub(crate) sequencer: Option<Weak<Mutex<Option<Arc<OperationFence>>>>>,
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

    // Drain without admission checks; retire custody outside state/sequencer locks.
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
            // Remove only this completed tail; a concurrent successor owns its own predecessor.
            if let Some(sequencer) = current.sequencer.as_ref().and_then(Weak::upgrade) {
                let retired = {
                    let mut tail = sequencer
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
