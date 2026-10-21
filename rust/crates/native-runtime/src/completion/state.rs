use crate::*;

pub(crate) struct CompletionState<T> {
    pub(crate) task_pending: bool,
    pub(crate) result: Option<io::Result<T>>,
    pub(crate) panic: Option<Box<dyn std::any::Any + Send>>,
    pub(crate) waker: Option<Waker>,
    pub(crate) _allocation: Option<ReadAllocation>,
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_wake: bool,
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_accounting: bool,
    #[cfg(test)]
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) broken_charge: bool,
}

impl<T> Default for CompletionState<T> {
    fn default() -> Self {
        Self {
            task_pending: false,
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
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
}

impl<T> CompletionState<T> {
    /// Moves the state's charge to the observer, which retires it after
    /// releasing the lock.
    pub(crate) fn take_charge(&mut self) -> Option<ReadAllocation> {
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        if self.broken_charge {
            return None;
        }
        self._allocation.take()
    }

    pub(crate) fn poll_result(&mut self, waker: &Waker) -> Poll<io::Result<T>> {
        if !self.task_pending
            && let Some(result) = self.result.take()
        {
            return Poll::Ready(result);
        }
        if !self
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            self.waker = Some(waker.clone());
        }
        Poll::Pending
    }
}

pub(crate) trait TaskTerminal: Send + Sync {
    #[cfg(feature = "test-support")]
    fn test_poll_completion(&self, waker: &Waker) -> Poll<()>;
    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>);
    fn publish(&self) -> Option<Waker>;
}

// Sole completion lock; no queue lock, callback, or wake is held under it.
impl<T: Send> TaskTerminal for CompletionMutex<CompletionState<T>> {
    #[cfg(feature = "test-support")]
    fn test_poll_completion(&self, waker: &Waker) -> Poll<()> {
        let mut state = self
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.task_pending && (state.result.is_some() || state.panic.is_some()) {
            Poll::Ready(())
        } else {
            state.waker = Some(waker.clone());
            Poll::Pending
        }
    }

    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>) {
        self.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .panic = Some(payload);
    }
    fn publish(&self) -> Option<Waker> {
        let mut state = self
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        if state.broken_accounting {
            return state.waker.take();
        }
        state.task_pending = false;
        #[cfg(test)]
        #[cfg(acyclic_native_runtime_loom)]
        if state.broken_wake {
            state.waker = None;
        }
        state.waker.take()
    }
}
