use crate::*;

pub(crate) struct CompletionState<T> {
    pub(crate) task_pending: bool,
    pub(crate) result: Option<io::Result<T>>,
    pub(crate) panic: Option<Box<dyn std::any::Any + Send>>,
    pub(crate) waker: Option<Waker>,
    pub(crate) _allocation: Option<ReadAllocation>,
}

/// Publication authority for the existing task's already-funded state Arc.
impl<T> Default for CompletionState<T> {
    fn default() -> Self {
        Self { task_pending: false, result: None, panic: None, waker: None, _allocation: None }
    }
}

impl<T> CompletionState<T> {
    pub(crate) fn poll_result(&mut self, waker: &Waker) -> Poll<io::Result<T>> {
        if !self.task_pending && let Some(result) = self.result.take() {
            return Poll::Ready(result);
        }
        if !self.waker.as_ref().is_some_and(|current| current.will_wake(waker)) {
            self.waker = Some(waker.clone());
        }
        Poll::Pending
    }
}

pub(crate) trait TaskTerminal: Send + Sync {
    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>);
    fn publish(&self) -> Option<Waker>;
}

impl<T: Send> TaskTerminal for Mutex<CompletionState<T>> {
    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>) {
        self.lock().unwrap_or_else(std::sync::PoisonError::into_inner).panic = Some(payload);
    }
    fn publish(&self) -> Option<Waker> {
        let mut state = self.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.task_pending = false;
        state.waker.take()
    }
}
