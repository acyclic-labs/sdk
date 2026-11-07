use crate::*;

/// The original native driver's sole terminal publisher.
///
/// Publishing a result consumes this owner. Losing the owner publishes
/// `BrokenPipe`, never a successful physical acknowledgement. Dropping the
/// observer leaves this owner and its allocation custody intact.
#[must_use]
pub struct NativeCompletionProducer<T> {
    pub(super) state: Option<Arc<CompletionMutex<CompletionState<T>>>>,
}

impl<T> NativeCompletionProducer<T> {
    pub(super) fn new(state: Arc<CompletionMutex<CompletionState<T>>>) -> Self {
        Self { state: Some(state) }
    }

    /// Publishes the original driver's terminal result and wakes its observer.
    /// Call only after retiring the operation's physical resources. Publication
    /// and arbitrary observer wakes must run outside the driver's locks.
    pub fn complete(mut self, result: io::Result<T>) {
        let (state, waker) = self.finish(result);
        drop(state);
        wake_capacity_waiter(waker);
    }

    pub(super) fn finish(
        &mut self,
        result: io::Result<T>,
    ) -> (Arc<CompletionMutex<CompletionState<T>>>, Option<Waker>) {
        let state = self.state.take().unwrap_or_else(|| std::process::abort());
        let waker = Self::publish(&state, result);
        (state, waker)
    }

    fn publish(
        state: &CompletionMutex<CompletionState<T>>,
        result: io::Result<T>,
    ) -> Option<Waker> {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.result = Some(result);
        state.waker.take()
    }
}

impl<T> Drop for NativeCompletionProducer<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let waker = Self::publish(&state, Err(io::ErrorKind::BrokenPipe.into()));
            drop(state);
            wake_capacity_waiter(waker);
        }
    }
}

pub(crate) struct CompletionState<T> {
    pub(crate) task_pending: bool,
    pub(crate) result: Option<io::Result<T>>,
    pub(crate) panic: Option<Box<dyn std::any::Any + Send>>,
    pub(crate) waker: Option<Waker>,
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl<T> Default for CompletionState<T> {
    fn default() -> Self {
        Self {
            task_pending: false,
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        }
    }
}

impl<T> CompletionState<T> {
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
    fn publish(&self, observation: &tracing::Span) -> Option<Waker>;
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
    fn publish(&self, observation: &tracing::Span) -> Option<Waker> {
        let mut state = self
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Capture the original cause before publishing: an observing thread
        // may take the result immediately after this lock is released. These
        // fixed values retain neither the result nor its allocation.
        let evidence = (!observation.is_disabled()).then(|| {
            let panicked = state.panic.is_some();
            let result = match &state.result {
                Some(Ok(_)) => ("ok", panicked.then_some("panic"), None),
                Some(Err(error)) => ("io_error", Some(obs::kind(error)), error.raw_os_error()),
                None if panicked => ("panic", Some("panic"), None),
                None => ("missing", Some("missing_result"), None),
            };
            (result, panicked)
        });
        state.task_pending = false;
        let waker = state.waker.take();
        drop(state);
        // Subscriber callbacks and arbitrary output wakes run outside both
        // admission and completion locks. Missing result is never success.
        if let Some(((terminal, error, os_error), panicked)) = evidence {
            obs::project(|| {
                observation.record("native.terminal", terminal);
                observation.record("native.panicked", panicked);
                if let Some(os_error) = os_error {
                    observation.record("error.os", os_error);
                }
                if terminal == "missing" {
                    observation.record("error.kind", "missing_result");
                    observation.record("outcome", "incomplete");
                } else {
                    acyclic_observability::record_outcome(observation, error);
                }
            });
        }
        waker
    }
}
