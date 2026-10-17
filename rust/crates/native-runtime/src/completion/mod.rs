use crate::*;
pub(crate) use fence::{OperationFence, reserve_if_needed};
pub(crate) use file::FileCompletion;
#[cfg(any(windows, target_os = "linux"))]
pub(crate) use file::submit_file_io;
pub(crate) use state::{CompletionState, TaskTerminal};

mod fence;
mod file;
mod state;

/// Runtime-independent completion of an owned file or host operation.
pub struct NativeCompletion<T> {
    #[cfg(feature = "test-support")]
    test_completion: Option<tokio::sync::oneshot::Receiver<io::Result<()>>>,
    pub(crate) pool: &'static LazyLock<io::Result<NativeWorkers>>,
    pub(crate) prepared_wake: Option<PreparedWake>,
    pub(crate) state: Option<Arc<CompletionMutex<CompletionState<T>>>>,
    pub(crate) ready: Option<io::Result<T>>,
    pub(crate) pending: Option<NativeJob>,
    pub(crate) waiter: Option<Arc<CapacityWaiter>>,
    pub(crate) predecessor: Option<Arc<OperationFence>>,
    pub(crate) completion: Option<Arc<OperationFence>>,
}

impl<T> Unpin for NativeCompletion<T> {}

impl NativeCompletion<Vec<Bytes>> {
    pub(crate) fn submit(
        file: Arc<NativeFilePayload>,
        reads: Vec<OwnedRead>,
        health: Option<Arc<FileHealth>>,
        span: obs::OperationSpan,
        #[cfg(windows)] overlapped: bool,
    ) -> Self {
        let span = span.sized(reads.len(), || reads.iter().map(|read| read.length).sum());
        let state = Arc::new(CompletionMutex::new(CompletionState::default()));
        Self::pending(
            NativeJob::Read {
                file,
                reads,
                span,
                #[cfg(windows)]
                overlapped,
                state: Arc::clone(&state),
                completion: None,
                health,
            },
            state,
            &NATIVE_WORKERS,
        )
    }
}

impl<T> NativeCompletion<T> {
    pub(crate) fn pending(
        job: NativeJob,
        state: Arc<CompletionMutex<CompletionState<T>>>,
        pool: &'static LazyLock<io::Result<NativeWorkers>>,
    ) -> Self {
        Self {
            #[cfg(feature = "test-support")]
            test_completion: None,
            pool,
            prepared_wake: None,
            state: Some(state),
            ready: None,
            pending: Some(job),
            waiter: None,
            predecessor: None,
            completion: None,
        }
    }

    pub(crate) fn ready(result: io::Result<T>) -> Self {
        Self {
            #[cfg(feature = "test-support")]
            test_completion: None,
            pool: &NATIVE_WORKERS,
            prepared_wake: None,
            state: None,
            ready: Some(result),
            pending: None,
            waiter: None,
            predecessor: None,
            completion: None,
        }
    }

    /// Waits through the same measured task engine.
    pub fn wait(
        mut self,
        deadline: Option<std::time::Instant>,
        cancellation: impl Future<Output = ()>,
        on_deadline: impl FnOnce(),
    ) -> io::Result<T> {
        if let Some(result) = self.ready.take() {
            return result;
        }
        let meter =
            measured_completion_meter(self.state.as_ref().ok_or(io::ErrorKind::InvalidInput)?)?;
        let wake = self.prepared_wake.take();
        wait_measured_completion(self, &meter, wake, deadline, cancellation, on_deadline)
    }
}

impl<T> Future for NativeCompletion<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(result) = this.ready.take() {
            return Poll::Ready(result);
        }
        if let Err(error) = reserve_if_needed(
            &mut this.predecessor,
            &mut this.completion,
            &mut this.pending,
        ) {
            this.pending = None;
            return Poll::Ready(Err(error));
        }
        if let Some(predecessor) = &this.predecessor {
            if !predecessor.ready_or_register(context.waker()) {
                if let Some(NativeJob::Read { span, .. } | NativeJob::Write { span, .. }) =
                    this.pending.as_ref()
                {
                    span.in_scope(|| {
                        tracing::trace!(name: "acyclic.runtime.fence_wait", "waiting for an earlier operation");
                    });
                }
                return Poll::Pending;
            }
            this.predecessor = None;
        }
        // Poisoned registry entries retain health after the job retires.
        if this
            .completion
            .as_ref()
            .and_then(|fence| fence.health.as_ref())
            .and_then(Weak::upgrade)
            .is_some_and(|health| health.uncertain.load(Ordering::Acquire))
        {
            let result: io::Result<T> = Err(io::Error::other(
                "prior native I/O completion on this file is uncertain",
            ));
            if let Some(NativeJob::Read { span, .. } | NativeJob::Write { span, .. }) =
                this.pending.as_ref()
            {
                span.record_error(result.as_ref().err());
            }
            this.pending = None;
            if let Some(completion) = &this.completion {
                completion.complete();
            }
            return Poll::Ready(result);
        }
        #[cfg(feature = "test-support")]
        {
            if this.test_completion.is_none()
                && crate::testing::Context::current().is_some()
                && let Some(job) = this.pending.take()
            {
                this.test_completion = Some(crate::testing::submit(job, this.pool));
            }
            if let Some(waiting) = &mut this.test_completion {
                match Pin::new(waiting).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(Ok(()))) => this.test_completion = None,
                    Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error)),
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(io::Error::other(
                            "controlled native owner failed",
                        )));
                    }
                }
            }
        }
        if std::ptr::eq(this.pool, &BLOCKING_WORKERS)
            && inline_blocking_allowed()
            && let Some(job) = this.pending.take()
        {
            let _ = job.run();
        }
        #[cfg(windows)]
        if !std::ptr::eq(this.pool, &BLOCKING_WORKERS)
            && inline_blocking_allowed()
            && let Some(job) = this.pending.take_if(|job| job.runs_in_place())
        {
            let _ = job.run();
        }
        match poll_submission(this.pool, &mut this.pending, &mut this.waiter, context) {
            Poll::Ready(Err(error)) => {
                this.pending = None;
                if let Some(completion) = &this.completion {
                    completion.complete();
                }
                return Poll::Ready(Err(error));
            }
            Poll::Ready(Ok(())) => {}
            Poll::Pending => return Poll::Pending,
        }
        let Some(state) = this.state.as_ref() else {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        };
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.task_pending
            && let Some(payload) = state.panic.take()
        {
            let waker = state.waker.take();
            let charge = state.take_charge();
            drop(state);
            drop((waker, charge));
            drop(this.pending.take());
            if let Some(waiter) = this.waiter.take()
                && let Ok(workers) = workers(this.pool, true)
            {
                remove_capacity_waiter(workers, Some(waiter));
            }
            drop(this.predecessor.take());
            drop(this.completion.take());
            std::panic::resume_unwind(payload);
        }
        let result = state.poll_result(context.waker());
        // The observer retires the state's charge when it takes the result:
        // the worker may still hold its reference for a moment after
        // publishing. An abandoned observer leaves the charge in the state,
        // where the worker's last reference retires it.
        let charge = result.is_ready().then(|| state.take_charge()).flatten();
        drop(state);
        drop(charge);
        result
    }
}

impl<T> Drop for NativeCompletion<T> {
    fn drop(&mut self) {
        #[cfg(feature = "test-support")]
        if self.test_completion.is_some() {
            // The independently owned completion registered this waker. The
            // cancelled caller must not clear its physical-terminal wake.
            return;
        }
        if self.pending.is_some()
            && let Some(completion) = &self.completion
        {
            completion.complete_after(self.predecessor.take());
        }
        if let Some(state) = &self.state {
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .waker = None;
        }
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        let Ok(workers) = self.pool.as_ref() else {
            return;
        };
        remove_capacity_waiter(workers, Some(waiter));
    }
}
