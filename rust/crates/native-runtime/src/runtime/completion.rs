use super::*;

/// Runtime-independent completion of an owned file or host operation.
pub struct NativeCompletion<T> {
    pub(crate) pool: &'static LazyLock<io::Result<NativeWorkers>>,
    pub(crate) prepared_wake: Option<Arc<BlockingWaitWake>>,
    pub(crate) state: Option<Arc<Mutex<CompletionState<T>>>>,
    pub(crate) ready: Option<io::Result<T>>,
    pub(crate) pending: Option<NativeJob>,
    pub(crate) waiter: Option<Arc<CapacityWaiter>>,
    pub(crate) sequencer: Option<FileSequencer>,
    pub(crate) uncertain: Option<Arc<AtomicBool>>,
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
        let state = Arc::new(Mutex::new(CompletionState::default()));
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
        state: Arc<Mutex<CompletionState<T>>>,
        pool: &'static LazyLock<io::Result<NativeWorkers>>,
    ) -> Self {
        let (NativeJob::Read { health, .. } | NativeJob::Unit { health, .. } | NativeJob::Task { health, .. }) = &job;
        let health = health.as_ref();
        let sequencer = health.map(|health| Arc::clone(&health.tail));
        let uncertain = health.map(|health| Arc::clone(&health.uncertain));
        Self {
            pool,
            prepared_wake: None,
            state: Some(state),
            ready: None,
            pending: Some(job),
            waiter: None,
            sequencer,
            uncertain,
            predecessor: None,
            completion: None,
        }
    }

    pub(crate) fn ready(result: io::Result<T>) -> Self {
        Self {
            pool: &NATIVE_WORKERS,
            prepared_wake: None,
            state: None,
            ready: Some(result),
            pending: None,
            waiter: None,
            sequencer: None,
            uncertain: None,
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
        let meter = measured_completion_meter(self.state.as_ref().expect("pending native completion"))?;
        let wake = self.prepared_wake.take();
        wait_measured_completion(self, meter, wake, deadline, cancellation, on_deadline)
    }
}

pub(crate) fn reserve_if_needed(
    sequencer: &mut Option<FileSequencer>,
    predecessor: &mut Option<Arc<OperationFence>>,
    completion: &mut Option<Arc<OperationFence>>,
    pending: &mut Option<NativeJob>,
) -> io::Result<()> {
    let Some(sequencer) = sequencer.take() else {
        return Ok(());
    };
    let mut tail = sequencer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    *predecessor = tail.clone();
    let allocation = match pending.as_ref() {
        Some(NativeJob::Task { meter: Some(meter), .. } | NativeJob::Unit { meter: Some(meter), .. }) => {
            Some(ReadAllocation::new(meter, std::mem::size_of::<OperationFence>())?)
        }
        _ => None,
    };
    let fence = Arc::new(OperationFence {
        sequencer: Some(Arc::downgrade(&sequencer)),
        _allocation: allocation,
        ..OperationFence::default()
    });
    if let Some(job) = pending {
        job.set_completion(Arc::clone(&fence));
    }
    *tail = Some(Arc::clone(&fence));
    *completion = Some(fence);
    Ok(())
}

impl<T> Future for NativeCompletion<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(result) = this.ready.take() {
            return Poll::Ready(result);
        }
        if let Err(error) =
            reserve_if_needed(&mut this.sequencer, &mut this.predecessor, &mut this.completion, &mut this.pending)
        {
            this.pending = None;
            return Poll::Ready(Err(error));
        }
        if let Some(predecessor) = &this.predecessor {
            if !predecessor.ready_or_register(context.waker()) {
                if let Some(NativeJob::Read { span, .. } | NativeJob::Unit { span, .. }) = this.pending.as_ref() {
                    span.in_scope(|| {
                        tracing::trace!(name: "acyclic.runtime.fence_wait", "waiting for an earlier operation");
                    });
                }
                return Poll::Pending;
            }
            this.predecessor = None;
        }
        if this.uncertain.as_ref().is_some_and(|uncertain| uncertain.load(Ordering::Acquire)) {
            let result: io::Result<T> = Err(io::Error::other("prior native I/O completion on this file is uncertain"));
            if let Some(NativeJob::Read { span, .. } | NativeJob::Unit { span, .. }) = this.pending.as_ref() {
                span.record_error(result.as_ref().err());
            }
            this.pending = None;
            if let Some(completion) = &this.completion {
                completion.complete();
            }
            return Poll::Ready(result);
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
            let _ = job.run_in_place();
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
        let mut state = this
            .state
            .as_ref()
            .expect("pending native completion")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.task_pending
            && let Some(payload) = state.panic.take()
        {
            let waker = state.waker.take();
            drop(state);
            drop(waker);
            drop(this.pending.take());
            if let Some(waiter) = this.waiter.take()
                && let Ok(workers) = workers(this.pool, true)
            {
                remove_capacity_waiter(workers, Some(waiter));
            }
            drop(this.sequencer.take());
            drop(this.uncertain.take());
            drop(this.predecessor.take());
            drop(this.completion.take());
            std::panic::resume_unwind(payload);
        }
        if !state.task_pending
            && let Some(result) = state.result.take()
        {
            return Poll::Ready(result);
        }
        if !state.waker.as_ref().is_some_and(|waker| waker.will_wake(context.waker())) {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl<T> Drop for NativeCompletion<T> {
    fn drop(&mut self) {
        if self.pending.is_some()
            && let Some(completion) = &self.completion
        {
            completion.complete_after(self.predecessor.take());
        }
        if let Some(state) = &self.state {
            state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).waker = None;
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
