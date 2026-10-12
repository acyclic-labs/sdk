use crate::*;
pub(crate) use wait::{BlockingWaitWake, allocate_blocking_wait_wake, measured_completion_meter, wait_measured_completion};

mod wait;

thread_local! {
    static INLINE_BLOCKING: Cell<bool> = const { Cell::new(false) };
}

/// Allows this thread to execute its own blocking operations inline.
pub struct InlineBlocking {
    pub(crate) previous: bool,
}

impl InlineBlocking {
    /// Admits inline host operations on this thread until the guard drops.
    #[must_use]
    pub fn enter() -> Self {
        Self {
            previous: INLINE_BLOCKING.replace(true),
        }
    }
}

impl Drop for InlineBlocking {
    fn drop(&mut self) {
        INLINE_BLOCKING.set(self.previous);
    }
}

#[must_use]
pub(crate) fn inline_blocking_allowed() -> bool {
    INLINE_BLOCKING.get()
}

/// Creates a host task; metered tasks may preallocate their admission and wait controls.
pub fn run_blocking_io<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Option<(Arc<dyn NativeReadMeter>, bool)>,
) -> io::Result<NativeCompletion<T>> {
    let (meter, allocation_meter) = match meter {
        Some((meter, true)) => {
            workers(&BLOCKING_WORKERS, true)?;
            let owner = meter.allocation_owner().unwrap_or_else(|| Arc::clone(&meter));
            (Some(meter), Some(owner))
        }
        Some((meter, false)) => (Some(meter), None),
        None => (None, None),
    };
    let mut task = task_completion(
        move || Ok(operation()), meter, allocation_meter.as_ref(), None, &BLOCKING_WORKERS,
    )?;
    if let Some(meter) = allocation_meter {
        task.waiter = Some(allocate_capacity_waiter(Some(&meter))?);
        task.prepared_wake = Some(allocate_blocking_wait_wake(&meter)?);
    }
    Ok(task)
}

pub(crate) fn task_completion<T: Send + 'static>(
    operation: impl FnOnce() -> io::Result<T> + Send + 'static,
    meter: Option<Arc<dyn NativeReadMeter>>,
    allocation_meter: Option<&Arc<dyn NativeReadMeter>>,
    health: Option<Arc<FileHealth>>,
    pool: &'static LazyLock<io::Result<NativeWorkers>>,
) -> io::Result<NativeCompletion<T>> {
    let allocation_meter = allocation_meter.or(meter.as_ref());
    if let Some(meter) = &meter { meter.check()?; }
    let allocation = allocation_meter.map(|meter| ReadAllocation::new(
        meter, std::mem::size_of::<Mutex<CompletionState<T>>>(),
    )).transpose()?;
    let state = Arc::new(Mutex::new(CompletionState {
        task_pending: true,
        result: None, panic: None, waker: None, _allocation: allocation,
    }));
    let completion = Arc::clone(&state);
    let span = if std::ptr::eq(pool, &BLOCKING_WORKERS) {
        obs::span!(DEBUG, "acyclic.runtime.blocking_io", outcome = obs::Empty, error.kind = obs::Empty)
    } else { obs::OperationSpan::new(tracing::Span::none()) };
    let observer = meter.clone();
    let operation = move || span.in_scope(|| {
        let admission = observer.as_ref().map_or(Ok(()), |meter| meter.check());
        let result = match admission {
            Ok(()) => operation(),
            Err(error) => Err(error),
        };
        span.record_error(result.as_ref().err());
        completion.lock().unwrap_or_else(std::sync::PoisonError::into_inner).result = Some(result);
    });
    let allocation = allocation_meter.map(|meter| ReadAllocation::new(
        meter, std::mem::size_of_val(&operation),
    )).transpose()?;
    Ok(NativeCompletion::pending(NativeJob::Task {
            operation: Box::new(operation), meter, terminal: Arc::clone(&state) as Arc<dyn TaskTerminal>,
            _allocation: allocation, health, completion: None,
        }, state, pool))
}
