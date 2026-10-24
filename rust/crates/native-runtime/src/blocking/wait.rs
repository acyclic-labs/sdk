use crate::*;

type BlockingWaitWake = crate::deadline::Waiter;

/// The waiter charge belongs to the waiting caller, independently of retained
/// notifier clones on the physical worker.
pub(crate) struct PreparedWake {
    wake: Arc<BlockingWaitWake>,
    _allocation: ReadAllocation,
}

pub(crate) fn allocate_blocking_wait_wake(
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<PreparedWake> {
    let allocation = ReadAllocation::new(meter, std::mem::size_of::<BlockingWaitWake>())?;
    Ok(PreparedWake {
        wake: Arc::new(BlockingWaitWake::new()),
        _allocation: allocation,
    })
}

pub(crate) fn measured_completion_meter<T>(
    state: &CompletionMutex<CompletionState<T>>,
) -> io::Result<Arc<dyn NativeReadMeter>> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        ._allocation
        .as_ref()
        .map(|allocation| Arc::clone(allocation.meter()))
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))
}

pub(crate) fn wait_measured_completion<T>(
    task: impl Future<Output = io::Result<T>>,
    meter: &Arc<dyn NativeReadMeter>,
    prepared_wake: Option<PreparedWake>,
    deadline: Option<std::time::Instant>,
    cancellation: impl Future<Output = ()>,
    on_deadline: impl FnOnce(),
) -> io::Result<T> {
    let prepared = match prepared_wake {
        Some(prepared) => prepared,
        None => allocate_blocking_wait_wake(meter)?,
    };
    let wake = &prepared.wake;
    let waker = Waker::from(Arc::clone(wake));
    let mut context = Context::from_waker(&waker);
    let mut task = std::pin::pin!(task);
    let mut cancellation = std::pin::pin!(cancellation);
    let mut deadline = deadline.map(crate::deadline::Deadline::at);
    let mut cancelled = false;
    let mut on_deadline = Some(on_deadline);
    loop {
        if !cancelled && cancellation.as_mut().poll(&mut context).is_ready() {
            cancelled = true;
            deadline = None;
        }
        if deadline.is_some_and(crate::deadline::Deadline::expired) {
            deadline = None;
            if let Some(on_deadline) = on_deadline.take() {
                on_deadline();
            }
        }
        match task.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => wake.wait(deadline),
        }
    }
}
