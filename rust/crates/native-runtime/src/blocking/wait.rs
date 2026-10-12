use crate::*;

pub(crate) struct BlockingWaitWake {
    pub(crate) notified: Mutex<bool>,
    pub(crate) changed: Condvar,
    pub(crate) _allocation: ReadAllocation,
}

impl BlockingWaitWake {
    pub(crate) fn notify(&self) {
        *self
            .notified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.changed.notify_one();
    }

    pub(crate) fn wait(&self, deadline: Option<std::time::Instant>) {
        let mut notified = self
            .notified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*notified {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let (next, timeout) = self
                    .changed
                    .wait_timeout(notified, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                notified = next;
                if timeout.timed_out() {
                    break;
                }
            } else {
                notified = self
                    .changed
                    .wait(notified)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        *notified = false;
    }
}

impl std::task::Wake for BlockingWaitWake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

pub(crate) fn allocate_blocking_wait_wake(
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<Arc<BlockingWaitWake>> {
    let allocation = ReadAllocation::new(meter, std::mem::size_of::<BlockingWaitWake>())?;
    Ok(Arc::new(BlockingWaitWake {
        notified: Mutex::new(false),
        changed: Condvar::new(),
        _allocation: allocation,
    }))
}

pub(crate) fn measured_completion_meter<T>(
    state: &Mutex<CompletionState<T>>,
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
    meter: Arc<dyn NativeReadMeter>,
    prepared_wake: Option<Arc<BlockingWaitWake>>,
    mut deadline: Option<std::time::Instant>,
    cancellation: impl Future<Output = ()>,
    on_deadline: impl FnOnce(),
) -> io::Result<T> {
    let wake = match prepared_wake {
        Some(wake) => wake,
        None => allocate_blocking_wait_wake(&meter)?,
    };
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut task = std::pin::pin!(task);
    let mut cancellation = std::pin::pin!(cancellation);
    let mut cancelled = false;
    let mut on_deadline = Some(on_deadline);
    loop {
        if !cancelled && cancellation.as_mut().poll(&mut context).is_ready() {
            cancelled = true;
            deadline = None;
        }
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
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
