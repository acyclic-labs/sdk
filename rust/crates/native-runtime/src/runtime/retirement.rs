use super::*;

pub(crate) struct FileCompletion<T> {
    pub(crate) state: Arc<Mutex<CompletionState<T>>>,
    pub(crate) fence: Option<Arc<OperationFence>>,
    pub(crate) health: Option<Arc<FileHealth>>,
    pub(crate) file: Option<Arc<NativeFilePayload>>,
    pub(crate) span: obs::OperationSpan,
    pub(crate) finished: bool,
}

impl<T> FileCompletion<T> {
    pub(crate) fn new(
        state: Arc<Mutex<CompletionState<T>>>,
        fence: Option<Arc<OperationFence>>,
        health: Option<Arc<FileHealth>>,
        file: Option<Arc<NativeFilePayload>>,
        span: obs::OperationSpan,
    ) -> Self {
        Self { state, fence, health, file, span, finished: false }
    }

    pub(crate) fn finish(mut self, result: io::Result<T>) -> Option<Waker> {
        let Self { state, fence, health, file, span, finished, .. } = &mut self;
        span.in_scope(|| {
            span.record_error(result.as_ref().err());
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            if result.as_ref().err().is_some_and(is_uncertain_io_error)
                && let (Some(file), Some(health)) = (file.as_ref(), health.as_ref())
            {
                poison_file_health(file, &health.uncertain);
            }
            // Retire the handle after quarantine decisions and before publishing Ready.
            drop(file.take());
            let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.result = Some(result);
            let waker = state.waker.take();
            drop(state);
            if let Some(fence) = fence.take() {
                fence.complete();
            }
            *finished = true;
            waker
        })
    }
}

impl<T> Drop for FileCompletion<T> {
    fn drop(&mut self) {
        if !self.finished {
            std::process::abort();
        }
    }
}

#[cfg(any(windows, target_os = "linux"))]
pub(crate) fn submit_file_io<T: Send + 'static>(
    completion: FileCompletion<T>,
    submit: impl FnOnce(Box<dyn FnOnce(io::Result<T>) + Send>) -> io::Result<()>,
) -> Option<Waker> {
    // A platform's Err means it accepted no I/O. This cell leaves the common
    let handoff = Arc::new(Mutex::new(Some(completion)));
    let callback_handoff = Arc::clone(&handoff);
    let callback = Box::new(move |result| {
        let completion = callback_handoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| std::process::abort());
        wake_capacity_waiter(completion.finish(result));
    });
    match submit(callback) {
        Ok(()) => None,
        Err(error) => handoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| std::process::abort())
            .finish(Err(error)),
    }
}
