use super::*;

impl NativeDemandSource {
    async fn request<W: FsWork + Send, T: Send + 'static>(
        &self,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
        job: impl FnOnce(Self, &NamespacePath, &mut CallbackWork) -> Result<T, DemandError>
        + Send
        + 'static,
    ) -> W::Output<T, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let mut context = match accounting.native_work() {
                Some(original) => CallbackWork::Original(original.clone()),
                None => CallbackWork::Ordinary(DemandWork::new(accounting.remaining().map_err(DemandError::from)?, cancellation)),
            };
            let cancellation = cancellation.clone();
            let (path, allocation) = {
                context.check()?;
                let bytes = path.retained_bytes();
                let copying = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
                context.admit(copying)?;
                let allocation = context.claim(bytes)?;
                let path = path.clone();
                context.record(copying)?;
                (path, allocation)
            };
            let permit = tokio::select! {
                acquired = self.inner.requests.clone().acquire_owned() => acquired.map_err(|_| DemandError::WorkerUnavailable)?,
                () = cancellation.cancelled() => { drop(path); context.release(allocation)?; return Err(DemandError::Cancelled); }
            };
            let source = self.clone();
            let stop = CancelWorkerOnDrop(match &mut context {
                CallbackWork::Original(original) => {
                    let (invocation, stop) = original.invocation_view().map_err(crate::native_name::original_name_error)?;
                    *original = invocation;
                    stop
                }
                CallbackWork::Ordinary(work) => {
                    let stop = CancellationToken::new();
                    work.cancellation = stop.clone();
                    stop
                }
            });
            let worker_stop = stop.0.clone();
            let mut worker = tokio::task::spawn_blocking(crate::obs::in_context(crate::obs::caller_context(), move || {
                let _permit = permit;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    worker_stop.check().map_err(|_| DemandError::Cancelled)?;
                    job(source, &path, &mut context)
                })).unwrap_or_else(|_| {
                    context.with_original_leaf(|work| work.poison_native_original_work());
                    Err(DemandError::OriginalWorkPoisoned)
                });
                drop(path);
                let released = context.release(allocation);
                (result.and_then(|value| released.map(|()| value)), context)
            }));
            let answer = tokio::select! {
                answer = &mut worker => answer,
                () = cancellation.cancelled() => { stop.0.cancel(); worker.await }
            }.map_err(|_| DemandError::WorkerUnavailable)?;
            finish_context(answer.0, answer.1, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }
}
