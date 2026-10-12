use crate::*;

pub(crate) enum NativeJob {
    Read {
        file: Arc<NativeFilePayload>,
        reads: Vec<OwnedRead>,
        span: obs::OperationSpan,
        #[cfg(windows)]
        overlapped: bool,
        state: Arc<Mutex<CompletionState<Vec<Bytes>>>>,
        completion: Option<Arc<OperationFence>>,
        health: Option<Arc<FileHealth>>,
    },
    Write {
        file: Arc<NativeFilePayload>,
        writes: Vec<OwnedWrite>,
        span: obs::OperationSpan,
        #[cfg(windows)]
        overlapped: bool,
        state: Arc<Mutex<CompletionState<()>>>,
        completion: Option<Arc<OperationFence>>,
        health: Option<Arc<FileHealth>>,
    },
    Task {
        operation: Box<dyn FnOnce() + Send>,
        meter: Option<Arc<dyn NativeReadMeter>>,
        terminal: Arc<dyn TaskTerminal>,
        _allocation: Option<ReadAllocation>,
        health: Option<Arc<FileHealth>>,
        completion: Option<Arc<OperationFence>>,
    },
}

impl NativeJob {
    pub(crate) fn meter(&self) -> Option<&Arc<dyn NativeReadMeter>> {
        match self {
            Self::Task { meter, .. } => meter.as_ref(),
            _ => None,
        }
    }

    #[cfg(windows)]
    pub(crate) const fn runs_in_place(&self) -> bool {
        matches!(
            self,
            Self::Read {
                overlapped: true,
                ..
            } | Self::Write {
                overlapped: true,
                ..
            } | Self::Task {
                health: Some(_),
                ..
            }
        )
    }

    pub(crate) fn run(self) -> Option<Waker> {
        match self {
            Self::Read {
                file,
                reads,
                span,
                #[cfg(windows)]
                overlapped,
                state,
                completion,
                health,
            } => span.in_scope(|| {
                let finish = FileCompletion::new(
                    state,
                    completion,
                    health,
                    Some(Arc::clone(&file)),
                    span.clone(),
                );
                #[cfg(windows)]
                if overlapped && inline_blocking_allowed() {
                    return finish.finish(windows::read_batch_in_place(&file, &reads));
                }
                #[cfg(target_os = "linux")]
                return submit_file_io(finish, |callback| {
                    linux::submit_read(file, reads, callback)
                });
                #[cfg(windows)]
                return submit_file_io(finish, |callback| {
                    windows::submit_read(file, overlapped, reads, callback)
                });
                #[cfg(not(any(windows, target_os = "linux")))]
                finish.finish(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        read_batch_impl(&file, &reads)
                    }))
                    .unwrap_or_else(|_| std::process::abort()),
                )
            }),
            Self::Write {
                file,
                writes,
                span,
                #[cfg(windows)]
                overlapped,
                state,
                completion,
                health,
            } => span.in_scope(|| {
                with_file_scope(health.as_ref(), || {
                    let finish = FileCompletion::new(
                        state,
                        completion,
                        health.clone(),
                        Some(Arc::clone(&file)),
                        span.clone(),
                    );
                    if let Err(error) = validate_write_batch(&writes) {
                        return finish.finish(Err(error));
                    }
                    #[cfg(windows)]
                    if overlapped && inline_blocking_allowed() {
                        return finish.finish(windows::write_batch_in_place(&file, &writes));
                    }
                    #[cfg(target_os = "linux")]
                    return submit_file_io(finish, |callback| {
                        linux::submit_write(file, writes, callback)
                    });
                    #[cfg(windows)]
                    return submit_file_io(finish, |callback| {
                        windows::submit_write(file, overlapped, writes, callback)
                    });
                    #[cfg(not(any(windows, target_os = "linux")))]
                    finish.finish(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            write_all_batch_owned(&file, writes)
                        }))
                        .unwrap_or_else(|_| std::process::abort()),
                    )
                })
            }),
            Self::Task {
                operation,
                meter,
                terminal,
                _allocation,
                health,
                completion,
            } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
                drop(_allocation);
                drop(meter);
                drop(health);
                if let Err(panic) = result {
                    terminal.stage_panic(panic);
                }
                if let Some(completion) = completion {
                    completion.complete();
                }
                terminal.publish()
            }
        }
    }
}
