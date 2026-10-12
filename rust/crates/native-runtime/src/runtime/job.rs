use super::*;

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
    Unit {
        file: Arc<NativeFilePayload>,
        operation: NativeUnitOperation,
        meter: Option<Arc<dyn NativeReadMeter>>,
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

pub(crate) enum NativeUnitOperation {
    Write(Vec<OwnedWrite>),
    Control {
        operation: Box<dyn FnOnce(&File, Option<&dyn NativeReadMeter>) -> io::Result<()> + Send>,
        _allocation: Option<ReadAllocation>,
    },
}

impl NativeJob {
    pub(crate) fn set_completion(&mut self, fence: Arc<OperationFence>) {
        let (Self::Read { completion, .. } | Self::Unit { completion, .. } | Self::Task { completion, .. }) = self;
        *completion = Some(fence);
    }

    pub(crate) fn meter(&self) -> Option<&Arc<dyn NativeReadMeter>> {
        match self {
            Self::Unit { meter, .. } | Self::Task { meter, .. } => meter.as_ref(),
            Self::Read { .. } => None,
        }
    }

    /// Whether this job may run on a thread that admits inline blocking: a positional read or write on an overlapped Windows handle, which the thread can issue and wait for itself without the driver.
    #[cfg(windows)]
    pub(crate) const fn runs_in_place(&self) -> bool {
        matches!(
            self,
            Self::Read { overlapped: true, .. }
                | Self::Task { health: Some(_), .. }
                | Self::Unit { meter: Some(_), .. }
                | Self::Unit { overlapped: true, operation: NativeUnitOperation::Write(_), .. }
        )
    }

    /// Runs a job [`Self::runs_in_place`] admits on the calling thread, completing its fence exactly as the driver's completion would.
    #[cfg(windows)]
    pub(crate) fn run_in_place(self) -> Option<Waker> {
        match self {
            Self::Read { file, reads, span, overlapped: true, state, completion, health } => span.in_scope(|| {
                FileCompletion::new(state, completion, health.clone(), Some(Arc::clone(&file)), span.clone())
                    .finish(windows::read_batch_in_place(&file, &reads))
            }),
            Self::Unit {
                file,
                operation: NativeUnitOperation::Write(writes),
                meter: None,
                span,
                overlapped: true,
                state,
                completion,
                health,
            } => span.in_scope(|| {
                let finish =
                    FileCompletion::new(state, completion, health.clone(), Some(Arc::clone(&file)), span.clone());
                let result = validate_write_batch(&writes).and_then(|()| windows::write_batch_in_place(&file, &writes));
                finish.finish(result)
            }),
            job => job.run(),
        }
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
                let finish =
                    FileCompletion::new(state, completion, health.clone(), Some(Arc::clone(&file)), span.clone());
                #[cfg(target_os = "linux")]
                return submit_file_io(finish, |callback| linux::submit_read(file, reads, callback));
                #[cfg(windows)]
                return submit_file_io(finish, |callback| windows::submit_read(file, overlapped, reads, callback));
                #[cfg(not(any(windows, target_os = "linux")))]
                {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read_batch_impl(&file, &reads)))
                            .unwrap_or_else(|_| std::process::abort());
                    finish.finish(result)
                }
            }),
            Self::Unit {
                file,
                operation,
                meter,
                span,
                #[cfg(windows)]
                overlapped,
                state,
                completion,
                health,
            } => span.in_scope(|| {
                let finish =
                    FileCompletion::new(state, completion, health.clone(), Some(Arc::clone(&file)), span.clone());
                #[cfg(any(windows, target_os = "linux"))]
                let operation = match operation {
                    NativeUnitOperation::Write(writes) => {
                        if let Err(error) = validate_write_batch(&writes) {
                            return finish.finish(Err(error));
                        }
                        #[cfg(target_os = "linux")]
                        return submit_file_io(finish, |callback| linux::submit_write(file, writes, callback));
                        #[cfg(windows)]
                        return submit_file_io(finish, |callback| {
                            windows::submit_write(file, overlapped, writes, callback)
                        });
                    }
                    operation => operation,
                };
                with_file_scope(health.as_ref().map(|health| &health.tail), || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if let Some(meter) = &meter {
                            meter.check()?;
                        }
                        match operation {
                            #[cfg(not(any(windows, target_os = "linux")))]
                            NativeUnitOperation::Write(writes) => write_all_batch_owned(&file, writes),
                            #[cfg(any(windows, target_os = "linux"))]
                            NativeUnitOperation::Write(_) => {
                                unreachable!("native writes are dispatched before bounded controls")
                            }
                            NativeUnitOperation::Control { operation, _allocation } => {
                                operation(&file, meter.as_deref())
                            }
                        }
                    }))
                    .unwrap_or_else(|_| std::process::abort());
                    drop(file);
                    finish.finish(result)
                })
            }),
            Self::Task { operation, meter, terminal, _allocation, health, completion } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
                drop(_allocation);
                drop(meter);
                drop(health);
                if let Some(completion) = completion {
                    completion.complete();
                }
                terminal.publish(result.err())
            }
        }
    }
}
