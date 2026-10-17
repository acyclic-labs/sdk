use super::*;

/// A borrowed chunk from one captured process pipe.
#[derive(Clone, Copy, Debug)]
pub enum ProcessOutput<'a> {
    /// Bytes newly read from standard output.
    Stdout(&'a [u8]),
    /// Bytes newly read from standard error.
    Stderr(&'a [u8]),
}

pub(super) fn observe_output(
    chunk: ProcessOutput<'_>,
    observe: &mut impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
) -> io::Result<()> {
    let action = observe(chunk)?;
    if !action.bytes.is_empty() || action.complete {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process input requires an active exchange",
        ));
    }
    Ok(())
}

/// A stopped capture, retaining its bounded output and observed cleanup state.
#[derive(Debug)]
pub struct ProcessCaptureFailure {
    /// Capture error, or the cleanup error when cleanup could not be confirmed.
    pub error: io::Error,
    /// Direct-child exit status if it was observed before capture stopped.
    pub status: Option<ExitStatus>,
    /// Bounded stdout prefix collected before capture stopped.
    pub stdout: Vec<u8>,
    /// Bounded stderr prefix collected before capture stopped.
    pub stderr: Vec<u8>,
    /// Whether containment cleanup succeeded and the direct child was reaped.
    pub cleanup_completed: bool,
}

/// Transient protocol action within the existing process capture owner.
#[derive(Default)]
pub struct ProcessInput {
    /// Exact additional bytes, ordered after all previously queued input.
    pub bytes: Vec<u8>,
    /// After queued input is written, close stdin and terminate containment.
    pub complete: bool,
}

/// Input supplied to the bounded process-capture loop.
#[derive(Clone, Copy)]
pub enum CaptureInput<'a> {
    /// A streaming exchange with bounded queued input.
    Exchange {
        /// Bytes written before observing output.
        initial: &'a [u8],
        /// Maximum queued input bytes.
        maximum: usize,
    },
    /// Exact input bytes followed by closing stdin.
    Literal(&'a [u8]),
}

pub(super) struct PendingStdin<'a> {
    bytes: std::borrow::Cow<'a, [u8]>,
    written: usize,
    maximum: usize,
    pub(super) complete: bool,
    stopped: bool,
    terminate_on_completion: bool,
}

impl<'a> PendingStdin<'a> {
    /// Measured exchange storage was claimed and allocated by the caller.
    pub(super) fn new_with_buffer(
        input: CaptureInput<'a>,
        buffer: Option<Vec<u8>>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        let (initial, maximum, terminate_on_completion) = match input {
            CaptureInput::Exchange { initial, maximum } => (initial, maximum, true),
            CaptureInput::Literal(initial) => (initial, initial.len(), false),
        };
        if (terminate_on_completion && maximum == 0) || initial.len() > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid process input allowance",
            ));
        }
        let bytes = if terminate_on_completion && meter.is_some() {
            let mut buffer = buffer.ok_or(io::ErrorKind::OutOfMemory)?;
            if !buffer.is_empty() || buffer.capacity() < maximum {
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            if let Some(meter) = meter {
                meter.admit_examination(0, initial.len() as u64)?;
            }
            buffer.extend_from_slice(initial);
            if let Some(meter) = meter {
                meter.record_examination(0, initial.len() as u64)?;
            }
            std::borrow::Cow::Owned(buffer)
        } else {
            std::borrow::Cow::Borrowed(initial)
        };
        Ok(Self {
            bytes,
            written: 0,
            maximum,
            complete: !terminate_on_completion,
            stopped: false,
            terminate_on_completion,
        })
    }

    pub(super) fn poll(
        &mut self,
        tree: &mut ProcessTree,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        let mut progressed = false;
        if self.written < self.bytes.len() {
            let suffix = self
                .bytes
                .get(self.written..)
                .ok_or_else(|| io::Error::other("invalid queued process input"))?;
            let mut reservation = meter
                .map(|meter| {
                    crate::NativeByteReservation::new(
                        meter,
                        crate::NativeOperationKind::Write,
                        suffix.len(),
                    )
                })
                .transpose()?;
            let pipe = tree
                .child
                .as_mut()
                .and_then(|child| child.stdin.as_mut())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "no owned process stdin")
                })?;
            let transfer = platform::write_pipe(pipe, suffix, meter);
            let actual = transfer
                .as_ref()
                .ok()
                .and_then(|(written, _)| *written)
                .unwrap_or(0);
            let recorded_bytes = reservation
                .as_mut()
                .map_or(Ok(()), |token| token.record(actual));
            if actual > suffix.len() {
                return Err(io::Error::other("invalid process write length"));
            }
            self.written += actual;
            progressed |= actual != 0;
            recorded_bytes?;
            let (written, recorded_operation) = transfer?;
            recorded_operation?;
            if written == Some(0) {
                if self.terminate_on_completion {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                self.written = self.bytes.len();
                self.complete = true;
            }
            if let Some(meter) = meter {
                meter.check()?;
            }
        }
        if self.complete && self.written == self.bytes.len() && !self.stopped {
            tree.capture_close_stdin(meter)?;
            if self.terminate_on_completion {
                tree.capture_terminate_descendants(meter)?;
            }
            self.stopped = true;
            progressed = true;
        }
        Ok(progressed)
    }

    pub(super) fn observe(
        &mut self,
        chunk: &[u8],
        observe: &mut impl FnMut(&[u8]) -> io::Result<ProcessInput>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        let action = observe(chunk)?;
        let total = self
            .bytes
            .len()
            .checked_add(action.bytes.len())
            .filter(|total| *total <= self.maximum)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "process input limit exceeded; effects may have occurred",
                )
            })?;
        if !action.bytes.is_empty() {
            if let Some(meter) = meter {
                let std::borrow::Cow::Owned(bytes) = &mut self.bytes else {
                    return Err(io::ErrorKind::OutOfMemory.into());
                };
                if bytes.capacity() < total {
                    return Err(io::ErrorKind::OutOfMemory.into());
                }
                meter.admit_examination(0, action.bytes.len() as u64)?;
                bytes.extend_from_slice(&action.bytes);
                meter.record_examination(0, action.bytes.len() as u64)?;
            } else {
                let bytes = self.bytes.to_mut();
                bytes
                    .try_reserve(total - bytes.len())
                    .map_err(io::Error::other)?;
                bytes.extend_from_slice(&action.bytes);
            }
        }
        self.complete = action.complete;
        Ok(())
    }
}

impl ProcessCaptureFailure {
    pub(super) fn before_capture(error: io::Error) -> Self {
        Self {
            error,
            status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            cleanup_completed: false,
        }
    }
}

impl std::fmt::Display for ProcessCaptureFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ProcessCaptureFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(super) fn drain_pipe<T: Read + platform::Pipe>(
    pipe: &mut Option<T>,
    output: &mut Vec<u8>,
    remaining: &mut usize,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<bool> {
    let Some(reader) = pipe.as_mut() else {
        return Ok(false);
    };
    let maximum = (*remaining).clamp(1, 64 * 1024);
    let admitted = maximum.min(*remaining);
    // Reserve storage before consuming the pipe. The measured path must never
    // acquire capacity from another live Root claim or allocate during capture.
    if meter.is_some() {
        if output.capacity().saturating_sub(output.len()) < admitted {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
    } else {
        output.try_reserve(admitted).map_err(io::Error::other)?;
    }
    let mut reservation = meter
        .map(|meter| {
            crate::NativeByteReservation::new(meter, crate::NativeOperationKind::Read, maximum)
        })
        .transpose()?;
    let before = output.len();
    let mut probe = [std::mem::MaybeUninit::<u8>::uninit(); 1];
    let buffer = if admitted == 0 {
        &mut probe[..]
    } else {
        output
            .spare_capacity_mut()
            .get_mut(..admitted)
            .ok_or_else(|| io::Error::from(io::ErrorKind::Other))?
    };
    let transfer = platform::read_pipe(reader, buffer, meter);
    let actual = transfer
        .as_ref()
        .ok()
        .and_then(|(read, _)| *read)
        .unwrap_or(0);
    let recorded_bytes = reservation
        .as_mut()
        .map_or(Ok(()), |token| token.record(actual));
    if actual > maximum {
        return Err(io::Error::other("pipe returned an invalid length"));
    }
    let retained = actual.min(admitted);
    // SAFETY: the synchronous pipe operation initialized exactly `actual`
    // bytes in the exclusively borrowed spare capacity. Publish that prefix
    // before any late accounting or cancellation error can leave capture.
    unsafe {
        output.set_len(before + retained);
    }
    *remaining -= retained;
    recorded_bytes?;
    let (read, recorded_operation) = transfer?;
    recorded_operation?;
    match read {
        Some(0) => {
            let (closed, recorded) =
                pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
                    drop(pipe.take());
                    Ok(())
                })?;
            closed?;
            recorded?;
        }
        Some(read) => {
            tracing::trace!(name: "acyclic.runtime.output_chunk", bytes = read);
            if retained != read {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "process output limit exceeded; effects may have occurred",
                ));
            }
        }
        None => return Ok(false),
    }
    if let Some(meter) = meter {
        meter.check()?;
    }
    Ok(true)
}

/// One pipe SDK call owns its own credit.
pub(super) fn pipe_operation_attempt<T>(
    meter: Option<&dyn crate::NativeReadMeter>,
    kind: crate::NativeOperationKind,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<(io::Result<T>, io::Result<()>)> {
    match meter {
        Some(meter) => {
            let mut token = crate::NativeOperationReservation::new(meter, kind, 1)?;
            Ok(token.attempt(operation))
        }
        None => Ok((operation(), Ok(()))),
    }
}
