#![allow(
    unsafe_code,
    reason = "process containment and bounded pipes use platform OS calls"
)]

use crate::obs;
use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

/// One child and descendants that remain in its operating-system containment.
///
/// Windows uses a Job object. Unix uses a process group, which cannot contain
/// a descendant that deliberately creates a new process group or session.
///
/// Choosing measured control transfers cleanup responsibility to the
/// caller: a failed bounded attempt retains custody, and Drop never hides an
/// unmetered retry. Ordinary command owners keep their existing Drop cleanup.
pub struct ProcessTree {
    child: Option<Child>,
    guard: platform::Guard,
    output_taken: bool,
    #[cfg(unix)]
    native_child: Option<libc::pid_t>,
    cleanup_on_drop: bool,
    #[cfg(unix)]
    spawn_resources: platform::SpawnResources,
}

/// A stopped capture, retaining its bounded output and observed cleanup state.
/// Effects may have occurred regardless of whether cleanup completed.
/// Measured failures retain every physically captured prefix. Unclosed pipes
/// remain with the same ProcessTree when finite cleanup cannot be admitted.
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
    /// Unix confirms group signal delivery, not reaping of every descendant.
    pub cleanup_completed: bool,
}

/// Transient protocol action within the existing process capture owner.
/// It is not an admission, receipt, scheduler or recovery instruction.
#[derive(Default)]
pub struct ProcessInput {
    /// Exact additional bytes, ordered after all previously queued input.
    pub bytes: Vec<u8>,
    /// After queued input is written, close stdin and terminate containment.
    /// Captured output is still drained within the original output/deadline bounds.
    pub complete: bool,
}

enum CaptureInput<'a> {
    Exchange { initial: &'a [u8], maximum: usize },
    Literal(&'a [u8]),
}

struct PendingStdin<'a> {
    bytes: std::borrow::Cow<'a, [u8]>,
    written: usize,
    maximum: usize,
    complete: bool,
    stopped: bool,
    terminate_on_completion: bool,
}

impl<'a> PendingStdin<'a> {
    /// Measured exchange storage was claimed and allocated by the caller.
    /// Literal input remains borrowed: it does not acquire a second claim.
    fn new_with_buffer(
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
            if let Some(meter) = meter { meter.admit_examination(0, initial.len() as u64)?; }
            buffer.extend_from_slice(initial);
            if let Some(meter) = meter { meter.record_examination(0, initial.len() as u64)?; }
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

    fn poll(
        &mut self, tree: &mut ProcessTree, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        let mut progressed = false;
        if self.written < self.bytes.len() {
            let suffix = self.bytes.get(self.written..)
                .ok_or_else(|| io::Error::other("invalid queued process input"))?;
            let mut reservation = meter.map(|meter| crate::NativeByteReservation::new(
                meter, crate::NativeOperationKind::Write, suffix.len(),
            )).transpose()?;
            let pipe = tree.child.as_mut().and_then(|child| child.stdin.as_mut())
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "no owned process stdin"))?;
            let transfer = platform::write_pipe_with_meter(pipe, suffix, meter);
            let actual = transfer.as_ref().ok().and_then(|(written, _)| *written).unwrap_or(0);
            let recorded_bytes = reservation.as_mut().map_or(Ok(()), |token| token.record(actual));
            if actual > suffix.len() { return Err(io::Error::other("invalid process write length")); }
            self.written += actual;
            progressed |= actual != 0;
            recorded_bytes?;
            let (_, recorded_operation) = transfer?;
            recorded_operation?;
            if let Some(meter) = meter { meter.check()?; }
        }
        if self.complete && self.written == self.bytes.len() && !self.stopped {
            tree.capture_close_stdin(meter)?;
            if self.terminate_on_completion { tree.capture_terminate_descendants(meter)?; }
            self.stopped = true;
            progressed = true;
        }
        Ok(progressed)
    }

    fn observe(
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
                if bytes.capacity() < total { return Err(io::ErrorKind::OutOfMemory.into()); }
                meter.admit_examination(0, action.bytes.len() as u64)?;
                bytes.extend_from_slice(&action.bytes);
                meter.record_examination(0, action.bytes.len() as u64)?;
            } else {
                let bytes = self.bytes.to_mut();
                bytes.try_reserve(total - bytes.len()).map_err(io::Error::other)?;
                bytes.extend_from_slice(&action.bytes);
            }
        }
        self.complete = action.complete;
        Ok(())
    }
}

impl ProcessCaptureFailure {
    fn before_capture(error: io::Error) -> Self {
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

impl ProcessTree {
    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        let mut custody = None;
        if let Err(admission) = Self::spawn_controlled(command, None, &mut custody) {
            if let Some(tree) = custody.as_mut() { tree.terminate()?; }
            return Err(admission);
        }
        custody.ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    /// Uses the original meter and the same spawn engine as ordinary commands.
    /// Any native owner acquired before an error remains in `custody`.
    pub(crate) fn spawn_with_meter_custody(
        command: &mut Command, meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        Self::spawn_controlled(command, Some(meter), custody)
    }

    fn spawn_controlled(
        command: &mut Command, meter: Option<&dyn crate::NativeReadMeter>,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() { return Err(io::ErrorKind::AlreadyExists.into()); }
        if let Some(meter) = meter {
            // These borrowed Command payloads are already prepared and claimed
            // by the caller. This adapter allocates/copies no argument, env or
            // cwd buffer; its actual visits are accounted independently.
            meter.examine(1, 0)?;
            for _ in command.get_args() { meter.examine(1, 0)?; }
            for _ in command.get_envs() { meter.examine(1, 0)?; }
            if command.get_current_dir().is_some() { meter.examine(1, 0)?; }
        }
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.spawn",
            program = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        let program = std::path::Path::new(command.get_program()).file_name();
        operation.record("program", program.and_then(|name| name.to_str()));
        operation.scope(|| operation.finish(platform::spawn_controlled(command, meter, custody)))
    }

    /// Starts a closed-stream Unix child using borrowed, terminated arguments,
    /// and retains it in this same process-tree owner before spawn cleanup.
    /// `args` excludes argv[0]. The only Rust-owned spawn buffer is the admitted
    /// pointer vector; opaque SDK/kernel storage is not guessed heap or RSS.
    ///
    /// # Errors
    ///
    /// Any denied leaf or native failure retains the exact spent prefix. If
    /// spawn succeeded before a later cleanup error, `custody` still owns its
    /// unreaped leader and guard. The caller must retain or explicitly clean it.
    #[cfg(unix)]
    pub fn spawn_closed_measured(
        program: &std::ffi::CStr, args: &[&std::ffi::CStr],
        meter: &dyn crate::NativeReadMeter, custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() { return Err(io::ErrorKind::AlreadyExists.into()); }
        platform::spawn_closed(program, args, meter, custody)
    }

    /// Transfers the input pipe to a host-owned streaming protocol.
    pub fn take_stdin(&mut self) -> Option<std::process::ChildStdin> {
        self.child.as_mut()?.stdin.take()
    }

    /// Transfers stdout to a host-owned reader, which owns its output bounds.
    pub fn take_stdout(&mut self) -> Option<std::process::ChildStdout> {
        self.child.as_mut()?.stdout.take()
    }

    /// Transfers stderr to a host-owned reader, which owns its output bounds.
    pub fn take_stderr(&mut self) -> Option<std::process::ChildStderr> {
        self.child.as_mut()?.stderr.take()
    }

    /// Reads owned stdout without waiting for data. `None` means not ready;
    /// `Some(0)` means EOF or no owned pipe. The host owns framing and bounds.
    pub fn read_stdout(&mut self, buffer: &mut [u8]) -> io::Result<Option<usize>> {
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty process read buffer",
            ));
        }
        let Some(pipe) = self.child.as_mut().and_then(|child| child.stdout.as_mut()) else {
            return Ok(Some(0));
        };
        platform::read_pipe(pipe, buffer)
    }

    /// Writes to owned stdin without waiting for pipe capacity. `None` means
    /// backpressure; a positive count may be smaller than the supplied slice.
    /// The host must retain the unwritten suffix and own admission, total input
    /// bounds, deadlines, intent checks and mandatory tree cleanup.
    pub fn write_stdin(&mut self, bytes: &[u8]) -> io::Result<Option<usize>> {
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty process write buffer",
            ));
        }
        let pipe = self
            .child
            .as_mut()
            .and_then(|child| child.stdin.as_mut())
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "no owned process stdin"))?;
        platform::write_pipe(pipe, bytes)
    }

    /// Closes only the retained stdin pipe. Tree containment remains owned here.
    pub fn close_stdin(&mut self) {
        if let Some(child) = self.child.as_mut() {
            drop(child.stdin.take());
        }
    }

    /// Polls the direct child without releasing ownership of its descendants.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.capture_try_wait(None)
    }

    /// Observes the same unreaped owned leader, admitting every actual waitid
    /// attempt (including EINTR retries). No meter is retained after this call.
    /// The caller now owns explicit cleanup; Drop cannot substitute unbounded
    /// termination after this measured observation.
    pub fn try_wait_measured(
        &mut self, meter: &dyn crate::NativeReadMeter,
    ) -> io::Result<Option<ExitStatus>> {
        self.capture_try_wait(Some(meter))
    }

    #[cfg(unix)]
    fn owned_child_id(&self) -> io::Result<libc::id_t> {
        self.native_child.map(|pid| pid as libc::id_t)
            .or_else(|| self.child.as_ref().map(Child::id))
            .ok_or_else(|| io::ErrorKind::Other.into())
    }

    #[cfg(unix)]
    fn try_wait_with_meter(
        &mut self, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<ExitStatus>> {
        platform::observe_exit_id(self.owned_child_id()?, meter)
    }

    fn select_control_meter(&mut self, meter: Option<&dyn crate::NativeReadMeter>) {
        if meter.is_some() {
            self.cleanup_on_drop = false;
            #[cfg(windows)]
            { self.guard.cleanup_on_drop = false; }
        }
    }

    fn capture_try_wait(
        &mut self, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<ExitStatus>> {
        self.select_control_meter(meter);
        #[cfg(unix)]
        return self.try_wait_with_meter(meter);
        #[cfg(windows)]
        {
            let child = self.child.as_mut().ok_or(io::ErrorKind::NotFound)?;
            process_control_attempt(meter, false, || child.try_wait())
        }
    }

    fn capture_close_stdin(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        self.select_control_meter(meter);
        if self.child.as_ref().is_some_and(|child| child.stdin.is_some()) {
            process_control_attempt(meter, true, || { self.close_stdin(); Ok(()) })?;
        }
        Ok(())
    }

    /// Observes direct-child exit within `timeout`, retaining descendant ownership.
    /// On Unix the exited leader remains unreaped until containment cleanup.
    pub fn wait(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.wait",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| operation.finish(self.wait_with_meter(timeout, None)))
    }

    /// Observes the same child using admitted probes and actual sleep attempts.
    pub fn wait_measured(
        &mut self, timeout: Duration, meter: &dyn crate::NativeReadMeter,
    ) -> io::Result<ExitStatus> {
        self.wait_with_meter(timeout, Some(meter))
    }

    fn wait_with_meter(
        &mut self, timeout: Duration, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<ExitStatus> {
        self.select_control_meter(meter);
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            if meter.is_some() { io::ErrorKind::InvalidInput.into() }
            else { io::Error::new(io::ErrorKind::InvalidInput, "process deadline overflow") }
        })?;
        if self.is_reaped() {
            if meter.is_some() { return Err(io::ErrorKind::Other.into()); }
            return Err(io::Error::other("process output was already collected"));
        }
        self.capture_close_stdin(meter)?;
        loop {
            if let Some(status) = self.capture_try_wait(meter)? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                if meter.is_some() { return Err(io::ErrorKind::TimedOut.into()); }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "process exit deadline exceeded; termination may be unresolved",
                ));
            }
            process_control_attempt(meter, false, || {
                std::thread::sleep(Duration::from_millis(1));
                Ok(())
            })?;
        }
    }

    /// Collects at most `max_bytes` across both pipes, with a deadline measured
    /// from this call. Closes stdin and terminates containment after child exit,
    /// timeout, or capture failure. A timeout does not imply effects rolled back.
    /// Cleanup has its own five-second observation window per platform phase;
    /// OS calls and scheduling are assumed to progress. Cleanup errors take
    /// precedence over capture errors. No rollback of child effects is implied.
    pub fn wait_with_output(&mut self, timeout: Duration, max_bytes: usize) -> io::Result<Output> {
        self.wait_with_output_checked(timeout, max_bytes, || Ok(()))
    }

    /// Checks host intent before each bounded drain using the same cleanup owner.
    /// A check error stops capture; cleanup failure takes precedence. The check
    /// must return promptly. Cancellation does not establish effect rollback.
    pub fn wait_with_output_checked(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        check: impl FnMut() -> io::Result<()>,
    ) -> io::Result<Output> {
        self.wait_with_output_observed(timeout, max_bytes, check)
            .map_err(|failure| failure.error)
    }

    /// Uses the same bounded capture and cleanup loop, preserving collected
    /// output on timeout, output overflow, intent failure or unresolved cleanup.
    /// A stopped capture's output is a prefix, not a complete process transcript.
    pub fn wait_with_output_observed(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        check: impl FnMut() -> io::Result<()>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(timeout, max_bytes, None, check, |_| {
            Ok(ProcessInput::default())
        }, None, None)
    }

    /// Writes the exact admitted input, closes stdin after its final byte and
    /// observes normal child completion in the existing bounded capture loop.
    /// Empty input closes stdin immediately. Unlike protocol completion, EOF
    /// does not terminate the process or its containment.
    pub fn wait_with_input_observed(
        &mut self,
        timeout: Duration,
        max_output_bytes: usize,
        input: &[u8],
        check: impl FnMut() -> io::Result<()>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(
            timeout, max_output_bytes, Some(CaptureInput::Literal(input)), check,
            |_| Ok(ProcessInput::default()), None, None,
        )
    }

    /// Performs a bounded streaming exchange in the same capture/cleanup loop.
    /// The host supplies an admitted protocol and checks durable intent. Neither
    /// callback may block or perform unbounded work. Input bounds cover the
    /// initial bytes plus every callback write, including already written bytes.
    /// Completion closes stdin and terminates the owned tree; exit status may
    /// therefore be unsuccessful even after the protocol received its result.
    pub fn wait_with_exchange_observed(
        &mut self,
        timeout: Duration,
        max_output_bytes: usize,
        max_input_bytes: usize,
        initial: &[u8],
        check: impl FnMut() -> io::Result<()>,
        observe: impl FnMut(&[u8]) -> io::Result<ProcessInput>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(
            timeout,
            max_output_bytes,
            Some(CaptureInput::Exchange { initial, maximum: max_input_bytes }),
            check,
            observe,
            None,
            None,
        )
    }

    /// Captures into the caller's already admitted concrete buffers using the
    /// original invocation meter; no second capture or cleanup loop is created.
    pub fn wait_with_output_observed_with_meter(
        &mut self, timeout: Duration, max_bytes: usize,
        buffers: [Vec<u8>; 3], meter: &dyn crate::NativeReadMeter,
        check: impl FnMut() -> io::Result<()>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(timeout, max_bytes, None, check,
            |_| Ok(ProcessInput::default()), Some(meter), Some(buffers))
    }

    /// Writes the same admitted literal input through the canonical capture owner.
    pub fn wait_with_input_observed_with_meter(
        &mut self, timeout: Duration, max_bytes: usize, input: &[u8],
        buffers: [Vec<u8>; 3], meter: &dyn crate::NativeReadMeter,
        check: impl FnMut() -> io::Result<()>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(timeout, max_bytes, Some(CaptureInput::Literal(input)),
            check, |_| Ok(ProcessInput::default()), Some(meter), Some(buffers))
    }

    /// Runs the same streaming exchange with original admitted buffer custody.
    #[allow(clippy::too_many_arguments, reason = "original bounds, buffers and callbacks remain explicit")]
    pub fn wait_with_exchange_observed_with_meter(
        &mut self, timeout: Duration, max_bytes: usize, max_input_bytes: usize,
        initial: &[u8], buffers: [Vec<u8>; 3], meter: &dyn crate::NativeReadMeter,
        check: impl FnMut() -> io::Result<()>,
        observe: impl FnMut(&[u8]) -> io::Result<ProcessInput>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.capture_output(timeout, max_bytes,
            Some(CaptureInput::Exchange { initial, maximum: max_input_bytes }),
            check, observe, Some(meter), Some(buffers))
    }

    fn capture_output(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        mut check: impl FnMut() -> io::Result<()>,
        mut observe: impl FnMut(&[u8]) -> io::Result<ProcessInput>,
        meter: Option<&dyn crate::NativeReadMeter>,
        buffers: Option<[Vec<u8>; 3]>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.select_control_meter(meter);
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.wait_with_output",
            mode = if input.is_some() {
                "exchange"
            } else {
                "output"
            },
            bytes = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        let mut remaining = max_bytes;
        let result = operation.scope(|| {
            if self.output_taken {
                return Err(ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                )));
            }
            let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
                ProcessCaptureFailure::before_capture(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process deadline overflow",
                ))
            })?;
            let (mut output, stdin_buffer) = match buffers {
                Some([stdout, stderr, stdin]) => ([stdout, stderr], Some(stdin)),
                None => ([Vec::new(), Vec::new()], None),
            };
            if meter.is_some() && output.iter().any(|buffer| !buffer.is_empty() || buffer.capacity() < max_bytes) {
                return Err(ProcessCaptureFailure::before_capture(io::ErrorKind::OutOfMemory.into()));
            }
            let mut input = input
                .map(|input| PendingStdin::new_with_buffer(input, stdin_buffer, meter))
                .transpose()
                .map_err(ProcessCaptureFailure::before_capture)?;
            if input.is_none() {
                self.capture_close_stdin(meter).map_err(ProcessCaptureFailure::before_capture)?;
            }
            let child = self.child.as_mut().ok_or_else(|| {
                ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                ))
            })?;
            self.output_taken = true;
            let mut stdout = child.stdout.take();
            let mut stderr = child.stderr.take();
            let mut status = None;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
                check()?;
                let mut progressed = input.as_mut().map_or(Ok(false), |stdin| stdin.poll(self, meter))?;
                let before_stdout = output[0].len();
                progressed |= drain_pipe(&mut stdout, &mut output[0], &mut remaining, meter)?
                    | drain_pipe(&mut stderr, &mut output[1], &mut remaining, meter)?;
                if let Some(input) = input.as_mut()
                    && !input.complete
                    && output[0].len() > before_stdout
                {
                    let chunk = output[0]
                        .get(before_stdout..)
                        .ok_or_else(|| io::Error::other("invalid observed process stdout"))?;
                    input.observe(chunk, &mut observe, meter)?;
                }
                if status.is_none()
                    && let Some(exit) = self.capture_try_wait(meter)?
                {
                    status = Some(exit);
                    self.capture_terminate_descendants(meter)?;
                }
                if status.is_some() && stdout.is_none() && stderr.is_none() {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "process output deadline exceeded; effects may have occurred",
                    ));
                }
                if !progressed {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }));
            // Both stopped capture and callback panic return outstanding pipe
            // custody to the original supervisor, never an unmetered drop.
            if let Some(child) = self.child.as_mut() {
                child.stdout = stdout;
                child.stderr = stderr;
            }
            let result = match result {
                Ok(result) => result,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            // Cleanup errors take precedence: a capture timeout alone does not
            // describe an unresolved termination. Keep Child ownership on error.
            let result = self.capture_terminate(meter).and(result).and_then(|()| {
                status.ok_or_else(|| io::Error::other("capture completed without an exit status"))
            });
            let [stdout, stderr] = output;
            match result {
                Ok(status) => Ok(Output {
                    status,
                    stdout,
                    stderr,
                }),
                Err(error) => Err(ProcessCaptureFailure {
                    error,
                    status,
                    stdout,
                    stderr,
                    cleanup_completed: self.is_reaped(),
                }),
            }
        });
        operation.record("bytes", max_bytes - remaining);
        operation.record_result(&result.as_ref().map_err(|failure| &failure.error));
        result
    }

    /// Terminates containment and reaps the direct child. Windows confirms the
    /// Job is empty; Unix confirms signal delivery, not descendant reaping.
    /// Repeated successful cleanup is a no-op. An error retains ownership.
    pub fn terminate(&mut self) -> io::Result<()> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.terminate",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| operation.finish(self.capture_terminate(None)))
    }

    /// Cleans up through the same containment/observation/reap engine, with
    /// admission at each signal, membership probe and wait attempt. An error
    /// retains identity and never launches an unmeasured cleanup retry in Drop.
    /// Success closes every retained stdin/stdout/stderr handle; Windows also
    /// retires setup, Child and Job handles. Capture uses synchronous polling,
    /// not detached stdio workers. Explicitly transferred `take_*` streams
    /// remain the receiving caller's lifecycle responsibility.
    pub fn terminate_measured(&mut self, meter: &dyn crate::NativeReadMeter) -> io::Result<()> {
        self.capture_terminate(Some(meter))
    }

    fn capture_terminate(
        &mut self, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        self.select_control_meter(meter);
        #[cfg(unix)]
        return self.terminate_with_meter(meter);
        #[cfg(windows)]
        {
            self.guard.release_setup(meter)?;
            self.capture_terminate_descendants(meter)?;
            self.capture_close_stdin(meter)?;
            if self.child.is_some() {
                let deadline = Instant::now() + Duration::from_secs(5);
                while self.capture_try_wait(meter)?.is_none() {
                    if Instant::now() >= deadline { return Err(io::ErrorKind::TimedOut.into()); }
                    process_control_attempt(meter, false, || {
                        std::thread::sleep(Duration::from_millis(1));
                        Ok(())
                    })?;
                }
                if let Some(child) = self.child.as_mut() {
                    if child.stdout.is_some() {
                        process_control_attempt(meter, true, || { drop(child.stdout.take()); Ok(()) })?;
                    }
                    if child.stderr.is_some() {
                        process_control_attempt(meter, true, || { drop(child.stderr.take()); Ok(()) })?;
                    }
                }
                // Child's private Win32 handle release is one opaque std SDK
                // boundary, not an estimate of hidden handle/kernel internals.
                process_control_attempt(meter, true, || { drop(self.child.take()); Ok(()) })?;
            }
            self.guard.close(meter)
        }
    }

    #[cfg(unix)]
    fn terminate_with_meter(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        self.spawn_resources.cleanup(meter)?;
        self.guard.terminate_with_meter(meter)?;
        self.capture_close_stdin(meter)?;
        if self.child.is_some() || self.native_child.is_some() {
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.try_wait_with_meter(meter)?.is_none() {
                if Instant::now() >= deadline {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                process_control_attempt(meter, false, || {
                    std::thread::sleep(Duration::from_millis(1));
                    Ok(())
                })?;
            }
            // Only actually retained standard pipes require a release call.
            // Denial keeps the observed leader unreaped and its remaining
            // streams owned by this same tree, rather than hiding their drops
            // in the final Child release.
            if let Some(child) = self.child.as_mut() {
                if child.stdout.is_some() {
                    process_control_attempt(meter, true, || { drop(child.stdout.take()); Ok(()) })?;
                }
                if child.stderr.is_some() {
                    process_control_attempt(meter, true, || { drop(child.stderr.take()); Ok(()) })?;
                }
            }
            let (reaped, recorded) = platform::reap_exit_id(self.owned_child_id()? as libc::pid_t, meter)?;
            if reaped.is_some() {
                self.child.take();
                self.native_child.take();
            }
            recorded?;
            if reaped.is_none() { return Err(io::ErrorKind::Other.into()); }
        }
        Ok(())
    }

    /// Whether containment cleanup succeeded and the direct child was reaped.
    /// Unix confirms group signal delivery, not reaping of every descendant;
    /// descendants that deliberately leave containment remain outside this claim.
    /// Any retained native spawn-configuration custody must also be released.
    #[must_use]
    pub const fn is_reaped(&self) -> bool {
        #[cfg(unix)]
        return self.child.is_none() && self.native_child.is_none() && !self.spawn_resources.pending();
        #[cfg(windows)]
        {
            self.child.is_none() && !self.guard.pending()
        }
    }

    /// Requests graceful Unix group termination, then performs mandatory tree
    /// cleanup after direct-child exit or `grace`. Windows has no group-wide
    /// graceful signal and uses Job termination immediately.
    pub fn terminate_after(&mut self, grace: Duration) -> io::Result<ExitStatus> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.terminate_after",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| operation.finish(self.terminate_after_unobserved(grace)))
    }

    fn terminate_after_unobserved(&mut self, grace: Duration) -> io::Result<ExitStatus> {
        #[cfg(unix)]
        self.guard.request_termination(None)?;
        #[cfg(windows)]
        self.terminate_descendants()?;
        let observed = self.wait(grace);
        let status = match observed {
            Ok(status) => status,
            Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                self.terminate_descendants()?;
                self.wait(Duration::from_secs(5))?
            }
            Err(error) => {
                self.terminate()?;
                return Err(error);
            }
        };
        self.terminate()?;
        Ok(status)
    }

    /// Signals termination to the entire tree while retaining the direct child
    /// so its captured output can still be collected.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        self.capture_terminate_descendants(None)
    }

    fn capture_terminate_descendants(
        &mut self, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        self.select_control_meter(meter);
        let containment = self.guard.terminate_with_meter(meter);
        #[cfg(unix)]
        return containment;
        #[cfg(windows)]
        {
            // Assignment failure leaves a real suspended Child outside the
            // Job. Independently admit its direct kill even if Job control
            // failed, retaining both owners and the original native cause.
            let direct = if !self.guard.assigned && !self.guard.direct_termination_sent {
                if let Some(child) = self.child.as_mut() {
                    process_control_attempt(meter, true, || {
                        child.kill()?;
                        self.guard.direct_termination_sent = true;
                        Ok(())
                    })
                } else { Ok(()) }
            } else { Ok(()) };
            containment.and(direct)
        }
    }
}

/// Reads one chunk; returns whether the pipe produced data or reached EOF,
/// so the caller sleeps only when both pipes are idle.
fn drain_pipe<T: Read + platform::Pipe>(
    pipe: &mut Option<T>,
    output: &mut Vec<u8>,
    remaining: &mut usize,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<bool> {
    let Some(reader) = pipe.as_mut() else {
        return Ok(false);
    };
    let maximum = (*remaining).min(64 * 1024).max(1);
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
    let mut reservation = meter.map(|meter| crate::NativeByteReservation::new(
        meter, crate::NativeOperationKind::Read, maximum,
    )).transpose()?;
    let before = output.len();
    let mut probe = [std::mem::MaybeUninit::<u8>::uninit(); 1];
    let buffer = if admitted == 0 {
        &mut probe[..]
    } else {
        output.spare_capacity_mut().get_mut(..admitted)
            .ok_or_else(|| io::Error::other("invalid process output capacity"))?
    };
    let transfer = platform::read_pipe_with_meter(reader, buffer, meter);
    let actual = transfer.as_ref().ok().and_then(|(read, _)| *read).unwrap_or(0);
    let recorded_bytes = reservation.as_mut().map_or(Ok(()), |token| token.record(actual));
    if actual > maximum { return Err(io::Error::other("pipe returned an invalid length")); }
    let retained = actual.min(admitted);
    // SAFETY: the synchronous pipe operation initialized exactly `actual`
    // bytes in the exclusively borrowed spare capacity. Publish that prefix
    // before any late accounting or cancellation error can leave capture.
    unsafe { output.set_len(before + retained); }
    *remaining -= retained;
    recorded_bytes?;
    let (read, recorded_operation) = transfer?;
    recorded_operation?;
    match read {
        Some(0) => {
            let (closed, recorded) = pipe_operation_attempt(
                meter, crate::NativeOperationKind::Write, || {
                    drop(pipe.take());
                    Ok(())
                },
            )?;
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
    if let Some(meter) = meter { meter.check()?; }
    Ok(true)
}

/// One pipe SDK call owns its own credit. A completed payload and a late
/// accounting error travel separately until its physical prefix is published.
fn pipe_operation_attempt<T>(
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

impl Drop for ProcessTree {
    fn drop(&mut self) {
        if !self.cleanup_on_drop { return; }
        let _ = self.terminate();
    }
}

/// One actual SDK/kernel attempt, not a prediction of its hidden internals.
/// Accepted calls run even if their bookkeeping reports a late error.
fn process_control_attempt<T>(
    meter: Option<&dyn crate::NativeReadMeter>, write: bool, call: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let kind = if write { crate::NativeOperationKind::Write } else { crate::NativeOperationKind::Read };
    let (result, recorded) = pipe_operation_attempt(meter, kind, call)?;
    match result {
        Err(error) => Err(error),
        Ok(value) => recorded.map(|()| value),
    }
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::io::{Read, Write};
    pub(super) use std::os::fd::AsRawFd as Pipe;
    use std::os::unix::process::CommandExt as _;
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::{Command, ExitStatus};
    #[cfg(test)]
    use std::process::Child;

    #[cfg(test)]
    pub(super) fn observe_exit(child: &Child) -> io::Result<Option<ExitStatus>> {
        observe_exit_id(child.id(), None)
    }

    pub(super) fn observe_exit_id(
        id: libc::id_t, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<ExitStatus>> {
        let flags = libc::WEXITED | libc::WNOWAIT | libc::WNOHANG;
        loop {
            // SAFETY: initialized OS output storage and the exclusively owned
            // child's ID. WNOWAIT retains the leader until group termination,
            // preventing its PID/PGID from being reused for an unrelated group.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: `info` is writable storage owned by this frame and `id`
            // names the exclusively owned child.
            let (waited, recorded) = super::pipe_operation_attempt(
                meter, crate::NativeOperationKind::Read, || {
                    if unsafe { libc::waitid(libc::P_PID, id, &raw mut info, flags) } == 0 { Ok(()) }
                    else { Err(io::Error::last_os_error()) }
                },
            )?;
            match waited {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => { recorded?; continue; }
                Err(error) => return Err(error),
                Ok(()) => recorded?,
            }
            // SAFETY: WEXITED waitid initialized the child-status union.
            if unsafe { info.si_pid() } == 0 {
                return Ok(None);
            }
            // SAFETY: the same WEXITED observation initialized the status field.
            let status = unsafe { info.si_status() };
            let raw = match info.si_code {
                libc::CLD_EXITED => status << 8,
                libc::CLD_KILLED => status,
                libc::CLD_DUMPED => status | 0x80,
                _ => return Err(io::ErrorKind::Other.into()),
            };
            return Ok(Some(ExitStatus::from_raw(raw)));
        }
    }

    pub(super) fn reap_exit_id(
        pid: libc::pid_t, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<ExitStatus>, io::Result<()>)> {
        loop {
            let mut status = 0;
            let (result, recorded) = super::pipe_operation_attempt(
                meter, crate::NativeOperationKind::Read, || {
                    // SAFETY: exclusively owned child ID and writable status.
                    let waited = unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) };
                    if waited < 0 { Err(io::Error::last_os_error()) } else { Ok(waited) }
                },
            )?;
            match result {
                Ok(0) => return Ok((None, recorded)),
                Ok(_) => return Ok((Some(ExitStatus::from_raw(status)), recorded)),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => { recorded?; }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn read_pipe(
        pipe: &mut (impl Read + Pipe),
        buffer: &mut [u8],
    ) -> io::Result<Option<usize>> {
        // SAFETY: initialized bytes may also be borrowed as MaybeUninit bytes.
        let buffer = unsafe { std::slice::from_raw_parts_mut(
            buffer.as_mut_ptr().cast(), buffer.len(),
        ) };
        let (read, recorded) = read_pipe_with_meter(pipe, buffer, None)?;
        recorded?;
        Ok(read)
    }

    pub(super) fn read_pipe_with_meter(
        pipe: &mut (impl Read + Pipe),
        buffer: &mut [std::mem::MaybeUninit<u8>],
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<usize>, io::Result<()>)> {
        nonblocking(pipe, meter)?;
        let (read, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Read, || {
                // SAFETY: the owned nonblocking fd receives this exclusively
                // borrowed writable allocation; read initializes its prefix.
                let read = unsafe { libc::read(
                    pipe.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len(),
                ) };
                if read < 0 { Err(io::Error::last_os_error()) } else { Ok(read as usize) }
            },
        )?;
        match read {
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) =>
                Ok((None, recorded)),
            Err(error) => { recorded?; Err(error) }
            Ok(read) => Ok((Some(read), recorded)),
        }
    }

    pub(super) fn write_pipe(
        pipe: &mut (impl Write + Pipe), bytes: &[u8],
    ) -> io::Result<Option<usize>> {
        let (written, recorded) = write_pipe_with_meter(pipe, bytes, None)?;
        recorded?;
        Ok(written)
    }

    pub(super) fn write_pipe_with_meter(
        pipe: &mut (impl Write + Pipe), bytes: &[u8],
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<usize>, io::Result<()>)> {
        nonblocking(pipe, meter)?;
        let (written, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Write, || pipe.write(bytes),
        )?;
        match written {
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) =>
                Ok((None, recorded)),
            Ok(0) => { recorded?; Err(io::Error::new(
                io::ErrorKind::WriteZero, "process pipe write stopped",
            )) }
            Err(error) => { recorded?; Err(error) }
            Ok(written) => Ok((Some(written), recorded)),
        }
    }

    fn nonblocking(pipe: &impl Pipe, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        let (flags, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Read, || {
                // SAFETY: this exclusively owned live fd is queried only.
                let flags = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETFL) };
                if flags == -1 { Err(io::Error::last_os_error()) } else { Ok(flags) }
            },
        )?;
        recorded?;
        let flags = flags?;
        if flags & libc::O_NONBLOCK != 0 { return Ok(()); }
        let (result, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Write, || {
                // SAFETY: add only O_NONBLOCK to this owner's original flags.
                if unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
                    Err(io::Error::last_os_error())
                } else { Ok(()) }
            },
        )?;
        recorded?;
        result
    }

    pub(super) struct Guard {
        process_group: libc::pid_t,
        active: bool,
    }

    /// Native spawn objects that a failed SDK destroy did not release.
    /// Inline custody retains their actual handles without an emergency heap
    /// allocation after a bounded failure.
    #[derive(Default)]
    pub(super) struct SpawnResources {
        actions: Option<libc::posix_spawn_file_actions_t>,
        attributes: Option<libc::posix_spawnattr_t>,
    }

    // SAFETY: these initialized SDK objects are exclusively owned values.
    // Native mutation requires &mut custody; shared access inspects only the
    // inline presence flags and never dereferences or mutates a native handle.
    unsafe impl Send for SpawnResources {}
    // SAFETY: shared access observes presence only; all SDK calls require an
    // exclusive mutable owner, as for Send above.
    unsafe impl Sync for SpawnResources {}

    impl SpawnResources {
        pub(super) const fn pending(&self) -> bool {
            self.actions.is_some() || self.attributes.is_some()
        }

        pub(super) fn cleanup(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
            if self.actions.is_some() {
                super::process_control_attempt(meter, true, || {
                    let actions = self.actions.as_mut().ok_or(io::ErrorKind::NotFound)?;
                    let status = unsafe { libc::posix_spawn_file_actions_destroy(actions) };
                    if status != 0 { return Err(io::Error::from_raw_os_error(status)); }
                    self.actions.take();
                    Ok(())
                })?;
            }
            if self.attributes.is_some() {
                super::process_control_attempt(meter, true, || {
                    let attributes = self.attributes.as_mut().ok_or(io::ErrorKind::NotFound)?;
                    let status = unsafe { libc::posix_spawnattr_destroy(attributes) };
                    if status != 0 { return Err(io::Error::from_raw_os_error(status)); }
                    self.attributes.take();
                    Ok(())
                })?;
            }
            Ok(())
        }
    }

    pub(super) fn spawn_controlled(
        command: &mut Command, meter: Option<&dyn crate::NativeReadMeter>,
        custody: &mut Option<super::ProcessTree>,
    ) -> io::Result<()> {
        command.process_group(0);
        // std owns the prepared arguments, environment and pipe configuration.
        // Command::spawn is one actual opaque SDK attempt; neither its private
        // allocations nor its hidden kernel calls are fabricated as metrics.
        super::process_control_attempt(meter, true, || {
            let child = command.spawn()?;
            // A Unix Child ID originated as pid_t at this same SDK boundary.
            let process_group = child.id() as libc::pid_t;
            *custody = Some(super::ProcessTree {
                child: Some(child), guard: Guard { process_group, active: true },
                output_taken: false, native_child: None,
                cleanup_on_drop: meter.is_none(),
                spawn_resources: SpawnResources::default(),
            });
            Ok(())
        })
    }

    #[cfg(target_vendor = "apple")]
    unsafe extern "C" {
        fn _NSGetEnviron() -> *mut *mut *mut libc::c_char;
    }

    #[cfg(not(target_vendor = "apple"))]
    unsafe extern "C" {
        static mut environ: *mut *mut libc::c_char;
    }

    pub(super) fn spawn_closed(
        program: &std::ffi::CStr, args: &[&std::ffi::CStr],
        meter: &dyn crate::NativeReadMeter, custody: &mut Option<super::ProcessTree>,
    ) -> io::Result<()> {
        let count = args.len().checked_add(2).ok_or(io::ErrorKind::InvalidInput)?;
        let bytes = count.checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let copied = bytes - std::mem::size_of::<*mut libc::c_char>();
        meter.admit_examination((count - 1) as u64, copied as u64)?;
        meter.allocate(bytes as u64)?;
        let mut arguments = Vec::new();
        if arguments.try_reserve_exact(count).is_err() {
            meter.release(bytes as u64);
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        arguments.push(program.as_ptr().cast_mut());
        arguments.extend(args.iter().map(|argument| argument.as_ptr().cast_mut()));
        arguments.push(std::ptr::null_mut());
        let result = (|| {
            meter.record_examination((count - 1) as u64, copied as u64)?;
            let attr_cleanup = meter.reserve_cleanup_write()?;
            let actions_cleanup = match meter.reserve_cleanup_write() {
                Ok(cleanup) => cleanup,
                Err(error) => { meter.cancel_cleanup_write(attr_cleanup); return Err(error); }
            };
            let mut attributes = std::mem::MaybeUninit::<libc::posix_spawnattr_t>::uninit();
            let mut actions = std::mem::MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
            let mut attr_initialized = false;
            let mut actions_initialized = false;
            let spawned = (|| {
                let native = |call: &mut dyn FnMut() -> libc::c_int| {
                    super::process_control_attempt(Some(meter), true, || {
                        let status = call();
                        if status == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(status)) }
                    })
                };
                // Each SDK call below uses this frame's exclusive native
                // storage. Initialization precedes setters and destruction;
                // terminated argv strings and inherited environment remain
                // borrowed and valid throughout synchronous posix_spawn.
                native(&mut || unsafe {
                    let status = libc::posix_spawnattr_init(attributes.as_mut_ptr());
                    if status == 0 { attr_initialized = true; }
                    status
                })?;
                native(&mut || unsafe {
                    let status = libc::posix_spawn_file_actions_init(actions.as_mut_ptr());
                    if status == 0 { actions_initialized = true; }
                    status
                })?;
                native(&mut || unsafe { libc::posix_spawnattr_setpgroup(attributes.as_mut_ptr(), 0) })?;
                native(&mut || unsafe { libc::posix_spawnattr_setflags(attributes.as_mut_ptr(), libc::POSIX_SPAWN_SETPGROUP as libc::c_short) })?;
                for descriptor in [0, 1, 2] {
                    native(&mut || unsafe {
                        libc::posix_spawn_file_actions_addopen(actions.as_mut_ptr(), descriptor,
                            c"/dev/null".as_ptr(), if descriptor == 0 { libc::O_RDONLY } else { libc::O_WRONLY }, 0)
                    })?;
                }
                let mut pid = 0;
                loop {
                    let (result, recorded) = super::pipe_operation_attempt(
                        Some(meter), crate::NativeOperationKind::Write, || unsafe {
                        #[cfg(target_vendor = "apple")]
                        let environment = *_NSGetEnviron();
                        #[cfg(not(target_vendor = "apple"))]
                        let environment = environ;
                        let status = libc::posix_spawn(&raw mut pid, program.as_ptr(), actions.as_ptr(), attributes.as_ptr(),
                            arguments.as_ptr(), environment);
                        if status == 0 {
                            *custody = Some(super::ProcessTree {
                                child: None, native_child: Some(pid),
                                guard: Guard { process_group: pid, active: true },
                                output_taken: false, cleanup_on_drop: false,
                                spawn_resources: SpawnResources::default(),
                            });
                        }
                        if status == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(status)) }
                    })?;
                    recorded?;
                    match result {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted && custody.is_none() => {}
                        result => { result?; break; }
                    }
                }
                Ok(())
            })();
            // Retire objects in reverse construction order. Both actual
            // destroys run even after cancellation or another cleanup failure.
            let mut retained = SpawnResources::default();
            let actions_result = if actions_initialized {
                let recorded = meter.record_cleanup_write(actions_cleanup);
                let status = unsafe { libc::posix_spawn_file_actions_destroy(actions.as_mut_ptr()) };
                if status != 0 { retained.actions = Some(unsafe { actions.assume_init() }); }
                recorded.and_then(|()| if status == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(status)) })
            } else { meter.cancel_cleanup_write(actions_cleanup); Ok(()) };
            let attr_result = if attr_initialized {
                let recorded = meter.record_cleanup_write(attr_cleanup);
                let status = unsafe { libc::posix_spawnattr_destroy(attributes.as_mut_ptr()) };
                if status != 0 { retained.attributes = Some(unsafe { attributes.assume_init() }); }
                recorded.and_then(|()| if status == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(status)) })
            } else { meter.cancel_cleanup_write(attr_cleanup); Ok(()) };
            if retained.pending() {
                let owner = custody.get_or_insert_with(|| super::ProcessTree {
                    child: None, native_child: None,
                    guard: Guard { process_group: 0, active: false },
                    output_taken: false, cleanup_on_drop: false,
                    spawn_resources: SpawnResources::default(),
                });
                owner.spawn_resources = retained;
            }
            spawned.and(actions_result).and(attr_result)
        })();
        drop(arguments);
        meter.release(bytes as u64);
        result
    }

    impl Guard {
        fn group_gone(&self, error: &io::Error, _meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<bool> {
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(true);
            }
            #[cfg(target_vendor = "apple")]
            if error.raw_os_error() == Some(libc::EPERM) {
                return group_has_no_live_members(self.process_group, _meter);
            }
            Ok(false)
        }

        pub(super) fn request_termination(
            &mut self, meter: Option<&dyn crate::NativeReadMeter>,
        ) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            let signal = super::process_control_attempt(meter, true, || {
                // SAFETY: the unreaped owned leader reserves this process group.
                if unsafe { libc::kill(-self.process_group, libc::SIGTERM) } == 0 { Ok(()) }
                else { Err(io::Error::last_os_error()) }
            });
            if signal.is_ok() { return Ok(()); }
            let error = signal.expect_err("failed graceful group signal");
            if self.group_gone(&error, meter)? {
                Ok(())
            } else {
                Err(error)
            }
        }

        pub(super) fn terminate_with_meter(
            &mut self, meter: Option<&dyn crate::NativeReadMeter>,
        ) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: a negative, nonzero pid addresses exactly this owned
            // process group; SIGKILL requires no shared memory or signal data.
            let signal = super::process_control_attempt(meter, true, || {
                if unsafe { libc::kill(-self.process_group, libc::SIGKILL) } == 0 {
                    self.active = false;
                    Ok(())
                } else { Err(io::Error::last_os_error()) }
            });
            if signal.is_ok() {
                return Ok(());
            }
            let error = signal.expect_err("failed group signal");
            if self.group_gone(&error, meter)? {
                self.active = false;
                Ok(())
            } else {
                Err(error)
            }
        }
    }

    #[cfg(target_vendor = "apple")]
    fn group_has_no_live_members(group: libc::pid_t, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<bool> {
        // Terminated descendants can remain visible until the OS reaps them.
        // Wait only for the same conservative membership proof; EPERM itself
        // never establishes that any member has exited. Denial stays an error
        // if this bounded observation does not establish an empty group or
        // exactly the independently observed exited owned leader.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if group_snapshot_has_no_live_members(group, meter)? {
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            super::process_control_attempt(meter, false, || {
                std::thread::sleep(std::time::Duration::from_millis(1));
                Ok(())
            })?;
        }
    }

    #[cfg(target_vendor = "apple")]
    fn group_snapshot_has_no_live_members(group: libc::pid_t, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<bool> {
        // XNU skips zombies in killpg1 and reports EPERM when none are signalled.
        // Accept only an empty group or a snapshot containing exactly the owned,
        // independently observed exited leader. Other members (even zombies)
        // retain the error: no authority or descendant-completion claim is made.
        // proc_listpids(PROC_PGRP_ONLY = 2) snapshots live and zombie membership
        // under the kernel process-list lock. Two slots distinguish a sole
        // leader from a larger, possibly truncated group without allocation.
        let mut members = [0_i32; 2];
        let bytes = i32::try_from(std::mem::size_of_val(&members)).map_err(io::Error::other)?;
        let id = u32::try_from(group).map_err(io::Error::other)?;
        // SAFETY: thread-local errno and a writable, correctly sized PID array.
        // libproc returns zero both for an empty list and for an errno failure.
        let read = super::process_control_attempt(meter, false, || {
            Ok(unsafe {
                *libc::__error() = 0;
                libc::proc_listpids(2, id, members.as_mut_ptr().cast(), bytes)
            })
        })?;
        if read <= 0 {
            let error = io::Error::last_os_error();
            return if read == 0 && error.raw_os_error() == Some(0) {
                Ok(true)
            } else {
                Err(error)
            };
        }
        if read == i32::try_from(std::mem::size_of::<libc::pid_t>()).map_err(io::Error::other)?
            && members.first() == Some(&group)
        {
            return Ok(observe_exit_id(id, meter)?.is_some());
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessTree;

    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    #[derive(Default)]
    pub(super) struct ControlCounts {
        pub(super) reads: AtomicU64, pub(super) writes: AtomicU64,
        allocations: AtomicU64, live: AtomicU64, peak: AtomicU64,
        items: AtomicU64, copied: AtomicU64, write_limit: AtomicU64,
        pub(super) reserved_reads: AtomicU64, pub(super) reserved_writes: AtomicU64,
    }

    pub(super) struct ControlMeter {
        pub(super) counts: ControlCounts,
        read_limit: u64,
        allocation_limit: u64,
        memory_limit: u64,
        cancelled: std::sync::atomic::AtomicBool,
        allocation_failure: std::sync::Mutex<Option<std::collections::TryReserveError>>,
    }

    impl ControlMeter {
        pub(super) fn new(reads: u64, writes: u64, allocations: u64, memory: u64) -> Self {
            Self {
                counts: ControlCounts { write_limit: AtomicU64::new(writes), ..ControlCounts::default() },
                read_limit: reads, allocation_limit: allocations, memory_limit: memory,
                cancelled: std::sync::atomic::AtomicBool::new(false),
                allocation_failure: std::sync::Mutex::new(None),
            }
        }
    }

    impl crate::NativeReadMeter for ControlMeter {
        fn check(&self) -> std::io::Result<()> {
            if self.cancelled.load(std::sync::atomic::Ordering::Acquire) {
                Err(std::io::ErrorKind::Interrupted.into())
            } else { Ok(()) }
        }
        fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
            let mut failure = self.allocation_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if failure.is_none() { *failure = Some(error); }
            std::io::ErrorKind::OutOfMemory.into()
        }
        fn reserve_operations(&self, kind: crate::NativeOperationKind, count: u64) -> std::io::Result<()> {
            let reserved = match kind {
                crate::NativeOperationKind::Read => {
                    self.admit_operations(count)?;
                    &self.counts.reserved_reads
                }
                crate::NativeOperationKind::Write => {
                    self.admit_write_operations(count)?;
                    &self.counts.reserved_writes
                }
                crate::NativeOperationKind::Durability => return Err(std::io::ErrorKind::Unsupported.into()),
            };
            reserved.fetch_add(count, Relaxed);
            Ok(())
        }
        fn cancel_reserved_operations(&self, kind: crate::NativeOperationKind, count: u64) {
            let reserved = match kind {
                crate::NativeOperationKind::Read => &self.counts.reserved_reads,
                crate::NativeOperationKind::Write => &self.counts.reserved_writes,
                crate::NativeOperationKind::Durability => unreachable!("process control cannot reserve durability"),
            };
            assert!(reserved.fetch_sub(count, Relaxed) >= count, "refund only owned unused reservations");
        }
        fn record_reserved_operation(&self, kind: crate::NativeOperationKind) -> std::io::Result<()> {
            let (reserved, completed) = match kind {
                crate::NativeOperationKind::Read => (&self.counts.reserved_reads, &self.counts.reads),
                crate::NativeOperationKind::Write => (&self.counts.reserved_writes, &self.counts.writes),
                crate::NativeOperationKind::Durability => return Err(std::io::ErrorKind::Unsupported.into()),
            };
            reserved.fetch_update(Relaxed, Relaxed, |count| count.checked_sub(1))
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            completed.fetch_add(1, Relaxed);
            Ok(())
        }
        fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
            self.check()?;
            if maximum == 0 { Ok(()) } else { Err(std::io::ErrorKind::Unsupported.into()) }
        }
        fn reserve_bytes(&self, kind: crate::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
            if kind == crate::NativeOperationKind::Durability { return Err(std::io::ErrorKind::Unsupported.into()); }
            self.check()?;
            // Process-control fixtures own no native payload I/O allowance.
            if maximum == 0 { Ok(()) } else { Err(std::io::ErrorKind::Unsupported.into()) }
        }
        fn cancel_reserved_bytes(&self, kind: crate::NativeOperationKind, maximum: u64) {
            assert_ne!(kind, crate::NativeOperationKind::Durability, "no process control durability byte token can exist");
            assert_eq!(maximum, 0, "only an admitted zero-body control scope can be refunded");
        }
        fn record_reserved_bytes(
            &self, kind: crate::NativeOperationKind, maximum: u64, actual: u64,
        ) -> std::io::Result<()> {
            if kind == crate::NativeOperationKind::Durability { return Err(std::io::ErrorKind::Unsupported.into()); }
            if maximum == 0 && actual == 0 { Ok(()) }
            else { Err(std::io::ErrorKind::InvalidInput.into()) }
        }
        fn allocate(&self, bytes: u64) -> std::io::Result<()> {
            self.check()?;
            let counts = &self.counts;
            if counts.allocations.load(Relaxed) == self.allocation_limit || counts.live.load(Relaxed) + bytes > self.memory_limit {
                return Err(std::io::ErrorKind::OutOfMemory.into());
            }
            counts.allocations.fetch_add(1, Relaxed);
            let live = counts.live.fetch_add(bytes, Relaxed) + bytes;
            counts.peak.fetch_max(live, Relaxed);
            Ok(())
        }
        fn allocate_refcount_header(&self) -> std::io::Result<()> { self.allocate(0) }
        fn adopt(&self, bytes: u64) -> std::io::Result<()> {
            self.check()?;
            let counts = &self.counts;
            if counts.live.load(Relaxed) + bytes > self.memory_limit { return Err(std::io::ErrorKind::OutOfMemory.into()); }
            let live = counts.live.fetch_add(bytes, Relaxed) + bytes;
            counts.peak.fetch_max(live, Relaxed);
            Ok(())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
            let previous = match self.counts.live.fetch_update(Relaxed, Relaxed, |live| live.checked_add(bytes)) {
                Ok(previous) => previous,
                Err(_) => return (0, Err(std::io::ErrorKind::OutOfMemory.into())),
            };
            let live = previous + bytes;
            self.counts.peak.fetch_max(live, Relaxed);
            // Only already-observed capacity is retained. There is no
            // allocation operation or fabricated successful native growth.
            let result = self.check().and_then(|()| {
                if live > self.memory_limit { Err(std::io::ErrorKind::OutOfMemory.into()) }
                else { Ok(()) }
            });
            (bytes, result)
        }
        fn release(&self, bytes: u64) { self.counts.live.fetch_sub(bytes, Relaxed); }
        fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> { self.check() }
        fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
            self.counts.items.fetch_add(items, Relaxed);
            self.counts.copied.fetch_add(copied, Relaxed);
            Ok(())
        }
        fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
            self.admit_examination(items, copied)?; self.record_examination(items, copied)
        }
        fn admit_bytes(&self, _: usize) -> std::io::Result<()> { self.check() }
        fn admit_read(&self, _: usize) -> std::io::Result<()> { self.admit_operations(1)?; self.record_operations(1) }
        fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
            self.check()?;
            if self.counts.reads.load(Relaxed)
                .checked_add(self.counts.reserved_reads.load(Relaxed))
                .and_then(|current| current.checked_add(additional))
                .is_none_or(|total| total > self.read_limit) {
                Err(std::io::ErrorKind::Other.into())
            } else { Ok(()) }
        }
        fn record_operations(&self, completed: u64) -> std::io::Result<()> {
            self.counts.reads.fetch_add(completed, Relaxed); Ok(())
        }
        fn record_read(&self, _: usize) -> std::io::Result<()> { Ok(()) }
        fn admit_write(&self, _: usize) -> std::io::Result<()> { self.admit_write_operations(1)?; self.record_write_operations(1) }
        fn record_write(&self, _: usize) -> std::io::Result<()> { Ok(()) }
        fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
            self.check()?;
            let counts = &self.counts;
            if counts.writes.load(Relaxed)
                .checked_add(counts.reserved_writes.load(Relaxed))
                .and_then(|current| current.checked_add(additional))
                .is_none_or(|total| total > counts.write_limit.load(Relaxed)) {
                Err(std::io::ErrorKind::Other.into())
            } else { Ok(()) }
        }
        fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
            self.counts.writes.fetch_add(completed, Relaxed); Ok(())
        }
        fn admit_durability(&self) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
        fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
            self.admit_write_operations(1)?;
            self.counts.write_limit.fetch_sub(1, Relaxed);
            Ok(1)
        }
        fn cancel_cleanup_write(&self, reservation: u64) { self.counts.write_limit.fetch_add(reservation, Relaxed); }
        fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
            let counts = &self.counts;
            counts.write_limit.fetch_add(reservation, Relaxed);
            let writes = counts.writes.fetch_add(1, Relaxed) + 1;
            assert!(writes <= counts.write_limit.load(Relaxed), "reserved cleanup never exceeds its exact budget");
            Ok(())
        }
    }

    #[test]
    #[cfg(unix)]
    fn measured_command_spawn_retains_original_child_and_counts_one_sdk_attempt() -> std::io::Result<()> {
        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "exit 7"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut custody = None;
        let denied = ControlMeter::new(0, 0, 0, 0);
        assert!(crate::spawn_process_tree_with_meter_custody(&mut command, &denied, &mut custody).is_err());
        assert!(custody.is_none());
        assert_eq!(denied.counts.writes.load(Relaxed), 0);
        let spawn = ControlMeter::new(0, 1, 0, 0);
        crate::spawn_process_tree_with_meter_custody(&mut command, &spawn, &mut custody)?;
        assert_eq!(spawn.counts.writes.load(Relaxed), 1, "one actual opaque Command::spawn SDK attempt");
        assert_eq!(spawn.counts.allocations.load(Relaxed), 0, "adapter neither copies nor predicts SDK heap");
        assert_eq!(spawn.counts.items.load(Relaxed), 3, "program and two borrowed arguments");
        let mut tree = custody.take().expect("original owned Child");
        let id = tree.child.as_ref().expect("actual Child").id();
        assert!(!tree.cleanup_on_drop);
        let cancelled = ControlMeter::new(1, 0, 0, 0);
        cancelled.cancelled.store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(tree.try_wait_measured(&cancelled).expect_err("cancellation is not an OS EINTR retry").kind(),
            std::io::ErrorKind::Interrupted);
        assert_eq!(cancelled.counts.reads.load(Relaxed), 0, "denied actual waitid never ran");
        let stopped = ControlMeter::new(0, 0, 0, 0);
        assert!(tree.terminate_measured(&stopped).is_err());
        assert_eq!(tree.child.as_ref().expect("denial retains same Child").id(), id);
        assert!(!tree.is_reaped());
        assert_eq!(tree.wait(std::time::Duration::from_secs(5))?.code(), Some(7));
        let terminal = ControlMeter::new(2, 1, 0, 0);
        tree.terminate_measured(&terminal)?;
        assert!(tree.is_reaped());
        assert_eq!(terminal.counts.writes.load(Relaxed), 1, "actual group signal");
        assert_eq!(terminal.counts.reads.load(Relaxed), 2, "actual waitid and physical reap");
        assert_eq!(terminal.counts.reserved_reads.load(Relaxed), 0);
        assert_eq!(terminal.counts.reserved_writes.load(Relaxed), 0);
        Ok(())
    }

    #[cfg(unix)]
    mod finite_data {
        use super::{ControlMeter, AtomicU64, Relaxed};
        use super::super::{CaptureInput, PendingStdin, ProcessInput, ProcessTree, drain_pipe, platform};
        use crate::{NativeOperationKind, NativeReadMeter};
        use std::io::{self, Write};
        use std::os::unix::net::UnixStream;

        struct DataMeter {
            control: ControlMeter,
            read_limit: u64,
            write_limit: u64,
            copy_limit: u64,
            read_bytes: AtomicU64,
            write_bytes: AtomicU64,
            held_read: AtomicU64,
            held_write: AtomicU64,
        }

        impl DataMeter {
            fn new(read: u64, write: u64, copied: u64) -> Self {
                Self {
                    control: ControlMeter::new(1024, 1024, 0, 0),
                    read_limit: read, write_limit: write, copy_limit: copied,
                    read_bytes: AtomicU64::new(0), write_bytes: AtomicU64::new(0),
                    held_read: AtomicU64::new(0), held_write: AtomicU64::new(0),
                }
            }
            fn bytes(&self, kind: NativeOperationKind) -> (&AtomicU64, &AtomicU64, u64) {
                match kind {
                    NativeOperationKind::Read => (&self.read_bytes, &self.held_read, self.read_limit),
                    NativeOperationKind::Write => (&self.write_bytes, &self.held_write, self.write_limit),
                    NativeOperationKind::Durability => unreachable!("pipe data cannot reserve durability bytes"),
                }
            }
        }

        impl NativeReadMeter for DataMeter {
            fn check(&self) -> io::Result<()> { self.control.check() }
            fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
                self.control.allocation_failed(error)
            }
            fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
                if kind == NativeOperationKind::Durability {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                self.control.reserve_operations(kind, count)
            }
            fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
                assert_ne!(kind, NativeOperationKind::Durability, "no pipe durability token can exist");
                self.control.cancel_reserved_operations(kind, count);
            }
            fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
                if kind == NativeOperationKind::Durability {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                self.control.record_reserved_operation(kind)
            }
            fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
                if kind == NativeOperationKind::Durability {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                self.check()?;
                let (spent, held, limit) = self.bytes(kind);
                held.fetch_update(Relaxed, Relaxed, |current| {
                    current.checked_add(maximum).filter(|total| {
                        spent.load(Relaxed).checked_add(*total).is_some_and(|total| total <= limit)
                    })
                }).map(|_| ()).map_err(|_| io::ErrorKind::FileTooLarge.into())
            }
            fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
                assert_ne!(kind, NativeOperationKind::Durability, "no pipe durability byte token can exist");
                assert!(self.bytes(kind).1.fetch_sub(maximum, Relaxed) >= maximum);
            }
            fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()> {
                if kind == NativeOperationKind::Durability {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                assert!(actual <= maximum);
                self.cancel_reserved_bytes(kind, maximum);
                self.bytes(kind).0.fetch_add(actual, Relaxed);
                Ok(())
            }
            fn allocate(&self, bytes: u64) -> io::Result<()> { self.control.allocate(bytes) }
            fn allocate_refcount_header(&self) -> io::Result<()> { self.control.allocate_refcount_header() }
            fn adopt(&self, bytes: u64) -> io::Result<()> { self.control.adopt(bytes) }
            fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
                self.control.record_completed_adoption(bytes)
            }
            fn release(&self, bytes: u64) { self.control.release(bytes); }
            fn admit_examination(&self, items: u64, copied: u64) -> io::Result<()> {
                self.control.admit_examination(items, copied)?;
                if self.control.counts.copied.load(Relaxed).checked_add(copied)
                    .is_some_and(|total| total <= self.copy_limit) { Ok(()) }
                else { Err(io::ErrorKind::OutOfMemory.into()) }
            }
            fn record_examination(&self, items: u64, copied: u64) -> io::Result<()> {
                self.control.record_examination(items, copied)
            }
            fn examine(&self, items: u64, copied: u64) -> io::Result<()> {
                self.admit_examination(items, copied)?; self.record_examination(items, copied)
            }
            fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
                self.check()?;
                if self.read_bytes.load(Relaxed).saturating_add(maximum as u64) <= self.read_limit { Ok(()) }
                else { Err(io::ErrorKind::FileTooLarge.into()) }
            }
            fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> {
                self.check()?;
                if self.write_bytes.load(Relaxed).saturating_add(maximum as u64) <= self.write_limit { Ok(()) }
                else { Err(io::ErrorKind::FileTooLarge.into()) }
            }
            fn admit_read(&self, maximum: usize) -> io::Result<()> {
                self.admit_bytes(maximum)?; self.control.admit_operations(1)?; self.control.record_operations(1)
            }
            fn admit_write(&self, maximum: usize) -> io::Result<()> {
                self.admit_write_bytes(maximum)?; self.control.admit_write_operations(1)?; self.control.record_write_operations(1)
            }
            fn record_read(&self, actual: usize) -> io::Result<()> {
                self.read_bytes.fetch_add(actual as u64, Relaxed); Ok(())
            }
            fn record_write(&self, actual: usize) -> io::Result<()> {
                self.write_bytes.fetch_add(actual as u64, Relaxed); Ok(())
            }
            fn admit_operations(&self, count: u64) -> io::Result<()> { self.control.admit_operations(count) }
            fn record_operations(&self, count: u64) -> io::Result<()> { self.control.record_operations(count) }
            fn admit_write_operations(&self, count: u64) -> io::Result<()> { self.control.admit_write_operations(count) }
            fn record_write_operations(&self, count: u64) -> io::Result<()> { self.control.record_write_operations(count) }
            fn admit_durability(&self) -> io::Result<()> { self.control.admit_durability() }
        }

        #[test]
        fn physical_output_and_one_byte_overflow_probe_keep_exact_custody() -> io::Result<()> {
            let (reader, mut writer) = UnixStream::pair()?;
            writer.write_all(b"abcd")?;
            let mut pipe = Some(reader);
            let mut output = Vec::with_capacity(3);
            let address = output.as_ptr();
            let meter = DataMeter::new(4, 0, 0);
            let mut remaining = 3;
            assert!(drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))?);
            assert_eq!(output, b"abc");
            assert_eq!(remaining, 0);
            assert_eq!(drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))
                .expect_err("real overflow byte").kind(), io::ErrorKind::FileTooLarge);
            assert_eq!(output, b"abc");
            assert_eq!(output.as_ptr(), address);
            assert_eq!(meter.read_bytes.load(Relaxed), 4);
            assert_eq!(meter.held_read.load(Relaxed), 0);
            assert_eq!(meter.control.counts.reads.load(Relaxed), 4);
            assert_eq!(meter.control.counts.writes.load(Relaxed), 1);
            assert_eq!(meter.control.counts.copied.load(Relaxed), 0);
            assert_eq!(meter.control.counts.allocations.load(Relaxed), 0);
            Ok(())
        }

        #[test]
        fn denied_capacity_and_probe_leave_unread_native_bytes() -> io::Result<()> {
            let (reader, mut writer) = UnixStream::pair()?;
            writer.write_all(b"abcd")?;
            let mut pipe = Some(reader);
            let meter = DataMeter::new(3, 0, 0);
            let mut output = Vec::with_capacity(2);
            let mut remaining = 3;
            assert_eq!(drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))
                .expect_err("no caller-owned capacity").kind(), io::ErrorKind::OutOfMemory);
            assert_eq!(meter.control.counts.reads.load(Relaxed), 0);
            output = Vec::with_capacity(3);
            drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))?;
            assert_eq!(drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))
                .expect_err("overflow probe needs its own byte").kind(), io::ErrorKind::FileTooLarge);
            assert_eq!(meter.control.counts.reads.load(Relaxed), 2);
            let mut byte = [0];
            assert_eq!(platform::read_pipe(pipe.as_mut().expect("owned reader"), &mut byte)?, Some(1));
            assert_eq!(byte, *b"d");
            assert_eq!(output, b"abc");
            Ok(())
        }

        #[test]
        fn idle_and_eof_refund_bytes_without_copying_or_allocation() -> io::Result<()> {
            let (reader, writer) = UnixStream::pair()?;
            let mut pipe = Some(reader);
            let mut output = Vec::with_capacity(8);
            let meter = DataMeter::new(8, 0, 0);
            let mut remaining = 8;
            assert!(!drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))?);
            assert_eq!(meter.held_read.load(Relaxed), 0);
            drop(writer);
            assert!(drain_pipe(&mut pipe, &mut output, &mut remaining, Some(&meter))?);
            assert!(pipe.is_none());
            assert!(output.is_empty());
            assert_eq!(meter.read_bytes.load(Relaxed), 0);
            assert_eq!(meter.control.counts.copied.load(Relaxed), 0);
            assert_eq!(meter.control.counts.allocations.load(Relaxed), 0);
            Ok(())
        }

        #[test]
        fn exchange_copies_only_actual_serializer_bytes_into_original_capacity() -> io::Result<()> {
            let meter = DataMeter::new(0, 0, 5);
            let buffer = Vec::with_capacity(5);
            let address = buffer.as_ptr();
            let mut input = PendingStdin::new_with_buffer(
                CaptureInput::Exchange { initial: b"ab", maximum: 5 }, Some(buffer), Some(&meter),
            )?;
            input.observe(b"response", &mut |_| Ok(ProcessInput {
                bytes: b"cde".to_vec(), complete: false,
            }), Some(&meter))?;
            assert_eq!(input.bytes.as_ref(), b"abcde");
            assert_eq!(input.bytes.as_ptr(), address);
            input.observe(b"empty", &mut |_| Ok(ProcessInput::default()), Some(&meter))?;
            assert_eq!(meter.control.counts.copied.load(Relaxed), 5);
            assert_eq!(input.observe(b"overflow", &mut |_| Ok(ProcessInput {
                bytes: b"x".to_vec(), complete: false,
            }), Some(&meter)).expect_err("full exchange buffer").kind(), io::ErrorKind::FileTooLarge);
            assert_eq!(input.bytes.as_ref(), b"abcde");
            assert_eq!(meter.control.counts.allocations.load(Relaxed), 0);
            assert_eq!(meter.control.counts.live.load(Relaxed), 0);
            Ok(())
        }

        #[test]
        fn real_partial_stdin_and_cancel_keep_exact_written_prefix() -> io::Result<()> {
            let mut command = std::process::Command::new("/bin/sh");
            command.args(["-c", "sleep 60"]).stdin(std::process::Stdio::piped());
            let mut tree = ProcessTree::spawn(&mut command)?;
            let bytes = vec![b'x'; 1024 * 1024];
            let meter = DataMeter::new(0, bytes.len() as u64, 0);
            let mut input = PendingStdin::new_with_buffer(
                CaptureInput::Literal(&bytes), None, Some(&meter),
            )?;
            assert!(input.poll(&mut tree, Some(&meter))?);
            assert!(input.written > 0 && input.written < bytes.len());
            let prefix = input.written;
            assert_eq!(meter.write_bytes.load(Relaxed), prefix as u64);
            assert_eq!(meter.held_write.load(Relaxed), 0);
            assert!(!input.poll(&mut tree, Some(&meter))?);
            assert_eq!(input.written, prefix);
            assert_eq!(meter.control.counts.reads.load(Relaxed), 2);
            assert_eq!(meter.control.counts.writes.load(Relaxed), 3);
            meter.control.cancelled.store(true, std::sync::atomic::Ordering::Release);
            assert_eq!(input.poll(&mut tree, Some(&meter)).expect_err("real caller cancellation").kind(),
                io::ErrorKind::Interrupted);
            assert_eq!(meter.write_bytes.load(Relaxed), prefix as u64);
            assert_eq!(meter.control.counts.copied.load(Relaxed), 0);
            tree.terminate()?;
            Ok(())
        }

        #[test]
        fn callback_cancellation_retains_captured_prefix_and_original_owner() -> io::Result<()> {
            let mut command = std::process::Command::new("/bin/sh");
            command.args(["-c", "printf out; printf err >&2; sleep 60"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let mut tree = ProcessTree::spawn(&mut command)?;
            let meter = DataMeter::new(32, 0, 0);
            let failure = tree.wait_with_exchange_observed_with_meter(
                std::time::Duration::from_secs(5), 16, 1, b"",
                [Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)],
                &meter, || Ok(()), |_| {
                    meter.control.cancelled.store(true, std::sync::atomic::Ordering::Release);
                    Err(io::ErrorKind::Interrupted.into())
                },
            ).expect_err("caller cancels after physical stdout delivery");
            assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
            assert_eq!(failure.stdout, b"out");
            assert!(b"err".starts_with(&failure.stderr));
            assert_eq!(meter.read_bytes.load(Relaxed), (failure.stdout.len() + failure.stderr.len()) as u64);
            assert!(!failure.cleanup_completed);
            assert!(!tree.is_reaped());
            assert!(tree.child.as_ref().expect("same retained child").stdout.is_some());
            assert_eq!(meter.control.counts.allocations.load(Relaxed), 0);
            assert_eq!(meter.control.counts.copied.load(Relaxed), 0);
            tree.terminate()?;
            Ok(())
        }

        #[test]
        fn callback_panic_preserves_original_meter_work_and_pipe_owner() -> io::Result<()> {
            let mut command = std::process::Command::new("/bin/sh");
            command.args(["-c", "printf out; sleep 60"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let mut tree = ProcessTree::spawn(&mut command)?;
            let meter = DataMeter::new(32, 0, 0);
            let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tree.wait_with_exchange_observed_with_meter(
                    std::time::Duration::from_secs(5), 16, 1, b"",
                    [Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)],
                    &meter, || Ok(()), |_| panic!("real protocol observer panic"),
                )
            }));
            assert!(stopped.is_err());
            assert_eq!(meter.read_bytes.load(Relaxed), 3);
            assert_eq!(meter.held_read.load(Relaxed), 0);
            assert!(!tree.is_reaped());
            assert!(tree.child.as_ref().expect("original supervisor").stdout.is_some());
            assert!(!tree.cleanup_on_drop, "Root must retain explicit finite cleanup custody");
            assert_eq!(meter.control.counts.copied.load(Relaxed), 0);
            tree.terminate()?;
            Ok(())
        }
    }

    #[test]
    #[cfg(unix)]
    fn reserved_control_probe_refunds_unused_credit_and_drains_after_cancellation() -> std::io::Result<()> {
        use crate::{NativeOperationKind, NativeOperationReservation, NativeReadMeter as _};
        let meter = ControlMeter::new(1, 0, 0, 0);
        {
            let _unused = NativeOperationReservation::new(&meter, NativeOperationKind::Read, 1)?;
            assert!(meter.admit_operations(1).is_err());
            assert_eq!(meter.counts.reads.load(Relaxed), 0);
        }
        meter.admit_operations(1)?;
        let pid = std::process::id() as libc::pid_t;
        let mut accepted = NativeOperationReservation::new(&meter, NativeOperationKind::Read, 1)?;
        meter.cancelled.store(true, std::sync::atomic::Ordering::Release);
        let (alive, recorded) = accepted.attempt(|| {
            // SAFETY: signal zero only observes the actual current process.
            unsafe { libc::kill(pid, 0) }
        });
        recorded?;
        assert_eq!(alive, 0);
        assert_eq!(meter.counts.reads.load(Relaxed), 1);
        assert_eq!(meter.counts.reserved_reads.load(Relaxed), 0);
        assert_eq!(meter.counts.writes.load(Relaxed), 0);
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn closed_spawn_exact_native_prefix_retains_one_same_engine_leader() -> std::io::Result<()> {
        use crate::NativeReadMeter as _;
        let bytes = (4 * std::mem::size_of::<*mut libc::c_char>()) as u64;
        let mut custody = None;
        let denied = ControlMeter::new(0, 9, 1, bytes);
        assert!(ProcessTree::spawn_closed_measured(c"/bin/sh", &[c"-c", c"exit 7"], &denied, &mut custody).is_err());
        assert!(custody.is_none(), "denied real spawn cannot publish a PID");
        assert_eq!(denied.counts.writes.load(Relaxed), 9, "seven setup leaves and two real destroys");
        assert_eq!(denied.counts.live.load(Relaxed), 0);
        let exact = ControlMeter::new(1, 10, 1, bytes);
        ProcessTree::spawn_closed_measured(c"/bin/sh", &[c"-c", c"exit 7"], &exact, &mut custody)?;
        assert_eq!(exact.counts.writes.load(Relaxed), 10);
        assert_eq!(exact.counts.allocations.load(Relaxed), 1);
        assert_eq!(exact.counts.peak.load(Relaxed), bytes);
        assert_eq!(exact.counts.live.load(Relaxed), 0);
        assert_eq!(exact.counts.items.load(Relaxed), 3);
        assert_eq!(exact.counts.copied.load(Relaxed), bytes - std::mem::size_of::<*mut libc::c_char>() as u64);
        let mut tree = custody.take().expect("actual owned leader");
        assert_eq!(tree.wait(std::time::Duration::from_secs(5))?.code(), Some(7));
        assert_eq!(tree.try_wait_measured(&exact)?.expect("same unreaped leader").code(), Some(7));
        assert_eq!(exact.counts.reads.load(Relaxed), 1);
        assert!(!tree.is_reaped());
        tree.terminate()?;
        assert!(tree.is_reaped());
        let cancelled = ControlMeter::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        cancelled.cancelled.store(true, std::sync::atomic::Ordering::Release);
        assert!(ProcessTree::spawn_closed_measured(c"/bin/sh", &[c"-c", c"exit 0"], &cancelled, &mut custody).is_err());
        assert!(custody.is_none());
        assert_eq!(cancelled.counts.allocations.load(Relaxed), 0);
        assert!(cancelled.check().is_err());
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn measured_termination_denied_observation_keeps_killed_unreaped_identity() -> std::io::Result<()> {
        let spawn = ControlMeter::new(u64::MAX, 10, 1, u64::MAX);
        let mut custody = None;
        ProcessTree::spawn_closed_measured(c"/bin/sh", &[c"-c", c"sleep 30"], &spawn, &mut custody)?;
        let mut tree = custody.take().expect("actual child");
        let cancelled = ControlMeter::new(0, 1, 0, 0);
        cancelled.cancelled.store(true, std::sync::atomic::Ordering::Release);
        assert!(tree.terminate_measured(&cancelled).is_err());
        assert!(tree.try_wait()?.is_none(), "cancelled control leaves the real child alive");
        assert_eq!(cancelled.counts.writes.load(Relaxed), 0);
        let prefix = ControlMeter::new(0, 1, 0, 0);
        assert!(tree.terminate_measured(&prefix).is_err());
        assert_eq!(prefix.counts.writes.load(Relaxed), 1, "the real group signal was sent");
        assert_eq!(prefix.counts.reads.load(Relaxed), 0, "denied waitid did not run");
        assert!(tree.native_child.is_some(), "no premature reap releases the group identity");
        assert!(!tree.cleanup_on_drop, "failed measured cleanup cannot secretly spend unbounded work");
        tree.wait(std::time::Duration::from_secs(5))?;
        let exact = ControlMeter::new(2, 0, 0, 0);
        tree.terminate_measured(&exact)?;
        assert_eq!(exact.counts.reads.load(Relaxed), 2, "one observation and one physical reap");
        assert_eq!(exact.counts.writes.load(Relaxed), 0, "already-delivered signal is not repeated");
        assert!(tree.is_reaped());
        Ok(())
    }
    #[test]
    #[cfg(unix)]
    fn measured_termination_releases_only_actual_retained_standard_pipes() -> std::io::Result<()> {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 30"]).stdin(Stdio::piped())
            .stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command)?;
        let prefix = ControlMeter::new(0, 1, 0, 0);
        assert!(tree.terminate_measured(&prefix).is_err());
        assert_eq!(prefix.counts.writes.load(Relaxed), 1, "group signal is the actual prefix");
        let child = tree.child.as_ref().expect("unreaped leader");
        assert!(child.stdin.is_some() && child.stdout.is_some() && child.stderr.is_some());
        tree.wait(std::time::Duration::from_secs(5))?;
        // Ordinary wait closed stdin while observing the actual leader exit.
        let exact = ControlMeter::new(2, 2, 0, 0);
        tree.terminate_measured(&exact)?;
        assert_eq!(exact.counts.writes.load(Relaxed), 2, "only the two still-owned output pipes close");
        assert_eq!(exact.counts.reads.load(Relaxed), 2, "one retained-leader observation and one reap");
        assert!(tree.is_reaped());
        Ok(())
    }

    use std::fs;
    use std::io::{Read as _, Write as _};
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    const MODE: &str = "ACYCLIC_PROCESS_TREE_TEST_MODE";
    const ROOT: &str = "ACYCLIC_PROCESS_TREE_TEST_ROOT";
    const BULK_BYTES: usize = 4 << 20;

    pub(super) fn command(mode: &str, root: &std::path::Path) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args([
                "--exact",
                "process_tree::tests::process_tree_helper",
                "--nocapture",
            ])
            .env_clear()
            .env(MODE, mode)
            .env(ROOT, root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    /// Waits for a helper's readiness marker. Helper start-up has no useful
    /// bound under load, so this waits for the marker itself and fails only
    /// once the helper has `exited` without writing it.
    pub(super) fn ready(root: &std::path::Path, name: &str, mut exited: impl FnMut() -> bool) {
        while !root.join(name).exists() {
            assert!(
                !exited() || root.join(name).exists(),
                "helper exited before it was ready"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn tree_exited(tree: &mut ProcessTree) -> bool {
        tree.try_wait().map_or(true, |status| status.is_some())
    }

    /// Asks any descendant that outlived its tree's cleanup to mark its
    /// escape, then checks that none did.
    pub(super) fn assert_no_escape(root: &std::path::Path) {
        fs::write(root.join("probe"), b"probe").expect("escape probe");
        thread::sleep(Duration::from_millis(500));
        assert!(!root.join("escaped").exists());
    }

    #[test]
    #[allow(
        clippy::zombie_processes,
        reason = "exit-parent deliberately tests parent-before-descendant exit"
    )]
    fn process_tree_helper() {
        let Ok(mode) = std::env::var(MODE) else {
            return;
        };
        let root = std::path::PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
        match mode.as_str() {
            "grandchild" => {
                fs::write(root.join("grandchild-ready"), b"ready").expect("grandchild ready");
                // Live until the probe written after cleanup, so the tree's
                // lifetime never races the test; bounded so no escapee lingers.
                let deadline = Instant::now() + Duration::from_secs(60);
                while !root.join("probe").exists() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(5));
                }
                if root.join("probe").exists() {
                    fs::write(root.join("escaped"), b"descendant survived")
                        .expect("escaped marker");
                }
            }
            "child" | "exit-parent" => {
                let mut grandchild = command("grandchild", &root);
                grandchild.stdout(Stdio::inherit()).stderr(Stdio::inherit());
                let mut grandchild = grandchild.spawn().expect("spawn grandchild");
                ready(&root, "grandchild-ready", || {
                    grandchild
                        .try_wait()
                        .map_or(true, |status| status.is_some())
                });
                fs::write(root.join("tree-ready"), b"ready").expect("tree ready");
                if mode == "child" {
                    let _ = grandchild.wait();
                }
            }
            "flood" => loop {
                std::io::stdout()
                    .write_all(&[b'o'; 8192])
                    .expect("stdout flood");
                std::io::stderr()
                    .write_all(&[b'e'; 8192])
                    .expect("stderr flood");
            },
            "partial" => {
                std::io::stdout()
                    .write_all(b"stdout-before-stop")
                    .expect("partial stdout");
                std::io::stderr()
                    .write_all(b"stderr-before-stop")
                    .expect("partial stderr");
                std::io::stdout().flush().expect("flush partial stdout");
                std::io::stderr().flush().expect("flush partial stderr");
                fs::write(root.join("partial-ready"), b"ready").expect("partial ready");
                thread::sleep(Duration::from_secs(30));
            }
            "output" => {
                // The host cleared the environment. Generic containment must
                // neither restore ambient credentials nor discard explicit input.
                assert_eq!(
                    std::env::var("EXPLICIT_PROCESS_INPUT").expect("explicit input"),
                    "allowed"
                );
                assert!(std::env::var_os("PATH").is_none());
                assert!(std::env::var_os("HOME").is_none());
                assert!(std::env::var_os("CODEX_HOME").is_none());
                std::io::stdout()
                    .write_all(b"stdout-marker")
                    .expect("stdout");
                std::io::stderr()
                    .write_all(b"stderr-marker")
                    .expect("stderr");
                let mut input = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut input)
                    .expect("closed stdin");
                assert!(input.is_empty());
                // Avoid test-harness stdout in the exact shared-budget fixture.
                std::process::exit(0);
            }
            "bulk" => {
                let chunk = [b'b'; 64 * 1024];
                for _ in 0..BULK_BYTES / chunk.len() {
                    std::io::stdout().write_all(&chunk).expect("bulk stdout");
                }
                std::process::exit(0);
            }
            "stdin-stall" | "stdin-echo" | "stdin-handshake" => stdin_helper(&mode, &root),
            "exit-code" => std::process::exit(7),
            _ => assert_eq!(mode, "known helper mode"),
        }
    }

    fn stdin_helper(mode: &str, root: &std::path::Path) {
        match mode {
            "stdin-stall" => {
                std::io::stdout()
                    .write_all(b"stdin-stall-marker")
                    .expect("stall marker");
                std::io::stdout().flush().expect("flush stall marker");
                fs::write(root.join("stdin-stall-ready"), b"ready").expect("stall ready");
                thread::sleep(Duration::from_secs(30));
            }
            "stdin-echo" => {
                let mut input = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut input)
                    .expect("input through EOF");
                std::io::stdout()
                    .write_all(&input)
                    .expect("echo exact input");
                std::process::exit(0);
            }
            "stdin-handshake" => {
                let mut first = [0; 6];
                std::io::stdin()
                    .read_exact(&mut first)
                    .expect("first input");
                assert_eq!(&first, b"first\n");
                std::io::stdout()
                    .write_all(b"first-ack\n")
                    .expect("first ack");
                std::io::stdout().flush().expect("flush first ack");
                let mut second = [0; 7];
                std::io::stdin()
                    .read_exact(&mut second)
                    .expect("second input");
                assert_eq!(&second, b"second\n");
                std::io::stderr()
                    .write_all(b"exchange-stderr\n")
                    .expect("exchange stderr");
                std::io::stderr().flush().expect("flush exchange stderr");
                std::io::stdout()
                    .write_all(b"second-ack\n")
                    .expect("second ack");
                std::io::stdout().flush().expect("flush second ack");
                thread::sleep(Duration::from_secs(30));
            }
            _ => panic!("unknown stdin helper mode"),
        }
    }

    #[test]
    fn spawn_failure_does_not_return_an_unowned_child() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let error = ProcessTree::spawn(&mut Command::new(temporary.path().join("missing")))
            .err()
            .expect("missing executable must not spawn");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn cleanup_is_idempotent_and_drop_contains_descendants() {
        for explicit in [false, true] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let mut tree =
                ProcessTree::spawn(&mut command("child", temporary.path())).expect("spawn tree");
            ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
            if explicit {
                tree.terminate().expect("terminate tree");
                tree.terminate().expect("repeat termination");
                assert!(tree.child.is_none());
            }
            drop(tree);
            assert_no_escape(temporary.path());
        }
    }

    #[test]
    fn capture_check_failure_cleans_up_the_owned_tree() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command("child", temporary.path())).expect("spawn tree");
        let mut checks = 0;
        let error = tree
            .wait_with_output_checked(Duration::from_secs(5), 4096, || {
                checks += 1;
                if temporary.path().join("tree-ready").exists() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    ))
                } else {
                    Ok(())
                }
            })
            .expect_err("cancellation must stop capture");
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(checks > 0);
        assert!(tree.child.is_none(), "explicit cleanup reaps the child");
        thread::sleep(Duration::from_secs(1));
        assert!(!temporary.path().join("escaped").exists());
    }

    #[test]
    fn owned_streaming_stdout_rejects_empty_reads_and_reaches_eof() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("output", temporary.path());
        command.env("EXPLICIT_PROCESS_INPUT", "allowed");
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn streaming tree");
        assert_eq!(
            tree.read_stdout(&mut []).expect_err("empty read").kind(),
            std::io::ErrorKind::InvalidInput
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut bytes = Vec::new();
        loop {
            let mut buffer = [0; 128];
            match tree.read_stdout(&mut buffer).expect("read owned stdout") {
                Some(0) => break,
                Some(read) => bytes.extend_from_slice(buffer.get(..read).expect("valid pipe read")),
                None => thread::sleep(Duration::from_millis(1)),
            }
            assert!(Instant::now() < deadline, "streaming output deadline");
        }
        tree.terminate().expect("clean streaming tree");
        assert!(bytes.ends_with(b"stdout-marker"));
    }

    #[test]
    fn graceful_cleanup_handles_an_exited_leader_and_its_descendants() {
        for mode in ["exit-code", "exit-parent", "child"] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let mut tree =
                ProcessTree::spawn(&mut command(mode, temporary.path())).expect("spawn tree");
            if mode != "exit-code" {
                ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
            }
            if mode != "child" {
                tree.wait(Duration::from_secs(5))
                    .expect("observe leader exit");
            }
            let status = tree
                .terminate_after(Duration::from_secs(1))
                .expect("graceful tree cleanup");
            if mode == "exit-code" {
                assert_eq!(status.code(), Some(7));
            }
            assert!(tree.child.is_none());
            assert_no_escape(temporary.path());
        }
    }

    /// Native file operations and blocking tasks run on workers, which must
    /// enter the caller's span so work nested in them keeps it.
    fn native_operations_in_caller_span(dispatch: &tracing::Dispatch, root: &std::path::Path) {
        fn block_on<F: std::future::Future>(future: F) -> F::Output {
            let mut future = std::pin::pin!(future);
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            loop {
                if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
                    return output;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
        let nested = |name: &'static str| {
            let dispatch = dispatch.clone();
            move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    drop(tracing::trace_span!("nested", name));
                });
            }
        };
        let file = crate::NativeFile::from_file(
            std::fs::File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(root.join("native"))
                .expect("open native file"),
        )
        .expect("native file");
        tracing::info_span!("test.caller").in_scope(|| {
            block_on(file.write_all_batch_async(vec![crate::OwnedWrite {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"traced"),
            }]))
            .expect("write");
            block_on(file.read_batch_async(vec![crate::OwnedRead {
                offset: 1,
                length: 5,
            }]))
            .expect("read");
            block_on(file.sync_async(crate::Durability::Full)).expect("sync");
            let control = nested("control");
            block_on(file.control_async(move |_| {
                control();
                Ok(())
            }))
            .expect("control");
            block_on(crate::run_blocking_io(nested("blocking"))).expect("blocking");
        });
    }

    fn assert_native_operation_spans(has: &dyn Fn(&str, &str, &str) -> bool) {
        for span in [
            "write_batch",
            "read_batch",
            "sync",
            "control",
            "blocking_io",
        ] {
            let span = format!("acyclic.runtime.{span}");
            assert!(has(&span, "parent", "test.caller"), "{span} parent");
            assert!(has(&span, "outcome", "ok"), "{span} outcome");
        }
        for (span, field, value) in [
            ("acyclic.runtime.write_batch", "bytes", "6"),
            ("acyclic.runtime.read_batch", "bytes", "5"),
            ("acyclic.runtime.read_batch", "batch_len", "1"),
            ("nested", "parent", "acyclic.runtime.control"),
            ("nested", "parent", "acyclic.runtime.blocking_io"),
        ] {
            assert!(has(span, field, value), "{span} {field}");
        }
    }

    type Seen = std::sync::Arc<std::sync::Mutex<Vec<(&'static str, &'static str, String)>>>;

    fn capture_process_spans(filtered: bool) -> (tracing::Dispatch, Seen) {
        use std::sync::Arc;
        use tracing::field::{Field, Visit};
        use tracing_subscriber::Layer as _;
        use tracing_subscriber::layer::{Context, SubscriberExt as _};

        struct Capture(Seen);
        struct Fields<'a>(
            &'static str,
            &'a mut Vec<(&'static str, &'static str, String)>,
        );
        impl Visit for Fields<'_> {
            fn record_str(&mut self, field: &Field, value: &str) {
                self.1.push((self.0, field.name(), value.to_owned()));
            }
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.1.push((self.0, field.name(), format!("{value:?}")));
            }
        }
        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attributes: &tracing::span::Attributes<'_>,
                id: &tracing::span::Id,
                context: Context<'_, S>,
            ) {
                let name = attributes.metadata().name();
                let mut seen = self.0.lock().unwrap();
                if let Some(parent) = context.span(id).and_then(|span| span.parent()) {
                    seen.push((name, "parent", parent.name().to_owned()));
                }
                attributes.record(&mut Fields(name, &mut seen));
            }
            fn on_record(
                &self,
                id: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                context: Context<'_, S>,
            ) {
                let name = context.span(id).unwrap().name();
                values.record(&mut Fields(name, &mut self.0.lock().unwrap()));
            }
        }

        let seen = Seen::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(
            Capture(Arc::clone(&seen)).with_filter(tracing_subscriber::filter::filter_fn(
                move |metadata| !filtered || metadata.name() == "test.process.sentinel",
            )),
        ));
        (dispatch, seen)
    }

    #[test]
    fn collection_emits_spans_with_byte_counts_and_no_paths() {
        // With one live dispatcher, a callsite that a concurrent test reaches
        // first caches only that thread's (absent) interest; a second one
        // makes every callsite consult this test's subscriber too.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let (dispatch, seen) = capture_process_spans(false);
        let _default = tracing::dispatcher::set_default(&dispatch);
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("output", temporary.path());
        command.env("EXPLICIT_PROCESS_INPUT", "allowed");
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        let output = tree
            .wait_with_output_observed(Duration::from_secs(5), 4096, || Ok(()))
            .expect("collect output");
        let failure = tree
            .wait_with_output_observed(Duration::ZERO, 0, || Ok(()))
            .expect_err("consumed capture fails before admission");
        assert_eq!(failure.error.kind(), std::io::ErrorKind::Other);
        native_operations_in_caller_span(&dispatch, temporary.path());
        let seen = seen.lock().unwrap();
        let has = |span: &str, field: &str, value: &str| {
            seen.iter()
                .any(|(s, f, v)| *s == span && *f == field && v == value)
        };
        let bytes = (output.stdout.len() + output.stderr.len()).to_string();
        assert!(
            has("acyclic.runtime.wait_with_output", "bytes", &bytes),
            "{seen:?}"
        );
        assert!(has("acyclic.runtime.wait_with_output", "outcome", "ok"));
        assert!(has("acyclic.runtime.wait_with_output", "outcome", "err"));
        assert!(has(
            "acyclic.runtime.wait_with_output",
            "error.kind",
            "other"
        ));
        assert!(has("acyclic.runtime.terminate", "outcome", "ok"));
        assert!(has("acyclic.runtime.spawn", "outcome", "ok"));
        assert_native_operation_spans(&has);
        let program = std::env::current_exe().expect("test executable");
        let program = program.file_name().and_then(|name| name.to_str());
        assert!(has(
            "acyclic.runtime.spawn",
            "program",
            program.expect("name")
        ));
        let root = temporary.path().to_string_lossy();
        for (span, field, value) in seen.iter() {
            assert!(
                !["path", "token", "content", "body", "authorization"].contains(field),
                "{span} records {field}"
            );
            assert!(
                !value.contains(&*root) && !value.contains("allowed"),
                "{value}"
            );
        }
        drop(seen);

        // Filtering the operation spans must not redirect completion or byte
        // records onto an enabled caller with the same field names.
        let (filtered_dispatch, filtered) = capture_process_spans(true);
        tracing::dispatcher::with_default(&filtered_dispatch, || {
            tracing::info_span!(
                "test.process.sentinel",
                program = "sentinel",
                bytes = "sentinel",
                outcome = "sentinel",
                error.kind = "sentinel"
            )
            .in_scope(|| {
                let mut missing =
                    std::process::Command::new(temporary.path().join("missing-program"));
                assert!(ProcessTree::spawn(&mut missing).is_err());
                let mut command = self::command("output", temporary.path());
                command.env("EXPLICIT_PROCESS_INPUT", "allowed");
                let mut tree = ProcessTree::spawn(&mut command).expect("filtered spawn");
                assert!(
                    tree.wait(Duration::from_secs(5))
                        .expect("filtered wait")
                        .success()
                );
                let output = tree
                    .wait_with_output_observed(Duration::from_secs(5), 4096, || Ok(()))
                    .expect("filtered output");
                assert!(!output.stdout.is_empty());
                tree.terminate().expect("filtered repeated cleanup");
                assert!(tree.terminate_after(Duration::ZERO).is_err());
            });
        });
        let filtered = filtered.lock().unwrap();
        assert_eq!(
            filtered.len(),
            4,
            "caller fields were overwritten: {filtered:?}"
        );
        assert!(
            filtered.iter().all(|(span, _, value)| {
                *span == "test.process.sentinel" && value == "sentinel"
            })
        );
    }

    #[test]
    fn collection_closes_stdin_and_preserves_explicit_environment() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("output", temporary.path());
        command
            .env("EXPLICIT_PROCESS_INPUT", "allowed")
            .stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        let output = tree
            .wait_with_output(Duration::from_secs(5), 4096)
            .expect("collect output");
        assert!(output.status.success(), "{output:?}");
        assert!(output.stdout.ends_with(b"stdout-marker"));
        assert_eq!(output.stderr, b"stderr-marker");
        assert!(tree.child.is_none());
        assert!(tree.wait_with_output(Duration::ZERO, 0).is_err());
        tree.terminate().expect("repeat cleanup");
        let mut command = self::command("output", temporary.path());
        command.env("EXPLICIT_PROCESS_INPUT", "allowed");
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        assert_eq!(
            tree.wait_with_output(
                Duration::from_secs(5),
                output.stdout.len() + output.stderr.len() - 1
            )
            .expect_err("combined pipes exceed budget")
            .kind(),
            std::io::ErrorKind::FileTooLarge
        );
    }

    #[test]
    fn stdin_backpressure_preserves_intent_checks_and_cleanup() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-stall", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn stalled reader");
        ready(temporary.path(), "stdin-stall-ready", || {
            tree_exited(&mut tree)
        });
        let input = vec![b'i'; BULK_BYTES];
        let mut offset = 0;
        let started = Instant::now();
        loop {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "write must not block"
            );
            match tree
                .write_stdin(input.get(offset..).expect("unwritten suffix"))
                .expect("nonblocking stdin write")
            {
                Some(written) => {
                    assert!(written > 0 && written <= input.len() - offset);
                    offset += written;
                }
                None => break,
            }
        }
        assert!(offset > 0 && offset < input.len());
        let mut checks = 0;
        let failure = tree
            .wait_with_output_observed(Duration::from_secs(5), 4096, || {
                checks += 1;
                Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "host stopped",
                ))
            })
            .expect_err("intent stops stalled exchange");
        assert_eq!(checks, 1);
        assert_eq!(failure.error.kind(), std::io::ErrorKind::Interrupted);
        assert!(failure.cleanup_completed);
        assert!(tree.is_reaped());
        assert_eq!(
            tree.write_stdin(b"after cleanup")
                .expect_err("no stdin")
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn partial_stdin_writes_preserve_exact_bytes_and_explicit_eof() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-echo", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn input echo");
        assert_eq!(
            tree.write_stdin(&[]).expect_err("empty write").kind(),
            std::io::ErrorKind::InvalidInput
        );
        let input: Vec<u8> = (0..BULK_BYTES)
            .map(|index| u8::try_from(index % 251).expect("byte pattern"))
            .collect();
        let mut offset = 0;
        let deadline = Instant::now() + Duration::from_secs(60);
        while offset < input.len() {
            assert!(Instant::now() < deadline, "bounded input transfer");
            if let Some(written) = tree
                .write_stdin(input.get(offset..).expect("unwritten suffix"))
                .expect("write input")
            {
                assert!(written > 0 && written <= input.len() - offset);
                offset += written;
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
        tree.close_stdin();
        tree.close_stdin();
        assert_eq!(
            tree.write_stdin(b"after EOF")
                .expect_err("closed stdin")
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
        let output = tree
            .wait_with_output(Duration::from_secs(60), 2 * BULK_BYTES)
            .expect("collect echoed input");
        assert!(output.status.success());
        assert!(output.stdout.ends_with(&input));
        assert!(output.stderr.is_empty());
        assert!(tree.is_reaped());
    }

    #[test]
    fn streaming_exchange_uses_the_same_bounded_capture_and_cleanup_owner() {
        use super::ProcessInput;
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-handshake", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn exchange");
        let mut seen = Vec::new();
        let mut phase = 0;
        let output = tree
            .wait_with_exchange_observed(
                Duration::from_secs(5),
                4096,
                13,
                b"first\n",
                || Ok(()),
                |chunk| {
                    seen.extend_from_slice(chunk);
                    if phase == 0 && seen.ends_with(b"first-ack\n") {
                        seen.clear();
                        phase = 1;
                        Ok(ProcessInput {
                            bytes: b"second\n".to_vec(),
                            complete: false,
                        })
                    } else if phase == 1 && seen.ends_with(b"second-ack\n") {
                        phase = 2;
                        Ok(ProcessInput {
                            complete: true,
                            ..ProcessInput::default()
                        })
                    } else {
                        Ok(ProcessInput::default())
                    }
                },
            )
            .expect("complete streaming exchange");
        assert_eq!(phase, 2);
        assert!(output.stdout.ends_with(b"first-ack\nsecond-ack\n"));
        assert_eq!(output.stderr, b"exchange-stderr\n");
        assert!(
            !output.status.success(),
            "protocol completion terminates the stalled server"
        );
        assert!(tree.is_reaped());
    }

    #[test]
    fn streaming_input_overflow_parser_failure_and_deadline_preserve_cleanup() {
        use super::ProcessInput;
        for parser_failure in [false, true] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let mut command = command("stdin-handshake", temporary.path());
            command.stdin(Stdio::piped());
            let mut tree = ProcessTree::spawn(&mut command).expect("spawn exchange");
            let mut seen = Vec::new();
            let failure = tree
                .wait_with_exchange_observed(
                    Duration::from_secs(5),
                    4096,
                    6,
                    b"first\n",
                    || Ok(()),
                    |chunk| {
                        seen.extend_from_slice(chunk);
                        if seen.ends_with(b"first-ack\n") {
                            if parser_failure {
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "invalid protocol",
                                ))
                            } else {
                                Ok(ProcessInput {
                                    bytes: b"second\n".to_vec(),
                                    complete: false,
                                })
                            }
                        } else {
                            Ok(ProcessInput::default())
                        }
                    },
                )
                .expect_err("stop streaming exchange");
            assert_eq!(
                failure.error.kind(),
                if parser_failure {
                    std::io::ErrorKind::InvalidData
                } else {
                    std::io::ErrorKind::FileTooLarge
                }
            );
            assert!(failure.stdout.ends_with(b"first-ack\n"));
            assert!(failure.cleanup_completed);
            assert!(tree.is_reaped());
        }
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-stall", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn stalled exchange");
        ready(temporary.path(), "stdin-stall-ready", || {
            tree_exited(&mut tree)
        });
        let mut checks = 0;
        let failure = tree
            .wait_with_exchange_observed(
                Duration::from_millis(20),
                4096,
                BULK_BYTES,
                &vec![b'i'; BULK_BYTES],
                || {
                    checks += 1;
                    Ok(())
                },
                |_| Ok(ProcessInput::default()),
            )
            .expect_err("deadline stops stalled input");
        assert_eq!(failure.error.kind(), std::io::ErrorKind::TimedOut);
        assert!(failure.stdout.ends_with(b"stdin-stall-marker"));
        assert!(checks > 1);
        assert!(failure.cleanup_completed);
        assert!(tree.is_reaped());
    }

    #[test]
    fn stopped_capture_preserves_output_prefix_and_cleanup_observation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree = ProcessTree::spawn(&mut command("partial", temporary.path()))
            .expect("spawn partial output");
        ready(temporary.path(), "partial-ready", || tree_exited(&mut tree));
        let failure = tree
            .wait_with_output_observed(Duration::from_millis(20), 4096, || Ok(()))
            .expect_err("partial process times out");
        assert_eq!(failure.error.kind(), std::io::ErrorKind::TimedOut);
        assert!(failure.stdout.ends_with(b"stdout-before-stop"));
        assert_eq!(failure.stderr, b"stderr-before-stop");
        assert!(failure.cleanup_completed);
        assert!(failure.status.is_none());
        assert!(tree.is_reaped());
    }

    #[test]
    fn capture_timeout_and_output_overflow_reap_owned_tree() {
        for (mode, timeout, limit, kind) in [
            (
                "child",
                Duration::from_millis(20),
                4096,
                std::io::ErrorKind::TimedOut,
            ),
            (
                "flood",
                Duration::from_secs(5),
                16384,
                std::io::ErrorKind::FileTooLarge,
            ),
        ] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let mut tree =
                ProcessTree::spawn(&mut command(mode, temporary.path())).expect("spawn tree");
            if mode == "child" {
                ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
                assert_eq!(
                    tree.wait(Duration::ZERO).expect_err("wait deadline").kind(),
                    std::io::ErrorKind::TimedOut
                );
                assert!(tree.child.is_some());
            }
            let started = Instant::now();
            let error = tree
                .wait_with_output_observed(timeout, limit, || Ok(()))
                .expect_err("capture must fail");
            assert_eq!(error.error.kind(), kind);
            assert!(error.to_string().contains("effects may have occurred"));
            assert!(error.cleanup_completed);
            assert!(error.stdout.len() + error.stderr.len() <= limit);
            if mode == "flood" {
                assert_eq!(error.stdout.len() + error.stderr.len(), limit);
            }
            assert!(started.elapsed() < Duration::from_secs(7));
            assert!(tree.child.is_none());
            tree.terminate().expect("repeat cleanup");
            assert_no_escape(temporary.path());
        }
    }

    #[test]
    fn bulk_output_is_collected_fully_within_its_exact_bound() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let output = ProcessTree::spawn(&mut command("bulk", temporary.path()))
            .expect("spawn bulk writer")
            .wait_with_output(Duration::from_secs(60), 2 * BULK_BYTES)
            .expect("collect bulk output");
        assert!(output.status.success());
        // The test harness prints a short header before the helper runs.
        let header = output.stdout.len() - BULK_BYTES;
        assert!(
            output
                .stdout
                .get(header..)
                .expect("bulk bytes")
                .iter()
                .all(|byte| *byte == b'b')
        );
        assert!(output.stderr.is_empty());
        let error = ProcessTree::spawn(&mut command("bulk", temporary.path()))
            .expect("spawn bulk writer")
            .wait_with_output(Duration::from_secs(60), output.stdout.len() - 1)
            .expect_err("bulk output exceeds bound");
        assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    }

    #[test]
    fn exited_parent_does_not_leave_descendant_holding_output_open() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command("exit-parent", temporary.path())).expect("spawn tree");
        let output = tree
            .wait_with_output(Duration::from_secs(5), 4096)
            .expect("collect exited parent");
        assert!(output.status.success());
        ready(temporary.path(), "tree-ready", || true);
        assert_no_escape(temporary.path());
    }

    #[cfg(unix)]
    #[test]
    fn exit_observation_retains_group_leader_until_cleanup() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("output", temporary.path());
        command.env("EXPLICIT_PROCESS_INPUT", "allowed");
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while tree.try_wait().expect("poll exit").is_none() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            tree.wait(Duration::from_secs(5))
                .expect("wait exit")
                .success()
        );
        // A second real waitid observation fails with ECHILD if try_wait or
        // wait reaped the leader, releasing its PID for reuse before cleanup.
        assert!(
            super::platform::observe_exit(tree.child.as_ref().expect("retained child"))
                .expect("leader remains waitable")
                .expect("exited leader")
                .success()
        );
        tree.terminate().expect("terminate and reap");
        assert!(tree.child.is_none());
        let mut tree = ProcessTree::spawn(&mut self::command("exit-code", temporary.path()))
            .expect("spawn nonzero exit");
        assert_eq!(
            tree.wait(Duration::from_secs(5))
                .expect("nonzero exit")
                .code(),
            Some(7)
        );
        tree.terminate().expect("reap nonzero exit");

        let mut tree = ProcessTree::spawn(&mut self::command("child", temporary.path()))
            .expect("spawn signalled child");
        tree.terminate_descendants().expect("signal tree");
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(
            tree.wait(Duration::from_secs(5))
                .expect("signal exit")
                .signal(),
            Some(libc::SIGKILL)
        );
        tree.terminate().expect("reap signal exit");
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::io::{Read, Write};
    use std::mem::{size_of, zeroed};
    use std::os::windows::process::CommandExt as _;
    use std::process::Command;
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_BROKEN_PIPE, ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Pipes::{PIPE_NOWAIT, PeekNamedPipe, SetNamedPipeHandleState};
    use windows_sys::Win32::System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
    };

    pub(super) use std::os::windows::io::AsRawHandle as Pipe;

    pub(super) fn write_pipe(
        pipe: &mut (impl Write + Pipe), bytes: &[u8],
    ) -> io::Result<Option<usize>> {
        let (written, recorded) = write_pipe_with_meter(pipe, bytes, None)?;
        recorded?;
        Ok(written)
    }

    pub(super) fn write_pipe_with_meter(
        pipe: &mut (impl Write + Pipe), bytes: &[u8],
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<usize>, io::Result<()>)> {
        let mode = PIPE_NOWAIT;
        let (changed, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Write, || {
                // SAFETY: this exclusively owned byte pipe has no pending I/O.
                if unsafe { SetNamedPipeHandleState(
                    pipe.as_raw_handle().cast(), &mode, std::ptr::null(), std::ptr::null(),
                ) } == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
            },
        )?;
        recorded?;
        changed?;
        let mut candidate = bytes;
        loop {
            let mut actual = 0;
            let (written, recorded) = super::pipe_operation_attempt(
                meter, crate::NativeOperationKind::Write, || {
                    // SAFETY: synchronous WriteFile borrows the existing input
                    // only for this call and reports its physical prefix.
                    if unsafe { windows_sys::Win32::Storage::FileSystem::WriteFile(
                        pipe.as_raw_handle().cast(), candidate.as_ptr(),
                        candidate.len().min(u32::MAX as usize) as u32,
                        &raw mut actual, std::ptr::null_mut(),
                    ) } == 0 { Err(io::Error::last_os_error()) } else { Ok(actual as usize) }
                },
            )?;
            if actual != 0 { return Ok((Some(actual as usize), recorded.and(written.map(|_| ())))); }
            match written {
                // Preserve the original logarithmic backpressure probes.
                Ok(0) if candidate.len() > 1 => {
                    recorded?;
                    candidate = candidate.get(..candidate.len() / 2)
                        .ok_or_else(|| io::Error::other("invalid process write prefix"))?;
                }
                Ok(0) => return Ok((None, recorded)),
                Err(error) if error.kind() == io::ErrorKind::Interrupted =>
                    return Ok((None, recorded)),
                Err(error) => { recorded?; return Err(error); }
                Ok(written) => return Ok((Some(written), recorded)),
            }
        }
    }

    pub(super) fn read_pipe(
        pipe: &mut (impl Read + Pipe), buffer: &mut [u8],
    ) -> io::Result<Option<usize>> {
        // SAFETY: initialized bytes may also be borrowed as MaybeUninit bytes.
        let buffer = unsafe { std::slice::from_raw_parts_mut(
            buffer.as_mut_ptr().cast(), buffer.len(),
        ) };
        let (read, recorded) = read_pipe_with_meter(pipe, buffer, None)?;
        recorded?;
        Ok(read)
    }

    pub(super) fn read_pipe_with_meter(
        pipe: &mut (impl Read + Pipe),
        buffer: &mut [std::mem::MaybeUninit<u8>],
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<usize>, io::Result<()>)> {
        let mut available = 0;
        let (peek, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Read, || {
                // SAFETY: peek observes this live exclusively owned pipe.
                if unsafe { PeekNamedPipe(
                    pipe.as_raw_handle().cast(), std::ptr::null_mut(), 0,
                    std::ptr::null_mut(), &raw mut available, std::ptr::null_mut(),
                ) } == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
            },
        )?;
        match peek {
            Err(error) if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.cast_signed()) =>
                return Ok((Some(0), recorded)),
            Err(error) => { recorded?; return Err(error); }
            Ok(()) => {}
        }
        if available == 0 { return Ok((None, recorded)); }
        recorded?;
        let length = buffer.len().min(available as usize).min(u32::MAX as usize);
        let mut actual = 0;
        let (read, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Read, || {
                // SAFETY: synchronous ReadFile initializes only the returned
                // prefix of this exclusively borrowed writable allocation.
                if unsafe { windows_sys::Win32::Storage::FileSystem::ReadFile(
                    pipe.as_raw_handle().cast(), buffer.as_mut_ptr().cast(),
                    length as u32, &raw mut actual, std::ptr::null_mut(),
                ) } == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
            },
        )?;
        // A failed synchronous call may still report a transferred prefix.
        // Publish it and defer its error until capture has retained the bytes.
        let recorded = recorded.and(read);
        Ok((Some(actual as usize), recorded))
    }

    pub(super) struct Guard {
        job: HANDLE,
        active: bool,
        pub(super) assigned: bool,
        termination_sent: bool,
        pub(super) direct_termination_sent: bool,
        pub(super) cleanup_on_drop: bool,
        snapshot: HANDLE,
        thread: HANDLE,
    }

    #[test]
    fn measured_windows_spawn_denial_retains_each_actual_native_prefix() -> io::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let temporary = tempfile::tempdir()?;
        for writes in 0..=4 {
            let meter = super::tests::ControlMeter::new(0, writes, 0, 0);
            let mut command = super::tests::command("child", temporary.path());
            let mut custody = None;
            assert!(crate::spawn_process_tree_with_meter_custody(&mut command, &meter, &mut custody).is_err());
            assert_eq!(meter.counts.writes.load(Relaxed), writes);
            assert_eq!(meter.counts.reads.load(Relaxed), 0, "denied snapshot SDK never ran");
            assert_eq!(meter.counts.reserved_writes.load(Relaxed), 0);
            if writes == 0 {
                assert!(custody.is_none(), "denied CreateJob cannot create native custody");
            } else {
                let mut tree = custody.take().expect("original native owner");
                assert!(!tree.guard.job.is_null());
                assert!(!tree.guard.cleanup_on_drop);
                assert!(!tree.cleanup_on_drop);
                assert_eq!(tree.child.is_some(), writes >= 3, "actual suspended Child, not a guessed spawn");
                assert_eq!(tree.guard.assigned, writes == 4, "only accepted real assignment enters the Job");
                assert!(!tree.is_reaped());
                // Tests explicitly settle the same owner, never Drop fallback.
                tree.terminate()?;
                assert!(tree.is_reaped());
            }
        }
        Ok(())
    }

    #[test]
    fn measured_windows_spawn_counts_observable_sdk_leaves_and_explicit_terminal_control() -> io::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let temporary = tempfile::tempdir()?;
        let meter = super::tests::ControlMeter::new(u64::MAX, u64::MAX, 0, 0);
        let mut command = super::tests::command("exit-code", temporary.path());
        let mut custody = None;
        crate::spawn_process_tree_with_meter_custody(&mut command, &meter, &mut custody)?;
        let mut tree = custody.take().expect("same original process owner");
        assert_eq!(meter.counts.writes.load(Relaxed), 7,
            "CreateJob, limits, opaque spawn, assignment, resume, thread close, snapshot close");
        assert!(meter.counts.reads.load(Relaxed) >= 3,
            "actual snapshot, first/next entry visits and thread open");
        assert_eq!(tree.wait(std::time::Duration::from_secs(5))?.code(), Some(7));
        let prefix = super::tests::ControlMeter::new(0, 1, 0, 0);
        assert!(tree.terminate_measured(&prefix).is_err());
        assert!(tree.child.is_some());
        assert!(tree.guard.termination_sent, "accepted Job signal retained before denied query");
        assert_eq!(prefix.counts.writes.load(Relaxed), 1);
        let terminal = super::tests::ControlMeter::new(2, 4, 0, 0);
        tree.terminate_measured(&terminal)?;
        assert!(tree.is_reaped());
        assert_eq!(terminal.counts.reads.load(Relaxed), 2, "Job emptiness and actual Child::try_wait SDK attempt");
        assert_eq!(terminal.counts.writes.load(Relaxed), 4, "two pipes, Child handle boundary and Job close");
        assert_eq!(terminal.counts.reserved_reads.load(Relaxed), 0);
        assert_eq!(terminal.counts.reserved_writes.load(Relaxed), 0);
        Ok(())
    }

    // SAFETY: these exclusively owned kernel handles have no thread affinity.
    // Mutation and every native call require the unique mutable Guard owner.
    // No Sync implementation permits unsynchronized native handle operations.
    unsafe impl Send for Guard {}

    pub(super) fn spawn_controlled(
        command: &mut Command, meter: Option<&dyn crate::NativeReadMeter>,
        custody: &mut Option<super::ProcessTree>,
    ) -> io::Result<()> {
        *custody = Some(super::ProcessTree {
            child: None, guard: Guard::empty(meter.is_none()),
            output_taken: false, cleanup_on_drop: meter.is_none(),
        });
        let initialized = custody.as_mut().ok_or(io::ErrorKind::NotFound)?.guard.initialize(meter);
        if initialized.is_err() && custody.as_ref().is_some_and(super::ProcessTree::is_reaped) {
            custody.take();
        }
        initialized?;
        let tree = custody.as_mut().ok_or(io::ErrorKind::NotFound)?;
        command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP);
        // Command's caller-prepared buffers and private SDK allocation are
        // opaque here. Count this actual spawn attempt, not hidden syscalls,
        // heap events, or an invented process/kernel byte acknowledgement.
        super::process_control_attempt(meter, true, || {
            tree.child = Some(command.spawn()?);
            Ok(())
        })?;
        let child = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?;
        super::process_control_attempt(meter, true, || {
            // SAFETY: both handles are owned and the child is still suspended.
            if unsafe { AssignProcessToJobObject(tree.guard.job, child.as_raw_handle().cast()) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                tree.guard.assigned = true;
                Ok(())
            }
        })?;
        tree.guard.resume_process(child.id(), meter)
    }

    #[cfg(test)]
    fn failed_admission(child: &mut Child, guard: &mut Guard, admission: io::Error) -> io::Error {
        // An assignment failure leaves a suspended child outside the Job.
        // Attempt both owners' cleanup and bound direct-exit observation; never
        // hide failed cleanup behind the original admission error.
        let job = guard.terminate();
        let killed = child.kill();
        let cleanup = (|| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                if child.try_wait()?.is_some() {
                    return job;
                }
                if let Err(error) = &killed {
                    return Err(io::Error::new(error.kind(), error.to_string()));
                }
                if std::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "unadmitted child termination is unresolved",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })();
        match cleanup {
            Ok(()) => admission,
            Err(error) => io::Error::new(
                error.kind(),
                format!("process admission failed ({admission}); cleanup unresolved ({error})"),
            ),
        }
    }

    impl Guard {
        const fn empty(cleanup_on_drop: bool) -> Self {
            Self {
                job: std::ptr::null_mut(), active: false, assigned: false,
                termination_sent: false, direct_termination_sent: false, cleanup_on_drop,
                snapshot: std::ptr::null_mut(), thread: std::ptr::null_mut(),
            }
        }

        #[cfg(test)]
        fn new() -> io::Result<Self> {
            let mut guard = Self::empty(true);
            guard.initialize(None)?;
            Ok(guard)
        }

        fn initialize(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
            super::process_control_attempt(meter, true, || {
                // SAFETY: unnamed Job with default security, owned on success.
                let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
                if job.is_null() { return Err(io::Error::last_os_error()); }
                self.job = job;
                self.active = true;
                Ok(())
            })?;
            // Measured owners never enable kill-on-close: bounded cleanup is
            // explicit, and a denied attempt cannot hide a tree kill in Drop.
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
            if meter.is_none() { limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE; }
            super::process_control_attempt(meter, true, || {
                // SAFETY: initialized fixed ABI structure and owned Job.
                if unsafe { SetInformationJobObject(
                    self.job, JobObjectExtendedLimitInformation, (&raw const limits).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ) } == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
            })
        }

        pub(super) const fn pending(&self) -> bool {
            !self.job.is_null() || !self.snapshot.is_null() || !self.thread.is_null()
        }

        pub(super) fn release_setup(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
            for handle in [&mut self.thread, &mut self.snapshot] {
                if !handle.is_null() {
                    super::process_control_attempt(meter, true, || {
                        // SAFETY: this exclusively owned handle is no longer borrowed.
                        if unsafe { CloseHandle(*handle) } == 0 { return Err(io::Error::last_os_error()); }
                        *handle = std::ptr::null_mut();
                        Ok(())
                    })?;
                }
            }
            Ok(())
        }

        pub(super) fn close(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
            self.release_setup(meter)?;
            if !self.job.is_null() {
                super::process_control_attempt(meter, true, || {
                    // SAFETY: cleanup observed an empty Job; this is its owned handle.
                    if unsafe { CloseHandle(self.job) } == 0 { return Err(io::Error::last_os_error()); }
                    self.job = std::ptr::null_mut();
                    Ok(())
                })?;
            }
            Ok(())
        }

        #[cfg(test)]
        pub(super) fn terminate(&mut self) -> io::Result<()> {
            self.terminate_with_meter(None)
        }

        pub(super) fn terminate_with_meter(
            &mut self, meter: Option<&dyn crate::NativeReadMeter>,
        ) -> io::Result<()> {
            if !self.active { return Ok(()); }
            if !self.termination_sent {
                super::process_control_attempt(meter, true, || {
                    // SAFETY: the exclusively owned live Job contains this tree.
                    if unsafe { TerminateJobObject(self.job, 1) } == 0 { return Err(io::Error::last_os_error()); }
                    self.termination_sent = true;
                    Ok(())
                })?;
            }
            // A successful signal is not a completion acknowledgement.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
                super::process_control_attempt(meter, false, || {
                    // SAFETY: writable fixed ABI structure and owned live Job.
                    if unsafe { QueryInformationJobObject(
                        self.job, JobObjectBasicAccountingInformation, (&raw mut accounting).cast(),
                        size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32, std::ptr::null_mut(),
                    ) } == 0 { return Err(io::Error::last_os_error()); }
                    if accounting.ActiveProcesses == 0 { self.active = false; }
                    Ok(())
                })?;
                if !self.active { return Ok(()); }
                if std::time::Instant::now() >= deadline { return Err(io::ErrorKind::TimedOut.into()); }
                super::process_control_attempt(meter, false, || {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    Ok(())
                })?;
            }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            if !self.cleanup_on_drop { return; }
            let _ = self.close(None);
        }
    }

    #[test]
    fn denied_job_cleanup_retains_child_and_reports_failure() {
        use windows_sys::Win32::Foundation::DuplicateHandle;
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            super::ProcessTree::spawn(&mut super::tests::command("child", temporary.path()))
                .expect("spawn tree");
        super::tests::ready(temporary.path(), "tree-ready", || {
            tree.try_wait().map_or(true, |status| status.is_some())
        });
        let mut restricted = std::ptr::null_mut();
        // SAFETY: both source and target are this process; the duplicate has
        // zero access rights. Keep the original handle for mandatory cleanup.
        let duplicated = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                tree.guard.job,
                GetCurrentProcess(),
                &raw mut restricted,
                0,
                0,
                0,
            )
        };
        assert_ne!(duplicated, 0);
        let original = std::mem::replace(
            &mut tree.guard,
            Guard {
                job: restricted,
                active: true,
                assigned: true,
                ..Guard::empty(true)
            },
        );
        let started = std::time::Instant::now();
        let result = tree.wait_with_output(std::time::Duration::ZERO, 4096);
        let retained = tree.child.is_some() && tree.guard.active;
        let recollection = tree.wait_with_output(std::time::Duration::ZERO, 4096);
        let restricted = std::mem::replace(&mut tree.guard, original);
        drop(restricted);
        tree.terminate().expect("cleanup with original authority");
        assert_eq!(
            result.expect_err("cleanup must fail").kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(retained);
        assert!(
            recollection
                .expect_err("consumed pipes cannot be collected again")
                .to_string()
                .contains("already collected")
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        super::tests::assert_no_escape(temporary.path());
    }

    #[test]
    fn denied_admission_still_observes_unassigned_child_exit() {
        use windows_sys::Win32::Foundation::DuplicateHandle;
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let temporary = tempfile::tempdir().expect("temporary directory");
        let guard = Guard::new().expect("create Job");
        let mut restricted = std::ptr::null_mut();
        // SAFETY: duplicate the live Job into this process without assign or
        // terminate authority; the original guard remains independently owned.
        let duplicated = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                guard.job,
                GetCurrentProcess(),
                &raw mut restricted,
                0,
                0,
                0,
            )
        };
        assert_ne!(duplicated, 0);
        let mut restricted = Guard {
            job: restricted,
            active: true,
            ..Guard::empty(true)
        };
        let mut command = super::tests::command("child", temporary.path());
        command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP);
        let mut child = command.spawn().expect("spawn suspended child");
        // SAFETY: live handles; this real assignment must fail for lack of rights.
        let assigned =
            unsafe { AssignProcessToJobObject(restricted.job, child.as_raw_handle().cast()) };
        assert_eq!(assigned, 0);
        let admission = io::Error::last_os_error();
        let error = failed_admission(&mut child, &mut restricted, admission);
        assert!(
            child
                .try_wait()
                .expect("observe unassigned child")
                .is_some()
        );
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("cleanup unresolved"));
        assert!(!temporary.path().join("tree-ready").exists());
    }

    impl Guard {
        fn resume_process(
            &mut self, process_id: u32, meter: Option<&dyn crate::NativeReadMeter>,
        ) -> io::Result<()> {
            super::process_control_attempt(meter, false, || {
                // SAFETY: SDK returns a newly owned system-thread snapshot.
                let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
                if snapshot == INVALID_HANDLE_VALUE { return Err(io::Error::last_os_error()); }
                self.snapshot = snapshot;
                Ok(())
            })?;
            let mut entry: THREADENTRY32 = unsafe { zeroed() };
            entry.dwSize = size_of::<THREADENTRY32>() as u32;
            let mut found = super::process_control_attempt(meter, false, || {
                // SAFETY: owned live snapshot and initialized writable ABI record.
                if unsafe { Thread32First(self.snapshot, &raw mut entry) } != 0 { return Ok(true); }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) { Ok(false) } else { Err(error) }
            })?;
            while found {
                if let Some(meter) = meter { meter.examine(1, 0)?; }
                if entry.th32OwnerProcessID == process_id {
                    super::process_control_attempt(meter, false, || {
                        // SAFETY: ID belongs to the just-observed snapshot entry.
                        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                        if thread.is_null() { return Err(io::Error::last_os_error()); }
                        self.thread = thread;
                        Ok(())
                    })?;
                    super::process_control_attempt(meter, true, || {
                        // SAFETY: owned live handle with resume access.
                        if unsafe { ResumeThread(self.thread) } == u32::MAX {
                            Err(io::Error::last_os_error())
                        } else { Ok(()) }
                    })?;
                    return self.release_setup(meter);
                }
                found = super::process_control_attempt(meter, false, || {
                    // SAFETY: owned snapshot and writable record remain valid.
                    if unsafe { Thread32Next(self.snapshot, &raw mut entry) } != 0 { return Ok(true); }
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) { Ok(false) } else { Err(error) }
                })?;
            }
            Err(io::ErrorKind::NotFound.into())
        }
    }
}
