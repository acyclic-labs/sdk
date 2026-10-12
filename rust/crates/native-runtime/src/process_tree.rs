#![allow(
    unsafe_code,
    reason = "process containment and bounded pipes use platform OS calls"
)]

use crate::obs;
use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

/// One child and descendants that remain in its operating-system containment.
pub struct ProcessTree {
    child: Option<Child>,
    guard: platform::Guard,
    output_taken: bool,
    #[cfg(unix)]
    native_child: Option<libc::pid_t>,
    cleanup_on_drop: bool,
    #[cfg(unix)]
    spawn_resources: platform::SpawnResources,
    #[cfg(unix)]
    prepared_spawn: Option<PreparedClosedProcess>,
}

/// Paid storage for a closed Unix spawn on the existing process-tree engine.
#[cfg(unix)]
pub struct PreparedClosedProcess {
    arguments: PreparedProcessBuffer<std::mem::MaybeUninit<*mut libc::c_char>>,
    environment: PreparedProcessBuffer<PreparedProcessBuffer<u8>>,
    environment_pointers: PreparedProcessBuffer<*mut libc::c_char>,
}

#[cfg(unix)]
struct PreparedProcessBuffer<T> {
    data: Vec<T>,
    _allocation: crate::ReadAllocation,
}

// Argument slots are inert outside synchronous spawn and are overwritten
// before reuse. Environment pointers address this frame's stable owned bytes.
#[cfg(unix)]
unsafe impl Send for PreparedClosedProcess {}

#[cfg(unix)]
impl std::fmt::Debug for PreparedClosedProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreparedClosedProcess")
            .field("argument_count", &self.arguments.data.len().saturating_sub(2))
            .field("environment_entries", &self.environment.data.len())
            .finish_non_exhaustive()
    }
}

#[cfg(unix)]
fn prepared_process_vec<T>(
    count: usize, meter: &std::sync::Arc<dyn crate::NativeReadMeter>,
) -> io::Result<PreparedProcessBuffer<T>> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or(io::ErrorKind::InvalidInput)?;
    let allocation = if bytes == 0 { crate::ReadAllocation::empty(meter) }
        else { crate::ReadAllocation::new(meter, bytes)? };
    let mut buffer = PreparedProcessBuffer { data: Vec::new(), _allocation: allocation };
    if bytes != 0 {
        buffer.data.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
        let actual = buffer.data.capacity().checked_mul(std::mem::size_of::<T>()).ok_or(io::ErrorKind::InvalidInput)?;
        buffer._allocation.reconcile_capacity(actual)?;
    }
    Ok(buffer)
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
            let transfer = platform::write_pipe(pipe, suffix, meter);
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
    /// Maximum observation time for each physical termination phase.
    pub const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Minimum delay between unfinished physical termination observations.
    pub const TERMINATION_POLL_INTERVAL: Duration = Duration::from_millis(1);

    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        let mut custody = None;
        if let Err(admission) = Self::spawn_controlled(command, None, &mut custody) {
            if let Some(tree) = custody.as_mut() { tree.terminate(None)?; }
            return Err(admission);
        }
        custody.ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    pub(crate) fn spawn_controlled(
        command: &mut Command, meter: Option<&dyn crate::NativeReadMeter>,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() { return Err(io::ErrorKind::AlreadyExists.into()); }
        if let Some(meter) = meter {
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

    /// Starts a closed-stream Unix child using borrowed, terminated arguments, and retains it in this same process-tree owner before spawn cleanup.
    #[cfg(unix)]
    pub fn spawn_closed_measured(
        program: &std::ffi::CStr, args: &[&std::ffi::CStr],
        meter: &dyn crate::NativeReadMeter, custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() { return Err(io::ErrorKind::AlreadyExists.into()); }
        platform::spawn_closed(program, args, meter, custody)
    }

    /// Snapshots the host's existing environment and pays known argument storage before a driver starts.
    #[cfg(unix)]
    pub fn prepare_closed_process(
        argument_count: usize, meter: std::sync::Arc<dyn crate::NativeReadMeter>,
    ) -> io::Result<PreparedClosedProcess> {
        let meter = meter.allocation_owner().unwrap_or(meter);
        meter.check()?;
        let argument_slots = argument_count.checked_add(2).ok_or(io::ErrorKind::InvalidInput)?;
        let mut arguments = prepared_process_vec(argument_slots, &meter)?;
        // MaybeUninit accepts uninitialized slots; actual pointer writes and their
        // copy receipt occur at the later synchronous spawn, never hypothetically.
        unsafe { arguments.data.set_len(argument_slots); }
        let inherited = platform::inherited_environment();
        let mut environment_count = 0_usize;
        if !inherited.is_null() {
            loop {
                meter.examine(1, 0)?;
                if unsafe { (*inherited.add(environment_count)).is_null() } { break; }
                environment_count = environment_count.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
            }
        }
        let mut environment = prepared_process_vec(environment_count, &meter)?;
        let pointer_count = environment_count.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        let mut environment_pointers = prepared_process_vec(pointer_count, &meter)?;
        for index in 0..environment_count {
            meter.examine(1, 0)?;
            let value = unsafe { *inherited.add(index) };
            let mut length = 0_usize;
            loop {
                meter.examine(1, 0)?;
                if unsafe { *value.add(length) } == 0 { break; }
                length = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
            }
            let terminated = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
            meter.admit_examination(0, terminated as u64)?;
            let mut bytes = prepared_process_vec(terminated, &meter)?;
            bytes.data.extend_from_slice(unsafe { std::slice::from_raw_parts(value.cast::<u8>(), terminated) });
            meter.record_examination(0, terminated as u64)?;
            environment.data.push(bytes);
        }
        let pointer_bytes = pointer_count.checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(io::ErrorKind::InvalidInput)? as u64;
        meter.admit_examination(environment_count as u64, pointer_bytes)?;
        environment_pointers.data.extend(environment.data.iter().map(|entry| entry.data.as_ptr().cast_mut().cast()));
        environment_pointers.data.push(std::ptr::null_mut());
        meter.record_examination(environment_count as u64, pointer_bytes)?;
        Ok(PreparedClosedProcess {
            arguments, environment, environment_pointers,
        })
    }

    /// Uses preowned storage on the same closed-stream Unix spawn engine.
    #[cfg(unix)]
    pub fn spawn_closed_prepared(
        program: &std::ffi::CStr, args: &[&std::ffi::CStr],
        frame: &mut Option<PreparedClosedProcess>, meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() { return Err(io::ErrorKind::AlreadyExists.into()); }
        let prepared = frame.as_mut().ok_or(io::ErrorKind::InvalidInput)?;
        let count = args.len().checked_add(2).ok_or(io::ErrorKind::InvalidInput)?;
        if prepared.arguments.data.len() != count { return Err(io::ErrorKind::InvalidInput.into()); }
        let bytes = count.checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        meter.admit_examination((count - 1) as u64, bytes as u64)?;
        prepared.arguments.data[0].write(program.as_ptr().cast_mut());
        for (slot, argument) in prepared.arguments.data[1..count - 1].iter_mut().zip(args) {
            slot.write(argument.as_ptr().cast_mut());
        }
        prepared.arguments.data[count - 1].write(std::ptr::null_mut());
        meter.record_examination((count - 1) as u64, bytes as u64)?;
        // Every slot is initialized above, and the caller retains all argument
        // data while this existing synchronous native spawn borrows it.
        let arguments = unsafe { std::slice::from_raw_parts(prepared.arguments.data.as_ptr().cast(), count) };
        let result = platform::spawn_closed_with_arguments(
            program, arguments, Some(prepared.environment_pointers.data.as_mut_ptr()), meter, custody,
        );
        if let Some(tree) = custody { tree.prepared_spawn = frame.take(); }
        result
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

    /// Reads owned stdout without waiting for data.
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
        // SAFETY: initialized bytes can be borrowed as MaybeUninit bytes.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast(), buffer.len()) };
        let (read, recorded) = platform::read_pipe(pipe, buffer, None)?;
        recorded?;
        Ok(read)
    }

    fn close_stdin(&mut self) {
        if let Some(child) = self.child.as_mut() {
            drop(child.stdin.take());
        }
    }

    /// Polls the direct child without releasing ownership of its descendants.
    pub fn try_wait(
        &mut self, meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<ExitStatus>> {
        self.select_control_meter(meter);
        #[cfg(unix)]
        return platform::observe_exit_id(self.owned_child_id()?, meter);
        #[cfg(windows)]
        {
            let child = self.child.as_mut().ok_or(io::ErrorKind::NotFound)?;
            process_control_attempt(meter, crate::NativeOperationKind::Read, || child.try_wait())
        }
    }

    #[cfg(unix)]
    fn owned_child_id(&self) -> io::Result<libc::id_t> {
        self.native_child.map(|pid| pid as libc::id_t)
            .or_else(|| self.child.as_ref().map(Child::id))
            .ok_or_else(|| io::ErrorKind::Other.into())
    }

    fn select_control_meter(&mut self, meter: Option<&dyn crate::NativeReadMeter>) {
        if meter.is_some() {
            self.cleanup_on_drop = false;
            #[cfg(windows)]
            { self.guard.cleanup_on_drop = false; }
        }
    }

    fn capture_close_stdin(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        self.select_control_meter(meter);
        if self.child.as_ref().is_some_and(|child| child.stdin.is_some()) {
            process_control_attempt(meter, crate::NativeOperationKind::Write, || { self.close_stdin(); Ok(()) })?;
        }
        Ok(())
    }

    /// Observes direct-child exit within `timeout`, retaining descendant ownership.
    pub fn wait(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let operation = obs::span!(
            INFO, "acyclic.runtime.wait", outcome = obs::Empty, error.kind = obs::Empty
        );
        operation.scope(|| operation.finish((|| {
            let deadline = Instant::now().checked_add(timeout)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "process deadline overflow"))?;
            if self.is_reaped() {
                return Err(io::Error::other("process output was already collected"));
            }
            self.close_stdin();
            loop {
                if let Some(status) = self.try_wait(None)? { return Ok(status); }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut,
                        "process exit deadline exceeded; termination may be unresolved"));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        })()))
    }

    /// Captures bounded output and optional input, preserving partial output on failure.
    #[allow(clippy::too_many_arguments)]
    pub fn wait_with_output(
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
                    && let Some(exit) = self.try_wait(meter)?
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
        operation.record_error(result.as_ref().err().map(|failure| &failure.error));
        result
    }

    /// Terminates containment and reaps the direct child.
    pub fn terminate(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        if meter.is_some() { return self.capture_terminate(meter); }
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.terminate",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| operation.finish(self.capture_terminate(meter)))
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
                let deadline = Instant::now() + Self::TERMINATION_TIMEOUT;
                while self.try_wait(meter)?.is_none() {
                    if Instant::now() >= deadline { return Err(io::ErrorKind::TimedOut.into()); }
                    process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                        std::thread::sleep(Self::TERMINATION_POLL_INTERVAL);
                        Ok(())
                    })?;
                }
                if let Some(child) = self.child.as_mut() {
                    if child.stdout.is_some() {
                        process_control_attempt(meter, crate::NativeOperationKind::Write, || { drop(child.stdout.take()); Ok(()) })?;
                    }
                    if child.stderr.is_some() {
                        process_control_attempt(meter, crate::NativeOperationKind::Write, || { drop(child.stderr.take()); Ok(()) })?;
                    }
                }
                // Child's private Win32 handle release is one opaque std SDK
                // boundary, not an estimate of hidden handle/kernel internals.
                process_control_attempt(meter, crate::NativeOperationKind::Write, || { drop(self.child.take()); Ok(()) })?;
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
            let deadline = Instant::now() + Self::TERMINATION_TIMEOUT;
            while platform::observe_exit_id(self.owned_child_id()?, meter)?.is_none() {
                if Instant::now() >= deadline {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                    std::thread::sleep(Self::TERMINATION_POLL_INTERVAL);
                    Ok(())
                })?;
            }
            // Only actually retained standard pipes require a release call.
            // Denial keeps the observed leader unreaped and its remaining
            // streams owned by this same tree, rather than hiding their drops
            // in the final Child release.
            if let Some(child) = self.child.as_mut() {
                if child.stdout.is_some() {
                    process_control_attempt(meter, crate::NativeOperationKind::Write, || { drop(child.stdout.take()); Ok(()) })?;
                }
                if child.stderr.is_some() {
                    process_control_attempt(meter, crate::NativeOperationKind::Write, || { drop(child.stderr.take()); Ok(()) })?;
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
        // No native child or configuration pointer can refer to this frame
        // after the physical reap and configuration cleanup above.
        drop(self.prepared_spawn.take());
        Ok(())
    }

    /// Whether containment cleanup succeeded and the direct child was reaped.
    #[must_use]
    pub const fn is_reaped(&self) -> bool {
        #[cfg(unix)]
        return self.child.is_none() && self.native_child.is_none() && !self.spawn_resources.pending();
        #[cfg(windows)]
        {
            self.child.is_none() && !self.guard.pending()
        }
    }

    /// Requests graceful Unix group termination, then performs mandatory tree cleanup after direct-child exit or `grace`.
    pub fn terminate_after(&mut self, grace: Duration) -> io::Result<ExitStatus> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.terminate_after",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| operation.finish((|| {
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
                self.terminate(None)?;
                return Err(error);
            }
        };
        self.terminate(None)?;
        Ok(status)
        })()))
    }

    /// Signals termination to the entire tree while retaining the direct child so its captured output can still be collected.
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
            let direct = if !self.guard.assigned && !self.guard.direct_termination_sent {
                if let Some(child) = self.child.as_mut() {
                    process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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

/// Reads one chunk; returns whether the pipe produced data or reached EOF, so the caller sleeps only when both pipes are idle.
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
            .ok_or_else(|| io::Error::from(io::ErrorKind::Other))?
    };
    let transfer = platform::read_pipe(reader, buffer, meter);
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

/// One pipe SDK call owns its own credit.
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
        let _ = self.terminate(None);
    }
}

/// One actual SDK/kernel attempt, not a prediction of its hidden internals.
fn process_control_attempt<T>(
    meter: Option<&dyn crate::NativeReadMeter>, kind: crate::NativeOperationKind, call: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
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
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<(Option<usize>, io::Result<()>)> {
        nonblocking(pipe, meter)?;
        let (written, recorded) = super::pipe_operation_attempt(
            meter, crate::NativeOperationKind::Write, || pipe.write(bytes),
        )?;
        match written {
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) =>
                Ok((None, recorded)),
            Ok(0) => { recorded?; Err(io::Error::from(io::ErrorKind::WriteZero)) }
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
                super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                    let actions = self.actions.as_mut().ok_or(io::ErrorKind::NotFound)?;
                    let status = unsafe { libc::posix_spawn_file_actions_destroy(actions) };
                    if status != 0 { return Err(io::Error::from_raw_os_error(status)); }
                    self.actions.take();
                    Ok(())
                })?;
            }
            if self.attributes.is_some() {
                super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
        super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
            let child = command.spawn()?;
            // A Unix Child ID originated as pid_t at this same SDK boundary.
            let process_group = child.id() as libc::pid_t;
            *custody = Some(super::ProcessTree {
                child: Some(child), guard: Guard { process_group, active: true },
                output_taken: false, native_child: None,
                cleanup_on_drop: meter.is_none(),
                spawn_resources: SpawnResources::default(),
                prepared_spawn: None,
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

    pub(super) fn inherited_environment() -> *mut *mut libc::c_char {
        // The existing closed-spawn host contract excludes concurrent unsafe
        // native environment mutation while these pointers are borrowed.
        unsafe {
            #[cfg(target_vendor = "apple")]
            return *_NSGetEnviron();
            #[cfg(not(target_vendor = "apple"))]
            return environ;
        }
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
        let result = meter.record_examination((count - 1) as u64, copied as u64)
            .and_then(|()| spawn_closed_with_arguments(program, &arguments, None, meter, custody));
        drop(arguments);
        meter.release(bytes as u64);
        result
    }

    pub(super) fn spawn_closed_with_arguments(
        program: &std::ffi::CStr, arguments: &[*mut libc::c_char],
        environment: Option<*mut *mut libc::c_char>, meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<super::ProcessTree>,
    ) -> io::Result<()> {
        let result = (|| {
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
                    super::process_control_attempt(Some(meter), crate::NativeOperationKind::Write, || {
                        let status = call();
                        if status == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(status)) }
                    })
                };
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
                        let environment = environment.unwrap_or_else(inherited_environment);
                        let status = libc::posix_spawn(&raw mut pid, program.as_ptr(), actions.as_ptr(), attributes.as_ptr(),
                            arguments.as_ptr(), environment);
                        if status == 0 {
                            *custody = Some(super::ProcessTree {
                                child: None, native_child: Some(pid),
                                guard: Guard { process_group: pid, active: true },
                                output_taken: false, cleanup_on_drop: false,
                                spawn_resources: SpawnResources::default(),
                                prepared_spawn: None,
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
                    prepared_spawn: None,
                });
                owner.spawn_resources = retained;
            }
            spawned.and(actions_result).and(attr_result)
        })();
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
            let signal = super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
            let signal = super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
        let deadline = std::time::Instant::now() + super::ProcessTree::TERMINATION_TIMEOUT;
        loop {
            if group_snapshot_has_no_live_members(group, meter)? {
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                std::thread::sleep(super::ProcessTree::TERMINATION_POLL_INTERVAL);
                Ok(())
            })?;
        }
    }

    #[cfg(target_vendor = "apple")]
    fn group_snapshot_has_no_live_members(group: libc::pid_t, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<bool> {
        let mut members = [0_i32; 2];
        let bytes = i32::try_from(std::mem::size_of_val(&members)).map_err(io::Error::other)?;
        let id = u32::try_from(group).map_err(io::Error::other)?;
        // SAFETY: thread-local errno and a writable, correctly sized PID array.
        // libproc returns zero both for an empty list and for an errno failure.
        let read = super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
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

    use std::io;
    #[cfg(unix)]
    #[test]
    fn prepared_closed_spawn_uses_owned_birth_environment_and_no_late_allocation()
    -> std::io::Result<()> {
        use std::sync::Arc;
        let sizing = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
        let frame = ProcessTree::prepare_closed_process(2, sizing.clone())?;
        let bytes = sizing.memory.load(std::sync::atomic::Ordering::Acquire);
        assert!(bytes > 0);
        drop(frame);
        assert_eq!(sizing.memory.load(std::sync::atomic::Ordering::Acquire), 0);
        let original = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, 0));
        let mut frame = Some(ProcessTree::prepare_closed_process(2, original.clone())?);
        assert_eq!(original.memory.load(std::sync::atomic::Ordering::Acquire), bytes);
        original.deny_allocations.store(true, std::sync::atomic::Ordering::Release);
        let prepared = frame.as_mut().expect("genuine funded birth frame");
        let path = prepared.environment.data.iter_mut()
            .find(|entry| entry.data.starts_with(b"PATH="))
            .ok_or(std::io::ErrorKind::NotFound)?;
        let end = path.data.len() - 1;
        if end <= 5 { return Err(std::io::ErrorKind::InvalidInput.into()); }
        path.data[5..end].fill(b'x');
        let mut custody = None;
        ProcessTree::spawn_closed_prepared(
            c"/bin/sh", &[c"-c", c"case \"$PATH\" in x*) exit 7;; *) exit 9;; esac"],
            &mut frame, original.as_ref(), &mut custody,
        )?;
        assert!(frame.is_none());
        let mut tree = custody.take().expect("same actual leader owns its frame");
        assert!(tree.prepared_spawn.is_some());
        assert_eq!(original.memory.load(std::sync::atomic::Ordering::Acquire), bytes);
        assert_eq!(tree.wait(std::time::Duration::from_secs(5))?.code(), Some(7));
        assert!(!tree.is_reaped());
        assert_eq!(original.memory.load(std::sync::atomic::Ordering::Acquire), bytes, "observed exit is not physical reap");
        tree.terminate(None)?;
        assert!(tree.is_reaped() && tree.prepared_spawn.is_none());
        assert_eq!(original.memory.load(std::sync::atomic::Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn prepared_closed_spawn_denial_retains_original_frame() -> io::Result<()> {
        let mut limits = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
        limits.cancel_after_operation = true;
        let meter = std::sync::Arc::new(limits);
        let mut frame = Some(ProcessTree::prepare_closed_process(2, meter.clone())?);
        let bytes = meter.memory.load(std::sync::atomic::Ordering::Acquire);
        let mut custody = None;
        assert_eq!(ProcessTree::spawn_closed_prepared(c"/bin/sh", &[c"-c"],
            &mut frame, meter.as_ref(), &mut custody).unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert!(custody.is_none() && frame.is_some());
        assert!(ProcessTree::spawn_closed_prepared(c"/bin/sh", &[c"-c", c"exit 7"],
            &mut frame, meter.as_ref(), &mut custody).is_err());
        assert!(custody.is_none() && frame.is_some());
        assert_eq!(meter.memory.load(std::sync::atomic::Ordering::Acquire), bytes);
        drop(frame);
        assert_eq!(meter.memory.load(std::sync::atomic::Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn measured_kill_with_denied_observation_keeps_owned_child() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut tree = ProcessTree::spawn(&mut command("child", temporary.path()))?;
        let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
        meter.operation_denial = Some(crate::NativeOperationKind::Read);
        assert!(tree.terminate(Some(&meter)).is_err());
        assert!(!tree.is_reaped());
        tree.wait(Duration::from_secs(5))?;
        tree.terminate(None)?;
        assert!(tree.is_reaped());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn denied_admission_after_suspended_spawn_retains_child() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
        meter.operation_denial = Some(crate::NativeOperationKind::Read);
        let mut custody = None;
        assert!(crate::spawn_process_tree_with_meter_custody(
            &mut command("exit-code", temporary.path()), &meter, &mut custody).is_err());
        let mut tree = custody.take().expect("suspended child remains owned");
        assert!(!tree.is_reaped());
        assert_eq!(tree.wait(Duration::from_secs(5)).unwrap_err().kind(), io::ErrorKind::TimedOut);
        tree.terminate(None)?;
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

    /// Waits for a helper's readiness marker.
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
        tree.try_wait(None).map_or(true, |status| status.is_some())
    }

    /// Asks any descendant that outlived its tree's cleanup to mark its escape, then checks that none did.
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
                tree.terminate(None).expect("terminate tree");
                tree.terminate(None).expect("repeat termination");
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
            .wait_with_output(Duration::from_secs(5), 4096, None, || {
                checks += 1;
                if temporary.path().join("tree-ready").exists() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    ))
                } else {
                    Ok(())
                }
            }, |_| Ok(crate::ProcessInput::default()), None, None)
            .expect_err("cancellation must stop capture");
        assert_eq!(error.error.kind(), std::io::ErrorKind::Interrupted);
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
        tree.terminate(None).expect("clean streaming tree");
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

    #[test]
    fn collection_closes_stdin_and_preserves_explicit_environment() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("output", temporary.path());
        command
            .env("EXPLICIT_PROCESS_INPUT", "allowed")
            .stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        let output = tree
            .wait_with_output(Duration::from_secs(5), 4096, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error)
            .expect("collect output");
        assert!(output.status.success(), "{output:?}");
        assert!(output.stdout.ends_with(b"stdout-marker"));
        assert_eq!(output.stderr, b"stderr-marker");
        assert!(tree.child.is_none());
        assert!(tree.wait_with_output(Duration::ZERO, 0, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error).is_err());
        tree.terminate(None).expect("repeat cleanup");
        let mut command = self::command("output", temporary.path());
        command.env("EXPLICIT_PROCESS_INPUT", "allowed");
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
        assert_eq!(
            tree.wait_with_output(Duration::from_secs(5), output.stdout.len() + output.stderr.len() - 1, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error)
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
        ready(temporary.path(), "stdin-stall-ready", || tree_exited(&mut tree));
        let input = vec![b'i'; BULK_BYTES];
        let mut checks = 0;
        let failure = tree.wait_with_output(Duration::from_secs(5), 4096,
            Some(crate::CaptureInput::Literal(&input)), || {
                checks += 1;
                if checks == 10 { Err(std::io::ErrorKind::Interrupted.into()) } else { Ok(()) }
            }, |_| Ok(crate::ProcessInput::default()), None, None,
        ).expect_err("intent stops stalled exchange");
        assert_eq!(failure.error.kind(), std::io::ErrorKind::Interrupted);
        assert!(failure.stdout.ends_with(b"stdin-stall-marker"));
        assert!(failure.cleanup_completed && tree.is_reaped());
    }

    #[test]
    fn partial_stdin_writes_preserve_exact_bytes_and_explicit_eof() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-echo", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn input echo");
        let input: Vec<u8> = (0..BULK_BYTES)
            .map(|index| u8::try_from(index % 251).expect("byte pattern")).collect();
        let output = tree.wait_with_output(Duration::from_secs(60), 2 * BULK_BYTES,
            Some(crate::CaptureInput::Literal(&input)), || Ok(()),
            |_| Ok(crate::ProcessInput::default()), None, None,
        ).expect("collect echoed input");
        assert!(output.status.success());
        assert_eq!(output.stdout, input);
        assert!(output.stderr.is_empty() && tree.is_reaped());
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
            .wait_with_output(Duration::from_secs(5), 4096, Some(crate::CaptureInput::Exchange { initial: b"first\n", maximum: 13 }), || Ok(()), |chunk| {
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
                            ..crate::ProcessInput::default()
                        })
                    } else {
                        Ok(crate::ProcessInput::default())
                    }
                }, None, None)
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
                .wait_with_output(Duration::from_secs(5), 4096, Some(crate::CaptureInput::Exchange { initial: b"first\n", maximum: 6 }), || Ok(()), |chunk| {
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
                            Ok(crate::ProcessInput::default())
                        }
                    }, None, None)
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
            .wait_with_output(Duration::from_millis(20), 4096, Some(crate::CaptureInput::Exchange { initial: &vec![b'i'; BULK_BYTES], maximum: BULK_BYTES }), || {
                    checks += 1;
                    Ok(())
                }, |_| Ok(crate::ProcessInput::default()), None, None)
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
            .wait_with_output(Duration::from_millis(20), 4096, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None)
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
                .wait_with_output(timeout, limit, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None)
                .expect_err("capture must fail");
            assert_eq!(error.error.kind(), kind);
            assert!(error.cleanup_completed);
            assert!(error.stdout.len() + error.stderr.len() <= limit);
            if mode == "flood" {
                assert_eq!(error.stdout.len() + error.stderr.len(), limit);
            }
            assert!(started.elapsed() < Duration::from_secs(7));
            assert!(tree.child.is_none());
            tree.terminate(None).expect("repeat cleanup");
            assert_no_escape(temporary.path());
        }
    }

    #[test]
    fn bulk_output_is_collected_fully_within_its_exact_bound() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let output = ProcessTree::spawn(&mut command("bulk", temporary.path()))
            .expect("spawn bulk writer")
            .wait_with_output(Duration::from_secs(60), 2 * BULK_BYTES, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error)
            .expect("collect bulk output");
        assert!(output.status.success());
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
            .wait_with_output(Duration::from_secs(60), output.stdout.len() - 1, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error)
            .expect_err("bulk output exceeds bound");
        assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    }

    #[test]
    fn exited_parent_does_not_leave_descendant_holding_output_open() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command("exit-parent", temporary.path())).expect("spawn tree");
        let output = tree
            .wait_with_output(Duration::from_secs(5), 4096, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error)
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
        while tree.try_wait(None).expect("poll exit").is_none() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            tree.wait(Duration::from_secs(5))
                .expect("wait exit")
                .success()
        );
        assert!(
            super::platform::observe_exit(tree.child.as_ref().expect("retained child"))
                .expect("leader remains waitable")
                .expect("exited leader")
                .success()
        );
        tree.terminate(None).expect("terminate and reap");
        assert!(tree.child.is_none());
        let mut tree = ProcessTree::spawn(&mut self::command("exit-code", temporary.path()))
            .expect("spawn nonzero exit");
        assert_eq!(
            tree.wait(Duration::from_secs(5))
                .expect("nonzero exit")
                .code(),
            Some(7)
        );
        tree.terminate(None).expect("reap nonzero exit");

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
        tree.terminate(None).expect("reap signal exit");
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::io::{Read, Write};
    use std::mem::{size_of, zeroed};
    use std::os::windows::process::CommandExt as _;
    use std::process::Command;
    #[cfg(test)]
    use std::process::Child;
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
        super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
            tree.child = Some(command.spawn()?);
            Ok(())
        })?;
        let child = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?;
        super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: unnamed Job with default security, owned on success.
                let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
                if job.is_null() { return Err(io::Error::last_os_error()); }
                self.job = job;
                self.active = true;
                Ok(())
            })?;
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
            if meter.is_none() { limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE; }
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
                    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
                super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
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
                super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                    // SAFETY: the exclusively owned live Job contains this tree.
                    if unsafe { TerminateJobObject(self.job, 1) } == 0 { return Err(io::Error::last_os_error()); }
                    self.termination_sent = true;
                    Ok(())
                })?;
            }
            let deadline = std::time::Instant::now() + super::ProcessTree::TERMINATION_TIMEOUT;
            loop {
                let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
                super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
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
                super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                    std::thread::sleep(super::ProcessTree::TERMINATION_POLL_INTERVAL);
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
            tree.try_wait(None).map_or(true, |status| status.is_some())
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
        let result = tree.wait_with_output(std::time::Duration::ZERO, 4096, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error);
        let retained = tree.child.is_some() && tree.guard.active;
        let recollection = tree.wait_with_output(std::time::Duration::ZERO, 4096, None, || Ok(()), |_| Ok(crate::ProcessInput::default()), None, None).map_err(|failure| failure.error);
        let restricted = std::mem::replace(&mut tree.guard, original);
        drop(restricted);
        tree.terminate(None).expect("cleanup with original authority");
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
        assert!(!temporary.path().join("tree-ready").exists());
    }

    impl Guard {
        fn resume_process(
            &mut self, process_id: u32, meter: Option<&dyn crate::NativeReadMeter>,
        ) -> io::Result<()> {
            super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: SDK returns a newly owned system-thread snapshot.
                let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
                if snapshot == INVALID_HANDLE_VALUE { return Err(io::Error::last_os_error()); }
                self.snapshot = snapshot;
                Ok(())
            })?;
            let mut entry: THREADENTRY32 = unsafe { zeroed() };
            entry.dwSize = size_of::<THREADENTRY32>() as u32;
            let mut found = super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: owned live snapshot and initialized writable ABI record.
                if unsafe { Thread32First(self.snapshot, &raw mut entry) } != 0 { return Ok(true); }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) { Ok(false) } else { Err(error) }
            })?;
            while found {
                if let Some(meter) = meter { meter.examine(1, 0)?; }
                if entry.th32OwnerProcessID == process_id {
                    super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                        // SAFETY: ID belongs to the just-observed snapshot entry.
                        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                        if thread.is_null() { return Err(io::Error::last_os_error()); }
                        self.thread = thread;
                        Ok(())
                    })?;
                    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                        // SAFETY: owned live handle with resume access.
                        if unsafe { ResumeThread(self.thread) } == u32::MAX {
                            Err(io::Error::last_os_error())
                        } else { Ok(()) }
                    })?;
                    return self.release_setup(meter);
                }
                found = super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
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
