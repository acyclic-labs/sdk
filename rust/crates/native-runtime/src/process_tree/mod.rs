#![allow(
    unsafe_code,
    reason = "process containment and bounded pipes use platform OS calls"
)]

use crate::obs;
use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

mod capture;

/// The clock the capture deadline reads. Tests freeze it so that expiry is
/// ordered by the capture loop's own checks instead of scheduler latency.
#[cfg(not(test))]
fn capture_now() -> Instant {
    Instant::now()
}

#[cfg(test)]
thread_local! {
    static CAPTURE_CLOCK: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn capture_now() -> Instant {
    CAPTURE_CLOCK.with(std::cell::Cell::get).unwrap_or_else(Instant::now)
}

/// Freezes this thread's capture clock at `now`, or restores wall time.
#[cfg(test)]
fn set_capture_clock(now: Option<Instant>) {
    CAPTURE_CLOCK.with(|clock| clock.set(now));
}
pub use capture::{CaptureInput, ProcessCaptureFailure, ProcessInput, ProcessOutput};
use capture::{PendingStdin, drain_pipe, observe_output, pipe_operation_attempt};
#[cfg(unix)]
mod prepared;
#[cfg(unix)]
pub use prepared::PreparedClosedProcess;
#[cfg(unix)]
use prepared::prepared_process_vec;
#[cfg(unix)]
#[path = "unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "windows.rs"]
mod platform;
#[cfg(test)]
mod tests;
#[cfg(all(test, windows))]
mod wait_tests;

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

impl ProcessTree {
    /// Maximum observation time for each physical termination phase.
    pub const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Minimum delay between unfinished physical termination observations.
    pub const TERMINATION_POLL_INTERVAL: Duration = Duration::from_millis(1);

    /// Starts a child in an owned operating-system process tree.
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        let mut custody = None;
        if let Err(admission) = Self::spawn_controlled(command, None, &mut custody) {
            if let Some(tree) = custody.as_mut() {
                tree.terminate(None)?;
            }
            return Err(admission);
        }
        custody.ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    /// Starts a child while retaining admitted custody and the original meter.
    pub fn spawn_controlled(
        command: &mut Command,
        meter: Option<&dyn crate::NativeReadMeter>,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        if let Some(meter) = meter {
            meter.examine(1, 0)?;
            for _ in command.get_args() {
                meter.examine(1, 0)?;
            }
            for _ in command.get_envs() {
                meter.examine(1, 0)?;
            }
            if command.get_current_dir().is_some() {
                meter.examine(1, 0)?;
            }
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
        operation.in_scope(|| operation.finish(platform::spawn_controlled(command, meter, custody)))
    }

    /// Starts a closed-stream Unix child using borrowed, terminated arguments, and retains it in this same process-tree owner before spawn cleanup.
    #[cfg(unix)]
    pub fn spawn_closed_measured(
        program: &std::ffi::CStr,
        args: &[&std::ffi::CStr],
        meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        platform::spawn_closed(program, args, meter, custody)
    }

    /// Snapshots the host's existing environment and pays known argument storage before a driver starts.
    #[cfg(unix)]
    pub fn prepare_closed_process(
        argument_count: usize,
        meter: std::sync::Arc<dyn crate::NativeReadMeter>,
    ) -> io::Result<PreparedClosedProcess> {
        let meter = meter.allocation_owner().unwrap_or(meter);
        meter.check()?;
        let argument_slots = argument_count
            .checked_add(2)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut arguments = prepared_process_vec(argument_slots, &meter)?;
        // Actual pointer writes and their copy receipt occur at the later
        // synchronous spawn, never hypothetically.
        arguments
            .data
            .resize(argument_slots, std::mem::MaybeUninit::uninit());
        let inherited = platform::inherited_environment();
        let mut environment_count = 0_usize;
        if !inherited.is_null() {
            loop {
                meter.examine(1, 0)?;
                // SAFETY: the host environment is a null-terminated pointer
                // array, and this index has not passed its terminator.
                if unsafe { (*inherited.add(environment_count)).is_null() } {
                    break;
                }
                environment_count = environment_count
                    .checked_add(1)
                    .ok_or(io::ErrorKind::InvalidInput)?;
            }
        }
        let mut environment = prepared_process_vec(environment_count, &meter)?;
        let pointer_count = environment_count
            .checked_add(1)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut environment_pointers = prepared_process_vec(pointer_count, &meter)?;
        for index in 0..environment_count {
            meter.examine(1, 0)?;
            // SAFETY: `index` precedes the terminator counted above.
            let value = unsafe { *inherited.add(index) };
            let mut length = 0_usize;
            loop {
                meter.examine(1, 0)?;
                // SAFETY: each entry is a NUL-terminated string, and this
                // offset has not passed its terminator.
                if unsafe { *value.add(length) } == 0 {
                    break;
                }
                length = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
            }
            let terminated = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
            meter.admit_examination(0, terminated as u64)?;
            let mut bytes = prepared_process_vec(terminated, &meter)?;
            // SAFETY: the entry's `length` bytes and its terminator were read above.
            bytes.data.extend_from_slice(unsafe {
                std::slice::from_raw_parts(value.cast::<u8>(), terminated)
            });
            meter.record_examination(0, terminated as u64)?;
            environment.data.push(bytes);
        }
        let pointer_bytes = pointer_count
            .checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(io::ErrorKind::InvalidInput)? as u64;
        meter.admit_examination(environment_count as u64, pointer_bytes)?;
        environment_pointers.data.extend(
            environment
                .data
                .iter()
                .map(|entry| entry.data.as_ptr().cast_mut().cast()),
        );
        environment_pointers.data.push(std::ptr::null_mut());
        meter.record_examination(environment_count as u64, pointer_bytes)?;
        Ok(PreparedClosedProcess {
            arguments,
            environment,
            environment_pointers,
        })
    }

    /// Uses preowned storage on the same closed-stream Unix spawn engine.
    #[cfg(unix)]
    pub fn spawn_closed_prepared(
        program: &std::ffi::CStr,
        args: &[&std::ffi::CStr],
        frame: &mut Option<PreparedClosedProcess>,
        meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<Self>,
    ) -> io::Result<()> {
        if custody.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        let prepared = frame.as_mut().ok_or(io::ErrorKind::InvalidInput)?;
        let count = args
            .len()
            .checked_add(2)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if prepared.arguments.data.len() != count {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bytes = count
            .checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        meter.admit_examination((count - 1) as u64, bytes as u64)?;
        let [first, middle @ .., last] = prepared.arguments.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        first.write(program.as_ptr().cast_mut());
        for (slot, argument) in middle.iter_mut().zip(args) {
            slot.write(argument.as_ptr().cast_mut());
        }
        last.write(std::ptr::null_mut());
        meter.record_examination((count - 1) as u64, bytes as u64)?;
        // SAFETY: every slot is initialized above, and the caller retains all
        // argument data while this existing synchronous native spawn borrows it.
        let arguments =
            unsafe { std::slice::from_raw_parts(prepared.arguments.data.as_ptr().cast(), count) };
        let result = platform::spawn_closed_with_arguments(
            program,
            arguments,
            Some(prepared.environment_pointers.data.as_mut_ptr()),
            meter,
            custody,
        );
        if let Some(tree) = custody {
            tree.prepared_spawn = frame.take();
        }
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
        let buffer =
            unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast(), buffer.len()) };
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
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
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
        match self.native_child {
            Some(pid) => libc::id_t::try_from(pid).map_err(io::Error::other),
            None => self
                .child
                .as_ref()
                .map(Child::id)
                .ok_or_else(|| io::ErrorKind::Other.into()),
        }
    }

    fn select_control_meter(&mut self, meter: Option<&dyn crate::NativeReadMeter>) {
        if meter.is_some() {
            self.cleanup_on_drop = false;
            #[cfg(windows)]
            {
                self.guard.cleanup_on_drop = false;
            }
        }
    }

    fn capture_close_stdin(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        self.select_control_meter(meter);
        if self
            .child
            .as_ref()
            .is_some_and(|child| child.stdin.is_some())
        {
            process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                self.close_stdin();
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Observes direct-child exit within `timeout`, retaining descendant ownership.
    pub fn wait(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.wait",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.in_scope(|| {
            operation.finish((|| {
                let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "process deadline overflow")
                })?;
                if self.is_reaped() {
                    return Err(io::Error::other("process output was already collected"));
                }
                self.close_stdin();
                #[cfg(windows)]
                return platform::wait_for_exit(
                    self.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                    deadline,
                );
                #[cfg(unix)]
                loop {
                    if let Some(status) = self.try_wait(None)? {
                        return Ok(status);
                    }
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "process exit deadline exceeded; termination may be unresolved",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            })())
        })
    }

    /// Captures bounded output and optional input, preserving partial output on failure.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "capture and cleanup share the same owned pipes, callbacks and deadline"
    )]
    pub fn wait_with_output(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        mut check: impl FnMut() -> io::Result<()>,
        mut observe: impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
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
        let result = operation.in_scope(|| {
            if self.output_taken {
                return Err(ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                )));
            }
            let deadline = capture_now().checked_add(timeout).ok_or_else(|| {
                ProcessCaptureFailure::before_capture(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process deadline overflow",
                ))
            })?;
            let (mut output, stdin_buffer) = match buffers {
                Some([stdout, stderr, stdin]) => ([stdout, stderr], Some(stdin)),
                None => ([Vec::new(), Vec::new()], None),
            };
            if meter.is_some()
                && output
                    .iter()
                    .any(|buffer| !buffer.is_empty() || buffer.capacity() < max_bytes)
            {
                return Err(ProcessCaptureFailure::before_capture(
                    io::ErrorKind::OutOfMemory.into(),
                ));
            }
            let mut input = input
                .map(|input| PendingStdin::new_with_buffer(input, stdin_buffer, meter))
                .transpose()
                .map_err(ProcessCaptureFailure::before_capture)?;
            if input.is_none() {
                self.capture_close_stdin(meter)
                    .map_err(ProcessCaptureFailure::before_capture)?;
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
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                loop {
                    check()?;
                    let mut progressed = input
                        .as_mut()
                        .map_or(Ok(false), |stdin| stdin.poll(self, meter))?;
                    let before_stdout = output[0].len();
                    let before_stderr = output[1].len();
                    let drained = (|| {
                        Ok::<_, io::Error>(
                            drain_pipe(&mut stdout, &mut output[0], &mut remaining, meter)?
                                | drain_pipe(&mut stderr, &mut output[1], &mut remaining, meter)?,
                        )
                    })();
                    if output[0].len() > before_stdout {
                        let chunk = output[0]
                            .get(before_stdout..)
                            .ok_or_else(|| io::Error::other("invalid observed process stdout"))?;
                        if let Some(input) = input.as_mut().filter(|input| !input.complete) {
                            input.observe(
                                chunk,
                                &mut |bytes| observe(ProcessOutput::Stdout(bytes)),
                                meter,
                            )?;
                        } else {
                            observe_output(ProcessOutput::Stdout(chunk), &mut observe)?;
                        }
                    }
                    if output[1].len() > before_stderr {
                        let chunk = output[1]
                            .get(before_stderr..)
                            .ok_or_else(|| io::Error::other("invalid observed process stderr"))?;
                        observe_output(ProcessOutput::Stderr(chunk), &mut observe)?;
                    }
                    progressed |= drained?;
                    if status.is_none()
                        && let Some(exit) = self.try_wait(meter)?
                    {
                        status = Some(exit);
                        self.capture_terminate_descendants(meter)?;
                    }
                    if status.is_some() && stdout.is_none() && stderr.is_none() {
                        // An exchange the child left before taking all of its
                        // queued input failed, whether or not a write saw the
                        // closed pipe before the exit was observed.
                        if input.as_ref().is_some_and(PendingStdin::undelivered) {
                            return Err(io::ErrorKind::BrokenPipe.into());
                        }
                        return Ok(());
                    }
                    if capture_now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "process output deadline exceeded; effects may have occurred",
                        ));
                    }
                    if !progressed {
                        std::thread::sleep(Duration::from_millis(1));
                    }
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
        if meter.is_some() {
            return self.capture_terminate(meter);
        }
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.terminate",
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.in_scope(|| operation.finish(self.capture_terminate(meter)))
    }

    fn capture_terminate(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        self.select_control_meter(meter);
        #[cfg(unix)]
        return self.terminate_with_meter(meter);
        #[cfg(windows)]
        {
            self.capture_terminate_descendants(meter)?;
            self.capture_close_stdin(meter)?;
            if self.child.is_some() {
                let deadline = Instant::now() + Self::TERMINATION_TIMEOUT;
                while self.try_wait(meter)?.is_none() {
                    if Instant::now() >= deadline {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                    process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                        std::thread::sleep(Self::TERMINATION_POLL_INTERVAL);
                        Ok(())
                    })?;
                }
                if let Some(child) = self.child.as_mut() {
                    if child.stdout.is_some() {
                        process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                            drop(child.stdout.take());
                            Ok(())
                        })?;
                    }
                    if child.stderr.is_some() {
                        process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                            drop(child.stderr.take());
                            Ok(())
                        })?;
                    }
                }
                // Child's private Win32 handle release is one opaque std SDK
                // boundary, not an estimate of hidden handle/kernel internals.
                process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                    drop(self.child.take());
                    Ok(())
                })?;
            }
            self.guard.close(meter)
        }
    }

    #[cfg(unix)]
    fn terminate_with_meter(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
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
                    process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                        drop(child.stdout.take());
                        Ok(())
                    })?;
                }
                if child.stderr.is_some() {
                    process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                        drop(child.stderr.take());
                        Ok(())
                    })?;
                }
            }
            let (reaped, recorded) =
                platform::reap_exit_id(self.owned_child_id()? as libc::pid_t, meter)?;
            if reaped.is_some() {
                self.child.take();
                self.native_child.take();
            }
            recorded?;
            if reaped.is_none() {
                return Err(io::ErrorKind::Other.into());
            }
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
        return self.child.is_none()
            && self.native_child.is_none()
            && !self.spawn_resources.pending();
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
        operation.in_scope(|| {
            operation.finish((|| {
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
            })())
        })
    }

    /// Signals termination to the entire tree while retaining the direct child so its captured output can still be collected.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        self.capture_terminate_descendants(None)
    }

    fn capture_terminate_descendants(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
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
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            };
            containment.and(direct)
        }
    }
}

/// Reads one chunk; returns whether the pipe produced data or reached EOF, so the caller sleeps only when both pipes are idle.
impl Drop for ProcessTree {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        let _ = self.terminate(None);
    }
}

/// One actual SDK/kernel attempt, not a prediction of its hidden internals.
fn process_control_attempt<T>(
    meter: Option<&dyn crate::NativeReadMeter>,
    kind: crate::NativeOperationKind,
    call: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let (result, recorded) = pipe_operation_attempt(meter, kind, call)?;
    match result {
        Err(error) => Err(error),
        Ok(value) => recorded.map(|()| value),
    }
}
