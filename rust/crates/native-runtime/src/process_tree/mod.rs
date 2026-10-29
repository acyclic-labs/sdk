#![allow(
    unsafe_code,
    reason = "process containment and bounded pipes use platform OS calls"
)]

use crate::obs;
use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

mod capture;
#[cfg(not(target_arch = "wasm32"))]
mod external_host;
#[cfg(not(target_arch = "wasm32"))]
pub use external_host::run_external_process_host;
#[cfg(test)]
mod capture_events_tests;
#[cfg(windows)]
#[path = "capture_wait_windows.rs"]
mod capture_wait;
#[cfg(windows)]
mod custodian;
mod events;
mod wake;
pub use wake::ProcessWake;
#[cfg(unix)]
mod exit_wait;
#[cfg(any(windows, test))]
mod job_members;
mod pump;
mod stream;
pub use stream::{ProcessCaptureStream, ProcessChunk};
#[cfg(windows)]
mod spawner;
pub use pump::{CaptureHandler, CaptureOutcome, CaptureSchedule, OwnedCaptureInput};
#[cfg(windows)]
mod custodian_admission;
mod custody;
pub use custody::{ProcessCustody, ProcessCustodyLease};
mod storage;
pub use storage::{ProcessDrain, ProcessTreeIo, ProcessTreeStorage};
#[cfg(unix)]
mod warden;

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
    CAPTURE_CLOCK
        .with(std::cell::Cell::get)
        .unwrap_or_else(Instant::now)
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
#[cfg(test)]
mod custody_tests;
#[cfg(unix)]
#[path = "unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "windows.rs"]
mod platform;
#[cfg(test)]
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod scale_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[cfg(windows)]
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
    custody: Option<custody::Record>,
    /// The direct child's pidfd, opened by its first wait and closed at reap.
    #[cfg(target_os = "linux")]
    exit_fd: Option<std::os::fd::OwnedFd>,
    /// Owner of overlapped stdin writes, created by the first write.
    writer: platform::Writer,
    /// Input owned by an incremental native-session supervisor. The writer
    /// drops before this storage, including a pending overlapped operation.
    queued_stdin: Option<PendingStdin<'static>>,
    /// Completion-port readiness of the retained stdout and stderr pipes,
    /// associated by the first [`ProcessTree::readiness`].
    #[cfg(windows)]
    readers: [Option<std::sync::Arc<events::PipeOperation>>; 2],
    /// The process-event hub every wait of this tree arms, held from before
    /// its child is created.
    _events: events::Events,
}

/// One armed wait on a process tree's retained pipes and direct child, served
/// by the shared process-event hub: epoll, kqueue or an I/O completion port.
/// It holds no thread and fires once.
pub struct ProcessReadiness(events::Wait);

impl std::future::Future for ProcessReadiness {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        std::pin::Pin::new(&mut self.0).poll(context)
    }
}

impl ProcessTree {
    fn retire_owner(&mut self, retirement: &mut storage::Retirement) {
        let result = (|| {
            if self.guard.needs_kill() {
                retirement.writes.attempt(|| self.guard.kill()).0?;
            }
            #[cfg(windows)]
            if !self.guard.assigned
                && !self.guard.direct_termination_sent
                && let Some(child) = self.child.as_mut()
            {
                retirement.writes.attempt(|| child.kill()).0?;
            }
            if let Some(child) = self.child.as_mut() {
                retirement.wait.attempt(|| child.wait()).0?;
            }
            #[cfg(windows)]
            if self.guard.pending() {
                retirement.writes.attempt(|| self.guard.close(None)).0?;
            }
            Ok::<_, io::Error>(())
        })();
        if let Err(error) = result {
            #[cfg(unix)]
            let recovery = format!("kill -TERM {}", std::process::id());
            #[cfg(windows)]
            let recovery = format!("taskkill /PID {} /T /F", std::process::id());
            eprintln!("step=retire native process owner: {error}; recovery: {recovery}");
        }
    }

    /// Maximum observation time for each physical termination phase.
    pub const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Kernel observations that decide a macOS process group whose kill was
    /// refused, for a group of up to 255 members: two listings and one probe
    /// per member, with no wait. A larger group needs its member count plus
    /// four. No termination phase observes on a clock: an exit wait reads
    /// once per exit event or deadline.
    pub const TERMINATION_OBSERVATIONS: u64 = 257;

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

    /// Starts a child whose whole tree is tied to this host process and
    /// recorded in `custody` until this owner observes it gone. If the host
    /// dies first, the tree is killed, and a reopened owner's
    /// [`ProcessCustody::fence_stale`] returns only after it is dead.
    /// `command` must not be reused for another spawn.
    pub fn spawn_in_custody(command: &mut Command, custody: &ProcessCustody) -> io::Result<Self> {
        let mut retained = None;
        if let Err(error) = Self::spawn_in_custody_controlled(command, custody, None, &mut retained)
        {
            if let Some(tree) = retained.as_mut() {
                tree.terminate(None)?;
            }
            return Err(error);
        }
        retained.ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    /// Starts a durably recorded tree on the original invocation's meter.
    /// Admission and cleanup failures leave every acquired tree and record in
    /// `retained`. The caller keeps this slot through recovery; an error does
    /// not mean that the tree is absent or that its effects can be settled.
    pub fn spawn_in_custody_controlled(
        command: &mut Command,
        custody: &ProcessCustody,
        meter: Option<&dyn crate::NativeReadMeter>,
        retained: &mut Option<Self>,
    ) -> io::Result<()> {
        if retained.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        #[cfg(unix)]
        {
            let directory =
                process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                    custody.arm(command)
                })?;
            let spawned = Self::spawn_controlled(command, meter, retained);
            if let Some(tree) = retained.as_mut()
                && (tree.child.is_some() || tree.native_child.is_some())
            {
                let pid = libc::pid_t::try_from(tree.owned_child_id()?)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                tree.custody = Some(custody::Record::new(directory, pid));
            }
            spawned
        }
        #[cfg(windows)]
        {
            // Event startup can fail. Finish it before acquiring a physical
            // custody record, so every later failure has a retained tree slot.
            let events = events::Events::start()?;
            let (record, recorded) =
                pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
                    custody.record()
                })?;
            let (record, name) = record?;
            platform::spawn_named(command, meter, retained, &name, record, recorded, events)
        }
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
        let mut arguments = crate::meter::ReadBuffer::new(argument_slots, &meter)?;
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
        let mut environment = crate::meter::ReadBuffer::new(environment_count, &meter)?;
        let pointer_count = environment_count
            .checked_add(1)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut environment_pointers = crate::meter::ReadBuffer::new(pointer_count, &meter)?;
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
            let mut bytes = crate::meter::ReadBuffer::new(terminated, &meter)?;
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
        if self.queued_stdin.is_some() {
            return None;
        }
        self.child.as_mut()?.stdin.take()
    }

    /// Queues one literal prompt without transferring the pipe or starting a
    /// writer thread. Uses the same partial-write/EOF pump as process capture.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        if self.queued_stdin.is_some() || self.output_taken {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        if self
            .child
            .as_ref()
            .and_then(|child| child.stdin.as_ref())
            .is_none()
        {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        self.queued_stdin = Some(PendingStdin::required_literal(bytes)?);
        Ok(())
    }

    /// Advances queued stdin once without waiting. True means all bytes were
    /// delivered and EOF sent; a peer closing early returns `BrokenPipe`.
    pub fn pump_stdin(&mut self) -> io::Result<bool> {
        self.pump_stdin_controlled(None)
    }

    fn pump_stdin_controlled(
        &mut self,
        owner: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    ) -> io::Result<bool> {
        let Some(mut input) = self.queued_stdin.take() else {
            return Ok(true);
        };
        input.meter_owner = owner.cloned();
        let result = input.poll(self, owner.map(|owner| owner.as_ref()));
        let complete = input.finished();
        if !complete {
            self.queued_stdin = Some(input);
        }
        result?;
        Ok(complete)
    }

    /// Refuses completion of a session whose queued prompt was not delivered.
    pub fn require_stdin_complete(&self) -> io::Result<()> {
        if self.queued_stdin.is_some() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(())
    }

    /// Transfers stdout to a host-owned reader, which owns its output bounds.
    pub fn take_stdout(&mut self) -> Option<std::process::ChildStdout> {
        self.child.as_mut()?.stdout.take()
    }

    /// Transfers stderr to a host-owned reader, which owns its output bounds.
    pub fn take_stderr(&mut self) -> Option<std::process::ChildStderr> {
        self.child.as_mut()?.stderr.take()
    }

    /// Arms one wait that fires when retained stdout or stderr has data or
    /// closed, queued stdin accepts bytes, or the direct child exits. Its
    /// owner then reads without waiting, as before; no thread or timer runs
    /// while it waits.
    pub fn readiness(&mut self) -> io::Result<ProcessReadiness> {
        let writing = self
            .queued_stdin
            .as_ref()
            .is_some_and(PendingStdin::writing);
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            if self.child.is_some() {
                self.prepare_exit_source()?;
            }
            let mut sources = Vec::with_capacity(4);
            if let Some(child) = &self.child {
                if let Some(pipe) = &child.stdout {
                    sources.push(events::Source::Readable(pipe.as_fd()));
                }
                if let Some(pipe) = &child.stderr {
                    sources.push(events::Source::Readable(pipe.as_fd()));
                }
                if writing && let Some(pipe) = &child.stdin {
                    sources.push(events::Source::Writable(pipe.as_fd()));
                }
            }
            sources.extend(self.exit_source()?);
            events::arm(&sources, None).map(ProcessReadiness)
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsHandle as _;
            let Some(child) = &self.child else {
                return events::arm(&[], None).map(ProcessReadiness);
            };
            let [stdout, stderr] = &mut self.readers;
            if stdout.is_none()
                && let Some(pipe) = &child.stdout
            {
                *stdout = Some(events::PipeOperation::reader(pipe)?);
            }
            if stderr.is_none()
                && let Some(pipe) = &child.stderr
            {
                *stderr = Some(events::PipeOperation::reader(pipe)?);
            }
            let mut sources = Vec::with_capacity(4);
            if child.stdout.is_some()
                && let Some(reader) = &self.readers[0]
            {
                sources.push(events::Source::Pipe(reader));
            }
            if child.stderr.is_some()
                && let Some(reader) = &self.readers[1]
            {
                sources.push(events::Source::Pipe(reader));
            }
            if writing && let Some(writer) = &self.writer.0 {
                sources.push(events::Source::Pipe(writer));
            }
            sources.push(events::Source::Exit(child.as_handle()));
            events::arm(&sources, None).map(ProcessReadiness)
        }
    }

    /// Reads owned stdout without waiting for data.
    pub fn read_stdout(&mut self, buffer: &mut [u8]) -> io::Result<Option<usize>> {
        self.read_stdout_controlled(buffer, None)
    }

    fn read_stdout_controlled(
        &mut self,
        buffer: &mut [u8],
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<usize>> {
        self.read_output_controlled(buffer, meter, false)
    }

    fn read_output_controlled(
        &mut self,
        buffer: &mut [u8],
        meter: Option<&dyn crate::NativeReadMeter>,
        stderr: bool,
    ) -> io::Result<Option<usize>> {
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty process read buffer",
            ));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(Some(0));
        };
        // SAFETY: initialized bytes can be borrowed as MaybeUninit bytes.
        let buffer =
            unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast(), buffer.len()) };
        let result = if stderr {
            child
                .stderr
                .as_mut()
                .map(|pipe| platform::read_pipe(pipe, buffer, meter))
        } else {
            child
                .stdout
                .as_mut()
                .map(|pipe| platform::read_pipe(pipe, buffer, meter))
        };
        let Some(result) = result else {
            return Ok(Some(0));
        };
        let (read, recorded) = result?;
        // A pipe at end of file is released, so no later readiness wait
        // reports its permanent EOF; reads then report EOF as before.
        if read == Some(0) {
            if stderr {
                drop(child.stderr.take());
            } else {
                drop(child.stdout.take());
            }
        }
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
                {
                    exit_wait::wait(self.owned_child_id()?, deadline)
                }
            })())
        })
    }

    /// Captures bounded output and optional input, preserving partial output
    /// on failure. The calling thread parks between capture turns; it never
    /// sleeps on a fixed interval.
    #[allow(
        clippy::too_many_arguments,
        reason = "capture and cleanup share the same owned pipes, callbacks and deadline"
    )]
    pub fn wait_with_output(
        &mut self,
        schedule: impl Into<CaptureSchedule>,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        check: impl FnMut() -> io::Result<()>,
        observe: impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        meter: Option<&dyn crate::NativeReadMeter>,
        buffers: Option<[Vec<u8>; 3]>,
        wake: Option<&[ProcessWake]>,
    ) -> Result<Output, ProcessCaptureFailure> {
        self.wait_with_output_controlled(
            schedule,
            max_bytes,
            input,
            check,
            observe,
            (meter, None),
            buffers,
            wake,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "same capture loop retains the caller's original accounting owner"
    )]
    fn wait_with_output_controlled(
        &mut self,
        schedule: impl Into<CaptureSchedule>,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        mut check: impl FnMut() -> io::Result<()>,
        mut observe: impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        (meter, owner): (
            Option<&dyn crate::NativeReadMeter>,
            Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
        ),
        buffers: Option<[Vec<u8>; 3]>,
        wake: Option<&[ProcessWake]>,
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
        let mut captured = 0;
        let result = operation.in_scope(|| {
            if self.output_taken {
                return Err(ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                )));
            }
            let (output, stdin_buffer) = match buffers {
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
            let input = input
                .map(|input| PendingStdin::new_with_buffer(input, stdin_buffer, meter, owner))
                .transpose()
                .map_err(ProcessCaptureFailure::before_capture)?;
            let mut pump = pump::Pump::new(self, schedule.into(), max_bytes, input, meter, output)?;
            let result = 'capture: loop {
                for signal in wake.unwrap_or_default() {
                    if let Err(error) = signal.consume() {
                        break 'capture Err(error);
                    }
                }
                match pump.turn(self, &mut check, &mut observe, meter) {
                    pump::Turn::Done(result) => break result,
                    pump::Turn::Wait => match pump.arm_with_wake(self, wake) {
                        Ok(wait) => wait.block(),
                        Err(error) => break Err(error),
                    },
                }
            };
            captured = pump.captured(max_bytes);
            pump.finish(self, result, meter)
        });
        operation.record("bytes", captured);
        operation.record_error(result.as_ref().err().map(|failure| &failure.error));
        result
    }

    /// Captures bounded output like [`Self::wait_with_output`], holding no
    /// thread while the child runs: each turn runs on the blocking pool, and
    /// between turns the capture awaits one readiness wait on its pipes, its
    /// exit and its next check. The returned future owns the tree until
    /// cleanup finishes: dropping it early terminates the tree from the
    /// dropping thread, so callers that may be cancelled spawn it. An `Err`
    /// means a turn could not be submitted, and the dropped tree cleans
    /// itself up.
    pub async fn capture<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> io::Result<CaptureOutcome<H>> {
        // How many turns a capture takes is OS readiness timing. A simulation
        // scope sees all of them as one physical submission, so a seeded
        // replay schedules the same work however the child's output arrives.
        #[cfg(feature = "test-support")]
        if crate::testing::Context::current().is_some() {
            return self
                .capture_physical(schedule, max_bytes, input, handler)
                .await;
        }
        self.capture_scheduled(schedule, max_bytes, input, handler)
            .await
    }

    /// Captures with each turn as one pool job, awaiting readiness between
    /// them without holding a thread.
    async fn capture_scheduled<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> io::Result<CaptureOutcome<H>> {
        let (mut tree, mut pump, mut handler) = match self
            .start_capture(schedule, max_bytes, input, handler)
            .await?
        {
            Ok(started) => started,
            Err(outcome) => return Ok(outcome),
        };
        let result = loop {
            let (turned, (returned_tree, returned_pump, returned_handler)) =
                crate::run_blocking_io(
                    move || {
                        let turn = pump::handled_turn(&mut pump, &mut tree, &mut handler);
                        (turn, (tree, pump, handler))
                    },
                    None,
                )?
                .await?;
            tree = returned_tree;
            pump = returned_pump;
            handler = returned_handler;
            match turned {
                pump::Turn::Done(result) => break result,
                pump::Turn::Wait => match pump.arm_with_wake(&mut tree, Some(handler.wakes())) {
                    Ok(wait) => wait.await,
                    Err(error) => break Err(error),
                },
            }
        };
        let (result, handler) = crate::run_blocking_io(
            move || {
                let result = pump.finish(&mut tree, result, None);
                drop(tree);
                (result, handler)
            },
            None,
        )?
        .await?;
        Ok(CaptureOutcome { handler, result })
    }

    /// Captures in one pool job that blocks on readiness between turns.
    #[cfg(feature = "test-support")]
    async fn capture_physical<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> io::Result<CaptureOutcome<H>> {
        let (mut tree, mut pump, mut handler) = match self
            .start_capture(schedule, max_bytes, input, handler)
            .await?
        {
            Ok(started) => started,
            Err(outcome) => return Ok(outcome),
        };
        crate::run_blocking_io(
            move || {
                let result = loop {
                    match pump::handled_turn(&mut pump, &mut tree, &mut handler) {
                        pump::Turn::Done(result) => break result,
                        pump::Turn::Wait => {
                            match pump.arm_with_wake(&mut tree, Some(handler.wakes())) {
                                Ok(wait) => wait.block(),
                                Err(error) => break Err(error),
                            }
                        }
                    }
                };
                let result = pump.finish(&mut tree, result, None);
                drop(tree);
                CaptureOutcome { handler, result }
            },
            None,
        )?
        .await
    }

    /// Starts a capture's pump, or cleans up a tree whose capture cannot start.
    async fn start_capture<H: CaptureHandler>(
        mut self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> io::Result<Result<StartedCapture<H>, CaptureOutcome<H>>> {
        let start = (|| {
            if self.output_taken {
                return Err(ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                )));
            }
            let input = input
                .map(PendingStdin::owned)
                .transpose()
                .map_err(ProcessCaptureFailure::before_capture)?;
            pump::Pump::new(
                &mut self,
                schedule,
                max_bytes,
                input,
                None,
                [Vec::new(), Vec::new()],
            )
        })();
        match start {
            Ok(pump) => Ok(Ok((self, pump, handler))),
            Err(failure) => {
                // Cleanup of an unstarted capture may block; it runs on the
                // pool like every other turn.
                crate::run_blocking_io(move || drop(self), None)?.await?;
                Ok(Err(CaptureOutcome {
                    handler,
                    result: Err(failure),
                }))
            }
        }
    }

    /// Opens this tree's exit source once, before a wait borrows it.
    #[cfg(unix)]
    fn prepare_exit_source(&mut self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.exit_fd.is_none() && (self.child.is_some() || self.native_child.is_some()) {
            let pid = libc::pid_t::try_from(self.owned_child_id()?)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            // SAFETY: pidfd_open takes the unreaped owned child's PID and no flags.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = std::os::fd::RawFd::try_from(fd).map_err(io::Error::other)?;
            // SAFETY: a successful pidfd_open returns one new owned fd.
            self.exit_fd = Some(unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) });
        }
        Ok(())
    }

    /// The readiness source of the direct child's exit, once prepared.
    #[cfg(unix)]
    fn exit_source(&self) -> io::Result<Option<events::Source<'_>>> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsFd as _;
            Ok(self
                .exit_fd
                .as_ref()
                .map(|fd| events::Source::Exit(fd.as_fd())))
        }
        #[cfg(target_vendor = "apple")]
        {
            if self.child.is_none() && self.native_child.is_none() {
                return Ok(None);
            }
            let pid = libc::pid_t::try_from(self.owned_child_id()?)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            Ok(Some(events::Source::Exit(pid, std::marker::PhantomData)))
        }
    }

    /// Arms one wait on the direct child's exit, waking by `deadline`.
    #[cfg(unix)]
    fn arm_exit(&mut self, deadline: Instant) -> io::Result<events::Wait> {
        self.prepare_exit_source()?;
        let exit = self.exit_source()?;
        events::arm(exit.as_slice(), Some(deadline))
    }

    /// Arms one wait on the direct child's exit, waking by `deadline`.
    #[cfg(windows)]
    fn arm_exit(&mut self, deadline: Instant) -> io::Result<events::Wait> {
        use std::os::windows::io::AsHandle as _;
        let exit = self
            .child
            .as_ref()
            .map(|child| events::Source::Exit(child.as_handle()));
        events::arm(exit.as_slice(), Some(deadline))
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
                        self.arm_exit(deadline)?.block();
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
            self.guard.close(meter)?;
            custody::Record::retire(&mut self.custody, meter)?;
            Ok(())
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
            exit_wait::wait_metered(self.owned_child_id()?, deadline, meter)?;
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
            // The exited leader still holds its PID, so no new tree's record
            // can take this name before it is removed.
            custody::Record::retire(&mut self.custody, meter)?;
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

    /// Whether containment cleanup, child reaping and recovery retirement succeeded.
    #[must_use]
    pub const fn is_reaped(&self) -> bool {
        // Recovery authority is part of retirement on every platform. An
        // unlink/accounting failure must remain retryable on the same owner.
        if self.custody.is_some() {
            return false;
        }
        #[cfg(unix)]
        return self.child.is_none()
            && self.native_child.is_none()
            && !self.spawn_resources.pending()
            && self.custody.is_none();
        #[cfg(windows)]
        {
            self.child.is_none() && !self.guard.pending() && self.custody.is_none()
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

/// A tree whose capture started, with its pump and handler.
type StartedCapture<H> = (ProcessTree, pump::Pump<'static>, H);

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
