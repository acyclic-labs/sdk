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
pub struct ProcessTree {
    child: Option<Child>,
    guard: platform::Guard,
    output_taken: bool,
}

/// A stopped capture, retaining its bounded output and observed cleanup state.
/// Effects may have occurred regardless of whether cleanup completed.
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

struct PendingStdin {
    bytes: Vec<u8>,
    written: usize,
    maximum: usize,
    complete: bool,
    stopped: bool,
}

impl PendingStdin {
    fn new(initial: &[u8], maximum: usize) -> io::Result<Self> {
        if maximum == 0 || initial.len() > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid process input allowance",
            ));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve(initial.len()).map_err(io::Error::other)?;
        bytes.extend_from_slice(initial);
        Ok(Self {
            bytes,
            written: 0,
            maximum,
            complete: false,
            stopped: false,
        })
    }

    fn poll(&mut self, tree: &mut ProcessTree) -> io::Result<bool> {
        let mut progressed = false;
        if self.written < self.bytes.len() {
            let suffix = self
                .bytes
                .get(self.written..)
                .ok_or_else(|| io::Error::other("invalid queued process input"))?;
            if let Some(written) = tree.write_stdin(suffix)? {
                if written == 0 || written > suffix.len() {
                    return Err(io::Error::other("invalid process write length"));
                }
                self.written += written;
                progressed = true;
            }
        }
        if self.complete && self.written == self.bytes.len() && !self.stopped {
            tree.close_stdin();
            tree.terminate_descendants()?;
            self.stopped = true;
            progressed = true;
        }
        Ok(progressed)
    }

    fn observe(
        &mut self,
        chunk: &[u8],
        observe: &mut impl FnMut(&[u8]) -> io::Result<ProcessInput>,
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
        self.bytes
            .try_reserve(total - self.bytes.len())
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(&action.bytes);
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
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.spawn",
            program = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        // The program's file name only: arguments and directories may be private.
        let program = std::path::Path::new(command.get_program()).file_name();
        operation.record("program", program.and_then(|name| name.to_str()));
        operation.scope(|| {
            operation.finish(platform::spawn(command).map(|(child, guard)| Self {
                child: Some(child),
                guard,
                output_taken: false,
            }))
        })
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
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("process output was already collected"))?;
        #[cfg(unix)]
        return platform::observe_exit(child);
        #[cfg(windows)]
        child.try_wait()
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
        operation.scope(|| operation.finish(self.wait_unobserved(timeout)))
    }

    fn wait_unobserved(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "process deadline overflow")
        })?;
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("process output was already collected"))?;
        drop(child.stdin.take());
        loop {
            if let Some(status) = self.try_wait()? {
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
        })
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
            Some((initial, max_input_bytes)),
            check,
            observe,
        )
    }

    fn capture_output(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<(&[u8], usize)>,
        mut check: impl FnMut() -> io::Result<()>,
        mut observe: impl FnMut(&[u8]) -> io::Result<ProcessInput>,
    ) -> Result<Output, ProcessCaptureFailure> {
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
            let mut input = input
                .map(|(initial, maximum)| PendingStdin::new(initial, maximum))
                .transpose()
                .map_err(ProcessCaptureFailure::before_capture)?;
            let child = self.child.as_mut().ok_or_else(|| {
                ProcessCaptureFailure::before_capture(io::Error::other(
                    "process output was already collected",
                ))
            })?;
            self.output_taken = true;
            if input.is_none() {
                drop(child.stdin.take());
            }
            let mut stdout = child.stdout.take();
            let mut stderr = child.stderr.take();
            let mut output = [Vec::new(), Vec::new()];
            let mut status = None;
            let result = (|| loop {
                check()?;
                let mut progressed = input.as_mut().map_or(Ok(false), |stdin| stdin.poll(self))?;
                let before_stdout = output[0].len();
                progressed |= drain_pipe(&mut stdout, &mut output[0], &mut remaining)?
                    | drain_pipe(&mut stderr, &mut output[1], &mut remaining)?;
                if let Some(input) = input.as_mut()
                    && !input.complete
                    && output[0].len() > before_stdout
                {
                    let chunk = output[0]
                        .get(before_stdout..)
                        .ok_or_else(|| io::Error::other("invalid observed process stdout"))?;
                    input.observe(chunk, &mut observe)?;
                }
                if status.is_none()
                    && let Some(exit) = self.try_wait()?
                {
                    status = Some(exit);
                    self.terminate_descendants()?;
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
            })();
            // Cleanup errors take precedence: a capture timeout alone does not
            // describe an unresolved termination. Keep Child ownership on error.
            let result = self.terminate().and(result).and_then(|()| {
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
        operation.scope(|| operation.finish(self.terminate_unobserved()))
    }

    fn terminate_unobserved(&mut self) -> io::Result<()> {
        self.terminate_descendants()?;
        if self.child.is_some() {
            self.wait(Duration::from_secs(5))?;
            let child = self
                .child
                .as_mut()
                .ok_or_else(|| io::Error::other("child ownership lost"))?;
            // Exit was observed above. This reaps the retained Unix leader;
            // Windows try_wait returns its already-cached terminal status.
            if child.try_wait()?.is_none() {
                return Err(io::Error::other("observed child exit is now unresolved"));
            }
            self.child.take();
        }
        Ok(())
    }

    /// Whether containment cleanup succeeded and the direct child was reaped.
    /// Unix confirms group signal delivery, not reaping of every descendant;
    /// descendants that deliberately leave containment remain outside this claim.
    #[must_use]
    pub const fn is_reaped(&self) -> bool {
        self.child.is_none()
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
        self.guard.request_termination()?;
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
        self.guard.terminate()
    }
}

/// Reads one chunk; returns whether the pipe produced data or reached EOF,
/// so the caller sleeps only when both pipes are idle.
fn drain_pipe<T: Read + platform::Pipe>(
    pipe: &mut Option<T>,
    output: &mut Vec<u8>,
    remaining: &mut usize,
) -> io::Result<bool> {
    let Some(reader) = pipe.as_mut() else {
        return Ok(false);
    };
    let mut buffer = [0; 64 * 1024];
    match platform::read_pipe(reader, &mut buffer)? {
        Some(0) => {
            pipe.take();
        }
        Some(read) => {
            tracing::trace!(name: "acyclic.runtime.output_chunk", bytes = read);
            let bytes = buffer
                .get(..read)
                .ok_or_else(|| io::Error::other("pipe returned an invalid length"))?;
            let admitted = read.min(*remaining);
            output.try_reserve(admitted).map_err(io::Error::other)?;
            output.extend_from_slice(
                bytes
                    .get(..admitted)
                    .ok_or_else(|| io::Error::other("invalid admitted pipe prefix"))?,
            );
            *remaining -= admitted;
            if admitted != read {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "process output limit exceeded; effects may have occurred",
                ));
            }
        }
        None => return Ok(false),
    }
    Ok(true)
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::io::{Read, Write};
    pub(super) use std::os::fd::AsRawFd as Pipe;
    use std::os::unix::process::CommandExt as _;
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::{Child, Command, ExitStatus};

    pub(super) fn observe_exit(child: &Child) -> io::Result<Option<ExitStatus>> {
        observe_exit_id(child.id())
    }

    fn observe_exit_id(id: libc::id_t) -> io::Result<Option<ExitStatus>> {
        let flags = libc::WEXITED | libc::WNOWAIT | libc::WNOHANG;
        loop {
            // SAFETY: initialized OS output storage and the exclusively owned
            // child's ID. WNOWAIT retains the leader until group termination,
            // preventing its PID/PGID from being reused for an unrelated group.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: `info` is writable storage owned by this frame and `id`
            // names the exclusively owned child.
            if unsafe { libc::waitid(libc::P_PID, id, &raw mut info, flags) } != 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
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
                _ => return Err(io::Error::other("unexpected child exit observation")),
            };
            return Ok(Some(ExitStatus::from_raw(raw)));
        }
    }

    pub(super) fn read_pipe(
        pipe: &mut (impl Read + Pipe),
        buffer: &mut [u8],
    ) -> io::Result<Option<usize>> {
        nonblocking(pipe)?;
        match pipe.read(buffer) {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(None)
            }
            result => result.map(Some),
        }
    }

    pub(super) fn write_pipe(
        pipe: &mut (impl Write + Pipe),
        bytes: &[u8],
    ) -> io::Result<Option<usize>> {
        nonblocking(pipe)?;
        match pipe.write(bytes) {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(None)
            }
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "process pipe write stopped",
            )),
            result => result.map(Some),
        }
    }

    fn nonblocking(pipe: &impl Pipe) -> io::Result<()> {
        // SAFETY: this owned pipe is used exclusively by the collector. Setting
        // O_NONBLOCK prevents backpressure or an escaped descendant from holding
        // up intent checks, deadline observation or cleanup.
        let flags = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETFL) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the same owned pipe; only O_NONBLOCK is added to the flags
        // just read from it.
        if unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) struct Guard {
        process_group: libc::pid_t,
        active: bool,
    }

    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Guard)> {
        command.process_group(0);
        let child = command.spawn()?;
        let process_group = libc::pid_t::try_from(child.id())
            .map_err(|_| io::Error::other("child process id does not fit pid_t"))?;
        Ok((
            child,
            Guard {
                process_group,
                active: true,
            },
        ))
    }

    impl Guard {
        fn group_gone(&self, error: &io::Error) -> io::Result<bool> {
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(true);
            }
            #[cfg(target_vendor = "apple")]
            if error.raw_os_error() == Some(libc::EPERM) {
                return group_has_no_live_members(self.process_group);
            }
            Ok(false)
        }

        pub(super) fn request_termination(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: the unreaped owned leader reserves this process group.
            if unsafe { libc::kill(-self.process_group, libc::SIGTERM) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if self.group_gone(&error)? {
                Ok(())
            } else {
                Err(error)
            }
        }

        pub(super) fn terminate(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: a negative, nonzero pid addresses exactly this owned
            // process group; SIGKILL requires no shared memory or signal data.
            if unsafe { libc::kill(-self.process_group, libc::SIGKILL) } == 0 {
                self.active = false;
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if self.group_gone(&error)? {
                self.active = false;
                Ok(())
            } else {
                Err(error)
            }
        }
    }

    #[cfg(target_vendor = "apple")]
    fn group_has_no_live_members(group: libc::pid_t) -> io::Result<bool> {
        // Terminated descendants can remain visible until the OS reaps them.
        // Wait only for the same conservative membership proof; EPERM itself
        // never establishes that any member has exited. Denial stays an error
        // if this bounded observation does not establish an empty group or
        // exactly the independently observed exited owned leader.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if group_snapshot_has_no_live_members(group)? {
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[cfg(target_vendor = "apple")]
    fn group_snapshot_has_no_live_members(group: libc::pid_t) -> io::Result<bool> {
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
        let read = unsafe {
            *libc::__error() = 0;
            libc::proc_listpids(2, id, members.as_mut_ptr().cast(), bytes)
        };
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
            return Ok(observe_exit_id(id)?.is_some());
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessTree;
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
    use std::process::{Child, Command};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_BROKEN_PIPE, HANDLE, INVALID_HANDLE_VALUE,
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
        pipe: &mut (impl Write + Pipe),
        bytes: &[u8],
    ) -> io::Result<Option<usize>> {
        let mode = PIPE_NOWAIT;
        // SAFETY: the exclusively owned anonymous byte pipe supports changing
        // this writer's wait mode. No background I/O or borrowed buffer survives
        // the call. Nonblocking WriteFile may write only the available capacity.
        if unsafe {
            SetNamedPipeHandleState(
                pipe.as_raw_handle().cast(),
                &mode,
                std::ptr::null(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut candidate = bytes;
        loop {
            match pipe.write(candidate) {
                // Windows may accept zero bytes when a large write cannot fit,
                // even with capacity for a smaller prefix. Probe progressively
                // smaller prefixes, bounded logarithmically by this input slice;
                // a zero-byte single-byte write establishes backpressure.
                Ok(0) if candidate.len() > 1 => {
                    candidate = candidate
                        .get(..candidate.len() / 2)
                        .ok_or_else(|| io::Error::other("invalid process write prefix"))?;
                }
                Ok(0) => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(None),
                result => return result.map(Some),
            }
        }
    }

    pub(super) fn read_pipe(
        pipe: &mut (impl Read + Pipe),
        buffer: &mut [u8],
    ) -> io::Result<Option<usize>> {
        let mut available = 0;
        // SAFETY: this live, owned anonymous pipe has one reader. Peek does not
        // consume data; only that reader can reduce the available byte count.
        if unsafe {
            PeekNamedPipe(
                pipe.as_raw_handle().cast(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &raw mut available,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.cast_signed()) {
                Ok(Some(0))
            } else {
                Err(error)
            };
        }
        if available == 0 {
            return Ok(None);
        }
        let length = buffer.len().min(available as usize);
        pipe.read(
            buffer
                .get_mut(..length)
                .ok_or_else(|| io::Error::other("invalid pipe read length"))?,
        )
        .map(Some)
    }

    pub(super) struct Guard {
        job: HANDLE,
        active: bool,
    }

    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Guard)> {
        let mut guard = Guard::new()?;
        command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP);
        let mut child = command.spawn()?;
        // SAFETY: the Job and Child each own live handles for the duration of
        // this call. The suspended child cannot create descendants before it
        // has been assigned to the kill-on-close Job.
        if unsafe { AssignProcessToJobObject(guard.job, child.as_raw_handle().cast()) } == 0 {
            let error = io::Error::last_os_error();
            return Err(failed_admission(&mut child, &mut guard, error));
        }
        if let Err(error) = resume_process(child.id()) {
            return Err(failed_admission(&mut child, &mut guard, error));
        }
        Ok((child, guard))
    }

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
        fn new() -> io::Result<Self> {
            // SAFETY: null security attributes and name request an unnamed Job
            // with default access controls.
            let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the structure is plain Win32 data and zero is its
            // documented initial state before setting the requested limit.
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `limits` is initialized and its exact size is provided.
            let configured = unsafe {
                SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    (&raw const limits).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .map_err(|_| io::Error::other("Job limit structure is too large"))?,
                )
            };
            if configured == 0 {
                let error = io::Error::last_os_error();
                // SAFETY: `job` is an owned live handle.
                unsafe { CloseHandle(job) };
                return Err(error);
            }
            Ok(Self { job, active: true })
        }

        pub(super) fn terminate(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: `job` remains owned until Drop closes it.
            if unsafe { TerminateJobObject(self.job, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // TerminateJobObject initiates termination; success alone does not
            // establish that descendants have stopped executing.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                // SAFETY: this plain Win32 structure is initialized before the
                // query writes its exact size through an owned live Job handle.
                let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
                // SAFETY: `job` is owned and live, and `accounting` is a writable
                // structure of exactly the size passed to the query.
                if unsafe {
                    QueryInformationJobObject(
                        self.job,
                        JobObjectBasicAccountingInformation,
                        (&raw mut accounting).cast(),
                        u32::try_from(size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                            .map_err(|_| {
                                io::Error::other("Job accounting structure is too large")
                            })?,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                if accounting.ActiveProcesses == 0 {
                    self.active = false;
                    return Ok(());
                }
                if std::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "process Job termination is still unresolved",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            // SAFETY: this is the final owned Job handle. Kill-on-close is the
            // fallback after explicit cleanup fails; closing it does not
            // manufacture a successful cleanup observation.
            unsafe { CloseHandle(self.job) };
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

    fn resume_process(process_id: u32) -> io::Result<()> {
        // SAFETY: flags request a system-owned snapshot handle.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: zero is the documented initialization; dwSize is set below.
        let mut entry: THREADENTRY32 = unsafe { zeroed() };
        entry.dwSize = u32::try_from(size_of::<THREADENTRY32>())
            .map_err(|_| io::Error::other("thread entry structure is too large"))?;
        // SAFETY: snapshot and entry are live for the iteration.
        let mut found = unsafe { Thread32First(snapshot, &raw mut entry) } != 0;
        let mut result = Err(io::Error::new(
            io::ErrorKind::NotFound,
            "suspended child thread was not found",
        ));
        while found {
            if entry.th32OwnerProcessID == process_id {
                // SAFETY: the thread id came from the live system snapshot.
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    result = Err(io::Error::last_os_error());
                } else {
                    // SAFETY: `thread` is a live handle with resume access.
                    result = if unsafe { ResumeThread(thread) } == u32::MAX {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    };
                    // SAFETY: this function owns the thread handle.
                    unsafe { CloseHandle(thread) };
                }
                break;
            }
            // SAFETY: snapshot and entry remain live for the next iteration.
            found = unsafe { Thread32Next(snapshot, &raw mut entry) } != 0;
        }
        // SAFETY: this function owns the snapshot handle.
        unsafe { CloseHandle(snapshot) };
        result
    }
}
