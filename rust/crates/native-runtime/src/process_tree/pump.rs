//! One capture, advanced in turns that never sleep.
//!
//! A turn moves every byte that is ready, observes exit and checks intent.
//! When nothing more can move, it returns, and the caller arms one wait on
//! exactly the sources that can make progress: the open output pipes, stdin
//! while input remains, the child's exit until it is observed, an explicit
//! control signal or the caller's deadline. A synchronous caller parks on that
//! wait; an async caller awaits it, holding no thread while the child runs.

use super::events::{self, Source};
use super::*;
use std::process::{ChildStderr, ChildStdout};

/// The original caller's timing policy, fixed before submission.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CaptureSchedule {
    /// Progress depends only on pipes, process exit and owner control.
    #[default]
    Unbounded,
    /// The caller's original request deadline.
    Deadline(Instant),
    /// An unrepresentable relative deadline, refused before capture.
    Overflow,
}

impl CaptureSchedule {
    /// Selects an optional caller deadline before any worker is queued.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        match capture_now().checked_add(timeout) {
            Some(deadline) => Self::Deadline(deadline),
            None => Self::Overflow,
        }
    }

    /// Uses the original caller's deadline without restarting it on a worker.
    #[must_use]
    pub const fn until(deadline: Instant) -> Self {
        Self::Deadline(deadline)
    }
}

impl From<Duration> for CaptureSchedule {
    fn from(timeout: Duration) -> Self {
        Self::new(timeout)
    }
}

/// Owned input for a capture that outlives its caller's borrows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedCaptureInput {
    /// Independently signalled input. Each transfer is bounded by `maximum`;
    /// completed transfers are retired, so total input is unbounded. Closing
    /// stdin does not terminate the process or require a stdout reply.
    Interactive {
        /// Initial owned bytes, written before requesting another transfer.
        initial: Vec<u8>,
        /// Caller-funded maximum bytes in one outstanding transfer.
        maximum: usize,
    },
    /// A streaming exchange with bounded queued input.
    Exchange {
        /// Bytes written before observing output.
        initial: Vec<u8>,
        /// Maximum queued input bytes.
        maximum: usize,
    },
    /// Exact input bytes followed by closing stdin.
    Literal(Vec<u8>),
}

/// Owned fixed control signals for one capture.
pub enum CaptureWakes {
    /// Zero or one signal without a collection allocation.
    One(Option<ProcessWake>),
    /// An existing collection, moved without copying.
    Many(Vec<ProcessWake>),
}

impl CaptureWakes {
    /// Borrows the retained signals without allocation or traversal.
    #[must_use]
    pub fn as_slice(&self) -> &[ProcessWake] {
        match self {
            Self::One(wake) => wake.as_slice(),
            Self::Many(wakes) => wakes,
        }
    }
}

impl From<Option<ProcessWake>> for CaptureWakes {
    fn from(wake: Option<ProcessWake>) -> Self {
        Self::One(wake)
    }
}

impl From<Vec<ProcessWake>> for CaptureWakes {
    fn from(wakes: Vec<ProcessWake>) -> Self {
        Self::Many(wakes)
    }
}

/// Metadata from the original capture operation, without pipe contents.
/// These are observations, not delivery acknowledgements from the child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureProgress {
    /// Input admitted to the original ordered buffer.
    InputAccepted(usize),
    /// Bytes accepted by the original OS pipe write, not necessarily consumed.
    InputWritten(usize),
    /// The original owner closed its stdin handle; the child may still run.
    StdinClosed,
    /// Bytes returned by the original stdout read.
    StdoutRead(usize),
    /// Bytes returned by the original stderr read.
    StderrRead(usize),
    /// The original stdout read returned EOF.
    StdoutEof,
    /// The original stderr read returned EOF.
    StderrEof,
    /// The original direct child's status, before descendant retirement.
    RootExited(ExitStatus),
}

/// The owner callbacks of an async capture, run on the blocking pool.
pub trait CaptureHandler: Send + 'static {
    /// Transfers the fixed signals once, on the first blocking turn.
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        Ok(None.into())
    }
    /// Confirms that capture may continue; an error stops and cleans it up.
    fn check(&mut self) -> io::Result<()>;
    /// Takes the next owned interactive transfer only after the previous
    /// transfer reached the OS pipe. Empty, incomplete input means pending:
    /// its producer must notify a signal returned by `take_wakes`, including
    /// on producer loss. Report that loss as an error, never an empty success.
    /// `complete` closes stdin after these bytes, without killing the tree.
    /// This callback is not used for literal input or stdout exchanges.
    fn input(&mut self) -> io::Result<ProcessInput> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interactive process input has no producer",
        ))
    }
    /// Observes newly captured output, returning exchange input.
    fn observe(&mut self, chunk: ProcessOutput<'_>) -> io::Result<ProcessInput>;
    /// Observes the original operation on its worker, without another reader.
    /// Keep this callback nonblocking. The handler owns correlation and any
    /// clock or fixed scalar records; capture creates none. Output callbacks
    /// can occur more than once. No observation substitutes for `retired`.
    /// Initial admission and stdin closure are reported on the first turn;
    /// a callback timestamp measures observation, not earlier setup work.
    fn progress(&mut self, _progress: CaptureProgress) {}
    /// Releases live execution admission after the original physical cleanup.
    /// Observation may retain this handler longer; it cannot delay retirement.
    /// Owner loss skips this callback and must remain an unproved retirement.
    fn retired(&mut self, _cleanup_completed: bool) {}
    /// Borrows opt-in telemetry owned by this same handler. Retain it through
    /// `retired`; the original result is recorded only after that callback
    /// returns. The default adds no observation or physical ownership.
    fn observation(&mut self) -> Option<&mut crate::obs::CaptureObservation> {
        None
    }
}

/// A finished async capture: its handler and the bounded result. A tree
/// whose cleanup did not complete was dropped on the blocking pool, which
/// retries its termination there.
pub struct CaptureOutcome<H> {
    /// The handler, returned to its owner after cleanup.
    pub handler: H,
    /// The bounded output, or the failure with partial output.
    pub result: Result<Output, ProcessCaptureFailure>,
}

impl<H: CaptureHandler> CaptureOutcome<H> {
    pub(super) fn new(mut handler: H, result: Result<Output, ProcessCaptureFailure>) -> Self {
        let cleanup_completed = result
            .as_ref()
            .map_or_else(|failure| failure.cleanup_completed, |_| true);
        handler.retired(cleanup_completed);
        if let Some(observation) = handler.observation() {
            observation.retired(
                cleanup_completed,
                result.as_ref().err().map(|failure| &failure.error),
            );
        }
        Self { handler, result }
    }
}

pub(super) struct Pump<'a> {
    deadline: Option<Instant>,
    remaining: Option<usize>,
    output: [Vec<u8>; 2],
    input: Option<PendingStdin<'a>>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    #[cfg(windows)]
    readers: [Option<std::sync::Arc<events::PipeOperation>>; 2],
    status: Option<ExitStatus>,
    streaming: bool,
    observed_start: bool,
    closed_stdin_on_start: bool,
}

/// What a turn left to do.
pub(super) enum Turn {
    /// Output ended and the child exited, or capture failed.
    Done(io::Result<()>),
    /// Nothing can move until a source or the wake time fires.
    Wait,
}

/// Runs one turn with a capture handler, calling one callback at a time.
pub(super) fn handled_turn<H: CaptureHandler>(
    pump: &mut Pump<'_>,
    tree: &mut ProcessTree,
    handler: &mut H,
    wakes: &[ProcessWake],
) -> Turn {
    for wake in wakes {
        if let Err(error) = wake.consume() {
            return Turn::Done(Err(error));
        }
    }
    let shared = std::cell::RefCell::new(handler);
    pump.turn(
        tree,
        &mut || shared.borrow_mut().check(),
        &mut |chunk| shared.borrow_mut().observe(chunk),
        &mut || shared.borrow_mut().input(),
        &mut |progress| shared.borrow_mut().progress(progress),
        None,
    )
}

impl<'a> Pump<'a> {
    pub(super) fn new(
        tree: &mut ProcessTree,
        schedule: CaptureSchedule,
        max_bytes: Option<usize>,
        input: Option<PendingStdin<'a>>,
        meter: Option<&dyn crate::NativeReadMeter>,
        output: [Vec<u8>; 2],
    ) -> Result<Self, ProcessCaptureFailure> {
        if tree.queued_stdin.is_some() {
            return Err(ProcessCaptureFailure::before_capture(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "queued stdin already belongs to the native session",
            )));
        }
        let deadline = match schedule {
            CaptureSchedule::Unbounded => None,
            CaptureSchedule::Deadline(deadline) => Some(deadline),
            CaptureSchedule::Overflow => {
                return Err(ProcessCaptureFailure::before_capture(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "process deadline overflow",
                )));
            }
        };
        let closed_stdin_on_start = input.is_none()
            && tree
                .child
                .as_ref()
                .is_some_and(|child| child.stdin.is_some());
        if input.is_none() {
            tree.capture_close_stdin(meter)
                .map_err(ProcessCaptureFailure::before_capture)?;
        }
        let child = tree.child.as_mut().ok_or_else(|| {
            ProcessCaptureFailure::before_capture(io::Error::other(
                "process output was already collected",
            ))
        })?;
        tree.output_taken = true;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        #[cfg(windows)]
        let readers = (|| {
            Ok::<_, io::Error>([
                stdout
                    .as_ref()
                    .map(events::PipeOperation::reader)
                    .transpose()?,
                stderr
                    .as_ref()
                    .map(events::PipeOperation::reader)
                    .transpose()?,
            ])
        })();
        #[cfg(windows)]
        let readers = match readers {
            Ok(readers) => readers,
            Err(error) => {
                // Return the pipes before reporting the failure.
                child.stdout = stdout;
                child.stderr = stderr;
                return Err(ProcessCaptureFailure::before_capture(error));
            }
        };
        Ok(Self {
            deadline,
            remaining: max_bytes,
            output,
            input,
            stdout,
            stderr,
            #[cfg(windows)]
            readers,
            status: None,
            streaming: false,
            observed_start: false,
            closed_stdin_on_start,
        })
    }

    pub(super) fn stream_output(&mut self) {
        self.streaming = true;
    }

    pub(super) fn take_output(&mut self) -> [Vec<u8>; 2] {
        std::mem::take(&mut self.output)
    }

    /// While output credit is withheld, only owner control or the deadline
    /// can advance capture. Readable output must not wake this wait. With
    /// neither producer, the caller waits only for its original output credit.
    pub(super) fn arm_control(
        &self,
        wakes: &[ProcessWake],
    ) -> io::Result<Option<events::Wait<'static>>> {
        if wakes.is_empty() && self.deadline.is_none() {
            return Ok(None);
        }
        let sources = wakes
            .iter()
            .map(|wake| {
                #[cfg(unix)]
                {
                    use std::os::fd::AsFd;
                    Source::Readable(wake.as_fd())
                }
                #[cfg(windows)]
                {
                    // SAFETY: the capture handler retains each event until this wait ends.
                    Source::Exit(unsafe {
                        std::os::windows::io::BorrowedHandle::borrow_raw(wake.raw())
                    })
                }
            })
            .collect::<Vec<_>>();
        events::arm(&sources, self.wake_at()).map(Some)
    }

    pub(super) fn check_deadline(&self) -> io::Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| capture_now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process output deadline exceeded; effects may have occurred",
            ));
        }
        Ok(())
    }

    fn wake_at(&self) -> Option<Instant> {
        // Tests can freeze the capture clock; native events use wall time.
        self.deadline
            .map(|deadline| Instant::now() + deadline.saturating_duration_since(capture_now()))
    }

    /// Moves everything that is ready. Never waits.
    pub(super) fn turn(
        &mut self,
        tree: &mut ProcessTree,
        check: &mut impl FnMut() -> io::Result<()>,
        observe: &mut impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        input: &mut impl FnMut() -> io::Result<ProcessInput>,
        progress: &mut impl FnMut(CaptureProgress),
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> Turn {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.advance(tree, check, observe, input, progress, meter)
        }));
        match result {
            Ok(Ok(true)) => Turn::Done(Ok(())),
            Ok(Ok(false)) => Turn::Wait,
            Ok(Err(error)) => Turn::Done(Err(error)),
            Err(panic) => {
                // A callback panic still returns pipe custody to the tree.
                self.restore(tree);
                std::panic::resume_unwind(panic)
            }
        }
    }

    fn advance(
        &mut self,
        tree: &mut ProcessTree,
        check: &mut impl FnMut() -> io::Result<()>,
        observe: &mut impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        input: &mut impl FnMut() -> io::Result<ProcessInput>,
        progress: &mut impl FnMut(CaptureProgress),
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        // Setup observations share the turn's panic boundary and physical
        // owner, just like output observations. Never call a new observer
        // before the tree, pipe buffers and handler have joined that owner.
        if !self.observed_start {
            self.observed_start = true;
            if let Some(input) = &self.input {
                progress(CaptureProgress::InputAccepted(input.accepted_bytes()));
            }
            if self.closed_stdin_on_start {
                progress(CaptureProgress::StdinClosed);
            }
        }
        loop {
            if self.streaming {
                self.check_deadline()?;
            }
            check()?;
            let mut progressed = self.input.as_mut().map_or(Ok(false), |stdin| {
                stdin.receive(input, progress)?;
                stdin.poll_observed(tree, meter, progress)
            })?;
            let [stdout_output, stderr_output] = &mut self.output;
            let before_stdout = stdout_output.len();
            let before_stderr = stderr_output.len();
            let drained = (|| {
                Ok::<_, io::Error>(
                    drain_pipe_observed(
                        &mut self.stdout,
                        stdout_output,
                        &mut self.remaining,
                        meter,
                        CaptureProgress::StdoutRead,
                        CaptureProgress::StdoutEof,
                        progress,
                    )? | drain_pipe_observed(
                        &mut self.stderr,
                        stderr_output,
                        &mut self.remaining,
                        meter,
                        CaptureProgress::StderrRead,
                        CaptureProgress::StderrEof,
                        progress,
                    )?,
                )
            })();
            if stdout_output.len() > before_stdout {
                let chunk = stdout_output
                    .get(before_stdout..)
                    .ok_or_else(|| io::Error::other("invalid observed process stdout"))?;
                if let Some(input) = self.input.as_mut().filter(|input| input.awaiting_reply()) {
                    input.observe(
                        chunk,
                        &mut |bytes| observe(ProcessOutput::Stdout(bytes)),
                        progress,
                        meter,
                    )?;
                } else {
                    observe_output(ProcessOutput::Stdout(chunk), observe)?;
                }
            }
            if stderr_output.len() > before_stderr {
                let chunk = stderr_output
                    .get(before_stderr..)
                    .ok_or_else(|| io::Error::other("invalid observed process stderr"))?;
                observe_output(ProcessOutput::Stderr(chunk), observe)?;
            }
            progressed |= drained?;
            // Exchange replies can only arrive on stdout. EOF withdraws that
            // producer even when the root remains alive with stderr open.
            if self.stdout.is_none()
                && let Some(input) = self.input.as_ref().filter(|input| input.awaiting_reply())
            {
                return Err(io::Error::new(
                    if input.undelivered() {
                        io::ErrorKind::BrokenPipe
                    } else {
                        io::ErrorKind::UnexpectedEof
                    },
                    "process stdout ended before exchange completion; effects may have occurred",
                ));
            }
            if self.status.is_none()
                && let Some(exit) = tree.try_wait(meter)?
            {
                self.status = Some(exit);
                progress(CaptureProgress::RootExited(exit));
                tree.capture_terminate_descendants(meter)?;
            }
            if self.status.is_some() && self.stdout.is_none() && self.stderr.is_none() {
                // An exchange the child left before taking all of its queued
                // input failed, whether or not a write saw the closed pipe
                // before the exit was observed.
                if self.input.as_ref().is_some_and(PendingStdin::undelivered) {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                return Ok(true);
            }
            self.check_deadline()?;
            if !progressed || (self.streaming && self.output.iter().any(|bytes| !bytes.is_empty()))
            {
                return Ok(false);
            }
        }
    }

    /// Arms one wait on every source that can make progress.
    pub(super) fn arm_with_wake(
        &self,
        tree: &mut ProcessTree,
        wakes: Option<&[ProcessWake]>,
    ) -> io::Result<events::Wait<'static>> {
        let wake_at = self.wake_at();
        let writing = self.input.as_ref().is_some_and(PendingStdin::writing);
        #[cfg(unix)]
        let wait = {
            use std::os::fd::AsFd as _;
            if self.status.is_none() {
                tree.prepare_exit_source()?;
            }
            let mut sources = Vec::with_capacity(4);
            if let Some(pipe) = &self.stdout {
                sources.push(Source::Readable(pipe.as_fd()));
            }
            if let Some(pipe) = &self.stderr {
                sources.push(Source::Readable(pipe.as_fd()));
            }
            let stdin = tree
                .child
                .as_ref()
                .and_then(|child| child.stdin.as_ref())
                .filter(|_| writing)
                .map(|pipe| pipe.as_fd());
            if let Some(pipe) = stdin {
                sources.push(Source::Writable(pipe));
            }
            if self.status.is_none() {
                sources.extend(tree.exit_source()?);
            }
            for wake in wakes.unwrap_or_default() {
                #[cfg(unix)]
                // SAFETY: each retained wake owns this descriptor through registration.
                sources.push(Source::Readable(unsafe {
                    std::os::fd::BorrowedFd::borrow_raw(wake.raw())
                }));
                #[cfg(windows)]
                // SAFETY: each retained wake owns this handle through registration.
                sources.push(Source::Exit(unsafe {
                    std::os::windows::io::BorrowedHandle::borrow_raw(wake.raw())
                }));
            }
            events::arm(&sources, wake_at)?
        };
        #[cfg(windows)]
        let wait = {
            let mut sources = Vec::with_capacity(4);
            let [stdout, stderr] = &self.readers;
            if self.stdout.is_some()
                && let Some(reader) = stdout
            {
                sources.push(Source::Pipe(reader));
            }
            if self.stderr.is_some()
                && let Some(reader) = stderr
            {
                sources.push(Source::Pipe(reader));
            }
            if writing && let Some(writer) = &tree.writer.0 {
                sources.push(Source::Pipe(writer));
            }
            if self.status.is_none()
                && let Some(child) = &tree.child
            {
                use std::os::windows::io::AsHandle as _;
                sources.push(Source::Exit(child.as_handle()));
            }
            for wake in wakes.unwrap_or_default() {
                #[cfg(unix)]
                // SAFETY: each retained wake owns this descriptor through registration.
                sources.push(Source::Readable(unsafe {
                    std::os::fd::BorrowedFd::borrow_raw(wake.raw())
                }));
                #[cfg(windows)]
                // SAFETY: each retained wake owns this handle through registration.
                sources.push(Source::Exit(unsafe {
                    std::os::windows::io::BorrowedHandle::borrow_raw(wake.raw())
                }));
            }
            events::arm(&sources, wake_at)?
        };
        // Report the original registration only after every source is armed.
        // Failed setup or owner loss drops the sender with the original tree.
        #[cfg(all(test, feature = "test-support", not(loom)))]
        if let Some(armed) = tree.capture_armed.take() {
            let _ = armed.send(());
        }
        Ok(wait)
    }

    /// Returns unread pipes to the tree so its cleanup releases them.
    pub(super) fn restore(&mut self, tree: &mut ProcessTree) {
        if let Some(child) = tree.child.as_mut() {
            child.stdout = self.stdout.take();
            child.stderr = self.stderr.take();
        }
    }

    /// Cleans up the tree and reports the bounded result.
    pub(super) fn finish(
        mut self,
        tree: &mut ProcessTree,
        result: io::Result<()>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> Result<Output, ProcessCaptureFailure> {
        // Both stopped capture and callback failure return outstanding pipe
        // custody to the original supervisor, never an unmetered drop.
        self.restore(tree);
        // Cleanup errors take precedence: a capture timeout alone does not
        // describe an unresolved termination. Keep Child ownership on error.
        let status = self.status;
        let result = tree.capture_terminate(meter).and(result).and_then(|()| {
            status.ok_or_else(|| io::Error::other("capture completed without an exit status"))
        });
        let [stdout, stderr] = self.output;
        match result {
            Ok(status) => Ok(Output {
                status,
                stdout,
                stderr,
            }),
            Err(error) => Err(ProcessCaptureFailure {
                error,
                status: self.status,
                stdout,
                stderr,
                cleanup_completed: tree.is_reaped(),
            }),
        }
    }

    /// Bytes captured so far, for the operation span.
    pub(super) fn captured(&self) -> usize {
        self.output.iter().map(Vec::len).sum()
    }
}
