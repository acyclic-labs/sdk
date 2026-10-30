//! One bounded capture, advanced in turns that never sleep.
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
    /// A capture bounded by `timeout` that checks only on its own events.
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

/// The owner callbacks of an async capture, run on the blocking pool.
pub trait CaptureHandler: Send + 'static {
    /// Transfers the fixed signals once, on the first blocking turn.
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        Ok(None.into())
    }
    /// Confirms that capture may continue; an error stops and cleans it up.
    fn check(&mut self) -> io::Result<()>;
    /// Observes newly captured output, returning exchange input.
    fn observe(&mut self, chunk: ProcessOutput<'_>) -> io::Result<ProcessInput>;
    /// Releases live execution admission after the original physical cleanup.
    /// Observation may retain this handler longer; it cannot delay retirement.
    /// Owner loss skips this callback and must remain an unproved retirement.
    fn retired(&mut self, _cleanup_completed: bool) {}
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
        handler.retired(
            result
                .as_ref()
                .map_or_else(|failure| failure.cleanup_completed, |_| true),
        );
        Self { handler, result }
    }
}

pub(super) struct Pump<'a> {
    deadline: Option<Instant>,
    remaining: usize,
    output: [Vec<u8>; 2],
    input: Option<PendingStdin<'a>>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    #[cfg(windows)]
    readers: [Option<std::sync::Arc<events::PipeOperation>>; 2],
    status: Option<ExitStatus>,
    streaming: bool,
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
        None,
    )
}

impl<'a> Pump<'a> {
    pub(super) fn new(
        tree: &mut ProcessTree,
        schedule: CaptureSchedule,
        max_bytes: usize,
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
        })
    }

    pub(super) fn stream_output(&mut self) {
        self.streaming = true;
    }

    pub(super) fn take_output(&mut self) -> [Vec<u8>; 2] {
        std::mem::take(&mut self.output)
    }

    /// While output credit is withheld, only owner control or the deadline
    /// can advance capture. Readable output must not wake this wait.
    pub(super) fn arm_control(&self, wakes: &[ProcessWake]) -> io::Result<events::Wait> {
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
        events::arm(&sources, self.wake_at())
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
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> Turn {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.advance(tree, check, observe, meter)
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
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        loop {
            if self.streaming {
                self.check_deadline()?;
            }
            check()?;
            let mut progressed = self
                .input
                .as_mut()
                .map_or(Ok(false), |stdin| stdin.poll(tree, meter))?;
            let [stdout_output, stderr_output] = &mut self.output;
            let before_stdout = stdout_output.len();
            let before_stderr = stderr_output.len();
            let drained = (|| {
                Ok::<_, io::Error>(
                    drain_pipe(&mut self.stdout, stdout_output, &mut self.remaining, meter)?
                        | drain_pipe(&mut self.stderr, stderr_output, &mut self.remaining, meter)?,
                )
            })();
            if stdout_output.len() > before_stdout {
                let chunk = stdout_output
                    .get(before_stdout..)
                    .ok_or_else(|| io::Error::other("invalid observed process stdout"))?;
                if let Some(input) = self.input.as_mut().filter(|input| !input.complete) {
                    input.observe(
                        chunk,
                        &mut |bytes| observe(ProcessOutput::Stdout(bytes)),
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
            if self.status.is_none()
                && let Some(exit) = tree.try_wait(meter)?
            {
                self.status = Some(exit);
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
    ) -> io::Result<events::Wait> {
        let wake_at = self.wake_at();
        let writing = self.input.as_ref().is_some_and(PendingStdin::writing);
        #[cfg(unix)]
        {
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
            events::arm(&sources, wake_at)
        }
        #[cfg(windows)]
        {
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
            events::arm(&sources, wake_at)
        }
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
    pub(super) fn captured(&self, max_bytes: usize) -> usize {
        max_bytes - self.remaining
    }
}
