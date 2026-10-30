//! The one native spawn-and-capture core. A local Machine's exec and a
//! checkout's `shell` both launch through it, so both compose, admit, spawn,
//! bound and classify a command identically: one worker submission checks
//! the owner, spawns the tree into its custody and hands the tree to its
//! capture before returning, so a lost waiter never drops an unsubmitted tree.
use super::*;
use crate::TaskSpawner;
use std::sync::Arc;

/// An original command and its capture ownership, submitted together once.
/// Command composition remains in [`ProcessCommand`]; this request adds no
/// second program, argument, environment or native process description.
pub struct CaptureRequest {
    /// The already composed original native command.
    pub command: ProcessCommand,
    /// Original physical process owner, retained through spawn acknowledgement.
    pub custody: Option<Arc<ProcessCustody>>,
    /// Original host-authorized kernel execution grant, when required.
    pub execution_grant: Option<crate::execution::ExecutionGrant>,
    /// Original literal, exchange or interactive input owner.
    pub input: Option<OwnedCaptureInput>,
    /// Original lifetime; queueing and the worker never renew it.
    pub schedule: CaptureSchedule,
    /// Shared stdout and stderr byte bound.
    pub maximum_output_bytes: usize,
}

/// Why a launch produced no capture. The two cases are distinct types of
/// outcome, so a definite refusal can never be reported as unknown.
pub enum LaunchFailure<H> {
    /// Refused before spawning, by the command itself, the owner check or
    /// the deadline: nothing ran. `failure` carries no output and
    /// `cleanup_completed = true`, so a runner reports it as a definite stop
    /// through [`ProcessCaptureFailure::stop`], like any other.
    Refused {
        /// The definite stop.
        failure: ProcessCaptureFailure,
        /// The original handler, already retired with cleanup completed.
        handler: H,
    },
    /// Spawning or its worker failed: the command may have run. Never resubmit.
    Indeterminate {
        /// Original spawn error, or producer loss before it could return one.
        error: io::Error,
        /// The original handler, retired unproved, unless the worker lost it.
        handler: Option<H>,
    },
}

impl<H> LaunchFailure<H> {
    fn lost() -> Self {
        Self::Indeterminate {
            error: io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native launch producer lost; recovery: reconcile the original command",
            ),
            handler: None,
        }
    }
}

/// How a capture stopped before its process ended on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureStop {
    /// The original deadline passed.
    Timeout,
    /// Output exceeded its byte bound.
    OutputLimit,
    /// The owner stopped the command.
    ControlStopped,
    /// Capture itself failed.
    Failed,
}

impl ProcessCaptureFailure {
    /// The one classification every runner reports.
    #[must_use]
    pub fn stop(&self) -> CaptureStop {
        match self.error.kind() {
            io::ErrorKind::TimedOut => CaptureStop::Timeout,
            io::ErrorKind::FileTooLarge => CaptureStop::OutputLimit,
            io::ErrorKind::Interrupted => CaptureStop::ControlStopped,
            _ => CaptureStop::Failed,
        }
    }
}

/// The capture owner that takes a spawned tree on the launch worker: the
/// collect or stream pump, or the scoped simulation's answer in place of a
/// process. A trait rather than an enum, so the tree moves straight into its
/// pump without a by-value variant the size of the tree.
trait Owner<H>: Send + 'static {
    type Output: Send + 'static;
    fn tree(
        self,
        tree: ProcessTree,
        schedule: CaptureSchedule,
        maximum: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> Self::Output;
    #[cfg(feature = "test-support")]
    fn answered(self, outcome: CaptureOutcome<H>) -> Self::Output;
}

struct Collect(TaskSpawner);

impl<H: CaptureHandler> Owner<H> for Collect {
    type Output = crate::TaskHandle<io::Result<CaptureOutcome<H>>>;
    fn tree(
        self,
        tree: ProcessTree,
        schedule: CaptureSchedule,
        maximum: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> Self::Output {
        self.0
            .spawn(tree.capture(schedule, maximum, input, handler))
    }
    #[cfg(feature = "test-support")]
    fn answered(self, outcome: CaptureOutcome<H>) -> Self::Output {
        self.0.spawn(async move { Ok(outcome) })
    }
}

struct Stream(TaskSpawner);

impl<H: CaptureHandler> Owner<H> for Stream {
    type Output = ProcessCaptureStream<H>;
    fn tree(
        self,
        tree: ProcessTree,
        schedule: CaptureSchedule,
        maximum: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> Self::Output {
        tree.stream_capture(&self.0, schedule, maximum, input, handler)
    }
    #[cfg(feature = "test-support")]
    fn answered(self, outcome: CaptureOutcome<H>) -> Self::Output {
        ProcessCaptureStream::answered(&self.0, outcome)
    }
}

impl CaptureRequest {
    /// Starts bounded collection and returns its original cleanup task.
    /// The task owns the tree before this method returns; dropping its waiter
    /// does not release physical custody before retirement.
    pub async fn capture<H: CaptureHandler>(
        self,
        handler: H,
    ) -> Result<crate::TaskHandle<io::Result<CaptureOutcome<H>>>, LaunchFailure<H>> {
        self.launch(handler, Collect(TaskSpawner::current())).await
    }

    /// Streams output with one batch of credit; see
    /// [`ProcessTree::stream_capture`].
    pub async fn stream<H: CaptureHandler>(
        self,
        handler: H,
    ) -> Result<ProcessCaptureStream<H>, LaunchFailure<H>> {
        self.launch(handler, Stream(TaskSpawner::current())).await
    }

    /// Submits the whole start to one native worker. `owner` receives the
    /// spawned tree there and takes custody of it before the worker returns.
    async fn launch<H: CaptureHandler, O: Owner<H>>(
        self,
        handler: H,
        owner: O,
    ) -> Result<O::Output, LaunchFailure<H>> {
        let submitted = crate::run_blocking_io(
            move || {
                // A panicking owner callback loses the handler, never the tree:
                // unwinding drops it, which terminates it on this worker.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    self.start(handler, owner)
                }))
                .unwrap_or_else(|_| Err(LaunchFailure::lost()))
            },
            None,
        );
        match submitted {
            Ok(started) => started.await.unwrap_or_else(|_| Err(LaunchFailure::lost())),
            Err(_) => Err(LaunchFailure::lost()),
        }
    }

    fn start<H: CaptureHandler, O: Owner<H>>(
        self,
        mut handler: H,
        owner: O,
    ) -> Result<O::Output, LaunchFailure<H>> {
        let refused = |error: io::Error, mut handler: H| {
            handler.retired(true);
            Err(LaunchFailure::Refused {
                failure: ProcessCaptureFailure {
                    error,
                    status: None,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    cleanup_completed: true,
                },
                handler,
            })
        };
        if self.command.get_program().is_empty()
            || self.command.get_program().as_encoded_bytes().contains(&0)
            || self
                .command
                .get_args()
                .any(|argument| argument.as_encoded_bytes().contains(&0))
        {
            return refused(io::ErrorKind::InvalidInput.into(), handler);
        }
        if let Err(error) = handler.check() {
            return refused(error, handler);
        }
        match self.schedule {
            CaptureSchedule::Deadline(deadline) if capture_now() >= deadline => {
                return refused(io::ErrorKind::TimedOut.into(), handler);
            }
            CaptureSchedule::Overflow => {
                return refused(io::ErrorKind::InvalidInput.into(), handler);
            }
            CaptureSchedule::Unbounded | CaptureSchedule::Deadline(_) => {}
        }
        let mut command = self.command;
        command
            .stdin(if self.input.is_some() {
                ProcessStdio::piped()
            } else {
                ProcessStdio::null()
            })
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        #[cfg(feature = "test-support")]
        if self.execution_grant.is_none() {
            // The boundary calls one callback at a time, as a live pump does.
            let shared = std::cell::RefCell::new(&mut handler);
            let answered = crate::testing::capture_process(
                crate::testing::ProcessCapture {
                    command: &command,
                    schedule: self.schedule,
                    maximum: self.maximum_output_bytes,
                    input: self.input.as_ref(),
                },
                &mut || shared.borrow_mut().check(),
                &mut |chunk| shared.borrow_mut().observe(chunk),
            );
            if let Some(result) = answered {
                return Ok(owner.answered(CaptureOutcome::new(handler, result)));
            }
        }
        let tree = match (&self.execution_grant, &self.custody) {
            (Some(grant), custody) => {
                ProcessTree::spawn_granted(command, grant, custody.as_deref())
            }
            (None, Some(custody)) => ProcessTree::spawn_in_custody(&mut command, custody),
            (None, None) => ProcessTree::spawn(&mut command),
        };
        let tree = match tree {
            Ok(tree) => tree,
            Err(error) => {
                handler.retired(false);
                return Err(LaunchFailure::Indeterminate {
                    error,
                    handler: Some(handler),
                });
            }
        };
        Ok(owner.tree(
            tree,
            self.schedule,
            self.maximum_output_bytes,
            self.input,
            handler,
        ))
    }
}

#[cfg(test)]
mod tests;
