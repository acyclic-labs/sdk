//! The one native spawn-and-capture core. A local Machine's exec and a
//! checkout's `shell` both launch through it, so both compose, admit, spawn,
//! bound and classify a command identically: one worker submission checks
//! the owner, spawns the tree into its custody and hands the tree to its
//! capture before returning, so a lost waiter never drops an unsubmitted tree.
use super::*;
use crate::TaskSpawner;
use std::{
    borrow::Borrow, collections::BTreeMap, ffi::OsString, path::PathBuf, process::Stdio, sync::Arc,
};

/// Host-owned configuration shared by every command one host launches.
/// Callers that launch many commands share it behind an `Arc`, so a launch
/// copies neither its environment nor its working directory.
pub struct NativeHost {
    /// Absolute directory a command's own `cwd` resolves against.
    pub cwd: PathBuf,
    /// Explicit environment, applied after any inheritance.
    pub environment: BTreeMap<String, String>,
    /// Whether commands also inherit this process's environment.
    pub inherit_environment: bool,
    /// Owner custody that ties each tree to this host and fences it on
    /// reopen; `None` spawns an ordinary owned tree.
    pub custody: Option<Arc<ProcessCustody>>,
}

impl NativeHost {
    /// Refuses a relative directory or an environment the OS cannot carry.
    /// Validate once when binding a host; a launch does not repeat it.
    pub fn validate(&self) -> io::Result<()> {
        if !self.cwd.is_absolute()
            || self.environment.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native host needs an absolute directory and a native environment",
            ));
        }
        #[cfg(windows)]
        if self
            .environment
            .keys()
            .map(|key| key.to_uppercase())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != self.environment.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate native environment key under Windows case folding",
            ));
        }
        Ok(())
    }
}

/// One composed command, launched exactly once.
pub struct NativeCommand {
    /// Program, then its arguments. A shell command is composed by
    /// [`acyclic_host_platform::shell::ShellSpec::compose`].
    pub argv: Vec<OsString>,
    /// Directory relative to the host's, or the host's own when `None`.
    pub cwd: Option<PathBuf>,
    /// Exact stdin bytes followed by EOF; `None` gives the child a null stdin.
    pub stdin: Option<Vec<u8>>,
    /// The original admission's deadline. Queueing and the worker never renew it.
    pub deadline: Instant,
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
        /// The original handler, retired unproved, unless the worker lost it.
        handler: Option<H>,
    },
}

impl<H> LaunchFailure<H> {
    const fn lost() -> Self {
        Self::Indeterminate { handler: None }
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

impl NativeCommand {
    /// Collects bounded output, observing borrowed chunks through `handler`.
    /// The capture owns the tree on its own task through cleanup, so a
    /// cancelled caller never leaves a tree behind. A lost producer is
    /// [`LaunchFailure::Indeterminate`] without the handler.
    pub async fn capture<H: CaptureHandler>(
        self,
        host: impl Borrow<NativeHost> + Send + 'static,
        handler: H,
    ) -> Result<CaptureOutcome<H>, LaunchFailure<H>> {
        self.launch(host, handler, Collect(TaskSpawner::current()))
            .await?
            .await
            .and_then(std::convert::identity)
            .map_err(|_| LaunchFailure::lost())
    }

    /// Streams output with one batch of credit; see
    /// [`ProcessTree::stream_capture`].
    pub async fn stream<H: CaptureHandler>(
        self,
        host: impl Borrow<NativeHost> + Send + 'static,
        handler: H,
    ) -> Result<ProcessCaptureStream<H>, LaunchFailure<H>> {
        self.launch(host, handler, Stream(TaskSpawner::current()))
            .await
    }

    /// Submits the whole start to one native worker. `owner` receives the
    /// spawned tree there and takes custody of it before the worker returns.
    async fn launch<H: CaptureHandler, O: Owner<H>>(
        self,
        host: impl Borrow<NativeHost> + Send + 'static,
        handler: H,
        owner: O,
    ) -> Result<O::Output, LaunchFailure<H>> {
        let submitted = crate::run_blocking_io(
            move || {
                // A panicking owner callback loses the handler, never the tree:
                // unwinding drops it, which terminates it on this worker.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    self.start(host.borrow(), handler, owner)
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
        host: &NativeHost,
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
        let Some((program, arguments)) = self.argv.split_first() else {
            return refused(io::ErrorKind::InvalidInput.into(), handler);
        };
        if self
            .argv
            .iter()
            .any(|argument| argument.as_encoded_bytes().contains(&0))
        {
            return refused(io::ErrorKind::InvalidInput.into(), handler);
        }
        if let Err(error) = handler.check() {
            return refused(error, handler);
        }
        if capture_now() >= self.deadline {
            return refused(io::ErrorKind::TimedOut.into(), handler);
        }
        let mut command = Command::new(program);
        if !host.inherit_environment {
            command.env_clear();
        }
        command
            .args(arguments)
            .current_dir(
                self.cwd
                    .map(|cwd| host.cwd.join(cwd))
                    .as_ref()
                    .unwrap_or(&host.cwd),
            )
            .envs(&host.environment)
            .stdin(if self.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(feature = "test-support")]
        {
            // The boundary calls one callback at a time, as a live pump does.
            let shared = std::cell::RefCell::new(&mut handler);
            let answered = crate::testing::capture_process(
                crate::testing::ProcessCapture {
                    command: &command,
                    deadline: self.deadline,
                    maximum: self.maximum_output_bytes,
                    input: self.stdin.as_deref().map(CaptureInput::Literal),
                },
                &mut || shared.borrow_mut().check(),
                &mut |chunk| shared.borrow_mut().observe(chunk),
            );
            if let Some(result) = answered {
                return Ok(owner.answered(CaptureOutcome::new(handler, result)));
            }
        }
        let tree = match &host.custody {
            Some(custody) => ProcessTree::spawn_in_custody(&mut command, custody),
            None => ProcessTree::spawn(&mut command),
        };
        let Ok(tree) = tree else {
            handler.retired(false);
            return Err(LaunchFailure::Indeterminate {
                handler: Some(handler),
            });
        };
        Ok(owner.tree(
            tree,
            CaptureSchedule::until(self.deadline),
            self.maximum_output_bytes,
            self.stdin.map(OwnedCaptureInput::Literal),
            handler,
        ))
    }
}

#[cfg(test)]
mod tests;
