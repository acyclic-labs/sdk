//! Shared native executable completion and wait-report custody.
//!
//! Report framing and request identity belong to the original native report port.
//! This composition keeps that port with the original operation and shutdown
//! event until the operation returns and its failure delivery is settled.

use acyclic_native_runtime::obs::{ReportPort, WaitReport};
use futures::{future::BoxFuture, future::Shared};
use std::{fmt, future::Future, io, sync::Arc};

type Failure = Box<dyn std::error::Error>;
/// One original host shutdown event, shared with the operation's cleanup path.
pub type Shutdown = Shared<BoxFuture<'static, Result<u8, Arc<io::Error>>>>;

/// The original CLI boundary at which a failure was returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CliPhase {
    /// The selected inherited completion endpoint was refused.
    ReportAdmission,
    /// The original coordination runtime could not be constructed.
    Runtime,
    /// The admitted endpoint could not enter that runtime's reactor.
    ReportRegistration,
    /// Explicit diagnostic filter or storage admission was refused.
    Diagnostics,
    /// The original tracing subscriber could not be installed.
    Subscriber,
    /// The original diagnostic output descriptor was refused.
    OutputAdmission,
    /// The admitted operation returned its own failure.
    Execution,
}

impl CliPhase {
    fn name(self) -> &'static str {
        match self {
            Self::ReportAdmission => "report-admission",
            Self::Runtime => "runtime",
            Self::ReportRegistration => "report-registration",
            Self::Diagnostics => "diagnostics",
            Self::Subscriber => "subscriber",
            Self::OutputAdmission => "output-admission",
            Self::Execution => "execution",
        }
    }
}

/// Delivery of a returned CLI failure, independent of its original cause.
#[derive(Debug)]
pub enum FailureReport {
    /// The original channel write returned successfully. Peer receipt
    /// and physical process retirement remain independent observations.
    Written,
    /// No complete failure frame was observed. `None` means no usable endpoint
    /// or runtime; `Some` retains the actual write or shutdown failure.
    Incomplete(Option<io::Error>),
}

/// The original CLI failure retained through its terminal disposition.
#[derive(Debug)]
pub struct CliFailure {
    /// Original setup or admitted-operation boundary.
    pub phase: CliPhase,
    /// The exact returned error; transport failure never replaces it.
    pub cause: Failure,
    /// The returned failure's independent delivery disposition. A diagnostic
    /// ACK does not substitute for this frame or for physical retirement.
    pub failure_report: FailureReport,
    /// Independent stderr delivery. Runtime admission failure leaves this
    /// incomplete; OS termination never starts another write.
    pub stderr_report: FailureReport,
}

impl fmt::Display for CliFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.cause, output)
    }
}

impl std::error::Error for CliFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// A nonzero exit status and its reason. An operation exits nonzero only by
/// returning one (or another error), so no status leaves without a reason.
#[derive(Debug)]
pub struct Exit {
    code: std::num::NonZeroU8,
    reason: String,
}

impl Exit {
    /// Exit status 1 with `reason`.
    #[must_use]
    pub fn failed(reason: impl Into<String>) -> Self {
        Self {
            code: std::num::NonZeroU8::MIN,
            reason: reason.into(),
        }
    }

    /// Exit status `code` with `reason`.
    #[must_use]
    pub fn new(code: std::num::NonZeroU8, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }

    /// The exit status.
    #[must_use]
    pub const fn code(&self) -> std::num::NonZeroU8 {
        self.code
    }
}

impl fmt::Display for Exit {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(&self.reason)
    }
}

impl std::error::Error for Exit {}

/// The shared executable completion. Failure output belongs to the original
/// runtime and shutdown scope; its terminal only returns the exit status.
/// Embedders can consume the original typed result before OS termination.
#[derive(Debug)]
pub struct CliCompletion(Result<(), CliFailure>);

impl CliCompletion {
    /// Returns the original error and its independent observation disposition.
    pub fn into_result(self) -> Result<(), CliFailure> {
        self.0
    }

    /// Retains a setup refusal and its independent report delivery outcome.
    pub fn rejected(phase: CliPhase, cause: Failure, report: FailureReport) -> Self {
        Self(Err(CliFailure {
            phase,
            cause,
            failure_report: report,
            stderr_report: FailureReport::Incomplete(None),
        }))
    }

    /// Retains an operation result and its independent failure delivery outcome.
    pub fn executed(result: Result<(), Failure>, report: Option<FailureReport>) -> Self {
        Self(result.map_err(|cause| CliFailure {
            phase: CliPhase::Execution,
            cause,
            failure_report: report.unwrap_or(FailureReport::Incomplete(None)),
            stderr_report: FailureReport::Incomplete(None),
        }))
    }

    /// Writes a failure through the caller's original native output admission.
    /// Success admits no descriptor and formats no text. The original shutdown
    /// event ends a blocked write without replacing the cause or its status.
    /// A synchronous native write retains custody until physical completion;
    /// an incomplete observation makes no claim that accepted bytes retired.
    pub async fn write_failure(
        mut self,
        shutdown: Shutdown,
        open: impl FnOnce() -> io::Result<acyclic_native_runtime::InheritedOutput>,
    ) -> Self {
        if let Err(failure) = &mut self.0 {
            failure.stderr_report = settle_failure(shutdown, async {
                let output = open()?;
                output.write_owned(failure.render().into_bytes()).await?;
                Ok(())
            })
            .await;
        }
        self
    }
}

impl CliFailure {
    /// The exit status: the [`Exit`]'s own, or 1 for any other error.
    #[must_use]
    pub fn code(&self) -> std::num::NonZeroU8 {
        self.cause
            .downcast_ref::<Exit>()
            .map_or(std::num::NonZeroU8::MIN, Exit::code)
    }

    /// The stderr line for delivery in the original runtime: executable and reason.
    #[must_use]
    pub fn render(&self) -> String {
        let name = std::env::args_os()
            .next()
            .and_then(|path| {
                std::path::Path::new(&path)
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "graphcoder".into());
        format!("{name}: {}\n", self.cause)
    }
}

impl std::process::Termination for CliCompletion {
    fn report(self) -> std::process::ExitCode {
        match self.0 {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(failure) => std::process::ExitCode::from(failure.code().get()),
        }
    }
}

/// Delivers the original failure through the original report port.
/// The supplied shutdown event ends a blocked write with an incomplete outcome.
pub async fn report_failure(
    port: Option<ReportPort>,
    shutdown: Shutdown,
    phase: CliPhase,
    cause: &(dyn std::error::Error + 'static),
) -> FailureReport {
    let Some(mut port) = port else {
        return FailureReport::Incomplete(None);
    };
    // The same channel's write or the original independently driven OS shutdown
    // event ends this wait. No new shutdown or result owner is introduced.
    let write = async {
        if phase == CliPhase::Execution {
            port.fail_execution(cause).await
        } else {
            port.reject_startup(phase.name(), cause).await
        }
    };
    settle_failure(shutdown, write).await
}

async fn settle_failure(
    shutdown: Shutdown,
    write: impl Future<Output = io::Result<()>>,
) -> FailureReport {
    tokio::select! {
        biased;
        stopped = shutdown => FailureReport::Incomplete(Some(match stopped {
            Ok(_) => io::Error::new(io::ErrorKind::Interrupted, "shutdown interrupted CLI failure delivery"),
            Err(error) => io::Error::new(error.kind(), error),
        })),
        result = write => match result {
            Ok(()) => FailureReport::Written,
            Err(error) => FailureReport::Incomplete(Some(error)),
        },
    }
}

// Reporting is polled alongside the original operation and shutdown. A blocked
// diagnostic sink cannot stop its producers or the original shutdown event.
/// Observes the original operation, report request and shutdown together.
///
/// A completed snapshot never completes or replaces the operation. The same
/// port retains any returned execution failure until its write or shutdown ends.
/// With graceful shutdown, the original operation owns its cleanup and terminal.
pub async fn observe(
    report: &WaitReport,
    port: io::Result<Option<ReportPort>>,
    shutdown: Shutdown,
    graceful: bool,
    operation: impl Future<Output = Result<(), Failure>>,
) -> (Result<(), Failure>, io::Result<()>, Option<FailureReport>) {
    let (mut port, mut admission_error) = match port {
        Ok(port) => (port, None),
        Err(error) => (None, Some(error)),
    };
    tokio::pin!(operation);
    let (result, mut diagnostic) = {
        let diagnostic = async {
            if let Some(error) = admission_error.take() {
                return Err(error);
            }
            let Some(port) = port.as_mut() else {
                return std::future::pending().await;
            };
            port.ready().await?;
            if let Some(request) = port.request().await? {
                let ack = port.snapshot(request, report);
                port.acknowledge(&ack).await?;
            }
            Ok(())
        };
        tokio::pin!(diagnostic);
        tokio::select! {
            result = &mut operation => (result, Ok(())),
            stopped = shutdown.clone() => (
                finish(stopped, graceful, &mut operation).await,
                Err(io::ErrorKind::Interrupted.into()),
            ),
            diagnostic = &mut diagnostic => {
                let result = tokio::select! {
                    result = &mut operation => result,
                    stopped = shutdown.clone() => finish(stopped, graceful, &mut operation).await,
                };
                (result, diagnostic)
            },
        }
    };
    if let Some(error) = admission_error {
        diagnostic = Err(error);
    }
    // A snapshot can precede the operation's terminal. Retain the same port
    // until that terminal and deliver its exact cause before returning it.
    // An `Exit` is the operation's reported outcome, not a failed operation:
    // only the latter is a report-port failure frame.
    let failure_report = match &result {
        Err(cause) if !cause.is::<Exit>() => {
            Some(report_failure(port, shutdown, CliPhase::Execution, cause.as_ref()).await)
        }
        _ => None,
    };
    (result, diagnostic, failure_report)
}

async fn finish(
    stopped: Result<u8, Arc<io::Error>>,
    graceful: bool,
    operation: impl Future<Output = Result<(), Failure>>,
) -> Result<(), Failure> {
    if graceful {
        return operation.await;
    }
    let code = stopped.map_err(|error| Box::new(io::Error::other(error)) as Failure)?;
    match std::num::NonZeroU8::new(code) {
        None => Ok(()),
        Some(code) => Err(Box::new(Exit::new(
            code,
            format!("stopped by signal (exit status {code})"),
        ))),
    }
}
