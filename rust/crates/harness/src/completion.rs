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

/// The shared executable completion, with no post-terminal output write.
/// Embedders can consume the original typed result before OS termination.
#[derive(Debug)]
pub struct CliCompletion(Result<std::process::ExitCode, CliFailure>);

impl CliCompletion {
    /// Returns the original error and its independent observation disposition.
    pub fn into_result(self) -> Result<std::process::ExitCode, CliFailure> {
        self.0
    }

    /// Retains a setup refusal and its independent report delivery outcome.
    pub fn rejected(phase: CliPhase, cause: Failure, report: FailureReport) -> Self {
        Self(Err(CliFailure {
            phase,
            cause,
            failure_report: report,
        }))
    }

    /// Retains an operation result and its independent failure delivery outcome.
    pub fn executed(result: Result<u8, Failure>, report: Option<FailureReport>) -> Self {
        Self(
            result
                .map(std::process::ExitCode::from)
                .map_err(|cause| CliFailure {
                    phase: CliPhase::Execution,
                    cause,
                    failure_report: report.unwrap_or(FailureReport::Incomplete(None)),
                }),
        )
    }
}

impl std::process::Termination for CliCompletion {
    fn report(self) -> std::process::ExitCode {
        // The original cause and write/incomplete disposition survive until
        // this shared terminal. Neither executable renders or discards a
        // returned setup error before its completion has been settled.
        match self.0 {
            Ok(code) => code,
            Err(_failure) => std::process::ExitCode::FAILURE,
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
    tokio::select! {
        result = write => match result {
            Ok(()) => FailureReport::Written,
            Err(error) => FailureReport::Incomplete(Some(error)),
        },
        stopped = shutdown => FailureReport::Incomplete(Some(match stopped {
            Ok(_) => io::Error::new(io::ErrorKind::Interrupted, "shutdown interrupted CLI failure delivery"),
            Err(error) => io::Error::other(error),
        })),
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
    operation: impl Future<Output = Result<u8, Failure>>,
) -> (Result<u8, Failure>, io::Result<()>, Option<FailureReport>) {
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
    let failure_report = match &result {
        Err(cause) => {
            Some(report_failure(port, shutdown, CliPhase::Execution, cause.as_ref()).await)
        }
        Ok(_) => None,
    };
    (result, diagnostic, failure_report)
}

async fn finish(
    stopped: Result<u8, Arc<io::Error>>,
    graceful: bool,
    operation: impl Future<Output = Result<u8, Failure>>,
) -> Result<u8, Failure> {
    if graceful {
        operation.await
    } else {
        stopped.map_err(|error| Box::new(io::Error::other(error)) as Failure)
    }
}
