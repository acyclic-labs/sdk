use acyclic_native_runtime::{
    InheritedOutput,
    obs::{ReportPort, WaitReport},
};
use acyclic_observability::{OutputFailure, TextOutput, TextReceiver};
use futures::{FutureExt as _, future::BoxFuture, future::Shared};
use std::{fmt, future::Future, io, sync::Arc};
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::SubscriberExt as _};

type Failure = Box<dyn std::error::Error>;
pub(super) type Shutdown = Shared<BoxFuture<'static, Result<u8, Arc<io::Error>>>>;

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

/// Observation of a setup rejection, independent of its original cause.
#[derive(Debug)]
pub enum StartupReport {
    /// The original announcement write returned successfully. Peer receipt
    /// and physical process retirement remain independent observations.
    Written,
    /// No complete announcement was observed. `None` means no usable endpoint
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
    /// Setup rejection observation. `None` identifies an admitted operation's
    /// failure, whose diagnostic ACK is not its result or retirement receipt.
    pub startup_report: Option<StartupReport>,
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

    pub(super) fn rejected(phase: CliPhase, cause: Failure, report: StartupReport) -> Self {
        Self(Err(CliFailure {
            phase,
            cause,
            startup_report: Some(report),
        }))
    }

    pub(super) fn executed(result: Result<u8, Failure>) -> Self {
        Self(
            result
                .map(std::process::ExitCode::from)
                .map_err(|cause| CliFailure {
                    phase: CliPhase::Execution,
                    cause,
                    startup_report: None,
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

pub(super) async fn reject_startup(
    port: Option<ReportPort>,
    shutdown: Shutdown,
    phase: CliPhase,
    cause: &(dyn std::error::Error + 'static),
) -> StartupReport {
    let Some(mut port) = port else {
        return StartupReport::Incomplete(None);
    };
    // Setup has admitted no operation. Only its same announcement write or
    // the original independently driven OS shutdown event can end this wait.
    tokio::select! {
        result = port.reject_startup(phase.name(), cause) => match result {
            Ok(()) => StartupReport::Written,
            Err(error) => StartupReport::Incomplete(Some(error)),
        },
        stopped = shutdown => StartupReport::Incomplete(Some(match stopped {
            Ok(_) => io::Error::new(io::ErrorKind::Interrupted, "shutdown interrupted startup rejection"),
            Err(error) => io::Error::other(error),
        })),
    }
}

/// Refusal to admit explicitly configured diagnostic text before dispatch.
/// Its display names the configuration recovery without writing to a sink.
#[derive(Debug)]
pub enum DiagnosticConfigurationError {
    /// Logging is enabled without caller-owned buffers or a CLI reservation.
    StorageRequired,
    /// Both the caller and CLI supplied diagnostic storage.
    ConflictingStorage,
    /// The explicit storage extent or allocation was refused.
    Storage(io::Error),
    /// The configured tracing filter could not be parsed.
    Filter(tracing_subscriber::filter::ParseError),
}

// Thin native adapter for the one shared delivery loop. No output is started
// after the original terminal, including error or formatting-failure summaries.
pub(super) async fn diagnostics<F: Future>(
    original: F,
    delivery: Option<(TextReceiver, InheritedOutput)>,
) -> (F::Output, Option<OutputFailure>) {
    let Some((receiver, output)) = delivery else {
        return (original.await, None);
    };
    let mut writer = Some(output);
    receiver
        .observe_with(original, async |record| {
            let output = writer
                .take()
                .expect("one original diagnostic write at a time");
            let (output, record) = record
                .write_owned(output, InheritedOutput::write_owned)
                .await?;
            writer = Some(output);
            drop(record);
            Ok(())
        })
        .await
}

impl fmt::Display for DiagnosticConfigurationError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageRequired => output.write_str(
                "configured diagnostics require explicit text storage; configure --diagnostic-records N and --diagnostic-record-bytes B, pass owned text buffers to main_with, or set GRAPHCODER_LOG=off",
            ),
            Self::ConflictingStorage => output.write_str(
                "diagnostic storage was supplied twice; use owned main_with buffers or the paired --diagnostic-records/--diagnostic-record-bytes reservation",
            ),
            Self::Storage(error) => write!(output,
                "diagnostic storage admission failed: {error}; reduce the explicit reservation or provide admitted owned buffers to main_with"),
            Self::Filter(error) => write!(output,
                "diagnostic filter refused: {error}; set GRAPHCODER_LOG to a valid filter or off"),
        }
    }
}
impl std::error::Error for DiagnosticConfigurationError {}

pub(super) fn storage(
    supplied: Option<Vec<Box<[u8]>>>,
    records: Option<usize>,
    record_bytes: Option<usize>,
) -> Result<Option<Vec<Box<[u8]>>>, DiagnosticConfigurationError> {
    let Some((records, record_bytes)) = records.zip(record_bytes) else {
        return Ok(supplied);
    };
    if supplied.is_some() {
        return Err(DiagnosticConfigurationError::ConflictingStorage);
    }
    let reserve = || -> io::Result<_> {
        records.checked_mul(record_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "diagnostic storage extent overflows",
            )
        })?;
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(records)
            .map_err(io::Error::other)?;
        for _ in 0..records {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(record_bytes)
                .map_err(io::Error::other)?;
            bytes.resize(record_bytes, 0);
            buffers.push(bytes.into_boxed_slice());
        }
        Ok(buffers)
    };
    reserve()
        .map(Some)
        .map_err(DiagnosticConfigurationError::Storage)
}

pub(super) fn subscriber<G>(
    report: &WaitReport,
    filter: Option<String>,
    grant: G,
) -> Result<
    (impl tracing::Subscriber + Send + Sync, Option<TextReceiver>),
    DiagnosticConfigurationError,
>
where
    G: FnOnce() -> Result<Option<Vec<Box<[u8]>>>, DiagnosticConfigurationError>,
{
    let filter = filter
        .map(EnvFilter::try_new)
        .transpose()
        .map_err(DiagnosticConfigurationError::Filter)?;
    let configured = match filter {
        Some(filter)
            if <EnvFilter as Layer<Registry>>::max_level_hint(&filter)
                != Some(tracing::level_filters::LevelFilter::OFF) =>
        {
            let buffers = grant()?.ok_or(DiagnosticConfigurationError::StorageRequired)?;
            let (output, receiver) =
                TextOutput::channel(buffers).map_err(DiagnosticConfigurationError::Storage)?;
            Some((output.with_filter(filter), receiver))
        }
        _ => None,
    };
    let (text, receiver): (Option<_>, Option<_>) = configured.unzip();
    let subscriber = tracing_subscriber::registry()
        .with(
            report
                .clone()
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target() == "acyclic.wait" && metadata.name() == "waiting"
                })),
        )
        .with(text);
    Ok((subscriber, receiver))
}

// One OS termination event is shared with the serving owner's original
// graceful shutdown path. It is also observed while startup is pending.
pub(super) fn shutdown() -> Shutdown {
    async {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .map_err(Arc::new)?;
            tokio::select! {
                result = tokio::signal::ctrl_c() => result.map(|()| 130).map_err(Arc::new),
                signal = terminate.recv() => signal.map(|()| 143)
                    .ok_or_else(|| Arc::new(io::Error::from(io::ErrorKind::BrokenPipe))),
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .map(|()| 130)
            .map_err(Arc::new)
    }
    .boxed()
    .shared()
}

// Reporting is polled alongside the original operation and shutdown. A blocked
// diagnostic sink cannot stop its producers or the original shutdown event.
pub(super) async fn observe(
    report: &WaitReport,
    port: io::Result<Option<ReportPort>>,
    shutdown: Shutdown,
    graceful: bool,
    operation: impl Future<Output = Result<u8, Failure>>,
) -> (Result<u8, Failure>, io::Result<()>) {
    let diagnostic = async {
        let Some(mut port) = port? else {
            return std::future::pending().await;
        };
        port.ready().await?;
        if let Some(request) = port.request().await? {
            let ack = port.snapshot(request, report);
            port.acknowledge(&ack).await?;
        }
        Ok(())
    };
    tokio::pin!(operation);
    tokio::pin!(diagnostic);
    let diagnostic = tokio::select! {
        result = &mut operation => return (result, Ok(())),
        stopped = shutdown.clone() => return (
            finish(stopped, graceful, operation).await,
            Err(io::ErrorKind::Interrupted.into()),
        ),
        result = &mut diagnostic => result,
    };
    let result = tokio::select! {
        result = &mut operation => result,
        stopped = shutdown => finish(stopped, graceful, operation).await,
    };
    (result, diagnostic)
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

#[cfg(test)]
mod tests;
