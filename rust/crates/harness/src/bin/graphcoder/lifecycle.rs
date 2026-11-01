use acyclic_native_runtime::obs::{ReportPort, WaitReport};
use acyclic_observability::{TextOutput, TextReceiver};
use futures::{FutureExt as _, future::BoxFuture, future::Shared};
use std::{fmt, future::Future, io, sync::Arc};
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::SubscriberExt as _};

type Failure = Box<dyn std::error::Error>;
pub(super) type Shutdown = Shared<BoxFuture<'static, Result<u8, Arc<io::Error>>>>;

#[derive(Debug)]
pub(super) enum DiagnosticConfigurationError {
    StorageRequired,
    ConflictingStorage,
    Storage(io::Error),
    Filter(tracing_subscriber::filter::ParseError),
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
