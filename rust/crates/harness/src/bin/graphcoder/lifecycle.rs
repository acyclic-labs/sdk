use acyclic_harness::completion::{CliPhase, Shutdown};
use acyclic_native_runtime::{InheritedOutput, obs::WaitReport};
use acyclic_observability::{OutputFailure, TextOutput, TextReceiver};
use futures::FutureExt as _;
use std::{fmt, future::Future, io, sync::Arc};
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::SubscriberExt as _};

#[cfg(test)]
use acyclic_harness::completion::{CliCompletion, FailureReport, observe, report_failure};

#[cfg(test)]
type Failure = Box<dyn std::error::Error>;

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

pub(super) fn setup(
    report: &WaitReport,
    supplied: Option<Vec<Box<[u8]>>>,
    records: Option<usize>,
    record_bytes: Option<usize>,
) -> Result<Option<(TextReceiver, InheritedOutput)>, (CliPhase, Box<dyn std::error::Error>)> {
    let (subscriber, receiver) = subscriber(report, std::env::var("GRAPHCODER_LOG").ok(), || {
        storage(supplied, records, record_bytes)
    })
    .map_err(|error| {
        (
            CliPhase::Diagnostics,
            Box::new(error) as Box<dyn std::error::Error>,
        )
    })?;
    subscriber.try_init().map_err(|error| {
        (
            CliPhase::Subscriber,
            Box::new(error) as Box<dyn std::error::Error>,
        )
    })?;
    receiver
        .map(|receiver| InheritedOutput::stderr().map(|writer| (receiver, writer)))
        .transpose()
        .map_err(|error| {
            (
                CliPhase::OutputAdmission,
                Box::new(error) as Box<dyn std::error::Error>,
            )
        })
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
            let output = writer.take().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "original diagnostic writer is absent",
                )
            })?;
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

#[cfg(test)]
mod tests;
