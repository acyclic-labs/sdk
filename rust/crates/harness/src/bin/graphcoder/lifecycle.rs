use acyclic_native_runtime::obs::{DiagnosticCompletion, WaitReport};
use futures::{FutureExt as _, future::BoxFuture, future::Shared};
use std::{future::Future, io, sync::Arc};
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

type Failure = Box<dyn std::error::Error>;
pub(super) type Shutdown = Shared<BoxFuture<'static, Result<u8, Arc<io::Error>>>>;

pub(super) fn subscriber(
    report: &WaitReport,
    filter: Option<String>,
) -> impl tracing::Subscriber + Send + Sync {
    tracing_subscriber::registry()
        .with(
            report
                .clone()
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target() == "acyclic.wait" && metadata.name() == "waiting"
                })),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::stderr)
                .with_filter(tracing_subscriber::EnvFilter::new(
                    filter.unwrap_or_else(|| "off".into()),
                )),
        )
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

// Snapshot before cancelling the operation: dropping its futures would erase
// the very waits that identify an interrupted turn. A diagnostic write failure
// must not replace the operation's result or interrupt graceful owner cleanup.
pub(super) async fn observe(
    report: &WaitReport,
    output: impl io::Write + Send + 'static,
    shutdown: Shutdown,
    graceful: bool,
    operation: impl Future<Output = Result<u8, Failure>>,
) -> (Result<u8, Failure>, io::Result<DiagnosticCompletion>) {
    tokio::pin!(operation);
    tokio::select! {
        result = &mut operation => (result, report.snapshot().try_write_to(output)),
        stopped = shutdown => {
            let diagnostic = report.snapshot().try_write_to(output);
            let result = if graceful {
                operation.await
            } else {
                stopped.map_err(|error| Box::new(io::Error::other(error)) as Failure)
            };
            (result, diagnostic)
        }
    }
}

#[cfg(test)]
mod tests;
