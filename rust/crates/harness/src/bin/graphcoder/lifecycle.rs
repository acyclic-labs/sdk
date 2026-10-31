use acyclic_native_runtime::obs::{ReportPort, WaitReport};
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
