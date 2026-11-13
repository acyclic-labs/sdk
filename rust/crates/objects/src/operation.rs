//! The original Auth operation budget, shared by both native transports.
use crate::Error;
use acyclic_auth::operation::Context;
use std::future::Future;
use std::time::Duration;

pub(crate) async fn run<T>(
    context: Option<&Context>,
    work: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    match context {
        Some(context) => context.run(work).await?,
        None => work.await,
    }
}

/// Semantic preparation and final admission contain no independent wait. The
/// transport owns the one Context wait around its I/O, including credentials.
pub(crate) async fn admit<T>(
    context: Option<&Context>,
    work: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    if let Some(context) = context {
        context.check()?;
    }
    let result = work.await;
    if let Some(context) = context {
        context.check()?;
    }
    result
}

/// Anchor a caller-selected relative timeout before credential resolution.
/// Context-bound requests use their original clock in the shared channel.
pub(crate) fn deadline(timeout: Option<Duration>) -> Result<Option<tokio::time::Instant>, Error> {
    timeout
        .map(|timeout| {
            tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| crate::wire::ErrorCode::InvalidArgument.into())
        })
        .transpose()
}
