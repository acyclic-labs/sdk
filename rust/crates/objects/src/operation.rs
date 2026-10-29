//! The original Auth operation budget, shared by both native transports.
use crate::Error;
use acyclic_auth::operation::Context;
use std::future::Future;
#[cfg(feature = "grpc")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The largest timeout the gRPC wire format can carry: eight digits of hours.
#[cfg(feature = "grpc")]
pub(super) const MAX_DEADLINE: Duration = Duration::from_secs(99_999_999 * 3600);

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

/// Project the original absolute deadline once; resolution consumes this same
/// budget. The host's Context event remains the local authority for stopping.
#[cfg(feature = "grpc")]
pub(crate) fn deadline(
    context: Option<&Context>,
    fallback: Duration,
) -> Result<tokio::time::Instant, Error> {
    let remaining = match context {
        Some(context) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::from(crate::wire::ErrorCode::Unavailable))?;
            Duration::from_millis(context.deadline_unix_millis())
                .checked_sub(now)
                .filter(|remaining| !remaining.is_zero())
                .ok_or(acyclic_auth::operation::Stopped::Deadline)?
        }
        None => fallback,
    };
    if remaining > MAX_DEADLINE {
        return Err(crate::wire::ErrorCode::InvalidArgument.into());
    }
    tokio::time::Instant::now()
        .checked_add(remaining)
        .ok_or_else(|| crate::wire::ErrorCode::InvalidArgument.into())
}
