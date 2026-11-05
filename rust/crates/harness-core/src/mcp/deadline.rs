//! Native MCP operations share one optional deadline, including admission and retirement.

use crate::{Error, Result};
use std::{future::Future, time::Duration};
use tokio::time::Instant;

pub(super) fn after(timeout: Option<Duration>) -> Result<Option<Instant>> {
    timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| Error::Invalid("MCP deadline exceeds the platform clock".into()))
        })
        .transpose()
}

pub(super) async fn until<T>(
    deadline: Option<Instant>,
    operation: impl Future<Output = T>,
) -> std::result::Result<T, tokio::time::error::Elapsed> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, operation).await,
        None => Ok(operation.await),
    }
}
