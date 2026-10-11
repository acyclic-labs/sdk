//! Deadlines interrupt only the existing invocation; durable ownership stays put.

use crate::{Error, OperationId, Result, TaskId, durable_host::CoordinatorTaskHost, scheduler::LeaseFence};
use acyclic_stream::StreamProvider;
use futures::future::{Either, select};
use std::{future::Future, pin::pin};
use crate::live::sleep_millis;

/// A timeout is not proof that a dispatched provider effect stopped.
pub(super) async fn run<P, T>(
    host: &CoordinatorTaskHost<P>,
    task: TaskId,
    fence: Option<&LeaseFence>,
    deadline: Option<u64>,
    operation: OperationId,
    future: impl Future<Output = Result<T>>,
    io: Option<&crate::filesystem::PreparationIo>,
    has_native_failure: impl Fn(&T) -> bool,
) -> Result<T>
where
    P: StreamProvider + 'static,
{
    if let Some(io) = io { io.check_cancellation()?; }
    let Some(deadline) = deadline else { return future.await; };
    if host.now_unix_millis() >= deadline {
        expire(host, task, fence, io).await?;
        return Err(Error::Indeterminate(operation));
    }
    let result = {
        let wait = pin!(wait_until(host, deadline));
        let future = pin!(future);
        match select(wait, future).await {
            Either::Left((elapsed, _)) => { elapsed?; None }
            Either::Right((result, _)) => Some(result),
        }
    };
    if let Some(result) = result {
        #[cfg(not(target_arch = "wasm32"))]
        if result.as_ref().is_err_and(|error| error.is_native_failure()) { return result; }
        if result.as_ref().is_ok_and(&has_native_failure) { return result; }
        if host.now_unix_millis() < deadline { return result; }
    }
    expire(host, task, fence, io).await?;
    Err(Error::Indeterminate(operation))
}

async fn expire<P: StreamProvider + 'static>(
    host: &CoordinatorTaskHost<P>, task: TaskId, fence: Option<&LeaseFence>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native expiry requires original observer",
        })?;
        let fence = fence.map(|fence| super::observed::clone_fence(fence, Some(io))).transpose()?;
        return host.expire_task_deadline_with_native_work(task, fence, work).await;
    }
    host.expire_task_deadline(task, fence.cloned()).await
}

/// Finite observation allowance, not a replacement execution deadline or grant.
pub(super) async fn observe<T>(
    milliseconds: u32,
    future: impl Future<Output = Result<T>>,
) -> Result<Option<T>> {
    let timer = pin!(sleep_millis(u64::from(milliseconds)));
    let future = pin!(future);
    match select(timer, future).await {
        Either::Left((elapsed, _)) => {
            elapsed?;
            Ok(None)
        }
        Either::Right((result, _)) => result.map(Some),
    }
}

async fn wait_until<P>(host: &CoordinatorTaskHost<P>, deadline: u64) -> Result<()>
where
    P: StreamProvider + 'static,
{
    loop {
        let now = host.now_unix_millis();
        if now >= deadline {
            return Ok(());
        }
        sleep_millis(deadline.saturating_sub(now).min(1_000)).await?;
    }
}

