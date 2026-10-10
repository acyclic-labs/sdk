//! Deadlines interrupt only the existing invocation; durable ownership stays put.

use crate::{Error, OperationId, Result, TaskId, durable_host::CoordinatorTaskHost, scheduler::LeaseFence};
use acyclic_stream::StreamProvider;
use futures::future::{Either, select};
use std::{future::Future, pin::pin};
use crate::live::sleep_millis;

/// A timeout is not proof that a dispatched provider effect stopped. Cancel the
/// original operation, keep its exact reservation, and leave retained intents
/// available to the existing settlement/reconciliation APIs.
pub(super) async fn run<P, T>(
    host: &CoordinatorTaskHost<P>,
    task: TaskId,
    fence: Option<&LeaseFence>,
    deadline: Option<u64>,
    operation: OperationId,
    future: impl Future<Output = Result<T>>,
) -> Result<T>
where
    P: StreamProvider + 'static,
{
    let Some(deadline) = deadline else {
        return future.await;
    };
    if host.now_unix_millis() >= deadline {
        host.expire_task_deadline(task, fence.cloned()).await?;
        return Err(Error::Indeterminate(operation));
    }
    // Poll the timer first: unavailable timer machinery fails before any effect.
    let result = {
        let wait = pin!(wait_until(host, deadline));
        let future = pin!(future);
        match select(wait, future).await {
            Either::Left((elapsed, _)) => {
                elapsed?;
                None
            }
            Either::Right((result, _)) => Some(result),
        }
    };
    // Drop the invocation before cancellation: it may hold a coordinator guard.
    // A synchronously-ready callback can cross the boundary within one poll.
    if let Some(result) = result {
        // A failed native admission is not evidence of a timed-out effect.
        // Preserve its cause/custody before clocks or cancellation perform IO.
        #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
        if matches!(&result,Err(Error::NativeWork {..})) { return result; }
        if host.now_unix_millis() < deadline {
            return result;
        }
    }
    host.expire_task_deadline(task, fence.cloned()).await?;
    Err(Error::Indeterminate(operation))
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
        // Re-read the injected owner clock after finite sleeps, including clock
        // jumps. The deadline is never replaced by a process-relative Instant.
        sleep_millis(deadline.saturating_sub(now).min(1_000)).await?;
    }
}

