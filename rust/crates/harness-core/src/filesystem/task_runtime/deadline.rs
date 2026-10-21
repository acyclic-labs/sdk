//! Deadlines interrupt only the existing invocation; durable ownership stays put.

use crate::conversation::BoundedContentFuture;
use crate::live::sleep_millis;
use crate::{
    Error, OperationId, Result, TaskId, durable_host::CoordinatorTaskHost, scheduler::LeaseFence,
};
use acyclic_stream::StreamProvider;
use futures::future::{Either, select};
use std::{future::Future, pin::pin};

/// A timeout is not proof that a dispatched provider effect stopped.
pub(super) async fn run<P, T, F>(
    host: &CoordinatorTaskHost<P>,
    task: TaskId,
    fence: Option<&LeaseFence>,
    deadline: Option<u64>,
    operation: OperationId,
    build: impl FnOnce() -> F,
    io: Option<&crate::filesystem::PreparationIo>,
    has_native_failure: impl Fn(&T) -> bool,
) -> Result<T>
where
    P: StreamProvider + 'static,
    F: Future<Output = Result<T>> + acyclic_stream::ProviderTask,
{
    if let Some(io) = io {
        io.check_cancellation()?;
    }
    if deadline.is_some_and(|deadline| host.now_unix_millis() >= deadline) {
        expire(host, task, fence, io)?.await?;
        return Err(Error::Indeterminate(operation));
    }
    let future = crate::conversation::BoundedContentFuture::new(io, build)?;
    let Some(deadline) = deadline else {
        return future.await;
    };
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
    if let Some(result) = result {
        #[cfg(not(target_arch = "wasm32"))]
        if result
            .as_ref()
            .is_err_and(|error| error.is_native_failure())
        {
            return result;
        }
        if result.as_ref().is_ok_and(&has_native_failure) {
            return result;
        }
        if host.now_unix_millis() < deadline {
            return result;
        }
    }
    expire(host, task, fence, io)?.await?;
    Err(Error::Indeterminate(operation))
}

/// Finite observation allowance, not a replacement execution deadline or grant.
pub(super) async fn observe<T, F>(
    milliseconds: u32,
    build: impl FnOnce() -> F,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Option<T>>
where
    F: Future<Output = Result<T>> + acyclic_stream::ProviderTask,
{
    let future = crate::conversation::BoundedContentFuture::new(io, build)?;
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

fn expire<'a, P: StreamProvider + 'static>(
    host: &'a CoordinatorTaskHost<P>,
    task: TaskId,
    fence: Option<&'a LeaseFence>,
    io: Option<&'a crate::filesystem::PreparationIo>,
) -> Result<BoundedContentFuture<'a, ()>> {
    BoundedContentFuture::new(io, || async move {
        host.expire_task_deadline(
            task,
            fence,
            &mut super::observed::history(io)?
                .as_mut()
                .map(|work| work as &mut dyn crate::store::HistoryIo),
        )
        .await
    })
}
