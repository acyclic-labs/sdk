//! Deadlines interrupt only the existing invocation; durable ownership stays put.

use crate::{Error, OperationId, Result, TaskId, durable_host::CoordinatorTaskHost, scheduler::LeaseFence};
use acyclic_stream::StreamProvider;
use futures::future::{Either, select};
use std::{future::Future, pin::pin};

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

#[cfg(not(target_arch = "wasm32"))]
async fn sleep_millis(milliseconds: u64) -> Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn sleep_millis(milliseconds: u64) -> Result<()> {
    let mut remaining = milliseconds;
    while remaining != 0 {
        // JavaScript timers interpret delays as signed 32-bit milliseconds.
        let interval = remaining.min(2_147_483_647);
        let bounded = u32::try_from(interval)
            .map_err(|_| Error::Invalid("task timer interval is not representable".into()))?;
        sleep_timeout(bounded).await?;
        remaining -= interval;
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn sleep_timeout(milliseconds: u32) -> Result<()> {
    use wasm_bindgen::{JsCast as _, JsValue};

    struct Timeout {
        global: JsValue,
        clear: js_sys::Function,
        handle: JsValue,
    }
    impl Drop for Timeout {
        fn drop(&mut self) {
            let _ = self.clear.call1(&self.global, &self.handle);
        }
    }
    let timer_error = |error: JsValue| {
        Error::Storage(format!("task deadline timer failed: {error:?}"))
    };
    // Use the global object, not Window: browser workers have the same timers.
    let global = js_sys::global();
    let set = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .map_err(timer_error)?
        .dyn_into::<js_sys::Function>()
        .map_err(timer_error)?;
    let clear = js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
        .map_err(timer_error)?
        .dyn_into::<js_sys::Function>()
        .map_err(timer_error)?;
    let mut handle = JsValue::UNDEFINED;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        match set.call2(&global, &resolve, &JsValue::from_f64(f64::from(milliseconds))) {
            Ok(value) => handle = value,
            Err(error) => {
                let _ = reject.call1(&JsValue::UNDEFINED, &error);
            }
        }
    });
    let _timeout = Timeout { global: global.into(), clear, handle };
    wasm_bindgen_futures::JsFuture::from(promise).await.map_err(timer_error)?;
    Ok(())
}
