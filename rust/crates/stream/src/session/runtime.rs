//! Executor seam for the shared session state machine.
use crate::{BoxProviderFuture, ProviderPlatform};

/// Scheduling supplied by the caller; the pool has no runtime dependency.
pub trait Executor: ProviderPlatform {
    /// Runs one owned pool operation until completion or cancellation.
    fn spawn(&self, future: BoxProviderFuture<'static, ()>);
    /// Deadline or reconnect backoff; never used to poll a follow.
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()>;
}

/// Native Tokio or browser event-loop scheduling.
#[derive(Default)]
pub struct PlatformExecutor;
#[cfg(not(target_arch = "wasm32"))]
pub(super) use acyclic_native_runtime::obs::OperationSpan;
#[cfg(target_arch = "wasm32")]
#[path = "runtime/browser.rs"]
mod browser;
#[cfg(target_arch = "wasm32")]
pub(super) use context::OperationSpan;
#[cfg(any(test, target_arch = "wasm32"))]
#[path = "runtime/context.rs"]
mod context;

#[cfg(not(target_arch = "wasm32"))]
impl Executor for PlatformExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        tokio::spawn(future);
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(milliseconds))).await
        })
    }
}
#[cfg(target_arch = "wasm32")]
impl Executor for PlatformExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        wasm_bindgen_futures::spawn_local(future);
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(browser::delay(milliseconds))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn boxed<S: futures::Stream + Send + 'static>(
    source: S,
) -> crate::BoxProviderStream<'static, S::Item> {
    Box::pin(source)
}
#[cfg(target_arch = "wasm32")]
pub(super) fn boxed<S: futures::Stream + 'static>(
    source: S,
) -> crate::BoxProviderStream<'static, S::Item> {
    Box::pin(source)
}
